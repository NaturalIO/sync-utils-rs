use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crossfire::*;
use tokio::{runtime::Runtime, time::timeout};

use super::*;

/*
 * WorkerPool that submit side without tokio Context, has one monitor runs in tokio runtime to
 * spawn and adjust long-lived workers
 */

#[allow(unused_must_use)]
pub struct WorkerPool<M: Send + Sized + Unpin + 'static, W: Worker<M>, S: WorkerPoolImpl<M, W>>(
    Arc<WorkerPoolInner<M, W, S>>,
);

struct WorkerPoolInner<M, W, S>
where
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
{
    worker_count: AtomicUsize,
    sender: MTx<Option<M>>,
    recv: MAsyncRx<Option<M>>,
    min_workers: usize,
    max_workers: usize,
    worker_timeout: Duration,
    inner: S,
    water: AtomicUsize,
    phantom: std::marker::PhantomData<W>, // to avoid complaining unused param
    closing: AtomicBool,
    notify_sender: MTx<Option<()>>,
    auto: bool,
    buffer_size: usize,
}

impl<M, W, S> Clone for WorkerPool<M, W, S>
where
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
{
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M, W, S> WorkerPool<M, W, S>
where
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
{
    pub fn new(
        inner: S, min_workers: usize, max_workers: usize, mut buffer_size: usize,
        worker_timeout: Duration, rt: &Runtime,
    ) -> Self {
        if buffer_size > max_workers * 2 {
            buffer_size = max_workers * 2;
        }
        let (sender, recv) = mpmc::bounded_tx_blocking_rx_async(buffer_size);
        let (noti_sender, noti_recv) = mpmc::bounded_tx_blocking_rx_async(1);
        assert!(min_workers > 0);
        assert!(max_workers >= min_workers);

        let auto: bool = min_workers < max_workers;
        if auto {
            assert!(worker_timeout != ZERO_DUARTION);
        }

        let pool = Arc::new(WorkerPoolInner {
            sender,
            recv,
            inner,
            worker_count: AtomicUsize::new(0),
            min_workers,
            max_workers,
            buffer_size,
            worker_timeout,
            phantom: Default::default(),
            closing: AtomicBool::new(false),
            water: AtomicUsize::new(0),
            notify_sender: noti_sender,
            auto,
        });
        let _pool = pool.clone();
        rt.spawn(async move {
            _pool.monitor(noti_recv).await;
        });
        Self(pool)
    }

    pub fn get_inner(&self) -> &S {
        &self.0.inner
    }

    // sending None will notify worker close
    // return non-null on send fail
    pub fn submit(&self, msg: M) -> Option<M> {
        let _self = self.0.as_ref();
        if _self.closing.load(Ordering::Acquire) {
            return Some(msg);
        }
        if _self.auto {
            let worker_count = _self.get_worker_count();
            let water = _self.water.fetch_add(1, Ordering::SeqCst);
            if worker_count < _self.max_workers {
                if water > worker_count + 1 || water > _self.buffer_size {
                    let _ = _self.notify_sender.try_send(Some(()));
                }
            }
        }
        match _self.sender.send(Some(msg)) {
            Ok(_) => return None,
            Err(SendError(t)) => return t,
        }
    }

    pub fn close(&self) {
        let _self = self.0.as_ref();
        if _self.closing.swap(true, Ordering::SeqCst) {
            return;
        }
        loop {
            let cur = self.get_worker_count();
            if cur == 0 {
                break;
            }
            debug!("worker pool closing: cur workers {}", cur);
            for _ in 0..cur {
                let _ = _self.sender.send(None);
            }
            std::thread::sleep(_self.worker_timeout);
            // TODO graceful exec all remaining
        }
        // Because bound is 1, send twice to unsure monitor coroutine exit
        let _ = _self.notify_sender.send(None);
        let _ = _self.notify_sender.send(None);
    }

    pub fn get_worker_count(&self) -> usize {
        self.0.get_worker_count()
    }
}

impl<M, W, S> WorkerPoolInner<M, W, S>
where
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
{
    async fn run_worker_simple(&self, mut worker: W) {
        if let Err(_) = worker.init().await {
            let _ = self.try_exit();
            worker.on_exit();
            return;
        }

        let recv = &self.recv;
        'WORKER_LOOP: loop {
            match recv.recv().await {
                Ok(item) => {
                    if item.is_none() {
                        let _ = self.try_exit();
                        break 'WORKER_LOOP;
                    }
                    worker.run(item.unwrap()).await;
                }
                Err(_) => {
                    // channel closed worker exit
                    let _ = self.try_exit();
                    break 'WORKER_LOOP;
                }
            }
        }
        worker.on_exit();
    }

    async fn run_worker_adjust(&self, mut worker: W) {
        if let Err(_) = worker.init().await {
            let _ = self.try_exit();
            worker.on_exit();
            return;
        }

        let worker_timeout = self.worker_timeout;
        let recv = &self.recv;
        let mut is_idle = false;
        'WORKER_LOOP: loop {
            if is_idle {
                match timeout(worker_timeout, recv.recv()).await {
                    Ok(res) => {
                        match res {
                            Ok(item) => {
                                if item.is_none() {
                                    let _ = self.try_exit();
                                    break 'WORKER_LOOP;
                                }
                                worker.run(item.unwrap()).await;
                                is_idle = false;
                                self.water.fetch_sub(1, Ordering::SeqCst);
                            }
                            Err(_) => {
                                // channel closed worker exit
                                let _ = self.try_exit();
                                worker.on_exit();
                            }
                        }
                    }
                    Err(_) => {
                        // timeout
                        if self.try_exit() {
                            break 'WORKER_LOOP;
                        }
                    }
                }
            } else {
                match recv.try_recv() {
                    Err(e) => {
                        if e.is_empty() {
                            is_idle = true;
                        } else {
                            let _ = self.try_exit();
                            break 'WORKER_LOOP;
                        }
                    }
                    Ok(Some(item)) => {
                        worker.run(item).await;
                        self.water.fetch_sub(1, Ordering::SeqCst);
                        is_idle = false;
                    }
                    Ok(None) => {
                        let _ = self.try_exit();
                        break 'WORKER_LOOP;
                    }
                }
            }
        }
        worker.on_exit();
    }

    #[inline(always)]
    pub fn get_worker_count(&self) -> usize {
        self.worker_count.load(Ordering::Acquire)
    }

    #[inline(always)]
    fn spawn(self: Arc<Self>) {
        self.worker_count.fetch_add(1, Ordering::SeqCst);
        let worker = self.inner.spawn();
        let _self = self.clone();
        tokio::spawn(async move {
            if _self.auto {
                _self.run_worker_adjust(worker).await
            } else {
                _self.run_worker_simple(worker).await
            }
        });
    }

    // check if idle worker should exit
    #[inline(always)]
    fn try_exit(&self) -> bool {
        if self.closing.load(Ordering::Acquire) {
            self.worker_count.fetch_sub(1, Ordering::SeqCst);
            return true;
        }
        if self.get_worker_count() > self.min_workers {
            if self.worker_count.fetch_sub(1, Ordering::SeqCst) <= self.min_workers {
                self.worker_count.fetch_add(1, Ordering::SeqCst); // rollback
            } else {
                return true; // worker exit
            }
        }
        return false;
    }

    async fn monitor(self: Arc<Self>, noti_recv: MAsyncRx<Option<()>>) {
        for _ in 0..self.min_workers {
            self.clone().spawn();
        }
        loop {
            if let Ok(Some(_)) = noti_recv.recv().await {
                if self.auto {
                    let worker_count = self.get_worker_count();
                    if worker_count > self.max_workers {
                        continue;
                    }
                    let mut pending_msg = self.sender.len();
                    if pending_msg > worker_count {
                        pending_msg -= worker_count;
                        if pending_msg > self.max_workers - worker_count {
                            pending_msg = self.max_workers - worker_count;
                        }
                        for _ in 0..pending_msg {
                            self.clone().spawn();
                        }
                    }
                } else {
                    continue;
                }
            } else {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::thread;

    use crossbeam::channel::{Sender, bounded};
    use tokio::time::{Duration, sleep};

    use super::*;

    #[allow(dead_code)]
    struct MyWorkerPoolImpl();

    struct MyWorker();

    struct MyMsg(i64, Sender<()>);

    impl WorkerPoolImpl<MyMsg, MyWorker> for MyWorkerPoolImpl {
        fn spawn(&self) -> MyWorker {
            MyWorker()
        }
    }

    #[async_trait]
    impl Worker<MyMsg> for MyWorker {
        async fn run(&mut self, msg: MyMsg) {
            sleep(Duration::from_millis(1)).await;
            println!("done {}", msg.0);
            let _ = msg.1.send(());
        }
    }

    type MyWorkerPool = WorkerPool<MyMsg, MyWorker, MyWorkerPoolImpl>;

    #[test]
    fn blocking_workerpool_adjust() {
        let min_workers = 1;
        let max_workers = 4;
        let worker_timeout = Duration::from_secs(1);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        let worker_pool = MyWorkerPool::new(
            MyWorkerPoolImpl(),
            min_workers,
            max_workers,
            10,
            worker_timeout,
            &rt,
        );

        let mut th_s = Vec::new();
        for i in 0..8 {
            let _pool = worker_pool.clone();
            th_s.push(thread::spawn(move || {
                let (done_tx, done_rx) = bounded(10);
                for j in 0..10 {
                    _pool.submit(MyMsg(i * 10 + j, done_tx.clone()));
                }
                for _j in 0..10 {
                    let _ = done_rx.recv();
                }
            }));
        }
        for th in th_s {
            let _ = th.join();
        }
        let workers = worker_pool.get_worker_count();
        println!("cur workers {} should reach max", workers);
        assert_eq!(workers, max_workers);

        thread::sleep(worker_timeout * 2);
        let workers = worker_pool.get_worker_count();
        println!("cur workers: {}, extra should exit due to timeout", workers);
        assert_eq!(workers, min_workers);

        let (done_tx, done_rx) = bounded(2);
        for j in 0..10 {
            worker_pool.submit(MyMsg(80 + j, done_tx.clone()));
            println!("send {}", j);
            let _ = done_rx.recv();
        }
        println!("closing");
        worker_pool.close();
        assert_eq!(worker_pool.get_worker_count(), 0);
        assert_eq!(worker_pool.0.water.load(Ordering::Acquire), 0)
    }

    #[test]
    fn blocking_workerpool_fixed() {
        let min_workers = 4;
        let max_workers = 4;
        let worker_timeout = Duration::from_secs(1);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        let worker_pool = MyWorkerPool::new(
            MyWorkerPoolImpl(),
            min_workers,
            max_workers,
            10,
            worker_timeout,
            &rt,
        );

        let mut th_s = Vec::new();
        for i in 0..8 {
            let _pool = worker_pool.clone();
            th_s.push(thread::spawn(move || {
                let (done_tx, done_rx) = bounded(10);
                for j in 0..10 {
                    _pool.submit(MyMsg(i * 10 + j, done_tx.clone()));
                }
                for _j in 0..10 {
                    let _ = done_rx.recv();
                }
            }));
        }
        for th in th_s {
            let _ = th.join();
        }
        let workers = worker_pool.get_worker_count();
        println!("cur workers {} should reach max", workers);
        assert_eq!(workers, max_workers);

        thread::sleep(worker_timeout * 2);
        let workers = worker_pool.get_worker_count();
        println!("cur workers {} should reach max", workers);
        assert_eq!(workers, max_workers);

        let (done_tx, done_rx) = bounded(2);
        for j in 0..10 {
            worker_pool.submit(MyMsg(80 + j, done_tx.clone()));
            println!("send {}", j);
            let _ = done_rx.recv();
        }
        println!("closing");
        worker_pool.close();
        assert_eq!(worker_pool.get_worker_count(), 0);
        assert_eq!(worker_pool.0.water.load(Ordering::Acquire), 0)
    }
}
