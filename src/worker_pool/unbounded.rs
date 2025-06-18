use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use crossfire::{mpmc, mpsc};
use tokio::time::{sleep, timeout};

use super::*;

#[allow(unused_must_use)]
pub struct WorkerPoolUnbounded<
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
>(Arc<WorkerPoolUnboundedInner<M, W, S>>);

struct WorkerPoolUnboundedInner<M, W, S>
where
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
{
    worker_count: AtomicUsize,
    sender: mpmc::TxUnbounded<Option<M>>,
    recv: mpmc::RxUnbounded<Option<M>>,
    min_workers: usize,
    max_workers: usize,
    worker_timeout: Duration,
    inner: S,
    phantom: std::marker::PhantomData<W>, // to avoid complaining unused param
    closing: AtomicBool,
    notify_sender: mpsc::TxBlocking<Option<()>, mpsc::SharedSenderBRecvF>,
    notify_recv: mpsc::RxFuture<Option<()>, mpsc::SharedSenderBRecvF>,
    water: AtomicUsize,
    auto: bool,
    real_thread: AtomicBool,
}

impl<M, W, S> Clone for WorkerPoolUnbounded<M, W, S>
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

impl<M, W, S> WorkerPoolUnbounded<M, W, S>
where
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
{
    pub fn new(inner: S, min_workers: usize, max_workers: usize, worker_timeout: Duration) -> Self {
        assert!(min_workers > 0);
        assert!(max_workers >= min_workers);

        let auto: bool = min_workers < max_workers;
        if auto {
            assert!(worker_timeout != ZERO_DUARTION);
        }
        let (sender, recv) = mpmc::unbounded_future();
        let (noti_sender, noti_recv) = mpsc::bounded_tx_blocking_rx_future(1);
        let pool = Arc::new(WorkerPoolUnboundedInner {
            sender,
            recv,
            inner,
            worker_count: AtomicUsize::new(0),
            min_workers,
            max_workers,
            worker_timeout,
            phantom: Default::default(),
            closing: AtomicBool::new(false),
            notify_sender: noti_sender,
            notify_recv: noti_recv,
            water: AtomicUsize::new(0),
            auto,
            real_thread: AtomicBool::new(false),
        });
        Self(pool)
    }

    pub fn get_inner(&self) -> &S {
        &self.0.inner
    }

    // If worker contains blocking logic, run worker in seperate threads
    pub fn set_use_thread(&mut self, ok: bool) {
        self.0.real_thread.store(ok, Ordering::Release);
    }

    pub async fn start(&self) {
        let _self = self.0.as_ref();
        for _ in 0.._self.min_workers {
            self.0.clone().spawn();
        }
        if _self.auto {
            let _pool = self.0.clone();
            tokio::spawn(async move {
                _pool.monitor().await;
            });
        }
    }

    pub async fn try_spawn(&self, num: usize) {
        let _self = self.0.as_ref();
        if !_self.auto {
            return;
        }
        for _ in 0..num {
            if _self.get_worker_count() >= _self.max_workers {
                return;
            }
            self.0.clone().spawn();
        }
    }

    pub async fn close(&self) {
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
            sleep(Duration::from_secs(1)).await;
            // TODO graceful exec all remaining
        }
        let _ = _self.notify_sender.try_send(None);
    }

    pub fn get_worker_count(&self) -> usize {
        self.0.get_worker_count()
    }
}

impl<M, W, S> WorkerPoolInf<M> for WorkerPoolUnbounded<M, W, S>
where
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
{
    // return non-null on send fail
    #[inline]
    fn submit(&self, msg: M) -> Option<M> {
        let _self = self.0.as_ref();
        if _self.closing.load(Ordering::Acquire) {
            return Some(msg);
        }
        if _self.auto {
            let worker_count = _self.get_worker_count();
            let water = _self.water.fetch_add(1, Ordering::SeqCst);
            if worker_count < _self.max_workers && water > worker_count + 1 {
                let _ = _self.notify_sender.try_send(Some(()));
            }
        }
        match _self.sender.send(Some(msg)) {
            Ok(_) => None,
            Err(mpmc::SendError(_msg)) => {
                return Some(_msg.unwrap());
            }
        }
    }
}

impl<M, W, S> WorkerPoolUnboundedInner<M, W, S>
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
        trace!("worker pool {} workers", self.get_worker_count());
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
        trace!("worker pool {} workers", self.get_worker_count());
    }

    #[inline(always)]
    pub fn get_worker_count(&self) -> usize {
        self.worker_count.load(Ordering::Acquire)
    }
    #[inline(always)]
    fn spawn(self: Arc<Self>) {
        let cur_count = self.worker_count.fetch_add(1, Ordering::SeqCst) + 1;
        let worker = self.inner.spawn();
        let _self = self.clone();
        if self.real_thread.load(Ordering::Acquire) {
            thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("runtime");
                rt.block_on(async move {
                    trace!("worker pool started worker {}", cur_count);
                    if _self.auto {
                        _self.run_worker_adjust(worker).await
                    } else {
                        _self.run_worker_simple(worker).await
                    }
                });
            });
        } else {
            tokio::spawn(async move {
                trace!("worker pool started worker {}", cur_count);
                if _self.auto {
                    _self.run_worker_adjust(worker).await
                } else {
                    _self.run_worker_simple(worker).await
                }
            });
        }
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

    async fn monitor(self: Arc<Self>) {
        let _self = self.as_ref();
        loop {
            match timeout(Duration::from_secs(1), _self.notify_recv.recv()).await {
                Err(_) => {
                    if _self.closing.load(Ordering::Acquire) {
                        return;
                    }
                    continue;
                }
                Ok(Ok(Some(_))) => {
                    if _self.closing.load(Ordering::Acquire) {
                        return;
                    }
                    let worker_count = _self.get_worker_count();
                    if worker_count > _self.max_workers {
                        continue;
                    }
                    let mut pending_msg = _self.sender.len();
                    if pending_msg > worker_count {
                        pending_msg -= worker_count;
                        if pending_msg > _self.max_workers - worker_count {
                            pending_msg = _self.max_workers - worker_count;
                        }
                        for _ in 0..pending_msg {
                            self.clone().spawn();
                        }
                    }
                }
                _ => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use crossfire::mpsc;
    use tokio::time::{Duration, sleep};

    use super::*;
    use atomic_waitgroup::WaitGroup;

    #[allow(dead_code)]
    struct MyWorkerPoolImpl();

    struct MyWorker();

    struct MyMsg(i64, mpsc::TxFuture<(), mpsc::SharedFutureBoth>);

    impl WorkerPoolImpl<MyMsg, MyWorker> for MyWorkerPoolImpl {
        fn spawn(&self) -> MyWorker {
            MyWorker()
        }
    }

    #[async_trait]
    impl Worker<MyMsg> for MyWorker {
        async fn init(&mut self) -> Result<(), ()> {
            println!("init done");
            Ok(())
        }

        async fn run(&mut self, msg: MyMsg) {
            sleep(Duration::from_millis(1)).await;
            println!("done {}", msg.0);
            let _ = msg.1.send(()).await;
        }
    }

    type MyWorkerPool = WorkerPoolUnbounded<MyMsg, MyWorker, MyWorkerPoolImpl>;

    #[test]
    fn unbounded_workerpool_adjust() {
        let _ = captains_log::recipe::stderr_test_logger(log::Level::Debug).build();
        let min_workers = 1;
        let max_workers = 4;
        let worker_timeout = Duration::from_secs(1);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        let worker_pool =
            MyWorkerPool::new(MyWorkerPoolImpl(), min_workers, max_workers, worker_timeout);
        rt.block_on(async move {
            worker_pool.start().await;
            let mut ths = Vec::new();
            for i in 0..5 {
                let _pool = worker_pool.clone();
                ths.push(tokio::task::spawn(async move {
                    let (done_tx, done_rx) = mpsc::bounded_future_both(10);
                    for j in 0..2 {
                        _pool.submit(MyMsg(i * 10 + j, done_tx.clone()));
                    }
                    for _j in 0..2 {
                        //println!("sender {} recv {}", i, _j);
                        let _ = done_rx.recv().await;
                    }
                }));
            }
            for th in ths {
                let _ = th.await;
            }
            let workers = worker_pool.get_worker_count();
            println!("cur workers {} should reach max", workers);
            assert_eq!(workers, max_workers);

            worker_pool.try_spawn(5).await;
            let workers = worker_pool.get_worker_count();
            println!("cur workers {} should reach max", workers);
            assert_eq!(workers, max_workers);

            sleep(worker_timeout * 2).await;
            let workers = worker_pool.get_worker_count();
            println!("cur workers: {}, extra should exit due to timeout", workers);
            assert_eq!(workers, min_workers);

            let (done_tx, done_rx) = mpsc::bounded_future_both(1);
            for j in 0..10 {
                worker_pool.submit(MyMsg(80 + j, done_tx.clone()));
                let _ = done_rx.recv().await;
            }
            println!("closing");
            worker_pool.close().await;
            assert_eq!(worker_pool.get_worker_count(), 0);
            assert_eq!(worker_pool.0.water.load(Ordering::Acquire), 0)
        });
    }

    #[allow(dead_code)]
    struct TestWorkerPoolImpl {
        id: AtomicUsize,
    }

    struct TestWorker {
        id: usize,
    }

    #[allow(dead_code)]
    struct TestMsg(usize, WaitGroup);

    impl WorkerPoolImpl<TestMsg, TestWorker> for TestWorkerPoolImpl {
        fn spawn(&self) -> TestWorker {
            let _id = self.id.fetch_add(1, Ordering::SeqCst);
            TestWorker { id: _id }
        }
    }

    #[async_trait]
    impl Worker<TestMsg> for TestWorker {
        async fn init(&mut self) -> Result<(), ()> {
            log::info!("worker {} init done", self.id);
            Ok(())
        }

        async fn run(&mut self, msg: TestMsg) {
            let run_time = (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .unwrap()
                .as_millis()
                % 10) as u64;
            sleep(Duration::from_millis(run_time)).await;
            msg.1.done();
        }
    }

    type TestWorkerPool = WorkerPoolUnbounded<TestMsg, TestWorker, TestWorkerPoolImpl>;

    #[test]
    fn unbounded_workerpool_run() {
        let _ = captains_log::recipe::stderr_test_logger(log::Level::Debug).build();

        log::info!("unbounded_workerpool test start");
        let min_workers = 8;
        let max_workers = 128;
        let worker_timeout = Duration::from_secs(5);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .unwrap();
        let worker_pool = TestWorkerPool::new(
            TestWorkerPoolImpl {
                id: AtomicUsize::new(0),
            },
            min_workers,
            max_workers,
            worker_timeout,
        );
        rt.block_on(async move {
            worker_pool.start().await;
            let total_threads = 10;
            let batch_msgs: usize = 10000;
            let wg = WaitGroup::new();
            wg.add(batch_msgs * total_threads);
            for thread in 0..total_threads {
                let _wg = wg.clone();
                let _pool = worker_pool.clone();
                tokio::spawn(async move {
                    log::info!("thread:{} run start", thread);
                    let batch_msg_start = thread * batch_msgs;
                    let mut submit_steps: u64 = (SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .ok()
                        .unwrap()
                        .as_millis()
                        % 100) as u64;
                    let mut current_submit_step = 0;
                    for i in batch_msg_start..(batch_msg_start + batch_msgs) {
                        let msg = TestMsg(i, _wg.clone());
                        if let Some(_msg) = _pool.submit(msg) {
                            _msg.1.done();
                        }
                        current_submit_step += 1;
                        if current_submit_step >= submit_steps {
                            sleep(Duration::from_millis(submit_steps % 100)).await;
                            current_submit_step = 0;
                            submit_steps = (SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .ok()
                                .unwrap()
                                .as_millis()
                                % 100) as u64;
                        }
                    }
                    log::info!("thread:{} run over", thread);
                });
            }
            wg.wait().await;
        });
        log::info!("unbounded_workerpool test over");
    }
}
