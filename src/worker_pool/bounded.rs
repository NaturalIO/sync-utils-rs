use std::{
    cell::UnsafeCell,
    future::Future,
    mem::transmute,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::*,
    thread,
    time::Duration,
};

use crossfire::*;
use tokio::time::{sleep, timeout};

use super::*;

#[allow(unused_must_use)]
pub struct WorkerPoolBounded<
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
>(Arc<WorkerPoolBoundedInner<M, W, S>>);

struct WorkerPoolBoundedInner<M, W, S>
where
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
{
    worker_count: AtomicUsize,
    sender: UnsafeCell<Option<MAsyncTx<Option<M>>>>,
    min_workers: usize,
    max_workers: usize,
    worker_timeout: Duration,
    inner: S,
    phantom: std::marker::PhantomData<W>, // to avoid complaining unused param
    closing: AtomicBool,
    notify_sender: MAsyncTx<Option<()>>,
    notify_recv: UnsafeCell<Option<AsyncRx<Option<()>>>>,
    auto: bool,
    channel_size: usize,
    real_thread: AtomicBool,
    bind_cpu: AtomicUsize,
    max_cpu: usize,
}

unsafe impl<M, W, S> Send for WorkerPoolBoundedInner<M, W, S>
where
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
{
}

unsafe impl<M, W, S> Sync for WorkerPoolBoundedInner<M, W, S>
where
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
{
}

impl<M, W, S> Clone for WorkerPoolBounded<M, W, S>
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

impl<M, W, S> WorkerPoolBounded<M, W, S>
where
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
{
    pub fn new(
        inner: S, min_workers: usize, max_workers: usize, channel_size: usize,
        worker_timeout: Duration,
    ) -> Self {
        assert!(min_workers > 0);
        assert!(max_workers >= min_workers);

        let auto: bool = min_workers < max_workers;
        if auto {
            assert!(worker_timeout != ZERO_DUARTION);
        }
        let (noti_sender, noti_recv) = mpsc::bounded_async(1);
        let pool = Arc::new(WorkerPoolBoundedInner {
            sender: UnsafeCell::new(None),
            inner,
            worker_count: AtomicUsize::new(0),
            min_workers,
            max_workers,
            channel_size,
            worker_timeout,
            phantom: Default::default(),
            closing: AtomicBool::new(false),
            notify_sender: noti_sender,
            notify_recv: UnsafeCell::new(Some(noti_recv)),
            auto,
            real_thread: AtomicBool::new(false),
            bind_cpu: AtomicUsize::new(0),
            max_cpu: num_cpus::get(),
        });
        Self(pool)
    }

    // If worker contains blocking logic, run worker in separate threads
    pub fn set_use_thread(&mut self, ok: bool) {
        self.0.real_thread.store(ok, Ordering::Release);
    }

    pub fn start(&self) {
        let _self = self.0.as_ref();
        let (sender, rx) = mpmc::bounded_async(_self.channel_size);
        _self._sender().replace(sender);

        for _ in 0.._self.min_workers {
            self.0.clone().spawn(true, rx.clone());
        }
        if _self.auto {
            let _pool = self.0.clone();
            let notify_recv: &mut Option<AsyncRx<Option<()>>> =
                unsafe { transmute(_self.notify_recv.get()) };
            let noti_rx = notify_recv.take().unwrap();
            tokio::spawn(async move {
                _pool.monitor(noti_rx, rx).await;
            });
        }
    }

    pub async fn close_async(&self) {
        let _self = self.0.as_ref();
        if _self.closing.swap(true, Ordering::SeqCst) {
            return;
        }
        if _self.auto {
            let _ = _self.notify_sender.send(None).await;
        }
        let sender = _self._sender().as_ref().unwrap();
        loop {
            let cur = self.get_worker_count();
            if cur == 0 {
                break;
            }
            debug!("worker pool closing: cur workers {}", cur);
            for _ in 0..cur {
                let _ = sender.send(None).await;
            }
            sleep(Duration::from_secs(1)).await;
        }
    }

    // must not use in runtime
    pub fn close(&self) {
        if let Ok(_rt) = tokio::runtime::Handle::try_current() {
            warn!("close in runtime thread, spawn close thread");
            let _self = self.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("runtime");
                rt.block_on(async move {
                    _self.close_async().await;
                });
            });
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            let _self = self.clone();
            rt.block_on(async move {
                _self.close_async().await;
            });
        }
    }

    pub fn get_worker_count(&self) -> usize {
        self.0.get_worker_count()
    }

    pub fn get_inner(&self) -> &S {
        &self.0.inner
    }

    #[inline]
    pub fn try_submit(&self, msg: M) -> Option<M> {
        let _self = self.0.as_ref();
        if _self.closing.load(Ordering::Acquire) {
            return Some(msg);
        }
        match _self._sender().as_ref().unwrap().try_send(Some(msg)) {
            Err(TrySendError::Disconnected(m)) => {
                return m;
            }
            Err(TrySendError::Full(m)) => {
                return m;
            }
            Ok(_) => return None,
        }
    }

    // return non-null on send fail
    #[inline]
    pub fn submit<'a>(&'a self, mut msg: M) -> SubmitFuture<'a, M> {
        let _self = self.0.as_ref();
        if _self.closing.load(Ordering::Acquire) {
            return SubmitFuture { send_f: None, res: Some(Err(msg)) };
        }
        let sender = _self._sender().as_ref().unwrap();
        if _self.auto {
            match sender.try_send(Some(msg)) {
                Err(TrySendError::Disconnected(m)) => {
                    return SubmitFuture { send_f: None, res: Some(Err(m.unwrap())) };
                }
                Err(TrySendError::Full(m)) => {
                    msg = m.unwrap();
                }
                Ok(_) => {
                    return SubmitFuture { send_f: None, res: Some(Ok(())) };
                }
            }
            let worker_count = _self.get_worker_count();
            if worker_count < _self.max_workers {
                let _ = _self.notify_sender.try_send(Some(()));
            }
        }
        let send_f = sender.send(Some(msg));
        return SubmitFuture { send_f: Some(send_f), res: None };
    }
}

pub struct SubmitFuture<'a, M: Send + Sized + Unpin + 'static> {
    send_f: Option<SendFuture<'a, Option<M>>>,
    res: Option<Result<(), M>>,
}

impl<'a, M: Send + Sized + Unpin + 'static> Future for SubmitFuture<'a, M> {
    type Output = Option<M>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let _self = self.get_mut();
        if _self.res.is_some() {
            match _self.res.take().unwrap() {
                Ok(()) => return Poll::Ready(None),
                Err(m) => return Poll::Ready(Some(m)),
            }
        }
        let send_f = _self.send_f.as_mut().unwrap();
        if let Poll::Ready(r) = Pin::new(send_f).poll(ctx) {
            match r {
                Ok(()) => return Poll::Ready(None),
                Err(SendError(e)) => {
                    return Poll::Ready(e);
                }
            }
        }
        Poll::Pending
    }
}

#[async_trait]
impl<M, W, S> WorkerPoolAsyncInf<M> for WorkerPoolBounded<M, W, S>
where
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
{
    #[inline]
    async fn submit(&self, msg: M) -> Option<M> {
        self.submit(msg).await
    }

    #[inline]
    fn try_submit(&self, msg: M) -> Option<M> {
        self.try_submit(msg)
    }
}

impl<M, W, S> WorkerPoolBoundedInner<M, W, S>
where
    M: Send + Sized + Unpin + 'static,
    W: Worker<M>,
    S: WorkerPoolImpl<M, W>,
{
    #[inline(always)]
    fn _sender(&self) -> &mut Option<MAsyncTx<Option<M>>> {
        unsafe { transmute(self.sender.get()) }
    }

    async fn run_worker_simple(&self, mut worker: W, rx: MAsyncRx<Option<M>>) {
        if let Err(_) = worker.init().await {
            let _ = self.try_exit();
            worker.on_exit();
            return;
        }
        loop {
            match rx.recv().await {
                Ok(item) => {
                    if item.is_none() {
                        let _ = self.try_exit();
                        break;
                    }
                    worker.run(item.unwrap()).await;
                }
                Err(_) => {
                    // channel closed worker exit
                    let _ = self.try_exit();
                    break;
                }
            }
        }
        worker.on_exit();
    }

    async fn run_worker_adjust(&self, mut worker: W, rx: MAsyncRx<Option<M>>) {
        if let Err(_) = worker.init().await {
            let _ = self.try_exit();
            worker.on_exit();
            return;
        }

        let worker_timeout = self.worker_timeout;
        let mut is_idle = false;
        'WORKER_LOOP: loop {
            if is_idle {
                match rx.recv_timeout(worker_timeout).await {
                    Ok(item) => {
                        if item.is_none() {
                            let _ = self.try_exit();
                            break 'WORKER_LOOP;
                        }
                        worker.run(item.unwrap()).await;
                        is_idle = false;
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        // channel closed worker exit
                        let _ = self.try_exit();
                        worker.on_exit();
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        // timeout
                        if self.try_exit() {
                            break 'WORKER_LOOP;
                        }
                    }
                }
            } else {
                match rx.try_recv() {
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
    fn spawn(self: Arc<Self>, initial: bool, rx: MAsyncRx<Option<M>>) {
        self.worker_count.fetch_add(1, Ordering::SeqCst);
        let worker = self.inner.spawn();
        let _self = self.clone();
        if self.real_thread.load(Ordering::Acquire) {
            let mut bind_cpu: Option<usize> = None;
            if _self.bind_cpu.load(Ordering::Acquire) <= _self.max_cpu {
                let cpu = _self.bind_cpu.fetch_add(1, Ordering::SeqCst);
                if cpu < _self.max_cpu {
                    bind_cpu = Some(cpu as usize);
                }
            }
            thread::spawn(move || {
                if let Some(cpu) = bind_cpu {
                    core_affinity::set_for_current(core_affinity::CoreId { id: cpu });
                }
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("runtime");
                rt.block_on(async move {
                    if initial || !_self.auto {
                        _self.run_worker_simple(worker, rx).await
                    } else {
                        _self.run_worker_adjust(worker, rx).await
                    }
                });
            });
        } else {
            tokio::spawn(async move {
                if initial || !_self.auto {
                    _self.run_worker_simple(worker, rx).await
                } else {
                    _self.run_worker_adjust(worker, rx).await
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

    async fn monitor(self: Arc<Self>, noti: AsyncRx<Option<()>>, rx: MAsyncRx<Option<M>>) {
        let _self = self.as_ref();
        loop {
            match timeout(Duration::from_secs(1), noti.recv()).await {
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
                    self.clone().spawn(false, rx.clone());
                }
                _ => {
                    println!("monitor exit");
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crossfire::*;
    use tokio::time::{Duration, sleep};

    use super::*;

    #[allow(dead_code)]
    struct MyWorkerPoolImpl();

    struct MyWorker();

    struct MyMsg(i64, MAsyncTx<()>);

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
            let _ = msg.1.send(()).await;
        }
    }

    type MyWorkerPool = WorkerPoolBounded<MyMsg, MyWorker, MyWorkerPoolImpl>;

    #[test]
    fn bounded_workerpool_adjust_close_async() {
        let min_workers = 1;
        let max_workers = 4;
        let worker_timeout = Duration::from_secs(1);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        let worker_pool =
            MyWorkerPool::new(MyWorkerPoolImpl(), min_workers, max_workers, 1, worker_timeout);
        let _worker_pool = worker_pool.clone();
        rt.block_on(async move {
            worker_pool.start();
            let mut th_s = Vec::new();
            for i in 0..5 {
                let _pool = worker_pool.clone();
                th_s.push(tokio::task::spawn(async move {
                    let (done_tx, done_rx) = mpsc::bounded_async(10);
                    for j in 0..2 {
                        let m = i * 10 + j;
                        println!("submit {} in {}/{}", m, j, i);
                        _pool.submit(MyMsg(m, done_tx.clone())).await;
                    }
                    for _j in 0..2 {
                        //println!("sender {} recv {}", i, _j);
                        let _ = done_rx.recv().await;
                    }
                }));
            }
            for th in th_s {
                let _ = th.await;
            }
            let workers = worker_pool.get_worker_count();
            println!("cur workers {} might reach max {}", workers, max_workers);
            //assert_eq!(workers, max_workers);

            sleep(worker_timeout * 2).await;
            let workers = worker_pool.get_worker_count();
            println!("cur workers: {}, extra should exit due to timeout", workers);
            assert_eq!(workers, min_workers);

            let (done_tx, done_rx) = mpsc::bounded_async(1);
            for j in 0..10 {
                worker_pool.submit(MyMsg(80 + j, done_tx.clone())).await;
                let _ = done_rx.recv().await;
            }
            println!("closing");
            _worker_pool.close();
            sleep(Duration::from_secs(2)).await;
            assert_eq!(_worker_pool.get_worker_count(), 0);
        });
    }

    #[test]
    fn bounded_workerpool_adjust_close() {
        let min_workers = 1;
        let max_workers = 4;
        let worker_timeout = Duration::from_secs(1);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        let worker_pool =
            MyWorkerPool::new(MyWorkerPoolImpl(), min_workers, max_workers, 1, worker_timeout);
        let _worker_pool = worker_pool.clone();
        rt.block_on(async move {
            worker_pool.start();
            let mut th_s = Vec::new();
            for i in 0..5 {
                let _pool = worker_pool.clone();
                th_s.push(tokio::task::spawn(async move {
                    let (done_tx, done_rx) = mpsc::bounded_async(10);
                    for j in 0..2 {
                        let m = i * 10 + j;
                        println!("submit {} in {}/{}", m, j, i);
                        _pool.submit(MyMsg(m, done_tx.clone())).await;
                    }
                    for _j in 0..2 {
                        //println!("sender {} recv {}", i, _j);
                        let _ = done_rx.recv().await;
                    }
                }));
            }
            for th in th_s {
                let _ = th.await;
            }
            let workers = worker_pool.get_worker_count();
            println!("cur workers {} might reach max {}", workers, max_workers);
            //assert_eq!(workers, max_workers);

            sleep(worker_timeout * 2).await;
            let workers = worker_pool.get_worker_count();
            println!("cur workers: {}, extra should exit due to timeout", workers);
            assert_eq!(workers, min_workers);

            let (done_tx, done_rx) = mpsc::bounded_async(1);
            for j in 0..10 {
                worker_pool.submit(MyMsg(80 + j, done_tx.clone())).await;
                let _ = done_rx.recv().await;
            }
        });
        println!("closing");
        _worker_pool.close();
        assert_eq!(_worker_pool.get_worker_count(), 0);
    }

    #[allow(dead_code)]
    struct MyBlockingWorkerPoolImpl();

    struct MyBlockingWorker();

    impl WorkerPoolImpl<MyMsg, MyBlockingWorker> for MyBlockingWorkerPoolImpl {
        fn spawn(&self) -> MyBlockingWorker {
            MyBlockingWorker()
        }
    }

    #[async_trait]
    impl Worker<MyMsg> for MyBlockingWorker {
        async fn run(&mut self, msg: MyMsg) {
            std::thread::sleep(Duration::from_millis(1));
            println!("done {}", msg.0);
            let _ = msg.1.send(()).await;
        }
    }

    type MyBlockingWorkerPool =
        WorkerPoolBounded<MyMsg, MyBlockingWorker, MyBlockingWorkerPoolImpl>;

    #[test]
    fn bounded_workerpool_adjust_real_thread() {
        let min_workers = 1;
        let max_workers = 4;
        let worker_timeout = Duration::from_secs(1);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        let mut worker_pool = MyBlockingWorkerPool::new(
            MyBlockingWorkerPoolImpl(),
            min_workers,
            max_workers,
            1,
            worker_timeout,
        );
        worker_pool.set_use_thread(true);
        let _worker_pool = worker_pool.clone();
        rt.block_on(async move {
            worker_pool.start();
            let mut th_s = Vec::new();
            for i in 0..5 {
                let _pool = worker_pool.clone();
                th_s.push(tokio::task::spawn(async move {
                    let (done_tx, done_rx) = mpsc::bounded_async(10);
                    for j in 0..2 {
                        let m = i * 10 + j;
                        println!("submit {} in {}/{}", m, j, i);
                        _pool.submit(MyMsg(m, done_tx.clone())).await;
                    }
                    for _j in 0..2 {
                        //println!("sender {} recv {}", i, _j);
                        let _ = done_rx.recv().await;
                    }
                }));
            }
            for th in th_s {
                let _ = th.await;
            }
            let workers = worker_pool.get_worker_count();
            println!("cur workers {} might reach max {}", workers, max_workers);
            //assert_eq!(workers, max_workers);

            sleep(worker_timeout * 2).await;
            let workers = worker_pool.get_worker_count();
            println!("cur workers: {}, extra should exit due to timeout", workers);
            assert_eq!(workers, min_workers);

            let (done_tx, done_rx) = mpsc::bounded_async(1);
            for j in 0..10 {
                worker_pool.submit(MyMsg(80 + j, done_tx.clone())).await;
                let _ = done_rx.recv().await;
            }
        });
        println!("closing");
        _worker_pool.close();
        assert_eq!(_worker_pool.get_worker_count(), 0);
    }
}
