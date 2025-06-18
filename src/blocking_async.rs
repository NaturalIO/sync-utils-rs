use std::{
    cell::UnsafeCell,
    future::Future,
    mem::transmute,
    sync::{Arc, Condvar, Mutex},
};

use tokio::runtime::Runtime;

/// For the use in blocking context.
/// spawn a future into given tokio runtime and wait for result.
///
struct BlockingFutureInner<R>
where
    R: Sync + Send + 'static,
{
    res: UnsafeCell<Option<R>>,
    cond: Condvar,
    done: Mutex<bool>,
}

impl<R> BlockingFutureInner<R>
where
    R: Sync + Send + 'static,
{
    #[inline(always)]
    fn done(&self, r: R) {
        let _res: &mut Option<R> = unsafe { transmute(self.res.get()) };
        _res.replace(r);
        let mut guard = self.done.lock().unwrap();
        *guard = true;
        self.cond.notify_one();
    }

    #[inline(always)]
    fn take_res(&self) -> R {
        let _res: &mut Option<R> = unsafe { transmute(self.res.get()) };
        _res.take().unwrap()
    }
}

unsafe impl<R> Send for BlockingFutureInner<R> where R: Sync + Send + Clone + 'static {}

unsafe impl<R> Sync for BlockingFutureInner<R> where R: Sync + Send + Clone + 'static {}

pub struct BlockingFuture<R: Sync + Send + 'static>(Arc<BlockingFutureInner<R>>);

impl<R> BlockingFuture<R>
where
    R: Sync + Send + Clone + 'static,
{
    #[inline(always)]
    pub fn new() -> Self {
        Self(Arc::new(BlockingFutureInner {
            res: UnsafeCell::new(None),
            cond: Condvar::new(),
            done: Mutex::new(false),
        }))
    }

    pub fn block_on<F>(&mut self, rt: &Runtime, f: F) -> R
    where
        F: Future<Output = R> + Send + Sync + 'static,
    {
        let _self = self.0.clone();
        let _ = rt.spawn(async move {
            let res = f.await;
            _self.done(res);
        });
        let _self = self.0.as_ref();
        let mut guard = _self.done.lock().unwrap();
        loop {
            if *guard {
                return _self.take_res();
            }
            guard = _self.cond.wait(guard).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use tokio::time::sleep;

    use super::*;

    #[test]
    fn test_spawn() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .unwrap();

        let mut bf = BlockingFuture::new();
        let res = bf.block_on(&rt, async move {
            sleep(Duration::from_secs(1)).await;
            println!("exec future");
            sleep(Duration::from_secs(1)).await;
            return "hello world".to_string();
        });
        println!("got res {}", res);
    }
}
