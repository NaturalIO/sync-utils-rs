pub mod bounded;
pub mod bounded_blocking;
pub mod unbounded;

use std::time::Duration;

pub const ZERO_DUARTION: Duration = Duration::from_secs(0);

#[async_trait]
pub trait Worker<M: Send + Sized + Unpin + 'static>: Send + Sync + Unpin + 'static {
    #[inline]
    async fn init(&mut self) -> Result<(), ()> {
        Ok(())
    }

    async fn run(&mut self, msg: M);

    fn on_exit(&self) {}
}

pub trait WorkerPoolImpl<M: Send + Sized + Unpin + 'static, W: Worker<M>>:
    Send + Sync + Sized + 'static
{
    fn spawn(&self) -> W;
}

pub trait WorkerPoolInf<M: Send + Sized + Unpin + 'static>: Send + Sync {
    fn submit(&self, msg: M) -> Option<M>;
}

#[async_trait]
pub trait WorkerPoolAsyncInf<M: Send + Sized + Unpin + 'static>: Send + Sync {
    async fn submit(&self, msg: M) -> Option<M>;

    fn try_submit(&self, msg: M) -> Option<M>;
}
