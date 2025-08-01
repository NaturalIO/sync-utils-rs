use std::{
    collections::LinkedList,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use parking_lot::Mutex;

struct NotifyOnceInner {
    loaded: AtomicBool,
    wakers: Mutex<LinkedList<Waker>>,
}

/// NotifyOnce Assumes:
///
/// One coroutine issue some loading job, multiple coroutines wait for it to complete.
///
/// ## example:
///
/// ``` rust
///
/// async fn foo() {
///     use sync_utils::notifier::NotifyOnce;
///     use tokio::time::*;
///     use std::sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}};
///     let noti = NotifyOnce::new();
///     let done = Arc::new(AtomicBool::new(false));
///     for _ in 0..10 {
///         let _noti = noti.clone();
///         let _done = done.clone();
///         tokio::spawn(async move {
///             assert_eq!(_done.load(Ordering::Acquire), false);
///             _noti.wait().await;
///             assert_eq!(_done.load(Ordering::Acquire), true);
///         });
///     }
///     sleep(Duration::from_secs(1)).await;
///     done.store(true, Ordering::Release);
///     noti.done();
/// }
/// ```

#[derive(Clone)]
pub struct NotifyOnce(Arc<NotifyOnceInner>);

impl NotifyOnce {
    pub fn new() -> Self {
        Self(Arc::new(NotifyOnceInner {
            loaded: AtomicBool::new(false),
            wakers: Mutex::new(LinkedList::new()),
        }))
    }

    #[inline]
    pub fn done(&self) {
        let _self = self.0.as_ref();
        _self.loaded.store(true, Ordering::Release);
        {
            let mut guard = _self.wakers.lock();
            while let Some(waker) = guard.pop_front() {
                waker.wake();
            }
        }
    }

    #[inline]
    pub async fn wait(&self) {
        NotifyOnceWaitFuture { inner: self.0.as_ref(), is_new: true }.await;
    }
}

struct NotifyOnceWaitFuture<'a> {
    inner: &'a NotifyOnceInner,
    is_new: bool,
}

impl<'a> Future for NotifyOnceWaitFuture<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let _self = self.get_mut();
        if _self.inner.loaded.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        if _self.is_new {
            {
                let mut guard = _self.inner.wakers.lock();
                guard.push_back(ctx.waker().clone());
            }
            _self.is_new = false;
            if _self.inner.loaded.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {

    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tokio::time::{Duration, sleep};

    use super::*;

    #[test]
    fn test_notify_once() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();

        rt.block_on(async move {
            let noti = NotifyOnce::new();
            let done = Arc::new(AtomicBool::new(false));
            let wait_count = Arc::new(AtomicUsize::new(0));
            let mut th_s = Vec::new();
            for _ in 0..10 {
                let _noti = noti.clone();
                let _done = done.clone();
                let _wait_count = wait_count.clone();
                th_s.push(tokio::spawn(async move {
                    assert_eq!(_done.load(Ordering::Acquire), false);
                    _noti.wait().await;
                    _wait_count.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(_done.load(Ordering::Acquire), true);
                }));
            }
            sleep(Duration::from_secs(1)).await;
            assert_eq!(wait_count.load(Ordering::Acquire), 0);
            done.store(true, Ordering::Release);
            noti.done();
            for th in th_s {
                let _ = th.await.expect("");
            }
            assert_eq!(wait_count.load(Ordering::Acquire), 10);
        });
    }
}
