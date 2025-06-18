use std::{
    hint::spin_loop,
    sync::atomic::{AtomicBool, Ordering},
};

pub struct Spinlock(AtomicBool);

impl Spinlock {
    pub fn new() -> Self {
        Self(AtomicBool::new(false))
    }

    #[inline(always)]
    pub fn lock(&self) {
        let mut c = 0;
        while self.0.swap(true, Ordering::SeqCst) {
            if c > 30 {
                std::thread::yield_now();
            } else {
                c += 1;
                spin_loop();
            }
        }
    }

    #[inline(always)]
    pub fn try_lock(&self) -> bool {
        self.0.swap(true, Ordering::SeqCst) == false
    }

    #[inline(always)]
    pub fn unlock(&self) {
        self.0.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn spinlock() {
        let lock = Spinlock::new();
        assert!(lock.try_lock());
        assert!(!lock.try_lock());
        lock.unlock();
        lock.lock();
        lock.unlock();
    }
}
