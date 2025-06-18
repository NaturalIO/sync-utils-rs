use std::sync::atomic::{AtomicU64, Ordering};

use chrono::offset::Utc;

static DELAYED_SEC: AtomicU64 = AtomicU64::new(0);

#[inline]
pub fn timestamp() -> u64 {
    Utc::now().timestamp() as u64
}

#[inline]
pub fn timestamp_subsec_nanos() -> (u64, u32) {
    let dt = Utc::now();
    (dt.timestamp() as u64, dt.timestamp_subsec_nanos())
}

/// Light-weight timestamp that update unix epoch every second in background,
/// to minimise the cost of time related system calls.
pub struct DelayedTime();

impl DelayedTime {
    /// Start the background time update thread once, and return the timestamp.
    ///
    /// NOTE: After program started or fork, remember to call this function.
    #[inline(always)]
    pub fn start() -> u64 {
        let now = timestamp();
        match DELAYED_SEC.compare_exchange(0, now, Ordering::SeqCst, Ordering::Acquire) {
            Ok(_) => {
                std::thread::spawn(|| {
                    let d = std::time::Duration::from_secs(1);
                    loop {
                        DELAYED_SEC.store(timestamp(), Ordering::Release);
                        std::thread::sleep(d);
                    }
                });
                now
            }
            Err(_now) => _now,
        }
    }

    /// Get the delayed timestamp in second. Start the update thread if not started.
    #[inline(always)]
    pub fn now() -> u64 {
        let now = DELAYED_SEC.load(Ordering::Acquire);
        if now == 0 { Self::start() } else { now }
    }

    /// Get the delayed timestamp in second. Use acture time when update thread not running.
    #[inline(always)]
    pub fn get() -> u64 {
        let now = DELAYED_SEC.load(Ordering::Acquire);
        if now == 0 { timestamp() } else { now }
    }
}

/// When the time (unix epoch) has been too long since `pre_ts`, return true.
///
/// `pre_ts`: old timestamp.
///
/// `duration`: unit in seconds.
#[inline]
pub fn check_timelapse(pre_ts: u64, duration: u64) -> bool {
    let now = DelayedTime::get();
    if now <= pre_ts {
        return false;
    }
    now - pre_ts >= duration
}
