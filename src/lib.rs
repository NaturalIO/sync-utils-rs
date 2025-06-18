//!
//! A bunch of utilities in async-await and blocking context.
//!

extern crate atomic_waitgroup;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate log;

/// Bithacks to minimise the cost
pub mod bithacks;

/// Sharding to minimise the cost
pub mod cpu;

/// Execute future in blocking context
pub mod blocking_async;

/// Execute once and notify
pub mod notifier;

/// timestamp utility
pub mod time;

/// A simple worker pool framework
pub mod worker_pool;

/// Re-export of crate [atomic-waitgroup](https://docs.rs/atomic_waitgroup)
pub mod waitgroup;
