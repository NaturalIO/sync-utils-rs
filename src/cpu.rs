extern crate lazy_static;
extern crate num_cpus;
use super::bithacks::round_up_to_power2;

const DEFAULT_CPUS: u32 = 4;

/// Determine the right sharding by cpu.
/// Minimise the cost of hashing function of remainder calculation.
/// Refer to `bithacks` module.
pub struct CpuShard(u32, u32);

impl CpuShard {
    #[inline]
    pub fn new(max_shard_limit: Option<u32>) -> Self {
        let mut cpus = num_cpus::get() as u32;
        if cpus < DEFAULT_CPUS {
            cpus = DEFAULT_CPUS;
        }
        if let Some(limit) = max_shard_limit {
            if cpus > limit {
                cpus = limit;
            }
        }
        let (shard, shift) = round_up_to_power2(cpus);
        Self(shard, shift)
    }

    #[inline]
    pub fn shards(&self) -> u32 {
        self.0
    }

    #[inline]
    pub fn shift(&self) -> u32 {
        self.1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_shards() {
        let cpu_shard = CpuShard::new(None);
        let cpus = num_cpus::get();
        println!("cpus={}", cpus);
        if cpus <= 4 {
            assert_eq!(cpu_shard.shards(), 4);
            assert_eq!(cpu_shard.shift(), 2);
        } else if cpus <= 8 {
            assert_eq!(cpu_shard.shards(), 8);
            assert_eq!(cpu_shard.shift(), 3);
        }
    }

    #[test]
    fn test_shards_limit() {
        let cpu_shard = CpuShard::new(Some(2));
        assert_eq!(cpu_shard.shards(), 2);
        assert_eq!(cpu_shard.shift(), 1);
    }
}
