//! bithacks functions.
//!
//! Implmented according to paper "Faster Remainder by Direct Computation Applications to Compilers and Software Libraries".

/// Given a 32-bit number, find the highest bit number which is set 1.
///
/// For example:
///
/// ``` text
///   for 0011(3), returns 1.
///   for 0100(4), returns 2.
/// ```
#[inline]
pub fn highest_bit_set(n: u32) -> u32 {
    let mut c: u32 = 0;
    let mut _n = n;
    if _n >= 65536 {
        _n >>= 16;
        c += 16;
    }
    if _n >= 256 {
        _n >>= 8;
        c += 8;
    }
    if _n >= 16 {
        _n >>= 4;
        c += 4;
    }
    if _n >= 4 {
        _n >>= 2;
        c += 2;
    }
    if _n >= 2 { c + 1 } else { c }
}

/// Given a 32-bit number, return the next highest power of 2, and the corresponding shift.
///
/// For example:
///
/// ``` text
///    if n is 7, then function will return (8, 3), which 8 = 1 << 3
/// ```
#[inline]
pub fn round_up_to_power2(n: u32) -> (u32, u32) {
    let mut _n = n;
    _n -= 1;
    _n |= _n >> 1;
    _n |= _n >> 2;
    _n |= _n >> 4;
    _n |= _n >> 8;
    _n |= _n >> 16;
    _n += 1;
    (_n, highest_bit_set(_n))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_highest_bit_set() {
        assert_eq!(highest_bit_set(1), 0);
        assert_eq!(highest_bit_set(3), 1);
        assert_eq!(highest_bit_set(4), 2);
        assert_eq!(highest_bit_set(33554432), 25);
        assert_eq!(highest_bit_set(1615855616), 30);
    }

    #[test]
    fn test_round_up_to_power2() {
        assert_eq!(round_up_to_power2(1), (1, 0));
        assert_eq!(round_up_to_power2(3), (4, 2));
        assert_eq!(round_up_to_power2(4), (4, 2));
        assert_eq!(round_up_to_power2(5), (8, 3));
        assert_eq!(round_up_to_power2(100), (128, 7));
        assert_eq!(round_up_to_power2(16 * 1024 - 100), (16 * 1024, 14));
    }
}
