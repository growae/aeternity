// Behavioral reference: aeminer c6b6a6d, aeminer_pow.erl; aeternity fa2a6d7, aec_target.erl.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use ae_types::{Hash256, Unsigned};
use num_bigint::{BigInt, BigUint, Sign};
use num_traits::{ToPrimitive, Zero};
use std::fmt;

pub const HIGHEST_TARGET: Target = Target(0x2100ffff);
pub const DIFFICULTY_WINDOW: usize = 17;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Target(pub u32);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetError {
    ZeroTarget,
    InvalidWindow,
    OutsideHeaderDomain,
    ZeroCapacity,
}
impl fmt::Display for TargetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for TargetError {}

impl Target {
    fn integer(self) -> BigUint {
        let mut exponent = (self.0 >> 24) as i32;
        if self.0 & 0x800000 != 0 {
            exponent = -exponent;
        }
        let significand = BigUint::from(self.0 & 0x7fffff);
        let shift = 8 * (exponent - 3);
        if shift >= 0 {
            significand << shift as usize
        } else {
            significand >> (-shift) as usize
        }
    }
    pub fn value(self) -> Unsigned {
        unsigned(self.integer())
    }
    pub fn from_value(value: &Unsigned) -> Result<Self, TargetError> {
        compact(BigInt::from_bytes_be(Sign::Plus, value.as_bytes()))
            .to_u32()
            .map(Self)
            .ok_or(TargetError::OutsideHeaderDomain)
    }
    pub fn accepts(self, hash: &Hash256) -> bool {
        BigUint::from_bytes_be(&hash.0) < self.integer()
    }
    pub fn difficulty(self) -> Result<Unsigned, TargetError> {
        let target = self.integer();
        if target.is_zero() {
            return Err(TargetError::ZeroTarget);
        }
        Ok(unsigned((HIGHEST_TARGET.integer() << 24usize) / target))
    }
}
fn unsigned(n: BigUint) -> Unsigned {
    Unsigned::from_field_bytes(&n.to_bytes_be()).expect("minimal big integer bytes")
}
fn compact(mut n: BigInt) -> BigInt {
    let mut exponent = 3i32;
    while n > BigInt::from(0x7fffffu32) {
        n >>= 8usize;
        exponent += 1;
    }
    while n > BigInt::zero() && n < BigInt::from(0x8000u32) {
        n <<= 8usize;
        exponent -= 1;
    }
    if exponent >= 0 {
        (BigInt::from(exponent) << 24usize) + n
    } else {
        (BigInt::from(-exponent) << 24usize) + 0x800000u32 + n
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkSample {
    pub height: u64,
    pub timestamp: u64,
    pub target: Target,
}

// The context supplies the 18 preceding key headers. Sorting and integer
// division, including negative solve-time truncation, follow the reference.
pub fn retarget(samples: &[WorkSample]) -> Result<Target, TargetError> {
    if samples.len() != DIFFICULTY_WINDOW + 1 {
        return Err(TargetError::InvalidWindow);
    }
    let mut ordered = samples.to_vec();
    ordered.sort_by_key(|x| x.height);
    let k = HIGHEST_TARGET.integer() << 32usize;
    let mut capacity = BigUint::zero();
    for sample in &ordered[1..] {
        let target = sample.target.integer();
        if target.is_zero() {
            return Err(TargetError::ZeroTarget);
        }
        capacity += &k / target;
    }
    if capacity.is_zero() {
        return Err(TargetError::ZeroCapacity);
    }
    let elapsed: i128 = ordered
        .windows(2)
        .map(|s| {
            (i128::from(s[1].timestamp) - i128::from(s[0].timestamp)).clamp(-540_000, 1_080_000)
        })
        .sum();
    let tempered = 3 * DIFFICULTY_WINDOW as i128 * 180_000 / 4 + 2523 * elapsed / 10000;
    let integer =
        BigInt::from(tempered) * BigInt::from(k) / (BigInt::from(180_000) * BigInt::from(capacity));
    let sci = compact(integer).min(BigInt::from(HIGHEST_TARGET.0));
    sci.to_u32()
        .map(Target)
        .ok_or(TargetError::OutsideHeaderDomain)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_zero_sign_bit_and_strict_threshold_match_reference() {
        assert_eq!(Target::from_value(&0.into()).unwrap(), Target(0x03000000));
        assert_eq!(Target(0x1d80ffff).value(), 0.into()); // sign bit negates exponent
        assert_eq!(
            Target(0x1d00ffff).difficulty().unwrap(),
            Unsigned::from_decimal("72057594037927936").unwrap()
        );
        let mut hash = Hash256::ZERO;
        hash.0[31] = 1;
        assert!(!Target(0x01010000).accepts(&hash));
        assert!(Target(0x01020000).accepts(&hash));
        assert_eq!(Target(0).difficulty(), Err(TargetError::ZeroTarget));
    }
}
