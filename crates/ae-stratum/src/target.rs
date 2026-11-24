use crate::{Error, integer};
use num_bigint::BigInt;
use num_traits::{ToPrimitive, Zero};

pub const MAX_SAMPLES: usize = 1024;
pub fn maximum() -> BigInt {
    BigInt::from(0xffffu32) << 240
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Difference {
    Unchanged,
    Increase(f64),
    Decrease(f64),
}
// This is pool policy, not the canonical key-block retarget function.
pub fn recalculate(
    previous: &[(BigInt, BigInt)],
    desired: &BigInt,
    maximum: &BigInt,
) -> Result<BigInt, Error> {
    integer(maximum)?;
    integer(desired)?;
    if previous.len() > MAX_SAMPLES {
        return Err(Error::Limit("samples"));
    }
    if previous.is_empty() {
        return Ok(maximum.clone());
    }
    let k = maximum << 32usize;
    let mut capacity = BigInt::zero();
    let mut elapsed = BigInt::zero();
    for (target, time) in previous {
        integer(target)?;
        integer(time)?;
        if target.is_zero() {
            return Err(Error::Arithmetic);
        }
        capacity += &k / target;
        elapsed += time;
    }
    let denominator = desired * capacity;
    if denominator.is_zero() {
        return Err(Error::Arithmetic);
    }
    let tempered =
        (BigInt::from(3 * previous.len()) * desired) / 4u8 + (elapsed * 2523u16) / 10000u16;
    Ok((tempered * k / denominator).min(maximum.clone()))
}
pub fn difference(new: &BigInt, old: &BigInt) -> Result<Difference, Error> {
    integer(new)?;
    integer(old)?;
    if new == old {
        return Ok(Difference::Unchanged);
    }
    if old.is_zero() {
        return Err(Error::Arithmetic);
    }
    let delta = if new > old { new - old } else { old - new };
    let delta = delta
        .to_f64()
        .filter(|n| n.is_finite())
        .ok_or(Error::Float)?;
    let denominator = old.to_f64().filter(|n| n.is_finite()).ok_or(Error::Float)?;
    let percent = delta / denominator * 100.0;
    if !percent.is_finite() {
        return Err(Error::Float);
    }
    Ok(if new > old {
        Difference::Increase(percent)
    } else {
        Difference::Decrease(percent)
    })
}
// Native fixed-width rendering: too-wide values are represented by '*'.
pub fn to_hex(value: &BigInt) -> Result<String, Error> {
    integer(value)?;
    let digits = value.to_str_radix(16);
    if digits.len() > 64 {
        return Ok("*".repeat(64));
    }
    Ok(format!("{digits:0>64}"))
}
pub fn from_hex(text: &str) -> Result<BigInt, Error> {
    if text.len() > 4096 {
        return Err(Error::Limit("target text"));
    }
    let digits = text
        .strip_prefix('+')
        .or_else(|| text.strip_prefix('-'))
        .unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error::Hex);
    }
    let value = BigInt::parse_bytes(text.as_bytes(), 16).ok_or(Error::Hex)?;
    integer(&value)?;
    Ok(value)
}
