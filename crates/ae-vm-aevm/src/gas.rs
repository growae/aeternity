// Gas rules: aevm_gas, fa2a6d7. Copyright (c) 2017, Aeternity Anstalt.
// ISC: repository LICENSE. Float rounding is consensus-visible in the original.
// Native limb conversion adapted from OTP-26.2.5.21 big.c.
// Copyright Ericsson AB 1996-2023. All Rights Reserved.
// Apache-2.0: licenses/Apache-2.0.txt at the repository root.
// Modified: safe Rust with explicit 64-bit limb/runtime selection.
use crate::opcode::GasTable;
use num_bigint::BigUint;
use num_traits::{FromPrimitive, Zero};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    BadArithmetic,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloatProfile {
    Otp26Word64,
}
fn divided(
    n: &BigUint,
    denominator: f64,
    ceiling: bool,
    profile: FloatProfile,
) -> Result<BigUint, Error> {
    // Erlang converts the numerator to a float before division. An overflowing
    // conversion is badarith even if integer division would have a finite result.
    // OTP big_to_double rounds after each native limb, not only once after
    // converting the whole integer. This can differ by one ULP near a tie.
    let mut f = 0.0f64;
    match profile {
        FloatProfile::Otp26Word64 => {
            for digit in n.iter_u64_digits().rev() {
                f = f * 18446744073709551616.0 + (digit as f64);
                if !f.is_finite() {
                    return Err(Error::BadArithmetic);
                }
            }
        }
    }
    let f = f / denominator;
    BigUint::from_f64(if ceiling { f.ceil() } else { f.floor() }).ok_or(Error::BadArithmetic)
}
pub fn memory(size: &BigUint, table: &GasTable, profile: FloatProfile) -> Result<BigUint, Error> {
    if size.bits() > 512 {
        return Err(Error::BadArithmetic);
    }
    Ok(size * table.memory + divided(&(size * size), 512.0, false, profile)?)
}
pub fn memory_limit(
    gas: &BigUint,
    table: &GasTable,
    profile: FloatProfile,
) -> Result<BigUint, Error> {
    if gas.is_zero() {
        return Ok(BigUint::zero());
    }
    if table.memory == 0 {
        return Err(Error::BadArithmetic);
    }
    let mut limit = gas / table.memory + 1u8;
    loop {
        let half = &limit >> 1usize;
        if memory(&half, table, profile)? <= *gas {
            return Ok(limit);
        }
        limit = half;
    }
}
pub fn all_but_one_64th(gas: &BigUint, profile: FloatProfile) -> Result<BigUint, Error> {
    Ok(gas - divided(gas, 64.0, false, profile)?)
}
pub fn copy_words(bytes: &BigUint, profile: FloatProfile) -> Result<BigUint, Error> {
    divided(bytes, 32.0, true, profile)
}
