// Historical aevm_eeevm arithmetic, fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC: repository LICENSE.
use crate::opcode::Version;
use num_bigint::{BigInt, BigUint, Sign};
use num_traits::{One, ToPrimitive, Zero};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Word(pub [u8; 32]);
impl Word {
    pub const ZERO: Self = Self([0; 32]);
    pub fn unsigned(self) -> BigUint {
        BigUint::from_bytes_be(&self.0)
    }
    pub fn signed(self) -> BigInt {
        BigInt::from_signed_bytes_be(&self.0)
    }
    pub fn wrapping(n: &BigInt) -> Self {
        let modulus = BigInt::one() << 256;
        let u: BigInt = (n % &modulus + &modulus) % &modulus;
        Self::from_unsigned(u.magnitude())
    }
    pub fn from_unsigned(n: &BigUint) -> Self {
        let bytes = (n & ((BigUint::one() << 256usize) - 1u8)).to_bytes_be();
        let mut out = [0; 32];
        out[32 - bytes.len()..].copy_from_slice(&bytes);
        Self(out)
    }
    pub fn is_zero(self) -> bool {
        self == Self::ZERO
    }
}
impl From<u64> for Word {
    fn from(n: u64) -> Self {
        let mut out = [0; 32];
        out[24..].copy_from_slice(&n.to_be_bytes());
        Self(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticError {
    OverflowOrDomain,
}
fn truncate(n: BigInt, vm: Version) -> Result<Word, ArithmeticError> {
    let word = Word::wrapping(&n);
    if vm.safe_math() && word.signed() != n {
        Err(ArithmeticError::OverflowOrDomain)
    } else {
        Ok(word)
    }
}
fn domain(fallback: Word, vm: Version) -> Result<Word, ArithmeticError> {
    if vm.safe_math() {
        Err(ArithmeticError::OverflowOrDomain)
    } else {
        Ok(fallback)
    }
}

// Inputs are stack words in pop order; the caller performs opcode admission.
pub fn binary(op: u8, a: Word, b: Word, vm: Version) -> Result<Word, ArithmeticError> {
    let u = a.unsigned();
    let v = b.unsigned();
    let integer = |n: BigUint| BigInt::from_biguint(Sign::Plus, n);
    let boolean = |b| Word::from(u64::from(b));
    Ok(match op {
        1 => return truncate(a.signed() + b.signed(), vm),
        2 => return truncate(a.signed() * b.signed(), vm),
        3 => return truncate(a.signed() - b.signed(), vm),
        4..=7 if b.is_zero() => return domain(Word::ZERO, vm),
        4 => Word::from_unsigned(&(u / v)),
        5 => return truncate(a.signed() / b.signed(), vm),
        6 => return truncate(integer(u % v), vm),
        7 => return truncate(a.signed() % b.signed(), vm),
        10 => {
            let (mut x, exponent) = if vm.safe_math() {
                (a.signed(), b.signed())
            } else {
                (integer(u), integer(v))
            };
            if exponent.sign() == Sign::Minus {
                return Err(ArithmeticError::OverflowOrDomain);
            }
            let mut y = exponent.magnitude().clone();
            let mut acc = BigInt::one();
            while y > BigUint::one() {
                // The original truncates each square before updating the accumulator.
                let square = truncate(&x * &x, vm)?.unsigned();
                if y.bit(0) {
                    acc *= &x;
                }
                x = integer(square);
                y >>= 1usize;
            }
            if !y.is_zero() {
                acc *= x;
            }
            return truncate(acc, vm);
        }
        11 => {
            let bytes = usize::from(a.0[31] & 31) + 1;
            let mut out = b.0;
            let fill = if out[32 - bytes] & 128 != 0 { 255 } else { 0 };
            out[..32 - bytes].fill(fill);
            Word(out)
        }
        16 => boolean(a < b),
        17 => boolean(a > b),
        18 => boolean(a.signed() < b.signed()),
        19 => boolean(a.signed() > b.signed()),
        20 => boolean(a == b),
        22 => Word::from_unsigned(&(u & v)),
        23 => Word::from_unsigned(&(u | v)),
        24 => Word::from_unsigned(&(u ^ v)),
        26 => Word::from(
            u.to_usize()
                .filter(|i| *i < 32)
                .map_or(0, |i| u64::from(b.0[i])),
        ),
        27..=29 => {
            let Some(shift) = u.to_usize().filter(|i| *i < 256) else {
                return domain(
                    if op == 29 && b.0[0] & 128 != 0 {
                        Word([255; 32])
                    } else {
                        Word::ZERO
                    },
                    vm,
                );
            };
            match op {
                27 => Word::from_unsigned(&(v << shift)),
                28 => Word::from_unsigned(&(v >> shift)),
                _ => Word::wrapping(&(b.signed() >> shift)),
            }
        }
        _ => return Err(ArithmeticError::OverflowOrDomain),
    })
}
pub fn modular(op: u8, a: Word, b: Word, c: Word, vm: Version) -> Result<Word, ArithmeticError> {
    if c.is_zero() {
        return domain(Word::ZERO, vm);
    }
    let n = match op {
        8 => a.unsigned() + b.unsigned(),
        9 => a.unsigned() * b.unsigned(),
        _ => return Err(ArithmeticError::OverflowOrDomain),
    };
    truncate(BigInt::from(n % c.unsigned()), vm)
}
