// MCL 11976be: ec.hpp mulSmallInt/mulArrayBase/mulVecNGLVT and bn.hpp GLV.
// Copyright (c) 2015 MITSUNARI Shigeo. See LICENSE-MCL.
use super::{
    Fr,
    field::{Field, Fp, Fp2},
    point::Point,
};
use crate::BlsField;
use num_bigint::{BigInt, BigUint, Sign};
use std::sync::LazyLock;
mod constants;
#[cfg(test)]
mod tests;

const ABS_Z: u64 = 0xd201000000010000;
static R_INVERSE: LazyLock<BigUint> = LazyLock::new(|| {
    let q = BlsField::Scalar.modulus();
    (BigUint::from(1u8) << 256usize).modpow(&(q - 2u8), q)
});
static Z2: LazyLock<BigInt> = LazyLock::new(|| BigInt::from(ABS_Z) * ABS_Z);
static V0: LazyLock<BigInt> =
    LazyLock::new(|| -((&*Z2 << 256usize) / BigInt::from(BlsField::Scalar.modulus().clone())));
static RW: LazyLock<Fp> = LazyLock::new(|| Fp::read(&constants::RW));
static G2: LazyLock<Fp2> = LazyLock::new(|| Fp2::read(&constants::G2));
static G3: LazyLock<Fp2> = LazyLock::new(|| Fp2::read(&constants::G3));

pub(super) fn decode(raw: &Fr) -> BigUint {
    // Original fromMont multiplies raw limbs by 1. Its result is canonical
    // for every 256-bit input, including inputs >= q. No decimal conversion.
    (BigUint::from_bytes_le(raw) * &*R_INVERSE) % BlsField::Scalar.modulus()
}

pub(super) trait ScalarField: Field {
    fn split(scalar: BigUint) -> Vec<BigInt>;
    fn lambda(point: &Point<Self>) -> Point<Self>;
}
impl ScalarField for Fp {
    fn split(scalar: BigUint) -> Vec<BigInt> {
        let x = BigInt::from(scalar);
        // VINT right-shifts the magnitude and copies the sign. BigInt's
        // arithmetic right shift would round negative values differently.
        let t = (&x * &*V0) / (BigInt::from(1u8) << 256usize);
        let b = (&x * 2u8) >> 256usize;
        vec![&x - (&t * (1u8 - &*Z2) + &b), -(t + b * &*Z2)]
    }
    fn lambda(point: &Point<Self>) -> Point<Self> {
        Point {
            x: point.x.mul(&RW),
            y: point.y.clone(),
            z: point.z.clone(),
        }
    }
}
impl ScalarField for Fp2 {
    fn split(mut scalar: BigUint) -> Vec<BigInt> {
        (0..4)
            .map(|i| {
                let n = BigInt::from(&scalar % ABS_Z);
                scalar /= ABS_Z;
                if i % 2 == 0 { n } else { -n }
            })
            .collect()
    }
    fn lambda(point: &Point<Self>) -> Point<Self> {
        Point {
            x: point.x.frobenius().mul(&G2),
            y: point.y.frobenius().mul(&G3),
            z: point.z.frobenius(),
        }
    }
}
pub(super) fn naf(value: BigInt, width: u32) -> Vec<i8> {
    let negative = value.sign() == Sign::Minus;
    let mut n = value.magnitude().clone();
    let mut out = Vec::new();
    while n.bits() != 0 {
        let mut digit = 0i8;
        if n.bit(0) {
            let low = n.iter_u64_digits().next().expect("nonzero magnitude");
            digit = (low & ((1 << width) - 1)) as i8;
            if digit >= 1 << (width - 1) {
                digit -= 1 << width;
                n += digit.unsigned_abs();
            } else {
                n -= digit as u8;
            }
        }
        out.push(if negative { -digit } else { digit });
        n >>= 1usize;
    }
    out
}
fn subtract<F: Field>(a: &Point<F>, b: &Point<F>) -> Point<F> {
    a.add(&b.negate())
}
fn small<F: Field>(point: &Point<F>, n: u64) -> Point<F> {
    match n {
        0 => Point::zero(),
        1 => point.clone(),
        2 => point.double(),
        3 => point.double().add(point),
        4 => point.double().double(),
        5 => point.double().double().add(point),
        6 => point.double().add(point).double(),
        7 => subtract(&point.double().double().double(), point),
        8 => point.double().double().double(),
        9 => point.double().double().double().add(point),
        10 => point.double().double().add(point).double(),
        11 => {
            let twice = point.double();
            twice.double().double().add(&twice).add(point)
        }
        12 => {
            let four = point.double().double();
            four.add(&four.double())
        }
        13 => {
            let four = point.double().double();
            four.add(&four.double()).add(point)
        }
        14 => subtract(&point.double().double().double(), point).double(),
        15 => subtract(&point.double().double().double().double(), point),
        16 => point.double().double().double().double(),
        _ => unreachable!(),
    }
}
pub(super) fn multiply<F: ScalarField>(point: &Point<F>, raw: &Fr) -> Point<F> {
    let scalar = decode(raw);
    let word = scalar.iter_u64_digits().next().unwrap_or(0);
    if scalar.bits() <= 64 && word <= 16 {
        return small(point, word);
    }
    let glv = scalar.bits() > 64;
    let width = if glv {
        5
    } else if word <= 1 << 32 {
        3
    } else {
        4
    };
    let parts = if glv {
        F::split(scalar)
    } else {
        vec![BigInt::from(scalar)]
    };
    let digits: Vec<_> = parts.into_iter().map(|p| naf(p, width)).collect();
    let size = 1 << (width - 2);
    let mut tables = vec![Vec::with_capacity(size); digits.len()];
    let twice = point.double();
    tables[0].push(point.clone());
    for j in 0..size {
        if j != 0 {
            let next = tables[0][j - 1].add(&twice);
            tables[0].push(next);
        }
        for k in 1..tables.len() {
            let next = F::lambda(&tables[k - 1][j]);
            tables[k].push(next);
        }
    }
    let mut result = Point::zero();
    for i in (0..digits.iter().map(Vec::len).max().unwrap_or(0)).rev() {
        result = result.double();
        for (table, digits) in tables.iter().zip(&digits) {
            let digit = digits.get(i).copied().unwrap_or(0);
            if digit != 0 {
                let p = &table[usize::from((digit.unsigned_abs() - 1) / 2)];
                result = if digit > 0 {
                    result.add(p)
                } else {
                    subtract(&result, p)
                };
            }
        }
    }
    result
}
