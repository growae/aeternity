// MCL 11976be: fp.cpp, low_func.hpp, fp_tower.hpp and generated base64.ll.
// Copyright (c) 2015 MITSUNARI Shigeo. See LICENSE-MCL.
use crate::BlsField;
use num_bigint::BigUint;
use std::sync::LazyLock;
#[cfg(test)]
mod tests;

static R: LazyLock<BigUint> = LazyLock::new(|| BigUint::from(1u8) << 384usize);
static MASK: LazyLock<BigUint> = LazyLock::new(|| &*R - 1u8);
static MASK448: LazyLock<BigUint> = LazyLock::new(|| (BigUint::from(1u8) << 448usize) - 1u8);
static R2: LazyLock<BigUint> = LazyLock::new(|| &*R * &*R);
static MASK768: LazyLock<BigUint> = LazyLock::new(|| &*R2 - 1u8);
static RADIX832: LazyLock<BigUint> = LazyLock::new(|| &*R2 << 64usize);
static P_RADIX: LazyLock<BigUint> = LazyLock::new(|| modulus() << 384usize);
static R3MOD: LazyLock<BigUint> = LazyLock::new(|| (&*R2 * &*R) % modulus());
fn modulus() -> &'static BigUint {
    BlsField::Base.modulus()
}
fn low(x: &BigUint) -> u64 {
    x.iter_u64_digits().next().unwrap_or(0)
}
// -p^-1 mod 2^64 for BLS12-381 Fp.
const N0: u64 = 0x89f3fffcfffcfffd;
fn subtract(x: &BigUint, y: &BigUint) -> BigUint {
    (x + &*R - y) & &*MASK
}
fn finish(x: BigUint) -> BigUint {
    let difference = subtract(&x, modulus());
    if difference.bit(383) { x } else { difference }
}

pub(super) trait Field: Clone + PartialEq {
    fn zero() -> Self;
    fn one() -> Self;
    fn curve_b() -> Self;
    fn is_zero(&self) -> bool;
    fn is_one(&self) -> bool {
        self == &Self::one()
    }
    fn add(&self, other: &Self) -> Self;
    fn sub(&self, other: &Self) -> Self;
    fn neg(&self) -> Self;
    fn mul(&self, other: &Self) -> Self;
    fn square(&self) -> Self {
        self.mul(self)
    }
    fn double(&self) -> Self {
        self.add(self)
    }
    fn inverse(&self) -> Self;
}

// Raw limbs are deliberately not canonicalized: emcl accepts every 48-byte
// string. Fixed-width intermediate overflow and NF sign tests are observable.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Fp(pub(super) BigUint);
impl Fp {
    pub(super) fn read(bytes: &[u8; 48]) -> Self {
        Self(BigUint::from_bytes_le(bytes))
    }
    pub(super) fn bytes(&self) -> [u8; 48] {
        let b = self.0.to_bytes_le();
        let mut out = [0; 48];
        out[..b.len()].copy_from_slice(&b);
        out
    }
    fn integer(n: u8) -> Self {
        Self((&*R * n) % modulus())
    }
    fn add_pre(&self, other: &Self) -> Self {
        Self((&self.0 + &other.0) & &*MASK)
    }
    fn product(&self, other: &Self) -> BigUint {
        &self.0 * &other.0
    }
    fn reduce(mut t: BigUint) -> Self {
        for _ in 0..6 {
            let m = low(&t).wrapping_mul(N0);
            t = (t + modulus() * m) >> 64usize;
        }
        Self(finish(t & &*MASK))
    }
}
impl Field for Fp {
    fn zero() -> Self {
        Self(BigUint::default())
    }
    fn one() -> Self {
        Self::integer(1)
    }
    fn curve_b() -> Self {
        Self::integer(4)
    }
    fn is_zero(&self) -> bool {
        self.0.bits() == 0
    }
    fn add(&self, other: &Self) -> Self {
        Self(finish((&self.0 + &other.0) & &*MASK))
    }
    fn sub(&self, other: &Self) -> Self {
        let t = subtract(&self.0, &other.0);
        Self(if t.bit(383) {
            (t + modulus()) & &*MASK
        } else {
            t
        })
    }
    fn neg(&self) -> Self {
        if self.is_zero() {
            Self::zero()
        } else {
            Self(subtract(modulus(), &self.0))
        }
    }
    fn mul(&self, other: &Self) -> Self {
        let mut t = BigUint::default();
        let mut words = other.0.iter_u64_digits();
        for _ in 0..6 {
            let word = words.next().unwrap_or(0);
            t = (t + &self.0 * word) & &*MASK448;
            let m = low(&t).wrapping_mul(N0);
            t = ((t + modulus() * m) & &*MASK448) >> 64usize;
        }
        Self(finish(t & &*MASK))
    }
    fn inverse(&self) -> Self {
        // Pinned release VINT/arm64: raw p inverts to R^2, while raw zero
        // and larger multiples of p invert to zero. Do not infer another
        // build's division-by-zero behavior from this compatibility profile.
        let inverse = if &self.0 == modulus() {
            BigUint::from(1u8)
        } else {
            self.0.modpow(&(modulus() - 2u8), modulus())
        };
        Self(inverse).mul(&Self(R3MOD.clone()))
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Fp2(pub(super) [Fp; 2]);
fn sub_pre(x: &BigUint, y: &BigUint) -> BigUint {
    (x + &*R2 - y) & &*MASK768
}
fn sub_dbl(x: &BigUint, y: &BigUint) -> BigUint {
    let difference = sub_pre(x, y);
    if x < y {
        (difference + &*P_RADIX) & &*MASK768
    } else {
        difference
    }
}
impl Fp2 {
    pub(super) fn read(bytes: &[[u8; 48]; 2]) -> Self {
        Self(bytes.each_ref().map(Fp::read))
    }
    pub(super) fn bytes(&self) -> [[u8; 48]; 2] {
        self.0.each_ref().map(Fp::bytes)
    }
    pub(super) fn add_pre(&self, other: &Self) -> Self {
        Self(std::array::from_fn(|i| self.0[i].add_pre(&other.0[i])))
    }
    pub(super) fn mul_xi(&self) -> Self {
        Self([self.0[0].sub(&self.0[1]), self.0[0].add(&self.0[1])])
    }
    pub(super) fn frobenius(&self) -> Self {
        Self([self.0[0].clone(), self.0[1].neg()])
    }
}
impl Field for Fp2 {
    fn zero() -> Self {
        Self([Fp::zero(), Fp::zero()])
    }
    fn one() -> Self {
        Self([Fp::one(), Fp::zero()])
    }
    fn curve_b() -> Self {
        Self([Fp::integer(4), Fp::integer(4)])
    }
    fn is_zero(&self) -> bool {
        self.0.iter().all(Field::is_zero)
    }
    fn add(&self, other: &Self) -> Self {
        Self(std::array::from_fn(|i| self.0[i].add(&other.0[i])))
    }
    fn sub(&self, other: &Self) -> Self {
        Self(std::array::from_fn(|i| self.0[i].sub(&other.0[i])))
    }
    fn neg(&self) -> Self {
        Self(self.0.each_ref().map(Field::neg))
    }
    fn mul(&self, other: &Self) -> Self {
        Fp2Wide::product(self, other).reduce()
    }
    fn square(&self) -> Self {
        let [a, b] = &self.0;
        Self([a.add(b).mul(&a.sub(b)), b.double().mul(a)])
    }
    fn inverse(&self) -> Self {
        let [a, b] = &self.0;
        let norm = Fp::reduce((a.product(a) + b.product(b)) & &*MASK768);
        let inv = norm.inverse();
        Self([a.mul(&inv), b.mul(&inv).neg()])
    }
}

// MCL's double-width field intermediates are not ordinary reduced Fp2 values.
#[derive(Clone)]
pub(super) struct Fp2Wide([BigUint; 2]);
fn add_dbl(x: &BigUint, y: &BigUint) -> BigUint {
    let sum = x + y;
    let difference = &sum + &*RADIX832 - &*P_RADIX;
    // Preserve the LLVM i448 high-half bit-384 selection, including a carry
    // from arbitrary raw operands. It is not an unrestricted modulus test.
    if difference.bit(768) {
        sum & &*MASK768
    } else {
        difference & &*MASK768
    }
}
impl Fp2Wide {
    pub(super) fn product(x: &Fp2, y: &Fp2) -> Self {
        let [a, b] = &x.0;
        let [c, d] = &y.0;
        let ac = a.product(c);
        let bd = b.product(d);
        let cross = a.add_pre(b).product(&c.add_pre(d));
        Self([sub_dbl(&ac, &bd), sub_pre(&sub_pre(&cross, &ac), &bd)])
    }
    pub(super) fn reduce(&self) -> Fp2 {
        Fp2(self.0.each_ref().map(|x| Fp::reduce(x.clone())))
    }
    pub(super) fn add(&self, other: &Self) -> Self {
        Self(std::array::from_fn(|i| add_dbl(&self.0[i], &other.0[i])))
    }
    pub(super) fn sub(&self, other: &Self) -> Self {
        Self(std::array::from_fn(|i| sub_dbl(&self.0[i], &other.0[i])))
    }
    pub(super) fn sub_special(&self, other: &Self) -> Self {
        Self([
            sub_dbl(&self.0[0], &other.0[0]),
            sub_pre(&self.0[1], &other.0[1]),
        ])
    }
    pub(super) fn mul_xi(&self) -> Self {
        Self([
            sub_dbl(&self.0[0], &self.0[1]),
            add_dbl(&self.0[0], &self.0[1]),
        ])
    }
}
