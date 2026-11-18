// MCL 11976be: fp_tower.hpp Fp6/Fp12, bn.hpp GLV2 and ec.hpp mulVecNGLVT.
// Copyright (c) 2015 MITSUNARI Shigeo. See LICENSE-MCL.
use super::{
    Fr, Gt,
    field::{Field, Fp2, Fp2Wide},
    scalar::{self, ScalarField},
};
use std::sync::LazyLock;
mod constants;
mod pairing;
#[cfg(test)]
mod tests;
static FROBENIUS: LazyLock<[Fp2; 5]> =
    LazyLock::new(|| constants::FROBENIUS.each_ref().map(Fp2::read));

#[derive(Clone)]
struct Fp6([Fp2; 3]);
struct Fp6Wide([Fp2Wide; 3]);
impl Fp6 {
    fn add(&self, other: &Self) -> Self {
        Self(std::array::from_fn(|i| self.0[i].add(&other.0[i])))
    }
    fn sub(&self, other: &Self) -> Self {
        Self(std::array::from_fn(|i| self.0[i].sub(&other.0[i])))
    }
    fn double(&self) -> Self {
        Self(self.0.each_ref().map(Field::double))
    }
    fn mul(&self, other: &Self) -> Self {
        Fp6Wide::product(self, other).reduce()
    }
    fn mul_v_add(&self, other: &Self) -> Self {
        Self([
            self.0[2].mul_xi().add(&other.0[0]),
            self.0[0].add(&other.0[1]),
            self.0[1].add(&other.0[2]),
        ])
    }
}
impl Fp6Wide {
    fn product(x: &Fp6, y: &Fp6) -> Self {
        let [a, b, c] = &x.0;
        let [d, e, f] = &y.0;
        let za = Fp2Wide::product(&b.add_pre(c), &e.add_pre(f));
        let zb = Fp2Wide::product(&a.add_pre(b), &e.add_pre(d));
        let zc = Fp2Wide::product(&a.add_pre(c), &d.add_pre(f));
        let be = Fp2Wide::product(b, e);
        let cf = Fp2Wide::product(c, f);
        let ad = Fp2Wide::product(a, d);
        let za = za.sub_special(&be).sub_special(&cf);
        let zb = zb.sub_special(&ad).sub_special(&be);
        let zc = zc.sub_special(&ad).sub_special(&cf);
        Self([za.mul_xi().add(&ad), zb.add(&cf.mul_xi()), zc.add(&be)])
    }
    fn reduce(&self) -> Fp6 {
        Fp6(self.0.each_ref().map(Fp2Wide::reduce))
    }
    fn sub(&self, other: &Self) -> Self {
        Self(std::array::from_fn(|i| self.0[i].sub(&other.0[i])))
    }
    fn mul_v_add(&self, other: &Self) -> Self {
        Self([
            self.0[2].mul_xi().add(&other.0[0]),
            self.0[0].add(&other.0[1]),
            self.0[1].add(&other.0[2]),
        ])
    }
}
#[derive(Clone)]
pub(super) struct Fp12([Fp6; 2]);
impl Fp12 {
    pub(super) fn read(bytes: &Gt) -> Self {
        Self(std::array::from_fn(|i| {
            Fp6(std::array::from_fn(|j| {
                Fp2::read(&[bytes[6 * i + 2 * j], bytes[6 * i + 2 * j + 1]])
            }))
        }))
    }
    pub(super) fn bytes(&self) -> Gt {
        std::array::from_fn(|i| self.0[i / 6].0[(i % 6) / 2].0[i % 2].bytes())
    }
    fn one() -> Self {
        Self([
            Fp6([Fp2::one(), Fp2::zero(), Fp2::zero()]),
            Fp6(std::array::from_fn(|_| Fp2::zero())),
        ])
    }
    pub(super) fn mul(&self, other: &Self) -> Self {
        let [a, b] = &self.0;
        let [c, d] = &other.0;
        let t1 = a.add(b);
        let t2 = c.add(d);
        let ac = Fp6Wide::product(a, c);
        let bd = Fp6Wide::product(b, d);
        Self([
            bd.mul_v_add(&ac).reduce(),
            Fp6Wide::product(&t1, &t2).sub(&ac).sub(&bd).reduce(),
        ])
    }
    fn square(&self) -> Self {
        let [a, b] = &self.0;
        let t0 = a.add(b).mul(&b.mul_v_add(a));
        let ab = a.mul(b);
        Self([t0.sub(&ab.mul_v_add(&ab)), ab.double()])
    }
    fn unitary_inverse(&self) -> Self {
        Self([
            self.0[0].clone(),
            Fp6(self.0[1].0.each_ref().map(Field::neg)),
        ])
    }
    fn frobenius(&self) -> Self {
        Self(std::array::from_fn(|i| {
            Fp6(std::array::from_fn(|j| {
                let index = 3 * i + j;
                let value = self.0[i].0[j].frobenius();
                if index == 0 {
                    value
                } else {
                    value.mul(&FROBENIUS[index - 1])
                }
            }))
        }))
    }
    pub(super) fn pow(&self, raw: &Fr) -> Self {
        // Fr.getBlock retains all four limbs. Operator::pow therefore uses
        // GLV even for zero and tiny scalars; no small-scalar shortcut.
        let digits: Vec<_> = Fp2::split(scalar::decode(raw))
            .into_iter()
            .map(|x| scalar::naf(x, 5))
            .collect();
        let mut tables: [Vec<Self>; 4] = std::array::from_fn(|_| Vec::with_capacity(8));
        let squared = self.square();
        tables[0].push(self.clone());
        for j in 0..8 {
            if j != 0 {
                let next = tables[0][j - 1].mul(&squared);
                tables[0].push(next);
            }
            for k in 1..4 {
                let next = tables[k - 1][j].frobenius();
                tables[k].push(next);
            }
        }
        let mut result = Self::one();
        for i in (0..digits.iter().map(Vec::len).max().unwrap_or(0)).rev() {
            result = result.square();
            for (table, digits) in tables.iter().zip(&digits) {
                let digit = digits.get(i).copied().unwrap_or(0);
                if digit != 0 {
                    let p = &table[usize::from((digit.unsigned_abs() - 1) / 2)];
                    result = if digit > 0 {
                        result.mul(p)
                    } else {
                        result.mul(&p.unitary_inverse())
                    };
                }
            }
        }
        result
    }
}
