// MCL 11976be include/mcl/ec.hpp: Jacobi coordinates and operation ordering.
// Copyright (c) 2015 MITSUNARI Shigeo. See LICENSE-MCL.
use super::field::Field;

#[derive(Clone, PartialEq, Debug)]
pub(super) struct Point<F> {
    pub(super) x: F,
    pub(super) y: F,
    pub(super) z: F,
}
impl<F: Field> Point<F> {
    pub(super) fn zero() -> Self {
        Self {
            x: F::zero(),
            y: F::zero(),
            z: F::zero(),
        }
    }
    pub(super) fn is_zero(&self) -> bool {
        self.z.is_zero()
    }
    pub(super) fn negate(&self) -> Self {
        if self.is_zero() {
            Self::zero()
        } else {
            Self {
                x: self.x.clone(),
                y: self.y.neg(),
                z: self.z.clone(),
            }
        }
    }
    pub(super) fn normalize(&self) -> Self {
        if self.is_zero() {
            return self.clone();
        }
        let iz = self.z.inverse();
        let iz2 = iz.square();
        Self {
            x: self.x.mul(&iz2),
            y: self.y.mul(&iz2).mul(&iz),
            z: F::one(),
        }
    }
    pub(super) fn is_valid(&self) -> bool {
        let x2 = self.x.square();
        let y2 = self.y.square();
        let z2 = self.z.square();
        let z4 = z2.square();
        // Preserve multiplication by a=0: noncanonical raw limbs can affect
        // intermediate NF operations even when the mathematical term is zero.
        let t = z4.mul(&F::zero()).add(&x2).mul(&self.x);
        y2 == t.add(&z4.mul(&z2).mul(&F::curve_b()))
    }
    pub(super) fn double(&self) -> Self {
        if self.is_zero() {
            return Self::zero();
        }
        let mut x2 = self.x.square();
        let mut y2 = self.y.square();
        let xy0 = self.x.add(&y2);
        y2 = y2.square();
        let xy = xy0.square().sub(&x2).sub(&y2).double();
        x2 = x2.add(&x2.double());
        let x = x2.square().sub(&xy).sub(&xy);
        let z = if self.z.is_one() {
            self.y.clone()
        } else {
            self.y.mul(&self.z)
        }
        .double();
        let y = xy.sub(&x).mul(&x2).sub(&y2.double().double().double());
        Self { x, y, z }
    }
    pub(super) fn add(&self, other: &Self) -> Self {
        if self.is_zero() {
            return other.clone();
        }
        if other.is_zero() {
            return self.clone();
        }
        let p_one = self.z.is_one();
        let q_one = other.z.is_one();
        let p_z2 = if p_one { F::one() } else { self.z.square() };
        let (u1, s1) = if q_one {
            (self.x.clone(), self.y.clone())
        } else {
            let q_z2 = other.z.square();
            (self.x.mul(&q_z2), q_z2.mul(&other.z).mul(&self.y))
        };
        let h = if p_one {
            other.x.clone()
        } else {
            other.x.mul(&p_z2)
        }
        .sub(&u1);
        let r = if p_one {
            other.y.clone()
        } else {
            p_z2.mul(&self.z).mul(&other.y)
        }
        .sub(&s1);
        if h.is_zero() {
            return if r.is_zero() {
                self.double()
            } else {
                Self::zero()
            };
        }
        let z = match (p_one, q_one) {
            (true, true) => h.clone(),
            (true, false) => h.mul(&other.z),
            (false, true) => self.z.mul(&h),
            (false, false) => self.z.mul(&other.z).mul(&h),
        };
        let h2 = h.square();
        let r2 = r.square();
        let u1 = u1.mul(&h2);
        let h3 = h2.mul(&h);
        let x = r2.sub(&u1).sub(&u1).sub(&h3);
        let y = u1.sub(&x).mul(&r).sub(&h3.mul(&s1));
        Self { x, y, z }
    }
}
