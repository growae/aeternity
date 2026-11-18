// MCL 11976be: bn.hpp Miller loop, sparse products and final exponentiation;
// fp_tower.hpp Fp6/Fp12 inversion and double-width squaring.
// Copyright (c) 2015 MITSUNARI Shigeo. See LICENSE-MCL.
use super::{Field, Fp2, Fp2Wide, Fp6, Fp6Wide, Fp12};
use crate::bls::{field::Fp, point::Point};
use std::sync::LazyLock;
mod constants;
static FROBENIUS2: LazyLock<[Fp; 5]> =
    LazyLock::new(|| constants::FROBENIUS2.each_ref().map(Fp::read));

impl Fp6Wide {
    fn square(x: &Fp6) -> Self {
        let [a, b, c] = &x.0;
        let twice = b.double();
        let bc = Fp2Wide::product(&twice, c);
        let ab = Fp2Wide::product(&twice, a);
        let aa = Fp2Wide::square(a);
        let cc = Fp2Wide::square(c);
        let t = Fp2Wide::square(&a.add(b).add(c));
        Self([
            aa.add(&bc.mul_xi()),
            cc.mul_xi().add(&ab),
            t.sub(&aa).sub(&bc).sub(&cc).sub(&ab),
        ])
    }
}
impl Fp6 {
    fn inverse(&self) -> Self {
        let [a, b, c] = &self.0;
        let aa = Fp2Wide::square(a);
        let bb = Fp2Wide::square(b);
        let cc = Fp2Wide::square(c);
        let ab = Fp2Wide::product(a, b);
        let bc = Fp2Wide::product(b, c);
        let ac = Fp2Wide::product(c, a);
        let pa = aa.sub(&bc.mul_xi()).reduce();
        let pb = cc.mul_xi().sub(&ab).reduce();
        let pc = bb.sub(&ac).reduce();
        let q = Fp2Wide::product(&pb, c)
            .add(&Fp2Wide::product(&pc, b))
            .mul_xi()
            .add_pre(&Fp2Wide::product(&pa, a))
            .reduce()
            .inverse();
        Self([pa.mul(&q), pb.mul(&q), pc.mul(&q)])
    }
    fn mul_01(&self, d: &Fp2, e: &Fp2) -> Self {
        let [a, b, c] = &self.0;
        let ad = Fp2Wide::product(a, d);
        let ce = Fp2Wide::product(c, e);
        let be = Fp2Wide::product(b, e);
        let cd = Fp2Wide::product(c, d);
        let t = Fp2Wide::product(&a.add(b), &d.add(e));
        Self([
            ad.add(&ce.mul_xi()).reduce(),
            t.sub(&ad).sub(&be).reduce(),
            be.add(&cd).reduce(),
        ])
    }
}
fn square_fp4(x0: &Fp2, x1: &Fp2) -> (Fp2, Fp2) {
    let t0 = Fp2Wide::square(x0);
    let t1 = Fp2Wide::square(x1);
    (
        t1.mul_xi().add(&t0).reduce(),
        Fp2Wide::square(&x0.add(x1)).sub(&t0).sub(&t1).reduce(),
    )
}
impl Fp12 {
    fn zero() -> Self {
        Self(std::array::from_fn(|_| {
            Fp6(std::array::from_fn(|_| Fp2::zero()))
        }))
    }
    fn is_zero(&self) -> bool {
        self.0.iter().all(|x| x.0.iter().all(Field::is_zero))
    }
    fn inverse(&self) -> Self {
        let [a, b] = &self.0;
        let aa = Fp6Wide::square(a);
        let bb = Fp6Wide::square(b);
        let t = Fp6Wide([
            aa.0[0].sub(&bb.0[2].mul_xi()),
            aa.0[1].sub(&bb.0[0]),
            aa.0[2].sub(&bb.0[1]),
        ])
        .reduce()
        .inverse();
        let out_b = b.mul(&t);
        Self([a.mul(&t), Fp6(out_b.0.each_ref().map(Field::neg))])
    }
    fn frobenius2(&self) -> Self {
        Self(std::array::from_fn(|i| {
            Fp6(std::array::from_fn(|j| {
                let n = i * 3 + j;
                if n == 0 {
                    self.0[i].0[j].clone()
                } else {
                    self.0[i].0[j].mul_fp(&FROBENIUS2[n - 1])
                }
            }))
        }))
    }
    fn cyclotomic_square(&self) -> Self {
        let [x0, x4, x3] = &self.0[0].0;
        let [x2, x1, x5] = &self.0[1].0;
        let (t0, t1) = square_fp4(x0, x1);
        let y0 = t0.sub(x0).double().add(&t0);
        let y1 = t1.add(x1).double().add(&t1);
        let (t0, t1) = square_fp4(x2, x3);
        let (t2, t3) = square_fp4(x4, x5);
        let y4 = t0.sub(x4).double().add(&t0);
        let y5 = t1.add(x5).double().add(&t1);
        let t0 = t3.mul_xi();
        let y2 = t0.add(x2).double().add(&t0);
        let y3 = t2.sub(x3).double().add(&t2);
        Self([Fp6([y0, y4, y3]), Fp6([y2, y1, y5])])
    }
    fn pow_z(&self) -> Self {
        let mut out = self.clone();
        let conjugate = self.unitary_inverse();
        for &digit in &constants::Z_DIGITS[1..] {
            out = out.cyclotomic_square();
            if digit > 0 {
                out = out.mul(self);
            } else if digit < 0 {
                out = out.mul(&conjugate);
            }
        }
        out.unitary_inverse()
    }
    pub(in crate::bls) fn final_exponentiation(&self) -> Self {
        if self.is_zero() {
            return Self::zero();
        }
        let z = self.frobenius2().mul(self);
        let x = z.inverse().mul(&z.unitary_inverse());
        // Preserve original expHardPartBLS12, including its factor of three.
        let mut a0 = x.pow_z().mul(&x.unitary_inverse());
        let mut a1 = a0.pow_z();
        a0 = a0.unitary_inverse().mul(&a1);
        a1 = a0.pow_z();
        a0 = a0.frobenius().mul(&a1);
        a1 = a0.pow_z().pow_z();
        let a2 = a0.frobenius2();
        a0 = a0.unitary_inverse().mul(&a1).mul(&a2);
        a1 = x.cyclotomic_square().mul(&x);
        a0.mul(&a1)
    }
    fn sparse(line: &Fp6) -> Self {
        let [a, b, c] = &line.0;
        // The pinned BLS12-381 curve is M-type: (a,c,0,0,b,0).
        Self([
            Fp6([a.clone(), c.clone(), Fp2::zero()]),
            Fp6([Fp2::zero(), b.clone(), Fp2::zero()]),
        ])
    }
    fn mul_sparse(&self, line: &Fp6) -> Self {
        let [a, b, c] = &line.0;
        let [z0, z1] = &self.0;
        let z1x1 = Fp6([z1.0[2].mul(b).mul_xi(), z1.0[0].mul(b), z1.0[1].mul(b)]);
        let z0x0 = z0.mul_01(a, c);
        let t0 = z0.add(z1).mul_01(a, &b.add(c));
        Self([
            Fp6([
                z0x0.0[0].add(&z1x1.0[2].mul_xi()),
                z0x0.0[1].add(&z1x1.0[0]),
                z0x0.0[2].add(&z1x1.0[1]),
            ]),
            t0.sub(&z0x0).sub(&z1x1),
        ])
    }
    pub(in crate::bls) fn miller_loop(p: &Point<Fp>, q: &Point<Fp2>) -> Self {
        let p = p.normalize();
        let q = q.normalize();
        if q.is_zero() {
            return Self::one();
        }
        let mut t = q.clone();
        let neg_q = q.negate();
        let adjusted = Point {
            x: p.x.double().add(&p.x),
            y: p.y.neg(),
            z: Fp::zero(),
        };
        let d = double_line(&mut t, &adjusted);
        let e = add_line(&mut t, &q, &p);
        let mut out = Self::sparse(&d).mul_sparse(&e);
        for &digit in &constants::MILLER_DIGITS[2..] {
            let e = double_line(&mut t, &adjusted);
            out = out.square().mul_sparse(&e);
            if digit != 0 {
                let e = add_line(&mut t, if digit > 0 { &q } else { &neg_q }, &p);
                out = out.mul_sparse(&e);
            }
        }
        out.unitary_inverse()
    }
}
fn update_line(line: Fp6, p: &Point<Fp>) -> Fp6 {
    let [a, b, c] = line.0;
    Fp6([a, b.mul_fp(&p.y), c.mul_fp(&p.x)])
}
fn double_line(q: &mut Point<Fp2>, p: &Point<Fp>) -> Fp6 {
    let mut t0 = q.z.square();
    let t4 = q.x.mul(&q.y).half();
    let t1 = q.y.square();
    let t3 = t0.double();
    let t5 = t0.add(&t1);
    t0 = t0.add(&t3);
    let t2 = t0.mul(&Fp2::curve_b());
    t0 = q.x.square();
    let mut t3 = t2.double().add(&t2);
    let x = t1.sub(&t3).mul(&t4);
    t3 = t3.add(&t1).half();
    let w0 = Fp2Wide::square(&t3);
    let w1 = Fp2Wide::square(&t2);
    let y = w0.sub(&w1).sub(&w1.add(&w1)).reduce();
    t3 = q.y.add(&q.z).square().sub(&t5);
    let z = t1.mul(&t3);
    *q = Point { x, y, z };
    update_line(Fp6([t2.sub(&t1), t3, t0]), p)
}
fn add_line(r: &mut Point<Fp2>, q: &Point<Fp2>, p: &Point<Fp>) -> Fp6 {
    let t1 = r.x.sub(&r.z.mul(&q.x));
    let t2 = r.y.sub(&r.z.mul(&q.y));
    let mut t3 = t1.square();
    let rx = t3.mul(&r.x);
    let mut t4 = t2.square();
    t3 = t3.mul(&t1);
    t4 = t4.mul(&r.z).add(&t3).sub(&rx).sub(&rx);
    let y = Fp2Wide::product(&t2, &rx.sub(&t4))
        .sub(&Fp2Wide::product(&t3, &r.y))
        .reduce();
    let x = t1.mul(&t4);
    let z = t3.mul(&r.z);
    *r = Point { x, y, z };
    let a = Fp2Wide::product(&t2, &q.x)
        .sub(&Fp2Wide::product(&t1, &q.y))
        .reduce();
    update_line(Fp6([a, t1, t2.neg()]), p)
}
