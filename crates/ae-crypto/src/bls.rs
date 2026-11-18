// Legacy emcl raw coordinates, not standard compressed BLS encodings.
// The explicit native profile must come from trusted chain compatibility data.
// MCL 11976be. Copyright (c) 2015 MITSUNARI Shigeo. See LICENSE-MCL.
mod field;
mod point;
mod scalar;
mod tower;
use field::{Field, Fp, Fp2};
use point::Point;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Mcl11976beLlvm64VintArm64,
}

pub type G1 = [[u8; 48]; 3];
pub type G2 = [[[u8; 48]; 2]; 3];
pub type Gt = [[u8; 48]; 12];
pub type Fr = [u8; 32];

fn read_g1(p: &G1) -> Point<Fp> {
    let [x, y, z] = p.each_ref().map(Fp::read);
    Point { x, y, z }
}
fn write_g1(p: Point<Fp>) -> G1 {
    [p.x.bytes(), p.y.bytes(), p.z.bytes()]
}
fn read_g2(p: &G2) -> Point<Fp2> {
    let [x, y, z] = p.each_ref().map(Fp2::read);
    Point { x, y, z }
}
fn write_g2(p: Point<Fp2>) -> G2 {
    [p.x.bytes(), p.y.bytes(), p.z.bytes()]
}

pub fn g1_negate(_: Profile, p: &G1) -> G1 {
    write_g1(read_g1(p).negate())
}
pub fn g1_normalize(_: Profile, p: &G1) -> G1 {
    write_g1(read_g1(p).normalize())
}
pub fn g1_valid(_: Profile, p: &G1) -> bool {
    read_g1(p).is_valid()
}
pub fn g1_zero(_: Profile, p: &G1) -> bool {
    read_g1(p).is_zero()
}
pub fn g1_add(_: Profile, p: &G1, q: &G1) -> G1 {
    write_g1(read_g1(p).add(&read_g1(q)))
}
pub fn g1_multiply(_: Profile, scalar: &Fr, p: &G1) -> G1 {
    write_g1(scalar::multiply(&read_g1(p), scalar))
}
pub fn g2_negate(_: Profile, p: &G2) -> G2 {
    write_g2(read_g2(p).negate())
}
pub fn g2_normalize(_: Profile, p: &G2) -> G2 {
    write_g2(read_g2(p).normalize())
}
pub fn g2_valid(_: Profile, p: &G2) -> bool {
    read_g2(p).is_valid()
}
pub fn g2_zero(_: Profile, p: &G2) -> bool {
    read_g2(p).is_zero()
}
pub fn g2_add(_: Profile, p: &G2, q: &G2) -> G2 {
    write_g2(read_g2(p).add(&read_g2(q)))
}
pub fn g2_multiply(_: Profile, scalar: &Fr, p: &G2) -> G2 {
    write_g2(scalar::multiply(&read_g2(p), scalar))
}

// MCL's mclBnGT_inv is unitaryInv, not generic Fp12 inversion. It conjugates
// even values outside the target subgroup: a + b*w becomes a - b*w.
pub fn gt_unitary_inverse(_: Profile, value: &Gt) -> Gt {
    std::array::from_fn(|i| {
        if i < 6 {
            value[i]
        } else {
            Fp::read(&value[i]).neg().bytes()
        }
    })
}
pub fn gt_add(_: Profile, left: &Gt, right: &Gt) -> Gt {
    std::array::from_fn(|i| Fp::read(&left[i]).add(&Fp::read(&right[i])).bytes())
}
pub fn gt_is_one(_: Profile, value: &Gt) -> bool {
    Fp::read(&value[0]).is_one() && value[1..].iter().all(|v| Fp::read(v).is_zero())
}
pub fn gt_multiply(_: Profile, left: &Gt, right: &Gt) -> Gt {
    tower::Fp12::read(left)
        .mul(&tower::Fp12::read(right))
        .bytes()
}
pub fn gt_power(_: Profile, value: &Gt, scalar: &Fr) -> Gt {
    tower::Fp12::read(value).pow(scalar).bytes()
}
