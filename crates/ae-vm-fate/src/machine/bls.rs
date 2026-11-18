// aefa_fate_op, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use ae_crypto::{BlsField, bls};

pub(super) fn supported(op: Opcode) -> bool {
    use Opcode::*;
    matches!(
        op,
        Bls12381IntToFr
            | Bls12381IntToFp
            | Bls12381G1Neg
            | Bls12381G1Norm
            | Bls12381G1Valid
            | Bls12381G1IsZero
            | Bls12381G1Add
            | Bls12381G1Mul
            | Bls12381G2Neg
            | Bls12381G2Norm
            | Bls12381G2Valid
            | Bls12381G2IsZero
            | Bls12381G2Add
            | Bls12381G2Mul
            | Bls12381GtInv
            | Bls12381GtIsOne
            | Bls12381GtAdd
            | Bls12381GtMul
            | Bls12381GtPow
            | Bls12381MillerLoop
            | Bls12381Pairing
            | Bls12381FinalExp
    )
}
fn name(op: Opcode) -> &'static str {
    use Opcode::*;
    match op {
        Bls12381IntToFr => "bls12_381_int_to_fr",
        Bls12381IntToFp => "bls12_381_int_to_fp",
        Bls12381G1Neg => "bls12_381_g1_neg",
        Bls12381G1Norm => "bls12_381_g1_norm",
        Bls12381G1Valid => "bls12_381_g1_valid",
        Bls12381G1IsZero => "bls12_381_g1_is_zero",
        Bls12381G1Add => "bls12_381_g1_add",
        Bls12381G1Mul => "bls12_381_g1_mul",
        Bls12381G2Neg => "bls12_381_g2_neg",
        Bls12381G2Norm => "bls12_381_g2_norm",
        Bls12381G2Valid => "bls12_381_g2_valid",
        Bls12381G2IsZero => "bls12_381_g2_is_zero",
        Bls12381G2Add => "bls12_381_g2_add",
        Bls12381G2Mul => "bls12_381_g2_mul",
        Bls12381GtInv => "bls12_381_gt_inv",
        Bls12381GtIsOne => "bls12_381_gt_is_one",
        Bls12381GtAdd => "bls12_381_gt_add",
        Bls12381GtMul => "bls12_381_gt_mul",
        Bls12381GtPow => "bls12_381_gt_pow",
        Bls12381MillerLoop => "bls12_381_miller_loop",
        Bls12381Pairing => "bls12_381_pairing",
        Bls12381FinalExp => "bls12_381_final_exp",
        _ => unreachable!(),
    }
}
fn field<const N: usize>(v: &Value) -> Option<[u8; N]> {
    let Value::Bytes(bytes) = v else {
        return None;
    };
    bytes.as_slice().try_into().ok()
}
fn fp(v: &Value) -> Option<[u8; 48]> {
    field(v)
}
fn tuple<const N: usize>(v: &Value) -> Option<&[Value; N]> {
    let Value::Tuple(values) = v else {
        return None;
    };
    values.as_slice().try_into().ok()
}
fn fp2(v: &Value) -> Option<[[u8; 48]; 2]> {
    let [a, b] = tuple(v)?;
    Some([fp(a)?, fp(b)?])
}
fn g1(v: &Value) -> Option<bls::G1> {
    let [x, y, z] = tuple(v)?;
    Some([fp(x)?, fp(y)?, fp(z)?])
}
fn g2(v: &Value) -> Option<bls::G2> {
    let [x, y, z] = tuple(v)?;
    Some([fp2(x)?, fp2(y)?, fp2(z)?])
}
fn gt(v: &Value) -> Option<bls::Gt> {
    let values = tuple::<12>(v)?;
    let mut out = [[0; 48]; 12];
    for (to, from) in out.iter_mut().zip(values) {
        *to = fp(from)?;
    }
    Some(out)
}
fn value1(p: bls::G1) -> Value {
    field_values(p)
}
fn value2(p: bls::G2) -> Value {
    Value::Tuple(p.into_iter().map(field_values).collect())
}
fn field_values<const N: usize>(p: [[u8; 48]; N]) -> Value {
    Value::Tuple(p.into_iter().map(|b| Value::Bytes(b.to_vec())).collect())
}
impl<S: CachedSubtreeBackend, O: MapOrder> Step<'_, '_, S, O> {
    pub(super) fn bls(&mut self, op: Opcode, a: &[Operand]) -> Result<(), Fault<S::Error>> {
        use Opcode::*;
        let name = name(op);
        if self.executor.context.vm_version < VmVersion::Two {
            return Err(Fault::Abort(Reason::Unavailable(name)));
        }
        let x = self.read(&a[1])?;
        let profile = self.executor.context.bls_profile;
        if matches!(
            op,
            Bls12381G1Add
                | Bls12381G2Add
                | Bls12381GtAdd
                | Bls12381G1Mul
                | Bls12381G2Mul
                | Bls12381GtMul
                | Bls12381GtPow
                | Bls12381MillerLoop
                | Bls12381Pairing
        ) {
            let y = self.read(&a[2])?;
            let result = match op {
                Bls12381MillerLoop | Bls12381Pairing => {
                    let (Some(p), Some(q)) = (g1(&x), g2(&y)) else {
                        return Err(self.type_error(name, vec![x, y]));
                    };
                    field_values(if op == Bls12381MillerLoop {
                        bls::miller_loop(profile, &p, &q)
                    } else {
                        bls::pairing(profile, &p, &q)
                    })
                }
                Bls12381G1Mul => {
                    let (Some(k), Some(p)) = (field(&x), g1(&y)) else {
                        return Err(self.type_error(name, vec![x, y]));
                    };
                    value1(bls::g1_multiply(profile, &k, &p))
                }
                Bls12381G2Mul => {
                    let (Some(k), Some(p)) = (field(&x), g2(&y)) else {
                        return Err(self.type_error(name, vec![x, y]));
                    };
                    value2(bls::g2_multiply(profile, &k, &p))
                }
                Bls12381G1Add => {
                    let (Some(p), Some(q)) = (g1(&x), g1(&y)) else {
                        return Err(self.type_error(name, vec![x, y]));
                    };
                    value1(bls::g1_add(profile, &p, &q))
                }
                Bls12381G2Add => {
                    let (Some(p), Some(q)) = (g2(&x), g2(&y)) else {
                        return Err(self.type_error(name, vec![x, y]));
                    };
                    value2(bls::g2_add(profile, &p, &q))
                }
                Bls12381GtAdd | Bls12381GtMul => {
                    let (Some(p), Some(q)) = (gt(&x), gt(&y)) else {
                        return Err(self.type_error(name, vec![x, y]));
                    };
                    field_values(if op == Bls12381GtAdd {
                        bls::gt_add(profile, &p, &q)
                    } else {
                        bls::gt_multiply(profile, &p, &q)
                    })
                }
                Bls12381GtPow => {
                    let (Some(p), Some(k)) = (gt(&x), field(&y)) else {
                        return Err(self.type_error(name, vec![x, y]));
                    };
                    field_values(bls::gt_power(profile, &p, &k))
                }
                _ => unreachable!(),
            };
            return self.write(&a[0], result);
        }
        if matches!(op, Bls12381GtInv | Bls12381GtIsOne | Bls12381FinalExp) {
            let Some(p) = gt(&x) else {
                return Err(self.type_error(name, vec![x]));
            };
            let result = match op {
                Bls12381GtInv => field_values(bls::gt_unitary_inverse(profile, &p)),
                Bls12381GtIsOne => Value::Boolean(bls::gt_is_one(profile, &p)),
                Bls12381FinalExp => field_values(bls::final_exponentiation(profile, &p)),
                _ => unreachable!(),
            };
            return self.write(&a[0], result);
        }
        if matches!(
            op,
            Bls12381G1Neg | Bls12381G1Norm | Bls12381G1Valid | Bls12381G1IsZero
        ) {
            let Some(p) = g1(&x) else {
                return Err(self.type_error(name, vec![x]));
            };
            let result = match op {
                Bls12381G1Neg => value1(bls::g1_negate(profile, &p)),
                Bls12381G1Norm => value1(bls::g1_normalize(profile, &p)),
                Bls12381G1Valid => Value::Boolean(bls::g1_valid(profile, &p)),
                Bls12381G1IsZero => Value::Boolean(bls::g1_zero(profile, &p)),
                _ => unreachable!(),
            };
            return self.write(&a[0], result);
        }
        if matches!(
            op,
            Bls12381G2Neg | Bls12381G2Norm | Bls12381G2Valid | Bls12381G2IsZero
        ) {
            let Some(p) = g2(&x) else {
                return Err(self.type_error(name, vec![x]));
            };
            let result = match op {
                Bls12381G2Neg => value2(bls::g2_negate(profile, &p)),
                Bls12381G2Norm => value2(bls::g2_normalize(profile, &p)),
                Bls12381G2Valid => Value::Boolean(bls::g2_valid(profile, &p)),
                Bls12381G2IsZero => Value::Boolean(bls::g2_zero(profile, &p)),
                _ => unreachable!(),
            };
            return self.write(&a[0], result);
        }
        let field = if op == Bls12381IntToFr {
            BlsField::Scalar
        } else {
            BlsField::Base
        };
        let Value::Integer(n) = &x else {
            return Err(self.type_error(name, vec![x]));
        };
        if n.sign() == Sign::Minus || n.bits() > field.modulus().bits() {
            return Err(self.type_error(name, vec![x]));
        }
        let n = n.to_biguint().expect("nonnegative integer");
        // MAX_FR is q - 1, whereas MAX_FP is the base-field modulus itself.
        if field == BlsField::Scalar && &n + 1u8 >= *field.modulus() {
            return Err(self.type_error(name, vec![x]));
        }
        let Some(bytes) = field.encode_integer(&n) else {
            return Err(self.type_error(name, vec![x]));
        };
        self.write(&a[0], Value::Bytes(bytes))
    }
}
