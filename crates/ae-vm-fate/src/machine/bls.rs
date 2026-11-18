// aefa_fate_op, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use ae_crypto::BlsField;

pub(super) fn supported(op: Opcode) -> bool {
    matches!(op, Opcode::Bls12381IntToFr | Opcode::Bls12381IntToFp)
}
impl<S: CachedSubtreeBackend, O: MapOrder> Step<'_, '_, S, O> {
    pub(super) fn bls(&mut self, op: Opcode, a: &[Operand]) -> Result<(), Fault<S::Error>> {
        let (field, name) = match op {
            Opcode::Bls12381IntToFr => (BlsField::Scalar, "bls12_381_int_to_fr"),
            Opcode::Bls12381IntToFp => (BlsField::Base, "bls12_381_int_to_fp"),
            _ => unreachable!(),
        };
        if self.executor.context.vm_version < VmVersion::Two {
            return Err(Fault::Abort(Reason::Unavailable(name)));
        }
        let x = self.read(&a[1])?;
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
