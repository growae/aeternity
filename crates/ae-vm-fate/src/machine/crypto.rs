// aefa_fate_op and aeu_crypto, pinned fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use crate::{
    encoding,
    gas::CostModel,
    value::{ObjectKind, ObjectPayload},
};

pub(super) fn supported(op: Opcode) -> bool {
    use Opcode::*;
    matches!(
        op,
        Sha3 | Sha256
            | Blake2B
            | Poseidon
            | VerifySig
            | VerifySigSecp256K1
            | EcverifySecp256K1
            | EcrecoverSecp256K1
    )
}
fn name(op: Opcode, vm: VmVersion) -> &'static str {
    match op {
        Opcode::VerifySig if vm < VmVersion::Three => "verify_sig_of_hash",
        Opcode::VerifySig => "verify_sig",
        Opcode::VerifySigSecp256K1 => "verify_sig_secp256k1",
        Opcode::EcverifySecp256K1 => "ecverify_secp256k1",
        Opcode::EcrecoverSecp256K1 => "ecrecover_secp256k1",
        _ => unreachable!(),
    }
}
fn bytes<const N: usize>(v: &Value) -> Option<&[u8; N]> {
    let Value::Bytes(b) = v else {
        return None;
    };
    b.as_slice().try_into().ok()
}
impl<S: CachedSubtreeBackend, O: MapOrder> Step<'_, '_, S, O> {
    pub(super) fn crypto(&mut self, op: Opcode, a: &[Operand]) -> Result<(), Fault<S::Error>> {
        use Opcode::*;
        if op == Poseidon && self.context.vm_version < VmVersion::Three {
            return Err(Fault::Abort(Reason::Unavailable("poseidon")));
        }
        let x = self.read(&a[1])?;
        if op == Poseidon {
            let y = self.read(&a[2])?;
            // Original MAX_FR is q - 1, and the guard uses strict inequality.
            // Keep this admission rule even though q - 1 is a field element.
            static LIMIT: std::sync::LazyLock<BigInt> = std::sync::LazyLock::new(|| {
                BigInt::from_bytes_be(
                    Sign::Plus,
                    &[
                        0x73, 0xed, 0xa7, 0x53, 0x29, 0x9d, 0x7d, 0x48, 0x33, 0x39, 0xd8, 0x08,
                        0x09, 0xa1, 0xd8, 0x05, 0x53, 0xbd, 0xa4, 0x02, 0xff, 0xfe, 0x5b, 0xfe,
                        0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00,
                    ],
                )
            });
            let scalar = |v: &Value| -> Option<[u8; 32]> {
                let Value::Integer(n) = v else {
                    return None;
                };
                if n.sign() == Sign::Minus || n >= &*LIMIT {
                    return None;
                }
                let (_, bytes) = n.to_bytes_be();
                let mut result = [0; 32];
                result[32 - bytes.len()..].copy_from_slice(&bytes);
                Some(result)
            };
            let (Some(left), Some(right)) = (scalar(&x), scalar(&y)) else {
                return Err(self.type_error("poseidon", vec![x, y]));
            };
            let hash = ae_crypto::poseidon_bls12_381(&left, &right)
                .expect("FATE admits only canonical scalar-field inputs");
            return self.write(
                &a[0],
                Value::Integer(BigInt::from_bytes_be(Sign::Plus, &hash)),
            );
        }
        if matches!(op, Sha3 | Sha256 | Blake2B) {
            let value = self.unfold(x, CostModel::UnfoldSerial)?;
            let input = match value {
                Value::String(b) | Value::Bytes(b) => b,
                other => {
                    let (b, nodes) =
                        encoding::encode_vm_counted(&other, self.remaining).map_err(|e| {
                            if e.error == DataError::ResourceLimit {
                                Fault::from(e.error)
                            } else {
                                self.native(e.native_exception)
                            }
                        })?;
                    self.remaining.max_nodes -= nodes;
                    self.remaining.max_bytes -= b.len();
                    b
                }
            };
            let hash = match op {
                Sha3 => ae_crypto::keccak256(&input),
                Sha256 => ae_crypto::sha256(&input),
                Blake2B => ae_crypto::blake2b_256(&input),
                _ => unreachable!(),
            };
            return self.write(&a[0], Value::Bytes(hash.0.to_vec()));
        }
        let y = self.read(&a[2])?;
        let operation = name(op, self.context.vm_version);
        if op == EcrecoverSecp256K1 {
            let (Some(digest), Some(signature)) = (bytes(&x), bytes(&y)) else {
                return Err(self.type_error(operation, vec![x, y]));
            };
            let recovered = ae_crypto::recover_secp256k1_address(digest, signature);
            return self.write(
                &a[0],
                Value::Variant {
                    arities: vec![0.into(), 1.into()],
                    tag: usize::from(recovered.is_some()),
                    values: recovered
                        .map(|a| Value::Bytes(a.to_vec()))
                        .into_iter()
                        .collect(),
                },
            );
        }
        let z = self.read(&a[3])?;
        let valid = match op {
            VerifySig => {
                let (
                    Value::Bytes(message),
                    Value::Object(ObjectKind::Address, ObjectPayload::Bytes(key)),
                    Some(signature),
                ) = (&x, &y, bytes(&z))
                else {
                    return Err(self.type_error(operation, vec![x, y, z]));
                };
                if self.context.vm_version < VmVersion::Three && message.len() != 32 {
                    return Err(self.type_error(operation, vec![x, y, z]));
                }
                let key = key
                    .as_slice()
                    .try_into()
                    .map_err(|_| self.native("badarg"))?;
                ae_crypto::verify_ed25519(
                    &ae_types::AccountPublicKey(key),
                    message,
                    &ae_types::Signature(*signature),
                )
            }
            VerifySigSecp256K1 => {
                let (Some(digest), Some(key), Some(signature)) = (bytes(&x), bytes(&y), bytes(&z))
                else {
                    return Err(self.type_error(operation, vec![x, y, z]));
                };
                ae_crypto::verify_secp256k1(key, digest, signature)
                    .map_err(|_| self.native("badarg"))?
            }
            EcverifySecp256K1 => {
                let (Some(digest), Some(key), Some(signature)) =
                    (bytes(&x), bytes::<20>(&y), bytes(&z))
                else {
                    return Err(self.type_error(operation, vec![x, y, z]));
                };
                ae_crypto::recover_secp256k1_address(digest, signature).as_ref() == Some(key)
            }
            _ => unreachable!(),
        };
        self.write(&a[0], Value::Boolean(valid))
    }
}
