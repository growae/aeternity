// aec_peer_connection light micro serialization, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use ae_types::Unsigned;

// Transport data only. The native envelope version is independent of the
// embedded header, and transaction hash fields are arbitrary binaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Light {
    pub version: Unsigned,
    pub header: Header,
    pub hashes: Vec<Vec<u8>>,
    pub proof: Option<Box<fraud::Proof>>,
}
impl Light {
    pub fn from_micro(block: &MicroBlock) -> Self {
        Self {
            version: u64::from(block.header.common.version).into(),
            header: Header::Micro(block.header.clone()),
            hashes: block
                .transactions
                .iter()
                .map(|t| t.hash().0.to_vec())
                .collect(),
            proof: block.proof.clone(),
        }
    }
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let fields = [
            Field {
                name: "header".into(),
                kind: FieldType::Binary,
            },
            Field {
                name: "tx_hashes".into(),
                kind: FieldType::List(Box::new(FieldType::Binary)),
            },
            Field {
                name: "pof".into(),
                kind: FieldType::List(Box::new(FieldType::Binary)),
            },
        ];
        Ok(object::serialize(
            ObjectTag::LightMicroBlock,
            &self.version,
            &fields,
            &[
                ("header".into(), Value::Binary(self.header.encode()?)),
                (
                    "tx_hashes".into(),
                    Value::List(self.hashes.iter().cloned().map(Value::Binary).collect()),
                ),
                (
                    "pof".into(),
                    Value::List(
                        self.proof
                            .as_ref()
                            .map(|p| p.encode().map(Value::Binary))
                            .transpose()?
                            .into_iter()
                            .collect(),
                    ),
                ),
            ],
        )?)
    }
    pub fn decode(bytes: &[u8], config: DecodeConfig<'_>) -> Result<Self, Error> {
        if bytes.len() > config.max_block_bytes {
            return Err(Error::ResourceLimit);
        }
        let e = object::decode_envelope_with_limits(bytes, config.body)?;
        if e.tag != ObjectTag::LightMicroBlock {
            return Err(object::ObjectError::WrongTag.into());
        }
        let [Rlp::Bytes(header), Rlp::List(hashes), Rlp::List(proofs)] = e.fields.as_slice() else {
            return Err(if e.fields.len() == 3 {
                object::ObjectError::WrongType
            } else {
                object::ObjectError::FieldCount
            }
            .into());
        };
        if hashes.len() > config.max_transactions {
            return Err(Error::ResourceLimit);
        }
        if hashes
            .iter()
            .chain(proofs)
            .any(|v| !matches!(v, Rlp::Bytes(_)))
        {
            return Err(object::ObjectError::WrongType.into());
        }
        let header = Header::decode(header, config.mode)?;
        let proof = match proofs.as_slice() {
            [] => None,
            [Rlp::Bytes(bytes)] => Some(Box::new(fraud::Proof::decode(
                bytes,
                config.mode,
                config.body,
            )?)),
            _ => return Err(Error::ProofShape),
        };
        Ok(Self {
            version: e.version,
            header,
            hashes: hashes
                .iter()
                .map(|v| {
                    let Rlp::Bytes(b) = v else { unreachable!() };
                    b.to_vec()
                })
                .collect(),
            proof,
        })
    }
}
