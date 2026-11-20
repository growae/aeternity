// aec_pof and aeu_sig, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC notice in repository LICENSE.
use ae_codec::{
    headers::{Header, HeaderError, HeaderMode},
    object::{self, Field, FieldType, ObjectTag, Value},
    rlp::{DecodeLimits, Rlp},
};
use ae_types::{AccountPublicKey, Hash256, Signature};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proof {
    pub first: Header,
    pub second: Header,
    // The original binary field is unconstrained until signature verification.
    pub public_key: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Object(object::ObjectError),
    Header(HeaderError),
    NotSiblings,
    SameHeader,
    Signature,
    BadPublicKeyLength,
}
impl From<object::ObjectError> for Error {
    fn from(e: object::ObjectError) -> Self {
        Self::Object(e)
    }
}
impl From<HeaderError> for Error {
    fn from(e: HeaderError) -> Self {
        Self::Header(e)
    }
}
fn schema() -> Vec<Field> {
    ["header1", "header2", "pubkey"]
        .into_iter()
        .map(|name| Field {
            name: name.into(),
            kind: FieldType::Binary,
        })
        .collect()
}
impl Proof {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        Ok(object::serialize(
            ObjectTag::Pof,
            &1.into(),
            &schema(),
            &[
                ("header1".into(), Value::Binary(self.first.encode()?)),
                ("header2".into(), Value::Binary(self.second.encode()?)),
                ("pubkey".into(), Value::Binary(self.public_key.clone())),
            ],
        )?)
    }
    pub fn decode(bytes: &[u8], mode: HeaderMode, limits: DecodeLimits) -> Result<Self, Error> {
        let e = object::decode_envelope_with_limits(bytes, limits)?;
        if e.tag != ObjectTag::Pof {
            return Err(object::ObjectError::WrongTag.into());
        }
        if e.version != 1.into() {
            return Err(object::ObjectError::WrongVersion.into());
        }
        let [Rlp::Bytes(first), Rlp::Bytes(second), Rlp::Bytes(key)] = e.fields.as_slice() else {
            return Err(if e.fields.len() == 3 {
                object::ObjectError::WrongType
            } else {
                object::ObjectError::FieldCount
            }
            .into());
        };
        Ok(Self {
            first: Header::decode(first, mode)?,
            second: Header::decode(second, mode)?,
            public_key: key.to_vec(),
        })
    }
    pub fn hash(&self) -> Result<Hash256, Error> {
        Ok(ae_crypto::blake2b_256(&self.encode()?))
    }
    pub fn validate(&self) -> Result<(), Error> {
        let (Header::Micro(first), Header::Micro(second)) = (&self.first, &self.second) else {
            return Err(Error::NotSiblings);
        };
        if first.common.previous_hash != second.common.previous_hash {
            return Err(Error::NotSiblings);
        }
        if self.first.hash()? == self.second.hash()? {
            return Err(Error::SameHeader);
        }
        let key = AccountPublicKey(
            self.public_key
                .as_slice()
                .try_into()
                .map_err(|_| Error::BadPublicKeyLength)?,
        );
        // The proof compares siblings and signatures, not their heights,
        // versions, times, state roots, or admissibility as full blocks.
        let first_ok = ae_crypto::verify_ed25519(
            &key,
            &self.first.signature_bytes()?,
            &Signature(first.signature),
        );
        let second_ok = ae_crypto::verify_ed25519(
            &key,
            &self.second.signature_bytes()?,
            &Signature(second.signature),
        );
        if first_ok && second_ok {
            Ok(())
        } else {
            Err(Error::Signature)
        }
    }
}
