// Aeternity fa2a6d7, aeo_oracles, aeo_query and aeo_utils.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use ae_codec::object::{self, Field, FieldType, ObjectTag, Value};
use ae_crypto::blake2b_256;
use ae_types::{AccountPublicKey, Hash256, Id, IdTag, Unsigned};

mod tree;
pub use tree::{Oracles, QueryFilter, Refund};
mod cache;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Object(object::ObjectError),
    Tree(ae_trie::Error),
    InvalidValue,
    InvalidCache,
    AlreadyPresent,
    InvalidIdType,
    ZeroResponseTtl,
    TooLowHeight,
    SkippedExpiryHeight,
}
impl From<object::ObjectError> for Error {
    fn from(e: object::ObjectError) -> Self {
        Self::Object(e)
    }
}
impl From<ae_trie::Error> for Error {
    fn from(e: ae_trie::Error) -> Self {
        Self::Tree(e)
    }
}
impl From<crate::sext::Error> for Error {
    fn from(_: crate::sext::Error) -> Self {
        Self::InvalidCache
    }
}
pub use ae_protocol::oracles::Ttl;
impl From<ae_protocol::oracles::TtlError> for Error {
    fn from(_: ae_protocol::oracles::TtlError) -> Self {
        Self::TooLowHeight
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Oracle {
    pub public_key: [u8; 32],
    pub query_format: Vec<u8>,
    pub response_format: Vec<u8>,
    pub query_fee: Unsigned,
    pub expires_by: Unsigned,
    pub abi_version: Unsigned,
}
impl Oracle {
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        let fields = vec![
            (
                "query_format".into(),
                Value::Binary(self.query_format.clone()),
            ),
            (
                "response_format".into(),
                Value::Binary(self.response_format.clone()),
            ),
            ("query_fee".into(), Value::Integer(self.query_fee.clone())),
            ("ttl".into(), Value::Integer(self.expires_by.clone())),
            (
                "abi_version".into(),
                Value::Integer(self.abi_version.clone()),
            ),
        ];
        Ok(object::serialize(
            ObjectTag::Oracle,
            &1.into(),
            &oracle_schema(),
            &fields,
        )?)
    }
    pub fn deserialize(public_key: [u8; 32], bytes: &[u8]) -> Result<Self, Error> {
        let fields = object::deserialize(bytes, ObjectTag::Oracle, &1.into(), &oracle_schema())?;
        let [
            (_, Value::Binary(query_format)),
            (_, Value::Binary(response_format)),
            (_, Value::Integer(query_fee)),
            (_, Value::Integer(expires_by)),
            (_, Value::Integer(abi_version)),
        ] = fields.as_slice()
        else {
            return Err(Error::InvalidValue);
        };
        Ok(Self {
            public_key,
            query_format: query_format.clone(),
            response_format: response_format.clone(),
            query_fee: query_fee.clone(),
            expires_by: expires_by.clone(),
            abi_version: abi_version.clone(),
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    pub id: [u8; 32],
    pub sender: Id,
    pub sender_nonce: Unsigned,
    pub oracle: Id,
    pub query: Vec<u8>,
    pub response: Option<Vec<u8>>,
    pub expires_by: Unsigned,
    pub response_ttl: Unsigned,
    pub fee: Unsigned,
}
impl Query {
    pub fn new(
        oracle: Id,
        sender: AccountPublicKey,
        nonce: Unsigned,
        query: Vec<u8>,
        fee: Unsigned,
        expires_by: Unsigned,
        response_ttl: Unsigned,
    ) -> Result<Self, Error> {
        if !matches!(oracle.tag, IdTag::Oracle | IdTag::Name) {
            return Err(Error::InvalidIdType);
        }
        if response_ttl == 0.into() {
            return Err(Error::ZeroResponseTtl);
        }
        Ok(Self {
            id: query_id(sender, &nonce, oracle.value).0,
            sender: Id::new(IdTag::Account, sender.0),
            sender_nonce: nonce,
            oracle,
            query,
            response: None,
            expires_by,
            response_ttl,
            fee,
        })
    }
    pub fn is_open(&self) -> bool {
        self.response.is_none()
    }
    pub fn add_response(&mut self, height: &Unsigned, response: Option<Vec<u8>>) {
        self.response = response;
        self.expires_by = height.add(&self.response_ttl);
    }
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        let fields = vec![
            ("sender_id".into(), Value::Id(self.sender)),
            (
                "sender_nonce".into(),
                Value::Integer(self.sender_nonce.clone()),
            ),
            ("oracle_id".into(), Value::Id(self.oracle)),
            ("query".into(), Value::Binary(self.query.clone())),
            ("has_response".into(), Value::Boolean(!self.is_open())),
            (
                "response".into(),
                Value::Binary(self.response.clone().unwrap_or_default()),
            ),
            ("ttl".into(), Value::Integer(self.expires_by.clone())),
            (
                "response_ttl".into(),
                Value::Integer(self.response_ttl.clone()),
            ),
            ("fee".into(), Value::Integer(self.fee.clone())),
        ];
        Ok(object::serialize(
            ObjectTag::OracleQuery,
            &1.into(),
            &query_schema(),
            &fields,
        )?)
    }
    pub fn deserialize(id: [u8; 32], bytes: &[u8]) -> Result<Self, Error> {
        let fields =
            object::deserialize(bytes, ObjectTag::OracleQuery, &1.into(), &query_schema())?;
        let [
            (_, Value::Id(sender)),
            (_, Value::Integer(nonce)),
            (_, Value::Id(oracle)),
            (_, Value::Binary(query)),
            (_, Value::Boolean(has_response)),
            (_, Value::Binary(response)),
            (_, Value::Integer(expires_by)),
            (_, Value::Integer(response_ttl)),
            (_, Value::Integer(fee)),
        ] = fields.as_slice()
        else {
            return Err(Error::InvalidValue);
        };
        // Original object decoding preserves historical fields without invoking
        // constructor validation, and discards response bytes when the flag is false.
        Ok(Self {
            id,
            sender: *sender,
            sender_nonce: nonce.clone(),
            oracle: *oracle,
            query: query.clone(),
            response: has_response.then(|| response.clone()),
            expires_by: expires_by.clone(),
            response_ttl: response_ttl.clone(),
            fee: fee.clone(),
        })
    }
}
pub fn query_id(
    sender: AccountPublicKey,
    nonce: &Unsigned,
    oracle_identifier: [u8; 32],
) -> Hash256 {
    let mut bytes = [0u8; 96];
    bytes[..32].copy_from_slice(&sender.0);
    let raw = nonce.as_bytes();
    let size = raw.len().min(32);
    // Erlang bit syntax retains only the low 256 nonce bits.
    bytes[64 - size..64].copy_from_slice(&raw[raw.len() - size..]);
    bytes[64..].copy_from_slice(&oracle_identifier);
    blake2b_256(&bytes)
}
pub fn ga_query_id(ga_nonce: [u8; 32], oracle: [u8; 32]) -> Hash256 {
    blake2b_256(&[ga_nonce.as_slice(), &oracle].concat())
}
fn schema(fields: &[(&str, FieldType)]) -> Vec<Field> {
    fields
        .iter()
        .map(|(name, kind)| Field {
            name: (*name).into(),
            kind: kind.clone(),
        })
        .collect()
}
fn oracle_schema() -> Vec<Field> {
    schema(&[
        ("query_format", FieldType::Binary),
        ("response_format", FieldType::Binary),
        ("query_fee", FieldType::Integer),
        ("ttl", FieldType::Integer),
        ("abi_version", FieldType::Integer),
    ])
}
fn query_schema() -> Vec<Field> {
    schema(&[
        ("sender_id", FieldType::Id),
        ("sender_nonce", FieldType::Integer),
        ("oracle_id", FieldType::Id),
        ("query", FieldType::Binary),
        ("has_response", FieldType::Boolean),
        ("response", FieldType::Binary),
        ("ttl", FieldType::Integer),
        ("response_ttl", FieldType::Integer),
        ("fee", FieldType::Integer),
    ])
}
