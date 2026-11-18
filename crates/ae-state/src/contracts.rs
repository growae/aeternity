// Contract object records: aect_contracts.erl, fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use ae_codec::object::{self, Field, FieldType, ObjectTag, Value};
use ae_crypto::blake2b_256;
use ae_protocol::contracts::Version;
use ae_types::{AccountPublicKey, Hash256, Id, IdTag, Unsigned};
use std::collections::BTreeMap;

mod proof;
pub mod store;
mod tree;
pub use proof::ContractProof;
pub use tree::{ContractWithCode, Contracts, LoadOptions, LoadedContract};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Object(object::ObjectError),
    Tree(ae_trie::Error),
    InvalidValue,
    InvalidVersion,
    InvalidIdType,
    InvalidCodeReference,
    InvalidStore,
    AlreadyPresent,
    NotPresent,
    NoSuchStore,
    WrongRootHash,
    BadProof,
    ResourceLimit,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Code {
    Inline(Vec<u8>),
    Reference(Id),
}
pub enum Nonce<'a> {
    Number(&'a Unsigned),
    Hash([u8; 32]),
}
pub fn contract_id(owner: AccountPublicKey, nonce: Nonce<'_>) -> Hash256 {
    let mut bytes = owner.0.to_vec();
    match nonce {
        Nonce::Number(n) => bytes.extend_from_slice(n.as_bytes()),
        Nonce::Hash(h) => bytes.extend_from_slice(&h),
    }
    blake2b_256(&bytes)
}
pub fn store_id(contract: [u8; 32]) -> [u8; 33] {
    let mut id = [0; 33];
    id[..32].copy_from_slice(&contract);
    id[32] = 16;
    id
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contract {
    pub public_key: [u8; 32],
    pub owner: AccountPublicKey,
    pub version: Version,
    pub code: Code,
    pub log: Vec<u8>,
    pub active: bool,
    pub referrers: Vec<[u8; 32]>,
    pub deposit: Unsigned,
    pub object_version: u8,
}
impl Contract {
    pub fn new(
        owner: AccountPublicKey,
        nonce: Nonce<'_>,
        version: Version,
        code: Code,
        deposit: Unsigned,
    ) -> Result<Self, Error> {
        if !version.legal() {
            return Err(Error::InvalidVersion);
        }
        if let Code::Reference(id) = &code
            && id.tag != IdTag::Contract
        {
            return Err(Error::InvalidIdType);
        }
        Ok(Self {
            public_key: contract_id(owner, nonce).0,
            owner,
            version,
            code,
            log: vec![],
            active: true,
            referrers: vec![],
            deposit,
            object_version: version.object_version(),
        })
    }
    pub fn validate_writes(&self, writes: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<(), Error> {
        if writes.keys().any(|key| !self.version.valid_store_key(key)) {
            return Err(Error::InvalidStore);
        }
        Ok(())
    }
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        let (bytes, refs) = match &self.code {
            Code::Inline(bytes) => (bytes.clone(), vec![]),
            Code::Reference(id) if self.object_version == 2 => (vec![], vec![Value::Id(*id)]),
            _ => return Err(Error::InvalidCodeReference),
        };
        let mut values = vec![
            Value::Id(Id::new(IdTag::Account, self.owner.0)),
            Value::Integer(self.version.pack()),
            Value::Binary(bytes),
        ];
        if self.object_version == 2 {
            values.push(Value::List(refs));
        }
        values.extend([
            Value::Binary(self.log.clone()),
            Value::Boolean(self.active),
            Value::List(
                self.referrers
                    .iter()
                    .map(|id| Value::Id(Id::new(IdTag::Contract, *id)))
                    .collect(),
            ),
            Value::Integer(self.deposit.clone()),
        ]);
        let schema = schema(self.object_version)?;
        let fields = schema
            .iter()
            .zip(values)
            .map(|(f, v)| (f.name.clone(), v))
            .collect::<Vec<_>>();
        Ok(object::serialize(
            ObjectTag::Contract,
            &u64::from(self.object_version).into(),
            &schema,
            &fields,
        )?)
    }
    pub fn deserialize(public_key: [u8; 32], bytes: &[u8]) -> Result<Self, Error> {
        let env = object::decode_envelope(bytes)?;
        let version = u8::try_from(env.version.to_u64().map_err(|_| Error::InvalidVersion)?)
            .map_err(|_| Error::InvalidVersion)?;
        let fields =
            object::deserialize(bytes, ObjectTag::Contract, &env.version, &schema(version)?)?;
        let mut values = fields.into_iter().map(|(_, v)| v);
        let Some(Value::Id(owner)) = values.next() else {
            return Err(Error::InvalidValue);
        };
        if owner.tag != IdTag::Account {
            return Err(Error::InvalidIdType);
        }
        let Some(Value::Integer(packed)) = values.next() else {
            return Err(Error::InvalidValue);
        };
        let Some(Value::Binary(raw_code)) = values.next() else {
            return Err(Error::InvalidValue);
        };
        let code = if version == 2 {
            let Some(Value::List(refs)) = values.next() else {
                return Err(Error::InvalidValue);
            };
            match refs.as_slice() {
                [] => Code::Inline(raw_code),
                [Value::Id(id)] if raw_code.is_empty() => Code::Reference(*id),
                _ => return Err(Error::InvalidCodeReference),
            }
        } else {
            Code::Inline(raw_code)
        };
        let Some(Value::Binary(log)) = values.next() else {
            return Err(Error::InvalidValue);
        };
        let Some(Value::Boolean(active)) = values.next() else {
            return Err(Error::InvalidValue);
        };
        let Some(Value::List(refs)) = values.next() else {
            return Err(Error::InvalidValue);
        };
        let referrers = refs
            .into_iter()
            .map(|v| match v {
                Value::Id(id) if id.tag == IdTag::Contract => Ok(id.value),
                _ => Err(Error::InvalidIdType),
            })
            .collect::<Result<_, _>>()?;
        let Some(Value::Integer(deposit)) = values.next() else {
            return Err(Error::InvalidValue);
        };
        Ok(Self {
            public_key,
            owner: AccountPublicKey(owner.value),
            version: Version::unpack(&packed),
            code,
            log,
            active,
            referrers,
            deposit,
            object_version: version,
        })
    }
}
fn schema(version: u8) -> Result<Vec<Field>, Error> {
    if !matches!(version, 1 | 2) {
        return Err(Error::InvalidVersion);
    }
    let mut fields = vec![
        ("owner_id", FieldType::Id),
        ("ct_version", FieldType::Integer),
        ("code", FieldType::Binary),
    ];
    if version == 2 {
        fields.push(("code_ref", FieldType::List(Box::new(FieldType::Id))));
    }
    fields.extend([
        ("log", FieldType::Binary),
        ("active", FieldType::Boolean),
        ("referrer_ids", FieldType::List(Box::new(FieldType::Id))),
        ("deposit", FieldType::Integer),
    ]);
    Ok(fields
        .into_iter()
        .map(|(name, kind)| Field {
            name: name.into(),
            kind,
        })
        .collect())
}
