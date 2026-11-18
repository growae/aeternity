// Object layouts and resolution: aeternity fa2a6d7, aens_names/aens_pointer/aens.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use ae_codec::object::{self, Field, FieldType, ObjectTag, Value};
use ae_trie::{Nibbles, NodeReader, Trie};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Object(object::ObjectError),
    Tree(ae_trie::Error),
    InvalidValue,
    InvalidPointer,
    NameNotFound,
    NameRevoked,
    PointerNotFound,
    PointerNotId,
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
pub enum PointerValue {
    Id(Id),
    Data(Vec<u8>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pointer {
    pub key: Vec<u8>,
    pub value: PointerValue,
}
impl Pointer {
    fn validate(&self) -> Result<(), Error> {
        match &self.value {
            PointerValue::Id(id)
                if matches!(
                    id.tag,
                    IdTag::Account | IdTag::Oracle | IdTag::Contract | IdTag::Channel
                ) =>
            {
                Ok(())
            }
            PointerValue::Data(bytes) if bytes.len() <= 1024 => Ok(()),
            _ => Err(Error::InvalidPointer),
        }
    }
    fn encode(&self, version: u64) -> Result<Value, Error> {
        self.validate()?;
        let value = match (&self.value, version) {
            (PointerValue::Id(id), 1) => Value::Id(*id),
            (PointerValue::Id(id), 2) => {
                let mut bytes = vec![1];
                bytes.extend_from_slice(&id.to_bytes());
                Value::Binary(bytes)
            }
            (PointerValue::Data(data), 2) => {
                let mut bytes = vec![2];
                bytes.extend_from_slice(data);
                Value::Binary(bytes)
            }
            _ => return Err(Error::InvalidPointer),
        };
        Ok(Value::Tuple(vec![Value::Binary(self.key.clone()), value]))
    }
    fn decode(value: Value, version: u64) -> Result<Self, Error> {
        let Value::Tuple(mut fields) = value else {
            return Err(Error::InvalidPointer);
        };
        let data = fields.pop().ok_or(Error::InvalidPointer)?;
        let Some(Value::Binary(key)) = fields.pop() else {
            return Err(Error::InvalidPointer);
        };
        let value = match (data, version) {
            (Value::Id(id), 1) => PointerValue::Id(id),
            (Value::Binary(bytes), 2) => match bytes.split_first() {
                Some((1, id)) => {
                    PointerValue::Id(Id::from_bytes(id).map_err(|_| Error::InvalidPointer)?)
                }
                Some((2, data)) => PointerValue::Data(data.to_vec()),
                _ => return Err(Error::InvalidPointer),
            },
            _ => return Err(Error::InvalidPointer),
        };
        let pointer = Self { key, value };
        pointer.validate()?;
        Ok(pointer)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Name {
    pub hash: [u8; 32],
    pub owner: Id,
    pub expires_by: Unsigned,
    pub status: Vec<u8>,
    pub client_ttl: Unsigned,
    pub pointers: Vec<Pointer>,
}
impl Name {
    pub fn new(hash: [u8; 32], owner: AccountPublicKey, expires_by: Unsigned) -> Self {
        Self {
            hash,
            owner: Id::new(IdTag::Account, owner.0),
            expires_by,
            status: b"claimed".to_vec(),
            client_ttl: 0.into(),
            pointers: vec![],
        }
    }
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        let version = if self
            .pointers
            .iter()
            .any(|p| matches!(p.value, PointerValue::Data(_)))
        {
            2
        } else {
            1
        };
        let fields = vec![
            ("owner_id".into(), Value::Id(self.owner)),
            ("expires_by".into(), Value::Integer(self.expires_by.clone())),
            ("status".into(), Value::Binary(self.status.clone())),
            ("client_ttl".into(), Value::Integer(self.client_ttl.clone())),
            (
                "pointers".into(),
                Value::List(
                    self.pointers
                        .iter()
                        .map(|p| p.encode(version))
                        .collect::<Result<_, _>>()?,
                ),
            ),
        ];
        Ok(object::serialize(
            ObjectTag::Name,
            &version.into(),
            &schema(version)?,
            &fields,
        )?)
    }
    pub fn deserialize(hash: [u8; 32], bytes: &[u8]) -> Result<Self, Error> {
        let version = object::decode_envelope(bytes)?
            .version
            .to_u64()
            .map_err(|_| Error::InvalidValue)?;
        let fields =
            object::deserialize(bytes, ObjectTag::Name, &version.into(), &schema(version)?)?;
        let [
            (_, Value::Id(owner)),
            (_, Value::Integer(expires_by)),
            (_, Value::Binary(status)),
            (_, Value::Integer(client_ttl)),
            (_, Value::List(pointers)),
        ] = fields.as_slice()
        else {
            return Err(Error::InvalidValue);
        };
        Ok(Self {
            hash,
            owner: *owner,
            expires_by: expires_by.clone(),
            status: status.clone(),
            client_ttl: client_ttl.clone(),
            pointers: pointers
                .iter()
                .cloned()
                .map(|p| Pointer::decode(p, version))
                .collect::<Result<_, _>>()?,
        })
    }
    pub fn resolve(&self, key: &[u8]) -> Result<&PointerValue, Error> {
        match self.status.as_slice() {
            b"revoked" => return Err(Error::NameRevoked),
            b"claimed" => {}
            _ => return Err(Error::InvalidValue),
        }
        if self.owner.tag != IdTag::Account {
            return Err(Error::InvalidValue);
        }
        // Expiration is handled by pre-block pruning, not by this lookup. The
        // first matching pointer wins; historical duplicate keys are retained.
        self.pointers
            .iter()
            .find(|p| p.key == key)
            .map(|p| &p.value)
            .ok_or(Error::PointerNotFound)
    }
}
fn schema(version: u64) -> Result<Vec<Field>, Error> {
    let value = match version {
        1 => FieldType::Id,
        2 => FieldType::Binary,
        _ => return Err(Error::InvalidValue),
    };
    Ok(vec![
        Field {
            name: "owner_id".into(),
            kind: FieldType::Id,
        },
        Field {
            name: "expires_by".into(),
            kind: FieldType::Integer,
        },
        Field {
            name: "status".into(),
            kind: FieldType::Binary,
        },
        Field {
            name: "client_ttl".into(),
            kind: FieldType::Integer,
        },
        Field {
            name: "pointers".into(),
            kind: FieldType::List(Box::new(FieldType::Tuple(vec![FieldType::Binary, value]))),
        },
    ])
}

pub trait NameResolver {
    fn resolve_account(&self, name_hash: [u8; 32]) -> Result<AccountPublicKey, Error>;
}
// Read directly from the names tree. Expiry caches are not needed for lookup;
// writes and pre-block expiry processing are separate state operations.
impl<S: NodeReader> NameResolver for Trie<S> {
    fn resolve_account(&self, hash: [u8; 32]) -> Result<AccountPublicKey, Error> {
        let Some(ae_trie::Value::Bytes(bytes)) = self.get(&Nibbles::from_bytes(&hash))? else {
            return Err(Error::NameNotFound);
        };
        let name = Name::deserialize(hash, &bytes)?;
        match name.resolve(b"account_pubkey")? {
            // The legacy resolver accepts any admitted ID tag here.
            PointerValue::Id(id) => Ok(AccountPublicKey(id.value)),
            PointerValue::Data(_) => Err(Error::PointerNotId),
        }
    }
}
