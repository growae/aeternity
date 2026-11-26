// Object layouts and resolution: aeternity fa2a6d7, aens_names/aens_pointer/aens.
// Copyright (c) 2017, aeternity developers. ISC notice in repository LICENSE.
use ae_codec::object::{self, Field, FieldType, ObjectTag, Value};
use ae_trie::{Nibbles, NodeReader, Trie};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};

mod cache;
pub mod hashing;
mod objects;
mod tree;
pub use objects::{Auction, Commitment};
pub use tree::{LockedFee, Names};

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
    AlreadyPresent,
    InvalidCache,
    IllegalSalt,
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

pub use ae_codec::name_pointer::{Pointer, PointerValue};
impl From<ae_codec::name_pointer::PointerError> for Error {
    fn from(_: ae_codec::name_pointer::PointerError) -> Self {
        Self::InvalidPointer
    }
}

pub fn sanitize_pointers(pointers: &[Pointer]) -> Vec<Pointer> {
    let mut seen = std::collections::BTreeSet::new();
    pointers
        .iter()
        .filter(|p| p.key.len() <= ae_protocol::names::POINTER_MAX_KEY_SIZE && seen.insert(&p.key))
        .take(ae_protocol::names::POINTERS_MAX_COUNT)
        .cloned()
        .collect()
}

pub fn pointers_valid_at(pointers: &[Pointer], protocol: ae_protocol::Protocol) -> bool {
    pointers.iter().all(|p| p.validate().is_ok())
        && (protocol >= ae_protocol::Protocol::Ceres
            || !pointers
                .iter()
                .any(|p| matches!(p.value, PointerValue::Data(_))))
        && (protocol < ae_protocol::Protocol::Iris || sanitize_pointers(pointers) == pointers)
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
pub trait NamePointerResolver {
    fn resolve_id(&self, name_hash: [u8; 32], pointer: &[u8]) -> Result<Id, Error>;
}
// Read directly from the names tree. Expiry caches are not needed for lookup;
// writes and pre-block expiry processing are separate state operations.
impl<S: NodeReader> NameResolver for Trie<S> {
    fn resolve_account(&self, hash: [u8; 32]) -> Result<AccountPublicKey, Error> {
        Ok(AccountPublicKey(
            self.resolve_id(hash, b"account_pubkey")?.value,
        ))
    }
}
impl<S: NodeReader> NamePointerResolver for Trie<S> {
    fn resolve_id(&self, hash: [u8; 32], pointer: &[u8]) -> Result<Id, Error> {
        let Some(ae_trie::Value::Bytes(bytes)) = self.get(&Nibbles::from_bytes(&hash))? else {
            return Err(Error::NameNotFound);
        };
        let name = Name::deserialize(hash, &bytes)?;
        match name.resolve(pointer)? {
            // The legacy resolver accepts any admitted ID tag here.
            PointerValue::Id(id) => Ok(*id),
            PointerValue::Data(_) => Err(Error::PointerNotId),
        }
    }
}
