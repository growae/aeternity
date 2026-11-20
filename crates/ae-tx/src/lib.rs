#![forbid(unsafe_code)]

use ae_codec::object::{self, ObjectError, Value};
use ae_types::TypeError;
use std::fmt;

pub mod channels;
mod contracts;
mod ga;
pub mod hc_vote;
mod names;
mod oracles;
mod schema;
mod signed;
mod spend;
mod transaction;
pub use channels::ChannelTransaction;
pub use contracts::{ContractOperation, ContractTransaction};
pub use ga::GaAttachTransaction;
pub use hc_vote::HcVote;
pub use names::{NameOperation, NameTransaction};
pub use oracles::{OracleOperation, OracleTransaction};
pub use schema::TransactionKind;
pub use signed::{SignedTransaction, verify_signatures};
pub use spend::Spend;
pub use transaction::storage;
pub use transaction::{
    GaMeta, Layer, PayingFor, Pricing, Transaction, TransactionLeaf, TransactionLimits,
    TransactionRef, Wrapper,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Object(ObjectError),
    Type(TypeError),
    NotTransaction,
    UnsupportedVersion,
    WrongTransactionKind,
    InvalidIdType,
    InvalidStateHash,
    OriginUnavailable,
    StateTransport(ae_state::transport::Error),
    InvalidPointer,
    InvalidSerializationVersion,
    InvalidTtlType,
    SignatureVerificationFailed,
    RecursivePayer,
    NotAllowedToPayFor(TransactionKind),
    UnsupportedTransaction(TransactionKind),
    ResourceLimit,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<ObjectError> for Error {
    fn from(e: ObjectError) -> Self {
        Self::Object(e)
    }
}
impl From<TypeError> for Error {
    fn from(e: TypeError) -> Self {
        Self::Type(e)
    }
}

// This schema-level representation is intentionally NOT a validated transaction.
// Callbacks additionally constrain IDs, TTL types, VM/ABI versions and embedded
// objects. Only a callback implementation may implement CanonicalTransaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireTransaction {
    kind: TransactionKind,
    version: u64,
    fields: Vec<(String, Value)>,
}
impl WireTransaction {
    pub fn new(
        kind: TransactionKind,
        version: u64,
        fields: Vec<(String, Value)>,
    ) -> Result<Self, Error> {
        let schema = kind.schema(version).ok_or(Error::UnsupportedVersion)?;
        object::serialize(kind.tag(), &version.into(), &schema, &fields)?;
        Ok(Self {
            kind,
            version,
            fields,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Self::decode_with_limits(bytes, ae_codec::rlp::DecodeLimits::default())
    }
    pub fn decode_with_limits(
        bytes: &[u8],
        limits: ae_codec::rlp::DecodeLimits,
    ) -> Result<Self, Error> {
        let envelope = object::decode_envelope_with_limits(bytes, limits)?;
        let kind = TransactionKind::from_tag(envelope.tag).ok_or(Error::NotTransaction)?;
        let version = envelope
            .version
            .to_u64()
            .map_err(|_| Error::UnsupportedVersion)?;
        let schema = kind.schema(version).ok_or(Error::UnsupportedVersion)?;
        if schema.len() != envelope.fields.len() {
            return Err(ObjectError::FieldCount.into());
        }
        let fields = schema
            .iter()
            .zip(&envelope.fields)
            .map(|(f, v)| Ok((f.name.clone(), object::decode_field(&f.kind, v)?)))
            .collect::<Result<_, ObjectError>>()?;
        Ok(Self {
            kind,
            version,
            fields,
        })
    }
    pub fn encode(&self) -> Vec<u8> {
        object::serialize(
            self.kind.tag(),
            &self.version.into(),
            &self.kind.schema(self.version).expect("checked schema"),
            &self.fields,
        )
        .expect("checked fields")
    }
    pub fn kind(&self) -> TransactionKind {
        self.kind
    }
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn fields(&self) -> &[(String, Value)] {
        &self.fields
    }
    pub fn field(&self, name: &str) -> Option<&Value> {
        self.fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }
}

// Encoding after decoding must include the callback's normalization, including
// nested signed transactions and embedded state objects where applicable.
pub trait CanonicalTransaction: Sized {
    fn decode(bytes: &[u8]) -> Result<Self, Error>;
    fn encode(&self) -> Vec<u8>;
}

impl From<ae_state::transport::Error> for Error {
    fn from(e: ae_state::transport::Error) -> Self {
        match e {
            ae_state::transport::Error::ResourceLimit => Self::ResourceLimit,
            e => Self::StateTransport(e),
        }
    }
}
impl Error {
    pub fn is_operational(&self) -> bool {
        matches!(
            self,
            Self::ResourceLimit
                | Self::Object(ObjectError::Rlp(ae_codec::rlp::DecodeError::ResourceLimit))
                | Self::StateTransport(ae_state::transport::Error::MissingAtomProfile(_))
        )
    }
}
