// aetx, aega_meta_tx and aec_paying_for_tx, aeternity fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{
    CanonicalTransaction, ContractTransaction, Error, GaAttachTransaction, NameTransaction,
    OracleTransaction, Spend, TransactionKind, WireTransaction,
    ga::{account, binary, int},
    signed,
};
use ae_codec::{
    object::{self, Field, FieldType, ObjectError, ObjectTag, Value},
    rlp::{DecodeLimits, Rlp},
};
use ae_crypto::blake2b_256;
use ae_protocol::{NetworkProfile, Protocol};
use ae_types::{AccountPublicKey, Hash256, Id, IdTag, Signature, Unsigned};

mod pricing;
pub use pricing::Pricing;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GaMeta {
    pub account: AccountPublicKey,
    pub auth_data: Vec<u8>,
    pub abi_version: Unsigned,
    pub fee: Unsigned,
    pub gas: Unsigned,
    pub gas_price: Unsigned,
    // Some is the pre-Iris v1 encoding; None is v2 and inherits inner TTL.
    pub ttl: Option<Unsigned>,
}
impl GaMeta {
    pub fn auth_id(&self) -> Hash256 {
        let mut b = self.account.0.to_vec();
        b.extend_from_slice(&self.auth_data);
        blake2b_256(&b)
    }
    pub fn serialize_auth_data(&self, network_prefixed_inner: &[u8]) -> Vec<u8> {
        object::serialize(
            ObjectTag::GaMetaTxAuthData,
            &1.into(),
            &[
                Field {
                    name: "fee".into(),
                    kind: FieldType::Integer,
                },
                Field {
                    name: "gas_price".into(),
                    kind: FieldType::Integer,
                },
                Field {
                    name: "tx_hash".into(),
                    kind: FieldType::Binary,
                },
            ],
            &[
                ("fee".into(), Value::Integer(self.fee.clone())),
                ("gas_price".into(), Value::Integer(self.gas_price.clone())),
                (
                    "tx_hash".into(),
                    Value::Binary(blake2b_256(network_prefixed_inner).0.to_vec()),
                ),
            ],
        )
        .expect("typed authentication data")
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayingFor {
    pub payer: AccountPublicKey,
    pub nonce: Unsigned,
    pub fee: Unsigned,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Wrapper {
    GaMeta(GaMeta),
    PayingFor(PayingFor),
}
impl Wrapper {
    pub fn kind(&self) -> TransactionKind {
        match self {
            Self::GaMeta(_) => TransactionKind::GaMeta,
            Self::PayingFor(_) => TransactionKind::PayingFor,
        }
    }
    fn encode(&self, signed_inner: Vec<u8>) -> Vec<u8> {
        let (version, mut fields) = match self {
            Self::GaMeta(m) => {
                let mut f = vec![
                    (
                        "ga_id".into(),
                        Value::Id(Id::new(IdTag::Account, m.account.0)),
                    ),
                    ("auth_data".into(), Value::Binary(m.auth_data.clone())),
                    ("abi_version".into(), Value::Integer(m.abi_version.clone())),
                    ("fee".into(), Value::Integer(m.fee.clone())),
                    ("gas".into(), Value::Integer(m.gas.clone())),
                    ("gas_price".into(), Value::Integer(m.gas_price.clone())),
                ];
                if let Some(ttl) = &m.ttl {
                    f.push(("ttl".into(), Value::Integer(ttl.clone())));
                }
                (if m.ttl.is_some() { 1 } else { 2 }, f)
            }
            Self::PayingFor(p) => (
                1,
                vec![
                    (
                        "payer_id".into(),
                        Value::Id(Id::new(IdTag::Account, p.payer.0)),
                    ),
                    ("nonce".into(), Value::Integer(p.nonce.clone())),
                    ("fee".into(), Value::Integer(p.fee.clone())),
                ],
            ),
        };
        fields.push(("tx".into(), Value::Binary(signed_inner)));
        WireTransaction::new(self.kind(), version, fields)
            .expect("typed wrapper schema")
            .encode()
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layer {
    wrapper: Wrapper,
    signatures: Vec<Signature>,
    wire_size: usize,
}
impl Layer {
    pub fn wrapper(&self) -> &Wrapper {
        &self.wrapper
    }
    pub fn inner_signatures(&self) -> &[Signature] {
        &self.signatures
    }
    pub fn wire_size(&self) -> usize {
        self.wire_size
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionLeaf {
    Spend(Spend),
    Name(NameTransaction),
    Oracle(OracleTransaction),
    Contract(ContractTransaction),
    GaAttach(GaAttachTransaction),
}
impl TransactionLeaf {
    pub fn kind(&self) -> TransactionKind {
        match self {
            Self::Spend(_) => TransactionKind::Spend,
            Self::Name(t) => t.operation().kind(),
            Self::Oracle(t) => t.operation().kind(),
            Self::Contract(t) => t.operation().kind(),
            Self::GaAttach(_) => TransactionKind::GaAttach,
        }
    }
    pub fn wire_size(&self) -> usize {
        match self {
            Self::Spend(t) => t.wire_size(),
            Self::Name(t) => t.wire_size(),
            Self::Oracle(t) => t.wire_size(),
            Self::Contract(t) => t.wire_size(),
            Self::GaAttach(t) => t.wire_size(),
        }
    }
    pub fn origin(&self) -> AccountPublicKey {
        match self {
            Self::Spend(t) => t.sender(),
            Self::Name(t) => t.account(),
            Self::Oracle(t) => t.account(),
            Self::Contract(t) => t.signer(),
            Self::GaAttach(t) => t.owner(),
        }
    }
    pub fn fee(&self) -> &Unsigned {
        match self {
            Self::Spend(t) => t.fee(),
            Self::Name(t) => t.fee(),
            Self::Oracle(t) => t.fee(),
            Self::Contract(t) => t.fee(),
            Self::GaAttach(t) => t.fee(),
        }
    }
    pub fn nonce(&self) -> &Unsigned {
        match self {
            Self::Spend(t) => t.nonce(),
            Self::Name(t) => t.nonce(),
            Self::Oracle(t) => t.nonce(),
            Self::Contract(t) => t.nonce(),
            Self::GaAttach(t) => t.nonce(),
        }
    }
    pub fn ttl(&self) -> &Unsigned {
        match self {
            Self::Spend(t) => t.ttl(),
            Self::Name(t) => t.ttl(),
            Self::Oracle(t) => t.ttl(),
            Self::Contract(t) => t.ttl(),
            Self::GaAttach(t) => t.ttl(),
        }
    }
    pub fn valid_at_protocol(&self, p: Protocol) -> bool {
        match self {
            Self::Spend(_) => true,
            Self::Name(t) => t.valid_at_protocol(p),
            Self::Oracle(t) => t.valid_at_protocol(p),
            Self::Contract(t) => t.valid_at_protocol(p),
            Self::GaAttach(t) => t.valid_at_protocol(p),
        }
    }
}
impl CanonicalTransaction for TransactionLeaf {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let tag = object::decode_envelope(bytes)?.tag;
        let kind = TransactionKind::from_tag(tag).ok_or(Error::NotTransaction)?;
        Ok(match kind {
            TransactionKind::Spend => Self::Spend(Spend::decode(bytes)?),
            TransactionKind::NamePreclaim
            | TransactionKind::NameClaim
            | TransactionKind::NameUpdate
            | TransactionKind::NameTransfer
            | TransactionKind::NameRevoke => Self::Name(NameTransaction::decode(bytes)?),
            TransactionKind::OracleRegister
            | TransactionKind::OracleExtend
            | TransactionKind::OracleQuery
            | TransactionKind::OracleResponse => Self::Oracle(OracleTransaction::decode(bytes)?),
            TransactionKind::ContractCreate | TransactionKind::ContractCall => {
                Self::Contract(ContractTransaction::decode(bytes)?)
            }
            TransactionKind::GaAttach => Self::GaAttach(GaAttachTransaction::decode(bytes)?),
            _ => return Err(Error::UnsupportedTransaction(kind)),
        })
    }
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Spend(t) => t.encode(),
            Self::Name(t) => t.encode(),
            Self::Oracle(t) => t.encode(),
            Self::Contract(t) => t.encode(),
            Self::GaAttach(t) => t.encode(),
        }
    }
}

// Layers are stored innermost first. Decode, traversal, encoding and Drop never
// recurse through the Rust stack. Unsupported channel/HC leaves stay explicit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    leaf: TransactionLeaf,
    layers: Vec<Layer>,
}
#[derive(Debug, Clone, Copy)]
pub struct TransactionRef<'a> {
    leaf: &'a TransactionLeaf,
    layers: &'a [Layer],
}
#[derive(Debug, Clone, Copy)]
pub struct TransactionLimits {
    pub rlp: DecodeLimits,
    pub max_layers: usize,
    pub max_total_bytes: usize,
}
impl Default for TransactionLimits {
    fn default() -> Self {
        Self {
            rlp: DecodeLimits::default(),
            max_layers: 4096,
            max_total_bytes: 256 * 1024 * 1024,
        }
    }
}
impl Transaction {
    pub fn new(leaf: TransactionLeaf) -> Self {
        Self {
            leaf,
            layers: vec![],
        }
    }
    pub fn view(&self) -> TransactionRef<'_> {
        TransactionRef {
            leaf: &self.leaf,
            layers: &self.layers,
        }
    }
    pub fn leaf(&self) -> &TransactionLeaf {
        &self.leaf
    }
    pub fn layers(&self) -> &[Layer] {
        &self.layers
    }
    // Match the original constructor: recursive paying-for is rejected by
    // deserialization and execution, not by construction or serialization.
    pub fn wrap(mut self, wrapper: Wrapper, mut signatures: Vec<Signature>) -> Self {
        signatures.sort();
        signatures.dedup();
        let wire_size = wrapper
            .encode(signed::encode_parts(&signatures, self.encode()))
            .len();
        self.layers.push(Layer {
            wrapper,
            signatures,
            wire_size,
        });
        self
    }
    pub fn validate_payer_nesting(&self) -> Result<(), Error> {
        let mut payer = false;
        for layer in &self.layers {
            if matches!(layer.wrapper, Wrapper::PayingFor(_)) {
                if payer {
                    return Err(Error::RecursivePayer);
                }
                payer = true;
            }
        }
        Ok(())
    }
    pub fn decode_with_limits(bytes: &[u8], limits: TransactionLimits) -> Result<Self, Error> {
        let mut current = bytes;
        let mut layers = Vec::new();
        let mut remaining = limits.max_total_bytes;
        loop {
            remaining = remaining
                .checked_sub(current.len())
                .ok_or(Error::ResourceLimit)?;
            let e = object::decode_envelope_with_limits(current, limits.rlp)?;
            let kind = TransactionKind::from_tag(e.tag).ok_or(Error::NotTransaction)?;
            if !matches!(kind, TransactionKind::GaMeta | TransactionKind::PayingFor) {
                let leaf = TransactionLeaf::decode(current)?;
                layers.reverse();
                let tx = Self { leaf, layers };
                tx.validate_payer_nesting()?;
                return Ok(tx);
            }
            if layers.len() >= limits.max_layers {
                return Err(Error::ResourceLimit);
            }
            let version = e.version.to_u64().map_err(|_| Error::UnsupportedVersion)?;
            let schema = kind.schema(version).ok_or(Error::UnsupportedVersion)?;
            if e.fields.len() != schema.len() {
                return Err(ObjectError::FieldCount.into());
            }
            let (last, fields) = e.fields.split_last().expect("wrapper schema");
            // Decode the header only; keep nested transaction binaries borrowed.
            let fields = schema
                .iter()
                .zip(fields)
                .map(|(f, v)| Ok((f.name.clone(), object::decode_field(&f.kind, v)?)))
                .collect::<Result<_, ObjectError>>()?;
            let Rlp::Bytes(signed_bytes) = last else {
                return Err(ObjectError::WrongType.into());
            };
            let w = WireTransaction {
                kind,
                version,
                fields,
            };
            let wrapper = match kind {
                TransactionKind::GaMeta => Wrapper::GaMeta(GaMeta {
                    account: account(&w, "ga_id")?,
                    auth_data: binary(&w, "auth_data"),
                    abi_version: int(&w, "abi_version"),
                    fee: int(&w, "fee"),
                    gas: int(&w, "gas"),
                    gas_price: int(&w, "gas_price"),
                    ttl: (version == 1).then(|| int(&w, "ttl")),
                }),
                TransactionKind::PayingFor => Wrapper::PayingFor(PayingFor {
                    payer: account(&w, "payer_id")?,
                    nonce: int(&w, "nonce"),
                    fee: int(&w, "fee"),
                }),
                _ => unreachable!(),
            };
            remaining = remaining
                .checked_sub(signed_bytes.len())
                .ok_or(Error::ResourceLimit)?;
            let (inner, signatures) = signed::decode_parts(signed_bytes, limits.rlp)?;
            layers.push(Layer {
                wrapper,
                signatures,
                wire_size: current.len(),
            });
            current = inner;
        }
    }
}
impl CanonicalTransaction for Transaction {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Self::decode_with_limits(bytes, TransactionLimits::default())
    }
    fn encode(&self) -> Vec<u8> {
        self.view().encode()
    }
}
impl<'a> TransactionRef<'a> {
    pub fn leaf(self) -> &'a TransactionLeaf {
        self.leaf
    }
    pub fn layers(self) -> &'a [Layer] {
        self.layers
    }
    pub fn layer(self) -> Option<&'a Layer> {
        self.layers.last()
    }
    pub fn inner(self) -> Option<Self> {
        self.layers.split_last().map(|(_, layers)| Self {
            leaf: self.leaf,
            layers,
        })
    }
    pub fn kind(self) -> TransactionKind {
        self.layer()
            .map_or_else(|| self.leaf.kind(), |l| l.wrapper.kind())
    }
    pub fn wire_size(self) -> usize {
        self.layer()
            .map_or_else(|| self.leaf.wire_size(), |l| l.wire_size)
    }
    pub fn origin(self) -> AccountPublicKey {
        match self.layer().map(|l| &l.wrapper) {
            Some(Wrapper::GaMeta(m)) => m.account,
            Some(Wrapper::PayingFor(p)) => p.payer,
            None => self.leaf.origin(),
        }
    }
    pub fn nonce(self) -> Unsigned {
        match self.layer().map(|l| &l.wrapper) {
            Some(Wrapper::GaMeta(_)) => 0.into(),
            Some(Wrapper::PayingFor(p)) => p.nonce.clone(),
            None => self.leaf.nonce().clone(),
        }
    }
    pub fn fee(self) -> &'a Unsigned {
        match self.layer().map(|l| &l.wrapper) {
            Some(Wrapper::GaMeta(m)) => &m.fee,
            Some(Wrapper::PayingFor(p)) => &p.fee,
            None => self.leaf.fee(),
        }
    }
    // None is aetx's max_ttl sentinel, including inherited inner zero TTL.
    pub fn ttl(self) -> Option<&'a Unsigned> {
        for layer in self.layers.iter().rev() {
            if let Wrapper::GaMeta(GaMeta { ttl: Some(ttl), .. }) = &layer.wrapper {
                return (ttl != &0.into()).then_some(ttl);
            }
        }
        let ttl = self.leaf.ttl();
        (ttl != &0.into()).then_some(ttl)
    }
    pub fn gas_price(self) -> Option<&'a Unsigned> {
        match self.layer().map(|l| &l.wrapper) {
            Some(Wrapper::GaMeta(m)) => Some(&m.gas_price),
            Some(Wrapper::PayingFor(_)) => None,
            None => match self.leaf {
                TransactionLeaf::Contract(t) => Some(t.gas_price()),
                TransactionLeaf::GaAttach(t) => Some(t.gas_price()),
                _ => None,
            },
        }
    }
    pub fn valid_at_protocol(self, p: Protocol) -> bool {
        self.layers.iter().all(|l| match &l.wrapper {
            Wrapper::GaMeta(m) => {
                p >= Protocol::Fortuna && (m.ttl.is_some() == (p < Protocol::Iris))
            }
            Wrapper::PayingFor(_) => p >= Protocol::Iris,
        }) && self.leaf.valid_at_protocol(p)
    }
    pub fn encode(self) -> Vec<u8> {
        let mut bytes = self.leaf.encode();
        for layer in self.layers {
            bytes = layer
                .wrapper
                .encode(signed::encode_parts(&layer.signatures, bytes));
        }
        bytes
    }
    pub fn auth_hash(self, protocol: Protocol, network: &NetworkProfile) -> Result<Hash256, Error> {
        let Some(Layer {
            wrapper: Wrapper::GaMeta(m),
            ..
        }) = self.layer()
        else {
            return Err(Error::WrongTransactionKind);
        };
        let mut b = network.network_id().to_vec();
        b.extend_from_slice(&self.inner().expect("meta inner").encode());
        Ok(blake2b_256(&if protocol >= Protocol::Ceres {
            m.serialize_auth_data(&b)
        } else {
            b
        }))
    }
}
