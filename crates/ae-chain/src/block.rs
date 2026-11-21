// aec_blocks binary transport, aeternity fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::fraud;
use ae_codec::{
    headers::{Header, HeaderError, HeaderMode, KeyHeader, MicroHeader},
    object::{self, Field, FieldType, ObjectTag, Value},
    rlp::{DecodeLimits, Rlp},
};
use ae_tx::{SignedTransaction, Transaction, TransactionLimits};
use ae_types::Hash256;

mod light;
pub use light::Light;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MicroBlock {
    pub header: MicroHeader,
    pub transactions: Vec<SignedTransaction<Transaction>>,
    pub proof: Option<Box<fraud::Proof>>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Block {
    Key(KeyHeader),
    Micro(MicroBlock),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Header(HeaderError),
    Object(object::ObjectError),
    Transaction(ae_tx::Error),
    Fraud(fraud::Error),
    ProofShape,
    ResourceLimit,
}
impl From<HeaderError> for Error {
    fn from(e: HeaderError) -> Self {
        Self::Header(e)
    }
}
impl From<object::ObjectError> for Error {
    fn from(e: object::ObjectError) -> Self {
        Self::Object(e)
    }
}
impl From<ae_tx::Error> for Error {
    fn from(e: ae_tx::Error) -> Self {
        Self::Transaction(e)
    }
}
impl From<fraud::Error> for Error {
    fn from(e: fraud::Error) -> Self {
        Self::Fraud(e)
    }
}
impl Error {
    pub fn is_operational(&self) -> bool {
        matches!(
            self,
            Self::ResourceLimit
                | Self::Object(object::ObjectError::Rlp(
                    ae_codec::rlp::DecodeError::ResourceLimit
                ))
                | Self::Fraud(fraud::Error::Object(object::ObjectError::Rlp(
                    ae_codec::rlp::DecodeError::ResourceLimit
                )))
        ) || matches!(self,Self::Transaction(e) if e.is_operational())
    }
}
#[derive(Clone, Copy)]
pub struct DecodeConfig<'a> {
    pub mode: HeaderMode,
    pub max_block_bytes: usize,
    pub max_transactions: usize,
    pub body: DecodeLimits,
    pub transaction: TransactionLimits,
    pub state: ae_state::transport::Config<'a>,
}
impl Default for DecodeConfig<'_> {
    fn default() -> Self {
        Self {
            mode: HeaderMode::ProofOfWork,
            max_block_bytes: 16 * 1024 * 1024,
            max_transactions: 100_000,
            body: DecodeLimits::default(),
            transaction: TransactionLimits::default(),
            state: Default::default(),
        }
    }
}
fn schema() -> Vec<Field> {
    ["txs", "pof"]
        .into_iter()
        .map(|name| Field {
            name: name.into(),
            kind: FieldType::List(Box::new(FieldType::Binary)),
        })
        .collect()
}
impl Block {
    pub fn header(&self) -> Header {
        match self {
            Self::Key(h) => Header::Key(h.clone()),
            Self::Micro(b) => Header::Micro(b.header.clone()),
        }
    }
    pub fn hash(&self) -> Result<Hash256, HeaderError> {
        self.header().hash()
    }
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = self.header().encode()?;
        if let Self::Micro(b) = self {
            let txs = Value::List(
                b.transactions
                    .iter()
                    .map(|t| Value::Binary(t.encode()))
                    .collect(),
            );
            let proof = Value::List(
                b.proof
                    .as_ref()
                    .map(|p| p.encode().map(Value::Binary))
                    .transpose()?
                    .into_iter()
                    .collect(),
            );
            bytes.extend(object::serialize(
                ObjectTag::MicroBlock,
                &u64::from(b.header.common.version).into(),
                &schema(),
                &[("txs".into(), txs), ("pof".into(), proof)],
            )?);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8], config: DecodeConfig<'_>) -> Result<Self, Error> {
        if bytes.len() > config.max_block_bytes {
            return Err(Error::ResourceLimit);
        }
        let (header, body) = Header::decode_partial(bytes, config.mode)?;
        let Header::Micro(header) = header else {
            let Header::Key(header) = header else {
                unreachable!()
            };
            return Ok(Self::Key(header));
        };
        let e = object::decode_envelope_with_limits(body, config.body)?;
        if e.tag != ObjectTag::MicroBlock {
            return Err(object::ObjectError::WrongTag.into());
        }
        if e.version != u64::from(header.common.version).into() {
            return Err(object::ObjectError::WrongVersion.into());
        }
        let [Rlp::List(txs), Rlp::List(proofs)] = e.fields.as_slice() else {
            return Err(if e.fields.len() == 2 {
                object::ObjectError::WrongType
            } else {
                object::ObjectError::FieldCount
            }
            .into());
        };
        if txs.len() > config.max_transactions {
            return Err(Error::ResourceLimit);
        }
        // Check the complete serialization template before invoking callbacks.
        if txs
            .iter()
            .chain(proofs)
            .any(|v| !matches!(v, Rlp::Bytes(_)))
        {
            return Err(object::ObjectError::WrongType.into());
        }
        let mut transactions = Vec::with_capacity(txs.len());
        for tx in txs {
            let Rlp::Bytes(tx) = tx else { unreachable!() };
            transactions.push(SignedTransaction::decode_with_state_config(
                tx,
                config.transaction,
                config.state,
            )?);
        }
        let proof = match proofs.as_slice() {
            [] => None,
            [Rlp::Bytes(bytes)] => Some(Box::new(fraud::Proof::decode(
                bytes,
                config.mode,
                config.body,
            )?)),
            _ => return Err(Error::ProofShape),
        };
        Ok(Self::Micro(MicroBlock {
            header,
            transactions,
            proof,
        }))
    }
}
