// aesc_*_tx.erl, aeternity fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{CanonicalTransaction, Error, TransactionKind, WireTransaction};
use ae_codec::object::Value;
use ae_protocol::Protocol;
use ae_state::{
    Trees,
    transport::{Config as StateConfig, Limits as StateLimits, Proofs},
};
use ae_trie::MemoryStore;
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};

mod encoding;
mod update;
pub use update::Update;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateDelegates {
    Shared(Vec<AccountPublicKey>),
    Separate {
        initiator: Vec<AccountPublicKey>,
        responder: Vec<AccountPublicKey>,
    },
}
// Canonical bytes retain equality and deterministic serialization without
// embedding backend handles in signed transactions. Decode validates the full
// tree import, including its historical expiry-cache reconstruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffchainTrees {
    bytes: Vec<u8>,
}
impl OffchainTrees {
    pub fn decode(bytes: &[u8], config: StateConfig<'_>) -> Result<Self, Error> {
        let t = Trees::deserialize_from_binary(
            bytes,
            MemoryStore::default(),
            MemoryStore::default(),
            config,
        )?;
        Ok(Self {
            bytes: t.serialize_to_binary(config.limits)?,
        })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn materialize(&self, config: StateConfig<'_>) -> Result<Trees, Error> {
        Ok(Trees::deserialize_from_binary(
            &self.bytes,
            MemoryStore::default(),
            MemoryStore::default(),
            config,
        )?)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Create {
        initiator: AccountPublicKey,
        initiator_amount: Unsigned,
        responder: AccountPublicKey,
        responder_amount: Unsigned,
        reserve: Unsigned,
        lock_period: Unsigned,
        delegates: CreateDelegates,
        state_hash: [u8; 32],
    },
    Deposit {
        channel: [u8; 32],
        from: AccountPublicKey,
        amount: Unsigned,
        state_hash: [u8; 32],
        round: Unsigned,
    },
    Withdraw {
        channel: [u8; 32],
        to: AccountPublicKey,
        amount: Unsigned,
        state_hash: [u8; 32],
        round: Unsigned,
    },
    CloseMutual {
        channel: [u8; 32],
        from: Id,
        initiator_amount: Unsigned,
        responder_amount: Unsigned,
    },
    CloseSolo {
        channel: [u8; 32],
        from: AccountPublicKey,
        payload: Vec<u8>,
        proof: Proofs,
    },
    Slash {
        channel: [u8; 32],
        from: AccountPublicKey,
        payload: Vec<u8>,
        proof: Proofs,
    },
    Settle {
        channel: [u8; 32],
        from: AccountPublicKey,
        initiator_amount: Unsigned,
        responder_amount: Unsigned,
    },
    SnapshotSolo {
        channel: [u8; 32],
        from: AccountPublicKey,
        payload: Vec<u8>,
    },
    SetDelegates {
        channel: [u8; 32],
        from: AccountPublicKey,
        initiator_delegates: Vec<Id>,
        responder_delegates: Vec<Id>,
        payload: Vec<u8>,
        state_hash: Vec<u8>,
        round: Unsigned,
    },
    ForceProgress {
        channel: [u8; 32],
        from: AccountPublicKey,
        payload: Vec<u8>,
        round: Unsigned,
        update: Update,
        state_hash: Vec<u8>,
        trees: OffchainTrees,
    },
    // None is wire version 2. Version 1 deliberately retains opaque update
    // binaries: its decoder does not call aesc_offchain_update:deserialize.
    Offchain {
        channel: [u8; 32],
        updates: Option<Vec<Vec<u8>>>,
        state_hash: Vec<u8>,
    },
}
impl Operation {
    pub fn kind(&self) -> TransactionKind {
        use TransactionKind as K;
        match self {
            Self::Create { .. } => K::ChannelCreate,
            Self::Deposit { .. } => K::ChannelDeposit,
            Self::Withdraw { .. } => K::ChannelWithdraw,
            Self::CloseMutual { .. } => K::ChannelCloseMutual,
            Self::CloseSolo { .. } => K::ChannelCloseSolo,
            Self::Slash { .. } => K::ChannelSlash,
            Self::Settle { .. } => K::ChannelSettle,
            Self::SnapshotSolo { .. } => K::ChannelSnapshotSolo,
            Self::SetDelegates { .. } => K::ChannelSetDelegates,
            Self::ForceProgress { .. } => K::ChannelForceProgress,
            Self::Offchain { .. } => K::ChannelOffchain,
        }
    }
    pub fn channel(&self) -> Option<[u8; 32]> {
        match self {
            Self::Create { .. } => None,
            Self::Deposit { channel, .. }
            | Self::Withdraw { channel, .. }
            | Self::CloseMutual { channel, .. }
            | Self::CloseSolo { channel, .. }
            | Self::Slash { channel, .. }
            | Self::Settle { channel, .. }
            | Self::SnapshotSolo { channel, .. }
            | Self::SetDelegates { channel, .. }
            | Self::ForceProgress { channel, .. }
            | Self::Offchain { channel, .. } => Some(*channel),
        }
    }
    pub fn payload(&self) -> Option<&[u8]> {
        match self {
            Self::CloseSolo { payload, .. }
            | Self::Slash { payload, .. }
            | Self::SnapshotSolo { payload, .. }
            | Self::SetDelegates { payload, .. }
            | Self::ForceProgress { payload, .. } => Some(payload),
            _ => None,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignerRequirement {
    Accounts(Vec<AccountPublicKey>),
    Channel([u8; 32]),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelTransaction {
    operation: Operation,
    fee: Unsigned,
    ttl: Unsigned,
    nonce: Unsigned,
    pub(crate) wire_size: usize,
    canonical: Vec<u8>,
}
impl ChannelTransaction {
    pub fn new(
        operation: Operation,
        nonce: Unsigned,
        fee: Unsigned,
        ttl: Unsigned,
        limits: StateLimits,
    ) -> Result<Self, Error> {
        let offchain = matches!(operation, Operation::Offchain { .. });
        let mut t = Self {
            operation,
            nonce,
            fee: if offchain { 0.into() } else { fee },
            ttl: if offchain { 0.into() } else { ttl },
            wire_size: 0,
            canonical: vec![],
        };
        t.canonical = t.wire(limits)?.encode();
        t.wire_size = t.canonical.len();
        Ok(t)
    }
    pub fn operation(&self) -> &Operation {
        &self.operation
    }
    pub fn kind(&self) -> TransactionKind {
        self.operation.kind()
    }
    pub fn fee(&self) -> &Unsigned {
        &self.fee
    }
    pub fn ttl(&self) -> &Unsigned {
        &self.ttl
    }
    pub fn nonce(&self) -> &Unsigned {
        &self.nonce
    }
    pub fn wire_size(&self) -> usize {
        self.wire_size
    }
    pub fn signer_requirement(&self) -> Result<SignerRequirement, Error> {
        match &self.operation {
            Operation::Create {
                initiator,
                responder,
                ..
            } => Ok(SignerRequirement::Accounts(vec![*initiator, *responder])),
            Operation::Deposit { channel, .. }
            | Operation::Withdraw { channel, .. }
            | Operation::CloseMutual { channel, .. }
            | Operation::SetDelegates { channel, .. }
            | Operation::Offchain { channel, .. } => Ok(SignerRequirement::Channel(*channel)),
            _ => Ok(SignerRequirement::Accounts(vec![self.origin()?])),
        }
    }
    pub fn origin(&self) -> Result<AccountPublicKey, Error> {
        match &self.operation {
            Operation::Create { initiator, .. } => Ok(*initiator),
            Operation::Deposit { from, .. }
            | Operation::CloseSolo { from, .. }
            | Operation::Slash { from, .. }
            | Operation::Settle { from, .. }
            | Operation::SnapshotSolo { from, .. }
            | Operation::SetDelegates { from, .. }
            | Operation::ForceProgress { from, .. } => Ok(*from),
            Operation::Withdraw { to, .. } => Ok(*to),
            Operation::CloseMutual { from, .. } if from.tag == IdTag::Account => {
                Ok(AccountPublicKey(from.value))
            }
            Operation::CloseMutual { .. } => Err(Error::InvalidIdType),
            Operation::Offchain { .. } => Err(Error::OriginUnavailable),
        }
    }
    pub fn gas(&self) -> Unsigned {
        match &self.operation {
            Operation::ForceProgress { update, .. } => update
                .amounts()
                .map_or_else(|| 0.into(), |(_, _, gas)| gas.clone()),
            _ => 0.into(),
        }
    }
    pub fn gas_price(&self) -> Option<&Unsigned> {
        match &self.operation {
            Operation::ForceProgress { update, .. } => update.amounts().map(|(_, p, _)| p),
            _ => None,
        }
    }
    pub fn base_gas(&self, p: Protocol) -> u64 {
        match self.operation {
            Operation::Offchain { .. } => 0,
            Operation::ForceProgress { .. } if p >= Protocol::Fortuna => 450_000,
            _ => 15_000,
        }
    }
    pub fn valid_at_protocol(&self, p: Protocol) -> Result<bool, Error> {
        self.valid_at_protocol_with_config(
            p,
            crate::TransactionLimits::default(),
            StateConfig::default(),
        )
    }
    pub fn valid_at_protocol_with_config(
        &self,
        p: Protocol,
        limits: crate::TransactionLimits,
        config: StateConfig<'_>,
    ) -> Result<bool, Error> {
        match &self.operation {
            Operation::Create { delegates, .. } => {
                return Ok(matches!(delegates, CreateDelegates::Shared(_)) == (p < Protocol::Iris));
            }
            Operation::Offchain { updates, .. } => {
                return Ok(if updates.is_some() {
                    p < Protocol::Lima
                } else {
                    p >= Protocol::Fortuna
                });
            }
            Operation::SetDelegates { .. } if p < Protocol::Iris => return Ok(false),
            _ => {}
        }
        let Some(payload) = self.operation.payload() else {
            return Ok(true);
        };
        if payload.is_empty() {
            return Ok(true);
        }
        let decoded = (|| {
            let (inner, _) = crate::signed::decode_parts(payload, limits.rlp)?;
            crate::Transaction::decode_with_state_config(inner, limits, config)
        })();
        match decoded {
            Ok(tx)
                if tx.leaf().kind() == TransactionKind::ChannelOffchain
                    && tx
                        .layers()
                        .iter()
                        .all(|l| matches!(l.wrapper(), crate::Wrapper::GaMeta(_))) =>
            {
                tx.view().valid_at_protocol(p)
            }
            Err(e) if e.is_operational() => Err(e),
            _ => Ok(false),
        }
    }
}
impl CanonicalTransaction for ChannelTransaction {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Self::decode_with_config(bytes, StateConfig::default())
    }
    fn encode(&self) -> Vec<u8> {
        self.canonical.clone()
    }
}
