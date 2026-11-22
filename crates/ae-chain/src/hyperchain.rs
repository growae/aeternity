// aec_chain_state and aec_consensus_hc insertion/execution, fa2a6d7.
// Copyright (c) Aeternity. ISC: repository LICENSE.
use crate::{admission, forks, key, rewards};
use ae_codec::headers::{Header, KeyHeader};
use ae_execution::{chain_history, transactions::batch};
use ae_hyperchain::{block as hc_block, chain, consensus_contract, signing};
use ae_state::Trees;
use ae_trie::MemoryStore;
use ae_types::{Hash256, Unsigned};
use std::sync::Arc;
pub mod candidate;
mod transition;
mod validation;
pub use transition::execute;
pub(crate) use transition::input_trees;
pub use validation::{keep_tip, score, standalone};

#[derive(Debug)]
pub enum AdmissionError {
    Encoding(ae_codec::headers::HeaderError),
    Protocol(ae_protocol::ProfileError),
    Body(admission::BodyError),
    Seal(signing::Error),
    FutureTimestamp,
    MicroInterval,
}
impl AdmissionError {
    pub fn is_operational(&self) -> bool {
        match self {
            Self::Protocol(e) => e.is_operational(),
            Self::Body(e) => e.is_operational(),
            _ => false,
        }
    }
}
#[derive(Debug, Clone)]
pub struct Rules {
    pub genesis_hash: Hash256,
    pub now_ms: u64,
    pub micro_cycle_ms: u64,
    pub configured_gas_limit: Unsigned,
    pub body_limits: admission::Limits,
    pub consensus: hc_block::Config,
    pub reward_delay: u64,
    pub fixed_coinbase: Unsigned,
    pub fee_distribution: [Unsigned; 3],
    pub max_reward_history: usize,
    pub max_calls: usize,
    pub custom_forks: forks::CustomForks,
    pub fork_limits: forks::Limits,
}
impl Rules {
    pub fn new(genesis_hash: Hash256, consensus: hc_block::Config) -> Self {
        Self {
            genesis_hash,
            consensus,
            now_ms: 0,
            micro_cycle_ms: 3_000,
            configured_gas_limit: 6_000_000.into(),
            body_limits: Default::default(),
            reward_delay: ae_protocol::coinbase::REWARD_DELAY,
            fixed_coinbase: 0.into(),
            fee_distribution: [0.into(), 100.into(), 0.into()],
            max_reward_history: 100_000,
            max_calls: 100_000,
            custom_forks: Default::default(),
            fork_limits: Default::default(),
        }
    }
}
pub struct Parent<'a, A = MemoryStore, C = MemoryStore> {
    pub header: &'a Header,
    pub key: &'a KeyHeader,
    pub view: &'a chain::View<A, C>,
    pub fees: &'a Unsigned,
    pub execution_history: Arc<chain_history::Snapshot>,
}
#[derive(Debug)]
pub enum Error<E, A = MemoryStore, C = MemoryStore> {
    Encoding(ae_codec::headers::HeaderError),
    Protocol(ae_protocol::ProfileError),
    Body(admission::BodyError),
    Seal(signing::Error),
    Micro(signing::MicroError<E, A, C>),
    Chain(chain::Error<A, C>),
    Consensus(hc_block::Error<E, A, C>),
    Contract(consensus_contract::Error<A, C>),
    Transform(key::Error<A, C>),
    Transactions(batch::Error<A, C>),
    Rewards(rewards::Error),
    Configuration,
    ParentState,
    PreviousHash,
    PreviousKeyHash,
    Height,
    Genesis,
    FutureTimestamp,
    MicroInterval,
    WrongTarget,
    InvalidEndOfEpoch,
    InvalidLeader,
    MissingLeader,
    StateHash { actual: Hash256, expected: Hash256 },
}
impl<E, A, C> From<AdmissionError> for Error<E, A, C> {
    fn from(error: AdmissionError) -> Self {
        match error {
            AdmissionError::Encoding(e) => Self::Encoding(e),
            AdmissionError::Protocol(e) => Self::Protocol(e),
            AdmissionError::Body(e) => Self::Body(e),
            AdmissionError::Seal(e) => Self::Seal(e),
            AdmissionError::FutureTimestamp => Self::FutureTimestamp,
            AdmissionError::MicroInterval => Self::MicroInterval,
        }
    }
}
impl<E, A, C> Error<E, A, C> {
    pub fn is_operational<H: hc_block::Host<Error = E>>(&self) -> bool {
        match self {
            Self::Protocol(e) => e.is_operational(),
            Self::Body(e) => e.is_operational(),
            Self::Micro(signing::MicroError::Leader(e)) | Self::Consensus(e) => {
                matches!(e, hc_block::Error::ParentNotSynced { .. }) || e.operational::<H>()
            }
            Self::Chain(e) => e.is_operational(),
            Self::Contract(e) => !e.is_native(),
            Self::Transform(e) => e.is_operational(),
            Self::Transactions(e) => e.failure.is_operational(),
            Self::Configuration | Self::ParentState | Self::MissingLeader | Self::Rewards(_) => {
                true
            }
            _ => false,
        }
    }
}
#[derive(Debug)]
pub struct Outcome<A, C> {
    pub trees: Trees<A, C>,
    pub fees: Unsigned,
    pub score: Unsigned,
    pub transactions: Vec<batch::Event>,
    pub consensus: Vec<hc_block::Event>,
    pub wait_ms: Option<Unsigned>,
}
