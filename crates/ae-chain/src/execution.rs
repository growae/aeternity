// PoW micro-block insertion stages, aec_chain_state/aec_consensus_bitcoin_ng,
// aeternity fa2a6d7. Copyright (c) 2017, Aeternity Anstalt. ISC: LICENSE.
use crate::{admission, block::MicroBlock};
use ae_codec::headers::{Header, HeaderError, KeyHeader};
use ae_consensus::{
    headers,
    target::{Target, TargetError},
};
use ae_execution::{
    Context, contracts,
    transactions::{self, batch},
};
use ae_protocol::{ConsensusType, Protocol};
use ae_state::Trees;
use ae_trie::{MemoryStore, NodeWriter};
use ae_types::{AccountPublicKey, Hash256};
use ae_vm_fate::{machine::program::remote::context::Consensus, maps::MapOrder};
use num_bigint::{BigInt, Sign};

#[derive(Clone, Copy)]
pub enum PreviousGeneration<'a> {
    Unavailable,
    Missing,
    Found(&'a KeyHeader),
}
pub struct Parent<'a> {
    pub headers: headers::Context<'a>,
    pub previous_generation: PreviousGeneration<'a>,
    pub fees: BigInt,
    pub reported_fraud: bool,
}
pub use crate::admission::PowRules as Rules;
#[derive(Debug)]
pub enum Error<A = MemoryStore, C = MemoryStore> {
    Admission(admission::PowError),
    Header(headers::Error),
    Encoding(HeaderError),
    Transactions(batch::Error<A, C>),
    WrongConsensus,
    ParentState,
    MissingHistory,
    InvalidHistory,
    ParentDifficulty(TargetError),
    WrongProofKey,
    StateHash { actual: Hash256, expected: Hash256 },
    DoubleReportedFraud,
}
impl<A, C> Error<A, C> {
    pub fn is_operational(&self) -> bool {
        match self {
            Self::Admission(admission::PowError::Header(headers::Error::Protocol(e)))
            | Self::Header(headers::Error::Protocol(e))
                if e.is_operational() =>
            {
                true
            }
            Self::Transactions(e) => e.failure.is_operational(),
            Self::Admission(admission::PowError::Body(e)) => e.is_operational(),
            Self::WrongConsensus
            | Self::ParentState
            | Self::MissingHistory
            | Self::InvalidHistory
            | Self::ParentDifficulty(_) => true,
            Self::Header(
                headers::Error::MissingHistory { .. } | headers::Error::InvalidHistory,
            ) => true,
            _ => false,
        }
    }
}
#[derive(Debug)]
pub struct ExecutedMicro {
    hash: Hash256,
    state_hash: Hash256,
    fees: BigInt,
    reported_fraud: bool,
    events: Vec<batch::Event>,
}
impl ExecutedMicro {
    pub fn hash(&self) -> Hash256 {
        self.hash
    }
    pub fn state_hash(&self) -> Hash256 {
        self.state_hash
    }
    pub fn fees(&self) -> &BigInt {
        &self.fees
    }
    pub fn reported_fraud(&self) -> bool {
        self.reported_fraud
    }
    pub fn events(&self) -> &[batch::Event] {
        &self.events
    }
}
// Parents and VM history snapshots must belong to the same selected branch.
// This publishes logical trees, never durable records or a selected chain tip.
// PoW's micro pre-transform is the identity; Hyperchain has a separate path.
pub fn pow_micro<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    block: &MicroBlock,
    trees: &mut Trees<A, C>,
    parent: &Parent<'_>,
    config: &transactions::Config<'_, O>,
    rules: &Rules,
) -> Result<ExecutedMicro, Error<A, C>> {
    micro(block, trees, parent, config, rules, false, None)
}
pub fn on_demand_micro<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    block: &MicroBlock,
    trees: &mut Trees<A, C>,
    parent: &Parent<'_>,
    config: &transactions::Config<'_, O>,
    rules: &Rules,
) -> Result<ExecutedMicro, Error<A, C>> {
    micro(block, trees, parent, config, rules, true, None)
}
pub(crate) fn micro<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    block: &MicroBlock,
    trees: &mut Trees<A, C>,
    parent: &Parent<'_>,
    config: &transactions::Config<'_, O>,
    rules: &Rules,
    on_demand: bool,
    observation: Option<&crate::instrumentation::Insertion>,
) -> Result<ExecutedMicro, Error<A, C>> {
    if config.consensus != ConsensusType::Pow {
        return Err(Error::WrongConsensus);
    }
    let header = Header::Micro(block.header.clone());
    let validate = if on_demand {
        admission::validate_on_demand_micro
    } else {
        admission::validate_pow_micro
    };
    validate(block, config.network, Some(parent.headers.previous), rules)
        .map_err(Error::Admission)?;
    if on_demand {
        headers::validate_linkage(&header, &parent.headers).map_err(Error::Header)?;
    } else {
        headers::validate_context_with_cycle(
            &header,
            &parent.headers,
            &Default::default(),
            rules.micro_cycle_ms,
        )
        .map_err(Error::Header)?;
    }
    if let Some(proof) = &block.proof
        && !on_demand
    {
        let key = if matches!(parent.headers.previous, Header::Micro(_)) {
            parent.headers.previous_key.miner
        } else {
            match parent.previous_generation {
                PreviousGeneration::Unavailable => return Err(Error::MissingHistory),
                PreviousGeneration::Missing => return Err(Error::WrongProofKey),
                PreviousGeneration::Found(k) => {
                    if Header::Key(k.clone()).hash().map_err(Error::Encoding)?.0
                        != parent.headers.previous_key.common.previous_key_hash
                    {
                        return Err(Error::InvalidHistory);
                    }
                    k.miner
                }
            }
        };
        if proof.public_key.as_slice() != key {
            return Err(Error::WrongProofKey);
        }
    }
    if trees.roots().hash().0 != parent.headers.previous.common().state_hash {
        return Err(Error::ParentState);
    }
    let key = parent.headers.previous_key;
    let protocol = Protocol::try_from(key.common.version).map_err(|e| Error::Header(e.into()))?;
    let context = Context::transaction(block.header.common.height, protocol);
    let gas_limit = config
        .network
        .fixed_block_gas_limit()
        .map_or_else(|| rules.configured_gas_limit.clone(), BigInt::from);
    let mut next = trees.clone();
    let apply = |config: &transactions::Config<'_, O>| {
        let mut observed = crate::instrumentation::Micro::new(observation);
        let result = batch::apply_micro_observed(
            &block.transactions,
            &mut next,
            &context,
            config,
            gas_limit.clone(),
            parent.fees.clone(),
            |result| observed.completed(result),
        );
        observed.finish(result)
    };
    let result = if on_demand {
        Ok(with_block_environment_difficulty(
            config,
            key,
            block.header.time,
            block.header.common.previous_key_hash,
            &gas_limit,
            ae_consensus::on_demand::TAG.into(),
            apply,
        ))
    } else {
        with_block_environment(
            config,
            key,
            block.header.time,
            block.header.common.previous_key_hash,
            &gas_limit,
            apply,
        )
    }
    .map_err(Error::ParentDifficulty)?
    .map_err(Error::Transactions)?;
    let actual = next.roots().hash();
    let expected = Hash256(block.header.common.state_hash);
    if actual != expected {
        return Err(Error::StateHash { actual, expected });
    }
    if block.proof.is_some() && parent.reported_fraud {
        return Err(Error::DoubleReportedFraud);
    }
    let result = ExecutedMicro {
        hash: header.hash().map_err(Error::Encoding)?,
        state_hash: actual,
        fees: result.fees,
        reported_fraud: parent.reported_fraud || block.proof.is_some(),
        events: result.transactions.events,
    };
    *trees = next;
    Ok(result)
}

pub(crate) fn with_block_environment<O: MapOrder, R>(
    config: &transactions::Config<'_, O>,
    key: &KeyHeader,
    timestamp: u64,
    key_hash: [u8; 32],
    gas_limit: &BigInt,
    f: impl FnOnce(&transactions::Config<'_, O>) -> R,
) -> Result<R, TargetError> {
    let difficulty = match config.consensus {
        ConsensusType::Pow => {
            let value = Target(key.target).difficulty()?;
            BigInt::from_bytes_be(Sign::Plus, value.as_bytes())
        }
        ConsensusType::Pos => key.target.into(),
    };
    Ok(with_block_environment_difficulty(
        config, key, timestamp, key_hash, gas_limit, difficulty, f,
    ))
}
pub(crate) fn with_block_environment_difficulty<O: MapOrder, R>(
    config: &transactions::Config<'_, O>,
    key: &KeyHeader,
    timestamp: u64,
    key_hash: [u8; 32],
    gas_limit: &BigInt,
    difficulty: BigInt,
    f: impl FnOnce(&transactions::Config<'_, O>) -> R,
) -> R {
    let fate_context = config.fate.map(|f| {
        let mut c = f.context.clone();
        c.in_auth = false;
        c.on_chain = true;
        c.reserved_gas = 0.into();
        c
    });
    let fate_env = config.fate.map(|f| {
        let mut e = f.environment.clone();
        e.auth_tx_hash = None;
        e.beneficiary = key.beneficiary;
        e.timestamp = timestamp.into();
        e.difficulty = difficulty.clone();
        e.block_gas_limit = gas_limit.clone();
        e.network_id = config.network.network_id().to_vec();
        e.consensus = match config.consensus {
            ConsensusType::Pow => Consensus::Pow,
            ConsensusType::Pos => Consensus::Pos,
        };
        e.key_hash = key_hash;
        e
    });
    let fate = config.fate.map(|f| contracts::fate::Config {
        context: fate_context.as_ref().expect("FATE template"),
        order: f.order,
        overrides: f.overrides,
        environment: fate_env.as_ref().expect("FATE environment"),
        history: f.history.clone(),
        auth_tx: None,
        metering_floor: None,
        diagnostic_profile: f.diagnostic_profile,
    });
    let aevm = config.aevm.map(|a| {
        let mut a = a.clone();
        a.network_id = config.network.network_id().into();
        a.environment.key_hash = key_hash;
        a.environment.beneficiary = AccountPublicKey(key.beneficiary);
        a.environment.difficulty = difficulty.clone();
        a.environment.timestamp_ms = timestamp.into();
        a.environment.block_gas_limit = gas_limit.clone();
        a.environment.auth_tx_hash = None;
        a.event_context = ae_execution::aevm_chain::EventContext::None;
        a
    });
    let config = transactions::Config {
        network: config.network,
        consensus: config.consensus,
        fate: fate.as_ref(),
        aevm: aevm.as_ref(),
        signature_limits: config.signature_limits,
        channel_limits: config.channel_limits,
        channel_state_atoms: config.channel_state_atoms,
        oracle_limits: config.oracle_limits,
        fate_limits: config.fate_limits,
        maximum_fate_steps: config.maximum_fate_steps,
        aevm_limits: config.aevm_limits,
    };
    f(&config)
}
