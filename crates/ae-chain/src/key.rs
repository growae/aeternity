// aec_chain_state:apply_node_transactions and aec_trees pre-transform, fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{
    admission,
    block::Block,
    execution::{Parent, with_block_environment},
    forks, rewards,
};
use ae_codec::headers::{Header, HeaderError, KeyHeader};
use ae_consensus::{headers, target::TargetError};
use ae_execution::{Context, transactions};
use ae_protocol::{ConsensusType, Protocol, coinbase};
use ae_state::{
    Trees,
    calls::{Call, Calls},
    contracts,
};
use ae_trie::{MemoryStore, NodeReader, NodeWriter};
use ae_types::{Hash256, Unsigned};
use ae_vm_fate::maps::MapOrder;
use num_bigint::Sign;

#[derive(Debug, Clone)]
pub struct Rules {
    pub admission: admission::PowRules,
    pub reward_delay: u64,
    pub genesis_hash: Hash256,
    pub reward_policy: Option<rewards::Policy>,
    pub custom_forks: forks::CustomForks,
    pub fork_limits: forks::Limits,
    pub max_calls: usize,
    pub max_reward_history: usize,
    pub checkpoints: headers::Checkpoints,
}
impl Rules {
    pub fn new(admission: admission::PowRules, genesis_hash: Hash256) -> Self {
        Self {
            admission,
            genesis_hash,
            reward_delay: coinbase::REWARD_DELAY,
            reward_policy: None,
            custom_forks: Default::default(),
            fork_limits: Default::default(),
            max_calls: 100_000,
            max_reward_history: 100_000,
            checkpoints: Default::default(),
        }
    }
}
#[derive(Debug)]
pub enum Error<A = MemoryStore, C = MemoryStore> {
    Admission(admission::PowError),
    Header(headers::Error),
    Encoding(HeaderError),
    WrongConsensus,
    ParentState,
    InvalidFees,
    InvalidGas,
    VersionRegression,
    GasFees(GasFeesError),
    Expiry(ae_execution::Error),
    ForkAsset,
    Migration(forks::Error<A, C>),
    Rewards(rewards::Error),
    Difficulty(TargetError),
    StateHash { actual: Hash256, expected: Hash256 },
}
impl<A, C> Error<A, C> {
    pub fn is_operational(&self) -> bool {
        match self {
            Self::Admission(admission::PowError::Body(e)) => e.is_operational(),
            Self::Header(
                headers::Error::InvalidHistory | headers::Error::MissingHistory { .. },
            ) => true,
            Self::Admission(_)
            | Self::Header(_)
            | Self::StateHash { .. }
            | Self::VersionRegression => false,
            _ => true,
        }
    }
}
#[derive(Debug)]
pub enum GasFeesError {
    Tree(ae_trie::Error),
    Call(contracts::Error),
    ResourceLimit,
}
pub fn gas_fees<S: NodeReader>(
    calls: &Calls<S>,
    max_calls: usize,
) -> Result<Unsigned, GasFeesError> {
    let mut total = Unsigned::from(0u64);
    for (i, item) in calls.tree().iter().enumerate() {
        if i >= max_calls {
            return Err(GasFeesError::ResourceLimit);
        }
        let (_, v) = item.map_err(GasFeesError::Tree)?;
        let ae_trie::Value::Bytes(bytes) = v else {
            return Err(GasFeesError::Call(contracts::Error::InvalidValue));
        };
        let call = Call::deserialize([0; 32], &bytes).map_err(GasFeesError::Call)?;
        total = total.add(&rewards::unsigned(
            rewards::number(&call.gas_used) * rewards::number(&call.gas_price),
        ));
    }
    Ok(total)
}
#[derive(Debug)]
pub struct Prepared<A = MemoryStore, C = MemoryStore> {
    trees: Trees<A, C>,
    fees: Unsigned,
    reward: Option<rewards::Award>,
}
impl<A: NodeReader, C: NodeReader> Prepared<A, C> {
    pub fn trees(&self) -> &Trees<A, C> {
        &self.trees
    }
    pub fn fees(&self) -> &Unsigned {
        &self.fees
    }
    pub fn reward(&self) -> Option<&rewards::Award> {
        self.reward.as_ref()
    }
    pub fn into_trees(self) -> Trees<A, C> {
        self.trees
    }
}
// Candidate state preparation is also used before the header state hash/seal
// are available. It does not certify the header or publish the chain tip.
pub fn prepare<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    trees: &Trees<A, C>,
    header: &KeyHeader,
    parent: &Parent<'_>,
    history: &rewards::History,
    execution: &transactions::Config<'_, O>,
    rules: &Rules,
) -> Result<Prepared<A, C>, Error<A, C>> {
    if execution.consensus != ConsensusType::Pow {
        return Err(Error::WrongConsensus);
    }
    if parent.fees.sign() == Sign::Minus {
        return Err(Error::InvalidFees);
    }
    if rules.admission.configured_gas_limit.sign() == Sign::Minus
        && execution.network.fixed_block_gas_limit().is_none()
    {
        return Err(Error::InvalidGas);
    }
    let current = Protocol::try_from(header.common.version).map_err(|e| Error::Header(e.into()))?;
    let previous = parent.headers.previous.common().version;
    if header.common.version < previous {
        return Err(Error::VersionRegression);
    }
    let gas = rewards::unsigned(
        execution
            .network
            .fixed_block_gas_limit()
            .map_or_else(|| rules.admission.configured_gas_limit.clone(), Into::into),
    );
    let fees = gas_fees(&trees.calls, rules.max_calls)
        .map_err(Error::GasFees)?
        .add(&rewards::unsigned(parent.fees.clone()));
    let context = Context::transaction(header.common.height, current);
    let mut next = trees.clone();
    next.calls.prune();
    ae_execution::oracles::prune(&mut next.accounts, &mut next.oracles, context.height)
        .map_err(Error::Expiry)?;
    ae_execution::names::prune(&mut next.accounts, &mut next.names, context.height, current)
        .map_err(Error::Expiry)?;
    if header.common.version > previous {
        let migration = if execution.network.fixed_block_gas_limit().is_some() {
            forks::public(execution.network, current).map_err(|_| Error::ForkAsset)?
        } else {
            forks::Migration {
                allocations: rules
                    .custom_forks
                    .get(&current)
                    .cloned()
                    .unwrap_or_default(),
                contracts: vec![],
            }
        };
        let key_hash = Header::Key(header.clone())
            .hash()
            .map_err(Error::Encoding)?
            .0;
        with_block_environment(
            execution,
            header,
            header.time,
            key_hash,
            &rewards::number(&gas),
            |config| {
                forks::apply(
                    &mut next,
                    &migration,
                    &context,
                    config,
                    &gas,
                    rules.fork_limits,
                )
            },
        )
        .map_err(Error::Difficulty)?
        .map_err(Error::Migration)?;
    }
    let policy = rewards::Policy::for_network(execution.network, rules.reward_policy.clone());
    let reward = rewards::grant(
        &mut next,
        header,
        parent.reported_fraud,
        &rewards::Schedule {
            history,
            delay: rules.reward_delay,
            policy: &policy,
            max_history: rules.max_reward_history,
            genesis_hash: rules.genesis_hash,
        },
    )
    .map_err(Error::Rewards)?;
    Ok(Prepared {
        trees: next,
        fees,
        reward,
    })
}
#[derive(Debug)]
pub struct Executed {
    hash: Hash256,
    state_hash: Hash256,
    closed_generation_fees: Unsigned,
    reward: Option<rewards::Award>,
}
impl Executed {
    pub fn hash(&self) -> Hash256 {
        self.hash
    }
    pub fn state_hash(&self) -> Hash256 {
        self.state_hash
    }
    pub fn closed_generation_fees(&self) -> &Unsigned {
        &self.closed_generation_fees
    }
    pub fn reward(&self) -> Option<&rewards::Award> {
        self.reward.as_ref()
    }
}
pub fn pow_key<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    header: &KeyHeader,
    trees: &mut Trees<A, C>,
    parent: &Parent<'_>,
    history: &rewards::History,
    execution: &transactions::Config<'_, O>,
    rules: &Rules,
) -> Result<Executed, Error<A, C>> {
    if execution.consensus != ConsensusType::Pow {
        return Err(Error::WrongConsensus);
    }
    let block = Block::Key(header.clone());
    admission::validate_pow(
        &block,
        execution.network,
        Some(parent.headers.previous),
        &rules.admission,
    )
    .map_err(Error::Admission)?;
    headers::validate_context(&block.header(), &parent.headers, &rules.checkpoints)
        .map_err(Error::Header)?;
    if trees.roots().hash().0 != parent.headers.previous.common().state_hash {
        return Err(Error::ParentState);
    }
    let prepared = prepare(trees, header, parent, history, execution, rules)?;
    let actual = prepared.trees.roots().hash();
    let expected = Hash256(header.common.state_hash);
    if actual != expected {
        return Err(Error::StateHash { actual, expected });
    }
    let r = Executed {
        hash: block.hash().map_err(Error::Encoding)?,
        state_hash: actual,
        closed_generation_fees: prepared.fees,
        reward: prepared.reward,
    };
    *trees = prepared.trees;
    Ok(r)
}
