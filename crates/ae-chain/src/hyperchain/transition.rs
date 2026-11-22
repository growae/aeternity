use super::{Error, Outcome, Parent, Rules, validation};
use crate::{block::Block, execution, key, rewards};
use ae_codec::headers::{Header, KeyHeader};
use ae_execution::{Context, transactions::batch};
use ae_hyperchain::{
    block as hc,
    chain::{self, Reader},
    epochs, signing,
};
use ae_protocol::{Protocol, coinbase};
use ae_trie::NodeWriter;
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use ae_vm_fate::{machine::program::remote::context::Consensus, maps::MapOrder};
use std::sync::Arc;

pub(super) fn frame<E, A: Clone, C: Clone>(
    parent: &Parent<'_, A, C>,
    key: &KeyHeader,
    height: u64,
    time: u64,
    hash: Hash256,
    gas: &Unsigned,
) -> Result<chain::View<A, C>, Error<E, A, C>> {
    let mut v = parent.view.clone();
    v.context = Context::transaction(
        height,
        Protocol::try_from(key.common.version).map_err(Error::Protocol)?,
    );
    v.history = parent.execution_history.clone();
    v.environment.auth_tx_hash = None;
    v.environment.origin = [0; 32];
    v.environment.gas_price = 0.into();
    v.environment.fee = 0.into();
    v.environment.beneficiary = key.beneficiary;
    v.environment.timestamp = time.into();
    v.environment.difficulty = key.target.into();
    v.environment.block_gas_limit = rewards::number(gas);
    v.environment.consensus = Consensus::Pos;
    v.environment.key_hash = hash.0;
    if let Some(a) = &mut v.aevm {
        a.environment.genesis_height = 0;
        a.environment.key_hash = hash.0;
        a.environment.history = v.history.clone();
        a.environment.beneficiary = AccountPublicKey(key.beneficiary);
        a.environment.difficulty = key.target.into();
        a.environment.timestamp_ms = time.into();
        a.environment.block_gas_limit = rewards::number(gas);
        a.environment.auth_tx_hash = None;
        a.network_id = v.environment.network_id.clone().into();
        a.event_context = ae_execution::aevm_chain::EventContext::None;
    }
    Ok(v)
}
fn grant<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder, S: chain::Source<A, C>, E>(
    reader: &Reader<A, C, O, S>,
    view: &mut chain::View<A, C>,
    header: &KeyHeader,
    history: &rewards::History,
    rules: &Rules,
) -> Result<(), Error<E, A, C>> {
    if header.common.height <= rules.reward_delay {
        return Ok(());
    }
    let after_height = header.common.height - rules.reward_delay + 1;
    let mut after = header;
    let mut left = rules.max_reward_history;
    while after.common.height > after_height {
        after = &rewards::predecessor(after, history, &mut left)
            .map_err(Error::Rewards)?
            .1
            .header;
    }
    if after.common.height != after_height {
        return Err(Error::Rewards(rewards::Error::HistoryInvalid));
    }
    let (_, current) = rewards::predecessor(after, history, &mut left).map_err(Error::Rewards)?;
    let (before_hash, before) =
        rewards::predecessor(&current.header, history, &mut left).map_err(Error::Rewards)?;
    if before.header.common.height == 0 && before_hash != rules.genesis_hash {
        return Err(Error::Rewards(rewards::Error::HistoryInvalid));
    }
    let fees = current
        .fees
        .as_ref()
        .ok_or(Error::Rewards(rewards::Error::FeesMissing(
            Header::Key(current.header.clone())
                .hash()
                .map_err(Error::Encoding)?,
        )))?;
    let reward = &rules.fixed_coinbase;
    for (i, k) in [&before.header, &current.header, after]
        .into_iter()
        .enumerate()
    {
        let part = rewards::unsigned(
            rewards::number(fees) * rewards::number(&rules.fee_distribution[i]) / 100u32,
        );
        let amount = if i == 1 { part.add(reward) } else { part };
        if k.hole || k.common.height == 0 {
            continue;
        }
        let next = reader
            .with_execution(view, |cfg| {
                epochs::apply(
                    &view.trees,
                    reader.profile.contracts,
                    &epochs::Transition::Reward {
                        height: (header.common.height - rules.reward_delay).into(),
                        beneficiary: AccountPublicKey(k.beneficiary),
                        amount,
                    },
                    &view.context,
                    cfg,
                )
            })
            .map_err(Error::Contract)?;
        view.trees = Arc::new(next);
    }
    Ok(())
}
// Logical execution never publishes durable records. The ledger owner stages
// this cache/host pair until its block/state/index batch commits. Native cache
// prefixes are retained on modeled native errors; operational failures retry.
pub fn execute<
    A: NodeWriter + Clone,
    C: NodeWriter + Clone,
    O: MapOrder,
    S: chain::Source<A, C>,
    H: hc::Host,
>(
    block: &Block,
    parent: &Parent<'_, A, C>,
    history: &rewards::History,
    reader: &Reader<A, C, O, S>,
    state: &mut hc::State,
    host: &mut H,
    rules: &Rules,
) -> Result<Outcome<A, C>, Error<H::Error, A, C>> {
    let mut next = state.clone();
    let mut staged = host.clone();
    let result = execute_inner(
        block,
        parent,
        history,
        reader,
        &mut next,
        &mut staged,
        rules,
    );
    if !result.as_ref().is_err_and(|e| e.is_operational::<H>()) {
        *state = next;
        *host = staged;
    }
    result
}
fn execute_inner<
    A: NodeWriter + Clone,
    C: NodeWriter + Clone,
    O: MapOrder,
    S: chain::Source<A, C>,
    H: hc::Host,
>(
    block: &Block,
    parent: &Parent<'_, A, C>,
    history: &rewards::History,
    reader: &Reader<A, C, O, S>,
    state: &mut hc::State,
    host: &mut H,
    rules: &Rules,
) -> Result<Outcome<A, C>, Error<H::Error, A, C>> {
    if parent.view.environment.consensus != Consensus::Pos
        || parent.view.environment.network_id != reader.profile.network.network_id()
        || rules.reward_delay < coinbase::FRAUD_REPORT_DELAY
        || reader.profile.context.in_auth
        || !reader.profile.context.on_chain
        || reader.profile.context.reserved_gas != 0.into()
    {
        return Err(Error::Configuration);
    }
    if block.header().common().height == 0 {
        return Err(Error::Genesis);
    }
    validation::standalone(block, Some(parent.header), &reader.profile.network, rules)?;
    validation::context(block, parent)?;
    let reader = reader.pin().map_err(Error::Chain)?;
    let h = block.header();
    let hash = h.hash().map_err(Error::Encoding)?;
    let gas = reader
        .profile
        .network
        .block_gas_limit(&rules.configured_gas_limit);
    let key = match block {
        Block::Key(k) => k,
        Block::Micro(_) => parent.key,
    };
    let key_hash = match block {
        Block::Key(_) => hash,
        Block::Micro(m) => Hash256(m.header.common.previous_key_hash),
    };
    let mut current = frame(
        parent,
        key,
        h.common().height,
        ae_consensus::headers::timestamp(&h),
        key_hash,
        &gas,
    )?;
    match block {
        Block::Key(k) => validation::key_target(k, parent, &reader, current.clone())?,
        Block::Micro(m) => signing::validate_micro(
            state,
            &reader,
            &m.header,
            ae_consensus::headers::timestamp(parent.header),
            host,
            rules.consensus.cache_limits,
        )
        .map_err(Error::Micro)?,
    }
    if parent.view.trees.roots().hash().0 != parent.header.common().state_hash {
        return Err(Error::ParentState);
    }
    let incoming = if matches!(parent.header, Header::Key(_)) {
        0.into()
    } else {
        parent.fees.clone()
    };
    let mut out = Outcome {
        trees: parent.view.trees.as_ref().clone(),
        fees: incoming.clone(),
        score: validation::score(block),
        transactions: vec![],
        consensus: vec![],
        wait_ms: None,
    };
    if let Block::Key(k) = block {
        let prepared = input_trees(&reader, parent, k, rules).map_err(Error::Transform)?;
        out.trees = prepared.trees;
        out.fees = prepared.fees;
    }
    let kind = match block {
        Block::Key(_) => hc::Kind::Key,
        Block::Micro(_) => hc::Kind::Micro,
    };
    let transformed = state
        .pre_transform(
            &reader,
            parent.view,
            &out.trees,
            host,
            kind,
            h.common().height,
            &rules.consensus,
        )
        .map_err(Error::Consensus)?;
    out.trees = transformed.trees;
    out.consensus = transformed.events;
    out.wait_ms = transformed.wait_ms;
    current.trees = Arc::new(out.trees);
    match block {
        Block::Key(k) => {
            validation::leader(k, state)?;
            grant(&reader, &mut current, k, history, rules)?;
            out.trees = current.trees.as_ref().clone();
        }
        Block::Micro(m) => {
            let mut trees = current.trees.as_ref().clone();
            let applied = reader
                .with_execution(&current, |cfg| {
                    batch::apply_micro(
                        &m.transactions,
                        &mut trees,
                        &current.context,
                        cfg,
                        rewards::number(&gas),
                        rewards::number(&incoming),
                    )
                })
                .map_err(Error::Transactions)?;
            out.trees = trees;
            out.fees = rewards::unsigned(applied.fees);
            out.transactions = applied.transactions.events;
        }
    }
    validation::root(&out.trees, &h)?;
    Ok(out)
}

pub(crate) fn input_trees<
    A: NodeWriter + Clone,
    C: NodeWriter + Clone,
    O: MapOrder,
    S: chain::Source<A, C>,
>(
    reader: &Reader<A, C, O, S>,
    parent: &Parent<'_, A, C>,
    k: &KeyHeader,
    rules: &Rules,
) -> Result<key::Transformed<A, C>, key::Error<A, C>> {
    let incoming = if matches!(parent.header, Header::Key(_)) {
        0.into()
    } else {
        parent.fees.clone()
    };
    let p = execution::Parent {
        headers: ae_consensus::headers::Context {
            previous: parent.header,
            previous_key: parent.key,
            recent_keys: &[],
        },
        previous_generation: execution::PreviousGeneration::Missing,
        fees: rewards::number(&incoming),
        reported_fraud: false,
    };
    let gas = reader
        .profile
        .network
        .block_gas_limit(&rules.configured_gas_limit);
    // pre_transform installs its incoming block environment for migrations;
    // expiry and gas fees use the exact predecessor trees in both paths.
    let hash = Header::Key(k.clone())
        .hash()
        .map_err(key::Error::Encoding)?;
    let current =
        frame::<(), _, _>(parent, k, k.common.height, k.time, hash, &gas).map_err(|e| match e {
            Error::Protocol(p) => key::Error::Header(p.into()),
            _ => unreachable!("frame protocol"),
        })?;
    reader.with_execution(&current, |cfg| {
        key::pre_transform(
            &parent.view.trees,
            k,
            &p,
            cfg,
            key::TransformRules {
                gas: &gas,
                custom_forks: &rules.custom_forks,
                fork_limits: rules.fork_limits,
                max_calls: rules.max_calls,
            },
        )
    })
}
