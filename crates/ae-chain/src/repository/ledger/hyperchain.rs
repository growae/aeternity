use super::*;
use crate::hyperchain as hc;
use ae_hyperchain::{block as consensus, chain};
use std::sync::Arc;

#[derive(Clone)]
pub struct HyperchainConfig<O> {
    pub identity: Identity,
    pub profile: Arc<chain::Profile<O>>,
    pub rules: hc::Rules,
    pub aevm: Option<ae_execution::contracts::aevm::Config>,
    pub limits: Limits,
}
#[derive(Debug)]
pub enum HyperchainError<E, S = MemoryStore> {
    Repository(Error),
    Common(InsertError<S>),
    Query(chain::Error<Overlay<S>, Overlay<S>>),
    Execution(Box<hc::Error<E, Overlay<S>, Overlay<S>>>),
}
impl<E, S> From<Error> for HyperchainError<E, S> {
    fn from(e: Error) -> Self {
        Self::Repository(e)
    }
}
impl<E, S> HyperchainError<E, S> {
    pub fn is_operational<H: consensus::Host<Error = E>>(&self) -> bool {
        match self {
            Self::Repository(_) => true,
            Self::Common(e) => e.is_operational(),
            Self::Query(e) => e.is_operational(),
            Self::Execution(e) => e.is_operational::<H>(),
        }
    }
}
// The caller owns a staged cache/host pair. Only the storage owner may install
// the pair after this sealed update receives a successful durable commit.
// No public input supplies precomputed output trees, scores or state roots.
pub fn prepare_hyperchain<S: Read, O: MapOrder + Clone, H: consensus::Host>(
    view: &View<S>,
    block: &Block,
    config: &HyperchainConfig<O>,
    now_ms: u64,
    state: &mut consensus::State,
    host: &mut H,
) -> Result<Update, HyperchainError<H::Error, S>> {
    let limits = config.limits;
    if view.identity != config.identity
        || view.identity.consensus != ConsensusType::Pos
        || !view.identity.matches(&config.profile.network)
        || view.identity.genesis != config.rules.genesis_hash
    {
        return Err(Error::Identity.into());
    }
    let current = block
        .hash()
        .map_err(|_| HyperchainError::Common(InsertError::Encoding))?;
    if view.block(current)?.is_some() {
        return Err(Error::AlreadyPresent.into());
    }
    let top = view.status.tip.ok_or(Error::Uninitialized)?;
    let header = block.header();
    if header.common().height == 0 {
        return Err(HyperchainError::Common(
            if matches!(block, Block::Micro(_)) {
                InsertError::GenesisMicro
            } else {
                InsertError::WrongGenesis
            },
        ));
    }
    let top_height = view.require_block(top)?.header().common().height;
    view.require_retained(header.common().height)?;
    check_policy(
        header.common().height,
        top_height,
        limits.policy,
        view.resilience().state,
        view.finalized_height()?,
    )
    .map_err(|(height, top)| HyperchainError::Common(InsertError::Policy { height, top }))?;
    let (network, mut writes) = view.resolve_protocol(
        &config.profile.network,
        header.common().height,
        limits.history,
    )?;
    if let (Block::Key(k), Some(fork)) = (block, network.community_fork()) {
        view.insert_signal(k, fork, limits.history, &mut writes)?;
    }
    let mut profile = config.profile.as_ref().clone();
    profile.network = network;
    let reader = super::super::hyperchain::reader(
        view.clone(),
        &config.identity,
        Arc::new(profile),
        super::super::hyperchain::Config {
            overlays: limits.overlay,
            maximum_history_headers: limits.history,
            configured_block_gas_limit: config.rules.configured_gas_limit.clone(),
            aevm: config.aevm.clone(),
        },
    )?;
    let parent_hash = Hash256(header.common().previous_hash);
    let previous = view.require_block(parent_hash)?.header();
    let key_hash = match &previous {
        Header::Key(_) => parent_hash,
        Header::Micro(m) => Hash256(m.common.previous_key_hash),
    };
    let previous_key = require_key(view, key_hash)?;
    let input = view.require_record(parent_hash)?;
    let parent_view = reader
        .view(&chain::RunEnv::Hash(parent_hash.0.to_vec()))
        .map_err(HyperchainError::Query)?;
    let branch = match block {
        Block::Key(k) => k,
        Block::Micro(_) => &previous_key,
    };
    let execution_history = Arc::new(super::super::vm_history(view, branch, limits.history)?);
    let mut history = rewards::History::new();
    if matches!(block,Block::Key(k) if k.common.height > config.rules.reward_delay) {
        let count = config
            .rules
            .reward_delay
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        if count > limits.history as u64 {
            return Err(Error::ResourceLimit.into());
        }
        let mut next = key_hash;
        for _ in 0..count {
            let header = require_key(view, next)?;
            let r = view.require_record(next)?;
            let previous = Hash256(header.common.previous_key_hash);
            let genesis = next == view.identity.genesis;
            history.insert(
                next,
                rewards::Record {
                    header,
                    fees: Some(r.fees),
                    reported_fraud: Some(r.fraud),
                },
            );
            if genesis {
                break;
            }
            next = previous;
        }
    }
    let parent = hc::Parent {
        header: &previous,
        key: &previous_key,
        view: &parent_view,
        fees: &input.fees,
        execution_history,
    };
    let mut rules = config.rules.clone();
    rules.now_ms = now_ms;
    let (mut next, mut staged) = (state.clone(), host.clone());
    let out = match hc::execute(
        block,
        &parent,
        &history,
        &reader,
        &mut next,
        &mut staged,
        &rules,
    ) {
        Ok(out) => out,
        Err(e) => {
            if !e.is_operational::<H>() {
                *state = next;
                *host = staged;
            }
            return Err(HyperchainError::Execution(Box::new(e)));
        }
    };
    let fork = if parent_hash != top && !view.children(parent_hash, limits.siblings)?.is_empty() {
        current
    } else {
        input.fork
    };
    let record = Record {
        roots: Roots::of(&out.trees),
        difficulty: out.score,
        fees: out.fees,
        fraud: false,
        fork,
    };
    let update = finish(
        view,
        block,
        Applied {
            state: out.trees,
            record,
            writes,
            events: out.transactions,
            consensus_events: out.consensus,
            consensus_state: Some(next.clone()),
            key_fraud: None,
            proof: None,
        },
        limits,
    )
    .map_err(HyperchainError::Common)?;
    *state = next;
    *host = staged;
    Ok(update)
}

#[derive(Debug)]
pub enum PinRequestError<S> {
    Repository(Error),
    Query(chain::Error<Overlay<S>, Overlay<S>>),
    Transform(crate::key::Error<Overlay<S>, Overlay<S>>),
}
impl<S> PinRequestError<S> {
    pub fn is_operational(&self) -> bool {
        match self {
            Self::Repository(_) => true,
            Self::Query(e) => e.is_operational(),
            Self::Transform(e) => e.is_operational(),
        }
    }
}
// Resolve from the incoming predecessor hash, not the currently selected tip.
// The shared key transform includes expiry and protocol migrations before the
// election contract is queried with native pre-transform context semantics.
pub fn hyperchain_pin_request<S: Read, O: MapOrder + Clone>(
    view: &View<S>,
    block: &Block,
    config: &HyperchainConfig<O>,
) -> Result<Option<Vec<u8>>, PinRequestError<S>> {
    use PinRequestError as E;
    let Block::Key(k) = block else {
        return Ok(None);
    };
    if k.common.height == 0 {
        return Ok(None);
    }
    if view.identity != config.identity
        || view.identity.consensus != ConsensusType::Pos
        || !view.identity.matches(&config.profile.network)
        || view.identity.genesis != config.rules.genesis_hash
    {
        return Err(E::Repository(Error::Identity));
    }
    let network = view
        .resolve_protocol(
            &config.profile.network,
            k.common.height,
            config.limits.history,
        )
        .map_err(E::Repository)?
        .0;
    let mut profile = config.profile.as_ref().clone();
    profile.network = network;
    let reader = super::super::hyperchain::reader(
        view.clone(),
        &config.identity,
        Arc::new(profile),
        super::super::hyperchain::Config {
            overlays: config.limits.overlay,
            maximum_history_headers: config.limits.history,
            configured_block_gas_limit: config.rules.configured_gas_limit.clone(),
            aevm: config.aevm.clone(),
        },
    )
    .map_err(E::Repository)?;
    let parent_hash = Hash256(k.common.previous_hash);
    let previous = view
        .require_block(parent_hash)
        .map_err(E::Repository)?
        .header();
    let key_hash = match &previous {
        Header::Key(_) => parent_hash,
        Header::Micro(m) => Hash256(m.common.previous_key_hash),
    };
    let previous_key = require_key(view, key_hash).map_err(E::Repository)?;
    let input = view.require_record(parent_hash).map_err(E::Repository)?;
    let parent_view = reader
        .view(&chain::RunEnv::Hash(parent_hash.0.to_vec()))
        .map_err(E::Query)?;
    let parent = hc::Parent {
        header: &previous,
        key: &previous_key,
        view: &parent_view,
        fees: &input.fees,
        execution_history: Arc::new(
            super::super::vm_history(view, k, config.limits.history).map_err(E::Repository)?,
        ),
    };
    let prepared = hc::input_trees(&reader, &parent, k, &config.rules).map_err(E::Transform)?;
    let result = consensus::pin_request(&reader, &parent_view, &prepared.trees, k.common.height)
        .map_err(E::Query)?;
    // Other native pin_info values are rejected by execution before Host::pin.
    Ok(match result {
        Some(ae_vm_fate::value::Value::Bytes(hash)) => Some(hash),
        _ => None,
    })
}
