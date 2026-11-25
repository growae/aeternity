// Borrowed repository preparation for a real PoS microblock candidate. The
// composing storage owner serializes the supplied HC cache/host during begin;
// packing and updates subsequently use this same immutable transaction view.
use super::*;
use crate::hyperchain::{self as hc, candidate};
use ae_hyperchain::block as consensus;
use ae_vm_fate::maps::MapOrder;

#[derive(Debug)]
pub enum Error<E, S> {
    Repository(super::Error),
    Query(chain::Error<Overlay<S>, Overlay<S>>),
    Candidate(candidate::Error<E, Overlay<S>, Overlay<S>>),
}
impl<E, S> Error<E, S> {
    pub fn host_error(&self) -> Option<&E> {
        match self {
            Self::Candidate(e) => e.host_error(),
            _ => None,
        }
    }
    pub fn is_operational<H: consensus::Host<Error = E>>(&self) -> bool {
        match self {
            Self::Repository(_) => true,
            Self::Query(e) => e.is_operational(),
            Self::Candidate(e) => e.is_operational::<H>(),
        }
    }
}
pub struct Prepared<S, O> {
    pub candidate: candidate::Micro<Overlay<S>, Overlay<S>>,
    pub reader: Reader<S, O>,
    pub status: super::super::Status,
    pub source_id: u64,
}
#[derive(Clone, Copy)]
pub struct Input {
    pub parent: Hash256,
    pub now: i128,
    pub limits: candidate::Limits,
}
pub fn micro<S: Read, O: MapOrder + Clone, H: consensus::Host>(
    view: &View<S>,
    execution: &super::super::HyperchainConfig<O>,
    input: Input,
    state: &mut consensus::State,
    host: &mut H,
) -> Result<Prepared<S, O>, Error<H::Error, S>> {
    let repository = Error::Repository;
    if view.identity() != &execution.identity
        || view.identity().consensus != ConsensusType::Pos
        || !view.identity().matches(&execution.profile.network)
        || view.identity().genesis != execution.rules.genesis_hash
    {
        return Err(repository(super::Error::Identity));
    }
    let block = view.require_block(input.parent).map_err(repository)?;
    let Block::Key(key) = block else {
        return Err(Error::Candidate(candidate::Error::WrongParent));
    };
    let header = Header::Key(key.clone());
    let reader = execution
        .reader(view.clone(), execution.profile.clone())
        .map_err(repository)?;
    let parent_view = reader
        .view(&chain::RunEnv::Hash(input.parent.0.to_vec()))
        .map_err(Error::Query)?;
    let record = view.require_record(input.parent).map_err(repository)?;
    let parent = hc::Parent {
        header: &header,
        key: &key,
        view: &parent_view,
        fees: &record.fees,
        execution_history: parent_view.history.clone(),
    };
    let proof = view
        .discovered_proof(Hash256(key.common.previous_key_hash))
        .map_err(repository)?
        .map(Box::new);
    let candidate = candidate::Micro::begin(
        &parent,
        &reader,
        state,
        host,
        &execution.rules,
        candidate::Start {
            now: input.now,
            proof,
            limits: input.limits,
        },
    )
    .map_err(Error::Candidate)?;
    Ok(Prepared {
        candidate,
        reader,
        status: view.status(),
        source_id: view.source().source_id(),
    })
}

pub struct PreparedKey<S, O> {
    pub candidate: candidate::Key<Overlay<S>, Overlay<S>>,
    pub reader: Reader<S, O>,
    pub status: super::super::Status,
    pub source_id: u64,
}
#[derive(Clone, Copy)]
pub struct KeyInput {
    pub parent: Hash256,
    pub production: candidate::KeyInput,
}
pub(crate) fn reward_history<S: Read>(
    view: &View<S>,
    key_hash: Hash256,
    height: u64,
    rules: &hc::Rules,
    limit: usize,
) -> Result<crate::rewards::History, super::Error> {
    use crate::rewards;
    fn require_key<S: Read>(
        view: &View<S>,
        hash: Hash256,
    ) -> Result<ae_codec::headers::KeyHeader, super::Error> {
        match view.require_block(hash)? {
            Block::Key(k) => Ok(k),
            _ => Err(super::Error::Corrupt("HC reward ancestor")),
        }
    }
    let mut history = rewards::History::new();
    if height > rules.reward_delay {
        let count = rules
            .reward_delay
            .checked_add(1)
            .ok_or(super::Error::ResourceLimit)?;
        if count > limit as u64 {
            return Err(super::Error::ResourceLimit);
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
    Ok(history)
}
pub fn key<S: Read, O: MapOrder + Clone, H: consensus::Host>(
    view: &View<S>,
    execution: &super::super::HyperchainConfig<O>,
    input: KeyInput,
    state: &mut consensus::State,
    host: &mut H,
) -> Result<PreparedKey<S, O>, Error<H::Error, S>> {
    let repository = Error::Repository;
    if view.identity() != &execution.identity
        || view.identity().consensus != ConsensusType::Pos
        || !view.identity().matches(&execution.profile.network)
        || view.identity().genesis != execution.rules.genesis_hash
    {
        return Err(repository(super::Error::Identity));
    }
    let header = view
        .require_block(input.parent)
        .map_err(repository)?
        .header();
    let key_hash = match &header {
        Header::Key(_) => input.parent,
        Header::Micro(m) => Hash256(m.common.previous_key_hash),
    };
    let Block::Key(key) = view.require_block(key_hash).map_err(repository)? else {
        return Err(repository(super::Error::Corrupt("HC candidate leader key")));
    };
    let height = header
        .common()
        .height
        .checked_add(u64::from(matches!(&header, Header::Key(_))))
        .ok_or_else(|| repository(super::Error::ResourceLimit))?;
    let mut profile = execution.profile.as_ref().clone();
    profile.network = view
        .protocol_profile(&profile.network, height, execution.limits.history)
        .map_err(repository)?;
    let reader = execution
        .reader(view.clone(), Arc::new(profile))
        .map_err(repository)?;
    let temporary = candidate::key_template::<H::Error, Overlay<S>, Overlay<S>>(
        &header,
        &key,
        &reader.profile.network,
        input.production,
    )
    .map_err(Error::Candidate)?;
    let execution_history = Arc::new(
        super::super::vm_history(view, &temporary, execution.limits.history).map_err(repository)?,
    );
    let parent_view = reader
        .view(&chain::RunEnv::Hash(input.parent.0.to_vec()))
        .map_err(Error::Query)?;
    let record = view.require_record(input.parent).map_err(repository)?;
    let history = reward_history(
        view,
        key_hash,
        height,
        &execution.rules,
        execution.limits.history,
    )
    .map_err(repository)?;
    let parent = hc::Parent {
        header: &header,
        key: &key,
        view: &parent_view,
        fees: &record.fees,
        execution_history,
    };
    let candidate = candidate::Key::prepare(
        &parent,
        &history,
        &reader,
        state,
        host,
        &execution.rules,
        input.production,
    )
    .map_err(Error::Candidate)?;
    Ok(PreparedKey {
        candidate,
        reader,
        status: view.status(),
        source_id: view.source().source_id(),
    })
}
