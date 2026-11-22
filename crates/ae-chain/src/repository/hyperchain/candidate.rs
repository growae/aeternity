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
    let reader = super::reader(
        view.clone(),
        &execution.identity,
        execution.profile.clone(),
        super::Config {
            overlays: execution.limits.overlay,
            maximum_history_headers: execution.limits.history,
            configured_block_gas_limit: execution.rules.configured_gas_limit.clone(),
            aevm: execution.aevm.clone(),
        },
    )
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
