// aec_block_micro_candidate:get_blocks/create_micro_block_env, fa2a6d7.
// Copyright (c) Aeternity. ISC: repository LICENSE.
pub use super::execution::{Config as Execution, Profile, Reader, Snapshot};
use super::{Identity, Read, Status, View};
use crate::{block::Block, hyperchain::candidate};
use ae_codec::headers::Header;
use ae_hyperchain::chain;
use ae_protocol::ConsensusType;
use ae_trie::Overlay;
use ae_types::Hash256;
use ae_vm_fate::maps::MapOrder;
use std::{convert::Infallible, sync::Arc};

pub mod key;

#[derive(Clone)]
pub struct Config<O> {
    pub identity: Identity,
    pub profile: Arc<Profile<O>>,
    pub execution: Execution,
    pub micro_cycle_ms: u64,
}
#[derive(Clone, Copy)]
pub struct Input {
    pub parent: Hash256,
    pub now: i128,
    pub limits: candidate::Limits,
}
#[derive(Debug)]
pub enum Error<S> {
    Repository(super::Error),
    Query(chain::Error<Overlay<S>, Overlay<S>>),
    Candidate(candidate::Error<Infallible, Overlay<S>, Overlay<S>>),
}
pub struct Prepared<S, O> {
    pub candidate: candidate::Micro<Overlay<S>, Overlay<S>>,
    pub reader: Reader<S, O>,
    pub status: Status,
    pub source_id: u64,
}

pub fn reader<S: Read, O>(
    view: View<S>,
    expected: &Identity,
    profile: Arc<Profile<O>>,
    config: Execution,
) -> Result<Reader<S, O>, super::Error> {
    super::execution::reader(view, expected, ConsensusType::Pow, profile, config)
}

// Every input comes from this same immutable view, including the selected
// history used by historical VMs. A later top never substitutes input.parent.
pub fn micro<S: Read, O: MapOrder + Clone>(
    view: &View<S>,
    config: &Config<O>,
    input: Input,
) -> Result<Prepared<S, O>, Error<S>> {
    let repository = Error::Repository;
    if view.identity() != &config.identity
        || view.identity().consensus != ConsensusType::Pow
        || !view.identity().matches(&config.profile.network)
    {
        return Err(repository(super::Error::Identity));
    }
    let header = view
        .require_block(input.parent)
        .map_err(repository)?
        .header();
    let key = match &header {
        Header::Key(key) => key.clone(),
        Header::Micro(micro) => {
            let Block::Key(key) = view
                .require_block(Hash256(micro.common.previous_key_hash))
                .map_err(repository)?
            else {
                return Err(repository(super::Error::Corrupt(
                    "PoW candidate leader key",
                )));
            };
            key
        }
    };
    let mut profile = config.profile.as_ref().clone();
    profile.network = view
        .protocol_profile(
            &profile.network,
            key.common.height,
            config.execution.maximum_history_headers,
        )
        .map_err(repository)?;
    let reader = reader(
        view.clone(),
        &config.identity,
        Arc::new(profile),
        config.execution.clone(),
    )
    .map_err(repository)?;
    let parent_view = reader
        .view(&chain::RunEnv::Hash(input.parent.0.to_vec()))
        .map_err(Error::Query)?;
    // Missing/corrupt reporting metadata is never treated as "not reported".
    let record = view.require_record(input.parent).map_err(repository)?;
    let proof = view
        .discovered_proof(Hash256(key.common.previous_key_hash))
        .map_err(repository)?
        .map(Box::new);
    let candidate = candidate::Micro::begin_pow(
        candidate::PowParent {
            header: &header,
            key: &key,
            view: &parent_view,
            reported_fraud: record.fraud,
        },
        &reader,
        &config.execution.configured_block_gas_limit,
        candidate::PowStart {
            now: input.now,
            micro_cycle_ms: config.micro_cycle_ms,
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
