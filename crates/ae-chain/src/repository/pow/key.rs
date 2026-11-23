// PoW key candidates retain all state, history and target inputs from one view.
use crate::{
    execution,
    key::{self as transition, candidate},
    repository::{self as repo, Identity, Read, Status, View, ledger},
};
use ae_codec::headers::{Header, KeyHeader};
use ae_consensus::target::{DIFFICULTY_WINDOW, Target};
use ae_execution::transactions;
use ae_protocol::ConsensusType;
use ae_trie::{Overlay, OverlayLimits};
use ae_types::Hash256;
use ae_vm_fate::maps::MapOrder;
use std::sync::Arc;

#[derive(Clone, Copy)]
pub struct Input {
    pub parent: Hash256,
    pub production: candidate::Input,
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub history: usize,
    pub overlay: OverlayLimits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            history: 1_024,
            overlay: Default::default(),
        }
    }
}
#[derive(Debug)]
pub enum Error<S> {
    Repository(repo::Error),
    BlockNotFound(Hash256),
    AdjustmentHistory(repo::Error),
    Candidate(candidate::Error<Overlay<S>, Overlay<S>>),
}
pub struct Prepared<S> {
    pub candidate: candidate::Candidate<Overlay<S>, Overlay<S>>,
    pub view: View<S>,
    pub status: Status,
    pub source_id: u64,
    adjustment: Vec<KeyHeader>,
    initial_target: Target,
}
impl<S: Read> Prepared<S> {
    pub fn finish(&self, time: u64) -> Result<KeyHeader, Error<S>> {
        self.candidate
            .finish(time, self.initial_target, &self.adjustment)
            .map_err(Error::Candidate)
    }
}
pub fn prepare<S: Read, O: MapOrder>(
    view: &View<S>,
    expected: &Identity,
    execution: &transactions::Config<'_, O>,
    rules: &transition::Rules,
    input: Input,
    limits: Limits,
) -> Result<Prepared<S>, Error<S>> {
    let repository = Error::Repository;
    if view.identity() != expected
        || expected.consensus != ConsensusType::Pow
        || execution.consensus != ConsensusType::Pow
        || !expected.matches(execution.network)
        || expected.genesis != rules.genesis_hash
    {
        return Err(repository(repo::Error::Identity));
    }
    if limits.history == 0 {
        return Err(repository(repo::Error::ResourceLimit));
    }
    let previous = view
        .block(input.parent)
        .map_err(repository)?
        .ok_or(Error::BlockNotFound(input.parent))?
        .header();
    let key_hash = match &previous {
        Header::Key(_) => input.parent,
        Header::Micro(m) => Hash256(m.common.previous_key_hash),
    };
    let key = ledger::require_key(view, key_hash).map_err(repository)?;
    let height = previous
        .common()
        .height
        .checked_add(1)
        .ok_or_else(|| repository(repo::Error::ResourceLimit))?;
    let network = view
        .protocol_profile(execution.network, height, limits.history)
        .map_err(repository)?;
    // The original builder looks up target history before calculating state.
    let mut adjustment = Vec::new();
    if previous.common().height >= (DIFFICULTY_WINDOW + 1) as u64 {
        if limits.history < DIFFICULTY_WINDOW + 1 {
            return Err(Error::AdjustmentHistory(repo::Error::ResourceLimit));
        }
        let mut next = key_hash;
        let mut expected_height = previous.common().height;
        for _ in 0..DIFFICULTY_WINDOW + 1 {
            let k = ledger::require_key(view, next).map_err(Error::AdjustmentHistory)?;
            if k.common.height != expected_height {
                return Err(Error::AdjustmentHistory(repo::Error::Corrupt(
                    "PoW target history height",
                )));
            }
            next = Hash256(k.common.previous_key_hash);
            expected_height = expected_height.saturating_sub(1);
            adjustment.push(k);
        }
    }
    let temporary = candidate::Candidate::<Overlay<S>, Overlay<S>>::template(
        &previous,
        &key,
        &network,
        input.production,
    )
    .map_err(Error::Candidate)?;
    let state = view
        .insertion_trees(input.parent, true, limits.overlay)
        .map_err(repository)?;
    let record = view.require_record(input.parent).map_err(repository)?;
    let history =
        ledger::reward_history(view, key_hash, height, rules.reward_delay, limits.history)
            .map_err(repository)?;
    let vm_history = if execution.fate.is_some() || execution.aevm.is_some() {
        Arc::new(repo::vm_history(view, &temporary, limits.history).map_err(repository)?)
    } else {
        Arc::default()
    };
    let parent = execution::Parent {
        headers: ae_consensus::headers::Context {
            previous: &previous,
            previous_key: &key,
            recent_keys: &adjustment,
        },
        previous_generation: execution::PreviousGeneration::Unavailable,
        fees: crate::rewards::number(&record.fees),
        reported_fraud: record.fraud,
    };
    let candidate = repo::history::with_history(execution, vm_history, &network, |config| {
        candidate::Candidate::prepare(&state, &parent, &history, config, rules, input.production)
    })
    .map_err(Error::Candidate)?;
    let initial_target = Target(
        ledger::require_key(view, expected.genesis)
            .map_err(repository)?
            .target,
    );
    Ok(Prepared {
        candidate,
        view: view.clone(),
        status: view.status(),
        source_id: view.source().source_id(),
        adjustment,
        initial_target,
    })
}
