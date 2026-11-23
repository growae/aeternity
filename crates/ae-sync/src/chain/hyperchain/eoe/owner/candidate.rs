// One fixed micro build: selected-state vote results, shared-pool packing,
// then native ordered vote application with one candidate-tree nonce refresh.
use super::{Completion, Owner, Request, RequestError, effects};
use crate::chain::hyperchain::{delivery, packing, scoped};
use ae_chain::{
    hyperchain::candidate as core,
    repository::{
        self as repo,
        hyperchain::{candidate as build, inputs},
    },
};
use ae_storage::{hyperchain::candidate as owned, pool::worker};
use ae_types::Hash256;
use ae_vm_fate::maps::MapOrder;
use num_bigint::BigInt;
use std::sync::Arc;

pub struct Input {
    pub parent: Hash256,
    pub limits: core::Limits,
    pub pins: Arc<inputs::Pins>,
    pub inputs: inputs::Limits,
    pub timeout: core::Timeout,
    pub window_ms: i64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Binding,
    Closed,
    Cancelled,
    Owner(super::Error),
    Request(RequestError),
    Source(crate::chain::Error),
    Repository(repo::Error),
    Votes(core::VoteError),
    Packing(packing::Error),
    Effects(delivery::Error),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    Waiting,
    Requested { refresh: bool },
    Answered { refresh: bool },
    Prepared,
    Effects(delivery::Progress),
    Packing(packing::Progress),
    Vote(core::VoteProgress),
    Complete,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Initial,
    AwaitInitial,
    Prepare,
    Create,
    Effects,
    StartPacking,
    Pack,
    Apply,
    Refresh,
    AwaitRefresh,
    Refreshed,
    Done,
}
pub struct Build<O> {
    phase: Phase,
    source: scoped::Producer<O>,
    client: super::Client,
    pool: worker::Client,
    input: Input,
    request: Option<Request>,
    answer: Option<Completion>,
    votes: Option<core::Votes>,
    candidate: Option<owned::Prepared<O>>,
    packing: Option<packing::Packing<O>>,
    effects: Option<effects::Pending>,
    reserved: BigInt,
    refreshes: u8,
    is_eoe: bool,
    failure: Option<Error>,
}
impl<O> std::fmt::Debug for Build<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Build")
            .field("phase", &self.phase)
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}
pub struct Completed<O> {
    pub candidate: owned::Prepared<O>,
    pub is_eoe: bool,
    pub reserved_gas: BigInt,
    pub refreshes: u8,
    pub acknowledged_effects: u64,
}
impl Owner {
    pub fn micro_candidate<O: MapOrder + Clone>(
        &self,
        source: scoped::Producer<O>,
        pool: worker::Client,
        input: Input,
    ) -> Result<Build<O>, Error> {
        self.ready().map_err(Error::Owner)?;
        let database = self.source.database.upgrade().ok_or(Error::Closed)?;
        if !source.uses_database(&database)
            || !pool.uses_database(&database)
            || self.parent.is_none()
        {
            return Err(Error::Binding);
        }
        source.check().map_err(Error::Source)?;
        Ok(Build {
            phase: Phase::Initial,
            source,
            client: self.client(),
            pool,
            input,
            request: None,
            answer: None,
            votes: None,
            candidate: None,
            packing: None,
            effects: None,
            reserved: 0.into(),
            refreshes: 0,
            is_eoe: false,
            failure: None,
        })
    }
}
pub(super) fn retryable(error: &Error) -> bool {
    match error {
        Error::Owner(
            super::Error::Backpressured
            | super::Error::ResourceLimit
            | super::Error::State(ae_hyperchain::eoe::local_host::SelectedError::ResourceLimit),
        ) => true,
        Error::Request(RequestError::Rejected(
            ae_hyperchain::eoe::coordinator::Error::ResourceLimit,
        )) => true,
        Error::Source(
            crate::chain::Error::Storage(repo::Error::Conflict | repo::Error::ResourceLimit)
            | crate::chain::Error::Validation {
                operational: true, ..
            },
        ) => true,
        Error::Repository(repo::Error::Conflict | repo::Error::ResourceLimit)
        | Error::Votes(core::VoteError::ResourceLimit) => true,
        Error::Packing(e) => packing::retryable(e),
        Error::Effects(e) => super::producer::retryable(e),
        _ => false,
    }
}
impl<O: MapOrder + Clone> Build<O> {
    pub fn ready(&self) -> bool {
        self.phase == Phase::Done && self.failure.is_none()
    }
    pub fn failure(&self) -> Option<&Error> {
        self.failure.as_ref()
    }
    pub fn candidate(&self) -> Option<&owned::Prepared<O>> {
        self.candidate.as_ref()
    }
    pub fn reserved_gas(&self) -> &BigInt {
        &self.reserved
    }
    pub fn refreshes(&self) -> u8 {
        self.refreshes
    }
    pub fn pending_effects(&self) -> Option<&std::collections::VecDeque<inputs::Effect>> {
        self.effects.as_ref().map(|p| &p.pending)
    }
    pub fn acknowledged_effects(&self) -> u64 {
        self.effects.as_ref().map_or(0, |p| p.acknowledged)
    }
    pub fn effect_in_flight(&self) -> bool {
        self.effects.as_ref().is_some_and(|p| p.in_flight())
    }
    pub fn cancel(&mut self) {
        if let Some(request) = &self.request {
            request.cancel();
        }
        self.failure = Some(Error::Cancelled);
    }
    pub(super) fn retire(mut self) -> Retiring {
        self.cancel();
        // An already delivered completion is pure candidate input. Dropping it
        // releases reply credits; no ledger or pool action can follow it.
        self.answer = None;
        Retiring {
            request: self.request.take(),
            packing: self.packing.take().map(packing::Packing::cancel),
            effects: self.effects.take(),
            report: Cancelled::default(),
            failure: None,
            eoe_retired: false,
        }
    }
    pub fn retry(&mut self) -> Result<(), Error> {
        if let Some(e) = &self.failure
            && !retryable(e)
        {
            return Err(e.clone());
        }
        if let Some(packing) = &mut self.packing {
            packing.retry().map_err(Error::Packing)?;
        }
        self.failure = None;
        Ok(())
    }
    // Drive Owner::step independently and handle its supervision outputs. No
    // storage cache mutex spans either EOE or ordinary-pool request/receipt.
    pub fn step(&mut self, owner: &mut Owner, now: i128) -> Result<Progress, Error> {
        if let Some(e) = &self.failure {
            return Err(e.clone());
        }
        let result = self.advance(owner, now);
        if let Err(e) = &result {
            self.failure = Some(e.clone());
        }
        result
    }
    fn advance(&mut self, owner: &mut Owner, now: i128) -> Result<Progress, Error> {
        if !self.client.same_runtime(&owner.client()) {
            return Err(Error::Binding);
        }
        if self.client.is_closed() || !self.pool.info().accepting {
            return Err(Error::Closed);
        }
        owner.ready().map_err(Error::Owner)?;
        self.source.check().map_err(Error::Source)?;
        match self.phase {
            Phase::Initial | Phase::Refresh => {
                let refresh = self.phase == Phase::Refresh;
                let request = if refresh {
                    owner.finalize_candidate(self.candidate.as_ref().expect("packed candidate"))
                } else {
                    owner.finalize()
                }
                .map_err(Error::Owner)?;
                self.request = Some(request);
                self.phase = if refresh {
                    Phase::AwaitRefresh
                } else {
                    Phase::AwaitInitial
                };
                if refresh {
                    self.refreshes += 1;
                }
                Ok(Progress::Requested { refresh })
            }
            Phase::AwaitInitial | Phase::AwaitRefresh => {
                let refresh = self.phase == Phase::AwaitRefresh;
                let result = self.request.as_mut().expect("pending request").try_reply();
                match result {
                    Ok(None) => Ok(Progress::Waiting),
                    Ok(Some(answer)) => {
                        self.answer = Some(answer);
                        self.request = None;
                        self.phase = if refresh {
                            Phase::Refreshed
                        } else {
                            Phase::Prepare
                        };
                        Ok(Progress::Answered { refresh })
                    }
                    Err(error) => {
                        let error = Error::Request(error);
                        if retryable(&error) {
                            self.request = None;
                            self.phase = if refresh {
                                self.refreshes -= 1;
                                Phase::Refresh
                            } else {
                                Phase::Initial
                            };
                        }
                        Err(error)
                    }
                }
            }
            Phase::Prepare => {
                let votes = core::Votes::new(
                    self.answer.as_ref().expect("initial result").replies(),
                    self.input.limits.signed_bytes,
                )
                .map_err(Error::Votes)?;
                let captured = owner
                    .source
                    .snapshots()
                    .view()
                    .map_err(|e| Error::Owner(super::Error::State(super::state_error(e))))?;
                let parent = captured
                    .value
                    .block(self.input.parent)
                    .map_err(Error::Repository)?;
                let reserved = match parent {
                    Some(block) => {
                        let header = block.header();
                        let protocol = ae_protocol::Protocol::try_from(header.common().version)
                            .map_err(|e| Error::Repository(repo::Error::Protocol(e)))?;
                        votes.reserved_gas(header.common().height, protocol)
                    }
                    None => 0.into(),
                };
                self.reserved = reserved;
                self.votes = Some(votes);
                self.answer = None;
                self.phase = Phase::Create;
                Ok(Progress::Answered { refresh: false })
            }
            Phase::Create => {
                let parent = owner.parent.as_ref().ok_or(Error::Binding)?;
                if parent.is_closed() {
                    return Err(Error::Closed);
                }
                let request = owned::Request {
                    binding: owner.producer_binding.clone(),
                    input: owned::Input::Micro(build::Input {
                        parent: self.input.parent,
                        now,
                        limits: self.input.limits,
                    }),
                    parent: parent.snapshot().cache,
                    pins: self.input.pins.clone(),
                    limits: self.input.inputs,
                };
                let mut candidate = self
                    .source
                    .candidate(owner.source.snapshots(), request, || {
                        if self.client.is_closed() || parent.is_closed() {
                            Err(crate::chain::Error::Closed)
                        } else {
                            Ok(())
                        }
                    })
                    .map_err(Error::Source)?;
                // Native entropy reads notify EOE before pool packing and
                // the later candidate-tree vote refresh. Even a failed
                // creation may have an observable successful effect prefix.
                self.effects = Some(effects::Pending::new(
                    &mut candidate.effects,
                    parent.clone(),
                    self.client.clone(),
                ));
                self.phase = Phase::Effects;
                self.candidate = Some(candidate);
                Ok(Progress::Prepared)
            }
            Phase::Effects => {
                let effects = self.effects.as_mut().expect("candidate effects");
                let progress = effects.step().map_err(Error::Effects)?;
                if effects.ready() {
                    self.phase = if self.candidate.as_ref().expect("prepared").outcome.is_ok() {
                        Phase::StartPacking
                    } else {
                        Phase::Done
                    };
                }
                Ok(Progress::Effects(progress))
            }
            Phase::StartPacking => {
                let candidate = self.candidate.take().expect("prepared candidate");
                match packing::Packing::start(
                    candidate,
                    self.pool.clone(),
                    packing::Config {
                        reserved_gas: self.reserved.clone(),
                        timeout: self.input.timeout,
                        window_ms: self.input.window_ms,
                    },
                ) {
                    Ok(packing) => {
                        self.packing = Some(packing);
                        self.phase = Phase::Pack;
                        Ok(Progress::Packing(packing::Progress::Waiting))
                    }
                    Err(error) => {
                        self.candidate = Some(error.input);
                        Err(Error::Packing(error.error))
                    }
                }
            }
            Phase::Pack => {
                let progress = self
                    .packing
                    .as_mut()
                    .expect("packing")
                    .step()
                    .map_err(Error::Packing)?;
                if self.packing.as_ref().expect("packing").ready() {
                    match self.packing.take().expect("packing").finish() {
                        Ok(candidate) => {
                            self.candidate = Some(candidate);
                            self.phase = Phase::Apply;
                        }
                        Err(error) => {
                            self.packing = Some(error.pending);
                            return Err(Error::Packing(error.error));
                        }
                    }
                }
                Ok(Progress::Packing(progress))
            }
            Phase::Apply => {
                let votes = self.votes.as_mut().expect("vote results");
                let owned::Candidate::Micro(prepared) = self
                    .candidate
                    .as_mut()
                    .expect("packed candidate")
                    .outcome
                    .as_mut()
                    .expect("successful candidate")
                else {
                    return Err(Error::Binding);
                };
                let progress = votes
                    .apply::<(), _, _, _, _>(&mut prepared.candidate, &prepared.reader)
                    .map_err(|e| Error::Packing(packing::execution(e)))?;
                if let Some(eoe) = votes.outcome() {
                    self.is_eoe = eoe;
                    self.phase = Phase::Done;
                } else if votes.needs_refresh() {
                    self.phase = Phase::Refresh;
                }
                Ok(Progress::Vote(progress))
            }
            Phase::Refreshed => {
                self.votes
                    .as_mut()
                    .expect("votes")
                    .refresh(self.answer.as_ref().expect("refresh reply").replies())
                    .map_err(Error::Votes)?;
                self.answer = None;
                self.phase = Phase::Apply;
                Ok(Progress::Answered { refresh: true })
            }
            Phase::Done => Ok(Progress::Complete),
        }
    }
    pub fn finish(self) -> Result<Completed<O>, Box<Self>> {
        if !self.ready() {
            return Err(Box::new(self));
        }
        Ok(Completed {
            candidate: self.candidate.expect("completed candidate"),
            is_eoe: self.is_eoe,
            reserved_gas: self.reserved,
            refreshes: self.refreshes,
            acknowledged_effects: self.effects.expect("completed effects").acknowledged,
        })
    }
}

#[derive(Debug, Default)]
pub struct Cancelled {
    pub finalization: Option<Result<(), RequestError>>,
    pub packing: Option<packing::CancelReport>,
    pub discarded_effects: std::collections::VecDeque<inputs::Effect>,
    pub acknowledged_effects: u64,
    pub effect_error: Option<delivery::Error>,
}
pub(super) struct Retiring {
    request: Option<Request>,
    packing: Option<packing::Cancelled>,
    effects: Option<effects::Pending>,
    report: Cancelled,
    failure: Option<Error>,
    eoe_retired: bool,
}
impl Retiring {
    pub(super) fn retire_pool(&mut self, old: &worker::Client) -> Result<bool, Error> {
        let matched = self
            .packing
            .as_mut()
            .map(|p| p.retire_pool(old))
            .transpose()
            .map_err(Error::Packing)?
            .unwrap_or(false);
        if matched && self.failure.as_ref().is_some_and(pool_departure) {
            self.failure = None;
        }
        Ok(matched)
    }
    pub(super) fn retire_eoe(&mut self, old: &super::Client) -> Result<(), Error> {
        if let Some(effects) = &mut self.effects
            && let Some(error) = effects.retire_eoe(old).map_err(Error::Effects)?
        {
            self.report.effect_error = Some(error);
        }
        self.eoe_retired = true;
        Ok(())
    }
    pub(super) fn retire_cache(
        &mut self,
        old: &ae_hyperchain::parent_cache::runtime::Client,
    ) -> Result<(), Error> {
        if let Some(effects) = &mut self.effects
            && let Some(error) = effects.retire_cache(old).map_err(Error::Effects)?
        {
            self.report.effect_error = Some(error);
        }
        Ok(())
    }
    pub fn step(&mut self) -> Result<bool, Error> {
        if let Some(e) = &self.failure {
            return Err(e.clone());
        }
        let result = self.advance();
        if let Err(e) = &result {
            self.failure = Some(e.clone());
        }
        result
    }
    fn advance(&mut self) -> Result<bool, Error> {
        if let Some(request) = &mut self.request {
            match request.try_reply() {
                Ok(None) => return Ok(false),
                Ok(Some(_completion)) => self.report.finalization = Some(Ok(())),
                Err(RequestError::Unknown) if !self.eoe_retired => {
                    return Err(Error::Request(RequestError::Unknown));
                }
                Err(e) => self.report.finalization = Some(Err(e)),
            }
            self.request = None;
        }
        if let Some(packing) = &mut self.packing {
            if !packing.step().map_err(Error::Packing)? {
                return Ok(false);
            }
            self.report.packing = Some(self.packing.take().expect("settled packing").finish());
        }
        if let Some(effects) = &mut self.effects {
            match effects.settle() {
                Ok(false) => return Ok(false),
                Ok(true) => (),
                Err(e) if super::producer::retryable(&e) && !effects.in_flight() => {
                    self.report.effect_error = Some(e)
                }
                Err(e) => return Err(Error::Effects(e)),
            }
            self.report.acknowledged_effects = effects.acknowledged;
            self.report.discarded_effects = std::mem::take(&mut effects.pending);
            self.effects = None;
        }
        Ok(true)
    }
    pub fn finish(self) -> Cancelled {
        assert!(
            self.request.is_none()
                && self.packing.is_none()
                && self.effects.is_none()
                && self.failure.is_none()
        );
        self.report
    }
}

pub(super) fn pool_departure(error: &Error) -> bool {
    matches!(
        error,
        Error::Packing(packing::Error::Pool {
            error: worker::Error::Stopped | worker::Error::Disconnected,
            ..
        })
    )
}
