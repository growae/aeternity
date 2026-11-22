// aec_conductor:hc_next_producer/hc_create_block, aeternity fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC: repository LICENSE.
use super::{Owner, candidate, effects, producer};
mod retirement;
use crate::chain::{
    self, Importer,
    hyperchain::{delivery, packing, scoped},
};
use ae_chain::{
    block::Block,
    hyperchain::candidate as core,
    repository::{
        self as repo,
        hyperchain::{candidate as build, inputs},
    },
};
use ae_hyperchain::{producer as decision, signing};
use ae_storage::{hyperchain::candidate as owned, pool::worker};
use ae_types::{AccountPublicKey, Hash256};
use ae_vm_fate::{maps::MapOrder, value::ObjectPayload};
use num_bigint::BigInt;
pub use retirement::{Retiring, Stopped};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone)]
pub struct Config {
    pub producer: producer::Config,
    pub micro: core::Limits,
    pub inputs: inputs::Limits,
    pub pins: Arc<inputs::Pins>,
    pub timeout: core::Timeout,
    pub release_info: u32,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            producer: Default::default(),
            micro: Default::default(),
            inputs: Default::default(),
            pins: Arc::new(Default::default()),
            timeout: core::Timeout::Disabled,
            release_info: 0,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Busy,
    Binding,
    Closed,
    Cancelled,
    Time,
    InvalidLeader,
    HoleCount(BigInt),
    UnexpectedReceipt,
    Owner(super::Error),
    Build(candidate::Error),
    Source(chain::Error),
    Decision(repo::hyperchain::producer::Failure),
    Key { operational: bool, reason: String },
    Effects(delivery::Error),
    Signing(signing::Error),
    Encoding(ae_chain::block::Error),
    Admission(chain::Failure),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Micro,
    Key,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    DecisionRequested,
    Waiting,
    Decided,
    Awoke,
    Micro(candidate::Progress),
    MicroSkipped,
    Signed(Kind),
    Submitted(Kind),
    Accepted(Kind),
    KeyPrepared,
    Effects(delivery::Progress),
    Complete,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Skipped {
    Empty,
    Failed { reason: String },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Wait(decision::Wait),
    RetryNotInCache,
    Produced {
        micro: Option<chain::Accepted>,
        key: chain::Accepted,
        is_eoe: bool,
        hole: bool,
    },
}
#[derive(Debug)]
pub struct Completed {
    pub outcome: Outcome,
    pub original_parent: Hash256,
    pub parent: Hash256,
    pub decisions: u64,
    pub acknowledged_effects: u64,
    pub skipped: Option<Skipped>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Decide,
    Decision,
    Wait,
    MicroStart,
    Micro,
    SignMicro,
    Key,
    KeyEffects,
    FinishKey,
    Seal,
    Submit(Kind),
    Receipt(Kind),
    Done,
}
#[derive(Debug, Clone, Copy)]
enum Wake {
    Done,
    Reconsider,
    Micro,
    FinishKey,
    Seal,
}
struct Lease(Arc<AtomicBool>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
// The caller drives Owner::step and routes all supervision outputs separately.
// This owns one fixed attempt, including unread admission receipts on failure.
// It does not keep a retired importer alive or replace accepted-effect delivery.
pub struct Attempt<O> {
    _lease: Lease,
    source: scoped::Producer<O>,
    client: super::Client,
    pool: worker::Client,
    config: Config,
    phase: Phase,
    wake: Wake,
    deadline: Option<Instant>,
    tick: Option<Instant>,
    original_parent: Hash256,
    parent: Hash256,
    decision: Option<producer::Decision>,
    decisions: u64,
    leader: Option<AccountPublicKey>,
    keys: Option<signing::Keys>,
    hole: bool,
    is_eoe: bool,
    micro: Option<candidate::Build<O>>,
    completed_micro: Option<candidate::Completed<O>>,
    key: Option<owned::Prepared<O>>,
    effects: Option<effects::Pending>,
    acknowledged: u64,
    block: Option<Block>,
    receipt: Option<chain::Ticket>,
    accepted_micro: Option<chain::Accepted>,
    skipped: Option<Skipped>,
    outcome: Option<Outcome>,
    failure: Option<Error>,
}
impl<O> std::fmt::Debug for Attempt<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Attempt")
            .field("phase", &self.phase)
            .field("parent", &self.parent)
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}
impl Owner {
    pub fn begin_production<O: MapOrder + Clone>(
        &mut self,
        source: scoped::Producer<O>,
        pool: worker::Client,
        config: Config,
    ) -> Result<Attempt<O>, Error> {
        self.ready().map_err(Error::Owner)?;
        let db = self.source.database.upgrade().ok_or(Error::Closed)?;
        if !source.uses_database(&db) || !pool.uses_database(&db) || self.parent.is_none() {
            return Err(Error::Binding);
        }
        if !pool.info().accepting || self.parent.as_ref().is_some_and(|p| p.is_closed()) {
            return Err(Error::Closed);
        }
        source.check().map_err(Error::Source)?;
        i64::try_from(&config.producer.production_time).map_err(|_| Error::Time)?;
        let parent = db
            .view()
            .map_err(|e| Error::Source(e.into()))?
            .status()
            .tip
            .ok_or(Error::Closed)?;
        if self.production.is_some()
            || self
                .attempt
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return Err(Error::Busy);
        }
        Ok(Attempt {
            _lease: Lease(self.attempt.clone()),
            source,
            client: self.client(),
            pool,
            config,
            phase: Phase::Decide,
            wake: Wake::Done,
            deadline: None,
            tick: None,
            original_parent: parent,
            parent,
            decision: None,
            decisions: 0,
            leader: None,
            keys: None,
            hole: false,
            is_eoe: false,
            micro: None,
            completed_micro: None,
            key: None,
            effects: None,
            acknowledged: 0,
            block: None,
            receipt: None,
            accepted_micro: None,
            skipped: None,
            outcome: None,
            failure: None,
        })
    }
}
fn source_retryable(e: &chain::Error) -> bool {
    matches!(
        e,
        chain::Error::Busy
            | chain::Error::Storage(repo::Error::Conflict | repo::Error::ResourceLimit)
            | chain::Error::Validation {
                operational: true,
                ..
            }
    )
}
fn retryable(e: &Error) -> bool {
    match e {
        Error::Owner(super::Error::ProductionSource(e)) => source_retryable(e),
        Error::Owner(super::Error::Producer(e)) => producer::retryable(e),
        Error::Owner(e) => candidate::retryable(&candidate::Error::Owner(e.clone())),
        Error::Build(e) => candidate::retryable(e),
        Error::Source(e) => source_retryable(e),
        Error::Effects(e) => producer::retryable(e),
        Error::Admission(f) => {
            f.progress.as_ref().is_some_and(|r| r.accepted.is_empty()) && source_retryable(&f.error)
        }
        _ => false,
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Attempt<O> {
    pub fn ready(&self) -> bool {
        self.phase == Phase::Done && self.failure.is_none()
    }
    pub fn failure(&self) -> Option<&Error> {
        self.failure.as_ref()
    }
    pub fn decision(&self) -> Option<&producer::Decision> {
        self.decision.as_ref()
    }
    pub fn parent(&self) -> Hash256 {
        self.parent
    }
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
    pub fn pending_block(&self) -> Option<&Block> {
        self.block.as_ref()
    }
    pub fn admission_in_flight(&self) -> bool {
        self.receipt.is_some()
    }
    // A terminally stopped attempt may transfer its unread receipt to the
    // lifecycle owner for reconciliation. Its sticky failure still forbids
    // retry, so observing an abandoned result can never resubmit this block.
    pub fn take_abandoned_admission(&mut self) -> Option<chain::Ticket> {
        if self.failure.as_ref().is_some_and(|e| !retryable(e)) {
            self.receipt.take()
        } else {
            None
        }
    }
    pub fn accepted_micro(&self) -> Option<&chain::Accepted> {
        self.accepted_micro.as_ref()
    }
    pub fn key_candidate(&self) -> Option<&owned::Prepared<O>> {
        self.key.as_ref()
    }
    pub fn cancel(&mut self) {
        if let Some(micro) = &mut self.micro {
            micro.cancel();
        }
        if let Some(receipt) = &self.receipt {
            receipt.cancel();
        }
        self.failure = Some(Error::Cancelled);
    }
    pub fn retry(&mut self) -> Result<(), Error> {
        if let Some(e) = &self.failure
            && !retryable(e)
        {
            return Err(e.clone());
        }
        if let Some(micro) = &mut self.micro {
            micro.retry().map_err(Error::Build)?;
        }
        self.failure = None;
        Ok(())
    }
    fn sleep(&mut self, now: Instant, after: &BigInt, wake: Wake) -> Result<(), Error> {
        let ms = if after <= &0.into() {
            0
        } else {
            u64::try_from(after).map_err(|_| Error::Time)?
        };
        self.deadline = Some(
            now.checked_add(Duration::from_millis(ms))
                .ok_or(Error::Time)?,
        );
        self.wake = wake;
        self.phase = Phase::Wait;
        Ok(())
    }
    pub fn step(
        &mut self,
        owner: &mut Owner,
        importer: &Importer<O>,
        wall_ms: u64,
        now: Instant,
    ) -> Result<Progress, Error> {
        if let Some(e) = &self.failure {
            return Err(e.clone());
        }
        let result = self.advance(owner, importer, wall_ms, now);
        if let Err(e) = &result {
            self.failure = Some(e.clone());
        }
        result
    }
    fn advance(
        &mut self,
        owner: &mut Owner,
        importer: &Importer<O>,
        wall_ms: u64,
        now: Instant,
    ) -> Result<Progress, Error> {
        if !self.client.same_runtime(&owner.client())
            || !self.source.uses_importer(importer)
            || !Arc::ptr_eq(&self._lease.0, &owner.attempt)
        {
            return Err(Error::Binding);
        }
        owner.ready().map_err(Error::Owner)?;
        self.source.check().map_err(Error::Source)?;
        if !self.pool.info().accepting || owner.parent.as_ref().is_none_or(|p| p.is_closed()) {
            return Err(Error::Closed);
        }
        if self.tick.is_some_and(|last| now < last) {
            return Err(Error::Time);
        }
        self.tick = Some(now);
        match self.phase {
            Phase::Decide => {
                owner
                    .prepare_producer_inner(
                        &self.source,
                        producer::Input {
                            now: wall_ms.into(),
                            config: self.config.producer.clone(),
                        },
                    )
                    .map_err(Error::Owner)?;
                self.phase = Phase::Decision;
                Ok(Progress::DecisionRequested)
            }
            Phase::Decision => {
                let Some(p) = owner.production() else {
                    return Err(Error::Binding);
                };
                if !p.ready() {
                    return Ok(Progress::Waiting);
                }
                self.acknowledged = self.acknowledged.saturating_add(p.acknowledged());
                let d = owner.take_production_inner().ok_or(Error::Binding)?;
                let result = d.outcome.clone();
                self.decision = Some(d);
                self.decisions = self.decisions.saturating_add(1);
                match result.map_err(Error::Decision)? {
                    decision::Decision::Wait { reason, after } => {
                        self.outcome = Some(Outcome::Wait(reason));
                        self.sleep(now, &after, Wake::Done)?;
                    }
                    decision::Decision::RetryNotInCache { after } => {
                        self.outcome = Some(Outcome::RetryNotInCache);
                        self.sleep(now, &after, Wake::Done)?;
                    }
                    decision::Decision::Produce {
                        leader,
                        holes,
                        after,
                    } => {
                        let ObjectPayload::Bytes(bytes) = leader else {
                            return Err(Error::InvalidLeader);
                        };
                        self.leader = Some(AccountPublicKey(
                            bytes.try_into().map_err(|_| Error::InvalidLeader)?,
                        ));
                        self.keys = Some(owner.signing.clone());
                        self.hole = holes.is_some();
                        if let Some(holes) = holes {
                            if after > 0.into() {
                                self.sleep(now, &after, Wake::Reconsider)?;
                            } else if holes > 0.into() {
                                self.phase = Phase::Key;
                            } else {
                                return Err(Error::HoleCount(holes));
                            }
                        } else {
                            self.sleep(now, &after, Wake::Micro)?;
                        }
                    }
                }
                Ok(Progress::Decided)
            }
            Phase::Wait => {
                if now < self.deadline.expect("scheduled wake") {
                    return Ok(Progress::Waiting);
                }
                self.deadline = None;
                self.phase = match self.wake {
                    Wake::Done => Phase::Done,
                    Wake::Micro => Phase::MicroStart,
                    Wake::FinishKey => Phase::FinishKey,
                    Wake::Seal => Phase::Seal,
                    Wake::Reconsider => {
                        let db = owner.source.database.upgrade().ok_or(Error::Closed)?;
                        self.parent = db
                            .view()
                            .map_err(|e| Error::Source(e.into()))?
                            .status()
                            .tip
                            .ok_or(Error::Closed)?;
                        Phase::Decide
                    }
                };
                Ok(Progress::Awoke)
            }
            Phase::MicroStart => {
                self.micro = Some(
                    owner
                        .micro_candidate(
                            self.source.clone(),
                            self.pool.clone(),
                            candidate::Input {
                                parent: self.parent,
                                limits: self.config.micro,
                                pins: self.config.pins.clone(),
                                inputs: self.config.inputs,
                                timeout: self.config.timeout,
                                window_ms: i64::try_from(&self.config.producer.production_time)
                                    .map_err(|_| Error::Time)?,
                            },
                        )
                        .map_err(Error::Build)?,
                );
                self.phase = Phase::Micro;
                Ok(Progress::Micro(candidate::Progress::Waiting))
            }
            Phase::Micro => {
                let micro = self.micro.as_mut().expect("micro build");
                let p = micro.step(owner, wall_ms.into()).map_err(Error::Build)?;
                if micro.ready() {
                    self.completed_micro = Some(
                        self.micro
                            .take()
                            .expect("ready build")
                            .finish()
                            .expect("ready build"),
                    );
                    self.phase = Phase::SignMicro;
                }
                Ok(Progress::Micro(p))
            }
            Phase::SignMicro => {
                let completed = self.completed_micro.as_ref().expect("finished micro");
                let block = match &completed.candidate.outcome {
                    Err(e) => {
                        self.skipped = Some(Skipped::Failed {
                            reason: e.reason.clone(),
                        });
                        None
                    }
                    Ok(owned::Candidate::Micro(p))
                        if p.candidate.block().transactions.is_empty() =>
                    {
                        self.skipped = Some(Skipped::Empty);
                        None
                    }
                    Ok(owned::Candidate::Micro(p)) => Some(Block::Micro(
                        p.candidate
                            .signed::<()>(
                                self.keys.as_ref().expect("signing"),
                                self.leader.expect("leader"),
                            )
                            .map_err(|e| {
                                Error::Build(candidate::Error::Packing(packing::execution(e)))
                            })?,
                    )),
                    _ => return Err(Error::Binding),
                };
                self.acknowledged = self
                    .acknowledged
                    .saturating_add(completed.acknowledged_effects);
                self.is_eoe = block.is_some() && completed.is_eoe;
                self.completed_micro = None;
                self.block = block;
                if self.block.is_some() {
                    self.phase = Phase::Submit(Kind::Micro);
                    Ok(Progress::Signed(Kind::Micro))
                } else {
                    self.phase = Phase::Key;
                    Ok(Progress::MicroSkipped)
                }
            }
            Phase::Key => {
                let parent = owner.parent.as_ref().ok_or(Error::Binding)?;
                let leader = self.leader.expect("leader");
                let mut key = self
                    .source
                    .candidate(
                        owner.source.snapshots(),
                        owned::Request {
                            binding: owner.producer_binding.clone(),
                            parent: parent.snapshot().cache,
                            pins: self.config.pins.clone(),
                            limits: self.config.inputs,
                            input: owned::Input::Key(build::KeyInput {
                                parent: self.parent,
                                production: core::KeyInput {
                                    miner: leader,
                                    beneficiary: leader,
                                    hole: self.hole,
                                    end_of_epoch: self.is_eoe,
                                    time: wall_ms,
                                    release_info: self.config.release_info,
                                },
                            }),
                        },
                        || {
                            if parent.is_closed() || self.client.is_closed() {
                                Err(chain::Error::Closed)
                            } else {
                                Ok(())
                            }
                        },
                    )
                    .map_err(Error::Source)?;
                self.effects = Some(effects::Pending::new(
                    &mut key.effects,
                    parent.clone(),
                    self.client.clone(),
                ));
                self.key = Some(key);
                self.phase = Phase::KeyEffects;
                Ok(Progress::KeyPrepared)
            }
            Phase::KeyEffects => {
                let effects = self.effects.as_mut().expect("key effects");
                let p = effects.step().map_err(Error::Effects)?;
                if effects.ready() {
                    self.acknowledged = self.acknowledged.saturating_add(effects.acknowledged);
                    let key = match self.key.as_ref().expect("key").outcome.as_ref() {
                        Ok(owned::Candidate::Key(p)) => &p.candidate,
                        Err(e) => {
                            return Err(Error::Key {
                                operational: e.operational,
                                reason: e.reason.clone(),
                            });
                        }
                        _ => return Err(Error::Binding),
                    };
                    if let Some(wait) = key.wait_ms() {
                        self.sleep(
                            now,
                            &BigInt::from_bytes_be(num_bigint::Sign::Plus, wait.as_bytes()),
                            Wake::FinishKey,
                        )?;
                    } else {
                        self.phase = Phase::FinishKey;
                    }
                }
                Ok(Progress::Effects(p))
            }
            Phase::FinishKey => {
                let Ok(owned::Candidate::Key(p)) = &self.key.as_ref().expect("key").outcome else {
                    return Err(Error::Binding);
                };
                let header = p
                    .candidate
                    .finish::<()>(wall_ms)
                    .map_err(|e| Error::Build(candidate::Error::Packing(packing::execution(e))))?;
                self.block = Some(Block::Key(header));
                self.phase = Phase::Seal;
                Ok(Progress::KeyPrepared)
            }
            Phase::Seal => {
                let Some(Block::Key(header)) = &mut self.block else {
                    return Err(Error::Binding);
                };
                let keys = self.keys.as_mut().expect("keys");
                match keys.seal(header).map_err(Error::Signing)? {
                    signing::Seal::Found(seal) => {
                        header.seal = seal;
                        owner.signing = keys.clone();
                        self.key = None;
                        self.phase = Phase::Submit(Kind::Key);
                        Ok(Progress::Signed(Kind::Key))
                    }
                    signing::Seal::NoSolution { retry_ms } => {
                        self.sleep(now, &retry_ms.into(), Wake::Seal)?;
                        Ok(Progress::Waiting)
                    }
                }
            }
            Phase::Submit(kind) => {
                let bytes = self
                    .block
                    .as_ref()
                    .expect("signed block")
                    .encode()
                    .map_err(Error::Encoding)?;
                self.receipt = Some(
                    importer
                        .import_created_block(bytes, wall_ms)
                        .map_err(Error::Source)?,
                );
                self.phase = Phase::Receipt(kind);
                Ok(Progress::Submitted(kind))
            }
            Phase::Receipt(kind) => {
                let Some(result) = self
                    .receipt
                    .as_mut()
                    .expect("admission receipt")
                    .try_result()
                else {
                    return Ok(Progress::Waiting);
                };
                self.receipt = None;
                let report = match result {
                    Ok(report) => report,
                    Err(failure) => {
                        let error = Error::Admission(failure);
                        if retryable(&error) {
                            self.phase = Phase::Submit(kind);
                        }
                        return Err(error);
                    }
                };
                let block = self.block.as_ref().expect("submitted block");
                let hash = block
                    .hash()
                    .map_err(|e| Error::Encoding(ae_chain::block::Error::Header(e)))?;
                if report.accepted.len() != 1 || report.accepted[0].hash != hash {
                    return Err(Error::UnexpectedReceipt);
                }
                let accepted = report.accepted.into_iter().next().expect("one block");
                self.block = None;
                if kind == Kind::Micro {
                    self.parent = hash;
                    self.accepted_micro = Some(accepted);
                    self.phase = Phase::Key;
                } else {
                    self.outcome = Some(Outcome::Produced {
                        micro: self.accepted_micro.clone(),
                        key: accepted,
                        is_eoe: self.is_eoe,
                        hole: self.hole,
                    });
                    self.phase = Phase::Done;
                }
                Ok(Progress::Accepted(kind))
            }
            Phase::Done => Ok(Progress::Complete),
        }
    }
    pub fn finish(self) -> Result<Completed, Box<Self>> {
        if !self.ready() {
            return Err(Box::new(self));
        }
        Ok(Completed {
            outcome: self.outcome.expect("completed outcome"),
            original_parent: self.original_parent,
            parent: self.parent,
            decisions: self.decisions,
            acknowledged_effects: self.acknowledged,
            skipped: self.skipped,
        })
    }
}
