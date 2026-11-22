// Live ordinary-pool packing for a captured Hyperchain micro candidate. No
// consensus/storage-owner mutex spans selection or failure-report receipts.
use ae_chain::{
    hyperchain::candidate as core,
    repository::{self as repo, hyperchain::candidate as build},
};
use ae_execution::transactions::reasons;
use ae_mempool::pool as policy;
use ae_storage::{
    hyperchain::candidate as owned,
    pool::{self, worker},
    snapshots,
};
use ae_vm_fate::maps::MapOrder;
use num_bigint::BigInt;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub reserved_gas: BigInt,
    pub timeout: core::Timeout,
    pub window_ms: i64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Selection,
    Failures,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Binding,
    Sequence,
    Pool { stage: Stage, error: worker::Error },
    Execution { reason: String, retryable: bool },
    UnmappedRejection,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    Waiting,
    Selected,
    Applied { accepted: usize, rejected: usize },
    Reported { removed: usize },
    Finished,
}
pub struct StartError<O> {
    pub input: owned::Prepared<O>,
    pub error: Error,
}
impl<O> std::fmt::Debug for StartError<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
pub struct FinishError<O> {
    pub pending: Packing<O>,
    pub error: Error,
}
impl<O> std::fmt::Debug for FinishError<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FinishError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
enum State {
    Ready,
    Selecting(worker::Ticket<policy::Candidate>),
    Selected(policy::Candidate),
    Reporting(Vec<pool::FailedTransaction>),
    Recording {
        reports: Vec<pool::FailedTransaction>,
        call: worker::Ticket<Vec<bool>>,
    },
    Done,
}
pub struct Packing<O> {
    core: core::Packing<owned::Store, owned::Store>,
    reader: repo::hyperchain::Reader<snapshots::Reader, O>,
    metadata: snapshots::Metadata,
    effects: repo::hyperchain::inputs::Effects,
    pool: worker::Client,
    clock: worker::Clock,
    state: State,
    failure: Option<Error>,
}
fn execution(e: core::Error<(), owned::Store, owned::Store>) -> Error {
    let retryable = match &e {
        core::Error::Transactions(e) => e.failure.is_operational(),
        core::Error::Accounting(e) => e.is_operational(),
        core::Error::ResourceLimit | core::Error::Tree(_) => true,
        _ => false,
    };
    let mut reason = format!("{e:?}");
    if reason.len() > 4096 {
        let end = (0..=4096)
            .rev()
            .find(|n| reason.is_char_boundary(*n))
            .unwrap();
        reason.truncate(end);
    }
    Error::Execution { reason, retryable }
}
fn pool_error(stage: Stage, error: worker::Error) -> Error {
    Error::Pool { stage, error }
}
fn retryable(e: &Error) -> bool {
    match e {
        Error::Execution { retryable, .. } => *retryable,
        Error::Pool { error, .. } => match error {
            worker::Error::Saturated => true,
            worker::Error::Service(pool::Failure::Repository(
                repo::Error::Conflict | repo::Error::ResourceLimit,
            )) => true,
            worker::Error::Service(pool::Failure::Policy(e)) => e.is_operational(),
            _ => false,
        },
        _ => false,
    }
}
impl<O: MapOrder + Clone> Packing<O> {
    pub fn start(
        input: owned::Prepared<O>,
        pool: worker::Client,
        config: Config,
    ) -> Result<Self, Box<StartError<O>>> {
        let clock = pool.clock();
        let make = || {
            let Ok(owned::Candidate::Micro(p)) = &input.outcome else {
                return Err(Error::Sequence);
            };
            let view = p.reader.selected.repository();
            if !pool.uses_source(input.metadata.source_id)
                || p.source_id != input.metadata.source_id
                || repo::Read::source_id(view.source()) != p.source_id
                || view.status() != p.status
                || p.status != input.metadata.status
            {
                return Err(Error::Binding);
            }
            core::Packing::new::<()>(
                p.candidate.clone(),
                config.reserved_gas,
                clock.now(),
                config.timeout,
                config.window_ms,
            )
            .map_err(execution)
        };
        let core = match make() {
            Ok(v) => v,
            Err(error) => return Err(Box::new(StartError { input, error })),
        };
        let owned::Candidate::Micro(p) = input.outcome.expect("validated micro candidate") else {
            unreachable!()
        };
        Ok(Self {
            core,
            reader: p.reader,
            metadata: input.metadata,
            effects: input.effects,
            pool,
            clock,
            state: State::Ready,
            failure: None,
        })
    }
    pub fn metadata(&self) -> &snapshots::Metadata {
        &self.metadata
    }
    pub fn effects(&self) -> &repo::hyperchain::inputs::Effects {
        &self.effects
    }
    pub fn failure(&self) -> Option<&Error> {
        self.failure.as_ref()
    }
    pub fn in_flight(&self) -> bool {
        matches!(self.state, State::Selecting(_) | State::Recording { .. })
    }
    pub fn expired(&self) -> bool {
        self.core.expired()
    }
    pub fn ready(&self) -> bool {
        self.failure.is_none() && matches!(self.state, State::Done)
    }
    // Only a definitely rejected operation may be retried. A lost receipt or
    // uncertain durable pool write keeps this exact attempt quarantined.
    pub fn retry(&mut self) -> Result<(), Error> {
        if let Some(e) = &self.failure
            && !retryable(e)
        {
            return Err(e.clone());
        }
        self.failure = None;
        Ok(())
    }
    pub fn step(&mut self) -> Result<Progress, Error> {
        if let Some(e) = &self.failure {
            return Err(e.clone());
        }
        let result = self.advance();
        if let Err(e) = &result {
            self.failure = Some(e.clone());
        }
        result
    }
    fn advance(&mut self) -> Result<Progress, Error> {
        match &mut self.state {
            State::Ready => {
                let Some(r) = self
                    .core
                    .request::<()>(|| self.clock.now())
                    .map_err(execution)?
                else {
                    self.state = State::Done;
                    return Ok(Progress::Finished);
                };
                let gas = if r.gas < &BigInt::from(0) {
                    0
                } else {
                    u64::try_from(r.gas).map_err(|_| Error::Execution {
                        reason: "candidate gas exceeds pool integer range".into(),
                        retryable: false,
                    })?
                };
                let options = worker::SelectOptions {
                    deadline: r
                        .deadlines
                        .bound
                        .map(|t| self.clock.instant(t))
                        .transpose()
                        .map_err(|e| pool_error(Stage::Selection, e))?,
                    ceiling: r
                        .deadlines
                        .ceiling
                        .map(|t| self.clock.instant(t))
                        .transpose()
                        .map_err(|e| pool_error(Stage::Selection, e))?,
                    packed: r.packed,
                };
                let call = self
                    .pool
                    .candidate_at(
                        self.reader.selected.repository().clone(),
                        r.parent,
                        gas,
                        r.ignored.clone(),
                        options,
                    )
                    .map_err(|e| pool_error(Stage::Selection, e))?;
                self.state = State::Selecting(call);
                Ok(Progress::Waiting)
            }
            State::Selecting(call) => match call.wait_timeout(Duration::ZERO) {
                Err(worker::Error::Timeout) => Ok(Progress::Waiting),
                Err(e) => {
                    let e = pool_error(Stage::Selection, e);
                    if retryable(&e) {
                        self.state = State::Ready;
                    }
                    Err(e)
                }
                Ok(selected) => {
                    // A reply is private to this submitted selection. It may
                    // not be redirected to another candidate or another pool.
                    let r = self
                        .core
                        .request::<()>(|| self.clock.now())
                        .map_err(execution)?
                        .ok_or(Error::Sequence)?;
                    if selected.block != r.parent {
                        return Err(Error::Binding);
                    }
                    self.state = State::Selected(selected);
                    Ok(Progress::Selected)
                }
            },
            State::Selected(selected) => {
                let txs = selected
                    .transactions
                    .iter()
                    .map(|tx| tx.as_ref().clone())
                    .collect::<Vec<_>>();
                let result = self
                    .core
                    .selected::<(), _, _>(&txs, selected.expired, &self.reader)
                    .map_err(execution)?;
                let progress = Progress::Applied {
                    accepted: result.accepted.len(),
                    rejected: result.rejected.len(),
                };
                let reports = result
                    .rejected
                    .iter()
                    .map(|(i, rejection)| {
                        let reason =
                            reasons::rejection(rejection).ok_or(Error::UnmappedRejection)?;
                        Ok(pool::FailedTransaction {
                            hash: txs[*i].hash(),
                            reason: reason.tag.into(),
                            tuple: reason.tuple,
                        })
                    })
                    .collect::<Result<Vec<_>, Error>>()?;
                self.state = if reports.is_empty() {
                    State::Ready
                } else {
                    State::Reporting(reports)
                };
                Ok(progress)
            }
            State::Reporting(reports) => {
                let call = self
                    .pool
                    .record_failures(reports.clone())
                    .map_err(|e| pool_error(Stage::Failures, e))?;
                self.state = State::Recording {
                    reports: std::mem::take(reports),
                    call,
                };
                Ok(Progress::Waiting)
            }
            State::Recording { reports, call } => match call.wait_timeout(Duration::ZERO) {
                Err(worker::Error::Timeout) => Ok(Progress::Waiting),
                Err(e) => {
                    let e = pool_error(Stage::Failures, e);
                    if retryable(&e) {
                        self.state = State::Reporting(std::mem::take(reports));
                    }
                    Err(e)
                }
                Ok(removed) => {
                    if removed.len() != reports.len() {
                        return Err(Error::Sequence);
                    }
                    let removed = removed.into_iter().filter(|b| *b).count();
                    self.state = State::Ready;
                    Ok(Progress::Reported { removed })
                }
            },
            State::Done => Ok(Progress::Finished),
        }
    }
    pub fn finish(self) -> Result<owned::Prepared<O>, Box<FinishError<O>>> {
        if !self.ready() {
            return Err(Box::new(FinishError {
                pending: self,
                error: Error::Sequence,
            }));
        }
        let Self {
            core,
            reader,
            metadata,
            effects,
            pool,
            clock,
            state,
            failure,
        } = self;
        match core.try_finish::<()>() {
            Ok(candidate) => Ok(owned::Prepared {
                outcome: Ok(owned::Candidate::Micro(build::Prepared {
                    candidate,
                    reader,
                    status: metadata.status,
                    source_id: metadata.source_id,
                })),
                effects,
                metadata,
            }),
            Err((core, error)) => {
                let error = execution(error);
                Err(Box::new(FinishError {
                    pending: Self {
                        core: *core,
                        reader,
                        metadata,
                        effects,
                        pool,
                        clock,
                        state,
                        failure,
                    },
                    error,
                }))
            }
        }
    }
}
