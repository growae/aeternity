// Live ordinary-pool packing for an immutable micro candidate. No
// consensus/storage-owner mutex spans selection or failure-report receipts.
use ae_chain::{hyperchain::candidate as core, repository as repo};
use ae_execution::transactions::reasons;
use ae_mempool::pool as policy;
pub use ae_storage::snapshots::Store;
use ae_storage::{
    pool::{self, worker},
    snapshots,
};
use ae_vm_fate::maps::MapOrder;
use num_bigint::BigInt;
use std::time::Duration;

pub struct Prepared<O> {
    pub candidate: core::Micro<Store, Store>,
    pub reader: repo::ExecutionReader<snapshots::Reader, O>,
    pub status: repo::Status,
    pub source_id: u64,
    pub metadata: snapshots::Metadata,
}

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
    pub input: Prepared<O>,
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
    Observed {
        stage: Stage,
        error: worker::Error,
    },
    Done,
}
pub struct Packing<O> {
    core: core::Packing<Store, Store>,
    reader: repo::ExecutionReader<snapshots::Reader, O>,
    metadata: snapshots::Metadata,
    pool: worker::Client,
    clock: worker::Clock,
    state: State,
    failure: Option<Error>,
}
pub(crate) fn execution(e: core::Error<(), Store, Store>) -> Error {
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
pub(crate) fn retryable(e: &Error) -> bool {
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
        input: Prepared<O>,
        pool: worker::Client,
        config: Config,
    ) -> Result<Self, Box<StartError<O>>> {
        let clock = pool.clock();
        let make = || {
            let p = &input;
            let view = p.reader.selected.repository();
            if !pool.uses_source(input.metadata.source_id)
                || p.source_id != input.metadata.source_id
                || repo::Read::source_id(view.source()) != p.source_id
                || view.status() != p.status
                || p.status != input.metadata.status
                || !view.identity().matches(&p.reader.profile.network)
                || p.candidate.environment().network_id != view.identity().network
                || !matches!(
                    (
                        view.identity().consensus,
                        p.candidate.environment().consensus
                    ),
                    (
                        ae_protocol::ConsensusType::Pow,
                        ae_vm_fate::machine::program::remote::context::Consensus::Pow
                    ) | (
                        ae_protocol::ConsensusType::Pos,
                        ae_vm_fate::machine::program::remote::context::Consensus::Pos
                    )
                )
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
        Ok(Self {
            core,
            reader: input.reader,
            metadata: input.metadata,
            pool,
            clock,
            state: State::Ready,
            failure: None,
        })
    }
    pub fn metadata(&self) -> &snapshots::Metadata {
        &self.metadata
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
                    let observed = e.clone();
                    let e = pool_error(Stage::Selection, e);
                    if retryable(&e) {
                        self.state = State::Ready;
                    } else {
                        self.state = State::Observed {
                            stage: Stage::Selection,
                            error: observed,
                        };
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
                    let observed = e.clone();
                    let e = pool_error(Stage::Failures, e);
                    if retryable(&e) {
                        self.state = State::Reporting(std::mem::take(reports));
                    } else {
                        self.state = State::Observed {
                            stage: Stage::Failures,
                            error: observed,
                        };
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
            State::Observed { stage, error } => Err(pool_error(*stage, error.clone())),
        }
    }
    pub fn cancel(self) -> Cancelled {
        let observed = match &self.state {
            State::Observed { stage, error } => Some((*stage, error.clone())),
            _ => None,
        };
        let (pending, discarded_failures) = match self.state {
            State::Selecting(call) => (Some(CancelReceipt::Selection(call)), 0),
            State::Recording { reports, call } => {
                (Some(CancelReceipt::Failures { reports, call }), 0)
            }
            State::Reporting(reports) => (None, reports.len()),
            _ => (None, 0),
        };
        Cancelled {
            pool: self.pool,
            retired: false,
            observed,
            pending,
            report: CancelReport {
                discarded_failures,
                ..Default::default()
            },
        }
    }
    pub fn finish(self) -> Result<Prepared<O>, Box<FinishError<O>>> {
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
            pool,
            clock,
            state,
            failure,
        } = self;
        match core.try_finish::<()>() {
            Ok(candidate) => Ok(Prepared {
                candidate,
                reader,
                status: metadata.status,
                source_id: metadata.source_id,
                metadata,
            }),
            Err((core, error)) => {
                let error = execution(error);
                Err(Box::new(FinishError {
                    pending: Self {
                        core: *core,
                        reader,
                        metadata,
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

#[derive(Debug, Default)]
pub struct CancelReport {
    pub selection: Option<Result<usize, worker::Error>>,
    pub failure_reports: Option<Result<Vec<bool>, worker::Error>>,
    pub discarded_failures: usize,
}
enum CancelReceipt {
    Selection(worker::Ticket<policy::Candidate>),
    Failures {
        reports: Vec<pool::FailedTransaction>,
        call: worker::Ticket<Vec<bool>>,
    },
}
pub struct Cancelled {
    pool: worker::Client,
    retired: bool,
    observed: Option<(Stage, worker::Error)>,
    pending: Option<CancelReceipt>,
    report: CancelReport,
}
impl Cancelled {
    pub fn retire_pool(&mut self, old: &worker::Client) -> Result<bool, Error> {
        if !old.info().joined {
            return Err(Error::Binding);
        }
        let matches = self.pool.same_worker(old);
        self.retired |= matches;
        Ok(matches)
    }
    pub fn step(&mut self) -> Result<bool, Error> {
        let (stage, result) = if let Some((stage, error)) = &self.observed {
            (*stage, Err(error.clone()))
        } else if let Some(pending) = &self.pending {
            match pending {
                CancelReceipt::Selection(call) => (
                    Stage::Selection,
                    call.wait_timeout(Duration::ZERO)
                        .map(|selected| CancelResult::Selection(selected.transactions.len())),
                ),
                CancelReceipt::Failures { reports, call } => (
                    Stage::Failures,
                    call.wait_timeout(Duration::ZERO).map(|removed| {
                        if removed.len() == reports.len() {
                            CancelResult::Failures(removed)
                        } else {
                            CancelResult::Unexpected
                        }
                    }),
                ),
            }
        } else {
            return Ok(true);
        };
        match result {
            Err(worker::Error::Timeout) => return Ok(false),
            Err(e)
                if !known_pool_rejection(&e)
                    && !(self.retired
                        && matches!(e, worker::Error::Stopped | worker::Error::Disconnected)) =>
            {
                // The ticket has been consumed. Keep its first terminal result
                // so a later joined-owner notification cannot turn Stopped into
                // Disconnected by polling the empty ticket again.
                self.pending = None;
                self.observed = Some((stage, e.clone()));
                return Err(pool_error(stage, e));
            }
            Err(e) => match stage {
                Stage::Selection => self.report.selection = Some(Err(e)),
                Stage::Failures => self.report.failure_reports = Some(Err(e)),
            },
            Ok(CancelResult::Selection(n)) => self.report.selection = Some(Ok(n)),
            Ok(CancelResult::Failures(removed)) => self.report.failure_reports = Some(Ok(removed)),
            Ok(CancelResult::Unexpected) => return Err(Error::Sequence),
        }
        self.pending = None;
        self.observed = None;
        Ok(true)
    }
    pub fn finish(self) -> CancelReport {
        assert!(self.pending.is_none() && self.observed.is_none());
        self.report
    }
}
enum CancelResult {
    Selection(usize),
    Failures(Vec<bool>),
    Unexpected,
}
fn known_pool_rejection(error: &worker::Error) -> bool {
    matches!(
        error,
        worker::Error::Service(pool::Failure::Repository(
            repo::Error::Conflict | repo::Error::ResourceLimit
        ))
    ) || matches!(error, worker::Error::Service(pool::Failure::Policy(e)) if !e.is_operational())
}

#[cfg(test)]
mod tests;
