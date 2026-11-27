// aestratum payout/confirmation timers, composed with the accounting owner.
// Accepted signer/pool requests retain their original tickets until settled.
use super::*;
use ae_mempool::pool::{Event, Signed};
use ae_storage::pool::{Submission as PoolSubmission, worker as txpool};
use ae_sync::preset_keys as keys;
use ae_types::{Signature, Unsigned};
use storage::payouts::{self as durable, Dispatch, Record};
mod gating;
pub mod preflight;
use gating::Authority;
use preflight::task;

#[derive(Clone)]
pub enum Pool {
    Fixed(txpool::Client),
    Managed(ae_sync::bindings::Admission),
}
impl From<txpool::Client> for Pool {
    fn from(client: txpool::Client) -> Self {
        Self::Fixed(client)
    }
}
impl From<ae_sync::bindings::Admission> for Pool {
    fn from(binding: ae_sync::bindings::Admission) -> Self {
        Self::Managed(binding)
    }
}
impl Pool {
    fn uses_database(&self, db: &Arc<Database>) -> bool {
        match self {
            Self::Fixed(c) => c.uses_database(db),
            Self::Managed(b) => b.uses_database(db),
        }
    }
    fn current(&self) -> Option<txpool::Client> {
        match self {
            Self::Fixed(c) => Some(c.clone()),
            Self::Managed(b) => b.current().map(|p| p.client),
        }
    }
    fn replacing(&self, error: &txpool::Error) -> bool {
        matches!(self, Self::Managed(_)) && matches!(error, txpool::Error::Stopped)
    }
}

pub struct Inputs {
    pub config: durable::Config,
    pub limits: durable::Limits,
    pub generation: ae_stratum::payout::Generation,
    pub signer: keys::Client,
    pub pool: Pool,
    pub snapshots: ae_storage::snapshots::Owner,
    pub preflight: ae_chain::repository::pow::key::Limits,
    pub payout_interval: Duration,
    pub confirmation_interval: Duration,
}
impl Inputs {
    pub fn new(
        config: durable::Config,
        signer: keys::Client,
        pool: impl Into<Pool>,
        snapshots: ae_storage::snapshots::Owner,
    ) -> Self {
        Self {
            config,
            limits: Default::default(),
            generation: ae_stratum::payout::Generation::Bounded(Default::default()),
            signer,
            pool: pool.into(),
            snapshots,
            preflight: Default::default(),
            payout_interval: Duration::from_secs(120),
            confirmation_interval: Duration::from_secs(10),
        }
    }
    fn validate(&self, db: &Arc<Database>) -> Result<(), Error> {
        if self.generation.validate().is_err()
            || !self.pool.uses_database(db)
            || !self.snapshots.uses_database(db)
            || [self.payout_interval, self.confirmation_interval]
                .iter()
                .any(|d| *d < Duration::from_millis(1) || *d > Duration::from_secs(86400))
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Idle,
    Pricing,
    Preflight,
    Signing,
    Submitting,
    Waiting,
}
#[derive(Debug, Clone)]
pub enum Notice {
    PoolUnavailable,
    Deferred(durable::Deferred),
    Preparation(ae_stratum::payout::Error),
    Signer(keys::Error),
    Pool(txpool::Error),
    Confirmed(durable::Completion),
    MissingReward,
    ExecutionFailed(Box<durable::ExecutionFailure>),
    ReceiptUnavailable(durable::ReceiptUnavailable),
    Preflight(Arc<task::Completion>),
    PreflightStart(Arc<task::Error>),
    AwaitingConfirmation {
        hash: ae_types::Hash256,
        block: ae_types::Hash256,
    },
}
#[derive(Debug, Clone)]
pub enum Withdrawal {
    PoolUnavailable,
    Stopped,
    Stale,
    Pool(txpool::Error),
    Preflight(Arc<task::Error>),
}
#[derive(Debug, Clone)]
pub struct Status {
    pub epoch: u64,
    pub phase: Phase,
    pub closing: bool,
    pub stopped: bool,
    pub waiting_for_pool: bool,
    pub last: Option<Notice>,
}
pub(super) struct Shared {
    pub limits: durable::Limits,
    status: Mutex<Status>,
    dispatch: Mutex<bool>,
}
impl Shared {
    pub(super) fn stop(&self) {
        *self.dispatch.lock().unwrap_or_else(|e| e.into_inner()) = false;
        self.status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closing = true;
    }
    fn running(&self) -> bool {
        *self.dispatch.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(super) fn status(&self) -> Status {
        self.status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    fn phase(&self, phase: Phase) {
        self.status.lock().unwrap_or_else(|e| e.into_inner()).phase = phase;
    }
    fn notice(&self, notice: Notice) {
        self.status.lock().unwrap_or_else(|e| e.into_inner()).last = Some(notice);
    }
}
#[derive(Debug, Clone)]
pub enum Completion {
    Price(Result<Unsigned, txpool::Error>),
    Preflight {
        record: Box<Record>,
        result: Arc<task::Completion>,
    },
    Unaccepted {
        record: Box<Record>,
        reason: Withdrawal,
    },
    Signature {
        record: Box<Record>,
        result: Result<Signature, keys::Error>,
    },
    Submission {
        record: Box<Record>,
        result: Result<PoolSubmission, txpool::Error>,
    },
}
#[derive(Debug)]
pub struct Report {
    pub status: Status,
    // A consumed result remains here when publication failed. It is never
    // replaced by a new request against a replacement service.
    pub unsettled: Option<Completion>,
}
enum Pending {
    Preflight {
        record: Box<Record>,
        task: task::Task,
        result: Option<Arc<task::Completion>>,
    },
    Unaccepted {
        record: Box<Record>,
        reason: Withdrawal,
    },
    Price {
        ticket: txpool::Ticket<Unsigned>,
        result: Option<Result<Unsigned, txpool::Error>>,
    },
    Signature {
        record: Box<Record>,
        ticket: keys::Ticket<Signature>,
        result: Option<Result<Signature, keys::Error>>,
        date: Option<Date>,
    },
    Submission {
        record: Box<Record>,
        ticket: txpool::Ticket<PoolSubmission>,
        result: Option<Result<PoolSubmission, txpool::Error>>,
    },
}
fn pool_result<T>(ticket: &txpool::Ticket<T>) -> Option<Result<T, txpool::Error>> {
    match ticket.wait_timeout(Duration::ZERO) {
        Err(txpool::Error::Timeout) => None,
        result => Some(result),
    }
}
impl Pending {
    fn poll(&mut self) -> bool {
        match self {
            Self::Preflight { task, result, .. } => {
                if result.is_none() {
                    *result = task.try_result().map(Arc::new);
                }
                result.is_some()
            }
            Self::Unaccepted { .. } => true,
            Self::Price { ticket, result } => {
                if result.is_none() {
                    *result = pool_result(ticket);
                }
                result.is_some()
            }
            Self::Signature { ticket, result, .. } => {
                if result.is_none() {
                    *result = ticket.try_result();
                }
                result.is_some()
            }
            Self::Submission { ticket, result, .. } => {
                if result.is_none() {
                    *result = pool_result(ticket);
                }
                result.is_some()
            }
        }
    }
    fn join(&mut self) {
        while !self.poll() {
            thread::sleep(Duration::from_millis(1));
        }
    }
    fn cancel(&self) {
        if let Self::Preflight { task, .. } = self {
            task.request_cancel();
        }
    }
    fn completion(self) -> Completion {
        match self {
            Self::Preflight { record, result, .. } => Completion::Preflight {
                record,
                result: result.expect("settled preflight"),
            },
            Self::Unaccepted { record, reason } => Completion::Unaccepted { record, reason },
            Self::Price { result, .. } => Completion::Price(result.expect("settled price")),
            Self::Signature { record, result, .. } => Completion::Signature {
                record,
                result: result.expect("settled signature"),
            },
            Self::Submission { record, result, .. } => Completion::Submission {
                record,
                result: result.expect("settled submission"),
            },
        }
    }
}
pub(super) struct Worker {
    pub shared: Arc<Shared>,
    inputs: Inputs,
    db: Arc<Database>,
    accounting: storage::Limits,
    clock: Arc<dyn Clock>,
    epoch: u64,
    pending: Option<Pending>,
    preflight: task::Builder,
    authority: Option<Authority>,
    check: bool,
    confirmation_hold: bool,
    next_payout: Instant,
    next_confirmation: Instant,
}
impl Worker {
    pub(super) fn start(
        writer: &mut storage::Writer,
        db: Arc<Database>,
        accounting: storage::Limits,
        inputs: Inputs,
        clock: Arc<dyn Clock>,
        admitter: &Admitter,
    ) -> Result<Self, Error> {
        inputs.validate(&db)?;
        let source = admitter
            .execution(inputs.snapshots.clone(), inputs.preflight)
            .map_err(|e| Error::PayoutPreflight(Arc::new(task::Error::Source(e))))?;
        let preflight = task::Builder::new(source, inputs.limits.payout);
        let info = retry(writer, |w| {
            w.begin_payouts(inputs.config.clone(), inputs.limits)
        })?;
        let restore_confirmation = info
            .pending
            .as_ref()
            .map(Record::hash)
            .transpose()?
            .flatten()
            .is_some();
        let now = Instant::now();
        Ok(Self {
            shared: Arc::new(Shared {
                limits: inputs.limits,
                dispatch: Mutex::new(true),
                status: Mutex::new(Status {
                    epoch: info.epoch,
                    phase: if info.pending.is_some() {
                        Phase::Waiting
                    } else {
                        Phase::Idle
                    },
                    closing: false,
                    stopped: false,
                    waiting_for_pool: false,
                    last: None,
                }),
            }),
            db,
            accounting,
            clock,
            epoch: info.epoch,
            pending: None,
            preflight,
            authority: None,
            check: info.pending.is_some(),
            confirmation_hold: false,
            next_payout: now + inputs.payout_interval,
            next_confirmation: if restore_confirmation {
                now
            } else {
                now + inputs.confirmation_interval
            },
            inputs,
        })
    }
    pub(super) fn request_check(&mut self) {
        self.check = true;
    }
    fn info(&self) -> Result<durable::Info, Error> {
        self.db
            .stratum(self.accounting)?
            .payouts(self.inputs.limits)?
            .filter(|i| i.epoch == self.epoch)
            .ok_or(Error::Mining("payout owner changed"))
    }
    fn wait_for_pool(&mut self, dispatch: bool) {
        self.check = dispatch;
        self.shared
            .status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .waiting_for_pool = true;
        self.shared.notice(Notice::PoolUnavailable);
    }
    fn pool(&mut self, dispatch: bool) -> Option<txpool::Client> {
        let pool = self.inputs.pool.current();
        if pool.is_none() {
            self.wait_for_pool(dispatch);
        } else {
            self.shared
                .status
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .waiting_for_pool = false;
        }
        pool
    }
    // Keep terminal replies in pending until every fallible publication is
    // complete. In particular pool tickets consume their replies only once.
    fn complete(&mut self, writer: &mut storage::Writer, dispatch: bool) -> Result<(), Error> {
        let Some(pending) = self.pending.as_mut() else {
            return Ok(());
        };
        if !pending.poll() {
            return Ok(());
        }
        // Read before taking the mutable result borrow. A failed read retains
        // the joined completion and its original accepted request for retry.
        let expected = if matches!(self.pending, Some(Pending::Preflight { .. })) {
            Some(self.info()?)
        } else {
            None
        };
        let epoch = self.epoch;
        let limits = self.inputs.limits;
        match self.pending.as_mut().expect("settled request") {
            Pending::Preflight {
                record,
                task,
                result: Some(result),
            } => {
                let request = record
                    .request
                    .as_ref()
                    .ok_or(Error::Mining("preflight request missing"))?;
                if expected.as_ref().and_then(|i| i.pending.as_ref()) != Some(record.as_ref())
                    || result.id != task.id()
                    || result.prepared.as_ref() != &request.prepared
                {
                    return Err(Error::Mining("preflight completion binding"));
                }
                let current = match &result.result {
                    Ok(checked) if dispatch => self
                        .preflight
                        .is_current(checked)
                        .map_err(|e| Error::PayoutPreflight(Arc::new(e)))?,
                    _ => false,
                };
                self.authority = match &result.result {
                    Ok(checked) if current => Some(Authority {
                        sequence: record.sequence,
                        checked: checked.clone(),
                    }),
                    _ => None,
                };
                self.check = dispatch && result.result.is_ok();
                self.shared.notice(Notice::Preflight(result.clone()));
            }
            Pending::Unaccepted { record, reason } => {
                retry(writer, |w| {
                    w.unaccepted_payout_submission(epoch, record, limits)
                })?;
                match reason {
                    Withdrawal::PoolUnavailable => self.wait_for_pool(dispatch),
                    Withdrawal::Stopped => {}
                    Withdrawal::Stale => self.check = dispatch,
                    Withdrawal::Pool(error) => {
                        self.shared.notice(Notice::Pool(error.clone()));
                        if self.inputs.pool.replacing(error) {
                            self.wait_for_pool(dispatch);
                        } else if *error != txpool::Error::Saturated {
                            return Err(Error::PayoutPool(error.clone()));
                        }
                    }
                    Withdrawal::Preflight(error) => {
                        return Err(Error::PayoutPreflight(error.clone()));
                    }
                }
            }
            Pending::Price {
                result: Some(result),
                ..
            } => match result {
                Ok(price) if dispatch => {
                    match retry(writer, |w| {
                        w.prepare_payout_with_generation(
                            epoch,
                            price.clone(),
                            self.inputs.generation,
                            limits,
                        )
                    }) {
                        Ok(durable::Selection::Prepared(_) | durable::Selection::Existing(_)) => {
                            self.check = true
                        }
                        Ok(durable::Selection::Deferred(reason)) => {
                            self.shared.notice(Notice::Deferred(reason))
                        }
                        Err(storage::Failure::Payout(error)) => {
                            self.shared.notice(Notice::Preparation(error))
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                Ok(_) => {}
                Err(error) if self.inputs.pool.replacing(error) => self.wait_for_pool(dispatch),
                Err(error) => return Err(Error::PayoutPool(error.clone())),
            },
            Pending::Signature {
                record,
                result: Some(result),
                date,
                ..
            } => match result {
                Ok(signature) => {
                    let date = match date {
                        Some(d) => *d,
                        None => {
                            let millis = u64::try_from(self.clock.unix_millis())
                                .map_err(|_| Error::Mining("payout clock outside u64"))?;
                            let d = Date::from_unix_millis(millis)
                                .map_err(|e| Error::Storage(e.into()))?;
                            *date = Some(d);
                            d
                        }
                    };
                    retry(writer, |w| {
                        w.sign_payout(epoch, record, *signature, date, limits)
                    })?;
                    self.check = true;
                }
                Err(error) => {
                    self.shared.notice(Notice::Signer(error.clone()));
                    if matches!(error, keys::Error::Disconnected | keys::Error::Stopped) {
                        return Err(Error::PayoutSigner(error.clone()));
                    }
                }
            },
            Pending::Submission {
                record,
                result: Some(result),
                ..
            } => match result {
                Ok(submission) => {
                    retry(writer, |w| {
                        w.record_payout_submission(epoch, record, submission.clone(), limits)
                    })?;
                }
                Err(error) if !error.is_operational() => {
                    let mut reason = format!("{error:?}");
                    let mut end = reason.len().min(4096);
                    while !reason.is_char_boundary(end) {
                        end -= 1;
                    }
                    reason.truncate(end);
                    retry(writer, |w| {
                        w.reject_payout_submission(epoch, record, reason.clone(), limits)
                    })?;
                    self.shared.notice(Notice::Pool(error.clone()));
                }
                Err(error) => return Err(Error::PayoutPool(error.clone())),
            },
            _ => unreachable!("settled reply"),
        }
        self.pending = None;
        Ok(())
    }
    pub(super) fn tick(
        &mut self,
        writer: &mut storage::Writer,
        dispatch: bool,
        now: Instant,
    ) -> Result<(), Error> {
        // Interval messages that arrived while an external request was active
        // cannot bypass that request. Coalesce missed intervals to bound work.
        let busy = self.pending.is_some();
        let confirm = now >= self.next_confirmation;
        if confirm {
            self.next_confirmation = now + self.inputs.confirmation_interval;
        }
        if now >= self.next_payout {
            self.next_payout = now + self.inputs.payout_interval;
            if !busy {
                self.check = true;
            }
        }
        self.complete(writer, dispatch)?;
        if self.pending.is_some() {
            return Ok(());
        }
        let mut info = self.info()?;
        self.shared.phase(if info.pending.is_some() {
            Phase::Waiting
        } else {
            Phase::Idle
        });
        if !dispatch || !self.shared.running() {
            return Ok(());
        }
        if (confirm || (self.check && !self.confirmation_hold))
            && let Some(record) = &info.pending
            && record.hash()?.is_some()
        {
            let result = retry(writer, |w| {
                w.confirm_scheduled_payout(self.epoch, record, self.inputs.limits)
            })?;
            if self.confirmation(result) {
                info = self.info()?;
            }
        }
        if self.confirmation_hold || !std::mem::take(&mut self.check) {
            return Ok(());
        }
        match info.pending {
            None => {
                let Some(pool) = self.pool(dispatch) else {
                    return Ok(());
                };
                let gate = self
                    .shared
                    .dispatch
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if !*gate {
                    return Ok(());
                }
                match pool.minimum_miner_gas_price() {
                    Ok(ticket) => {
                        self.pending = Some(Pending::Price {
                            ticket,
                            result: None,
                        });
                        self.shared.phase(Phase::Pricing);
                    }
                    Err(txpool::Error::Saturated) => {
                        self.shared.notice(Notice::Pool(txpool::Error::Saturated))
                    }
                    Err(error) if self.inputs.pool.replacing(&error) => {
                        drop(gate);
                        self.wait_for_pool(dispatch);
                    }
                    Err(error) => return Err(Error::PayoutPool(error)),
                }
            }
            Some(record) if record.signed.is_none() && record.request.is_some() => {
                let shared = self.shared.clone();
                let gate = shared.dispatch.lock().unwrap_or_else(|e| e.into_inner());
                if !*gate {
                    return Ok(());
                }
                if !self.authorize(&record)? {
                    return Ok(());
                }
                let prepared = &record.request.as_ref().expect("prepared request").prepared;
                match self
                    .inputs
                    .signer
                    .sign_binary(prepared.signing_bytes(), self.inputs.config.caller)
                {
                    Ok(ticket) => {
                        self.pending = Some(Pending::Signature {
                            record: Box::new(record),
                            ticket,
                            result: None,
                            date: None,
                        });
                        self.shared.phase(Phase::Signing);
                    }
                    Err(keys::Error::Busy) => self.shared.notice(Notice::Signer(keys::Error::Busy)),
                    Err(error) => return Err(Error::PayoutSigner(error)),
                }
            }
            Some(record) => match record.signed.as_ref().map(|s| &s.dispatch) {
                Some(Dispatch::Ready(_)) => {
                    // Do not publish/retract Sending intents on every poll
                    // while a managed pool binding is unavailable.
                    if self.pool(dispatch).is_none() {
                        return Ok(());
                    }
                    {
                        let shared = self.shared.clone();
                        let gate = shared.dispatch.lock().unwrap_or_else(|e| e.into_inner());
                        if !*gate || !self.authorize(&record)? {
                            return Ok(());
                        }
                    }
                    let tx =
                        Signed::decode(&record.signed.as_ref().expect("signed record").transaction)
                            .map_err(|_| Error::Mining("saved payout transaction"))?;
                    let record = retry(writer, |w| {
                        w.begin_payout_submission(self.epoch, &record, self.inputs.limits)
                    })?;
                    self.submit(writer, record, tx, dispatch)?;
                }
                Some(Dispatch::Sending(_)) => {
                    retry(writer, |w| {
                        w.reconcile_payout(self.epoch, &record, self.inputs.limits)
                    })?;
                }
                _ => {}
            },
        }
        Ok(())
    }
    // Return true only when storage atomically removed the pending payment.
    fn confirmation(&mut self, result: durable::Confirmation) -> bool {
        match result {
            durable::Confirmation::Confirmed(done) => {
                self.authority = None;
                self.confirmation_hold = false;
                self.shared.notice(Notice::Confirmed(done));
                self.shared.phase(Phase::Idle);
                true
            }
            durable::Confirmation::Failed(failure) => {
                self.confirmation_hold = true;
                self.shared.notice(Notice::ExecutionFailed(failure));
                false
            }
            durable::Confirmation::Unavailable(receipt) => {
                self.confirmation_hold = true;
                self.shared.notice(Notice::ReceiptUnavailable(receipt));
                false
            }
            durable::Confirmation::MissingReward => {
                self.confirmation_hold = true;
                self.shared.notice(Notice::MissingReward);
                false
            }
            durable::Confirmation::Pending => {
                self.confirmation_hold = false;
                let mut status = self.shared.status.lock().unwrap_or_else(|e| e.into_inner());
                if matches!(
                    status.last,
                    Some(
                        Notice::ExecutionFailed(_)
                            | Notice::ReceiptUnavailable(_)
                            | Notice::MissingReward
                    )
                ) {
                    status.last = None;
                }
                false
            }
        }
    }
    pub(super) fn finish(&mut self, writer: &mut storage::Writer) -> Result<(), Error> {
        self.shared.stop();
        if let Some(pending) = self.pending.as_mut() {
            pending.cancel();
            pending.join();
        }
        self.tick(writer, false, Instant::now())
    }
    pub(super) fn report(&mut self) -> Report {
        self.shared.stop();
        if let Some(pending) = self.pending.as_mut() {
            pending.cancel();
            pending.join();
        }
        self.shared
            .status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stopped = true;
        Report {
            status: self.shared.status(),
            unsettled: self.pending.take().map(Pending::completion),
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        // This field is dropped before the accounting Writer, including unwind.
        self.shared.stop();
        if let Some(pending) = self.pending.as_mut() {
            pending.cancel();
            pending.join();
        }
        self.shared
            .status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stopped = true;
    }
}
#[cfg(test)]
mod tests;
