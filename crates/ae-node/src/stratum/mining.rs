// One ordered accounting owner publishes work, records solutions, and joins
// ordinary chain admissions. A miner acknowledgement confirms accounting only.
use super::*;
pub mod payouts;
pub mod production;
pub mod rounds;
use ae_storage::stratum::mining as journal;
use ae_stratum::{accounting::Date, candidate::Candidate};
use ae_sync::chain::{
    self,
    pow::{
        generator::Clock,
        stratum::{Admission, Admitter},
    },
};
use std::{
    future::Future,
    pin::Pin,
    time::{Duration, Instant},
};

pub struct Inputs {
    pub admitter: Admitter,
    pub limits: journal::Limits,
    pub strictly_follow_top: bool,
    pub clock: Arc<dyn Clock>,
    pub production: Option<production::Inputs>,
    pub rounds: Option<rounds::Inputs>,
    pub payouts: Option<payouts::Inputs>,
}
#[derive(Clone)]
pub(super) struct Read {
    db: Arc<Database>,
    accounting: storage::Limits,
    limits: journal::Limits,
    epoch: u64,
    production: Option<Arc<production::Shared>>,
    rounds: Option<storage::rounds::Limits>,
    payouts: Option<Arc<payouts::Shared>>,
}
#[derive(Clone)]
pub struct Client(pub(super) super::Client);
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Publish {
        sequence: u64,
        candidate: Candidate,
        date: Date,
    },
    Acknowledge(journal::Record),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Published(Box<journal::Publication>),
    Acknowledged(Id),
}
pub struct Submission {
    pub publication: Option<u64>,
    pub completion: Pin<Box<dyn Future<Output = Result<Reply, pool::Error>> + Send>>,
}
pub(super) struct Job {
    pub request: Request,
    pub reply: oneshot::Sender<Result<Reply, pool::Error>>,
    credit: Arc<Credit>,
}
#[derive(Debug)]
pub struct Pending {
    pub request: Request,
    pub unknown: bool,
    _credit: Arc<Credit>,
}
impl Job {
    pub(super) fn reject(self, unknown: bool, error: pool::Error, report: &mut Report) {
        let _ = self.reply.send(Err(error));
        report.controls.push(Pending {
            request: self.request,
            unknown,
            _credit: self.credit,
        });
    }
}
impl Client {
    pub fn payout_status(&self) -> Option<payouts::Status> {
        self.read().payouts.as_ref().map(|p| p.status())
    }
    pub fn payout(&self) -> Result<Option<storage::payouts::Info>, Error> {
        let r = self.read();
        r.payouts
            .as_ref()
            .map(|p| Ok(r.db.stratum(r.accounting)?.payouts(p.limits)?))
            .transpose()
            .map(Option::flatten)
    }
    pub fn rounds(&self) -> Result<Option<storage::rounds::Info>, Error> {
        self.read()
            .rounds
            .map(|limits| {
                self.read()
                    .db
                    .stratum(self.read().accounting)?
                    .rounds(limits)
                    .map_err(Error::from)
            })
            .transpose()
            .map(Option::flatten)
    }

    pub fn production_status(&self) -> Option<production::Status> {
        self.read().production.as_ref().map(|p| p.status())
    }
    pub fn attach_production(&self, services: services::Client) -> Result<(), Error> {
        self.read()
            .production
            .as_ref()
            .ok_or(Error::Configuration)?
            .attach(services)
    }
    pub(super) fn stop_payouts(&self) {
        if let Some(p) = &self.read().payouts {
            p.stop();
        }
    }
    pub(super) fn stop_production(&self) {
        if let Some(p) = &self.read().production {
            p.stop();
        }
    }

    fn read(&self) -> &Read {
        self.0.shared.mining.as_ref().expect("mining client")
    }
    pub fn epoch(&self) -> u64 {
        self.read().epoch
    }
    pub fn info(&self) -> Result<journal::Info, Error> {
        let r = self.read();
        Ok(r.db.stratum(r.accounting)?.mining(r.limits)?.info().clone())
    }
    pub fn get(&self, id: Id) -> Result<Option<journal::Record>, Error> {
        let r = self.read();
        Ok(r.db.stratum(r.accounting)?.mining(r.limits)?.get(id)?)
    }
    pub fn page(
        &self,
        phase: journal::Phase,
        after: Option<Id>,
        count: usize,
    ) -> Result<(Vec<journal::Record>, bool), Error> {
        let r = self.read();
        Ok(r.db
            .stratum(r.accounting)?
            .mining(r.limits)?
            .page(phase, after, count)?)
    }
    // Only broadcast candidate.work() after Published { published: true }.
    // Closing or dropping a completion never erases an accepted publication.
    pub fn publish(&self, candidate: Candidate, date: Date) -> Result<Submission, pool::Error> {
        if self.read().production.is_some() {
            return Err(pool::Error::Invalid);
        }
        date.validate().map_err(|_| pool::Error::Invalid)?;
        let mut state = self
            .0
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let sequence = state.next_publication.ok_or(pool::Error::Limit)?;
        let result = self.enqueue(
            &state,
            Request::Publish {
                sequence,
                candidate,
                date,
            },
        )?;
        state.next_publication = sequence.checked_add(1);
        Ok(result)
    }
    pub fn acknowledge(&self, record: journal::Record) -> Result<Submission, pool::Error> {
        if !matches!(record.state, journal::State::Done(_)) {
            return Err(pool::Error::Invalid);
        }
        let state = self
            .0
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.enqueue(&state, Request::Acknowledge(record))
    }
    fn enqueue(&self, state: &super::State, request: Request) -> Result<Submission, pool::Error> {
        let sender = state.sender.as_ref().ok_or(pool::Error::Closed)?;
        let bytes = 1024
            + match &request {
                Request::Publish { candidate, .. } => candidate.sealing_bytes().len(),
                Request::Acknowledge(record) => {
                    record.encoded_len().map_err(|_| pool::Error::Invalid)?
                }
            };
        // The public request and queued copy are both charged while retained.
        let credit = self.0.shared.budget.reserve(bytes * 2)?;
        let (reply, receive) = oneshot::channel();
        sender
            .try_send(Message::Control(Box::new(Job {
                request: request.clone(),
                reply,
                credit: credit.clone(),
            })))
            .map_err(|e| match e {
                mpsc::TrySendError::Full(_) => pool::Error::Busy,
                mpsc::TrySendError::Disconnected(_) => pool::Error::Closed,
            })?;
        Ok(Submission {
            publication: match request {
                Request::Publish { sequence, .. } => Some(sequence),
                _ => None,
            },
            completion: Box::pin(async move {
                let _credit = credit;
                receive.await.unwrap_or(Err(pool::Error::Unknown))
            }),
        })
    }
}
#[derive(Debug)]
pub struct Unsettled {
    pub record: journal::Record,
    pub result: Option<Result<chain::Report, chain::Failure>>,
}
struct Inflight {
    record: journal::Record,
    admission: Admission,
}
impl Inflight {
    fn join(&mut self) {
        while self.admission.poll().is_none() {
            thread::sleep(Duration::from_millis(1));
        }
    }
}
impl Drop for Inflight {
    fn drop(&mut self) {
        // Cancellation requests do not establish absence. Retain/join the
        // actual worker before releasing this owner's exclusive storage lease.
        self.admission.cancel();
        self.join();
    }
}
pub(super) struct Worker {
    pub read: Read,
    inputs: Inputs,
    epoch: journal::Epoch,
    production: Option<production::Worker>,
    rounds: Option<storage::rounds::Limits>,
    payouts: Option<payouts::Worker>,
    inflight: Option<Inflight>,
    retry_at: Instant,
}
impl Worker {
    pub(super) fn start(
        writer: &mut storage::Writer,
        db: Arc<Database>,
        accounting: storage::Limits,
        ingress: IngressLimits,
        mut inputs: Inputs,
    ) -> Result<Self, Error> {
        if inputs.payouts.is_some() && inputs.rounds.is_none() {
            return Err(Error::Configuration);
        }
        let payouts = inputs
            .payouts
            .take()
            .map(|config| {
                payouts::Worker::start(
                    writer,
                    db.clone(),
                    accounting,
                    config,
                    inputs.clock.clone(),
                    &inputs.admitter,
                )
            })
            .transpose()?;
        let production = inputs
            .production
            .take()
            .map(|config| production::Worker::new(&inputs.admitter, config, inputs.clock.clone()))
            .transpose()?;
        let rounds = inputs
            .rounds
            .take()
            .map(|config| rounds::start(writer, config))
            .transpose()?;
        let epoch = writer.begin_mining(inputs.limits)?;
        let mut worker = Self {
            read: Read {
                db,
                accounting,
                limits: inputs.limits,
                epoch: epoch.value(),
                production: production.as_ref().map(|p| p.shared.clone()),
                rounds,
                payouts: payouts.as_ref().map(|p| p.shared.clone()),
            },
            production,
            rounds,
            payouts,
            inputs,
            epoch,
            inflight: None,
            retry_at: Instant::now(),
        };
        // The previous actor's guard was joined BEFORE its Writer lease could
        // be acquired. On process restart, all previous workers are gone.
        while let Some(record) = worker.first(journal::Phase::Submitting)? {
            retry(writer, |w| {
                w.reconcile_settled_submission(&record, worker.inputs.limits)
            })?;
        }
        let mut after = None;
        loop {
            let (receipts, more) = worker
                .read
                .db
                .stratum(accounting)?
                .ingress(ingress)?
                .pending(after, ingress.solutions.min(128))?;
            for receipt in receipts {
                retry(writer, |w| {
                    w.prepare_solution(&worker.epoch, receipt.id, worker.inputs.limits)
                })?;
                after = Some(receipt.id);
            }
            if !more {
                break;
            }
        }
        // Startup does not dispatch: failure before ingress activation cannot
        // leave an accepted chain worker behind the caller's back.
        worker.retry_at = Instant::now();
        Ok(worker)
    }
    fn first(&self, phase: journal::Phase) -> Result<Option<journal::Record>, Error> {
        Ok(self
            .read
            .db
            .stratum(self.read.accounting)?
            .mining(self.read.limits)?
            .page(phase, None, 1)?
            .0
            .into_iter()
            .next())
    }
    pub(super) fn accept(
        &mut self,
        writer: &mut storage::Writer,
        epoch: &Epoch,
        sequence: u64,
        input: Input,
        limits: IngressLimits,
    ) -> Result<Receipt, storage::Failure> {
        let receipt = writer.accept_mining(
            epoch,
            sequence,
            input,
            limits,
            &self.epoch,
            self.inputs.limits,
        )?;
        if receipt.input.solution.is_some()
            && let Some(production) = self.production.as_mut()
            && self
                .read
                .db
                .stratum(self.read.accounting)?
                .mining(self.inputs.limits)?
                .get(receipt.id)?
                .is_some_and(|r| r.candidate.is_some())
        {
            production.refresh_requested = true;
        }
        Ok(receipt)
    }
    pub(super) fn control(
        &mut self,
        writer: &mut storage::Writer,
        request: &Request,
    ) -> Result<Reply, Error> {
        Ok(match request {
            Request::Publish {
                sequence,
                candidate,
                date,
            } => Reply::Published(Box::new(retry(writer, |w| {
                w.publish_work(
                    &self.epoch,
                    *sequence,
                    candidate.clone(),
                    *date,
                    self.inputs.limits,
                )
            })?)),
            Request::Acknowledge(record) => {
                retry(writer, |w| {
                    w.acknowledge_solution(record, self.inputs.limits)
                })?;
                Reply::Acknowledged(record.receipt.id)
            }
        })
    }
    pub(super) fn tick(
        &mut self,
        writer: &mut storage::Writer,
        dispatch: bool,
    ) -> Result<(), Error> {
        if dispatch
            && let Some(limits) = self.rounds
            && let Some(receipt) = rounds::tick(writer, limits)?
            && matches!(receipt.outcome, storage::rounds::Outcome::Reward { .. })
            && let Some(payouts) = self.payouts.as_mut()
        {
            payouts.request_check();
        }
        if let Some(payouts) = self.payouts.as_mut() {
            payouts.tick(writer, dispatch, Instant::now())?;
        }
        let frame = retry(writer, |w| w.refresh_work(&self.epoch, self.inputs.limits))?;
        if let Some(production) = self.production.as_mut() {
            production.tick(writer, &self.epoch, self.inputs.limits, frame, dispatch)?;
        }
        if let Some(inflight) = self.inflight.as_mut() {
            if let Some(result) = inflight.admission.poll().cloned() {
                let outcome = outcome(&inflight.record, result)?;
                retry(writer, |w| {
                    w.finish_submission(&inflight.record, outcome.clone(), self.inputs.limits)
                })?;
                self.inflight.take();
            } else {
                return Ok(());
            }
        }
        if !dispatch || Instant::now() < self.retry_at {
            return Ok(());
        }
        let Some(record) = self.first(journal::Phase::Ready)? else {
            return Ok(());
        };
        let solution = record
            .solution()?
            .ok_or(Error::Mining("ready solution missing"))?;
        let now = u64::try_from(self.inputs.clock.unix_millis())
            .map_err(|_| Error::Mining("clock outside u64"))?;
        let record = retry(writer, |w| w.begin_submission(&record, self.inputs.limits))?;
        match self
            .inputs
            .admitter
            .submit(&solution, now, self.inputs.strictly_follow_top)
        {
            Ok(admission) => self.inflight = Some(Inflight { record, admission }),
            Err(error) => {
                // No ticket was accepted. Requeue only this exact attempt;
                // neither a timeout nor a later operational failure comes here.
                retry(writer, |w| {
                    w.unaccepted_submission(&record, self.inputs.limits)
                })?;
                if error == chain::Error::Busy {
                    self.retry_at = Instant::now() + Duration::from_millis(100);
                } else {
                    return Err(Error::Chain(error));
                }
            }
        }
        Ok(())
    }
    pub(super) fn finish(&mut self, writer: &mut storage::Writer) -> Result<(), Error> {
        if let Some(p) = self.production.as_mut() {
            p.stop();
        }
        if let Some(inflight) = self.inflight.as_mut() {
            inflight.join();
        }
        if let Some(payouts) = self.payouts.as_mut() {
            payouts.finish(writer)?;
        }
        self.tick(writer, false)
    }
    pub(super) fn payout_report(&mut self) -> Option<payouts::Report> {
        self.payouts.take().map(|mut p| p.report())
    }
    pub(super) fn production_report(&mut self) -> Option<production::Report> {
        self.production.take().map(|p| p.finish())
    }
    pub(super) fn unsettled(&mut self) -> Option<Unsettled> {
        let mut inflight = self.inflight.take()?;
        inflight.join();
        Some(Unsettled {
            record: inflight.record.clone(),
            result: inflight.admission.result().cloned(),
        })
    }
}
fn outcome(
    record: &journal::Record,
    result: Result<chain::Report, chain::Failure>,
) -> Result<journal::Outcome, Error> {
    use journal::{Accepted, Outcome, Rejection};
    match result {
        Ok(report) => {
            let solution = record
                .solution()?
                .ok_or(Error::Mining("admitted solution missing"))?;
            let [accepted] = report.accepted.as_slice() else {
                return Err(Error::Mining("admission cardinality"));
            };
            if accepted.hash != solution.hash() || accepted.micro_transactions.is_some() {
                return Err(Error::Mining("admission binding"));
            }
            Ok(Outcome::Accepted(match accepted.outcome {
                chain::Outcome::Present => Accepted::Present,
                chain::Outcome::Committed { revision } => Accepted::Committed(revision),
            }))
        }
        Err(failure) => {
            if failure.progress != Some(chain::Report::default()) {
                return Err(Error::Admission(failure));
            }
            let reason = match &failure.error {
                chain::Error::Stale => Rejection::Stale,
                chain::Error::Policy { .. }
                | chain::Error::BlockedByWhitelist
                | chain::Error::Refused(_) => Rejection::Policy,
                chain::Error::Hash
                | chain::Error::Block(_)
                | chain::Error::Validation {
                    operational: false, ..
                } => Rejection::Invalid,
                _ => return Err(Error::Admission(failure)),
            };
            let mut detail = failure.error.to_string();
            let mut end = detail.len().min(4096);
            while !detail.is_char_boundary(end) {
                end -= 1;
            }
            detail.truncate(end);
            Ok(Outcome::Rejected { reason, detail })
        }
    }
}
#[cfg(test)]
mod tests;
