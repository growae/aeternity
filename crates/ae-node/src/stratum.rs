// Bounded ordered ingress for the live Stratum transport. Accounting is
// durable before a positive miner response. Mining adds joined chain admission.
pub mod configured;
pub mod mining;
use ae_storage::{
    Database,
    stratum::{
        self as storage,
        ingress::{Epoch, Limits as IngressLimits, Receipt},
    },
};
use ae_stratum::{
    ingress::{Id, Input},
    runtime::{self, pool},
    services, share,
};
use std::{
    sync::{Arc, Mutex, mpsc},
    thread,
};
use tokio::sync::{oneshot, watch};
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub records: usize,
    pub bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            records: 256,
            bytes: 4 << 20,
        }
    }
}
#[derive(Debug)]
pub enum Error {
    Configuration,
    Setup(crate::configuration::stratum::Error),
    Storage(storage::Failure),
    Runtime(runtime::Error),
    Io(std::io::ErrorKind),
    Panicked,
    Chain(ae_sync::chain::Error),
    Admission(ae_sync::chain::Failure),
    Mining(&'static str),
    Services(services::Error),
    PreparationSetup(ae_sync::chain::pow::producer::mining::Error),
    ProductionKeys(ae_sync::keys::Error),
    ProductionController(ae_sync::chain::pow::producer::Error),
    PayoutPool(ae_storage::pool::worker::Error),
    PayoutSigner(ae_sync::preset_keys::Error),
    PayoutPreflight(Arc<mining::payouts::preflight::task::Error>),
}
impl From<storage::Failure> for Error {
    fn from(e: storage::Failure) -> Self {
        Self::Storage(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug)]
struct Budget {
    used: Mutex<(usize, usize)>,
    limits: Limits,
}
#[derive(Debug)]
struct Credit {
    budget: Arc<Budget>,
    bytes: usize,
}
impl Budget {
    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Arc<Credit>, pool::Error> {
        if bytes > self.limits.bytes {
            return Err(pool::Error::Limit);
        }
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        if used.0 >= self.limits.records || bytes > self.limits.bytes.saturating_sub(used.1) {
            return Err(pool::Error::Busy);
        }
        used.0 += 1;
        used.1 += bytes;
        Ok(Arc::new(Credit {
            budget: self.clone(),
            bytes,
        }))
    }
}
impl Drop for Credit {
    fn drop(&mut self) {
        let mut used = self.budget.used.lock().unwrap_or_else(|e| e.into_inner());
        used.0 -= 1;
        used.1 -= self.bytes;
    }
}
struct Job {
    id: Id,
    input: Input,
    reply: oneshot::Sender<Result<Id, pool::Error>>,
    credit: Arc<Credit>,
}
enum Message {
    Share(Job),
    Control(Box<mining::Job>),
}
// Fields drop in declaration order, including on an unexpected panic.
struct WorkerState {
    mining: Option<mining::Worker>,
    writer: storage::Writer,
}
struct State {
    sender: Option<mpsc::SyncSender<Message>>,
    next: Option<u64>,
    next_publication: Option<u64>,
}
struct Shared {
    state: Mutex<State>,
    budget: Arc<Budget>,
    epoch: u64,
    closed: watch::Sender<bool>,
    report: Mutex<Option<Report>>,
    mining: Option<mining::Read>,
}
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
}
impl Client {
    pub fn mining(&self) -> Option<mining::Client> {
        self.shared
            .mining
            .as_ref()
            .map(|_| mining::Client(self.clone()))
    }
    pub fn epoch(&self) -> u64 {
        self.shared.epoch
    }
    pub fn is_closed(&self) -> bool {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sender
            .is_none()
    }
    pub fn retained(&self) -> (usize, usize) {
        *self
            .shared
            .budget
            .used
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
    pub fn take_dropped_report(&self) -> Option<Report> {
        self.shared
            .report
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }
    pub fn close(&self) {
        if let Some(mining) = self.mining() {
            mining.stop_payouts();
        }
        self.shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sender = None;
        self.shared.closed.send_replace(true);
    }
}
impl pool::Sink for Client {
    fn closed(&self) -> watch::Receiver<bool> {
        self.shared.closed.subscribe()
    }
    fn submit(&self, effects: &[share::Effect]) -> Result<pool::Submission, pool::Error> {
        let input = Input::from_effects(effects).map_err(|_| pool::Error::Invalid)?;
        let bytes = 512 + input.encode().map_err(|_| pool::Error::Invalid)?.len();
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        let sender = state.sender.as_ref().ok_or(pool::Error::Closed)?;
        let sequence = state.next.ok_or(pool::Error::Limit)?;
        let credit = self.shared.budget.reserve(bytes)?;
        let id = Id {
            epoch: self.shared.epoch,
            sequence,
        };
        let (reply, receive) = oneshot::channel();
        let job = Job {
            id,
            input,
            reply,
            credit: credit.clone(),
        };
        sender.try_send(Message::Share(job)).map_err(|e| match e {
            mpsc::TrySendError::Full(_) => pool::Error::Busy,
            mpsc::TrySendError::Disconnected(_) => pool::Error::Closed,
        })?;
        state.next = sequence.checked_add(1);
        Ok(pool::Submission {
            id,
            completion: Box::pin(async move {
                let _credit = credit;
                receive.await.unwrap_or(Err(pool::Error::Unknown))
            }),
        })
    }
}
#[derive(Debug)]
pub struct Pending {
    pub id: Id,
    pub input: Input,
    pub unknown: bool,
    _credit: Arc<Credit>,
}
#[derive(Debug, Default)]
pub struct Report {
    pub failure: Option<Error>,
    pub last: Option<Receipt>,
    pub pending: Vec<Pending>,
    pub controls: Vec<mining::Pending>,
    pub admission: Option<mining::Unsettled>,
    pub production: Option<mining::production::Report>,
    pub payout: Option<mining::payouts::Report>,
}
pub struct Accounting {
    client: Client,
    join: Option<thread::JoinHandle<Report>>,
}
impl Accounting {
    pub fn start(
        db: Arc<Database>,
        accounting: storage::Limits,
        ingress: IngressLimits,
        limits: Limits,
    ) -> Result<Self, Error> {
        Self::start_with(db, accounting, ingress, limits, |_| {})
    }
    pub fn start_mining(
        db: Arc<Database>,
        accounting: storage::Limits,
        ingress: IngressLimits,
        limits: Limits,
        inputs: mining::Inputs,
    ) -> Result<Self, Error> {
        Self::start_inner(db, accounting, ingress, limits, Some(inputs), |_| {})
    }
    fn start_with(
        db: Arc<Database>,
        accounting: storage::Limits,
        ingress: IngressLimits,
        limits: Limits,
        before: impl FnMut(Id) + Send + 'static,
    ) -> Result<Self, Error> {
        Self::start_inner(db, accounting, ingress, limits, None, before)
    }
    fn start_inner(
        db: Arc<Database>,
        accounting: storage::Limits,
        ingress: IngressLimits,
        limits: Limits,
        inputs: Option<mining::Inputs>,
        mut before: impl FnMut(Id) + Send + 'static,
    ) -> Result<Self, Error> {
        if !(1..=65536).contains(&limits.records) || !(1024..=1 << 30).contains(&limits.bytes) {
            return Err(Error::Configuration);
        }
        if inputs
            .as_ref()
            .is_some_and(|i| !i.admitter.uses_database(&db))
        {
            return Err(Error::Configuration);
        }
        let mut writer = storage::Writer::start(db.clone(), accounting)?;
        let mining = inputs
            .map(|i| mining::Worker::start(&mut writer, db, accounting, ingress, i))
            .transpose()?;
        let epoch = writer.begin_ingress(ingress)?;
        let (sender, receive) = mpsc::sync_channel(limits.records);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                sender: Some(sender),
                next: Some(1),
                next_publication: Some(1),
            }),
            budget: Arc::new(Budget {
                used: Mutex::new((0, 0)),
                limits,
            }),
            epoch: epoch.value(),
            closed: watch::channel(false).0,
            report: Mutex::new(None),
            mining: mining.as_ref().map(|w| w.read.clone()),
        });
        let client = Client { shared };
        let worker = client.clone();
        let join = thread::Builder::new()
            .name("ae-stratum-accounting".into())
            .spawn(move || {
                let mut state = WorkerState { mining, writer };
                run(
                    &mut state.writer,
                    &epoch,
                    ingress,
                    receive,
                    worker,
                    &mut before,
                    state.mining.as_mut(),
                )
            })
            .map_err(|e| Error::Io(e.kind()))?;
        Ok(Self {
            client,
            join: Some(join),
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn shutdown(mut self) -> Report {
        self.client.close();
        self.join
            .take()
            .expect("accounting thread")
            .join()
            .expect("caught accounting worker panic")
    }
}
impl Drop for Accounting {
    fn drop(&mut self) {
        self.client.close();
        if let Some(join) = self.join.take() {
            let report = join.join().expect("caught accounting worker panic");
            *self
                .client
                .shared
                .report
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(report);
        }
    }
}
fn retry<T>(
    writer: &mut storage::Writer,
    mut operation: impl FnMut(&mut storage::Writer) -> Result<T, storage::Failure>,
) -> Result<T, storage::Failure> {
    // These retries only repeat exact storage operations. A canonical status
    // CAS can lose to an unrelated chain commit; no external admission is
    // repeated here. Bound retries and retain the input if pressure persists.
    let mut recovered = false;
    for attempt in 0..4 {
        let result = operation(writer);
        match &result {
            Err(_) if attempt < 3 && !recovered && writer.recovery_required() => {
                writer.recover()?;
                recovered = true;
            }
            Err(error) if attempt < 3 && error.is_conflict() => {}
            _ => return result,
        }
    }
    unreachable!("last attempt returns its exact result")
}
fn caught<T>(f: impl FnOnce() -> Result<T, Error>) -> Result<T, Error> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_or(Err(Error::Panicked))
}
fn run(
    writer: &mut storage::Writer,
    epoch: &Epoch,
    limits: IngressLimits,
    receive: mpsc::Receiver<Message>,
    client: Client,
    before: &mut impl FnMut(Id),
    mut mining: Option<&mut mining::Worker>,
) -> Report {
    let mut report = Report::default();
    loop {
        if report.failure.is_none()
            && let Some(w) = mining.as_deref_mut()
            && let Err(e) = caught(|| w.tick(writer, !client.is_closed()))
        {
            report.failure = Some(e);
            client.close();
        }
        let message = if mining.is_some() {
            match receive.recv_timeout(std::time::Duration::from_millis(10)) {
                Ok(m) => m,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        } else {
            match receive.recv() {
                Ok(m) => m,
                Err(_) => break,
            }
        };
        match message {
            Message::Share(job) => {
                if report.failure.is_some() {
                    let _ = job.reply.send(Err(pool::Error::Closed));
                    report.pending.push(Pending {
                        id: job.id,
                        input: job.input,
                        unknown: false,
                        _credit: job.credit,
                    });
                    continue;
                }
                let result = caught(|| {
                    before(job.id);
                    if let Some(w) = mining.as_deref_mut() {
                        w.refresh_producer(writer)?;
                    }
                    Ok(retry(writer, |writer| match mining.as_deref_mut() {
                        Some(w) => {
                            w.accept(writer, epoch, job.id.sequence, job.input.clone(), limits)
                        }
                        None => writer.accept(epoch, job.id.sequence, job.input.clone(), limits),
                    })?)
                });
                match result {
                    Ok(r) => {
                        let _ = job.reply.send(Ok(r.id));
                        report.last = Some(r);
                    }
                    Err(error) => {
                        let unknown =
                            writer.recovery_required() || matches!(error, Error::Panicked);
                        client.close();
                        report.failure = Some(error);
                        let _ = job.reply.send(Err(if unknown {
                            pool::Error::Unknown
                        } else {
                            pool::Error::Rejected
                        }));
                        report.pending.push(Pending {
                            id: job.id,
                            input: job.input,
                            unknown,
                            _credit: job.credit,
                        });
                    }
                }
            }
            Message::Control(job) => {
                if report.failure.is_some() {
                    job.reject(false, pool::Error::Closed, &mut report);
                    continue;
                }
                let result = caught(|| {
                    mining
                        .as_deref_mut()
                        .ok_or(Error::Configuration)?
                        .control(writer, &job.request)
                });
                match result {
                    Ok(reply) => {
                        let _ = job.reply.send(Ok(reply));
                    }
                    Err(error) => {
                        let unknown =
                            writer.recovery_required() || matches!(error, Error::Panicked);
                        client.close();
                        report.failure = Some(error);
                        job.reject(
                            unknown,
                            if unknown {
                                pool::Error::Unknown
                            } else {
                                pool::Error::Rejected
                            },
                            &mut report,
                        );
                    }
                }
            }
        }
    }
    client.close();
    if let Some(w) = mining {
        if report.failure.is_none()
            && let Err(e) = caught(|| w.finish(writer))
        {
            report.failure = Some(e);
        }
        report.admission = w.unsettled();
        report.production = w.production_report();
        report.payout = w.payout_report();
    }
    report
}
#[derive(Debug)]
pub struct ServiceReport {
    pub transport: runtime::Report,
    pub accounting: Report,
}
pub struct Service {
    transport: Option<runtime::Owner>,
    accounting: Option<Accounting>,
}
impl Service {
    fn attach_production(accounting: &Accounting, transport: &runtime::Owner) -> Result<(), Error> {
        if let Some(client) = accounting.client().mining()
            && client.production_status().is_some()
        {
            client.attach_production(transport.client().services())?;
        }
        Ok(())
    }
    fn stop_production(&self) {
        if let Some(accounting) = &self.accounting
            && let Some(client) = accounting.client().mining()
        {
            client.stop_production();
        }
    }

    pub fn bind(
        db: Arc<Database>,
        config: runtime::Config,
        accounting: storage::Limits,
        ingress: IngressLimits,
        limits: Limits,
    ) -> Result<Self, Error> {
        let accounting = Accounting::start(db, accounting, ingress, limits)?;
        let transport = runtime::Owner::with_pool(config, Arc::new(accounting.client()))
            .map_err(Error::Runtime)?;
        Self::attach_production(&accounting, &transport)?;
        Ok(Self {
            transport: Some(transport),
            accounting: Some(accounting),
        })
    }
    pub fn with_sources(
        db: Arc<Database>,
        config: runtime::Config,
        accounting: storage::Limits,
        ingress: IngressLimits,
        limits: Limits,
        sources: impl services::Sources + 'static,
    ) -> Result<Self, Error> {
        let accounting = Accounting::start(db, accounting, ingress, limits)?;
        let transport =
            runtime::Owner::with_sources_and_pool(config, sources, Arc::new(accounting.client()))
                .map_err(Error::Runtime)?;
        Self::attach_production(&accounting, &transport)?;
        Ok(Self {
            transport: Some(transport),
            accounting: Some(accounting),
        })
    }
    pub fn bind_mining(
        db: Arc<Database>,
        config: runtime::Config,
        accounting: storage::Limits,
        ingress: IngressLimits,
        limits: Limits,
        inputs: mining::Inputs,
    ) -> Result<Self, Error> {
        let accounting = Accounting::start_mining(db, accounting, ingress, limits, inputs)?;
        let transport = runtime::Owner::with_pool(config, Arc::new(accounting.client()))
            .map_err(Error::Runtime)?;
        Self::attach_production(&accounting, &transport)?;
        Ok(Self {
            transport: Some(transport),
            accounting: Some(accounting),
        })
    }
    pub fn with_sources_mining(
        db: Arc<Database>,
        config: runtime::Config,
        accounting: storage::Limits,
        ingress: IngressLimits,
        limits: Limits,
        inputs: mining::Inputs,
        sources: impl services::Sources + 'static,
    ) -> Result<Self, Error> {
        let accounting = Accounting::start_mining(db, accounting, ingress, limits, inputs)?;
        let transport =
            runtime::Owner::with_sources_and_pool(config, sources, Arc::new(accounting.client()))
                .map_err(Error::Runtime)?;
        Self::attach_production(&accounting, &transport)?;
        Ok(Self {
            transport: Some(transport),
            accounting: Some(accounting),
        })
    }
    pub fn client(&self) -> runtime::Client {
        self.transport.as_ref().expect("live transport").client()
    }
    pub fn accounting(&self) -> Client {
        self.accounting.as_ref().expect("live accounting").client()
    }
    pub fn shutdown(mut self) -> ServiceReport {
        self.stop_production();
        let transport = self.transport.take().expect("transport").shutdown();
        let accounting = self.accounting.take().expect("accounting").shutdown();
        ServiceReport {
            transport,
            accounting,
        }
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.stop_production();
        drop(self.transport.take());
        drop(self.accounting.take());
    }
}
#[cfg(test)]
mod tests;
