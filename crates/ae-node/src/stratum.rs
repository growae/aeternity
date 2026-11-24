// Bounded ordered ingress for the live Stratum transport. Accounting is
// durable before a positive miner response; block admission/payouts are separate.
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
    Storage(storage::Failure),
    Runtime(runtime::Error),
    Io(std::io::ErrorKind),
    Panicked,
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
struct State {
    sender: Option<mpsc::SyncSender<Job>>,
    next: Option<u64>,
}
struct Shared {
    state: Mutex<State>,
    budget: Arc<Budget>,
    epoch: u64,
    closed: watch::Sender<bool>,
    report: Mutex<Option<Report>>,
}
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
}
impl Client {
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
        sender.try_send(job).map_err(|e| match e {
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
    fn start_with(
        db: Arc<Database>,
        accounting: storage::Limits,
        ingress: IngressLimits,
        limits: Limits,
        mut before: impl FnMut(Id) + Send + 'static,
    ) -> Result<Self, Error> {
        if !(1..=65536).contains(&limits.records) || !(1024..=1 << 30).contains(&limits.bytes) {
            return Err(Error::Configuration);
        }
        let mut writer = storage::Writer::start(db, accounting)?;
        let epoch = writer.begin_ingress(ingress)?;
        let (sender, receive) = mpsc::sync_channel(limits.records);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                sender: Some(sender),
                next: Some(1),
            }),
            budget: Arc::new(Budget {
                used: Mutex::new((0, 0)),
                limits,
            }),
            epoch: epoch.value(),
            closed: watch::channel(false).0,
            report: Mutex::new(None),
        });
        let client = Client { shared };
        let worker = client.clone();
        let join = thread::Builder::new()
            .name("ae-stratum-accounting".into())
            .spawn(move || run(&mut writer, &epoch, ingress, receive, worker, &mut before))
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
fn run(
    writer: &mut storage::Writer,
    epoch: &Epoch,
    limits: IngressLimits,
    receive: mpsc::Receiver<Job>,
    client: Client,
    before: &mut impl FnMut(Id),
) -> Report {
    let mut report = Report::default();
    while let Ok(job) = receive.recv() {
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
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            before(job.id);
            match writer.accept(epoch, job.id.sequence, job.input.clone(), limits) {
                Ok(r) => Ok(r),
                Err(_) if writer.recovery_required() => {
                    // Only an exactly verified failed/committed batch can be
                    // retried with the identical id. No external effect occurs.
                    writer.recover()?;
                    writer.accept(epoch, job.id.sequence, job.input.clone(), limits)
                }
                Err(e) => Err(e),
            }
        }));
        match result {
            Ok(Ok(r)) => {
                let _ = job.reply.send(Ok(r.id));
                report.last = Some(r);
            }
            failure => {
                let (error, unknown) = match failure {
                    Ok(Err(e)) => (Error::Storage(e), writer.recovery_required()),
                    Err(_) => (Error::Panicked, true),
                    _ => unreachable!(),
                };
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
    client.close();
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
        drop(self.transport.take());
        drop(self.accounting.take());
    }
}
#[cfg(test)]
mod tests;
