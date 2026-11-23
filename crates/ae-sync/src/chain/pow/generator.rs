// aec_block_generator.erl, aeternity fa2a6d7. ISC: repository LICENSE.
// The coordinator never executes transactions. One joined child performs
// actual snapshot preparation, live pool packing or incremental updates.
use super::packing;
use ae_chain::{
    hyperchain::candidate as core,
    repository::{self as repo, pow},
};
use ae_storage::{Database, events, pool::worker as pool, pow as owned, snapshots};
use ae_types::Hash256;
use ae_vm_fate::maps::MapOrder;
use core::Signed;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

mod receipts;
mod service;
mod task;
pub use receipts::{RequestError, Ticket};

pub trait Clock: Send + Sync + 'static {
    fn unix_millis(&self) -> i128;
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn unix_millis(&self) -> i128 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_millis() as i128,
            Err(e) => -(e.duration().as_millis() as i128),
        }
    }
}
#[derive(Clone)]
pub struct Config<O> {
    pub preparation: pow::Config<O>,
    pub candidate: core::Limits,
    pub packing: packing::Config,
    pub controls: usize,
    pub publications: usize,
    pub queued_transactions: usize,
    pub queued_bytes: usize,
    pub feed_batch: usize,
    pub poll_interval: Duration,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Stopped,
    Configuration,
    Binding,
    Ownership(snapshots::Error),
    Repository(repo::Error),
    Chain(events::Error),
    PoolFeed(pool::events::ReadError),
    ChainClosed,
    PoolClosed,
    QueueLimit,
    Sequence,
    Preparation(String),
    Packing(packing::Error),
    Update(String),
    Worker(String),
    Panicked,
}
#[derive(Debug, Clone, Default)]
pub struct Status {
    pub running: bool,
    pub accepting: bool,
    pub generating: bool,
    pub worker: bool,
    pub retiring: bool,
    pub queued_transactions: usize,
    pub queued_bytes: usize,
    pub retries: u64,
    pub retry_pending: bool,
    pub epoch: u64,
    pub published: u64,
    pub attempts: u64,
    pub updates: u64,
    pub unchanged: u64,
    pub preemptions: u64,
    pub retired: u64,
    pub settled_selections: u64,
    pub settled_failure_reports: u64,
    pub stopped_feed_gaps: u64,
    pub last_error: Option<Error>,
    pub failure: Option<Error>,
}
pub struct Candidate<O> {
    prepared: owned::Prepared<O>,
    _credit: receipts::Credit,
}
impl<O> Candidate<O> {
    pub fn prepared(&self) -> &owned::Prepared<O> {
        &self.prepared
    }
}
pub struct Publication<O> {
    pub sequence: u64,
    pub epoch: u64,
    pub candidate: Arc<Candidate<O>>,
    _credit: receipts::Credit,
}
#[derive(Clone, Copy)]
enum Command {
    Start,
    Stop,
}
struct Control {
    command: Command,
    reply: Arc<receipts::Slot>,
}
impl Drop for Control {
    fn drop(&mut self) {
        self.reply.complete(Err(RequestError::Closed));
    }
}
struct State<O> {
    status: Status,
    shutdown: bool,
    controls: VecDeque<Control>,
    candidate: Option<Arc<Candidate<O>>>,
    publications: VecDeque<Arc<Publication<O>>>,
}
struct Shared<O> {
    source_id: u64,
    micro_cycle_ms: u64,
    state: Mutex<State<O>>,
    wake: Condvar,
    controls: Arc<Mutex<usize>>,
    publications: Arc<Mutex<usize>>,
    candidates: Arc<Mutex<usize>>,
    limits: (usize, usize),
}
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}
pub struct Client<O>(Arc<Shared<O>>);
impl<O> Clone for Client<O> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<O> Client<O> {
    pub fn uses_source(&self, source_id: u64) -> bool {
        self.0.source_id == source_id
    }
    pub fn micro_cycle_ms(&self) -> u64 {
        self.0.micro_cycle_ms
    }
    pub fn status(&self) -> Status {
        lock(&self.0.state).status.clone()
    }
    pub fn candidate(&self) -> Option<Arc<Candidate<O>>> {
        lock(&self.0.state).candidate.clone()
    }
    // Publications are retained until acknowledged. Clones keep their capacity
    // credit, so acknowledging while retaining data cannot evade the bound.
    pub fn front(&self) -> Option<Arc<Publication<O>>> {
        lock(&self.0.state).publications.front().cloned()
    }
    pub fn acknowledge(&self, publication: &Arc<Publication<O>>) -> bool {
        let mut state = lock(&self.0.state);
        if state
            .publications
            .front()
            .is_some_and(|p| Arc::ptr_eq(p, publication))
        {
            state.publications.pop_front();
            self.0.wake.notify_all();
            true
        } else {
            false
        }
    }
    fn submit(&self, command: Command) -> Result<Ticket, RequestError> {
        let mut state = lock(&self.0.state);
        if !state.status.accepting {
            return Err(RequestError::Closed);
        }
        let credit = receipts::Credit::acquire(&self.0.controls, self.0.limits.0)
            .ok_or(RequestError::Busy)?;
        let slot = Arc::new(receipts::Slot::new(credit));
        state.controls.push_back(Control {
            command,
            reply: slot.clone(),
        });
        self.0.wake.notify_all();
        Ok(Ticket(slot))
    }
    pub fn start(&self) -> Result<Ticket, RequestError> {
        self.submit(Command::Start)
    }
    pub fn stop(&self) -> Result<Ticket, RequestError> {
        self.submit(Command::Stop)
    }
    // Independent fence: retained ordinary receipts cannot prevent shutdown.
    pub fn request_shutdown(&self) {
        let mut state = lock(&self.0.state);
        state.status.accepting = false;
        state.shutdown = true;
        self.0.wake.notify_all();
    }
}
pub struct Report {
    pub status: Status,
    // An unresolved accepted pool result stays attached to its exact worker.
    pub retirement: Option<packing::Cancelled>,
    pub last_retirement: Option<packing::CancelReport>,
    _lease: owned::GeneratorLease,
}
pub struct Worker<O> {
    client: Client<O>,
    worker: Option<JoinHandle<Report>>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Worker<O> {
    pub fn spawn(
        database: Arc<Database>,
        snapshots: snapshots::Owner,
        pool: pool::Client,
        config: Config<O>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        if !(1..=1024).contains(&config.controls)
            || !(1..=128).contains(&config.publications)
            || !(1..=100_000).contains(&config.queued_transactions)
            || !(1..=1 << 30).contains(&config.queued_bytes)
            || !(1..=1024).contains(&config.feed_batch)
            || config.poll_interval.is_zero()
            || config.poll_interval > Duration::from_secs(1)
        {
            return Err(Error::Configuration);
        }
        let (view, chain_cursor) = database.event_snapshot().map_err(Error::Repository)?;
        if !snapshots.uses_database(&database)
            || !pool.uses_source(repo::Read::source_id(view.source()))
            || view.identity() != &config.preparation.identity
            || view.identity().consensus != ae_protocol::ConsensusType::Pow
            || !view.identity().matches(&config.preparation.profile.network)
        {
            return Err(Error::Binding);
        }
        let lease = owned::GeneratorLease::acquire(&database).map_err(Error::Ownership)?;
        let tip = view.status().tip;
        let source_id = repo::Read::source_id(view.source());
        drop(view);
        let shared = Arc::new(Shared {
            source_id,
            micro_cycle_ms: config.preparation.micro_cycle_ms,
            state: Mutex::new(State {
                status: Status {
                    running: true,
                    accepting: true,
                    ..Default::default()
                },
                shutdown: false,
                controls: VecDeque::new(),
                candidate: None,
                publications: VecDeque::new(),
            }),
            wake: Condvar::new(),
            controls: Arc::new(Mutex::new(0)),
            publications: Arc::new(Mutex::new(0)),
            candidates: Arc::new(Mutex::new(0)),
            limits: (config.controls, config.publications),
        });
        let service = service::Service::new(
            shared.clone(),
            database.events(),
            chain_cursor,
            tip,
            snapshots,
            pool,
            config,
            clock,
            lease,
        );
        let worker = thread::Builder::new()
            .name("ae-pow-generator".into())
            .spawn(move || service.run())
            .map_err(|e| Error::Worker(e.to_string()))?;
        Ok(Self {
            client: Client(shared),
            worker: Some(worker),
        })
    }
}
impl<O> Worker<O> {
    pub fn client(&self) -> Client<O> {
        self.client.clone()
    }
    pub fn is_finished(&self) -> bool {
        self.worker.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub fn shutdown(mut self) -> thread::Result<Report> {
        self.client.request_shutdown();
        self.worker.take().expect("owned generator").join()
    }
}
impl<O> Drop for Worker<O> {
    fn drop(&mut self) {
        self.client.request_shutdown();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
impl<O> crate::production_group::Child for Worker<O> {
    type Exit = Report;
    fn is_finished(&self) -> bool {
        self.is_finished()
    }
    fn shutdown(self) -> thread::Result<Report> {
        self.shutdown()
    }
}
pub fn retry_delay(failures: u64) -> Duration {
    Duration::from_millis(if failures >= 8 {
        5000
    } else {
        100 << failures.saturating_sub(2)
    })
}
