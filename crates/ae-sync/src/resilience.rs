use crate::owned;
use ae_chain::{repository, resilience as policy};
use ae_storage::{Database, policy as storage};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::watch;

pub use crate::owned::{Fault, Limits as QueueLimits};
pub type Info = owned::Info<Error>;
pub type Ticket<T> = owned::Ticket<T, Error>;
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub initial: policy::Config,
    pub queue: QueueLimits,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            initial: policy::Config {
                depth: 100,
                from_start: false,
            },
            queue: Default::default(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Worker(Fault),
    Storage(repository::Error),
    Closed,
    Revision,
}
impl From<Fault> for Error {
    fn from(e: Fault) -> Self {
        Self::Worker(e)
    }
}
impl From<repository::Error> for Error {
    fn from(e: repository::Error) -> Self {
        Self::Storage(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Configuration {
    pub value: policy::Config,
    pub revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivery {
    Accepted,
    Coalesced,
    Unavailable,
    Rejected(Error),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub configuration: Configuration,
    pub delivery: Delivery,
}
struct Shared {
    storage: storage::Store,
    initial: policy::Config,
    config: Mutex<Configuration>,
    child: Mutex<Weak<Dispatch>>,
    changed: watch::Sender<()>,
    #[cfg(test)]
    startup_gate: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
}
#[derive(Clone)]
pub struct Source(Arc<Shared>);
impl Source {
    pub fn initial(&self) -> policy::Config {
        self.0.initial
    }
    pub fn uses_database(&self, db: &Arc<Database>) -> bool {
        self.0.storage.uses_database(db)
    }
    pub fn is_closed(&self) -> bool {
        self.0.storage.is_closed()
    }
    fn open(&self) -> Result<(), Error> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        if self.0.storage.recovery_required() {
            return Err(repository::Error::RecoveryRequired.into());
        }
        Ok(())
    }
    pub fn configuration(&self) -> Result<Configuration, Error> {
        self.open()?;
        Ok(*self.0.config.lock().unwrap_or_else(|e| e.into_inner()))
    }
    pub fn snapshot(&self) -> Result<policy::Snapshot, Error> {
        Ok(self.0.storage.snapshot()?)
    }
    // This changes external configuration, not cached activation. The callback
    // rereads this source; before first synchronization its cache stays put.
    pub fn configure(&self, value: policy::Config) -> Result<Change, Error> {
        self.open()?;
        let configuration = {
            let mut config = self.0.config.lock().unwrap_or_else(|e| e.into_inner());
            let revision = config.revision.checked_add(1).ok_or(Error::Revision)?;
            *config = Configuration { value, revision };
            *config
        };
        let delivery = self.deliver(Control::Configuration);
        self.0.changed.send_replace(());
        Ok(Change {
            configuration,
            delivery,
        })
    }
    // Called at the actual completion, never by replaying retained sync counts.
    pub fn notify_sync_done(&self) -> Delivery {
        self.deliver(Control::Synced)
    }
    fn deliver(&self, control: Control) -> Delivery {
        if self.open().is_err() {
            return Delivery::Unavailable;
        }
        let child = self
            .0
            .child
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade();
        child.map_or(Delivery::Unavailable, |c| c.deliver(control))
    }
    pub(crate) fn subscribe(&self) -> watch::Receiver<()> {
        self.0.changed.subscribe()
    }
}
pub struct Owner {
    storage: Option<storage::Owner>,
    source: Source,
    queue: QueueLimits,
}
impl Owner {
    pub fn new(db: Arc<Database>, config: Config) -> Result<Self, Error> {
        let storage = storage::Owner::new(db)?;
        let source = Source(Arc::new(Shared {
            storage: storage.store(),
            initial: config.initial,
            config: Mutex::new(Configuration {
                value: config.initial,
                revision: 0,
            }),
            child: Mutex::new(Weak::new()),
            changed: watch::channel(()).0,
            #[cfg(test)]
            startup_gate: Mutex::new(None),
        }));
        Ok(Self {
            storage: Some(storage),
            source,
            queue: config.queue,
        })
    }
    pub fn source(&self) -> Source {
        self.source.clone()
    }
    pub fn start(&self) -> Result<Worker, Error> {
        Worker::spawn(self.source(), self.queue)
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.storage.take();
        self.source.0.changed.send_replace(());
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Control {
    Synced,
    Configuration,
}
impl Control {
    fn index(self) -> usize {
        match self {
            Self::Synced => 0,
            Self::Configuration => 1,
        }
    }
}
#[derive(PartialEq)]
enum Stage {
    Starting,
    Running,
    Stopped,
}
struct Phase {
    stage: Stage,
    pending: [bool; 2],
    deferred: VecDeque<Control>,
    subscribed: bool,
}
struct Dispatch {
    client: owned::Client<State>,
    phase: Mutex<Phase>,
}
impl Dispatch {
    fn deliver(&self, kind: Control) -> Delivery {
        let mut phase = self.phase.lock().unwrap_or_else(|e| e.into_inner());
        if phase.stage == Stage::Stopped || kind == Control::Synced && !phase.subscribed {
            return Delivery::Unavailable;
        }
        let index = kind.index();
        if phase.pending[index] {
            return Delivery::Coalesced;
        }
        phase.pending[index] = true;
        if phase.stage == Stage::Starting {
            phase.deferred.push_back(kind);
            return Delivery::Accepted;
        }
        match self.enqueue(kind) {
            Ok(()) => Delivery::Accepted,
            Err(e) => {
                phase.pending[index] = false;
                Delivery::Rejected(e)
            }
        }
    }
    fn enqueue(&self, kind: Control) -> Result<(), Error> {
        // One executing control plus at most one pending control of each kind.
        // These fixed-size effects share FIFO order with ordinary requests.
        drop(self.client.submit_control(move |s| s.control(kind))?);
        Ok(())
    }
    fn started(&self) -> Result<(), Error> {
        let mut phase = self.phase.lock().unwrap_or_else(|e| e.into_inner());
        if phase.stage != Stage::Starting {
            return Err(Error::Closed);
        }
        phase.stage = Stage::Running;
        while let Some(kind) = phase.deferred.pop_front() {
            self.enqueue(kind)?;
        }
        Ok(())
    }
    fn close(&self) {
        self.phase.lock().unwrap_or_else(|e| e.into_inner()).stage = Stage::Stopped;
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub configuration: Configuration,
    pub policy: policy::Snapshot,
    pub sync_subscribed: bool,
    pub startup_reads: u64,
    pub sync_callbacks: u64,
    pub configuration_callbacks: u64,
}
struct State {
    lease: storage::Lease,
    source: Source,
    dispatch: Arc<Dispatch>,
    sync_callbacks: u64,
    configuration_callbacks: u64,
}
impl owned::Actor for State {
    type Error = Error;
    const CONTROL_REQUESTS: usize = 3;
    fn before(&mut self) -> Result<(), Error> {
        self.source.open()
    }
    fn fatal(&self, _error: &Error) -> bool {
        // A callback failure ends this child. Durable uncertainty separately
        // latches the database and prevents the parent from restarting it.
        true
    }
}
impl Drop for State {
    fn drop(&mut self) {
        self.dispatch.close();
        self.source.0.changed.send_replace(());
    }
}
impl State {
    fn start(&mut self) -> Result<(), Error> {
        let first = self.source.configuration()?.value;
        self.lease.update(
            policy::Event::Start,
            policy::Config {
                from_start: false,
                ..first
            },
        )?;
        #[cfg(test)]
        {
            let gate = self.source.0.startup_gate.lock().unwrap().take();
            if let Some((entered, release)) = gate {
                entered.send(()).unwrap();
                release.recv().unwrap();
            }
        }
        let second = self.source.configuration()?.value;
        self.lease.update(policy::Event::Start, second)?;
        self.source.0.changed.send_replace(());
        Ok(())
    }
    fn control(&mut self, kind: Control) -> Result<(), Error> {
        {
            let mut phase = self
                .dispatch
                .phase
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            phase.pending[kind.index()] = false;
            if kind == Control::Synced {
                if !phase.subscribed {
                    return Ok(());
                }
                phase.subscribed = false;
            }
        }
        let event = match kind {
            Control::Synced => {
                self.sync_callbacks = self.sync_callbacks.saturating_add(1);
                policy::Event::Synced
            }
            Control::Configuration => {
                self.configuration_callbacks = self.configuration_callbacks.saturating_add(1);
                policy::Event::ConfigChanged
            }
        };
        let result = self
            .lease
            .update(event, self.source.configuration()?.value)
            .map_err(Into::into);
        self.source.0.changed.send_replace(());
        result.map(|_| ())
    }
    fn snapshot(&self) -> Result<Snapshot, Error> {
        Ok(Snapshot {
            configuration: self.source.configuration()?,
            policy: self.lease.snapshot()?,
            sync_subscribed: self
                .dispatch
                .phase
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .subscribed,
            startup_reads: 2,
            sync_callbacks: self.sync_callbacks,
            configuration_callbacks: self.configuration_callbacks,
        })
    }
}
#[derive(Clone)]
pub struct Client {
    dispatch: Arc<Dispatch>,
    source: Source,
}
impl Client {
    pub fn source(&self) -> Source {
        self.source.clone()
    }
    pub fn info(&self) -> Info {
        self.dispatch.client.info()
    }
    pub fn snapshot(&self) -> Result<Ticket<Snapshot>, Error> {
        self.dispatch.client.submit(128, |s| s.snapshot())
    }
    pub fn clear_finalized_height(&self) -> Result<Ticket<()>, Error> {
        self.dispatch.client.submit(128, |s| {
            s.lease.clear_finalized_height()?;
            s.source.0.changed.send_replace(());
            Ok(())
        })
    }
}
pub struct Worker {
    inner: Option<owned::Worker<State>>,
    client: Client,
}
impl Worker {
    pub(crate) fn spawn(source: Source, queue: QueueLimits) -> Result<Self, Error> {
        let lease = source.0.storage.acquire()?;
        let created = Arc::new(Mutex::new(None));
        let publish = created.clone();
        let src = source.clone();
        let inner = owned::Worker::spawn_with_client("ae-resilience", queue, move |client| {
            let dispatch = Arc::new(Dispatch {
                client,
                phase: Mutex::new(Phase {
                    stage: Stage::Starting,
                    pending: [false; 2],
                    deferred: VecDeque::new(),
                    subscribed: true,
                }),
            });
            *src.0.child.lock().unwrap_or_else(|e| e.into_inner()) = Arc::downgrade(&dispatch);
            *publish.lock().unwrap_or_else(|e| e.into_inner()) = Some(dispatch.clone());
            let mut state = State {
                lease,
                source: src,
                dispatch,
                sync_callbacks: 0,
                configuration_callbacks: 0,
            };
            state.start()?;
            Ok(state)
        })?;
        let dispatch = created
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("initialized resilience worker");
        let worker = Self {
            inner: Some(inner),
            client: Client { dispatch, source },
        };
        worker.client.dispatch.started()?;
        Ok(worker)
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn info(&self) -> Info {
        self.client.info()
    }
    pub fn request_shutdown(&self) {
        self.client.dispatch.close();
        if let Some(inner) = &self.inner {
            inner.request_shutdown();
        }
    }
    pub fn shutdown(mut self) -> Result<(), Error> {
        self.request_shutdown();
        self.inner
            .take()
            .expect("owned resilience worker")
            .shutdown()
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}

#[cfg(test)]
mod tests;
