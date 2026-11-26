// aec_sync ancestor agreement, successor batches and generation work,
// aeternity fa2a6d7. Copyright (c) 2017, Aeternity Anstalt. ISC: LICENSE.
use super::{Importer, Outcome, blocking, network};
use crate::{peer, tasks::Tasks};
use ae_chain::repository;
use ae_p2p::{
    message::{self, Details, Kind, Message, Value},
    runtime as net,
};
use ae_storage::Database;
use ae_types::{Hash256, PeerPublicKey};
use ae_vm_fate::maps::{MapOrder, order::Otp26};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, watch};

pub mod agreement;
pub mod blocks;
pub mod coordination;
pub mod lifecycle;
pub mod metrics;
mod service;
mod session;
mod shared;
pub mod statistics;
mod wire;
pub use agreement::Point;
pub use service::Service;

#[derive(Debug, Clone)]
pub struct Config {
    pub peers: usize,
    pub concurrent: usize,
    pub forwarded: usize,
    pub headers: usize,
    pub maximum_successors: usize,
    pub probes: usize,
    pub chain_hints: usize,
    pub queued_headers: usize,
    pub forward_chunks: usize,
    pub resilience: ae_chain::resilience::Config,
    pub refresh_interval: Duration,
    pub block_jobs: usize,
    pub block_bytes: usize,
    pub read_threads: usize,
    pub read_jobs: usize,
    pub read_bytes: usize,
    pub timeout: Duration,
    pub capacity_timeout: Duration,
    pub retry_delay: Duration,
    pub messages: message::Limits,
    pub events: crate::events::Config,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            peers: 128,
            concurrent: 8,
            forwarded: 256,
            headers: 100,
            maximum_successors: 4096,
            probes: 256,
            chain_hints: 4096,
            queued_headers: 8192,
            forward_chunks: 15,
            resilience: ae_chain::resilience::Config {
                depth: 100,
                from_start: false,
            },
            refresh_interval: Duration::from_secs(1),
            block_jobs: 8,
            block_bytes: 256 << 20,
            read_threads: 2,
            read_jobs: 8,
            read_bytes: 128 << 20,
            timeout: Duration::from_secs(60),
            capacity_timeout: Duration::from_secs(60),
            retry_delay: Duration::from_millis(25),
            messages: Default::default(),
            events: Default::default(),
        }
    }
}
impl Config {
    fn coordination_limits(&self) -> coordination::Limits {
        coordination::Limits {
            hints: self.chain_hints,
            peers: self.peers,
            items: self.queued_headers,
            chunk: self.headers,
            forward_chunks: self.forward_chunks,
        }
    }

    fn block_weight(&self) -> Option<usize> {
        self.messages
            .body_bytes
            .checked_add(self.messages.work_bytes)?
            .checked_mul(3)
    }

    fn validate(&self) -> Result<(), Error> {
        self.coordination_limits()
            .validate()
            .map_err(|_| Error::Configuration)?;
        if [
            self.peers,
            self.concurrent,
            self.forwarded,
            self.maximum_successors,
            self.probes,
            self.block_jobs,
        ]
        .iter()
        .any(|n| *n == 0 || *n > 65_536)
            || self.block_bytes > 1 << 30
            || self.block_bytes < self.block_weight().unwrap_or(usize::MAX)
            || self.concurrent > self.peers
            || self.headers == 0
            || self.headers > self.maximum_successors
            || self
                .messages
                .body_bytes
                .checked_add(self.messages.work_bytes)
                .is_none_or(|n| n > self.read_bytes)
        {
            return Err(Error::Configuration);
        }
        for duration in [
            self.timeout,
            self.capacity_timeout,
            self.retry_delay,
            self.refresh_interval,
        ] {
            if duration.is_zero() || tokio::time::Instant::now().checked_add(duration).is_none() {
                return Err(Error::Configuration);
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Events(crate::events::Error),
    Configuration,
    Busy,
    Closed,
    Stale,
    Timeout,
    Clock,
    Header,
    Block(ae_chain::block::Error),
    Gossip(ae_chain::gossip::Error),
    Hash,
    KeyExpected,
    MicroExpected,
    NoProgress,
    ResourceLimit,
    WorkerPanicked,
    RecoveryRequired,
    BlockedByWhitelist,
    NonConsecutive,
    MalformedResponse,
    ApplicationClosed,
    Refused(Vec<u8>),
    Coordination(coordination::Error),
    Message(message::Error),
    Network(network::Error),
    Runtime(net::Error),
    Storage(repository::Error),
    Progress(lifecycle::Error),
    Import(super::Failure),
    Policy(super::Error),
    Resilience(crate::resilience::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<coordination::Error> for Error {
    fn from(e: coordination::Error) -> Self {
        Self::Coordination(e)
    }
}
impl From<repository::Error> for Error {
    fn from(e: repository::Error) -> Self {
        Self::Storage(e)
    }
}
impl From<network::Error> for Error {
    fn from(e: network::Error) -> Self {
        Self::Network(e)
    }
}
impl From<net::Error> for Error {
    fn from(e: net::Error) -> Self {
        Self::Runtime(e)
    }
}
impl From<message::Error> for Error {
    fn from(e: message::Error) -> Self {
        Self::Message(e)
    }
}
impl From<blocking::Error> for Error {
    fn from(e: blocking::Error) -> Self {
        match e {
            blocking::Error::Busy => Self::Busy,
            blocking::Error::Panicked => Self::WorkerPanicked,
            blocking::Error::Configuration | blocking::Error::Spawn => Self::Configuration,
            _ => Self::Closed,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Queued,
    Header,
    Matching,
    Waiting,
    Agreement,
    Successors,
    Generation,
    Forward,
    Finished,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    pub phase: Phase,
    pub task: Option<u64>,
    pub members: usize,
    pub buffered: usize,
    // Header announcements and agreement search positions are not admission.
    pub target: Option<Point>,
    pub agreed: Option<Point>,
    // Cursor and selected tip are read back from actual committed storage.
    pub cursor: Option<Point>,
    pub selected: Option<Point>,
    pub queries: u64,
    pub generations: u64,
    pub committed: u64,
    pub present: u64,
    pub retries: u64,
    pub peer_exhausted: bool,
}
impl Default for Progress {
    fn default() -> Self {
        Self {
            phase: Phase::Queued,
            task: None,
            members: 0,
            buffered: 0,
            target: None,
            agreed: None,
            cursor: None,
            selected: None,
            queries: 0,
            generations: 0,
            committed: 0,
            present: 0,
            retries: 0,
            peer_exhausted: false,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finish {
    Forward { requested: Hash256 },
    // The peer supplied an empty successor batch and its final forward
    // generation was imported. This is not global/mainnet synchronization.
    Exhausted { target_selected: bool },
}
#[derive(Debug, Clone)]
pub struct Completion {
    pub peer: PeerPublicKey,
    pub generation: net::Generation,
    pub progress: Progress,
    pub result: Result<Finish, Error>,
}
#[derive(Debug, Clone)]
pub struct Active {
    pub peer: PeerPublicKey,
    pub generation: net::Generation,
    pub progress: Progress,
}
#[derive(Debug, Clone, Default)]
pub struct Counts {
    pub started: u64,
    pub finished: u64,
    pub failed: u64,
    // Active sessions dropped before completion; excludes queued actions.
    pub cancelled: u64,
    pub coalesced: u64,
    pub replaced: u64,
    pub done_announcements: u64,
    pub refused: u64,
}
#[derive(Debug, Clone)]
pub struct Status {
    pub running: bool,
    pub joined: bool,
    pub active: Vec<Active>,
    pub queued: usize,
    pub counts: Counts,
    pub last: Option<Completion>,
    pub last_error: Option<Error>,
    pub blocks: blocks::Status,
    pub lifecycle: lifecycle::Status,
    pub statistics: statistics::Status,
    pub metrics: metrics::Status,
}
#[derive(Clone)]
pub struct Handle {
    membership: PeerSet,
    database: std::sync::Weak<Database>,
    event_owner: Arc<()>,
    stop: watch::Sender<bool>,
    resilience: watch::Sender<ae_chain::resilience::Config>,
    policy_source: Option<crate::resilience::Source>,
    status: watch::Receiver<Status>,
    events: crate::events::Feed<lifecycle::Event>,
    logs: crate::events::Feed<metrics::Log>,
    logs_origin: crate::events::Cursor,
}
impl Handle {
    pub fn peers_in_sync(&self) -> Result<Vec<PeerPublicKey>, Error> {
        self.membership.peers()
    }
    pub fn subscribe_logs(
        &self,
        after: Option<crate::events::Cursor>,
    ) -> Result<crate::events::Subscription<metrics::Log>, crate::events::Error> {
        self.logs
            .subscribe(Some(after.unwrap_or_else(|| self.logs_origin.clone())))
    }
    pub fn events(&self) -> crate::events::Feed<lifecycle::Event> {
        self.events.clone()
    }
    pub(crate) fn uses_database(&self, db: &Arc<Database>) -> bool {
        self.database.ptr_eq(&Arc::downgrade(db))
    }
    pub(crate) fn owns(&self, db: &Arc<Database>, events: &Events) -> bool {
        self.database.ptr_eq(&Arc::downgrade(db)) && Arc::ptr_eq(&self.event_owner, &events.owner)
    }
    // Updates are coalesced to the latest configuration, like a config snapshot.
    pub fn set_resilience(&self, config: ae_chain::resilience::Config) -> Result<(), Error> {
        if let Some(source) = &self.policy_source {
            if *self.stop.borrow() || self.resilience.is_closed() {
                return Err(Error::Closed);
            }
            let change = source.configure(config).map_err(Error::Resilience)?;
            if let crate::resilience::Delivery::Rejected(error) = change.delivery {
                return Err(Error::Resilience(error));
            }
            Ok(())
        } else {
            self.resilience.send(config).map_err(|_| Error::Closed)
        }
    }

    pub fn shutdown(&self) {
        self.stop.send_replace(true);
    }
    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.status.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.status.borrow_and_update().clone())
    }
}
// Retains the original application's byte permit. Capabilities, unhandled
// messages and status events remain available to the rest of the node.
pub struct Events {
    receiver: mpsc::Receiver<peer::Event>,
    owner: Arc<()>,
}
impl Events {
    pub async fn recv(&mut self) -> Option<peer::Event> {
        self.receiver.recv().await
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Start(Hash256),
    Forward(Hash256),
}
type HeaderLane = (net::Generation, Weak<tokio::sync::Mutex<()>>);
struct Context<O> {
    importer: Arc<Importer<O>>,
    network: network::Handle,
    config: Config,
    reads: blocking::Pool,
    policy_source: Option<crate::resilience::Source>,
    stopped: AtomicBool,
    wake: watch::Sender<()>,
    shared: Mutex<shared::Book<O>>,
    headers: Mutex<BTreeMap<PeerPublicKey, HeaderLane>>,
}
trait PeerMembership: Send + Sync {
    fn peers_in_sync(&self) -> Result<Vec<PeerPublicKey>, Error>;
}
// A connection group binds to one synchronizer. Readers must not keep that
// owner alive, or acquire a replacement through an unrelated global slot.
#[derive(Clone)]
pub(crate) struct PeerSet(Weak<dyn PeerMembership>);
impl PeerSet {
    fn peers(&self) -> Result<Vec<PeerPublicKey>, Error> {
        self.0.upgrade().ok_or(Error::Closed)?.peers_in_sync()
    }
    pub(crate) fn contains(&self, peer: PeerPublicKey) -> bool {
        // An unavailable owner provides no evidence for suppressing a ping.
        self.peers().is_ok_and(|peers| peers.contains(&peer))
    }
}
impl<O: MapOrder + Send + Sync + 'static> PeerMembership for Context<O> {
    fn peers_in_sync(&self) -> Result<Vec<PeerPublicKey>, Error> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        Ok(self
            .shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .peers_in_sync())
    }
}
pub fn create<O: MapOrder + Send + Sync + 'static>(
    importer: Importer<O>,
    network: network::Handle,
    application: peer::Handle,
    incoming: peer::Events,
    config: Config,
) -> Result<(Handle, Events, Service<O>), Error> {
    create_bound(importer, network, application, incoming, config, None)
}
pub(crate) fn create_bound<O: MapOrder + Send + Sync + 'static>(
    importer: Importer<O>,
    network: network::Handle,
    application: peer::Handle,
    incoming: peer::Events,
    config: Config,
    binding: Option<crate::bindings::Binding<crate::resilience::Client>>,
) -> Result<(Handle, Events, Service<O>), Error> {
    config.validate()?;
    let policy_source = match binding {
        Some(binding) => {
            if !binding.uses_database(&importer.context.db) {
                return Err(Error::Configuration);
            }
            // Withdrawal retains the previous client descriptor. Its source
            // belongs to the same parent, so a child replacement between the
            // dependency wait and this constructor need not restart sync.
            let held = binding.snapshot();
            if held.closed {
                return Err(Error::Closed);
            }
            let source = held.value.ok_or(Error::Closed)?.source();
            // A group restart must not overwrite the parent's live external config.
            if source.is_closed() {
                return Err(Error::Closed);
            }
            if !source.uses_database(&importer.context.db) || source.initial() != config.resilience
            {
                return Err(Error::Configuration);
            }
            Some(source)
        }
        None => None,
    };
    if !network.uses_database(&importer.context.db)
        || !application.uses_database(&importer.context.db)
        || importer
            .download_weight()
            .checked_mul(3)
            .is_none_or(|n| n > config.block_bytes)
    {
        return Err(Error::Configuration);
    }
    let mut book = shared::Book::with_events(config.events)?;
    book.policy_source = policy_source.clone();
    let business_events = book.feed();
    let (metrics, logs, logs_origin) = metrics::Owner::new(config.events)?;
    let reads = blocking::Pool::named(
        "ae-chain-sync",
        config.read_threads,
        config.read_jobs,
        config.read_bytes,
    )?;
    let (send, events) = mpsc::channel(config.forwarded);
    let (stop, stopped) = watch::channel(false);
    let (resilience, policy_config) = watch::channel(config.resilience);
    let (wake, changed) = watch::channel(());
    let current = Status {
        running: true,
        joined: false,
        active: vec![],
        queued: 0,
        counts: Counts::default(),
        last: None,
        last_error: None,
        blocks: Default::default(),
        lifecycle: Default::default(),
        statistics: Default::default(),
        metrics: metrics.status(),
    };
    let (status, observed) = watch::channel(current.clone());
    let database = Arc::downgrade(&importer.context.db);
    let event_owner = Arc::new(());
    let context = Arc::new(Context {
        importer: Arc::new(importer),
        network,
        config,
        reads,
        policy_source: policy_source.clone(),
        stopped: AtomicBool::new(false),
        wake,
        shared: Mutex::new(book),
        headers: Mutex::new(BTreeMap::new()),
    });
    let membership: Arc<dyn PeerMembership> = context.clone();
    let membership = PeerSet(Arc::downgrade(&membership));
    application
        .bind_sync(membership.clone())
        .map_err(|_| Error::Configuration)?;
    Ok((
        Handle {
            membership,
            database,
            event_owner: event_owner.clone(),
            stop,
            resilience,
            policy_source,
            status: observed,
            events: business_events,
            logs,
            logs_origin,
        },
        Events {
            receiver: events,
            owner: event_owner,
        },
        Service::new(
            context,
            metrics,
            application,
            incoming,
            send,
            stopped,
            changed,
            status,
            current,
            policy_config,
        ),
    ))
}

#[cfg(test)]
mod tests;
