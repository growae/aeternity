// aec_sync ancestor agreement, successor batches and generation work,
// aeternity fa2a6d7. Copyright (c) 2017, Aeternity Anstalt. ISC: LICENSE.
use super::{Importer, Outcome, blocking, network};
use crate::{peer, tasks::Tasks};
use ae_chain::repository;
use ae_p2p::{
    message::{self, Details, Kind, Message, Value},
    runtime as net,
};
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
pub mod coordination;
mod service;
mod session;
mod shared;
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
    pub read_threads: usize,
    pub read_jobs: usize,
    pub read_bytes: usize,
    pub timeout: Duration,
    pub capacity_timeout: Duration,
    pub retry_delay: Duration,
    pub messages: message::Limits,
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
            read_threads: 2,
            read_jobs: 8,
            read_bytes: 128 << 20,
            timeout: Duration::from_secs(60),
            capacity_timeout: Duration::from_secs(60),
            retry_delay: Duration::from_millis(25),
            messages: Default::default(),
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
        ]
        .iter()
        .any(|n| *n == 0 || *n > 65_536)
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
        for duration in [self.timeout, self.capacity_timeout, self.retry_delay] {
            if duration.is_zero() || tokio::time::Instant::now().checked_add(duration).is_none() {
                return Err(Error::Configuration);
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Busy,
    Closed,
    Stale,
    Timeout,
    Clock,
    Header,
    Hash,
    KeyExpected,
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
    Import(super::Failure),
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
}
#[derive(Clone)]
pub struct Handle {
    stop: watch::Sender<bool>,
    status: watch::Receiver<Status>,
}
impl Handle {
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
pub struct Events(mpsc::Receiver<peer::Event>);
impl Events {
    pub async fn recv(&mut self) -> Option<peer::Event> {
        self.0.recv().await
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
    stopped: AtomicBool,
    wake: watch::Sender<()>,
    shared: Mutex<shared::Book<O>>,
    headers: Mutex<BTreeMap<PeerPublicKey, HeaderLane>>,
}
pub fn create<O: MapOrder + Send + Sync + 'static>(
    importer: Importer<O>,
    network: network::Handle,
    application: peer::Handle,
    incoming: peer::Events,
    config: Config,
) -> Result<(Handle, Events, Service<O>), Error> {
    config.validate()?;
    if !network.uses_database(&importer.context.db)
        || !application.uses_database(&importer.context.db)
    {
        return Err(Error::Configuration);
    }
    let reads = blocking::Pool::named(
        "ae-chain-sync",
        config.read_threads,
        config.read_jobs,
        config.read_bytes,
    )?;
    let (send, events) = mpsc::channel(config.forwarded);
    let (stop, stopped) = watch::channel(false);
    let (wake, changed) = watch::channel(());
    let current = Status {
        running: true,
        joined: false,
        active: vec![],
        queued: 0,
        counts: Counts::default(),
        last: None,
        last_error: None,
    };
    let (status, observed) = watch::channel(current.clone());
    let context = Arc::new(Context {
        importer: Arc::new(importer),
        network,
        config,
        reads,
        stopped: AtomicBool::new(false),
        wake,
        shared: Mutex::new(shared::Book::new()),
        headers: Mutex::new(BTreeMap::new()),
    });
    Ok((
        Handle {
            stop,
            status: observed,
        },
        Events(events),
        Service::new(
            context,
            application,
            incoming,
            send,
            stopped,
            changed,
            status,
            current,
        ),
    ))
}

#[cfg(test)]
mod tests;
