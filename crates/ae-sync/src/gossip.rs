use crate::{blocking, pool, tasks::Tasks};
use ae_mempool::{hc_votes as votes, pool::Signed};
use ae_p2p::{
    message::{self, Kind, Message, Value},
    runtime as net,
};
use ae_protocol::{ConsensusType, NetworkProfile, Protocol};
use ae_storage::{Database, pool::worker};
use ae_tx::TransactionKind;
use ae_types::Unsigned;
use std::{
    collections::{BTreeMap, VecDeque, hash_map::RandomState},
    hash::BuildHasher,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
mod operations;
mod service;
pub use service::Service;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    VoteWorker(crate::votes::Error),
    Busy,
    Closed,
    Timeout,
    Storage,
    Protocol,
    IdExhausted,
    Worker(worker::Error),
    Votes(votes::Error),
    Feed(worker::events::ReadError),
    SnapshotIncomplete { visible: usize, total: usize },
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<blocking::Error> for Error {
    fn from(e: blocking::Error) -> Self {
        match e {
            blocking::Error::Busy => Self::Busy,
            blocking::Error::Configuration => Self::Configuration,
            _ => Self::Closed,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    pub enabled: bool,
    pub max_peers: usize,
    pub snapshot_limit: usize,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_peers: 16,
            snapshot_limit: 100_000,
        }
    }
}
impl Policy {
    fn validate(self) -> Result<(), Error> {
        if self.max_peers > 65_536 || self.snapshot_limit > 1_000_000 {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub commands: usize,
    pub command_bytes: usize,
    pub forwarded: usize,
    pub operations: usize,
    pub blocking_threads: usize,
    pub blocking_jobs: usize,
    pub blocking_bytes: usize,
    pub vote_events: usize,
    pub input_bytes: usize,
    pub input_items: usize,
    pub feed_batch: usize,
    pub work_timeout: Duration,
    pub fanout_timeout: Duration,
    pub messages: message::Limits,
    pub votes: votes::Limits,
    pub policy: Policy,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            commands: 64,
            command_bytes: 64 * 1024 * 1024,
            forwarded: 64,
            operations: 8,
            blocking_threads: 2,
            blocking_jobs: 8,
            blocking_bytes: 64 * 1024 * 1024,
            vote_events: 64,
            input_bytes: 16 * 1024 * 1024,
            input_items: 100_000,
            feed_batch: 64,
            work_timeout: Duration::from_secs(330),
            fanout_timeout: Duration::from_secs(33),
            messages: Default::default(),
            votes: Default::default(),
            policy: Default::default(),
        }
    }
}
impl Config {
    fn validate(self) -> Result<(), Error> {
        let max = Semaphore::MAX_PERMITS.min(u32::MAX as usize);
        for n in [
            self.commands,
            self.command_bytes,
            self.forwarded,
            self.operations,
            self.vote_events,
            self.input_bytes,
            self.input_items,
            self.feed_batch,
        ] {
            if n == 0 || n > max {
                return Err(Error::Configuration);
            }
        }
        if self.blocking_bytes < self.input_bytes || self.command_bytes < 128 {
            return Err(Error::Configuration);
        }
        for d in [self.work_timeout, self.fanout_timeout] {
            if d.is_zero() || tokio::time::Instant::now().checked_add(d).is_none() {
                return Err(Error::Configuration);
            }
        }
        self.policy.validate()
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Feed {
    Live,
    Recovering,
    Paused(Error),
    Closed,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counts {
    pub decoded: u64,
    pub malformed: u64,
    pub ordinary_admitted: u64,
    pub rejected: u64,
    pub votes_admitted: u64,
    pub fanout_events: u64,
    pub sent: u64,
    pub send_failed: u64,
    pub suppressed: u64,
    pub gaps: u64,
    pub recovered: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub running: bool,
    pub policy: Policy,
    pub feed: Feed,
    pub operations: usize,
    pub counts: Counts,
    pub last_error: Option<Error>,
}
pub struct Events(mpsc::Receiver<pool::Forwarded>);
impl Events {
    pub async fn recv(&mut self) -> Option<pool::Forwarded> {
        self.0.recv().await
    }
}
pub struct Ticket<T>(oneshot::Receiver<Result<T, Error>>);
impl<T> Ticket<T> {
    pub async fn wait(self) -> Result<T, Error> {
        self.0.await.unwrap_or(Err(Error::Closed))
    }
}
enum Command {
    Vote {
        bytes: Vec<u8>,
        event: votes::Event,
        reply: oneshot::Sender<Result<votes::PushOutcome, Error>>,
        _permit: OwnedSemaphorePermit,
    },
    Epoch {
        epoch: Unsigned,
        reply: oneshot::Sender<Result<(), Error>>,
        _permit: OwnedSemaphorePermit,
    },
    Peek {
        epoch: Unsigned,
        kind: Option<Unsigned>,
        maximum: usize,
        reply: oneshot::Sender<Result<crate::votes::Batch, Error>>,
        _permit: OwnedSemaphorePermit,
    },
}
#[derive(Clone)]
pub struct Handle {
    database: std::sync::Weak<Database>,
    commands: mpsc::Sender<Command>,
    bytes: Arc<Semaphore>,
    policy: watch::Sender<Policy>,
    stop: watch::Sender<bool>,
    status: watch::Receiver<Status>,
    config: Config,
}
impl Handle {
    pub(crate) fn uses_database(&self, db: &Arc<Database>) -> bool {
        self.database.ptr_eq(&Arc::downgrade(db))
    }
    fn reserve(
        &self,
        bytes: usize,
    ) -> Result<(mpsc::Permit<'_, Command>, OwnedSemaphorePermit), Error> {
        if *self.stop.borrow() || self.commands.is_closed() {
            return Err(Error::Closed);
        }
        let slot = self.commands.try_reserve().map_err(|_| Error::Busy)?;
        let amount = u32::try_from(bytes).map_err(|_| Error::Busy)?;
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(amount)
            .map_err(|_| Error::Busy)?;
        Ok((slot, permit))
    }
    pub fn push_vote(
        &self,
        bytes: Vec<u8>,
        event: votes::Event,
    ) -> Result<Ticket<votes::PushOutcome>, Error> {
        if bytes.len() > self.config.input_bytes {
            return Err(Error::Busy);
        }
        let (slot, permit) = self.reserve(bytes.len().saturating_add(128))?;
        let (reply, wait) = oneshot::channel();
        slot.send(Command::Vote {
            bytes,
            event,
            reply,
            _permit: permit,
        });
        Ok(Ticket(wait))
    }
    pub fn advance_epoch(&self, epoch: Unsigned) -> Result<Ticket<()>, Error> {
        let (slot, permit) = self.reserve(epoch.as_bytes().len().saturating_add(128))?;
        let (reply, wait) = oneshot::channel();
        slot.send(Command::Epoch {
            epoch,
            reply,
            _permit: permit,
        });
        Ok(Ticket(wait))
    }
    pub fn peek_votes(
        &self,
        epoch: Unsigned,
        kind: Option<Unsigned>,
        maximum: usize,
    ) -> Result<Ticket<crate::votes::Batch>, Error> {
        if maximum > self.config.votes.maximum_entries {
            return Err(Error::Busy);
        }
        let weight = epoch
            .as_bytes()
            .len()
            .saturating_add(kind.as_ref().map_or(0, |k| k.as_bytes().len()))
            .saturating_add(128);
        let (slot, permit) = self.reserve(weight)?;
        let (reply, wait) = oneshot::channel();
        slot.send(Command::Peek {
            epoch,
            kind,
            maximum,
            reply,
            _permit: permit,
        });
        Ok(Ticket(wait))
    }
    pub fn set_policy(&self, policy: Policy) -> Result<(), Error> {
        policy.validate()?;
        if *self.stop.borrow() || self.commands.is_closed() {
            return Err(Error::Closed);
        }
        self.policy.send_replace(policy);
        Ok(())
    }
    pub fn retry_feed(&self) -> Result<(), Error> {
        let policy = *self.policy.borrow();
        self.set_policy(policy)
    }
    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.status.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.status.borrow_and_update().clone())
    }
    pub fn shutdown(&self) {
        self.stop.send_replace(true);
    }
}
#[derive(Clone)]
struct Context {
    db: Arc<Database>,
    admission: worker::Client,
    network: NetworkProfile,
    consensus: ConsensusType,
    votes: Arc<Mutex<votes::VotePool>>,
    managed_votes: Option<crate::votes::Binding>,
    vote_replies: Arc<Semaphore>,
    published: mpsc::Sender<Arc<Signed>>,
    config: Config,
}
#[allow(clippy::too_many_arguments)]
pub fn create(
    db: Arc<Database>,
    admission: worker::Client,
    peers: impl Into<pool::Reader>,
    incoming: pool::Events,
    network: NetworkProfile,
    consensus: ConsensusType,
    config: Config,
) -> Result<(Handle, Events, Service), Error> {
    if !admission.uses_database(&db) {
        return Err(Error::Configuration);
    }
    let binding = crate::bindings::Admission::fixed(&db, admission);
    create_bound(db, binding, peers, incoming, network, consensus, config)
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_bound(
    db: Arc<Database>,
    binding: crate::bindings::Admission,
    peers: impl Into<pool::Reader>,
    incoming: pool::Events,
    network: NetworkProfile,
    consensus: ConsensusType,
    config: Config,
) -> Result<(Handle, Events, Service), Error> {
    create_with_votes(
        db, binding, peers, incoming, network, consensus, config, None,
    )
}
#[allow(clippy::too_many_arguments)]
pub fn create_with_votes(
    db: Arc<Database>,
    binding: crate::bindings::Admission,
    peers: impl Into<pool::Reader>,
    incoming: pool::Events,
    network: NetworkProfile,
    consensus: ConsensusType,
    config: Config,
    managed_votes: Option<crate::votes::Binding>,
) -> Result<(Handle, Events, Service), Error> {
    if binding.managed() && (consensus == ConsensusType::Pos) != managed_votes.is_some() {
        return Err(Error::Configuration);
    }
    if let Some(votes) = &managed_votes
        && (consensus != ConsensusType::Pos || !votes.uses_database(&db))
    {
        return Err(Error::Configuration);
    }
    let vote_consumer = managed_votes
        .as_ref()
        .map(|v| {
            v.current()
                .ok_or(crate::votes::Error::Worker(crate::votes::Fault::Stopped))?
                .publications()
                .listen()
        })
        .transpose()
        .map_err(Error::VoteWorker)?;
    config.validate()?;
    if !binding.uses_database(&db) {
        return Err(Error::Configuration);
    }
    let snapshot = binding.snapshot();
    let bound = snapshot.value.ok_or(Error::Configuration)?;
    let admission = bound.client;

    if !db
        .view()
        .map_err(|_| Error::Storage)?
        .identity()
        .matches(&network)
    {
        return Err(Error::Configuration);
    }
    let jobs = blocking::Pool::new(
        config.blocking_threads,
        config.blocking_jobs,
        config.blocking_bytes,
    )?;
    // One reserved waiter cannot consume the workers needed to admit the event
    // that wakes it. Its Condvar wait checks cancellation every 100 milliseconds.
    let feed_jobs = blocking::Pool::new(1, 1, 128)?;
    let (commands, receiver) = mpsc::channel(config.commands);
    let (forwarded, events) = mpsc::channel(config.forwarded);
    let (published, vote_events) = mpsc::channel(config.vote_events);
    let (policy, policies) = watch::channel(config.policy);
    let (stop, stopped) = watch::channel(false);
    let current = Status {
        running: true,
        policy: config.policy,
        feed: Feed::Live,
        operations: 0,
        counts: Default::default(),
        last_error: None,
    };
    let (status, view) = watch::channel(current.clone());
    let cursor = bound.after;
    let database = Arc::downgrade(&db);
    let context = Context {
        db,
        admission,
        network,
        consensus,
        votes: Arc::new(Mutex::new(Default::default())),
        managed_votes,
        vote_replies: Arc::new(Semaphore::new(config.command_bytes)),
        published,
        config,
    };
    let service = Service::new(
        context,
        peers.into(),
        incoming,
        receiver,
        forwarded,
        vote_events,
        vote_consumer,
        policies,
        stopped,
        status,
        current,
        cursor,
        jobs,
        feed_jobs,
        binding,
        snapshot.lease,
        snapshot.generation,
    );
    Ok((
        Handle {
            database,
            commands,
            bytes: Arc::new(Semaphore::new(config.command_bytes)),
            policy,
            stop,
            status: view,
            config,
        },
        Events(events),
        service,
    ))
}
#[derive(Clone, PartialEq, Eq)]
enum Key {
    Feed,
    Fanout,
    Operation(u64),
}
enum FeedResult {
    Batch(worker::events::Batch),
    Snapshot(worker::events::Snapshot),
}
#[derive(Default)]
struct Input {
    decoded: u64,
    malformed: u64,
    admitted: u64,
    rejected: u64,
    votes: u64,
}
struct Fanout {
    sent: u64,
    failed: u64,
}
enum Done {
    Feed(Result<FeedResult, Error>),
    Fanout(Result<Fanout, Error>),
    Operation(Result<Input, Error>),
}
