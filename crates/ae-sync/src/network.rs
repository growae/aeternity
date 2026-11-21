use crate::{blocking, pool, registry, resolver, transport};
use ae_p2p::{noise, registry as policy, runtime as net};
use ae_protocol::NetworkProfile;
use ae_storage::{Database, pool::worker as admission};
use std::{net::SocketAddrV4, sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

mod service;
pub use service::{Exit, Service};

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: Option<SocketAddrV4>,
    pub commands: usize,
    pub command_bytes: usize,
    pub events: usize,
    pub completions: usize,
    pub retry_interval: Duration,
    pub ping_timeout: Duration,
    pub registry: registry::Config,
    pub worker: registry::worker::Settings,
    pub startup: registry::startup::Configuration,
    pub startup_limits: registry::startup::Settings,
    pub resolver: resolver::Settings,
    pub transport: transport::Settings,
    pub pool: pool::Config,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            listen: Some(SocketAddrV4::new(std::net::Ipv4Addr::UNSPECIFIED, 3015)),
            commands: 256,
            command_bytes: 32 << 20,
            events: 256,
            completions: 512,
            retry_interval: Duration::from_millis(10),
            ping_timeout: Duration::from_secs(60),
            registry: Default::default(),
            worker: Default::default(),
            startup: Default::default(),
            startup_limits: Default::default(),
            resolver: Default::default(),
            transport: transport::Settings {
                runtime: net::Config {
                    defer_pings: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            pool: pool::Config {
                peers: 512,
                ..Default::default()
            },
        }
    }
}
impl Config {
    fn validate(&self) -> Result<(), Error> {
        let frame_bytes = self
            .registry
            .policy
            .pool
            .maximum_host_bytes
            .checked_add(512)
            .ok_or(Error::Configuration)?;
        if [self.commands, self.events, self.completions]
            .iter()
            .any(|n| *n == 0 || *n > 65_536)
            || self.command_bytes < 256
            || self.command_bytes > 1 << 30
            || self.retry_interval.is_zero()
            || self.retry_interval > Duration::from_secs(1)
            || self.ping_timeout.is_zero()
            || tokio::time::Instant::now()
                .checked_add(self.ping_timeout)
                .is_none()
            || self.worker.single_request_bytes < frame_bytes
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Busy,
    Closed,
    Unsupported,
    TimeOverflow,
    Storage,
    Registry(String),
    Resolver(resolver::Error),
    Transport(transport::Error),
    Pool(pool::Error),
    Io(std::io::ErrorKind),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<registry::worker::Error> for Error {
    fn from(error: registry::worker::Error) -> Self {
        Self::Registry(error.to_string())
    }
}
impl From<resolver::Error> for Error {
    fn from(error: resolver::Error) -> Self {
        Self::Resolver(error)
    }
}
impl From<transport::Error> for Error {
    fn from(error: transport::Error) -> Self {
        Self::Transport(error)
    }
}
impl From<pool::Error> for Error {
    fn from(error: pool::Error) -> Self {
        Self::Pool(error)
    }
}
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.kind())
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counts {
    pub accepted: u64,
    pub rejected: u64,
    pub dialed: u64,
    pub probe_alive: u64,
    pub probe_dead: u64,
    pub closed: u64,
    pub pings_completed: u64,
    pub pings_failed: u64,
    pub storage_recoveries: u64,
}
#[derive(Debug, Clone)]
pub struct Status {
    pub running: bool,
    pub ready: bool,
    pub joined: bool,
    pub listening: Option<SocketAddrV4>,
    pub policy: Option<registry::worker::Inspection>,
    pub startup: Option<registry::startup::Progress>,
    pub sockets: usize,
    pub resolutions: usize,
    pub pings: usize,
    pub counts: Counts,
    pub failure: Option<Error>,
}
pub struct Ping {
    pub token: policy::Token,
    pub client: net::Client,
    pub deadline: tokio::time::Instant,
    reply: oneshot::Sender<PingResult>,
}
struct PingResult {
    success: bool,
    processed: Option<oneshot::Sender<()>>,
}
impl Ping {
    // Both completed outcomes reschedule, as native log_ping does. Abandonment
    // or an application deadline instead retires the connection generation.
    pub fn complete(self, success: bool) -> bool {
        self.reply
            .send(PingResult {
                success,
                processed: None,
            })
            .is_ok()
    }
    // A deferred wire callback must retain its owner until the result is
    // recorded. Otherwise a subsequent negotiation failure can retire the
    // connection and discard an already delivered caller result.
    pub async fn complete_and_wait(self, success: bool) -> bool {
        let (processed, receiver) = oneshot::channel();
        if self
            .reply
            .send(PingResult {
                success,
                processed: Some(processed),
            })
            .is_err()
        {
            return false;
        }
        matches!(
            tokio::time::timeout_at(self.deadline, receiver).await,
            Ok(Ok(()))
        )
    }
}
pub enum Event {
    Ping(Box<Ping>),
    Notice(policy::Effect),
}
pub struct Events(mpsc::Receiver<Event>);
impl Events {
    pub async fn recv(&mut self) -> Option<Event> {
        self.0.recv().await
    }
}
struct Response {
    result: Result<policy::Outcome, Error>,
    _slots: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
pub struct Ticket(oneshot::Receiver<Response>);
impl Ticket {
    pub async fn wait(self) -> Result<policy::Outcome, Error> {
        self.0.await.map_err(|_| Error::Closed)?.result
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Connection {
    pub token: policy::Token,
    pub peer: ae_types::PeerPublicKey,
    pub address: std::net::Ipv4Addr,
    pub kind: policy::Kind,
    pub admitted: bool,
    pub closing: bool,
}
pub struct ConnectionTicket {
    accepted: Ticket,
    result: oneshot::Receiver<Option<Connection>>,
}
pub struct PoolCountsTicket {
    accepted: Ticket,
    result: oneshot::Receiver<Option<registry::worker::PoolCounts>>,
}
impl PoolCountsTicket {
    pub async fn wait(self) -> Result<Option<registry::worker::PoolCounts>, Error> {
        self.accepted.wait().await?;
        self.result.await.map_err(|_| Error::Closed)
    }
}
impl ConnectionTicket {
    pub async fn wait(self) -> Result<Option<Connection>, Error> {
        self.accepted.wait().await?;
        self.result.await.map_err(|_| Error::Closed)
    }
}
enum Work {
    Policy(policy::Command),
    PingReceived(net::Client),
    PingClosing {
        client: net::Client,
        close: Option<Box<net::PingClose>>,
    },
    FirstPing {
        client: net::Client,
        port: u16,
        retirement: net::PingRetirement,
    },
    Disconnect(net::Client),
    Suspect(net::Client),
    Synchronize(net::Client),
    Connection {
        client: net::Client,
        reply: Option<oneshot::Sender<Option<Connection>>>,
    },
    Learn {
        client: net::Client,
        peers: Vec<policy::Info>,
    },
    PoolCounts {
        client: net::Client,
        reply: Option<oneshot::Sender<Option<registry::worker::PoolCounts>>>,
    },
}
struct Command {
    work: Work,
    reply: oneshot::Sender<Response>,
    slots: OwnedSemaphorePermit,
    bytes: OwnedSemaphorePermit,
}
impl Command {
    fn finish(self, result: Result<policy::Outcome, Error>) {
        let _ = self.reply.send(Response {
            result,
            _slots: self.slots,
            _bytes: self.bytes,
        });
    }
}
#[derive(Clone)]
pub struct Handle {
    database: std::sync::Weak<Database>,
    network_id: Arc<[u8]>,
    peers: pool::Reader,
    commands: mpsc::Sender<Command>,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    stopped: watch::Sender<bool>,
    status: watch::Receiver<Status>,
    maximum_host_bytes: usize,
    deferred_pings: bool,
}
impl Handle {
    pub fn first_ping(
        &self,
        client: net::Client,
        received: &net::Received,
    ) -> Result<Ticket, Error> {
        if received.generation != *client.generation()
            || received.message().kind() != ae_p2p::message::Kind::Ping
            || received.ping.is_none_or(|p| p.busy)
        {
            return Err(Error::Configuration);
        }
        let port = transport::advertised_port(received.message())?;
        let retirement = client
            .ping_retirement(received)
            .map_err(|e| Error::Transport(transport::Error::Runtime(e)))?;
        self.submit(
            Work::FirstPing {
                client,
                port,
                retirement,
            },
            256,
        )
    }
    pub fn defers_pings(&self) -> bool {
        self.deferred_pings
    }
    pub(crate) fn uses_database(&self, database: &Arc<Database>) -> bool {
        self.database.ptr_eq(&Arc::downgrade(database))
    }
    pub fn peers(&self) -> pool::Reader {
        self.peers.clone()
    }
    pub fn network_id(&self) -> &[u8] {
        &self.network_id
    }
    fn submit(&self, work: Work, weight: usize) -> Result<Ticket, Error> {
        if *self.stopped.borrow() || self.commands.is_closed() {
            return Err(Error::Closed);
        }
        let command = self.commands.try_reserve().map_err(|_| Error::Busy)?;
        let slots = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let weight = u32::try_from(weight).map_err(|_| Error::Busy)?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(weight)
            .map_err(|_| Error::Busy)?;
        let (reply, receiver) = oneshot::channel();
        command.send(Command {
            work,
            reply,
            slots,
            bytes,
        });
        Ok(Ticket(receiver))
    }
    pub fn command(&self, command: policy::Command) -> Result<Ticket, Error> {
        use policy::Command as C;
        let bytes = match &command {
            C::Add(peer) => peer.host.len(),
            C::Resolve { peer, .. } | C::Block(peer) => peer.host.len(),
            C::Delete(_)
            | C::Unblock(_)
            | C::IsBlocked(_)
            | C::Connect(_)
            | C::Probe(_)
            | C::Suspect(_) => 0,
            C::Sample {
                maximum, exclude, ..
            } => maximum
                .checked_mul(128 + self.maximum_host_bytes)
                .and_then(|n| {
                    exclude
                        .len()
                        .checked_mul(32)
                        .and_then(|extra| n.checked_add(extra))
                })
                .ok_or(Error::Busy)?,
            C::SampleConnected { maximum, .. } => maximum
                .checked_mul(128 + self.maximum_host_bytes)
                .ok_or(Error::Busy)?,
            _ => return Err(Error::Unsupported),
        };
        self.submit(
            Work::Policy(command),
            bytes.checked_add(256).ok_or(Error::Busy)?,
        )
    }
    // Call after a permanent connection's unsolicited ping response completes.
    // Temporary connections use ping_closing with the driver's continuation so
    // registry retirement precedes Close and the next incoming callback.
    pub fn ping_received(&self, client: net::Client) -> Result<Ticket, Error> {
        self.submit(Work::PingReceived(client), 256)
    }
    pub fn ping_closing(
        &self,
        client: net::Client,
        close: net::PingClose,
    ) -> Result<Ticket, Error> {
        if close.generation() != client.generation() || !close.is_ready() {
            return Err(Error::Configuration);
        }
        self.submit(
            Work::PingClosing {
                client,
                close: Some(Box::new(close)),
            },
            256,
        )
    }
    pub fn disconnect(&self, client: net::Client) -> Result<Ticket, Error> {
        self.submit(Work::Disconnect(client), 256)
    }
    pub fn suspect(&self, client: net::Client) -> Result<Ticket, Error> {
        self.submit(Work::Suspect(client), 256)
    }
    pub fn synchronize(&self, client: net::Client) -> Result<Ticket, Error> {
        self.submit(Work::Synchronize(client), 256)
    }
    pub fn connection(&self, client: net::Client) -> Result<ConnectionTicket, Error> {
        let (reply, result) = oneshot::channel();
        let accepted = self.submit(
            Work::Connection {
                client,
                reply: Some(reply),
            },
            256,
        )?;
        Ok(ConnectionTicket { accepted, result })
    }
    pub fn pool_counts(&self, client: net::Client) -> Result<PoolCountsTicket, Error> {
        let (reply, result) = oneshot::channel();
        let accepted = self.submit(
            Work::PoolCounts {
                client,
                reply: Some(reply),
            },
            256,
        )?;
        Ok(PoolCountsTicket { accepted, result })
    }
    // The source is the authenticated socket address, never its saved pool
    // grouping address. Accepted rows are independent native-style additions;
    // an error after a prefix is not permission to replay the whole batch.
    pub fn learn(&self, client: net::Client, peers: Vec<policy::Info>) -> Result<Ticket, Error> {
        if peers.len() > 32 {
            return Err(Error::Busy);
        }
        let weight = peers
            .iter()
            .try_fold(256usize, |n, p| {
                n.checked_add(64).and_then(|n| n.checked_add(p.host.len()))
            })
            .ok_or(Error::Busy)?;
        self.submit(Work::Learn { client, peers }, weight)
    }
    pub fn shutdown(&self) {
        self.stopped.send_replace(true);
    }
    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.status.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.status.borrow_and_update().clone())
    }
}

// Provision the storage-bound registry on a joined blocking worker. The Noise
// prologue always uses this database's genesis and the checked network profile.
// No listener or dial starts until the returned Service is driven.
pub async fn create(
    db: Arc<Database>,
    admission: admission::Client,
    identity: Arc<noise::Identity>,
    network: NetworkProfile,
    config: Config,
) -> Result<(Handle, Events, pool::Events, Service), Error> {
    config.validate()?;
    let setup = blocking::Pool::new(1, 1, 256).map_err(|_| Error::Configuration)?;
    let store = db.clone();
    let policy_config = config.registry.clone();
    let local = identity.public_key();
    let pending = setup
        .submit(256, move |_| {
            let binding = store.view().map_err(|_| Error::Storage)?.identity().clone();
            if !binding.matches(&network) {
                return Err(Error::Configuration);
            }
            let registry = registry::Service::start(store, local, policy_config)
                .map_err(|e| Error::Registry(e.to_string()))?;
            Ok((registry, binding))
        })
        .map_err(|_| Error::Storage)?;
    let prepared = pending.await;
    setup.stop();
    setup.join().await;
    let (registry, binding) = prepared.map_err(|_| Error::Storage)??;
    let network = Arc::new(
        noise::Network::new(1, binding.genesis, &binding.network)
            .map_err(|_| Error::Configuration)?,
    );
    let worker = registry::worker::Worker::start(registry, config.worker)?;
    let resolver = resolver::Resolver::start(config.resolver)?;
    let sockets = transport::Sockets::new(identity, network, config.transport)?;
    let database = Arc::downgrade(&db);
    let (peers, incoming, pool_service) = pool::create(db, admission, config.pool)?;
    let (commands, receiver) = mpsc::channel(config.commands);
    let (published, events) = mpsc::channel(config.events);
    let (stop, stopped) = watch::channel(false);
    let current = Status {
        running: true,
        ready: false,
        joined: false,
        listening: None,
        policy: None,
        startup: None,
        sockets: 0,
        resolutions: 0,
        pings: 0,
        counts: Default::default(),
        failure: None,
    };
    let (status, observed) = watch::channel(current.clone());
    let handle = Handle {
        database,
        network_id: binding.network.into(),
        peers: peers.reader(),
        commands,
        slots: Arc::new(Semaphore::new(config.commands)),
        bytes: Arc::new(Semaphore::new(config.command_bytes)),
        stopped: stop,
        status: observed,
        maximum_host_bytes: config.registry.policy.pool.maximum_host_bytes,
        deferred_pings: config.transport.runtime.defer_pings,
    };
    let service = Service::new(
        config,
        worker,
        resolver,
        sockets,
        peers,
        pool_service,
        receiver,
        published,
        stopped,
        status,
        current,
    );
    Ok((handle, Events(events), incoming, service))
}
