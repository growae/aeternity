use crate::{blocking, gossip, network, tasks::Tasks};
use ae_p2p::{
    message::{self, ChainPoi},
    ping, registry as policy, runtime as net,
};
use ae_storage::Database;
use ae_types::{PeerPublicKey, Unsigned};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

pub mod capabilities;
use capabilities::worker as capability_worker;
pub mod info;
mod lane;
mod service;
pub mod snapshot;
pub use service::Service;

#[derive(Debug, Clone)]
pub struct Config {
    pub connections: usize,
    pub queued_per_connection: usize,
    pub events: usize,
    pub event_bytes: usize,
    pub blocking_threads: usize,
    pub blocking_jobs: usize,
    pub blocking_bytes: usize,
    pub operation_timeout: Duration,
    pub port: u16,
    pub port_mapping: Option<crate::upnp::ListenerSource>,
    pub ping: ping::Config,
    pub messages: message::Limits,
    pub capabilities: Vec<ChainPoi>,
    pub capability_limits: capabilities::Limits,
    pub node_info: Option<info::Metadata>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            connections: 512,
            queued_per_connection: 16,
            events: 256,
            event_bytes: 128 << 20,
            blocking_threads: 2,
            blocking_jobs: 8,
            blocking_bytes: 128 << 20,
            operation_timeout: Duration::from_secs(60),
            port: 3015,
            port_mapping: None,
            ping: Default::default(),
            messages: Default::default(),
            capabilities: vec![],
            capability_limits: Default::default(),
            node_info: Some(info::Metadata::current()),
        }
    }
}
impl Config {
    fn validate(&self) -> Result<(), Error> {
        if [self.connections, self.queued_per_connection, self.events]
            .iter()
            .any(|n| *n == 0 || *n > 65_536)
            || self.event_bytes < 256
            || self.event_bytes > 1 << 30
            || self.blocking_bytes < 256
            || self
                .messages
                .body_bytes
                .checked_add(self.messages.work_bytes)
                .is_none_or(|n| n > self.blocking_bytes)
            || self.operation_timeout.is_zero()
            || tokio::time::Instant::now()
                .checked_add(self.operation_timeout)
                .is_none()
        {
            return Err(Error::Configuration);
        }
        self.ping.validate().map_err(Error::Ping)?;
        self.capability_limits.validate()?;
        if let Some(metadata) = &self.node_info {
            metadata.validate().map_err(Error::Message)?;
        }
        message::encode_capabilities(&self.capabilities, self.messages).map_err(Error::Message)?;
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
    MalformedCapabilities,
    Capabilities(capabilities::Error),
    CapabilityWorker(capability_worker::Fault),
    Storage(String),
    Network(network::Error),
    Runtime(net::Error),
    Ping(ping::Error),
    Message(message::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<capabilities::Error> for Error {
    fn from(e: capabilities::Error) -> Self {
        Self::Capabilities(e)
    }
}
impl From<capability_worker::Error> for Error {
    fn from(e: capability_worker::Error) -> Self {
        match e {
            capability_worker::Error::Registry(e) => Self::Capabilities(e),
            capability_worker::Error::Worker(capability_worker::Fault::Stopped) => {
                Self::Capabilities(capabilities::Error::Closed)
            }
            capability_worker::Error::Worker(e) => Self::CapabilityWorker(e),
        }
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
impl From<ping::Error> for Error {
    fn from(e: ping::Error) -> Self {
        Self::Ping(e)
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
            _ => Self::Closed,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainIntent {
    Generation(Vec<u8>),
    Done,
    Start { best: Vec<u8>, difficulty: Unsigned },
}
pub enum EventKind {
    // These are untrusted announcements and native synchronization decisions.
    // Delivery is not a claim that a block was fetched, validated or selected.
    Chain {
        client: net::Client,
        intent: ChainIntent,
    },
    Status {
        client: net::Client,
        temporary: bool,
        genesis: Vec<u8>,
        best: Vec<u8>,
        difficulty: Unsigned,
    },
    Capabilities {
        client: net::Client,
        chain_poi: ChainPoi,
    },
    Unhandled {
        client: net::Client,
        message: net::Received,
    },
    Network(policy::Effect),
}
pub struct Event {
    kind: EventKind,
    _bytes: OwnedSemaphorePermit,
}
impl Event {
    pub(crate) fn into_kind(self) -> EventKind {
        self.kind
    }
    pub fn kind(&self) -> &EventKind {
        &self.kind
    }
}
pub struct Events(mpsc::Receiver<Event>);
impl Events {
    pub async fn recv(&mut self) -> Option<Event> {
        self.0.recv().await
    }
}
#[derive(Clone)]
struct Publisher {
    events: mpsc::Sender<Event>,
    bytes: Arc<Semaphore>,
}
impl Publisher {
    fn send(&self, kind: EventKind, weight: usize) -> Result<(), Error> {
        let bytes =
            u32::try_from(weight.checked_add(256).ok_or(Error::Busy)?).map_err(|_| Error::Busy)?;
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| Error::Busy)?;
        self.events
            .try_send(Event {
                kind,
                _bytes: permit,
            })
            .map_err(|_| Error::Busy)
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counts {
    pub incoming_pings: u64,
    pub outgoing_pings: u64,
    pub probes: u64,
    pub rejected: u64,
    pub served: u64,
    pub failed_connections: u64,
    pub analytics_submitted: u64,
    pub analytics_rejected: u64,
}
#[derive(Debug, Clone)]
pub struct Status {
    pub running: bool,
    pub joined: bool,
    pub connections: usize,
    pub counts: Counts,
    pub last_error: Option<Error>,
}
#[derive(Clone)]
pub struct Handle {
    db: Arc<Database>,
    stop: watch::Sender<bool>,
    target: watch::Sender<u64>,
    status: watch::Receiver<Status>,
    capabilities: CapabilityStore,
}
type CapabilityStore = capability_worker::Binding;
impl Handle {
    pub fn shutdown(&self) {
        self.stop.send_replace(true);
    }
    fn capability_open(&self) -> Result<(), Error> {
        if *self.stop.borrow() {
            Err(Error::Capabilities(capabilities::Error::Closed))
        } else {
            Ok(())
        }
    }
    pub async fn set_capability(
        &self,
        key: &str,
        value: &capabilities::Value,
    ) -> Result<(), Error> {
        self.capability_open()?;
        Ok(capability_worker::call(&self.capabilities, |c| c.set(key, value)).await?)
    }
    pub async fn delete_capability(&self, key: &str) -> Result<(), Error> {
        self.capability_open()?;
        Ok(capability_worker::call(&self.capabilities, |c| c.delete(key)).await?)
    }
    pub async fn capability(&self, key: &str) -> Result<Option<capabilities::Value>, Error> {
        self.capability_open()?;
        Ok(capability_worker::call(&self.capabilities, |c| c.get(key)).await?)
    }
    pub async fn capabilities(&self) -> Result<BTreeMap<String, capabilities::Value>, Error> {
        self.capability_open()?;
        Ok(capability_worker::call(&self.capabilities, |c| c.local()).await?)
    }
    pub async fn peers_with_capability(
        &self,
        key: &str,
    ) -> Result<Vec<capabilities::PeerValue<net::Generation>>, Error> {
        self.capability_open()?;
        Ok(capability_worker::call(&self.capabilities, |c| c.peers_with(key)).await?)
    }
    // A tracked header target supplied by the synchronizer, not a selected
    // height or proof of block validity. Ping announcements do not set it.
    pub fn set_sync_target(&self, height: u64) -> Result<(), Error> {
        if *self.stop.borrow() || !self.status.borrow().running {
            return Err(Error::Closed);
        }
        self.target.send_replace(height);
        Ok(())
    }
    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }
    pub(crate) fn uses_database(&self, db: &Arc<Database>) -> bool {
        Arc::ptr_eq(&self.db, db)
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.status.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.status.borrow_and_update().clone())
    }
}
enum Input {
    Message(net::Received),
    Ping(Box<network::Ping>),
}
#[derive(Clone)]
struct Context {
    analytics: Option<crate::bindings::PeerAnalytics>,
    db: Arc<Database>,
    network: network::Handle,
    config: Arc<Config>,
    published: Publisher,
    target: watch::Receiver<u64>,
    progress: watch::Sender<Counts>,
    storage_bytes: Arc<Semaphore>,
    capabilities: CapabilityStore,
}
struct Lane {
    client: net::Client,
    input: mpsc::Sender<Input>,
    _lease: capability_worker::Lease,
}
impl Drop for Lane {
    fn drop(&mut self) {
        self.client.shutdown();
    }
}

// The caller drives network and gossip alongside this service and joins them on
// shutdown. This owner drives every application lane and joins its storage reads.
pub fn create(
    db: Arc<Database>,
    network: network::Handle,
    scheduled: network::Events,
    incoming: gossip::Events,
    config: Config,
) -> Result<(Handle, Events, Service), Error> {
    create_bound(db, network, scheduled, incoming, config, None, None)
}
pub fn create_with_analytics(
    db: Arc<Database>,
    network: network::Handle,
    scheduled: network::Events,
    incoming: gossip::Events,
    config: Config,
    analytics: crate::analytics::Client,
) -> Result<(Handle, Events, Service), Error> {
    let binding = crate::bindings::PeerAnalytics::fixed(&db, analytics);
    create_bound(
        db,
        network,
        scheduled,
        incoming,
        config,
        Some(binding),
        None,
    )
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_bound(
    db: Arc<Database>,
    network: network::Handle,
    scheduled: network::Events,
    incoming: gossip::Events,
    config: Config,
    analytics: Option<crate::bindings::PeerAnalytics>,
    capabilities: Option<CapabilityStore>,
) -> Result<(Handle, Events, Service), Error> {
    if analytics.as_ref().is_some_and(|a| !a.uses_database(&db)) {
        return Err(Error::Configuration);
    }
    config.validate()?;
    if !network.uses_database(&db) || !network.defers_pings() {
        return Err(Error::Configuration);
    }
    let (capabilities, owned_capabilities) = match capabilities {
        Some(binding) => {
            // Managed maps belong to the independent worker. Legacy connection
            // configuration must not silently seed them again after a restart.
            if !binding.uses_database(&db)
                || !config.capabilities.is_empty()
                || binding
                    .current()
                    .is_none_or(|c| c.limits() != config.capability_limits)
            {
                return Err(Error::Configuration);
            }
            (binding, None)
        }
        None => {
            let worker = capability_worker::Worker::seeded(
                capability_worker::Config {
                    registry: config.capability_limits,
                    ..Default::default()
                },
                config.capabilities.last().cloned(),
            )?;
            (CapabilityStore::fixed(&db, worker.client()), Some(worker))
        }
    };
    let jobs = blocking::Pool::new(
        config.blocking_threads,
        config.blocking_jobs,
        config.blocking_bytes,
    )?;
    let (send, events) = mpsc::channel(config.events);
    let published = Publisher {
        events: send,
        bytes: Arc::new(Semaphore::new(config.event_bytes)),
    };
    let (stop, stopped) = watch::channel(false);
    let (target, targets) = watch::channel(0);
    let current = Status {
        running: true,
        joined: false,
        connections: 0,
        counts: Default::default(),
        last_error: None,
    };
    let (status, observed) = watch::channel(current.clone());
    let (progress, _) = watch::channel(Counts::default());
    let context = Context {
        analytics,
        db: db.clone(),
        network,
        storage_bytes: Arc::new(Semaphore::new(config.blocking_bytes)),
        config: Arc::new(config),
        published,
        target: targets,
        progress,
        capabilities: capabilities.clone(),
    };
    Ok((
        Handle {
            db,
            stop,
            target,
            status: observed,
            capabilities,
        },
        Events(events),
        Service::new(
            context,
            scheduled,
            incoming,
            stopped,
            status,
            current,
            jobs,
            owned_capabilities,
        ),
    ))
}
