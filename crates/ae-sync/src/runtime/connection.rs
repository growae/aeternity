use crate::{chain, chain::sync, conductor, gossip, maintenance, network, observe, peer};
use ae_chain::key::Rules;
use ae_execution::transactions::owned::Owned;
use ae_p2p::noise;
use ae_storage::{Database, gc::worker as gc, pool::worker as admission};
use ae_vm_fate::maps::{MapOrder, order::Otp26};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, time::Instant};

mod group;
use super::restart;

// This owns the currently implemented connection services. Durable admission
// and GC workers belong to the independent worker hierarchy. Their explicit
// fixed clients identify one lifetime; managed bindings recover independently.
pub struct Inputs<O = Otp26> {
    pub database: Arc<Database>,
    pub admission: admission::Client,
    pub gc: Option<gc::Client>,
    pub identity: Arc<noise::Identity>,
    pub execution: Owned<O>,
    pub rules: Rules,
}

#[derive(Clone)]
pub struct Config {
    pub network: network::Config,
    pub gossip: gossip::Config,
    pub peer: peer::Config,
    pub importer: chain::Config,
    pub sync: sync::Config,
    pub propagation: conductor::Config,
    pub maintenance: maintenance::Config,
    pub observer: observe::Config,
    pub health_interval: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            network: Default::default(),
            gossip: Default::default(),
            peer: Default::default(),
            importer: Default::default(),
            sync: Default::default(),
            propagation: Default::default(),
            maintenance: Default::default(),
            observer: Default::default(),
            health_interval: Duration::from_millis(100),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Child {
    GossipCache,
    Importer,
    Network,
    Gossip,
    Peer,
    Sync,
    Propagation,
    Maintenance,
    Observer,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub child: Child,
    pub detail: String,
}
impl Failure {
    fn new(child: Child, detail: impl std::fmt::Debug) -> Self {
        Self {
            child,
            detail: format!("{detail:?}"),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Closed,
    Startup(Failure),
    RestartLimit,
    Dependency(Child),
    RecoveryRequired,
    Generation,
    Aborted,
    KeysClosed,
    RoutingClosed,
    AnalyticsClosed,
    CapabilitiesClosed,
    IdentityChanged,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone, Default)]
pub struct Status {
    pub running: bool,
    pub ready: bool,
    // True only after every started async task and owned blocking worker has
    // been joined. Aborting/dropping the owner never reports joined shutdown.
    pub joined: bool,
    pub generation: u64,
    pub starts: u64,
    pub restart_attempts: u64,
    pub terminated_generations: u64,
    pub last_failure: Option<Failure>,
    pub error: Option<Error>,
}

// Retaining this snapshot never redirects commands to a later generation.
#[derive(Clone)]
pub struct Generation {
    pub number: u64,
    pub cache: Option<admission::CacheHandle>,
    pub network: network::Handle,
    pub gossip: gossip::Handle,
    pub peer: peer::Handle,
    pub sync: sync::Handle,
    pub propagation: conductor::Handle,
    pub maintenance: Option<maintenance::Handle>,
    pub observer: observe::Handle,
}
#[derive(Clone)]
pub struct Handle {
    stop: watch::Sender<bool>,
    status: watch::Receiver<Status>,
    generation: watch::Receiver<Option<Generation>>,
}
impl Handle {
    pub fn shutdown(&self) {
        self.stop.send_replace(true);
    }
    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }
    pub fn current(&self) -> Option<Generation> {
        self.generation.borrow().clone()
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.status.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.status.borrow_and_update().clone())
    }
}

pub struct Service<O, F> {
    inputs: Inputs<O>,
    dependencies: super::workers::Bindings,
    cache: Option<admission::CacheController>,
    config: Config,
    outputs: F,
    stopped: watch::Receiver<bool>,
    published: watch::Sender<Status>,
    generation: watch::Sender<Option<Generation>>,
    current: Status,
}

// No listener/dial or service starts here. The output factory must return a
// fresh owned destination per generation; it runs before networking starts.
pub fn create<O, F>(
    inputs: Inputs<O>,
    config: Config,
    outputs: F,
) -> Result<(Handle, Service<O, F>), Error>
where
    O: MapOrder + Clone + Send + Sync + 'static,
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    let dependencies = super::workers::Bindings {
        keys: None,
        routing: None,
        analytics: None,
        pool_gc: None,
        capabilities: None,
        admission: crate::bindings::Admission::fixed(&inputs.database, inputs.admission.clone()),
        gc: inputs.gc.as_ref().map(|client| {
            crate::bindings::GarbageCollection::fixed(&inputs.database, client.clone())
        }),
    };
    create_with_workers(inputs, config, outputs, dependencies)
}
// The worker supervisor owns these bindings; connection-group restarts never
// replace the independent worker owner or its gossip cache.
pub fn create_with_workers<O, F>(
    inputs: Inputs<O>,
    config: Config,
    outputs: F,
    dependencies: super::workers::Bindings,
) -> Result<(Handle, Service<O, F>), Error>
where
    O: MapOrder + Clone + Send + Sync + 'static,
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    create_inner(inputs, config, outputs, dependencies, None)
}
// A managed received-transaction cache belongs to this connection group. Its
// stable lookup remains with independent admission workers while each group
// generation owns a fresh cache instance.
pub fn create_with_owned_cache<O, F>(
    inputs: Inputs<O>,
    config: Config,
    outputs: F,
    dependencies: super::workers::Bindings,
    cache: admission::CacheController,
) -> Result<(Handle, Service<O, F>), Error>
where
    O: MapOrder + Clone + Send + Sync + 'static,
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    create_inner(inputs, config, outputs, dependencies, Some(cache))
}
fn create_inner<O, F>(
    inputs: Inputs<O>,
    config: Config,
    outputs: F,
    dependencies: super::workers::Bindings,
    cache: Option<admission::CacheController>,
) -> Result<(Handle, Service<O, F>), Error>
where
    O: MapOrder + Clone + Send + Sync + 'static,
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    if let Some(cache) = &cache
        && let Some(pool) = dependencies.admission.current()
        && !cache.uses_cache(&pool.client.gossip_cache())
    {
        return Err(Error::Configuration);
    }
    if dependencies
        .routing
        .as_ref()
        .is_some_and(|r| !r.uses_database(&inputs.database))
        || dependencies
            .analytics
            .as_ref()
            .is_some_and(|a| !a.uses_database(&inputs.database))
        || dependencies
            .capabilities
            .as_ref()
            .is_some_and(|c| !c.uses_database(&inputs.database))
        || !dependencies.admission.uses_database(&inputs.database)
        || dependencies
            .keys
            .as_ref()
            .is_some_and(|k| !k.uses_database(&inputs.database))
        || dependencies
            .gc
            .as_ref()
            .is_some_and(|g| !g.uses_database(&inputs.database))
        || dependencies.gc.is_some() != inputs.gc.is_some()
    {
        return Err(Error::Configuration);
    }
    if config.health_interval.is_zero()
        || config.health_interval > Duration::from_secs(1)
        || !inputs.admission.uses_database(&inputs.database)
        || inputs
            .gc
            .as_ref()
            .is_some_and(|g| !g.uses_database(&inputs.database))
    {
        return Err(Error::Configuration);
    }
    let current = Status {
        running: true,
        ..Default::default()
    };
    let (published, status) = watch::channel(current.clone());
    let (generation, observed) = watch::channel(None);
    let (stop, stopped) = watch::channel(false);
    Ok((
        Handle {
            stop,
            status,
            generation: observed,
        },
        Service {
            inputs,
            dependencies,
            cache,
            config,
            outputs,
            stopped,
            published,
            generation,
            current,
        },
    ))
}

impl<O, F> Service<O, F>
where
    O: MapOrder + Clone + Send + Sync + 'static,
    F: FnMut() -> Result<observe::Outputs, observe::Error> + Send + 'static,
{
    fn stopping(&self) -> bool {
        *self.stopped.borrow() || self.stopped.has_changed().is_err()
    }
    fn health(&self) -> Result<(), Error> {
        if self
            .dependencies
            .analytics
            .as_ref()
            .is_some_and(|a| a.is_closed())
        {
            return Err(Error::AnalyticsClosed);
        }
        if self
            .dependencies
            .routing
            .as_ref()
            .is_some_and(|r| r.is_closed())
        {
            return Err(Error::RoutingClosed);
        }
        if let Some(keys) = &self.dependencies.keys {
            if keys.is_closed() {
                return Err(Error::KeysClosed);
            }
            if let Some(client) = keys.current()
                && client.info().keys.peer != self.inputs.identity.public_key()
            {
                return Err(Error::IdentityChanged);
            }
        }
        let pool = &self.dependencies.admission;
        if pool.is_closed()
            || (!pool.managed() && pool.current().is_none_or(|p| !p.client.info().running))
        {
            return Err(Error::Dependency(Child::Gossip));
        }
        if self.dependencies.gc.as_ref().is_some_and(|gc| {
            gc.is_closed()
                || (!gc.managed()
                    && gc
                        .current()
                        .is_none_or(|g| g.info().is_none_or(|i| !i.running)))
        }) {
            return Err(Error::Dependency(Child::Maintenance));
        }
        let chain = self.inputs.database.events().info();
        let gc = self.inputs.database.gc_events().info();
        if chain.closed || gc.closed {
            return Err(Error::RecoveryRequired);
        }
        // Keep the established failure priority when the parent closes every binding.
        if self
            .dependencies
            .capabilities
            .as_ref()
            .is_some_and(|c| c.is_closed())
        {
            return Err(Error::CapabilitiesClosed);
        }
        Ok(())
    }
    async fn wait_dependencies(&mut self) -> Result<(), Error> {
        loop {
            if self.stopping() {
                return Err(Error::Closed);
            }
            self.health()?;
            if self.dependencies.admission.current().is_some()
                && self
                    .dependencies
                    .capabilities
                    .as_ref()
                    .is_none_or(|c| c.current().is_some())
                && self
                    .dependencies
                    .analytics
                    .as_ref()
                    .is_none_or(|a| a.current().is_some())
                && self
                    .dependencies
                    .routing
                    .as_ref()
                    .is_none_or(|r| r.current().is_some())
                && self
                    .dependencies
                    .keys
                    .as_ref()
                    .is_none_or(|k| k.current().is_some())
                && self
                    .dependencies
                    .gc
                    .as_ref()
                    .is_none_or(|g| g.current().is_some())
            {
                return Ok(());
            }
            tokio::select! {
                _ = self.stopped.changed() => return Err(Error::Closed),
                _ = self.dependencies.admission.changed() => (),
                _ = async { match &mut self.dependencies.capabilities {Some(c)=>c.changed().await,None=>std::future::pending().await}}=>(),
                _ = async {match &mut self.dependencies.analytics {Some(a)=>a.changed().await,None=>std::future::pending().await}}=>(),
                _ = async {match &mut self.dependencies.routing {Some(r)=>r.changed().await,None=>std::future::pending().await}}=>(),
                _ = async { match &mut self.dependencies.keys { Some(k) => k.changed().await, None => std::future::pending().await } } => (),
                _ = async { match &mut self.dependencies.gc { Some(gc) => gc.changed().await, None => std::future::pending().await } } => (),
            }
        }
    }
    fn publish(&self) {
        self.published.send_replace(self.current.clone());
    }

    pub async fn run(mut self) -> Status {
        let origin = Instant::now();
        let mut budget = restart::Budget::default();
        let mut ever_ready = false;
        loop {
            if self.stopping() {
                break;
            }
            if let Err(error) = self.health() {
                self.current.error = Some(error);
                break;
            }
            if let Err(error) = self.wait_dependencies().await {
                if !self.stopping() {
                    self.current.error = Some(error);
                }
                break;
            }
            let Some(number) = self.current.generation.checked_add(1) else {
                self.current.error = Some(Error::Generation);
                break;
            };
            self.current.generation = number;
            self.current.starts += 1;
            self.publish();
            let mut group = group::Group::new();
            let prepared = group
                .prepare(
                    &self.inputs,
                    &self.dependencies,
                    &self.config,
                    &mut self.outputs,
                    self.cache.as_mut(),
                    number,
                )
                .await;
            let mut failure = None;
            let mut terminal = None;
            match prepared {
                Err(error) => failure = Some(error),
                Ok(mut generation) => {
                    if !self.stopping() {
                        group.launch();
                        let mut health = tokio::time::interval(self.config.health_interval);
                        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        loop {
                            if self.stopping() {
                                break;
                            }
                            if let Err(error) = self.health() {
                                terminal = Some(error);
                                break;
                            }
                            if group.recovery_required() {
                                terminal = Some(Error::RecoveryRequired);
                                break;
                            }
                            if let Some(error) = group.cache_failure() {
                                failure = Some(error);
                                break;
                            }
                            if !self.current.ready && generation.network.status().ready {
                                ever_ready = true;
                                self.current.ready = true;
                                self.generation.send_replace(Some(generation.clone()));
                                self.publish();
                            }
                            tokio::select! {
                                biased;
                                _ = self.stopped.changed() => break,
                                result = group.next() => { failure = Some(result); break; },
                                _ = generation.network.changed(), if !self.current.ready => (),
                                _ = health.tick() => (),
                            }
                        }
                    }
                }
            }
            self.current.ready = false;
            self.generation.send_replace(None);
            self.publish();
            // OTP timestamps the restart decision before terminating siblings.
            // A slow joined teardown must not move the attempt across a second.
            let restart_at = origin.elapsed().as_secs();
            group.shutdown().await;
            self.current.terminated_generations += 1;
            // A failed listener can close consumers before its own cleanup
            // completes. Attribute initial startup to the actual bind failure.
            if !ever_ready && let Some(network_failure) = group.network_failure() {
                failure = Some(network_failure);
            }
            if let Some(error) = failure {
                self.current.last_failure = Some(error);
            }
            // A commit can become uncertain while shutdown joins its importer.
            if group.recovery_required() {
                terminal = Some(Error::RecoveryRequired);
            }
            if self.stopping() {
                break;
            }
            if let Some(error) = terminal.or_else(|| self.health().err()) {
                self.current.error = Some(error);
                break;
            }
            if !ever_ready {
                self.current.error = self.current.last_failure.clone().map(Error::Startup);
                break;
            }
            self.current.restart_attempts += 1;
            if !budget.record(restart_at) {
                self.current.error = Some(Error::RestartLimit);
                break;
            }
            self.publish();
            // OTP failed starts count as attempts and retry via a queued cast.
            // Yield between attempts; no invented backoff changes the budget.
            tokio::task::yield_now().await;
        }
        self.current.running = false;
        self.current.ready = false;
        self.current.joined = true;
        self.generation.send_replace(None);
        self.publish();
        self.current.clone()
    }
}
impl<O, F> Drop for Service<O, F> {
    fn drop(&mut self) {
        self.generation.send_replace(None);
        if !self.current.joined {
            self.current.running = false;
            self.current.ready = false;
            self.current.error.get_or_insert(Error::Aborted);
            self.published.send_replace(self.current.clone());
        }
    }
}
