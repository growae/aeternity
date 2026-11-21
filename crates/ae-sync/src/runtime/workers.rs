use crate::{analytics, bindings, blocking, pool_gc, routing};
use ae_chain::repository::retention;
use ae_protocol::NetworkProfile;
use ae_storage::{Database, gc, pool};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::Instant,
};

#[derive(Debug, Clone, Copy, Default)]
pub struct GarbageCollection {
    pub policy: retention::Config,
    pub limits: gc::Limits,
}
#[derive(Debug, Clone)]
pub struct Config {
    pub routing: Option<routing::Setup>,
    pub analytics: Option<analytics::Config>,
    pub pool_gc: Option<pool_gc::Config>,
    pub keys: Option<crate::keys::Config>,
    pub pool: pool::Config,
    pub admission: pool::worker::Settings,
    pub gc: Option<GarbageCollection>,
    pub poll: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            routing: None,
            analytics: None,
            pool_gc: Some(Default::default()),
            keys: None,
            pool: Default::default(),
            admission: Default::default(),
            gc: Some(Default::default()),
            poll: Duration::from_millis(100),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Child {
    Routing,
    Keys,
    Admission,
    Analytics,
    PoolMaintenance,
    GarbageCollection,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Closed,
    Busy,
    Stale,
    Missing,
    Generation,
    Aborted,
    Start { child: Child, reason: String },
    Worker,
    RecoveryRequired,
    RestartLimit,
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
    pub joined: bool,
    pub admission_generation: u64,
    pub gc_generation: u64,
    pub keys_generation: u64,
    pub routing_generation: u64,
    pub analytics_generation: u64,
    pub pool_gc_generation: u64,
    pub admission: Option<pool::worker::Info>,
    pub gc: Option<gc::worker::Info>,
    pub keys: Option<crate::keys::Info>,
    pub routing: Option<routing::Info>,
    pub analytics: Option<analytics::Info>,
    pub pool_gc: Option<pool_gc::Info>,
    pub restart_attempts: u64,
    pub last_failure: Option<Error>,
    pub error: Option<Error>,
}
#[derive(Clone)]
pub struct Bindings {
    pub routing: Option<bindings::MetricsRouting>,
    pub analytics: Option<bindings::PeerAnalytics>,
    pub pool_gc: Option<bindings::Binding<pool_gc::Client>>,
    pub keys: Option<bindings::Binding<crate::keys::Client>>,
    pub admission: bindings::Admission,
    pub gc: Option<bindings::GarbageCollection>,
}
pub struct Ticket(oneshot::Receiver<Result<u64, Error>>);
impl Ticket {
    pub async fn wait(self) -> Result<u64, Error> {
        self.0.await.map_err(|_| Error::Closed)?
    }
}
struct Restart {
    child: Child,
    generation: u64,
    reply: oneshot::Sender<Result<u64, Error>>,
    #[cfg(test)]
    stop_only: bool,
}
#[derive(Clone)]
pub struct Handle {
    stopped: watch::Sender<bool>,
    status: watch::Receiver<Status>,
    commands: mpsc::Sender<Restart>,
    bindings: Bindings,
}
impl Handle {
    pub fn shutdown(&self) {
        self.stopped.send_replace(true);
    }
    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }
    pub fn bindings(&self) -> Bindings {
        self.bindings.clone()
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.status.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.status.borrow_and_update().clone())
    }
    // Administrative replacement is conditional on the observed incarnation.
    // A queued stale request cannot restart an already replaced worker.
    pub fn restart(&self, child: Child, generation: u64) -> Result<Ticket, Error> {
        if *self.stopped.borrow() || self.commands.is_closed() {
            return Err(Error::Closed);
        }
        let (reply, result) = oneshot::channel();
        self.commands
            .try_send(Restart {
                child,
                generation,
                reply,
                #[cfg(test)]
                stop_only: false,
            })
            .map_err(|_| Error::Busy)?;
        Ok(Ticket(result))
    }
}
pub struct Service {
    database: Arc<Database>,
    network: NetworkProfile,
    config: Config,
    cache: pool::worker::GossipCache,
    admission: Option<pool::worker::Worker>,
    gc: Option<gc::worker::Worker>,
    keys: Option<crate::keys::Worker>,
    routing: Option<routing::Worker>,
    routing_owner: Option<routing::Owner>,
    analytics: Option<analytics::Worker>,
    pool_gc: Option<pool_gc::Worker>,
    admission_binding: bindings::Admission,
    pool_gc_binding: Option<bindings::Owner<pool_gc::Client>>,
    analytics_binding: Option<bindings::Owner<analytics::Client>>,
    routing_binding: Option<bindings::Owner<routing::Client>>,
    pool_binding: bindings::Owner<bindings::Pool>,
    gc_binding: Option<bindings::Owner<gc::worker::Client>>,
    keys_binding: Option<bindings::Owner<crate::keys::Client>>,
    jobs: blocking::Pool,
    stopped: watch::Receiver<bool>,
    commands: mpsc::Receiver<Restart>,
    published: watch::Sender<Status>,
    current: Status,
    budget: super::restart::Budget,
    origin: Instant,
    #[cfg(test)]
    restart_gate: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
}
pub fn create(
    database: Arc<Database>,
    network: NetworkProfile,
    config: Config,
    cache: pool::worker::GossipCache,
) -> Result<(Handle, Service), Error> {
    if config.poll.is_zero() || config.poll > Duration::from_secs(1) {
        return Err(Error::Configuration);
    }
    let jobs =
        blocking::Pool::named("ae-worker-supervisor", 1, 2, 256).map_err(|_| Error::Worker)?;
    let (pool_binding, admission) = bindings::Owner::new(&database);
    let (routing_binding, routing) = if config.routing.is_some() {
        let (owner, reader) = bindings::Owner::new(&database);
        (Some(owner), Some(reader))
    } else {
        (None, None)
    };
    let (analytics_binding, analytics) = if config.analytics.is_some() {
        let (owner, reader) = bindings::Owner::new(&database);
        (Some(owner), Some(reader))
    } else {
        (None, None)
    };
    let (pool_gc_binding, pool_gc) = if config.pool_gc.is_some() {
        let (owner, reader) = bindings::Owner::new(&database);
        (Some(owner), Some(reader))
    } else {
        (None, None)
    };
    let admission_binding = admission.clone();
    let (keys_binding, keys) = if config.keys.is_some() {
        let (owner, reader) = bindings::Owner::new(&database);
        (Some(owner), Some(reader))
    } else {
        (None, None)
    };
    let (gc_binding, gc) = match config.gc {
        Some(_) => {
            let (owner, reader) = bindings::Owner::new(&database);
            (Some(owner), Some(reader))
        }
        None => (None, None),
    };
    let current = Status {
        running: true,
        ..Default::default()
    };
    let (published, status) = watch::channel(current.clone());
    let (stop, stopped) = watch::channel(false);
    let (commands, receiver) = mpsc::channel(32);
    Ok((
        Handle {
            stopped: stop,
            status,
            commands,
            bindings: Bindings {
                admission,
                gc,
                keys,
                routing,
                analytics,
                pool_gc,
            },
        },
        Service {
            database,
            network,
            config,
            cache,
            admission: None,
            gc: None,
            keys: None,
            routing: None,
            routing_owner: None,
            analytics: None,
            pool_gc: None,
            admission_binding,
            pool_gc_binding,
            analytics_binding,
            routing_binding,
            pool_binding,
            gc_binding,
            keys_binding,
            jobs,
            stopped,
            commands: receiver,
            published,
            current,
            budget: Default::default(),
            origin: Instant::now(),
            #[cfg(test)]
            restart_gate: None,
        },
    ))
}
enum Started {
    PoolMaintenance(pool_gc::Worker),
    Analytics(analytics::Worker),
    Routing(routing::Worker, Option<routing::Owner>),
    Keys(crate::keys::Worker),
    Admission(pool::worker::Worker),
    Gc(gc::worker::Worker),
}
impl Service {
    fn stopping(&self) -> bool {
        *self.stopped.borrow() || self.stopped.has_changed().is_err()
    }
    fn healthy(&self) -> Result<(), Error> {
        if self.database.events().info().closed || self.database.gc_events().info().closed {
            Err(Error::RecoveryRequired)
        } else {
            Ok(())
        }
    }
    fn generation(&self, child: Child) -> u64 {
        match child {
            Child::Routing => self.current.routing_generation,
            Child::Analytics => self.current.analytics_generation,
            Child::PoolMaintenance => self.current.pool_gc_generation,
            Child::Keys => self.current.keys_generation,
            Child::Admission => self.current.admission_generation,
            Child::GarbageCollection => self.current.gc_generation,
        }
    }
    fn publish(&mut self) {
        self.current.routing = self.routing.as_ref().map(|w| w.info());
        self.current.analytics = self.analytics.as_ref().map(|w| w.info());
        self.current.pool_gc = self.pool_gc.as_ref().map(|w| w.info());
        self.current.keys = self.keys.as_ref().map(|w| w.info());
        self.current.admission = self.admission.as_ref().map(|w| w.info());
        self.current.gc = self.gc.as_ref().map(|w| w.info());
        self.published.send_replace(self.current.clone());
    }
    async fn start(&mut self, child: Child) -> Result<(), Error> {
        let generation = self
            .generation(child)
            .checked_add(1)
            .ok_or(Error::Generation)?;
        let db = self.database.clone();
        let config = self.config.clone();
        let network = self.network.clone();
        let cache = self.cache.clone();
        let admission_binding = self.admission_binding.clone();
        let routing_store = self.routing_owner.as_ref().map(|o| o.store());
        let created = self
            .jobs
            .submit(128, move |_| match child {
                Child::PoolMaintenance => pool_gc::Worker::spawn(
                    &db,
                    admission_binding,
                    config.pool_gc.ok_or(Error::Missing)?,
                )
                .map(Started::PoolMaintenance)
                .map_err(|e| Error::Start {
                    child,
                    reason: format!("{e:?}"),
                }),
                Child::Analytics => {
                    analytics::Worker::spawn(config.analytics.ok_or(Error::Missing)?)
                        .map(Started::Analytics)
                        .map_err(|e| Error::Start {
                            child,
                            reason: format!("{e:?}"),
                        })
                }
                Child::Routing => {
                    let start = || -> Result<Started, routing::Error> {
                        let owner = if routing_store.is_none() {
                            Some(routing::Owner::new(
                                config.routing.ok_or(routing::Error::Configuration)?,
                            )?)
                        } else {
                            None
                        };
                        let store = routing_store
                            .or_else(|| owner.as_ref().map(|o| o.store()))
                            .expect("configured routing");
                        Ok(Started::Routing(routing::Worker::spawn(store)?, owner))
                    };
                    start().map_err(|e| Error::Start {
                        child,
                        reason: format!("{e:?}"),
                    })
                }
                Child::Keys => crate::keys::Worker::spawn(config.keys.ok_or(Error::Missing)?)
                    .map(Started::Keys)
                    .map_err(|e| Error::Start {
                        child,
                        reason: format!("{e:?}"),
                    }),
                Child::Admission => pool::worker::Worker::spawn_with_cache(
                    db,
                    network,
                    config.pool,
                    pool::worker::Settings {
                        origins_interval: if config.pool_gc.is_some() {
                            None
                        } else {
                            config.admission.origins_interval
                        },
                        ..config.admission
                    },
                    cache,
                )
                .map(Started::Admission)
                .map_err(|e| Error::Start {
                    child,
                    reason: format!("{e:?}"),
                }),
                Child::GarbageCollection => {
                    let config = config.gc.ok_or(Error::Missing)?;
                    gc::worker::Worker::spawn(db, config.policy, config.limits)
                        .map(Started::Gc)
                        .map_err(|e| Error::Start {
                            child,
                            reason: format!("{e:?}"),
                        })
                }
            })
            .map_err(|_| Error::Worker)?
            .await
            .map_err(|_| Error::Worker)??;
        match created {
            Started::PoolMaintenance(worker) => {
                self.pool_gc_binding
                    .as_ref()
                    .expect("configured pool GC")
                    .install(worker.client(), generation);
                self.pool_gc = Some(worker);
                self.current.pool_gc_generation = generation;
            }
            Started::Analytics(worker) => {
                self.analytics_binding
                    .as_ref()
                    .expect("configured analytics")
                    .install(worker.client(), generation);
                self.analytics = Some(worker);
                self.current.analytics_generation = generation;
            }
            Started::Routing(worker, owner) => {
                if let Some(owner) = owner {
                    self.routing_owner = Some(owner);
                }
                self.routing_binding
                    .as_ref()
                    .expect("configured routing")
                    .install(worker.client(), generation);
                self.routing = Some(worker);
                self.current.routing_generation = generation;
            }
            Started::Keys(worker) => {
                self.keys_binding
                    .as_ref()
                    .expect("configured keys")
                    .install(worker.client(), generation);
                self.keys = Some(worker);
                self.current.keys_generation = generation;
            }
            Started::Admission(worker) => {
                self.pool_binding
                    .install(bindings::Pool::new(worker.client()), generation);
                self.admission = Some(worker);
                self.current.admission_generation = generation;
            }
            Started::Gc(worker) => {
                self.gc_binding
                    .as_ref()
                    .expect("configured GC")
                    .install(worker.client(), generation);
                self.gc = Some(worker);
                self.current.gc_generation = generation;
            }
        }
        self.publish();
        Ok(())
    }
    async fn retire(&mut self, child: Child) -> Result<(), Error> {
        let pending = match child {
            Child::PoolMaintenance => {
                if let Some(binding) = &self.pool_gc_binding {
                    binding.withdraw();
                }
                self.pool_gc.take().map(|worker| {
                    worker.request_shutdown();
                    self.jobs.submit(128, move |_| {
                        let _ = worker.shutdown();
                    })
                })
            }
            Child::Analytics => {
                if let Some(binding) = &self.analytics_binding {
                    binding.withdraw();
                }
                self.analytics.take().map(|worker| {
                    worker.request_shutdown();
                    self.jobs.submit(128, move |_| {
                        let _ = worker.shutdown();
                    })
                })
            }
            Child::Routing => {
                if let Some(binding) = &self.routing_binding {
                    binding.withdraw();
                }
                self.routing.take().map(|worker| {
                    worker.request_shutdown();
                    self.jobs.submit(128, move |_| {
                        let _ = worker.shutdown();
                    })
                })
            }
            Child::Keys => {
                if let Some(binding) = &self.keys_binding {
                    binding.withdraw();
                }
                self.keys.take().map(|worker| {
                    worker.request_shutdown();
                    self.jobs.submit(128, move |_| {
                        let _ = worker.shutdown();
                    })
                })
            }
            Child::Admission => {
                self.pool_binding.withdraw();
                self.admission.take().map(|worker| {
                    worker.request_shutdown();
                    self.jobs.submit(128, move |_| {
                        let _ = worker.shutdown();
                    })
                })
            }
            Child::GarbageCollection => {
                if let Some(binding) = &self.gc_binding {
                    binding.withdraw();
                }
                self.gc.take().map(|worker| {
                    worker.request_shutdown();
                    self.jobs.submit(128, move |_| {
                        let _ = worker.shutdown();
                    })
                })
            }
        };
        if let Some(pending) = pending {
            pending
                .map_err(|_| Error::Worker)?
                .await
                .map_err(|_| Error::Worker)?;
        }
        self.publish();
        Ok(())
    }
    async fn replace(&mut self, child: Child) -> Result<u64, Error> {
        // One budget belongs to this supervisor, shared by all children.
        self.current.ready = false;
        self.publish();
        loop {
            if self.stopping() {
                return Err(Error::Closed);
            }
            self.healthy()?;
            self.current.restart_attempts += 1;
            let allowed = self.budget.record(self.origin.elapsed().as_secs());
            self.retire(child).await?;
            if !allowed {
                return Err(Error::RestartLimit);
            }
            #[cfg(test)]
            if let Some((entered, release)) = self.restart_gate.take() {
                let _ = entered.send(());
                let _ = release.await;
            }
            if self.stopping() {
                return Err(Error::Closed);
            }
            self.healthy()?;
            match self.start(child).await {
                Ok(()) => {
                    self.current.ready = true;
                    self.publish();
                    return Ok(self.generation(child));
                }
                Err(error) => {
                    self.current.last_failure = Some(error);
                    self.publish();
                }
            }
            tokio::task::yield_now().await;
        }
    }
    async fn run_inner(&mut self) -> Result<(), Error> {
        if self.stopping() {
            return Ok(());
        }
        self.healthy()?;
        if self.config.routing.is_some() {
            self.start(Child::Routing).await?;
        }
        if self.stopping() {
            return Ok(());
        }
        if self.config.keys.is_some() {
            self.start(Child::Keys).await?;
        }
        if self.stopping() {
            return Ok(());
        }
        self.start(Child::Admission).await?;
        if self.stopping() {
            return Ok(());
        }
        if self.config.analytics.is_some() {
            self.start(Child::Analytics).await?;
        }
        if self.stopping() {
            return Ok(());
        }
        if self.config.pool_gc.is_some() {
            self.start(Child::PoolMaintenance).await?;
        }
        if self.stopping() {
            return Ok(());
        }
        if self.config.gc.is_some() {
            self.start(Child::GarbageCollection).await?;
        }
        if self.stopping() {
            return Ok(());
        }
        self.current.ready = true;
        self.publish();
        let mut interval = tokio::time::interval(self.config.poll);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if self.stopping() {
                return Ok(());
            }
            self.healthy()?;
            let exited = if self.routing.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::Routing)
            } else if self.keys.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::Keys)
            } else if self.admission.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::Admission)
            } else if self.analytics.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::Analytics)
            } else if self.pool_gc.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::PoolMaintenance)
            } else if self.gc.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::GarbageCollection)
            } else {
                None
            };
            if let Some(child) = exited {
                self.replace(child).await?;
                continue;
            }
            self.publish();
            tokio::select! {
                biased;
                _ = self.stopped.changed() => return Ok(()),
                command = self.commands.recv() => {
                    let Some(command) = command else { return Ok(()); };
                    let result = if (command.child == Child::PoolMaintenance && self.config.pool_gc.is_none()) || (command.child == Child::Analytics && self.config.analytics.is_none()) || (command.child == Child::Routing && self.config.routing.is_none()) || (command.child == Child::GarbageCollection && self.config.gc.is_none()) || (command.child == Child::Keys && self.config.keys.is_none()) { Err(Error::Missing) }
                        else if command.generation != self.generation(command.child) { Err(Error::Stale) }
                        else { self.requested(&command).await };
                    let terminal = result.as_ref().err().filter(|e| !matches!(e, Error::Stale | Error::Missing)).cloned();
                    let _ = command.reply.send(result);
                    if let Some(error) = terminal { return Err(error); }
                },
                _ = interval.tick() => (),
            }
        }
    }
    async fn requested(&mut self, command: &Restart) -> Result<u64, Error> {
        #[cfg(test)]
        if command.stop_only {
            match command.child {
                Child::Routing => self.routing.as_ref().unwrap().request_shutdown(),
                Child::Analytics => self.analytics.as_ref().unwrap().request_shutdown(),
                Child::PoolMaintenance => self.pool_gc.as_ref().unwrap().request_shutdown(),
                Child::Keys => self.keys.as_ref().unwrap().request_shutdown(),
                Child::Admission => self.admission.as_ref().unwrap().request_shutdown(),
                Child::GarbageCollection => self.gc.as_ref().unwrap().request_shutdown(),
            }
            return Ok(self.generation(command.child));
        }
        self.replace(command.child).await
    }
    pub async fn run(mut self) -> Status {
        if let Err(error) = self.run_inner().await
            && !(error == Error::Closed && self.stopping())
        {
            self.current.error = Some(error);
        }
        self.current.ready = false;
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            let _ = command.reply.send(Err(Error::Closed));
        }
        self.pool_binding.withdraw();
        if let Some(binding) = &self.routing_binding {
            binding.withdraw();
        }
        if let Some(binding) = &self.keys_binding {
            binding.withdraw();
        }
        if let Some(binding) = &self.gc_binding {
            binding.withdraw();
        }
        if let Some(binding) = &self.analytics_binding {
            binding.withdraw();
        }
        if let Some(binding) = &self.pool_gc_binding {
            binding.withdraw();
        }
        for child in [
            Child::GarbageCollection,
            Child::PoolMaintenance,
            Child::Analytics,
            Child::Admission,
            Child::Keys,
            Child::Routing,
        ] {
            if let Err(error) = self.retire(child).await {
                self.current.error.get_or_insert(error);
            }
        }
        self.pool_binding.close();
        if let Some(binding) = &self.pool_gc_binding {
            binding.close();
        }
        if let Some(binding) = &self.analytics_binding {
            binding.close();
        }
        if let Some(binding) = &self.routing_binding {
            binding.close();
        }
        if let Some(binding) = &self.keys_binding {
            binding.close();
        }
        if let Some(binding) = &self.gc_binding {
            binding.close();
        }
        self.routing_owner.take();
        self.jobs.stop();
        self.jobs.join().await;
        self.current.running = false;
        self.current.joined = true;
        self.publish();
        self.current.clone()
    }
}

#[cfg(test)]
mod tests;
impl Drop for Service {
    fn drop(&mut self) {
        if let Some(binding) = &self.keys_binding {
            binding.close();
        }
        self.pool_binding.close();
        if let Some(binding) = &self.pool_gc_binding {
            binding.close();
        }
        if let Some(binding) = &self.analytics_binding {
            binding.close();
        }
        if let Some(binding) = &self.routing_binding {
            binding.close();
        }
        if let Some(binding) = &self.gc_binding {
            binding.close();
        }
        self.jobs.stop();
        if !self.current.joined {
            self.current.running = false;
            self.current.ready = false;
            self.current.error.get_or_insert(Error::Aborted);
            self.published.send_replace(self.current.clone());
        }
    }
}
