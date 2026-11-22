use crate::peer::capabilities::worker as capabilities;
use crate::votes;
use crate::{analytics, bindings, blocking, pool_gc, resilience, routing, upnp};
use ae_chain::repository::retention;
use ae_protocol::{ConsensusType, NetworkProfile};
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
    pub upnp: Option<upnp::Config>,
    pub votes: votes::Config,
    pub routing: Option<routing::Setup>,
    pub analytics: Option<analytics::Config>,
    pub pool_gc: Option<pool_gc::Config>,
    pub resilience: Option<resilience::Config>,
    pub capabilities: Option<capabilities::Config>,
    pub keys: Option<crate::keys::Config>,
    pub pool: pool::Config,
    pub admission: pool::worker::Settings,
    pub gc: Option<GarbageCollection>,
    pub poll: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            upnp: None,
            votes: Default::default(),
            routing: None,
            analytics: None,
            pool_gc: Some(Default::default()),
            resilience: Some(Default::default()),
            capabilities: Some(Default::default()),
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
    Upnp,
    Routing,
    Keys,
    Admission,
    Analytics,
    PoolMaintenance,
    Resilience,
    Capabilities,
    GarbageCollection,
    Votes,
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
    pub upnp_generation: u64,
    pub votes_generation: u64,
    pub votes: Option<votes::Info>,
    pub upnp: Option<upnp::Info>,
    pub upnp_cleanup_error: Option<upnp::Error>,
    pub admission_generation: u64,
    pub gc_generation: u64,
    pub keys_generation: u64,
    pub routing_generation: u64,
    pub analytics_generation: u64,
    pub pool_gc_generation: u64,
    pub resilience_generation: u64,
    pub capabilities_generation: u64,
    pub admission: Option<pool::worker::Info>,
    pub gc: Option<gc::worker::Info>,
    pub keys: Option<crate::keys::Info>,
    pub routing: Option<routing::Info>,
    pub analytics: Option<analytics::Info>,
    pub pool_gc: Option<pool_gc::Info>,
    pub resilience: Option<resilience::Info>,
    pub capabilities: Option<capabilities::Info>,
    pub restart_attempts: u64,
    pub last_failure: Option<Error>,
    pub error: Option<Error>,
}
#[derive(Clone)]
pub struct Bindings {
    pub upnp: Option<bindings::Binding<upnp::Client>>,
    pub votes: Option<votes::Binding>,
    pub upnp_listener: Option<upnp::ListenerSource>,
    pub routing: Option<bindings::MetricsRouting>,
    pub analytics: Option<bindings::PeerAnalytics>,
    pub pool_gc: Option<bindings::Binding<pool_gc::Client>>,
    pub resilience: Option<bindings::Binding<resilience::Client>>,
    pub capabilities: Option<bindings::Binding<capabilities::Client>>,
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
    upnp: Option<upnp::Worker>,
    upnp_binding: Option<bindings::Owner<upnp::Client>>,
    votes: Option<votes::Worker>,
    votes_binding: Option<bindings::Owner<votes::Client>>,
    vote_publications: Option<votes::Publications>,
    admission: Option<pool::worker::Worker>,
    gc: Option<gc::worker::Worker>,
    keys: Option<crate::keys::Worker>,
    routing: Option<routing::Worker>,
    routing_owner: Option<routing::Owner>,
    analytics: Option<analytics::Worker>,
    pool_gc: Option<pool_gc::Worker>,
    resilience: Option<resilience::Worker>,
    resilience_owner: Option<resilience::Owner>,
    resilience_binding: Option<bindings::Owner<resilience::Client>>,
    capabilities: Option<capabilities::Worker>,
    capabilities_binding: Option<bindings::Owner<capabilities::Client>>,
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
    mut config: Config,
    cache: pool::worker::GossipCache,
) -> Result<(Handle, Service), Error> {
    if config.poll.is_zero() || config.poll > Duration::from_secs(1) {
        return Err(Error::Configuration);
    }
    let upnp_listener = config.upnp.as_mut().map(|config| {
        let listener = upnp::ListenerSource::default();
        config.listener = Some(listener.clone());
        listener
    });
    let jobs =
        blocking::Pool::named("ae-worker-supervisor", 1, 2, 256).map_err(|_| Error::Worker)?;
    let (upnp_binding, upnp) = if config.upnp.is_some() {
        let (owner, reader) = bindings::Owner::new(&database);
        (Some(owner), Some(reader))
    } else {
        (None, None)
    };
    let (votes_binding, votes) = if config.pool.consensus == ConsensusType::Pos {
        let (owner, reader) = bindings::Owner::new(&database);
        (Some(owner), Some(reader))
    } else {
        (None, None)
    };
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
    let (resilience_binding, resilience) = if config.resilience.is_some() {
        let (owner, reader) = bindings::Owner::new(&database);
        (Some(owner), Some(reader))
    } else {
        (None, None)
    };
    let (capabilities_binding, capabilities) = if config.capabilities.is_some() {
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
                upnp,
                votes,
                upnp_listener,
                admission,
                gc,
                keys,
                routing,
                analytics,
                pool_gc,
                resilience,
                capabilities,
            },
        },
        Service {
            database,
            network,
            config,
            cache,
            admission: None,
            upnp_binding,
            votes: None,
            votes_binding,
            vote_publications: None,
            gc: None,
            keys: None,
            upnp: None,
            routing: None,
            routing_owner: None,
            analytics: None,
            pool_gc: None,
            resilience: None,
            resilience_owner: None,
            resilience_binding,
            capabilities: None,
            capabilities_binding,
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
    Upnp(upnp::Worker),
    Resilience(resilience::Worker, Option<resilience::Owner>),
    Capabilities(capabilities::Worker),
    PoolMaintenance(pool_gc::Worker),
    Analytics(analytics::Worker),
    Routing(routing::Worker, Option<routing::Owner>),
    Keys(crate::keys::Worker),
    Admission(pool::worker::Worker),
    Gc(gc::worker::Worker),
    Votes(votes::Worker, votes::Publications),
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
            Child::Upnp => self.current.upnp_generation,
            Child::Votes => self.current.votes_generation,
            Child::Resilience => self.current.resilience_generation,
            Child::Capabilities => self.current.capabilities_generation,
            Child::Routing => self.current.routing_generation,
            Child::Analytics => self.current.analytics_generation,
            Child::PoolMaintenance => self.current.pool_gc_generation,
            Child::Keys => self.current.keys_generation,
            Child::Admission => self.current.admission_generation,
            Child::GarbageCollection => self.current.gc_generation,
        }
    }
    fn publish(&mut self) {
        self.current.upnp = self.upnp.as_ref().map(|w| w.info());
        self.current.votes = self.votes.as_ref().map(|w| w.info());
        self.current.resilience = self.resilience.as_ref().map(|w| w.info());
        self.current.capabilities = self.capabilities.as_ref().map(|w| w.info());
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
        let vote_publications = self.vote_publications.clone();
        let admission_binding = self.admission_binding.clone();
        let routing_store = self.routing_owner.as_ref().map(|o| o.store());
        let resilience_source = self.resilience_owner.as_ref().map(|o| o.source());
        let created = self
            .jobs
            .submit(128, move |_| match child {
                Child::Upnp => upnp::Worker::spawn(config.upnp.ok_or(Error::Missing)?)
                    .map(Started::Upnp)
                    .map_err(|e| Error::Start {
                        child,
                        reason: e.to_string(),
                    }),
                Child::Votes => {
                    let start = || -> Result<Started, votes::Error> {
                        let publications = match vote_publications {
                            Some(p) => p,
                            None => votes::Publications::new(&db, config.votes)?,
                        };
                        let worker = votes::Worker::spawn(
                            db,
                            network,
                            cache,
                            config.votes,
                            publications.clone(),
                        )?;
                        Ok(Started::Votes(worker, publications))
                    };
                    start().map_err(|e| Error::Start {
                        child,
                        reason: e.to_string(),
                    })
                }
                Child::Resilience => {
                    let config = config.resilience.ok_or(Error::Missing)?;
                    let start = || -> Result<Started, resilience::Error> {
                        let owner = if resilience_source.is_none() {
                            Some(resilience::Owner::new(db, config)?)
                        } else {
                            None
                        };
                        let source = resilience_source
                            .or_else(|| owner.as_ref().map(|o| o.source()))
                            .expect("configured resilience");
                        Ok(Started::Resilience(
                            resilience::Worker::spawn(source, config.queue)?,
                            owner,
                        ))
                    };
                    start().map_err(|e| Error::Start {
                        child,
                        reason: e.to_string(),
                    })
                }
                Child::Capabilities => {
                    capabilities::Worker::spawn(config.capabilities.ok_or(Error::Missing)?)
                        .map(Started::Capabilities)
                        .map_err(|e| Error::Start {
                            child,
                            reason: e.to_string(),
                        })
                }
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
            Started::Upnp(worker) => {
                self.upnp_binding
                    .as_ref()
                    .expect("configured UPnP")
                    .install(worker.client(), generation);
                self.upnp = Some(worker);
                self.current.upnp_generation = generation;
            }
            Started::Votes(worker, publications) => {
                self.votes_binding
                    .as_ref()
                    .expect("configured PoS vote pool")
                    .install(worker.client(), generation);
                self.vote_publications = Some(publications);
                self.votes = Some(worker);
                self.current.votes_generation = generation;
            }
            Started::Resilience(worker, owner) => {
                if let Some(owner) = owner {
                    self.resilience_owner = Some(owner);
                }
                self.resilience_binding
                    .as_ref()
                    .expect("configured resilience")
                    .install(worker.client(), generation);
                self.resilience = Some(worker);
                self.current.resilience_generation = generation;
            }
            Started::Capabilities(worker) => {
                self.capabilities_binding
                    .as_ref()
                    .expect("configured capabilities")
                    .install(worker.client(), generation);
                self.capabilities = Some(worker);
                self.current.capabilities_generation = generation;
            }
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
            Child::Upnp => {
                if let Some(binding) = &self.upnp_binding {
                    binding.withdraw();
                }
                self.upnp.take().map(|worker| {
                    worker.request_shutdown();
                    self.jobs.submit(128, move |_| {
                        let _ = worker.shutdown();
                    })
                })
            }
            Child::Votes => {
                if let Some(binding) = &self.votes_binding {
                    binding.withdraw();
                }
                self.votes.take().map(|worker| {
                    worker.request_shutdown();
                    self.jobs.submit(128, move |_| {
                        let _ = worker.shutdown();
                    })
                })
            }
            Child::Resilience => {
                if let Some(binding) = &self.resilience_binding {
                    binding.withdraw();
                }
                self.resilience.take().map(|worker| {
                    worker.request_shutdown();
                    self.jobs.submit(128, move |_| {
                        let _ = worker.shutdown();
                    })
                })
            }
            Child::Capabilities => {
                if let Some(binding) = &self.capabilities_binding {
                    binding.withdraw();
                }
                self.capabilities.take().map(|worker| {
                    worker.request_shutdown();
                    self.jobs.submit(128, move |_| {
                        let _ = worker.shutdown();
                    })
                })
            }
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
        if self.config.upnp.is_some() {
            self.start(Child::Upnp).await?;
        }
        if self.stopping() {
            return Ok(());
        }
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
        if self.config.resilience.is_some() {
            self.start(Child::Resilience).await?;
        }
        if self.stopping() {
            return Ok(());
        }
        if self.config.capabilities.is_some() {
            self.start(Child::Capabilities).await?;
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
        if self.config.pool.consensus == ConsensusType::Pos {
            self.start(Child::Votes).await?;
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
            let exited = if self.upnp.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::Upnp)
            } else if self.routing.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::Routing)
            } else if self.keys.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::Keys)
            } else if self.admission.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::Admission)
            } else if self.analytics.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::Analytics)
            } else if self.pool_gc.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::PoolMaintenance)
            } else if self.resilience.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::Resilience)
            } else if self
                .capabilities
                .as_ref()
                .is_some_and(|w| !w.info().running)
            {
                Some(Child::Capabilities)
            } else if self.gc.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::GarbageCollection)
            } else if self.votes.as_ref().is_some_and(|w| !w.info().running) {
                Some(Child::Votes)
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
                    let result = if (command.child == Child::Votes && self.config.pool.consensus != ConsensusType::Pos) || (command.child == Child::Upnp && self.config.upnp.is_none()) || (command.child == Child::Resilience && self.config.resilience.is_none()) || (command.child == Child::Capabilities && self.config.capabilities.is_none()) || (command.child == Child::PoolMaintenance && self.config.pool_gc.is_none()) || (command.child == Child::Analytics && self.config.analytics.is_none()) || (command.child == Child::Routing && self.config.routing.is_none()) || (command.child == Child::GarbageCollection && self.config.gc.is_none()) || (command.child == Child::Keys && self.config.keys.is_none()) { Err(Error::Missing) }
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
                Child::Upnp => self.upnp.as_ref().unwrap().request_shutdown(),
                Child::Votes => self.votes.as_ref().unwrap().request_shutdown(),
                Child::Resilience => self.resilience.as_ref().unwrap().request_shutdown(),
                Child::Capabilities => self.capabilities.as_ref().unwrap().request_shutdown(),
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
    async fn prepare_upnp(&mut self) {
        let Some(client) = self.upnp.as_ref().map(|w| w.client()) else {
            return;
        };
        if !client.info().running {
            return;
        }
        let result = if client.info().accepting {
            match client.prepare_stop() {
                Ok(ticket) => tokio::time::timeout(upnp::CLEANUP_TIMEOUT, ticket)
                    .await
                    .unwrap_or(Err(upnp::Error::Timeout)),
                Err(e) => Err(e),
            }
        } else {
            // An explicit application prepare_stop may already be in flight.
            tokio::time::timeout(upnp::CLEANUP_TIMEOUT, async {
                loop {
                    let info = client.info();
                    if info.prepared || !info.running {
                        break info.last_error.map_or(Ok(()), Err);
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap_or(Err(upnp::Error::Timeout))
        };
        self.current.upnp_cleanup_error = result.err();
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
        self.prepare_upnp().await;
        if let Some(binding) = &self.upnp_binding {
            binding.withdraw();
        }
        self.pool_binding.withdraw();
        if let Some(binding) = &self.votes_binding {
            binding.withdraw();
        }
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
        if let Some(binding) = &self.capabilities_binding {
            binding.withdraw();
        }
        if let Some(binding) = &self.resilience_binding {
            binding.withdraw();
        }
        for child in [
            Child::Votes,
            Child::GarbageCollection,
            Child::Capabilities,
            Child::Resilience,
            Child::PoolMaintenance,
            Child::Analytics,
            Child::Admission,
            Child::Keys,
            Child::Routing,
            Child::Upnp,
        ] {
            if let Err(error) = self.retire(child).await {
                self.current.error.get_or_insert(error);
            }
        }
        if let Some(binding) = &self.capabilities_binding {
            binding.close();
        }
        if let Some(binding) = &self.votes_binding {
            binding.close();
        }
        if let Some(publications) = &self.vote_publications {
            publications.close();
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
        if let Some(binding) = &self.resilience_binding {
            binding.close();
        }
        if let Some(binding) = &self.upnp_binding {
            binding.close();
        }
        self.resilience_owner.take();
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
        if let Some(binding) = &self.upnp_binding {
            binding.close();
        }
        if let Some(binding) = &self.resilience_binding {
            binding.close();
        }
        if let Some(binding) = &self.keys_binding {
            binding.close();
        }
        if let Some(binding) = &self.capabilities_binding {
            binding.close();
        }
        if let Some(binding) = &self.votes_binding {
            binding.close();
        }
        if let Some(publications) = &self.vote_publications {
            publications.close();
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
