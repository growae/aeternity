// Continuous PoS production and owned child shutdown. Scheduling follows
// aec_conductor (aeternity fa2a6d7); ISC copyright: repository LICENSE.
use super::{delivery, eoe, parent, pinning, scoped};
use crate::{chain, resilience, votes};
use ae_chain::{publication, repository as repo};
use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::{
    eoe::{coordinator, local_host, runtime as eoe_runtime},
    parent_cache::{self, runtime as cache},
    parent_connector::{fetch::Backend, runtime as connector},
};
use ae_storage::{Database, events, pool, snapshots};
use ae_types::AccountPublicKey;
use ae_vm_fate::maps::MapOrder;
use eoe::owner::{self, production};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Instant,
};

pub mod pool_supervision;
pub mod runtime;
pub mod session;
pub mod supervision;

pub struct ParentConfig {
    pub pins: Option<PinConfig>,
    pub cache: parent_cache::Config,
    pub cache_runtime: cache::Config,
    pub connector: connector::Config,
    pub hosts: usize,
    pub seed: [u8; 16],
}
pub struct PinConfig {
    pub chain: crate::parent_pins::Chain,
    pub network: Vec<u8>,
    pub accounts: BTreeMap<AccountPublicKey, AccountPublicKey>,
    pub signing: Option<crate::parent_pins::posting::Signing>,
    pub agent: Option<pinning::Config>,
    pub nodes: Vec<crate::parent_pins::Node>,
    pub service: crate::parent_pins::Config,
    pub journal: ae_storage::parent_posts::Limits,
}
pub struct Config {
    pub eoe: owner::Config,
    pub production: production::Config,
    pub pool: pool::Config,
    pub pool_worker: pool::worker::Settings,
    pub admission: chain::Config,
    pub snapshots: snapshots::Config,
    pub resilience: Option<resilience::Config>,
    pub gossip: pool::worker::GossipCache,
    pub parent: ParentConfig,
    pub events_per_step: usize,
    pub producing: bool,
    pub block_producer: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    NotBlockProducer,
    Time,
    Closed,
    Backpressured,
    Source(eoe::Error),
    Repository(ae_chain::repository::Error),
    Events(events::Error),
    Rollback(u64),
    Importer(chain::Error),
    Pool(pool::worker::Error),
    Votes(votes::Error),
    Resilience(resilience::Error),
    Parent(String),
    Owner(owner::Error),
    Delivery(delivery::Error),
    Production(production::Error),
    Pinning(pinning::Error),
    Utilities(parent::Error),
    StaleSession { requested: u64, current: u64 },
    SessionGenerationLimit,
    DriverPanicked,
    ConsensusRestartLimit,
    PoolRestartLimit,
    Supervision(eoe_runtime::Output),
}
fn eoe_departure(error: &Error) -> bool {
    use owner::candidate;
    match error {
        Error::Owner(e) | Error::Production(production::Error::Owner(e)) => {
            owner::recovery::departure(e)
        }
        Error::Delivery(e) | Error::Production(production::Error::Effects(e)) => {
            delivery::eoe_departure(e)
        }
        Error::Production(production::Error::Closed) => true,
        Error::Production(production::Error::Build(e)) => match e {
            candidate::Error::Closed | candidate::Error::Request(owner::RequestError::Unknown) => {
                true
            }
            candidate::Error::Owner(e) => owner::recovery::departure(e),
            candidate::Error::Effects(e) => delivery::eoe_departure(e),
            _ => false,
        },
        _ => false,
    }
}
fn key_departure(error: &Error) -> bool {
    use crate::preset_keys::Error as KeyError;
    match error {
        Error::Owner(e) | Error::Production(production::Error::Owner(e)) => owner::key_departure(e),
        Error::Production(production::Error::Preset(
            KeyError::Stopped | KeyError::Disconnected,
        )) => true,
        Error::Delivery(delivery::Error::Preset(KeyError::Stopped | KeyError::Disconnected))
        | Error::Production(production::Error::Effects(delivery::Error::Preset(
            KeyError::Stopped | KeyError::Disconnected,
        ))) => true,
        _ => false,
    }
}
#[derive(Debug)]
pub enum Output {
    SessionReplaced {
        retired: session::Status,
        generation: u64,
    },
    Pinning(Box<pinning::Report>),
    ConsensusSupervision(supervision::Notice),
    VoteSupervision(owner::vote_pool::Notice),
    PoolSupervision(pool_supervision::Notice),
    Attempt(production::Completed),
    Stopped(Box<production::Stopped>),
    Rejected {
        error: production::Error,
        parent: ae_types::Hash256,
        micro: Option<chain::Accepted>,
    },
    // This output is retained until consumed. It is never treated as proof
    // that a failed service was recovered or its accepted work was cancelled.
    Supervision(eoe_runtime::Output),
}
impl Output {
    pub fn poll_pinning(&mut self) {
        match self {
            Self::Pinning(report) => report.poll_children(),
            Self::ConsensusSupervision(notice) => notice.poll_children(),
            _ => (),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    Idle,
    Advanced,
    Started,
    Retrying,
    Production(production::Progress),
    Output,
}
#[derive(Debug, Clone)]
pub struct Status {
    pub session_generation: u64,
    pub session_replacing: bool,
    pub consensus: supervision::Status,
    pub votes: owner::vote_pool::Status,
    pub pool: pool_supervision::Status,
    pub producing: bool,
    pub stopping: bool,
    pub stops: u64,
    pub block_producer: bool,
    pub attempts: u64,
    pub completed: u64,
    pub rejected: u64,
    pub retries: u64,
    pub selected_created_keys: u64,
    pub top_changes: u64,
    pub epoch_revision: u64,
    pub cursor: events::Cursor,
    pub deadline: Option<Instant>,
    pub failure: Option<Error>,
}
#[derive(Clone)]
struct Observations {
    pub consensus: supervision::Status,
    pub votes: owner::vote_pool::Status,
    pub selected_created_keys: u64,
    pub top_changes: u64,
    pub epoch_revision: u64,
    pub cursor: events::Cursor,
    pub failure: Option<Error>,
}

// A shutdown report preserves transient work instead of replaying it into
// replacement children. Receipt results are observed only after importer join.
pub struct Shutdown<O> {
    pub status: Status,
    pub output: Option<Output>,
    pub attempt: Option<production::Attempt<O>>,
    pub retiring: Option<production::Retiring<O>>,
    pub session_retirement: Option<session::Status>,
    pub production_driver: Option<runtime::DriverReport>,
    pub admission: Option<Result<chain::Report, chain::Failure>>,
    pub owner: owner::Shutdown,
    pub delivery: delivery::Delivery<O, eoe::StateReader, eoe::StateReader>,
    pub accepted: Result<repo::hyperchain::inputs::Effects, chain::Error>,
    pub queued_outputs: Vec<Output>,
    pub tops: Option<TopDelivery>,
    pub pool: Result<pool::worker::Info, pool::worker::Error>,
    pub pool_notices: Vec<pool_supervision::Notice>,
    pub parent: Option<Result<cache::Exit, String>>,
    pub connector_joined: bool,
    pub pins_joined: bool,
    pub pinning: Option<Result<pinning::Exit, pinning::Error>>,
    pub consensus: supervision::Shutdown,
    pub resilience: Option<Result<(), resilience::Error>>,
}
pub struct TopDelivery {
    pub records: VecDeque<Arc<publication::Payload>>,
    pub after: events::Cursor,
    pub receipt: Option<cache::Call>,
    cache: Option<cache::Client>,
}
// Public clients name fixed child generations. Only this owner can publish
// replacement connector routes or construct a new pinning worker.
pub struct Conductor<O: MapOrder + Clone + Send + Sync + 'static> {
    services: Services<O>,
    session: session::Live<O>,
    session_config: session::Config,
    session_retirement: Option<session::Status>,
    production: production::Config,
    retiring: Option<production::Retiring<O>>,
    pending: Option<Output>,
    status: Observations,
    tick: Option<Instant>,
    production_tick: Option<Instant>,
}

struct Services<O: MapOrder + Clone + Send + Sync + 'static> {
    owner: Option<owner::Owner>,
    importer: Arc<chain::Importer<O>>,
    tops: Option<TopDelivery>,
    producer: scoped::Producer<O>,
    delivery: Option<delivery::Delivery<O, eoe::StateReader, eoe::StateReader>>,
    pool: pool_supervision::Pool,
    parent: Option<cache::Runtime>,
    connector: Option<connector::Runtime>,
    pins: Option<crate::parent_pins::Service>,
    pinning: Option<pinning::Service>,
    consensus: supervision::Supervisor<O>,
    parent_utilities: Option<parent::Client<O>>,
    resilience: Option<resilience::Owner>,
    resilience_worker: Option<resilience::Worker>,
    resilience_config: Option<resilience::Config>,
    source: events::Reader,
    event_limit: usize,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Conductor<O> {
    pub fn start(
        database: Arc<Database>,
        execution: repo::HyperchainConfig<O>,
        keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
        config: Config,
        backend: Arc<impl Backend>,
    ) -> Result<Self, Error> {
        Self::start_inner(
            database,
            execution,
            keys,
            config,
            backend,
            |source, execution| {
                super::targets::Live::new(&source, execution).map_err(Error::Source)
            },
        )
    }
    // Explicit alternate provider for controlled networks and reference tests.
    // Ordinary startup derives targets from the same live snapshot authority
    // used by committed epochs and candidate/EOE state access.
    pub fn start_with_targets(
        database: Arc<Database>,
        execution: repo::HyperchainConfig<O>,
        keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
        config: Config,
        backend: Arc<impl Backend>,
        targets: impl cache::Targets,
    ) -> Result<Self, Error> {
        Self::start_inner(database, execution, keys, config, backend, |_, _| {
            Ok(targets)
        })
    }
    fn start_inner<T: cache::Targets>(
        database: Arc<Database>,
        execution: repo::HyperchainConfig<O>,
        keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
        mut config: Config,
        backend: Arc<impl Backend>,
        targets: impl FnOnce(Arc<eoe::Source>, &repo::HyperchainConfig<O>) -> Result<T, Error>,
    ) -> Result<Self, Error> {
        execution
            .validate_execution()
            .map_err(|e| Error::Importer(e.into()))?;
        if config.producing && !config.block_producer {
            return Err(Error::NotBlockProducer);
        }
        if config
            .parent
            .pins
            .as_ref()
            .is_some_and(|p| p.network.len() > 4096 || p.accounts.len() > 65536)
            || config.events_per_step == 0
            || config.production.parent_pins.is_some()
            || (config.parent.pins.is_some() && !config.production.pins.is_empty())
            || config.events_per_step > 65_536
            || config.pool.consensus != ae_protocol::ConsensusType::Pos
            || !(repo::hyperchain::producer::Binding {
                network: config.eoe.eoe.host.network.clone(),
                election: config.eoe.eoe.host.election_contract,
                parent_start: config.eoe.eoe.host.parent_start_height.clone(),
                parent_length: config.eoe.eoe.host.parent_epoch_length.clone(),
                block_time: config.eoe.eoe.block_time.clone(),
            })
            .matches(&execution)
        {
            return Err(Error::Configuration);
        }
        if config.parent.pins.as_ref().is_some_and(|p| {
            p.chain == crate::parent_pins::Chain::Aeternity
                && p.signing.as_ref().is_some_and(|s| s.network != p.network)
        }) {
            return Err(Error::Utilities(parent::Error::Configuration));
        }
        // Install the policy parent before capturing the importer's scope.
        // Its child may later move to optional-worker supervision; the policy
        // parent remains with this producer generation throughout child gaps.
        let resilience = config
            .resilience
            .map(|c| resilience::Owner::new(database.clone(), c))
            .transpose()
            .map_err(Error::Resilience)?;
        let resilience_worker = resilience
            .as_ref()
            .map(resilience::Owner::start)
            .transpose()
            .map_err(Error::Resilience)?;
        let events = database.events();
        let cursor = events.cursor();
        let (child, child_revision) = {
            let view = database.view().map_err(|e| Error::Importer(e.into()))?;
            let hash = view.status().tip.ok_or(Error::Configuration)?;
            let block = view
                .require_block(hash)
                .map_err(|e| Error::Importer(e.into()))?;
            (
                parent_cache::ChildTop {
                    hash,
                    height: block.header().common().height.into(),
                },
                view.status().revision,
            )
        };
        let selected = eoe::Source::new(
            database.clone(),
            execution.identity.clone(),
            execution.profile.network.clone(),
            config.snapshots,
        )
        .map_err(Error::Source)?;
        let targets = targets(selected.clone(), &execution)?;
        let keys = Arc::new(keys);
        let mut configured_keys = ae_hyperchain::signing::Keys::from_shared(keys.clone());
        if let Some(pins) = &config.parent.pins
            && let Some(signing) = &pins.signing
        {
            match &signing.keys {
                crate::parent_pins::posting::SigningKeys::Local(parent_keys) => {
                    configured_keys = configured_keys.merged(parent_keys);
                }
                crate::parent_pins::posting::SigningKeys::Worker(_) => {
                    return Err(Error::Configuration);
                }
            }
        }
        let key_service = owner::KeyService::start(&database, configured_keys)
            .map_err(|error| Error::Owner(owner::Error::Preset(error)))?;
        // Build both directions together. Only supervision owns the connector link.
        let agent = config.parent.pins.as_ref().and_then(|p| p.agent.clone());
        let pin_execution = config.parent.pins.as_ref().map(|_| execution.clone());
        let metadata = config.parent.pins.as_ref().map(|p| {
            (
                p.network.clone(),
                p.accounts.clone(),
                p.service.maximum_payload_bytes,
            )
        });
        let pins = config
            .parent
            .pins
            .take()
            .map(|mut pins| {
                let journal = crate::parent_pins::posting::Journal {
                    database: database.clone(),
                    network: pins.network.clone(),
                    limits: pins.journal,
                };
                if let Some(signing) = &mut pins.signing {
                    signing.keys =
                        crate::parent_pins::posting::SigningKeys::Worker(key_service.binding());
                }
                crate::parent_pins::Service::spawn_journalled(
                    pins.chain,
                    pins.nodes,
                    pins.service,
                    pins.signing,
                    journal,
                )
            })
            .transpose()
            .map_err(|e| Error::Parent(format!("pin service: {e:?}")))?;
        config.production.parent_pins = pins.as_ref().map(crate::parent_pins::Service::client);
        let mailbox = cache::Mailbox::new(
            parent_cache::Cache::new(config.parent.cache.clone(), child),
            config.parent.cache_runtime,
        )
        .map_err(|e| Error::Parent(e.to_string()))?;
        let route = cache::Route::new(mailbox.client());
        let (connector, connector_factory) = supervision::ConnectorFactory::start(
            backend,
            config.parent.hosts,
            config.parent.seed,
            route.clone(),
            config.parent.connector,
        )
        .map_err(|e| Error::Parent(e.to_string()))?;
        let parent = mailbox
            .spawn(
                targets.clone(),
                connector_factory.link(),
                execution.profile.order.clone(),
            )
            .map_err(|e| Error::Parent(e.to_string()))?;
        let cache_factory = supervision::CacheFactory::new(
            supervision::CacheInputs {
                database: database.clone(),
                config: config.parent.cache,
                runtime: config.parent.cache_runtime,
                targets,
                order: execution.profile.order.clone(),
                connector: connector_factory.link(),
                route: route.clone(),
            },
            parent.client(),
            child_revision,
        );
        if config.eoe.calls.requests == 0 {
            return Err(Error::Owner(owner::Error::Configuration));
        }
        let mut pool = pool_supervision::Pool::start(
            database.clone(),
            execution.profile.network.clone(),
            config.pool,
            config.pool_worker,
            config.gossip.clone(),
            config.eoe.calls.requests,
        )
        .map_err(Error::Pool)?;
        let order = execution.profile.order.clone();
        let cached = scoped::Cached {
            cache: parent.client(),
            pins: match &config.production.parent_pins {
                Some(client) => scoped::PinSource::Remote(client.clone()),
                None => scoped::PinSource::Captured(config.production.pins.clone()),
            },
            parent_start: config.eoe.eoe.host.parent_start_height.clone(),
            parent_length: config.eoe.eoe.host.parent_epoch_length.clone(),
            limits: config.production.inputs,
        };
        let (importer, accepted) = chain::Importer::new_hyperchain_scoped(
            database.clone(),
            execution,
            scoped::RoutedCached::new(cached, route.clone()),
            config.admission,
        )
        .map_err(Error::Importer)?;
        let producer = accepted.producer();
        let parent_utilities = metadata
            .map(|(network, accounts, maximum_payload_bytes)| {
                parent::Client::new(parent::Inputs {
                    database: database.clone(),
                    source: selected.clone(),
                    execution: pin_execution.as_ref().expect("pin execution").clone(),
                    producer: producer.clone(),
                    pins: pins.as_ref().expect("pin service").client(),
                    network,
                    accounts,
                    maximum_payload_bytes,
                })
            })
            .transpose()
            .map_err(Error::Utilities)?;
        let publications =
            votes::Publications::new(&database, config.eoe.votes).map_err(Error::Votes)?;
        let owner = owner::Owner::start_routed(
            owner::Inputs {
                source: selected.clone(),
                database: database.clone(),
                publications,
                gossip: config.gossip,
                parent: Some(parent.client()),
            },
            config.eoe,
            keys,
            order,
            route.clone(),
            key_service,
        )
        .map_err(Error::Owner)?;
        pool.set_group(owner.worker_group());
        let mut delivery = owner.parent_delivery(accepted).map_err(Error::Owner)?;
        delivery.set_route(route);
        let notices = agent.as_ref().map_or(1, |c| c.outputs);
        let (pinning, factory) = agent
            .map(|config| {
                pinning::Factory::start_managed(
                    pinning::Inputs {
                        database,
                        source: selected,
                        execution: pin_execution.expect("agent execution"),
                        producer: producer.clone(),
                        pool: pool.client(),
                        parent: parent_utilities.as_ref().expect("parent utilities").clone(),
                        keys: owner.signing().clone(),
                    },
                    config,
                    pool.binding(),
                    owner.preset_key_binding(),
                )
            })
            .transpose()
            .map_err(Error::Pinning)?
            .map_or((None, None), |(child, factory)| {
                (Some(child), Some(factory))
            });
        let consensus =
            supervision::Supervisor::new(factory, connector_factory, Some(cache_factory), notices);
        let consensus_status = consensus.status();
        let votes_status = owner.vote_status();
        let session_config = session::Config {
            producing: config.producing,
            block_producer: config.block_producer,
        };
        importer
            .database()
            .publish_selected_height()
            .map_err(Error::Repository)?;
        Ok(Self {
            services: Services {
                owner: Some(owner),
                importer: Arc::new(importer),
                tops: None,
                producer,
                delivery: Some(delivery),
                pool,
                parent: Some(parent),
                connector: Some(connector),
                pins,
                pinning,
                consensus,
                parent_utilities,
                resilience,
                resilience_worker,
                resilience_config: config.resilience,
                source: events,
                event_limit: config.events_per_step,
            },
            session: session::Live::new(session_config, 1),
            session_config,
            session_retirement: None,
            production: config.production,
            retiring: None,
            pending: None,
            tick: None,
            production_tick: None,
            status: Observations {
                consensus: consensus_status,
                votes: votes_status,
                selected_created_keys: 0,
                top_changes: 0,
                epoch_revision: cursor.revision(),
                cursor,
                failure: None,
            },
        })
    }
    pub fn status(&self) -> Status {
        Status {
            session_generation: self.session.status.generation,
            session_replacing: self.session_retirement.is_some(),
            producing: self.session.status.producing,
            stopping: self.session.status.stopping,
            stops: self.session.status.stops,
            block_producer: self.session.status.block_producer,
            attempts: self.session.status.attempts,
            completed: self.session.status.completed,
            rejected: self.session.status.rejected,
            retries: self.session.status.retries,
            deadline: self.session.status.deadline,
            consensus: self.status.consensus.clone(),
            votes: self
                .services
                .owner
                .as_ref()
                .map_or_else(|| self.status.votes.clone(), owner::Owner::vote_status),
            pool: self.services.pool.status(),
            selected_created_keys: self.status.selected_created_keys,
            top_changes: self.status.top_changes,
            epoch_revision: self.status.epoch_revision,
            cursor: self.status.cursor.clone(),
            failure: self.status.failure.clone(),
        }
    }

    pub fn importer(&self) -> Arc<chain::Importer<O>> {
        self.services.importer.clone()
    }
    pub fn pool(&self) -> pool::worker::Client {
        self.services.pool.client()
    }
    pub fn pool_binding(&self) -> crate::bindings::Admission {
        self.services.pool.binding()
    }
    pub fn request_pool_restart(
        &self,
        generation: u64,
    ) -> Result<(), pool_supervision::ControlError> {
        self.services.pool.request_restart(generation)
    }
    pub fn votes(&self) -> votes::Client {
        self.services.owner.as_ref().expect("owner").votes()
    }
    pub fn vote_binding(&self) -> votes::Binding {
        self.services.owner.as_ref().expect("owner").vote_binding()
    }
    pub fn preset_key_binding(&self) -> crate::bindings::PresetKeys {
        self.services
            .owner
            .as_ref()
            .expect("owner")
            .preset_key_binding()
    }
    pub fn request_key_restart(&self, generation: u64) -> Result<(), Error> {
        if self.status.failure.is_some() || generation != self.status.consensus.key_generation {
            return Err(Error::Configuration);
        }
        self.services
            .owner
            .as_ref()
            .expect("owner")
            .request_keys_restart()
            .map_err(Error::Owner)
    }
    pub fn request_vote_restart(
        &self,
        generation: u64,
    ) -> Result<(), owner::vote_pool::ControlError> {
        self.services
            .owner
            .as_ref()
            .expect("owner")
            .request_vote_restart(generation)
    }
    pub(crate) fn worker_group(&self) -> crate::runtime::restart::workers::Group {
        self.services.owner.as_ref().expect("owner").worker_group()
    }
    pub fn eoe(&self) -> eoe::Client {
        self.services
            .owner
            .as_ref()
            .expect("owner")
            .runtime_client()
    }
    pub(crate) fn optional_resilience(
        &mut self,
    ) -> Option<(
        resilience::Source,
        resilience::Config,
        Option<resilience::Worker>,
    )> {
        self.services.resilience.as_ref().map(|owner| {
            (
                owner.source(),
                self.services
                    .resilience_config
                    .expect("configured resilience"),
                self.services.resilience_worker.take(),
            )
        })
    }
    pub fn parent(&self) -> cache::Client {
        self.services.consensus.parent()
    }
    pub fn connector(&self) -> connector::Client {
        self.services.consensus.connector()
    }
    pub fn parent_pins(&self) -> Option<crate::parent_pins::Client> {
        self.services
            .pins
            .as_ref()
            .map(crate::parent_pins::Service::client)
    }
    pub fn parent_utilities(&self) -> Option<parent::Client<O>> {
        self.services.parent_utilities.clone()
    }
    pub fn pinning(&self) -> Option<pinning::Handle> {
        self.services.pinning.as_ref().map(pinning::Service::handle)
    }
    pub fn attempt(&self) -> Option<&production::Attempt<O>> {
        self.session.attempt.as_ref()
    }
    pub fn output(&self) -> Option<&Output> {
        self.pending.as_ref()
    }
    pub fn take_output(&mut self) -> Option<Output> {
        self.pending.take()
    }
    pub fn start_production(&mut self) -> Result<(), Error> {
        if self.session_retirement.is_some() {
            return Err(Error::Backpressured);
        }
        if !self.session.status.block_producer {
            return Err(Error::NotBlockProducer);
        }
        if let Some(e) = &self.status.failure {
            return Err(e.clone());
        }
        if !self.session.status.producing {
            self.session.status.producing = true;
            self.session.restart = true;
        }
        Ok(())
    }
    // Cancellation prevents new producer work immediately. Previously accepted
    // calls are retired by step before another attempt can acquire the lease.
    // Independent admission, pool, votes and parent services remain available.
    pub fn stop_production(&mut self) -> Result<(), Error> {
        self.session.status.producing = false;
        self.session.restart = false;
        self.session.status.deadline = None;
        if let Some(attempt) = self.session.attempt.take() {
            match self
                .services
                .owner
                .as_mut()
                .expect("owner")
                .retire_production(attempt)
            {
                Ok(retiring) => {
                    self.retiring = Some(retiring);
                    self.session.status.stopping = true;
                }
                Err(attempt) => {
                    self.session.attempt = Some(*attempt);
                    let error = Error::Production(production::Error::Binding);
                    self.status.failure = Some(error.clone());
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    pub fn step(&mut self, wall_ms: u64, now: Instant) -> Result<Progress, Error> {
        if let Some(e) = &self.status.failure {
            return Err(e.clone());
        }
        self.driven_step(Some((wall_ms, now)), now)
    }
    fn driven_step(
        &mut self,
        production: Option<(u64, Instant)>,
        now: Instant,
    ) -> Result<Progress, Error> {
        if let Some(error) = &self.status.failure {
            return Err(error.clone());
        }
        let result = self.advance(production, now);
        if let Err(e) = &result {
            if self.services.pool.replacement_pending()
                && matches!(
                    e,
                    Error::Production(production::Error::Closed)
                        | Error::Production(production::Error::Build(
                            owner::candidate::Error::Closed
                        ))
                        | Error::Production(production::Error::Build(
                            owner::candidate::Error::Packing(super::packing::Error::Pool {
                                error: pool::worker::Error::Stopped
                                    | pool::worker::Error::Disconnected,
                                ..
                            })
                        ))
                )
            {
                return Ok(Progress::Advanced);
            }
            if self
                .services
                .owner
                .as_ref()
                .is_some_and(owner::Owner::replacement_pending)
                && eoe_departure(e)
            {
                return Ok(Progress::Advanced);
            }
            if self
                .services
                .owner
                .as_ref()
                .is_some_and(owner::Owner::keys_replacement_pending)
                && key_departure(e)
            {
                return Ok(Progress::Advanced);
            }
            self.status.failure = Some(e.clone());
            self.services.pool.close();
            self.services
                .owner
                .as_ref()
                .expect("owner")
                .close_vote_binding();
            self.services.owner.as_ref().expect("owner").close_keys();
            if matches!(e, Error::ConsensusRestartLimit) {
                self.eoe().request_stop();
                self.parent().request_stop();
                self.connector().request_stop();
                if let Some(agent) = &self.services.pinning {
                    agent.handle().request_stop();
                }
                if let Some(pins) = &self.services.pins {
                    pins.stop();
                }
            }
            if matches!(
                e,
                Error::Owner(owner::Error::VoteRestartLimit) | Error::PoolRestartLimit
            ) {
                self.services.owner.as_ref().expect("owner").stop_votes();
                self.services.pool.request_shutdown();
                if let Some(worker) = &self.services.resilience_worker {
                    worker.request_shutdown();
                }
            }
        }
        result
    }
    fn advance(
        &mut self,
        production: Option<(u64, Instant)>,
        now: Instant,
    ) -> Result<Progress, Error> {
        if self.tick.is_some_and(|last| now < last) {
            return Err(Error::Time);
        }
        self.tick = Some(now);
        self.services.producer.check().map_err(Error::Importer)?;
        self.services
            .owner
            .as_mut()
            .expect("owner")
            .poll_votes()
            .map_err(Error::Owner)?;
        let health = self.parent().snapshot().health;
        if matches!(health, cache::Health::Failed(_))
            && self
                .services
                .parent
                .as_ref()
                .is_some_and(|p| !p.replacement_pending())
        {
            return Err(Error::Parent(format!("cache: {health:?}")));
        }
        let health = self.connector().snapshot().health;
        if matches!(health, connector::Health::WorkStopped(_)) {
            return Err(Error::Parent(format!("connector: {health:?}")));
        }
        if (self.services.pool.replacement_pending()
            || self
                .services
                .parent
                .as_ref()
                .is_some_and(cache::Runtime::replacement_pending)
            || self
                .services
                .owner
                .as_ref()
                .is_some_and(owner::Owner::replacement_pending)
            || self
                .services
                .owner
                .as_ref()
                .is_some_and(owner::Owner::keys_replacement_pending))
            && let Some(attempt) = self.session.attempt.take()
        {
            match self
                .services
                .owner
                .as_mut()
                .expect("owner")
                .retire_production(attempt)
            {
                Ok(retiring) => {
                    self.retiring = Some(retiring);
                    self.session.status.stopping = true;
                    self.session.restart = self.session.status.producing;
                    self.session.status.deadline = None;
                }
                Err(attempt) => {
                    self.session.attempt = Some(*attempt);
                    return Err(Error::Production(production::Error::Binding));
                }
            }
        }
        let retiring = &mut self.retiring;
        self.services.pool.poll(|old| {
            if let Some(retiring) = retiring {
                retiring.retire_pool(old).map_err(Error::Production)?;
            }
            Ok(())
        })?;
        let old_cache_generation = self.status.consensus.cache_generation;
        let delivery = self.services.delivery.as_mut().expect("delivery");
        let tops = &mut self.services.tops;
        let retiring = &mut self.retiring;
        let supervised = self
            .services
            .consensus
            .poll_cache(&mut self.services.parent, now, |old, exit| {
                exit.delivery = delivery.retire_cache(old).map_err(Error::Delivery)?;
                if let Some(tops) = tops
                    && tops.cache.as_ref().is_some_and(|c| c.same_runtime(old))
                    && let Some(call) = &mut tops.receipt
                {
                    let result = match call.try_reply() {
                        Ok(Some(result)) => result,
                        Err(e) => Err(e),
                        Ok(None) => return Err(Error::Configuration),
                    };
                    tops.receipt = None;
                    tops.cache = None;
                    exit.top = Some(supervision::RetiredTop {
                        record: tops.records.front().expect("top receipt").clone(),
                        result,
                    });
                }
                if let Some(retiring) = retiring {
                    retiring.retire_cache(old).map_err(Error::Production)?;
                }
                Ok(())
            })
            .and_then(|_| {
                self.services.consensus.poll(
                    &mut self.services.pinning,
                    &mut self.services.connector,
                    now,
                )
            })
            .and_then(|_| {
                self.services
                    .consensus
                    .poll_keys(self.services.owner.as_mut().expect("owner"), now)
            })
            .and_then(|_| {
                self.services.consensus.poll_eoe(
                    self.services.owner.as_mut().expect("owner"),
                    now,
                    |old, exit| {
                        exit.delivery = delivery.retire_eoe(old).map_err(Error::Delivery)?;
                        if let Some(retiring) = retiring {
                            retiring.retire_eoe(old).map_err(Error::Production)?;
                        }
                        Ok(())
                    },
                )
            });
        self.status.consensus = self.services.consensus.status();
        supervised?;
        if self.status.consensus.cache_generation != old_cache_generation {
            self.services
                .owner
                .as_mut()
                .expect("owner")
                .replace_parent(self.services.consensus.parent());
        }
        // Retained outputs apply backpressure to production, not ownership:
        // shutdown still joins every child and transfers this output intact.
        if self.pending.is_some() {
            return Ok(Progress::Output);
        }
        if let Some(notice) = self.services.pool.take_notice() {
            self.pending = Some(Output::PoolSupervision(notice));
            return Ok(Progress::Output);
        }
        if let Some(notice) = self
            .services
            .owner
            .as_mut()
            .expect("owner")
            .take_vote_notice()
        {
            self.pending = Some(Output::VoteSupervision(notice));
            return Ok(Progress::Output);
        }
        if let Some(notice) = self.services.consensus.take_notice() {
            self.status.consensus = self.services.consensus.status();
            self.pending = Some(Output::ConsensusSupervision(notice));
            return Ok(Progress::Output);
        }
        if self.status.consensus.cache_restarting {
            return Ok(Progress::Advanced);
        }
        if self.status.consensus.keys_restarting {
            return Ok(Progress::Advanced);
        }
        if self.status.consensus.eoe_restarting
            && !self.services.owner.as_ref().expect("owner").recovering()
        {
            return Ok(Progress::Advanced);
        }
        if let Some(agent) = &self.services.pinning {
            if let Some(report) = agent.try_output() {
                self.pending = Some(Output::Pinning(Box::new(report)));
                return Ok(Progress::Output);
            }
            if let Some(error) = agent.handle().status().failure
                && !agent.replacement_pending()
            {
                return Err(Error::Pinning(error));
            }
        }
        let mut advanced = self.consume_events()?;
        let owner = self.services.owner.as_mut().expect("owner");
        match owner.step().map_err(Error::Owner)? {
            owner::Progress::Output(eoe_runtime::Output::Service(
                coordinator::ServiceOutput::Publication(local_host::Output::ParentBlock { .. }),
            )) => advanced = true,
            owner::Progress::Output(o) => {
                // The final native stop output can become visible just before
                // its worker returns. Close admission now; joining and output
                // retirement still precede replacement on the next step.
                if matches!(
                    o,
                    eoe_runtime::Output::Service(coordinator::ServiceOutput::Stopped)
                ) {
                    owner.runtime_client().request_stop();
                }
                let fatal = matches!(o, eoe_runtime::Output::WorkStopped(_));
                self.pending = Some(Output::Supervision(o.clone()));
                if fatal {
                    return Err(Error::Supervision(o));
                }
                return Ok(Progress::Output);
            }
            owner::Progress::Idle => (),
            _ => advanced = true,
        }
        self.status.epoch_revision = owner.epoch_cursor().revision();
        if owner.recovering() {
            return Ok(Progress::Advanced);
        }
        match self
            .services
            .delivery
            .as_mut()
            .expect("delivery")
            .step()
            .map_err(Error::Delivery)?
        {
            delivery::Progress::Idle
            | delivery::Progress::Waiting
            | delivery::Progress::Backpressured
            | delivery::Progress::SourceBusy => (),
            _ => advanced = true,
        }
        if self.services.pool.replacement_pending() && !self.services.pool.client().info().joined {
            return Ok(Progress::Advanced);
        }
        if let Some(retiring) = &mut self.retiring {
            if retiring
                .step(owner, &self.services.importer)
                .map_err(Error::Production)?
            {
                let stopped = self
                    .retiring
                    .take()
                    .expect("retiring")
                    .finish()
                    .expect("settled");
                self.session.status.stopping = false;
                self.session.status.stops = self.session.status.stops.saturating_add(1);
                self.pending = Some(Output::Stopped(Box::new(stopped)));
                return Ok(Progress::Output);
            }
            return Ok(Progress::Advanced);
        }
        if self.session_retirement.is_some() {
            self.install_session()?;
            return Ok(Progress::Output);
        }
        let Some((wall_ms, now)) = production else {
            return Ok(if advanced {
                Progress::Advanced
            } else {
                Progress::Idle
            });
        };
        if self.production_tick.is_some_and(|last| now < last) {
            return Err(Error::Time);
        }
        self.production_tick = Some(now);
        if let Some(attempt) = &mut self.session.attempt {
            let p = match attempt.step(owner, &self.services.importer, wall_ms, now) {
                Ok(p) => p,
                Err(e) if definitely_busy(&e) => {
                    attempt.retry().map_err(Error::Production)?;
                    self.session.status.retries = self.session.status.retries.saturating_add(1);
                    return Ok(Progress::Retrying);
                }
                Err(e) if native_rejection(&e) => {
                    let parent = attempt.parent();
                    let micro = attempt.accepted_micro().cloned();
                    self.session.attempt = None;
                    self.session.restart = true;
                    self.session.status.deadline = None;
                    self.session.status.rejected = self.session.status.rejected.saturating_add(1);
                    self.pending = Some(Output::Rejected {
                        error: e,
                        parent,
                        micro,
                    });
                    return Ok(Progress::Output);
                }
                Err(e) => return Err(Error::Production(e)),
            };
            self.session.status.deadline = attempt.deadline();
            if attempt.ready() {
                let completed = self
                    .session
                    .attempt
                    .take()
                    .expect("attempt")
                    .finish()
                    .expect("ready");
                if matches!(
                    completed.outcome,
                    production::Outcome::Wait(_) | production::Outcome::RetryNotInCache
                ) {
                    self.session.restart = true;
                }
                self.session.status.completed = self.session.status.completed.saturating_add(1);
                self.session.status.deadline = None;
                self.pending = Some(Output::Attempt(completed));
                return Ok(Progress::Output);
            }
            return Ok(Progress::Production(p));
        }
        // Consume the committed backlog before starting a new attempt. Otherwise
        // an old locally-created event could incorrectly restart a later attempt.
        let caught_up = self.status.cursor.revision()
            == self.services.source.info().status.revision
            && self.status.epoch_revision >= self.status.cursor.revision();
        if self.session.status.producing
            && self.session.restart
            && caught_up
            && !self.services.pool.replacement_pending()
        {
            self.session.attempt = Some(
                owner
                    .begin_production(
                        self.services.producer.clone(),
                        self.services.pool.client(),
                        self.production.clone(),
                    )
                    .map_err(Error::Production)?,
            );
            self.session.restart = false;
            self.session.status.attempts = self.session.status.attempts.saturating_add(1);
            return Ok(Progress::Started);
        }
        Ok(if advanced {
            Progress::Advanced
        } else {
            Progress::Idle
        })
    }
    fn consume_events(&mut self) -> Result<bool, Error> {
        if self.services.tops.is_none() {
            let batch = self
                .services
                .source
                .after(&self.status.cursor, self.services.event_limit)
                .map_err(Error::Events)?;
            if batch.closed {
                return Err(Error::Closed);
            }
            if batch.records.is_empty() {
                self.status.cursor = batch.cursor;
                return Ok(false);
            }
            self.services.tops = Some(TopDelivery {
                records: batch.records.into(),
                after: batch.cursor,
                receipt: None,
                cache: None,
            });
        }
        let pending = self.services.tops.as_mut().expect("top batch");
        let record = pending.records.front().expect("top event");
        let context = record.context();
        if context.origin == publication::Origin::Rollback {
            return Err(Error::Rollback(record.change().revision));
        }
        let changed = record.change().old_tip != Some(record.change().new_tip);
        let key = context.block.filter(|b| b.kind == publication::Kind::Key);
        if changed
            && let Some(block) = key
            && (pending.receipt.is_some()
                || record.change().revision > self.services.consensus.cache_revision())
        {
            if let Some(call) = &mut pending.receipt {
                match call.try_reply() {
                    Ok(None) => return Ok(false),
                    Ok(Some(Ok(parent_cache::Reply::None))) => {
                        pending.receipt = None;
                        pending.cache = None;
                    }
                    other => return Err(Error::Parent(format!("child top receipt: {other:?}"))),
                }
            } else {
                let cache = self.services.consensus.parent();
                match cache.call(parent_cache::Event::ChildTop(parent_cache::ChildTop {
                    hash: block.hash,
                    height: block.height.into(),
                })) {
                    Ok(call) => {
                        pending.receipt = Some(call);
                        pending.cache = Some(cache);
                        return Ok(true);
                    }
                    Err(cache::Error::Full) => return Ok(false),
                    Err(e) => return Err(Error::Parent(format!("child top submission: {e:?}"))),
                }
            }
        }
        if changed {
            self.status.top_changes = self.status.top_changes.saturating_add(1);
        }
        if restart_on_top(context, changed) {
            self.status.selected_created_keys = self.status.selected_created_keys.saturating_add(1);
            self.session.restart = true;
        }
        pending.records.pop_front();
        if pending.records.is_empty() {
            self.status.cursor = self.services.tops.take().expect("top batch").after;
        }
        Ok(true)
    }
    pub fn shutdown(mut self) -> Shutdown<O> {
        self.finish()
    }
    fn finish(&mut self) -> Shutdown<O> {
        self.session.status.producing = false;
        if let Some(attempt) = &mut self.session.attempt {
            attempt.cancel();
        }
        if let Some(agent) = &self.services.pinning {
            agent.handle().request_stop();
        }
        self.services.pool.close();
        self.services.importer.stop();
        if let Some(pins) = &self.services.pins {
            pins.stop();
        }
        if let Some(attempt) = &mut self.session.attempt {
            attempt.join_candidate();
        }
        if let Some(retiring) = &mut self.retiring {
            retiring.join_candidate();
        }
        self.services.importer.jobs.join_blocking();
        let admission = self
            .session
            .attempt
            .as_mut()
            .and_then(production::Attempt::take_abandoned_admission)
            .or_else(|| {
                self.retiring
                    .as_mut()
                    .and_then(production::Retiring::take_abandoned_admission)
            })
            .map(|mut ticket| {
                ticket.try_result().unwrap_or(Err(chain::Failure {
                    progress: None,
                    error: chain::Error::Closed,
                }))
            });
        // The importer has joined: take any accepted effects still in its host.
        // Delivery itself retains its previously acquired batch and receipt.
        let mut delivery = self.services.delivery.take().expect("delivery");
        let accepted = delivery.take_source_after_join();
        let mut pinning = self.services.pinning.take().map(pinning::Service::shutdown);
        self.status.votes = self.services.owner.as_ref().expect("owner").vote_status();
        let owner = self.services.owner.take().expect("owner").shutdown();
        let pool = self.services.pool.shutdown();
        let pool_notices = self.services.pool.take_notices();
        if let Some(Ok(exit)) = &mut pinning {
            exit.poll_children();
        }
        if let Some(output) = &mut self.pending {
            output.poll_pinning();
        }
        let consensus = self.services.consensus.finish();
        self.status.consensus = consensus.status.clone();
        let parent = self.services.parent.take().map(|cache| {
            cache
                .shutdown()
                .map_err(|_| "parent cache panicked".to_string())
        });
        let connector_joined = self
            .services
            .connector
            .take()
            .is_none_or(|c| c.shutdown().is_ok());
        let pins_joined = self
            .services
            .pins
            .take()
            .is_none_or(|pins| pins.shutdown().is_ok());
        let resilience = self
            .services
            .resilience_worker
            .take()
            .map(resilience::Worker::shutdown);
        self.services.resilience.take();
        Shutdown {
            status: self.status(),
            output: self.pending.take(),
            attempt: self.session.attempt.take(),
            retiring: self.retiring.take(),
            session_retirement: self.session_retirement.take(),
            production_driver: None,
            admission,
            owner,
            delivery,
            accepted,
            queued_outputs: vec![],
            tops: self.services.tops.take(),
            pool,
            pool_notices,
            parent,
            connector_joined,
            pins_joined,
            pinning,
            consensus,
            resilience,
        }
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Drop for Conductor<O> {
    fn drop(&mut self) {
        if self.services.owner.is_some() {
            let _ = self.finish();
        }
    }
}

fn definitely_busy(error: &production::Error) -> bool {
    fn busy(e: &chain::Error) -> bool {
        matches!(
            e,
            chain::Error::Busy | chain::Error::Storage(repo::Error::Conflict)
        )
    }
    match error {
        production::Error::Source(e) => busy(e),
        production::Error::Admission(f) => {
            f.progress.as_ref().is_some_and(|r| r.accepted.is_empty()) && busy(&f.error)
        }
        _ => false,
    }
}
fn native_rejection(error: &production::Error) -> bool {
    match error {
        production::Error::Decision(f) => !f.operational,
        production::Error::Key { operational, .. } => !operational,
        production::Error::Admission(f) => {
            f.progress.as_ref().is_some_and(|r| r.accepted.is_empty())
                && matches!(
                    f.error,
                    chain::Error::Validation {
                        operational: false,
                        ..
                    }
                )
        }
        _ => false,
    }
}

fn restart_on_top(context: publication::Context, changed: bool) -> bool {
    changed
        && context.origin == publication::Origin::Created
        && context
            .block
            .is_some_and(|b| b.kind == publication::Kind::Key)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_conductor_top_and_candidate_failure_restart_decisions_match() {
        let data: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/erlang_hc_conductor.json"
        ))
        .unwrap();
        for row in data["cases"].as_array().unwrap() {
            let context = publication::Context {
                origin: match row["origin"].as_str().unwrap() {
                    "created" => publication::Origin::Created,
                    "received" => publication::Origin::Received,
                    "synced" => publication::Origin::Synced,
                    _ => panic!(),
                },
                block: Some(publication::BlockInfo {
                    hash: ae_types::Hash256::ZERO,
                    previous: ae_types::Hash256::ZERO,
                    height: 2,
                    kind: if row["block_type"] == "key" {
                        publication::Kind::Key
                    } else {
                        publication::Kind::Micro
                    },
                }),
            };
            let restart = row["running"].as_bool().unwrap()
                && restart_on_top(context, row["changed"].as_bool().unwrap());
            assert_eq!(
                u64::from(restart),
                row["result"]["dispatches"].as_u64().unwrap()
            );
        }
        for row in data["replies"].as_array().unwrap() {
            let failure = production::Error::Decision(repo::hyperchain::producer::Failure {
                kind: repo::hyperchain::producer::Kind::Chain,
                operational: false,
                reason: row["reason"].as_str().unwrap().into(),
            });
            let retry = row["running"].as_bool().unwrap() && native_rejection(&failure);
            assert_eq!(
                u64::from(retry),
                row["result"]["dispatches"].as_u64().unwrap()
            );
        }
        assert_eq!(
            (
                data["cases"].as_array().unwrap().len(),
                data["replies"].as_array().unwrap().len()
            ),
            (24, 8)
        );
    }
}
