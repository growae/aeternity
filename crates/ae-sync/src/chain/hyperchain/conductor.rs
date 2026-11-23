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

pub mod runtime;
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
    ConsensusRestartLimit,
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
#[derive(Debug)]
pub enum Output {
    Pinning(Box<pinning::Report>),
    ConsensusSupervision(supervision::Notice),
    VoteSupervision(owner::vote_pool::Notice),
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
    pub consensus: supervision::Status,
    pub votes: owner::vote_pool::Status,
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
// A shutdown report preserves transient work instead of replaying it into
// replacement children. Receipt results are observed only after importer join.
pub struct Shutdown<O> {
    pub status: Status,
    pub output: Option<Output>,
    pub attempt: Option<production::Attempt<O>>,
    pub retiring: Option<production::Retiring<O>>,
    pub admission: Option<Result<chain::Report, chain::Failure>>,
    pub owner: owner::Shutdown,
    pub delivery: delivery::Delivery<O, eoe::StateReader, eoe::StateReader>,
    pub accepted: Result<repo::hyperchain::inputs::Effects, chain::Error>,
    pub queued_outputs: Vec<Output>,
    pub tops: Option<TopDelivery>,
    pub pool: Result<pool::worker::Info, pool::worker::Error>,
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
    owner: Option<owner::Owner>,
    importer: Arc<chain::Importer<O>>,
    tops: Option<TopDelivery>,
    producer: scoped::Producer<O>,
    delivery: Option<delivery::Delivery<O, eoe::StateReader, eoe::StateReader>>,
    pool: Option<pool::worker::Worker>,
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
    production: production::Config,
    event_limit: usize,
    attempt: Option<production::Attempt<O>>,
    retiring: Option<production::Retiring<O>>,
    restart: bool,
    pending: Option<Output>,
    status: Status,
    tick: Option<Instant>,
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
            .map(|pins| {
                let journal = crate::parent_pins::posting::Journal {
                    database: database.clone(),
                    network: pins.network.clone(),
                    limits: pins.journal,
                };
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
        let pool = pool::worker::Worker::spawn_with_cache(
            database.clone(),
            execution.profile.network.clone(),
            config.pool,
            config.pool_worker,
            config.gossip.clone(),
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
        )
        .map_err(Error::Owner)?;
        let mut delivery = owner.parent_delivery(accepted).map_err(Error::Owner)?;
        delivery.set_route(route);
        let notices = agent.as_ref().map_or(1, |c| c.outputs);
        let (pinning, factory) = agent
            .map(|config| {
                pinning::Factory::start(
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
        Ok(Self {
            owner: Some(owner),
            importer: Arc::new(importer),
            tops: None,
            producer,
            delivery: Some(delivery),
            pool: Some(pool),
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
            production: config.production,
            event_limit: config.events_per_step,
            attempt: None,
            retiring: None,
            restart: config.producing,
            pending: None,
            tick: None,
            status: Status {
                consensus: consensus_status,
                votes: votes_status,
                producing: config.producing,
                stopping: false,
                stops: 0,
                block_producer: config.block_producer,
                attempts: 0,
                completed: 0,
                rejected: 0,
                retries: 0,
                selected_created_keys: 0,
                top_changes: 0,
                epoch_revision: cursor.revision(),
                cursor,
                deadline: None,
                failure: None,
            },
        })
    }
    pub fn status(&self) -> Status {
        let mut status = self.status.clone();
        if let Some(owner) = &self.owner {
            status.votes = owner.vote_status();
        }
        status
    }
    pub fn importer(&self) -> Arc<chain::Importer<O>> {
        self.importer.clone()
    }
    pub fn pool(&self) -> pool::worker::Client {
        self.pool.as_ref().expect("pool").client()
    }
    pub fn votes(&self) -> votes::Client {
        self.owner.as_ref().expect("owner").votes()
    }
    pub fn vote_binding(&self) -> votes::Binding {
        self.owner.as_ref().expect("owner").vote_binding()
    }
    pub fn request_vote_restart(
        &self,
        generation: u64,
    ) -> Result<(), owner::vote_pool::ControlError> {
        self.owner
            .as_ref()
            .expect("owner")
            .request_vote_restart(generation)
    }
    pub(crate) fn worker_group(&self) -> crate::runtime::restart::workers::Group {
        self.owner.as_ref().expect("owner").worker_group()
    }
    pub fn eoe(&self) -> eoe::Client {
        self.owner.as_ref().expect("owner").runtime_client()
    }
    pub(crate) fn optional_resilience(
        &mut self,
    ) -> Option<(
        resilience::Source,
        resilience::Config,
        Option<resilience::Worker>,
    )> {
        self.resilience.as_ref().map(|owner| {
            (
                owner.source(),
                self.resilience_config.expect("configured resilience"),
                self.resilience_worker.take(),
            )
        })
    }
    pub fn parent(&self) -> cache::Client {
        self.consensus.parent()
    }
    pub fn connector(&self) -> connector::Client {
        self.consensus.connector()
    }
    pub fn parent_pins(&self) -> Option<crate::parent_pins::Client> {
        self.pins.as_ref().map(crate::parent_pins::Service::client)
    }
    pub fn parent_utilities(&self) -> Option<parent::Client<O>> {
        self.parent_utilities.clone()
    }
    pub fn pinning(&self) -> Option<pinning::Handle> {
        self.pinning.as_ref().map(pinning::Service::handle)
    }
    pub fn attempt(&self) -> Option<&production::Attempt<O>> {
        self.attempt.as_ref()
    }
    pub fn output(&self) -> Option<&Output> {
        self.pending.as_ref()
    }
    pub fn take_output(&mut self) -> Option<Output> {
        self.pending.take()
    }
    pub fn start_production(&mut self) -> Result<(), Error> {
        if !self.status.block_producer {
            return Err(Error::NotBlockProducer);
        }
        if let Some(e) = &self.status.failure {
            return Err(e.clone());
        }
        if !self.status.producing {
            self.status.producing = true;
            self.restart = true;
        }
        Ok(())
    }
    // Cancellation prevents new producer work immediately. Previously accepted
    // calls are retired by step before another attempt can acquire the lease.
    // Independent admission, pool, votes and parent services remain available.
    pub fn stop_production(&mut self) -> Result<(), Error> {
        self.status.producing = false;
        self.restart = false;
        self.status.deadline = None;
        if let Some(attempt) = self.attempt.take() {
            match self
                .owner
                .as_mut()
                .expect("owner")
                .retire_production(attempt)
            {
                Ok(retiring) => {
                    self.retiring = Some(retiring);
                    self.status.stopping = true;
                }
                Err(attempt) => {
                    self.attempt = Some(*attempt);
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
        let result = self.advance(wall_ms, now);
        if let Err(e) = &result {
            if self
                .owner
                .as_ref()
                .is_some_and(owner::Owner::replacement_pending)
                && eoe_departure(e)
            {
                return Ok(Progress::Advanced);
            }
            self.status.failure = Some(e.clone());
            self.owner.as_ref().expect("owner").close_vote_binding();
            if matches!(e, Error::Owner(owner::Error::VoteRestartLimit)) {
                self.owner.as_ref().expect("owner").stop_votes();
                self.pool.as_ref().expect("pool").request_shutdown();
                if let Some(worker) = &self.resilience_worker {
                    worker.request_shutdown();
                }
            }
        }
        result
    }
    fn advance(&mut self, wall_ms: u64, now: Instant) -> Result<Progress, Error> {
        if self.tick.is_some_and(|last| now < last) {
            return Err(Error::Time);
        }
        self.tick = Some(now);
        self.producer.check().map_err(Error::Importer)?;
        if !self.pool().info().accepting {
            return Err(Error::Closed);
        }
        self.owner
            .as_mut()
            .expect("owner")
            .poll_votes()
            .map_err(Error::Owner)?;
        let health = self.parent().snapshot().health;
        if matches!(health, cache::Health::Failed(_))
            && self
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
        if (self
            .parent
            .as_ref()
            .is_some_and(cache::Runtime::replacement_pending)
            || self
                .owner
                .as_ref()
                .is_some_and(owner::Owner::replacement_pending))
            && let Some(attempt) = self.attempt.take()
        {
            match self
                .owner
                .as_mut()
                .expect("owner")
                .retire_production(attempt)
            {
                Ok(retiring) => {
                    self.retiring = Some(retiring);
                    self.status.stopping = true;
                    self.restart = self.status.producing;
                    self.status.deadline = None;
                }
                Err(attempt) => {
                    self.attempt = Some(*attempt);
                    return Err(Error::Production(production::Error::Binding));
                }
            }
        }
        let old_cache_generation = self.status.consensus.cache_generation;
        let delivery = self.delivery.as_mut().expect("delivery");
        let tops = &mut self.tops;
        let retiring = &mut self.retiring;
        let supervised = self
            .consensus
            .poll_cache(&mut self.parent, now, |old, exit| {
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
                self.consensus
                    .poll(&mut self.pinning, &mut self.connector, now)
            })
            .and_then(|_| {
                self.consensus
                    .poll_eoe(self.owner.as_mut().expect("owner"), now, |old, exit| {
                        exit.delivery = delivery.retire_eoe(old).map_err(Error::Delivery)?;
                        if let Some(retiring) = retiring {
                            retiring.retire_eoe(old).map_err(Error::Production)?;
                        }
                        Ok(())
                    })
            });
        self.status.consensus = self.consensus.status();
        supervised?;
        if self.status.consensus.cache_generation != old_cache_generation {
            self.owner
                .as_mut()
                .expect("owner")
                .replace_parent(self.consensus.parent());
        }
        // Retained outputs apply backpressure to production, not ownership:
        // shutdown still joins every child and transfers this output intact.
        if self.pending.is_some() {
            return Ok(Progress::Output);
        }
        if let Some(notice) = self.owner.as_mut().expect("owner").take_vote_notice() {
            self.pending = Some(Output::VoteSupervision(notice));
            return Ok(Progress::Output);
        }
        if let Some(notice) = self.consensus.take_notice() {
            self.status.consensus = self.consensus.status();
            self.pending = Some(Output::ConsensusSupervision(notice));
            return Ok(Progress::Output);
        }
        if self.status.consensus.cache_restarting {
            return Ok(Progress::Advanced);
        }
        if self.status.consensus.eoe_restarting && !self.owner.as_ref().expect("owner").recovering()
        {
            return Ok(Progress::Advanced);
        }
        if let Some(agent) = &self.pinning {
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
        let owner = self.owner.as_mut().expect("owner");
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
        if let Some(retiring) = &mut self.retiring {
            if retiring
                .step(owner, &self.importer)
                .map_err(Error::Production)?
            {
                let stopped = self
                    .retiring
                    .take()
                    .expect("retiring")
                    .finish()
                    .expect("settled");
                self.status.stopping = false;
                self.status.stops = self.status.stops.saturating_add(1);
                self.pending = Some(Output::Stopped(Box::new(stopped)));
                return Ok(Progress::Output);
            }
            return Ok(Progress::Advanced);
        }
        if let Some(attempt) = &mut self.attempt {
            let p = match attempt.step(owner, &self.importer, wall_ms, now) {
                Ok(p) => p,
                Err(e) if definitely_busy(&e) => {
                    attempt.retry().map_err(Error::Production)?;
                    self.status.retries = self.status.retries.saturating_add(1);
                    return Ok(Progress::Retrying);
                }
                Err(e) if native_rejection(&e) => {
                    let parent = attempt.parent();
                    let micro = attempt.accepted_micro().cloned();
                    self.attempt = None;
                    self.restart = true;
                    self.status.deadline = None;
                    self.status.rejected = self.status.rejected.saturating_add(1);
                    self.pending = Some(Output::Rejected {
                        error: e,
                        parent,
                        micro,
                    });
                    return Ok(Progress::Output);
                }
                Err(e) => return Err(Error::Production(e)),
            };
            self.status.deadline = attempt.deadline();
            if attempt.ready() {
                let completed = self
                    .attempt
                    .take()
                    .expect("attempt")
                    .finish()
                    .expect("ready");
                if matches!(
                    completed.outcome,
                    production::Outcome::Wait(_) | production::Outcome::RetryNotInCache
                ) {
                    self.restart = true;
                }
                self.status.completed = self.status.completed.saturating_add(1);
                self.status.deadline = None;
                self.pending = Some(Output::Attempt(completed));
                return Ok(Progress::Output);
            }
            return Ok(Progress::Production(p));
        }
        // Consume the committed backlog before starting a new attempt. Otherwise
        // an old locally-created event could incorrectly restart a later attempt.
        let caught_up = self.status.cursor.revision() == self.source.info().status.revision
            && self.status.epoch_revision >= self.status.cursor.revision();
        if self.status.producing && self.restart && caught_up {
            self.attempt = Some(
                owner
                    .begin_production(
                        self.producer.clone(),
                        self.pool.as_ref().expect("pool").client(),
                        self.production.clone(),
                    )
                    .map_err(Error::Production)?,
            );
            self.restart = false;
            self.status.attempts = self.status.attempts.saturating_add(1);
            return Ok(Progress::Started);
        }
        Ok(if advanced {
            Progress::Advanced
        } else {
            Progress::Idle
        })
    }
    fn consume_events(&mut self) -> Result<bool, Error> {
        if self.tops.is_none() {
            let batch = self
                .source
                .after(&self.status.cursor, self.event_limit)
                .map_err(Error::Events)?;
            if batch.closed {
                return Err(Error::Closed);
            }
            if batch.records.is_empty() {
                self.status.cursor = batch.cursor;
                return Ok(false);
            }
            self.tops = Some(TopDelivery {
                records: batch.records.into(),
                after: batch.cursor,
                receipt: None,
                cache: None,
            });
        }
        let pending = self.tops.as_mut().expect("top batch");
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
                || record.change().revision > self.consensus.cache_revision())
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
                let cache = self.consensus.parent();
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
            self.restart = true;
        }
        pending.records.pop_front();
        if pending.records.is_empty() {
            self.status.cursor = self.tops.take().expect("top batch").after;
        }
        Ok(true)
    }
    pub fn shutdown(mut self) -> Shutdown<O> {
        self.finish()
    }
    fn finish(&mut self) -> Shutdown<O> {
        self.status.producing = false;
        if let Some(attempt) = &mut self.attempt {
            attempt.cancel();
        }
        if let Some(agent) = &self.pinning {
            agent.handle().request_stop();
        }
        self.importer.stop();
        if let Some(pins) = &self.pins {
            pins.stop();
        }
        if let Some(attempt) = &mut self.attempt {
            attempt.join_candidate();
        }
        if let Some(retiring) = &mut self.retiring {
            retiring.join_candidate();
        }
        self.importer.jobs.join_blocking();
        let admission = self
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
        let mut delivery = self.delivery.take().expect("delivery");
        let accepted = delivery.take_source_after_join();
        let mut pinning = self.pinning.take().map(pinning::Service::shutdown);
        self.status.votes = self.owner.as_ref().expect("owner").vote_status();
        let owner = self.owner.take().expect("owner").shutdown();
        let pool = self.pool.take().expect("pool").shutdown();
        if let Some(Ok(exit)) = &mut pinning {
            exit.poll_children();
        }
        if let Some(output) = &mut self.pending {
            output.poll_pinning();
        }
        let consensus = self.consensus.finish();
        self.status.consensus = consensus.status.clone();
        let parent = self.parent.take().map(|cache| {
            cache
                .shutdown()
                .map_err(|_| "parent cache panicked".to_string())
        });
        let connector_joined = self.connector.take().is_none_or(|c| c.shutdown().is_ok());
        let pins_joined = self.pins.take().is_none_or(|pins| pins.shutdown().is_ok());
        let resilience = self
            .resilience_worker
            .take()
            .map(resilience::Worker::shutdown);
        self.resilience.take();
        Shutdown {
            status: self.status.clone(),
            output: self.pending.take(),
            attempt: self.attempt.take(),
            retiring: self.retiring.take(),
            admission,
            owner,
            delivery,
            accepted,
            queued_outputs: vec![],
            tops: self.tops.take(),
            pool,
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
        if self.owner.is_some() {
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
