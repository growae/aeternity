// Committed native epoch notifications and current selected-state access are
// separate inputs: a side-branch new_epoch need not change the selected tip.
use ae_chain::{publication, repository as repo};
use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::eoe::{coordinator, local_host, pool, runtime};
use ae_protocol::{ConsensusType, NetworkProfile, Protocol};
use ae_storage::{Database, events, snapshots};
use ae_types::{AccountPublicKey, Unsigned};
use ae_vm_fate::maps::MapOrder;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, Weak},
};

pub mod owner;
pub mod service;
pub type StateReader = ae_storage::hyperchain::candidate::Store;
pub type Runtime = runtime::Runtime<StateReader, StateReader>;
pub type Client = runtime::Client<StateReader, StateReader>;
pub type Tip = local_host::SelectedTip<StateReader, StateReader>;

pub struct Source {
    database: Weak<Database>,
    snapshots: snapshots::Owner,
    events: events::Reader,
    identity: repo::Identity,
    network: NetworkProfile,
    genesis_protocol: Protocol,
    live: Mutex<Weak<()>>,
    epochs_live: Mutex<Weak<()>>,
}
pub struct State {
    pub tip: Tip,
    pub metadata: snapshots::Metadata,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Identity,
    AlreadyRunning,
    State(local_host::SelectedError),
    StateHost(local_host::Error),
    Worker(String),
    Source(events::Error),
    SourceClosed,
    Rollback { revision: u64 },
    EpochRange,
    ResourceLimit,
    Closed,
    ReceiptLost,
    UnexpectedReply,
    Pool(pool::Error),
    Rejected(coordinator::Error),
}
fn state_error(e: snapshots::Error) -> local_host::SelectedError {
    match e {
        snapshots::Error::ResourceLimit
        | snapshots::Error::Repository(repo::Error::ResourceLimit) => {
            local_host::SelectedError::ResourceLimit
        }
        e => local_host::SelectedError::Backend(format!("{e:?}")),
    }
}
impl Source {
    pub(crate) fn uses_database(&self, database: &Arc<Database>) -> bool {
        self.database.ptr_eq(&Arc::downgrade(database))
    }
    pub(crate) fn matches(&self, identity: &repo::Identity, network: &NetworkProfile) -> bool {
        &self.identity == identity && &self.network == network
    }
    pub fn new(
        database: Arc<Database>,
        identity: repo::Identity,
        network: NetworkProfile,
        limits: snapshots::Config,
    ) -> Result<Arc<Self>, Error> {
        if identity.consensus != ConsensusType::Pos
            || !identity.matches(&network)
            || database
                .view()
                .map_err(|e| Error::State(state_error(snapshots::Error::Repository(e))))?
                .identity()
                != &identity
        {
            return Err(Error::Identity);
        }
        let events = database.events();
        let weak_database = Arc::downgrade(&database);
        let snapshots =
            snapshots::Owner::start(database, limits).map_err(|e| Error::State(state_error(e)))?;
        let genesis_protocol = network.protocol_at(0).map_err(|_| Error::Identity)?;
        Ok(Arc::new(Self {
            database: weak_database,
            snapshots,
            events,
            identity,
            network,
            genesis_protocol,
            live: Mutex::new(Weak::new()),
            epochs_live: Mutex::new(Weak::new()),
        }))
    }
    pub fn snapshots(&self) -> &snapshots::Owner {
        &self.snapshots
    }
    pub fn capture(&self) -> Result<State, local_host::SelectedError> {
        let expected = self.identity.clone();
        let genesis = self.genesis_protocol;
        let captured = self
            .snapshots
            .capture(move |view| {
                if view.identity() != &expected {
                    return Err(repo::Error::Identity);
                }
                let Some(hash) = view.status().tip else {
                    return Ok((None, genesis));
                };
                let block = view.require_block(hash)?;
                let header = block.header();
                view.require_retained(header.common().height)?;
                let roots = view.require_record(hash)?.roots;
                if roots.state.hash().0 != header.common().state_hash {
                    return Err(repo::Error::Corrupt("EOE selected roots"));
                }
                let protocol = Protocol::try_from(header.common().version)
                    .map_err(|_| repo::Error::Corrupt("EOE selected protocol"))?;
                Ok((Some(roots), protocol))
            })
            .map_err(state_error)?;
        let trees = captured
            .value
            .0
            .as_ref()
            .map(|roots| {
                roots
                    .load(StateReader::new(
                        captured.reader().clone(),
                        Default::default(),
                    ))
                    .map(Arc::new)
            })
            .transpose()
            .map_err(|e| state_error(snapshots::Error::Repository(e)))?;
        Ok(State {
            metadata: captured.metadata().clone(),
            tip: Tip {
                trees,
                protocol: captured.value.1,
                consensus: ConsensusType::Pos,
            },
        })
    }
    // Create one EOE authority and its fixed epoch-feed consumer together. The
    // caller transfers Runtime to the vote/output owner, then runs Epochs.
    // This compatibility entry point keeps its original runtime-owned pool.
    pub fn connect(
        self: &Arc<Self>,
        config: Config,
        keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
        order: impl MapOrder + Send + 'static,
    ) -> Result<Connected, Error> {
        self.connect_with(config, keys, order, Services::default())
            .map(|(connected, _)| connected)
    }
    pub fn connect_independent(
        self: &Arc<Self>,
        config: Config,
        keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
        order: impl MapOrder + Send + 'static,
        hub: pool::Hub,
    ) -> Result<Connected, Error> {
        self.connect_with(
            config,
            keys,
            order,
            Services {
                pool: Some(hub),
                ..Default::default()
            },
        )
        .map(|(connected, _)| connected)
    }
    fn connect_with(
        self: &Arc<Self>,
        config: Config,
        keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
        order: impl MapOrder + Send + 'static,
        services: Services,
    ) -> Result<(Connected, ae_hyperchain::signing::Keys), Error> {
        config.epochs.validate()?;
        let Services {
            gossip,
            parent,
            route,
            pool,
        } = services;
        if config.host.network != self.network {
            return Err(Error::Identity);
        }
        // Both entry points reserve runtime ownership under the epoch gate.
        // A legacy connection must not create a second feed during an
        // independent runtime gap, when only the epoch lease remains alive.
        let (epoch_lease, lease) = {
            let mut epochs_live = self
                .epochs_live
                .lock()
                .map_err(|_| Error::Worker("poisoned epoch owner".into()))?;
            if epochs_live.upgrade().is_some() {
                return Err(Error::AlreadyRunning);
            }
            let mut live = self
                .live
                .lock()
                .map_err(|_| Error::Worker("poisoned EOE source owner".into()))?;
            if live.upgrade().is_some() {
                return Err(Error::AlreadyRunning);
            }
            let lease = Arc::new(());
            *live = Arc::downgrade(&lease);
            let epoch_lease = pool.as_ref().map(|_| {
                let lease = Arc::new(());
                *epochs_live = Arc::downgrade(&lease);
                lease
            });
            (epoch_lease, lease)
        };
        let initial = self.capture().map_err(Error::State)?;
        let cursor = initial.metadata.cursor.clone();
        let protocol = initial.tip.protocol;
        // Do not pin the startup tip for the whole runtime. Native admission
        // and pending finalization read the then-current chain, independently
        // of notification backlog. A newly started pool has epoch zero.
        drop(initial);
        let mut host = local_host::LocalHost::new(
            config.host,
            Tip {
                trees: None,
                protocol,
                consensus: ConsensusType::Pos,
            },
            keys,
        )
        .with_selected_source(Arc::new(Live {
            source: self.clone(),
            _lease: lease,
        }));
        if let Some(hub) = &pool {
            host = host
                .with_independent_pool(hub.clone())
                .map_err(Error::StateHost)?;
        }
        host.set_parent_cache(parent);
        if let Some(route) = route {
            host.set_parent_route(route);
        }
        let signer = host.signer();
        let mut coordinator =
            coordinator::Coordinator::new(host, config.block_time, config.coordinator);
        coordinator.set_live_gossip_cache(gossip);
        let runtime = Runtime::spawn(coordinator, order, config.runtime)
            .map_err(|e| Error::Worker(e.to_string()))?;
        let epochs = Epochs {
            source: self.clone(),
            cursor,
            target: match pool {
                Some(hub) => Target::Independent {
                    hub,
                    _lease: epoch_lease.expect("epoch lease"),
                },
                None => Target::Legacy(runtime.client()),
            },
            config: config.epochs,
            pending: None,
            receipt: None,
            failure: None,
            acknowledged: 0,
        };
        Ok((Connected { runtime, epochs }, signer))
    }
}
#[derive(Default)]
pub(super) struct Services {
    pub gossip: Option<Arc<dyn ae_mempool::gossip::Transactional>>,
    pub parent: Option<ae_hyperchain::parent_cache::runtime::Client>,
    pub route: Option<ae_hyperchain::parent_cache::runtime::Route>,
    pub pool: Option<pool::Hub>,
}
struct Live {
    source: Arc<Source>,
    _lease: Arc<()>,
}
impl local_host::SelectedSource<StateReader, StateReader> for Live {
    fn capture(&self) -> Result<Tip, local_host::SelectedError> {
        self.source.capture().map(|s| s.tip)
    }
}
pub struct Config {
    pub host: local_host::Config,
    pub block_time: Unsigned,
    pub coordinator: coordinator::Config,
    pub runtime: runtime::Config,
    pub epochs: Limits,
}
pub struct Connected {
    pub runtime: Runtime,
    pub epochs: Epochs,
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub maximum_epochs: usize,
    pub maximum_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            maximum_epochs: 1024,
            maximum_bytes: 16 << 20,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<(), Error> {
        if !(1..=65536).contains(&self.maximum_epochs)
            || !(1..=1 << 30).contains(&self.maximum_bytes)
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
struct Pending {
    _payload: Arc<publication::Payload>,
    cursor: events::Cursor,
    epochs: VecDeque<Unsigned>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    Idle,
    Loaded,
    Submitted,
    Waiting,
    Backpressured,
    Acknowledged,
}
enum Target {
    Legacy(Client),
    Independent { hub: pool::Hub, _lease: Arc<()> },
}
impl Target {
    fn is_closed(&self) -> bool {
        match self {
            Self::Legacy(client) => client.is_closed(),
            Self::Independent { hub, .. } => hub.is_closed(),
        }
    }
    fn stop(&self) {
        match self {
            Self::Legacy(client) => client.request_stop(),
            Self::Independent { hub, .. } => hub.close(),
        }
    }
}
pub struct Epochs {
    source: Arc<Source>,
    cursor: events::Cursor,
    target: Target,
    config: Limits,
    pending: Option<Pending>,
    receipt: Option<runtime::Call>,
    failure: Option<Error>,
    acknowledged: u64,
}
impl Drop for Epochs {
    fn drop(&mut self) {
        self.target.stop();
    }
}
impl Epochs {
    pub fn cursor(&self) -> &events::Cursor {
        &self.cursor
    }
    pub fn acknowledged(&self) -> u64 {
        self.acknowledged
    }
    pub fn failure(&self) -> Option<&Error> {
        self.failure.as_ref()
    }
    pub fn in_flight(&self) -> bool {
        self.receipt.is_some()
    }
    pub fn retained_bytes(&self) -> usize {
        self.pending
            .as_ref()
            .map_or(0, |p| p._payload.retained_bytes())
    }
    pub fn retry(&mut self, config: Limits) -> Result<(), Error> {
        config.validate()?;
        if let Some(e) = &self.failure
            && !matches!(e, Error::ResourceLimit | Error::Rejected(_))
        {
            return Err(e.clone());
        }
        if self.pending.as_ref().is_some_and(|p| {
            p._payload.retained_bytes() > config.maximum_bytes
                || p.epochs.len() > config.maximum_epochs
        }) {
            return Err(Error::ResourceLimit);
        }
        self.config = config;
        self.failure = None;
        Ok(())
    }
    // Nonblocking downstream submission/receipt polling. Only definitely
    // rejected commands may retry. Gaps, rollback, failed/reopened sources and
    // unknown receipts close the affected authority. Independent epoch feeds
    // survive EOE absence with the same cursor; a terminal source failure
    // closes the Hub instead of skipping retained events.
    pub fn step(&mut self) -> Result<Progress, Error> {
        if let Some(e) = &self.failure {
            return Err(e.clone());
        }
        let result = self.advance();
        if let Err(e) = &result {
            if !matches!(e, Error::ResourceLimit | Error::Rejected(_)) {
                self.target.stop();
            }
            self.failure = Some(e.clone());
        }
        result
    }
    fn acknowledge(&mut self) -> Progress {
        let pending = self.pending.as_mut().expect("acknowledged epoch event");
        pending.epochs.pop_front().expect("submitted epoch");
        self.acknowledged = self.acknowledged.saturating_add(1);
        if pending.epochs.is_empty() {
            self.cursor = pending.cursor.clone();
            self.pending = None;
        }
        Progress::Acknowledged
    }
    fn advance(&mut self) -> Result<Progress, Error> {
        let info = self.source.events.info();
        if let Some(e) = info.failure {
            return Err(Error::Source(events::Error::Failed(e)));
        }
        if info.closed {
            return Err(Error::SourceClosed);
        }
        if let Some(call) = &mut self.receipt {
            let Some(reply) = call.try_reply().map_err(|_| Error::ReceiptLost)? else {
                if self.target.is_closed() {
                    return Err(Error::ReceiptLost);
                }
                return Ok(Progress::Waiting);
            };
            self.receipt = None;
            match reply {
                Ok(runtime::Applied::Unit) => (),
                Ok(_) => return Err(Error::UnexpectedReply),
                Err(coordinator::Error::Stopped) => return Err(Error::Closed),
                Err(e) => return Err(Error::Rejected(e)),
            }
            return Ok(self.acknowledge());
        }
        if self.target.is_closed() {
            return Err(Error::Closed);
        }
        if self.pending.is_none() {
            let batch = self
                .source
                .events
                .after(&self.cursor, 1)
                .map_err(Error::Source)?;
            let Some(payload) = batch.records.into_iter().next() else {
                return Ok(Progress::Idle);
            };
            if payload.context().origin == publication::Origin::Rollback {
                return Err(Error::Rollback {
                    revision: payload.change().revision,
                });
            }
            if payload.retained_bytes() > self.config.maximum_bytes {
                return Err(Error::ResourceLimit);
            }
            let mut epochs = VecDeque::new();
            for effect in payload.effects() {
                if let publication::Effect::Consensus {
                    event: ae_hyperchain::block::Event::NewEpoch(epoch),
                    ..
                } = effect
                {
                    if epochs.len() >= self.config.maximum_epochs {
                        return Err(Error::ResourceLimit);
                    }
                    let ae_vm_fate::value::Value::Integer(epoch) = &epoch.epoch else {
                        return Err(Error::EpochRange);
                    };
                    if epoch.sign() == num_bigint::Sign::Minus {
                        return Err(Error::EpochRange);
                    }
                    epochs.push_back(
                        Unsigned::from_field_bytes(&epoch.to_bytes_be().1)
                            .map_err(|_| Error::EpochRange)?,
                    );
                }
            }
            if epochs.is_empty() {
                self.cursor = batch.cursor;
                return Ok(Progress::Acknowledged);
            }
            self.pending = Some(Pending {
                _payload: payload,
                cursor: batch.cursor,
                epochs,
            });
            return Ok(Progress::Loaded);
        }
        let epoch = self
            .pending
            .as_ref()
            .expect("loaded event")
            .epochs
            .front()
            .expect("nonempty event")
            .clone();
        let client = match &self.target {
            Target::Independent { hub, .. } => {
                if epoch.as_bytes().len().saturating_add(128) > hub.config().maximum_input_bytes {
                    return Err(Error::ResourceLimit);
                }
                return match hub.new_epoch(epoch) {
                    Ok(()) => Ok(self.acknowledge()),
                    Err(pool::Error::ResourceLimit) => Ok(Progress::Backpressured),
                    Err(pool::Error::Closed) => Err(Error::Closed),
                    Err(error) => Err(Error::Pool(error)),
                };
            }
            Target::Legacy(client) => client,
        };
        match client.call(runtime::Command::NewEpoch(epoch)) {
            Ok(call) => self.receipt = Some(call),
            Err(runtime::SubmitError::Full(_)) => return Ok(Progress::Backpressured),
            Err(runtime::SubmitError::Disconnected(_)) => return Err(Error::Closed),
            Err(runtime::SubmitError::ResourceLimit(_)) => return Err(Error::ResourceLimit),
        }
        Ok(Progress::Submitted)
    }
}
