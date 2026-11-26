// aec_sync generation download/insertion, aeternity fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{blocking, network};
use ae_chain::{
    block::Block,
    key::Rules,
    repository::{self, ledger},
};
use ae_execution::transactions::owned::Owned;
use ae_p2p::{
    message::{self, Details, Kind, Message, Value},
    runtime as net,
};
use ae_protocol::ConsensusType;
use ae_storage::Database;
use ae_types::Hash256;
use ae_vm_fate::maps::{MapOrder, order::Otp26};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

mod download;
pub use download::Download;
pub mod generation;
pub mod hyperchain;
mod import;
pub mod on_demand;
pub mod packing;
pub mod pending;
mod policy;
pub mod pow;
pub mod query;
mod strategy;
pub mod sync;
#[cfg(test)]
mod tests;
pub use generation::Direction;

#[derive(Clone)]
pub struct Config {
    pub jobs: usize,
    pub bytes: usize,
    pub blocks: usize,
    pub conflicts: usize,
    pub timeout: Duration,
    pub ledger: ledger::Limits,
    pub messages: message::Limits,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            jobs: 8,
            bytes: 128 << 20,
            blocks: 4096,
            conflicts: 4,
            timeout: Duration::from_secs(60),
            ledger: ledger::Limits {
                policy: ledger::Policy {
                    origin: ledger::Origin::Sync,
                    gossip_depth: 5,
                },
                ..Default::default()
            },
            messages: Default::default(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Busy,
    Closed,
    Cancelled,
    Timeout,
    Stale,
    RecoveryRequired,
    WorkerPanicked,
    Hash,
    WrongConsensus,
    BlockedByWhitelist,
    Policy {
        height: u64,
        top: u64,
    },
    Refused(Vec<u8>),
    Message(message::Error),
    Block(ae_chain::block::Error),
    Generation(generation::Error),
    Network(network::Error),
    Runtime(net::Error),
    Storage(repository::Error),
    Validation {
        operational: bool,
        reason: String,
    },
    PolicyUncertain {
        error: repository::Error,
    },
    CommitUncertain {
        hash: Hash256,
        error: repository::Error,
    },
}
impl From<repository::Error> for Error {
    fn from(e: repository::Error) -> Self {
        match e {
            repository::Error::RecoveryRequired => Self::RecoveryRequired,
            repository::Error::PolicyClosed => Self::Closed,
            other => Self::Storage(other),
        }
    }
}
impl From<message::Error> for Error {
    fn from(e: message::Error) -> Self {
        Self::Message(e)
    }
}
impl From<net::Error> for Error {
    fn from(e: net::Error) -> Self {
        Self::Runtime(e)
    }
}
impl From<network::Error> for Error {
    fn from(e: network::Error) -> Self {
        Self::Network(e)
    }
}
impl From<blocking::Error> for Error {
    fn from(e: blocking::Error) -> Self {
        match e {
            blocking::Error::Busy => Self::Busy,
            blocking::Error::Cancelled => Self::Cancelled,
            blocking::Error::Panicked => Self::WorkerPanicked,
            blocking::Error::Configuration | blocking::Error::Spawn => Self::Configuration,
            blocking::Error::Stopped => Self::Closed,
        }
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Present,
    Committed { revision: u64 },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Accepted {
    pub hash: Hash256,
    pub outcome: Outcome,
    // Count from the successfully handled input, including Present outcomes.
    // None identifies a key block; this is not a canonical transaction count.
    pub micro_transactions: Option<u64>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    // An acknowledged prefix, not the peer's height or a sync-completion claim.
    pub accepted: Vec<Accepted>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    // None means the wait ended without observing the worker's result. An
    // in-flight block can already be committed; inspect durable state.
    pub progress: Option<Report>,
    pub error: Error,
}

struct Context<O> {
    db: Arc<Database>,
    policy_scope: ae_storage::policy::Scope,
    strategy: strategy::Strategy<O>,
    config: Config,
    stopped: AtomicBool,
    shutdown: tokio::sync::watch::Sender<bool>,
    recovery: Mutex<Option<Error>>,
    height: query::Height,
    #[cfg(test)]
    commit_hook: Option<CommitHook>,
    #[cfg(test)]
    policy_hook: Option<PolicyHook>,
}
#[cfg(test)]
type CommitHook = Arc<
    dyn Fn(&Database, repository::Update) -> Result<ae_storage::Committed, repository::Error>
        + Send
        + Sync,
>;
#[cfg(test)]
type PolicyHook = Arc<
    dyn Fn(
            &Database,
            ae_chain::resilience::Event,
            ae_chain::resilience::Config,
        ) -> Result<ae_chain::resilience::Snapshot, repository::Error>
        + Send
        + Sync,
>;
pub struct Importer<O = Otp26> {
    context: Arc<Context<O>>,
    jobs: blocking::Pool,
    owns_shutdown: bool,
}
pub struct Ticket {
    pending: blocking::Pending<Result<Report, Failure>>,
    timeout: Duration,
}
impl Ticket {
    // Poll this receipt once to completion. An empty poll retains the same
    // accepted job; it is not a timeout or permission to resubmit the block.
    pub fn try_result(&mut self) -> Option<Result<Report, Failure>> {
        self.pending.try_result().map(|r| {
            r.map_err(|e| Failure {
                progress: None,
                error: e.into(),
            })
            .and_then(|r| r)
        })
    }
    pub fn cancel(&self) {
        self.pending.cancel();
    }
    pub async fn wait(self) -> Result<Report, Failure> {
        tokio::time::timeout(self.timeout, self.pending)
            .await
            .map_err(|_| Failure {
                progress: None,
                error: Error::Timeout,
            })?
            .map_err(|e| Failure {
                progress: None,
                error: e.into(),
            })?
    }
}
impl<O: MapOrder + Send + Sync + 'static> Importer<O> {
    fn pow_preparation(
        &self,
        snapshots: ae_storage::snapshots::Owner,
        limits: repository::pow::key::Limits,
    ) -> Result<pow::preparation::Builder<O>, pow::producer::mining::Error>
    where
        O: Clone,
    {
        use pow::producer::mining::Error;
        let strategy::Strategy::Pow(profile) = &self.context.strategy else {
            return Err(Error::Binding);
        };
        // This owner prepares Cuckoo CPU/Stratum work. On-demand callers use
        // the algorithm-aware candidate builder without a Cuckoo worker.
        if profile.algorithm != ae_protocol::ConsensusAlgorithm::Cuckoo {
            return Err(Error::Binding);
        }
        let identity = self
            .database()
            .view()
            .map_err(Error::Repository)?
            .identity()
            .clone();
        pow::preparation::Builder::new(
            snapshots,
            pow::preparation::Config {
                identity,
                execution: Arc::new(profile.execution.clone()),
                rules: Arc::new(profile.rules.clone()),
                limits,
            },
        )
        .map_err(|_| Error::Configuration)
    }
    // The runtime owner retains the same importer lifetime through service
    // shutdown so a late uncertain commit cannot be hidden by task teardown.
    pub(crate) fn shared(&self) -> Self {
        Self {
            context: self.context.clone(),
            jobs: self.jobs.clone(),
            owns_shutdown: self.owns_shutdown,
        }
    }
    pub(crate) fn borrowed(&self) -> Self {
        Self {
            context: self.context.clone(),
            jobs: self.jobs.clone(),
            owns_shutdown: false,
        }
    }
    pub(crate) fn database(&self) -> &Arc<Database> {
        &self.context.db
    }
    pub(crate) fn gossip_depth(&self) -> u64 {
        self.context.config.ledger.policy.gossip_depth
    }
    pub(crate) fn network(&self) -> &ae_protocol::NetworkProfile {
        self.context.strategy.network()
    }
    pub(crate) fn consensus(&self) -> ConsensusType {
        match self.context.strategy.mode() {
            ae_codec::headers::HeaderMode::ProofOfWork => ConsensusType::Pow,
            ae_codec::headers::HeaderMode::Hyperchain => ConsensusType::Pos,
        }
    }
    pub(crate) fn algorithm(&self) -> ae_protocol::ConsensusAlgorithm {
        match &self.context.strategy {
            strategy::Strategy::Pow(profile) => profile.algorithm,
            strategy::Strategy::Hyperchain(_) => ae_protocol::ConsensusAlgorithm::Hyperchain,
        }
    }
    pub(crate) fn stop_owned(&self) {
        if self.owns_shutdown {
            self.stop();
        }
    }
    pub(crate) async fn join_owned(&self) {
        if self.owns_shutdown {
            self.join().await;
        }
    }
    pub(crate) async fn settle(&self) {
        if self.owns_shutdown {
            self.join().await;
        } else {
            self.jobs.drain().await;
        }
    }
    pub fn new(
        db: Arc<Database>,
        execution: Owned<O>,
        mut rules: Rules,
        config: Config,
    ) -> Result<Self, Error> {
        if execution.consensus() != ConsensusType::Pow {
            return Err(Error::WrongConsensus);
        }
        let algorithm = {
            let view = db.view()?;
            if !view.identity().matches(execution.network())
                || view.identity().genesis != rules.genesis_hash
            {
                return Err(Error::Storage(repository::Error::Identity));
            }
            if view.identity().consensus != ConsensusType::Pow
                || view.config().mode != ae_codec::headers::HeaderMode::ProofOfWork
                || view.status().tip.is_none()
            {
                return Err(Error::Configuration);
            }
            view.identity().algorithm
        };
        // On-demand's dirty_validate_key_hash_at_height callback accepts all
        // hashes. Its generic linkage and protocol checks still run normally.
        if algorithm == ae_protocol::ConsensusAlgorithm::OnDemand {
            rules.checkpoints.clear();
        }
        Self::with_strategy(
            db,
            strategy::Strategy::Pow(Box::new(strategy::Pow {
                algorithm,
                execution,
                rules,
            })),
            config,
        )
    }
    fn validate_config(config: &Config) -> Result<(), Error> {
        if config.blocks == 0
            || config.blocks > 65_536
            || config.conflicts > 1024
            || !matches!(config.ledger.policy.origin, ledger::Origin::Sync)
            || config.timeout.is_zero()
            || tokio::time::Instant::now()
                .checked_add(config.timeout)
                .is_none()
            || config
                .messages
                .body_bytes
                .checked_add(config.messages.work_bytes)
                .is_none_or(|n| n > config.bytes)
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
    fn with_strategy(
        db: Arc<Database>,
        strategy: strategy::Strategy<O>,
        config: Config,
    ) -> Result<Self, Error> {
        Self::validate_config(&config)?;
        let policy_scope = db.policy_scope()?;
        let height = query::Height::new(&db)?;
        let jobs = blocking::Pool::named("ae-chain-import", 1, config.jobs, config.bytes)?;
        Ok(Self {
            context: Arc::new(Context {
                db,
                policy_scope,
                strategy,
                config,
                stopped: AtomicBool::new(false),
                shutdown: tokio::sync::watch::channel(false).0,
                recovery: Mutex::new(None),
                height,
                #[cfg(test)]
                commit_hook: None,
                #[cfg(test)]
                policy_hook: None,
            }),
            jobs,
            owns_shutdown: true,
        })
    }
    pub fn recovery_required(&self) -> bool {
        self.context.db.events().info().failure.is_some()
            || self.context.db.policy_recovery_required()
            || self
                .context
                .recovery
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_some()
    }
    pub fn recovery_reason(&self) -> Option<Error> {
        // The first cause survives task cancellation and shutdown. It does
        // not identify a complete committed prefix; storage must be reconciled.
        self.context
            .recovery
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .or_else(|| {
                (self.context.db.policy_recovery_required()
                    || self.context.db.events().info().failure.is_some())
                .then_some(Error::RecoveryRequired)
            })
    }
    pub fn available(&self) -> (usize, usize) {
        self.jobs.available()
    }
    pub fn stop(&self) {
        self.context.stopped.store(true, Ordering::Release);
        self.context.shutdown.send_replace(true);
        self.jobs.stop();
    }
    pub async fn join(&self) {
        self.jobs.join().await;
    }

    // Offline/retained packets use the same decoder and actual ledger as live
    // downloads. The source packet and decoded-work reservation remain charged
    // through completion, including unread replies.
    pub fn import(
        &self,
        packet: Vec<u8>,
        requested: Hash256,
        direction: Direction,
        now_ms: u64,
    ) -> Result<Ticket, Error> {
        self.check_open()?;
        if packet.len() > self.context.config.messages.body_bytes.saturating_add(2) {
            return Err(Error::Busy);
        }
        self.submit(
            self.reserve()?,
            Input::Packet(packet),
            requested,
            direction,
            now_ms,
        )
    }
    // A reconstructed or full gossip block still goes through standalone
    // validation, gossip admission policy, execution and durable publication.
    // The bytes are decoded within the same joined import worker and quota as
    // generation insertion. Successful light reconstruction alone is not admission.
    pub fn import_block(&self, bytes: Vec<u8>, now_ms: u64) -> Result<Ticket, Error> {
        self.import_block_with_origin(bytes, now_ms, ledger::Origin::Gossip)
    }
    pub fn import_synced_block(&self, bytes: Vec<u8>, now_ms: u64) -> Result<Ticket, Error> {
        self.import_block_with_origin(bytes, now_ms, ledger::Origin::Sync)
    }
    pub fn import_created_block(&self, bytes: Vec<u8>, now_ms: u64) -> Result<Ticket, Error> {
        self.import_block_with_origin(bytes, now_ms, ledger::Origin::Created)
    }
    // PoW strictly-follow-top production still uses ordinary validation and
    // execution. The selected parent is checked on every conflict retry; the
    // repository's expected revision makes that check atomic with commit.
    pub fn import_created_block_at(
        &self,
        bytes: Vec<u8>,
        now_ms: u64,
        parent: Hash256,
    ) -> Result<Ticket, Error> {
        self.check_open()?;
        if self.consensus() != ConsensusType::Pow {
            return Err(Error::WrongConsensus);
        }
        if bytes.len() > self.context.config.messages.body_bytes {
            return Err(Error::Busy);
        }
        let context = self.context.clone();
        self.reserve()?
            .submit(move |cancel| context.apply_created_block_at(bytes, now_ms, cancel, parent))
            .map(|pending| Ticket {
                pending,
                timeout: self.context.config.timeout,
            })
            .map_err(Into::into)
    }
    fn import_block_with_origin(
        &self,
        bytes: Vec<u8>,
        now_ms: u64,
        origin: ledger::Origin,
    ) -> Result<Ticket, Error> {
        self.check_open()?;
        if bytes.len() > self.context.config.messages.body_bytes {
            return Err(Error::Busy);
        }
        let context = self.context.clone();
        self.reserve()?
            .submit(move |cancel| context.apply_block(bytes, now_ms, cancel, origin))
            .map(|pending| Ticket {
                pending,
                timeout: self.context.config.timeout,
            })
            .map_err(Into::into)
    }
    pub(crate) fn check_open(&self) -> Result<(), Error> {
        if self.context.stopped.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if self.recovery_required() {
            return Err(Error::RecoveryRequired);
        }
        if self.context.policy_scope.is_closed() || !self.context.db.policy_ready() {
            return Err(Error::Closed);
        }
        Ok(())
    }
    fn reserve(&self) -> Result<blocking::Reservation, Error> {
        self.check_open()?;
        let weight = self
            .context
            .config
            .messages
            .body_bytes
            .checked_add(self.context.config.messages.work_bytes)
            .ok_or(Error::Busy)?;
        self.jobs.reserve(weight).map_err(Into::into)
    }
    fn submit(
        &self,
        reservation: blocking::Reservation,
        input: Input,
        requested: Hash256,
        direction: Direction,
        now_ms: u64,
    ) -> Result<Ticket, Error> {
        self.check_open()?;
        let context = self.context.clone();
        reservation
            .submit(move |cancel| context.apply(input, requested, direction, now_ms, cancel))
            .map(|pending| Ticket {
                pending,
                timeout: self.context.config.timeout,
            })
            .map_err(Into::into)
    }
}
impl<O> Drop for Importer<O> {
    fn drop(&mut self) {
        if self.owns_shutdown {
            self.context.stopped.store(true, Ordering::Release);
            self.context.shutdown.send_replace(true);
            self.jobs.stop();
        }
    }
}
enum Input {
    Packet(Vec<u8>),
    Live(Box<Live>),
}
struct Live {
    received: net::Received,
    client: net::Client,
}
