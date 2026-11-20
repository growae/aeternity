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
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub mod generation;
mod import;
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
    Generation(generation::Error),
    Network(network::Error),
    Runtime(net::Error),
    Storage(repository::Error),
    Validation {
        operational: bool,
        reason: String,
    },
    CommitUncertain {
        hash: Hash256,
        error: repository::Error,
    },
}
impl From<repository::Error> for Error {
    fn from(e: repository::Error) -> Self {
        Self::Storage(e)
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
    execution: Owned<O>,
    rules: Rules,
    config: Config,
    stopped: AtomicBool,
    shutdown: tokio::sync::watch::Sender<bool>,
    recovery: AtomicBool,
    #[cfg(test)]
    commit_hook: Option<CommitHook>,
}
#[cfg(test)]
type CommitHook = Arc<
    dyn Fn(&Database, repository::Update) -> Result<ae_storage::Committed, repository::Error>
        + Send
        + Sync,
>;
pub struct Importer<O = Otp26> {
    context: Arc<Context<O>>,
    jobs: blocking::Pool,
}
pub struct Ticket {
    pending: blocking::Pending<Result<Report, Failure>>,
    timeout: Duration,
}
impl Ticket {
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
    pub fn new(
        db: Arc<Database>,
        execution: Owned<O>,
        rules: Rules,
        config: Config,
    ) -> Result<Self, Error> {
        if execution.consensus() != ConsensusType::Pow {
            return Err(Error::WrongConsensus);
        }
        {
            let view = db.view()?;
            if !view.identity().matches(execution.network())
                || view.identity().genesis != rules.genesis_hash
            {
                return Err(Error::Storage(repository::Error::Identity));
            }
            if view.config().mode != ae_codec::headers::HeaderMode::ProofOfWork
                || view.status().tip.is_none()
            {
                return Err(Error::Configuration);
            }
        }
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
        let jobs = blocking::Pool::named("ae-chain-import", 1, config.jobs, config.bytes)?;
        Ok(Self {
            context: Arc::new(Context {
                db,
                execution,
                rules,
                config,
                stopped: AtomicBool::new(false),
                shutdown: tokio::sync::watch::channel(false).0,
                recovery: AtomicBool::new(false),
                #[cfg(test)]
                commit_hook: None,
            }),
            jobs,
        })
    }
    pub fn recovery_required(&self) -> bool {
        self.context.recovery.load(Ordering::Acquire)
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
    fn check_open(&self) -> Result<(), Error> {
        if self.context.stopped.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if self.recovery_required() {
            return Err(Error::RecoveryRequired);
        }
        Ok(())
    }
    fn reserve(&self) -> Result<blocking::Reservation<'_>, Error> {
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
        reservation: blocking::Reservation<'_>,
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
    pub async fn fetch(
        &self,
        owner: &network::Handle,
        client: net::Client,
        requested: Hash256,
        direction: Direction,
        now_ms: u64,
    ) -> Result<Report, Failure> {
        let empty = |error| Failure {
            progress: Some(Report::default()),
            error,
        };
        self.check_open().map_err(empty)?;
        if !owner.uses_database(&self.context.db) {
            return Err(empty(Error::Configuration));
        }
        let limits = self.context.config.messages;
        let transport = client.message_limits();
        // The driver decodes the response before handing it to the importer.
        // Its limits must fit the capacity reserved here, not just the later
        // re-encoding check in the blocking worker.
        if transport.body_bytes > limits.body_bytes
            || transport.work_bytes > limits.work_bytes
            || transport.items > limits.items
            || transport.depth > limits.depth
        {
            return Err(empty(Error::Configuration));
        }
        let reservation = self.reserve().map_err(empty)?;
        let mut shutdown = self.context.shutdown.subscribe();
        let download = async {
            if client.is_closed()
                || owner
                    .connection(client.clone())
                    .map_err(|e| empty(e.into()))?
                    .wait()
                    .await
                    .map_err(|e| empty(e.into()))?
                    .is_none()
            {
                return Err(empty(Error::Stale));
            }
            let request = Message::new(
                Kind::GetGeneration,
                1,
                BTreeMap::from([
                    ("hash".into(), Value::Binary(requested.0.to_vec())),
                    (
                        "forward".into(),
                        Value::Boolean(direction == Direction::Forward),
                    ),
                ]),
                self.context.config.messages,
            )
            .map_err(|e| empty(e.into()))?;
            let reply = client
                .request(&request, None)
                .map_err(|e| empty(e.into()))?
                .await
                .map_err(|e| empty(e.into()))?;
            let net::Reply::Response(received) = reply else {
                return Err(empty(Error::Stale));
            };
            if received.generation != *client.generation()
                || client.is_closed()
                || owner
                    .connection(client.clone())
                    .map_err(|e| empty(e.into()))?
                    .wait()
                    .await
                    .map_err(|e| empty(e.into()))?
                    .is_none()
            {
                return Err(empty(Error::Stale));
            }
            Ok(Live { received, client })
        };
        let operation = async {
            let live = tokio::select! {
                biased;
                _ = shutdown.wait_for(|stopped| *stopped) => return Err(empty(Error::Closed)),
                result = download => result?,
            };
            self.submit(
                reservation,
                Input::Live(Box::new(live)),
                requested,
                direction,
                now_ms,
            )
            .map_err(empty)?
            .wait()
            .await
        };
        // Dropping the accepted import wait signals cancellation; a block
        // already at its durable commit boundary can still finish. Callers
        // must inspect the database, never infer rollback from a timeout.
        tokio::time::timeout(self.context.config.timeout, operation)
            .await
            .map_err(|_| Failure {
                progress: None,
                error: Error::Timeout,
            })?
    }
}
impl<O> Drop for Importer<O> {
    fn drop(&mut self) {
        self.context.stopped.store(true, Ordering::Release);
        self.context.shutdown.send_replace(true);
        self.jobs.stop();
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
