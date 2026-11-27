// Bounded plain-TCP Stratum owner. Deterministic session logic remains in
// session; this layer supplies OS time, socket I/O and cancellation. Pool
// outputs are handed off explicitly, not executed as accounting/admission.
mod budget;
mod connection;
pub mod pool;
#[cfg(test)]
mod tests;
use crate::{services, session, share, wire::json};
use budget::Budget;
pub use budget::Lease;
use std::{
    collections::{BTreeMap, VecDeque},
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Busy,
    Closed,
    Limit,
    Panicked,
    Io(std::io::ErrorKind),
    Session(session::Error),
    Services(services::Error),
    Pool(pool::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.kind())
    }
}
impl From<services::Error> for Error {
    fn from(e: services::Error) -> Self {
        Self::Services(e)
    }
}
#[derive(Debug, Clone)]
pub struct Limits {
    pub connections: usize,
    pub acceptors: usize,
    pub frame_bytes: usize,
    pub read_bytes: usize,
    pub commands_per_connection: usize,
    pub output_records: usize,
    pub output_bytes: usize,
    pub snapshot_records: usize,
    pub snapshot_bytes: usize,
    pub operation_timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            connections: 64,
            acceptors: 10,
            frame_bytes: 65536,
            read_bytes: 8192,
            commands_per_connection: 16,
            output_records: 256,
            output_bytes: 4 << 20,
            snapshot_records: 16,
            snapshot_bytes: 128 << 20,
            operation_timeout: Duration::from_secs(30),
        }
    }
}
#[derive(Debug, Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub session: session::Config,
    pub services: services::Config,
    pub limits: Limits,
}
impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        self.session.validate().map_err(Error::Session)?;
        let l = &self.limits;
        if !(1..=4096).contains(&l.acceptors)
            || !(1..=4096).contains(&l.connections)
            || l.connections > self.services.connections
            || !(1..=65536).contains(&l.frame_bytes)
            || !(1..=65536).contains(&l.read_bytes)
            || !(1..=1024).contains(&l.commands_per_connection)
            || !(1..=65536).contains(&l.output_records)
            || !(4096..=1 << 30).contains(&l.output_bytes)
            || !(1..=4096).contains(&l.snapshot_records)
            || !(8192..=1 << 30).contains(&l.snapshot_bytes)
            || l.operation_timeout.is_zero()
            || l.operation_timeout > Duration::from_secs(3600)
            || self.session.message_timeout_ms > 86_400_000
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitReason {
    Session(session::StopReason),
    Shutdown,
    Administrative,
    PeerClosed,
    IncompleteFrame,
    FrameLimit,
    OperationTimeout,
    Io(std::io::ErrorKind),
    Services(services::Error),
    Engine(session::Error),
    Clock,
    OutputLimit,
    Pool(pool::Error),
    Panicked,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exit {
    pub id: u64,
    pub peer: SocketAddr,
    pub reason: ExitReason,
    pub last_phase: session::Phase,
    pub read_bytes: u64,
    pub written_bytes: u64,
    pub unsent_bytes: usize,
    // Accepted session effects not handed off to the output consumer. Caller
    // must reconcile these before assuming the request had no pool effect.
    pub pending_pool: Vec<share::Effect>,
    pub pending_receipt: Option<crate::ingress::Id>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    Pool {
        connection: u64,
        effects: Vec<share::Effect>,
    },
    Closed(Exit),
}
fn effect_bytes(e: &share::Effect) -> usize {
    match e {
        share::Effect::SubmitShare {
            account, target, ..
        } => 256 + account.len() + target.bits().div_ceil(8) as usize,
        share::Effect::SubmitSolution { .. } => 512,
    }
}
impl Output {
    fn bytes(&self) -> usize {
        256 + match self {
            Self::Pool { effects, .. } => effects.iter().map(effect_bytes).sum::<usize>(),
            Self::Closed(e) => e.pending_pool.iter().map(effect_bytes).sum(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub id: u64,
    pub peer: SocketAddr,
    pub session: session::Session,
    pub read_bytes: u64,
    pub written_bytes: u64,
}
pub type Reply<T> = oneshot::Receiver<Result<T, Error>>;
enum Command {
    Update(session::Update, oneshot::Sender<Result<(), Error>>),
    Status(oneshot::Sender<Result<Lease<Snapshot>, Error>>),
}
#[derive(Clone)]
pub struct Connection {
    id: u64,
    peer: SocketAddr,
    commands: mpsc::Sender<Command>,
    close: watch::Sender<bool>,
}
impl Connection {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }
    pub fn is_closed(&self) -> bool {
        self.commands.is_closed()
    }
    pub fn close(&self) {
        self.close.send_replace(true);
    }
    pub fn update(&self, update: session::Update) -> Result<Reply<()>, Error> {
        session::Session::new()
            .set(update.clone())
            .map_err(Error::Session)?;
        let (tx, rx) = oneshot::channel();
        self.send(Command::Update(update, tx))?;
        Ok(rx)
    }
    pub fn status(&self) -> Result<Reply<Lease<Snapshot>>, Error> {
        let (tx, rx) = oneshot::channel();
        self.send(Command::Status(tx))?;
        Ok(rx)
    }
    fn send(&self, command: Command) -> Result<(), Error> {
        self.commands.try_send(command).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::Busy,
            mpsc::error::TrySendError::Closed(_) => Error::Closed,
        })
    }
}
struct Outbox {
    queue: Mutex<VecDeque<Lease<Output>>>,
    budget: Arc<Budget>,
}
impl Outbox {
    // Leave an unaccepted output with the caller so it can retry or retain
    // it in the shutdown report without cloning its effects.
    fn push(&self, output: &mut Option<Output>) -> Result<(), Error> {
        let credit = self
            .budget
            .reserve(output.as_ref().expect("pending output").bytes())?;
        self.queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(Lease::new(output.take().expect("pending output"), credit));
        Ok(())
    }
    fn pop(&self) -> Option<Lease<Output>> {
        self.queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front()
    }
}
struct Shared {
    connections: Mutex<BTreeMap<u64, Connection>>,
    outbox: Outbox,
    snapshots: Arc<Budget>,
    config: watch::Sender<session::Config>,
    stop: watch::Sender<bool>,
    waiting_pool: AtomicUsize,
    closed: AtomicBool,
    dropped_report: Mutex<Option<Report>>,
    services: services::Client,
    pool: Option<Arc<dyn pool::Sink>>,
}
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
    address: SocketAddr,
}
impl Client {
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }
    pub fn services(&self) -> services::Client {
        self.shared.services.clone()
    }
    pub fn connections(&self) -> Vec<Connection> {
        self.shared
            .connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }
    // After dropping the owner, surviving clients can recover its terminal
    // report and accepted but unhanded pool effects exactly once.
    pub fn take_dropped_report(&self) -> Option<Report> {
        let mut report = self
            .shared
            .dropped_report
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()?;
        report.outputs = self
            .shared
            .outbox
            .queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect();
        Some(report)
    }
    pub fn next_output(&self) -> Option<Lease<Output>> {
        self.shared.outbox.pop()
    }
    pub fn pending_pool_batches(&self) -> usize {
        self.shared.waiting_pool.load(Ordering::Acquire)
    }
    pub fn retained_output(&self) -> (usize, usize) {
        self.shared.outbox.budget.usage()
    }
    pub fn retained_snapshots(&self) -> (usize, usize) {
        self.shared.snapshots.usage()
    }
    // Current configuration is read for every event/completion. It does not
    // retroactively change an already scheduled timer or allocated nonce.
    pub fn configure(&self, config: session::Config) -> Result<(), Error> {
        config.validate().map_err(Error::Session)?;
        if config.message_timeout_ms > 86_400_000 {
            return Err(Error::Configuration);
        }
        if self.is_closed() || *self.shared.stop.borrow() {
            return Err(Error::Closed);
        }
        self.shared.config.send_replace(config);
        Ok(())
    }
    pub fn request_shutdown(&self) {
        self.shared.stop.send_replace(true);
    }
}
#[derive(Debug)]
pub struct Report {
    pub failure: Option<Error>,
    pub services: services::Info,
    pub outputs: Vec<Lease<Output>>,
    pub unpublished_exits: Vec<Exit>,
}
pub struct Owner {
    client: Client,
    join: Option<thread::JoinHandle<Report>>,
}
impl Owner {
    pub fn bind(config: Config) -> Result<Self, Error> {
        config.validate()?;
        let services = services::Owner::new(config.services.clone())?;
        Self::start(config, services, None)
    }
    pub fn with_sources(
        config: Config,
        sources: impl services::Sources + 'static,
    ) -> Result<Self, Error> {
        config.validate()?;
        let services = services::Owner::with_sources(config.services.clone(), sources)?;
        Self::start(config, services, None)
    }
    pub fn with_pool(config: Config, pool: Arc<dyn pool::Sink>) -> Result<Self, Error> {
        config.validate()?;
        let services = services::Owner::new(config.services.clone())?;
        Self::start(config, services, Some(pool))
    }
    pub fn with_sources_and_pool(
        config: Config,
        sources: impl services::Sources + 'static,
        pool: Arc<dyn pool::Sink>,
    ) -> Result<Self, Error> {
        config.validate()?;
        let services = services::Owner::with_sources(config.services.clone(), sources)?;
        Self::start(config, services, Some(pool))
    }
    fn start(
        config: Config,
        services: services::Owner,
        pool: Option<Arc<dyn pool::Sink>>,
    ) -> Result<Self, Error> {
        let limits = &config.limits;
        let shared = Arc::new(Shared {
            connections: Mutex::new(BTreeMap::new()),
            outbox: Outbox {
                queue: Mutex::new(VecDeque::new()),
                budget: Budget::new(limits.output_records, limits.output_bytes),
            },
            snapshots: Budget::new(limits.snapshot_records, limits.snapshot_bytes),
            config: watch::channel(config.session.clone()).0,
            stop: watch::channel(false).0,
            waiting_pool: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            dropped_report: Mutex::new(None),
            services: services.client(),
            pool,
        });
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let state = shared.clone();
        let join = thread::Builder::new()
            .name("ae-stratum".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    runtime.block_on(async {
                        let listener = TcpListener::bind(config.bind).await?;
                        let address = listener.local_addr()?;
                        let _ = tx.send(Ok(address));
                        Ok::<_, Error>(serve(listener, state.clone(), config.limits).await)
                    })
                }))
                .unwrap_or(Err(Error::Panicked));
                state.closed.store(true, Ordering::Release);
                state
                    .connections
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clear();
                let services = services.shutdown();
                let (failure, unpublished_exits) = match result {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tx.send(Err(e.clone()));
                        (Some(e), Vec::new())
                    }
                };
                Report {
                    failure,
                    services,
                    outputs: Vec::new(),
                    unpublished_exits,
                }
            })?;
        match rx.recv() {
            Ok(Ok(address)) => Ok(Self {
                client: Client { shared, address },
                join: Some(join),
            }),
            other => {
                let _ = join.join();
                Err(other.ok().and_then(Result::err).unwrap_or(Error::Panicked))
            }
        }
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn shutdown(mut self) -> Report {
        self.client.request_shutdown();
        let mut report = self
            .join
            .take()
            .expect("owner thread")
            .join()
            .expect("caught owner panic");
        report.outputs = self
            .client
            .shared
            .outbox
            .queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect();
        report
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.client.request_shutdown();
        if let Some(thread) = self.join.take()
            && let Ok(report) = thread.join()
        {
            *self
                .client
                .shared
                .dropped_report
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(report);
        }
    }
}
async fn serve(
    listener: TcpListener,
    shared: Arc<Shared>,
    limits: Limits,
) -> (Option<Error>, Vec<Exit>) {
    let listener = Arc::new(listener);
    let mut accepts = JoinSet::new();
    let mut tasks = JoinSet::new();
    let mut stop = shared.stop.subscribe();
    let mut changes = shared.services.changes();
    let mut pool_closed = shared.pool.as_ref().map(|pool| pool.closed());
    let mut credits = shared.outbox.budget.changed.subscribe();
    let mut pending = None;
    let mut failure = None;
    loop {
        if *stop.borrow() || shared.services.is_closed() {
            break;
        }
        if pool_closed.as_ref().is_some_and(|closed| *closed.borrow()) {
            failure = Some(Error::Pool(pool::Error::Closed));
            break;
        }
        if let Some(exit) = pending.take() {
            let mut output = Some(Output::Closed(exit));
            if shared.outbox.push(&mut output).is_err() {
                let Some(Output::Closed(exit)) = output else {
                    unreachable!("unaccepted exit");
                };
                pending = Some(exit);
            }
        }
        if pending.is_none() {
            let available = limits.connections.saturating_sub(tasks.len());
            while accepts.len() < limits.acceptors.min(available) {
                let listener = listener.clone();
                accepts.spawn(async move { listener.accept().await });
            }
        }
        tokio::select! {
            _ = stop.changed() => {},
            _ = changes.changed() => {},
            dropped = async { match &mut pool_closed { Some(closed) => closed.changed().await.is_err(), None => std::future::pending::<bool>().await } } => { if dropped { failure=Some(Error::Pool(pool::Error::Closed)); break; } },
            _ = credits.changed(), if pending.is_some() => {},
            result = tasks.join_next(), if !tasks.is_empty() && pending.is_none() => {
                match result.expect("task exists") {
                    Ok(exit) => { pending = Some(exit); },
                    Err(_) => { failure = Some(Error::Panicked); break; }
                }
            },
            accepted = accepts.join_next(), if !accepts.is_empty() && pending.is_none() => {
                let (stream, peer) = match accepted.expect("pending accept") { Ok(Ok(v)) => v, Ok(Err(e)) => { failure = Some(e.into()); break; }, Err(_) => { failure=Some(Error::Panicked); break; } };
                // These options precede session initialization, as in the original handler.
                if let Err(e) = socket2::SockRef::from(&stream).set_keepalive(true) {
                    failure = Some(e.into()); break;
                }
                let lease = match shared.services.connect() {
                    Ok(c) => c,
                    Err(services::Error::Busy | services::Error::Limit) => { drop(stream); continue; },
                    Err(e) => { failure = Some(e.into()); break; }
                };
                let id = lease.id();
                let (commands, rx) = mpsc::channel(limits.commands_per_connection);
                let close = watch::channel(false).0;
                let handle = Connection { id, peer, commands, close: close.clone() };
                shared.connections.lock().unwrap_or_else(|e| e.into_inner()).insert(id, handle);
                tasks.spawn(connection::run(stream, peer, lease, shared.clone(), limits.clone(), rx, close.subscribe()));
            }
        }
    }
    accepts.abort_all();
    while accepts.join_next().await.is_some() {}
    drop(listener);
    shared.stop.send_replace(true);
    let mut exits: Vec<Exit> = pending.into_iter().collect();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(e) => exits.push(e),
            Err(_) => failure = Some(Error::Panicked),
        }
    }
    (failure, exits)
}
