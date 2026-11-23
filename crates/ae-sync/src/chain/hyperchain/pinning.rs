// Owned aec_pinning_agent event processing, aeternity fa2a6d7.
use super::{eoe, parent, scoped};
use crate::{chain, parent_pins};
use ae_chain::{publication, repository as repo};
use ae_hyperchain::{pinning as core, signing::Keys};
use ae_storage::{Database, events, pinning_agent as durable, pinning_children as children, pool};
use ae_types::AccountPublicKey;
use ae_vm_fate::maps::MapOrder;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};
mod host;
mod journal;

#[derive(Debug, Clone)]
pub struct Config {
    pub outputs: usize,
    pub maximum_event_bytes: usize,
    pub maximum_events: usize,
    pub transaction: core::Limits,
    pub children: children::Limits,
    pub maximum_recovery_records: usize,
    pub checkpoint_bytes: usize,
    pub poll: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            outputs: 16,
            maximum_event_bytes: 16 << 20,
            maximum_events: 1024,
            transaction: Default::default(),
            children: Default::default(),
            maximum_recovery_records: 65_536,
            checkpoint_bytes: 16 << 20,
            poll: Duration::from_millis(10),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Cancelled,
    Closed,
    ResourceLimit,
    Spawn,
    Worker,
    State(&'static str),
    LeaderUnavailable,
    Rollback(u64),
    Events(events::Error),
    Source(chain::Error),
    Parent(parent_pins::Error),
    Utilities(parent::Error),
    Pool(pool::worker::Error),
    Transaction(core::Error),
    Children(children::Failure),
    ChildUnresolved(ae_types::Hash256),
    Journal(durable::Failure),
    RecoveryGap(u64),
    RecoveryReorganization(u64),
    StaleEpoch,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildKind {
    Note,
    Proof,
}
pub enum ChildResult {
    Known(Result<pool::Submission, pool::worker::Error>),
    Pending(pool::worker::Ticket<pool::Submission>),
    Recovered(children::Receipt),
    Unresolved,
}
impl std::fmt::Debug for ChildResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Known(r) => f.debug_tuple("Known").field(r).finish(),
            Self::Pending(_) => f.write_str("Pending"),
            Self::Recovered(r) => f.debug_tuple("Recovered").field(r).finish(),
            Self::Unresolved => f.write_str("Unresolved"),
        }
    }
}
#[derive(Debug)]
pub enum Effect {
    Parent(parent_pins::posting::Outcome),
    RecoveredParent(durable::ParentResult),
    Child {
        kind: ChildKind,
        transaction: Arc<ae_mempool::pool::Signed>,
        result: ChildResult,
    },
}
#[derive(Debug)]
pub struct Report {
    pub event: core::Event,
    pub state: core::State,
    pub effects: Vec<Effect>,
    pub error: Option<Error>,
}
impl Report {
    pub fn poll_children(&mut self) {
        for e in &mut self.effects {
            if let Effect::Child { result, .. } = e
                && let ChildResult::Pending(ticket) = result
            {
                match ticket.wait_timeout(Duration::ZERO) {
                    Err(pool::worker::Error::Timeout) => (),
                    value => *result = ChildResult::Known(value),
                }
            }
        }
    }
}
#[derive(Debug, Clone)]
pub struct Status {
    pub state: core::State,
    pub cursor: events::Cursor,
    pub completed: u64,
    pub running: bool,
    pub failure: Option<Error>,
}
struct Shared {
    stop: AtomicBool,
    status: Mutex<Status>,
}
#[derive(Clone)]
pub struct Handle(Arc<Shared>);
impl Handle {
    pub fn status(&self) -> Status {
        self.0
            .status
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
    pub fn request_stop(&self) {
        self.0.stop.store(true, Ordering::Release);
    }
}
pub struct Pending {
    pub payload: Option<Arc<publication::Payload>>,
    pub after: events::Cursor,
    pub events: VecDeque<core::Event>,
}
pub struct Exit {
    _lease: Option<Arc<()>>,
    pub status: Status,
    pub pending: Option<Pending>,
    pub retained: Option<Report>,
    pub outputs: Vec<Report>,
}
impl Exit {
    pub fn poll_children(&mut self) {
        if let Some(report) = &mut self.retained {
            report.poll_children();
        }
        for report in &mut self.outputs {
            report.poll_children();
        }
    }
}
pub struct Inputs<O> {
    pub database: Arc<Database>,
    pub source: Arc<eoe::Source>,
    pub execution: repo::HyperchainConfig<O>,
    pub producer: scoped::Producer<O>,
    pub pool: pool::worker::Client,
    pub parent: parent::Client<O>,
    pub keys: Keys,
}
pub struct Service {
    lease: Arc<()>,
    handle: Handle,
    outputs: mpsc::Receiver<Report>,
    worker: Option<JoinHandle<Exit>>,
}
impl Service {
    pub fn spawn<O: MapOrder + Clone + Send + Sync + 'static>(
        input: Inputs<O>,
        config: Config,
    ) -> Result<Self, Error> {
        if !(1..=4096).contains(&config.outputs)
            || !input.parent.bound_to(&input.producer, &input.source)
            || !(1..=65536).contains(&config.maximum_events)
            || !(1..=1 << 30).contains(&config.maximum_event_bytes)
            || config.poll.is_zero()
            || config.poll > Duration::from_secs(1)
            || !(1..=65536).contains(&config.transaction.hash_bytes)
            || !(1..=65536).contains(&config.transaction.integer_bytes)
            || config.transaction.network_bytes > 65536
            || !input
                .source
                .matches(&input.execution.identity, &input.execution.profile.network)
            || !input.source.uses_database(&input.database)
            || !input.producer.uses_database(&input.database)
            || !input.pool.uses_database(&input.database)
        {
            return Err(Error::Configuration);
        }
        if !(1..=1_000_000).contains(&config.maximum_recovery_records)
            || !input.parent.service().durable_posts()
        {
            return Err(Error::Configuration);
        }
        input.producer.check().map_err(Error::Source)?;
        input.parent.check().map_err(Error::Utilities)?;
        let lease = input.producer.pinning_lease().map_err(Error::Source)?;
        let children = children::Writer::start(input.database.clone(), config.children)
            .map_err(Error::Children)?;
        let source = input.database.events();
        let binding = input.parent.pinning_binding().map_err(Error::Utilities)?;
        let agent =
            durable::Writer::start(input.database.clone(), binding, journal::limits(&config))
                .map_err(Error::Journal)?;
        let checkpoint = agent.checkpoint().map_err(Error::Journal)?.clone();
        journal::preflight(&input.database, &checkpoint, &config)?;
        let cursor = source
            .at_revision(checkpoint.consumed.revision)
            .map_err(Error::Events)?;
        let handle = Handle(Arc::new(Shared {
            stop: AtomicBool::new(false),
            status: Mutex::new(Status {
                state: checkpoint.state,
                cursor,
                completed: checkpoint.completed,
                running: true,
                failure: None,
            }),
        }));
        let shared = handle.0.clone();
        let (sender, outputs) = mpsc::sync_channel(config.outputs);
        let host = host::Host {
            source: Arc::downgrade(&input.source),
            execution: input.execution,
            producer: input.producer,
            pool: input.pool,
            parent: input.parent,
            keys: input.keys,
            config: config.clone(),
            shared: shared.clone(),
            effects: vec![],
            children,
            agent,
            database: input.database,
        };
        let worker = thread::Builder::new()
            .name("ae-hc-pinning".into())
            .spawn(move || run(host, source, sender, shared))
            .map_err(|_| Error::Spawn)?;
        Ok(Self {
            lease,
            handle,
            outputs,
            worker: Some(worker),
        })
    }
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
    pub fn try_output(&self) -> Option<Report> {
        self.outputs.try_recv().ok()
    }
    pub fn shutdown(mut self) -> Result<Exit, Error> {
        self.handle.request_stop();
        let mut exit = self
            .worker
            .take()
            .expect("pinning worker")
            .join()
            .map_err(|_| Error::Worker)?;
        exit._lease = Some(self.lease.clone());
        exit.outputs.extend(self.outputs.try_iter());
        exit.poll_children();
        Ok(exit)
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.handle.request_stop();
        if let Some(w) = self.worker.take() {
            let _ = w.join();
        }
    }
}
fn load<O: MapOrder + Clone + Send + Sync + 'static>(
    host: &mut host::Host<O>,
    source: &events::Reader,
    cursor: &events::Cursor,
) -> Result<Option<Pending>, Error> {
    if let Some(p) = &host.agent.checkpoint().map_err(Error::Journal)?.pending {
        return journal::pending(p, source, None).map(Some);
    }
    let payload = match source.after(cursor, 1) {
        Ok(batch) => {
            if batch.closed {
                return Err(Error::Closed);
            }
            let Some(payload) = batch.records.into_iter().next() else {
                return Ok(None);
            };
            if payload.retained_bytes() > host.config.maximum_event_bytes {
                return Err(Error::ResourceLimit);
            }
            Some(payload)
        }
        Err(events::Error::Gap { .. }) => {
            journal::preflight(
                &host.database,
                host.agent.checkpoint().map_err(Error::Journal)?,
                &host.config,
            )?;
            None
        }
        Err(e) => return Err(Error::Events(e)),
    };
    let batch = journal::batch(
        &host.database,
        cursor
            .revision()
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?,
        &host.config,
    )?;
    let pending = journal::pending(&batch, source, payload)?;
    host.agent.begin(batch).map_err(Error::Journal)?;
    Ok(Some(pending))
}
fn run<O: MapOrder + Clone + Send + Sync + 'static>(
    mut host: host::Host<O>,
    source: events::Reader,
    sender: mpsc::SyncSender<Report>,
    shared: Arc<Shared>,
) -> Exit {
    struct End(Arc<Shared>);
    impl Drop for End {
        fn drop(&mut self) {
            let mut status = self.0.status.lock().unwrap_or_else(|p| p.into_inner());
            if status.running {
                status.running = false;
                status.failure.get_or_insert(Error::Worker);
            }
        }
    }
    let _end = End(shared.clone());
    let mut status = shared
        .status
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let mut pending: Option<Pending> = None;
    let mut retained = None;
    loop {
        *shared.status.lock().unwrap_or_else(|p| p.into_inner()) = status.clone();
        if shared.stop.load(Ordering::Acquire) {
            break;
        }
        if let Some(report) = retained.take() {
            match sender.try_send(report) {
                Ok(()) => (),
                Err(mpsc::TrySendError::Full(r)) => {
                    retained = Some(r);
                    thread::sleep(host.config.poll);
                    continue;
                }
                Err(mpsc::TrySendError::Disconnected(r)) => {
                    retained = Some(r);
                    break;
                }
            }
        }
        if status.failure.is_some() {
            break;
        }
        if pending.is_none() {
            match load(&mut host, &source, &status.cursor) {
                Ok(Some(p)) => pending = Some(p),
                Ok(None) => {
                    thread::sleep(host.config.poll);
                    continue;
                }
                Err(e) => {
                    status.failure = Some(e);
                    break;
                }
            }
        }
        let p = pending.as_mut().expect("event batch");
        let Some(event) = p.events.front().cloned() else {
            if let Err(error) = host.agent.advance().map_err(Error::Journal) {
                status.failure = Some(error);
                break;
            }
            status.cursor = p.after.clone();
            pending = None;
            continue;
        };
        let observed = status.state.enabled || matches!(event, core::Event::Epoch { .. });
        let result = status
            .state
            .apply(&event, &mut host)
            .and_then(|()| host.agent.finish(&status.state).map_err(Error::Journal));
        let error = result.err();
        retained = (observed || error.is_some() || !host.effects.is_empty()).then(|| Report {
            event,
            state: status.state.clone(),
            effects: std::mem::take(&mut host.effects),
            error: error.clone(),
        });
        if let Some(error) = error {
            status.failure = Some(error);
        } else {
            p.events.pop_front();
            status.completed = host
                .agent
                .checkpoint()
                .expect("acknowledged checkpoint")
                .completed;
        }
    }
    status.running = false;
    *shared.status.lock().unwrap_or_else(|p| p.into_inner()) = status.clone();
    Exit {
        _lease: None,
        status,
        pending,
        retained,
        outputs: vec![],
    }
}
