use super::{Config as ServiceConfig, Failure, Inspection, Service};
use crate::Database;
use ae_chain::repository::ReadConfig;
use ae_protocol::NetworkProfile;
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
mod client;
pub mod events;
mod gossip;
mod reply;
pub use client::{Client, Clock, SelectOptions};
pub use gossip::{CacheController, CacheHandle, CacheOwner, GossipCache, LifecycleError};
pub use reply::Ticket;

#[derive(Debug, Clone, Copy)]
pub struct Settings {
    // These bounds include the executing request, not only the waiting queue.
    pub requests: usize,
    pub request_bytes: usize,
    pub single_request_bytes: usize,
    pub max_peek: usize,
    pub origins_interval: Option<Duration>,
    pub event_count: usize,
    pub event_bytes: usize,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            requests: 1024,
            request_bytes: 64 * 1024 * 1024,
            single_request_bytes: 16 * 1024 * 1024 + BASE_WEIGHT,
            max_peek: 100_000,
            origins_interval: Some(Duration::from_secs(30)),
            event_count: 1024,
            event_bytes: 64 * 1024 * 1024,
        }
    }
}
const BASE_WEIGHT: usize = 128;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Service(Failure),
    InvalidSettings,
    Cache(ae_mempool::gossip::Error),
    CacheUnavailable,
    Saturated,
    RequestTooLarge,
    Stopped,
    // The accepted request may have completed. Neither timeout nor a lost
    // reply is a cancellation or permission to replay a mutation blindly.
    Timeout,
    Disconnected,
    Panicked,
    Spawn(String),
}
impl From<Failure> for Error {
    fn from(value: Failure) -> Self {
        Self::Service(value)
    }
}
impl Error {
    pub fn is_operational(&self) -> bool {
        match self {
            Self::Service(failure) => failure.is_operational(),
            _ => true,
        }
    }
}
#[derive(Debug, Clone)]
pub struct Info {
    pub version: u64,
    pub running: bool,
    pub accepting: bool,
    pub queued: usize,
    pub in_flight: bool,
    pub retained_request_bytes: usize,
    pub submitted: u64,
    pub completed: u64,
    pub background_runs: u64,
    pub pool: Inspection,
    // Most recent operation's error, including an ordinary policy rejection.
    pub last_error: Option<Error>,
}
struct Completion {
    error: Option<Error>,
    reply: Box<dyn FnOnce() + Send>,
}
trait Work: Send {
    fn perform(self: Box<Self>, service: &mut Service) -> Completion;
    fn fail(self: Box<Self>, error: Error);
}
struct Job<T, F> {
    reply: reply::Sender<T>,
    action: F,
}
impl<T, F> Work for Job<T, F>
where
    T: Send + 'static,
    F: FnOnce(&mut Service) -> Result<T, Failure> + Send + 'static,
{
    fn perform(self: Box<Self>, service: &mut Service) -> Completion {
        let Job { reply, action } = *self;
        let result = action(service).map_err(Error::Service);
        Completion {
            error: result.as_ref().err().cloned(),
            reply: Box::new(move || {
                reply.send(result);
            }),
        }
    }
    fn fail(self: Box<Self>, error: Error) {
        self.reply.send(Err(error));
    }
}
struct Envelope {
    work: Box<dyn Work>,
    bytes: usize,
}
struct Control {
    info: Info,
    queue: VecDeque<Envelope>,
    outstanding: usize,
    pending: bool,
    stop: bool,
    feed: events::Feed,
}
pub(crate) struct Shared {
    database: std::sync::Weak<Database>,
    control: Mutex<Control>,
    changed: Condvar,
    settings: Settings,
    record: ReadConfig,
    origin: Instant,
    gossip: GossipCache,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Control> {
        // Only mailbox/diagnostic state is recovered here. Durable and pool
        // invariants never live under this mutex.
        self.control.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn signal(&self, control: &mut Control) {
        control.info.version = control.info.version.wrapping_add(1);
        self.changed.notify_all();
    }
    fn wake(&self) {
        let mut c = self.lock();
        if c.info.running && !c.stop {
            c.pending = true;
            self.changed.notify_all();
        }
    }
}
pub struct Worker {
    shared: Arc<Shared>,
    join: Option<JoinHandle<()>>,
}
impl Worker {
    pub fn spawn(
        db: Arc<Database>,
        network: NetworkProfile,
        config: ServiceConfig,
        settings: Settings,
    ) -> Result<Self, Error> {
        Self::spawn_with_cache(db, network, config, settings, GossipCache::default())
    }
    pub fn spawn_with_cache(
        db: Arc<Database>,
        network: NetworkProfile,
        config: ServiceConfig,
        settings: Settings,
        gossip: GossipCache,
    ) -> Result<Self, Error> {
        if settings.requests == 0
            || settings.single_request_bytes < BASE_WEIGHT
            || settings.request_bytes < BASE_WEIGHT
            || settings
                .origins_interval
                .is_some_and(|d| d.is_zero() || Instant::now().checked_add(d).is_none())
        {
            return Err(Error::InvalidSettings);
        }
        let service = Service::start(db.clone(), network, config)?;
        let shared = Arc::new(Shared {
            database: Arc::downgrade(&db),
            control: Mutex::new(Control {
                info: Info {
                    version: 0,
                    running: true,
                    accepting: true,
                    queued: 0,
                    in_flight: false,
                    retained_request_bytes: 0,
                    submitted: 0,
                    completed: 0,
                    background_runs: 0,
                    pool: service.inspection(),
                    last_error: None,
                },
                queue: VecDeque::new(),
                outstanding: 0,
                pending: true,
                stop: false,
                feed: events::Feed::new(settings.event_count, settings.event_bytes),
            }),
            changed: Condvar::new(),
            settings,
            record: db.config.clone(),
            origin: Instant::now(),
            gossip,
        });
        // Register before spawning, with an initial refresh pending. A commit
        // between service restoration and registration is therefore not lost.
        let mut registration = db
            .pool_worker
            .lock()
            .map_err(|_| Error::Service(crate::backend_lock().into()))?;
        *registration = Arc::downgrade(&shared);
        let run_shared = shared.clone();
        let join = thread::Builder::new()
            .name("ae-transaction-pool".into())
            .spawn(move || run(service, run_shared))
            .map_err(|e| Error::Spawn(e.to_string()))?;
        Ok(Self {
            shared,
            join: Some(join),
        })
    }
    pub fn client(&self) -> Client {
        Client {
            shared: self.shared.clone(),
        }
    }
    pub fn info(&self) -> Info {
        self.client().info()
    }
    pub fn wait_for_change(&self, version: u64, timeout: Duration) -> Info {
        self.client().wait_for_change(version, timeout)
    }
    pub fn shutdown(mut self) -> Result<Info, Error> {
        self.stop()?;
        Ok(self.info())
    }
    // Owned shutdown may be requested without blocking the async supervisor.
    // The owner must still call shutdown/drop to join the thread.
    pub fn request_shutdown(&self) {
        let mut c = self.shared.lock();
        c.info.accepting = false;
        c.stop = true;
        self.shared.signal(&mut c);
    }
    fn stop(&mut self) -> Result<(), Error> {
        self.request_shutdown();
        if let Some(join) = self.join.take() {
            join.join().map_err(|_| Error::Panicked)?;
        }
        Ok(())
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
impl Database {
    pub(crate) fn wake_pool(&self) {
        // Notifications cannot turn an acknowledged durable commit into an
        // error, and carry no unbounded event payload. The service reads events.
        let shared = self
            .pool_worker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade();
        if let Some(shared) = shared {
            shared.wake();
        }
    }
}
struct Running(Arc<Shared>);
impl Drop for Running {
    fn drop(&mut self) {
        let queue = {
            let mut c = self.0.lock();
            c.info.running = false;
            c.info.accepting = false;
            c.info.in_flight = false;
            c.info.queued = 0;
            c.info.retained_request_bytes = 0;
            c.outstanding = 0;
            c.pending = false;
            if thread::panicking() {
                c.info.last_error = Some(Error::Panicked);
            }
            let queue = std::mem::take(&mut c.queue);
            self.0.signal(&mut c);
            queue
        };
        for envelope in queue {
            envelope.work.fail(Error::Stopped);
        }
    }
}
enum Action {
    Request(Envelope),
    Refresh,
    Origins,
}
fn run(mut service: Service, shared: Arc<Shared>) {
    let _running = Running(shared.clone());
    let mut next_origins = shared
        .settings
        .origins_interval
        .and_then(|d| Instant::now().checked_add(d));
    let mut background_last = false;
    loop {
        let action = {
            let mut c = shared.lock();
            loop {
                // Graceful shutdown finishes accepted requests in FIFO order.
                // New requests and background work stop immediately.
                if c.stop && c.queue.is_empty() {
                    return;
                }
                let due = next_origins.is_some_and(|t| Instant::now() >= t);
                if !c.stop && (c.pending || due) && (c.queue.is_empty() || !background_last) {
                    c.pending = false;
                    c.info.in_flight = true;
                    background_last = true;
                    shared.signal(&mut c);
                    break if due {
                        Action::Origins
                    } else {
                        Action::Refresh
                    };
                }
                if let Some(envelope) = c.queue.pop_front() {
                    c.info.queued = c.queue.len();
                    c.info.in_flight = true;
                    background_last = false;
                    shared.signal(&mut c);
                    break Action::Request(envelope);
                }
                c = if let Some(next) = next_origins {
                    shared
                        .changed
                        .wait_timeout(c, next.saturating_duration_since(Instant::now()))
                        .unwrap_or_else(|e| e.into_inner())
                        .0
                } else {
                    shared.changed.wait(c).unwrap_or_else(|e| e.into_inner())
                };
            }
        };
        match action {
            Action::Request(envelope) => {
                let completion = envelope.work.perform(&mut service);
                {
                    let mut c = shared.lock();
                    c.outstanding -= 1;
                    c.info.retained_request_bytes -= envelope.bytes;
                    c.info.in_flight = false;
                    c.info.completed = c.info.completed.saturating_add(1);
                    c.info.pool = service.inspection();
                    c.info.last_error = completion.error;
                    shared.signal(&mut c);
                }
                (completion.reply)();
            }
            action => {
                let result = if matches!(action, Action::Origins) {
                    let result = service.collect(true, 0).map(|_| ());
                    next_origins = shared
                        .settings
                        .origins_interval
                        .and_then(|d| Instant::now().checked_add(d));
                    result
                } else {
                    service.refresh().map(|_| ())
                };
                let mut c = shared.lock();
                c.info.in_flight = false;
                c.info.background_runs = c.info.background_runs.saturating_add(1);
                c.info.pool = service.inspection();
                c.info.last_error = result.err().map(Error::Service);
                shared.signal(&mut c);
            }
        }
    }
}
#[cfg(test)]
mod tests;
