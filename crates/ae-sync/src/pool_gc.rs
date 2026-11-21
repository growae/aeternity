// aec_tx_pool_gc, aeternity fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC: repository LICENSE.
use crate::bindings;
use ae_storage::{Database, pool::worker as admission};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use rand::Rng;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex, Weak},
    task::{Context, Poll},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

pub const INTERVAL: Duration = Duration::from_secs(30);
pub fn initial_delay(random: u32) -> Result<Duration, Error> {
    if !(1..=10_000).contains(&random) {
        return Err(Error::Configuration);
    }
    Ok(INTERVAL + Duration::from_millis(u64::from(random)))
}
#[derive(Debug, Clone, Copy)]
pub struct Config {
    // Include executing requests; admission has its own independent bounds.
    pub requests: usize,
    pub request_bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            requests: 128,
            request_bytes: 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Busy,
    Stopped,
    AdmissionUnavailable,
    BindingClosed,
    RecoveryRequired,
    Admission(admission::Error),
    Disconnected,
    Spawn,
    Panicked,
}
#[derive(Debug, Clone, Default)]
pub struct Info {
    pub running: bool,
    pub accepting: bool,
    pub joined: bool,
    pub pending: usize,
    pub pending_bytes: usize,
    pub submitted: u64,
    pub completed: u64,
    pub background_runs: u64,
    pub last_admission_generation: Option<u64>,
    pub last_error: Option<Error>,
    pub failure: Option<Error>,
}
pub struct Ticket<T>(oneshot::Receiver<Result<T, Error>>);
impl<T> Ticket<T> {
    pub fn blocking_wait(self) -> Result<T, Error> {
        self.0.blocking_recv().map_err(|_| Error::Disconnected)?
    }
}
impl<T> Future for Ticket<T> {
    type Output = Result<T, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|r| r.unwrap_or(Err(Error::Disconnected)))
    }
}
struct Completed {
    error: Option<Error>,
    reply: Box<dyn FnOnce() + Send>,
}
trait Work: Send {
    fn wait(self: Box<Self>) -> Completed;
}
struct Job<T> {
    ticket: admission::Ticket<T>,
    reply: oneshot::Sender<Result<T, Error>>,
}
impl<T: Send + 'static> Work for Job<T> {
    fn wait(self: Box<Self>) -> Completed {
        let Job { ticket, reply } = *self;
        let result = ticket.wait().map_err(Error::Admission);
        Completed {
            error: result.as_ref().err().cloned(),
            reply: Box::new(move || {
                let _ = reply.send(result);
            }),
        }
    }
}
struct Queued {
    bytes: usize,
    work: Box<dyn Work>,
}
struct Control {
    info: Info,
    queue: VecDeque<Queued>,
    stop: bool,
}
struct Shared {
    control: Mutex<Control>,
    changed: Condvar,
    config: Config,
    admission: bindings::Admission,
    database: Weak<Database>,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Control> {
        self.control.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn stop(&self) {
        let mut state = self.lock();
        state.stop = true;
        state.info.accepting = false;
        self.changed.notify_all();
    }
    fn target(&self) -> Result<(admission::Client, u64), Error> {
        let db = self.database.upgrade().ok_or(Error::BindingClosed)?;
        if db.events().info().closed || db.gc_events().info().closed {
            return Err(Error::RecoveryRequired);
        }
        let snapshot = self.admission.snapshot();
        if snapshot.closed {
            return Err(Error::BindingClosed);
        }
        if !snapshot.available {
            return Err(Error::AdmissionUnavailable);
        }
        let client = snapshot.value.ok_or(Error::AdmissionUnavailable)?.client;
        if client.origins_interval().is_some() {
            return Err(Error::Configuration);
        }
        Ok((client, snapshot.generation))
    }
}
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
}
impl Client {
    pub fn info(&self) -> Info {
        self.shared.lock().info.clone()
    }
    fn submit<T: Send + 'static>(
        &self,
        bytes: usize,
        action: impl FnOnce(&admission::Client) -> Result<admission::Ticket<T>, admission::Error>,
    ) -> Result<Ticket<T>, Error> {
        let mut state = self.shared.lock();
        if !state.info.accepting {
            return Err(Error::Stopped);
        }
        if state.info.pending >= self.shared.config.requests
            || bytes
                > self
                    .shared
                    .config
                    .request_bytes
                    .saturating_sub(state.info.pending_bytes)
        {
            return Err(Error::Busy);
        }
        // Admission accepts NOW, while the maintenance request is accepted.
        // Replacement never changes the destination of queued work.
        let (client, generation) = self.shared.target()?;
        let ticket = action(&client).map_err(Error::Admission)?;
        let (reply, result) = oneshot::channel();
        state.queue.push_back(Queued {
            bytes,
            work: Box::new(Job { ticket, reply }),
        });
        state.info.pending += 1;
        state.info.pending_bytes += bytes;
        state.info.submitted = state.info.submitted.saturating_add(1);
        state.info.last_admission_generation = Some(generation);
        self.shared.changed.notify_all();
        Ok(Ticket(result))
    }
    pub fn ttl(&self, hash: Hash256) -> Result<Ticket<i128>, Error> {
        self.submit(128, |c| c.ttl(hash))
    }
    pub fn shorten_ttl(&self, hash: Hash256, ttl: i128) -> Result<Ticket<i128>, Error> {
        self.submit(128, |c| c.shorten_ttl(hash, ttl))
    }
    pub fn adjust_ttl(&self, difference: i128) -> Result<Ticket<()>, Error> {
        self.submit(128, |c| c.adjust_ttl(difference))
    }
    pub fn note_origin(
        &self,
        origin: Option<AccountPublicKey>,
        nonce: Unsigned,
    ) -> Result<Ticket<()>, Error> {
        let bytes = 128usize
            .checked_add(nonce.as_bytes().len())
            .ok_or(Error::Busy)?;
        self.submit(bytes, |c| c.note_origin(origin, nonce))
    }
    pub fn delete(&self, hash: Hash256) -> Result<Ticket<()>, Error> {
        self.submit(128, |c| c.delete(hash))
    }
    pub fn collect_at(
        &self,
        height: u64,
        synchronized: bool,
        limit: usize,
    ) -> Result<Ticket<Vec<Hash256>>, Error> {
        self.submit(128, |c| c.collect_at(height, synchronized, limit))
    }
    pub fn collect_origins(&self) -> Result<Ticket<Vec<Hash256>>, Error> {
        self.submit(128, |c| c.collect(true, 0))
    }
}
pub struct Worker {
    client: Client,
    thread: Option<JoinHandle<()>>,
}
impl Worker {
    pub fn spawn(
        database: &Arc<Database>,
        admission: bindings::Admission,
        config: Config,
    ) -> Result<Self, Error> {
        Self::start(
            database,
            admission,
            config,
            initial_delay(rand::thread_rng().gen_range(1..=10_000))?,
            INTERVAL,
        )
    }
    fn start(
        database: &Arc<Database>,
        admission: bindings::Admission,
        config: Config,
        initial: Duration,
        interval: Duration,
    ) -> Result<Self, Error> {
        if !admission.uses_database(database)
            || config.requests == 0
            || config.requests > 65_536
            || config.request_bytes < 128
            || config.request_bytes > 64 * 1024 * 1024
        {
            return Err(Error::Configuration);
        }
        let shared = Arc::new(Shared {
            control: Mutex::new(Control {
                info: Info {
                    running: true,
                    accepting: true,
                    ..Default::default()
                },
                queue: VecDeque::new(),
                stop: false,
            }),
            changed: Condvar::new(),
            config,
            admission,
            database: Arc::downgrade(database),
        });
        shared.target()?;
        let client = Client {
            shared: shared.clone(),
        };
        let thread = thread::Builder::new()
            .name("ae-pool-gc".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(&shared, initial, interval)
                }));
                let mut s = shared.lock();
                s.info.running = false;
                s.info.accepting = false;
                s.info.failure = match result {
                    Ok(result) => result.err(),
                    Err(_) => Some(Error::Panicked),
                };
                // Any already accepted admission operations still belong to the
                // exact admission worker. Normal shutdown always joins their replies.
                s.queue.clear();
                s.info.pending = 0;
                s.info.pending_bytes = 0;
                shared.changed.notify_all();
            })
            .map_err(|_| Error::Spawn)?;
        Ok(Self {
            client,
            thread: Some(thread),
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn info(&self) -> Info {
        self.client.info()
    }
    pub fn request_shutdown(&self) {
        self.client.shared.stop();
    }
    fn join(&mut self) -> Result<(), Error> {
        self.request_shutdown();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| Error::Panicked)?;
        }
        let mut s = self.client.shared.lock();
        s.info.joined = true;
        s.info.failure.clone().map_or(Ok(()), Err)
    }
    pub fn shutdown(mut self) -> Result<(), Error> {
        self.join()
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.join();
    }
}
enum Action {
    Request(Queued),
    Origins,
}
fn run(shared: &Shared, initial: Duration, interval: Duration) -> Result<(), Error> {
    let mut next = Instant::now() + initial;
    let mut background_last = false;
    let mut failure = None;
    loop {
        let action = {
            let mut state = shared.lock();
            loop {
                if shared.admission.is_closed() {
                    state.stop = true;
                    state.info.accepting = false;
                    failure = Some(Error::BindingClosed);
                }
                if state.stop && state.queue.is_empty() {
                    return failure.map_or(Ok(()), Err);
                }
                if !state.stop
                    && Instant::now() >= next
                    && (state.queue.is_empty() || !background_last)
                {
                    background_last = true;
                    break Action::Origins;
                }
                if let Some(request) = state.queue.pop_front() {
                    background_last = false;
                    break Action::Request(request);
                }
                let wait = next
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(100));
                state = shared
                    .changed
                    .wait_timeout(state, wait)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
        };
        match action {
            Action::Request(request) => {
                let completed = request.work.wait();
                {
                    let mut state = shared.lock();
                    state.info.pending -= 1;
                    state.info.pending_bytes -= request.bytes;
                    state.info.completed = state.info.completed.saturating_add(1);
                    state.info.last_error = completed.error;
                }
                (completed.reply)();
            }
            Action::Origins => {
                let result = shared.target().and_then(|(client, generation)| {
                    shared.lock().info.last_admission_generation = Some(generation);
                    client
                        .collect(true, 0)
                        .and_then(|t| t.wait())
                        .map_err(Error::Admission)
                });
                // Native scheduling is relative to callback completion.
                next = Instant::now() + interval;
                let mut state = shared.lock();
                state.info.background_runs = state.info.background_runs.saturating_add(1);
                state.info.last_error = result.err();
            }
        }
    }
}
#[cfg(test)]
mod tests;
