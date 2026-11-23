// Process-owned aec_preset_keys state, aeternity fa2a6d7.
// Requests are bound to one incarnation. Accepted work is never replayed.
use ae_codec::headers::{Header, KeyHeader, MicroHeader};
use ae_hyperchain::signing::{self, Keys};
use ae_types::{AccountPublicKey, Signature};
use rand::Rng;
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex, MutexGuard, Weak},
    thread::{self, JoinHandle},
    time::Duration,
};

mod receipt;
use receipt::{Job, Pending, Reply};
pub use receipt::{Observed, Ticket};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidSettings,
    ResourceLimit,
    Busy,
    Stopped,
    // Execution may have completed before the reply was lost.
    Disconnected,
    Spawn(std::io::ErrorKind),
    NoActive,
    NoCandidate,
    KeyNotFound,
    Signing(Box<signing::Error>),
    StaleSelection,
}
impl Error {
    fn signing(error: signing::Error) -> Self {
        Self::Signing(Box::new(error))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub requests: usize,
    pub retained_bytes: usize,
    pub message_bytes: usize,
    pub keys: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            requests: 64,
            retained_bytes: 16 * 1024 * 1024,
            message_bytes: 1024 * 1024,
            keys: 65_536,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub requests: usize,
    pub bytes: usize,
}
struct Quota {
    config: Config,
    used: Mutex<Usage>,
}
#[derive(Clone)]
pub struct Capacity(Arc<Quota>);
impl Capacity {
    pub fn new(config: Config) -> Result<Self, Error> {
        if config.requests == 0 || config.retained_bytes == 0 || config.message_bytes == 0 {
            return Err(Error::InvalidSettings);
        }
        Ok(Self(Arc::new(Quota {
            config,
            used: Mutex::new(Usage::default()),
        })))
    }
    pub fn usage(&self) -> Usage {
        *lock(&self.0.used)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Selection {
    pub active: Option<AccountPublicKey>,
    pub candidate: Option<AccountPublicKey>,
}
#[derive(Debug, Clone)]
pub struct Snapshot {
    revision: u64,
    pub selection: Selection,
    origin: Arc<()>,
}
impl Snapshot {
    pub fn revision(&self) -> u64 {
        self.revision
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    Shutdown,
    Panicked,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Info {
    pub accepting: bool,
    pub retained_requests: usize,
    pub retained_bytes: usize,
    pub exit: Option<Exit>,
    pub joined: bool,
}

struct State {
    keys: Keys,
    revision: u64,
    origin: Arc<()>,
}
impl State {
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            revision: self.revision,
            selection: Selection {
                active: self.keys.active(),
                candidate: self.keys.candidate(),
            },
            origin: self.origin.clone(),
        }
    }
    fn select(&mut self, selection: Selection) -> Result<(), Error> {
        if selection == self.snapshot().selection {
            return Ok(());
        }
        let revision = self.revision.checked_add(1).ok_or(Error::ResourceLimit)?;
        let mut keys = self.keys.fresh();
        if let Some(key) = selection.active {
            keys.promote(key).map_err(|_| Error::KeyNotFound)?;
        }
        if let Some(key) = selection.candidate {
            keys.set_candidate(key).map_err(|_| Error::KeyNotFound)?;
        }
        self.keys = keys;
        self.revision = revision;
        Ok(())
    }
}

struct Queue {
    jobs: VecDeque<Box<dyn Job>>,
    accepting: bool,
    requests: usize,
    bytes: usize,
    exit: Option<Exit>,
    joined: bool,
}
struct Shared {
    queue: Mutex<Queue>,
    wake: Condvar,
    config: Config,
    capacity: Capacity,
}
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

// Clients contain no key material and cannot migrate to another worker.
#[derive(Clone)]
pub struct Client(Arc<Shared>);
impl Client {
    #[cfg(test)]
    pub(crate) fn pause_for_test(&self) -> (Ticket<()>, std::sync::mpsc::Sender<()>) {
        let (entered, waiting) = std::sync::mpsc::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let ticket = self
            .submit(0, || {
                move |_| {
                    let _ = entered.send(());
                    let _ = blocked.recv();
                    Ok(())
                }
            })
            .unwrap();
        waiting.recv_timeout(Duration::from_secs(5)).unwrap();
        (ticket, release)
    }
    #[cfg(test)]
    pub(crate) fn panic_for_test(&self) -> Ticket<()> {
        self.submit(0, || |_| panic!("controlled preset-key worker panic"))
            .unwrap()
    }
    pub fn capacity(&self) -> Capacity {
        self.0.capacity.clone()
    }
    pub fn same_worker(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn info(&self) -> Info {
        let q = lock(&self.0.queue);
        Info {
            accepting: q.accepting,
            retained_requests: q.requests,
            retained_bytes: q.bytes,
            exit: q.exit,
            joined: q.joined,
        }
    }
    // Build payloads only after reserving quota. Credits cover queued, running
    // and unread replies, including clones of a completed ticket.
    fn submit<T, F>(&self, payload: usize, build: impl FnOnce() -> F) -> Result<Ticket<T>, Error>
    where
        T: Clone + Send + 'static,
        F: FnOnce(&mut State) -> Result<T, Error> + Send + 'static,
    {
        let bytes = payload
            .checked_add(std::mem::size_of::<Pending<T, F>>())
            .and_then(|n| {
                n.checked_add(std::mem::size_of::<Reply<T>>() + 2 * std::mem::size_of::<usize>())
            })
            // A terminal signing error owns one boxed diagnostic. Reserve
            // its maximum size even when the request ultimately succeeds.
            .and_then(|n| n.checked_add(std::mem::size_of::<signing::Error>()))
            .ok_or(Error::ResourceLimit)?;
        let mut q = lock(&self.0.queue);
        if !q.accepting {
            return Err(Error::Stopped);
        }
        if bytes > self.0.config.retained_bytes {
            return Err(Error::ResourceLimit);
        }
        let mut used = lock(&self.0.capacity.0.used);
        if used.requests >= self.0.config.requests
            || bytes > self.0.config.retained_bytes.saturating_sub(used.bytes)
        {
            return Err(Error::Busy);
        }
        let work = build();
        let reply = Arc::new(Reply::new(
            Arc::downgrade(&self.0),
            self.0.capacity.clone(),
            bytes,
        ));
        q.requests += 1;
        q.bytes += bytes;
        used.requests += 1;
        used.bytes += bytes;
        q.jobs
            .push_back(Box::new(Pending::new(reply.clone(), work)));
        drop(used);
        drop(q);
        self.0.wake.notify_one();
        Ok(Ticket(reply))
    }
    pub fn is_ready(&self) -> Result<Ticket<bool>, Error> {
        self.submit(0, || |_| Ok(true))
    }
    pub fn active(&self) -> Result<Ticket<AccountPublicKey>, Error> {
        self.submit(0, || |s| s.keys.active().ok_or(Error::NoActive))
    }
    pub fn candidate(&self) -> Result<Ticket<AccountPublicKey>, Error> {
        self.submit(0, || |s| s.keys.candidate().ok_or(Error::NoCandidate))
    }
    pub fn is_key_present(&self, key: AccountPublicKey) -> Result<Ticket<bool>, Error> {
        self.submit(0, || move |s| Ok(s.keys.contains(key)))
    }
    pub fn set_candidate(&self, key: AccountPublicKey) -> Result<Ticket<()>, Error> {
        self.submit(0, || {
            move |s| {
                let mut selection = s.snapshot().selection;
                selection.candidate = Some(key);
                s.select(selection)
            }
        })
    }
    pub fn promote_candidate(&self, key: AccountPublicKey) -> Result<Ticket<()>, Error> {
        self.submit(0, || {
            move |s| {
                let active = s.keys.contains(key).then_some(key);
                s.select(Selection {
                    active,
                    candidate: None,
                })?;
                active.map(|_| ()).ok_or(Error::KeyNotFound)
            }
        })
    }
    pub fn set_random_candidate(&self) -> Result<Ticket<AccountPublicKey>, Error> {
        self.submit(0, || {
            |s| {
                let len = s.keys.public_keys().len();
                if len == 0 {
                    return Err(Error::KeyNotFound);
                }
                let key = s
                    .keys
                    .public_keys()
                    .nth(rand::thread_rng().gen_range(0..len))
                    .unwrap();
                let mut selection = s.snapshot().selection;
                selection.candidate = Some(key);
                s.select(selection)?;
                Ok(key)
            }
        })
    }
    pub fn snapshot(&self) -> Result<Ticket<Snapshot>, Error> {
        self.submit(0, || |s| Ok(s.snapshot()))
    }
    pub fn commit_selection(
        &self,
        expected: Snapshot,
        selection: Selection,
    ) -> Result<Ticket<Snapshot>, Error> {
        self.submit(0, || {
            move |s| {
                if !Arc::ptr_eq(&expected.origin, &s.origin) || expected.revision != s.revision {
                    return Err(Error::StaleSelection);
                }
                s.select(selection)?;
                Ok(s.snapshot())
            }
        })
    }
    pub fn sign_binary(
        &self,
        message: &[u8],
        by: AccountPublicKey,
    ) -> Result<Ticket<Signature>, Error> {
        if message.len() > self.0.config.message_bytes {
            return Err(Error::ResourceLimit);
        }
        self.submit(message.len(), || {
            let message = message.to_vec();
            move |s| s.keys.sign(by, &message).map_err(Error::signing)
        })
    }
    pub fn sign_micro(
        &self,
        header: MicroHeader,
        by: Option<AccountPublicKey>,
    ) -> Result<Ticket<MicroHeader>, Error> {
        self.submit(0, || {
            move |s| {
                let by = by.or(s.keys.active()).ok_or(Error::NoActive)?;
                s.keys.sign_micro(&header, Some(by)).map_err(Error::signing)
            }
        })
    }
    pub fn key_header_signature(
        &self,
        header: KeyHeader,
        by: AccountPublicKey,
    ) -> Result<Ticket<Signature>, Error> {
        self.submit(0, || {
            move |s| {
                let bytes = Header::Key(header)
                    .signature_bytes()
                    .map_err(|e| Error::signing(signing::Error::Encoding(e)))?;
                s.keys.sign(by, &bytes).map_err(Error::signing)
            }
        })
    }
}

pub struct Worker {
    client: Client,
    thread: Option<JoinHandle<()>>,
}
impl Worker {
    pub fn spawn(keys: Keys, config: Config) -> Result<Self, Error> {
        Self::spawn_with_capacity(keys, Capacity::new(config)?)
    }
    pub fn spawn_with_capacity(keys: Keys, capacity: Capacity) -> Result<Self, Error> {
        let config = capacity.0.config;
        if keys.public_keys().len() > config.keys {
            return Err(Error::ResourceLimit);
        }
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                jobs: VecDeque::new(),
                accepting: true,
                requests: 0,
                bytes: 0,
                exit: None,
                joined: false,
            }),
            wake: Condvar::new(),
            config,
            capacity,
        });
        let client = Client(shared.clone());
        let mut state = State {
            keys: keys.fresh(),
            revision: 0,
            origin: Arc::new(()),
        };
        let thread = thread::Builder::new()
            .name("ae-preset-keys".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    loop {
                        let job = {
                            let mut q = lock(&shared.queue);
                            while q.jobs.is_empty() && q.accepting {
                                q = shared.wake.wait(q).unwrap_or_else(|e| e.into_inner());
                            }
                            q.jobs.pop_front()
                        };
                        match job {
                            Some(job) => job.run(&mut state),
                            None => break,
                        }
                    }
                }));
                let jobs = {
                    let mut q = lock(&shared.queue);
                    q.accepting = false;
                    std::mem::take(&mut q.jobs)
                };
                // Never drop receipts while holding the quota mutex.
                drop(jobs);
                lock(&shared.queue).exit = Some(if result.is_ok() {
                    Exit::Shutdown
                } else {
                    Exit::Panicked
                });
            })
            .map_err(|e| Error::Spawn(e.kind()))?;
        Ok(Self {
            client,
            thread: Some(thread),
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub fn request_shutdown(&self) {
        lock(&self.client.0.queue).accepting = false;
        self.client.0.wake.notify_all();
    }
    fn join(&mut self) {
        self.request_shutdown();
        if let Some(thread) = self.thread.take() {
            let result = thread.join();
            let mut q = lock(&self.client.0.queue);
            if result.is_err() {
                q.exit = Some(Exit::Panicked);
            }
            q.joined = true;
        }
    }
    pub fn retire(mut self) -> Info {
        self.join();
        self.client.info()
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.join();
    }
}

#[cfg(test)]
mod tests;
