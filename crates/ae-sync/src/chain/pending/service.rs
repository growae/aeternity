// Runtime ownership for the conductor's separate pending-header cache.
// No automatic production, sealing, admission, or replay of accepted requests.
use super::{Cache, Host};
use crate::{bindings, chain, keys};
use ae_chain::repository::{self, pow::key};
use ae_codec::headers::KeyHeader;
use ae_storage::snapshots;
use ae_types::{AccountPublicKey, Hash256};
use ae_vm_fate::maps::MapOrder;
use chain::pow::{generator::Clock, preparation};
use std::{
    collections::VecDeque,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll},
    thread::{self, JoinHandle},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    // Includes active, queued and externally retained response receipts.
    pub requests: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self { requests: 16 }
    }
}
impl Limits {
    pub fn valid(self) -> bool {
        (1..=128).contains(&self.requests)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Config {
    // Capability is independent of whether a producer is currently running.
    // Stratum callers supply the retained reward key here, not a worker key.
    pub beneficiary: Option<AccountPublicKey>,
    pub release_info: u32,
    pub key: key::Limits,
    pub limits: Limits,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Binding,
    Busy,
    Closed,
    Sequence,
    Spawn,
    Disconnected,
    Panicked,
    NotFound,
    BeneficiaryNotConfigured,
    BeneficiaryNotApplicable,
    KeysUnavailable,
    Keys(keys::Error),
    Source(chain::Error),
    Repository(repository::Error),
    Preparation(String),
}
impl Error {
    fn fatal(&self) -> bool {
        matches!(
            self,
            Self::Panicked | Self::Keys(keys::Error::Panicked | keys::Error::Disconnected)
        )
    }
}
#[derive(Debug, Clone, Default)]
pub struct Status {
    pub running: bool,
    pub accepting: bool,
    pub joined: bool,
    pub pending: usize,
    pub retained_requests: usize,
    pub accepted: u64,
    pub completed: u64,
    pub aborted: u64,
    pub cached_parent: Option<Hash256>,
    pub last_error: Option<Error>,
    pub failure: Option<Error>,
}
#[derive(Debug)]
pub struct Completion {
    pub result: Result<Arc<KeyHeader>, Error>,
    _credit: OwnedSemaphorePermit,
}
pub struct Ticket(oneshot::Receiver<Arc<Completion>>);
impl Ticket {
    // Dropping or timing out a receiver never cancels accepted cache work.
    pub fn blocking_wait(self) -> Result<Arc<Completion>, Error> {
        self.0.blocking_recv().map_err(|_| Error::Disconnected)
    }
}
impl Future for Ticket {
    type Output = Result<Arc<Completion>, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|r| r.map_err(|_| Error::Disconnected))
    }
}
struct Request {
    reply: oneshot::Sender<Arc<Completion>>,
    credit: OwnedSemaphorePermit,
}
struct State {
    status: Status,
    queue: VecDeque<Request>,
}
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    credits: Arc<Semaphore>,
    limit: usize,
}
fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value.lock().unwrap_or_else(|error| error.into_inner())
}
impl Shared {
    fn stop(&self) {
        lock(&self.state).status.accepting = false;
        self.credits.close();
        self.changed.notify_all();
    }
    fn finish(&self, failure: Option<Error>) {
        self.stop();
        let mut state = lock(&self.state);
        state.status.running = false;
        state.status.failure = failure.clone();
        state.status.aborted += state.status.pending as u64;
        state.status.pending = 0;
        for request in state.queue.drain(..) {
            let _ = request.reply.send(Arc::new(Completion {
                result: Err(failure.clone().unwrap_or(Error::Closed)),
                _credit: request.credit,
            }));
        }
    }
}
#[derive(Clone)]
pub struct Client(Arc<Shared>);
impl Client {
    pub fn request_shutdown(&self) {
        self.0.stop();
    }
    pub fn same_service(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn status(&self) -> Status {
        let mut status = lock(&self.0.state).status.clone();
        status.retained_requests = self.0.limit - self.0.credits.available_permits();
        status
    }
    pub fn get(&self) -> Result<Ticket, Error> {
        let mut state = lock(&self.0.state);
        if !state.status.accepting {
            return Err(Error::Closed);
        }
        let next = state
            .status
            .accepted
            .checked_add(1)
            .ok_or(Error::Sequence)?;
        let credit = self
            .0
            .credits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let (reply, result) = oneshot::channel();
        state.queue.push_back(Request { reply, credit });
        state.status.accepted = next;
        state.status.pending += 1;
        self.0.changed.notify_one();
        Ok(Ticket(result))
    }
}
pub struct Worker {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}
impl Worker {
    pub fn spawn<O: MapOrder + Clone + Send + Sync + 'static>(
        importer: &chain::Importer<O>,
        snapshots: snapshots::Owner,
        keys: bindings::Binding<keys::Client>,
        clock: Arc<dyn Clock>,
        config: Config,
    ) -> Result<Self, Error> {
        if !config.limits.valid() || config.key.history == 0 {
            return Err(Error::Configuration);
        }
        if !snapshots.uses_database(importer.database()) || !keys.uses_database(importer.database())
        {
            return Err(Error::Binding);
        }
        importer.check_open().map_err(Error::Source)?;
        let chain::strategy::Strategy::Pow(profile) = &importer.context.strategy else {
            return Err(Error::Binding);
        };
        // The algorithm-aware preparation path handles both PoW families.
        // The Cuckoo-only mining factory remains unchanged.
        let builder = preparation::Builder::new(
            snapshots,
            preparation::Config {
                identity: importer
                    .database()
                    .view()
                    .map_err(Error::Repository)?
                    .identity()
                    .clone(),
                execution: Arc::new(profile.execution.clone()),
                rules: Arc::new(profile.rules.clone()),
                limits: config.key,
            },
        )
        .map_err(|_| Error::Configuration)?;
        let host = Providers {
            importer: importer.borrowed(),
            builder,
            keys,
            clock,
            config,
        };
        Self::start(host, config.beneficiary.is_some(), config.limits)
    }
    pub fn spawn_hyperchain<O: MapOrder + Clone + Send + Sync + 'static>(
        importer: &chain::Importer<O>,
        block_producer: bool,
        limits: Limits,
    ) -> Result<Self, Error> {
        if importer.algorithm() != ae_protocol::ConsensusAlgorithm::Hyperchain {
            return Err(Error::Binding);
        }
        importer.check_open().map_err(Error::Source)?;
        Self::start(
            Hyperchain {
                importer: importer.borrowed(),
            },
            block_producer,
            limits,
        )
    }
    fn start<H: Host<Error = Error> + Send + 'static>(
        host: H,
        has_beneficiary: bool,
        limits: Limits,
    ) -> Result<Self, Error> {
        if !limits.valid() {
            return Err(Error::Configuration);
        }
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                status: Status {
                    running: true,
                    accepting: true,
                    ..Default::default()
                },
                queue: VecDeque::new(),
            }),
            changed: Condvar::new(),
            credits: Arc::new(Semaphore::new(limits.requests)),
            limit: limits.requests,
        });
        let owner = shared.clone();
        let thread = thread::Builder::new()
            .name("ae-pending-key".into())
            .spawn(move || {
                let failure = catch_unwind(AssertUnwindSafe(|| run(&owner, host, has_beneficiary)))
                    .unwrap_or(Some(Error::Panicked));
                owner.finish(failure);
            })
            .map_err(|_| Error::Spawn)?;
        Ok(Self {
            shared,
            thread: Some(thread),
        })
    }
    pub fn client(&self) -> Client {
        Client(self.shared.clone())
    }
    pub fn request_shutdown(&self) {
        self.shared.stop();
    }
    pub fn shutdown(mut self) -> Status {
        self.join()
    }
    fn join(&mut self) -> Status {
        self.request_shutdown();
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                self.shared.finish(Some(Error::Panicked));
            }
            lock(&self.shared.state).status.joined = true;
        }
        self.client().status()
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.join();
    }
}
struct Hyperchain<O> {
    importer: chain::Importer<O>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Host for Hyperchain<O> {
    type Error = Error;
    fn top(&mut self) -> Result<Option<Hash256>, Error> {
        selected(&self.importer)
    }
    fn beneficiary(&mut self) -> Result<AccountPublicKey, Error> {
        // aec_consensus_hc:next_beneficiary/0 returns not_applicable. Preserve
        // its failed request without constructing a fake candidate or making
        // this known unsupported operation crash the production owner.
        Err(Error::BeneficiaryNotApplicable)
    }
    fn candidate_key(&mut self) -> Result<AccountPublicKey, Error> {
        Err(Error::Binding)
    }
    fn create(
        &mut self,
        _: Hash256,
        _: AccountPublicKey,
        _: AccountPublicKey,
    ) -> Result<Option<KeyHeader>, Error> {
        Err(Error::Binding)
    }
}
fn selected<O: MapOrder + Send + Sync + 'static>(
    importer: &chain::Importer<O>,
) -> Result<Option<Hash256>, Error> {
    importer.check_open().map_err(Error::Source)?;
    Ok(importer
        .database()
        .view()
        .map_err(Error::Repository)?
        .status()
        .tip)
}
struct Providers<O> {
    importer: chain::Importer<O>,
    builder: preparation::Builder<O>,
    keys: bindings::Binding<keys::Client>,
    clock: Arc<dyn Clock>,
    config: Config,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Host for Providers<O> {
    type Error = Error;
    fn top(&mut self) -> Result<Option<Hash256>, Error> {
        selected(&self.importer)
    }
    fn beneficiary(&mut self) -> Result<AccountPublicKey, Error> {
        self.config
            .beneficiary
            .ok_or(Error::BeneficiaryNotConfigured)
    }
    fn candidate_key(&mut self) -> Result<AccountPublicKey, Error> {
        let keys = self.keys.current().ok_or(Error::KeysUnavailable)?;
        keys.candidate_public_key()
            .map_err(Error::Keys)?
            .blocking_wait()
            .map_err(Error::Keys)
    }
    fn create(
        &mut self,
        parent: Hash256,
        beneficiary: AccountPublicKey,
        miner: AccountPublicKey,
    ) -> Result<Option<KeyHeader>, Error> {
        let task = self
            .builder
            .start(
                preparation::Request {
                    parent,
                    beneficiary,
                    miner,
                    release_info: self.config.release_info,
                },
                self.clock.clone(),
            )
            .map_err(construction_error)?;
        let completion = task.wait().ok_or(Error::Disconnected)?;
        match completion.result {
            // Drop the state and immutable snapshot here, before caching or
            // retaining any response. Only the bounded header crosses owners.
            Ok(prepared) => Ok(Some(prepared.header)),
            Err(preparation::Error::Preparation(ae_storage::pow::key::Error::Candidate(
                key::Error::BlockNotFound(_),
            ))) => Ok(None),
            Err(error) => Err(construction_error(error)),
        }
    }
}
fn construction_error(error: preparation::Error) -> Error {
    use ae_storage::pow::key::Error as Storage;
    match &error {
        preparation::Error::Panicked => return Error::Panicked,
        preparation::Error::Busy
        | preparation::Error::Preparation(Storage::Snapshot(snapshots::Error::ResourceLimit))
        | preparation::Error::Preparation(Storage::Candidate(
            key::Error::Repository(repository::Error::ResourceLimit)
            | key::Error::AdjustmentHistory(repository::Error::ResourceLimit),
        )) => return Error::Busy,
        // These are explicit {error, _} returns from the original constructor.
        // Storage/capacity failures and arithmetic exceptions are distinct.
        preparation::Error::Preparation(Storage::Candidate(
            key::Error::BlockNotFound(_)
            | key::Error::AdjustmentHistory(
                repository::Error::Missing(_) | repository::Error::MissingCanonicalKey(_),
            ),
        )) => return Error::NotFound,
        preparation::Error::Finish(key::Error::Candidate(
            ae_chain::key::candidate::Error::Adjustment { expected, .. },
        )) if *expected > 0 => return Error::NotFound,
        _ => (),
    }
    // Preserve a bounded diagnostic without retaining execution trees/errors.
    struct Text(String);
    impl std::fmt::Write for Text {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            for c in text.chars() {
                if self.0.len() + c.len_utf8() > 4096 {
                    return Err(std::fmt::Error);
                }
                self.0.push(c);
            }
            Ok(())
        }
    }
    let mut text = Text(String::new());
    let _ = std::fmt::write(&mut text, format_args!("{error:?}"));
    Error::Preparation(text.0)
}

fn run<H: Host<Error = Error>>(
    shared: &Shared,
    mut host: H,
    has_beneficiary: bool,
) -> Option<Error> {
    let mut cache = Cache::default();
    loop {
        let request = {
            let state = lock(&shared.state);
            let mut state = shared
                .changed
                .wait_while(state, |s| s.status.accepting && s.queue.is_empty())
                .unwrap_or_else(|error| error.into_inner());
            state.queue.pop_front()?
        };
        let result = cache
            .get(&mut host, has_beneficiary)
            .map_err(|error| match error {
                super::Error::NotFound => Error::NotFound,
                super::Error::BeneficiaryNotConfigured => Error::BeneficiaryNotConfigured,
                super::Error::Host(error) => error,
            });
        let failure = result.as_ref().err().filter(|error| error.fatal()).cloned();
        {
            let mut state = lock(&shared.state);
            state.status.completed += 1;
            state.status.pending -= 1;
            state.status.cached_parent = cache.cached().map(|h| Hash256(h.common.previous_hash));
            state.status.last_error = result.as_ref().err().cloned();
        }
        let _ = request.reply.send(Arc::new(Completion {
            result,
            _credit: request.credit,
        }));
        if failure.is_some() {
            return failure;
        }
    }
}

#[cfg(test)]
mod tests;
