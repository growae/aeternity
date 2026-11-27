pub mod external;

use ae_codec::headers::KeyHeader;
use ae_consensus::{
    cuckoo::{
        self,
        solver::{self, Input, Limits, Nonces, Progress},
    },
    target::Target,
};
use ae_crypto::blake2b_256;
use ae_types::Hash256;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Work {
    pub hash: Hash256,
    pub target: Target,
    pub edge_bits: u8,
    pub nonces: Nonces,
}
impl Work {
    // The default entry point preserves the public-network graph size.
    pub fn key(header: &KeyHeader, nonces: Nonces) -> Result<Self, Error> {
        Self::key_with_edge_bits(header, nonces, cuckoo::EdgeBits::default())
    }
    pub fn key_with_edge_bits(
        header: &KeyHeader,
        nonces: Nonces,
        edge_bits: cuckoo::EdgeBits,
    ) -> Result<Self, Error> {
        Ok(Self {
            hash: blake2b_256(&cuckoo::sealing_bytes(header).map_err(|_| Error::Header)?),
            target: Target(header.target),
            edge_bits: edge_bits.get(),
            nonces,
        })
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Config {
    // Includes completed receipts/results still retained by callers.
    pub requests: usize,
    pub maximum_attempts: u64,
    // work bounds the sum of charged solver work across the whole job.
    pub solver: Limits,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            requests: 16,
            maximum_attempts: 64,
            solver: Limits::default(),
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        if !(1..=65536).contains(&self.requests) || self.maximum_attempts == 0 {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Id {
    pub worker: u64,
    pub sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Header,
    Busy,
    Closed,
    Sequence,
    Spawn,
    Panicked,
    Cancelled,
    InvalidProof,
    External(external::Error),
    Solve {
        nonce: u64,
        failure: solver::Failure,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Found { nonce: u64, proof: Box<[u64; 42]> },
    Exhausted,
    // External no_value maps to native no_solution, without attested coverage.
    NoSolution,
}
#[derive(Debug, Clone)]
pub struct Completion {
    pub id: Id,
    pub work: Work,
    // A panic during an active search leaves its total accounting unknown.
    pub attempts: Option<u64>,
    pub work_units: Option<u64>,
    pub result: Result<Outcome, Error>,
    pub external: Option<external::Report>,
    _credit: Arc<Credit>,
}
#[derive(Debug)]
struct Credit(Arc<AtomicUsize>);
impl Drop for Credit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
struct Receipt {
    id: Id,
    work: Work,
    cancelled: AtomicBool,
    result: Mutex<Option<Arc<Completion>>>,
    ready: Condvar,
    credit: Arc<Credit>,
}
#[derive(Clone)]
pub struct Ticket(Arc<Receipt>);
impl Ticket {
    pub fn id(&self) -> Id {
        self.0.id
    }
    pub fn work(&self) -> Work {
        self.0.work
    }
    // A request to cancel is not an acknowledgement. Work which already
    // completed retains its exact result; observe the receipt for the outcome.
    pub fn request_cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
    }
    // Linearize a first cancellation request with result publication. A false
    // return means completion or another cancellation request already won.
    // The eventual receipt remains authoritative and is never rewritten.
    pub fn cancel_if_pending(&self) -> bool {
        let result = lock(&self.0.result);
        result.is_none() && !self.0.cancelled.swap(true, Ordering::AcqRel)
    }
    pub fn try_result(&self) -> Option<Arc<Completion>> {
        lock(&self.0.result).clone()
    }
    pub fn blocking_wait(&self) -> Arc<Completion> {
        let result = self
            .0
            .ready
            .wait_while(lock(&self.0.result), |r| r.is_none())
            .unwrap_or_else(|e| e.into_inner());
        result.as_ref().expect("completed receipt").clone()
    }
}
#[derive(Debug, Clone)]
pub struct Status {
    pub worker: u64,
    pub running: bool,
    pub accepting: bool,
    pub submitted: u64,
    pub completed: u64,
    pub pending: usize,
    pub active: Option<Id>,
    pub nonce: Option<u64>,
    pub progress: Option<Progress>,
    pub external: Option<external::Report>,
    pub failure: Option<Error>,
}
struct State {
    status: Status,
    queue: VecDeque<Arc<Receipt>>,
    active: Option<Arc<Receipt>>,
}
#[cfg(test)]
type Hook = Box<dyn FnMut(Progress) + Send>;
#[cfg(test)]
type ExternalHook = Box<dyn FnMut(external::Report) + Send>;
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    credits: Arc<AtomicUsize>,
    config: Config,
    external: Option<external::Config>,
    #[cfg(test)]
    hook: Mutex<Option<Hook>>,
    #[cfg(test)]
    external_hook: Mutex<Option<ExternalHook>>,
}
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
#[derive(Clone)]
pub struct Client(Arc<Shared>);
impl Client {
    pub fn status(&self) -> Status {
        lock(&self.0.state).status.clone()
    }
    pub fn retained(&self) -> usize {
        self.0.credits.load(Ordering::Acquire)
    }
    pub fn submit(&self, work: Work) -> Result<Ticket, Error> {
        let mut s = lock(&self.0.state);
        if !s.status.accepting {
            return Err(Error::Closed);
        }
        if !(1..=63).contains(&work.edge_bits)
            || work.nonces.repeats() > self.0.config.maximum_attempts
        {
            return Err(Error::Configuration);
        }
        if self.retained() >= self.0.config.requests {
            return Err(Error::Busy);
        }
        let sequence = s.status.submitted.checked_add(1).ok_or(Error::Sequence)?;
        self.0.credits.fetch_add(1, Ordering::AcqRel);
        let receipt = Arc::new(Receipt {
            id: Id {
                worker: s.status.worker,
                sequence,
            },
            work,
            cancelled: AtomicBool::new(false),
            result: Mutex::new(None),
            ready: Condvar::new(),
            credit: Arc::new(Credit(self.0.credits.clone())),
        });
        s.queue.push_back(receipt.clone());
        s.status.submitted = sequence;
        s.status.pending += 1;
        self.0.wake.notify_one();
        Ok(Ticket(receipt))
    }
    pub fn request_shutdown(&self) {
        let mut s = lock(&self.0.state);
        s.status.accepting = false;
        for receipt in s.active.iter().chain(s.queue.iter()) {
            receipt.cancelled.store(true, Ordering::Release);
        }
        self.0.wake.notify_all();
    }
}
pub struct Worker {
    client: Client,
    thread: Option<JoinHandle<()>>,
}
static NEXT_WORKER: AtomicU64 = AtomicU64::new(1);
impl Worker {
    pub fn spawn(config: Config) -> Result<Self, Error> {
        Self::spawn_backend(config, None)
    }
    pub fn spawn_external(config: Config, external: external::Config) -> Result<Self, Error> {
        external.validate()?;
        Self::spawn_backend(config, Some(external))
    }
    fn spawn_backend(config: Config, external: Option<external::Config>) -> Result<Self, Error> {
        config.validate()?;
        let worker = NEXT_WORKER
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| v.checked_add(1))
            .map_err(|_| Error::Sequence)?;
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                status: Status {
                    worker,
                    running: true,
                    accepting: true,
                    submitted: 0,
                    completed: 0,
                    pending: 0,
                    active: None,
                    nonce: None,
                    progress: None,
                    external: None,
                    failure: None,
                },
                queue: VecDeque::new(),
                active: None,
            }),
            wake: Condvar::new(),
            credits: Arc::new(AtomicUsize::new(0)),
            config,
            external,
            #[cfg(test)]
            hook: Mutex::new(None),
            #[cfg(test)]
            external_hook: Mutex::new(None),
        });
        let owned = shared.clone();
        let thread = thread::Builder::new()
            .name("ae-pow-miner".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&owned)));
                finish(&owned, result.err().map(|_| Error::Panicked));
            })
            .map_err(|_| Error::Spawn)?;
        Ok(Self {
            client: Client(shared),
            thread: Some(thread),
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    #[cfg(test)]
    pub(crate) fn on_progress(&self, hook: impl FnMut(Progress) + Send + 'static) {
        *lock(&self.client.0.hook) = Some(Box::new(hook));
    }
    #[cfg(test)]
    pub(crate) fn on_external_progress(&self, hook: impl FnMut(external::Report) + Send + 'static) {
        *lock(&self.client.0.external_hook) = Some(Box::new(hook));
    }
    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub fn request_shutdown(&self) {
        self.client.request_shutdown();
    }
    pub fn shutdown(mut self) -> Status {
        self.join()
    }
    fn join(&mut self) -> Status {
        self.request_shutdown();
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            finish(&self.client.0, Some(Error::Panicked));
        }
        self.client.status()
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.join();
    }
}
fn complete(
    receipt: &Receipt,
    attempts: Option<u64>,
    work_units: Option<u64>,
    result: Result<Outcome, Error>,
    external: Option<external::Report>,
) {
    let mut slot = lock(&receipt.result);
    if slot.is_none() {
        *slot = Some(Arc::new(Completion {
            id: receipt.id,
            work: receipt.work,
            attempts,
            work_units,
            result,
            external,
            _credit: receipt.credit.clone(),
        }));
        receipt.ready.notify_all();
    }
}
fn finish(shared: &Shared, error: Option<Error>) {
    let mut s = lock(&shared.state);
    if let Some(receipt) = s.active.take() {
        complete(
            &receipt,
            None,
            None,
            Err(error.clone().unwrap_or(Error::Cancelled)),
            None,
        );
        s.status.completed += 1;
    }
    while let Some(receipt) = s.queue.pop_front() {
        complete(
            &receipt,
            Some(0),
            Some(0),
            Err(error.clone().unwrap_or(Error::Cancelled)),
            None,
        );
        s.status.completed += 1;
    }
    s.status.pending = 0;
    s.status.active = None;
    s.status.running = false;
    s.status.accepting = false;
    s.status.failure = error;
    shared.wake.notify_all();
}
fn run(shared: &Shared) {
    loop {
        let receipt = {
            let mut s = shared
                .wake
                .wait_while(lock(&shared.state), |s| {
                    s.status.accepting && s.queue.is_empty()
                })
                .unwrap_or_else(|e| e.into_inner());
            let Some(receipt) = s.queue.pop_front() else {
                return;
            };
            s.status.active = Some(receipt.id);
            s.status.nonce = None;
            s.status.progress = None;
            s.status.external = None;
            s.active = Some(receipt.clone());
            receipt
        };
        let (attempts, work, result, external) = if let Some(config) = &shared.external {
            let (report, result) = external::execute(shared, &receipt, config);
            // An external executable does not attest how many nonces or solver
            // operations it attempted. Only unstarted work is known to be zero.
            let count = report.pid.is_none().then_some(0);
            (count, count, result, Some(report))
        } else {
            let (attempts, work, result) = mine(shared, &receipt);
            (Some(attempts), Some(work), result, None)
        };
        let mut s = lock(&shared.state);
        complete(&receipt, attempts, work, result, external);
        s.active = None;
        s.status.active = None;
        s.status.pending -= 1;
        s.status.completed += 1;
    }
}
fn mine(shared: &Shared, receipt: &Receipt) -> (u64, u64, Result<Outcome, Error>) {
    let mut work = 0;
    for index in 0..receipt.work.nonces.repeats() {
        if receipt.cancelled.load(Ordering::Acquire) {
            return (index, work, Err(Error::Cancelled));
        }
        let nonce = receipt.work.nonces.at(index).expect("reserved nonce");
        let input = Input {
            hash: receipt.work.hash,
            nonce,
            target: receipt.work.target,
            edge_bits: receipt.work.edge_bits,
        };
        let result = solver::solve(
            input,
            Limits {
                work: shared.config.solver.work - work,
                ..shared.config.solver
            },
            |progress| {
                {
                    let mut s = lock(&shared.state);
                    s.status.nonce = Some(nonce);
                    s.status.progress = Some(progress);
                }
                #[cfg(test)]
                if let Some(hook) = lock(&shared.hook).as_mut() {
                    hook(progress);
                }
                receipt.cancelled.load(Ordering::Acquire)
            },
        );
        match result {
            Ok(report) => {
                work += report.progress.work;
                if let Some(proof) = report.solution {
                    let valid =
                        cuckoo::verify_proof_from_hash(input.hash, nonce, &proof, input.edge_bits)
                            && cuckoo::solution_hash(&proof, input.edge_bits)
                                .is_some_and(|h| input.target.accepts(&h));
                    return (
                        index + 1,
                        work,
                        if valid {
                            Ok(Outcome::Found {
                                nonce,
                                proof: Box::new(proof),
                            })
                        } else {
                            Err(Error::InvalidProof)
                        },
                    );
                }
            }
            Err(failure) => {
                work += failure.progress.work;
                return (index + 1, work, Err(Error::Solve { nonce, failure }));
            }
        }
    }
    (receipt.work.nonces.repeats(), work, Ok(Outcome::Exhausted))
}

#[cfg(test)]
mod tests;
