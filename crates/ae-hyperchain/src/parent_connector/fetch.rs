// Bounded parallel parent reads, preserving aeu_lib:pmap's reverse host order.
// Network adapters must bound allocations and honor Context cancellation/deadline.
use super::{Consensus, Response};
use crate::parent_block::ParentBlock;
use num_bigint::BigInt;
use std::{
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    Top,
    Height(BigInt),
    Hash(Vec<u8>),
}
impl Query {
    pub fn fits(&self, maximum: usize) -> bool {
        match self {
            Self::Top => true,
            Self::Height(h) => h.bits().div_ceil(8) < maximum as u64,
            Self::Hash(h) => h.len() <= maximum,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Remote {
    Block(ParentBlock),
    NotFound,
    OtherError,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendError {
    Unavailable,
    ResourceLimit,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Busy,
    ResourceLimit,
    Backend(BackendError),
    Stopped,
}
#[derive(Clone)]
pub struct Context {
    deadline: Instant,
    cancel: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
}
impl Context {
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Acquire)
            || self.stop.load(Ordering::Acquire)
            || Instant::now() >= self.deadline
    }
    pub fn remaining(&self) -> Duration {
        if self.cancelled() {
            Duration::ZERO
        } else {
            self.deadline.saturating_duration_since(Instant::now())
        }
    }
}
pub trait Backend: Send + Sync + 'static {
    // Host indexes follow configured order. Credentials/HTTP details belong to
    // the adapter. Transport failures are Remote::OtherError; local work or
    // storage exhaustion is an operational BackendError, never a quorum vote.
    fn fetch(
        &self,
        host: usize,
        query: &Query,
        seed: &[u8; 16],
        context: &Context,
    ) -> Result<Remote, BackendError>;
}
struct Task {
    id: u64,
    query: Arc<Query>,
    seed: [u8; 16],
    context: Context,
}
struct Answer {
    id: u64,
    host: usize,
    completed: Instant,
    result: Option<Result<Remote, BackendError>>,
}
struct Worker {
    sender: Option<SyncSender<Task>>,
    thread: Option<JoinHandle<()>>,
    busy: bool,
}
struct Batch {
    id: u64,
    deadline: Instant,
    results: Vec<Option<Response<usize>>>,
    remaining: usize,
    fault: Option<Error>,
}
impl Batch {
    fn accept(&mut self, answer: Answer, workers: &mut [Worker]) {
        workers[answer.host].busy = false;
        if answer.id != self.id {
            return;
        }
        self.remaining -= 1;
        if answer.completed > self.deadline {
            return;
        }
        match answer.result {
            Some(Ok(remote)) => {
                self.results[answer.host] = Some(match remote {
                    Remote::Block(block) => Response::Block {
                        block,
                        node: answer.host,
                    },
                    Remote::NotFound => Response::NotFound,
                    Remote::OtherError => Response::OtherError,
                })
            }
            Some(Err(e)) => {
                self.fault.get_or_insert(Error::Backend(e));
            }
            None => {}
        }
    }
}
pub struct Pool {
    workers: Vec<Worker>,
    answers: Option<Receiver<Answer>>,
    stop: Arc<AtomicBool>,
    cancel: Option<Arc<AtomicBool>>,
    next: u64,
}
impl Pool {
    pub fn spawn(
        backend: Arc<impl Backend>,
        hosts: usize,
        maximum_hosts: usize,
        stop: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        if hosts > maximum_hosts {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "parent host limit exceeded",
            ));
        }
        let (answers, receiver) = mpsc::sync_channel(hosts.max(1));
        let mut pool = Self {
            workers: vec![],
            answers: Some(receiver),
            stop: stop.clone(),
            cancel: None,
            next: 0,
        };
        for host in 0..hosts {
            let (sender, requests) = mpsc::sync_channel::<Task>(1);
            let backend = backend.clone();
            let answers = answers.clone();
            let stop = stop.clone();
            let worker = thread::Builder::new()
                .name(format!("ae-parent-fetch-{host}"))
                .spawn(move || {
                    while let Ok(task) = requests.recv() {
                        if stop.load(Ordering::Acquire) {
                            break;
                        }
                        let result = catch_unwind(AssertUnwindSafe(|| {
                            backend.fetch(host, &task.query, &task.seed, &task.context)
                        }))
                        .ok();
                        if answers
                            .send(Answer {
                                id: task.id,
                                host,
                                completed: Instant::now(),
                                result,
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                })?;
            pool.workers.push(Worker {
                sender: Some(sender),
                thread: Some(worker),
                busy: false,
            });
        }
        Ok(pool)
    }
    pub fn fetch(
        &mut self,
        query: Query,
        seed: [u8; 16],
        timeout: Duration,
        maximum_query_bytes: usize,
        limits: super::Limits,
    ) -> Result<Consensus<usize>, Error> {
        if self.stop.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        if !query.fits(maximum_query_bytes) || self.workers.len() > limits.maximum_responses {
            return Err(Error::ResourceLimit);
        }
        let answers = self.answers.as_ref().ok_or(Error::Stopped)?;
        while let Ok(answer) = answers.try_recv() {
            self.workers[answer.host].busy = false;
        }
        // Cancelled jobs retain their fixed worker slot until the adapter
        // returns. Give cooperative deadline cleanup time before declaring a
        // local capacity stop; never spawn replacement threads without bounds.
        let ready_deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(Error::ResourceLimit)?;
        while self.workers.iter().any(|w| w.busy) {
            if self.stop.load(Ordering::Acquire) {
                return Err(Error::Stopped);
            }
            if Instant::now() >= ready_deadline {
                return Err(Error::Busy);
            }
            match answers.recv_timeout(
                ready_deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(10)),
            ) {
                Ok(answer) => self.workers[answer.host].busy = false,
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Err(Error::Stopped),
            }
        }
        let id = self.next;
        self.next = self.next.checked_add(1).ok_or(Error::ResourceLimit)?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(Error::ResourceLimit)?;
        let cancel = Arc::new(AtomicBool::new(false));
        self.cancel = Some(cancel.clone());
        let context = Context {
            deadline,
            cancel: cancel.clone(),
            stop: self.stop.clone(),
        };
        let query = Arc::new(query);
        for worker in &mut self.workers {
            worker
                .sender
                .as_ref()
                .ok_or(Error::Stopped)?
                .try_send(Task {
                    id,
                    query: query.clone(),
                    seed,
                    context: context.clone(),
                })
                .map_err(|_| Error::Stopped)?;
            worker.busy = true;
        }
        let mut batch = Batch {
            id,
            deadline,
            results: vec![None; self.workers.len()],
            remaining: self.workers.len(),
            fault: None,
        };
        while batch.remaining > 0 && Instant::now() < deadline {
            if self.stop.load(Ordering::Acquire) {
                batch.fault = Some(Error::Stopped);
                break;
            }
            match answers.recv_timeout(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(10)),
            ) {
                Ok(answer) => batch.accept(answer, &mut self.workers),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    batch.fault = Some(Error::Stopped);
                    break;
                }
            }
        }
        cancel.store(true, Ordering::Release);
        // A collector descheduled at the boundary must not lose answers that
        // actually completed in time and are already waiting in its mailbox.
        while let Ok(answer) = answers.try_recv() {
            batch.accept(answer, &mut self.workers);
        }
        if self.stop.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        if let Some(e) = batch.fault {
            return Err(e);
        }
        let good: Vec<_> = batch.results.into_iter().rev().flatten().collect();
        super::consensus(&good, self.workers.len(), limits).map_err(|_| Error::ResourceLimit)
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(cancel) = self.cancel.take() {
            cancel.store(true, Ordering::Release);
        }
        for worker in &mut self.workers {
            worker.sender.take();
        }
        self.answers.take();
        for worker in &mut self.workers {
            if let Some(handle) = worker.thread.take() {
                let _ = handle.join();
            }
        }
    }
}
