// aec_parent_connector block-fetch, top-poll and seed behavior (fa2a6d7).
// Copyright (c) 2022, Aeternity. ISC notice in repository LICENSE.
use super::{
    Consensus,
    fetch::{self, Backend, Pool, Query},
};
use crate::parent_block::ParentBlock;
use std::{
    collections::VecDeque,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkError {
    Full,
    Closed,
    ResourceLimit,
}
// An error means the sink accepted nothing. Retrying a completed fetch must
// not duplicate a post that the sink already accepted.
pub trait Sink: Send + 'static {
    fn post(&mut self, block: &ParentBlock) -> Result<(), SinkError>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Full,
    Closed,
    Timeout,
    ResourceLimit,
    Fetch(fetch::Error),
    Sink(SinkError),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    Running,
    Backpressure,
    WorkStopped(Error),
    Stopped,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub seed: [u8; 16],
    pub top: Option<ParentBlock>,
    pub health: Health,
}
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub command_capacity: usize,
    pub maximum_hosts: usize,
    pub maximum_query_bytes: usize,
    pub response_limits: super::Limits,
    pub fetch_timeout: Duration,
    pub fetch_interval: Option<Duration>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            command_capacity: 64,
            maximum_hosts: 64,
            maximum_query_bytes: 1024 * 1024,
            response_limits: super::Limits::default(),
            fetch_timeout: Duration::from_secs(10),
            fetch_interval: Some(Duration::from_secs(10)),
        }
    }
}
pub(crate) type ReplySender = SyncSender<Result<Consensus<usize>, Error>>;
pub(crate) struct FetchRequest {
    pub query: Query,
    pub reply: Option<ReplySender>,
}
enum Job {
    Barrier(SyncSender<()>),
    Fetch {
        query: Query,
        reply: Option<ReplySender>,
    },
    CheckTop {
        scheduled: bool,
    },
}
struct Control {
    stop: Arc<AtomicBool>,
    retry: AtomicBool,
    state: Mutex<Snapshot>,
    enqueue: Mutex<()>,
}
struct Queued {
    at: Instant,
    job: Job,
}
#[derive(Clone)]
pub struct Client {
    sender: SyncSender<Queued>,
    control: Arc<Control>,
    maximum_query_bytes: usize,
}
impl Client {
    fn submit_owned(&self, job: Job) -> Result<(), (Error, Job)> {
        if self.control.stop.load(Ordering::Acquire) {
            return Err((Error::Closed, job));
        }
        if matches!(&job,Job::Fetch{query,..} if !query.fits(self.maximum_query_bytes)) {
            return Err((Error::ResourceLimit, job));
        }
        let _enqueue = self
            .control
            .enqueue
            .lock()
            .expect("parent connector enqueue");
        self.sender
            .try_send(Queued {
                at: Instant::now(),
                job,
            })
            .map_err(|e| match e {
                TrySendError::Full(item) => (Error::Full, item.job),
                TrySendError::Disconnected(item) => (Error::Closed, item.job),
            })
    }
    fn submit(&self, job: Job) -> Result<(), Error> {
        self.submit_owned(job).map_err(|(e, _)| e)
    }
    pub(crate) fn forward(&self, request: FetchRequest) -> Result<(), (Error, FetchRequest)> {
        self.submit_owned(Job::Fetch {
            query: request.query,
            reply: request.reply,
        })
        .map_err(|(error, job)| {
            let Job::Fetch { query, reply } = job else {
                unreachable!("forwarded fetch")
            };
            (error, FetchRequest { query, reply })
        })
    }
    pub fn fetch(&self, query: Query) -> Result<Consensus<usize>, Error> {
        self.fetch_timeout(query, Duration::from_secs(5))
    }
    pub fn fetch_timeout(
        &self,
        query: Query,
        timeout: Duration,
    ) -> Result<Consensus<usize>, Error> {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.submit(Job::Fetch {
            query,
            reply: Some(sender),
        })?;
        receiver.recv_timeout(timeout).map_err(|e| match e {
            RecvTimeoutError::Timeout => Error::Timeout,
            RecvTimeoutError::Disconnected => Error::Closed,
        })?
    }
    pub fn request(&self, query: Query) -> Result<(), Error> {
        self.submit(Job::Fetch { query, reply: None })
    }
    pub fn barrier(&self, timeout: Duration) -> Result<(), Error> {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.submit(Job::Barrier(sender))?;
        receiver.recv_timeout(timeout).map_err(|e| match e {
            RecvTimeoutError::Timeout => Error::Timeout,
            RecvTimeoutError::Disconnected => Error::Closed,
        })
    }
    pub fn check_top(&self) -> Result<(), Error> {
        self.submit(Job::CheckTop { scheduled: false })
    }
    pub fn retry(&self) {
        let state = self.control.state.lock().expect("parent connector state");
        if matches!(state.health, Health::WorkStopped(_)) {
            self.control.retry.store(true, Ordering::Release);
        }
    }
    // This handle always names one worker generation. Accepted fetches are
    // cancelled with that worker; they are never forwarded to its replacement.
    pub fn request_stop(&self) {
        self.control.stop.store(true, Ordering::Release);
    }
    pub fn snapshot(&self) -> Snapshot {
        let mut snapshot = self
            .control
            .state
            .lock()
            .expect("parent connector state")
            .clone();
        if self.control.stop.load(Ordering::Acquire)
            && !matches!(snapshot.health, Health::WorkStopped(_))
        {
            snapshot.health = Health::Stopped;
        }
        snapshot
    }
}
pub struct Runtime {
    client: Client,
    worker: Option<JoinHandle<()>>,
}
#[derive(Debug, Clone)]
pub struct Retired {
    pub snapshot: Snapshot,
    pub panicked: bool,
}
impl Runtime {
    pub fn spawn(
        backend: Arc<impl Backend>,
        hosts: usize,
        seed: [u8; 16],
        sink: impl Sink,
        config: Config,
    ) -> io::Result<Self> {
        if config.command_capacity == 0
            || config.maximum_query_bytes == 0
            || config.fetch_timeout.is_zero()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "parent connector limits must be positive",
            ));
        }
        let stop = Arc::new(AtomicBool::new(false));
        let pool = Pool::spawn(backend, hosts, config.maximum_hosts, stop.clone())?;
        let control = Arc::new(Control {
            stop,
            retry: AtomicBool::new(false),
            state: Mutex::new(Snapshot {
                seed,
                top: None,
                health: Health::Running,
            }),
            enqueue: Mutex::new(()),
        });
        let (sender, receiver) = mpsc::sync_channel(config.command_capacity);
        let client = Client {
            sender,
            control: control.clone(),
            maximum_query_bytes: config.maximum_query_bytes,
        };
        let due = match config.fetch_interval {
            Some(delay) => Some(Instant::now().checked_add(delay).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "parent poll interval overflow")
            })?),
            None => None,
        };
        let worker = thread::Builder::new()
            .name("ae-parent-connector".into())
            .spawn(move || run(pool, sink, config, receiver, control, due))?;
        Ok(Self {
            client,
            worker: Some(worker),
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn is_finished(&self) -> bool {
        self.worker.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub fn replacement_pending(&self) -> bool {
        !matches!(self.client.snapshot().health, Health::WorkStopped(_))
            && (self.client.control.stop.load(Ordering::Acquire) || self.is_finished())
    }
    pub fn retire(self) -> Retired {
        let client = self.client();
        let panicked = self.shutdown().is_err();
        Retired {
            snapshot: client.snapshot(),
            panicked,
        }
    }
    pub fn shutdown(mut self) -> thread::Result<()> {
        self.client.control.stop.store(true, Ordering::Release);
        self.worker.take().expect("live connector").join()
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.client.control.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
struct Completion {
    state: Snapshot,
    post: Option<ParentBlock>,
    reply: Option<(ReplySender, Consensus<usize>)>,
    scheduled: bool,
}
fn execute(
    pool: &mut Pool,
    job: &Job,
    state: &Snapshot,
    config: Config,
) -> Result<Completion, Error> {
    let query = match job {
        Job::Barrier(_) => unreachable!("barrier handled before fetch"),
        Job::Fetch { query, .. } => query.clone(),
        Job::CheckTop { .. } => Query::Top,
    };
    let result = pool
        .fetch(
            query,
            state.seed,
            config.fetch_timeout,
            config.maximum_query_bytes,
            config.response_limits,
        )
        .map_err(Error::Fetch)?;
    let mut next = state.clone();
    next.health = Health::Running;
    let mut post = None;
    let mut reply = None;
    let mut scheduled = false;
    match job {
        Job::Barrier(_) => unreachable!("barrier handled before fetch"),
        Job::Fetch {
            reply: Some(sender),
            ..
        } => reply = Some((sender.clone(), result)),
        Job::Fetch { reply: None, .. } => {
            if let Consensus::Block { block, .. } = result {
                post = Some(block);
            }
        }
        Job::CheckTop { scheduled: s } => {
            scheduled = *s;
            if let Consensus::Block { block, .. } = result
                && next.top.as_ref() != Some(&block)
            {
                post = Some(block.clone());
                next.top = Some(block);
            }
            next.seed = u128::from_be_bytes(next.seed).wrapping_add(1).to_be_bytes();
        }
    }
    Ok(Completion {
        state: next,
        post,
        reply,
        scheduled,
    })
}
fn status(control: &Control, health: Health) {
    control.state.lock().expect("parent connector state").health = health;
}
fn wait_retry(control: &Control, error: Error) -> bool {
    if error == Error::Fetch(fetch::Error::Stopped) && control.stop.load(Ordering::Acquire) {
        return false;
    }
    status(control, Health::WorkStopped(error));
    while !control.stop.load(Ordering::Acquire) {
        let mut state = control.state.lock().expect("parent connector state");
        if control.retry.swap(false, Ordering::AcqRel) {
            state.health = Health::Running;
            return true;
        }
        drop(state);
        thread::park_timeout(Duration::from_millis(10));
    }
    false
}
fn run(
    mut pool: Pool,
    mut sink: impl Sink,
    config: Config,
    commands: Receiver<Queued>,
    control: Arc<Control>,
    mut due: Option<Instant>,
) {
    let mut queue: VecDeque<Queued> = VecDeque::new();
    'service: while !control.stop.load(Ordering::Acquire) {
        if due.is_some_and(|d| d <= Instant::now()) {
            // A timer that became due during a slow fetch precedes commands
            // enqueued afterward, even though this loop could not read it yet.
            for _ in 0..config.command_capacity {
                match commands.try_recv() {
                    Ok(j) => queue.push_back(j),
                    Err(_) => break,
                }
            }
            let at = due.expect("due timer");
            let position = queue
                .iter()
                .position(|item| item.at > at)
                .unwrap_or(queue.len());
            queue.insert(
                position,
                Queued {
                    at,
                    job: Job::CheckTop { scheduled: true },
                },
            );
            due = None;
        }
        let job = match queue.pop_front() {
            Some(item) => item.job,
            None => match commands.recv_timeout(due.map_or(Duration::from_millis(10), |d| {
                d.saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(10))
            })) {
                Ok(item) => item.job,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            },
        };
        if let Job::Barrier(sender) = job {
            let _ = sender.try_send(());
            continue;
        }
        let completed = loop {
            let state = control
                .state
                .lock()
                .expect("parent connector state")
                .clone();
            match execute(&mut pool, &job, &state, config) {
                Ok(done) => break done,
                Err(e) => {
                    if !wait_retry(&control, e) {
                        break 'service;
                    }
                }
            }
        };
        if let Some(block) = &completed.post {
            loop {
                if control.stop.load(Ordering::Acquire) {
                    break 'service;
                }
                match sink.post(block) {
                    Ok(()) => break,
                    Err(SinkError::Full) => {
                        status(&control, Health::Backpressure);
                        thread::park_timeout(Duration::from_millis(10));
                    }
                    Err(SinkError::Closed) => break 'service,
                    Err(e) => {
                        if !wait_retry(&control, Error::Sink(e)) {
                            break 'service;
                        }
                    }
                }
            }
        }
        *control.state.lock().expect("parent connector state") = completed.state;
        if let Some((sender, result)) = completed.reply {
            let _ = sender.try_send(Ok(result));
        }
        if completed.scheduled {
            due = config
                .fetch_interval
                .and_then(|delay| Instant::now().checked_add(delay));
        }
    }
    control.stop.store(true, Ordering::Release);
    // Retain an explicit work-stop even after cancellation, so stopping a
    // failed worker cannot turn its unresolved condition into a normal exit.
    let mut state = control.state.lock().expect("parent connector state");
    if !matches!(state.health, Health::WorkStopped(_)) {
        state.health = Health::Stopped;
    }
}
