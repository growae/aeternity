// aec_parent_chain_cache process behavior (aeternity fa2a6d7).
// Copyright (c) 2022, Aeternity. ISC notice in repository LICENSE.
use super::{Cache, ChildTop, Event, FetchResult, Host, LookupError, Reply, StepError};
use crate::{
    parent_block::ParentBlock,
    parent_connector::{Consensus, fetch::Query, runtime as connector},
};
use ae_types::Unsigned;
use ae_vm_fate::maps::MapOrder;
use num_bigint::BigInt;
use std::{
    collections::VecDeque,
    io,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetError {
    Unavailable,
    Native,
}
pub trait Targets: Clone + Send + 'static {
    fn targets(&mut self, child_height: &Unsigned) -> Result<Vec<BigInt>, TargetError>;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Full,
    Closed,
    Timeout,
    ResourceLimit,
    OutboxFull,
    Targets(TargetError),
    Connector(connector::Error),
    Core(super::Error),
    FailureBacklog,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Health {
    Running,
    Stopped,
    Failed(Error),
}
#[derive(Debug, Clone)]
pub struct Failure {
    pub event: Event,
    pub error: Error,
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub cache: super::Limits,
    pub maximum_command_bytes: usize,
    pub maximum_outbox_requests: usize,
    pub maximum_outbox_bytes: usize,
    pub maximum_failures: usize,
    pub maximum_failure_bytes: usize,
    pub maximum_timers: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            cache: Default::default(),
            maximum_command_bytes: 16 * 1024 * 1024,
            maximum_outbox_requests: 4096,
            maximum_outbox_bytes: 64 * 1024 * 1024,
            maximum_failures: 64,
            maximum_failure_bytes: 64 * 1024 * 1024,
            maximum_timers: 64,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub command_capacity: usize,
    pub limits: Limits,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            command_capacity: 64,
            limits: Default::default(),
        }
    }
}
#[derive(Clone, Default)]
pub struct ConnectorLink(Arc<Mutex<Option<connector::Client>>>);
impl ConnectorLink {
    pub fn new(client: connector::Client) -> Self {
        Self(Arc::new(Mutex::new(Some(client))))
    }
    pub fn replace(&self, client: Option<connector::Client>) {
        *self.0.lock().expect("parent connector link") = client;
    }
    fn current(&self) -> Option<connector::Client> {
        self.0.lock().expect("parent connector link").clone()
    }
}
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub cache: Arc<Cache>,
    pub health: Health,
    pub lookup_calls: u64,
    pub processed_events: u64,
    pub pending_requests: usize,
    pub outbound_error: Option<connector::Error>,
    pub failed_events: usize,
}
struct State {
    cache: Arc<Cache>,
    health: Health,
    lookup_calls: u64,
    processed_events: u64,
}
struct Failures {
    items: VecDeque<Failure>,
    bytes: usize,
}
struct Control {
    alive: AtomicBool,
    stop: AtomicBool,
    state: Mutex<State>,
    limits: Arc<Mutex<Limits>>,
    failures: Mutex<Failures>,
    enqueue: Mutex<()>,
    outbox: Mutex<Option<Arc<Outbox>>>,
}
type ReplySender = SyncSender<Result<Reply, Error>>;
enum Task {
    Event {
        event: Event,
        reply: Option<ReplySender>,
    },
    Barrier(SyncSender<()>),
}
struct Queued {
    at: Instant,
    task: Task,
}
#[derive(Clone)]
pub struct Client {
    sender: SyncSender<Queued>,
    control: Arc<Control>,
}
pub struct Call {
    receiver: Receiver<Result<Reply, Error>>,
}
impl Call {
    pub fn wait(self, timeout: Duration) -> Result<Reply, Error> {
        self.receiver.recv_timeout(timeout).map_err(call_error)?
    }
}
fn call_error(e: RecvTimeoutError) -> Error {
    match e {
        RecvTimeoutError::Timeout => Error::Timeout,
        RecvTimeoutError::Disconnected => Error::Closed,
    }
}
impl Client {
    fn submit(&self, task: Task) -> Result<(), Error> {
        if !self.control.alive.load(Ordering::Acquire) || self.control.stop.load(Ordering::Acquire)
        {
            return Err(Error::Closed);
        }
        if let Task::Event { event, .. } = &task {
            check_event(
                event,
                self.control
                    .limits
                    .lock()
                    .expect("cache limits")
                    .maximum_command_bytes,
            )?;
        }
        let _enqueue = self.control.enqueue.lock().expect("cache enqueue");
        self.sender
            .try_send(Queued {
                at: Instant::now(),
                task,
            })
            .map_err(|e| match e {
                TrySendError::Full(_) => Error::Full,
                TrySendError::Disconnected(_) => Error::Closed,
            })
    }
    pub fn call(&self, event: Event) -> Result<Call, Error> {
        let (reply, receiver) = mpsc::sync_channel(1);
        self.submit(Task::Event {
            event,
            reply: Some(reply),
        })?;
        Ok(Call { receiver })
    }
    pub fn cast(&self, event: Event) -> Result<(), Error> {
        self.submit(Task::Event { event, reply: None })
    }
    pub fn post_block(&self, block: ParentBlock) -> Result<(), Error> {
        self.cast(Event::PostBlock(block))
    }
    pub fn child_top(&self, top: ChildTop) -> Result<(), Error> {
        self.cast(Event::ChildTop(top))
    }
    pub fn get_once(&self, height: BigInt) -> Result<Result<ParentBlock, LookupError>, Error> {
        self.get_once_timeout(height, Duration::from_secs(5))
    }
    pub fn get_once_timeout(
        &self,
        height: BigInt,
        timeout: Duration,
    ) -> Result<Result<ParentBlock, LookupError>, Error> {
        match self.call(Event::Get(height))?.wait(timeout)? {
            Reply::Block(result) => Ok(result),
            _ => unreachable!("cache lookup reply"),
        }
    }
    // Legacy Timeout counts only 10ms sleeps, not time spent in each call.
    // A positive remainder always causes another call, even if below ten.
    pub fn get_block(
        &self,
        height: BigInt,
        mut timeout_ms: BigInt,
    ) -> Result<Result<ParentBlock, LookupError>, Error> {
        loop {
            let reply = self.get_once(height.clone())?;
            if reply.is_ok() || timeout_ms <= BigInt::from(0) {
                return Ok(reply);
            }
            thread::sleep(Duration::from_millis(10));
            timeout_ms -= 10;
        }
    }
    pub fn barrier(&self, timeout: Duration) -> Result<(), Error> {
        let (s, r) = mpsc::sync_channel(1);
        self.submit(Task::Barrier(s))?;
        r.recv_timeout(timeout).map_err(call_error)
    }
    pub fn configure(&self, limits: Limits) {
        *self.control.limits.lock().expect("cache limits") = limits;
    }
    pub fn take_failure(&self) -> Option<Failure> {
        let mut failures = self.control.failures.lock().expect("cache failures");
        let f = failures.items.pop_front()?;
        failures.bytes -= event_bytes(&f.event).expect("bounded failed event");
        Some(f)
    }
    pub fn snapshot(&self) -> Snapshot {
        let s = self.control.state.lock().expect("cache state");
        let mut health = s.health.clone();
        if !self.control.alive.load(Ordering::Acquire) && health == Health::Running {
            health = Health::Stopped;
        }
        let (pending_requests, outbound_error) = self
            .control
            .outbox
            .lock()
            .expect("cache outbox handle")
            .as_ref()
            .map_or((0, None), |o| {
                let q = o.queue.lock().expect("cache outbox");
                (q.jobs.len(), q.error)
            });
        Snapshot {
            cache: s.cache.clone(),
            health,
            lookup_calls: s.lookup_calls,
            processed_events: s.processed_events,
            pending_requests,
            outbound_error,
            failed_events: self
                .control
                .failures
                .lock()
                .expect("cache failures")
                .items
                .len(),
        }
    }
}
#[derive(Clone)]
pub struct PostSink(Client);
impl connector::Sink for PostSink {
    fn post(&mut self, block: &ParentBlock) -> Result<(), connector::SinkError> {
        // gen_server:cast to an absent cache is accepted and discarded.
        if !self.0.control.alive.load(Ordering::Acquire)
            || self.0.control.stop.load(Ordering::Acquire)
        {
            return Ok(());
        }
        check_block(
            block,
            self.0
                .control
                .limits
                .lock()
                .expect("cache limits")
                .maximum_command_bytes,
        )
        .map_err(|_| connector::SinkError::ResourceLimit)?;
        self.0
            .post_block(block.clone())
            .or_else(|e| if e == Error::Closed { Ok(()) } else { Err(e) })
            .map_err(|e| match e {
                Error::Full => connector::SinkError::Full,
                _ => connector::SinkError::ResourceLimit,
            })
    }
}
pub struct Mailbox {
    client: Client,
    receiver: Receiver<Queued>,
}
impl Mailbox {
    pub fn new(cache: Cache, config: Config) -> io::Result<Self> {
        if config.command_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache mailbox capacity must be positive",
            ));
        }
        let (sender, receiver) = mpsc::sync_channel(config.command_capacity);
        let control = Arc::new(Control {
            alive: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            state: Mutex::new(State {
                cache: Arc::new(cache),
                health: Health::Running,
                lookup_calls: 0,
                processed_events: 0,
            }),
            limits: Arc::new(Mutex::new(config.limits)),
            failures: Mutex::new(Failures {
                items: VecDeque::new(),
                bytes: 0,
            }),
            enqueue: Mutex::new(()),
            outbox: Mutex::new(None),
        });
        Ok(Self {
            client: Client { sender, control },
            receiver,
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn sink(&self) -> PostSink {
        PostSink(self.client.clone())
    }
    pub fn spawn(
        self,
        targets: impl Targets,
        link: ConnectorLink,
        order: impl MapOrder + Send + 'static,
    ) -> io::Result<Runtime> {
        let outbox = Arc::new(Outbox {
            queue: Mutex::new(Outbound {
                jobs: VecDeque::new(),
                bytes: 0,
                error: None,
            }),
            stop: AtomicBool::new(false),
            ready: Condvar::new(),
            link,
            limits: self.client.control.limits.clone(),
        });
        *self
            .client
            .control
            .outbox
            .lock()
            .expect("cache outbox handle") = Some(outbox.clone());
        let sender_box = outbox.clone();
        let pump = thread::Builder::new()
            .name("ae-parent-cache-outbox".into())
            .spawn(move || sender_box.run())?;
        let control = self.client.control.clone();
        control.alive.store(true, Ordering::Release);
        let worker_control = control.clone();
        let worker_box = outbox.clone();
        let worker = match thread::Builder::new()
            .name("ae-parent-cache".into())
            .spawn(move || run(targets, worker_box, order, self.receiver, worker_control))
        {
            Ok(w) => w,
            Err(e) => {
                control.alive.store(false, Ordering::Release);
                outbox.close();
                let _ = pump.join();
                return Err(e);
            }
        };
        Ok(Runtime {
            client: self.client,
            worker: Some(worker),
            outbox,
            pump: Some(pump),
        })
    }
}
#[derive(Debug)]
pub struct Exit {
    pub reason: Option<Error>,
    pub unreported: Option<Failure>,
    pub pending_requests: usize,
}
pub struct Runtime {
    client: Client,
    worker: Option<JoinHandle<Exit>>,
    outbox: Arc<Outbox>,
    pump: Option<JoinHandle<()>>,
}
impl Runtime {
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn shutdown(mut self) -> thread::Result<Exit> {
        self.client.control.stop.store(true, Ordering::Release);
        let result = self.worker.take().expect("live cache worker").join();
        self.outbox.close();
        let pump = self.pump.take().expect("live cache outbox").join();
        pump?;
        result.map(|mut e| {
            e.pending_requests = self.outbox.queue.lock().expect("cache outbox").jobs.len();
            e
        })
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.client.control.stop.store(true, Ordering::Release);
        if let Some(w) = self.worker.take() {
            let _ = w.join();
        }
        self.outbox.close();
        if let Some(p) = self.pump.take() {
            let _ = p.join();
        }
    }
}

struct Outbound {
    jobs: VecDeque<connector::FetchRequest>,
    bytes: usize,
    error: Option<connector::Error>,
}
struct Outbox {
    queue: Mutex<Outbound>,
    stop: AtomicBool,
    ready: Condvar,
    link: ConnectorLink,
    limits: Arc<Mutex<Limits>>,
}
impl Outbox {
    fn close(&self) {
        // Synchronize with the predicate check before an idle worker sleeps.
        let _queue = self.queue.lock().expect("cache outbox");
        self.stop.store(true, Ordering::Release);
        self.ready.notify_all();
    }
    fn append(&self, requests: Vec<connector::FetchRequest>) -> Result<(), Error> {
        if requests.is_empty() {
            return Ok(());
        }
        let limits = *self.limits.lock().expect("cache limits");
        let mut bytes = 0usize;
        for r in &requests {
            bytes = bytes
                .checked_add(query_bytes(&r.query)?)
                .ok_or(Error::ResourceLimit)?;
        }
        let mut q = self.queue.lock().expect("cache outbox");
        if let Some(error) = q.error {
            return Err(Error::Connector(error));
        }
        let count = q
            .jobs
            .len()
            .checked_add(requests.len())
            .ok_or(Error::ResourceLimit)?;
        let size = q.bytes.checked_add(bytes).ok_or(Error::ResourceLimit)?;
        if count > limits.maximum_outbox_requests || size > limits.maximum_outbox_bytes {
            return Err(Error::OutboxFull);
        }
        q.bytes = size;
        q.jobs.extend(requests);
        self.ready.notify_one();
        Ok(())
    }
    fn fetch(&self, height: &BigInt, control: &Control) -> Result<FetchResult, Error> {
        let (s, r) = mpsc::sync_channel(1);
        self.append(vec![connector::FetchRequest {
            query: Query::Height(height.clone()),
            reply: Some(s),
        }])?;
        let due = Instant::now() + Duration::from_secs(5);
        loop {
            if control.stop.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            match r.recv_timeout(
                due.saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(10)),
            ) {
                Ok(Ok(Consensus::Block { block, .. })) => return Ok(FetchResult::Block(block)),
                Ok(Ok(Consensus::NotFound)) => return Ok(FetchResult::NotFound),
                Ok(Ok(Consensus::NoAgreement)) => return Ok(FetchResult::NoAgreement),
                Ok(Err(connector::Error::Closed | connector::Error::Timeout))
                | Err(RecvTimeoutError::Disconnected) => return Ok(FetchResult::Exited),
                Ok(Err(e)) => return Err(Error::Connector(e)),
                Err(RecvTimeoutError::Timeout) => {
                    if Instant::now() >= due {
                        return Ok(FetchResult::Exited);
                    }
                }
            }
        }
    }
    fn run(&self) {
        while !self.stop.load(Ordering::Acquire) {
            let mut q = self.queue.lock().expect("cache outbox");
            while q.jobs.is_empty() && !self.stop.load(Ordering::Acquire) {
                q = self.ready.wait(q).expect("cache outbox ready");
            }
            if self.stop.load(Ordering::Acquire) {
                break;
            }
            let request = q.jobs.pop_front().expect("ready outbound request");
            let size = query_bytes(&request.query).expect("bounded outbound query");
            let result = match self.link.current() {
                Some(client) => client.forward(request),
                None => Err((connector::Error::Closed, request)),
            };
            match result {
                Ok(()) => {
                    q.bytes -= size;
                    q.error = None;
                }
                Err((connector::Error::Closed, request)) => {
                    if let Some(reply) = request.reply {
                        let _ = reply.try_send(Err(connector::Error::Closed));
                    }
                    q.bytes -= size;
                    q.error = None;
                }
                Err((e, mut request)) => {
                    if e != connector::Error::Full
                        && let Some(reply) = request.reply.take()
                    {
                        let _ = reply.try_send(Err(e));
                        q.bytes -= size;
                        q.error = None;
                        continue;
                    }
                    q.jobs.push_front(request);
                    q.error = if e == connector::Error::Full {
                        None
                    } else {
                        Some(e)
                    };
                    drop(q);
                    thread::park_timeout(Duration::from_millis(2));
                }
            }
        }
    }
}
#[derive(Clone)]
struct LocalHost<T> {
    targets: T,
    outbox: Arc<Outbox>,
    control: Arc<Control>,
    requests: Vec<BigInt>,
    timers: Vec<Instant>,
}
impl<T: Targets> Host for LocalHost<T> {
    type Error = Error;
    fn is_operational(e: &Error) -> bool {
        !matches!(e, Error::Targets(TargetError::Native))
    }
    fn targets(&mut self, height: &Unsigned) -> Result<Vec<BigInt>, Error> {
        self.targets.targets(height).map_err(Error::Targets)
    }
    fn now_ms(&mut self) -> BigInt {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => BigInt::from(d.as_millis()),
            Err(e) => -BigInt::from(e.duration().as_millis()),
        }
    }
    fn request(&mut self, height: &BigInt) -> Result<(), Error> {
        self.requests.push(height.clone());
        Ok(())
    }
    fn fetch(&mut self, height: &BigInt) -> Result<FetchResult, Error> {
        self.outbox.fetch(height, &self.control)
    }
    fn retry_initialize(&mut self, after: u64) -> Result<(), Error> {
        self.timers.push(
            Instant::now()
                .checked_add(Duration::from_millis(after))
                .ok_or(Error::ResourceLimit)?,
        );
        Ok(())
    }
}
fn record_failure(control: &Control, f: Failure) -> Result<(), Failure> {
    let bytes = event_bytes(&f.event).unwrap_or(usize::MAX);
    let limits = *control.limits.lock().expect("cache limits");
    let mut failures = control.failures.lock().expect("cache failures");
    let Some(total) = failures.bytes.checked_add(bytes) else {
        return Err(f);
    };
    if failures.items.len() >= limits.maximum_failures || total > limits.maximum_failure_bytes {
        return Err(f);
    }
    failures.bytes = total;
    failures.items.push_back(f);
    Ok(())
}
fn run<T: Targets>(
    targets: T,
    outbox: Arc<Outbox>,
    order: impl MapOrder,
    commands: Receiver<Queued>,
    control: Arc<Control>,
) -> Exit {
    struct Alive(Arc<Control>);
    impl Drop for Alive {
        fn drop(&mut self) {
            self.0.alive.store(false, Ordering::Release);
        }
    }
    let _alive = Alive(control.clone());
    let mut host = LocalHost {
        targets,
        outbox: outbox.clone(),
        control: control.clone(),
        requests: vec![],
        timers: vec![],
    };
    let mut timers: Vec<Instant> = Vec::new();
    let mut pending: Option<Queued> = None;
    let mut startup = true;
    let mut exit = Exit {
        reason: None,
        unreported: None,
        pending_requests: 0,
    };
    while !control.stop.load(Ordering::Acquire) {
        let task = if startup {
            startup = false;
            Task::Event {
                event: Event::Initialize,
                reply: None,
            }
        } else {
            // One lookahead message is enough to merge the mailbox and timer
            // order. Buffered commands cannot accumulate behind repeated timers.
            let enqueue = control.enqueue.lock().expect("cache enqueue");
            if pending.is_none() {
                pending = commands.try_recv().ok();
            }
            let due = timers.first().copied();
            let timer_first = due.is_some_and(|at| {
                at <= Instant::now() && pending.as_ref().is_none_or(|q| q.at > at)
            });
            drop(enqueue);
            if timer_first {
                timers.remove(0);
                Task::Event {
                    event: Event::Initialize,
                    reply: None,
                }
            } else if let Some(item) = pending.take() {
                item.task
            } else {
                let wait = due.map_or(Duration::from_millis(10), |at| {
                    at.saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(10))
                });
                match commands.recv_timeout(wait) {
                    Ok(item) => pending = Some(item),
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }
                continue;
            }
        };
        let Task::Event { event, reply } = task else {
            if let Task::Barrier(s) = task {
                let _ = s.try_send(());
            }
            continue;
        };
        let current = {
            let mut s = control.state.lock().expect("cache state");
            s.processed_events = s.processed_events.saturating_add(1);
            if matches!(event, Event::Get(_)) {
                s.lookup_calls = s.lookup_calls.saturating_add(1);
            }
            s.cache.clone()
        };
        let limits = *control.limits.lock().expect("cache limits");
        let mut next = (*current).clone();
        let mut staged = host.clone();
        staged.requests.clear();
        staged.timers.clear();
        let mut result = next
            .step(event.clone(), &mut staged, &order, limits.cache)
            .map_err(|e| match e {
                StepError::Cache(e) => Error::Core(e),
                StepError::Host(e) => e,
            });
        if control.stop.load(Ordering::Acquire) {
            break;
        }
        let native = matches!(
            &result,
            Err(Error::Core(super::Error::Native(_)) | Error::Targets(TargetError::Native))
        );
        if result.is_ok() || native {
            if timers.len().saturating_add(staged.timers.len()) > limits.maximum_timers {
                result = Err(Error::ResourceLimit);
            } else {
                let requests = staged
                    .requests
                    .iter()
                    .cloned()
                    .map(|h| connector::FetchRequest {
                        query: Query::Height(h),
                        reply: None,
                    })
                    .collect();
                match outbox.append(requests) {
                    Ok(()) => {
                        timers.append(&mut staged.timers);
                        timers.sort();
                        staged.requests.clear();
                        host = staged;
                        if result.is_ok() {
                            control.state.lock().expect("cache state").cache = Arc::new(next);
                        }
                    }
                    Err(e) => result = Err(e),
                }
            }
        }
        let fatal = matches!(
            &result,
            Err(Error::Core(super::Error::Native(_)) | Error::Targets(TargetError::Native))
        );
        if let Some(sender) = reply {
            let _ = sender.try_send(result.clone());
        } else if let Err(error) = &result
            && let Err(f) = record_failure(
                &control,
                Failure {
                    event,
                    error: error.clone(),
                },
            )
        {
            exit.reason = Some(Error::FailureBacklog);
            exit.unreported = Some(f);
            break;
        }
        if fatal {
            exit.reason = result.err();
            break;
        }
    }
    control.state.lock().expect("cache state").health =
        exit.reason.clone().map_or(Health::Stopped, Health::Failed);
    exit
}
fn integer_bytes(n: &BigInt) -> Result<usize, Error> {
    usize::try_from(n.bits().div_ceil(8))
        .map_err(|_| Error::ResourceLimit)?
        .checked_add(1)
        .ok_or(Error::ResourceLimit)
}
fn query_bytes(q: &Query) -> Result<usize, Error> {
    match q {
        Query::Top => Ok(0),
        Query::Height(h) => integer_bytes(h),
        Query::Hash(h) => Ok(h.len()),
    }
}
fn block_bytes(b: &ParentBlock) -> Result<usize, Error> {
    [
        b.hash.len(),
        b.previous_hash.len(),
        b.height.as_bytes().len(),
        b.time.as_bytes().len(),
    ]
    .into_iter()
    .try_fold(0usize, |s, n| s.checked_add(n).ok_or(Error::ResourceLimit))
}
fn event_bytes(e: &Event) -> Result<usize, Error> {
    match e {
        Event::PostBlock(b) => block_bytes(b),
        Event::Get(h) => integer_bytes(h),
        Event::ChildTop(t) => 32usize
            .checked_add(t.height.as_bytes().len())
            .ok_or(Error::ResourceLimit),
        _ => Ok(0),
    }
}
fn check_event(e: &Event, maximum: usize) -> Result<(), Error> {
    if event_bytes(e)? > maximum {
        Err(Error::ResourceLimit)
    } else {
        Ok(())
    }
}
fn check_block(b: &ParentBlock, maximum: usize) -> Result<(), Error> {
    if block_bytes(b)? > maximum {
        Err(Error::ResourceLimit)
    } else {
        Ok(())
    }
}
