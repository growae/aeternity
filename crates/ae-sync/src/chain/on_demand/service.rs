// Bounded live ownership of the existing synchronous consensus command path.
// Dropping a ticket never cancels an accepted command or releases its receipt.
use super::{
    control::{Command, Kind},
    runtime,
};
use crate::chain::pow::packing;
use ae_storage::pool::worker as pool;
use ae_vm_fate::maps::MapOrder;
use std::{
    any::Any,
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    // Includes queued, active, unacknowledged and externally retained receipts.
    pub requests: usize,
    // Retained command allocations, with room for the controller's input copy.
    // Block results also obey the supplied controller's per-command limits.
    pub command_bytes: usize,
    pub poll: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            requests: 8,
            command_bytes: 1 << 20,
            poll: Duration::from_millis(10),
        }
    }
}
impl Config {
    pub fn validate(self) -> Result<(), Error> {
        if !(1..=128).contains(&self.requests)
            || !(1..=16 << 20).contains(&self.command_bytes)
            || self.poll.is_zero()
            || self.poll > Duration::from_secs(1)
        {
            Err(Error::Configuration)
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Closed,
    Busy,
    ResourceLimit,
    Sequence,
    Spawn(String),
    Cancelled,
    RecoveryRequired,
    Panicked,
    ReceiptLost,
    Retirement(packing::Error),
}
#[derive(Debug, Clone, Default)]
pub struct Status {
    pub running: bool,
    pub accepting: bool,
    pub joined: bool,
    pub active: Option<u64>,
    pub queued: usize,
    pub retained_requests: usize,
    pub retained_command_bytes: usize,
    pub accepted: u64,
    pub completed: u64,
    pub cancelled: u64,
    pub failure: Option<Error>,
}
#[derive(Debug, Default)]
struct Usage {
    requests: usize,
    bytes: usize,
}
#[derive(Debug)]
struct Credit {
    usage: Arc<Mutex<Usage>>,
    bytes: usize,
}
impl Drop for Credit {
    fn drop(&mut self) {
        let mut u = lock(&self.usage);
        u.requests -= 1;
        u.bytes -= self.bytes;
    }
}
#[derive(Debug)]
pub struct Accepted {
    pub id: u64,
    pub command: Command,
    _credit: Credit,
}
#[derive(Debug)]
pub struct Completion {
    pub accepted: Arc<Accepted>,
    // A report may contain a committed prefix even when its command failed.
    // Err(Cancelled) denotes a queued command that never entered the controller.
    pub result: Result<runtime::Report, Error>,
}
#[derive(Debug)]
pub struct Rejected {
    pub error: Error,
    pub command: Command,
}
pub struct Ticket {
    accepted: Arc<Accepted>,
    result: watch::Receiver<Option<Arc<Completion>>>,
}
impl Ticket {
    pub fn accepted(&self) -> &Arc<Accepted> {
        &self.accepted
    }
    pub fn try_result(&self) -> Result<Option<Arc<Completion>>, Error> {
        // Keep the read guard until closure is checked. Otherwise a sender
        // can publish successfully and close between these two observations.
        let result = self.result.borrow();
        if let Some(r) = result.clone() {
            return Ok(Some(r));
        }
        self.result.has_changed().map_err(|_| Error::ReceiptLost)?;
        Ok(None)
    }
    pub async fn wait(mut self) -> Result<Arc<Completion>, Error> {
        loop {
            if let Some(r) = self.result.borrow().clone() {
                return Ok(r);
            }
            self.result
                .changed()
                .await
                .map_err(|_| Error::ReceiptLost)?;
        }
    }
}
struct Request {
    accepted: Arc<Accepted>,
    reply: watch::Sender<Option<Arc<Completion>>>,
}
struct State {
    status: Status,
    shutdown: bool,
    active: Option<Arc<Accepted>>,
    requests: VecDeque<Request>,
    outputs: VecDeque<Arc<Completion>>,
}
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    stop: runtime::Stop,
    config: Config,
    usage: Arc<Mutex<Usage>>,
    // Only implicit Worker drop deposits here. Explicit shutdown returns its
    // receipt directly, so a client cannot steal retirement from a root owner.
    dropped: Mutex<Option<Report>>,
}
#[derive(Clone)]
pub struct Client(Arc<Shared>);
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
fn command_bytes(c: &Command) -> Option<usize> {
    let extra = match c {
        Command::Until { hashes, .. } => hashes.iter().try_fold(
            hashes
                .capacity()
                .checked_mul(std::mem::size_of::<Vec<u8>>())?,
            |n, h| n.checked_add(h.capacity()),
        )?,
        Command::Blocks {
            kind: Kind::Unknown(k),
            ..
        } => k.capacity(),
        _ => 0,
    };
    std::mem::size_of::<Command>()
        .checked_add(extra)?
        .checked_mul(2)
}
impl Client {
    pub fn submit(&self, command: Command) -> Result<Ticket, Box<Rejected>> {
        let mut s = lock(&self.0.state);
        let mut usage = lock(&self.0.usage);
        let bytes = command_bytes(&command);
        let next = s.status.accepted.checked_add(1);
        let error = if !s.status.accepting || s.shutdown || self.0.stop.requested() {
            Some(Error::Closed)
        } else if next.is_none() {
            Some(Error::Sequence)
        } else if bytes
            .and_then(|n| usage.bytes.checked_add(n))
            .is_none_or(|n| n > self.0.config.command_bytes)
        {
            Some(Error::ResourceLimit)
        } else if usage.requests >= self.0.config.requests {
            Some(Error::Busy)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(Box::new(Rejected { error, command }));
        }
        let bytes = bytes.unwrap();
        usage.requests += 1;
        usage.bytes += bytes;
        drop(usage);
        let accepted = Arc::new(Accepted {
            id: next.unwrap(),
            command,
            _credit: Credit {
                usage: self.0.usage.clone(),
                bytes,
            },
        });
        let (reply, result) = watch::channel(None);
        s.status.accepted = accepted.id;
        s.requests.push_back(Request {
            accepted: accepted.clone(),
            reply,
        });
        drop(s);
        self.0.wake.notify_all();
        Ok(Ticket { accepted, result })
    }
    pub fn status(&self) -> Status {
        let s = lock(&self.0.state);
        let mut status = s.status.clone();
        status.accepting &= !self.0.stop.requested();
        status.active = s.active.as_ref().map(|a| a.id);
        status.queued = s.requests.len();
        let u = lock(&self.0.usage);
        status.retained_requests = u.requests;
        status.retained_command_bytes = u.bytes;
        status
    }
    pub fn front(&self) -> Option<Arc<Completion>> {
        lock(&self.0.state).outputs.front().cloned()
    }
    pub fn acknowledge(&self, output: &Arc<Completion>) -> bool {
        let mut s = lock(&self.0.state);
        if !s.outputs.front().is_some_and(|r| Arc::ptr_eq(r, output)) {
            return false;
        }
        s.outputs.pop_front();
        true
    }
    pub fn request_shutdown(&self) {
        let mut s = lock(&self.0.state);
        s.status.accepting = false;
        s.shutdown = true;
        self.0.stop.request();
        drop(s);
        self.0.wake.notify_all();
    }
    pub fn take_dropped_report(&self) -> Option<Report> {
        lock(&self.0.dropped).take()
    }
}
pub struct Report {
    pub status: Status,
    pub outputs: Vec<Arc<Completion>>,
    pub unresolved: Vec<Arc<Accepted>>,
    pub cancelled_packing: Option<packing::CancelReport>,
    pub panic: Option<Box<dyn Any + Send>>,
    retirement: Option<runtime::Retired>,
    pool: pool::Client,
}
pub struct Released {
    pub status: Status,
    pub outputs: Vec<Arc<Completion>>,
    pub cancelled_packing: Option<packing::CancelReport>,
}
impl Report {
    pub fn is_settled(&self) -> bool {
        self.panic.is_none()
            && self.unresolved.is_empty()
            && self.retirement.as_ref().is_some_and(|r| !r.pending_pool())
    }
    pub fn poll(&mut self) -> Result<bool, Error> {
        if let Some(r) = &mut self.retirement
            && let Some(cancelled) = r.poll(&self.pool).map_err(Error::Retirement)?
        {
            self.cancelled_packing = Some(cancelled);
        }
        Ok(self.is_settled())
    }
    pub fn release(self) -> Result<Released, Box<Self>> {
        if !self.is_settled() {
            return Err(Box::new(self));
        }
        Ok(Released {
            status: self.status,
            outputs: self.outputs,
            cancelled_packing: self.cancelled_packing,
        })
    }
}
pub struct StartError<O> {
    pub error: Error,
    pub controller: runtime::Controller<O>,
}
impl<O> std::fmt::Debug for StartError<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> StartError<O> {
    pub fn retire(self) -> Report {
        let pool = self.controller.pool_client();
        Report {
            status: Status {
                joined: true,
                failure: Some(self.error),
                ..Default::default()
            },
            outputs: vec![],
            unresolved: vec![],
            cancelled_packing: None,
            panic: None,
            retirement: Some(self.controller.retire()),
            pool,
        }
    }
}
pub struct Worker {
    client: Client,
    thread: Option<JoinHandle<Report>>,
    pool: pool::Client,
}
impl Worker {
    pub fn spawn<O: MapOrder + Clone + Send + Sync + 'static>(
        controller: runtime::Controller<O>,
        config: Config,
    ) -> Result<Self, Box<StartError<O>>> {
        let error = config.validate().err().or_else(|| {
            if controller.recovery_required() {
                Some(Error::RecoveryRequired)
            } else if controller.stop_handle().requested() {
                Some(Error::Closed)
            } else {
                None
            }
        });
        if let Some(error) = error {
            return Err(Box::new(StartError { error, controller }));
        }
        let pool = controller.pool_client();
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                status: Status {
                    running: true,
                    accepting: true,
                    ..Default::default()
                },
                shutdown: false,
                active: None,
                requests: VecDeque::new(),
                outputs: VecDeque::new(),
            }),
            stop: controller.stop_handle(),
            config,
            usage: Arc::new(Mutex::new(Usage::default())),
            wake: Condvar::new(),
            dropped: Mutex::new(None),
        });
        // Keep the prepared owner recoverable when the OS refuses thread spawn.
        let handoff = Arc::new(Mutex::new(Some(controller)));
        let source = handoff.clone();
        let state = shared.clone();
        let thread = match thread::Builder::new()
            .name("ae-on-demand-commands".into())
            .spawn(move || {
                let controller = lock(&source).take().expect("single command owner");
                run(controller, state)
            }) {
            Ok(t) => t,
            Err(e) => {
                return Err(Box::new(StartError {
                    error: Error::Spawn(e.to_string()),
                    controller: lock(&handoff).take().expect("unstarted command owner"),
                }));
            }
        };
        Ok(Self {
            client: Client(shared),
            thread: Some(thread),
            pool,
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
    }
    fn join(&mut self) -> Report {
        let mut report = match self.thread.take().expect("command owner thread").join() {
            Ok(r) => r,
            Err(panic) => {
                let mut s = lock(&self.client.0.state);
                cancel_queued(&mut s);
                let unresolved = s.active.take().into_iter().collect();
                Report {
                    status: s.status.clone(),
                    outputs: s.outputs.drain(..).collect(),
                    unresolved,
                    cancelled_packing: None,
                    panic: Some(panic),
                    retirement: None,
                    pool: self.pool.clone(),
                }
            }
        };
        report.status.running = false;
        report.status.accepting = false;
        report.status.joined = true;
        report.status.active = None;
        report.status.queued = 0;
        if report.panic.is_some() {
            report.status.failure = Some(Error::Panicked);
        }
        let mut s = lock(&self.client.0.state);
        s.status = report.status.clone();
        let u = lock(&self.client.0.usage);
        report.status.retained_requests = u.requests;
        report.status.retained_command_bytes = u.bytes;
        report
    }
    pub fn shutdown(mut self) -> Report {
        self.client.request_shutdown();
        self.join()
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.client.request_shutdown();
        if self.thread.is_some() {
            let report = self.join();
            *lock(&self.client.0.dropped) = Some(report);
        }
    }
}
fn complete(s: &mut State, request: Request, result: Result<runtime::Report, Error>) {
    if matches!(&result, Err(Error::Cancelled)) {
        s.status.cancelled += 1;
    }
    s.status.completed += 1;
    let completion = Arc::new(Completion {
        accepted: request.accepted,
        result,
    });
    s.outputs.push_back(completion.clone());
    request.reply.send_replace(Some(completion));
}
fn cancel_queued(s: &mut State) {
    while let Some(request) = s.requests.pop_front() {
        complete(s, request, Err(Error::Cancelled));
    }
}
fn run<O: MapOrder + Clone + Send + Sync + 'static>(
    mut controller: runtime::Controller<O>,
    shared: Arc<Shared>,
) -> Report {
    let pool = controller.pool_client();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        loop {
            let request = {
                let mut s = lock(&shared.state);
                while !s.shutdown && s.requests.is_empty() && !shared.stop.requested() {
                    s = shared
                        .wake
                        .wait_timeout(s, shared.config.poll)
                        .unwrap_or_else(|e| e.into_inner())
                        .0;
                }
                if shared.stop.requested() {
                    s.shutdown = true;
                    s.status.accepting = false;
                }
                if s.shutdown {
                    cancel_queued(&mut s);
                    break;
                }
                let request = s.requests.pop_front().unwrap();
                s.active = Some(request.accepted.clone());
                request
            };
            let report = controller.execute(request.accepted.command.clone());
            let failed = report.recovery_required;
            let mut s = lock(&shared.state);
            complete(&mut s, request, Ok(report));
            s.active = None;
            if failed {
                s.status.failure = Some(Error::RecoveryRequired);
                s.status.accepting = false;
                s.shutdown = true;
                shared.stop.request();
            }
        }
    }));
    let mut s = lock(&shared.state);
    s.status.running = false;
    s.status.accepting = false;
    s.shutdown = true;
    shared.stop.request();
    if outcome.is_err() {
        s.status.failure = Some(Error::Panicked);
    }
    cancel_queued(&mut s);
    let mut report = Report {
        status: s.status.clone(),
        outputs: s.outputs.drain(..).collect(),
        unresolved: s.active.take().into_iter().collect(),
        cancelled_packing: None,
        panic: outcome.err(),
        retirement: None,
        pool,
    };
    drop(s);
    report.retirement = Some(controller.retire());
    if let Err(error) = report.poll() {
        report.status.failure.get_or_insert(error);
    }
    report
}
