use super::{Config, Error, Histogram, Part, Points, Value};
use crate::observe::reporter::{self, Clock};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll},
    thread,
};
use tokio::sync::{oneshot, watch};
mod policy;
pub use policy::{Action, Policy, Rule, Threshold};

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub workers: usize,
    pub history: usize,
    pub requests: usize,
    pub pending_bytes: usize,
    pub retained_bytes: usize,
    pub values: reporter::Limits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            workers: 32,
            history: 4096,
            requests: 128,
            pending_bytes: 1 << 20,
            retained_bytes: 16 << 20,
            values: reporter::Limits::default(),
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub workers: usize,
    pub pending: usize,
    pub pending_bytes: usize,
    pub retained_bytes: usize,
}
struct Quota {
    limits: Limits,
    usage: Mutex<Usage>,
}
#[derive(Clone)]
pub struct Budget(Arc<Quota>);
impl Budget {
    pub fn new(limits: Limits) -> Result<Self, Error> {
        limits.values.validate().map_err(|_| Error::Limit)?;
        if limits.workers == 0
            || limits.workers > 1024
            || limits.history == 0
            || limits.history > 65_536
            || limits.requests == 0
            || limits.requests > 65_536
            || limits.pending_bytes == 0
            || limits.pending_bytes > 1 << 30
            || limits.retained_bytes == 0
            || limits.retained_bytes > 1 << 30
        {
            return Err(Error::Limit);
        }
        Ok(Self(Arc::new(Quota {
            limits,
            usage: Mutex::new(Usage::default()),
        })))
    }
    pub fn usage(&self) -> Usage {
        *self.0.usage.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn admit(&self, bytes: usize) -> Result<Arc<Credit>, Error> {
        let mut u = self.0.usage.lock().unwrap_or_else(|e| e.into_inner());
        if u.pending >= self.0.limits.requests
            || bytes > self.0.limits.pending_bytes.saturating_sub(u.pending_bytes)
        {
            return Err(Error::Busy);
        }
        u.pending += 1;
        u.pending_bytes += bytes;
        Ok(Arc::new(Credit {
            budget: self.clone(),
            bytes,
        }))
    }
    fn worker(&self) -> Result<Permit, Error> {
        let mut u = self.0.usage.lock().unwrap_or_else(|e| e.into_inner());
        if u.workers >= self.0.limits.workers {
            return Err(Error::Limit);
        }
        u.workers += 1;
        Ok(Permit(self.clone()))
    }
    fn values(&self) -> reporter::Limits {
        reporter::Limits {
            retained_bytes: self.0.limits.retained_bytes,
            ..self.0.limits.values
        }
    }
}
struct Permit(Budget);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0
            .0
            .usage
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .workers -= 1;
    }
}
struct Credit {
    budget: Budget,
    bytes: usize,
}
impl Drop for Credit {
    fn drop(&mut self) {
        let mut u = self
            .budget
            .0
            .usage
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        u.pending -= 1;
        u.pending_bytes -= self.bytes;
    }
}
struct Charge {
    budget: Budget,
    bytes: usize,
}
impl Charge {
    fn resize(&mut self, bytes: usize) -> Result<(), Error> {
        let mut u = self
            .budget
            .0
            .usage
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let next = u.retained_bytes - self.bytes;
        if bytes > self.budget.0.limits.retained_bytes.saturating_sub(next) {
            return Err(Error::Limit);
        }
        u.retained_bytes = next + bytes;
        self.bytes = bytes;
        Ok(())
    }
}
impl Drop for Charge {
    fn drop(&mut self) {
        self.budget
            .0
            .usage
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retained_bytes -= self.bytes;
    }
}
pub struct Ticket<T> {
    receiver: oneshot::Receiver<Result<T, Error>>,
    _credit: Arc<Credit>,
}
impl<T> Future for Ticket<T> {
    type Output = Result<T, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(cx)
            .map(|r| r.unwrap_or(Err(Error::Closed)))
    }
}
impl<T> Ticket<T> {
    pub fn blocking_wait(self) -> Result<T, Error> {
        self.receiver.blocking_recv().unwrap_or(Err(Error::Closed))
    }
}
#[derive(Debug, Clone, Default)]
pub struct Info {
    pub generation: u64,
    pub running: bool,
    pub recovering: bool,
    pub closing: bool,
    pub joined: bool,
    pub previous_joined: bool,
    pub failures: u64,
    pub completed: u64,
    pub last_error: Option<Error>,
    pub terminal_action: Option<Action>,
}
type Completion = Box<dyn FnOnce() + Send>;
type Work = Box<dyn FnOnce(&mut Histogram, &mut Charge, i128) -> (Completion, bool) + Send>;
struct Task {
    work: Work,
    _credit: Arc<Credit>,
}
struct Queue {
    tasks: VecDeque<Task>,
    info: Info,
}
struct Shared {
    queue: Mutex<Queue>,
    wake: Condvar,
    changed: watch::Sender<()>,
    done: watch::Sender<bool>,
    budget: Budget,
    clock: Arc<dyn Clock>,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn now(&self) -> i128 {
        self.clock.now().unix_microseconds.div_euclid(1000) - 1_258_000_000_000
    }
    fn shutdown(&self) {
        self.lock().info.closing = true;
        self.wake.notify_all();
        self.changed.send_replace(());
    }
}
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
    updates: watch::Receiver<()>,
}
impl Client {
    pub fn info(&self) -> Info {
        self.shared.lock().info.clone()
    }
    pub async fn changed(&mut self) {
        let _ = self.updates.changed().await;
    }
    pub fn shutdown(&self) {
        self.shared.shutdown();
    }
    fn call<T: Send + 'static>(
        &self,
        bytes: usize,
        action: impl FnOnce(&mut Histogram, &mut Charge, i128) -> Result<T, Error> + Send + 'static,
    ) -> Result<Ticket<T>, Error> {
        let mut q = self.shared.lock();
        if q.info.closing {
            return Err(Error::Closed);
        }
        if !q.info.running {
            return Err(Error::Busy);
        }
        let credit = self.shared.budget.admit(bytes)?;
        let (reply, receiver) = oneshot::channel();
        q.tasks.push_back(Task {
            _credit: credit.clone(),
            work: Box::new(move |h, charge, now| {
                let result = action(h, charge, now);
                let fatal = matches!(result, Err(Error::Arithmetic));
                (
                    Box::new(move || {
                        let _ = reply.send(result);
                    }),
                    fatal,
                )
            }),
        });
        self.shared.wake.notify_one();
        Ok(Ticket {
            receiver,
            _credit: credit,
        })
    }
    pub fn update(&self, value: &Value) -> Result<Ticket<()>, Error> {
        let bytes =
            128usize.saturating_add(value.bytes(self.shared.budget.0.limits.values.report)?);
        let value = value.clone();
        self.call(bytes, move |h, charge, now| {
            h.update_with(now, &value, |_, next| charge.resize(next))
        })
    }
    pub fn reset(&self) -> Result<Ticket<()>, Error> {
        self.call(128, |h, charge, _| {
            h.reset();
            charge.resize(h.retained_bytes())
        })
    }
    pub fn values(&self, points: &Points) -> Result<Ticket<Vec<(Part, Value)>>, Error> {
        let limits = self.shared.budget.0.limits.values;
        limits.points(points).map_err(|_| Error::Limit)?;
        let defaults;
        let requested = if *points == Points::default_points() {
            defaults = super::datapoints();
            &defaults[..]
        } else {
            match points {
                Points::One(p) => std::slice::from_ref(p),
                Points::Many(v) => v,
            }
        };
        // Reserve the full possible represented response before admission.
        // Credits remain held by an unread receipt after the worker finishes.
        let bytes = requested.iter().try_fold(128usize, |sum, p| {
            let part = limits.report.part(p).map_err(|_| Error::Limit)?;
            Ok(sum
                .saturating_add(part.saturating_mul(2))
                .saturating_add(64)
                .saturating_add(limits.report.integer_bits.div_ceil(8) as usize))
        })?;
        let points = points.clone();
        self.call(bytes, move |h, _, now| h.values(now, &points))
    }
}
pub struct Owner {
    client: Client,
    thread: Option<thread::JoinHandle<()>>,
}
impl Owner {
    pub fn spawn(
        config: Config,
        policy: Policy,
        budget: Budget,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        config.validate(budget.values())?;
        policy.validate(budget.0.limits.history)?;
        let policy = policy.clone();
        let permit = budget.worker()?;
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                tasks: VecDeque::new(),
                info: Info::default(),
            }),
            wake: Condvar::new(),
            changed: watch::channel(()).0,
            done: watch::channel(false).0,
            budget,
            clock,
        });
        let client = Client {
            updates: shared.changed.subscribe(),
            shared: shared.clone(),
        };
        let (ready, initialized) = std::sync::mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("ae-metric-probe-owner".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    supervise(&shared, config, policy, ready)
                }))
                .unwrap_or(Err(Error::Panicked));
                let discarded = {
                    let mut q = shared.lock();
                    q.info.running = false;
                    q.info.recovering = false;
                    q.info.closing = true;
                    if let Err(e) = result {
                        q.info.last_error = Some(e);
                    }
                    std::mem::take(&mut q.tasks)
                };
                drop(discarded);
                drop(permit);
                shared.done.send_replace(true);
                shared.changed.send_replace(());
            })
            .map_err(|_| Error::Spawn)?;
        let mut owner = Self {
            client,
            thread: Some(thread),
        };
        match initialized.recv() {
            Ok(Ok(())) => Ok(owner),
            other => {
                owner.join();
                Err(other.ok().and_then(Result::err).unwrap_or(Error::Closed))
            }
        }
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }
    pub async fn finish(&mut self) {
        self.client.shutdown();
        let mut done = self.client.shared.done.subscribe();
        while !*done.borrow_and_update() {
            if done.changed().await.is_err() {
                break;
            }
        }
        self.join();
    }
    pub fn join(&mut self) {
        self.client.shutdown();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
            self.client.shared.lock().info.joined = true;
            self.client.shared.changed.send_replace(());
        }
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.join();
    }
}

struct Worker {
    shared: Arc<Shared>,
    thread: Option<thread::JoinHandle<Result<(), Error>>>,
}
impl Worker {
    fn join(&mut self) -> Result<(), Error> {
        self.thread
            .take()
            .expect("worker joined once")
            .join()
            .unwrap_or(Err(Error::Panicked))
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            self.shared.shutdown();
            let _ = thread.join();
        }
    }
}
fn supervise(
    shared: &Arc<Shared>,
    config: Config,
    policy: Policy,
    ready: std::sync::mpsc::SyncSender<Result<(), Error>>,
) -> Result<(), Error> {
    let mut ready = Some(ready);
    let mut history = policy::History::new(policy, shared.budget.0.limits.history);
    loop {
        let mut q = shared.lock();
        if q.info.closing {
            return Ok(());
        }
        q.info.generation = q.info.generation.checked_add(1).ok_or(Error::Sequence)?;
        q.info.running = true;
        q.info.recovering = false;
        drop(q);
        let s = shared.clone();
        let thread = match thread::Builder::new()
            .name("ae-metric-histogram".into())
            .spawn(move || work(&s, config))
        {
            Ok(thread) => thread,
            Err(_) => {
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(Error::Spawn));
                }
                return Err(Error::Spawn);
            }
        };
        let mut worker = Worker {
            shared: shared.clone(),
            thread: Some(thread),
        };
        if let Some(ready) = ready.take() {
            let _ = ready.send(Ok(()));
        }
        shared.changed.send_replace(());
        let result = worker.join();
        let discarded = {
            let mut q = shared.lock();
            q.info.running = false;
            q.info.previous_joined = true;
            q.info.recovering = result.is_err() && !q.info.closing;
            if let Err(e) = &result {
                q.info.last_error = Some(e.clone());
            }
            std::mem::take(&mut q.tasks)
        };
        drop(discarded);
        let mut q = shared.lock();
        if q.info.closing || result.is_ok() {
            return result;
        }
        q.info.failures = q.info.failures.checked_add(1).ok_or(Error::Sequence)?;
        drop(q);
        let action = history.failure(shared.now())?;
        if action != Action::Restart {
            shared.lock().info.terminal_action = Some(action);
            return Ok(());
        }
    }
}
fn work(shared: &Shared, config: Config) -> Result<(), Error> {
    let mut charge = Charge {
        budget: shared.budget.clone(),
        bytes: 0,
    };
    // Drop the retained histogram before returning its memory reservation,
    // including during unwinding from an actual worker panic.
    let mut histogram = Histogram::new(config, shared.budget.values(), shared.now())?;
    loop {
        let task = {
            let mut q = shared.lock();
            loop {
                if let Some(task) = q.tasks.pop_front() {
                    break task;
                }
                if q.info.closing {
                    return Ok(());
                }
                q = shared.wake.wait(q).unwrap_or_else(|e| e.into_inner());
            }
        };
        let (complete, fatal) = (task.work)(&mut histogram, &mut charge, shared.now());
        {
            let mut q = shared.lock();
            q.info.completed = q.info.completed.checked_add(1).ok_or(Error::Sequence)?;
            if fatal {
                q.info.running = false;
                q.info.recovering = true;
            }
        }
        complete();
        shared.changed.send_replace(());
        if fatal {
            return Err(Error::Arithmetic);
        }
    }
}

#[cfg(test)]
mod tests;
