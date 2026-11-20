use super::{Delivery, DeliveryId, Recovery, Service};
use ae_p2p::registry::{Command, Counts, Outcome};
use std::{
    collections::VecDeque,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Condvar, Mutex, Weak},
    task::{Context, Poll},
    thread::{self, JoinHandle},
};
use tokio::sync::oneshot;

#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub requests: usize,
    pub bytes: usize,
    pub single_request_bytes: usize,
    pub maximum_sample: usize,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            requests: 1024,
            bytes: 32 << 20,
            single_request_bytes: 4 << 20,
            maximum_sample: 1024,
        }
    }
}
#[derive(Debug)]
pub enum Error {
    Service(super::Error),
    Configuration,
    Saturated,
    RequestTooLarge,
    Stopped,
    Panicked,
    Spawn(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inspection {
    pub policy_revision: u64,
    pub storage_revision: u64,
    pub counts: Counts,
    pub effects: usize,
    pub effect_bytes: usize,
    pub recovery_required: bool,
    pub closed: bool,
}
struct State {
    queue: VecDeque<Box<dyn Work>>,
    stopped: bool,
    outstanding: usize,
    bytes: usize,
}
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    settings: Settings,
    host_bytes: usize,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn stop(&self) {
        self.lock().stopped = true;
        self.wake.notify_all();
    }
}
struct Permit {
    shared: Weak<Shared>,
    bytes: usize,
}
impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            let mut s = shared.lock();
            s.outstanding -= 1;
            s.bytes -= self.bytes;
        }
    }
}
struct Response<T> {
    result: Result<T, Error>,
    _permit: Permit,
}
// Dropping an observation does not cancel an accepted mutation. Outstanding
// quotas include execution and unread responses, not just the waiting queue.
// A lost reply is not permission to replay the mutation blindly.
pub struct Ticket<T> {
    receiver: oneshot::Receiver<Response<T>>,
}
impl<T> Future for Ticket<T> {
    type Output = Result<T, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(reply)) => Poll::Ready(reply.result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Stopped)),
        }
    }
}
trait Work: Send {
    fn run(self: Box<Self>, service: &mut Service) -> bool;
    fn reject(self: Box<Self>);
}
struct Job<T, F> {
    action: F,
    reply: oneshot::Sender<Response<T>>,
    permit: Permit,
}
impl<T: Send + 'static, F: FnOnce(&mut Service) -> Result<T, super::Error> + Send + 'static> Work
    for Job<T, F>
{
    fn run(self: Box<Self>, service: &mut Service) -> bool {
        let Self {
            action,
            reply,
            permit,
        } = *self;
        let result = catch_unwind(AssertUnwindSafe(|| action(service)));
        let panicked = result.is_err();
        let result = result
            .map_err(|_| Error::Panicked)
            .and_then(|r| r.map_err(Error::Service));
        let _ = reply.send(Response {
            result,
            _permit: permit,
        });
        !panicked
    }
    fn reject(self: Box<Self>) {
        let _ = self.reply.send(Response {
            result: Err(Error::Stopped),
            _permit: self.permit,
        });
    }
}
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
}
pub struct Worker {
    shared: Arc<Shared>,
    join: Option<JoinHandle<Service>>,
}
impl Worker {
    pub fn start(mut service: Service, settings: Settings) -> Result<Self, Error> {
        if settings.requests == 0
            || settings.requests > 1_000_000
            || settings.bytes == 0
            || settings.bytes > 1 << 30
            || settings.single_request_bytes == 0
            || settings.single_request_bytes > settings.bytes
            || settings.maximum_sample == 0
            || settings.maximum_sample > 1_000_000
        {
            return Err(Error::Configuration);
        }
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                stopped: false,
                outstanding: 0,
                bytes: 0,
            }),
            wake: Condvar::new(),
            settings,
            host_bytes: service.core.host_bytes,
        });
        let owner = shared.clone();
        let join = thread::Builder::new()
            .name("ae-peer-registry".into())
            .spawn(move || {
                loop {
                    let mut state = owner.lock();
                    while !state.stopped && state.queue.is_empty() {
                        state = owner.wake.wait(state).unwrap_or_else(|e| e.into_inner());
                    }
                    if state.stopped {
                        let waiting = std::mem::take(&mut state.queue);
                        drop(state);
                        for job in waiting {
                            job.reject();
                        }
                        break;
                    }
                    let job = state.queue.pop_front().unwrap();
                    drop(state);
                    if !job.run(&mut service) {
                        service.core.failed = true;
                        owner.stop();
                    }
                }
                service
            })
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
    // Explicit stop rejects waiting work, joins the executing operation, then
    // returns ownership so a supervisor can drain effects/retire resources.
    pub fn shutdown(mut self) -> Result<Service, Error> {
        self.shared.stop();
        self.join
            .take()
            .unwrap()
            .join()
            .map_err(|_| Error::Panicked)
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.shared.stop();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}
impl Client {
    fn submit<
        T: Send + 'static,
        F: FnOnce(&mut Service) -> Result<T, super::Error> + Send + 'static,
    >(
        &self,
        bytes: usize,
        action: F,
    ) -> Result<Ticket<T>, Error> {
        let mut state = self.shared.lock();
        let settings = self.shared.settings;
        if state.stopped {
            return Err(Error::Stopped);
        }
        if bytes > settings.single_request_bytes {
            return Err(Error::RequestTooLarge);
        }
        if state.outstanding >= settings.requests || bytes > settings.bytes - state.bytes {
            return Err(Error::Saturated);
        }
        state.outstanding += 1;
        state.bytes += bytes;
        let (reply, receiver) = oneshot::channel();
        state.queue.push_back(Box::new(Job {
            action,
            reply,
            permit: Permit {
                shared: Arc::downgrade(&self.shared),
                bytes,
            },
        }));
        self.shared.wake.notify_one();
        Ok(Ticket { receiver })
    }
    pub fn apply(&self, now: u64, command: Command) -> Result<Ticket<Outcome>, Error> {
        let extra = match &command {
            Command::Add(p) => p.host.len(),
            Command::Block(p)
            | Command::Resolve { peer: p, .. }
            | Command::Accept { peer: p, .. } => p.host.len(),
            Command::Sample {
                maximum, exclude, ..
            } => self
                .sample_bytes(*maximum)?
                .checked_add(
                    exclude
                        .len()
                        .checked_mul(32)
                        .ok_or(Error::RequestTooLarge)?,
                )
                .ok_or(Error::RequestTooLarge)?,
            Command::SampleConnected { maximum, .. } => self.sample_bytes(*maximum)?,
            _ => 0,
        };
        let bytes = 256usize.checked_add(extra).ok_or(Error::RequestTooLarge)?;
        self.submit(bytes, move |s| s.apply(now, command))
    }
    fn sample_bytes(&self, maximum: usize) -> Result<usize, Error> {
        if maximum > self.shared.settings.maximum_sample {
            return Err(Error::RequestTooLarge);
        }
        maximum
            .checked_mul(128 + self.shared.host_bytes)
            .ok_or(Error::RequestTooLarge)
    }
    pub fn configure_startup(
        &self,
        now: u64,
        configuration: super::startup::Configuration,
        settings: super::startup::Settings,
    ) -> Result<Ticket<super::startup::Progress>, Error> {
        let mut bytes = 256usize;
        for uri in configuration
            .peers
            .iter()
            .chain(&configuration.blocked_peers)
            .chain(configuration.defaults.iter().flatten())
        {
            bytes = bytes
                .checked_add(64)
                .and_then(|n| n.checked_add(uri.len()))
                .ok_or(Error::RequestTooLarge)?;
        }
        self.submit(bytes, move |s| {
            s.configure_startup(now, configuration, settings)
        })
    }
    pub fn advance_startup(
        &self,
        now: u64,
        maximum: usize,
    ) -> Result<Ticket<super::startup::Progress>, Error> {
        self.submit(256, move |s| s.advance_startup(now, maximum))
    }
    pub fn startup_status(&self) -> Result<Ticket<Option<super::startup::Progress>>, Error> {
        self.submit(256, |s| Ok(s.startup_status()))
    }
    pub fn startup_diagnostics(
        &self,
        offset: usize,
        maximum: usize,
    ) -> Result<Ticket<Vec<super::startup::Diagnostic>>, Error> {
        if maximum > self.shared.settings.maximum_sample {
            return Err(Error::RequestTooLarge);
        }
        let bytes = maximum
            .checked_mul(128)
            .and_then(|n| n.checked_add(256))
            .ok_or(Error::RequestTooLarge)?;
        self.submit(bytes, move |s| s.startup_diagnostics(offset, maximum))
    }
    pub fn recover(&self) -> Result<Ticket<Recovery>, Error> {
        self.submit(256, Service::recover)
    }
    pub fn next_effect(&self) -> Result<Ticket<Option<Delivery>>, Error> {
        self.submit(256 + self.shared.host_bytes, |s| {
            Ok(s.next_effect()?.cloned())
        })
    }
    pub fn next_timer(&self) -> Result<Ticket<Option<ae_p2p::registry::Timer>>, Error> {
        self.submit(256, |s| {
            s.core.ready()?;
            Ok(s.policy().timers().min_by_key(|t| t.deadline))
        })
    }
    pub fn acknowledge(&self, id: DeliveryId) -> Result<Ticket<bool>, Error> {
        self.submit(256, move |s| Ok(s.acknowledge(id)))
    }
    pub fn inspect(&self) -> Result<Ticket<Inspection>, Error> {
        self.submit(256, |s| {
            let (effects, effect_bytes) = s.queued_effects();
            Ok(Inspection {
                policy_revision: s.policy().revision(),
                storage_revision: s.version().revision(),
                counts: s.policy().counts(),
                effects,
                effect_bytes,
                recovery_required: s.recovery_required(),
                closed: s.policy().is_closed(),
            })
        })
    }
    pub fn outstanding(&self) -> (usize, usize) {
        let s = self.shared.lock();
        (s.outstanding, s.bytes)
    }
}
#[cfg(test)]
mod tests;
