use super::{Binding, Client, Error as WorkerError, Settings, Worker};
use crate::observe::supervision as core;
use crate::{observe::SinkInfo, runtime::restart::Window};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Prepared,
    Starting,
    Running,
    Retiring,
    Empty,
    Exhausted,
    Stopped,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Closed,
    Busy,
    Stale,
    NoChild,
    Exists,
    ChildLimit,
    ParentLimit,
    Generation,
    Worker(WorkerError),
}
#[derive(Debug, Clone)]
pub struct Status {
    pub phase: Phase,
    pub generation: u64,
    pub supervisor_generation: u64,
    pub child_restart_attempts: u64,
    pub parent_restart_attempts: u64,
    pub replacements: u64,
    pub last_error: Option<WorkerError>,
}
#[derive(Clone)]
pub struct Generation {
    pub number: u64,
    pub client: Client,
}
struct State {
    status: Status,
    child: Option<Generation>,
    retired: SinkInfo,
}
struct Shared {
    pipeline: bool,
    state: Mutex<State>,
    closed: AtomicBool,
    updated: watch::Sender<()>,
    halt: watch::Sender<bool>,
}
impl core::Halt for Shared {
    fn halt(&self) {
        self.closed.store(true, Ordering::Release);
        self.halt.send_replace(true);
        let child = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .child
            .clone();
        if let Some(child) = child {
            child.client.request_shutdown();
        }
        self.updated.send_replace(());
    }
}
pub struct Ticket(oneshot::Receiver<Result<u64, Error>>);
impl Ticket {
    pub async fn wait(self) -> Result<u64, Error> {
        self.0.await.map_err(|_| Error::Closed)?
    }
}
enum Action {
    Restart(u64),
    RestartSupervisor(u64),
    Start(u64),
}
struct Command {
    action: Action,
    reply: oneshot::Sender<Result<u64, Error>>,
}
#[derive(Clone)]
pub struct Handle {
    shared: Arc<Shared>,
    updates: watch::Receiver<()>,
    commands: mpsc::Sender<Command>,
}
impl Handle {
    pub(super) fn is_pipeline(&self) -> bool {
        self.shared.pipeline
    }
    pub fn status(&self) -> Status {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .status
            .clone()
    }
    pub fn current(&self) -> Option<Generation> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .child
            .clone()
    }
    pub fn client(&self) -> Client {
        Client(Binding::Managed(self.clone()), None, None, None, None)
    }
    pub fn shutdown(&self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.halt.send_replace(true);
        if let Some(g) = self.current() {
            g.client.request_shutdown();
        }
    }
    pub async fn changed(&mut self) {
        let _ = self.updates.changed().await;
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
    pub fn info(&self) -> SinkInfo {
        let s = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut info = s.child.as_ref().map_or_else(
            || s.retired.clone(),
            |g| totals(&s.retired, g.client.info()),
        );
        info.running = matches!(
            s.status.phase,
            Phase::Prepared | Phase::Starting | Phase::Running | Phase::Retiring
        );
        info.ready &= s.status.phase == Phase::Running;
        if s.child.is_none() {
            info.ready = false;
            info.connected = false;
        }
        info
    }
    pub(super) fn submit(&self, lines: Vec<Vec<u8>>) -> bool {
        let mut s = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        if !self.shared.closed.load(Ordering::Acquire)
            && matches!(
                s.status.phase,
                Phase::Prepared | Phase::Starting | Phase::Running
            )
            && let Some(child) = &s.child
        {
            return child.client.submit(lines);
        }
        s.retired.dropped = s.retired.dropped.saturating_add(1);
        false
    }
    pub(super) fn report(
        &self,
        event: crate::observe::report::Event,
    ) -> Result<(), crate::observe::pipeline::Error> {
        let s = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        if self.shared.closed.load(Ordering::Acquire)
            || !matches!(
                s.status.phase,
                Phase::Prepared | Phase::Starting | Phase::Running
            )
        {
            return Err(crate::observe::pipeline::Error::Closed);
        }
        s.child
            .as_ref()
            .ok_or(crate::observe::pipeline::Error::Closed)?
            .client
            .report(event)
    }
    fn send(&self, action: Action) -> Result<Ticket, Error> {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let (reply, result) = oneshot::channel();
        self.commands
            .try_send(Command { action, reply })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::Busy,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })?;
        Ok(Ticket(result))
    }
    // Replacements are fenced by the observed child; starting an empty dynamic
    // supervisor is a separate action fenced by that supervisor's incarnation.
    pub fn restart(&self, generation: u64) -> Result<Ticket, Error> {
        self.send(Action::Restart(generation))
    }
    pub fn start(&self, supervisor_generation: u64) -> Result<Ticket, Error> {
        self.send(Action::Start(supervisor_generation))
    }
    pub fn restart_supervisor(&self, supervisor_generation: u64) -> Result<Ticket, Error> {
        self.send(Action::RestartSupervisor(supervisor_generation))
    }
}
fn totals(old: &SinkInfo, mut next: SinkInfo) -> SinkInfo {
    next.open_attempts = next.open_attempts.saturating_add(old.open_attempts);
    next.accepted = next.accepted.saturating_add(old.accepted);
    next.written = next.written.saturating_add(old.written);
    next.skipped = next.skipped.saturating_add(old.skipped);
    next.failed = next.failed.saturating_add(old.failed);
    next.dropped = next.dropped.saturating_add(old.dropped);
    next.panicked |= old.panicked;
    next.last_error = next.last_error.or(old.last_error);
    next
}

// Drives the single aec_metrics_logger dynamic child. The parent boundary is
// exposed separately so an application can own reporter siblings outside it.
// Accepted mailbox data belongs to one child and is never replayed on restart.
pub struct Owner {
    handle: Handle,
    commands: mpsc::Receiver<Command>,
    command: Option<Command>,
    pending: Option<oneshot::Sender<Result<u64, Error>>>,
    halted: watch::Receiver<bool>,
    status: Status,
    settings: Settings,
    worker: Option<Worker>,
    activated: bool,
    registered: bool,
    replacement: bool,
    child_budget: Window<3, 10>,
    parent_budget: Window<5, 10>,
    parent: Option<core::Budget>,
    parent_replacement: bool,
    clock: Box<dyn Fn() -> u64 + Send>,
    factory: Box<dyn FnMut(Settings) -> Result<Worker, WorkerError> + Send>,
}
impl Owner {
    pub fn prepare_with_parent(
        settings: Settings,
        parent: core::Budget,
    ) -> Result<Self, WorkerError> {
        let mut owner = Self::prepare(settings)?;
        parent
            .bind(core::Group::Logger, owner.handle.shared.clone())
            .map_err(|e| match e {
                core::BindError::Exists => WorkerError::Configuration,
                core::BindError::Exhausted => WorkerError::RestartLimit,
            })?;
        owner.parent = Some(parent);
        Ok(owner)
    }
    pub fn prepare(settings: Settings) -> Result<Self, WorkerError> {
        settings.validate()?;
        let origin = Instant::now();
        Self::configured(
            settings,
            Box::new(move || origin.elapsed().as_secs()),
            Box::new(Worker::prepare),
        )
    }
    fn configured(
        settings: Settings,
        clock: Box<dyn Fn() -> u64 + Send>,
        factory: Box<dyn FnMut(Settings) -> Result<Worker, WorkerError> + Send>,
    ) -> Result<Self, WorkerError> {
        settings.validate()?;
        let status = Status {
            phase: Phase::Prepared,
            generation: 0,
            supervisor_generation: 1,
            child_restart_attempts: 0,
            parent_restart_attempts: 0,
            replacements: 0,
            last_error: None,
        };
        let (halt, halted) = watch::channel(false);
        let (updated, updates) = watch::channel(());
        let (tx, commands) = mpsc::channel(1);
        let shared = Arc::new(Shared {
            pipeline: settings.pipeline.is_some(),
            state: Mutex::new(State {
                status: status.clone(),
                child: None,
                retired: SinkInfo {
                    joined: true,
                    ..Default::default()
                },
            }),
            closed: AtomicBool::new(false),
            updated,
            halt,
        });
        let mut owner = Self {
            handle: Handle {
                shared,
                updates,
                commands: tx,
            },
            commands,
            command: None,
            pending: None,
            halted,
            status,
            settings,
            worker: None,
            activated: false,
            registered: false,
            replacement: false,
            child_budget: Window::default(),
            parent_budget: Window::default(),
            parent: None,
            parent_replacement: false,
            clock,
            factory,
        };
        if let Err(error) = owner.construct() {
            owner.initial_failure(error);
        }
        owner.publish();
        Ok(owner)
    }
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
    pub fn client(&self) -> Client {
        self.handle.client()
    }
    pub fn activate(&mut self) {
        self.activated = true;
        if !self.closed()
            && let Some(worker) = &self.worker
        {
            worker.activate();
        }
    }
    fn closed(&self) -> bool {
        self.handle.shared.closed.load(Ordering::Acquire)
    }
    fn publish(&self) {
        self.handle
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .status = self.status.clone();
        self.handle.shared.updated.send_replace(());
    }
    fn construct(&mut self) -> Result<(), WorkerError> {
        if self.closed() {
            return Err(WorkerError::Closed);
        }
        self.status.generation = self
            .status
            .generation
            .checked_add(1)
            .ok_or(WorkerError::Sequence)?;
        let worker = catch_unwind(AssertUnwindSafe(|| (self.factory)(self.settings.clone())))
            .unwrap_or_else(|_| Err(WorkerError::Io(std::io::ErrorKind::Other)))?;
        self.handle
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .child = Some(Generation {
            number: self.status.generation,
            client: worker.client(),
        });
        if self.activated {
            worker.activate();
        }
        self.worker = Some(worker);
        self.status.phase = if self.activated {
            Phase::Starting
        } else {
            Phase::Prepared
        };
        Ok(())
    }
    fn reply(&mut self, result: Result<u64, Error>) {
        if let Some(reply) = self.pending.take() {
            // A completed receipt exposes the corresponding incarnation and
            // readiness even when its caller runs on another executor.
            self.publish();
            let _ = reply.send(result);
        }
    }
    fn initial_failure(&mut self, error: WorkerError) {
        self.status.phase = Phase::Empty;
        self.status.last_error = Some(error.clone());
        self.reply(Err(Error::Worker(error)));
    }
    async fn retire(&mut self, drain: bool) {
        if self.worker.is_some() {
            self.status.phase = Phase::Retiring;
            self.publish();
            let worker = self.worker.as_mut().expect("owned retiring child");
            worker.0.stop(drain);
            worker.finish().await;
            // Keep ownership across the suspension point. A caller may drop
            // this wait and later resume retirement or shut the owner down.
            let worker = self.worker.take().expect("joined retiring child");
            let info = worker.client().info();
            let mut s = self
                .handle
                .shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            s.retired = totals(&s.retired, info);
            s.retired.ready = false;
            s.child = None;
        }
    }
    fn replace(&mut self) {
        loop {
            if self.closed() {
                return;
            }
            self.status.child_restart_attempts =
                self.status.child_restart_attempts.saturating_add(1);
            if !self.child_budget.record((self.clock)()) {
                self.status.last_error = Some(WorkerError::RestartLimit);
                self.replace_parent(true);
                return;
            }
            self.replacement = true;
            match self.construct() {
                Ok(()) => return,
                Err(error) => {
                    self.status.last_error = Some(error.clone());
                    if error == WorkerError::Sequence {
                        self.status.phase = Phase::Exhausted;
                        self.reply(Err(Error::Generation));
                        return;
                    }
                }
            }
        }
    }
    fn replace_parent(&mut self, child_exhausted: bool) {
        self.parent_replacement = false;
        if self.closed() {
            self.reply(Err(Error::Closed));
            return;
        }
        self.status.parent_restart_attempts = self.status.parent_restart_attempts.saturating_add(1);
        self.registered = false;
        self.replacement = false;
        let allowed = match &self.parent {
            Some(parent) => parent.record(core::Child::LoggerSupervisor),
            None => self.parent_budget.record((self.clock)()),
        };
        if allowed {
            if let Some(next) = self.status.supervisor_generation.checked_add(1) {
                self.status.supervisor_generation = next;
                self.child_budget = Window::default();
                self.status.phase = Phase::Empty;
                self.reply(if child_exhausted {
                    Err(Error::ChildLimit)
                } else {
                    Ok(next)
                });
            } else {
                self.status.phase = Phase::Exhausted;
                self.reply(Err(Error::Generation));
            }
        } else {
            self.status.last_error = Some(WorkerError::RestartLimit);
            self.status.phase = Phase::Exhausted;
            self.reply(Err(Error::ParentLimit));
        }
    }
    pub async fn poll(&mut self) {
        if self.closed() {
            self.finish().await;
            return;
        }
        if self.status.phase == Phase::Retiring {
            self.retire(false).await;
            if self.closed() {
                self.finish().await;
            } else if self.parent_replacement {
                self.replace_parent(false);
            } else if self.registered {
                self.replace();
            } else {
                self.initial_failure(
                    self.status
                        .last_error
                        .clone()
                        .unwrap_or(WorkerError::Closed),
                );
            }
            self.publish();
            return;
        }
        if let Some(worker) = &self.worker {
            let client = worker.client();
            let info = client.info();
            if info.ready {
                self.registered = true;
                if self.replacement {
                    self.status.replacements = self.status.replacements.saturating_add(1);
                    self.replacement = false;
                }
                self.status.phase = Phase::Running;
                self.reply(Ok(self.status.generation));
            }
            if !info.running {
                if info.panicked || info.last_error.is_some() {
                    self.status.last_error = Some(
                        client
                            .input()
                            .and_then(|input| input.info().last_error)
                            .map(WorkerError::Pipeline)
                            .unwrap_or_else(|| {
                                WorkerError::Io(
                                    info.last_error.unwrap_or(std::io::ErrorKind::Other),
                                )
                            }),
                    );
                }
                self.retire(false).await;
                if self.closed() {
                    self.finish().await;
                    return;
                }
                if self.registered {
                    self.replace();
                } else {
                    self.initial_failure(
                        self.status
                            .last_error
                            .clone()
                            .unwrap_or(WorkerError::Closed),
                    );
                }
            }
        }
        if self.pending.is_none() {
            let command = self
                .command
                .take()
                .or_else(|| self.commands.try_recv().ok());
            if let Some(Command { action, reply }) = command {
                let error = if self.status.phase == Phase::Exhausted {
                    Some(Error::ParentLimit)
                } else {
                    match action {
                        Action::Restart(generation) if generation != self.status.generation => {
                            Some(Error::Stale)
                        }
                        Action::Restart(_) if self.status.phase != Phase::Running => {
                            Some(Error::NoChild)
                        }
                        Action::Start(generation)
                            if generation != self.status.supervisor_generation =>
                        {
                            Some(Error::Stale)
                        }
                        Action::Start(_) if self.status.phase != Phase::Empty => {
                            Some(Error::Exists)
                        }
                        Action::RestartSupervisor(generation)
                            if generation != self.status.supervisor_generation =>
                        {
                            Some(Error::Stale)
                        }
                        _ => None,
                    }
                };
                if let Some(error) = error {
                    let _ = reply.send(Err(error));
                } else {
                    self.pending = Some(reply);
                    match action {
                        Action::Restart(_) => {
                            self.retire(false).await;
                            self.replace();
                        }
                        Action::Start(_) => {
                            self.registered = false;
                            self.replacement = false;
                            if let Err(error) = self.construct() {
                                self.initial_failure(error);
                            }
                        }
                        Action::RestartSupervisor(_) => {
                            self.parent_replacement = true;
                            self.retire(false).await;
                            self.replace_parent(false);
                        }
                    }
                }
            }
        }
        self.publish();
    }
    pub async fn changed(&mut self) {
        if self.status.phase == Phase::Stopped {
            std::future::pending::<()>().await;
            return;
        }
        tokio::select! {
            _ = self.halted.changed() => (),
            _ = async { self.worker.as_mut().expect("active child").0.changed().await }, if self.worker.is_some() => (),
            command = self.commands.recv(), if self.pending.is_none() && self.command.is_none() => { self.command = command; },
        }
    }
    pub async fn finish(&mut self) {
        if self.status.phase == Phase::Stopped {
            return;
        }
        self.handle.shutdown();
        if self.parent.as_ref().is_some_and(|p| p.info().exhausted) {
            self.status.last_error = Some(WorkerError::RestartLimit);
        }
        self.retire(true).await;
        self.status.phase = Phase::Stopped;
        self.reply(Err(Error::Closed));
        if let Some(command) = self.command.take() {
            let _ = command.reply.send(Err(Error::Closed));
        }
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            let _ = command.reply.send(Err(Error::Closed));
        }
        self.publish();
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.handle.shutdown();
        if let Some(mut worker) = self.worker.take() {
            worker.0.abort();
            let mut s = self
                .handle
                .shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            s.retired = totals(&s.retired, worker.client().info());
            s.retired.ready = false;
            s.child = None;
        }
        self.status.phase = Phase::Stopped;
        self.publish();
    }
}

#[cfg(test)]
mod tests;
