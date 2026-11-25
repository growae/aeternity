use super::*;
use std::{
    sync::{Condvar, Mutex, MutexGuard, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
mod driver;
mod receipt;
pub mod supervision;
#[cfg(test)]
mod tests;
pub use driver::{Driver, DriverReport};
use receipt::{Job, Slot};
pub use receipt::{Observed, Reply, RequestError, Ticket, Usage};
fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(|e| e.into_inner())
}

pub trait Clock: Send + 'static {
    fn now(&self) -> Result<(u64, Instant), Error>;
}
impl<T: Clock + ?Sized> Clock for Box<T> {
    fn now(&self) -> Result<(u64, Instant), Error> {
        (**self).now()
    }
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> Result<(u64, Instant), Error> {
        Ok((
            u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| Error::Time)?
                    .as_millis(),
            )
            .map_err(|_| Error::Time)?,
            Instant::now(),
        ))
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub poll: Duration,
    pub outputs: usize,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            poll: Duration::from_millis(10),
            outputs: 16,
        }
    }
}
impl Settings {
    fn valid(self) -> bool {
        !self.poll.is_zero()
            && self.poll <= Duration::from_secs(1)
            && self.outputs > 0
            && self.outputs <= 65_536
    }
}
#[derive(Clone, Copy)]
enum Command {
    Start,
    Stop,
    Restart,
    Advance(u64, Instant),
}
impl Command {
    fn is_drive(self) -> bool {
        matches!(self, Self::Advance(..))
    }
}
struct Control {
    stop: bool,
    ready: bool,
    quiescing: bool,
    quiesced: bool,
    commands: VecDeque<Job>,
    sequence: u64,
    submitted: u64,
    applied: u64,
    rejected: u64,
    status: Status,
    joined: bool,
    driver_lease: bool,
    driver_generation: u64,
    driver_exited: bool,
    driver_failure: Option<Error>,
    driver_work: Option<Ticket>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Controls {
    pub submitted: u64,
    pub applied: u64,
    pub rejected: u64,
    pub pending: usize,
}
struct Shared {
    control: Mutex<Control>,
    wake: Condvar,
    capacity: Arc<Mutex<Usage>>,
}
impl Shared {
    fn new(status: Status) -> Arc<Self> {
        Arc::new(Self {
            control: Mutex::new(Control {
                stop: false,
                ready: false,
                quiescing: false,
                quiesced: false,
                commands: VecDeque::new(),
                sequence: 0,
                submitted: 0,
                applied: 0,
                rejected: 0,
                status,
                joined: false,
                driver_lease: false,
                driver_generation: 0,
                driver_exited: false,
                driver_failure: None,
                driver_work: None,
            }),
            wake: Condvar::new(),
            capacity: Arc::new(Mutex::new(Usage::default())),
        })
    }
}
#[derive(Clone)]
pub struct Handle(Arc<Shared>);
impl Handle {
    pub fn status(&self) -> Status {
        let c = lock(&self.0.control);
        let mut status = c.status.clone();
        if status.failure.is_none() {
            status.failure = c.driver_failure.clone();
        }
        status
    }
    // Independent service health does not turn a driver unwind into a service
    // failure or silently clear a genuine service failure on replacement.
    pub fn service_status(&self) -> Status {
        lock(&self.0.control).status.clone()
    }
    pub fn joined(&self) -> bool {
        lock(&self.0.control).joined
    }
    pub fn session(&self) -> SessionClient {
        SessionClient {
            shared: self.0.clone(),
            generation: lock(&self.0.control).status.session_generation,
        }
    }
    pub fn start_production(&self) -> Result<(), Error> {
        self.legacy(Command::Start)
    }
    pub fn stop_production(&self) -> Result<(), Error> {
        self.legacy(Command::Stop)
    }
    fn legacy(&self, command: Command) -> Result<(), Error> {
        self.session()
            .submit(command)
            .map(drop)
            .map_err(|e| match e {
                RequestError::Closed | RequestError::Unknown => Error::Closed,
                RequestError::Busy | RequestError::Retiring => Error::Backpressured,
                RequestError::Stale { requested, current } => {
                    Error::StaleSession { requested, current }
                }
                RequestError::NotBlockProducer => Error::NotBlockProducer,
                RequestError::Terminal => self.status().failure.unwrap_or(Error::Closed),
            })
    }
    // This fence has its own fixed slot, so unread control receipts cannot
    // prevent root shutdown from quiescing production. New requests close;
    // accepted controls are classified before the final stop is acknowledged.
    pub fn quiesce(&self) {
        lock(&self.0.control).quiescing = true;
        self.0.wake.notify_all();
    }
    pub fn quiesced(&self) -> bool {
        lock(&self.0.control).quiesced
    }
    pub fn controls(&self) -> Controls {
        let c = lock(&self.0.control);
        Controls {
            submitted: c.submitted,
            applied: c.applied,
            rejected: c.rejected,
            pending: c.commands.iter().filter(|j| !j.command.is_drive()).count(),
        }
    }
    pub fn usage(&self) -> Usage {
        *lock(&self.0.capacity)
    }
    pub fn request_shutdown(&self) {
        lock(&self.0.control).stop = true;
        self.0.wake.notify_all();
    }
}
#[derive(Clone)]
pub struct SessionClient {
    shared: Arc<Shared>,
    generation: u64,
}
impl SessionClient {
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn start_production(&self) -> Result<Ticket, RequestError> {
        self.submit(Command::Start)
    }
    pub fn stop_production(&self) -> Result<Ticket, RequestError> {
        self.submit(Command::Stop)
    }
    pub fn restart(&self) -> Result<Ticket, RequestError> {
        self.submit(Command::Restart)
    }
    fn submit(&self, command: Command) -> Result<Ticket, RequestError> {
        let mut c = lock(&self.shared.control);
        if c.stop || c.joined || c.quiescing {
            return Err(RequestError::Closed);
        }
        check(&c.status, self.generation, command)?;
        let driver = command.is_drive();
        let sequence = c.sequence.checked_add(1).ok_or(RequestError::Busy)?;
        let submitted = if driver {
            c.submitted
        } else {
            c.submitted.checked_add(1).ok_or(RequestError::Busy)?
        };
        let mut usage = lock(&self.shared.capacity);
        if (driver && usage.driver >= 1) || (!driver && usage.controls >= 64) {
            return Err(RequestError::Busy);
        }
        if driver {
            usage.driver += 1;
        } else {
            usage.controls += 1;
        }
        drop(usage);
        let reply = Slot::new(&self.shared, driver, sequence, self.generation);
        let ticket = Ticket(reply.clone());
        if driver {
            c.driver_work = Some(ticket.clone());
        }
        c.sequence = sequence;
        c.submitted = submitted;
        c.commands.push_back(Job {
            generation: self.generation,
            command,
            reply,
            executing: false,
        });
        self.shared.wake.notify_all();
        Ok(ticket)
    }
}
fn check(status: &Status, generation: u64, command: Command) -> Result<(), RequestError> {
    if generation != status.session_generation {
        return Err(RequestError::Stale {
            requested: generation,
            current: status.session_generation,
        });
    }
    if status.session_replacing && !matches!(command, Command::Stop) {
        return Err(RequestError::Retiring);
    }
    if status.failure.is_some() && !matches!(command, Command::Stop) {
        return Err(RequestError::Terminal);
    }
    if matches!(command, Command::Start) && !status.block_producer {
        return Err(RequestError::NotBlockProducer);
    }
    Ok(())
}
fn execute<O: MapOrder + Clone + Send + Sync + 'static>(
    c: &mut Conductor<O>,
    job: &Job,
) -> Result<Reply, RequestError> {
    check(&c.status(), job.generation, job.command)?;
    match job.command {
        Command::Start => c.start_production().map(|()| Reply::Applied),
        Command::Stop => c.stop_production().map(|()| Reply::Applied),
        Command::Restart => c
            .restart_production_session(job.generation)
            .map(|()| Reply::Applied),
        Command::Advance(wall, now) => c
            .driven_step(Some((wall, now)), Instant::now())
            .map(Reply::Advanced),
    }
    .map_err(|_| RequestError::Terminal)
}
struct ServiceGuard(Arc<Shared>);
impl Drop for ServiceGuard {
    fn drop(&mut self) {
        let jobs = {
            let mut c = lock(&self.0.control);
            c.stop = true;
            let controls = c.commands.iter().filter(|j| !j.command.is_drive()).count() as u64;
            c.applied += controls;
            c.rejected += controls;
            std::mem::take(&mut c.commands)
        };
        // Unexecuted requests get Closed; an unwinding executing Job publishes
        // Unknown before this guard runs. Neither is replayed into a new owner.
        drop(jobs);
        self.0.wake.notify_all();
    }
}

// The stable service owner is independent of the production driver. It alone
// mutates consensus services and accepted production work. A driver never
// borrows this state across a possible unwind.
pub struct Authority<O: MapOrder + Clone + Send + Sync + 'static> {
    handle: Handle,
    outputs: mpsc::Receiver<Output>,
    thread: Option<JoinHandle<Shutdown<O>>>,
}
pub struct SpawnError<O: MapOrder + Clone + Send + Sync + 'static> {
    pub error: String,
    pub conductor: Conductor<O>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Authority<O> {
    pub fn spawn(conductor: Conductor<O>, settings: Settings) -> Result<Self, Box<SpawnError<O>>> {
        let shared = Shared::new(conductor.status());
        Self::spawn_inner(conductor, shared, settings)
    }
    fn spawn_inner(
        conductor: Conductor<O>,
        shared: Arc<Shared>,
        settings: Settings,
    ) -> Result<Self, Box<SpawnError<O>>> {
        if !settings.valid() {
            return Err(Box::new(SpawnError {
                error: "invalid conductor runtime settings".into(),
                conductor,
            }));
        }
        let handle = Handle(shared.clone());
        let (sender, outputs) = mpsc::sync_channel(settings.outputs);
        let input = Arc::new(Mutex::new(Some(conductor)));
        let moved = input.clone();
        let thread = match thread::Builder::new()
            .name("ae-hc-services".into())
            .spawn(move || {
                let _guard = ServiceGuard(shared.clone());
                let mut conductor = lock(&moved).take().expect("conductor");
                lock(&shared.control).ready = true;
                shared.wake.notify_all();
                let mut stopped_driver = 0;
                loop {
                    if lock(&shared.control).stop {
                        break;
                    }
                    for _ in 0..65 {
                        let Some(mut job) = ({
                            let mut c = lock(&shared.control);
                            if c.stop { None } else { c.commands.pop_front() }
                        }) else {
                            break;
                        };
                        let quiescing = lock(&shared.control).quiescing;
                        job.executing = true;
                        let result = if quiescing
                            && matches!(job.command, Command::Advance(..) | Command::Restart)
                        {
                            Err(RequestError::Closed)
                        } else {
                            execute(&mut conductor, &job)
                        };
                        {
                            let mut control = lock(&shared.control);
                            if !job.command.is_drive() {
                                control.applied += 1;
                                control.rejected += u64::from(result.is_err());
                            }
                            control.status = conductor.status();
                        }
                        job.reply.complete(result);
                    }
                    let (quiescing, departed) = {
                        let control = lock(&shared.control);
                        (
                            control.quiescing,
                            (control.driver_exited && control.driver_generation > stopped_driver)
                                .then_some(control.driver_generation),
                        )
                    };
                    if quiescing || departed == Some(conductor.status().session_generation) {
                        let _ = conductor.stop_production();
                    }
                    if let Some(generation) = departed {
                        stopped_driver = generation;
                    }
                    if conductor.status.failure.is_none() {
                        let _ = conductor.driven_step(None, Instant::now());
                    }
                    if quiescing {
                        let _ = conductor.stop_production();
                    }
                    if let Some(output) = conductor.take_output() {
                        match sender.try_send(output) {
                            Ok(()) => (),
                            Err(mpsc::TrySendError::Full(o)) => conductor.pending = Some(o),
                            Err(mpsc::TrySendError::Disconnected(o)) => {
                                conductor.pending = Some(o);
                                break;
                            }
                        }
                    }
                    let mut control = lock(&shared.control);
                    control.status = conductor.status();
                    control.quiesced = control.quiescing
                        && control.commands.is_empty()
                        && !control.status.producing;
                    shared.wake.notify_all();
                    if control.stop {
                        break;
                    }
                    if !control.commands.is_empty() {
                        continue;
                    }
                    control = shared
                        .wake
                        .wait_timeout(control, settings.poll)
                        .unwrap_or_else(|e| e.into_inner())
                        .0;
                    if control.stop {
                        break;
                    }
                }
                let shutdown = conductor.shutdown();
                let mut control = lock(&shared.control);
                control.status = shutdown.status.clone();
                control.joined = true;
                control.stop = true;
                shutdown
            }) {
            Ok(thread) => thread,
            Err(error) => {
                return Err(Box::new(SpawnError {
                    error: error.to_string(),
                    conductor: lock(&input).take().expect("unspawned conductor"),
                }));
            }
        };
        Ok(Self {
            handle,
            outputs,
            thread: Some(thread),
        })
    }
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub fn try_output(&self) -> Option<Output> {
        self.outputs.try_recv().ok()
    }
    pub fn shutdown(mut self) -> thread::Result<Shutdown<O>> {
        self.handle.request_shutdown();
        self.thread
            .take()
            .expect("service thread")
            .join()
            .map(|mut report| {
                report.queued_outputs.extend(self.outputs.try_iter());
                for output in &mut report.queued_outputs {
                    output.poll_pinning();
                }
                report
            })
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Drop for Authority<O> {
    fn drop(&mut self) {
        self.handle.request_shutdown();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub struct Runtime<O: MapOrder + Clone + Send + Sync + 'static> {
    authority: Option<Authority<O>>,
    driver: Option<Driver>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Runtime<O> {
    pub fn spawn(
        conductor: Conductor<O>,
        clock: impl Clock,
        settings: Settings,
    ) -> Result<Self, Box<SpawnError<O>>> {
        if !settings.valid() {
            return Err(Box::new(SpawnError {
                error: "invalid conductor runtime settings".into(),
                conductor,
            }));
        }
        let shared = Shared::new(conductor.status());
        // Spawn the driver behind the readiness barrier first. Failure of
        // either thread constructor returns the still-owned Conductor.
        let driver = match Driver::spawn(Handle(shared.clone()), clock, settings.poll) {
            Ok(driver) => driver,
            Err(error) => return Err(Box::new(SpawnError { error, conductor })),
        };
        match Authority::spawn_inner(conductor, shared, settings) {
            Ok(authority) => Ok(Self {
                authority: Some(authority),
                driver: Some(driver),
            }),
            Err(error) => {
                drop(driver);
                Err(error)
            }
        }
    }
    pub fn handle(&self) -> Handle {
        self.authority.as_ref().expect("authority").handle()
    }
    pub fn try_output(&self) -> Option<Output> {
        self.authority.as_ref().expect("authority").try_output()
    }
    pub fn service_is_finished(&self) -> bool {
        self.authority.as_ref().is_none_or(Authority::is_finished)
    }
    pub fn is_finished(&self) -> bool {
        self.service_is_finished() || self.driver.as_ref().is_none_or(Driver::is_finished)
    }
    pub fn into_parts(mut self) -> (Authority<O>, Driver) {
        (self.authority.take().unwrap(), self.driver.take().unwrap())
    }
    pub fn shutdown(mut self) -> thread::Result<Shutdown<O>> {
        let driver = self.driver.take().expect("driver");
        driver.request_shutdown();
        let result = self.authority.take().expect("authority").shutdown();
        let mut driver = driver.shutdown();
        driver.poll();
        result.map(|mut report| {
            if report.status.failure.is_none() {
                report.status.failure = driver.failure.clone();
            }
            report.production_driver = Some(driver);
            report
        })
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Drop for Runtime<O> {
    fn drop(&mut self) {
        if let Some(driver) = &self.driver {
            driver.request_shutdown();
        }
        if let Some(authority) = self.authority.take() {
            drop(authority);
        }
        if let Some(driver) = self.driver.take() {
            drop(driver);
        }
    }
}
