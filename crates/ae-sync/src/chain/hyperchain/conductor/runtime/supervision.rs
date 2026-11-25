use super::{
    Authority, Clock, Conductor, Driver, DriverReport, Handle, Observed, Output, Reply,
    RequestError, Runtime, Settings, SpawnError, Ticket,
};
use crate::runtime::restart::Window;
use ae_vm_fate::maps::MapOrder;
use std::{
    panic::{RefUnwindSafe, catch_unwind},
    thread,
};

// HC creates candidates inside the conductor (aec_conductor:hc_create_block,
// fa2a6d7). Its joined candidate/signing/admission work belongs to the logical
// production session; there is no synthetic block-generator child here.
// aec_conductor_sup supplies the inclusive five-in-fifty restart policy.
pub trait Factory: Send + RefUnwindSafe + 'static {
    fn clock(&self, generation: u64) -> Result<Box<dyn Clock>, String>;
}
impl<F, C> Factory for F
where
    F: Fn(u64) -> Result<C, String> + Send + RefUnwindSafe + 'static,
    C: Clock,
{
    fn clock(&self, generation: u64) -> Result<Box<dyn Clock>, String> {
        self(generation).map(|c| Box::new(c) as Box<dyn Clock>)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Running,
    Retiring,
    Replacing,
    Starting,
    Retry,
    Failed,
    Stopped,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Time,
    RestartLimit,
    GenerationLimit,
    Service(super::Error),
    ServiceStopped,
    Control(RequestError),
    SessionChanged,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub phase: Phase,
    pub generation: u64,
    pub attempts: u64,
    pub failure: Option<Error>,
    pub pending_notice: bool,
}
#[derive(Debug)]
pub enum Notice {
    Retired(DriverReport),
    StartFailed { generation: u64, error: String },
    Restarted { generation: u64 },
}
pub struct Report {
    pub status: Status,
    pub pending: Option<Notice>,
    pub retired: Option<DriverReport>,
    pub restart: Option<Observed>,
    pub driver: Option<DriverReport>,
}
pub struct Shutdown<O> {
    pub service: thread::Result<super::Shutdown<O>>,
    pub report: Report,
}
pub struct Group<O: MapOrder + Clone + Send + Sync + 'static> {
    authority: Option<Authority<O>>,
    driver: Option<Driver>,
    factory: Box<dyn Factory>,
    settings: Settings,
    status: Status,
    budget: Window<5, 50>,
    last_second: Option<u64>,
    retired: Option<DriverReport>,
    restart: Option<Ticket>,
    pending: Option<Notice>,
}
fn clock(factory: &dyn Factory, generation: u64) -> Result<Box<dyn Clock>, String> {
    catch_unwind(|| factory.clock(generation))
        .unwrap_or_else(|_| Err("production clock constructor panicked".into()))
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Group<O> {
    pub fn spawn(
        conductor: Conductor<O>,
        factory: impl Factory,
        settings: Settings,
    ) -> Result<Self, Box<SpawnError<O>>> {
        Self::spawn_boxed(conductor, Box::new(factory), settings)
    }
    pub fn spawn_boxed(
        conductor: Conductor<O>,
        factory: Box<dyn Factory>,
        settings: Settings,
    ) -> Result<Self, Box<SpawnError<O>>> {
        if !settings.valid() {
            return Err(Box::new(SpawnError {
                error: "invalid conductor runtime settings".into(),
                conductor,
            }));
        }
        let generation = conductor.status().session_generation;
        let first = match clock(&*factory, generation) {
            Ok(clock) => clock,
            Err(error) => return Err(Box::new(SpawnError { error, conductor })),
        };
        let (authority, driver) = Runtime::spawn(conductor, first, settings)?.into_parts();
        Ok(Self {
            authority: Some(authority),
            driver: Some(driver),
            factory,
            settings,
            status: Status {
                phase: Phase::Running,
                generation,
                attempts: 0,
                failure: None,
                pending_notice: false,
            },
            budget: Window::default(),
            last_second: None,
            retired: None,
            restart: None,
            pending: None,
        })
    }
    pub fn handle(&self) -> Handle {
        self.authority.as_ref().expect("authority").handle()
    }
    pub fn status(&self) -> Status {
        Status {
            pending_notice: self.pending.is_some(),
            ..self.status.clone()
        }
    }
    pub fn try_output(&self) -> Option<Output> {
        self.authority.as_ref().expect("authority").try_output()
    }
    pub fn take_notice(&mut self) -> Option<Notice> {
        self.pending.take()
    }
    pub fn service_is_finished(&self) -> bool {
        self.authority.as_ref().is_none_or(Authority::is_finished)
    }
    fn fail(&mut self, error: Error) -> Error {
        if let Some(driver) = &self.driver {
            driver.request_shutdown();
        }
        self.status.phase = Phase::Failed;
        self.status.failure.get_or_insert(error).clone()
    }
    fn budget(&mut self, second: u64) {
        let failure = match self.status.attempts.checked_add(1) {
            Some(attempts) => {
                self.status.attempts = attempts;
                (!self.budget.record(second)).then_some(Error::RestartLimit)
            }
            None => Some(Error::GenerationLimit),
        };
        if let Some(error) = failure {
            self.fail(error);
        }
    }
    /// Poll with monotonically increasing process seconds. Retired drivers
    /// are joined only after they exit. A blocked driver is never detached.
    /// Consume both output streams: unread notices stop replacement, while
    /// unread service outputs retain accepted work before session installation.
    pub fn poll(&mut self, second: u64) -> Result<(), Error> {
        if self.last_second.is_some_and(|last| second < last) {
            return Err(Error::Time);
        }
        self.last_second = Some(second);
        let service = self.handle().service_status();
        if let Some(error) = service.failure {
            return Err(self.fail(Error::Service(error)));
        }
        if self.service_is_finished() {
            return Err(self.fail(Error::ServiceStopped));
        }
        if self.pending.is_some() {
            return Ok(());
        }
        if let Some(error) = &self.status.failure {
            return Err(error.clone());
        }
        match self.status.phase {
            Phase::Running => {
                if !self.driver.as_ref().expect("running driver").is_finished() {
                    return Ok(());
                }
                self.budget(second);
                self.retired = Some(self.driver.take().expect("departed driver").shutdown());
                if self.status.failure.is_none() {
                    self.status.phase = Phase::Retiring;
                }
                self.settle_retired();
            }
            Phase::Retiring => self.settle_retired(),
            Phase::Replacing => {
                if self.restart.is_none() {
                    if service.session_generation != self.status.generation {
                        return Err(self.fail(Error::SessionChanged));
                    }
                    match self.handle().session().restart() {
                        Ok(ticket) => self.restart = Some(ticket),
                        Err(RequestError::Busy | RequestError::Retiring) => return Ok(()),
                        Err(error) => return Err(self.fail(Error::Control(error))),
                    }
                }
                match self.restart.as_ref().expect("restart ticket").try_result() {
                    None => return Ok(()),
                    Some(Ok(Reply::Applied)) => (),
                    Some(Err(error)) => return Err(self.fail(Error::Control(error))),
                    Some(Ok(_)) => return Err(self.fail(Error::SessionChanged)),
                }
                let next = self
                    .status
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| self.fail(Error::GenerationLimit))?;
                // Refresh after the receipt; service status can precede it.
                let current = self.handle().service_status();
                if current.session_generation == self.status.generation {
                    return Ok(());
                }
                if current.session_generation != next || current.session_replacing {
                    return Err(self.fail(Error::SessionChanged));
                }
                self.restart.take();
                self.status.generation = next;
                self.status.phase = Phase::Starting;
            }
            Phase::Starting => {
                if service.session_generation != self.status.generation || service.session_replacing
                {
                    return Err(self.fail(Error::SessionChanged));
                }
                match clock(&*self.factory, self.status.generation)
                    .and_then(|c| Driver::spawn(self.handle(), c, self.settings.poll))
                {
                    Ok(driver) => {
                        self.driver = Some(driver);
                        self.status.phase = Phase::Running;
                        self.pending = Some(Notice::Restarted {
                            generation: self.status.generation,
                        });
                    }
                    Err(error) => {
                        self.status.phase = Phase::Retry;
                        self.pending = Some(Notice::StartFailed {
                            generation: self.status.generation,
                            error,
                        });
                    }
                }
            }
            Phase::Retry => {
                self.budget(second);
                if self.status.failure.is_none() {
                    self.status.phase = Phase::Starting;
                }
            }
            Phase::Failed | Phase::Stopped => (),
        }
        Ok(())
    }
    fn settle_retired(&mut self) {
        let retired = self.retired.as_mut().expect("retired driver");
        retired.poll();
        if matches!(retired.pending, Some(Observed::Pending(_))) {
            return;
        }
        if let Some(Observed::Known {
            result: Err(error), ..
        }) = retired.pending
        {
            self.fail(Error::Control(error));
        }
        self.pending = self.retired.take().map(Notice::Retired);
        if self.status.failure.is_none() {
            self.status.phase = Phase::Replacing;
        }
    }
    pub fn shutdown(mut self) -> Shutdown<O> {
        if let Some(driver) = &self.driver {
            driver.request_shutdown();
        }
        let service = self.authority.take().expect("authority").shutdown();
        let mut driver = self.driver.take().map(Driver::shutdown);
        for report in [&mut driver, &mut self.retired].into_iter().flatten() {
            report.poll();
        }
        if let Some(Notice::Retired(report)) = &mut self.pending {
            report.poll();
        }
        let restart = self.restart.take().map(Observed::from);
        self.status.phase = Phase::Stopped;
        Shutdown {
            service,
            report: Report {
                status: self.status(),
                pending: self.pending.take(),
                retired: self.retired.take(),
                restart,
                driver,
            },
        }
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Drop for Group<O> {
    fn drop(&mut self) {
        if let Some(driver) = &self.driver {
            driver.request_shutdown();
        }
        self.authority.take();
        self.driver.take();
    }
}

#[cfg(test)]
mod tests;
