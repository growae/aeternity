// aec_conductor_sup:init/1, aeternity fa2a6d7; OTP26 supervisor restart/2.
// ISC copyright: repository LICENSE. This owner is separate from consensus
// and worker supervisors. Application adapters must keep those owners outside
// the two replaceable children.
use crate::runtime::restart::Window;
use std::{panic::RefUnwindSafe, thread};

/// An independently owned child. Shutdown must close admission and join all
/// owned work, returning retained results without replaying accepted requests.
/// It must also join on Drop, including when a constructor unwinds.
///
/// This cooperative boundary cannot force-kill a Rust thread after the native
/// 5000ms timeout. A stuck child prevents replacement; it is never detached.
pub trait Child {
    type Exit;
    fn is_finished(&self) -> bool;
    fn shutdown(self) -> thread::Result<Self::Exit>;
}

/// Constructors create fresh children from configuration and stable service
/// clients. They must clean up partial resources on error/unwind. They never
/// receive retired child state, commands, outputs or accepted receipts.
pub trait Factory: RefUnwindSafe {
    type Generator: Child;
    type Conductor: Child;
    type Error;
    fn generator(&self, generation: u64) -> Result<Self::Generator, Self::Error>;
    fn conductor(
        &self,
        generation: u64,
        generator: &Self::Generator,
    ) -> Result<Self::Conductor, Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Running,
    Starting,
    Retry,
    Failed,
    Stopped,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Member {
    Generator,
    Conductor,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Time,
    RestartLimit,
    GenerationLimit,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    pub phase: Phase,
    pub generator_generation: u64,
    pub conductor_generation: u64,
    pub attempts: u64,
    pub failure: Option<Error>,
    pub pending_notice: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    Idle,
    Output,
    Stopped,
}
#[derive(Debug)]
pub enum StartError<E> {
    Returned(E),
    Panicked,
    GenerationLimit,
}
pub struct StartFailure<E> {
    pub child: Member,
    pub generation: u64,
    pub error: StartError<E>,
}
pub struct Retired<G, C> {
    pub generator: Option<(u64, thread::Result<G>)>,
    pub conductor: Option<(u64, thread::Result<C>)>,
}
pub enum Notice<G, C, E> {
    Retired(Retired<G, C>),
    StartFailed(StartFailure<E>),
    Restarted {
        generator_generation: u64,
        conductor_generation: u64,
    },
}
pub type GeneratorExit<F> = <<F as Factory>::Generator as Child>::Exit;
pub type ConductorExit<F> = <<F as Factory>::Conductor as Child>::Exit;
pub type Output<F> = Notice<GeneratorExit<F>, ConductorExit<F>, <F as Factory>::Error>;
pub type Retirement<F> = Retired<GeneratorExit<F>, ConductorExit<F>>;
pub struct StartupFailure<F: Factory> {
    pub factory: F,
    pub failure: StartFailure<F::Error>,
    pub retired: Retirement<F>,
}
pub struct Shutdown<F: Factory> {
    pub status: Status,
    pub pending: Option<Output<F>>,
    pub retired: Retirement<F>,
}

pub struct Owner<F: Factory> {
    factory: Option<F>,
    generator: Option<F::Generator>,
    conductor: Option<F::Conductor>,
    status: Status,
    budget: Window<5, 50>,
    last_second: Option<u64>,
    pending: Option<Output<F>>,
}
impl<F: Factory> Owner<F> {
    pub fn start(factory: F) -> Result<Self, Box<StartupFailure<F>>> {
        let mut owner = Self {
            factory: Some(factory),
            generator: None,
            conductor: None,
            status: Status {
                phase: Phase::Starting,
                generator_generation: 0,
                conductor_generation: 0,
                attempts: 0,
                failure: None,
                pending_notice: false,
            },
            budget: Window::default(),
            last_second: None,
            pending: None,
        };
        if let Err(failure) = owner.construct() {
            let retired = owner.retire();
            return Err(Box::new(StartupFailure {
                factory: owner.factory.take().expect("factory"),
                failure,
                retired,
            }));
        }
        owner.status.phase = Phase::Running;
        Ok(owner)
    }
    pub fn status(&self) -> Status {
        Status {
            pending_notice: self.pending.is_some(),
            ..self.status
        }
    }
    // Expose only a fully started pair. Cloned application clients must name
    // these exact generations; this owner does not redirect old clients.
    pub fn children(&self) -> Option<(&F::Generator, &F::Conductor)> {
        if self.status.phase != Phase::Running {
            return None;
        }
        Some((self.generator.as_ref()?, self.conductor.as_ref()?))
    }
    pub fn take_notice(&mut self) -> Option<Output<F>> {
        self.pending.take()
    }
    /// `second` is a monotonically increasing, process-wide integer clock.
    /// The restart window is inclusive at 50 seconds, as in OTP26.
    /// Joining a retiring child is synchronous; drive this owner separately
    /// from independent services that must continue during its shutdown.
    pub fn poll(&mut self, second: u64) -> Result<Progress, Error> {
        if self.last_second.is_some_and(|last| second < last) {
            return Err(Error::Time);
        }
        self.last_second = Some(second);
        if self.pending.is_some() {
            return Ok(Progress::Output);
        }
        if let Some(error) = self.status.failure {
            return Err(error);
        }
        match self.status.phase {
            Phase::Running => {
                let (generator, conductor) = self.children().expect("running pair");
                if !generator.is_finished() && !conductor.is_finished() {
                    return Ok(Progress::Idle);
                }
                self.restart(second);
            }
            Phase::Retry => self.restart(second),
            Phase::Starting => match self.construct() {
                Ok(()) => {
                    self.status.phase = Phase::Running;
                    self.pending = Some(Notice::Restarted {
                        generator_generation: self.status.generator_generation,
                        conductor_generation: self.status.conductor_generation,
                    });
                }
                Err(failure) => {
                    self.status.phase = Phase::Retry;
                    self.pending = Some(Notice::StartFailed(failure));
                }
            },
            Phase::Stopped | Phase::Failed => return Ok(Progress::Stopped),
        }
        Ok(Progress::Output)
    }
    fn restart(&mut self, second: u64) {
        // OTP records the request before terminating siblings, including a
        // retry after a partial constructor failure. Cleanup still happens on
        // exhaustion, but a sixth pair is never constructed in the window.
        let error = match self.status.attempts.checked_add(1) {
            Some(attempts) => {
                self.status.attempts = attempts;
                (!self.budget.record(second)).then_some(Error::RestartLimit)
            }
            None => Some(Error::GenerationLimit),
        };
        self.status.failure = error;
        self.status.phase = if error.is_some() {
            Phase::Failed
        } else {
            Phase::Starting
        };
        self.pending = Some(Notice::Retired(self.retire()));
    }
    fn construct(&mut self) -> Result<(), StartFailure<F::Error>> {
        let generator_generation = next(&mut self.status.generator_generation, Member::Generator)?;
        let factory = self.factory.as_ref().expect("factory");
        self.generator = Some(start(Member::Generator, generator_generation, || {
            factory.generator(generator_generation)
        })?);
        let conductor_generation = next(&mut self.status.conductor_generation, Member::Conductor)?;
        // Only the factory is required to be unwind-safe. The generator is
        // borrowed for construction, and is retired before another attempt.
        let generator = self.generator.as_ref().expect("generator");
        self.conductor = Some(start(Member::Conductor, conductor_generation, || {
            factory.conductor(conductor_generation, generator)
        })?);
        Ok(())
    }
    fn retire(&mut self) -> Retirement<F> {
        // Sequential reverse order is intentional: generator shutdown must
        // not overtake conductor cleanup, even after a partial failed start.
        let conductor = self
            .conductor
            .take()
            .map(|child| (self.status.conductor_generation, child.shutdown()));
        let generator = self
            .generator
            .take()
            .map(|child| (self.status.generator_generation, child.shutdown()));
        Retired {
            generator,
            conductor,
        }
    }
    pub fn shutdown(mut self) -> Shutdown<F> {
        self.status.phase = Phase::Stopped;
        let retired = self.retire();
        Shutdown {
            status: self.status(),
            pending: self.pending.take(),
            retired,
        }
    }
}
impl<F: Factory> Drop for Owner<F> {
    fn drop(&mut self) {
        let _ = self.retire();
    }
}
fn next<E>(generation: &mut u64, child: Member) -> Result<u64, StartFailure<E>> {
    *generation = generation.checked_add(1).ok_or(StartFailure {
        child,
        generation: *generation,
        error: StartError::GenerationLimit,
    })?;
    Ok(*generation)
}
fn start<T, E>(
    child: Member,
    generation: u64,
    construct: impl FnOnce() -> Result<T, E>,
) -> Result<T, StartFailure<E>> {
    // Child construction does not mutate an existing application generation.
    // Any partially constructed resources must use joined RAII ownership.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(construct))
        .map_err(|_| StartError::Panicked)
        .and_then(|result| result.map_err(StartError::Returned))
        .map_err(|error| StartFailure {
            child,
            generation,
            error,
        })
}

#[cfg(test)]
mod tests;
