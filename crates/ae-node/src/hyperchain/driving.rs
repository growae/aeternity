use super::*;
use std::time::Instant;

pub(super) enum Input {
    Single(Box<dyn producer::Clock>),
    Supervised(Box<dyn production::Factory>),
}
pub(super) enum Runtime<O: MapOrder + Clone + Send + Sync + 'static> {
    Single(producer::Runtime<O>),
    Supervised(Box<production::Group<O>>, Instant),
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Runtime<O> {
    pub fn spawn(
        conductor: Conductor<O>,
        input: Input,
        settings: producer::Settings,
    ) -> Result<Self, Box<producer::SpawnError<O>>> {
        match input {
            Input::Single(clock) => {
                producer::Runtime::spawn(conductor, clock, settings).map(Self::Single)
            }
            Input::Supervised(factory) => {
                production::Group::spawn_boxed(conductor, factory, settings)
                    .map(|group| Self::Supervised(Box::new(group), Instant::now()))
            }
        }
    }
    pub fn handle(&self) -> producer::Handle {
        match self {
            Self::Single(p) => p.handle(),
            Self::Supervised(p, _) => p.handle(),
        }
    }
    pub fn status(&self) -> conductor::Status {
        match self {
            Self::Single(p) => p.handle().status(),
            Self::Supervised(p, _) => p.handle().service_status(),
        }
    }
    pub fn production_status(&self) -> Option<production::Status> {
        match self {
            Self::Single(_) => None,
            Self::Supervised(p, _) => Some(p.status()),
        }
    }
    pub fn take_notice(&mut self) -> Option<production::Notice> {
        match self {
            Self::Single(_) => None,
            Self::Supervised(p, _) => p.take_notice(),
        }
    }
    pub fn try_output(&self) -> Option<Output> {
        match self {
            Self::Single(p) => p.try_output(),
            Self::Supervised(p, _) => p.try_output(),
        }
    }
    pub fn service_is_finished(&self) -> bool {
        match self {
            Self::Single(p) => p.service_is_finished(),
            Self::Supervised(p, _) => p.service_is_finished(),
        }
    }
    pub fn constructs_clock(&self) -> bool {
        matches!(self, Self::Supervised(p, _) if p.status().phase == production::Phase::Starting)
    }
    pub fn poll(&mut self) -> Result<(), Error> {
        match self {
            Self::Supervised(p, epoch) => {
                p.poll(epoch.elapsed().as_secs()).map_err(Error::Production)
            }
            Self::Single(p) => {
                if let Some(e) = p.handle().status().failure {
                    Err(Error::Producer(e))
                } else if p.is_finished() {
                    Err(Error::ChildStopped(Child::Producer))
                } else {
                    Ok(())
                }
            }
        }
    }
    pub fn shutdown(
        self,
    ) -> (
        Result<conductor::Shutdown<O>, Error>,
        Option<production::Report>,
    ) {
        let (service, report) = match self {
            Self::Single(p) => (p.shutdown(), None),
            Self::Supervised(p, _) => {
                let e = p.shutdown();
                (e.service, Some(e.report))
            }
        };
        (
            service.map_err(|_| Error::ChildTask(Child::Producer)),
            report,
        )
    }
}
