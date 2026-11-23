// aec_consensus_sup:init/1: permanent one_for_one children, 3 / 30 seconds.
// Explicit operational work-stops remain failures requiring reconciliation;
// normal process exits and panics are eligible for replacement after join.
use super::*;
use crate::runtime::restart::Window;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Status {
    pub pinning_generation: u64,
    pub attempts: u64,
    pub restarting: bool,
    pub pending_notices: usize,
}
#[derive(Debug)]
pub enum Notice {
    Retired {
        generation: u64,
        child: Box<pinning::Retired>,
    },
    Restarted {
        generation: u64,
    },
    StartFailed {
        attempt: u64,
        error: pinning::Error,
    },
}
impl Notice {
    pub fn poll_children(&mut self) {
        if let Self::Retired { child, .. } = self {
            child.poll_children();
        }
    }
}
pub struct Shutdown {
    pub status: Status,
    pub notices: Vec<Notice>,
}
pub(super) struct Supervisor<O> {
    factory: Option<pinning::Factory<O>>,
    budget: Window<3, 30>,
    origin: Option<Instant>,
    status: Status,
    notices: VecDeque<Notice>,
    limit: usize,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Supervisor<O> {
    pub(super) fn new(factory: Option<pinning::Factory<O>>, limit: usize) -> Self {
        Self {
            status: Status {
                pinning_generation: u64::from(factory.is_some()),
                ..Default::default()
            },
            factory,
            budget: Default::default(),
            origin: None,
            notices: VecDeque::new(),
            limit,
        }
    }
    pub(super) fn status(&self) -> Status {
        Status {
            pending_notices: self.notices.len(),
            ..self.status.clone()
        }
    }
    pub(super) fn poll(
        &mut self,
        child: &mut Option<pinning::Service>,
        now: Instant,
    ) -> Result<(), Error> {
        let Some(factory) = &self.factory else {
            return Ok(());
        };
        let origin = *self.origin.get_or_insert(now);
        if let Some(service) = child.as_ref() {
            self.status.restarting = service.replacement_pending();
        }
        if self.notices.len() >= self.limit {
            return Ok(());
        }
        if let Some(service) = child
            && service.finished()
            && service.replacement_pending()
        {
            let retired = child.take().expect("finished child").retire();
            self.notices.push_back(Notice::Retired {
                generation: self.status.pinning_generation,
                child: Box::new(retired),
            });
            self.status.restarting = true;
        }
        if child.is_some() || !self.status.restarting || self.notices.len() >= self.limit {
            return Ok(());
        }
        self.status.attempts = self
            .status
            .attempts
            .checked_add(1)
            .ok_or(Error::Configuration)?;
        if !self
            .budget
            .record(now.saturating_duration_since(origin).as_secs())
        {
            return Err(Error::ConsensusRestartLimit);
        }
        let replacement =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| factory.spawn()))
                .unwrap_or(Err(pinning::Error::Worker));
        match replacement {
            Ok(service) => {
                self.status.pinning_generation = self
                    .status
                    .pinning_generation
                    .checked_add(1)
                    .ok_or(Error::Configuration)?;
                *child = Some(service);
                self.status.restarting = false;
                self.notices.push_back(Notice::Restarted {
                    generation: self.status.pinning_generation,
                });
            }
            Err(error) => self.notices.push_back(Notice::StartFailed {
                attempt: self.status.attempts,
                error,
            }),
        }
        Ok(())
    }
    pub(super) fn take_notice(&mut self) -> Option<Notice> {
        self.notices.pop_front()
    }
    pub(super) fn finish(&mut self) -> Shutdown {
        let status = self.status();
        let mut notices: Vec<_> = self.notices.drain(..).collect();
        for notice in &mut notices {
            notice.poll_children();
        }
        self.factory.take();
        Shutdown { status, notices }
    }
}
