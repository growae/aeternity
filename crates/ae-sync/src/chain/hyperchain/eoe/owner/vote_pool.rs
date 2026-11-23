use super::*;
use crate::{bindings, runtime::restart::workers::Group};

#[derive(Debug, Clone, Default)]
pub struct Status {
    pub generation: u64,
    pub restarting: bool,
    pub restart_attempts: u64,
    pub pending_notices: usize,
}
#[derive(Debug)]
pub enum Notice {
    Retired {
        generation: u64,
        worker: Box<votes::Retired>,
    },
    Restarted {
        generation: u64,
    },
    StartFailed {
        attempt: u64,
        error: votes::Error,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlError {
    Closed,
    Stale,
}
pub(super) struct Pool {
    worker: Option<votes::Worker>,
    client: votes::Client,
    database: std::sync::Weak<Database>,
    gossip: GossipCache,
    config: votes::Config,
    limits: ae_mempool::hc_votes::Limits,
    publisher: bindings::Owner<votes::Client>,
    binding: votes::Binding,
    group: Group,
    generation: u64,
    notices: VecDeque<Notice>,
    maximum_notices: usize,
}
impl Pool {
    pub fn start(
        database: Arc<Database>,
        gossip: GossipCache,
        config: votes::Config,
        relay: &votes::independent::Relay,
        limits: ae_mempool::hc_votes::Limits,
        maximum_notices: usize,
    ) -> Result<Self, votes::Error> {
        let worker = votes::Worker::spawn_independent(
            database.clone(),
            gossip.clone(),
            config,
            relay,
            limits,
        )?;
        let client = worker.client();
        let (publisher, binding) = bindings::Owner::new(&database);
        publisher.install(client.clone(), 1);
        Ok(Self {
            worker: Some(worker),
            client,
            database: Arc::downgrade(&database),
            gossip,
            config,
            limits,
            publisher,
            binding,
            group: Group::default(),
            generation: 1,
            notices: VecDeque::new(),
            maximum_notices,
        })
    }
    pub fn client(&self) -> votes::Client {
        self.client.clone()
    }
    pub fn binding(&self) -> votes::Binding {
        self.binding.clone()
    }
    pub fn group(&self) -> Group {
        self.group.clone()
    }
    pub fn status(&self) -> Status {
        Status {
            generation: self.generation,
            restarting: !self.binding.is_closed() && !self.client.info().accepting,
            restart_attempts: self.group.attempts(),
            pending_notices: self.notices.len(),
        }
    }
    pub fn request_restart(&self, generation: u64) -> Result<(), ControlError> {
        if self.binding.is_closed() || self.group.exhausted() {
            return Err(ControlError::Closed);
        }
        if generation != self.generation {
            return Err(ControlError::Stale);
        }
        if let Some(worker) = &self.worker {
            self.publisher.withdraw();
            worker.request_shutdown();
        }
        Ok(())
    }
    pub fn poll(&mut self, relay: &votes::independent::Relay) -> Result<(), Error> {
        if relay.hub().is_closed() {
            return Err(Error::Votes(votes::Error::AuthorityClosed));
        }
        if self.binding.is_closed() {
            return Err(Error::Closed);
        }
        if self.group.exhausted() {
            return Err(Error::VoteRestartLimit);
        }
        if self.client.info().accepting {
            return Ok(());
        }
        self.publisher.withdraw();
        if self.worker.as_ref().is_some_and(|w| !w.is_finished())
            || self.notices.len() >= self.maximum_notices
        {
            return Ok(());
        }
        let Some(mut permit) = self.group.try_acquire() else {
            return Ok(());
        };
        if let Some(worker) = self.worker.take() {
            self.notices.push_back(Notice::Retired {
                generation: self.generation,
                worker: Box::new(worker.retire()),
            });
        }
        // Even a one-slot outbox must transfer the old report before publishing
        // a replacement. Epoch delivery and the independent relay keep running.
        if self.notices.len() >= self.maximum_notices {
            return Ok(());
        }
        if !permit.record() {
            return Err(Error::VoteRestartLimit);
        }
        let generation = self.generation.checked_add(1).ok_or(Error::Configuration)?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            votes::Worker::spawn_independent(
                self.database.upgrade().ok_or(votes::Fault::Stopped)?,
                self.gossip.clone(),
                self.config,
                relay,
                self.limits,
            )
        }))
        .unwrap_or(Err(votes::Fault::Panicked.into()));
        match result {
            Ok(worker) => {
                self.client = worker.client();
                self.worker = Some(worker);
                self.generation = generation;
                self.publisher.install(self.client.clone(), generation);
                self.notices.push_back(Notice::Restarted { generation });
            }
            Err(error) => self.notices.push_back(Notice::StartFailed {
                attempt: self.group.attempts(),
                error,
            }),
        }
        Ok(())
    }
    pub fn take_notice(&mut self) -> Option<Notice> {
        self.notices.pop_front()
    }
    pub fn close(&self) {
        self.publisher.close();
    }
    pub fn request_shutdown(&self) {
        self.close();
        if let Some(worker) = &self.worker {
            worker.request_shutdown();
        }
    }
    pub fn shutdown(&mut self) -> Result<(), votes::Error> {
        self.close();
        self.worker.take().map_or(Ok(()), votes::Worker::shutdown)
    }
    pub fn take_notices(&mut self) -> Vec<Notice> {
        self.notices.drain(..).collect()
    }
}

impl Owner {
    pub fn vote_binding(&self) -> votes::Binding {
        self.votes.binding()
    }
    pub fn vote_status(&self) -> Status {
        self.votes.status()
    }
    pub fn request_vote_restart(&self, generation: u64) -> Result<(), ControlError> {
        self.votes.request_restart(generation)
    }
    pub fn take_vote_notice(&mut self) -> Option<Notice> {
        self.votes.take_notice()
    }
    pub(crate) fn worker_group(&self) -> Group {
        self.votes.group()
    }
    pub(crate) fn close_vote_binding(&self) {
        self.votes.close();
    }
    pub(crate) fn stop_votes(&self) {
        self.votes.request_shutdown();
    }
    pub(crate) fn poll_votes(&mut self) -> Result<(), Error> {
        self.votes.poll(
            self.output
                .as_ref()
                .expect("output owner")
                .relay()
                .expect("independent relay"),
        )
    }
}

#[cfg(test)]
mod tests;
