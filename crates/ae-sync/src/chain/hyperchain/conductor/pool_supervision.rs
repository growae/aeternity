use super::*;
use crate::{
    bindings,
    runtime::restart::workers::{Group, Permit},
};
use ae_protocol::NetworkProfile;
use std::{
    sync::Weak,
    thread::{self, JoinHandle},
};

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
        worker: Box<pool::worker::Retired>,
    },
    Restarted {
        generation: u64,
    },
    StartFailed {
        attempt: u64,
        error: pool::worker::Error,
    },
}
pub use owner::vote_pool::ControlError;
struct Starting {
    thread: JoinHandle<Result<pool::worker::Worker, pool::worker::Error>>,
    _permit: Permit,
}
pub(super) struct Pool {
    worker: Option<pool::worker::Worker>,
    starting: Option<Starting>,
    client: pool::worker::Client,
    database: Weak<Database>,
    network: NetworkProfile,
    config: pool::Config,
    settings: pool::worker::Settings,
    gossip: pool::worker::GossipCache,
    publisher: bindings::Owner<bindings::Pool>,
    binding: bindings::Admission,
    group: Group,
    generation: u64,
    notices: VecDeque<Notice>,
    maximum_notices: usize,
    #[cfg(test)]
    restore_gate: Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>,
}
impl Pool {
    pub fn start(
        database: Arc<Database>,
        network: NetworkProfile,
        config: pool::Config,
        settings: pool::worker::Settings,
        gossip: pool::worker::GossipCache,
        maximum_notices: usize,
    ) -> Result<Self, pool::worker::Error> {
        if maximum_notices == 0 {
            return Err(pool::worker::Error::InvalidSettings);
        }
        let worker = pool::worker::Worker::spawn_with_cache(
            database.clone(),
            network.clone(),
            config.clone(),
            settings,
            gossip.clone(),
        )?;
        let client = worker.client();
        let (publisher, binding) = bindings::Owner::new(&database);
        publisher.install(bindings::Pool::new(client.clone()), 1);
        Ok(Self {
            worker: Some(worker),
            starting: None,
            client,
            database: Arc::downgrade(&database),
            network,
            config,
            settings,
            gossip,
            publisher,
            binding,
            group: Group::default(),
            generation: 1,
            notices: VecDeque::new(),
            maximum_notices,
            #[cfg(test)]
            restore_gate: None,
        })
    }
    pub fn set_group(&mut self, group: Group) {
        self.group = group;
    }
    pub fn client(&self) -> pool::worker::Client {
        self.client.clone()
    }
    pub fn binding(&self) -> bindings::Admission {
        self.binding.clone()
    }
    pub fn replacement_pending(&self) -> bool {
        !self.client.info().accepting
    }
    pub fn status(&self) -> Status {
        Status {
            generation: self.generation,
            restarting: !self.binding.is_closed() && self.replacement_pending(),
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
        self.publisher.withdraw();
        if let Some(worker) = &self.worker {
            worker.request_shutdown();
        }
        Ok(())
    }
    pub fn poll(
        &mut self,
        retired: impl FnOnce(&pool::worker::Client) -> Result<(), Error>,
    ) -> Result<(), Error> {
        if self.binding.is_closed() {
            return Err(Error::Closed);
        }
        if self.group.exhausted() {
            return Err(Error::PoolRestartLimit);
        }
        if self.client.info().accepting {
            return Ok(());
        }
        self.publisher.withdraw();
        if let Some(starting) = &self.starting {
            if !starting.thread.is_finished() {
                return Ok(());
            }
            let starting = self.starting.take().expect("completed pool constructor");
            let result = starting
                .thread
                .join()
                .unwrap_or(Err(pool::worker::Error::Panicked));
            // Keep the native worker-group permit through completion and publication.
            self.publish(result)?;
            return Ok(());
        }
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
            retired(&self.client)?;
        }
        if self.notices.len() >= self.maximum_notices {
            return Ok(());
        }
        if !permit.record() {
            return Err(Error::PoolRestartLimit);
        }
        let database = self.database.clone();
        let network = self.network.clone();
        let config = self.config.clone();
        let settings = self.settings;
        let gossip = self.gossip.clone();
        #[cfg(test)]
        let gate = self.restore_gate.take();
        // Restoring persisted membership can do substantial storage work. It
        // must not block conductor polling, epochs or independent consensus.
        match thread::Builder::new()
            .name("ae-pool-restore".into())
            .spawn(move || {
                #[cfg(test)]
                if let Some((started, release)) = gate {
                    let _ = started.send(());
                    release
                        .recv_timeout(std::time::Duration::from_secs(10))
                        .expect("restore gate");
                }
                pool::worker::Worker::spawn_with_cache(
                    database.upgrade().ok_or(pool::worker::Error::Stopped)?,
                    network,
                    config,
                    settings,
                    gossip,
                )
            }) {
            Ok(thread) => {
                self.starting = Some(Starting {
                    thread,
                    _permit: permit,
                })
            }
            Err(error) => self.publish(Err(pool::worker::Error::Spawn(error.to_string())))?,
        }
        Ok(())
    }
    fn publish(
        &mut self,
        result: Result<pool::worker::Worker, pool::worker::Error>,
    ) -> Result<(), Error> {
        match result {
            Ok(worker) => {
                let generation = self.generation.checked_add(1).ok_or(Error::Configuration)?;
                self.client = worker.client();
                self.worker = Some(worker);
                self.generation = generation;
                self.publisher
                    .install(bindings::Pool::new(self.client.clone()), generation);
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
    pub fn take_notices(&mut self) -> Vec<Notice> {
        self.notices.drain(..).collect()
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
    pub fn shutdown(&mut self) -> Result<pool::worker::Info, pool::worker::Error> {
        self.request_shutdown();
        if let Some(starting) = self.starting.take() {
            let worker = starting
                .thread
                .join()
                .unwrap_or(Err(pool::worker::Error::Panicked))?;
            return worker.shutdown();
        }
        self.worker
            .take()
            .map_or_else(|| Ok(self.client.info()), pool::worker::Worker::shutdown)
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[cfg(test)]
mod tests;
