// aec_consensus_sup:init/1: permanent one_for_one children, 3 / 30 seconds.
// Explicit operational work-stops remain failures requiring reconciliation;
// normal process exits and panics are eligible for replacement after join.
use super::*;
use crate::runtime::restart::Window;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Status {
    pub pinning_generation: u64,
    pub connector_generation: u64,
    pub cache_generation: u64,
    pub eoe_generation: u64,
    pub key_generation: u64,
    pub attempts: u64,
    pub restarting: bool,
    pub pinning_restarting: bool,
    pub connector_restarting: bool,
    pub cache_restarting: bool,
    pub eoe_restarting: bool,
    pub keys_restarting: bool,
    pub pending_notices: usize,
}

mod cache_supervision;
mod eoe_supervision;
mod key_supervision;
#[cfg(test)]
mod tests;
pub use cache_supervision::{CacheExit, RetiredTop};
pub(super) use cache_supervision::{CacheFactory, CacheInputs};
pub use eoe_supervision::{EoeExit, EoeSettlement};

#[derive(Debug)]
pub enum Notice {
    KeysRetired {
        generation: u64,
        info: crate::preset_keys::Info,
        previous_failure: Option<owner::Error>,
    },
    KeysRestarted {
        generation: u64,
    },
    KeysStartFailed {
        attempt: u64,
        error: owner::Error,
    },
    EoeRetired {
        generation: u64,
        child: Box<EoeExit>,
    },
    EoeRestarted {
        generation: u64,
    },
    EoeStartFailed {
        attempt: u64,
        error: owner::Error,
    },
    CacheRetired {
        generation: u64,
        child: Box<CacheExit>,
    },
    CacheRestarted {
        generation: u64,
        revision: u64,
    },
    CacheStartFailed {
        attempt: u64,
        error: Error,
    },
    ConnectorRetired {
        generation: u64,
        child: connector::Retired,
    },
    ConnectorRestarted {
        generation: u64,
    },
    ConnectorStartFailed {
        attempt: u64,
        error: String,
    },
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
    pub retiring_eoe: Option<EoeSettlement>,
}
pub(super) struct ConnectorFactory {
    // Fields drop in order: discard the callable's backend reference before
    // releasing its final owned reference on the joined resource-owner thread.
    spawn: Box<dyn Fn() -> std::io::Result<connector::Runtime> + Send + Sync>,
    link: cache::ConnectorLink,
    client: connector::Client,
    _backend: BackendOwner,
}
struct BackendOwner {
    release: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl BackendOwner {
    fn start(backend: Arc<impl Backend>) -> std::io::Result<Self> {
        let (release, wait) = std::sync::mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("ae-parent-backend-owner".into())
            .spawn(move || {
                let _ = wait.recv();
                // HTTP backends own Tokio runtimes. Their final destructor
                // must run outside an async context, including worker gaps.
                drop(backend);
            })?;
        Ok(Self {
            release: Some(release),
            thread: Some(thread),
        })
    }
}
impl Drop for BackendOwner {
    fn drop(&mut self) {
        self.release.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
impl ConnectorFactory {
    pub(super) fn start(
        backend: Arc<impl Backend>,
        hosts: usize,
        seed: [u8; 16],
        sink: impl connector::Sink + Clone + Sync,
        config: connector::Config,
    ) -> std::io::Result<(connector::Runtime, Self)> {
        let owner = BackendOwner::start(backend.clone())?;
        let spawn = Box::new(move || {
            connector::Runtime::spawn(backend.clone(), hosts, seed, sink.clone(), config)
        });
        let child = spawn()?;
        let client = child.client();
        Ok((
            child,
            Self {
                spawn,
                link: cache::ConnectorLink::new(client.clone()),
                client,
                _backend: owner,
            },
        ))
    }
    pub(super) fn link(&self) -> cache::ConnectorLink {
        self.link.clone()
    }
}
pub(super) struct Supervisor<O> {
    factory: Option<pinning::Factory<O>>,
    connector: ConnectorFactory,
    cache: Option<CacheFactory>,
    budget: Window<3, 30>,
    origin: Option<Instant>,
    status: Status,
    notices: VecDeque<Notice>,
    limit: usize,
    retiring_eoe: Option<EoeSettlement>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Supervisor<O> {
    pub(super) fn new(
        factory: Option<pinning::Factory<O>>,
        connector: ConnectorFactory,
        cache: Option<CacheFactory>,
        limit: usize,
    ) -> Self {
        Self {
            status: Status {
                pinning_generation: u64::from(factory.is_some()),
                connector_generation: 1,
                cache_generation: u64::from(cache.is_some()),
                eoe_generation: 1,
                key_generation: 1,
                ..Default::default()
            },
            factory,
            connector,
            cache,
            budget: Default::default(),
            origin: None,
            notices: VecDeque::new(),
            limit,
            retiring_eoe: None,
        }
    }
    pub(super) fn status(&self) -> Status {
        Status {
            restarting: self.status.pinning_restarting
                || self.status.connector_restarting
                || self.status.cache_restarting
                || self.status.eoe_restarting
                || self.status.keys_restarting,
            pending_notices: self.notices.len(),
            ..self.status.clone()
        }
    }
    // During a replacement gap, callers receive the closed old generation.
    // Once published, new callers receive the new one; old clones never move.
    pub(super) fn connector(&self) -> connector::Client {
        self.connector.client.clone()
    }
    pub(super) fn poll(
        &mut self,
        child: &mut Option<pinning::Service>,
        connector: &mut Option<connector::Runtime>,
        now: Instant,
    ) -> Result<(), Error> {
        let origin = *self.origin.get_or_insert(now);
        let second = now.saturating_duration_since(origin).as_secs();
        self.poll_connector(connector, second)?;
        self.poll_pinning(child, second)
    }
    fn record(&mut self, second: u64) -> Result<(), Error> {
        self.status.attempts = self
            .status
            .attempts
            .checked_add(1)
            .ok_or(Error::Configuration)?;
        if self.budget.record(second) {
            Ok(())
        } else {
            Err(Error::ConsensusRestartLimit)
        }
    }
    fn poll_connector(
        &mut self,
        child: &mut Option<connector::Runtime>,
        second: u64,
    ) -> Result<(), Error> {
        if let Some(service) = child.as_ref() {
            self.status.connector_restarting = service.replacement_pending();
        }
        if self.notices.len() >= self.limit {
            return Ok(());
        }
        if let Some(service) = child
            && service.is_finished()
            && service.replacement_pending()
        {
            // Detach routing before joining. Requests accepted by the old
            // mailbox are completed/closed there and are not submitted again.
            self.connector.link.replace(None);
            let retired = child.take().expect("finished connector").retire();
            self.notices.push_back(Notice::ConnectorRetired {
                generation: self.status.connector_generation,
                child: retired,
            });
            self.status.connector_restarting = true;
        }
        if child.is_some() || !self.status.connector_restarting || self.notices.len() >= self.limit
        {
            return Ok(());
        }
        self.record(second)?;
        let replacement =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.connector.spawn)()))
                .map_err(|_| "parent connector constructor panicked".to_string())
                .and_then(|r| r.map_err(|e| e.to_string()));
        match replacement {
            Ok(service) => {
                self.status.connector_generation = self
                    .status
                    .connector_generation
                    .checked_add(1)
                    .ok_or(Error::Configuration)?;
                self.connector.client = service.client();
                self.connector
                    .link
                    .replace(Some(self.connector.client.clone()));
                *child = Some(service);
                self.status.connector_restarting = false;
                self.notices.push_back(Notice::ConnectorRestarted {
                    generation: self.status.connector_generation,
                });
            }
            Err(error) => self.notices.push_back(Notice::ConnectorStartFailed {
                attempt: self.status.attempts,
                error,
            }),
        }
        Ok(())
    }
    fn poll_pinning(
        &mut self,
        child: &mut Option<pinning::Service>,
        second: u64,
    ) -> Result<(), Error> {
        if self.factory.is_none() {
            return Ok(());
        }
        if let Some(service) = child.as_ref() {
            self.status.pinning_restarting = service.replacement_pending();
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
            self.status.pinning_restarting = true;
        }
        if child.is_some() || !self.status.pinning_restarting || self.notices.len() >= self.limit {
            return Ok(());
        }
        self.record(second)?;
        let factory = self.factory.as_ref().expect("pinning factory");
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
                self.status.pinning_restarting = false;
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
        self.connector.link.replace(None);
        if let Some(cache) = &self.cache {
            cache.route.replace(None);
        }
        Shutdown {
            status,
            notices,
            retiring_eoe: self.retiring_eoe.take(),
        }
    }
}
