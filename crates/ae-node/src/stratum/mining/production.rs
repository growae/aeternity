// Continuous native Stratum work lifecycle. Database publication precedes
// notification; backpressure retains one exact undelivered publication.
use super::*;
use ae_storage::snapshots;
use ae_sync::chain::pow::{
    preparation,
    stratum::{Prepared, Preparer},
};
use ae_types::AccountPublicKey;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Inputs {
    pub snapshots: snapshots::Owner,
    pub preparation: ae_chain::repository::pow::key::Limits,
    pub miner: AccountPublicKey,
    pub beneficiary: AccountPublicKey,
    pub release_info: u32,
    pub retry: Duration,
    pub cleanup_interval: Duration,
    pub cache_age: Duration,
}
impl Inputs {
    pub fn new(
        snapshots: snapshots::Owner,
        miner: AccountPublicKey,
        beneficiary: AccountPublicKey,
    ) -> Self {
        Self {
            snapshots,
            preparation: Default::default(),
            miner,
            beneficiary,
            release_info: 0,
            retry: Duration::from_millis(100),
            cleanup_interval: Duration::from_secs(900),
            cache_age: Duration::from_secs(900),
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct Status {
    pub running: bool,
    pub waiting_for_services: bool,
    pub preparing: Option<preparation::Id>,
    pub published: u64,
    pub broadcasts: u64,
    pub queued: u64,
    pub closed_connections: u64,
    pub stale: u64,
    pub retries: u64,
    pub feed_gaps: u64,
    pub cleanups: u64,
    pub waiting_broadcast: bool,
    pub last_error: Option<String>,
    pub last: Option<journal::Publication>,
}
pub(super) struct Shared {
    stopped: AtomicBool,
    services: Mutex<Option<services::Client>>,
    status: Mutex<Status>,
}
impl Shared {
    pub(super) fn status(&self) -> Status {
        self.status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub(super) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }
    pub(super) fn attach(&self, services: services::Client) -> Result<(), Error> {
        if services.is_closed() || self.stopped.load(Ordering::Acquire) {
            return Err(Error::Configuration);
        }
        let mut current = self.services.lock().unwrap_or_else(|e| e.into_inner());
        if current.is_some() {
            return Err(Error::Configuration);
        }
        *current = Some(services);
        Ok(())
    }
}
#[derive(Debug)]
pub struct Report {
    pub status: Status,
    pub last_preparation: Option<Result<Prepared, ae_sync::chain::pow::stratum::Error>>,
    pub pending_publication: Option<journal::Publication>,
}
pub(super) struct Worker {
    pub shared: Arc<Shared>,
    builder: Preparer,
    config: Inputs,
    clock: Arc<dyn Clock>,
    task: Option<preparation::Task>,
    task_reset: u64,
    last: Option<Result<Prepared, ae_sync::chain::pow::stratum::Error>>,
    pending: Option<journal::Publication>,
    retry_at: Instant,
    cleanup_at: Instant,
    pub refresh_requested: bool,
}
impl Worker {
    pub fn new(admitter: &Admitter, config: Inputs, clock: Arc<dyn Clock>) -> Result<Self, Error> {
        if config.retry.is_zero()
            || config.retry > Duration::from_secs(3600)
            || config.cleanup_interval.is_zero()
            || config.cleanup_interval > Duration::from_secs(86400)
            || config.cache_age.is_zero()
            || config.cache_age > Duration::from_secs(86400)
        {
            return Err(Error::Configuration);
        }
        let builder = admitter
            .preparer(config.snapshots.clone(), config.preparation)
            .map_err(Error::PreparationSetup)?;
        let cleanup_at = Instant::now() + config.cleanup_interval;
        Ok(Self {
            shared: Arc::new(Shared {
                stopped: AtomicBool::new(false),
                services: Mutex::new(None),
                status: Mutex::new(Status {
                    running: true,
                    waiting_for_services: true,
                    ..Default::default()
                }),
            }),
            builder,
            config,
            clock,
            task: None,
            task_reset: 0,
            last: None,
            pending: None,
            retry_at: Instant::now(),
            cleanup_at,
            refresh_requested: true,
        })
    }
    fn update(&self, f: impl FnOnce(&mut Status)) {
        f(&mut self.shared.status.lock().unwrap_or_else(|e| e.into_inner()));
    }
    fn backoff(&mut self, message: String) {
        let mut end = message.len().min(4096);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        self.update(|s| {
            s.retries += 1;
            s.last_error = Some(message[..end].to_owned());
        });
        self.retry_at = Instant::now() + self.config.retry;
    }
    fn millis(&self) -> Result<u64, Error> {
        u64::try_from(self.clock.unix_millis())
            .map_err(|_| Error::Mining("production clock outside u64"))
    }
    fn date(&self) -> Result<Date, Error> {
        Date::from_unix_millis(self.millis()?).map_err(|e| Error::Storage(e.into()))
    }
    pub fn stop(&mut self) {
        self.shared.stop();
        if let Some(task) = &self.task {
            task.request_cancel();
        }
    }
    pub fn tick(
        &mut self,
        writer: &mut storage::Writer,
        epoch: &journal::Epoch,
        limits: journal::Limits,
        frame: journal::Frame,
        enabled: bool,
    ) -> Result<(), Error> {
        if !enabled {
            self.stop();
        }
        let running = !self.shared.stopped.load(Ordering::Acquire);
        self.update(|s| {
            s.running = running;
            s.feed_gaps += u64::from(frame.gap);
        });
        if let Some(task) = &self.task
            && (!running || frame.reset_revision != self.task_reset)
        {
            task.request_cancel();
        }
        let usable = running && self.task_reset == frame.reset_revision;
        let completion = self.task.as_mut().and_then(preparation::Task::try_result);
        if let Some(done) = completion {
            self.task.take();
            self.update(|s| s.preparing = None);
            self.last = Some(Prepared::from_completion(done));
            match self.last.as_ref().expect("completed preparation") {
                Ok(prepared) if usable && frame.top == Some(prepared.parent()) => {
                    let sequence = frame
                        .info
                        .last
                        .as_ref()
                        .map_or(Some(1), |p| p.sequence.checked_add(1))
                        .ok_or(Error::Mining("production sequence exhausted"))?;
                    let candidate = prepared.candidate().as_ref().clone();
                    let date = self.date()?;
                    let publication = retry(writer, |w| {
                        w.publish_work(epoch, sequence, candidate.clone(), date, limits)
                    })?;
                    self.update(|s| {
                        s.last = Some(publication.clone());
                        s.published += u64::from(publication.published);
                    });
                    self.refresh_requested = false;
                    if publication.published {
                        self.pending = Some(publication);
                    } else {
                        self.update(|s| s.stale += 1);
                    }
                }
                Ok(_) => self.update(|s| s.stale += 1),
                Err(error) if running => self.backoff(format!("{error:?}")),
                Err(_) => {}
            }
        }
        if !running {
            return Ok(());
        }
        if Instant::now() >= self.cleanup_at {
            let before = self
                .millis()?
                .saturating_sub(self.config.cache_age.as_millis() as u64);
            let before = Date::from_unix_millis(before).map_err(|e| Error::Storage(e.into()))?;
            retry(writer, |w| w.cleanup_work(epoch, before, limits))?;
            self.cleanup_at = Instant::now() + self.config.cleanup_interval;
            self.update(|s| s.cleanups += 1);
        }
        let services = self
            .shared
            .services
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        self.update(|s| s.waiting_for_services = services.is_none());
        let Some(services) = services else {
            return Ok(());
        };
        if Instant::now() < self.retry_at {
            return Ok(());
        }
        if let Some(publication) = &self.pending {
            // Re-read after publication and after any pressure delay. The
            // cached last receipt alone cannot revive retired active work.
            let current = retry(writer, |w| w.refresh_work(epoch, limits))?;
            if !current.info.active.contains(&publication.candidate) {
                self.pending.take();
                self.update(|s| s.stale += 1);
            } else {
                let work = publication
                    .candidate
                    .work()
                    .map_err(|_| Error::Mining("production work encoding"))?;
                if self.shared.stopped.load(Ordering::Acquire) {
                    return Ok(());
                }
                match services.broadcast(ae_stratum::session::Chain::Block(work)) {
                    Ok(result) => {
                        self.pending.take();
                        self.update(|s| {
                            s.broadcasts += 1;
                            s.queued += result.queued as u64;
                            s.closed_connections += result.closed as u64;
                            s.last_error = None;
                        });
                    }
                    Err(services::Error::Busy | services::Error::Limit) => {
                        self.backoff("Stratum notification capacity".into());
                        self.update(|s| s.waiting_broadcast = true);
                        return Ok(());
                    }
                    Err(_) if self.shared.stopped.load(Ordering::Acquire) => return Ok(()),
                    Err(error) => return Err(Error::Services(error)),
                }
            }
            self.update(|s| s.waiting_broadcast = self.pending.is_some());
            // The frame predates this publication. Obtain a fresh frame on
            // the next tick instead of launching a duplicate preparation.
            return Ok(());
        }
        if self.task.is_some() || frame.info.counts[0] != 0 || frame.info.counts[1] != 0 {
            return Ok(());
        }
        let needs = frame.info.active.is_empty()
            || (self.refresh_requested
                && frame
                    .info
                    .active
                    .first()
                    .is_some_and(|c| Some(c.parent()) != frame.top));
        if !needs {
            self.refresh_requested = false;
            return Ok(());
        }
        let Some(parent) = frame.top else {
            return Ok(());
        };
        let request = preparation::Request {
            parent,
            miner: self.config.miner,
            beneficiary: self.config.beneficiary,
            release_info: self.config.release_info,
        };
        match self.builder.start(request, self.clock.clone()) {
            Ok(task) => {
                self.update(|s| s.preparing = Some(task.id()));
                self.task_reset = frame.reset_revision;
                self.task = Some(task);
            }
            Err(error) => self.backoff(format!("{error:?}")),
        }
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn task_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_some_and(preparation::Task::is_finished)
    }
    pub fn finish(mut self) -> Report {
        self.stop();
        if let Some(task) = self.task.take()
            && let Some(done) = task.wait()
        {
            self.last = Some(Prepared::from_completion(done));
        }
        self.update(|s| {
            s.running = false;
            s.preparing = None;
            s.waiting_broadcast = self.pending.is_some();
        });
        Report {
            status: self.shared.status(),
            last_preparation: self.last.take(),
            pending_publication: self.pending.take(),
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.stop(); /* Task Drop cancels and joins before releasing snapshots. */
    }
}
