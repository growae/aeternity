use crate::chain::sync::{self, lifecycle::PeerDone};
use ae_storage::{Database, gc::worker};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, time::Instant};

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub poll_interval: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Closed,
    SyncClosed,
    GcClosed,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone, Default)]
pub struct Status {
    pub running: bool,
    pub joined: bool,
    pub cause: Option<PeerDone>,
    pub notifications: u64,
    pub gc: Option<worker::Info>,
    pub last_error: Option<Error>,
}
#[derive(Clone)]
pub struct Handle {
    stop: watch::Sender<bool>,
    status: watch::Receiver<Status>,
}
impl Handle {
    pub fn shutdown(&self) {
        self.stop.send_replace(true);
    }
    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }
    pub async fn changed(&mut self) -> Result<Status, Error> {
        self.status.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.status.borrow_and_update().clone())
    }
}
pub struct Service {
    sync: sync::Handle,
    gc: crate::bindings::GarbageCollection,
    lease: Option<Arc<()>>,
    config: Config,
    stopped: watch::Receiver<bool>,
    published: watch::Sender<Status>,
    current: Status,
}
pub fn create(
    database: &Arc<Database>,
    sync: sync::Handle,
    gc: worker::Client,
    config: Config,
) -> Result<(Handle, Service), Error> {
    if !gc.uses_database(database) || gc.info().is_none_or(|i| !i.running) {
        return Err(Error::Configuration);
    }
    create_bound(
        database,
        sync,
        crate::bindings::GarbageCollection::fixed(database, gc),
        config,
    )
}
pub(crate) fn create_bound(
    database: &Arc<Database>,
    sync: sync::Handle,
    gc: crate::bindings::GarbageCollection,
    config: Config,
) -> Result<(Handle, Service), Error> {
    if config.poll_interval.is_zero()
        || Instant::now().checked_add(config.poll_interval).is_none()
        || !sync.uses_database(database)
        || !gc.uses_database(database)
    {
        return Err(Error::Configuration);
    }
    let current = Status {
        running: true,
        ..Default::default()
    };
    let (published, status) = watch::channel(current.clone());
    let (stop, stopped) = watch::channel(false);
    Ok((
        Handle { stop, status },
        Service {
            sync,
            gc,
            lease: None,
            config,
            stopped,
            published,
            current,
        },
    ))
}
impl Service {
    fn reconcile(&mut self) -> Result<(), Error> {
        let source = self.sync.status();
        if !source.running {
            return Err(Error::SyncClosed);
        }
        let snapshot = self.gc.snapshot();
        if snapshot.closed {
            return Err(Error::GcClosed);
        }
        let Some(client) = snapshot.value else {
            self.current.gc = None;
            return Ok(());
        };
        let info = client.info();
        if !snapshot.available || info.as_ref().is_none_or(|i| !i.running) {
            self.current.gc = info;
            return if self.gc.managed() {
                Ok(())
            } else {
                Err(Error::GcClosed)
            };
        }
        let info = info.expect("running GC");
        if self
            .lease
            .as_ref()
            .is_none_or(|old| !Arc::ptr_eq(old, &snapshot.lease))
        {
            self.current.cause = None;
            self.lease = Some(snapshot.lease);
        }
        self.current.gc = Some(info);
        if self.current.cause.is_none()
            && let Some(cause) = source.lifecycle.gc_ready
        {
            // Readiness was latched at a real completion with no active task.
            // Neither diagnostic overflow nor a late subscription may lose it.
            // Polling idle/100% progress must never manufacture this signal.
            if !client.on_sync_done(false) {
                return if self.gc.managed() {
                    Ok(())
                } else {
                    Err(Error::GcClosed)
                };
            }
            self.current.notifications += 1;
            self.current.cause = Some(cause);
            self.current.gc = client.info();
        }
        Ok(())
    }
    pub async fn run(mut self) -> Status {
        let mut interval = tokio::time::interval(self.config.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if *self.stopped.borrow() {
                break;
            }
            if let Err(error) = self.reconcile() {
                self.current.last_error = Some(error);
                break;
            }
            self.published.send_replace(self.current.clone());
            tokio::select! {
                _=self.stopped.changed()=>break,
                result=self.sync.changed()=>if result.is_err() { self.current.last_error=Some(Error::SyncClosed);break; },
                _=interval.tick()=>(),
                _=self.gc.changed()=>(),
            }
        }
        self.current.running = false;
        self.current.joined = true;
        self.published.send_replace(self.current.clone());
        self.current.clone()
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        if !self.current.joined {
            self.current.running = false;
            self.current.last_error.get_or_insert(Error::Closed);
            self.published.send_replace(self.current.clone());
        }
    }
}
