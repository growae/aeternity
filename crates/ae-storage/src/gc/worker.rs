use super::*;
use std::{
    sync::Condvar,
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct Info {
    pub version: u64,
    pub running: bool,
    pub enabled: bool,
    pub synced: bool,
    pub faulted: bool,
    pub failures: u8,
    pub pages: u64,
    pub observed_revision: u64,
    pub state: State,
    pub last_error: Option<String>,
}
impl Info {
    fn set_enabled(&mut self, enabled: bool) -> bool {
        let prior = self.enabled;
        self.enabled = enabled;
        if enabled && self.faulted {
            self.faulted = false;
            self.failures = 0;
        }
        prior
    }
    fn sync_done(&mut self, sync_in_progress: bool) {
        self.synced |= !sync_in_progress;
    }
    fn failed(&mut self, error: String) {
        self.failures = self.failures.saturating_add(1).min(2);
        self.last_error = Some(error);
        if self.failures == 2 {
            self.enabled = false;
            self.faulted = true;
        }
    }
}
fn schedule_baseline(state: &State, height: u64) -> u64 {
    if state.epoch == 0 { height } else { 0 }
}
struct Control {
    info: Info,
    pending: bool,
    stop: bool,
}
pub(crate) struct Shared {
    control: Mutex<Control>,
    scheduling: Mutex<()>,
    changed: Condvar,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Control> {
        self.control.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn signal(&self, control: &mut Control) {
        control.info.version = control.info.version.wrapping_add(1);
        self.changed.notify_all();
    }
    fn wake(&self) {
        let mut control = self.lock();
        control.pending = true;
        self.changed.notify_all();
    }
}

// One owned worker per database. Dropping the handle joins the thread; dropping
// or disabling a worker never cancels durable collection or resets retention.
pub struct Worker {
    database: std::sync::Weak<Database>,
    shared: Arc<Shared>,
    join: Option<JoinHandle<()>>,
}
// A client identifies this exact worker lifetime without keeping its thread
// alive or silently following a replacement registered on the same database.
#[derive(Clone)]
pub struct Client {
    database: std::sync::Weak<Database>,
    shared: std::sync::Weak<Shared>,
}
impl Client {
    pub fn uses_database(&self, db: &Arc<Database>) -> bool {
        self.database.ptr_eq(&Arc::downgrade(db))
    }
    pub fn info(&self) -> Option<Info> {
        self.shared.upgrade().map(|s| s.lock().info.clone())
    }
    pub fn on_sync_done(&self, sync_in_progress: bool) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        let mut control = shared.lock();
        if control.stop || !control.info.running {
            return false;
        }
        control.info.sync_done(sync_in_progress);
        control.pending = true;
        shared.signal(&mut control);
        true
    }
}
impl Worker {
    pub fn client(&self) -> Client {
        Client {
            database: self.database.clone(),
            shared: Arc::downgrade(&self.shared),
        }
    }
    pub fn spawn(db: Arc<Database>, config: Config, limits: Limits) -> Result<Self, Error> {
        config.validate()?;
        if limits.rows == 0 || limits.bytes == 0 {
            return Err(Error::ResourceLimit);
        }
        let view = db.view()?;
        let state = view.retention()?;
        // The native first startup starts its history interval at the current
        // height when no collection has ever been recorded. This is scheduling
        // state only: it must not claim that history has already been removed.
        let baseline = schedule_baseline(
            &state,
            view.status()
                .tip
                .map(|hash| view.require_block(hash).map(|b| b.header().common().height))
                .transpose()?
                .unwrap_or(0),
        );
        let shared = Arc::new(Shared {
            control: Mutex::new(Control {
                info: Info {
                    version: 0,
                    running: true,
                    enabled: config.enabled,
                    synced: false,
                    faulted: false,
                    failures: 0,
                    pages: 0,
                    observed_revision: view.status().revision,
                    state,
                    last_error: None,
                },
                pending: true,
                stop: false,
            }),
            scheduling: Mutex::new(()),
            changed: Condvar::new(),
        });
        drop(view);
        let mut registration = db.gc_worker.lock().map_err(|_| backend_lock())?;
        if registration
            .upgrade()
            .is_some_and(|s| s.lock().info.running)
        {
            return Err(Error::GcActive);
        }
        *registration = Arc::downgrade(&shared);
        let thread_shared = shared.clone();
        let thread_db = db.clone();
        let join = thread::Builder::new()
            .name("ae-state-gc".into())
            .spawn(move || run(thread_db, thread_shared, config, limits, baseline))
            .map_err(|e| {
                shared.lock().info.running = false;
                Error::Backend(e.to_string())
            })?;
        Ok(Self {
            database: Arc::downgrade(&db),
            shared,
            join: Some(join),
        })
    }
    pub fn info(&self) -> Info {
        self.shared.lock().info.clone()
    }
    // Serialize with cycle starts: after disabling returns, later commits cannot
    // start collection using an earlier enabled snapshot. Active work drains.
    pub fn set_enabled(&self, enabled: bool) -> bool {
        let _scheduling = self
            .shared
            .scheduling
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut control = self.shared.lock();
        let prior = control.info.set_enabled(enabled);
        control.pending = true;
        self.shared.signal(&mut control);
        prior
    }
    // Call after a chain_sync_done notification, with the synchronization
    // service's current progress. A later syncing episode cannot undo the latch.
    pub fn on_sync_done(&self, sync_in_progress: bool) {
        let mut control = self.shared.lock();
        control.info.sync_done(sync_in_progress);
        control.pending = true;
        self.shared.signal(&mut control);
    }
    // Latest-state notifications are bounded and coalesced. Callers use version
    // to avoid a race between reading info and waiting; no unbounded event queue.
    pub fn wait_for_change(&self, version: u64, timeout: Duration) -> Info {
        let (control, _) = self
            .shared
            .changed
            .wait_timeout_while(self.shared.lock(), timeout, |c| {
                c.info.running && c.info.version == version
            })
            .unwrap_or_else(|e| e.into_inner());
        control.info.clone()
    }
    pub fn shutdown(mut self) -> Result<Info, Error> {
        self.stop()?;
        Ok(self.info())
    }
    pub fn request_shutdown(&self) {
        let mut control = self.shared.lock();
        control.stop = true;
        self.shared.changed.notify_all();
    }
    fn stop(&mut self) -> Result<(), Error> {
        self.request_shutdown();
        if let Some(join) = self.join.take() {
            join.join()
                .map_err(|_| Error::Backend("GC worker panicked".into()))?;
        }
        Ok(())
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
impl Database {
    pub(crate) fn wake_gc(&self) {
        // A notification cannot turn an acknowledged durable commit into an
        // error. Poison recovery is safe here: only an optional weak handle is
        // stored under this mutex, never chain or retention state.
        let shared = self
            .gc_worker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade();
        if let Some(shared) = shared {
            shared.wake();
        }
    }
}
struct Running(Arc<Shared>);
impl Drop for Running {
    fn drop(&mut self) {
        let mut control = self.0.lock();
        control.info.running = false;
        if thread::panicking() {
            control.info.faulted = true;
            control.info.last_error = Some("GC worker panicked".into());
        }
        self.0.signal(&mut control);
    }
}
fn observe(db: &Database) -> Result<(Status, State), Error> {
    let view = db.view()?;
    Ok((view.status(), view.retention()?))
}
fn run(db: Arc<Database>, shared: Arc<Shared>, config: Config, limits: Limits, baseline: u64) {
    let _running = Running(shared.clone());
    loop {
        let (enabled, faulted) = {
            let mut control = shared
                .changed
                .wait_while(shared.lock(), |c| !c.pending && !c.stop)
                .unwrap_or_else(|e| e.into_inner());
            if control.stop {
                return;
            }
            control.pending = false;
            (control.info.enabled, control.info.faulted)
        };
        let result = observe(&db).and_then(|(status, state)| {
            {
                let mut control = shared.lock();
                if state.epoch != control.info.state.epoch && !control.info.faulted {
                    control.info.failures = 0;
                }
                control.info.observed_revision = status.revision;
                control.info.state = state.clone();
            }
            if faulted {
                Ok(false)
            } else if state.active() {
                db.gc_step(limits)?;
                Ok(true)
            } else if enabled {
                let _scheduling = shared.scheduling.lock().unwrap_or_else(|e| e.into_inner());
                let (enabled, synced, allowed) = {
                    let control = shared.lock();
                    (
                        control.info.enabled,
                        control.info.synced,
                        !control.stop && !control.info.faulted,
                    )
                };
                if enabled && allowed {
                    db.begin_gc_at(
                        config.trees,
                        Some((Config { enabled, ..config }, synced)),
                        Some(baseline),
                        || Ok(()),
                    )?;
                }
                Ok(false)
            } else {
                Ok(false)
            }
        });
        let observed = observe(&db);
        let mut control = shared.lock();
        if let Ok((status, state)) = &observed {
            control.info.observed_revision = status.revision;
            control.info.state = state.clone();
        }
        match result.and_then(|page| observed.map(|_| page)) {
            Ok(page) => {
                control.info.pages = control.info.pages.saturating_add(u64::from(page));
                if !control.info.state.active() && !control.info.faulted {
                    control.info.failures = 0;
                    control.info.last_error = None;
                }
                control.pending |= control.info.state.active() && !control.info.faulted;
            }
            Err(error) => {
                control.info.failed(format!("{error:?}"));
                if !control.info.faulted {
                    control.pending = true;
                }
            }
        }
        shared.signal(&mut control);
        drop(control);
        thread::yield_now();
    }
}

#[cfg(test)]
mod tests;
