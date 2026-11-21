use ae_chain::{
    publication::Payload,
    repository::{Status, Update},
};
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub records: usize,
    pub bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            records: 1024,
            bytes: 16 << 20,
        }
    }
}
impl Config {
    pub(crate) fn validate(self) -> Result<(), super::Error> {
        if self.records == 0 || self.records > 65_536 || self.bytes == 0 || self.bytes > 1 << 30 {
            Err(super::Error::ResourceLimit)
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone)]
pub struct Cursor {
    owner: Arc<()>,
    revision: u64,
}
impl Cursor {
    pub fn revision(&self) -> u64 {
        self.revision
    }
}
impl PartialEq for Cursor {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.owner, &other.owner) && self.revision == other.revision
    }
}
impl Eq for Cursor {}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    UncertainWrite,
    Discontinuity,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    WrongSource,
    Ahead,
    Limit,
    Gap { missed: u64, resume: Cursor },
    Failed(Failure),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone)]
pub struct Info {
    pub status: Status,
    pub records: usize,
    pub bytes: usize,
    pub evicted: u64,
    pub unretained: u64,
    pub closed: bool,
    pub failure: Option<Failure>,
    pub limits: Config,
}
#[derive(Debug)]
pub struct Batch {
    pub records: Vec<Arc<Payload>>,
    pub cursor: Cursor,
    pub closed: bool,
}
struct State {
    info: Info,
    records: VecDeque<Arc<Payload>>,
}
struct Shared {
    owner: Arc<()>,
    state: Mutex<State>,
    changed: Condvar,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn cursor(&self, revision: u64) -> Cursor {
        Cursor {
            owner: self.owner.clone(),
            revision,
        }
    }
    fn oldest(&self, state: &State) -> Cursor {
        self.cursor(
            state
                .records
                .front()
                .map_or(state.info.status.revision, |r| r.change().revision - 1),
        )
    }
    fn check(&self, state: &State, cursor: &Cursor) -> Result<(), Error> {
        if !Arc::ptr_eq(&cursor.owner, &self.owner) {
            return Err(Error::WrongSource);
        }
        if let Some(error) = state.info.failure {
            return Err(Error::Failed(error));
        }
        if cursor.revision > state.info.status.revision {
            return Err(Error::Ahead);
        }
        let resume = self.oldest(state);
        if cursor.revision < resume.revision {
            return Err(Error::Gap {
                missed: resume.revision - cursor.revision,
                resume,
            });
        }
        Ok(())
    }
    fn read(&self, state: &State, cursor: &Cursor, limit: usize) -> Result<Batch, Error> {
        if limit == 0 || limit > 65_536 {
            return Err(Error::Limit);
        }
        self.check(state, cursor)?;
        let records: Vec<_> = state
            .records
            .iter()
            .filter(|r| r.change().revision > cursor.revision)
            .take(limit)
            .cloned()
            .collect();
        let revision = records
            .last()
            .map_or(cursor.revision, |r| r.change().revision);
        Ok(Batch {
            records,
            cursor: self.cursor(revision),
            closed: state.info.closed,
        })
    }
}
// Exactly one source per opened database. Readers retain only transient
// records, never the database or its writer lock. Reopen creates a new owner.
pub(crate) struct Source {
    shared: Arc<Shared>,
    config: Config,
}
impl Source {
    pub fn new(status: Status, config: Config) -> Self {
        Self {
            config,
            shared: Arc::new(Shared {
                owner: Arc::new(()),
                state: Mutex::new(State {
                    info: Info {
                        status,
                        records: 0,
                        bytes: 0,
                        evicted: 0,
                        unretained: 0,
                        closed: false,
                        failure: None,
                        limits: config,
                    },
                    records: VecDeque::new(),
                }),
                changed: Condvar::new(),
            }),
        }
    }
    pub fn reader(&self) -> Reader {
        Reader {
            shared: self.shared.clone(),
        }
    }
    pub fn prepare(&self, update: &Update) -> Option<Arc<Payload>> {
        Payload::capture(update, self.config.bytes).map(Arc::new)
    }
    pub fn fail(&self, failure: Failure) {
        let mut state = self.shared.lock();
        state.info.failure.get_or_insert(failure);
        state.info.closed = true;
        self.shared.changed.notify_all();
    }
    pub fn publish(&self, update: &Update, payload: Option<Arc<Payload>>) {
        let mut state = self.shared.lock();
        if state.info.closed {
            return;
        }
        if state.info.status != update.expected() {
            state.info.failure = Some(Failure::Discontinuity);
            state.info.closed = true;
            self.shared.changed.notify_all();
            return;
        }
        state.info.status = Status {
            revision: update.change().revision,
            tip: Some(update.change().new_tip),
        };
        if let Some(payload) = payload {
            let bytes = payload.retained_bytes();
            while state.records.len() >= self.config.records
                || state.info.bytes > self.config.bytes - bytes
            {
                let old = state
                    .records
                    .pop_front()
                    .expect("record fits source budget");
                state.info.bytes -= old.retained_bytes();
                state.info.evicted = state.info.evicted.saturating_add(1);
            }
            state.info.bytes += bytes;
            state.records.push_back(payload);
        } else {
            state.info.evicted = state
                .info
                .evicted
                .saturating_add(state.records.len() as u64);
            state.info.unretained = state.info.unretained.saturating_add(1);
            state.records.clear();
            state.info.bytes = 0;
        }
        state.info.records = state.records.len();
        self.shared.changed.notify_all();
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        self.shared.lock().info.closed = true;
        self.shared.changed.notify_all();
    }
}
#[derive(Clone)]
pub struct Reader {
    shared: Arc<Shared>,
}
impl Reader {
    pub fn info(&self) -> Info {
        self.shared.lock().info.clone()
    }
    pub fn cursor(&self) -> Cursor {
        self.shared.cursor(self.shared.lock().info.status.revision)
    }
    pub fn oldest(&self) -> Cursor {
        self.shared.oldest(&self.shared.lock())
    }
    pub fn after(&self, cursor: &Cursor, limit: usize) -> Result<Batch, Error> {
        self.shared.read(&self.shared.lock(), cursor, limit)
    }
    pub fn wait(&self, cursor: &Cursor, limit: usize, timeout: Duration) -> Result<Batch, Error> {
        if limit == 0 || limit > 65_536 || Instant::now().checked_add(timeout).is_none() {
            return Err(Error::Limit);
        }
        let (state, _) = self
            .shared
            .changed
            .wait_timeout_while(self.shared.lock(), timeout, |state| {
                !state.info.closed
                    && self.shared.check(state, cursor).is_ok()
                    && state.info.status.revision == cursor.revision
            })
            .unwrap_or_else(|e| e.into_inner());
        self.shared.read(&state, cursor, limit)
    }
}

#[cfg(test)]
#[path = "events/tests.rs"]
mod tests;
