use ae_chain::repository::{
    Status,
    retention::{Phase, State},
};
use std::{
    collections::VecDeque,
    mem::size_of,
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
            bytes: 1 << 20,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Switch,
    ScansComplete,
    // Rust's physical deletion/cancellation have separate operational effects.
    CollectionComplete,
    Cancelled,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub sequence: u64,
    pub kind: Kind,
    pub chain: Status,
    pub state: State,
}
impl Record {
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + 64 + self.state.cursor.len()
    }
}
#[derive(Debug, Clone)]
pub struct Cursor {
    owner: Arc<()>,
    sequence: u64,
}
impl Cursor {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}
impl PartialEq for Cursor {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.owner, &other.owner) && self.sequence == other.sequence
    }
}
impl Eq for Cursor {}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    UncertainWrite,
    Discontinuity,
    Sequence,
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
    pub state: State,
    // Revision observed at the last GC mutation, not necessarily the latest tip.
    pub chain: Status,
    pub sequence: u64,
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
    pub records: Vec<Arc<Record>>,
    pub cursor: Cursor,
    pub closed: bool,
}
struct History {
    info: Info,
    records: VecDeque<Arc<Record>>,
}
struct Shared {
    owner: Arc<()>,
    state: Mutex<History>,
    changed: Condvar,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, History> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn cursor(&self, sequence: u64) -> Cursor {
        Cursor {
            owner: self.owner.clone(),
            sequence,
        }
    }
    fn oldest(&self, state: &History) -> Cursor {
        self.cursor(
            state
                .records
                .front()
                .map_or(state.info.sequence, |r| r.sequence - 1),
        )
    }
    fn check(&self, state: &History, cursor: &Cursor) -> Result<(), Error> {
        if !Arc::ptr_eq(&cursor.owner, &self.owner) {
            return Err(Error::WrongSource);
        }
        if let Some(failure) = state.info.failure {
            return Err(Error::Failed(failure));
        }
        if cursor.sequence > state.info.sequence {
            return Err(Error::Ahead);
        }
        let resume = self.oldest(state);
        if cursor.sequence < resume.sequence {
            return Err(Error::Gap {
                missed: resume.sequence - cursor.sequence,
                resume,
            });
        }
        Ok(())
    }
    fn read(&self, state: &History, cursor: &Cursor, limit: usize) -> Result<Batch, Error> {
        if limit == 0 || limit > 65_536 {
            return Err(Error::Limit);
        }
        self.check(state, cursor)?;
        let records: Vec<_> = state
            .records
            .iter()
            .filter(|r| r.sequence > cursor.sequence)
            .take(limit)
            .cloned()
            .collect();
        let sequence = records.last().map_or(cursor.sequence, |r| r.sequence);
        Ok(Batch {
            records,
            cursor: self.cursor(sequence),
            closed: state.info.closed,
        })
    }
}
pub(crate) struct Source {
    shared: Arc<Shared>,
}
impl Source {
    pub fn new(state: State, chain: Status, limits: Config) -> Self {
        Self {
            shared: Arc::new(Shared {
                owner: Arc::new(()),
                state: Mutex::new(History {
                    info: Info {
                        state,
                        chain,
                        sequence: 0,
                        records: 0,
                        bytes: 0,
                        evicted: 0,
                        unretained: 0,
                        closed: false,
                        failure: None,
                        limits,
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
    pub fn fail(&self, failure: Failure) {
        let mut state = self.shared.lock();
        state.info.failure.get_or_insert(failure);
        state.info.closed = true;
        self.shared.changed.notify_all();
    }
    // Called only after acknowledged writes, while holding the database writer.
    // Concurrent insertion barriers update pending protection without inventing
    // a business effect. A subsequent drain can complete another scan wave.
    pub fn advance(&self, prior: &State, next: &State, chain: Status) {
        let mut history = self.shared.lock();
        if history.info.closed {
            return;
        }
        if &history.info.state != prior {
            history.info.failure = Some(Failure::Discontinuity);
            history.info.closed = true;
            self.shared.changed.notify_all();
            return;
        }
        let kind = effect(prior, next);
        history.info.state = next.clone();
        history.info.chain = chain;
        if let Some(kind) = kind {
            let Some(sequence) = history.info.sequence.checked_add(1) else {
                history.info.failure = Some(Failure::Sequence);
                history.info.closed = true;
                self.shared.changed.notify_all();
                return;
            };
            history.info.sequence = sequence;
            let record = Record {
                sequence,
                kind,
                chain,
                state: next.clone(),
            };
            let bytes = record.retained_bytes();
            let limits = history.info.limits;
            if bytes > limits.bytes {
                history.info.evicted = history
                    .info
                    .evicted
                    .saturating_add(history.records.len() as u64);
                history.info.unretained = history.info.unretained.saturating_add(1);
                history.records.clear();
                history.info.bytes = 0;
            } else {
                while history.records.len() >= limits.records
                    || history.info.bytes > limits.bytes - bytes
                {
                    let old = history.records.pop_front().expect("record fits budget");
                    history.info.bytes -= old.retained_bytes();
                    history.info.evicted = history.info.evicted.saturating_add(1);
                }
                history.info.bytes += bytes;
                history.records.push_back(Arc::new(record));
            }
            history.info.records = history.records.len();
        }
        self.shared.changed.notify_all();
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        self.shared.lock().info.closed = true;
        self.shared.changed.notify_all();
    }
}
fn effect(prior: &State, next: &State) -> Option<Kind> {
    if next.epoch != prior.epoch && next.phase == Phase::Roots {
        Some(Kind::Switch)
    } else if prior.active() && next.phase == Phase::Cancelled {
        Some(Kind::Cancelled)
    } else if prior.active() && next.phase == Phase::Complete {
        Some(Kind::CollectionComplete)
    } else if next.phase == Phase::Sweep
        && next.pending == 0
        && (prior.phase == Phase::Roots || prior.pending != 0)
    {
        Some(Kind::ScansComplete)
    } else {
        None
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
        self.shared.cursor(self.shared.lock().info.sequence)
    }
    pub fn oldest(&self) -> Cursor {
        self.shared.oldest(&self.shared.lock())
    }
    // State/cursor is one transient observation. Database::gc_event_snapshot
    // additionally pins the corresponding durable database view.
    pub fn snapshot(&self) -> (Info, Cursor) {
        let state = self.shared.lock();
        (state.info.clone(), self.shared.cursor(state.info.sequence))
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
            .wait_timeout_while(self.shared.lock(), timeout, |s| {
                !s.info.closed
                    && self.shared.check(s, cursor).is_ok()
                    && s.info.sequence == cursor.sequence
            })
            .unwrap_or_else(|e| e.into_inner());
        self.shared.read(&state, cursor, limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{Temp, identity, plan};
    #[test]
    fn sequence_exhaustion_fails_only_the_gc_feed_and_foreign_or_invalid_reads_are_explicit() {
        let temp = Temp::new();
        let db = crate::Database::open(&temp.0, identity()).unwrap();
        db.commit(plan(&db)).unwrap();
        let reader = db.gc_events();
        let mut ahead = reader.cursor();
        ahead.sequence += 1;
        assert_eq!(reader.after(&ahead, 1).unwrap_err(), Error::Ahead);
        assert_eq!(reader.after(&reader.cursor(), 0).unwrap_err(), Error::Limit);
        assert_eq!(
            reader.wait(&reader.cursor(), 1, Duration::MAX).unwrap_err(),
            Error::Limit
        );
        db.gc_events.shared.lock().info.sequence = u64::MAX;
        assert!(db.begin_gc(Default::default()).unwrap());
        assert_eq!(db.gc_state().unwrap().epoch, 1);
        assert_eq!(
            reader.after(&reader.cursor(), 1).unwrap_err(),
            Error::Failed(Failure::Sequence)
        );
        assert!(!db.events().info().closed);
    }
}
