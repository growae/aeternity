use super::*;
use crate::pool::{Signed, Submission};
use ae_chain::repository::Status;
use ae_mempool::pool::{Event, PushOutcome};

#[derive(Clone, Debug)]
pub struct Cursor {
    stream: Arc<()>,
    sequence: u64,
}
impl PartialEq for Cursor {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.stream, &other.stream) && self.sequence == other.sequence
    }
}
impl Eq for Cursor {}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadError {
    WrongStream,
    Ahead,
    Lagged { missed: u64, resume: Cursor },
    InvalidLimit,
}
#[derive(Clone, Debug)]
pub struct Accepted {
    pub sequence: u64,
    pub event: Event,
    pub transaction: Arc<Signed>,
    pub chain: Status,
    pub revision: u64,
}
#[derive(Debug)]
pub struct Batch {
    pub events: Vec<Accepted>,
    pub cursor: Cursor,
    pub closed: bool,
}
#[derive(Debug)]
pub struct Snapshot {
    pub transactions: Vec<Arc<Signed>>,
    pub cursor: Cursor,
    pub pool: Inspection,
    pub complete: bool,
}
pub(super) struct Feed {
    stream: Arc<()>,
    latest: u64,
    retained: usize,
    events: VecDeque<(Accepted, usize)>,
    count_limit: usize,
    byte_limit: usize,
}
impl Feed {
    pub(super) fn new(count_limit: usize, byte_limit: usize) -> Self {
        Self {
            stream: Arc::new(()),
            latest: 0,
            retained: 0,
            events: VecDeque::new(),
            count_limit,
            byte_limit,
        }
    }
    pub(super) fn cursor(&self) -> Cursor {
        Cursor {
            stream: self.stream.clone(),
            sequence: self.latest,
        }
    }
    pub(super) fn oldest(&self) -> Cursor {
        Cursor {
            stream: self.stream.clone(),
            sequence: self
                .events
                .front()
                .map_or(self.latest, |r| r.0.sequence - 1),
        }
    }
    pub(super) fn publish(
        &mut self,
        transaction: Arc<Signed>,
        submission: &Submission,
        bytes: usize,
    ) {
        let PushOutcome::Accepted { event, .. } = submission.outcome else {
            return;
        };
        if self.latest == u64::MAX {
            self.stream = Arc::new(());
            self.latest = 0;
            self.events.clear();
            self.retained = 0;
        }
        self.latest += 1;
        if self.count_limit == 0 || bytes > self.byte_limit {
            // An unretainable event is an explicit gap, never a silent hole
            // behind an apparently contiguous cursor.
            self.events.clear();
            self.retained = 0;
            return;
        }
        while self.events.len() >= self.count_limit || self.retained > self.byte_limit - bytes {
            let (_, size) = self.events.pop_front().expect("retained event fits");
            self.retained -= size;
        }
        self.events.push_back((
            Accepted {
                sequence: self.latest,
                event,
                transaction,
                chain: submission.chain,
                revision: submission.revision,
            },
            bytes,
        ));
        self.retained += bytes;
    }
    fn check(&self, cursor: &Cursor) -> Result<(), ReadError> {
        if !Arc::ptr_eq(&cursor.stream, &self.stream) {
            return Err(ReadError::WrongStream);
        }
        if cursor.sequence > self.latest {
            return Err(ReadError::Ahead);
        }
        let resume = self.oldest();
        if cursor.sequence < resume.sequence {
            return Err(ReadError::Lagged {
                missed: resume.sequence - cursor.sequence,
                resume,
            });
        }
        Ok(())
    }
    pub(super) fn ready(&self, cursor: &Cursor) -> bool {
        self.check(cursor).is_err() || cursor.sequence != self.latest
    }
    pub(super) fn read(
        &self,
        cursor: &Cursor,
        limit: usize,
        closed: bool,
    ) -> Result<Batch, ReadError> {
        if limit == 0 {
            return Err(ReadError::InvalidLimit);
        }
        self.check(cursor)?;
        let events: Vec<_> = self
            .events
            .iter()
            .filter(|r| r.0.sequence > cursor.sequence)
            .take(limit)
            .map(|r| r.0.clone())
            .collect();
        let sequence = events.last().map_or(cursor.sequence, |r| r.sequence);
        Ok(Batch {
            events,
            cursor: Cursor {
                stream: self.stream.clone(),
                sequence,
            },
            closed,
        })
    }
}
impl Client {
    pub fn event_cursor(&self) -> Cursor {
        self.shared.lock().feed.cursor()
    }
    pub fn oldest_event_cursor(&self) -> Cursor {
        self.shared.lock().feed.oldest()
    }
    pub fn events_after(&self, cursor: &Cursor, limit: usize) -> Result<Batch, ReadError> {
        let c = self.shared.lock();
        c.feed.read(cursor, limit, !c.info.running)
    }
    pub fn wait_for_events(
        &self,
        cursor: &Cursor,
        limit: usize,
        timeout: Duration,
    ) -> Result<Batch, ReadError> {
        if limit == 0 {
            return Err(ReadError::InvalidLimit);
        }
        let (c, _) = self
            .shared
            .changed
            .wait_timeout_while(self.shared.lock(), timeout, |c| {
                c.info.running && !c.feed.ready(cursor)
            })
            .unwrap_or_else(|e| e.into_inner());
        c.feed.read(cursor, limit, !c.info.running)
    }
    pub fn gossip_snapshot(&self, max: usize) -> Result<Ticket<Snapshot>, Error> {
        if max > self.shared.settings.max_peek {
            return Err(Error::RequestTooLarge);
        }
        let shared = Arc::downgrade(&self.shared);
        self.submit(BASE_WEIGHT, move |s| {
            let transactions = s.peek(max, None, None)?;
            let pool = s.inspection();
            // This request still owns the worker; another accepted transaction
            // cannot be inserted between the snapshot and its event cursor.
            let shared = shared
                .upgrade()
                .expect("executing worker owns shared state");
            let cursor = shared.lock().feed.cursor();
            let complete = transactions.len() == pool.active + pool.visited;
            Ok(Snapshot {
                transactions,
                cursor,
                pool,
                complete,
            })
        })
    }
}
#[cfg(test)]
mod tests;
