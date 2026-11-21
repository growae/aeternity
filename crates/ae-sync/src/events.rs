use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::SystemTime,
};
use tokio::sync::watch;

// One typed channel per business topic. Retention is bounded; a lagging
// subscriber must reconcile the explicit gap against its domain's snapshot.
// Required control effects must not depend on consuming this retained history.
pub trait Event: Send + Sync + 'static {
    fn retained_bytes(&self) -> usize;
}
#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub records: usize,
    pub bytes: usize,
    pub event_bytes: usize,
    pub subscribers: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            records: 1024,
            bytes: 4 << 20,
            event_bytes: 1 << 20,
            subscribers: 128,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Closed,
    ForeignCursor,
    Subscribers,
    EventTooLarge,
    Sequence,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
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
#[derive(Debug)]
pub struct Record<E> {
    pub cursor: Cursor,
    pub time: SystemTime,
    pub event: E,
    bytes: usize,
}
#[derive(Debug)]
pub enum Delivery<E> {
    Event(Arc<Record<E>>),
    Gap { missed: u64 },
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Info {
    pub sequence: u64,
    pub retained: usize,
    pub bytes: usize,
    pub subscribers: usize,
    pub evicted: u64,
    pub closed: bool,
}
struct State<E> {
    records: VecDeque<Arc<Record<E>>>,
    info: Info,
}
struct Shared<E> {
    owner: Arc<()>,
    config: Config,
    state: Mutex<State<E>>,
    changed: watch::Sender<()>,
}
impl<E> Shared<E> {
    fn cursor(&self, sequence: u64) -> Cursor {
        Cursor {
            owner: self.owner.clone(),
            sequence,
        }
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State<E>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}
// This owner is deliberately not Clone. Its drop closes the channel even if
// readers survive; a replacement owner always has a different cursor identity.
pub struct Channel<E: Event> {
    shared: Arc<Shared<E>>,
}
pub struct Feed<E: Event> {
    shared: Arc<Shared<E>>,
}
impl<E: Event> Clone for Feed<E> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}
pub fn channel<E: Event>(config: Config) -> Result<(Channel<E>, Feed<E>), Error> {
    if config.records == 0
        || config.records > 65_536
        || config.subscribers == 0
        || config.subscribers > 65_536
        || config.event_bytes == 0
        || config.bytes > 1 << 30
        || config
            .event_bytes
            .checked_add(64)
            .is_none_or(|n| n > config.bytes)
    {
        return Err(Error::Configuration);
    }
    let shared = Arc::new(Shared {
        owner: Arc::new(()),
        config,
        state: Mutex::new(State {
            records: VecDeque::new(),
            info: Info::default(),
        }),
        changed: watch::channel(()).0,
    });
    Ok((
        Channel {
            shared: shared.clone(),
        },
        Feed { shared },
    ))
}
impl<E: Event> Channel<E> {
    pub fn publish(&self, event: E) -> Result<Cursor, Error> {
        let payload = event.retained_bytes();
        if payload > self.shared.config.event_bytes {
            return Err(Error::EventTooLarge);
        }
        let bytes = payload.checked_add(64).ok_or(Error::EventTooLarge)?;
        let mut s = self.shared.lock();
        if s.info.closed {
            return Err(Error::Closed);
        }
        let next = s.info.sequence.checked_add(1).ok_or(Error::Sequence)?;
        while s.records.len() >= self.shared.config.records
            || s.info.bytes > self.shared.config.bytes - bytes
        {
            let old = s.records.pop_front().expect("retained event fits");
            s.info.bytes -= old.bytes;
            s.info.evicted = s.info.evicted.saturating_add(1);
        }
        let cursor = self.shared.cursor(next);
        s.records.push_back(Arc::new(Record {
            cursor: cursor.clone(),
            time: SystemTime::now(),
            event,
            bytes,
        }));
        s.info.sequence = next;
        s.info.retained = s.records.len();
        s.info.bytes += bytes;
        self.shared.changed.send_replace(());
        Ok(cursor)
    }
    pub fn close(&self) {
        self.shared.lock().info.closed = true;
        self.shared.changed.send_replace(());
    }
}
impl<E: Event> Drop for Channel<E> {
    fn drop(&mut self) {
        self.close();
    }
}
impl<E: Event> Feed<E> {
    pub fn info(&self) -> Info {
        self.shared.lock().info
    }
    pub fn cursor(&self) -> Cursor {
        self.shared.cursor(self.shared.lock().info.sequence)
    }
    pub fn subscribe(&self, after: Option<Cursor>) -> Result<Subscription<E>, Error> {
        let mut s = self.shared.lock();
        let cursor = after.unwrap_or_else(|| self.shared.cursor(s.info.sequence));
        if !Arc::ptr_eq(&cursor.owner, &self.shared.owner) || cursor.sequence > s.info.sequence {
            return Err(Error::ForeignCursor);
        }
        if s.info.subscribers == self.shared.config.subscribers {
            return Err(Error::Subscribers);
        }
        s.info.subscribers += 1;
        Ok(Subscription {
            shared: self.shared.clone(),
            cursor,
            changed: self.shared.changed.subscribe(),
        })
    }
}
pub struct Subscription<E: Event> {
    shared: Arc<Shared<E>>,
    cursor: Cursor,
    changed: watch::Receiver<()>,
}
impl<E: Event> Subscription<E> {
    pub fn cursor(&self) -> Cursor {
        self.cursor.clone()
    }
    pub async fn recv(&mut self) -> Option<Delivery<E>> {
        loop {
            {
                let s = self.shared.lock();
                if let Some(first) = s.records.front() {
                    let oldest = first.cursor.sequence;
                    if self.cursor.sequence < oldest - 1 {
                        let missed = oldest - 1 - self.cursor.sequence;
                        self.cursor.sequence = oldest - 1;
                        return Some(Delivery::Gap { missed });
                    }
                    if self.cursor.sequence < s.info.sequence {
                        let index = usize::try_from(self.cursor.sequence + 1 - oldest).ok()?;
                        let event = s.records.get(index)?.clone();
                        self.cursor = event.cursor.clone();
                        return Some(Delivery::Event(event));
                    }
                }
                if s.info.closed {
                    return None;
                }
                self.changed.borrow_and_update();
            }
            if self.changed.changed().await.is_err() {
                return None;
            }
        }
    }
}
impl<E: Event> Drop for Subscription<E> {
    fn drop(&mut self) {
        self.shared.lock().info.subscribers -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    impl Event for Vec<u8> {
        fn retained_bytes(&self) -> usize {
            self.len()
        }
    }
    #[tokio::test]
    async fn cancelled_wait_preserves_cursor_and_sequence_exhaustion_does_not_evict_history() {
        let (owner, feed) = channel::<Vec<u8>>(Config::default()).unwrap();
        let mut sub = feed.subscribe(None).unwrap();
        let cursor = sub.cursor();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(1), sub.recv())
                .await
                .is_err()
        );
        assert_eq!(sub.cursor(), cursor);
        owner.publish(vec![1]).unwrap();
        assert!(matches!(sub.recv().await, Some(Delivery::Event(_))));
        owner.shared.lock().info.sequence = u64::MAX;
        let before = feed.info();
        assert_eq!(owner.publish(vec![2]), Err(Error::Sequence));
        assert_eq!(feed.info(), before);
        drop(sub);
        let mut tail = feed.subscribe(None).unwrap();
        let waiter = tokio::spawn(async move { tail.recv().await });
        tokio::task::yield_now().await;
        drop(owner);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert_eq!(feed.info().subscribers, 0);
    }
    #[tokio::test]
    async fn independent_subscribers_report_gaps_drain_and_release_their_slots() {
        let (owner, feed) = channel::<Vec<u8>>(Config {
            records: 2,
            bytes: 256,
            event_bytes: 64,
            subscribers: 2,
        })
        .unwrap();
        let cursor = feed.cursor();
        let mut fast = feed.subscribe(None).unwrap();
        let slow = feed.subscribe(None).unwrap();
        assert!(matches!(feed.subscribe(None), Err(Error::Subscribers)));
        for n in 0..5 {
            owner.publish(vec![n; 64]).unwrap();
            let Some(Delivery::Event(e)) = fast.recv().await else {
                panic!()
            };
            assert_eq!(e.event[0], n);
        }
        drop(slow);
        let mut slow = feed.subscribe(Some(cursor)).unwrap();
        assert!(matches!(
            slow.recv().await,
            Some(Delivery::Gap { missed: 3 })
        ));
        drop(owner);
        for n in 3..5 {
            let Some(Delivery::Event(e)) = slow.recv().await else {
                panic!()
            };
            assert_eq!(e.event[0], n);
        }
        assert!(slow.recv().await.is_none());
        assert!(fast.recv().await.is_none());
        assert_eq!(feed.info().bytes, 256);
        assert!(feed.info().closed);
        drop(fast);
        drop(slow);
        assert_eq!(feed.info().subscribers, 0);
    }
    #[tokio::test]
    async fn failed_publication_does_not_advance_cursor_and_replacement_rejects_old_cursor() {
        let (owner, feed) = channel::<Vec<u8>>(Config {
            records: 10,
            bytes: 128,
            event_bytes: 64,
            subscribers: 1,
        })
        .unwrap();
        let start = feed.cursor();
        let mut sub = feed.subscribe(None).unwrap();
        assert_eq!(owner.publish(vec![0; 65]), Err(Error::EventTooLarge));
        assert_eq!(feed.cursor(), start);
        let task = tokio::spawn(async move { sub.recv().await });
        tokio::task::yield_now().await;
        owner.publish(vec![1; 64]).unwrap();
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap(),
            Some(Delivery::Event(_))
        ));
        for _ in 0..3 {
            owner.publish(vec![2; 64]).unwrap();
        }
        assert_eq!(feed.info().retained, 1);
        assert_eq!(feed.info().evicted, 3);
        owner.close();
        assert_eq!(owner.publish(vec![]), Err(Error::Closed));
        let (_new, other) = channel::<Vec<u8>>(Config::default()).unwrap();
        assert!(matches!(
            other.subscribe(Some(feed.cursor())),
            Err(Error::ForeignCursor)
        ));
    }
}
