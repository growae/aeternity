use super::*;
use std::collections::VecDeque;

#[derive(Clone, Debug)]
pub struct Cursor {
    owner: Arc<()>,
    sequence: u64,
}
impl Cursor {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}
#[derive(Debug)]
pub struct Record {
    pub cursor: Cursor,
    pub category: &'static str,
    // A bounded JSON line, including its newline. No transport handles or
    // permits, transaction bodies, signatures or secret keys are retained.
    bytes: Vec<u8>,
}
impl Record {
    pub fn json_line(&self) -> &[u8] {
        &self.bytes
    }
}
pub enum Delivery {
    Record(Arc<Record>),
    Gap { missed: u64 },
}
pub(super) struct Ring {
    owner: Arc<()>,
    records: VecDeque<Arc<Record>>,
    bytes: usize,
    next: u64,
    pub evicted: u64,
    closed: bool,
    changed: watch::Sender<u64>,
}
impl Ring {
    pub fn new() -> Self {
        Self {
            owner: Arc::new(()),
            records: VecDeque::new(),
            bytes: 0,
            next: 0,
            evicted: 0,
            closed: false,
            changed: watch::channel(0).0,
        }
    }
    pub fn cursor(&self) -> Cursor {
        Cursor {
            owner: self.owner.clone(),
            sequence: self.next,
        }
    }
    pub fn retained(&self) -> (usize, usize) {
        (self.records.len(), self.bytes)
    }
    pub fn push(
        &mut self,
        category: &'static str,
        data: Value,
        config: &Config,
    ) -> Result<Arc<Record>, Error> {
        let next = self.next.checked_add(1).ok_or(Error::Sequence)?;
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Time)?;
        let mut bytes = serde_json::to_vec(&json!({"sequence":next,"time_us":time.as_micros().to_string(),"category":category,"data":data})).map_err(|_| Error::Encoding)?;
        bytes.push(b'\n');
        if bytes.len() > config.record_bytes {
            return Err(Error::RecordTooLarge);
        }
        while self.records.len() >= config.records
            || self.bytes + bytes.len() > config.history_bytes
        {
            let old = self.records.pop_front().expect("bounded retained record");
            self.bytes -= old.bytes.len();
            self.evicted = self.evicted.saturating_add(1);
        }
        self.next = next;
        let record = Arc::new(Record {
            cursor: self.cursor(),
            category,
            bytes,
        });
        self.bytes += record.bytes.len();
        self.records.push_back(record.clone());
        self.changed.send_replace(next);
        Ok(record)
    }
    pub fn close(&mut self) {
        self.closed = true;
        self.changed.send_replace(self.next);
    }
}
pub struct Subscription {
    ring: Arc<Mutex<Ring>>,
    changed: watch::Receiver<u64>,
    cursor: Cursor,
}
impl Subscription {
    pub(super) fn new(ring: Arc<Mutex<Ring>>, after: Option<Cursor>) -> Result<Self, Error> {
        let r = ring.lock().map_err(|_| Error::Closed)?;
        let cursor = after.unwrap_or_else(|| r.cursor());
        if !Arc::ptr_eq(&cursor.owner, &r.owner) || cursor.sequence > r.next {
            return Err(Error::ForeignCursor);
        }
        let changed = r.changed.subscribe();
        drop(r);
        Ok(Self {
            ring,
            changed,
            cursor,
        })
    }
    pub fn cursor(&self) -> Cursor {
        self.cursor.clone()
    }
    pub async fn recv(&mut self) -> Option<Delivery> {
        loop {
            {
                let r = self.ring.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(first) = r.records.front() {
                    let oldest = first.cursor.sequence;
                    if self.cursor.sequence < oldest - 1 {
                        let missed = oldest - 1 - self.cursor.sequence;
                        self.cursor.sequence = oldest - 1;
                        return Some(Delivery::Gap { missed });
                    }
                    if self.cursor.sequence < r.next {
                        let i = usize::try_from(self.cursor.sequence + 1 - oldest).ok()?;
                        let record = r.records.get(i)?.clone();
                        self.cursor = record.cursor.clone();
                        return Some(Delivery::Record(record));
                    }
                }
                if r.closed {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn slow_reader_reports_exact_gap_and_drains_after_close() {
        let ring = Arc::new(Mutex::new(Ring::new()));
        let mut reader = Subscription::new(ring.clone(), None).unwrap();
        let config = Config {
            records: 3,
            ..Default::default()
        };
        for n in 0..10 {
            ring.lock()
                .unwrap()
                .push("test", json!(n), &config)
                .unwrap();
        }
        ring.lock().unwrap().close();
        assert!(matches!(
            reader.recv().await,
            Some(Delivery::Gap { missed: 7 })
        ));
        for n in 7..10 {
            let Some(Delivery::Record(r)) = reader.recv().await else {
                panic!()
            };
            assert_eq!(
                serde_json::from_slice::<Value>(r.json_line()).unwrap()["data"],
                n
            );
            assert_eq!(r.cursor.sequence(), n + 1);
        }
        assert!(reader.recv().await.is_none());
        assert_eq!(ring.lock().unwrap().evicted, 7);
    }
    #[tokio::test]
    async fn byte_limit_rejection_foreign_cursor_and_waiting_reader() {
        let ring = Arc::new(Mutex::new(Ring::new()));
        let mut reader = Subscription::new(ring.clone(), None).unwrap();
        let foreign = Ring::new().cursor();
        assert!(matches!(
            Subscription::new(ring.clone(), Some(foreign)),
            Err(Error::ForeignCursor)
        ));
        let mut future = reader.cursor();
        future.sequence = 1;
        assert!(matches!(
            Subscription::new(ring.clone(), Some(future)),
            Err(Error::ForeignCursor)
        ));
        let config = Config {
            history_bytes: 1024,
            record_bytes: 1024,
            ..Default::default()
        };
        let waiting = tokio::spawn(async move { reader.recv().await });
        tokio::task::yield_now().await;
        ring.lock()
            .unwrap()
            .push("test", json!("x".repeat(700)), &config)
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), waiting)
                .await
                .unwrap()
                .unwrap(),
            Some(Delivery::Record(_))
        ));
        let cursor = ring.lock().unwrap().cursor();
        let mut resumed = Subscription::new(ring.clone(), Some(cursor.clone())).unwrap();
        assert_eq!(
            ring.lock()
                .unwrap()
                .push("test", json!("x".repeat(1024)), &config)
                .unwrap_err(),
            Error::RecordTooLarge
        );
        assert_eq!(ring.lock().unwrap().cursor().sequence(), cursor.sequence());
        for _ in 0..3 {
            ring.lock()
                .unwrap()
                .push("test", json!("x".repeat(700)), &config)
                .unwrap();
        }
        let (count, bytes) = ring.lock().unwrap().retained();
        assert_eq!(count, 1);
        assert!(bytes <= 1024);
        assert!(matches!(
            resumed.recv().await,
            Some(Delivery::Gap { missed: 2 })
        ));
        assert!(matches!(resumed.recv().await, Some(Delivery::Record(_))));
        let closed = tokio::spawn(async move { resumed.recv().await });
        tokio::task::yield_now().await;
        ring.lock().unwrap().close();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), closed)
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
    }
}
