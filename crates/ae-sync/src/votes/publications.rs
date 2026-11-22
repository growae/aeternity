use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, watch};

pub struct Publication {
    pub transaction: Arc<Signed>,
    pub event: policy::Event,
    _bytes: OwnedSemaphorePermit,
}
struct Shared {
    database: Weak<Database>,
    send: mpsc::Sender<Publication>,
    receive: Mutex<mpsc::Receiver<Publication>>,
    bytes: Arc<Semaphore>,
    replies: Arc<Semaphore>,
    config: Config,
    consumer: AtomicBool,
    worker: AtomicBool,
    closed: watch::Sender<bool>,
}
#[derive(Clone)]
pub struct Publications(Arc<Shared>);
impl Publications {
    pub fn new(db: &Arc<Database>, config: Config) -> Result<Self, Error> {
        config.validate()?;
        let (send, receive) = mpsc::channel(config.events);
        let (closed, _) = watch::channel(false);
        Ok(Self(Arc::new(Shared {
            database: Arc::downgrade(db),
            send,
            receive: Mutex::new(receive),
            bytes: Arc::new(Semaphore::new(config.event_bytes)),
            replies: Arc::new(Semaphore::new(config.reply_bytes)),
            config,
            consumer: AtomicBool::new(false),
            worker: AtomicBool::new(false),
            closed,
        })))
    }
    pub fn uses_database(&self, db: &Arc<Database>) -> bool {
        self.0.database.ptr_eq(&Arc::downgrade(db))
    }
    pub(super) fn replies(&self, config: Config) -> Result<Arc<Semaphore>, Error> {
        let original = self.0.config;
        if original.events != config.events
            || original.event_bytes != config.event_bytes
            || original.reply_bytes != config.reply_bytes
        {
            return Err(Fault::Configuration.into());
        }
        Ok(self.0.replies.clone())
    }
    pub fn listen(&self) -> Result<Consumer, Error> {
        if *self.0.closed.borrow() {
            return Err(Fault::Stopped.into());
        }
        self.0
            .consumer
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Fault::Busy)?;
        Ok(Consumer(self.clone()))
    }
    pub(crate) fn close(&self) {
        self.0.closed.send_replace(true);
    }
    pub(super) fn claim(&self) -> Result<Claim, Error> {
        if *self.0.closed.borrow() {
            return Err(Fault::Stopped.into());
        }
        self.0
            .worker
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Fault::Busy)?;
        Ok(Claim(self.clone()))
    }
    pub(super) fn reserve(&self, bytes: usize) -> Result<Reservation, Error> {
        if *self.0.closed.borrow() {
            return Err(Fault::Stopped.into());
        }
        let bytes = u32::try_from(bytes).map_err(|_| Fault::Busy)?;
        let bytes = self
            .0
            .bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| Fault::Busy)?;
        let slot = self
            .0
            .send
            .clone()
            .try_reserve_owned()
            .map_err(|_| Fault::Busy)?;
        Ok(Reservation { slot, bytes })
    }
}
pub(super) struct Reservation {
    slot: mpsc::OwnedPermit<Publication>,
    bytes: OwnedSemaphorePermit,
}
impl Reservation {
    pub fn send(self, transaction: Arc<Signed>, event: policy::Event) {
        self.slot.send(Publication {
            transaction,
            event,
            _bytes: self.bytes,
        });
    }
}
pub(super) struct Claim(Publications);
impl Drop for Claim {
    fn drop(&mut self) {
        self.0.0.worker.store(false, Ordering::Release);
    }
}
pub struct Consumer(Publications);
impl Consumer {
    pub async fn recv(&mut self) -> Option<Publication> {
        let mut closed = self.0.0.closed.subscribe();
        if *closed.borrow() {
            return None;
        }
        let mut receive = self.0.0.receive.lock().await;
        tokio::select! {biased;_=closed.changed()=>None,p=receive.recv()=>p}
    }
}
impl Drop for Consumer {
    fn drop(&mut self) {
        self.0.0.consumer.store(false, Ordering::Release);
    }
}
