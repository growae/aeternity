use ae_storage::{Database, gc::worker as gc, pool::worker};
use std::sync::{Arc, Weak};
use tokio::sync::watch;

// A lookup returns an exact client, never a proxy that can redirect an accepted
// operation. Only the owning supervisor can publish replacements.
#[derive(Clone)]
pub struct Pool {
    pub client: worker::Client,
    pub(crate) after: worker::events::Cursor,
}
impl Pool {
    pub(crate) fn new(client: worker::Client) -> Self {
        Self {
            after: client.event_cursor(),
            client,
        }
    }
}
pub type PeerAnalytics = Binding<crate::analytics::Client>;
pub type MetricsRouting = Binding<crate::routing::Client>;
pub type Admission = Binding<Pool>;
pub type GarbageCollection = Binding<gc::Client>;
pub type PresetKeys = Binding<crate::preset_keys::Client>;

#[derive(Clone)]
pub struct Binding<T: Clone> {
    database: Weak<Database>,
    source: Source<T>,
}
#[derive(Clone)]
enum Source<T: Clone> {
    Fixed(Snapshot<T>),
    Managed(watch::Receiver<Snapshot<T>>),
}
#[derive(Clone)]
pub(crate) struct Snapshot<T> {
    pub value: Option<T>,
    pub lease: Arc<()>,
    pub generation: u64,
    pub available: bool,
    pub closed: bool,
}
impl<T: Clone> Binding<T> {
    pub fn current(&self) -> Option<T> {
        let snapshot = self.snapshot();
        if snapshot.available && !snapshot.closed {
            snapshot.value
        } else {
            None
        }
    }
    pub fn is_closed(&self) -> bool {
        self.snapshot().closed
    }
    pub fn uses_database(&self, db: &Arc<Database>) -> bool {
        self.database.ptr_eq(&Arc::downgrade(db))
    }
    pub(crate) fn managed(&self) -> bool {
        matches!(self.source, Source::Managed(_))
    }
    pub(crate) fn snapshot(&self) -> Snapshot<T> {
        match &self.source {
            Source::Fixed(value) => value.clone(),
            Source::Managed(source) => {
                let mut value = source.borrow().clone();
                value.closed |= source.has_changed().is_err();
                value
            }
        }
    }
    pub(crate) async fn changed(&mut self) {
        match &mut self.source {
            Source::Fixed(_) => std::future::pending().await,
            Source::Managed(source) => {
                let _ = source.changed().await;
            }
        }
    }
    fn fixed_value(db: &Arc<Database>, value: T) -> Self {
        Self {
            database: Arc::downgrade(db),
            source: Source::Fixed(Snapshot {
                value: Some(value),
                lease: Arc::new(()),
                generation: 0,
                available: true,
                closed: false,
            }),
        }
    }
}
impl PeerAnalytics {
    pub(crate) fn fixed(db: &Arc<Database>, client: crate::analytics::Client) -> Self {
        Self::fixed_value(db, client)
    }
}
impl Binding<crate::peer::capabilities::worker::Client> {
    pub(crate) fn fixed(
        db: &Arc<Database>,
        client: crate::peer::capabilities::worker::Client,
    ) -> Self {
        Self::fixed_value(db, client)
    }
}
impl MetricsRouting {
    pub(crate) fn fixed(db: &Arc<Database>, client: crate::routing::Client) -> Self {
        Self::fixed_value(db, client)
    }
}
impl Admission {
    pub(crate) fn fixed(db: &Arc<Database>, client: worker::Client) -> Self {
        Self::fixed_value(db, Pool::new(client))
    }
}
impl Binding<crate::votes::Client> {
    pub(crate) fn fixed(db: &Arc<Database>, client: crate::votes::Client) -> Self {
        Self::fixed_value(db, client)
    }
}
impl GarbageCollection {
    pub(crate) fn fixed(db: &Arc<Database>, client: gc::Client) -> Self {
        Self::fixed_value(db, client)
    }
}

pub(crate) struct Owner<T: Clone>(watch::Sender<Snapshot<T>>);
impl<T: Clone> Owner<T> {
    pub fn new(db: &Arc<Database>) -> (Self, Binding<T>) {
        let (send, source) = watch::channel(Snapshot {
            value: None,
            lease: Arc::new(()),
            generation: 0,
            available: false,
            closed: false,
        });
        (
            Self(send),
            Binding {
                database: Arc::downgrade(db),
                source: Source::Managed(source),
            },
        )
    }
    pub fn install(&self, value: T, generation: u64) {
        self.0.send_replace(Snapshot {
            value: Some(value),
            lease: Arc::new(()),
            generation,
            available: true,
            closed: false,
        });
    }
    pub fn withdraw(&self) {
        self.0.send_modify(|value| value.available = false);
    }
    // Mirror availability without sharing publisher ownership. Closing this
    // local binding must neither close its source nor be undone by a late poll.
    pub(crate) fn mirror(&self, source: Snapshot<T>) {
        self.0.send_if_modified(|current| {
            if current.closed
                || (Arc::ptr_eq(&current.lease, &source.lease)
                    && current.available == source.available
                    && current.closed == source.closed
                    && current.generation == source.generation)
            {
                return false;
            }
            *current = source;
            true
        });
    }
    pub fn close(&self) {
        self.0.send_modify(|value| {
            value.available = false;
            value.closed = true;
        });
    }
}
impl<T: Clone> Drop for Owner<T> {
    fn drop(&mut self) {
        self.close();
    }
}
