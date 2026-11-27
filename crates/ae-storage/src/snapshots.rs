// Owned immutable trie access for services whose lifetime cannot borrow a DB.
// Each bounded worker owns the database Arc and borrows its actual snapshot
// on its own stack. No lifetime extension or mutable live-read adapter.
use crate::{Database, Snapshot, events};
use ae_chain::{repository as repo, resilience};
use ae_crypto::blake2b_256;
use ae_trie::{Error as TrieError, NodeReader};
use ae_types::Hash256;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
};

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub maximum_snapshots: usize,
    pub command_capacity: usize,
    pub maximum_node_bytes: usize,
    pub maximum_read_bytes: usize,
    pub maximum_scan_entries: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            maximum_snapshots: 8,
            command_capacity: 16,
            maximum_node_bytes: 64 << 20,
            maximum_read_bytes: 64 << 20,
            maximum_scan_entries: 100_000,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Config,
    AlreadyRunning,
    NotRunning,
    ResourceLimit,
    Repository(repo::Error),
    Worker(String),
}
pub(crate) struct Shared {
    database: Arc<Database>,
    config: Config,
    live: AtomicUsize,
}
// One configuration/limit owner per open database. Outstanding readers retain
// the lease even after this factory is dropped; replacement cannot evade limits.
#[derive(Clone)]
pub struct Owner(Arc<Shared>);
impl Owner {
    pub fn start(database: Arc<Database>, config: Config) -> Result<Self, Error> {
        if !(1..=128).contains(&config.maximum_snapshots)
            || !(1..=1024).contains(&config.command_capacity)
            || !(1..=1 << 30).contains(&config.maximum_node_bytes)
            || !(1..=1 << 30).contains(&config.maximum_read_bytes)
            || !(1..=1_000_000).contains(&config.maximum_scan_entries)
        {
            return Err(Error::Config);
        }
        let shared = Arc::new(Shared {
            database: database.clone(),
            config,
            live: AtomicUsize::new(0),
        });
        {
            let mut owner = database
                .snapshot_owner
                .lock()
                .map_err(|_| Error::Worker("poisoned snapshot ownership".into()))?;
            if owner.upgrade().is_some() {
                return Err(Error::AlreadyRunning);
            }
            *owner = Arc::downgrade(&shared);
        }
        Ok(Self(shared))
    }
    // Additional services share the established owner's exact limits. Readers
    // retain this authority even after the original factory is dropped.
    pub fn current(database: &Arc<Database>) -> Result<Self, Error> {
        database
            .snapshot_owner
            .lock()
            .map_err(|_| Error::Worker("poisoned snapshot ownership".into()))?
            .upgrade()
            .map(Self)
            .ok_or(Error::NotRunning)
    }
    pub fn live_snapshots(&self) -> usize {
        self.0.live.load(Ordering::Acquire)
    }
    pub fn uses_database(&self, database: &Arc<Database>) -> bool {
        Arc::ptr_eq(&self.0.database, database)
    }
    // All repository reads, including metadata and prefix scans, go to the
    // same pinned snapshot as trie reads. No live database fallback exists.
    pub fn view(&self) -> Result<Captured<repo::View<Reader>>, Error> {
        let captured = self.capture(|view| Ok(view.config().clone()))?;
        let value = repo::View::load_with_config(captured.reader.clone(), captured.value)
            .map_err(Error::Repository)?;
        Ok(Captured {
            value,
            metadata: captured.metadata,
            reader: captured.reader,
        })
    }
    // The inspection and all subsequent node reads use the same snapshot.
    // Inspect metadata/roots here; leave account/contract lookups lazy when
    // their native ordering depends on later entropy or callback results.
    pub fn capture<T, F>(&self, inspect: F) -> Result<Captured<T>, Error>
    where
        T: Send + 'static,
        F: FnOnce(&repo::View<Snapshot<'_>>) -> Result<T, repo::Error> + Send + 'static,
    {
        self.0
            .live
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.0.config.maximum_snapshots).then_some(n + 1)
            })
            .map_err(|_| Error::ResourceLimit)?;
        let slot = Slot(self.0.clone());
        let (ready, initialized) = mpsc::sync_channel(1);
        let (sender, commands) = mpsc::sync_channel::<Request>(self.0.config.command_capacity);
        let worker = thread::Builder::new()
            .name("ae-state-snapshot".into())
            .spawn(move || {
                let slot = slot;
                let (view, cursor) = match slot.0.database.event_snapshot() {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = ready.send(Err(Error::Repository(e)));
                        return;
                    }
                };
                let value = match inspect(&view) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = ready.send(Err(Error::Repository(e)));
                        return;
                    }
                };
                let meta = Metadata {
                    status: view.status(),
                    source_id: repo::Read::source_id(view.source()),
                    resilience: view.resilience(),
                    cursor,
                };
                if ready.send(Ok((value, meta))).is_err() {
                    return;
                }
                for request in commands {
                    request.run(view.source(), slot.0.config);
                }
            })
            .map_err(|e| Error::Worker(e.to_string()))?;
        let mut inner = Inner {
            sender: Some(sender),
            worker: Some(worker),
            metadata: None,
            config: self.0.config,
        };
        let (value, metadata) = initialized
            .recv()
            .map_err(|_| Error::Worker("snapshot initialization ended".into()))??;
        inner.metadata = Some(metadata.clone());
        Ok(Captured {
            value,
            metadata,
            reader: Reader(Arc::new(inner)),
        })
    }
}
struct Slot(Arc<Shared>);
impl Drop for Slot {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::AcqRel);
    }
}
#[derive(Debug, Clone)]
pub struct Metadata {
    pub status: repo::Status,
    pub source_id: u64,
    pub resilience: resilience::Snapshot,
    pub cursor: events::Cursor,
}
pub struct Captured<T> {
    pub value: T,
    metadata: Metadata,
    reader: Reader,
}
impl<T> Captured<T> {
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
    pub fn reader(&self) -> &Reader {
        &self.reader
    }
}
mod reads;
use reads::Request;
struct Inner {
    sender: Option<SyncSender<Request>>,
    worker: Option<JoinHandle<()>>,
    metadata: Option<Metadata>,
    config: Config,
}
impl Drop for Inner {
    fn drop(&mut self) {
        // The last reader closes the channel before joining. Workers never own
        // a Reader, so there is no ownership cycle or self-join.
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
#[derive(Clone)]
pub struct Reader(Arc<Inner>);
pub type Store = ae_trie::Overlay<Reader>;
impl std::fmt::Debug for Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedNodeReader").finish_non_exhaustive()
    }
}
impl NodeReader for Reader {
    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, TrieError> {
        let (reply, receive) = mpsc::sync_channel(1);
        self.0
            .sender
            .as_ref()
            .expect("live reader has a sender")
            .try_send(Request::Node { hash: *hash, reply })
            .map_err(|e| match e {
                TrySendError::Full(_) => TrieError::ResourceLimit,
                TrySendError::Disconnected(_) => closed(),
            })?;
        receive.recv().map_err(|_| closed())?
    }
}
fn closed() -> TrieError {
    TrieError::Backend("pinned snapshot worker ended".into())
}
fn node(
    snapshot: &Snapshot<'_>,
    hash: Hash256,
    limit: usize,
) -> Result<Option<Vec<u8>>, TrieError> {
    // Inspect length before allocating a reply. Hash checking matches Snapshot.
    let bytes = snapshot
        .snapshot
        .get_pinned(repo::key(repo::NODES, hash.0))
        .map_err(|e| TrieError::Backend(e.to_string()))?;
    let Some(bytes) = bytes else { return Ok(None) };
    if bytes.len() > limit {
        return Err(TrieError::ResourceLimit);
    }
    if blake2b_256(&bytes) != hash {
        return Err(TrieError::HashMismatch(hash));
    }
    Ok(Some(bytes.to_vec()))
}

#[cfg(test)]
mod tests;
