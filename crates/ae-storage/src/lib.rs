#![forbid(unsafe_code)]
use ae_chain::repository::{self as repo, Error, Identity, Read, Status, Update, View};
use ae_crypto::blake2b_256;
use ae_trie::NodeReader;
use ae_types::Hash256;
use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch, WriteOptions};
use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
static NEXT_SOURCE: AtomicU64 = AtomicU64::new(1);

pub struct Database {
    db: DB,
    identity: Identity,
    writer: Mutex<()>,
    source_id: u64,
}
#[derive(Clone)]
pub struct Snapshot<'a> {
    snapshot: Arc<rocksdb::Snapshot<'a>>,
    source_id: u64,
}
impl std::fmt::Debug for Snapshot<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("source_id", &self.source_id)
            .finish_non_exhaustive()
    }
}
fn backend(e: rocksdb::Error) -> Error {
    Error::Backend(e.to_string())
}
fn write_options() -> WriteOptions {
    let mut o = WriteOptions::default();
    o.set_sync(true);
    o.disable_wal(false);
    o
}
impl Database {
    pub fn open(path: impl AsRef<Path>, identity: Identity) -> Result<Self, Error> {
        if Identity::decode(&identity.encode())? != identity {
            return Err(Error::Identity);
        }
        let mut o = Options::default();
        o.create_if_missing(true);
        o.set_paranoid_checks(true);
        o.set_max_open_files(64);
        o.set_write_buffer_size(16 * 1024 * 1024);
        o.set_max_write_buffer_number(2);
        o.set_max_background_jobs(2);
        let db = DB::open(&o, path).map_err(backend)?;
        match db.get(repo::IDENTITY).map_err(backend)? {
            Some(b) => {
                if Identity::decode(&b)? != identity {
                    return Err(Error::Identity);
                }
            }
            None => {
                if db
                    .iterator(IteratorMode::Start)
                    .next()
                    .transpose()
                    .map_err(backend)?
                    .is_some()
                {
                    return Err(Error::Corrupt("unrecognized database"));
                }
                let mut batch = WriteBatch::default();
                batch.put(repo::IDENTITY, identity.encode());
                batch.put(repo::STATUS, Status::default().encode());
                db.write_opt(batch, &write_options()).map_err(backend)?;
            }
        }
        let source_id = NEXT_SOURCE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| Error::ResourceLimit)?;
        let result = Self {
            db,
            identity,
            writer: Mutex::new(()),
            source_id,
        };
        result.view()?.verify_tip()?;
        Ok(result)
    }
    pub fn view(&self) -> Result<View<Snapshot<'_>>, Error> {
        View::load(Snapshot {
            snapshot: Arc::new(self.db.snapshot()),
            source_id: self.source_id,
        })
    }
    pub fn commit(&self, update: Update) -> Result<Committed, Error> {
        self.commit_checked(update, || Ok(()))
    }
    fn commit_checked(
        &self,
        update: Update,
        mut check: impl FnMut() -> Result<(), Error>,
    ) -> Result<Committed, Error> {
        let _guard = self
            .writer
            .lock()
            .map_err(|_| Error::Backend("poisoned chain writer".into()))?;
        if update.source_id() != self.source_id || update.identity() != &self.identity {
            return Err(Error::Identity);
        }
        let current = Status::decode(
            &self
                .db
                .get(repo::STATUS)
                .map_err(backend)?
                .ok_or(Error::Corrupt("missing status"))?,
        )?;
        if current != update.expected() {
            return Err(Error::Conflict);
        }
        let mut batch = WriteBatch::default();
        for (k, v) in update.writes() {
            check()?;
            match v {
                Some(b) => batch.put(k, b),
                None => batch.delete(k),
            }
        }
        check()?;
        self.db
            .write_opt(batch, &write_options())
            .map_err(backend)?;
        Ok(Committed { update })
    }
}
// A receipt is available only after RocksDB acknowledges the synchronous WAL
// batch. Subscribers may resume using the persistent revision event stream.
#[derive(Debug)]
pub struct Committed {
    update: Update,
}
impl Committed {
    pub fn update(&self) -> &Update {
        &self.update
    }
    pub fn change(&self) -> &repo::Change {
        self.update.change()
    }
}
impl NodeReader for Snapshot<'_> {
    fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        let b = self
            .snapshot
            .get(repo::key(repo::NODES, h.0))
            .map_err(|e| ae_trie::Error::Backend(e.to_string()))?;
        if b.as_ref().is_some_and(|b| blake2b_256(b) != *h) {
            return Err(ae_trie::Error::HashMismatch(*h));
        }
        Ok(b)
    }
}
impl Read for Snapshot<'_> {
    fn source_id(&self) -> u64 {
        self.source_id
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.snapshot.get(key).map_err(backend)
    }
    fn prefix(&self, prefix: &[u8], limit: usize) -> Result<repo::Entries, Error> {
        let mut out = vec![];
        for pair in self
            .snapshot
            .iterator(IteratorMode::From(prefix, Direction::Forward))
        {
            let (k, v) = pair.map_err(backend)?;
            if !k.starts_with(prefix) {
                break;
            }
            if out.len() >= limit {
                return Err(Error::ResourceLimit);
            }
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests;
