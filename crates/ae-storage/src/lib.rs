#![forbid(unsafe_code)]
use ae_chain::repository::{self as repo, Error, Identity, Read, ReadConfig, Status, Update, View};
use ae_chain::resilience;
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
    writer: Mutex<resilience::Snapshot>,
    source_id: u64,
    config: ReadConfig,
}
#[derive(Clone)]
pub struct Snapshot<'a> {
    snapshot: Arc<rocksdb::Snapshot<'a>>,
    source_id: u64,
    policy: resilience::Snapshot,
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
        Self::open_with_config(path, identity, ReadConfig::default())
    }
    pub fn open_with_config(
        path: impl AsRef<Path>,
        identity: Identity,
        config: ReadConfig,
    ) -> Result<Self, Error> {
        Self::open_with_recovery_limits(path, identity, config, Default::default())
    }
    pub fn open_with_recovery_limits(
        path: impl AsRef<Path>,
        identity: Identity,
        config: ReadConfig,
        recovery: repo::rollback::Limits,
    ) -> Result<Self, Error> {
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
        let mut upgrade = false;
        match db.get(repo::IDENTITY).map_err(backend)? {
            Some(b) => {
                if Identity::decode(&b)? != identity {
                    return Err(Error::Identity);
                }
                upgrade = b != identity.encode();
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
            writer: Mutex::new(Default::default()),
            source_id,
            config,
        };
        result.view()?.verify_tip()?;
        if upgrade {
            // Current readers retain legacy block and transaction formats.
            // Mark the new writer capability atomically only after validation;
            // old binaries then refuse to open instead of losing metadata.
            let mut batch = WriteBatch::default();
            batch.put(repo::IDENTITY, result.identity.encode());
            result
                .db
                .write_opt(batch, &write_options())
                .map_err(backend)?;
        }
        result.enforce_community_fork(recovery)?;
        Ok(result)
    }
    pub fn rollback(
        &self,
        target: repo::rollback::Target,
        limits: repo::rollback::Limits,
    ) -> Result<Committed, repo::rollback::Failure> {
        let plan = repo::rollback::prepare(&self.view()?, target, limits)?;
        Ok(self.commit(plan)?)
    }
    pub fn enforce_community_fork(
        &self,
        limits: repo::rollback::Limits,
    ) -> Result<Option<Committed>, Error> {
        let view = self.view()?;
        let Some(target) = repo::rollback::community_fork_target(&view)? else {
            return Ok(None);
        };
        let plan = repo::rollback::prepare(&view, target, limits).map_err(|error| match error {
            repo::rollback::Failure::Repository(e) => e,
            _ => Error::Corrupt("community rollback target"),
        })?;
        Ok(Some(self.commit(plan)?))
    }
    pub fn view(&self) -> Result<View<Snapshot<'_>>, Error> {
        let policy = self.writer.lock().map_err(|_| backend_lock())?;
        self.view_locked(&policy)
    }
    fn view_locked(&self, policy: &resilience::Snapshot) -> Result<View<Snapshot<'_>>, Error> {
        View::load_with_config(
            Snapshot {
                snapshot: Arc::new(self.db.snapshot()),
                source_id: self.source_id,
                policy: *policy,
            },
            self.config.clone(),
        )
    }
    pub fn open_with_resilience(
        path: impl AsRef<Path>,
        identity: Identity,
        read: ReadConfig,
        policy: resilience::Config,
    ) -> Result<Self, Error> {
        let db = Self::open_with_config(path, identity, read)?;
        db.update_resilience(resilience::Event::Start, policy)?;
        Ok(db)
    }
    // Config is an explicit snapshot of external configuration. A worker Start
    // retains runtime state; reopening the database starts a fresh lifecycle.
    pub fn update_resilience(
        &self,
        event: resilience::Event,
        config: resilience::Config,
    ) -> Result<resilience::Snapshot, Error> {
        self.update_resilience_checked(event, config, || Ok(()))
    }
    fn update_resilience_checked(
        &self,
        event: resilience::Event,
        config: resilience::Config,
        check: impl FnOnce() -> Result<(), Error>,
    ) -> Result<resilience::Snapshot, Error> {
        let mut policy = self.writer.lock().map_err(|_| backend_lock())?;
        let view = self.view_locked(&policy)?;
        let (state, ensure) = policy.state.transition(event, config);
        let old = view.finalized_height()?;
        let new = if let (Some(depth), Some(tip)) = (ensure, view.status().tip) {
            resilience::finalized_at(view.require_block(tip)?.header().common().height, depth)
                .or(old)
        } else {
            old
        };
        if policy.state != state || old != new {
            let generation = policy
                .generation
                .checked_add(1)
                .ok_or(Error::ResourceLimit)?;
            check()?;
            if new != old {
                let mut batch = WriteBatch::default();
                batch.put(
                    repo::FINALIZED,
                    repo::encode_finalized(new.expect("positive finalized height"))?,
                );
                // An engine error can have an uncertain durable outcome. Old
                // plans must be invalidated even when the write returns Err.
                policy.generation = generation;
                self.db
                    .write_opt(batch, &write_options())
                    .map_err(backend)?;
            }
            *policy = resilience::Snapshot { generation, state };
        }
        Ok(*policy)
    }
    pub fn clear_finalized_height(&self) -> Result<(), Error> {
        let mut policy = self.writer.lock().map_err(|_| backend_lock())?;
        if self.view_locked(&policy)?.finalized_height()?.is_some() {
            let generation = policy
                .generation
                .checked_add(1)
                .ok_or(Error::ResourceLimit)?;
            let mut batch = WriteBatch::default();
            batch.delete(repo::FINALIZED);
            policy.generation = generation;
            self.db
                .write_opt(batch, &write_options())
                .map_err(backend)?;
        }
        Ok(())
    }
    pub fn commit(&self, update: Update) -> Result<Committed, Error> {
        self.commit_checked(update, || Ok(()))
    }
    fn commit_checked(
        &self,
        update: Update,
        mut check: impl FnMut() -> Result<(), Error>,
    ) -> Result<Committed, Error> {
        let guard = self
            .writer
            .lock()
            .map_err(|_| Error::Backend("poisoned chain writer".into()))?;
        if update.source_id() != self.source_id || update.identity() != &self.identity {
            return Err(Error::Identity);
        }
        if update.policy_generation() != guard.generation {
            return Err(Error::Conflict);
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
    fn visit_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        visit: &mut repo::Visitor<'_>,
    ) -> Result<(), Error> {
        for (count, pair) in self
            .snapshot
            .iterator(IteratorMode::From(start, Direction::Forward))
            .enumerate()
        {
            let (k, v) = pair.map_err(backend)?;
            if k.as_ref() >= end {
                break;
            }
            if count >= limit {
                return Err(Error::ResourceLimit);
            }
            visit(&k, &v)?;
        }
        Ok(())
    }
    fn range(&self, start: &[u8], end: &[u8], limit: usize) -> Result<repo::Entries, Error> {
        let mut out = vec![];
        for pair in self
            .snapshot
            .iterator(IteratorMode::From(start, Direction::Forward))
        {
            let (k, v) = pair.map_err(backend)?;
            if k.as_ref() >= end {
                break;
            }
            if out.len() >= limit {
                return Err(Error::ResourceLimit);
            }
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }
    fn source_id(&self) -> u64 {
        self.source_id
    }
    fn resilience(&self) -> resilience::Snapshot {
        self.policy
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
fn backend_lock() -> Error {
    Error::Backend("poisoned chain writer".into())
}

#[cfg(test)]
mod tests;
