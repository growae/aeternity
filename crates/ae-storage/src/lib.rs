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
pub mod events;
pub mod gc;
pub mod hyperchain;
pub mod peers;
pub mod policy;
pub mod pool;
pub mod replay;

#[derive(Debug, Clone, Copy, Default)]
pub struct Notifications {
    pub chain: events::Config,
    pub gc: gc::events::Config,
}

pub struct Database {
    db: DB,
    identity: Identity,
    writer: Mutex<resilience::Snapshot>,
    policy_owner: Mutex<std::sync::Weak<policy::Inner>>,
    policy_recovery: std::sync::atomic::AtomicBool,
    policy_managed: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    policy_write_failure: std::sync::atomic::AtomicU8,
    source_id: u64,
    config: ReadConfig,
    gc_worker: Mutex<std::sync::Weak<gc::worker::Shared>>,
    pool_owner: Mutex<std::sync::Weak<()>>,
    pool_worker: Mutex<std::sync::Weak<pool::worker::Shared>>,
    peer_owner: Mutex<std::sync::Weak<()>>,
    hyperchain_owner: Mutex<std::sync::Weak<()>>,
    events: events::Source,
    gc_events: gc::events::Source,
    #[cfg(test)]
    gc_write_failure: std::sync::atomic::AtomicU8,
    #[cfg(test)]
    event_write_failure: std::sync::atomic::AtomicU8,
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
        Self::open_mode(path, identity, config, recovery, false)
    }
    fn open_mode(
        path: impl AsRef<Path>,
        identity: Identity,
        config: ReadConfig,
        recovery: repo::rollback::Limits,
        importing: bool,
    ) -> Result<Self, Error> {
        Self::open_configured(
            path,
            identity,
            config,
            recovery,
            importing,
            Default::default(),
        )
    }
    pub fn open_with_events(
        path: impl AsRef<Path>,
        identity: Identity,
        config: ReadConfig,
        recovery: repo::rollback::Limits,
        events: events::Config,
    ) -> Result<Self, Error> {
        Self::open_with_notifications(
            path,
            identity,
            config,
            recovery,
            Notifications {
                chain: events,
                ..Default::default()
            },
        )
    }
    pub fn open_with_notifications(
        path: impl AsRef<Path>,
        identity: Identity,
        config: ReadConfig,
        recovery: repo::rollback::Limits,
        notifications: Notifications,
    ) -> Result<Self, Error> {
        Self::open_configured(path, identity, config, recovery, false, notifications)
    }
    fn open_configured(
        path: impl AsRef<Path>,
        identity: Identity,
        config: ReadConfig,
        recovery: repo::rollback::Limits,
        importing: bool,
        notifications: Notifications,
    ) -> Result<Self, Error> {
        notifications.chain.validate()?;
        notifications.gc.validate()?;
        if Identity::decode(&identity.encode())? != identity
            || identity.header_mode() != config.mode
        {
            return Err(Error::Identity);
        }
        let mut o = Options::default();
        o.create_if_missing(true);
        o.set_paranoid_checks(true);
        o.set_max_open_files(64);
        o.set_write_buffer_size(16 * 1024 * 1024);
        o.set_max_write_buffer_number(2);
        o.set_max_background_jobs(2);
        let path = path.as_ref();
        let db = DB::open(&o, path).map_err(backend)?;
        if !importing
            && db.get(replay::PROGRESS).map_err(backend)?.is_none()
            && replay::has_marker(path)?
        {
            return Err(Error::IncompleteImport);
        }
        if importing && db.get(replay::PROGRESS).map_err(backend)?.is_none() {
            for entry in db.iterator(IteratorMode::Start) {
                let (key, _) = entry.map_err(backend)?;
                if key.as_ref() != repo::IDENTITY && key.as_ref() != repo::STATUS {
                    return Err(Error::NotEmpty);
                }
            }
        }
        if importing
            && db.get(replay::PROGRESS).map_err(backend)?.is_none()
            && db
                .get(repo::STATUS)
                .map_err(backend)?
                .map(|b| Status::decode(&b))
                .transpose()?
                .is_some_and(|s| s != Status::default())
        {
            return Err(Error::NotEmpty);
        }
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
        let event_status = Status::decode(
            &db.get(repo::STATUS)
                .map_err(backend)?
                .ok_or(Error::Corrupt("missing status"))?,
        )?;
        let gc_state = db
            .get(repo::GC_STATE)
            .map_err(backend)?
            .map(|b| repo::retention::State::decode(&b))
            .transpose()?
            .unwrap_or_default();
        let result = Self {
            db,
            identity,
            writer: Mutex::new(Default::default()),
            policy_owner: Mutex::new(Default::default()),
            policy_recovery: std::sync::atomic::AtomicBool::new(false),
            policy_managed: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            policy_write_failure: std::sync::atomic::AtomicU8::new(0),
            source_id,
            config,
            gc_worker: Mutex::new(Default::default()),
            pool_owner: Mutex::new(Default::default()),
            pool_worker: Mutex::new(Default::default()),
            peer_owner: Mutex::new(Default::default()),
            hyperchain_owner: Mutex::new(Default::default()),
            events: events::Source::new(event_status, notifications.chain),
            gc_events: gc::events::Source::new(gc_state, event_status, notifications.gc),
            #[cfg(test)]
            gc_write_failure: std::sync::atomic::AtomicU8::new(0),
            #[cfg(test)]
            event_write_failure: std::sync::atomic::AtomicU8::new(0),
        };
        result.view()?.verify_tip()?;
        result.validate_gc()?;
        result.validate_replay(importing)?;
        result.validate_pool()?;
        result.validate_peers()?;
        if upgrade {
            // Current readers retain legacy block and transaction formats.
            // Mark the new writer capability atomically only after validation;
            // old binaries then refuse to open instead of losing metadata.
            let mut batch = WriteBatch::default();
            batch.put(repo::IDENTITY, result.identity.encode());
            if result.identity.consensus == ae_protocol::ConsensusType::Pos
                && result.db.get(repo::HC_STATE).map_err(backend)?.is_none()
            {
                let view = result.view()?;
                if view.status().tip.is_some() {
                    // Version 10 admitted only genesis. Preserve its explicit
                    // empty historical event interval when upgrading.
                    let checkpoint = view.hyperchain_checkpoint()?;
                    batch.put(
                        repo::HC_STATE,
                        checkpoint.encode(result.config.hyperchain_metadata)?,
                    );
                }
            }
            result
                .db
                .write_opt(batch, &write_options())
                .map_err(backend)?;
        }
        if !importing {
            result.enforce_community_fork(recovery)?;
        }
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
    pub fn hyperchain_reader<O>(
        &self,
        identity: &Identity,
        profile: Arc<repo::hyperchain::Profile<O>>,
        config: repo::hyperchain::Config,
    ) -> Result<repo::hyperchain::Reader<Snapshot<'_>, O>, Error> {
        repo::hyperchain::reader(self.view()?, identity, profile, config)
    }
    pub fn gc_events(&self) -> gc::events::Reader {
        self.gc_events.reader()
    }
    pub fn gc_event_snapshot(&self) -> Result<(View<Snapshot<'_>>, gc::events::Cursor), Error> {
        let policy = self.writer.lock().map_err(|_| backend_lock())?;
        let view = self.view_locked(&policy)?;
        let (info, cursor) = self.gc_events.reader().snapshot();
        if info.closed || info.state != view.retention()? {
            return Err(Error::Backend("GC event source requires reopen".into()));
        }
        Ok((view, cursor))
    }
    pub fn events(&self) -> events::Reader {
        self.events.reader()
    }
    // Commit and snapshot both take writer -> event-source locks. A consumer
    // can reconcile this pinned view and then replay strictly after its cursor.
    pub fn event_snapshot(&self) -> Result<(View<Snapshot<'_>>, events::Cursor), Error> {
        let policy = self.writer.lock().map_err(|_| backend_lock())?;
        let view = self.view_locked(&policy)?;
        let reader = self.events.reader();
        let info = reader.info();
        if info.closed || info.status != view.status() {
            return Err(Error::Backend("commit event source requires reopen".into()));
        }
        Ok((view, reader.cursor()))
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
        self.update_resilience_owned(event, config, None, check)
    }
    fn update_resilience_owned(
        &self,
        event: resilience::Event,
        config: resilience::Config,
        owner: Option<&Arc<policy::Inner>>,
        check: impl FnOnce() -> Result<(), Error>,
    ) -> Result<resilience::Snapshot, Error> {
        let mut policy = self.writer.lock().map_err(|_| backend_lock())?;
        self.authorize_policy(owner)?;
        self.ensure_policy_recovered()?;
        if event != resilience::Event::Start
            && owner.is_some_and(|o| !o.ready.load(Ordering::Acquire))
        {
            return Err(Error::PolicyClosed);
        }
        self.policy_operation(|| {
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
                    self.write_policy(batch)?;
                }
                *policy = resilience::Snapshot { generation, state };
            }
            if let Some(owner) = owner {
                owner.ready.store(true, Ordering::Release);
            }
            Ok(*policy)
        })
    }
    pub fn clear_finalized_height(&self) -> Result<(), Error> {
        self.clear_finalized_owned(None)
    }
    fn clear_finalized_owned(&self, owner: Option<&Arc<policy::Inner>>) -> Result<(), Error> {
        let mut policy = self.writer.lock().map_err(|_| backend_lock())?;
        self.authorize_policy(owner)?;
        self.ensure_policy_recovered()?;
        self.policy_operation(|| {
            if self.view_locked(&policy)?.finalized_height()?.is_some() {
                let generation = policy
                    .generation
                    .checked_add(1)
                    .ok_or(Error::ResourceLimit)?;
                let mut batch = WriteBatch::default();
                batch.delete(repo::FINALIZED);
                policy.generation = generation;
                self.write_policy(batch)?;
            }
            Ok(())
        })
    }
    pub fn commit(&self, update: Update) -> Result<Committed, Error> {
        self.commit_checked(update, || Ok(()))
    }
    fn commit_checked(
        &self,
        update: Update,
        check: impl FnMut() -> Result<(), Error>,
    ) -> Result<Committed, Error> {
        self.commit_extra(update, None, check)
    }
    fn commit_extra(
        &self,
        update: Update,
        extra: Option<(&[u8], &[u8])>,
        mut check: impl FnMut() -> Result<(), Error>,
    ) -> Result<Committed, Error> {
        let guard = self
            .writer
            .lock()
            .map_err(|_| Error::Backend("poisoned chain writer".into()))?;
        self.ensure_policy_recovered()?;
        if !self.policy_ready() {
            return Err(Error::PolicyClosed);
        }
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
        let pool_writes = self.pool_chain_writes(update.change(), update.writes())?;
        let extra_bytes = extra
            .map_or(Some(0), |(k, v)| k.len().checked_add(v.len()))
            .ok_or(Error::ResourceLimit)?;
        let total = pool::write_bytes(update.writes())?
            .checked_add(pool::write_bytes(&pool_writes)?)
            .and_then(|n| n.checked_add(extra_bytes))
            .ok_or(Error::ResourceLimit)?;
        if total > update.commit_bytes_limit() {
            return Err(Error::ResourceLimit);
        }
        let mut batch = WriteBatch::default();
        for (k, v) in update.writes().iter().chain(pool_writes.iter()) {
            check()?;
            match v {
                Some(b) => batch.put(k, b),
                None => batch.delete(k),
            }
        }
        let gc_progress = self.gc_barrier(&update, &mut batch)?;
        if let Some((key, value)) = extra {
            batch.put(key, value);
        }
        check()?;
        let publication = self.events.prepare(&update);
        let result = self.write_commit(batch);
        self.wake_gc();
        self.wake_pool();
        if let Err(error) = result {
            self.events.fail(events::Failure::UncertainWrite);
            self.gc_events.fail(gc::events::Failure::UncertainWrite);
            return Err(error);
        }
        if let Some((prior, next)) = gc_progress {
            self.gc_events.advance(
                &prior,
                &next,
                Status {
                    revision: update.change().revision,
                    tip: Some(update.change().new_tip),
                },
            );
        }
        self.events.publish(&update, publication);
        Ok(Committed { update })
    }
    fn write_commit(&self, batch: WriteBatch) -> Result<(), Error> {
        #[cfg(test)]
        let failure = self.event_write_failure.swap(0, Ordering::Relaxed);
        #[cfg(test)]
        if failure == 1 {
            return Err(Error::Backend("injected unacknowledged write".into()));
        }
        self.db
            .write_opt(batch, &write_options())
            .map_err(backend)?;
        #[cfg(test)]
        if failure == 2 {
            return Err(Error::Backend("injected acknowledgment loss".into()));
        }
        Ok(())
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
