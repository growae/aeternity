use super::*;
use repo::retention::{Config, Phase, Selection, State};
use std::collections::BTreeMap;
pub mod events;
pub mod worker;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub rows: usize,
    pub bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            rows: 100,
            bytes: 16 * 1024 * 1024,
        }
    }
}
fn add(value: &mut u64, count: usize) -> Result<(), Error> {
    *value = value
        .checked_add(u64::try_from(count).map_err(|_| Error::ResourceLimit)?)
        .ok_or(Error::ResourceLimit)?;
    Ok(())
}
fn after(prefix: u8, cursor: &[u8]) -> Vec<u8> {
    if cursor.is_empty() {
        vec![prefix]
    } else {
        let mut next = cursor.to_vec();
        next.push(0);
        next
    }
}
fn marker(bytes: Option<Vec<u8>>) -> Result<Option<u8>, Error> {
    match bytes.as_deref() {
        None => Ok(None),
        Some([n @ 0..=1]) => Ok(Some(*n)),
        _ => Err(Error::Corrupt("GC mark")),
    }
}
fn enqueue(
    db: &DB,
    writes: &mut repo::Writes,
    state: &mut State,
    hash: Hash256,
) -> Result<(), Error> {
    let key = repo::key(repo::GC_MARKS, hash.0);
    let value = match writes.get(&key) {
        Some(v) => v.clone(),
        None => db.get(&key).map_err(backend)?,
    };
    if marker(value)? != Some(1) {
        let pending_key = repo::key(repo::GC_PENDING, hash.0);
        let pending = match writes.get(&pending_key) {
            Some(value) => value.clone(),
            None => db.get(&pending_key).map_err(backend)?,
        };
        match pending {
            None => add(&mut state.pending, 1)?,
            Some(value) if value.is_empty() => {}
            _ => return Err(Error::Corrupt("GC pending node")),
        }
        writes.insert(key, Some(vec![0]));
        writes.insert(pending_key, Some(vec![]));
    }
    Ok(())
}
fn batch(writes: repo::Writes) -> WriteBatch {
    let mut batch = WriteBatch::default();
    for (key, value) in writes {
        match value {
            Some(value) => batch.put(key, value),
            None => batch.delete(key),
        }
    }
    batch
}
impl Database {
    pub fn gc_state(&self) -> Result<State, Error> {
        self.view()?.retention()
    }
    pub(super) fn validate_gc(&self) -> Result<(), Error> {
        let state = self.gc_state()?;
        let pending = self
            .db
            .iterator(IteratorMode::From(&[repo::GC_PENDING], Direction::Forward))
            .next()
            .transpose()
            .map_err(backend)?
            .is_some_and(|(key, _)| key.first() == Some(&repo::GC_PENDING));
        if state.active() && pending != (state.pending != 0) {
            return Err(Error::Corrupt("GC pending count"));
        }
        if !state.active() {
            for prefix in [repo::GC_MARKS, repo::GC_PENDING] {
                if let Some(pair) = self
                    .db
                    .iterator(IteratorMode::From(&[prefix], Direction::Forward))
                    .next()
                    && pair.map_err(backend)?.0.first() == Some(&prefix)
                {
                    return Err(Error::Corrupt("orphaned GC work"));
                }
            }
        }
        Ok(())
    }
    // Explicit maintenance starts at the current selected key top. Automatic
    // service scheduling uses maybe_begin_gc with the original height policy.
    pub fn begin_gc(&self, trees: Selection) -> Result<bool, Error> {
        self.begin_gc_checked(trees, None, || Ok(()))
    }
    pub fn maybe_begin_gc(&self, config: Config, synced: bool) -> Result<bool, Error> {
        self.begin_gc_checked(config.trees, Some((config, synced)), || Ok(()))
    }
    fn begin_gc_checked(
        &self,
        trees: Selection,
        configured: Option<(Config, bool)>,
        check: impl FnOnce() -> Result<(), Error>,
    ) -> Result<bool, Error> {
        self.begin_gc_at(trees, configured, None, check)
    }
    fn begin_gc_at(
        &self,
        trees: Selection,
        configured: Option<(Config, bool)>,
        generation_baseline: Option<u64>,
        check: impl FnOnce() -> Result<(), Error>,
    ) -> Result<bool, Error> {
        let mut policy = self.writer.lock().map_err(|_| backend_lock())?;
        let view = self.view_locked(&policy)?;
        let prior = view.retention()?;
        if let Some((config, _)) = configured {
            config.validate()?;
        }
        let Some(mut top) = view.status().tip else {
            return Ok(false);
        };
        let mut header = view.require_block(top)?.header();
        // Worker notifications may coalesce a selected key and its microblocks.
        // Resolve the currently selected generation while holding the writer.
        if generation_baseline.is_some() && matches!(header, ae_codec::headers::Header::Micro(_)) {
            let height = header.common().height;
            top = Hash256(header.common().previous_key_hash);
            header = view.require_block(top)?.header();
            if !matches!(header, ae_codec::headers::Header::Key(_))
                || header.common().height != height
                || view.canonical_key(height)? != Some(top)
            {
                return Err(Error::Corrupt("GC selected generation"));
            }
        }
        let key_block = matches!(header, ae_codec::headers::Header::Key(_));
        let height = header.common().height;
        if let Some((config, synced)) = configured {
            let mut schedule = prior.clone();
            if let Some(baseline) = generation_baseline {
                schedule.height = schedule.height.max(baseline);
            }
            if !config.eligible(height, key_block, synced, &schedule)? {
                return Ok(false);
            }
        } else if !key_block {
            return Err(Error::UnsupportedBlock);
        }
        let mut state = prior.start(height, trees)?;
        let generation = policy
            .generation
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        let mut batch = WriteBatch::default();
        batch.delete_range([repo::GC_MARKS], [repo::GC_MARKS + 1]);
        batch.delete_range([repo::GC_PENDING], [repo::GC_PENDING + 1]);
        let roots: std::collections::BTreeSet<_> = view
            .require_record(top)?
            .roots
            .array()
            .into_iter()
            .flatten()
            .collect();
        for hash in roots {
            batch.put(repo::key(repo::GC_MARKS, hash.0), [0]);
            batch.put(repo::key(repo::GC_PENDING, hash.0), []);
            add(&mut state.pending, 1)?;
        }
        batch.put(repo::GC_STATE, state.encode());
        check()?;
        // A failed engine write can have an uncertain durable outcome. Invalidate
        // old insertion/rollback plans even if the engine returns an error.
        policy.generation = generation;
        self.write_gc(batch)?;
        self.gc_events.advance(&prior, &state, view.status());
        Ok(true)
    }
    pub fn cancel_gc(&self) -> Result<State, Error> {
        let guard = self.writer.lock().map_err(|_| backend_lock())?;
        let view = self.view_locked(&guard)?;
        let prior = view.retention()?;
        let mut state = prior.clone();
        if state.active() {
            state.phase = Phase::Cancelled;
            state.pending = 0;
            state.cursor.clear();
            let mut batch = WriteBatch::default();
            batch.delete_range([repo::GC_MARKS], [repo::GC_MARKS + 1]);
            batch.delete_range([repo::GC_PENDING], [repo::GC_PENDING + 1]);
            batch.put(repo::GC_STATE, state.encode());
            self.write_gc(batch)?;
            self.gc_events.advance(&prior, &state, view.status());
        }
        // Cancellation never promises that previously removed state returned.
        Ok(state)
    }
    pub fn gc_step(&self, limits: Limits) -> Result<State, Error> {
        self.gc_step_checked(limits, || Ok(()))
    }
    fn gc_step_checked(
        &self,
        limits: Limits,
        check: impl FnOnce() -> Result<(), Error>,
    ) -> Result<State, Error> {
        if limits.rows == 0 || limits.bytes == 0 {
            return Err(Error::ResourceLimit);
        }
        let guard = self.writer.lock().map_err(|_| backend_lock())?;
        let view = self.view_locked(&guard)?;
        let prior = view.retention()?;
        let mut state = prior.clone();
        if !state.active() {
            return Ok(state);
        }
        let mut writes = BTreeMap::new();
        let mut used = 0usize;
        let mut rows = 0usize;
        let initial_pending = state.pending;
        // Work added by concurrent commits preempts deletion, including after
        // sweeping has begun. Marks and pending references publish atomically.
        for pair in self
            .db
            .iterator(IteratorMode::From(&[repo::GC_PENDING], Direction::Forward))
        {
            let (key, value) = pair.map_err(backend)?;
            if key.first() != Some(&repo::GC_PENDING) || rows == limits.rows {
                break;
            }
            if key.len() != 33 || !value.is_empty() {
                return Err(Error::Corrupt("GC pending node"));
            }
            let hash =
                Hash256::try_from(&key[1..]).map_err(|_| Error::Corrupt("GC pending hash"))?;
            let raw = view
                .source()
                .node(&hash)?
                .ok_or(ae_trie::Error::MissingNode(hash))?;
            if used.checked_add(raw.len()).is_none_or(|n| n > limits.bytes) {
                if rows == 0 {
                    return Err(Error::ResourceLimit);
                } else {
                    break;
                }
            }
            let children = ae_trie::scan::inspect_node(hash, &raw, view.source().decode_limits())?;
            state.pending = state
                .pending
                .checked_sub(1)
                .ok_or(Error::Corrupt("GC pending count"))?;
            writes.insert(repo::key(repo::GC_MARKS, hash.0), Some(vec![1]));
            writes.insert(key.to_vec(), None);
            for child in children {
                enqueue(&self.db, &mut writes, &mut state, child)?;
            }
            used += raw.len();
            rows += 1;
            add(&mut state.nodes, 1)?;
            add(&mut state.bytes, raw.len())?;
        }
        let mut complete = false;
        if rows == 0 {
            if initial_pending != 0 {
                return Err(Error::Corrupt("GC pending count"));
            }
            let prefix = if state.phase == Phase::Roots {
                repo::RECORDS
            } else {
                repo::NODES
            };
            let start = after(prefix, &state.cursor);
            let mut exhausted = true;
            for pair in self
                .db
                .iterator(IteratorMode::From(&start, Direction::Forward))
            {
                let (key, value) = pair.map_err(backend)?;
                if key.first() != Some(&prefix) {
                    break;
                }
                if rows == limits.rows {
                    exhausted = false;
                    break;
                }
                if used
                    .checked_add(value.len())
                    .is_none_or(|n| n > limits.bytes)
                {
                    if rows == 0 {
                        return Err(Error::ResourceLimit);
                    }
                    exhausted = false;
                    break;
                }
                if state.phase == Phase::Roots {
                    if key.len() != 33 {
                        return Err(Error::Corrupt("GC state record key"));
                    }
                    {
                        let hash = Hash256::try_from(&key[1..])
                            .map_err(|_| Error::Corrupt("GC block hash"))?;
                        let block_bytes = self
                            .db
                            .get(repo::key(repo::BLOCKS, hash.0))
                            .map_err(backend)?
                            .ok_or(Error::Missing(hash))?;
                        let size = value
                            .len()
                            .checked_add(block_bytes.len())
                            .and_then(|n| n.checked_add(used))
                            .filter(|n| *n <= limits.bytes);
                        let Some(size) = size else {
                            if rows == 0 {
                                return Err(Error::ResourceLimit);
                            }
                            exhausted = false;
                            break;
                        };
                        let record = repo::Record::decode(&value)?;
                        let header = view.require_block(hash)?.header();
                        let height = header.common().height;
                        if header.common().state_hash != record.roots.state.hash().0 {
                            return Err(Error::Corrupt("GC state binding"));
                        }
                        for (i, root) in record.roots.array().into_iter().enumerate() {
                            if state.retains(i, height)
                                && let Some(hash) = root
                            {
                                enqueue(&self.db, &mut writes, &mut state, hash)?;
                            }
                        }
                        used = size;
                    }
                    add(&mut state.records, 1)?;
                } else {
                    if key.len() != 33 {
                        return Err(Error::Corrupt("GC stored node key"));
                    }
                    match marker(
                        self.db
                            .get(repo::key(repo::GC_MARKS, &key[1..]))
                            .map_err(backend)?,
                    )? {
                        None => {
                            writes.insert(key.to_vec(), None);
                            add(&mut state.removed, 1)?;
                        }
                        Some(1) => {}
                        _ => return Err(Error::Corrupt("unfinished GC mark")),
                    }
                    used += value.len();
                }
                rows += 1;
                state.cursor = key.to_vec();
            }
            if exhausted {
                state.cursor.clear();
                if state.phase == Phase::Roots {
                    state.phase = Phase::Sweep;
                } else {
                    state.phase = Phase::Complete;
                    complete = true;
                }
            }
        }
        writes.insert(repo::GC_STATE.to_vec(), Some(state.encode()));
        let mut batch = batch(writes);
        if complete {
            batch.delete_range([repo::GC_MARKS], [repo::GC_MARKS + 1]);
            batch.delete_range([repo::GC_PENDING], [repo::GC_PENDING + 1]);
        }
        if batch.size_in_bytes() > limits.bytes {
            return Err(Error::ResourceLimit);
        }
        check()?;
        self.write_gc(batch)?;
        self.gc_events.advance(&prior, &state, view.status());
        Ok(state)
    }
    fn write_gc(&self, batch: WriteBatch) -> Result<(), Error> {
        let result = self.write_gc_engine(batch);
        self.wake_gc();
        if result.is_err() {
            self.gc_events.fail(events::Failure::UncertainWrite);
        }
        result
    }
    fn write_gc_engine(&self, batch: WriteBatch) -> Result<(), Error> {
        #[cfg(test)]
        let failure = self.gc_write_failure.swap(0, Ordering::Relaxed);
        #[cfg(test)]
        if failure == 1 {
            return Err(Error::Backend("injected GC write failure".into()));
        }
        self.db
            .write_opt(batch, &write_options())
            .map_err(backend)?;
        #[cfg(test)]
        if failure == 2 {
            return Err(Error::Backend("injected GC acknowledgment loss".into()));
        }
        Ok(())
    }
    pub(super) fn gc_barrier(
        &self,
        update: &Update,
        batch: &mut WriteBatch,
    ) -> Result<Option<(State, State)>, Error> {
        let mut state = self
            .db
            .get(repo::GC_STATE)
            .map_err(backend)?
            .map(|b| State::decode(&b))
            .transpose()?
            .unwrap_or_default();
        if !state.active() {
            return Ok(None);
        }
        let prior = state.clone();
        let mut writes = BTreeMap::new();
        for (key, value) in update.writes() {
            let Some(value) = value else {
                continue;
            };
            if key.first() == Some(&repo::NODES) {
                let hash = Hash256::try_from(&key[1..])
                    .map_err(|_| Error::Corrupt("GC inserted node key"))?;
                enqueue(&self.db, &mut writes, &mut state, hash)?;
            } else if key.first() == Some(&repo::RECORDS) {
                for hash in repo::Record::decode(value)?
                    .roots
                    .array()
                    .into_iter()
                    .flatten()
                {
                    enqueue(&self.db, &mut writes, &mut state, hash)?;
                }
            }
        }
        let mut total = 0usize;
        writes.insert(repo::GC_STATE.to_vec(), Some(state.encode()));
        for (key, value) in update.writes().iter().chain(writes.iter()) {
            total = total
                .checked_add(key.len())
                .and_then(|n| n.checked_add(value.as_ref().map_or(0, Vec::len)))
                .ok_or(Error::ResourceLimit)?;
            if total > update.commit_bytes_limit() {
                return Err(Error::ResourceLimit);
            }
        }
        for (key, value) in writes {
            batch.put(key, value.expect("enqueue only inserts"));
        }
        Ok(Some((prior, state)))
    }
}

#[cfg(test)]
mod tests;
