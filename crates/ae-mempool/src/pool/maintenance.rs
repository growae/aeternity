use super::*;
#[derive(Debug, Clone)]
pub struct FailureSettings {
    pub enabled: bool,
    pub common: BTreeMap<String, u64>,
    pub transactions: BTreeMap<String, BTreeMap<String, u64>>,
}
impl FailureSettings {
    pub fn limit(&self, kind: &str, reason: &str) -> Option<u64> {
        if !self.enabled {
            return None;
        }
        let specific = self.transactions.get(kind);
        specific
            .and_then(|m| m.get(reason))
            .or_else(|| self.common.get(reason))
            .or_else(|| specific.and_then(|m| m.get("fallback")))
            .or_else(|| self.common.get("fallback"))
            .copied()
            .or(Some(20))
    }
    pub fn reason<'a>(&self, tag: &'a str, tuple: bool) -> &'a str {
        if tuple && matches!(tag, "bad_label" | "invalid_codepoint") {
            "invalid_name"
        } else {
            tag
        }
    }
}
include!("failure_defaults.rs");
pub struct Reconcile<'a> {
    pub key_top: bool,
    pub requeue: &'a [Arc<Signed>],
    pub confirmed: &'a [Arc<Signed>],
}
impl Pool {
    pub fn ttl(&self, hash: Hash256) -> Result<i128, Error> {
        self.gc
            .get(&hash)
            .map(|(ttl, _)| *ttl)
            .ok_or(Error::NotFound)
    }
    // Existing transaction keys stay attached to their original GC record.
    pub fn shorten_ttl(&mut self, hash: Hash256, ttl: i128) -> Result<i128, Error> {
        let (current, _) = self.gc.get_mut(&hash).ok_or(Error::NotFound)?;
        *current = (*current).min(ttl);
        Ok(*current)
    }
    pub fn adjust_ttl(&mut self, difference: i128) -> Result<(), Error> {
        // Validate every subtraction before changing any metadata.
        for (ttl, _) in self.gc.values() {
            ttl.checked_sub(difference).ok_or(Error::ResourceLimit)?;
        }
        for (ttl, _) in self.gc.values_mut() {
            *ttl -= difference;
        }
        Ok(())
    }
    pub fn origins_len(&self) -> usize {
        self.origins.len()
    }

    pub fn update_gc_height(&mut self, local: u64, target: u64) {
        self.gc_height = self.gc_height.max(local).max(target);
    }
    pub fn record_failure(&mut self, hash: Hash256, limit: Option<u64>) -> Result<bool, Error> {
        let Some((_, key)) = self.gc.get(&hash) else {
            return Ok(false);
        };
        let key = key.clone();
        let entry = self
            .visited
            .get_mut(&key)
            .or_else(|| self.active.get_mut(&key))
            .ok_or(Error::NotFound)?;
        let next = entry.failures.checked_add(1).ok_or(Error::ResourceLimit)?;
        if limit.is_some_and(|l| next >= l) {
            self.remove(&key);
            Ok(true)
        } else {
            entry.failures = next;
            Ok(false)
        }
    }
    // The caller supplies the membership changes of one committed chain event.
    // All input decoding/lookups/budgets succeed before any pool mutation.
    pub fn reconcile(
        &mut self,
        change: Reconcile<'_>,
        mut lookup: impl FnMut(AccountPublicKey) -> Result<Option<AccountInfo>, Error>,
        record: ae_tx::storage::Config<'_>,
    ) -> Result<(), Error> {
        if change
            .requeue
            .len()
            .checked_add(change.confirmed.len())
            .is_none_or(|n| n > self.limits.work)
        {
            return Err(Error::ResourceLimit);
        }
        let confirmed: BTreeSet<_> = change
            .confirmed
            .iter()
            .map(|tx| {
                self.encode_size(tx, record)?;
                Key::of(tx)
            })
            .collect::<Result<_, Error>>()?;
        let confirmed_hashes: BTreeSet<_> = confirmed.iter().map(|k| k.hash).collect();
        let mut pending = BTreeMap::new();
        let mut cache: BTreeMap<AccountPublicKey, Option<AccountInfo>> = BTreeMap::new();
        let mut cached = |k| {
            if let Some(a) = cache.get(&k) {
                return Ok(a.clone());
            }
            let a = lookup(k)?;
            cache.insert(k, a.clone());
            Ok(a)
        };
        for tx in change.requeue {
            let bytes = self.encode_size(tx, record)?;
            let key = Key::of(tx)?;
            if confirmed_hashes.contains(&key.hash) || pending.contains_key(&key) {
                continue;
            }
            let expiry = self.expiry(tx, self.residency(tx, &mut cached)?)?;
            pending.insert(key, (tx.clone(), bytes, expiry));
        }
        let mut entries = self.len();
        let mut bytes = self.bytes;
        for key in &confirmed {
            for map in [&self.active, &self.visited] {
                if let Some(e) = map.get(key) {
                    entries -= 1;
                    bytes -= e.bytes;
                }
            }
        }
        for (key, (_, n, _)) in &pending {
            if let Some(e) = self.active.get(key) {
                bytes -= e.bytes;
            } else {
                entries = entries.checked_add(1).ok_or(Error::ResourceLimit)?;
            }
            bytes = bytes.checked_add(*n).ok_or(Error::ResourceLimit)?;
        }
        if entries > self.limits.entries || bytes > self.limits.retained_record_bytes {
            return Err(Error::ResourceLimit);
        }
        for key in confirmed {
            self.remove(&key);
            self.note_origin(key.origin, key.nonce);
        }
        for (key, (tx, bytes, expiry)) in pending {
            self.insert(key, tx, bytes, expiry);
        }
        if change.key_top {
            self.revisit();
        }
        Ok(())
    }
    pub fn note_origin(&mut self, origin: Option<AccountPublicKey>, nonce: Unsigned) {
        // The original capacity check also blocks updates to existing entries.
        if self.origins.len() < 5000 {
            self.origins
                .entry(origin)
                .and_modify(|n| *n = (*n).clone().max(nonce.clone()))
                .or_insert(nonce);
        }
    }
    pub fn collect_expired(
        &mut self,
        height: u64,
        max: usize,
        removable: impl FnMut(Hash256) -> Result<bool, Error>,
    ) -> Result<Vec<Hash256>, Error> {
        if height < self.gc_height {
            return Ok(vec![]);
        }
        self.collect_at(height, max, removable)
    }
    // Asynchronous native gc/2 does not apply sync_gc/1's height gate.
    pub fn collect_at(
        &mut self,
        height: u64,
        max: usize,
        mut removable: impl FnMut(Hash256) -> Result<bool, Error>,
    ) -> Result<Vec<Hash256>, Error> {
        let mut remove = vec![];
        let mut work = 0;
        for (hash, (ttl, key)) in &self.gc {
            if remove.len() == max {
                break;
            }
            work += 1;
            if work > self.limits.work {
                return Err(Error::ResourceLimit);
            }
            if *ttl <= i128::from(height) && removable(*hash)? {
                remove.push(key.clone());
            }
        }
        let hashes = remove.iter().map(|k| k.hash).collect();
        for key in remove {
            self.remove(&key);
        }
        Ok(hashes)
    }
    pub fn collect_origins(
        &mut self,
        mut lookup: impl FnMut(AccountPublicKey) -> Result<Option<AccountInfo>, Error>,
        mut removable: impl FnMut(Hash256) -> Result<bool, Error>,
    ) -> Result<Vec<Hash256>, Error> {
        let mut removed = BTreeSet::new();
        let mut clear = vec![];
        let mut work = 0;
        for (origin, nonce) in self.origins.iter().take(100) {
            let candidates: Vec<_> = self
                .nonces
                .get(origin)
                .into_iter()
                .flat_map(|n| n.range(..=nonce.clone()))
                .flat_map(|(_, keys)| keys)
                .take(50)
                .cloned()
                .collect();
            if candidates.is_empty() {
                clear.push(*origin);
                continue;
            }
            let Some(account) = origin.map(&mut lookup).transpose()?.flatten() else {
                continue;
            };
            for key in candidates {
                work += 1;
                if work > self.limits.work {
                    return Err(Error::ResourceLimit);
                }
                if key.nonce <= account.nonce && removable(key.hash)? {
                    removed.insert(key);
                }
            }
        }
        for origin in clear {
            self.origins.remove(&origin);
        }
        let hashes = removed.iter().map(|k| k.hash).collect();
        for key in removed {
            self.remove(&key);
        }
        Ok(hashes)
    }
}
