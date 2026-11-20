// Durable ordinary-pool membership is independent of authenticated chain state.
// aec_db/aec_tx_pool, aeternity fa2a6d7. Copyright (c) 2018-2019,
// Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use ae_mempool::pool::{Location, Signed};
use ae_trie::Value;
use ae_types::Unsigned;
use std::collections::BTreeSet;
mod service;
pub mod sync;
pub mod worker;
pub use service::{Config, Failure, Inspection, Service, Submission};
pub const RECORDS: u8 = 16;
pub const MEMBERS: u8 = 17;
pub const REVISION: &[u8] = b"\0pool";
const MEMBER: &[u8] = &[1];
fn encode_revision(revision: u64) -> Vec<u8> {
    Value::List(vec![
        Value::Bytes(vec![1]),
        Value::Bytes(Unsigned::from(revision).as_bytes().to_vec()),
    ])
    .encode()
}
fn decode_revision(bytes: Option<Vec<u8>>) -> Result<u64, Error> {
    let Some(bytes) = bytes else {
        return Ok(0);
    };
    if bytes.len() > 16 {
        return Err(Error::Corrupt("pool revision length"));
    }
    let Value::List(fields) =
        Value::decode(&bytes).map_err(|_| Error::Corrupt("pool revision RLP"))?
    else {
        return Err(Error::Corrupt("pool revision"));
    };
    let [Value::Bytes(version), Value::Bytes(number)] = fields.as_slice() else {
        return Err(Error::Corrupt("pool revision fields"));
    };
    let revision = Unsigned::from_field_bytes(number)
        .and_then(|n| n.to_u64())
        .map_err(|_| Error::Corrupt("pool revision integer"))?;
    if version != &[1] || revision == 0 || encode_revision(revision) != bytes {
        return Err(Error::Corrupt("pool revision encoding"));
    }
    Ok(revision)
}
fn member(value: Option<Vec<u8>>) -> Result<bool, Error> {
    match value.as_deref() {
        None => Ok(false),
        Some(MEMBER) => Ok(true),
        _ => Err(Error::Corrupt("pool membership")),
    }
}
fn hash_key(key: &[u8]) -> Result<Hash256, Error> {
    key.get(1..)
        .and_then(|b| b.try_into().ok())
        .map(Hash256)
        .ok_or(Error::Corrupt("pool hash key"))
}
fn decode_record(
    hash: Hash256,
    bytes: &[u8],
    config: ae_tx::storage::Config<'_>,
) -> Result<Signed, Error> {
    let tx = ae_tx::storage::decode(bytes, config)?;
    if tx.hash() != hash {
        return Err(Error::Corrupt("pool transaction hash"));
    }
    Ok(tx)
}
pub struct Stored<S: Read> {
    chain: View<S>,
    revision: u64,
}
impl<S: Read> Stored<S> {
    pub fn load(chain: View<S>) -> Result<Self, Error> {
        let revision = decode_revision(chain.source().get(REVISION)?)?;
        Ok(Self { chain, revision })
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn chain(&self) -> &View<S> {
        &self.chain
    }
    pub fn transaction(&self, hash: Hash256) -> Result<Option<Signed>, Error> {
        // A block's record has precedence, matching the original shared record.
        // Local pool submissions never replace authenticated block metadata.
        if let Some(tx) = self.chain.transaction(hash)? {
            return Ok(Some(tx));
        }
        self.chain
            .source()
            .get(&repo::key(RECORDS, hash.0))?
            .map(|b| decode_record(hash, &b, self.chain.config().transaction_record()))
            .transpose()
    }
    pub fn location(&self, hash: Hash256) -> Result<Location, Error> {
        if let Some(block) = self.chain.location(hash)? {
            return Ok(Location::Block(block));
        }
        let present = member(self.chain.source().get(&repo::key(MEMBERS, hash.0))?)?;
        let known = self.transaction(hash)?.is_some();
        match (present, known) {
            (true, true) => Ok(Location::Pool),
            (true, false) => Err(Error::Corrupt("pool member has no record")),
            (false, true) => Ok(Location::Deleted),
            (false, false) => Ok(Location::Unknown),
        }
    }
    pub fn members(&self, limit: usize) -> Result<Vec<Hash256>, Error> {
        let mut out = vec![];
        self.chain
            .source()
            .visit_range(&[MEMBERS], &[MEMBERS + 1], limit, &mut |k, v| {
                member(Some(v.to_vec()))?;
                out.push(hash_key(k)?);
                Ok(())
            })?;
        Ok(out)
    }
    fn retain(&self, tx: &Signed, writes: &mut repo::Writes) -> Result<(), Error> {
        let hash = tx.hash();
        if let Some(old) = self.transaction(hash)?
            && old != *tx
        {
            return Err(Error::TransactionMetadataConflict);
        }
        if !matches!(self.location(hash)?, Location::Block(_)) {
            writes.insert(
                repo::key(RECORDS, hash.0),
                Some(ae_tx::storage::encode(
                    tx,
                    self.chain.config().transaction_record(),
                )?),
            );
            writes.insert(repo::key(MEMBERS, hash.0), Some(MEMBER.to_vec()));
        }
        Ok(())
    }
}
pub(super) fn write_bytes(writes: &repo::Writes) -> Result<usize, Error> {
    writes.iter().try_fold(0usize, |n, (k, v)| {
        n.checked_add(k.len())
            .and_then(|n| n.checked_add(v.as_ref().map_or(0, Vec::len)))
            .ok_or(Error::ResourceLimit)
    })
}
impl Database {
    pub fn pool_view(&self) -> Result<Stored<Snapshot<'_>>, Error> {
        Stored::load(self.view()?)
    }
    pub(super) fn validate_pool(&self) -> Result<(), Error> {
        let revision = decode_revision(self.db.get(REVISION).map_err(backend)?)?;
        if revision == 0 {
            for prefix in [RECORDS, MEMBERS] {
                if let Some(item) = self
                    .db
                    .iterator(IteratorMode::From(&[prefix], Direction::Forward))
                    .next()
                {
                    let (key, _) = item.map_err(backend)?;
                    if key.first() == Some(&prefix) {
                        return Err(Error::Corrupt("pool records without revision"));
                    }
                }
            }
        }
        Ok(())
    }
    // Called under the same writer lock as canonical location updates.
    pub(super) fn pool_chain_writes(
        &self,
        change: &repo::Change,
        chain_writes: &repo::Writes,
    ) -> Result<repo::Writes, Error> {
        let mut writes = repo::Writes::new();
        let confirmed: BTreeSet<_> = change.confirm.iter().map(|(h, _)| *h).collect();
        let requeue: BTreeSet<_> = change
            .requeue
            .iter()
            .copied()
            .filter(|h| !confirmed.contains(h))
            .collect();
        for hash in requeue {
            let key = repo::key(repo::TRANSACTIONS, hash.0);
            let bytes = match chain_writes.get(&key) {
                Some(v) => v.clone(),
                None => self.db.get(&key).map_err(backend)?,
            }
            .ok_or(Error::Corrupt("requeued transaction has no record"))?;
            // Legacy records are read through the existing version-aware view.
            let tx = if bytes.first() == Some(&0) {
                decode_record(hash, &bytes, self.config.transaction_record())?
            } else {
                let view = self.view_locked(&Default::default())?;
                view.transaction(hash)?
                    .ok_or(Error::Corrupt("legacy requeue record"))?
            };
            writes.insert(
                repo::key(RECORDS, hash.0),
                Some(ae_tx::storage::encode(
                    &tx,
                    self.config.transaction_record(),
                )?),
            );
            writes.insert(repo::key(MEMBERS, hash.0), Some(MEMBER.to_vec()));
        }
        for hash in confirmed {
            writes.insert(repo::key(MEMBERS, hash.0), None);
        }
        for hash in &change.discarded {
            if !member(self.db.get(repo::key(MEMBERS, hash.0)).map_err(backend)?)? {
                writes.insert(repo::key(RECORDS, hash.0), None);
            }
        }
        if !writes.is_empty() {
            let revision = decode_revision(self.db.get(REVISION).map_err(backend)?)?
                .checked_add(1)
                .ok_or(Error::ResourceLimit)?;
            writes.insert(REVISION.to_vec(), Some(encode_revision(revision)));
        }
        Ok(writes)
    }
    fn commit_pool(
        &self,
        stored: &Stored<Snapshot<'_>>,
        mut writes: repo::Writes,
        limit: usize,
        mut check: impl FnMut() -> Result<(), Error>,
    ) -> Result<u64, Error> {
        let _guard = self.writer.lock().map_err(|_| backend_lock())?;
        if stored.chain.source().source_id() != self.source_id
            || stored.chain.identity() != &self.identity
        {
            return Err(Error::Identity);
        }
        let status = Status::decode(
            &self
                .db
                .get(repo::STATUS)
                .map_err(backend)?
                .ok_or(Error::Corrupt("missing status"))?,
        )?;
        let revision = decode_revision(self.db.get(REVISION).map_err(backend)?)?;
        if stored.chain.status() != status || stored.revision != revision {
            return Err(Error::Conflict);
        }
        if writes.is_empty() {
            check()?;
            return Ok(revision);
        }
        let next = revision.checked_add(1).ok_or(Error::ResourceLimit)?;
        writes.insert(REVISION.to_vec(), Some(encode_revision(next)));
        if write_bytes(&writes)? > limit {
            return Err(Error::ResourceLimit);
        }
        let mut batch = WriteBatch::default();
        for (key, value) in writes {
            check()?;
            match value {
                Some(value) => batch.put(key, value),
                None => batch.delete(key),
            }
        }
        check()?;
        self.db
            .write_opt(batch, &write_options())
            .map_err(backend)?;
        Ok(next)
    }
}
#[cfg(test)]
mod tests;
