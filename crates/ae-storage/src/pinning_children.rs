// Durable child pin intents. Exact signed bytes precede pool admission;
// a missing receipt is never permission to create a new transaction/nonce.
pub use super::parent_posts::{Chain, Limits};
use super::*;
use ae_mempool::pool::{Location, Signed};
use ae_trie::Value;
use ae_types::{AccountPublicKey, Unsigned};
mod codec;
pub use codec::{Intent, Kind, Receipt, Record};
const RECORDS: u8 = 21;
const METADATA: &[u8] = b"\0child-pins";
const MAX_RECORDS: usize = 1_000_000;
const MAX_BYTES: usize = 1 << 30;
const MAX_TRANSACTION: usize = 16 << 20;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Storage(Error),
    AlreadyRunning,
    RecoveryRequired,
}
impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Storage(e)
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Failure {}
fn number(n: u64) -> Value {
    bytes(Unsigned::from(n).as_bytes())
}
fn bytes(b: impl AsRef<[u8]>) -> Value {
    Value::Bytes(b.as_ref().to_vec())
}
fn blob(v: &Value) -> Result<&[u8], Error> {
    if let Value::Bytes(b) = v {
        Ok(b)
    } else {
        Err(Error::Corrupt("child pin bytes"))
    }
}
fn integer(v: &Value) -> Result<u64, Error> {
    Unsigned::from_field_bytes(blob(v)?)
        .and_then(|n| n.to_u64())
        .map_err(|_| Error::Corrupt("child pin integer"))
}
fn flat(raw: &[u8], max: usize, items: usize) -> Result<Value, Error> {
    ae_codec::rlp::decode_with_limits(
        raw,
        ae_codec::rlp::DecodeLimits {
            max_input_bytes: max,
            max_depth: 1,
            max_items: items,
        },
    )
    .map(|v| Value::from_rlp(&v))
    .map_err(|_| Error::Corrupt("child pin RLP"))
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Metadata {
    revision: u64,
    records: u64,
    bytes: u64,
}
impl Metadata {
    fn encode(self) -> Vec<u8> {
        Value::List(vec![
            number(1),
            number(self.revision),
            number(self.records),
            number(self.bytes),
        ])
        .encode()
    }
    fn decode(raw: Option<Vec<u8>>) -> Result<Self, Error> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        let Value::List(v) = flat(&raw, 40, 5)? else {
            return Err(Error::Corrupt("child pin metadata"));
        };
        let [version, revision, records, length] = v.as_slice() else {
            return Err(Error::Corrupt("child pin metadata fields"));
        };
        let m = Self {
            revision: integer(revision)?,
            records: integer(records)?,
            bytes: integer(length)?,
        };
        if integer(version)? != 1
            || m.revision == 0
            || m.records == 0
            || m.revision < m.records
            || m.records > MAX_RECORDS as u64
            || m.bytes == 0
            || m.bytes > MAX_BYTES as u64
            || m.encode() != raw
        {
            return Err(Error::Corrupt("child pin metadata encoding"));
        }
        Ok(m)
    }
    fn check(self, l: Limits) -> Result<(), Error> {
        l.validate()?;
        if self.records > l.records as u64 || self.bytes > l.bytes as u64 {
            Err(Error::ResourceLimit)
        } else {
            Ok(())
        }
    }
}
pub struct Stored<'a> {
    source: Snapshot<'a>,
    metadata: Metadata,
}
impl Stored<'_> {
    pub fn revision(&self) -> u64 {
        self.metadata.revision
    }
    pub fn get(&self, id: Hash256, limit: usize) -> Result<Option<Record>, Error> {
        let key = repo::key(RECORDS, id);
        self.source
            .get(&key)?
            .map(|raw| Record::decode(&key, &raw, limit))
            .transpose()
    }
    pub fn records(&self, limits: Limits) -> Result<Vec<Record>, Error> {
        self.metadata.check(limits)?;
        let mut result = vec![];
        let mut length = 0usize;
        self.source.visit_range(
            &[RECORDS],
            &[RECORDS + 1],
            limits.records,
            &mut |key, raw| {
                length = length
                    .checked_add(key.len())
                    .and_then(|n| n.checked_add(raw.len()))
                    .ok_or(Error::ResourceLimit)?;
                if length > limits.bytes {
                    return Err(Error::ResourceLimit);
                }
                result.push(Record::decode(key, raw, limits.transaction_bytes)?);
                Ok(())
            },
        )?;
        if result.len() as u64 != self.metadata.records || length as u64 != self.metadata.bytes {
            return Err(Error::Corrupt("child pin metadata totals"));
        }
        Ok(result)
    }
}
impl Database {
    pub fn pinning_children(&self) -> Result<Stored<'_>, Error> {
        let policy = self.writer.lock().map_err(|_| backend_lock())?;
        let source = Snapshot {
            snapshot: Arc::new(self.db.snapshot()),
            source_id: self.source_id,
            policy: *policy,
        };
        let metadata = Metadata::decode(source.get(METADATA)?)?;
        Ok(Stored { source, metadata })
    }
    pub(super) fn validate_pinning_children(&self) -> Result<(), Error> {
        // Validate every bounded record before upgrading writer capability. A
        // corrupt or partial journal must never be mistaken for no submission.
        let records = self.pinning_children()?.records(Limits {
            records: MAX_RECORDS,
            bytes: MAX_BYTES,
            transaction_bytes: MAX_TRANSACTION,
        })?;
        if records
            .iter()
            .any(|r| r.intent.network != self.identity.network)
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reservation {
    New(Record),
    Existing(Record),
}
pub struct Writer {
    db: Arc<Database>,
    limits: Limits,
    _lease: Arc<()>,
    uncertain: bool,
}
impl Writer {
    pub fn start(db: Arc<Database>, limits: Limits) -> Result<Self, Failure> {
        limits.validate()?;
        let lease = Arc::new(());
        {
            let mut owner = db
                .pinning_children_owner
                .lock()
                .map_err(|_| backend_lock())?;
            if owner.upgrade().is_some() {
                return Err(Failure::AlreadyRunning);
            }
            *owner = Arc::downgrade(&lease);
        }
        let records = db.pinning_children()?.records(limits)?;
        if records
            .iter()
            .any(|r| r.intent.network != db.identity.network)
        {
            return Err(Error::Identity.into());
        }
        Ok(Self {
            db,
            limits,
            _lease: lease,
            uncertain: false,
        })
    }
    pub fn recovery_required(&self) -> bool {
        self.uncertain
    }
    // Recovery verifies durable truth; it never authorizes another admission.
    pub fn recover(&mut self) -> Result<Vec<Record>, Failure> {
        let r = self.db.pinning_children()?.records(self.limits)?;
        self.uncertain = false;
        Ok(r)
    }
    pub fn get(&self, intent: &Intent) -> Result<Option<Record>, Failure> {
        if self.uncertain {
            return Err(Failure::RecoveryRequired);
        }
        let r = self
            .db
            .pinning_children()?
            .get(intent.id()?, self.limits.transaction_bytes)?;
        if r.as_ref().is_some_and(|r| r.intent != *intent) {
            return Err(Error::Conflict.into());
        }
        Ok(r)
    }
    pub fn reserve(&mut self, record: Record) -> Result<Reservation, Failure> {
        self.reserve_with(record, |db, batch| {
            db.db.write_opt(batch, &write_options()).map_err(backend)
        })
    }
    fn reserve_with(
        &mut self,
        record: Record,
        write: impl FnOnce(&Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Reservation, Failure> {
        if self.uncertain {
            return Err(Failure::RecoveryRequired);
        }
        if record.receipt.is_some() {
            return Err(Error::Conflict.into());
        }
        if record.intent.network != self.db.identity.network {
            return Err(Error::Identity.into());
        }
        record.encode(self.limits.transaction_bytes)?;
        if let Some(old) = self.get(&record.intent)? {
            return Ok(Reservation::Existing(old));
        }
        self.commit(&record, None, write)?;
        Ok(Reservation::New(record))
    }
    // Retain the exact returned receipt. Reconciliation uses observe below;
    // neither transition releases the intent or permits a fresh nonce.
    pub fn record_receipt(
        &mut self,
        expected: &Record,
        receipt: Receipt,
    ) -> Result<Record, Failure> {
        self.receipt_with(expected, receipt, |db, batch| {
            db.db.write_opt(batch, &write_options()).map_err(backend)
        })
    }
    fn receipt_with(
        &mut self,
        expected: &Record,
        receipt: Receipt,
        write: impl FnOnce(&Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Record, Failure> {
        if self.uncertain {
            return Err(Failure::RecoveryRequired);
        }
        let old = self.get(&expected.intent)?.ok_or(Error::Conflict)?;
        if old != *expected {
            return Err(Error::Conflict.into());
        }
        if let Some(previous) = &old.receipt {
            return if *previous == receipt {
                Ok(old)
            } else {
                Err(Error::Conflict.into())
            };
        }
        let mut next = old.clone();
        next.receipt = Some(receipt);
        self.commit(&next, Some(&old), write)?;
        Ok(next)
    }
    // An exact transaction in a coherent pool/chain snapshot is evidence of
    // prior retention, not a recreated admission reply or proof of finality.
    pub fn observe(&mut self, expected: &Record) -> Result<Option<Record>, Failure> {
        if self.uncertain {
            return Err(Failure::RecoveryRequired);
        }
        if self.get(&expected.intent)?.as_ref() != Some(expected) {
            return Err(Error::Conflict.into());
        }
        if expected.receipt.is_some() {
            return Ok(Some(expected.clone()));
        }
        let tx = expected.signed()?;
        let observed = {
            let pool = self.db.pool_view()?;
            let location = pool.location(tx.hash())?;
            if location == Location::Unknown {
                return Ok(None);
            }
            let stored = pool
                .transaction(tx.hash())?
                .ok_or(Error::Corrupt("child pin observed transaction"))?;
            if stored.encode() != expected.transaction {
                return Err(Error::TransactionMetadataConflict.into());
            }
            Receipt::Observed {
                location,
                chain: pool.chain().status(),
                revision: pool.revision(),
            }
        };
        self.record_receipt(expected, observed).map(Some)
    }
    fn commit(
        &mut self,
        next: &Record,
        expected: Option<&Record>,
        write: impl FnOnce(&Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<(), Failure> {
        let key = repo::key(RECORDS, next.intent.id()?);
        let raw = next.encode(self.limits.transaction_bytes)?;
        let _guard = self.db.writer.lock().map_err(|_| backend_lock())?;
        self.db.ensure_policy_recovered()?;
        let prior = self
            .db
            .db
            .get(&key)
            .map_err(backend)?
            .map(|v| Record::decode(&key, &v, self.limits.transaction_bytes))
            .transpose()?;
        if prior.as_ref() != expected {
            return Err(Error::Conflict.into());
        }
        let mut metadata = Metadata::decode(self.db.db.get(METADATA).map_err(backend)?)?;
        metadata.revision = metadata
            .revision
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        if let Some(old) = &prior {
            metadata.bytes = metadata
                .bytes
                .checked_sub((key.len() + old.encode(self.limits.transaction_bytes)?.len()) as u64)
                .ok_or(Error::Corrupt("child pin byte count"))?;
        } else {
            metadata.records = metadata
                .records
                .checked_add(1)
                .ok_or(Error::ResourceLimit)?;
        }
        metadata.bytes = metadata
            .bytes
            .checked_add((key.len() + raw.len()) as u64)
            .ok_or(Error::ResourceLimit)?;
        metadata.check(self.limits)?;
        let mut batch = WriteBatch::default();
        batch.put(key, raw);
        batch.put(METADATA, metadata.encode());
        self.uncertain = true;
        write(&self.db, batch)?;
        self.uncertain = false;
        Ok(())
    }
}
#[cfg(test)]
mod tests;
