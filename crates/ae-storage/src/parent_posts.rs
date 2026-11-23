// Local pre-broadcast journal. These records are not canonical chain state.
// Reserving an intent never authorizes replay: even an unacknowledged attempt
// may have reached the parent. No deletion or automatic retry API is provided.
use super::*;
use ae_trie::Value;
use ae_types::{AccountPublicKey, Unsigned};

const RECORDS: u8 = 20;
const METADATA: &[u8] = b"\0parent-posts";
const MAX_RECORDS: usize = 1_000_000;
const MAX_BYTES: usize = 1 << 30;
const MAX_TRANSACTION: usize = 16 << 20;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chain {
    Aeternity,
    Dogecoin,
    Bitcoin,
}
impl Chain {
    fn number(self) -> u64 {
        match self {
            Self::Aeternity => 0,
            Self::Dogecoin => 1,
            Self::Bitcoin => 2,
        }
    }
    fn decode(n: u64) -> Result<Self, Error> {
        match n {
            0 => Ok(Self::Aeternity),
            1 => Ok(Self::Dogecoin),
            2 => Ok(Self::Bitcoin),
            _ => Err(Error::Corrupt("parent post chain")),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intent {
    pub chain: Chain,
    pub network: Vec<u8>,
    pub account: AccountPublicKey,
    pub payload: Vec<u8>,
}
impl Intent {
    fn validate(&self) -> Result<(), Error> {
        if self.network.len() > 4096 || self.payload.is_empty() || self.payload.len() > 65536 {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
    // Endpoint, credentials, nonce, fee and amount are deliberately excluded.
    // Changing them cannot evade an existing possibly broadcast pin intent.
    pub fn id(&self) -> Result<Hash256, Error> {
        self.validate()?;
        let encoded = Value::List(vec![
            bytes(b"ae-parent-post-v1"),
            number(self.chain.number()),
            bytes(&self.network),
            bytes(self.account.0),
            bytes(&self.payload),
        ])
        .encode();
        Ok(blake2b_256(&encoded))
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub intent: Intent,
    // The service projects only scheme/host/port, never URL credentials.
    pub endpoint: Vec<u8>,
    // Exact signed AE bytes or the exact wallet-signed Dogecoin hex string.
    pub transaction: Vec<u8>,
    // An observed broadcast response is not proof of inclusion/finality.
    // None means possible submission, including the crash-before-send window.
    pub response: Option<Vec<u8>>,
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub records: usize,
    pub bytes: usize,
    pub transaction_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            records: 100_000,
            bytes: 256 << 20,
            transaction_bytes: 4 << 20,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<(), Error> {
        if !(1..=MAX_RECORDS).contains(&self.records)
            || !(1..=MAX_BYTES).contains(&self.bytes)
            || !(1..=MAX_TRANSACTION).contains(&self.transaction_bytes)
        {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
}
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
        Err(Error::Corrupt("parent post bytes"))
    }
}
fn integer(v: &Value) -> Result<u64, Error> {
    Unsigned::from_field_bytes(blob(v)?)
        .and_then(|n| n.to_u64())
        .map_err(|_| Error::Corrupt("parent post integer"))
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
    .map_err(|_| Error::Corrupt("parent post RLP"))
}
impl Record {
    fn encode(&self, limit: usize) -> Result<Vec<u8>, Error> {
        self.intent.validate()?;
        if self.endpoint.is_empty()
            || self.endpoint.len() > 4096
            || self
                .endpoint
                .iter()
                .any(|&b| b <= 32 || b >= 127 || matches!(b, b'@' | b'?' | b'#'))
            || self.transaction.is_empty()
            || self.transaction.len() > limit.min(MAX_TRANSACTION)
            || self.response.as_ref().is_some_and(|h| h.len() > 65536)
        {
            return Err(Error::ResourceLimit);
        }
        Ok(Value::List(vec![
            number(1),
            number(self.intent.chain.number()),
            bytes(&self.intent.network),
            bytes(self.intent.account.0),
            bytes(&self.intent.payload),
            bytes(&self.endpoint),
            bytes(&self.transaction),
            number(u64::from(self.response.is_some())),
            bytes(self.response.as_deref().unwrap_or_default()),
        ])
        .encode())
    }
    fn decode(key: &[u8], raw: &[u8], limit: usize) -> Result<Self, Error> {
        let Value::List(v) = flat(raw, MAX_TRANSACTION + 140_000, 10)? else {
            return Err(Error::Corrupt("parent post record"));
        };
        let [
            version,
            chain,
            network,
            account,
            payload,
            endpoint,
            transaction,
            present,
            response,
        ] = v.as_slice()
        else {
            return Err(Error::Corrupt("parent post fields"));
        };
        let present = integer(present)?;
        if integer(version)? != 1 || present > 1 || (present == 0 && !blob(response)?.is_empty()) {
            return Err(Error::Corrupt("parent post version or response"));
        }
        let r = Self {
            intent: Intent {
                chain: Chain::decode(integer(chain)?)?,
                network: blob(network)?.to_vec(),
                account: AccountPublicKey(
                    blob(account)?
                        .try_into()
                        .map_err(|_| Error::Corrupt("parent post account"))?,
                ),
                payload: blob(payload)?.to_vec(),
            },
            endpoint: blob(endpoint)?.to_vec(),
            transaction: blob(transaction)?.to_vec(),
            response: (present == 1)
                .then(|| blob(response).map(Vec::from))
                .transpose()?,
        };
        if r.encode(MAX_TRANSACTION)
            .map_err(|_| Error::Corrupt("parent post bounds"))?
            != raw
            || key != repo::key(RECORDS, r.intent.id()?)
        {
            return Err(Error::Corrupt("parent post encoding or key"));
        }
        if r.transaction.len() > limit {
            return Err(Error::ResourceLimit);
        }
        Ok(r)
    }
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
            return Err(Error::Corrupt("parent post metadata"));
        };
        let [version, revision, records, length] = v.as_slice() else {
            return Err(Error::Corrupt("parent post metadata fields"));
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
            return Err(Error::Corrupt("parent post metadata encoding"));
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
            return Err(Error::Corrupt("parent post metadata totals"));
        }
        Ok(result)
    }
}
impl Database {
    pub fn parent_posts(&self) -> Result<Stored<'_>, Error> {
        let policy = self.writer.lock().map_err(|_| backend_lock())?;
        let source = Snapshot {
            snapshot: Arc::new(self.db.snapshot()),
            source_id: self.source_id,
            policy: *policy,
        };
        let metadata = Metadata::decode(source.get(METADATA)?)?;
        Ok(Stored { source, metadata })
    }
    pub(super) fn validate_parent_posts(&self) -> Result<(), Error> {
        // Validate every bounded record before upgrading writer capability. A
        // corrupt or partial journal must never be mistaken for no submission.
        self.parent_posts()?.records(Limits {
            records: MAX_RECORDS,
            bytes: MAX_BYTES,
            transaction_bytes: MAX_TRANSACTION,
        })?;
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
            let mut owner = db.parent_posts_owner.lock().map_err(|_| backend_lock())?;
            if owner.upgrade().is_some() {
                return Err(Failure::AlreadyRunning);
            }
            *owner = Arc::downgrade(&lease);
        }
        db.parent_posts()?.records(limits)?;
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
    // Recovery verifies durable truth; it never authorizes a second broadcast.
    pub fn recover(&mut self) -> Result<Vec<Record>, Failure> {
        let r = self.db.parent_posts()?.records(self.limits)?;
        self.uncertain = false;
        Ok(r)
    }
    pub fn get(&self, intent: &Intent) -> Result<Option<Record>, Failure> {
        if self.uncertain {
            return Err(Failure::RecoveryRequired);
        }
        let r = self
            .db
            .parent_posts()?
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
        if record.response.is_some() {
            return Err(Error::Conflict.into());
        }
        record.encode(self.limits.transaction_bytes)?;
        if let Some(old) = self.get(&record.intent)? {
            return Ok(Reservation::Existing(old));
        }
        self.commit(&record, None, write)?;
        Ok(Reservation::New(record))
    }
    // Reconcile an observed response only against the exact retained record.
    // The caller supplies evidence; this local operation does not establish
    // chain inclusion or finality and never releases the duplicate guard.
    pub fn record_response(&mut self, expected: &Record, hash: Vec<u8>) -> Result<Record, Failure> {
        self.response_with(expected, hash, |db, batch| {
            db.db.write_opt(batch, &write_options()).map_err(backend)
        })
    }
    fn response_with(
        &mut self,
        expected: &Record,
        hash: Vec<u8>,
        write: impl FnOnce(&Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Record, Failure> {
        if self.uncertain {
            return Err(Failure::RecoveryRequired);
        }
        let old = self.get(&expected.intent)?.ok_or(Error::Conflict)?;
        if old != *expected {
            return Err(Error::Conflict.into());
        }
        if let Some(previous) = &old.response {
            return if *previous == hash {
                Ok(old)
            } else {
                Err(Error::Conflict.into())
            };
        }
        let mut next = old.clone();
        next.response = Some(hash);
        self.commit(&next, Some(&old), write)?;
        Ok(next)
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
                .ok_or(Error::Corrupt("parent post byte count"))?;
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
