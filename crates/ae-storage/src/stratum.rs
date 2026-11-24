// Durable operational Stratum accounting, not canonical chain state or a
// payout broadcast journal. Every batch uses one immutable repository view.
use super::{Database, Snapshot, backend, backend_lock, write_options};
use ae_chain::repository::Error;
use ae_stratum::{accounting as core, proof::Number, rewards::Amounts};
use ae_trie::Value;
use ae_types::{Hash256, Unsigned};
use num_bigint::BigInt;
use rocksdb::WriteBatch;
use std::sync::Arc;

const RECORDS: u8 = 22;
const METADATA: &[u8] = b"\0stratum";
const MAX_RECORDS: usize = 1_000_000;
const MAX_BYTES: usize = 1 << 30;
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub records: usize,
    pub bytes: usize,
    pub transaction: core::Limits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            records: 100_000,
            bytes: 256 << 20,
            transaction: Default::default(),
        }
    }
}
impl Limits {
    fn maximum() -> Self {
        Self {
            records: MAX_RECORDS,
            bytes: MAX_BYTES,
            transaction: core::Limits {
                records: MAX_RECORDS,
                bytes: MAX_BYTES,
                record_bytes: 64 << 20,
                maps: ae_stratum::rewards::Limits {
                    entries: MAX_RECORDS,
                    key_bytes: 65536,
                    bytes: MAX_BYTES,
                },
            },
        }
    }
    fn validate(self) -> Result<(), Failure> {
        self.transaction.validate()?;
        if !(1..=MAX_RECORDS).contains(&self.records) || !(1..=MAX_BYTES).contains(&self.bytes) {
            return Err(Error::ResourceLimit.into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Storage(Error),
    Accounting(core::Error),
    AlreadyRunning,
    RecoveryRequired,
    MutationInQuery,
}
impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Storage(e)
    }
}
impl From<core::Error> for Failure {
    fn from(e: core::Error) -> Self {
        Self::Accounting(e)
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Failure {}
fn storage(e: core::Error) -> Error {
    match e {
        core::Error::Backend(e) => Error::Backend(e),
        core::Error::Limit => Error::ResourceLimit,
        _ => Error::Corrupt("Stratum accounting record"),
    }
}
fn accounting(e: rocksdb::Error) -> core::Error {
    core::Error::Backend(e.to_string())
}
fn number(n: u64) -> Value {
    Value::Bytes(Unsigned::from(n).as_bytes().to_vec())
}
fn integer(v: &Value) -> Result<u64, Error> {
    let Value::Bytes(v) = v else {
        return Err(Error::Corrupt("Stratum metadata integer"));
    };
    Unsigned::from_field_bytes(v)
        .and_then(|n| n.to_u64())
        .map_err(|_| Error::Corrupt("Stratum metadata integer"))
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Info {
    pub revision: u64,
    pub next_key: Option<u64>,
    pub counts: [u64; 6],
    pub bytes: u64,
}
impl Default for Info {
    fn default() -> Self {
        Self {
            revision: 0,
            next_key: Some(u64::MAX),
            counts: [0; 6],
            bytes: 0,
        }
    }
}
impl Info {
    fn encode(self) -> Vec<u8> {
        Value::List(vec![
            number(1),
            number(self.revision),
            Value::List(self.next_key.into_iter().map(number).collect()),
            Value::List(self.counts.into_iter().map(number).collect()),
            number(self.bytes),
        ])
        .encode()
    }
    fn decode(raw: Option<&[u8]>) -> Result<Self, Error> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        let v = ae_codec::rlp::decode_with_limits(
            raw,
            ae_codec::rlp::DecodeLimits {
                max_input_bytes: 128,
                max_items: 17,
                max_depth: 2,
            },
        )
        .map_err(|_| Error::Corrupt("Stratum metadata RLP"))?;
        let Value::List(v) = Value::from_rlp(&v) else {
            return Err(Error::Corrupt("Stratum metadata"));
        };
        let [
            version,
            revision,
            Value::List(next),
            Value::List(counts),
            bytes,
        ] = v.as_slice()
        else {
            return Err(Error::Corrupt("Stratum metadata fields"));
        };
        let next = match next.as_slice() {
            [] => None,
            [n] => Some(integer(n)?),
            _ => return Err(Error::Corrupt("Stratum next key")),
        };
        let mut totals = [0; 6];
        if counts.len() != 6 {
            return Err(Error::Corrupt("Stratum table counts"));
        }
        for (i, n) in counts.iter().enumerate() {
            totals[i] = integer(n)?;
        }
        let m = Self {
            revision: integer(revision)?,
            next_key: next,
            counts: totals,
            bytes: integer(bytes)?,
        };
        if integer(version)? != 1 || m.revision == 0 || m.encode() != raw {
            return Err(Error::Corrupt("Stratum metadata encoding"));
        }
        m.check(Limits::maximum())?;
        Ok(m)
    }
    fn check(self, l: Limits) -> Result<(), Error> {
        let count = self
            .counts
            .iter()
            .try_fold(0u64, |n, v| n.checked_add(*v))
            .ok_or(Error::ResourceLimit)?;
        if count > l.records as u64 || self.bytes > l.bytes as u64 {
            return Err(Error::ResourceLimit);
        }
        if (count == 0) != (self.bytes == 0) {
            return Err(Error::Corrupt("Stratum empty totals"));
        }
        Ok(())
    }
    fn allocated(self, r: &core::Record) -> Result<(), Error> {
        let k = match r {
            core::Record::Hash { key, .. } | core::Record::Round(key) => Some(*key),
            core::Record::Share(s) => Some(s.key),
            _ => None,
        };
        if let (Some(k), Some(next)) = (k, self.next_key)
            && k <= next
        {
            return Err(Error::Corrupt("Stratum unallocated key"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Version {
    source: u64,
    revision: u64,
}
impl Version {
    pub fn revision(self) -> u64 {
        self.revision
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Round,
    Share {
        hash: Hash256,
        target: BigInt,
        miner: Vec<u8>,
    },
    Solution {
        candidate: Hash256,
        block: Hash256,
    },
    Reward(core::Reward),
    StorePayments {
        reward: core::Reward,
        percent: Number,
    },
    Payment(core::Payment),
    UpdatePayment {
        payment: Box<core::Payment>,
        absolute: Amounts,
        hash: Hash256,
        date: core::Date,
    },
    DeletePayment(core::PaymentId),
    Candidate(core::Candidate),
    DeleteCandidatesBefore(core::Date),
    DeleteRecords(u64),
    Clear(core::Table),
}
impl Command {
    fn operation(self, next: &mut Option<u64>) -> Result<core::Operation, core::Error> {
        let allocate = |next: &mut Option<u64>| {
            let k = next.ok_or(core::Error::Limit)?;
            *next = k.checked_sub(1);
            Ok::<_, core::Error>(k)
        };
        Ok(match self {
            Self::Round => core::Operation::Round(allocate(next)?),
            Self::Share {
                hash,
                target,
                miner,
            } => core::Operation::Share(core::Share {
                key: allocate(next)?,
                hash,
                target,
                miner,
            }),
            Self::Solution { candidate, block } => core::Operation::Solution { candidate, block },
            Self::Reward(r) => core::Operation::Reward(r),
            Self::StorePayments { reward, percent } => {
                core::Operation::StorePayments { reward, percent }
            }
            Self::Payment(p) => core::Operation::Payment(p),
            Self::UpdatePayment {
                payment,
                absolute,
                hash,
                date,
            } => core::Operation::UpdatePayment {
                payment,
                absolute,
                hash,
                date,
            },
            Self::DeletePayment(p) => core::Operation::DeletePayment(p),
            Self::Candidate(c) => core::Operation::Candidate(c),
            Self::DeleteCandidatesBefore(d) => core::Operation::DeleteCandidatesBefore(d),
            Self::DeleteRecords(h) => core::Operation::DeleteRecords(h),
            Self::Clear(t) => core::Operation::Clear(t),
        })
    }
}
fn key(k: &[u8]) -> Vec<u8> {
    let mut out = vec![RECORDS];
    out.extend_from_slice(k);
    out
}
fn valid_key(k: &[u8]) -> Result<core::Table, core::Error> {
    let t = core::Table::from_byte(*k.first().ok_or(core::Error::Corrupt("Stratum key"))?)?;
    let len = match t {
        core::Table::Hashes | core::Table::Candidates => 33,
        core::Table::Payments => 13,
        _ => 9,
    };
    if k.len() != len {
        return Err(core::Error::Corrupt("Stratum key length"));
    }
    Ok(t)
}
fn bound(k: &[u8]) -> Result<(), core::Error> {
    if k.len() > 34 || k.first().is_some_and(|b| !(1..=7).contains(b)) {
        return Err(core::Error::Corrupt("Stratum range bound"));
    }
    Ok(())
}
pub struct Stored<'a> {
    source: Snapshot<'a>,
    info: Info,
    limits: Limits,
}
impl Stored<'_> {
    pub fn info(&self) -> Info {
        self.info
    }
    pub fn version(&self) -> Version {
        Version {
            source: self.source.source_id,
            revision: self.info.revision,
        }
    }
    pub fn query<T>(
        &self,
        f: impl FnOnce(&mut core::Transaction<'_, Self>) -> Result<T, core::Error>,
    ) -> Result<T, Failure> {
        let (value, plan) = core::prepare(self, self.limits.transaction, f)?;
        if !plan.is_empty() {
            return Err(Failure::MutationInQuery);
        }
        Ok(value)
    }
    // Stream over bounded records. This never materializes the whole pool.
    pub fn verify(&self) -> Result<(), Failure> {
        self.info.check(self.limits)?;
        let mut counts = [0u64; 6];
        let mut bytes = 0usize;
        let mut records = 0usize;
        let mut it = self.source.snapshot.raw_iterator();
        it.seek([RECORDS]);
        while let Some(k) = it.key() {
            if !k.starts_with(&[RECORDS]) {
                break;
            }
            let raw = it.value().ok_or(Error::Corrupt("Stratum iterator value"))?;
            records = records.checked_add(1).ok_or(Error::ResourceLimit)?;
            bytes = bytes
                .checked_add(k.len())
                .and_then(|n| n.checked_add(raw.len()))
                .ok_or(Error::ResourceLimit)?;
            if records > self.limits.records
                || bytes > self.limits.bytes
                || raw.len() > self.limits.transaction.record_bytes
            {
                return Err(Error::ResourceLimit.into());
            }
            let t = valid_key(&k[1..])?;
            let r = core::Record::decode(&k[1..], raw, self.limits.transaction)?;
            self.info.allocated(&r)?;
            counts[t as usize - 1] += 1;
            it.next();
        }
        it.status().map_err(backend)?;
        if counts != self.info.counts || bytes as u64 != self.info.bytes {
            return Err(Error::Corrupt("Stratum metadata totals").into());
        }
        Ok(())
    }
}
impl core::Read for Stored<'_> {
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, core::Error> {
        valid_key(k)?;
        let raw = self
            .source
            .snapshot
            .get_pinned(key(k))
            .map_err(accounting)?;
        raw.map(|v| {
            if v.len() > self.limits.transaction.record_bytes
                || k.len() + v.len() > self.limits.transaction.bytes
            {
                return Err(core::Error::Limit);
            }
            Ok(v.to_vec())
        })
        .transpose()
    }
    fn scan(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        reverse: bool,
    ) -> Result<core::Scan, core::Error> {
        bound(start)?;
        bound(end)?;
        if start > end {
            return Err(core::Error::Corrupt("Stratum reversed range"));
        }
        if limit == 0 || limit > self.limits.transaction.records {
            return Err(core::Error::Limit);
        }
        if start == end {
            return Ok(core::Scan::default());
        }
        let start = key(start);
        let end = key(end);
        let mut it = self.source.snapshot.raw_iterator();
        if reverse {
            it.seek_for_prev(&end);
            if it.key() == Some(end.as_slice()) {
                it.prev();
            }
        } else {
            it.seek(&start);
        }
        let mut out = core::Scan::default();
        let mut bytes = 0usize;
        while let Some(k) = it.key() {
            if k < start.as_slice() || k >= end.as_slice() {
                break;
            }
            if out.entries.len() == limit {
                out.more = true;
                break;
            }
            let v = it
                .value()
                .ok_or(core::Error::Corrupt("Stratum iterator value"))?;
            valid_key(&k[1..])?;
            bytes = bytes
                .checked_add(k.len())
                .and_then(|n| n.checked_add(v.len()))
                .ok_or(core::Error::Limit)?;
            if bytes > self.limits.transaction.bytes
                || v.len() > self.limits.transaction.record_bytes
            {
                return Err(core::Error::Limit);
            }
            out.entries.push((k[1..].to_vec(), v.to_vec()));
            if reverse {
                it.prev();
            } else {
                it.next();
            }
        }
        it.status().map_err(accounting)?;
        Ok(out)
    }
}
impl Database {
    fn stratum_snapshot(
        &self,
        limits: Limits,
        policy: ae_chain::resilience::Snapshot,
    ) -> Result<Stored<'_>, Failure> {
        let source = Snapshot {
            snapshot: Arc::new(self.db.snapshot()),
            source_id: self.source_id,
            policy,
        };
        let raw = source.snapshot.get_pinned(METADATA).map_err(backend)?;
        let info = Info::decode(raw.as_deref())?;
        info.check(limits)?;
        drop(raw);
        Ok(Stored {
            source,
            info,
            limits,
        })
    }
    pub fn stratum(&self, limits: Limits) -> Result<Stored<'_>, Failure> {
        limits.validate()?;
        let guard = self.writer.lock().map_err(|_| backend_lock())?;
        self.stratum_snapshot(limits, *guard)
    }
    pub(super) fn validate_stratum(&self) -> Result<(), Error> {
        self.stratum(Limits::maximum())
            .and_then(|s| s.verify())
            .map_err(|e| match e {
                Failure::Storage(e) => e,
                Failure::Accounting(e) => storage(e),
                _ => Error::Corrupt("Stratum opening validation"),
            })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub version: Version,
    pub replies: Vec<core::Reply>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recovery {
    Verified(Version),
    NotApplied(Version),
    Applied(Commit),
}
struct Pending {
    before: Info,
    after: Info,
    plan: core::Plan,
    replies: Vec<core::Reply>,
}
pub struct Writer {
    db: Arc<Database>,
    limits: Limits,
    _lease: Arc<()>,
    pending: Option<Pending>,
}
impl Writer {
    pub fn start(db: Arc<Database>, limits: Limits) -> Result<Self, Failure> {
        limits.validate()?;
        let lease = Arc::new(());
        {
            let mut slot = db.stratum_owner.lock().map_err(|_| backend_lock())?;
            if slot.upgrade().is_some() {
                return Err(Failure::AlreadyRunning);
            }
            *slot = Arc::downgrade(&lease);
        }
        db.stratum(limits)?.verify()?;
        Ok(Self {
            db,
            limits,
            _lease: lease,
            pending: None,
        })
    }
    pub fn recovery_required(&self) -> bool {
        self.pending.is_some()
    }
    pub fn version(&self) -> Result<Version, Failure> {
        if self.recovery_required() {
            return Err(Failure::RecoveryRequired);
        }
        Ok(self.db.stratum(self.limits)?.version())
    }
    pub fn commit(&mut self, expected: Version, commands: Vec<Command>) -> Result<Commit, Failure> {
        self.commit_with(expected, commands, |db, batch| {
            db.db.write_opt(batch, &write_options()).map_err(backend)
        })
    }
    fn commit_with(
        &mut self,
        expected: Version,
        commands: Vec<Command>,
        write: impl FnOnce(&Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Commit, Failure> {
        if self.recovery_required() {
            return Err(Failure::RecoveryRequired);
        }
        if commands.len() > self.limits.transaction.records {
            return Err(Error::ResourceLimit.into());
        }
        let guard = self.db.writer.lock().map_err(|_| backend_lock())?;
        self.db.ensure_policy_recovered()?;
        let stored = self.db.stratum_snapshot(self.limits, *guard)?;
        if stored.version() != expected {
            return Err(Error::Conflict.into());
        }
        let before = stored.info;
        let mut after = before;
        let (replies, plan) = core::prepare(&stored, self.limits.transaction, |tx| {
            commands
                .into_iter()
                .map(|c| tx.apply(c.operation(&mut after.next_key)?))
                .collect::<Result<Vec<_>, _>>()
        })?;
        if plan.is_empty() {
            return Ok(Commit {
                version: expected,
                replies,
            });
        }
        let mut batch = WriteBatch::default();
        for (k, new) in plan.writes() {
            let table = valid_key(k)?;
            let index = table as usize - 1;
            let full = key(k);
            let old = plan
                .originals()
                .get(k)
                .ok_or(Error::Corrupt("Stratum original missing"))?;
            if let Some(v) = old {
                core::Record::decode(k, v, self.limits.transaction)?;
                after.counts[index] = after.counts[index]
                    .checked_sub(1)
                    .ok_or(Error::Corrupt("Stratum record count"))?;
                after.bytes = after
                    .bytes
                    .checked_sub((full.len() + v.len()) as u64)
                    .ok_or(Error::Corrupt("Stratum byte count"))?;
            }
            if let Some(v) = new {
                let r = core::Record::decode(k, v, self.limits.transaction)?;
                after.allocated(&r)?;
                after.counts[index] = after.counts[index]
                    .checked_add(1)
                    .ok_or(Error::ResourceLimit)?;
                after.bytes = after
                    .bytes
                    .checked_add((full.len() + v.len()) as u64)
                    .ok_or(Error::ResourceLimit)?;
                batch.put(full, v);
            } else {
                batch.delete(full);
            }
        }
        after.revision = after.revision.checked_add(1).ok_or(Error::ResourceLimit)?;
        after.check(self.limits)?;
        batch.put(METADATA, after.encode());
        drop(stored);
        self.pending = Some(Pending {
            before,
            after,
            plan,
            replies,
        });
        write(&self.db, batch)?;
        let pending = self.pending.take().expect("prepared accounting write");
        Ok(Commit {
            version: Version {
                source: expected.source,
                revision: after.revision,
            },
            replies: pending.replies,
        })
    }
    // No side effects are retried here. Retained replies are released only
    // after exact durable metadata/record verification of the pending batch.
    pub fn recover(&mut self) -> Result<Recovery, Failure> {
        use ae_stratum::accounting::Read;
        let (version, applied) = {
            let stored = self.db.stratum(self.limits)?;
            stored.verify()?;
            let mut applied = None;
            if let Some(p) = &self.pending {
                let values = if stored.info == p.after {
                    applied = Some(true);
                    p.plan.writes()
                } else if stored.info == p.before {
                    applied = Some(false);
                    p.plan.originals()
                } else {
                    return Err(Error::Conflict.into());
                };
                for (k, v) in values {
                    if stored.get(k)? != *v {
                        return Err(Error::Conflict.into());
                    }
                }
            }
            (stored.version(), applied)
        };
        Ok(match applied {
            None => Recovery::Verified(version),
            Some(false) => {
                self.pending = None;
                Recovery::NotApplied(version)
            }
            Some(true) => {
                let p = self.pending.take().expect("pending accounting write");
                Recovery::Applied(Commit {
                    version,
                    replies: p.replies,
                })
            }
        })
    }
}
#[cfg(test)]
mod tests;
