// Peer address records are local metadata, independent of canonical state.
// aec_db.erl:1938-1953, aeternity fa2a6d7. Copyright (c) 2018-2019,
// Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use ae_trie::Value;
use ae_types::{PeerPublicKey, Unsigned};
use std::{collections::BTreeMap, net::IpAddr};

pub const RECORDS: u8 = 18;
pub const METADATA: &[u8] = b"\0peers";
const MAX_HOST: usize = 65_536;
const MAX_RECORDS: usize = 1_000_000;
const MAX_BYTES: usize = 1 << 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub id: PeerPublicKey,
    pub host: Vec<u8>,
    pub address: IpAddr,
    pub source: IpAddr,
    pub port: u16,
    pub trusted: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    Put(Record),
    Delete(PeerPublicKey),
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub records: usize,
    pub bytes: usize,
    pub host_bytes: usize,
    pub changes: usize,
    pub commit_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            records: 100_000,
            bytes: 64 << 20,
            host_bytes: 1024,
            changes: 100_000,
            commit_bytes: 8 << 20,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<(), Error> {
        if self.records == 0
            || self.records > MAX_RECORDS
            || self.bytes == 0
            || self.bytes > MAX_BYTES
            || self.host_bytes == 0
            || self.host_bytes > MAX_HOST
            || self.changes == 0
            || self.changes > MAX_RECORDS
            || self.commit_bytes == 0
            || self.commit_bytes > MAX_BYTES
        {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
}
#[derive(Debug)]
pub enum Failure {
    Storage(Error),
    AlreadyRunning,
    RestartRequired,
}
impl From<Error> for Failure {
    fn from(value: Error) -> Self {
        Self::Storage(value)
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Failure {}

fn number(n: u64) -> Value {
    Value::Bytes(Unsigned::from(n).as_bytes().to_vec())
}
fn integer(v: &Value) -> Result<u64, Error> {
    let Value::Bytes(b) = v else {
        return Err(Error::Corrupt("peer integer"));
    };
    Unsigned::from_field_bytes(b)
        .and_then(|n| n.to_u64())
        .map_err(|_| Error::Corrupt("peer integer"))
}
fn bytes(v: &Value) -> Result<&[u8], Error> {
    match v {
        Value::Bytes(b) => Ok(b),
        _ => Err(Error::Corrupt("peer bytes")),
    }
}
fn ip(v: &Value) -> Result<IpAddr, Error> {
    let b = bytes(v)?;
    match b.len() {
        4 => Ok(IpAddr::from(
            <[u8; 4]>::try_from(b).expect("length checked"),
        )),
        16 => Ok(IpAddr::from(
            <[u8; 16]>::try_from(b).expect("length checked"),
        )),
        _ => Err(Error::Corrupt("peer IP length")),
    }
}
fn address(ip: IpAddr) -> Value {
    Value::Bytes(match ip {
        IpAddr::V4(a) => a.octets().to_vec(),
        IpAddr::V6(a) => a.octets().to_vec(),
    })
}
fn decode_flat(raw: &[u8], maximum_bytes: usize, maximum_items: usize) -> Result<Value, Error> {
    let value = ae_codec::rlp::decode_with_limits(
        raw,
        ae_codec::rlp::DecodeLimits {
            max_input_bytes: maximum_bytes,
            max_items: maximum_items,
            max_depth: 1,
        },
    )
    .map_err(|_| Error::Corrupt("peer RLP"))?;
    Ok(Value::from_rlp(&value))
}
impl Record {
    fn encode(&self, limit: usize) -> Result<Vec<u8>, Error> {
        if self.host.len() > limit.min(MAX_HOST) {
            return Err(Error::ResourceLimit);
        }
        Ok(Value::List(vec![
            number(1),
            Value::Bytes(self.id.0.to_vec()),
            Value::Bytes(self.host.clone()),
            address(self.address),
            address(self.source),
            number(self.port.into()),
            number(self.trusted.into()),
        ])
        .encode())
    }
    fn decode(key: &[u8], raw: &[u8], limit: usize) -> Result<Self, Error> {
        if raw.len() > MAX_HOST + 128 {
            return Err(Error::Corrupt("peer record length"));
        }
        let Value::List(fields) = decode_flat(raw, MAX_HOST + 128, 8)? else {
            return Err(Error::Corrupt("peer record"));
        };
        let [version, id, host, addr, source, port, trusted] = fields.as_slice() else {
            return Err(Error::Corrupt("peer record fields"));
        };
        let id = PeerPublicKey(
            bytes(id)?
                .try_into()
                .map_err(|_| Error::Corrupt("peer id length"))?,
        );
        if integer(version)? != 1 || key != repo::key(RECORDS, id.0) {
            return Err(Error::Corrupt("peer record version or key"));
        }
        let trust = integer(trusted)?;
        if trust > 1 {
            return Err(Error::Corrupt("peer trust"));
        }
        let result = Self {
            id,
            host: bytes(host)?.to_vec(),
            address: ip(addr)?,
            source: ip(source)?,
            port: integer(port)?
                .try_into()
                .map_err(|_| Error::Corrupt("peer port"))?,
            trusted: trust == 1,
        };
        if result.encode(MAX_HOST)? != raw {
            return Err(Error::Corrupt("peer record encoding"));
        }
        if result.host.len() > limit {
            return Err(Error::ResourceLimit);
        }
        Ok(result)
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
        if raw.len() > 40 {
            return Err(Error::Corrupt("peer metadata length"));
        }
        let Value::List(fields) = decode_flat(&raw, 40, 5)? else {
            return Err(Error::Corrupt("peer metadata"));
        };
        let [version, revision, records, length] = fields.as_slice() else {
            return Err(Error::Corrupt("peer metadata fields"));
        };
        let result = Self {
            revision: integer(revision)?,
            records: integer(records)?,
            bytes: integer(length)?,
        };
        if integer(version)? != 1
            || result.revision == 0
            || result.records > MAX_RECORDS as u64
            || result.bytes > MAX_BYTES as u64
            || (result.records == 0) != (result.bytes == 0)
            || result.encode() != raw
        {
            return Err(Error::Corrupt("peer metadata encoding"));
        }
        Ok(result)
    }
    fn check(self, limits: Limits) -> Result<(), Error> {
        limits.validate()?;
        if self.records > limits.records as u64 || self.bytes > limits.bytes as u64 {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
}
// A version is bound to one open database, not to its changing chain tip.
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
pub struct Stored<'a> {
    source: Snapshot<'a>,
    metadata: Metadata,
}
impl Stored<'_> {
    pub fn version(&self) -> Version {
        Version {
            source: self.source.source_id,
            revision: self.metadata.revision,
        }
    }
    pub fn get(&self, id: PeerPublicKey, host_bytes: usize) -> Result<Option<Record>, Error> {
        let key = repo::key(RECORDS, id.0);
        self.source
            .get(&key)?
            .map(|raw| Record::decode(&key, &raw, host_bytes))
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
                result.push(Record::decode(key, raw, limits.host_bytes)?);
                Ok(())
            },
        )?;
        if result.len() as u64 != self.metadata.records || length as u64 != self.metadata.bytes {
            return Err(Error::Corrupt("peer metadata totals"));
        }
        Ok(result)
    }
}
impl Database {
    pub fn peer_view(&self) -> Result<Stored<'_>, Error> {
        let policy = self.writer.lock().map_err(|_| backend_lock())?;
        let source = Snapshot {
            snapshot: Arc::new(self.db.snapshot()),
            source_id: self.source_id,
            policy: *policy,
        };
        let metadata = Metadata::decode(source.get(METADATA)?)?;
        Ok(Stored { source, metadata })
    }
    pub(super) fn validate_peers(&self) -> Result<(), Error> {
        let metadata = Metadata::decode(self.db.get(METADATA).map_err(backend)?)?;
        if metadata.revision == 0
            && let Some(pair) = self
                .db
                .iterator(IteratorMode::From(&[RECORDS], Direction::Forward))
                .next()
            && pair.map_err(backend)?.0.first() == Some(&RECORDS)
        {
            return Err(Error::Corrupt("peer records without metadata"));
        }
        Ok(())
    }
}
// This lease serializes all supported peer writes. A higher layer must hold it
// for the entire registry lifetime and publish policy only after acknowledgement.
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
            let mut owner = db.peer_owner.lock().map_err(|_| backend_lock())?;
            if owner.upgrade().is_some() {
                return Err(Failure::AlreadyRunning);
            }
            *owner = Arc::downgrade(&lease);
        }
        db.peer_view()?.records(limits)?;
        Ok(Self {
            db,
            limits,
            _lease: lease,
            uncertain: false,
        })
    }
    pub fn version(&self) -> Result<Version, Error> {
        Ok(self.db.peer_view()?.version())
    }
    // Recovery returns the complete durable truth. The registry owner must
    // reconcile/rebuild its state before executing any further lifecycle effects.
    pub fn recover(&mut self) -> Result<(Version, Vec<Record>), Failure> {
        let view = self.db.peer_view()?;
        let rows = view.records(self.limits)?;
        let version = view.version();
        self.uncertain = false;
        Ok((version, rows))
    }
    pub fn commit(&mut self, version: Version, changes: &[Change]) -> Result<Version, Failure> {
        self.commit_checked(
            version,
            changes,
            || Ok(()),
            |db, batch| db.db.write_opt(batch, &write_options()).map_err(backend),
        )
    }
    fn commit_checked(
        &mut self,
        version: Version,
        changes: &[Change],
        mut check: impl FnMut() -> Result<(), Error>,
        write: impl FnOnce(&Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Version, Failure> {
        if self.uncertain {
            return Err(Failure::RestartRequired);
        }
        if version.source != self.db.source_id {
            return Err(Error::Identity.into());
        }
        if changes.len() > self.limits.changes {
            return Err(Error::ResourceLimit.into());
        }
        // Bound every supplied operation, including overwritten keys, before
        // reducing to one final write per key in original operation order.
        let mut writes = BTreeMap::new();
        let mut supplied = 0usize;
        for change in changes {
            check()?;
            let (key, value) = match change {
                Change::Put(record) => (
                    repo::key(RECORDS, record.id.0),
                    Some(record.encode(self.limits.host_bytes)?),
                ),
                Change::Delete(id) => (repo::key(RECORDS, id.0), None),
            };
            supplied = supplied
                .checked_add(key.len())
                .and_then(|n| n.checked_add(value.as_ref().map_or(0, Vec::len)))
                .ok_or(Error::ResourceLimit)?;
            if supplied > self.limits.commit_bytes {
                return Err(Error::ResourceLimit.into());
            }
            writes.insert(key, value);
        }
        let _guard = self.db.writer.lock().map_err(|_| backend_lock())?;
        let mut metadata = Metadata::decode(self.db.db.get(METADATA).map_err(backend)?)?;
        if metadata.revision != version.revision {
            return Err(Error::Conflict.into());
        }
        if writes.is_empty() {
            check()?;
            return Ok(version);
        }
        metadata.revision = metadata
            .revision
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        for (key, value) in &writes {
            if let Some(old) = self.db.db.get(key).map_err(backend)? {
                Record::decode(key, &old, self.limits.host_bytes)?;
                metadata.records = metadata
                    .records
                    .checked_sub(1)
                    .ok_or(Error::Corrupt("peer record count"))?;
                metadata.bytes = metadata
                    .bytes
                    .checked_sub((key.len() + old.len()) as u64)
                    .ok_or(Error::Corrupt("peer byte count"))?;
            }
            if let Some(raw) = value {
                metadata.records = metadata
                    .records
                    .checked_add(1)
                    .ok_or(Error::ResourceLimit)?;
                metadata.bytes = metadata
                    .bytes
                    .checked_add((key.len() + raw.len()) as u64)
                    .ok_or(Error::ResourceLimit)?;
            }
        }
        metadata.check(self.limits)?;
        writes.insert(METADATA.to_vec(), Some(metadata.encode()));
        if pool::write_bytes(&writes)? > self.limits.commit_bytes {
            return Err(Error::ResourceLimit.into());
        }
        let mut batch = WriteBatch::default();
        for (key, value) in writes {
            check()?;
            match value {
                Some(v) => batch.put(key, v),
                None => batch.delete(key),
            }
        }
        check()?;
        if let Err(error) = write(&self.db, batch) {
            self.uncertain = true;
            return Err(error.into());
        }
        Ok(Version {
            source: version.source,
            revision: metadata.revision,
        })
    }
}
#[cfg(test)]
mod tests;
