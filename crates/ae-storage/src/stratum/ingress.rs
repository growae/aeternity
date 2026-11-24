// A single ordered consumer owns an epoch. Only the most recent receipt is
// replayable; older sequence numbers are rejected, never applied again. Pending
// solutions survive epoch changes until canonical admission is integrated.
use super::*;
use ae_stratum::ingress::{Id, Input};
use std::sync::Weak;
const SOLUTIONS: u8 = 23;
const META: &[u8] = b"\0stratum-ingress";
const RECORD_BYTES: usize = 16384;
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub solutions: usize,
    pub bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            solutions: 100_000,
            bytes: 256 << 20,
        }
    }
}
impl Limits {
    pub(super) fn maximum() -> Self {
        Self {
            solutions: MAX_RECORDS,
            bytes: MAX_BYTES,
        }
    }
    fn validate(self) -> Result<(), Error> {
        if self.solutions == 0
            || self.solutions > MAX_RECORDS
            || self.bytes == 0
            || self.bytes > MAX_BYTES
        {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub struct Epoch {
    value: u64,
    source: u64,
    owner: Weak<()>,
}
impl Epoch {
    pub fn value(&self) -> u64 {
        self.value
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    pub id: Id,
    pub input: Input,
    pub share_key: u64,
}
fn flat(raw: &[u8]) -> Result<Value, Failure> {
    let v = ae_codec::rlp::decode_with_limits(
        raw,
        ae_codec::rlp::DecodeLimits {
            max_input_bytes: RECORD_BYTES,
            max_items: 12,
            max_depth: 2,
        },
    )
    .map_err(|_| Error::Corrupt("Stratum ingress RLP"))?;
    Ok(Value::from_rlp(&v))
}
impl Receipt {
    fn encode(&self) -> Result<Vec<u8>, Failure> {
        self.id.validate()?;
        Ok(Value::List(vec![
            number(1),
            number(self.id.epoch),
            number(self.id.sequence),
            number(self.share_key),
            Value::Bytes(self.input.encode()?),
        ])
        .encode())
    }
    fn decode(raw: &[u8]) -> Result<Self, Failure> {
        let Value::List(v) = flat(raw)? else {
            return Err(Error::Corrupt("Stratum receipt").into());
        };
        let [version, epoch, sequence, key, Value::Bytes(input)] = v.as_slice() else {
            return Err(Error::Corrupt("Stratum receipt fields").into());
        };
        let r = Self {
            id: Id {
                epoch: integer(epoch)?,
                sequence: integer(sequence)?,
            },
            share_key: integer(key)?,
            input: Input::decode(input)?,
        };
        if integer(version)? != 1 || r.encode()? != raw {
            return Err(Error::Corrupt("Stratum receipt encoding").into());
        }
        Ok(r)
    }
    fn key(&self) -> Vec<u8> {
        key(self.id)
    }
}
fn key(id: Id) -> Vec<u8> {
    let mut key = vec![SOLUTIONS];
    key.extend_from_slice(&id.epoch.to_be_bytes());
    key.extend_from_slice(&id.sequence.to_be_bytes());
    key
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Metadata {
    epoch: u64,
    last: Option<Receipt>,
    solutions: u64,
    bytes: u64,
}
impl Metadata {
    fn sequence(&self) -> u64 {
        self.last.as_ref().map_or(0, |r| r.id.sequence)
    }
    fn encode(&self) -> Result<Vec<u8>, Failure> {
        Ok(Value::List(vec![
            number(1),
            number(self.epoch),
            Value::List(
                self.last
                    .as_ref()
                    .map(|r| r.encode().map(Value::Bytes))
                    .transpose()?
                    .into_iter()
                    .collect(),
            ),
            number(self.solutions),
            number(self.bytes),
        ])
        .encode())
    }
    fn decode(raw: Option<&[u8]>) -> Result<Self, Failure> {
        let Some(raw) = raw else {
            return Ok(Self::default());
        };
        let Value::List(v) = flat(raw)? else {
            return Err(Error::Corrupt("Stratum ingress metadata").into());
        };
        let [version, epoch, Value::List(last), solutions, bytes] = v.as_slice() else {
            return Err(Error::Corrupt("Stratum ingress fields").into());
        };
        let last = match last.as_slice() {
            [] => None,
            [Value::Bytes(b)] => Some(Receipt::decode(b)?),
            _ => return Err(Error::Corrupt("Stratum ingress last receipt").into()),
        };
        let m = Self {
            epoch: integer(epoch)?,
            last,
            solutions: integer(solutions)?,
            bytes: integer(bytes)?,
        };
        if integer(version)? != 1
            || m.epoch == 0
            || m.last.as_ref().is_some_and(|r| r.id.epoch != m.epoch)
            || m.encode()? != raw
        {
            return Err(Error::Corrupt("Stratum ingress metadata encoding").into());
        }
        m.check(Limits::maximum())?;
        Ok(m)
    }
    fn check(&self, l: Limits) -> Result<(), Failure> {
        l.validate()?;
        if self.solutions > l.solutions as u64 || self.bytes > l.bytes as u64 {
            return Err(Error::ResourceLimit.into());
        }
        if (self.solutions == 0) != (self.bytes == 0) {
            return Err(Error::Corrupt("Stratum ingress empty totals").into());
        }
        Ok(())
    }
}
pub struct Inbox<'a> {
    source: Snapshot<'a>,
    accounting: Info,
    metadata: Metadata,
    limits: Limits,
}
impl<'a> Stored<'a> {
    pub fn ingress(&self, limits: Limits) -> Result<Inbox<'a>, Failure> {
        limits.validate()?;
        let raw = self.source.snapshot.get_pinned(META).map_err(backend)?;
        let metadata = Metadata::decode(raw.as_deref())?;
        metadata.check(limits)?;
        Ok(Inbox {
            source: self.source.clone(),
            accounting: self.info,
            metadata,
            limits,
        })
    }
}
impl Inbox<'_> {
    pub fn epoch(&self) -> u64 {
        self.metadata.epoch
    }
    pub fn sequence(&self) -> u64 {
        self.metadata.sequence()
    }
    pub fn last(&self) -> Option<&Receipt> {
        self.metadata.last.as_ref()
    }
    fn valid(&self, r: &Receipt) -> Result<(), Failure> {
        if r.id.epoch > self.metadata.epoch
            || (r.id.epoch == self.metadata.epoch && r.id.sequence > self.sequence())
        {
            return Err(Error::Corrupt("Stratum ingress future receipt").into());
        }
        self.accounting
            .allocated(&core::Record::Round(r.share_key))?;
        Ok(())
    }
    fn record(&self, key: &[u8], raw: &[u8]) -> Result<Receipt, Failure> {
        let r = Receipt::decode(raw)?;
        if r.key() != key || r.input.solution.is_none() {
            return Err(Error::Corrupt("Stratum pending solution key").into());
        }
        self.valid(&r)?;
        Ok(r)
    }
    pub fn pending(
        &self,
        after: Option<Id>,
        count: usize,
    ) -> Result<(Vec<Receipt>, bool), Failure> {
        if count == 0 || count > self.limits.solutions {
            return Err(Error::ResourceLimit.into());
        }
        let mut it = self.source.snapshot.raw_iterator();
        if let Some(id) = after {
            id.validate()?;
            let k = key(id);
            it.seek(&k);
            if it.key() == Some(k.as_slice()) {
                it.next();
            }
        } else {
            it.seek([SOLUTIONS]);
        }
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        let mut more = false;
        while let Some(k) = it.key() {
            if !k.starts_with(&[SOLUTIONS]) {
                break;
            }
            if rows.len() == count {
                more = true;
                break;
            }
            let v = it
                .value()
                .ok_or(Error::Corrupt("Stratum pending solution value"))?;
            bytes = bytes
                .checked_add(k.len())
                .and_then(|n| n.checked_add(v.len()))
                .ok_or(Error::ResourceLimit)?;
            if bytes > self.limits.bytes {
                return Err(Error::ResourceLimit.into());
            }
            rows.push(self.record(k, v)?);
            it.next();
        }
        it.status().map_err(backend)?;
        Ok((rows, more))
    }
    pub fn verify(&self) -> Result<(), Failure> {
        self.metadata.check(self.limits)?;
        if let Some(r) = self.last() {
            self.valid(r)?;
        }
        let mut it = self.source.snapshot.raw_iterator();
        it.seek([SOLUTIONS]);
        let mut count = 0u64;
        let mut bytes = 0u64;
        let mut latest_solution = false;
        while let Some(k) = it.key() {
            if !k.starts_with(&[SOLUTIONS]) {
                break;
            }
            let raw = it
                .value()
                .ok_or(Error::Corrupt("Stratum pending solution value"))?;
            count = count.checked_add(1).ok_or(Error::ResourceLimit)?;
            bytes = bytes
                .checked_add(k.len() as u64)
                .and_then(|n| n.checked_add(raw.len() as u64))
                .ok_or(Error::ResourceLimit)?;
            if count > self.limits.solutions as u64 || bytes > self.limits.bytes as u64 {
                return Err(Error::ResourceLimit.into());
            }
            let r = self.record(k, raw)?;
            if let Some(last) = self.last()
                && last.id == r.id
            {
                if last != &r {
                    return Err(Error::Corrupt("Stratum last solution receipt").into());
                }
                latest_solution = true;
            }
            it.next();
        }
        it.status().map_err(backend)?;
        if count != self.metadata.solutions
            || bytes != self.metadata.bytes
            || (self.last().is_some_and(|r| r.input.solution.is_some()) && !latest_solution)
        {
            return Err(Error::Corrupt("Stratum ingress totals").into());
        }
        Ok(())
    }
    fn extra(&self, after: &Metadata) -> Result<Extra, Failure> {
        let before = self
            .source
            .snapshot
            .get_pinned(META)
            .map_err(backend)?
            .map(|v| v.to_vec());
        Ok(Extra {
            key: META.to_vec(),
            before,
            after: Some(after.encode()?),
        })
    }
}
impl Writer {
    pub fn begin_ingress(&mut self, limits: Limits) -> Result<Epoch, Failure> {
        self.begin_ingress_with(limits, |db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        })
    }
    fn begin_ingress_with(
        &mut self,
        limits: Limits,
        write: impl FnOnce(&Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Epoch, Failure> {
        let version = self.version()?;
        let stored = self.db.stratum(self.limits)?;
        let inbox = stored.ingress(limits)?;
        let mut metadata = inbox.metadata.clone();
        metadata.epoch = metadata.epoch.checked_add(1).ok_or(Error::ResourceLimit)?;
        metadata.last = None;
        let epoch = Epoch {
            value: metadata.epoch,
            source: self.db.source_id,
            owner: Arc::downgrade(&self._lease),
        };
        let extra = inbox.extra(&metadata)?;
        drop(inbox);
        drop(stored);
        self.commit_extra(version, vec![], vec![extra], write)?;
        Ok(epoch)
    }
    pub fn accept(
        &mut self,
        epoch: &Epoch,
        sequence: u64,
        input: Input,
        limits: Limits,
    ) -> Result<Receipt, Failure> {
        self.accept_with(epoch, sequence, input, limits, |db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        })
    }
    fn accept_with(
        &mut self,
        epoch: &Epoch,
        sequence: u64,
        input: Input,
        limits: Limits,
        write: impl FnOnce(&Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Receipt, Failure> {
        input.validate()?;
        let id = Id {
            epoch: epoch.value,
            sequence,
        };
        id.validate()?;
        if epoch.source != self.db.source_id || !epoch.owner.ptr_eq(&Arc::downgrade(&self._lease)) {
            return Err(Error::Conflict.into());
        }
        let version = self.version()?;
        let stored = self.db.stratum(self.limits)?;
        let inbox = stored.ingress(limits)?;
        if inbox.epoch() != epoch.value {
            return Err(Error::Conflict.into());
        }
        if let Some(r) = inbox.last()
            && r.id == id
        {
            return if r.input == input {
                Ok(r.clone())
            } else {
                Err(Error::Conflict.into())
            };
        }
        if inbox.sequence().checked_add(1) != Some(sequence) {
            return Err(Error::Conflict.into());
        }
        let receipt = Receipt {
            id,
            input,
            share_key: stored.info.next_key.ok_or(Error::ResourceLimit)?,
        };
        let mut metadata = inbox.metadata.clone();
        metadata.last = Some(receipt.clone());
        let mut extra = vec![];
        if receipt.input.solution.is_some() {
            let raw = receipt.encode()?;
            let k = receipt.key();
            metadata.solutions = metadata
                .solutions
                .checked_add(1)
                .ok_or(Error::ResourceLimit)?;
            metadata.bytes = metadata
                .bytes
                .checked_add((k.len() + raw.len()) as u64)
                .ok_or(Error::ResourceLimit)?;
            extra.push(Extra {
                key: k,
                before: None,
                after: Some(raw),
            });
        }
        metadata.check(limits)?;
        extra.push(inbox.extra(&metadata)?);
        let command = Command::Share {
            hash: receipt.input.hash,
            target: receipt.input.target.clone(),
            miner: receipt.input.account.as_bytes().to_vec(),
        };
        drop(inbox);
        drop(stored);
        let done = self.commit_extra(version, vec![command], extra, write)?;
        debug_assert!(
            matches!(done.replies.as_slice(),[core::Reply::Record(r)] if matches!(&**r,core::Record::Share(s) if s.key==receipt.share_key))
        );
        Ok(receipt)
    }
}
#[cfg(test)]
mod tests;
