// aestratum_db fa2a6d7. A bounded transaction overlay over immutable reads;
// persistence, key allocation and wall-clock dates belong to the owner.
mod codec;
use crate::{
    proof::Number,
    rewards::{self, Amounts, Map},
};
use ae_types::Hash256;
use num_bigint::BigInt;
use std::collections::BTreeMap;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Backend(String),
    Corrupt(&'static str),
    Limit,
    MissingHash,
    MissingShare,
    NoRange,
    TransformedReward,
    Aborted,
    Rewards(rewards::Error),
}
impl From<rewards::Error> for Error {
    fn from(e: rewards::Error) -> Self {
        Self::Rewards(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Table {
    Hashes = 1,
    Shares = 2,
    Rounds = 3,
    Rewards = 4,
    Payments = 5,
    Candidates = 6,
}
impl Table {
    pub const ALL: [Self; 6] = [
        Self::Hashes,
        Self::Shares,
        Self::Rounds,
        Self::Rewards,
        Self::Payments,
        Self::Candidates,
    ];
    pub fn from_byte(n: u8) -> Result<Self, Error> {
        Self::ALL
            .into_iter()
            .find(|t| *t as u8 == n)
            .ok_or(Error::Corrupt("accounting table"))
    }
    pub fn prefix(self) -> Vec<u8> {
        vec![self as u8]
    }
    pub fn end(self) -> Vec<u8> {
        vec![self as u8 + 1]
    }
}
pub type PaymentId = (u64, u32);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Date(pub [u32; 6]);
impl Date {
    pub fn from_unix_millis(ms: u64) -> Result<Self, Error> {
        if ms > 253_402_300_799_999 {
            return Err(Error::Corrupt("accounting UTC timestamp"));
        }
        let seconds = ms / 1000;
        let mut days = seconds / 86400;
        let mut year = 1970u32;
        let leap = |year: u32| {
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
        };
        loop {
            let length = if leap(year) { 366 } else { 365 };
            if days < length {
                break;
            }
            days -= length;
            year += 1;
        }
        let months = [
            31u64,
            if leap(year) { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        let mut month = 0;
        while days >= months[month] {
            days -= months[month];
            month += 1;
        }
        let value = Self([
            year,
            month as u32 + 1,
            days as u32 + 1,
            ((seconds / 3600) % 24) as u32,
            ((seconds / 60) % 60) as u32,
            (seconds % 60) as u32,
        ]);
        value.validate()?;
        Ok(value)
    }

    pub fn validate(self) -> Result<(), Error> {
        let [year, month, day, hour, minute, second] = self.0;
        let leap =
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
        let days = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                if leap {
                    29
                } else {
                    28
                }
            }
            _ => 0,
        };
        if year > 9999 || day == 0 || day > days || hour > 23 || minute > 59 || second > 59 {
            return Err(Error::Corrupt("accounting date"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Share {
    pub key: u64,
    pub hash: Hash256,
    pub target: BigInt,
    pub miner: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Shares {
    Relative(Map),
    Transformed,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reward {
    pub height: u64,
    pub hash: Hash256,
    pub pool: Shares,
    pub miners: Shares,
    pub amount: BigInt,
    pub round_key: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Payment {
    pub id: PaymentId,
    pub total: BigInt,
    pub relative: Map,
    pub absolute: Option<Amounts>,
    pub tx_hash: Option<Hash256>,
    pub nonce: Option<BigInt>,
    pub fee: Option<BigInt>,
    pub gas: Option<BigInt>,
    pub run_gas: Option<BigInt>,
    pub date: Option<Date>,
}
impl From<rewards::Payment> for Payment {
    fn from(p: rewards::Payment) -> Self {
        Self {
            id: p.id,
            total: p.total,
            relative: p.relative,
            absolute: None,
            tx_hash: None,
            nonce: None,
            fee: None,
            gas: None,
            run_gas: None,
            date: None,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub hash: Hash256,
    pub header: Vec<u8>,
    pub record: Vec<u8>,
    pub date: Date,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Hash { hash: Hash256, key: u64 },
    Share(Share),
    Round(u64),
    Reward(Reward),
    Payment(Payment),
    Candidate(Candidate),
}
pub fn key(table: Table, suffix: impl AsRef<[u8]>) -> Vec<u8> {
    let mut k = table.prefix();
    k.extend_from_slice(suffix.as_ref());
    k
}
pub fn payment_key(id: PaymentId) -> Vec<u8> {
    let mut suffix = id.0.to_be_bytes().to_vec();
    suffix.extend_from_slice(&id.1.to_be_bytes());
    key(Table::Payments, suffix)
}
impl Record {
    pub fn table(&self) -> Table {
        match self {
            Self::Hash { .. } => Table::Hashes,
            Self::Share(_) => Table::Shares,
            Self::Round(_) => Table::Rounds,
            Self::Reward(_) => Table::Rewards,
            Self::Payment(_) => Table::Payments,
            Self::Candidate(_) => Table::Candidates,
        }
    }
    pub fn key(&self) -> Vec<u8> {
        match self {
            Self::Hash { hash, .. } => key(Table::Hashes, hash),
            Self::Share(s) => key(Table::Shares, s.key.to_be_bytes()),
            Self::Round(k) => key(Table::Rounds, k.to_be_bytes()),
            Self::Reward(r) => key(Table::Rewards, r.height.to_be_bytes()),
            Self::Payment(p) => payment_key(p.id),
            Self::Candidate(c) => key(Table::Candidates, c.hash),
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub records: usize,
    pub bytes: usize,
    pub record_bytes: usize,
    pub maps: rewards::Limits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            records: 100_000,
            bytes: 64 << 20,
            record_bytes: 16 << 20,
            maps: Default::default(),
        }
    }
}
impl Limits {
    pub fn validate(self) -> Result<(), Error> {
        self.maps.validate()?;
        if self.records == 0
            || self.records > 1_000_000
            || self.bytes == 0
            || self.bytes > 1 << 30
            || self.record_bytes == 0
            || self.record_bytes > 64 << 20
        {
            return Err(Error::Limit);
        }
        Ok(())
    }
}
pub type Entries = Vec<(Vec<u8>, Vec<u8>)>;
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Scan {
    pub entries: Entries,
    pub more: bool,
}
// Half-open lexicographic ranges, in the requested direction. `more` must be
// true when additional rows exist; silently truncated reads are not valid.
pub trait Read {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    fn scan(&self, start: &[u8], end: &[u8], limit: usize, reverse: bool) -> Result<Scan, Error>;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Round(u64),
    Share(Share),
    Solution {
        candidate: Hash256,
        block: Hash256,
    },
    Reward(Reward),
    StorePayments {
        reward: Reward,
        percent: Number,
    },
    Payment(Payment),
    UpdatePayment {
        payment: Box<Payment>,
        absolute: Amounts,
        hash: Hash256,
        date: Date,
    },
    DeletePayment(PaymentId),
    Candidate(Candidate),
    DeleteCandidatesBefore(Date),
    DeleteRecords(u64),
    Clear(Table),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Unit,
    Record(Box<Record>),
    Records(Vec<Record>),
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Keys {
    pub hashes: Vec<Hash256>,
    pub shares: Vec<u64>,
    pub rounds: Vec<u64>,
    pub rewards: Vec<u64>,
    pub payments: Vec<PaymentId>,
}
pub type Writes = BTreeMap<Vec<u8>, Option<Vec<u8>>>;
#[derive(Debug)]
pub struct Plan {
    writes: Writes,
    originals: Writes,
}
impl Plan {
    pub fn writes(&self) -> &Writes {
        &self.writes
    }
    pub fn originals(&self) -> &Writes {
        &self.originals
    }
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }
}
pub struct Transaction<'a, R> {
    source: &'a R,
    limits: Limits,
    writes: Writes,
    originals: Writes,
    reads: usize,
    read_bytes: usize,
    write_bytes: usize,
    failed: bool,
}
pub fn prepare<R: Read, T>(
    source: &R,
    limits: Limits,
    action: impl FnOnce(&mut Transaction<'_, R>) -> Result<T, Error>,
) -> Result<(T, Plan), Error> {
    limits.validate()?;
    let mut tx = Transaction {
        source,
        limits,
        writes: BTreeMap::new(),
        originals: BTreeMap::new(),
        reads: 0,
        read_bytes: 0,
        write_bytes: 0,
        failed: false,
    };
    let result = action(&mut tx)?;
    if tx.failed {
        return Err(Error::Aborted);
    }
    Ok((
        result,
        Plan {
            writes: tx.writes,
            originals: tx.originals,
        },
    ))
}
impl<R: Read> Transaction<'_, R> {
    fn charge(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(), Error> {
        self.reads = self.reads.checked_add(1).ok_or(Error::Limit)?;
        self.read_bytes = self
            .read_bytes
            .checked_add(key.len() + value.map_or(0, <[u8]>::len))
            .ok_or(Error::Limit)?;
        if self.reads > self.limits.records
            || self.read_bytes > self.limits.bytes
            || value.is_some_and(|v| v.len() > self.limits.record_bytes)
        {
            return Err(Error::Limit);
        }
        Ok(())
    }
    fn raw(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if let Some(v) = self.writes.get(key) {
            return Ok(v.clone());
        }
        let v = self.source.get(key)?;
        self.charge(key, v.as_deref())?;
        Ok(v)
    }
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Record>, Error> {
        self.raw(key)?
            .map(|v| Record::decode(key, &v, self.limits))
            .transpose()
    }
    fn put(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<(), Error> {
        if !self.originals.contains_key(&key) {
            let old = self.raw(&key)?;
            self.originals.insert(key.clone(), old);
        }
        let prior = self
            .writes
            .get(&key)
            .map_or(0, |v| key.len() + v.as_ref().map_or(0, Vec::len));
        self.write_bytes = self
            .write_bytes
            .checked_sub(prior)
            .and_then(|n| n.checked_add(key.len() + value.as_ref().map_or(0, Vec::len)))
            .ok_or(Error::Limit)?;
        if self.write_bytes > self.limits.bytes
            || (!self.writes.contains_key(&key) && self.writes.len() >= self.limits.records)
        {
            return Err(Error::Limit);
        }
        self.writes.insert(key, value);
        Ok(())
    }
    fn store(&mut self, record: Record) -> Result<Record, Error> {
        let bytes = record.encode(self.limits)?;
        self.put(record.key(), Some(bytes))?;
        Ok(record)
    }
    fn remove(&mut self, key: Vec<u8>) -> Result<(), Error> {
        self.put(key, None)
    }
    pub fn range(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        reverse: bool,
    ) -> Result<(Vec<Record>, bool), Error> {
        if limit == 0 {
            return Err(Error::Limit);
        }
        if start > end {
            return Err(Error::Corrupt("accounting range"));
        }
        if start == end {
            return Ok((vec![], false));
        }
        let changed = self.writes.range(start.to_vec()..end.to_vec()).count();
        let count = limit.checked_add(changed).ok_or(Error::Limit)?;
        if count > self.limits.records.saturating_sub(self.reads) {
            return Err(Error::Limit);
        }
        let rows = self.source.scan(start, end, count, reverse)?;
        if rows.entries.len() > count || rows.more && rows.entries.len() < count {
            return Err(Error::Corrupt("accounting scan truncation"));
        }
        let mut merged = BTreeMap::new();
        let mut prior: Option<Vec<u8>> = None;
        for (k, v) in rows.entries {
            if k.as_slice() < start
                || k.as_slice() >= end
                || prior
                    .as_ref()
                    .is_some_and(|p| if reverse { p <= &k } else { p >= &k })
            {
                return Err(Error::Corrupt("accounting scan order"));
            }
            self.charge(&k, Some(&v))?;
            Record::decode(&k, &v, self.limits)?;
            prior = Some(k.clone());
            merged.insert(k, v);
        }
        for (k, v) in self.writes.range(start.to_vec()..end.to_vec()) {
            if let Some(v) = v {
                merged.insert(k.clone(), v.clone());
            } else {
                merged.remove(k);
            }
        }
        let more = rows.more || merged.len() > limit;
        let iter: Box<dyn Iterator<Item = (&Vec<u8>, &Vec<u8>)>> = if reverse {
            Box::new(merged.iter().rev())
        } else {
            Box::new(merged.iter())
        };
        let result = iter
            .take(limit)
            .map(|(k, v)| Record::decode(k, v, self.limits))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((result, more))
    }
    pub fn records(&mut self, table: Table) -> Result<Vec<Record>, Error> {
        self.collect(&table.prefix(), &table.end())
    }
    fn collect(&mut self, start: &[u8], end: &[u8]) -> Result<Vec<Record>, Error> {
        let mut cursor = start.to_vec();
        let mut result = Vec::new();
        loop {
            let remaining = self.limits.records.saturating_sub(
                self.reads + self.writes.range(cursor.clone()..end.to_vec()).count(),
            );
            if remaining == 0 {
                return Err(Error::Limit);
            }
            let (rows, more) = self.range(&cursor, end, 128.min(remaining), false)?;
            if !more {
                result.extend(rows);
                return Ok(result);
            }
            cursor = rows
                .last()
                .ok_or(Error::Corrupt("empty accounting page"))?
                .key();
            cursor.push(0);
            result.extend(rows);
        }
    }
    pub fn hash(&mut self, hash: Hash256) -> Result<Option<u64>, Error> {
        match self.get(&key(Table::Hashes, hash))? {
            Some(Record::Hash { key, .. }) => Ok(Some(key)),
            None => Ok(None),
            _ => Err(Error::Corrupt("hash record")),
        }
    }
    pub fn reward(&mut self, height: u64) -> Result<Option<Reward>, Error> {
        match self.get(&key(Table::Rewards, height.to_be_bytes()))? {
            Some(Record::Reward(r)) => Ok(Some(r)),
            None => Ok(None),
            _ => Err(Error::Corrupt("reward record")),
        }
    }
    pub fn candidate(&mut self, hash: Hash256) -> Result<Option<Candidate>, Error> {
        match self.get(&key(Table::Candidates, hash))? {
            Some(Record::Candidate(c)) => Ok(Some(c)),
            None => Ok(None),
            _ => Err(Error::Corrupt("candidate record")),
        }
    }
    pub fn payments(&mut self, height: u64) -> Result<Vec<Payment>, Error> {
        let start = key(Table::Payments, height.to_be_bytes());
        let end = if height == u64::MAX {
            Table::Payments.end()
        } else {
            key(Table::Payments, (height + 1).to_be_bytes())
        };
        self.collect(&start, &end)?
            .into_iter()
            .map(|r| {
                if let Record::Payment(p) = r {
                    Ok(p)
                } else {
                    Err(Error::Corrupt("payment record"))
                }
            })
            .collect()
    }
    pub fn select_payments(
        &mut self,
        paid: bool,
        limit: Option<usize>,
    ) -> Result<Vec<Payment>, Error> {
        if limit == Some(0) {
            return Err(Error::Limit);
        }
        let mut cursor = Table::Payments.prefix();
        let end = Table::Payments.end();
        let mut result = Vec::new();
        loop {
            let remaining = self.limits.records.saturating_sub(
                self.reads + self.writes.range(cursor.clone()..end.clone()).count(),
            );
            if remaining == 0 {
                return Err(Error::Limit);
            }
            let (rows, more) = self.range(&cursor, &end, 128.min(remaining), false)?;
            for row in &rows {
                let Record::Payment(p) = row else {
                    return Err(Error::Corrupt("payment record"));
                };
                if p.tx_hash.is_some() == paid {
                    result.push(p.clone());
                    if limit == Some(result.len()) {
                        return Ok(result);
                    }
                }
            }
            if !more {
                return Ok(result);
            }
            cursor = rows
                .last()
                .ok_or(Error::Corrupt("empty payment page"))?
                .key();
            cursor.push(0);
        }
    }
    pub fn shares_range(&mut self, share: u64, last_n: usize) -> Result<(u64, u64), Error> {
        let (previous, _) = self.range(
            &Table::Rounds.prefix(),
            &key(Table::Rounds, share.to_be_bytes()),
            1,
            true,
        )?;
        let first = match previous.first() {
            Some(Record::Round(k)) => *k,
            None => share,
            _ => return Err(Error::Corrupt("round record")),
        };
        let (rounds, _) = self.range(
            &key(Table::Rounds, first.to_be_bytes()),
            &Table::Rounds.end(),
            last_n.checked_add(1).ok_or(Error::Limit)?,
            false,
        )?;
        match rounds.last() {
            Some(Record::Round(last)) => Ok((first, *last)),
            None => Err(Error::NoRange),
            _ => Err(Error::Corrupt("round record")),
        }
    }
    pub fn miner_rewards(
        &mut self,
        share: u64,
        target: &BigInt,
        last_n: usize,
    ) -> Result<(Map, u64), Error> {
        let (first, last) = self.shares_range(share, last_n)?;
        let end = if last == u64::MAX {
            Table::Shares.end()
        } else {
            key(Table::Shares, (last + 1).to_be_bytes())
        };
        let rows = self.collect(&key(Table::Shares, first.to_be_bytes()), &end)?;
        let shares = rows
            .into_iter()
            .map(|r| {
                if let Record::Share(s) = r {
                    Ok((s.miner, s.target))
                } else {
                    Err(Error::Corrupt("share record"))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((
            rewards::miner_shares(&shares, target, self.limits.maps)?,
            first,
        ))
    }
    pub fn keys(&mut self, height: u64) -> Result<Option<Keys>, Error> {
        let Some(r) = self.reward(height)? else {
            return Ok(None);
        };
        let mut keys = Keys {
            rewards: vec![height],
            ..Default::default()
        };
        for record in self.collect(
            &key(Table::Shares, r.round_key.to_be_bytes()),
            &Table::Shares.end(),
        )? {
            let Record::Share(s) = record else {
                return Err(Error::Corrupt("share record"));
            };
            keys.shares.push(s.key);
            keys.hashes.push(s.hash);
        }
        for record in self.collect(
            &key(Table::Rounds, r.round_key.to_be_bytes()),
            &Table::Rounds.end(),
        )? {
            let Record::Round(k) = record else {
                return Err(Error::Corrupt("round record"));
            };
            keys.rounds.push(k);
        }
        keys.payments = self.payments(height)?.into_iter().map(|p| p.id).collect();
        Ok(Some(keys))
    }
    pub fn apply(&mut self, op: Operation) -> Result<Reply, Error> {
        if self.failed {
            return Err(Error::Aborted);
        }
        let result = self.apply_inner(op);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn apply_inner(&mut self, op: Operation) -> Result<Reply, Error> {
        let record = match op {
            Operation::Round(k) => Some(self.store(Record::Round(k))?),
            Operation::Share(s) => {
                let hash = s.hash;
                let k = s.key;
                let r = self.store(Record::Share(s))?;
                self.store(Record::Hash { hash, key: k })?;
                Some(r)
            }
            Operation::Solution { candidate, block } => {
                let k = self.hash(candidate)?.ok_or(Error::MissingHash)?;
                let Some(Record::Share(mut s)) = self.get(&key(Table::Shares, k.to_be_bytes()))?
                else {
                    return Err(Error::MissingShare);
                };
                self.remove(key(Table::Hashes, candidate))?;
                self.store(Record::Hash {
                    hash: block,
                    key: k,
                })?;
                s.hash = block;
                self.store(Record::Share(s))?;
                None
            }
            Operation::Reward(r) => Some(self.store(Record::Reward(r))?),
            Operation::StorePayments { reward, percent } => {
                let (Shares::Relative(pool), Shares::Relative(miners)) =
                    (reward.pool, reward.miners)
                else {
                    return Err(Error::TransformedReward);
                };
                let payments = rewards::payments(
                    reward.height,
                    &reward.amount,
                    &pool,
                    &miners,
                    &percent,
                    self.limits.maps,
                )?;
                let mut out = Vec::new();
                for p in payments {
                    out.push(self.store(Record::Payment(p.into()))?);
                }
                return Ok(Reply::Records(out));
            }
            Operation::Payment(p) => Some(self.store(Record::Payment(p))?),
            Operation::UpdatePayment {
                mut payment,
                absolute,
                hash,
                date,
            } => {
                payment.absolute = Some(absolute);
                payment.tx_hash = Some(hash);
                payment.date = Some(date);
                Some(self.store(Record::Payment(*payment))?)
            }
            Operation::DeletePayment(id) => {
                self.remove(payment_key(id))?;
                None
            }
            Operation::Candidate(c) => Some(self.store(Record::Candidate(c))?),
            Operation::DeleteCandidatesBefore(date) => {
                date.validate()?;
                for record in self.records(Table::Candidates)? {
                    let Record::Candidate(c) = record else {
                        return Err(Error::Corrupt("candidate record"));
                    };
                    if c.date < date {
                        self.remove(key(Table::Candidates, c.hash))?;
                    }
                }
                None
            }
            Operation::DeleteRecords(height) => {
                if let Some(keys) = self.keys(height)? {
                    for h in keys.hashes {
                        self.remove(key(Table::Hashes, h))?;
                    }
                    for k in keys.shares {
                        self.remove(key(Table::Shares, k.to_be_bytes()))?;
                    }
                    for k in keys.rounds {
                        self.remove(key(Table::Rounds, k.to_be_bytes()))?;
                    }
                    for h in keys.rewards {
                        self.remove(key(Table::Rewards, h.to_be_bytes()))?;
                    }
                    for id in keys.payments {
                        self.remove(payment_key(id))?;
                    }
                }
                None
            }
            Operation::Clear(table) => {
                for record in self.records(table)? {
                    self.remove(record.key())?;
                }
                None
            }
        };
        Ok(record.map_or(Reply::Unit, |r| Reply::Record(Box::new(r))))
    }
}
