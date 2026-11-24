// Durable pool work and admission intents. Accounting precedes conductor
// eligibility exactly as in aestratum. Canonical insertion remains external.
use super::*;
use ae_chain::repository as repo;
use ae_stratum::{
    candidate::{Candidate, Sealed},
    ingress::{Id, Input},
};
use ingress::Receipt;
use std::sync::Weak;
mod codec;
const RECORDS: u8 = 24;
const INDEX: u8 = 25;
const META: &[u8] = b"\0stratum-mining";
const RECORD_BYTES: usize = 24576;
const CACHE_TAG: &[u8] = b"ae-stratum-candidate-v1";
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub active: usize,
    pub records: usize,
    pub bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            active: 256,
            records: 100_000,
            bytes: 256 << 20,
        }
    }
}
impl Limits {
    pub(super) fn maximum() -> Self {
        Self {
            active: 4096,
            records: MAX_RECORDS,
            bytes: MAX_BYTES,
        }
    }
    fn validate(self) -> Result<(), Failure> {
        if !(1..=4096).contains(&self.active)
            || !(1..=MAX_RECORDS).contains(&self.records)
            || !(1..=MAX_BYTES).contains(&self.bytes)
        {
            return Err(Error::ResourceLimit.into());
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Ready,
    Submitting,
    Done,
}
impl Phase {
    fn byte(self) -> u8 {
        match self {
            Self::Ready => 0,
            Self::Submitting => 1,
            Self::Done => 2,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accepted {
    Committed(u64),
    Present,
    Observed(u64),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    Invalid,
    Stale,
    Policy,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    MissingCandidate,
    Inactive,
    Accepted(Accepted),
    Rejected { reason: Rejection, detail: String },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Ready,
    Submitting,
    Done(Outcome),
}
impl State {
    pub fn phase(&self) -> Phase {
        match self {
            Self::Ready => Phase::Ready,
            Self::Submitting => Phase::Submitting,
            Self::Done(_) => Phase::Done,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub receipt: Receipt,
    pub epoch: u64,
    pub candidate: Option<Candidate>,
    // Native Solution changes the CURRENT hash-indexed share. This is not
    // necessarily the source receipt's share for a delayed legacy solution.
    pub accounted_key: Option<u64>,
    pub attempts: u64,
    pub state: State,
}
impl Record {
    pub fn encoded_len(&self) -> Result<usize, Failure> {
        Ok(self.encode()?.len())
    }
    pub fn solution(&self) -> Result<Option<Sealed>, Failure> {
        self.candidate
            .as_ref()
            .map(|c| c.solution(&self.receipt.input).map_err(Into::into))
            .transpose()
    }
    fn key(&self) -> Vec<u8> {
        key(self.receipt.id)
    }
    fn index(&self) -> Vec<u8> {
        index(self.state.phase(), self.receipt.id)
    }
    fn weight(&self) -> Result<u64, Failure> {
        Ok((17 + 18 + self.encode()?.len()) as u64)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publication {
    pub sequence: u64,
    pub candidate: Candidate,
    pub date: core::Date,
    pub published: bool,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Info {
    pub epoch: u64,
    pub last: Option<Publication>,
    pub active: Vec<Candidate>,
    pub counts: [u64; 3],
    pub bytes: u64,
}
impl Info {
    fn check(&self, limits: Limits) -> Result<(), Failure> {
        limits.validate()?;
        let count = self
            .counts
            .iter()
            .try_fold(0u64, |n, v| n.checked_add(*v))
            .ok_or(Error::ResourceLimit)?;
        if count > limits.records as u64
            || self.bytes > limits.bytes as u64
            || self.active.len() > limits.active
        {
            return Err(Error::ResourceLimit.into());
        }
        if (count == 0) != (self.bytes == 0) {
            return Err(Error::Corrupt("Stratum mining totals").into());
        }
        Ok(())
    }
    fn sequence(&self) -> u64 {
        self.last.as_ref().map_or(0, |p| p.sequence)
    }
}
fn key(id: Id) -> Vec<u8> {
    let mut k = vec![RECORDS];
    k.extend_from_slice(&id.epoch.to_be_bytes());
    k.extend_from_slice(&id.sequence.to_be_bytes());
    k
}
fn index(phase: Phase, id: Id) -> Vec<u8> {
    let mut k = vec![INDEX, phase.byte()];
    k.extend_from_slice(&id.epoch.to_be_bytes());
    k.extend_from_slice(&id.sequence.to_be_bytes());
    k
}
fn id(bytes: &[u8]) -> Result<Id, Failure> {
    if bytes.len() != 16 {
        return Err(Error::Corrupt("Stratum mining identity").into());
    }
    let id = Id {
        epoch: u64::from_be_bytes(bytes[..8].try_into().expect("fixed epoch")),
        sequence: u64::from_be_bytes(bytes[8..].try_into().expect("fixed sequence")),
    };
    id.validate()?;
    Ok(id)
}
pub struct Journal<'a> {
    source: Snapshot<'a>,
    accounting: super::Info,
    inbox: ingress::Inbox<'a>,
    info: Info,
    limits: Limits,
}
impl<'a> Stored<'a> {
    pub fn mining(&self, limits: Limits) -> Result<Journal<'a>, Failure> {
        limits.validate()?;
        let raw = self.source.snapshot.get_pinned(META).map_err(backend)?;
        let info = Info::decode(raw.as_deref())?;
        info.check(limits)?;
        Ok(Journal {
            source: self.source.clone(),
            accounting: self.info,
            inbox: self.ingress(ingress::Limits::maximum())?,
            info,
            limits,
        })
    }
}
impl Journal<'_> {
    pub fn info(&self) -> &Info {
        &self.info
    }
    fn valid(&self, record: &Record) -> Result<(), Failure> {
        if record.epoch > self.info.epoch {
            return Err(Error::Corrupt("Stratum future mining epoch").into());
        }
        self.inbox.valid(&record.receipt)?;
        if self.inbox.get(record.receipt.id)?.as_ref() != Some(&record.receipt) {
            return Err(Error::Corrupt("Stratum mining source receipt").into());
        }
        if let Some(k) = record.accounted_key {
            self.accounting.allocated(&core::Record::Round(k))?;
        }
        Ok(())
    }
    pub fn get(&self, id: Id) -> Result<Option<Record>, Failure> {
        id.validate()?;
        self.source
            .snapshot
            .get_pinned(key(id))
            .map_err(backend)?
            .map(|raw| {
                let r = Record::decode(&raw)?;
                if r.receipt.id != id {
                    return Err(Error::Corrupt("Stratum mining record key").into());
                }
                self.valid(&r)?;
                Ok(r)
            })
            .transpose()
    }
    pub fn page(
        &self,
        phase: Phase,
        after: Option<Id>,
        count: usize,
    ) -> Result<(Vec<Record>, bool), Failure> {
        if count == 0 || count > self.limits.records {
            return Err(Error::ResourceLimit.into());
        }
        let mut it = self.source.snapshot.raw_iterator();
        if let Some(after) = after {
            after.validate()?;
            let k = index(phase, after);
            it.seek(&k);
            if it.key() == Some(k.as_slice()) {
                it.next();
            }
        } else {
            it.seek([INDEX, phase.byte()]);
        }
        let mut records = vec![];
        let mut bytes = 0u64;
        let mut more = false;
        while let Some(k) = it.key() {
            if !k.starts_with(&[INDEX, phase.byte()]) {
                break;
            }
            if records.len() == count {
                more = true;
                break;
            }
            if k.len() != 18 || it.value() != Some([].as_slice()) {
                return Err(Error::Corrupt("Stratum mining index").into());
            }
            let r = self
                .get(id(&k[2..])?)?
                .ok_or(Error::Corrupt("Stratum mining index orphan"))?;
            if r.state.phase() != phase {
                return Err(Error::Corrupt("Stratum mining index phase").into());
            }
            bytes = bytes.checked_add(r.weight()?).ok_or(Error::ResourceLimit)?;
            if bytes > self.limits.bytes as u64 {
                return Err(Error::ResourceLimit.into());
            }
            records.push(r);
            it.next();
        }
        it.status().map_err(backend)?;
        Ok((records, more))
    }
    pub fn verify(&self) -> Result<(), Failure> {
        self.info.check(self.limits)?;
        let mut counts = [0u64; 3];
        let mut bytes = 0u64;
        let mut it = self.source.snapshot.raw_iterator();
        it.seek([RECORDS]);
        while let Some(k) = it.key() {
            if !k.starts_with(&[RECORDS]) {
                break;
            }
            if k.len() != 17 {
                return Err(Error::Corrupt("Stratum mining key length").into());
            }
            let r = self
                .get(id(&k[1..])?)?
                .ok_or(Error::Corrupt("Stratum mining record missing"))?;
            let raw = it.value().ok_or(Error::Corrupt("Stratum mining value"))?;
            if r.encode()? != raw
                || self
                    .source
                    .snapshot
                    .get_pinned(r.index())
                    .map_err(backend)?
                    .as_deref()
                    != Some([].as_slice())
            {
                return Err(Error::Corrupt("Stratum mining indexed record").into());
            }
            counts[r.state.phase().byte() as usize] += 1;
            bytes = bytes.checked_add(r.weight()?).ok_or(Error::ResourceLimit)?;
            if counts.iter().sum::<u64>() > self.limits.records as u64
                || bytes > self.limits.bytes as u64
            {
                return Err(Error::ResourceLimit.into());
            }
            it.next();
        }
        it.status().map_err(backend)?;
        if counts != self.info.counts || bytes != self.info.bytes {
            return Err(Error::Corrupt("Stratum mining stored totals").into());
        }
        let mut indexed = [0u64; 3];
        it.seek([INDEX]);
        while let Some(k) = it.key() {
            if !k.starts_with(&[INDEX]) {
                break;
            }
            if k.len() != 18 || k[1] > 2 || it.value() != Some([].as_slice()) {
                return Err(Error::Corrupt("Stratum mining index encoding").into());
            }
            let r = self
                .get(id(&k[2..])?)?
                .ok_or(Error::Corrupt("Stratum mining orphan index"))?;
            if r.index() != k {
                return Err(Error::Corrupt("Stratum mining wrong index").into());
            }
            indexed[k[1] as usize] += 1;
            if indexed[k[1] as usize] > counts[k[1] as usize] {
                return Err(Error::Corrupt("Stratum mining excess index").into());
            }
            it.next();
        }
        it.status().map_err(backend)?;
        if indexed != counts {
            return Err(Error::Corrupt("Stratum mining index totals").into());
        }
        Ok(())
    }
    fn extra(&self, after: &Info) -> Result<Extra, Failure> {
        after.check(self.limits)?;
        Ok(Extra {
            key: META.to_vec(),
            before: self
                .source
                .snapshot
                .get_pinned(META)
                .map_err(backend)?
                .map(|v| v.to_vec()),
            after: Some(after.encode()?),
        })
    }
    fn changes(
        &self,
        before: Option<&Record>,
        after: Option<&Record>,
        mut info: Info,
    ) -> Result<Vec<Extra>, Failure> {
        let mut extras = vec![];
        if let Some(r) = before {
            info.counts[r.state.phase().byte() as usize] = info.counts
                [r.state.phase().byte() as usize]
                .checked_sub(1)
                .ok_or(Error::Corrupt("Stratum mining count"))?;
            info.bytes = info
                .bytes
                .checked_sub(r.weight()?)
                .ok_or(Error::Corrupt("Stratum mining bytes"))?;
        }
        if let Some(r) = after {
            info.counts[r.state.phase().byte() as usize] = info.counts
                [r.state.phase().byte() as usize]
                .checked_add(1)
                .ok_or(Error::ResourceLimit)?;
            info.bytes = info
                .bytes
                .checked_add(r.weight()?)
                .ok_or(Error::ResourceLimit)?;
        }
        let r = after
            .or(before)
            .ok_or(Error::Corrupt("Stratum mining empty change"))?;
        extras.push(Extra {
            key: r.key(),
            before: before.map(Record::encode).transpose()?,
            after: after.map(Record::encode).transpose()?,
        });
        match (before, after) {
            (Some(a), Some(b)) if a.index() == b.index() => {}
            (a, b) => {
                if let Some(a) = a {
                    extras.push(Extra {
                        key: a.index(),
                        before: Some(vec![]),
                        after: None,
                    });
                }
                if let Some(b) = b {
                    extras.push(Extra {
                        key: b.index(),
                        before: None,
                        after: Some(vec![]),
                    });
                }
            }
        }
        extras.push(self.extra(&info)?);
        Ok(extras)
    }
}
fn cached(stored: &Stored<'_>, hash: Hash256) -> Result<Option<Candidate>, Failure> {
    let Some(c) = stored.query(|tx| tx.candidate(hash))? else {
        return Ok(None);
    };
    // Older generic accounting rows are not silently reinterpreted as work.
    if c.record != CACHE_TAG {
        return Err(Error::Corrupt("Stratum unbound candidate cache").into());
    }
    let candidate = Candidate::decode(&c.header)?;
    if candidate.hash() != c.hash {
        return Err(Error::Corrupt("Stratum candidate cache hash").into());
    }
    Ok(Some(candidate))
}
pub(super) struct Prepared {
    pub commands: Vec<Command>,
    pub extra: Vec<Extra>,
    pub record: Record,
}
pub(super) fn prepare(
    stored: &Stored<'_>,
    receipt: &Receipt,
    new_share: Option<u64>,
    limits: Limits,
) -> Result<Prepared, Failure> {
    let journal = stored.mining(limits)?;
    if journal.info.epoch == 0 || receipt.input.solution.is_none() {
        return Err(Error::Conflict.into());
    }
    let mut info = journal.info.clone();
    let candidate = cached(stored, receipt.input.hash)?;
    let mut commands = vec![];
    let (state, accounted_key) = if let Some(c) = &candidate {
        let sealed = c.solution(&receipt.input)?;
        let key = match new_share {
            Some(k) => k,
            None => stored
                .query(|tx| tx.hash(receipt.input.hash))?
                .ok_or(core::Error::MissingHash)?,
        };
        commands.push(Command::Solution {
            candidate: c.hash(),
            block: sealed.hash(),
        });
        let found = info
            .active
            .iter()
            .any(|a| a.sealing_bytes() == c.sealing_bytes());
        if found {
            info.active.clear();
        }
        (
            if found {
                State::Ready
            } else {
                State::Done(Outcome::Inactive)
            },
            Some(key),
        )
    } else {
        (State::Done(Outcome::MissingCandidate), None)
    };
    let record = Record {
        receipt: receipt.clone(),
        epoch: info.epoch,
        candidate,
        accounted_key,
        attempts: 0,
        state,
    };
    let extra = journal.changes(None, Some(&record), info)?;
    Ok(Prepared {
        commands,
        extra,
        record,
    })
}
impl Writer {
    pub(super) fn check_mining_epoch(
        &self,
        epoch: &Epoch,
        journal: &Journal<'_>,
    ) -> Result<(), Failure> {
        if epoch.value != journal.info.epoch
            || epoch.source != self.db.source_id
            || !epoch.owner.ptr_eq(&Arc::downgrade(&self._lease))
        {
            return Err(Error::Conflict.into());
        }
        Ok(())
    }
    pub fn begin_mining(&mut self, limits: Limits) -> Result<Epoch, Failure> {
        self.begin_mining_with(limits, |db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        })
    }
    fn begin_mining_with(
        &mut self,
        limits: Limits,
        write: impl FnOnce(&Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Epoch, Failure> {
        if self.db.identity.consensus != ae_protocol::ConsensusType::Pow {
            return Err(Error::Identity.into());
        }
        let version = self.version()?;
        let stored = self.db.stratum(self.limits)?;
        let journal = stored.mining(limits)?;
        let mut info = journal.info.clone();
        info.epoch = info.epoch.checked_add(1).ok_or(Error::ResourceLimit)?;
        info.active.clear();
        info.last = None;
        let epoch = Epoch {
            value: info.epoch,
            source: self.db.source_id,
            owner: Arc::downgrade(&self._lease),
        };
        let extra = journal.extra(&info)?;
        drop(journal);
        drop(stored);
        self.commit_extra(version, vec![], vec![extra], write)?;
        Ok(epoch)
    }
    pub fn publish_work(
        &mut self,
        epoch: &Epoch,
        sequence: u64,
        candidate: Candidate,
        date: core::Date,
        limits: Limits,
    ) -> Result<Publication, Failure> {
        self.publish_with(epoch, sequence, candidate, date, limits, |db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        })
    }
    fn publish_with(
        &mut self,
        epoch: &Epoch,
        sequence: u64,
        candidate: Candidate,
        date: core::Date,
        limits: Limits,
        write: impl FnOnce(&Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Publication, Failure> {
        date.validate()?;
        let version = self.version()?;
        let stored = self.db.stratum(self.limits)?;
        let journal = stored.mining(limits)?;
        self.check_mining_epoch(epoch, &journal)?;
        if let Some(last) = &journal.info.last
            && last.sequence == sequence
        {
            return if last.candidate == candidate && last.date == date {
                Ok(last.clone())
            } else {
                Err(Error::Conflict.into())
            };
        }
        if sequence == 0 || journal.info.sequence().checked_add(1) != Some(sequence) {
            return Err(Error::Conflict.into());
        }
        let view = repo::View::load_with_config(stored.source.clone(), self.db.config.clone())?;
        let publication = Publication {
            sequence,
            published: view.status().tip == Some(candidate.parent()),
            candidate,
            date,
        };
        let mut info = journal.info.clone();
        let mut commands = vec![];
        if publication.published {
            if info.active.len() == limits.active {
                return Err(Error::ResourceLimit.into());
            }
            info.active.insert(0, publication.candidate.clone());
            commands.push(Command::Candidate(core::Candidate {
                hash: publication.candidate.hash(),
                header: publication.candidate.sealing_bytes().to_vec(),
                record: CACHE_TAG.to_vec(),
                date,
            }));
        }
        info.last = Some(publication.clone());
        let extra = journal.extra(&info)?;
        let checks = vec![status_check(&stored)?];
        drop(view);
        drop(journal);
        drop(stored);
        self.commit_checked(version, commands, vec![extra], checks, write)?;
        Ok(publication)
    }
    pub fn accept_mining(
        &mut self,
        ingress: &ingress::Epoch,
        sequence: u64,
        input: Input,
        ingress_limits: ingress::Limits,
        mining: &Epoch,
        limits: Limits,
    ) -> Result<Receipt, Failure> {
        self.accept_maybe_mining(
            ingress,
            sequence,
            input,
            ingress_limits,
            Some((mining, limits)),
            |db, b| db.db.write_opt(b, &write_options()).map_err(backend),
        )
    }
    pub fn prepare_solution(
        &mut self,
        epoch: &Epoch,
        id: Id,
        limits: Limits,
    ) -> Result<Record, Failure> {
        let version = self.version()?;
        let stored = self.db.stratum(self.limits)?;
        let journal = stored.mining(limits)?;
        self.check_mining_epoch(epoch, &journal)?;
        if let Some(record) = journal.get(id)? {
            return Ok(record);
        }
        let receipt = journal.inbox.get(id)?.ok_or(Error::Conflict)?;
        let prepared = prepare(&stored, &receipt, None, limits)?;
        drop(journal);
        drop(stored);
        self.commit_extra(version, prepared.commands, prepared.extra, |db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        })?;
        Ok(prepared.record)
    }
    fn change(
        &mut self,
        expected: &Record,
        after: Option<&Record>,
        limits: Limits,
        checks: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        extra: Vec<Extra>,
        write: impl FnOnce(&Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<(), Failure> {
        let version = self.version()?;
        let stored = self.db.stratum(self.limits)?;
        let journal = stored.mining(limits)?;
        let current = journal.get(expected.receipt.id)?;
        if after.is_some() && current.as_ref() == after {
            return Ok(());
        }
        if current.as_ref() != Some(expected) {
            return Err(Error::Conflict.into());
        }
        let mut changes = journal.changes(Some(expected), after, journal.info.clone())?;
        changes.extend(extra);
        drop(journal);
        drop(stored);
        self.commit_checked(version, vec![], changes, checks, write)?;
        Ok(())
    }
    pub fn begin_submission(
        &mut self,
        expected: &Record,
        limits: Limits,
    ) -> Result<Record, Failure> {
        if expected.state != State::Ready {
            return Err(Error::Conflict.into());
        }
        let mut after = expected.clone();
        after.attempts = after.attempts.checked_add(1).ok_or(Error::ResourceLimit)?;
        after.state = State::Submitting;
        self.change(expected, Some(&after), limits, vec![], vec![], |db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        })?;
        Ok(after)
    }
    pub fn unaccepted_submission(
        &mut self,
        expected: &Record,
        limits: Limits,
    ) -> Result<Record, Failure> {
        if expected.state != State::Submitting {
            return Err(Error::Conflict.into());
        }
        let mut after = expected.clone();
        after.state = State::Ready;
        self.change(expected, Some(&after), limits, vec![], vec![], |db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        })?;
        Ok(after)
    }
    // The owner must first join every earlier admission using this journal.
    // Absence while an old worker can still commit is not a recovery fence.
    pub fn reconcile_settled_submission(
        &mut self,
        expected: &Record,
        limits: Limits,
    ) -> Result<Record, Failure> {
        if expected.state != State::Submitting {
            return Err(Error::Conflict.into());
        }
        let stored = self.db.stratum(self.limits)?;
        let observed = observe(&stored, &self.db.config, expected)?;
        let checks = vec![status_check(&stored)?];
        drop(stored);
        let mut after = expected.clone();
        after.state = observed.map_or(State::Ready, |revision| {
            State::Done(Outcome::Accepted(Accepted::Observed(revision)))
        });
        self.change(expected, Some(&after), limits, checks, vec![], |db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        })?;
        Ok(after)
    }
    pub fn finish_submission(
        &mut self,
        expected: &Record,
        outcome: Outcome,
        limits: Limits,
    ) -> Result<Record, Failure> {
        if expected.state != State::Submitting
            || matches!(outcome, Outcome::MissingCandidate | Outcome::Inactive)
        {
            return Err(Error::Conflict.into());
        }
        let mut after = expected.clone();
        after.state = State::Done(outcome.clone());
        // An acknowledged historical result remains replayable after rollback.
        // Only a NEW accepted result needs a current canonical observation.
        if self
            .db
            .stratum(self.limits)?
            .mining(limits)?
            .get(expected.receipt.id)?
            .as_ref()
            == Some(&after)
        {
            return Ok(after);
        }
        let mut checks = vec![];
        if let Outcome::Accepted(accepted) = outcome {
            let stored = self.db.stratum(self.limits)?;
            let revision = observe(&stored, &self.db.config, expected)?.ok_or(Error::Conflict)?;
            if matches!(accepted,Accepted::Committed(r)|Accepted::Observed(r) if r==0 || r>revision)
            {
                return Err(Error::Conflict.into());
            }
            checks.push(status_check(&stored)?);
        }
        self.change(expected, Some(&after), limits, checks, vec![], |db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        })?;
        Ok(after)
    }
    pub fn acknowledge_solution(
        &mut self,
        expected: &Record,
        limits: Limits,
    ) -> Result<(), Failure> {
        if !matches!(expected.state, State::Done(_)) {
            return Err(Error::Conflict.into());
        }
        let stored = self.db.stratum(self.limits)?;
        let journal = stored.mining(limits)?;
        if journal.get(expected.receipt.id)?.is_none()
            && journal.inbox.get(expected.receipt.id)?.is_none()
        {
            return Ok(());
        }
        drop(journal);
        let extras = stored
            .ingress(ingress::Limits::maximum())?
            .remove(&expected.receipt)?;
        drop(stored);
        self.change(expected, None, limits, vec![], extras, |db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        })
    }
}
fn status_check(stored: &Stored<'_>) -> Result<(Vec<u8>, Option<Vec<u8>>), Failure> {
    Ok((
        repo::STATUS.to_vec(),
        stored
            .source
            .snapshot
            .get_pinned(repo::STATUS)
            .map_err(backend)?
            .map(|b| b.to_vec()),
    ))
}
fn observe(
    stored: &Stored<'_>,
    config: &repo::ReadConfig,
    expected: &Record,
) -> Result<Option<u64>, Failure> {
    let solution = expected.solution()?.ok_or(Error::Conflict)?;
    let view = repo::View::load_with_config(stored.source.clone(), config.clone())?;
    match (view.block(solution.hash())?, view.record(solution.hash())?) {
        (None, None) => Ok(None),
        (Some(block), Some(record))
            if block
                .encode()
                .map_err(|_| Error::Corrupt("Stratum observed block encoding"))?
                == solution.bytes()
                && record.roots.state.hash().0 == solution.header().common.state_hash =>
        {
            Ok(Some(view.status().revision))
        }
        _ => Err(Error::Corrupt("Stratum observed block and state").into()),
    }
}
#[cfg(test)]
mod tests;
