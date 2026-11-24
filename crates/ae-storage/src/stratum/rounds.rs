// Original aestratum deferred keyblock callbacks over a durable mailbox.
// This prepares reward/payment accounting; it never signs or broadcasts funds.
use super::*;
use ae_chain::{block::Block, repository as repo};
use ae_codec::headers::{Header, HeaderMode, KeyHeader};
use ae_consensus::target::Target;
use ae_stratum::rewards::{self, Map};
use num_bigint::Sign;
use std::collections::{BTreeSet, VecDeque};
mod codec;
const META: &[u8] = b"\0stratum-rounds";
const MAX_PENDING: usize = 4096;
const MAX_META: usize = 8 << 20;
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub pending: usize,
    pub events: usize,
    pub bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            pending: 256,
            events: 128,
            bytes: 1 << 20,
        }
    }
}
impl Limits {
    pub(super) fn maximum() -> Self {
        Self {
            pending: MAX_PENDING,
            events: 65536,
            bytes: MAX_META,
        }
    }
    fn validate(self) -> Result<(), Failure> {
        if !(1..=MAX_PENDING).contains(&self.pending)
            || !(1..=65536).contains(&self.events)
            || !(1024..=MAX_META).contains(&self.bytes)
        {
            return Err(Error::ResourceLimit.into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub pool: Map,
    pub percent: Number,
    pub last_n: usize,
    pub delay: u64,
}
impl Config {
    pub fn from_percentages(
        weights: Map,
        last_n: usize,
        delay: u64,
        limits: rewards::Limits,
    ) -> Result<Self, Failure> {
        let percent = rewards::sum(&weights, limits).map_err(core::Error::from)?;
        let pool = rewards::relative(&weights, &percent, limits).map_err(core::Error::from)?;
        let config = Self {
            pool,
            percent,
            last_n,
            delay,
        };
        config.validate(limits)?;
        Ok(config)
    }
    fn validate(&self, limits: rewards::Limits) -> Result<(), Failure> {
        rewards::check_map(&self.pool, limits).map_err(core::Error::from)?;
        rewards::check_number(&self.percent).map_err(core::Error::from)?;
        if self.last_n > MAX_RECORDS {
            return Err(Error::ResourceLimit.into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Job {
    Top {
        revision: u64,
        header: Box<KeyHeader>,
    },
    Round {
        revision: u64,
    },
    // A round was committed. Its computation must finish before another
    // mailbox callback or accepted share can pass this callback.
    Compute {
        revision: u64,
        height: u64,
        round: u64,
    },
}
impl Job {
    fn revision(&self) -> u64 {
        match self {
            Self::Top { revision, .. }
            | Self::Round { revision }
            | Self::Compute { revision, .. } => *revision,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Top,
    Delay,
    Existing,
    NotOurShare,
    NoRange,
    Reward {
        hash: Hash256,
        amount: BigInt,
        payments: usize,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    pub sequence: u64,
    pub event: u64,
    pub observed: u64,
    pub height: u64,
    pub round: Option<u64>,
    pub outcome: Outcome,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    pub config: Config,
    pub cursor: u64,
    pub header: KeyHeader,
    // Native init captures protocol; top_changed only changes height/balance.
    pub protocol: u32,
    pub next: u64,
    pub pending: VecDeque<Job>,
    pub last: Option<Receipt>,
}
impl Info {
    pub fn height(&self) -> u64 {
        self.header.common.height
    }
    pub fn computing(&self) -> bool {
        matches!(self.pending.front(), Some(Job::Compute { .. }))
    }
    fn validate(&self, limits: Limits, accounting: core::Limits) -> Result<(), Failure> {
        limits.validate()?;
        self.config.validate(accounting.maps)?;
        Header::Key(self.header.clone())
            .encode()
            .map_err(|_| Error::Corrupt("Stratum round header"))?;
        if self.next == 0 || self.pending.len() > limits.pending {
            return Err(Error::ResourceLimit.into());
        }
        if self.last.as_ref().map_or(self.next != 1, |r| {
            r.sequence.checked_add(1) != Some(self.next)
        }) {
            return Err(Error::Corrupt("Stratum round sequence").into());
        }
        if let Some(receipt) = &self.last {
            if receipt.event == 0
                || receipt.event > self.cursor
                || receipt.observed < receipt.event
                || matches!(receipt.outcome, Outcome::Top) != receipt.round.is_none()
            {
                return Err(Error::Corrupt("Stratum round receipt binding").into());
            }
            if let Outcome::Reward {
                amount, payments, ..
            } = &receipt.outcome
                && (amount.sign() == Sign::Minus
                    || amount.bits() > 16384
                    || *payments > MAX_RECORDS)
            {
                return Err(Error::Corrupt("Stratum reward receipt values").into());
            }
        }
        let mut revisions = BTreeSet::new();
        for (index, job) in self.pending.iter().enumerate() {
            if job.revision() == 0
                || job.revision() > self.cursor
                || !revisions.insert(job.revision())
            {
                return Err(Error::Corrupt("Stratum round source revision").into());
            }
            match job {
                Job::Top { header, .. } => {
                    Header::Key(header.as_ref().clone())
                        .encode()
                        .map_err(|_| Error::Corrupt("Stratum queued header"))?;
                }
                Job::Compute { height, .. } if index != 0 || *height != self.height() => {
                    return Err(Error::Corrupt("Stratum round computation order").into());
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn complete(&mut self, receipt: Receipt) -> Result<(), Failure> {
        if receipt.sequence != self.next {
            return Err(Error::Conflict.into());
        }
        self.next = self.next.checked_add(1).ok_or(Error::ResourceLimit)?;
        self.last = Some(receipt);
        Ok(())
    }
}
impl Stored<'_> {
    pub fn rounds(&self, limits: Limits) -> Result<Option<Info>, Failure> {
        limits.validate()?;
        let info = self
            .source
            .snapshot
            .get_pinned(META)
            .map_err(backend)?
            .map(|raw| Info::decode(&raw, limits, self.limits.transaction))
            .transpose()?;
        if let Some(info) = &info {
            let raw = self
                .source
                .snapshot
                .get(repo::STATUS)
                .map_err(backend)?
                .ok_or(Error::Corrupt("Stratum round chain status"))?;
            if info.cursor > repo::Status::decode(&raw)?.revision {
                return Err(Error::Corrupt("Stratum round future cursor").into());
            }
            for job in &info.pending {
                let raw = self
                    .source
                    .snapshot
                    .get(repo::key(repo::EVENTS, job.revision().to_be_bytes()))
                    .map_err(backend)?
                    .ok_or(Error::Corrupt("Stratum round source event missing"))?;
                let event = repo::Change::decode(&raw)?;
                if event.revision != job.revision()
                    || event.inserted.is_none()
                    || event.old_tip.is_none()
                    || event.old_tip == Some(event.new_tip)
                {
                    return Err(Error::Corrupt("Stratum round event binding").into());
                }
                if let Job::Top { header, .. } = job
                    && Header::Key(header.as_ref().clone())
                        .hash()
                        .map_err(|_| Error::Corrupt("Stratum round hash"))?
                        != event.new_tip
                {
                    return Err(Error::Corrupt("Stratum round queued header binding").into());
                }
                if let Job::Compute { round, .. } = job {
                    let key = core::key(core::Table::Rounds, round.to_be_bytes());
                    let raw = core::Read::get(self, &key)?
                        .ok_or(Error::Corrupt("Stratum pending round missing"))?;
                    if core::Record::decode(&key, &raw, self.limits.transaction)?
                        != core::Record::Round(*round)
                    {
                        return Err(Error::Corrupt("Stratum pending round binding").into());
                    }
                }
            }
        }
        Ok(info)
    }
}
fn status(stored: &Stored<'_>) -> Result<(Vec<u8>, Option<Vec<u8>>), Failure> {
    Ok((
        repo::STATUS.to_vec(),
        stored.source.snapshot.get(repo::STATUS).map_err(backend)?,
    ))
}
fn view<'a>(
    stored: &Stored<'a>,
    config: repo::ReadConfig,
) -> Result<repo::View<Snapshot<'a>>, Failure> {
    Ok(repo::View::load_with_config(stored.source.clone(), config)?)
}
fn top_key(view: &repo::View<Snapshot<'_>>) -> Result<KeyHeader, Failure> {
    if view.identity().consensus != ae_protocol::ConsensusType::Pow {
        return Err(Error::Identity.into());
    }
    let top = view.status().tip.ok_or(Error::Uninitialized)?;
    match view.require_block(top)? {
        Block::Key(header) => Ok(header),
        Block::Micro(block) => {
            match view.require_block(Hash256(block.header.common.previous_key_hash))? {
                Block::Key(header) => Ok(header),
                _ => Err(Error::Corrupt("Stratum top key").into()),
            }
        }
    }
}
fn extra(stored: &Stored<'_>, info: &Info, limits: Limits) -> Result<Extra, Failure> {
    Ok(Extra {
        key: META.to_vec(),
        before: stored.source.snapshot.get(META).map_err(backend)?,
        after: Some(info.encode(limits, stored.limits.transaction)?),
    })
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Stage {
    Initialize,
    Capture,
    Round,
    Finish,
}
impl Writer {
    pub fn begin_rounds(&mut self, config: Config, limits: Limits) -> Result<Info, Failure> {
        self.begin_rounds_with(config, limits, |_, db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        })
    }
    fn begin_rounds_with(
        &mut self,
        config: Config,
        limits: Limits,
        mut write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Info, Failure> {
        let version = self.version()?;
        let stored = self.db.stratum(self.limits)?;
        if let Some(info) = stored.rounds(limits)? {
            if info.config != config {
                return Err(Error::Identity.into());
            }
            return Ok(info);
        }
        let view = view(&stored, self.db.config.clone())?;
        let header = top_key(&view)?;
        let info = Info {
            config,
            cursor: view.status().revision,
            protocol: header.common.version,
            header,
            next: 1,
            pending: VecDeque::new(),
            last: None,
        };
        let commands = if stored.info.counts[core::Table::Rounds as usize - 1] == 0 {
            vec![Command::Round]
        } else {
            vec![]
        };
        let changes = vec![extra(&stored, &info, limits)?];
        let checks = vec![status(&stored)?];
        drop(view);
        drop(stored);
        self.commit_checked(version, commands, changes, checks, |db, b| {
            write(Stage::Initialize, db, b)
        })?;
        Ok(info)
    }
    pub fn capture_rounds(&mut self, limits: Limits) -> Result<Info, Failure> {
        self.capture_rounds_with(limits, |_, db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        })
    }
    fn capture_rounds_with(
        &mut self,
        limits: Limits,
        mut write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Info, Failure> {
        let version = self.version()?;
        let stored = self.db.stratum(self.limits)?;
        let mut info = stored.rounds(limits)?.ok_or(Error::Uninitialized)?;
        let original = info.clone();
        let view = view(&stored, self.db.config.clone())?;
        if info.cursor > view.status().revision {
            return Err(Error::Corrupt("Stratum round cursor").into());
        }
        for _ in 0..limits.events {
            if info.cursor == view.status().revision {
                break;
            }
            let revision = info.cursor.checked_add(1).ok_or(Error::ResourceLimit)?;
            let change = view
                .event(revision)?
                .ok_or(Error::Corrupt("Stratum round event"))?;
            // Bootstrap and administrative rollback are not native top_changed
            // insertion notifications. Side branches likewise send no event.
            if change.inserted.is_some()
                && change.old_tip.is_some()
                && change.old_tip != Some(change.new_tip)
                && let Block::Key(header) = view.require_block(change.new_tip)?
            {
                if info.pending.len() == limits.pending {
                    break;
                }
                info.pending.push_back(Job::Top {
                    revision,
                    header: Box::new(header),
                });
            }
            info.cursor = revision;
        }
        let changes = if info == original {
            vec![]
        } else {
            vec![extra(&stored, &info, limits)?]
        };
        let checks = vec![status(&stored)?];
        drop(view);
        drop(stored);
        self.commit_checked(version, vec![], changes, checks, |db, b| {
            write(Stage::Capture, db, b)
        })?;
        Ok(info)
    }
    pub fn step_rounds(&mut self, sequence: u64, limits: Limits) -> Result<Receipt, Failure> {
        self.step_rounds_with(sequence, limits, |_, db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)
        })
    }
    pub(super) fn step_rounds_with(
        &mut self,
        sequence: u64,
        limits: Limits,
        mut write: impl FnMut(Stage, &Database, WriteBatch) -> Result<(), Error>,
    ) -> Result<Receipt, Failure> {
        loop {
            let version = self.version()?;
            let stored = self.db.stratum(self.limits)?;
            let mut info = stored.rounds(limits)?.ok_or(Error::Uninitialized)?;
            if let Some(receipt) = info.last.as_ref().filter(|r| r.sequence == sequence) {
                return Ok(receipt.clone());
            }
            if sequence != info.next {
                return Err(Error::Conflict.into());
            }
            let job = info.pending.pop_front().ok_or(Error::Conflict)?;
            let view = view(&stored, self.db.config.clone())?;
            let mut commands = vec![];
            let receipt = match job {
                Job::Top { revision, header } => {
                    info.header = *header;
                    info.pending.push_back(Job::Round { revision });
                    Receipt {
                        sequence,
                        event: revision,
                        observed: view.status().revision,
                        height: info.height(),
                        round: None,
                        outcome: Outcome::Top,
                    }
                }
                Job::Round { revision } => {
                    let round = stored.info.next_key.ok_or(Error::ResourceLimit)?;
                    info.pending.push_front(Job::Compute {
                        revision,
                        height: info.height(),
                        round,
                    });
                    let changes = vec![extra(&stored, &info, limits)?];
                    drop(view);
                    drop(stored);
                    self.commit_checked(
                        version,
                        vec![Command::Round],
                        changes,
                        vec![],
                        |db, b| write(Stage::Round, db, b),
                    )?;
                    continue;
                }
                Job::Compute {
                    revision,
                    height,
                    round,
                } => {
                    let reward_height = height.checked_sub(info.config.delay).filter(|h| *h > 0);
                    let outcome = match reward_height {
                        None => Outcome::Delay,
                        Some(h) => compute(&stored, &view, h, &info.config, &mut commands)?,
                    };
                    Receipt {
                        sequence,
                        event: revision,
                        observed: view.status().revision,
                        height,
                        round: Some(round),
                        outcome,
                    }
                }
            };
            info.complete(receipt.clone())?;
            let mut changes = vec![extra(&stored, &info, limits)?];
            if receipt.outcome == Outcome::Top
                && let Some(owner) = self.payout_owner
            {
                changes.push(super::payouts::round_context(
                    &stored,
                    &view,
                    owner,
                    receipt.height,
                )?);
            }
            let checks = vec![status(&stored)?];
            drop(view);
            drop(stored);
            self.commit_checked(version, commands, changes, checks, |db, b| {
                write(Stage::Finish, db, b)
            })?;
            return Ok(receipt);
        }
    }
}
fn compute(
    stored: &Stored<'_>,
    view: &repo::View<Snapshot<'_>>,
    height: u64,
    config: &Config,
    commands: &mut Vec<Command>,
) -> Result<Outcome, Failure> {
    if stored.query(|tx| tx.reward(height))?.is_some() {
        return Ok(Outcome::Existing);
    }
    let hash = view.canonical_key(height)?.ok_or(Error::Corrupt(
        "Stratum reward canonical height unavailable",
    ))?;
    let Block::Key(header) = view.require_block(hash)? else {
        return Err(Error::Corrupt("Stratum reward key header").into());
    };
    if header.common.height != height {
        return Err(Error::Corrupt("Stratum reward height binding").into());
    }
    let Some(share) = stored.query(|tx| tx.hash(hash))? else {
        return Ok(Outcome::NotOurShare);
    };
    let fees = view.require_record(hash)?.fees;
    let coinbase = ae_protocol::coinbase::mining_reward(&height.into());
    let amount = BigInt::from_bytes_be(Sign::Plus, fees.add(&coinbase).as_bytes());
    let target = BigInt::from_bytes_be(Sign::Plus, Target(header.target).value().as_bytes());
    let (miners, round_key) =
        match stored.query(|tx| tx.miner_rewards(share, &target, config.last_n)) {
            Err(Failure::Accounting(core::Error::NoRange)) => return Ok(Outcome::NoRange),
            result => result?,
        };
    let reward = core::Reward {
        height,
        hash,
        pool: core::Shares::Relative(config.pool.clone()),
        miners: core::Shares::Relative(miners.clone()),
        amount: amount.clone(),
        round_key,
    };
    let payments = rewards::payments(
        height,
        &amount,
        &config.pool,
        &miners,
        &config.percent,
        stored.limits.transaction.maps,
    )
    .map_err(core::Error::from)?
    .len();
    commands.push(Command::StorePayments {
        reward: reward.clone(),
        percent: config.percent.clone(),
    });
    commands.push(Command::Reward(core::Reward {
        pool: core::Shares::Transformed,
        miners: core::Shares::Transformed,
        ..reward
    }));
    Ok(Outcome::Reward {
        hash,
        amount,
        payments,
    })
}
#[cfg(test)]
mod tests;
