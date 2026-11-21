// aec_sync_stats, aeternity fa2a6d7.
// Copyright (c) 2024, Aeternity Anstalt. ISC: repository LICENSE.
use super::Point;

pub const WINDOW: usize = 1000;
const LEVELS: [(u64, u64); 5] = [(1, 1000), (1000, 5), (5000, 2), (10000, 2), (20000, 5)];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Capacity,
    Overflow,
    UnknownTask,
    UndefinedEstimate,
    EmptyEstimate,
    ZeroDuration,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sample {
    pub height: u64,
    pub start_us: i128,
    pub end_us: i128,
    pub micro_blocks: u64,
    pub transactions: u64,
    pub work_us: Option<i128>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Aggregate {
    pub step: u64,
    pub limit: Option<u64>,
    pub entries: u64,
    pub generations: u64,
    pub micro_blocks: u64,
    pub transactions: u64,
    pub work_us: i128,
    pub start_us: Option<i128>,
    pub end_us: Option<i128>,
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rates {
    pub milliseconds_per_generation: f64,
    pub wall_seconds: f64,
    pub work_seconds: f64,
    pub efficiency_percent: f64,
}
impl Aggregate {
    fn empty(step: u64, limit: Option<u64>) -> Self {
        Self {
            step,
            limit,
            entries: 0,
            generations: 0,
            micro_blocks: 0,
            transactions: 0,
            work_us: 0,
            start_us: None,
            end_us: None,
        }
    }
    fn add(&mut self, sample: Sample, work_us: i128) -> Result<bool, Error> {
        self.entries = self.entries.checked_add(1).ok_or(Error::Overflow)?;
        self.generations = self
            .generations
            .checked_add(self.step)
            .ok_or(Error::Overflow)?;
        self.micro_blocks = self
            .micro_blocks
            .checked_add(sample.micro_blocks)
            .ok_or(Error::Overflow)?;
        self.transactions = self
            .transactions
            .checked_add(sample.transactions)
            .ok_or(Error::Overflow)?;
        self.work_us = self.work_us.checked_add(work_us).ok_or(Error::Overflow)?;
        self.start_us.get_or_insert(sample.start_us);
        self.end_us = Some(sample.end_us);
        Ok(self.limit == Some(self.entries))
    }
    pub fn rates(&self) -> Result<Option<Rates>, Error> {
        let (Some(start), Some(end)) = (self.start_us, self.end_us) else {
            return Ok(None);
        };
        let wall = end.checked_sub(start).ok_or(Error::Overflow)?;
        if wall == 0 || self.generations == 0 {
            return Err(Error::ZeroDuration);
        }
        let efficiency = self.work_us.checked_mul(100).ok_or(Error::Overflow)?;
        Ok(Some(Rates {
            milliseconds_per_generation: self.work_us as f64
                / (1000_u128 * u128::from(self.generations)) as f64,
            wall_seconds: wall as f64 / 1_000_000.0,
            work_seconds: self.work_us as f64 / 1_000_000.0,
            efficiency_percent: efficiency as f64 / wall as f64,
        }))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub id: u64,
    pub target: Point,
    pub total: Aggregate,
    pub levels: [Aggregate; 5],
    pub rollovers: [u64; 5],
    pub last_rollovers: [Option<Aggregate>; 5],
    // None and Some(0) preserve the native undefined/empty distinction.
    pub window_len: Option<usize>,
    pub newest: Option<Sample>,
    pub oldest: Option<Sample>,
    pub generations_per_minute: Result<f64, Error>,
    pub last_update_ms: Option<i128>,
    // A diagnostic failure never changes block admission or durable progress.
    pub failure: Option<Error>,
}
#[derive(Debug, Clone, Default)]
pub struct Status {
    pub tasks: Vec<Snapshot>,
    pub terminated: u64,
    pub last_terminated: Option<Snapshot>,
}
impl Status {
    pub fn estimate(&self, task: u64) -> Result<f64, Error> {
        self.tasks
            .iter()
            .find(|s| s.id == task)
            .ok_or(Error::UnknownTask)?
            .generations_per_minute
    }
}

#[derive(Debug, Clone)]
pub struct Task {
    id: u64,
    target: Point,
    total: Aggregate,
    levels: [Aggregate; 5],
    rollovers: [u64; 5],
    last_rollovers: [Option<Aggregate>; 5],
    window: Option<Vec<Sample>>,
    last_update_ms: Option<i128>,
    maximum_batch: usize,
}
impl Task {
    pub fn new(id: u64, target: Point, maximum_batch: usize) -> Result<Self, Error> {
        if maximum_batch == 0 || maximum_batch > 65_536 {
            return Err(Error::Capacity);
        }
        Ok(Self {
            id,
            target,
            total: Aggregate::empty(1, None),
            levels: LEVELS.map(|(step, limit)| Aggregate::empty(step, Some(limit))),
            rollovers: [0; 5],
            last_rollovers: [None; 5],
            window: None,
            last_update_ms: None,
            maximum_batch,
        })
    }
    pub fn update_target(&mut self, target: Point, now_ms: i128) {
        if (target.height, target.hash) > (self.target.height, self.target.hash) {
            self.target = target;
            self.last_update_ms = Some(now_ms);
        }
    }
    // Returns rollover logs in original order. Updates are atomic on local
    // capacity/arithmetic failure; native logging arithmetic can also fail.
    pub fn post(&mut self, samples: &[Sample], now_ms: i128) -> Result<Vec<Aggregate>, Error> {
        if samples.len() > self.maximum_batch {
            return Err(Error::Capacity);
        }
        let mut next = self.clone();
        let mut emitted = vec![];
        for sample in samples {
            let mut input = *sample;
            let mut work = match sample.work_us {
                Some(work) => work,
                None => sample
                    .end_us
                    .checked_sub(sample.start_us)
                    .ok_or(Error::Overflow)?,
            };
            for level in 0..5 {
                if !next.levels[level].add(input, work)? {
                    break;
                }
                let full = next.levels[level];
                // Original evaluates log arguments before forwarding a full
                // aggregate to the next level, even with a no-op log sink.
                full.rates()?;
                emitted.push(full);
                next.rollovers[level] = next.rollovers[level]
                    .checked_add(1)
                    .ok_or(Error::Overflow)?;
                next.last_rollovers[level] = Some(full);
                next.levels[level] = Aggregate::empty(full.step, full.limit);
                input = Sample {
                    height: sample.height,
                    start_us: full.start_us.expect("nonempty aggregate"),
                    end_us: full.end_us.expect("nonempty aggregate"),
                    micro_blocks: full.micro_blocks,
                    transactions: full.transactions,
                    work_us: None,
                };
                work = full.work_us;
            }
        }
        for sample in samples {
            let work = match sample.work_us {
                Some(work) => work,
                None => sample
                    .end_us
                    .checked_sub(sample.start_us)
                    .ok_or(Error::Overflow)?,
            };
            next.total.add(*sample, work)?;
        }
        next.window = Some(match &self.window {
            None => samples.iter().rev().copied().collect(),
            Some(old) => samples
                .iter()
                .rev()
                .chain(old.iter())
                .take(WINDOW)
                .copied()
                .collect(),
        });
        next.last_update_ms = Some(now_ms);
        *self = next;
        Ok(emitted)
    }
    pub fn estimate(&self) -> Result<f64, Error> {
        let window = self.window.as_ref().ok_or(Error::UndefinedEstimate)?;
        let first = window.first().ok_or(Error::EmptyEstimate)?;
        let last = window.last().expect("nonempty");
        let span = first
            .end_us
            .checked_sub(last.start_us)
            .ok_or(Error::Overflow)?;
        if span == 0 {
            return Err(Error::ZeroDuration);
        }
        // This denominator is always 1000, not the actual window length.
        let per_generation = (span as f64 / 1000.0) / 1_000_000.0;
        let rounded = (1000.0 * (60.0 / per_generation)).round();
        // Erlang round returns an integer, so rounded zero loses its sign.
        Ok(if rounded == 0.0 {
            0.0
        } else {
            rounded / 1000.0
        })
    }
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            id: self.id,
            target: self.target,
            total: self.total,
            levels: self.levels,
            rollovers: self.rollovers,
            last_rollovers: self.last_rollovers,
            window_len: self.window.as_ref().map(Vec::len),
            newest: self.window.as_ref().and_then(|w| w.first()).copied(),
            oldest: self.window.as_ref().and_then(|w| w.last()).copied(),
            generations_per_minute: self.estimate(),
            last_update_ms: self.last_update_ms,
            failure: None,
        }
    }
}
