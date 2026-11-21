// aec_sync:agree_on_height, aeternity fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC: repository LICENSE.
use ae_types::Hash256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Point {
    pub height: u64,
    pub hash: Hash256,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    Known(Point),
    Header(u64),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Probe(Probe),
    Agreed(Point),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    NoProgress,
    Finished,
}
#[derive(Debug, Clone, Copy)]
enum Phase {
    Initial,
    Backward { upper: u64, distance: u64 },
    Binary { lower: Point, upper: u64 },
    Finished,
}
pub struct Search {
    height: u64,
    phase: Phase,
    genesis: Hash256,
    next: Step,
}
impl Search {
    pub fn new(local_height: u64, remote: Point, genesis: Hash256) -> Self {
        let height = local_height.min(remote.height);
        let next = Step::Probe(if remote.height == height {
            Probe::Known(remote)
        } else if height == 0 {
            Probe::Known(Point {
                height,
                hash: genesis,
            })
        } else {
            Probe::Header(height)
        });
        Self {
            height,
            phase: Phase::Initial,
            genesis,
            next,
        }
    }
    pub fn next(&self) -> Step {
        self.next
    }
    fn query(&mut self, height: u64) {
        self.height = height;
        self.next = Step::Probe(if height == 0 {
            Probe::Known(Point {
                height,
                hash: self.genesis,
            })
        } else {
            Probe::Header(height)
        });
    }
    fn agree(&mut self, point: Point) {
        self.phase = Phase::Finished;
        self.next = Step::Agreed(point);
    }
    fn binary(&mut self, lower: Point, upper: u64) -> Result<(), Error> {
        if upper <= lower.height {
            return Err(Error::NoProgress);
        }
        if upper - lower.height == 1 {
            self.agree(lower);
        } else {
            self.phase = Phase::Binary { lower, upper };
            self.query(lower.height + (upper - lower.height) / 2);
        }
        Ok(())
    }
    pub fn observe(&mut self, hash: Hash256, connected: bool) -> Result<Step, Error> {
        let point = Point {
            height: self.height,
            hash,
        };
        match self.phase {
            Phase::Finished => return Err(Error::Finished),
            Phase::Initial if connected => self.agree(point),
            Phase::Initial => {
                if self.height == 0 {
                    return Err(Error::NoProgress);
                }
                self.phase = Phase::Backward {
                    upper: self.height,
                    distance: 1,
                };
                self.query(self.height.saturating_sub(1));
            }
            Phase::Backward { upper, .. } if connected => self.binary(point, upper)?,
            Phase::Backward { distance, .. } => {
                if self.height == 0 {
                    return Err(Error::NoProgress);
                }
                let distance = distance.saturating_mul(2);
                self.phase = Phase::Backward {
                    upper: self.height,
                    distance,
                };
                self.query(self.height.saturating_sub(distance));
            }
            Phase::Binary { lower, upper } => {
                if connected {
                    self.binary(point, upper)?;
                } else {
                    self.binary(lower, self.height)?;
                }
            }
        }
        Ok(self.next)
    }
}
