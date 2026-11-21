// aec_sync shared chain and chunk scheduling, aeternity fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC: repository LICENSE.
use super::Point;
use ae_types::{Hash256, PeerPublicKey};
use std::collections::VecDeque;

mod chains;
mod merge;
mod schedule;
pub use chains::{Chain, Comparison, Side, TaskMatch, match_tasks};
pub use schedule::{Effect, Item, NextPool, Ready, ResultInput, Scheduler, State, Work};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub hints: usize,
    pub peers: usize,
    pub items: usize,
    pub chunk: usize,
    pub forward_chunks: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            hints: 4096,
            peers: 128,
            items: 4096,
            chunk: 100,
            forward_chunks: 15,
        }
    }
}
impl Limits {
    pub fn validate(self) -> Result<(), Error> {
        if [self.hints, self.peers, self.items, self.chunk]
            .into_iter()
            .any(|n| n == 0 || n > 65_536)
            || self.chunk > self.items
            || self.forward_chunks > 65_536
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    Empty,
    Order,
    Capacity,
    Identity,
    State,
    Overflow,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[cfg(test)]
mod tests;
