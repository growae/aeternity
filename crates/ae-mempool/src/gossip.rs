// aec_tx_gossip_cache, aeternity fa2a6d7. Copyright (c) 2018,
// Aeternity Anstalt. ISC: repository LICENSE.
use ae_types::{Hash256, Unsigned};
use std::collections::{BTreeSet, VecDeque};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    pub hit: Unsigned,
    pub miss: Unsigned,
}
impl Default for Stats {
    fn default() -> Self {
        Self {
            hit: 0.into(),
            miss: 0.into(),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidCapacity,
    ResourceLimit,
}
#[derive(Debug, Clone)]
pub struct Cache {
    capacity: usize,
    limit: usize,
    queue: VecDeque<Hash256>,
    members: BTreeSet<Hash256>,
    stats: Stats,
}
impl Default for Cache {
    fn default() -> Self {
        Self::new(200, 100_000).expect("valid default cache bounds")
    }
}
impl Cache {
    pub fn new(capacity: usize, limit: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::InvalidCapacity);
        }
        if capacity > limit {
            return Err(Error::ResourceLimit);
        }
        Ok(Self {
            capacity,
            limit,
            queue: Default::default(),
            members: Default::default(),
            stats: Default::default(),
        })
    }
    pub fn check(&mut self, hash: Hash256) -> bool {
        if self.members.contains(&hash) {
            self.stats.hit = self.stats.hit.add(&1.into());
            return true;
        }
        // Changing the configured size does not trim existing entries. On a
        // miss the original evicts just one and keeps its previous size.
        if self.queue.len() >= self.capacity {
            let old = self
                .queue
                .pop_front()
                .expect("positive capacity and nonempty cache");
            self.members.remove(&old);
        }
        self.members.insert(hash);
        self.queue.push_back(hash);
        self.stats.miss = self.stats.miss.add(&1.into());
        false
    }
    pub fn reconfigure(&mut self, capacity: usize) -> Result<(), Error> {
        if capacity == 0 {
            return Err(Error::InvalidCapacity);
        }
        if capacity > self.limit {
            return Err(Error::ResourceLimit);
        }
        self.capacity = capacity;
        Ok(())
    }
    pub fn reset(&mut self) {
        self.queue.clear();
        self.members.clear();
        self.stats = Default::default();
    }
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
    pub fn len(&self) -> usize {
        self.queue.len()
    }
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn entries(&self) -> impl Iterator<Item = Hash256> + '_ {
        self.queue.iter().copied()
    }
}
#[cfg(test)]
mod tests;
