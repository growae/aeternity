// aec_consensus_hc validator schedule cache, aeternity fa2a6d7.
// Copyright (c) 2022, Aeternity. ISC notice in repository LICENSE.
use ae_vm_fate::value::ObjectPayload;
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub maximum_slots: usize,
    pub maximum_gc_span: usize,
    pub maximum_address_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            maximum_slots: 1_000_000,
            maximum_gc_span: 1_000_000,
            maximum_address_bytes: 64 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ResourceLimit,
    InvalidRange,
    NotInCache,
    EpochDidNotEnd,
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Cache {
    epochs: Vec<BigInt>,
    ranges: BTreeMap<BigInt, (BigInt, BigInt)>,
    schedule: BTreeMap<BigInt, ObjectPayload>,
}
impl Cache {
    // Restore Rust-owned cache metadata without replaying insert/GC effects.
    pub fn restore(
        epochs: Vec<BigInt>,
        ranges: BTreeMap<BigInt, (BigInt, BigInt)>,
        schedule: BTreeMap<BigInt, ObjectPayload>,
        limits: Limits,
    ) -> Result<Self, Error> {
        if epochs.len() > 5 || ranges.len() > 5 {
            return Err(Error::ResourceLimit);
        }
        let cache = Self {
            epochs,
            ranges,
            schedule,
        };
        cache.validate_size(limits)?;
        Ok(cache)
    }
    pub fn epochs(&self) -> &[BigInt] {
        &self.epochs
    }
    pub fn ranges(&self) -> &BTreeMap<BigInt, (BigInt, BigInt)> {
        &self.ranges
    }
    pub fn schedule(&self) -> &BTreeMap<BigInt, ObjectPayload> {
        &self.schedule
    }
    pub fn leader(&self, height: &BigInt) -> Result<&ObjectPayload, Error> {
        self.schedule.get(height).ok_or(Error::NotInCache)
    }
    pub fn valid_producer(
        &self,
        height: &BigInt,
        producer: &ObjectPayload,
        hole: bool,
    ) -> Result<bool, Error> {
        let leader = self.leader(height)?;
        Ok(if hole {
            self.schedule
                .range(height.clone()..)
                .any(|(_, p)| p == producer)
        } else {
            leader == producer
        })
    }
    pub fn own_slot(
        &self,
        from: &BigInt,
        to: &BigInt,
        maximum_reads: usize,
        mut present: impl FnMut(&ObjectPayload) -> bool,
    ) -> Result<Option<(BigInt, ObjectPayload)>, Error> {
        let mut h = from.clone();
        let mut reads = 0;
        while &h <= to {
            if reads >= maximum_reads {
                return Err(Error::ResourceLimit);
            }
            reads += 1;
            let Ok(p) = self.leader(&h) else {
                return Ok(None);
            };
            if present(p) {
                return Ok(Some((h, p.clone())));
            }
            h += 1;
        }
        Ok(None)
    }
    pub fn insert(
        &mut self,
        epoch: BigInt,
        first: BigInt,
        length: BigInt,
        leaders: Vec<ObjectPayload>,
        limits: Limits,
    ) -> Result<(), Error> {
        if self.leader(&first).is_ok() {
            return Ok(());
        }
        if leaders.len() > limits.maximum_slots {
            return Err(Error::ResourceLimit);
        }
        let mut next = self.clone();
        next.epochs.push(epoch.clone());
        next.ranges
            .insert(epoch, (first.clone(), &first + length - 1));
        for (i, p) in leaders.into_iter().enumerate() {
            next.schedule.insert(&first + i, p);
        }
        if next.epochs.len() > 5 {
            let old = next.epochs.remove(0);
            if let Some((first, last)) = next.ranges.remove(&old) {
                let span: BigInt = last - first.clone() + 1;
                if span < 0.into() {
                    return Err(Error::InvalidRange);
                }
                if span.to_usize().is_none_or(|n| n > limits.maximum_gc_span) {
                    return Err(Error::ResourceLimit);
                }
                next.schedule
                    .retain(|h, _| h < &first || h >= &(&first + &span));
            }
        }
        next.validate_size(limits)?;
        *self = next;
        Ok(())
    }
    fn validate_size(&self, limits: Limits) -> Result<(), Error> {
        if self.schedule.len() > limits.maximum_slots {
            return Err(Error::ResourceLimit);
        }
        let mut remaining = limits.maximum_address_bytes;
        for p in self.schedule.values() {
            let mut pending = vec![p];
            while let Some(p) = pending.pop() {
                match p {
                    ObjectPayload::Bytes(b) => {
                        remaining = remaining.checked_sub(b.len()).ok_or(Error::ResourceLimit)?
                    }
                    ObjectPayload::List(xs) => {
                        remaining = remaining
                            .checked_sub(xs.len())
                            .ok_or(Error::ResourceLimit)?;
                        pending.extend(xs);
                    }
                }
            }
        }
        Ok(())
    }
}
