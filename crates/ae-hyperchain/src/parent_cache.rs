// aec_parent_chain_cache, aeternity fa2a6d7.
// Copyright (c) 2022, Aeternity. ISC notice in repository LICENSE.
use crate::parent_block::ParentBlock;
use ae_types::{Hash256, Unsigned};
use ae_vm_fate::{
    maps::{MapOrder, Traversal},
    value::Value,
};
use num_bigint::{BigInt, Sign};
use std::collections::BTreeMap;
pub mod runtime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub child_start_height: Unsigned,
    pub max_size: Unsigned,
    pub retry_interval: Unsigned,
    pub finality: Unsigned,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildTop {
    pub height: Unsigned,
    pub hash: Hash256,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Requested(BigInt),
    Block(ParentBlock),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupError {
    NotInCache,
    NotFinal,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchResult {
    Block(ParentBlock),
    NotFound,
    NoAgreement,
    Exited,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Native {
    EmptyInitializeTargets,
    EmptyMaximumTargets,
    EmptyEviction,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ResourceLimit,
    InvalidOrder,
    Fate(ae_vm_fate::Error),
    Native(Native),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepError<E> {
    Cache(Error),
    Host(E),
}
impl<E> From<Error> for StepError<E> {
    fn from(e: Error) -> Self {
        Self::Cache(e)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub maximum_entries: usize,
    pub maximum_index_entries: usize,
    pub maximum_targets: usize,
    pub maximum_requests: usize,
    pub maximum_bytes: usize,
    pub order: ae_vm_fate::Limits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            maximum_entries: 100_000,
            maximum_index_entries: 100_000,
            maximum_targets: 100_000,
            maximum_requests: 100_000,
            maximum_bytes: 64 * 1024 * 1024,
            order: Default::default(),
        }
    }
}
// Host clones isolate requests, retry scheduling and other emitted effects.
// Target acquisition and I/O/resource faults remain explicit operational errors.
pub trait Host: Clone {
    type Error;
    fn is_operational(error: &Self::Error) -> bool;
    fn targets(&mut self, child_height: &Unsigned) -> Result<Vec<BigInt>, Self::Error>;
    fn now_ms(&mut self) -> BigInt;
    fn request(&mut self, height: &BigInt) -> Result<(), Self::Error>;
    fn fetch(&mut self, height: &BigInt) -> Result<FetchResult, Self::Error>;
    fn retry_initialize(&mut self, after_ms: u64) -> Result<(), Self::Error>;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Initialize,
    ChildTop(ChildTop),
    PostBlock(ParentBlock),
    Get(BigInt),
    Ignore,
    UnknownCall,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    None,
    Block(Result<ParentBlock, LookupError>),
    Unhandled,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cache {
    config: Config,
    child_top: ChildTop,
    blocks: BTreeMap<BigInt, Entry>,
    hash_index: BTreeMap<Vec<u8>, BigInt>,
    top_height: Unsigned,
}
pub fn integer(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}
impl Cache {
    pub fn new(config: Config, child_top: ChildTop) -> Self {
        Self {
            config,
            child_top,
            blocks: BTreeMap::new(),
            hash_index: BTreeMap::new(),
            top_height: 0.into(),
        }
    }
    pub fn config(&self) -> &Config {
        &self.config
    }
    pub fn child_top(&self) -> &ChildTop {
        &self.child_top
    }
    pub fn top_height(&self) -> &Unsigned {
        &self.top_height
    }
    pub fn blocks(&self) -> &BTreeMap<BigInt, Entry> {
        &self.blocks
    }
    pub fn hash_index(&self) -> &BTreeMap<Vec<u8>, BigInt> {
        &self.hash_index
    }
    pub fn lookup(&self, height: &BigInt) -> Result<&ParentBlock, LookupError> {
        match self.blocks.get(height) {
            Some(Entry::Block(block))
                if height <= &(integer(&self.top_height) - integer(&self.config.finality)) =>
            {
                Ok(block)
            }
            Some(Entry::Block(_)) => Err(LookupError::NotFinal),
            _ => Err(LookupError::NotInCache),
        }
    }
    pub fn step<H: Host>(
        &mut self,
        event: Event,
        host: &mut H,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Reply, StepError<H::Error>> {
        self.check_limits(limits)?;
        let mut budget = Budget::new(limits);
        budget.event(&event)?;
        let mut next = self.clone();
        let mut staged = host.clone();
        let result = next.handle(event, &mut staged, order, &mut budget);
        let reply = match result {
            Ok(reply) => reply,
            Err(e) => {
                if matches!(&e, StepError::Cache(Error::Native(_)))
                    || matches!(&e,StepError::Host(e) if !H::is_operational(e))
                {
                    *host = staged;
                }
                return Err(e);
            }
        };
        next.check_limits(limits)?;
        *self = next;
        *host = staged;
        Ok(reply)
    }
    fn check_limits(&self, limits: Limits) -> Result<(), Error> {
        if self.blocks.len() > limits.maximum_entries
            || self.hash_index.len() > limits.maximum_index_entries
        {
            return Err(Error::ResourceLimit);
        }
        let mut b = Budget::new(limits);
        for n in [
            &self.config.child_start_height,
            &self.config.max_size,
            &self.config.retry_interval,
            &self.config.finality,
            &self.child_top.height,
            &self.top_height,
        ] {
            b.add(n.as_bytes().len())?;
        }
        b.add(32)?;
        for (height, entry) in &self.blocks {
            b.big(height)?;
            match entry {
                Entry::Requested(time) => b.big(time)?,
                Entry::Block(block) => b.block(block)?,
            }
        }
        for (hash, height) in &self.hash_index {
            b.add(hash.len())?;
            b.big(height)?;
        }
        Ok(())
    }
    fn targets<H: Host>(
        &self,
        host: &mut H,
        budget: &mut Budget,
    ) -> Result<Vec<BigInt>, StepError<H::Error>> {
        let targets = host
            .targets(&self.child_top.height)
            .map_err(StepError::Host)?;
        if targets.len() > budget.limits.maximum_targets {
            return Err(Error::ResourceLimit.into());
        }
        for n in &targets {
            budget.big(n)?;
        }
        Ok(targets)
    }
    fn handle<H: Host>(
        &mut self,
        event: Event,
        host: &mut H,
        order: &impl MapOrder,
        budget: &mut Budget,
    ) -> Result<Reply, StepError<H::Error>> {
        match event {
            Event::Initialize => {
                let mut targets = self.targets(host, budget)?;
                targets.sort();
                targets.reverse();
                let Some((first, rest)) = targets.split_first() else {
                    return Err(Error::Native(Native::EmptyInitializeTargets).into());
                };
                match host.fetch(first).map_err(StepError::Host)? {
                    FetchResult::Block(block) => {
                        budget.block(&block)?;
                        self.maybe_request(rest, host, budget)?;
                        self.post(block, host, order, budget)?;
                    }
                    FetchResult::NotFound | FetchResult::NoAgreement | FetchResult::Exited => {
                        host.retry_initialize(1000).map_err(StepError::Host)?
                    }
                }
                Ok(Reply::None)
            }
            Event::ChildTop(top) => {
                let flush = self.child_top.height.add(&self.config.max_size) < top.height;
                self.child_top = top;
                // Original clears blocks only. Its unused hash index survives.
                if flush {
                    self.blocks.clear();
                }
                let targets = self.targets(host, budget)?;
                self.maybe_request(&targets, host, budget)?;
                Ok(Reply::None)
            }
            Event::PostBlock(block) => {
                self.post(block, host, order, budget)?;
                Ok(Reply::None)
            }
            Event::Get(height) => {
                let result = self.lookup(&height).cloned();
                if result.is_err() {
                    self.request(&height, host, budget)?;
                }
                Ok(Reply::Block(result))
            }
            Event::Ignore => Ok(Reply::None),
            Event::UnknownCall => Ok(Reply::Unhandled),
        }
    }
    fn request<H: Host>(
        &mut self,
        height: &BigInt,
        host: &mut H,
        budget: &mut Budget,
    ) -> Result<(), StepError<H::Error>> {
        let now = host.now_ms();
        budget.big(&now)?;
        let request = match self.blocks.get(height) {
            None => true,
            Some(Entry::Requested(time)) => now > time + integer(&self.config.retry_interval),
            Some(Entry::Block(_)) => false,
        };
        if request {
            if budget.requests >= budget.limits.maximum_requests {
                return Err(Error::ResourceLimit.into());
            }
            budget.requests += 1;
            host.request(height).map_err(StepError::Host)?;
            self.blocks.insert(height.clone(), Entry::Requested(now));
        };
        Ok(())
    }
    fn maybe_request<H: Host>(
        &mut self,
        heights: &[BigInt],
        host: &mut H,
        budget: &mut Budget,
    ) -> Result<(), StepError<H::Error>> {
        for height in heights {
            if self.lookup(height) == Err(LookupError::NotInCache) {
                self.request(height, host, budget)?;
            }
        }
        Ok(())
    }
    fn post<H: Host>(
        &mut self,
        block: ParentBlock,
        host: &mut H,
        order: &impl MapOrder,
        budget: &mut Budget,
    ) -> Result<(), StepError<H::Error>> {
        let height = block.height.clone();
        let targets = self.targets(host, budget)?;
        let maximum = targets
            .iter()
            .max()
            .ok_or(Error::Native(Native::EmptyMaximumTargets))?;
        if &integer(&height) <= maximum {
            self.hash_index.insert(block.hash.clone(), integer(&height));
            self.blocks.insert(integer(&height), Entry::Block(block));
        }
        // Equality already evicts. Requests can separately grow beyond max_size,
        // and every post removes at most one entry, including out-of-range posts.
        if Unsigned::from(self.blocks.len() as u64) >= self.config.max_size {
            let Some((_, entry)) = self.blocks.pop_first() else {
                return Err(Error::Native(Native::EmptyEviction).into());
            };
            if let Entry::Block(b) = entry {
                self.hash_index.remove(&b.hash);
            }
        }
        let keys: Vec<_> = self
            .blocks
            .keys()
            .map(|n| Value::Integer(n.clone()))
            .collect();
        let indices = order
            .indices(
                &keys.iter().collect::<Vec<_>>(),
                Traversal::ToList,
                budget.limits.order,
            )
            .map_err(Error::Fate)?;
        if indices.len() != keys.len() {
            return Err(Error::InvalidOrder.into());
        }
        let mut seen = vec![false; keys.len()];
        let heights: Vec<_> = self.blocks.keys().cloned().collect();
        let mut ordered = Vec::with_capacity(heights.len());
        for i in indices {
            let slot = seen.get_mut(i).ok_or(Error::InvalidOrder)?;
            if *slot {
                return Err(Error::InvalidOrder.into());
            }
            *slot = true;
            ordered.push(heights[i].clone());
        }
        self.maybe_request(&ordered, host, budget)?;
        self.top_height = self.top_height.clone().max(height);
        Ok(())
    }
}
struct Budget {
    limits: Limits,
    bytes: usize,
    requests: usize,
}
impl Budget {
    fn new(limits: Limits) -> Self {
        Self {
            limits,
            bytes: 0,
            requests: 0,
        }
    }
    fn add(&mut self, n: usize) -> Result<(), Error> {
        self.bytes = self.bytes.checked_add(n).ok_or(Error::ResourceLimit)?;
        if self.bytes > self.limits.maximum_bytes {
            Err(Error::ResourceLimit)
        } else {
            Ok(())
        }
    }
    fn big(&mut self, n: &BigInt) -> Result<(), Error> {
        let bytes = n.bits().div_ceil(8);
        self.add(
            usize::try_from(bytes)
                .map_err(|_| Error::ResourceLimit)?
                .saturating_add(1),
        )
    }
    fn block(&mut self, b: &ParentBlock) -> Result<(), Error> {
        for n in [
            b.hash.len(),
            b.previous_hash.len(),
            b.height.as_bytes().len(),
            b.time.as_bytes().len(),
        ] {
            self.add(n)?;
        }
        Ok(())
    }
    fn event(&mut self, e: &Event) -> Result<(), Error> {
        match e {
            Event::Get(n) => self.big(n),
            Event::PostBlock(b) => self.block(b),
            Event::ChildTop(t) => {
                self.add(32)?;
                self.add(t.height.as_bytes().len())
            }
            _ => Ok(()),
        }
    }
}
