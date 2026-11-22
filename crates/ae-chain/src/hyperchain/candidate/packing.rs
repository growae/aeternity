use super::*;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Timeout {
    Disabled,
    Milliseconds(u64),
    // Invalid environment values use the native 1000 ms default.
    #[default]
    Default,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadlines {
    pub bound: Option<i64>,
    pub ceiling: Option<i64>,
}
impl Deadlines {
    pub fn new(now: i64, timeout: Timeout, window: i64) -> Option<Self> {
        let amount = match timeout {
            Timeout::Disabled | Timeout::Milliseconds(0) => {
                return Some(Self {
                    bound: None,
                    ceiling: None,
                });
            }
            Timeout::Milliseconds(n) => n,
            Timeout::Default => 1000,
        };
        let fits = window.max(1);
        let trim = amount.min(fits as u64) as i64;
        Some(Self {
            bound: Some(now.checked_add(trim)?),
            ceiling: Some(now.checked_add(fits)?),
        })
    }
    fn expired(self, mut now: impl FnMut() -> i64, packed: bool) -> bool {
        self.bound.is_some_and(|d| now() >= d)
            && (packed || self.ceiling.is_some_and(|d| now() >= d))
    }
}
pub type FinishFailure<E, A, C> = (Box<Packing<A, C>>, Error<E, A, C>);

pub struct PackRequest<'a> {
    pub parent: Hash256,
    pub gas: &'a BigInt,
    pub ignored: &'a BTreeSet<Hash256>,
    pub deadlines: Deadlines,
    pub packed: bool,
}
// The composing worker obtains selections from its one shared pool between
// calls. No pool, clock, networking or mutable global state enters execution.
pub struct Packing<A = MemoryStore, C = MemoryStore> {
    candidate: Micro<A, C>,
    remaining: BigInt,
    ignored: BTreeSet<Hash256>,
    deadlines: Deadlines,
    offered: usize,
    passes: usize,
    awaiting: bool,
    done: bool,
    expired: bool,
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> Packing<A, C> {
    pub fn new<E>(
        candidate: Micro<A, C>,
        reserved_gas: BigInt,
        now: i64,
        timeout: Timeout,
        window: i64,
    ) -> Result<Self, Error<E, A, C>> {
        if !candidate.block.transactions.is_empty() {
            return Err(Error::Sequence);
        }
        let remaining = &candidate.gas_limit - reserved_gas;
        let deadlines = Deadlines::new(now, timeout, window).ok_or(Error::ResourceLimit)?;
        Ok(Self {
            candidate,
            remaining,
            deadlines,
            ignored: Default::default(),
            offered: 0,
            passes: 0,
            awaiting: false,
            done: false,
            expired: false,
        })
    }
    pub fn expired(&self) -> bool {
        self.expired
    }
    pub fn request<E>(
        &mut self,
        now: impl FnMut() -> i64,
    ) -> Result<Option<PackRequest<'_>>, Error<E, A, C>> {
        if self.done {
            return Ok(None);
        }
        let packed = !self.candidate.block.transactions.is_empty();
        if !self.awaiting && self.deadlines.expired(now, packed) {
            self.done = true;
            self.expired = true;
            return Ok(None);
        }
        if self.passes >= self.candidate.limits.passes {
            return Err(Error::ResourceLimit);
        }
        self.awaiting = true;
        Ok(Some(PackRequest {
            parent: Hash256(self.candidate.block.header.common.previous_hash),
            gas: &self.remaining,
            ignored: &self.ignored,
            deadlines: self.deadlines,
            packed,
        }))
    }
    pub fn selected<E, O: MapOrder, S: chain::Source<A, C>>(
        &mut self,
        transactions: &[Signed],
        expired: bool,
        reader: &chain::Reader<A, C, O, S>,
    ) -> Result<Applied<A, C>, Error<E, A, C>> {
        if !self.awaiting || self.done {
            return Err(Error::Sequence);
        }
        let offered = self
            .offered
            .checked_add(transactions.len())
            .ok_or(Error::ResourceLimit)?;
        if offered > self.candidate.limits.offered {
            return Err(Error::ResourceLimit);
        }
        charge::<E, A, C>(transactions, self.candidate.limits.signed_bytes)?;
        if transactions.is_empty() {
            self.awaiting = false;
            self.done = true;
            self.expired = expired;
            return Ok(Applied {
                accepted: vec![],
                rejected: vec![],
            });
        }
        let candidate = &mut self.candidate;
        let mut trees = candidate.view.trees.as_ref().clone();
        let receipt = reader
            .with_execution(&candidate.view, |cfg| {
                batch::apply(
                    transactions,
                    &mut trees,
                    &candidate.view.context,
                    cfg,
                    &Default::default(),
                )
            })
            .map_err(Error::Transactions)?;
        let accepted = receipt.accepted.iter().map(|(i, _)| *i).collect::<Vec<_>>();
        let txs = accepted
            .iter()
            .map(|i| transactions[*i].clone())
            .collect::<Vec<_>>();
        let gas = candidate.used::<E>(&txs, candidate.key_height, &trees)?;
        candidate.append::<E>(txs)?;
        candidate.view.trees = Arc::new(trees);
        self.remaining -= gas;
        self.ignored.extend(transactions.iter().map(Signed::hash));
        self.offered = offered;
        self.passes += 1;
        self.awaiting = false;
        Ok(Applied {
            accepted,
            rejected: receipt.rejected,
        })
    }
    pub fn finish<E>(self) -> Result<Micro<A, C>, Error<E, A, C>> {
        self.try_finish().map_err(|(_, error)| error)
    }
    // Retain the complete attempt if final root construction cannot finish.
    pub fn try_finish<E>(mut self) -> Result<Micro<A, C>, FinishFailure<E, A, C>> {
        if !self.done || self.awaiting {
            return Err((Box::new(self), Error::Sequence));
        }
        if let Err(error) = self.candidate.refresh::<E>() {
            return Err((Box::new(self), error));
        }
        Ok(self.candidate)
    }
}
