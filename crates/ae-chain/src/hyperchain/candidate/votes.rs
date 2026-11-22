// aec_conductor:hc_apply_vote/hc_apply_rest_votes, aeternity fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use ae_hyperchain::eoe::{Reply, local_host::Finalized};
use std::collections::VecDeque;

pub type VoteResults = [Reply<Finalized>; 2];
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteError {
    ResourceLimit,
    Sequence,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteProgress {
    Applied,
    Refresh,
    Finished(bool),
}
#[derive(Debug, Clone, Copy)]
enum Phase {
    First { retried: bool },
    Refresh,
    Rest,
    Done(bool),
}
// Only the original fork result decides EOE, including after a nonce refresh.
// Operational errors retain both the current candidate and the next vote.
#[derive(Debug)]
pub struct Votes {
    pending: VecDeque<Signed>,
    phase: Phase,
    original_eoe: bool,
    maximum_bytes: usize,
}
fn transactions(
    results: &VoteResults,
    maximum_bytes: usize,
) -> Result<VecDeque<Signed>, VoteError> {
    let mut bytes = 0usize;
    for result in results {
        if let Reply::Finalized(Finalized::Signed(tx)) = result {
            bytes = bytes
                .checked_add(tx.encode().len())
                .ok_or(VoteError::ResourceLimit)?;
            if bytes > maximum_bytes {
                return Err(VoteError::ResourceLimit);
            }
        }
    }
    Ok(results
        .iter()
        .filter_map(|r| match r {
            Reply::Finalized(Finalized::Signed(tx)) => Some(tx.as_ref().clone()),
            _ => None,
        })
        .collect())
}
impl Votes {
    pub fn new(results: &VoteResults, maximum_bytes: usize) -> Result<Self, VoteError> {
        let pending = transactions(results, maximum_bytes)?;
        let phase = if pending.is_empty() {
            Phase::Done(false)
        } else {
            Phase::First { retried: false }
        };
        Ok(Self {
            pending,
            phase,
            original_eoe: matches!(&results[0], Reply::Finalized(Finalized::Signed(_))),
            maximum_bytes,
        })
    }
    // Before packing, price every successful initial result at the parent
    // block's height and protocol, not at the candidate microblock's height.
    pub fn reserved_gas(&self, height: u64, protocol: Protocol) -> BigInt {
        self.pending
            .iter()
            .map(|tx| {
                tx.transaction()
                    .view()
                    .pricing(&height.into(), protocol)
                    .gas_limit
            })
            .sum()
    }
    pub fn needs_refresh(&self) -> bool {
        matches!(self.phase, Phase::Refresh)
    }
    pub fn outcome(&self) -> Option<bool> {
        if let Phase::Done(eoe) = self.phase {
            Some(eoe)
        } else {
            None
        }
    }
    pub fn refresh(&mut self, results: &VoteResults) -> Result<(), VoteError> {
        if !self.needs_refresh() {
            return Err(VoteError::Sequence);
        }
        let pending = transactions(results, self.maximum_bytes)?;
        self.phase = if pending.is_empty() {
            Phase::Done(false)
        } else {
            Phase::First { retried: true }
        };
        self.pending = pending;
        Ok(())
    }
    pub fn apply<
        E,
        A: NodeWriter + Clone,
        C: NodeWriter + Clone,
        O: MapOrder,
        S: chain::Source<A, C>,
    >(
        &mut self,
        candidate: &mut Micro<A, C>,
        reader: &chain::Reader<A, C, O, S>,
    ) -> Result<VoteProgress, Error<E, A, C>> {
        match self.phase {
            Phase::Refresh => return Ok(VoteProgress::Refresh),
            Phase::Done(eoe) => return Ok(VoteProgress::Finished(eoe)),
            _ => (),
        }
        let tx = self.pending.front().ok_or(Error::Sequence)?;
        match candidate.update::<E, _, _>(std::slice::from_ref(tx), reader) {
            Ok(_) => {
                self.pending.pop_front();
                self.phase = if self.pending.is_empty() {
                    Phase::Done(self.original_eoe)
                } else {
                    Phase::Rest
                };
                Ok(VoteProgress::Applied)
            }
            Err(Error::NoUpdate | Error::Full) => {
                self.pending.clear();
                let eoe = match self.phase {
                    Phase::First { retried: false } => {
                        self.phase = Phase::Refresh;
                        return Ok(VoteProgress::Refresh);
                    }
                    Phase::First { retried: true } => false,
                    Phase::Rest => self.original_eoe,
                    _ => return Err(Error::Sequence),
                };
                self.phase = Phase::Done(eoe);
                Ok(VoteProgress::Finished(eoe))
            }
            Err(error) => Err(error),
        }
    }
}
