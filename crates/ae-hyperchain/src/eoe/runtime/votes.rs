use super::*;
use ae_mempool::hc_votes::VotePool;
use ae_tx::HcVote;
use std::sync::atomic::AtomicUsize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteQuery {
    pub epoch: Unsigned,
    pub kind: Option<Unsigned>,
    // Zero returns statistics alone. A query never removes pool entries.
    pub maximum: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct Data {
    epoch: Unsigned,
    entries: usize,
    retained_wire_bytes: usize,
    votes: Vec<HcVote>,
}
#[derive(Debug)]
struct Retained {
    data: Data,
    _credit: Credit,
}
// Clones share one immutable allocation and its byte charge. Completed,
// unread replies and replies held after worker shutdown keep the charge.
#[derive(Debug, Clone)]
pub struct VoteSnapshot(Arc<Retained>);
impl PartialEq for VoteSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.0.data == other.0.data
    }
}
impl Eq for VoteSnapshot {}
impl VoteSnapshot {
    pub fn epoch(&self) -> &Unsigned {
        &self.0.data.epoch
    }
    pub fn entries(&self) -> usize {
        self.0.data.entries
    }
    pub fn retained_wire_bytes(&self) -> usize {
        self.0.data.retained_wire_bytes
    }
    pub fn votes(&self) -> &[HcVote] {
        &self.0.data.votes
    }
}
#[derive(Debug)]
pub(super) struct Budget {
    maximum: usize,
    used: AtomicUsize,
}
#[derive(Debug)]
struct Credit {
    budget: Arc<Budget>,
    bytes: usize,
}
impl Drop for Credit {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
impl Budget {
    pub fn new(maximum: usize) -> Self {
        Self {
            maximum,
            used: AtomicUsize::new(0),
        }
    }
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
    pub fn capture(
        self: &Arc<Self>,
        pool: &VotePool,
        query: VoteQuery,
    ) -> Result<VoteSnapshot, coordinator::Error> {
        let base = pool.epoch().as_bytes().len().checked_add(256);
        let bytes = pool
            .peek(&query.epoch, query.kind.as_ref())
            .take(query.maximum)
            .try_fold(base.ok_or(coordinator::Error::ResourceLimit)?, |n, vote| {
                n.checked_add(vote.wire_size())
                    .and_then(|n| n.checked_add(128))
                    .ok_or(coordinator::Error::ResourceLimit)
            })?;
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|n| *n <= self.maximum)
            })
            .map_err(|_| coordinator::Error::ResourceLimit)?;
        let credit = Credit {
            budget: self.clone(),
            bytes,
        };
        Ok(VoteSnapshot(Arc::new(Retained {
            data: Data {
                epoch: pool.epoch().clone(),
                entries: pool.len(),
                retained_wire_bytes: pool.retained_wire_bytes(),
                votes: pool
                    .peek(&query.epoch, query.kind.as_ref())
                    .take(query.maximum)
                    .cloned()
                    .collect(),
            },
            _credit: credit,
        })))
    }
}
