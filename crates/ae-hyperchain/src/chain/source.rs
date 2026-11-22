use super::{Selection, View};
use num_bigint::BigInt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum LookupError {
    InvalidHash,
    StateGarbageCollected,
    Unavailable(Vec<u8>),
    ResourceLimit,
    Backend(Arc<dyn std::error::Error + Send + Sync>),
}
impl LookupError {
    pub fn is_operational(&self) -> bool {
        !matches!(self, Self::InvalidHash)
    }
}

// A capture must keep headers, canonical indexes and roots at one revision.
// Implementations may resolve lazily; a whole-chain in-memory index is not
// required. Returned views must retain that same snapshot for subsequent reads.
pub trait Read<A, C>: Clone {
    fn top_height(&self) -> Result<Option<BigInt>, LookupError>;
    fn key_hash(&self, height: &BigInt) -> Result<Option<Vec<u8>>, LookupError>;
    fn view(&self, hash: &[u8]) -> Result<Arc<View<A, C>>, LookupError>;
}
pub trait Source<A, C>: Clone {
    type Snapshot: Read<A, C>;
    fn capture(&self) -> Result<Self::Snapshot, LookupError>;
}
impl<A, C> Read<A, C> for Arc<Selection<A, C>> {
    fn top_height(&self) -> Result<Option<BigInt>, LookupError> {
        Ok(self.top.clone())
    }
    fn key_hash(&self, height: &BigInt) -> Result<Option<Vec<u8>>, LookupError> {
        Ok(self.heights.get(height).cloned())
    }
    fn view(&self, hash: &[u8]) -> Result<Arc<View<A, C>>, LookupError> {
        self.views
            .get(hash)
            .cloned()
            .ok_or_else(|| LookupError::Unavailable(hash.to_vec()))
    }
}
impl<A: Clone, C: Clone> Source<A, C> for super::Selected<A, C> {
    type Snapshot = Arc<Selection<A, C>>;
    fn capture(&self) -> Result<Self::Snapshot, LookupError> {
        Ok(self.snapshot())
    }
}

#[derive(Clone)]
pub struct Pinned<S>(pub S);
impl<A, C, S: Read<A, C>> Source<A, C> for Pinned<S> {
    type Snapshot = S;
    fn capture(&self) -> Result<S, LookupError> {
        Ok(self.0.clone())
    }
}
