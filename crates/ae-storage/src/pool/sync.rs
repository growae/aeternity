use super::*;
use ae_mempool::sync as protocol;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Repository(Error),
    Protocol(protocol::Error),
}
impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Repository(e)
    }
}
impl From<protocol::Error> for Failure {
    fn from(e: protocol::Error) -> Self {
        Self::Protocol(e)
    }
}
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub chain: Status,
    pub revision: u64,
    pub tree: Arc<protocol::Snapshot>,
}
#[derive(Debug, Clone, Copy)]
pub struct GetLimits {
    pub requests: usize,
    pub wire_bytes: usize,
}
impl Default for GetLimits {
    fn default() -> Self {
        Self {
            requests: 100_000,
            wire_bytes: 16 * 1024 * 1024,
        }
    }
}
impl<S: Read> Stored<S> {
    pub fn sync_snapshot(&self, limits: protocol::Limits) -> Result<Snapshot, Failure> {
        // The original folds durable membership, not the transient active/
        // visited queues. One immutable DB view binds all membership reads.
        let hashes = self.members(limits.entries)?;
        let tree = Arc::new(protocol::Snapshot::build(hashes, limits)?);
        Ok(Snapshot {
            chain: self.chain.status(),
            revision: self.revision,
            tree,
        })
    }
    pub fn sync_get(
        &self,
        hashes: &[Vec<u8>],
        limits: GetLimits,
    ) -> Result<Vec<Arc<Signed>>, Error> {
        if hashes.len() > limits.requests {
            return Err(Error::ResourceLimit);
        }
        let mut remaining = limits.wire_bytes;
        let mut out = vec![];
        for bytes in hashes {
            let Ok(hash) = Hash256::try_from(bytes.as_slice()) else {
                continue;
            };
            // Requested records need not belong to the sync snapshot or even
            // the current pool. Missing records are skipped; order/duplicates
            // are preserved. Storage faults surface instead of becoming misses.
            if let Some(tx) = self.transaction(hash)? {
                remaining = remaining
                    .checked_sub(tx.encode().len())
                    .ok_or(Error::ResourceLimit)?;
                out.push(Arc::new(tx));
            }
        }
        Ok(out)
    }
}
#[cfg(test)]
mod tests;
