use super::*;
use crate::pool::{Signed, Submission};
use ae_mempool::{
    gossip::{Cache, Stats},
    pool::Event,
};

// Separate cache ownership allows a supervisor to restart just the pool
// worker while retaining the original independent gossip-cache lifetime.
#[derive(Clone)]
pub struct GossipCache {
    cache: Arc<Mutex<Cache>>,
    timeout: Duration,
}
impl Default for GossipCache {
    fn default() -> Self {
        Self {
            cache: Default::default(),
            timeout: Duration::from_secs(3),
        }
    }
}
impl GossipCache {
    pub fn new(capacity: usize, limit: usize) -> Result<Self, ae_mempool::gossip::Error> {
        Ok(Self {
            cache: Arc::new(Mutex::new(Cache::new(capacity, limit)?)),
            timeout: Duration::from_secs(3),
        })
    }
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    fn lock(&self, recover: bool) -> Result<std::sync::MutexGuard<'_, Cache>, Error> {
        let start = Instant::now();
        loop {
            match self.cache.try_lock() {
                Ok(guard) => return Ok(guard),
                Err(std::sync::TryLockError::Poisoned(error)) => {
                    return if recover {
                        Ok(error.into_inner())
                    } else {
                        Err(Error::CacheUnavailable)
                    };
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    let remaining = self.timeout.saturating_sub(start.elapsed());
                    if remaining.is_zero() {
                        return Err(Error::CacheUnavailable);
                    }
                    thread::sleep(remaining.min(Duration::from_millis(1)));
                }
            }
        }
    }
    pub fn stats(&self) -> Result<Stats, Error> {
        self.lock(false).map(|c| c.stats().clone())
    }
    pub fn reconfigure(&self, capacity: usize) -> Result<(), Error> {
        self.lock(false)?
            .reconfigure(capacity)
            .map_err(Error::Cache)
    }
    pub fn reset(&self) -> Result<(), Error> {
        let mut cache = self.lock(true)?;
        cache.reset();
        self.cache.clear_poison();
        Ok(())
    }
    // Checking records a miss, matching the native shared first-level filter.
    pub fn check(&self, hash: ae_types::Hash256) -> bool {
        // Cache failure must not reject a transaction. The original API
        // returns a miss when its process is unavailable or its call times out.
        self.lock(false).map(|mut c| c.check(hash)).unwrap_or(false)
    }
}
impl Client {
    pub fn gossip_cache(&self) -> GossipCache {
        self.shared.gossip.clone()
    }
    pub fn receive(&self, transaction: Arc<Signed>) -> Result<Ticket<Submission>, Error> {
        if !self.shared.lock().info.accepting {
            return Err(Error::Stopped);
        }
        let cached = self.shared.gossip.check(transaction.hash());
        // A miss is retained even if validation, queueing or later storage
        // fails. This is the original first-level duplicate filter behavior.
        self.push(transaction, Event::Received, cached)
    }
}
#[cfg(test)]
mod tests;
