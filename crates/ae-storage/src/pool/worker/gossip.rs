use super::*;
use crate::pool::{Signed, Submission};
use ae_mempool::{
    gossip::{Cache, Stats},
    pool::Event,
};

mod lifecycle;
pub use lifecycle::{CacheController, CacheHandle, CacheOwner, LifecycleError};

#[derive(Clone)]
enum Source {
    Fixed(Arc<Mutex<Cache>>),
    Managed(Arc<lifecycle::State>),
}

// Separate cache ownership allows a supervisor to restart just the pool
// worker while retaining the original independent gossip-cache lifetime.
#[derive(Clone)]
pub struct GossipCache {
    source: Source,
    timeout: Duration,
}
impl Default for GossipCache {
    fn default() -> Self {
        Self {
            source: Source::Fixed(Default::default()),
            timeout: Duration::from_secs(3),
        }
    }
}
impl GossipCache {
    pub fn new(capacity: usize, limit: usize) -> Result<Self, ae_mempool::gossip::Error> {
        Ok(Self {
            source: Source::Fixed(Arc::new(Mutex::new(Cache::new(capacity, limit)?))),
            timeout: Duration::from_secs(3),
        })
    }
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    fn instance(&self) -> Result<Arc<Mutex<Cache>>, Error> {
        match &self.source {
            Source::Fixed(cache) => Ok(cache.clone()),
            Source::Managed(state) => state.current(),
        }
    }
    fn lock<'a>(
        &self,
        instance: &'a Mutex<Cache>,
        recover: bool,
    ) -> Result<std::sync::MutexGuard<'a, Cache>, Error> {
        let start = Instant::now();
        loop {
            if let Source::Managed(state) = &self.source
                && !state.is_current(instance)
            {
                return Err(Error::CacheUnavailable);
            }
            match instance.try_lock() {
                Ok(guard) => {
                    if let Source::Managed(state) = &self.source
                        && !state.is_current(instance)
                    {
                        return Err(Error::CacheUnavailable);
                    }
                    return Ok(guard);
                }
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
        let instance = self.instance()?;
        self.lock(&instance, false).map(|c| c.stats().clone())
    }
    pub fn reconfigure(&self, capacity: usize) -> Result<(), Error> {
        let instance = self.instance()?;
        let mut cache = self.lock(&instance, false)?;
        match &self.source {
            Source::Fixed(_) => cache.reconfigure(capacity).map_err(Error::Cache),
            Source::Managed(state) => state.reconfigure(&instance, &mut cache, capacity),
        }
    }
    pub fn reset(&self) -> Result<(), Error> {
        let instance = self.instance()?;
        let mut cache = self.lock(&instance, matches!(self.source, Source::Fixed(_)))?;
        cache.reset();
        instance.clear_poison();
        Ok(())
    }
    // Checking records a miss, matching the native shared first-level filter.
    pub fn check(&self, hash: ae_types::Hash256) -> bool {
        // Cache failure must not reject a transaction. The original API
        // returns a miss when its process is unavailable or its call times out.
        let Ok(instance) = self.instance() else {
            return false;
        };
        self.check_instance(&instance, hash)
    }
    fn check_instance(&self, instance: &Mutex<Cache>, hash: ae_types::Hash256) -> bool {
        self.lock(instance, false)
            .map(|mut c| c.check(hash))
            .unwrap_or(false)
    }
}
impl ae_mempool::gossip::Transactional for GossipCache {
    fn transact(&self, operation: ae_mempool::gossip::Transaction<'_>) {
        let instance = self.instance().ok();
        let mut guard = None;
        let mut staged = None;
        let mut attempted = false;
        // Do not lock/clone for callbacks that never check a transaction hash
        // (notably parent/finalization I/O). At most one bounded lock attempt
        // is made per callback. A retired instance never overwrites its successor.
        let commit = operation(&mut |hash| {
            if !attempted {
                attempted = true;
                guard = instance
                    .as_ref()
                    .and_then(|cache| self.lock(cache, false).ok());
                staged = guard.as_deref().cloned();
            }
            staged.as_mut().is_some_and(|cache| cache.check(hash))
        });
        if commit && let (Some(mut guard), Some(staged)) = (guard, staged) {
            *guard = staged;
        }
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
