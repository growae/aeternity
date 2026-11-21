use super::*;
use std::sync::Weak;

pub(super) struct State(Mutex<Inner>);
struct Inner {
    template: Cache,
    current: Option<Arc<Mutex<Cache>>>,
    closed: bool,
}
impl State {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // The binding lock never contains cache operations or caller code.
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub(super) fn current(&self) -> Result<Arc<Mutex<Cache>>, Error> {
        self.lock().current.clone().ok_or(Error::CacheUnavailable)
    }
    pub(super) fn is_current(&self, instance: &Mutex<Cache>) -> bool {
        self.lock()
            .current
            .as_ref()
            .is_some_and(|c| std::ptr::eq(&**c, instance))
    }
    pub(super) fn reconfigure(
        &self,
        instance: &Arc<Mutex<Cache>>,
        cache: &mut Cache,
        capacity: usize,
    ) -> Result<(), Error> {
        let mut inner = self.lock();
        if inner
            .current
            .as_ref()
            .is_none_or(|c| !Arc::ptr_eq(c, instance))
        {
            return Err(Error::CacheUnavailable);
        }
        inner.template.reconfigure(capacity).map_err(Error::Cache)?;
        cache.reconfigure(capacity).map_err(Error::Cache)
    }
    fn retire(&self, instance: &Arc<Mutex<Cache>>) {
        let mut inner = self.lock();
        if inner
            .current
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, instance))
        {
            inner.current = None;
        }
    }
    fn running(&self, instance: &Arc<Mutex<Cache>>) -> bool {
        !instance.is_poisoned()
            && self
                .lock()
                .current
                .as_ref()
                .is_some_and(|c| Arc::ptr_eq(c, instance))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleError {
    AlreadyRunning,
    Closed,
}
// A single controller creates cache incarnations. Cloned lookup clients have
// no authority to install one and never retain a mutable cache across restart.
pub struct CacheController(Arc<State>);
pub struct CacheOwner {
    state: Arc<State>,
    instance: Arc<Mutex<Cache>>,
}
#[derive(Clone)]
pub struct CacheHandle {
    state: Weak<State>,
    instance: Weak<Mutex<Cache>>,
}
impl CacheController {
    pub fn shutdown(&self) {
        let mut inner = self.0.lock();
        inner.closed = true;
        inner.current = None;
    }
    pub fn uses_cache(&self, cache: &GossipCache) -> bool {
        matches!(&cache.source, Source::Managed(state) if Arc::ptr_eq(&self.0, state))
    }
    pub fn start(&mut self) -> Result<CacheOwner, LifecycleError> {
        let mut inner = self.0.lock();
        if inner.closed {
            return Err(LifecycleError::Closed);
        }
        if inner.current.is_some() {
            return Err(LifecycleError::AlreadyRunning);
        }
        let instance = Arc::new(Mutex::new(inner.template.clone()));
        inner.current = Some(instance.clone());
        Ok(CacheOwner {
            state: self.0.clone(),
            instance,
        })
    }
}
impl Drop for CacheController {
    fn drop(&mut self) {
        self.shutdown();
    }
}
impl CacheOwner {
    pub fn handle(&self) -> CacheHandle {
        CacheHandle {
            state: Arc::downgrade(&self.state),
            instance: Arc::downgrade(&self.instance),
        }
    }
    pub fn is_running(&self) -> bool {
        self.state.running(&self.instance)
    }
    pub fn shutdown(&self) {
        self.state.retire(&self.instance);
    }
}
impl Drop for CacheOwner {
    fn drop(&mut self) {
        self.shutdown();
    }
}
impl CacheHandle {
    pub fn is_running(&self) -> bool {
        match (self.state.upgrade(), self.instance.upgrade()) {
            (Some(state), Some(instance)) => state.running(&instance),
            _ => false,
        }
    }
    pub fn shutdown(&self) {
        if let (Some(state), Some(instance)) = (self.state.upgrade(), self.instance.upgrade()) {
            state.retire(&instance);
        }
    }
}
impl GossipCache {
    pub fn managed(
        capacity: usize,
        limit: usize,
    ) -> Result<(Self, CacheController), ae_mempool::gossip::Error> {
        let state = Arc::new(State(Mutex::new(Inner {
            template: Cache::new(capacity, limit)?,
            current: None,
            closed: false,
        })));
        Ok((
            Self {
                source: Source::Managed(state.clone()),
                timeout: Duration::from_secs(3),
            },
            CacheController(state),
        ))
    }
}
