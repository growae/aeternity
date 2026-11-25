use super::{Prepared, Settings};
use crate::configuration::{
    resolution::Resolved,
    runtime::{Core, Error},
};
use ae_sync::{keys, runtime::workers};

fn keys(core: &Core, target: &mut Option<keys::Config>) {
    let mut settings = core.keys.clone();
    if let Some(old) = target {
        settings.requests = old.requests;
    }
    *target = Some(settings);
}
fn gc(core: &Core, target: &mut Option<workers::GarbageCollection>) {
    target.get_or_insert_default().policy = core.gc;
}
fn pool(core: &Core, target: &mut ae_storage::pool::Config) {
    // Consensus and resource limits are independent of operational pool policy.
    target.policy = core.pool.clone();
    target.failures = core.failures.clone();
}
fn resilience(core: &Core, target: &mut Option<ae_sync::resilience::Config>) {
    target.get_or_insert_default().initial = core.resilience;
}
fn pow<P>(core: &Core, target: &mut crate::pow::Config<P>) -> Result<(), Error> {
    // Validate before changing any caller-owned settings.
    ae_mempool::gossip::Cache::new(core.cache_size, target.cache_limit).map_err(Error::Cache)?;
    target.cache_entries = core.cache_size;
    keys(core, &mut target.workers.keys);
    gc(core, &mut target.workers.gc);
    pool(core, &mut target.workers.pool);
    resilience(core, &mut target.workers.resilience);
    target.connections.sync.resilience = core.resilience;
    Ok(())
}
impl Settings {
    pub fn apply_network(&mut self, network: &crate::configuration::network::Network) {
        match self {
            Self::Cuckoo(s) => network.apply(&mut s.config.connections),
            Self::OnDemand(s) => network.apply(&mut s.config.connections),
            Self::Hyperchain(s) => network.apply(&mut s.config.connections),
        }
    }
    pub fn apply_core(&mut self, core: &Core) -> Result<(), Error> {
        match self {
            Self::Cuckoo(s) => pow(core, &mut s.config),
            Self::OnDemand(s) => pow(core, &mut s.config),
            Self::Hyperchain(s) => {
                if !s.cache.uses_cache(&s.runtime.gossip) {
                    return Err(Error::CacheBinding);
                }
                s.cache
                    .configure_idle(core.cache_size)
                    .map_err(Error::CacheConfiguration)?;
                keys(core, &mut s.config.workers.keys);
                gc(core, &mut s.config.workers.gc);
                pool(core, &mut s.runtime.pool);
                resilience(core, &mut s.runtime.resilience);
                s.config.connections.sync.resilience = core.resilience;
                // The independent maintenance child borrows conductor admission.
                // Its presence must not also enable admission's internal timer.
                if s.config.workers.pool_gc.is_some() {
                    s.runtime.pool_worker.origins_interval = None;
                }
                Ok(())
            }
        }
    }
}
impl Prepared {
    pub fn configure_optional_network(
        &self,
        resolved: &Resolved<'_>,
        settings: &mut Settings,
    ) -> Result<(), crate::configuration::network::Error> {
        use crate::configuration::network::{Error, Optional};
        if self.family() != settings.family() {
            return Err(Error::Family);
        }
        let optional = Optional::resolve(resolved)?;
        match settings {
            Settings::Cuckoo(s) => optional.apply(&mut s.config.workers, &mut s.config.connections),
            Settings::OnDemand(s) => {
                optional.apply(&mut s.config.workers, &mut s.config.connections)
            }
            Settings::Hyperchain(s) => {
                optional.apply_hyperchain(&mut s.config.workers, &mut s.config.connections)
            }
        }
        Ok(())
    }
    pub fn configure_discovery(
        &mut self,
        resolved: &Resolved<'_>,
        settings: &mut Settings,
    ) -> Result<(), crate::configuration::discovery::Error> {
        use crate::configuration::discovery::{Discovery, Error};
        if self.family() != settings.family() {
            return Err(Error::Family);
        }
        let discovery = Discovery::resolve(resolved)?;
        match settings {
            Settings::Cuckoo(s) => discovery.apply(&mut s.config.connections)?,
            Settings::OnDemand(s) => discovery.apply(&mut s.config.connections)?,
            Settings::Hyperchain(s) => {
                discovery.apply(&mut s.config.connections)?;
                s.runtime.admission.ledger.policy.gossip_depth = discovery.gossip_depth();
            }
        }
        if let super::PreparedInner::Pow(prepared) = &mut self.0 {
            prepared.set_gossip_depth(discovery.gossip_depth());
        }
        Ok(())
    }
    pub fn configure_network(
        &self,
        resolved: &Resolved<'_>,
        settings: &mut Settings,
    ) -> Result<(), crate::configuration::network::Error> {
        if self.family() != settings.family() {
            return Err(crate::configuration::network::Error::Family);
        }
        let network = crate::configuration::network::Network::resolve(resolved)?;
        settings.apply_network(&network);
        Ok(())
    }
    pub fn configure_core(
        &self,
        resolved: &Resolved<'_>,
        settings: &mut Settings,
    ) -> Result<(), Error> {
        if self.family() != settings.family() {
            return Err(Error::Family);
        }
        let core = Core::resolve(resolved, self.configured().startup_directory())?;
        settings.apply_core(&core)
    }
}
