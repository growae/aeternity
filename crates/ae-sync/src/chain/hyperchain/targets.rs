use super::eoe;
use ae_chain::repository as repo;
use ae_hyperchain::{
    chain,
    parent_cache::runtime::{TargetError, Targets},
};
use ae_types::Unsigned;
use ae_vm_fate::maps::MapOrder;
use num_bigint::BigInt;
use std::sync::{Arc, Weak};

// A callback captures the then-current canonical chain. Retaining this provider
// never pins the startup tip or keeps a retired selected-state owner alive.
#[derive(Clone)]
pub struct Live<O> {
    source: Weak<eoe::Source>,
    execution: repo::HyperchainConfig<O>,
}
impl<O: Clone> Live<O> {
    pub fn new(
        source: &Arc<eoe::Source>,
        execution: &repo::HyperchainConfig<O>,
    ) -> Result<Self, eoe::Error> {
        if !source.matches(&execution.identity, &execution.profile.network) {
            return Err(eoe::Error::Identity);
        }
        Ok(Self {
            source: Arc::downgrade(source),
            execution: execution.clone(),
        })
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Targets for Live<O> {
    fn targets(&mut self, height: &Unsigned) -> Result<Vec<BigInt>, TargetError> {
        let source = self.source.upgrade().ok_or(TargetError::Unavailable)?;
        let captured = source
            .snapshots()
            .view()
            .map_err(|_| TargetError::Unavailable)?;
        let view = captured.value;
        let height_u64 = height.to_u64().map_err(|_| TargetError::Native)?;
        // Missing heights are native query failures. Resolve any signalled
        // protocol only after establishing that the requested key exists.
        if view
            .canonical_key(height_u64)
            .map_err(|_| TargetError::Unavailable)?
            .is_none()
        {
            return Err(TargetError::Native);
        }
        let mut profile = self.execution.profile.as_ref().clone();
        profile.network = view
            .protocol_profile(&profile.network, height_u64, self.execution.limits.history)
            .map_err(|_| TargetError::Unavailable)?;
        let reader = self
            .execution
            .reader(view, Arc::new(profile))
            .map_err(|_| TargetError::Unavailable)?;
        chain::ParentTargets {
            reader,
            epoch_length: self.execution.rules.consensus.parent_epoch_length.clone(),
            start_height: self.execution.rules.consensus.parent_start.clone(),
        }
        .targets(height)
    }
}
