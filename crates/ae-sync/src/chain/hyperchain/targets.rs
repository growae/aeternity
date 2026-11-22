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
    identity: repo::Identity,
    profile: Arc<chain::Profile<O>>,
    query: repo::hyperchain::Config,
    length: Unsigned,
    start: Unsigned,
}
impl<O> Live<O> {
    pub fn new(
        source: &Arc<eoe::Source>,
        execution: &repo::HyperchainConfig<O>,
    ) -> Result<Self, eoe::Error> {
        if !source.matches(&execution.identity, &execution.profile.network) {
            return Err(eoe::Error::Identity);
        }
        Ok(Self {
            source: Arc::downgrade(source),
            identity: execution.identity.clone(),
            profile: execution.profile.clone(),
            query: repo::hyperchain::Config {
                overlays: execution.limits.overlay,
                maximum_history_headers: execution.limits.history,
                configured_block_gas_limit: execution.rules.configured_gas_limit.clone(),
                aevm: execution.aevm.clone(),
            },
            length: execution.rules.consensus.parent_epoch_length.clone(),
            start: execution.rules.consensus.parent_start.clone(),
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
        let mut profile = self.profile.as_ref().clone();
        profile.network = view
            .protocol_profile(
                &profile.network,
                height_u64,
                self.query.maximum_history_headers,
            )
            .map_err(|_| TargetError::Unavailable)?;
        let reader =
            repo::hyperchain::reader(view, &self.identity, Arc::new(profile), self.query.clone())
                .map_err(|_| TargetError::Unavailable)?;
        chain::ParentTargets {
            reader,
            epoch_length: self.length.clone(),
            start_height: self.start.clone(),
        }
        .targets(height)
    }
}
