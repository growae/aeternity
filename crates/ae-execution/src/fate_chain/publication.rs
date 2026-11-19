// aefa_chain_api:final_trees / aeprimop_state:cache_write_through, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{Error, remote::State};
use ae_state::{Trees, calls::Calls, channels::Channels};
use ae_trie::NodeWriter;
use ae_vm_fate::{
    Limits,
    machine::{Exception, program::remote::HostFailure},
};

impl<A: NodeWriter + Clone, C: NodeWriter + Clone> State<A, C> {
    // Project the local invocation cache without mutating it. Call/channel
    // trees are carried through unchanged; on-chain fallback caches are not
    // part of an off-chain invocation's output. This is not a durable commit
    // or transaction acceptance: the caller must select success/rollback.
    pub fn final_trees(
        &self,
        calls: &Calls<C>,
        channels: &Channels<C>,
        limits: Limits,
    ) -> Result<(Trees<A, C>, Limits), HostFailure<Error>> {
        let remaining = self.contract_cache.budget(limits)?;
        let remaining = self.name_cache.budget(remaining)?;
        let remaining = self.oracle_reads.budget(remaining)?;
        let contracts = self.contract_cache.materialize(&self.contracts, limits)?;
        // Earlier host operations may have projected intermediate cache
        // values for reads. Rebuild against their saved base, so superseded
        // TTLs are not added to the final expiry indexes.
        let mut names = self.name_base.as_ref().unwrap_or(&self.names).clone();
        self.name_cache
            .flush(&mut names)
            .map_err(|error| match error {
                HostFailure::Backend(Error::Names(ae_state::names::Error::AlreadyPresent)) => {
                    HostFailure::Exception(Exception::Native("already_present"))
                }
                error => error,
            })?;
        let mut oracles = self.oracle_base.as_ref().unwrap_or(&self.oracles).clone();
        self.oracle_reads.flush(&mut oracles)?;
        Ok((
            Trees {
                accounts: self.accounts.clone(),
                calls: calls.clone(),
                channels: channels.clone(),
                contracts,
                names,
                oracles,
            },
            remaining,
        ))
    }
}
