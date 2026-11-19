// aefa_chain_api:contract_store and aeprimop_state:get_contract, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{
    Error,
    remote::{State, Store},
};
use ae_state::contracts::{self, store::Store as ContractStore};
use ae_trie::{Nibbles, NodeReader, OwnedReadView, Trie};
use ae_vm_fate::{
    Limits,
    machine::{Exception, program::remote::HostFailure},
};

impl<A: NodeReader, C: NodeReader + Clone> State<A, C> {
    // Supply an immutable store snapshot at the executor's NeedContract
    // boundary. The retried VM instruction performs store_loaded at its
    // original checkpoint; supplying bytes here does not populate that cache.
    pub fn contract_store_source(
        &self,
        key: [u8; 32],
        limits: Limits,
    ) -> Result<(Store<OwnedReadView<C>>, Limits), HostFailure<Error>> {
        let mut remaining = self.contract_cache.budget(limits)?;
        remaining.max_nodes = remaining
            .max_nodes
            .checked_sub(1)
            .ok_or(HostFailure::ResourceLimit)?;
        remaining.max_bytes = remaining
            .max_bytes
            .checked_sub(32)
            .ok_or(HostFailure::ResourceLimit)?;
        let pending = self.contract_cache.pending.get(&key);
        let existing = self
            .contracts
            .is_contract(key)
            .map_err(|e| HostFailure::Backend(Error::Contracts(e)))?;
        if !existing && pending.is_none() {
            return Err(HostFailure::Exception(Exception::Native("aeprimop_state")));
        }
        let tree = if existing {
            self.contracts
                .tree()
                .owned_read_only_subtree(&Nibbles::from_bytes(&contracts::store_id(key)))
                .map_err(|e| HostFailure::Backend(Error::Contracts(e.into())))?
        } else {
            Trie::new(OwnedReadView::new(self.contracts.tree().store().clone()))
        };
        let mut store = ContractStore::new(tree);
        if let Some(pending) = pending {
            store.put_map(pending.writes.clone());
        }
        Ok((Store(crate::fate_store::Backend(store)), remaining))
    }
}
