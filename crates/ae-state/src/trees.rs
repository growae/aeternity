use crate::{
    StateRoots, accounts::Accounts, calls::Calls, contracts::Contracts, names::Names,
    oracles::Oracles,
};
use ae_trie::{MemoryStore, NodeReader, Trie};

// A complete logical state includes the auxiliary expiry tries, even though
// they are excluded from the consensus commitment. Durable publication must
// retain all of these roots together.
#[derive(Debug, Clone)]
pub struct Trees<A = MemoryStore, S = MemoryStore> {
    pub accounts: Accounts<A>,
    pub calls: Calls<S>,
    pub channels: Trie<S>,
    pub contracts: Contracts<S>,
    pub names: Names<S, S>,
    pub oracles: Oracles<S, S>,
}
impl<A: NodeReader, S: NodeReader> Trees<A, S> {
    pub fn roots(&self) -> StateRoots {
        StateRoots {
            accounts: self.accounts.root_hash(),
            calls: self.calls.root_hash(),
            channels: self.channels.root_hash(),
            contracts: self.contracts.root_hash(),
            names: self.names.root_hash(),
            oracles: self.oracles.root_hash(),
        }
    }
}
