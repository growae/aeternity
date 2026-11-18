#![forbid(unsafe_code)]

pub mod accounts;
pub mod calls;
pub mod contracts;
pub mod genesis;
pub mod names;
pub mod oracles;
mod sext;

use ae_crypto::blake2b_256;
use ae_types::Hash256;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateRoots {
    pub accounts: Option<Hash256>,
    pub calls: Option<Hash256>,
    pub channels: Option<Hash256>,
    pub contracts: Option<Hash256>,
    pub names: Option<Hash256>,
    pub oracles: Option<Hash256>,
}
impl StateRoots {
    pub fn hash(&self) -> Hash256 {
        let mut bytes = Vec::with_capacity(200);
        bytes.extend_from_slice(&1u64.to_be_bytes());
        for root in [
            self.accounts,
            self.calls,
            self.channels,
            self.contracts,
            self.names,
            self.oracles,
        ] {
            bytes.extend_from_slice(root.unwrap_or(Hash256::ZERO).as_ref());
        }
        blake2b_256(&bytes)
    }
}
