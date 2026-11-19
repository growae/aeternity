// aec_vm_chain get_balance and address predicates, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use ae_state::{
    accounts::{AccountError, Accounts},
    contracts::{self, Contracts},
    oracles::Oracles,
};
use ae_trie::{MemoryStore, Nibbles, NodeReader};
use ae_types::{AccountPublicKey, Unsigned};
use ae_vm_aevm::{primops::AddressKind, word::Word};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Accounts(AccountError),
    Contracts(contracts::Error),
    Oracles(ae_trie::Error),
    OracleState(ae_state::oracles::Error),
    Names(ae_state::names::Error),
    Primitive(Box<crate::Error>),
}
mod contract_types;
pub use contract_types::decode_code;
mod name;
mod name_write;
mod oracle;
mod oracle_write;
mod state;
pub(crate) use state::event_size;
pub use state::{EnvironmentEvent, EventContext, Host, State};
pub struct View<'a, A = MemoryStore, C = MemoryStore> {
    pub accounts: &'a Accounts<A>,
    pub contracts: &'a Contracts<C>,
    pub oracles: &'a Oracles<C, C>,
}
// The outer trees are visible to the invocation. A channel invocation also
// supplies its on-chain trees; address predicates always read that inner view.
pub struct Reads<'a, A = MemoryStore, C = MemoryStore> {
    pub top: View<'a, A, C>,
    pub onchain: Option<View<'a, A, C>>,
}
impl<A: NodeReader, C: NodeReader> Reads<'_, A, C> {
    pub fn balance(&self, key: AccountPublicKey) -> Result<Unsigned, Error> {
        if let Some(account) = self.top.accounts.lookup(key).map_err(Error::Accounts)? {
            return Ok(account.balance().clone());
        }
        if let Some(inner) = &self.onchain
            && let Some(account) = inner.accounts.lookup(key).map_err(Error::Accounts)?
        {
            return Ok(account.balance().clone());
        }
        Ok(0.into())
    }
    pub fn address(&self, kind: AddressKind, key: Word) -> Result<Word, Error> {
        let trees = self.onchain.as_ref().unwrap_or(&self.top);
        let yes = match kind {
            AddressKind::Payable => trees
                .accounts
                .lookup(AccountPublicKey(key.0))
                .map_err(Error::Accounts)?
                .is_none_or(|a| a.is_payable()),
            AddressKind::Contract => trees
                .contracts
                .is_contract(key.0)
                .map_err(Error::Contracts)?,
            // The original only tests presence, without decoding oracle records.
            AddressKind::Oracle => trees
                .oracles
                .tree()
                .get(&Nibbles::from_bytes(&key.0))
                .map_err(Error::Oracles)?
                .is_some(),
        };
        Ok(Word::from(u64::from(yes)))
    }
}
