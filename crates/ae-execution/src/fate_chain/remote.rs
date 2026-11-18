use super::*;
use ae_trie::{MemoryStore, Nibbles};
use ae_vm_fate::{
    machine::{
        Exception,
        program::remote::{Host, HostFailure, LoadedCode},
    },
    store::{
        Backend,
        read::CachedSubtreeBackend,
        write::{RawMap, SubtreeBackend},
    },
};
use num_bigint::{BigInt, Sign};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct State<A = MemoryStore, C = MemoryStore> {
    pub accounts: Accounts<A>,
    pub contracts: Contracts<C>,
    pub height: u64,
    pub events: Vec<TransferEvent>,
}
fn failure(error: Error) -> HostFailure<Error> {
    match error {
        Error::ResourceLimit => HostFailure::ResourceLimit,
        e => HostFailure::Backend(e),
    }
}
impl<A: NodeWriter + Clone, C: NodeReader + Clone> Host<Error> for State<A, C> {
    fn code(
        &mut self,
        key: &[u8],
        limits: Limits,
    ) -> Result<Option<LoadedCode>, HostFailure<Error>> {
        let Ok(fixed) = <[u8; 32]>::try_from(key) else {
            return match self
                .contracts
                .tree()
                .get(&Nibbles::from_bytes(key))
                .map_err(|e| failure(Error::Contracts(e.into())))?
            {
                None => Ok(None),
                Some(_) => Err(HostFailure::Backend(Error::UnsupportedContractKey(
                    key.to_vec(),
                ))),
            };
        };
        match contract_fate_bytecode(&self.contracts, fixed, limits).map_err(failure)? {
            CodeLookup::Loaded(code) => Ok(Some(LoadedCode {
                code: Arc::new(code.code),
                vm_version: code.vm_version,
            })),
            CodeLookup::Unavailable => Ok(None),
            CodeLookup::Exception(e) => Err(HostFailure::Exception(Exception::Host {
                operation: "contract_fate_bytecode",
                detail: format!("{e:?}"),
            })),
        }
    }
    fn transfer(
        &mut self,
        from: [u8; 32],
        to: [u8; 32],
        value: &BigInt,
    ) -> Result<(), HostFailure<Error>> {
        if value.sign() == Sign::Minus {
            return Err(HostFailure::Call("negative_value"));
        }
        let amount = Unsigned::from_field_bytes(&value.to_bytes_be().1)
            .expect("unsigned magnitude is canonical");
        match transfer_value(
            &mut self.accounts,
            AccountPublicKey(from),
            AccountPublicKey(to),
            &amount,
        )
        .map_err(failure)?
        {
            Transfer::Rejected(TransferFailure::AccountNotFound) => {
                Err(HostFailure::Call("account_not_found"))
            }
            Transfer::Rejected(TransferFailure::InsufficientFunds) => {
                Err(HostFailure::Call("insufficient_funds"))
            }
            Transfer::Transferred(events) => {
                self.events.splice(0..0, *events);
                Ok(())
            }
        }
    }
    fn height(&self) -> u64 {
        self.height
    }
}

// Unify state-host and VM store backend errors without erasing their typed
// causes or converting operational failures into a protected-call result.
#[derive(Debug, Clone)]
pub struct Store<S = MemoryStore>(pub crate::fate_store::Backend<S>);
impl<S: NodeReader + Clone> Backend for Store<S> {
    type Error = Error;
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, Error> {
        self.0.get(key).map_err(Error::Contracts)
    }
    fn get_cached(&mut self, key: &[u8]) -> Result<Vec<u8>, Error> {
        self.0.get_cached(key).map_err(Error::Contracts)
    }
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.0.put(key, value);
    }
}
impl<S: NodeReader + Clone> SubtreeBackend for Store<S> {
    fn subtree(
        &self,
        prefix: &[u8],
        maximum_bytes: usize,
    ) -> Result<Option<(RawMap, usize)>, Error> {
        self.0
            .subtree(prefix, maximum_bytes)
            .map_err(Error::Contracts)
    }
}
impl<S: NodeReader + Clone> CachedSubtreeBackend for Store<S> {
    fn subtree_cached(
        &mut self,
        prefix: &[u8],
        maximum_bytes: usize,
    ) -> Result<Option<(RawMap, usize)>, Error> {
        self.0
            .subtree_cached(prefix, maximum_bytes)
            .map_err(Error::Contracts)
    }
}
