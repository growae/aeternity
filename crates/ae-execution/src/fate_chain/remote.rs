use super::*;
use ae_trie::{MemoryStore, Nibbles};
use ae_vm_fate::{
    machine::{
        Exception,
        program::remote::context::{AddressTest, Environment},
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
    pub oracles: ae_state::oracles::Oracles<C, C>,
    pub oracle_reads: super::oracle::Cache,
    pub onchain: Option<Box<View<A, C>>>,
    pub environment: Environment,
    pub history: Arc<super::history::Snapshot>,
}
#[derive(Debug, Clone)]
pub struct View<A = MemoryStore, C = MemoryStore> {
    pub accounts: Accounts<A>,
    pub contracts: Contracts<C>,
    pub oracles: ae_state::oracles::Oracles<C, C>,
    pub oracle_reads: super::oracle::Cache,
}
fn key<S: NodeReader>(
    tree: &ae_trie::Trie<S>,
    key: &[u8],
    error: fn(Vec<u8>) -> Error,
    tree_error: fn(ae_trie::Error) -> Error,
) -> Result<Option<[u8; 32]>, HostFailure<Error>> {
    match key.try_into() {
        Ok(k) => Ok(Some(k)),
        Err(_) => match tree
            .get(&Nibbles::from_bytes(key))
            .map_err(|e| HostFailure::Backend(tree_error(e)))?
        {
            None => Ok(None),
            Some(_) => Err(HostFailure::Backend(error(key.to_vec()))),
        },
    }
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
            Transfer::Rejected(TransferFailure::AccountNotPayable) => {
                unreachable!("value transfer ignores payability")
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
    fn spend(
        &mut self,
        from: [u8; 32],
        to: &[u8],
        value: &BigInt,
    ) -> Result<(), HostFailure<Error>> {
        let to = <[u8; 32]>::try_from(to)
            .map_err(|_| HostFailure::Exception(Exception::Native("function_clause")))?;
        if value.sign() == Sign::Minus {
            return Err(HostFailure::Exception(Exception::Native("function_clause")));
        }
        let amount =
            Unsigned::from_field_bytes(&value.to_bytes_be().1).expect("canonical magnitude");
        match super::spend(
            &mut self.accounts,
            AccountPublicKey(from),
            AccountPublicKey(to),
            &amount,
        )
        .map_err(failure)?
        {
            Transfer::Transferred(events) => {
                self.events.splice(0..0, *events);
                Ok(())
            }
            Transfer::Rejected(r) => Err(HostFailure::Primop {
                operation: "spend",
                reason: match r {
                    TransferFailure::AccountNotFound => "account_not_found",
                    TransferFailure::InsufficientFunds => "insufficient_funds",
                    TransferFailure::AccountNotPayable => "account_is_not_payable",
                },
            }),
        }
    }
    fn oracle_read(
        &mut self,
        request: ae_vm_fate::machine::program::remote::hosts::OracleRead<'_>,
        limits: Limits,
    ) -> Result<ae_vm_fate::value::Value, HostFailure<Error>> {
        match &mut self.onchain {
            Some(v) => super::oracle::read(&v.oracles, &mut v.oracle_reads, request, limits),
            None => super::oracle::read(&self.oracles, &mut self.oracle_reads, request, limits),
        }
    }
    fn environment(&self) -> &Environment {
        &self.environment
    }
    fn balance(&mut self, k: &[u8]) -> Result<Option<BigInt>, HostFailure<Error>> {
        let local = key(self.accounts.tree(), k, Error::UnsupportedAccountKey, |e| {
            Error::Accounts(e.into())
        })?;
        let amount = if let Some(k) = local {
            account_balance(
                &self.accounts,
                self.onchain.as_ref().map(|v| &v.accounts),
                AccountPublicKey(k),
            )
            .map_err(failure)?
        } else {
            if let Some(v) = &self.onchain {
                key(v.accounts.tree(), k, Error::UnsupportedAccountKey, |e| {
                    Error::Accounts(e.into())
                })?;
            }
            None
        };
        Ok(amount.map(|n| BigInt::from_bytes_be(Sign::Plus, n.as_bytes())))
    }
    fn creator(&mut self, k: [u8; 32]) -> Result<[u8; 32], HostFailure<Error>> {
        self.contracts
            .record(k)
            .map_err(|e| failure(e.into()))?
            .map(|c| c.owner.0)
            .ok_or(HostFailure::Exception(Exception::Native("badmatch")))
    }
    fn address_test(&mut self, what: AddressTest, k: &[u8]) -> Result<bool, HostFailure<Error>> {
        if matches!(what, AddressTest::Oracle) {
            return match &mut self.onchain {
                Some(v) => {
                    super::oracle::lookup(&v.oracles, &mut v.oracle_reads, k).map(|v| v.is_some())
                }
                None => super::oracle::lookup(&self.oracles, &mut self.oracle_reads, k)
                    .map(|v| v.is_some()),
            };
        }
        let (accounts, contracts) = match &self.onchain {
            Some(v) => (&v.accounts, &v.contracts),
            None => (&self.accounts, &self.contracts),
        };
        match what {
            AddressTest::Payable => Ok(
                match key(accounts.tree(), k, Error::UnsupportedAccountKey, |e| {
                    Error::Accounts(e.into())
                })? {
                    Some(k) => accounts
                        .lookup(AccountPublicKey(k))
                        .map_err(|e| failure(e.into()))?
                        .is_none_or(|a| a.is_payable()),
                    None => true,
                },
            ),
            AddressTest::Contract => Ok(
                match key(contracts.tree(), k, Error::UnsupportedContractKey, |e| {
                    Error::Contracts(e.into())
                })? {
                    Some(k) => contracts
                        .record(k)
                        .map_err(|e| failure(e.into()))?
                        .is_some(),
                    None => false,
                },
            ),
            AddressTest::Oracle => unreachable!(),
        }
    }
    fn block_hash(
        &mut self,
        height: u64,
        vm: Version,
        limits: Limits,
    ) -> Result<[u8; 32], HostFailure<Error>> {
        super::history::block_hash(
            &self.history,
            height,
            self.environment.key_hash,
            vm,
            limits.max_nodes,
        )
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
