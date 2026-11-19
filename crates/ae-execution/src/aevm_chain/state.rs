// aec_vm_chain spend_tx/apply_transaction and store callbacks, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use ae_state::Trees;
use ae_trie::NodeWriter;
use ae_tx::{Spend, TransactionKind};
use ae_types::{Hash256, Id};
use ae_vm_aevm::{
    execution::{Budget, Stop},
    maps::Store,
    primops::{ChainHost, HostResult},
    runtime::{Fault, Native, StoreHost},
};
use num_bigint::{BigInt, Sign};
use std::{marker::PhantomData, sync::Arc};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum EventContext {
    #[default]
    None,
    Transaction {
        kind: TransactionKind,
        hash: Hash256,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentEvent {
    pub event: crate::Event,
    pub context: EventContext,
}
// These roots and environment events belong to one invocation. Returning this
// snapshot never publishes durable roots; parent failure discards it in full.
#[derive(Debug, Clone)]
pub struct State<A = MemoryStore, C = MemoryStore> {
    pub trees: Trees<A, C>,
    pub onchain: Option<Arc<Trees<A, C>>>,
    pub account: AccountPublicKey,
    pub context: Arc<crate::Context>,
    pub event_context: EventContext,
    pub events: Arc<Vec<EnvironmentEvent>>,
}
impl<A: NodeReader, C: NodeReader> State<A, C> {
    pub fn reads(&self) -> Reads<'_, A, C> {
        fn view<A, C>(t: &Trees<A, C>) -> View<'_, A, C> {
            View {
                accounts: &t.accounts,
                contracts: &t.contracts,
                oracles: &t.oracles,
            }
        }
        Reads {
            top: view(&self.trees),
            onchain: self.onchain.as_deref().map(view),
        }
    }
}
pub struct Host<A = MemoryStore, C = MemoryStore>(PhantomData<fn() -> (A, C)>);
impl<A, C> Default for Host<A, C> {
    fn default() -> Self {
        Self(PhantomData)
    }
}
fn native(reason: &'static str) -> Fault<Error> {
    Fault::Native(Native::ChainException {
        class: "error",
        reason,
    })
}
fn result<T>(e: crate::Error) -> HostResult<T, Error> {
    use crate::Error as E;
    let reason = match e {
        E::InvalidAtProtocol => "invalid_at_protocol",
        E::BadTransaction => "bad_transaction",
        E::TooLowFee => "too_low_fee",
        E::TtlExpired => "ttl_expired",
        E::AccountNotFound => "account_not_found",
        E::NotBasicAccount => "not_a_basic_account",
        E::NotGeneralizedAccount => "not_a_generalized_account",
        E::GeneralizedAccountsUnavailable => "generalize_accounts_not_available_at_protocol",
        E::GaNonceMustBeZero => "nonce_in_ga_tx_should_be_zero",
        E::BadGaEnvironment => "bad_ga_tx_env",
        E::NonceAlreadyUsed => "tx_nonce_already_used_for_account",
        E::NonceTooHigh => "tx_nonce_too_high_for_account",
        E::InsufficientFunds => "insufficient_funds",
        E::AccountNotPayable => "account_is_not_payable",
        E::ReferenceException { class, reason } => {
            return Err(Fault::Native(Native::ChainException { class, reason }));
        }
        E::State(e) => return Err(Fault::Host(Error::Accounts(e))),
        e => return Err(Fault::Host(Error::Primitive(Box::new(e)))),
    };
    Ok(Err(reason.into()))
}
impl<A: NodeReader, C: NodeReader> Host<A, C> {
    // None preserves aec_vm_chain's missing-contract sentinel, which is not an
    // aect_contracts_store record and must not masquerade as an empty store.
    pub fn store(&self, chain: &State<A, C>) -> Result<Option<Store>, Fault<Error>> {
        let key = chain.account.0;
        let loaded = chain
            .trees
            .contracts
            .lookup(key, contracts::LoadOptions::default())
            .map_err(|e| Fault::Host(Error::Contracts(e)))?;
        loaded
            .map(|c| {
                c.store
                    .expect("store requested")
                    .contents()
                    .map_err(|e| Fault::Host(Error::Contracts(e)))
            })
            .transpose()
    }
    pub fn prepare(
        &self,
        recipient: Id,
        value: &BigInt,
        chain: &State<A, C>,
        budget: &mut Budget,
    ) -> HostResult<Spend, Error> {
        budget.work(1)?;
        let account = chain
            .trees
            .accounts
            .lookup(chain.account)
            .map_err(|e| Fault::Host(Error::Accounts(e)))?
            .ok_or(Fault::Native(Native::BadMatchNone))?;
        budget.bytes(account.nonce().as_bytes().len())?;
        let nonce = account.nonce().add(&1.into());
        if value.sign() == Sign::Minus {
            return Err(native("function_clause"));
        }
        budget
            .bytes(usize::try_from(value.bits().div_ceil(8)).map_err(|_| Stop::ResourceLimit)?)?;
        let amount =
            Unsigned::from_field_bytes(&value.to_bytes_be().1).expect("nonnegative magnitude");
        let mut tx = Spend::new(chain.account, recipient).map_err(|_| native("function_clause"))?;
        tx.set_nonce(nonce);
        tx.set_amount(amount);
        budget.bytes(tx.wire_size())?;
        Ok(Ok(tx))
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> Host<A, C> {
    pub fn apply(
        &self,
        tx: &Spend,
        chain: &State<A, C>,
        budget: &mut Budget,
    ) -> HostResult<State<A, C>, Error> {
        budget.work(1)?;
        budget.bytes(tx.wire_size())?;
        let mut next = chain.clone();
        let receipt = match crate::process_spend(
            tx,
            &mut next.trees.accounts,
            &next.trees.names,
            &next.context,
        ) {
            Ok(r) => r,
            Err(e) => return result(e),
        };
        let event_size = |e: &crate::Event| match e {
            crate::Event::Spend { amount, .. }
            | crate::Event::FeeDebit { amount, .. }
            | crate::Event::OracleQueryFee { amount, .. }
            | crate::Event::OracleRefund { amount, .. }
            | crate::Event::NameDebit { amount, .. }
            | crate::Event::NameRefund { amount, .. }
            | crate::Event::NameFeeLocked { amount } => {
                128usize.saturating_add(amount.as_bytes().len())
            }
        };
        budget.work(
            chain
                .events
                .len()
                .checked_add(receipt.events.len())
                .ok_or(Stop::ResourceLimit)?,
        )?;
        for e in chain
            .events
            .iter()
            .map(|e| &e.event)
            .chain(receipt.events.iter())
        {
            budget.bytes(event_size(e))?;
        }
        let mut events: Vec<_> = receipt
            .events
            .into_iter()
            .map(|event| EnvironmentEvent {
                event,
                context: chain.event_context.clone(),
            })
            .collect();
        events.extend(chain.events.iter().cloned());
        next.events = Arc::new(events);
        Ok(Ok(next))
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> StoreHost for Host<A, C> {
    type Chain = State<A, C>;
    type Error = Error;
    fn set_store(&self, store: Store, chain: &Self::Chain) -> Result<Self::Chain, Fault<Error>> {
        let contract = chain
            .trees
            .contracts
            .record(chain.account.0)
            .map_err(|e| Fault::Host(Error::Contracts(e)))?
            .ok_or(Fault::Native(Native::CaseClauseNone))?;
        if contract.validate_writes(&store).is_err() {
            return Err(Fault::Native(Native::IllegalStore(store)));
        }
        let mut next = chain.clone();
        next.trees
            .contracts
            .enter(&contract, &store)
            .map_err(|e| Fault::Host(Error::Contracts(e)))?;
        Ok(next)
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> ChainHost for Host<A, C> {
    type Spend = Spend;
    fn get_store(&self, chain: &Self::Chain) -> Result<Store, Fault<Error>> {
        self.store(chain)?.ok_or_else(|| native("function_clause"))
    }
    fn prepare_spend(
        &self,
        recipient: Id,
        value: &BigInt,
        chain: &Self::Chain,
        budget: &mut Budget,
    ) -> HostResult<Spend, Error> {
        self.prepare(recipient, value, chain, budget)
    }
    fn apply_spend(
        &self,
        spend: Spend,
        chain: &Self::Chain,
        budget: &mut Budget,
    ) -> HostResult<Self::Chain, Error> {
        self.apply(&spend, chain, budget)
    }
    fn address(
        &self,
        kind: AddressKind,
        address: Word,
        chain: &Self::Chain,
        budget: &mut Budget,
    ) -> HostResult<Word, Error> {
        budget.work(1)?;
        Ok(Ok(chain
            .reads()
            .address(kind, address)
            .map_err(Fault::Host)?))
    }
}
