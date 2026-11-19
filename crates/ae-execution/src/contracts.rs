// aetx / aeprimop contract_call and aect_dispatch, fa2a6d7.
// Copyright (c) 2017, 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{Context, Origin, fate_chain};
use ae_state::{
    Trees,
    accounts::Accounts,
    calls::{Call, ReturnType},
    names::NamePointerResolver,
};
use ae_trie::{NodeReader, NodeWriter};
use ae_tx::{ContractOperation, ContractTransaction};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};
use ae_vm_fate::{
    Limits,
    machine::{Exception, Reason},
    maps::MapOrder,
};
use num_bigint::{BigInt, Sign};
use std::collections::BTreeMap;

mod create;
pub use create::{execute_signed_create, process_create};
pub mod aevm;
pub mod fate;

#[derive(Debug)]
pub enum Error {
    Primitive(crate::Error),
    State(ae_state::contracts::Error),
    Host(ae_vm_fate::machine::program::remote::HostFailure<fate_chain::Error>),
    Vm(ae_vm_fate::machine::Error<fate_chain::Error>),
    Exception(Exception),
    NativeException(Box<ae_vm_fate::native::Exception>),
    Rejected(&'static str),
    ResourceLimit,
    UnsupportedVm(u16),
    UnsupportedOperation,
    UnformattedDiagnostic(Reason),
    BytecodePreparation(ae_vm_fate::code::CodeError),
    UnsupportedLogPayload,
    InvalidVmGas,
    Suspended,
}
impl From<crate::Error> for Error {
    fn from(e: crate::Error) -> Self {
        Self::Primitive(e)
    }
}
impl From<ae_state::accounts::AccountError> for Error {
    fn from(e: ae_state::accounts::AccountError) -> Self {
        Self::Primitive(e.into())
    }
}
impl From<ae_state::contracts::Error> for Error {
    fn from(e: ae_state::contracts::Error) -> Self {
        Self::State(e)
    }
}
impl From<ae_state::names::Error> for Error {
    fn from(e: ae_state::names::Error) -> Self {
        Self::Primitive(e.into())
    }
}
impl From<ae_vm_fate::machine::Error<fate_chain::Error>> for Error {
    fn from(e: ae_vm_fate::machine::Error<fate_chain::Error>) -> Self {
        Self::Vm(e)
    }
}
impl From<ae_vm_fate::machine::program::remote::HostFailure<fate_chain::Error>> for Error {
    fn from(e: ae_vm_fate::machine::program::remote::HostFailure<fate_chain::Error>) -> Self {
        Self::Host(e)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Delta {
        account: AccountPublicKey,
        amount: Unsigned,
        debit: bool,
        info: &'static str,
    },
    Internal(fate_chain::TransferEvent),
    Aevm(crate::aevm_chain::EnvironmentEvent),
}
#[derive(Debug, Clone)]
pub struct Receipt {
    pub transaction_hash: Option<ae_types::Hash256>,
    pub call: Call,
    // Includes the transaction's base and incoming-byte gas in addition to VM gas.
    pub gas: Unsigned,
    pub events: Vec<Event>,
}

pub(crate) struct Completion<A, C> {
    pub call: Call,
    pub trees: Trees<A, C>,
    pub events: Vec<Event>,
}
pub(crate) struct PreparedCall<A, C> {
    contract: ae_state::contracts::Contract,
    call: Call,
    trees: Trees<A, C>,
    rollback: Trees<A, C>,
    events: Vec<Event>,
}

// The context's payer and authenticated identities must have been produced by
// validated wrappers. Signature verification remains required for dry runs.
#[allow(clippy::too_many_arguments)]
pub fn execute_signed_call<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    signed: &ae_tx::SignedTransaction<ContractTransaction>,
    trees: &mut Trees<A, C>,
    network: &ae_protocol::NetworkProfile,
    context: &Context,
    vm: &fate::Config<'_, O>,
    limits: Limits,
    maximum_steps: usize,
) -> Result<Receipt, Error> {
    let caller = signed.transaction().signer();
    let signers = if context.ga_authenticated.contains(&caller) {
        vec![]
    } else {
        vec![caller]
    };
    signed
        .verify_signers(&signers, network, context.protocol, context.payer.is_some())
        .map_err(crate::Error::from)?;
    let mut receipt = process_call(
        signed.transaction(),
        trees,
        context,
        vm,
        limits,
        maximum_steps,
    )?;
    receipt.transaction_hash = Some(signed.hash());
    Ok(receipt)
}
pub(crate) fn big(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}
pub(crate) fn unsigned(n: BigInt) -> Result<Unsigned, Error> {
    if n.sign() == Sign::Minus {
        return Err(Error::InvalidVmGas);
    }
    Ok(Unsigned::from_field_bytes(&n.to_bytes_be().1).expect("canonical magnitude"))
}
fn product(a: &Unsigned, b: &Unsigned) -> Unsigned {
    unsigned(big(a) * big(b)).expect("nonnegative product")
}
fn account<S: NodeReader>(
    accounts: &Accounts<S>,
    key: AccountPublicKey,
) -> Result<ae_state::accounts::Account, Error> {
    accounts
        .lookup(key)?
        .ok_or(Error::Primitive(crate::Error::AccountNotFound))
}
fn spend<S: NodeWriter>(
    accounts: &mut Accounts<S>,
    key: AccountPublicKey,
    amount: &Unsigned,
) -> Result<(), Error> {
    let mut a = account(accounts, key)?;
    if a.balance() < amount {
        return Err(crate::Error::InsufficientFunds.into());
    }
    a.spend_without_nonce_bump(amount)?;
    accounts.enter(&a)?;
    Ok(())
}
fn earn<S: NodeWriter>(
    accounts: &mut Accounts<S>,
    key: AccountPublicKey,
    amount: &Unsigned,
) -> Result<(), Error> {
    let mut a = account(accounts, key)?;
    a.earn(amount);
    accounts.enter(&a)?;
    Ok(())
}
fn delta(
    events: &mut Vec<Event>,
    account: AccountPublicKey,
    amount: Unsigned,
    debit: bool,
    info: &'static str,
) {
    events.insert(
        0,
        Event::Delta {
            account,
            amount,
            debit,
            info,
        },
    );
}

fn check(tx: &ContractTransaction, context: &Context) -> Result<(), Error> {
    if context.origin != Origin::Contract {
        if tx.fee() < &tx.minimum_fee(context.protocol, context.origin == Origin::Generalized) {
            return Err(crate::Error::TooLowFee.into());
        }
        if tx.gas_price() < &context.protocol.minimum_gas_price().into() {
            return Err(Error::Rejected("too_low_gas_price"));
        }
        if tx.ttl() != &0.into() && tx.ttl() < &context.height.into() {
            return Err(crate::Error::TtlExpired.into());
        }
        if !tx.valid_at_protocol(context.protocol) {
            return Err(crate::Error::InvalidAtProtocol.into());
        }
    }
    Ok(())
}

// Signature and wrapper authentication precede this callback, as for the
// existing primitive processors. No caller roots change on any Err result.
pub fn process_call<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: &ContractTransaction,
    trees: &mut Trees<A, C>,
    context: &Context,
    vm: &fate::Config<'_, O>,
    limits: Limits,
    maximum_steps: usize,
) -> Result<Receipt, Error> {
    let prepared = prepare_call(tx, trees, context)?;
    let ContractOperation::Call { call_data, .. } = tx.operation() else {
        unreachable!()
    };
    let completed = fate::execute(
        tx,
        call_data,
        &prepared.contract,
        prepared.call,
        false,
        prepared.trees,
        context,
        vm,
        limits,
        maximum_steps,
    )?;
    settle(
        tx,
        trees,
        prepared.rollback,
        context,
        completed,
        prepared.events,
    )
}
fn prepare_call<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: &ContractTransaction,
    trees: &Trees<A, C>,
    context: &Context,
) -> Result<PreparedCall<A, C>, Error> {
    let ContractOperation::Call {
        contract: target,
        abi_version,
        ..
    } = tx.operation()
    else {
        return Err(Error::UnsupportedOperation);
    };
    check(tx, context)?;
    let caller = tx.signer();
    let mut next = trees.clone();
    // process_call_from_contract has no inc_account_nonce instruction. It
    // checks and increments only after balance/version/stack admission below.
    if context.origin != Origin::Contract {
        let mut changes = BTreeMap::new();
        crate::prepare_sender(&next.accounts, &mut changes, caller, tx.nonce(), context)?;
        next.accounts
            .apply(&changes.into_values().collect::<Vec<_>>())?;
    }
    let key = if target.tag == IdTag::Name {
        if context.protocol < ae_protocol::Protocol::Ceres {
            return Err(Error::Rejected(
                "contract_call_by_name_hash_not_available_at_protocol",
            ));
        }
        let key = next
            .names
            .resolve_id(target.value, b"contract_pubkey")?
            .value;
        // Original name lookup enters the primop cache, flushed before the VM.
        let name = next
            .names
            .name(target.value)?
            .ok_or(ae_state::names::Error::NameNotFound)?;
        next.names.enter_name(&name)?;
        key
    } else {
        target.value
    };
    // VM failure retains nonce processing and the named-target read cache.
    let rollback = next.clone();
    let total = if context.origin == Origin::Contract {
        tx.amount().clone()
    } else {
        tx.fee()
            .add(&product(tx.gas(), tx.gas_price()))
            .add(tx.amount())
    };
    let payer_amount = total
        .checked_sub(tx.amount())
        .expect("total includes amount");
    let mut events = Vec::new();
    let caller_amount = if let Some(payer) = context.payer.filter(|_| payer_amount != 0.into()) {
        spend(&mut next.accounts, payer, &payer_amount)?;
        delta(&mut events, payer, payer_amount, true, "Chain.fee");
        tx.amount().clone()
    } else {
        total
    };
    let a = account(&next.accounts, caller)?;
    if a.balance() < &caller_amount {
        return Err(crate::Error::InsufficientFunds.into());
    }
    let contract = next
        .contracts
        .record(key)?
        .ok_or(Error::Rejected("contract_does_not_exist"))?;
    if !contract.version.can_call(context.protocol) {
        return Err(Error::Rejected("wrong_vm_version"));
    }
    if abi_version != &Unsigned::from(u64::from(contract.version.abi)) {
        return Err(Error::Rejected("wrong_abi_version"));
    }
    if context.origin != Origin::Contract && !tx.call_stack().is_empty() {
        return Err(Error::Rejected("nonempty_call_stack"));
    }
    if context.origin == Origin::Contract {
        let mut a = account(&next.accounts, caller)?;
        if tx.nonce() <= a.nonce() {
            return Err(crate::Error::NonceAlreadyUsed.into());
        }
        if tx.nonce() != &a.nonce().add(&1.into()) {
            return Err(crate::Error::NonceTooHigh.into());
        }
        a.set_nonce(tx.nonce().clone());
        next.accounts.enter(&a)?;
    }
    spend(&mut next.accounts, caller, &caller_amount)?;
    delta(&mut events, caller, caller_amount, true, "Contract.amount");
    earn(&mut next.accounts, AccountPublicKey(key), tx.amount())?;
    delta(
        &mut events,
        AccountPublicKey(key),
        tx.amount().clone(),
        false,
        "Contract.amount",
    );
    let caller_id = Id::new(
        if context.origin == Origin::Contract {
            IdTag::Contract
        } else {
            IdTag::Account
        },
        caller.0,
    );
    let call = Call::new(
        caller_id,
        tx.nonce().clone(),
        key,
        Some(target.value),
        context.height.into(),
        tx.gas_price().clone(),
    );
    Ok(PreparedCall {
        contract,
        call,
        trees: next,
        rollback,
        events,
    })
}
fn settle<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: &ContractTransaction,
    trees: &mut Trees<A, C>,
    rollback: Trees<A, C>,
    context: &Context,
    completed: Completion<A, C>,
    mut events: Vec<Event>,
) -> Result<Receipt, Error> {
    let caller = tx.signer();
    let is_create = matches!(tx.operation(), ContractOperation::Create { .. });
    let internal = context.origin == Origin::Contract && !is_create;
    let mut call = completed.call;
    if is_create && call.return_type == ReturnType::Ok {
        call.return_value.clear();
    }
    let mut next = if call.return_type == ReturnType::Ok {
        let mut out = completed.trees;
        events.splice(0..0, completed.events);
        if !internal {
            let refund = product(
                &tx.gas()
                    .checked_sub(&call.gas_used)
                    .map_err(|_| Error::InvalidVmGas)?,
                tx.gas_price(),
            );
            let payer = context.payer.unwrap_or(caller);
            earn(&mut out.accounts, payer, &refund)?;
            delta(&mut events, payer, refund, false, "Chain.refund");
        }
        out
    } else {
        events.clear();
        let mut out = rollback;
        if !internal {
            if call.return_type == ReturnType::Error
                && context.protocol > ae_protocol::Protocol::Fortuna
                && !context.dry_run
            {
                call.return_value.clear();
            }
            let amount = tx.fee().add(&product(&call.gas_used, tx.gas_price()));
            let payer = context.payer.unwrap_or(caller);
            spend(&mut out.accounts, payer, &amount)?;
            delta(&mut events, payer, amount, true, "Contract.gas");
        }
        out
    };
    if !internal {
        if let Some(nonce) = context.ga_nonces.get(&caller) {
            let nonce: [u8; 32] = nonce
                .as_slice()
                .try_into()
                .map_err(|_| Error::Exception(Exception::Native("function_clause")))?;
            let target: [u8; 32] = call
                .target()
                .try_into()
                .map_err(|_| Error::Exception(Exception::Native("function_clause")))?;
            call.id = ae_state::calls::ga_call_id(nonce, target).0;
        }
        next.calls.insert(&call).map_err(|e| {
            if e == ae_state::contracts::Error::AlreadyPresent {
                Error::Exception(Exception::Native("already_present"))
            } else {
                e.into()
            }
        })?;
    }
    let gas = tx.used_gas(&call.gas_used);
    *trees = next;
    Ok(Receipt {
        transaction_hash: None,
        call,
        gas,
        events,
    })
}
