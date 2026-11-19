// aetx/aetx_sign/aec_paying_for_tx/aeprimop, aeternity fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{Context, Origin, contracts, names, oracles};
use ae_protocol::{NetworkProfile, Protocol};
use ae_state::{Trees, calls::Call};
use ae_trie::{MemoryStore, NodeWriter};
use ae_tx::{
    ContractOperation, SignedTransaction, Transaction, TransactionKind, TransactionLeaf,
    TransactionRef, Wrapper,
};
use ae_types::{AccountPublicKey, Hash256, Signature, Unsigned};
use ae_vm_fate::maps::{MapOrder, order::Otp26};
use num_bigint::{BigInt, Sign};
use std::collections::BTreeMap;

mod signatures;
pub use signatures::{SignatureError, SignatureLimits, verify, verify_with_context};

pub struct Config<'a, O = Otp26> {
    pub network: &'a NetworkProfile,
    pub fate: Option<&'a contracts::fate::Config<'a, O>>,
    pub aevm: Option<&'a contracts::aevm::Config>,
    pub signature_limits: SignatureLimits,
    pub oracle_limits: oracles::abi::Limits,
    pub fate_limits: ae_vm_fate::Limits,
    pub maximum_fate_steps: usize,
    pub aevm_limits: ae_vm_aevm::execution::Limits,
}
impl<'a> Config<'a> {
    pub fn primitives(network: &'a NetworkProfile) -> Self {
        Self {
            network,
            fate: None,
            aevm: None,
            signature_limits: SignatureLimits::default(),
            oracle_limits: oracles::abi::Limits::default(),
            fate_limits: ae_vm_fate::Limits::default(),
            maximum_fate_steps: 1_000_000,
            aevm_limits: ae_vm_aevm::execution::Limits::default(),
        }
    }
}
#[derive(Debug)]
pub enum Error<A = MemoryStore, C = MemoryStore> {
    Primitive(crate::Error),
    Contract(Box<contracts::Error>),
    Aevm(Box<contracts::aevm::Error<A, C>>),
    Signature(SignatureError),
    RecursivePayer,
    MinimumFeeCaseClause(BigInt),
    ContractCallContextCase(Origin),
    InvalidGas,
    MissingVmConfiguration,
    UnsupportedTransaction(TransactionKind),
}
impl<A, C> From<crate::Error> for Error<A, C> {
    fn from(e: crate::Error) -> Self {
        Self::Primitive(e)
    }
}
impl<A, C> From<SignatureError> for Error<A, C> {
    fn from(e: SignatureError) -> Self {
        Self::Signature(e)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Primitive(crate::Event),
    Contract(contracts::Event),
}
#[derive(Debug, Clone)]
pub struct Receipt {
    pub transaction_hash: Option<Hash256>,
    pub gas: Unsigned,
    pub call: Option<Call>,
    // New events, in original aetx_env order (most recent first).
    pub events: Vec<Event>,
    pub context: Context,
}
fn big(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}
fn size_gas(n: usize) -> BigInt {
    BigInt::from(n) * 20
}
fn unsigned<A, C>(n: BigInt) -> Result<Unsigned, Error<A, C>> {
    if n.sign() == Sign::Minus {
        return Err(Error::InvalidGas);
    }
    Ok(Unsigned::from_field_bytes(&n.to_bytes_be().1).expect("nonnegative magnitude"))
}
// Strict aetx_sign:verify entry point; dry runs still verify signatures.
pub fn execute_signed<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    signed: &SignedTransaction<Transaction>,
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
) -> Result<Receipt, Error<A, C>> {
    verify(
        signed.transaction().view(),
        signed.signatures(),
        config.network,
        context.protocol,
        context.payer.is_some(),
        config.signature_limits,
    )?;
    let mut receipt = process(signed.transaction(), trees, context, config)?;
    receipt.transaction_hash = Some(signed.hash());
    Ok(receipt)
}
// aetx_sign:verify_w_env. Authenticated identities must come from completed
// enclosing GA execution, never from untrusted transaction fields.
pub fn execute_signed_with_context<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    signed: &SignedTransaction<Transaction>,
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
) -> Result<Receipt, Error<A, C>> {
    verify_with_context(
        signed.transaction().view(),
        signed.signatures(),
        config.network,
        context,
        config.signature_limits,
    )?;
    let mut receipt = process(signed.transaction(), trees, context, config)?;
    receipt.transaction_hash = Some(signed.hash());
    Ok(receipt)
}

// All roots (including expiry indexes) remain owned by the caller until every
// payer/inner action succeeds. Backends must keep cloned root handles isolated;
// unreachable staged nodes need not be erased. Durable commit is a later layer.
pub fn process<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: &Transaction,
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
) -> Result<Receipt, Error<A, C>> {
    let mut next = trees.clone();
    let mut context = context.clone();
    let mut current = tx.view();
    let mut prior_events = Vec::new();
    let mut overhead = BigInt::from(0);
    while let Some(layer) = current.layer() {
        let Wrapper::PayingFor(payer) = layer.wrapper() else {
            return Err(Error::UnsupportedTransaction(current.kind()));
        };
        check_payer(current, &context)?;
        let inner = current.inner().expect("payer inner");
        if inner
            .layers()
            .iter()
            .any(|l| matches!(l.wrapper(), Wrapper::PayingFor(_)))
        {
            return Err(Error::RecursivePayer);
        }
        let mut changes = BTreeMap::new();
        crate::prepare_sender(
            &next.accounts,
            &mut changes,
            payer.payer,
            &payer.nonce,
            &context,
        )?;
        let fee_payer = crate::fee_payer(payer.payer, &payer.fee, &context);
        crate::debit(&next.accounts, &mut changes, fee_payer, &payer.fee)?;
        next.accounts
            .apply(&changes.into_values().collect::<Vec<_>>())
            .map_err(crate::Error::from)?;
        prior_events.insert(
            0,
            Event::Primitive(crate::Event::FeeDebit {
                account: fee_payer,
                amount: payer.fee.clone(),
            }),
        );
        context.payer = Some(payer.payer);
        verify_with_context(
            inner,
            layer.inner_signatures(),
            config.network,
            &context,
            config.signature_limits,
        )?;
        overhead +=
            BigInt::from(3_000) + size_gas(current.wire_size()) - size_gas(inner.wire_size());
        current = inner;
    }
    let mut result = process_leaf(current.leaf(), &mut next, &context, config)?;
    result.gas = unsigned(big(&result.gas) + overhead)?;
    result.events.extend(prior_events);
    *trees = next;
    Ok(result)
}
fn check_payer<A, C>(tx: TransactionRef<'_>, context: &Context) -> Result<(), Error<A, C>> {
    if context.origin == Origin::Contract {
        return Ok(());
    }
    let mut fee_gas = tx.pricing(&context.height.into(), context.protocol).fee_gas;
    if context.origin == Origin::Generalized && context.protocol < Protocol::Iris {
        fee_gas -= size_gas(tx.wire_size());
    }
    let minimum = fee_gas * context.protocol.minimum_gas_price();
    if minimum == BigInt::from(0) {
        return Err(crate::Error::OracleTransaction(oracles::Failure::TooLowAbsoluteTtl).into());
    }
    if minimum.sign() == Sign::Minus {
        return Err(Error::MinimumFeeCaseClause(minimum));
    }
    if big(tx.fee()) < minimum {
        return Err(crate::Error::TooLowFee.into());
    }
    if tx.ttl().is_some_and(|ttl| ttl < &context.height.into()) {
        return Err(crate::Error::TtlExpired.into());
    }
    if !tx.valid_at_protocol(context.protocol) {
        return Err(crate::Error::InvalidAtProtocol.into());
    }
    Ok(())
}
fn process_leaf<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    leaf: &TransactionLeaf,
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
) -> Result<Receipt, Error<A, C>> {
    let primitive = match leaf {
        TransactionLeaf::Spend(t) => Some(crate::process_spend(
            t,
            &mut trees.accounts,
            &trees.names,
            context,
        )?),
        TransactionLeaf::Name(t) => Some(names::process(
            t,
            &mut trees.accounts,
            &mut trees.names,
            context,
        )?),
        TransactionLeaf::Oracle(t) => Some(oracles::process_with_limits(
            t,
            &mut trees.accounts,
            &mut trees.oracles,
            &trees.names,
            context,
            config.oracle_limits,
        )?),
        TransactionLeaf::Contract(_) => None,
        TransactionLeaf::GaAttach(_) => {
            return Err(Error::UnsupportedTransaction(TransactionKind::GaAttach));
        }
    };
    if let Some(r) = primitive {
        return Ok(Receipt {
            transaction_hash: None,
            gas: r.gas,
            call: None,
            events: r.events.into_iter().map(Event::Primitive).collect(),
            context: context.clone(),
        });
    }
    let TransactionLeaf::Contract(t) = leaf else {
        unreachable!()
    };
    // aect_call_tx:process asserts this before evaluating its instructions.
    // The VM's separate process_call_from_contract path is not this callback.
    if matches!(t.operation(), ContractOperation::Call { .. }) && context.origin == Origin::Contract
    {
        return Err(Error::ContractCallContextCase(context.origin));
    }
    // Shared contract admission validates the actual record/ABI before VM
    // execution. Routing by the requested ABI adds no premature tree lookup.
    let aevm = t.operation().abi_version() == 1.into();
    let r = if aevm {
        let vm = config.aevm.ok_or(Error::MissingVmConfiguration)?;
        match t.operation() {
            ContractOperation::Create { .. } => {
                contracts::aevm::process_create(t, trees, context, vm, config.aevm_limits)
            }
            ContractOperation::Call { .. } => {
                contracts::aevm::process_call(t, trees, context, vm, config.aevm_limits)
            }
        }
        .map_err(|e| Error::Aevm(Box::new(e)))?
    } else {
        let vm = config.fate.ok_or(Error::MissingVmConfiguration)?;
        match t.operation() {
            ContractOperation::Create { .. } => contracts::process_create(
                t,
                trees,
                context,
                vm,
                config.fate_limits,
                config.maximum_fate_steps,
            ),
            ContractOperation::Call { .. } => contracts::process_call(
                t,
                trees,
                context,
                vm,
                config.fate_limits,
                config.maximum_fate_steps,
            ),
        }
        .map_err(|e| Error::Contract(Box::new(e)))?
    };
    Ok(Receipt {
        transaction_hash: None,
        gas: r.gas,
        call: Some(r.call),
        events: r.events.into_iter().map(Event::Contract).collect(),
        context: context.clone(),
    })
}
