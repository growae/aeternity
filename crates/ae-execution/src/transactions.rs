// aetx/aetx_sign/aec_paying_for_tx/aeprimop, aeternity fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{Context, Origin, contracts, names, oracles};
use ae_protocol::{ConsensusType, NetworkProfile, Protocol};
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

pub mod accounting;
pub mod batch;
mod failures;
mod signatures;
mod wrappers;
pub use signatures::{
    SignatureError, SignatureLimits, signer_keys, verify, verify_with_authenticated_ids,
    verify_with_channel_state, verify_with_context, verify_with_trees,
};

pub struct Config<'a, O = Otp26> {
    pub network: &'a NetworkProfile,
    pub consensus: ConsensusType,
    pub fate: Option<&'a contracts::fate::Config<'a, O>>,
    pub aevm: Option<&'a contracts::aevm::Config>,
    pub signature_limits: SignatureLimits,
    pub channel_limits: crate::channels::unilateral::Limits,
    pub channel_state_atoms: Option<&'a std::collections::BTreeSet<Vec<u8>>>,
    pub oracle_limits: oracles::abi::Limits,
    pub fate_limits: ae_vm_fate::Limits,
    pub maximum_fate_steps: usize,
    pub aevm_limits: ae_vm_aevm::execution::Limits,
}
impl<'a> Config<'a> {
    pub fn primitives(network: &'a NetworkProfile) -> Self {
        Self {
            network,
            consensus: ConsensusType::Pow,
            fate: None,
            aevm: None,
            signature_limits: SignatureLimits::default(),
            channel_limits: Default::default(),
            channel_state_atoms: None,
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
    Channel(Box<crate::channels::Error>),
    Aevm(Box<contracts::aevm::Error<A, C>>),
    Signature(SignatureError),
    AuthenticationTransaction(ae_tx::Error),
    RecursivePayer,
    NotAllowedToPayFor(TransactionKind),
    MinimumFeeCaseClause(BigInt),
    ContractCallContextCase(Origin),
    InvalidGas,
    HcVoteCannotBeProcessed,
    MissingVmConfiguration,
    UnsupportedTransaction(TransactionKind),
}
impl<A, C> From<crate::Error> for Error<A, C> {
    fn from(e: crate::Error) -> Self {
        Self::Primitive(e)
    }
}
impl<A, C> From<crate::channels::Error> for Error<A, C> {
    fn from(e: crate::channels::Error) -> Self {
        Self::Channel(Box::new(e))
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
    Channel {
        channel: [u8; 32],
        transaction_hash: Hash256,
        transaction_type: TransactionKind,
    },
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
    verify_with_channel_state(
        signed.transaction().view(),
        signed.signatures(),
        config.network,
        context.protocol,
        context.payer.is_some(),
        config.signature_limits,
        &trees.channels,
    )?;
    let mut receipt =
        process_with_signed_source(signed.transaction(), trees, context, config, Some(signed))?;
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
    verify_with_trees(
        signed.transaction().view(),
        signed.signatures(),
        config.network,
        context,
        config.signature_limits,
        &trees.channels,
    )?;
    let mut receipt =
        process_with_signed_source(signed.transaction(), trees, context, config, Some(signed))?;
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
    process_with_signed_source(tx, trees, context, config, None)
}
pub fn process_with_signed_source<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: &Transaction,
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
    source: Option<&SignedTransaction<Transaction>>,
) -> Result<Receipt, Error<A, C>> {
    wrappers::process(tx, trees, context, config, source)
}

fn check_wrapper<A, C>(
    tx: TransactionRef<'_>,
    context: &Context,
    consensus: ConsensusType,
) -> Result<(), Error<A, C>> {
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
    if tx
        .gas_price()
        .is_some_and(|price| price < &context.protocol.minimum_gas_price().into())
    {
        return Err(Error::Contract(Box::new(contracts::Error::Rejected(
            "too_low_gas_price",
        ))));
    }
    if tx.ttl().is_some_and(|ttl| ttl < &context.height.into()) {
        return Err(crate::Error::TtlExpired.into());
    }
    if !tx
        .valid_at_consensus(context.protocol, consensus)
        .map_err(Error::AuthenticationTransaction)?
    {
        return Err(crate::Error::InvalidAtProtocol.into());
    }
    Ok(())
}
fn process_leaf<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    leaf: &TransactionLeaf,
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
    source: Option<&SignedTransaction<Transaction>>,
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
        TransactionLeaf::HcVote(_) => {
            // aetx checks fee before protocol validity; the transport-only vote
            // has zero fee gas even when the GA context removes its wire size.
            return Err(if context.origin == Origin::Contract {
                Error::HcVoteCannotBeProcessed
            } else {
                crate::Error::OracleTransaction(oracles::Failure::TooLowAbsoluteTtl).into()
            });
        }
        TransactionLeaf::Channel(t) => {
            if matches!(t.operation(), ae_tx::channels::Operation::Offchain { .. }) {
                return Err(if context.origin == Origin::Contract {
                    crate::channels::Error::Native(crate::channels::NativeFailure::FunctionClause)
                } else {
                    crate::channels::Error::Rejected("too_low_abs_ttl")
                }
                .into());
            }
            if matches!(
                t.operation(),
                ae_tx::channels::Operation::ForceProgress { .. }
            ) {
                return crate::channels::force_progress::process(t, trees, context, source, config);
            }
            if matches!(
                t.operation(),
                ae_tx::channels::Operation::CloseSolo { .. }
                    | ae_tx::channels::Operation::Slash { .. }
                    | ae_tx::channels::Operation::SnapshotSolo { .. }
                    | ae_tx::channels::Operation::SetDelegates { .. }
            ) {
                return crate::channels::unilateral::process(
                    t,
                    trees,
                    context,
                    source,
                    config,
                    config.channel_limits,
                );
            }
            return crate::channels::process(
                t,
                trees,
                context,
                source,
                config.signature_limits.max_encoded_bytes,
            )
            .map_err(|e| match e {
                crate::channels::Error::Unsupported(kind) => Error::UnsupportedTransaction(kind),
                e => Error::Channel(Box::new(e)),
            });
        }
        TransactionLeaf::GaAttach(t) => {
            let r = if t.version().abi == 1 {
                contracts::aevm::process_attach(
                    t,
                    trees,
                    context,
                    config.aevm.ok_or(Error::MissingVmConfiguration)?,
                    config.aevm_limits,
                )
                .map_err(|e| Error::Aevm(Box::new(e)))?
            } else {
                contracts::process_attach(
                    t,
                    trees,
                    context,
                    config.fate.ok_or(Error::MissingVmConfiguration)?,
                    config.fate_limits,
                    config.maximum_fate_steps,
                )
                .map_err(|e| Error::Contract(Box::new(e)))?
            };
            return Ok(Receipt {
                transaction_hash: None,
                gas: r.gas,
                call: Some(r.call),
                events: r.events.into_iter().map(Event::Contract).collect(),
                context: context.clone(),
            });
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
