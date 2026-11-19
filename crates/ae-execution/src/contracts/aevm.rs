// aect_dispatch:run/run_common and aec_vm_chain:call_contract, fa2a6d7.
// Copyright (c) 2017-2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::{Completion, Event, Receipt, big, prepare_call, settle};
use crate::{Context, Origin, aevm_chain};
use ae_state::{
    Trees,
    calls::{Call, LogEntry, ReturnType},
    contracts::{Code, Contract},
};
use ae_trie::{MemoryStore, NodeWriter};
use ae_tx::{ContractOperation, ContractTransaction};
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};
use ae_vm_aevm::{
    execution::{self, Budget, Failure, Limits, Stop},
    gas::FloatProfile,
    opcode::Version,
    outer_call::{self, Request, Response},
    runtime::{Fault as RuntimeFault, Native, Outcome},
    store::OrderProfile,
    word::Word,
};
use num_bigint::{BigInt, BigUint, Sign};
use std::sync::Arc;
mod abi;
mod host;

#[derive(Debug)]
pub enum HostError {
    Chain(aevm_chain::Error),
    Transaction(super::Error),
}
pub type Error<A = MemoryStore, C = MemoryStore> =
    outer_call::Fault<HostError, aevm_chain::State<A, C>>;
#[derive(Debug, Clone)]
pub struct Environment {
    pub beneficiary: AccountPublicKey,
    pub difficulty: BigInt,
    pub timestamp_ms: BigInt,
    // Supplied by the selected network/protocol configuration, never read from
    // process environment during deterministic execution.
    pub block_gas_limit: BigInt,
    pub auth_tx_hash: Option<Vec<u8>>,
}
#[derive(Debug, Clone)]
pub struct Config {
    pub environment: Environment,
    pub network_id: Arc<[u8]>,
    pub event_context: aevm_chain::EventContext,
    pub float_profile: FloatProfile,
    pub store_order: OrderProfile,
}
pub struct Host<'a, A = MemoryStore, C = MemoryStore> {
    pub config: &'a Config,
    source: aevm_chain::Host<A, C>,
}
impl<'a, A, C> Host<'a, A, C> {
    pub fn new(config: &'a Config) -> Self {
        Self {
            config,
            source: aevm_chain::Host::default(),
        }
    }
}
fn chain_fault<T>(
    r: Result<T, RuntimeFault<aevm_chain::Error>>,
) -> Result<T, RuntimeFault<HostError>> {
    r.map_err(|e| match e {
        RuntimeFault::Stop(e) => RuntimeFault::Stop(e),
        RuntimeFault::Native(n) => RuntimeFault::Native(n),
        RuntimeFault::Host(e) => RuntimeFault::Host(HostError::Chain(e)),
    })
}
fn transaction_fault(e: super::Error) -> RuntimeFault<HostError> {
    RuntimeFault::Host(HostError::Transaction(e))
}
fn magnitude(n: &BigUint) -> Unsigned {
    Unsigned::from_field_bytes(&n.to_bytes_be()).expect("canonical nonnegative magnitude")
}
fn error_bytes(f: Failure) -> Vec<u8> {
    match f {
        Failure::OutOfGas => b"out_of_gas".to_vec(),
        Failure::OutOfStack => b"out_of_stack".to_vec(),
        Failure::Arithmetic => b"arithmetic_error".to_vec(),
        Failure::IllegalInstruction(op) => format!("illegal_instruction_{op:02X}").into_bytes(),
        Failure::Call(s) => match s.as_str() {
            "not_allowed_off_chain"
            | "bad_call_data"
            | "unknown_error"
            | "unknown_contract"
            | "unknown_function"
            | "function_is_not_payable"
            | "account_is_not_payable"
            | "reentrant_call"
            | "arithmetic_error" => s.into_bytes(),
            _ => b"unknown_error".to_vec(),
        },
        _ => b"unknown_error".to_vec(),
    }
}
fn call_error(bytes: &[u8]) -> String {
    match bytes {
        b"bad_call_data"
        | b"out_of_gas"
        | b"out_of_stack"
        | b"not_allowed_off_chain"
        | b"reentrant_call"
        | b"unknown_function"
        | b"unknown_contract"
        | b"unknown_error"
        | b"function_is_not_payable"
        | b"account_is_not_payable" => String::from_utf8(bytes.to_vec()).expect("ASCII"),
        _ => "unknown_error".into(),
    }
}
pub fn process_call<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: &ContractTransaction,
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config,
    limits: Limits,
) -> Result<Receipt, Error<A, C>> {
    process_budgeted(tx, trees, context, config, &mut Budget::new(limits))
}
#[allow(clippy::too_many_arguments)]
pub fn execute_signed_call<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    signed: &ae_tx::SignedTransaction<ContractTransaction>,
    trees: &mut Trees<A, C>,
    network: &ae_protocol::NetworkProfile,
    context: &Context,
    config: &Config,
    limits: Limits,
) -> Result<Receipt, Error<A, C>> {
    let caller = signed.transaction().signer();
    let signers = if context.ga_authenticated.contains(&caller) {
        vec![]
    } else {
        vec![caller]
    };
    signed
        .verify_signers(&signers, network, context.protocol, context.payer.is_some())
        .map_err(|e| transaction_fault(crate::Error::from(e).into()))?;
    let mut r = process_call(signed.transaction(), trees, context, config, limits)?;
    r.transaction_hash = Some(signed.hash());
    Ok(r)
}
fn process_budgeted<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: &ContractTransaction,
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config,
    budget: &mut Budget,
) -> Result<Receipt, Error<A, C>> {
    budget.work(1)?;
    budget.bytes(tx.wire_size())?;
    let p = prepare_call(tx, trees, context).map_err(transaction_fault)?;
    let completed = execute(tx, &p.contract, p.call, p.trees, context, config, budget)?;
    settle(tx, trees, p.rollback, context, completed, p.events)
        .map_err(|e| transaction_fault(e).into())
}
fn execute<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    tx: &ContractTransaction,
    contract: &Contract,
    mut call: Call,
    trees: Trees<A, C>,
    context: &Context,
    config: &Config,
    budget: &mut Budget,
) -> Result<Completion<A, C>, Error<A, C>> {
    let Some(version) =
        Version::from_id(u32::from(contract.version.vm)).filter(|v| *v != Version::Solidity1)
    else {
        return Err(transaction_fault(super::Error::UnsupportedVm(contract.version.vm)).into());
    };
    let ContractOperation::Call { call_data, .. } = tx.operation() else {
        unreachable!()
    };
    let bytes = match &contract.code {
        Code::Inline(b) => b.clone(),
        Code::Reference(id) => {
            if id.tag != IdTag::Contract {
                return Err(RuntimeFault::Native(Native::ChainException {
                    class: "error",
                    reason: "function_clause",
                })
                .into());
            }
            budget.work(1)?;
            let c = trees
                .contracts
                .record(id.value)
                .map_err(|e| transaction_fault(e.into()))?
                .ok_or_else(|| {
                    transaction_fault(super::Error::Rejected("contract_does_not_exist"))
                })?;
            match c.code {
                Code::Inline(b) => b,
                Code::Reference(id) => {
                    return Err(RuntimeFault::Native(Native::BadMatchCodeReference(id)).into());
                }
            }
        }
    };
    let code = chain_fault(aevm_chain::decode_code(&bytes, budget))?;
    let (input, output) = match abi::check(call_data, &code, tx.amount() != &0.into(), budget)? {
        Ok(t) => t,
        Err(reason) => {
            call.return_type = ReturnType::Error;
            call.return_value = reason.as_bytes().to_vec();
            call.gas_used = tx.gas().clone();
            return Ok(Completion {
                call,
                trees,
                events: vec![],
            });
        }
    };
    let mut child_context = context.clone();
    child_context.origin = Origin::Contract;
    // aeprimop's run_contract explicitly sets off_chain=false, even when its
    // call_contract caller uses channel trees. The parent retains its own view.
    let chain = aevm_chain::State {
        trees,
        onchain: None,
        account: AccountPublicKey(contract.public_key),
        vm_version: version,
        network_id: config.network_id.clone(),
        context: Arc::new(child_context),
        event_context: config.event_context.clone(),
        events: Arc::new(vec![]),
    };
    let mut state = execution::State::new(
        version,
        code.byte_code,
        BigUint::from_bytes_be(tx.gas().as_bytes()),
        config.float_profile,
    );
    state.protocol = context.protocol as u32;
    state.abi_version = u32::from(contract.version.abi);
    state.data = call_data.clone();
    state.out_type = Some(output);
    state.auth_tx_hash = config.environment.auth_tx_hash.clone();
    state.call_stack = tx.call_stack().iter().map(big).collect();
    state.context = execution::Context {
        address: BigUint::from_bytes_be(&contract.public_key).into(),
        creator: BigUint::from_bytes_be(&contract.owner.0).into(),
        origin: BigUint::from_bytes_be(&if version == Version::Sophia1 {
            tx.signer().0
        } else {
            tx.call_origin()
        })
        .into(),
        caller: BigUint::from_bytes_be(&tx.signer().0).into(),
        value: big(tx.amount()),
        gas_price: big(tx.gas_price()),
        coinbase: BigUint::from_bytes_be(&config.environment.beneficiary.0).into(),
        timestamp: config.environment.timestamp_ms.clone(),
        number: context.height.into(),
        difficulty: config.environment.difficulty.clone(),
        gas_limit: config.environment.block_gas_limit.clone(),
    };
    let host = Host::new(config);
    state.map_store = chain_fault(host.source.store(&chain))?
        .ok_or(RuntimeFault::Native(Native::BadMatchNone))?;
    for (k, v) in &state.map_store {
        budget.work(1)?;
        budget.bytes(k.len().checked_add(v.len()).ok_or(Stop::ResourceLimit)?)?;
    }
    let result = outer_call::invoke_budgeted(
        &state,
        Some(&input),
        &chain,
        &host,
        config.store_order,
        budget,
    );
    let (gas, events, trees) = match result {
        Ok(Outcome::Complete { state, chain }) => {
            call.return_type = ReturnType::Ok;
            call.return_value = state.output;
            call.log = state
                .logs
                .into_iter()
                .map(|l| LogEntry {
                    address: l.address.0.to_vec(),
                    topics: l.topics.into_iter().map(|w| w.0.to_vec()).collect(),
                    data: l.data,
                })
                .collect();
            (
                state.gas,
                chain.events.iter().cloned().map(Event::Aevm).collect(),
                chain.trees,
            )
        }
        Ok(Outcome::Error { reason, gas }) => {
            call.return_type = ReturnType::Error;
            call.return_value = error_bytes(reason);
            (gas, vec![], chain.trees)
        }
        Ok(Outcome::Revert { output, gas }) => {
            call.return_type = ReturnType::Revert;
            call.return_value = output;
            (gas, vec![], chain.trees)
        }
        Err(outer_call::Fault::Runtime(RuntimeFault::Native(
            Native::ReentrantCall | Native::BadCallData,
        ))) => {
            call.return_type = ReturnType::Error;
            call.return_value = if matches!(
                result,
                Err(outer_call::Fault::Runtime(RuntimeFault::Native(
                    Native::ReentrantCall
                )))
            ) {
                b"reentrant_call".to_vec()
            } else {
                b"bad_call_data".to_vec()
            };
            (0u8.into(), vec![], chain.trees)
        }
        Err(e) => return Err(e),
    };
    let initial = BigUint::from_bytes_be(tx.gas().as_bytes());
    if gas > initial {
        return Err(transaction_fault(super::Error::InvalidVmGas).into());
    }
    call.gas_used = magnitude(&(initial - gas));
    Ok(Completion {
        call,
        trees,
        events,
    })
}
