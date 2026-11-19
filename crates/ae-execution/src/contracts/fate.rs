use super::{Error, big, unsigned};
use crate::{
    Context, Origin,
    fate_chain::{
        self, TransferEvent,
        remote::{State as Host, Store},
    },
};
use ae_codec::{rlp, sophia::SophiaCode};
use ae_state::{
    Trees,
    calls::{Call, LogEntry, ReturnType},
    contracts::{Code, Contract},
};
use ae_trie::{NodeWriter, OwnedReadView};
use ae_tx::ContractTransaction;
use ae_vm_fate::{
    Limits, abi, encoding,
    machine::{
        self, Executor, Reason,
        program::remote::{self, invocation},
    },
    maps::{MapOrder, listing::Overrides},
    types::Version,
    value::{ObjectPayload, Value},
};
use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

pub struct Config<'a, O> {
    pub context: &'a machine::Context,
    pub order: &'a O,
    pub overrides: &'a Overrides,
    pub environment: &'a remote::context::Environment,
    pub history: Arc<fate_chain::history::Snapshot>,
    pub auth_tx: Option<Arc<ae_tx::WireTransaction>>,
    pub metering_floor: Option<u32>,
}
pub(crate) struct Completion<A, C> {
    pub call: Call,
    pub trees: Trees<A, C>,
    pub events: Vec<TransferEvent>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: &ContractTransaction,
    calldata: &[u8],
    contract: &Contract,
    mut call: Call,
    trees: Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
    limits: Limits,
    maximum_steps: usize,
) -> Result<Completion<A, C>, Error> {
    let vm = match contract.version.vm {
        5 => Version::One,
        7 => Version::Two,
        8 => Version::Three,
        n => return Err(Error::UnsupportedVm(n)),
    };
    let wrapped = match &contract.code {
        Code::Inline(bytes) => Cow::Borrowed(bytes.as_slice()),
        Code::Reference(id) => {
            if id.tag != ae_types::IdTag::Contract {
                return Err(Error::Exception(machine::Exception::Native(
                    "function_clause",
                )));
            }
            let reference = trees
                .contracts
                .record(id.value)?
                .ok_or(Error::Rejected("contract_does_not_exist"))?;
            let Code::Inline(bytes) = reference.code else {
                return Err(Error::Exception(machine::Exception::Native("badmatch")));
            };
            Cow::Owned(bytes)
        }
    };
    let code = SophiaCode::decode_with_limits(
        &wrapped,
        rlp::DecodeLimits {
            max_input_bytes: limits.max_bytes,
            max_items: limits.max_nodes,
            max_depth: limits.max_depth,
        },
    )
    .map_err(|e| match e {
        ae_codec::object::ObjectError::Rlp(rlp::DecodeError::ResourceLimit) => Error::ResourceLimit,
        e => Error::State(ae_state::contracts::Error::Object(e)),
    })?;
    match abi::decode_vm_calldata(calldata, limits) {
        Ok(_) => {}
        Err(ae_vm_fate::Error::ResourceLimit) => return Err(Error::ResourceLimit),
        Err(_) => {
            call.return_type = ReturnType::Error;
            call.gas_used = tx.gas().clone();
            call.return_value = if sanitize(context) {
                vec![]
            } else {
                b"bad_call_data".to_vec()
            };
            return Ok(Completion {
                call,
                trees,
                events: vec![],
            });
        }
    }
    if contract.version.abi != 3 {
        return Err(Error::Exception(machine::Exception::Native("illegal_abi")));
    }
    let mut environment = config.environment.clone();
    environment.origin = tx.call_origin();
    environment.gas_price = big(tx.gas_price());
    environment.fee = big(tx.fee());
    let host = Host {
        contract_cache: Default::default(),
        auth_tx: config.auth_tx.clone(),
        accounts: trees.accounts.clone(),
        contracts: trees.contracts.clone(),
        height: context.height,
        authentication: fate_chain::remote::Authentication {
            dry_run: context.dry_run,
            payer: context.payer,
            ga_authenticated: context.ga_authenticated.clone(),
            ga_nonces: context.ga_nonces.clone(),
        },
        events: vec![],
        names: trees.names.clone(),
        name_cache: Default::default(),
        oracles: trees.oracles.clone(),
        oracle_reads: Default::default(),
        name_base: None,
        oracle_base: None,
        onchain: None,
        environment,
        history: config.history.clone(),
    };
    let (store, limits) = host.contract_store_source(contract.public_key, limits)?;
    let spec = invocation::Spec {
        contract: contract.public_key,
        bytecode: &code.byte_code,
        calldata,
        vm_version: vm,
        gas: Value::Integer(big(tx.gas())),
        value: Value::Integer(big(tx.amount())),
        store,
        allow_init: false,
    };
    let env = invocation::Environment {
        protocol: context.protocol as u32,
        caller: Some(ObjectPayload::Bytes(tx.actor().value.to_vec())),
        metering_floor: config.metering_floor,
        debug: false,
    };
    let terminal = run(&spec, &env, &host, config, limits, maximum_steps)?;
    let (state, return_type, output, mut remaining) = match terminal {
        remote::Outcome::Returned {
            state,
            remaining_limits,
        } => {
            let output = encoding::encode(&state.program.machine.accumulator.top, remaining_limits)
                .map_err(|e| Error::Vm(machine::Error::Data(e)))?;
            (state, ReturnType::Ok, output, remaining_limits)
        }
        remote::Outcome::Abort {
            state,
            reason,
            remaining_limits,
        } => {
            let output = if sanitize(context) {
                vec![]
            } else {
                diagnostic(&reason).ok_or(Error::UnformattedDiagnostic(reason))?
            };
            (state, ReturnType::Error, output, remaining_limits)
        }
        remote::Outcome::Revert {
            state,
            value,
            remaining_limits,
        } => {
            let output = encoding::encode(&Value::String(value), remaining_limits)
                .map_err(|e| Error::Vm(machine::Error::Data(e)))?;
            (state, ReturnType::Revert, output, remaining_limits)
        }
        remote::Outcome::Exception(e) => return Err(Error::Exception(e)),
        remote::Outcome::Break { .. } => return Err(Error::Suspended),
        remote::Outcome::Continue { .. } => unreachable!("run returns a terminal outcome"),
    };
    remaining.max_bytes = remaining
        .max_bytes
        .checked_sub(output.len())
        .ok_or(Error::ResourceLimit)?;
    call.gas_used = unsigned(big(tx.gas()) - &state.program.machine.meter.gas)?;
    if &call.gas_used > tx.gas() {
        return Err(Error::InvalidVmGas);
    }
    call.return_type = return_type;
    call.return_value = output;
    if return_type == ReturnType::Ok {
        for log in &state.session.logs {
            let topics = log
                .topics
                .iter()
                .map(|p| match p {
                    ObjectPayload::Bytes(bytes) => Ok(bytes.clone()),
                    _ => Err(Error::UnsupportedLogPayload),
                })
                .collect::<Result<Vec<_>, _>>()?;
            call.log.push(LogEntry {
                address: log.contract.to_vec(),
                topics,
                data: log.payload.clone(),
            });
        }
        let (trees, _) =
            state
                .session
                .host
                .final_trees(&trees.calls, &trees.channels, remaining)?;
        Ok(Completion {
            call,
            trees,
            events: state.session.host.events,
        })
    } else {
        Ok(Completion {
            call,
            trees,
            events: vec![],
        })
    }
}
fn sanitize(context: &Context) -> bool {
    context.origin != Origin::Contract
        && context.protocol > ae_protocol::Protocol::Fortuna
        && !context.dry_run
}

type VmStore<C> = Store<OwnedReadView<C>>;
fn run<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    spec: &invocation::Spec<'_, VmStore<C>>,
    env: &invocation::Environment,
    host: &Host<A, C>,
    config: &Config<'_, O>,
    limits: Limits,
    maximum_steps: usize,
) -> Result<remote::Outcome<VmStore<C>, Host<A, C>>, Error> {
    let mut sources = BTreeMap::new();
    let out = (Executor {
        context: config.context,
        order: config.order,
        overrides: config.overrides,
        contracts: &sources,
    })
    .setup_invocation(spec, env, host, limits)?;
    let remote::Outcome::Continue {
        mut state,
        remaining_limits: mut limits,
    } = out
    else {
        return Ok(out);
    };
    for _ in 0..maximum_steps {
        match (Executor {
            context: config.context,
            order: config.order,
            overrides: config.overrides,
            contracts: &sources,
        })
        .advance_remote_program(&state, limits)
        {
            Err(machine::Error::NeedContract(key)) => {
                if sources.contains_key(&key) {
                    return Err(Error::Vm(machine::Error::NeedContract(key)));
                }
                let (store, remaining) = state.session.host.contract_store_source(key, limits)?;
                sources.insert(key, store);
                limits = remaining;
            }
            Err(e) => return Err(e.into()),
            Ok(remote::Outcome::Continue {
                state: next,
                remaining_limits,
            }) => {
                state = next;
                limits = remaining_limits;
                // A protected rollback/redeployment may reuse an address.
                // The next instruction must load from its current host state.
                sources.clear();
            }
            Ok(remote::Outcome::Returned {
                state,
                remaining_limits,
            }) => {
                return Ok((Executor {
                    context: config.context,
                    order: config.order,
                    overrides: config.overrides,
                    contracts: &sources,
                })
                .finalize_remote_program(&state, remaining_limits)?);
            }
            Ok(other) => return Ok(other),
        }
    }
    Err(Error::ResourceLimit)
}
// Only byte-exact diagnostic forms are exposed. Other dry-run diagnostics
// remain explicit operational stops until the Erlang term printer is ported.
fn diagnostic(reason: &Reason) -> Option<Vec<u8>> {
    let literal = match reason {
        Reason::OutOfGas => "Out of gas",
        Reason::MissingMapKey => "Maps: Key does not exist",
        Reason::HeadEmpty => "Head on empty list",
        Reason::TailEmpty => "Tail on empty list",
        Reason::DivisionByZero => "Arithmetic error: division by zero",
        Reason::ModuloByZero => "Arithmetic error: mod by zero",
        Reason::InvalidInitCall => "Calling init is not allowed in this context",
        Reason::ReentrantCall => "Reentrant call",
        Reason::RemoteTypeMismatch => "Type of remote function does not match expected type",
        Reason::LogIllegalBits => "Illegal bits in log",
        Reason::UserExit(bytes) => return Some(bytes.clone()),
        Reason::FunctionArity { given, expected } => {
            return Some(format!("Expected {expected} arguments, got {given}").into_bytes());
        }
        Reason::NonPayableFunction(Value::String(hash))
        | Reason::PrivateFunction(Value::String(hash))
        | Reason::MissingFunction(Value::String(hash))
            if hash.len() <= 4 =>
        {
            let term = format!(
                "<<{}>>",
                hash.iter().map(u8::to_string).collect::<Vec<_>>().join(",")
            );
            return Some(
                match reason {
                    Reason::NonPayableFunction(_) => {
                        format!("Function with hash {term} is not payable")
                    }
                    Reason::PrivateFunction(_) => format!("Function with hash {term} is private"),
                    _ => format!("Trying to call undefined function: {term}"),
                }
                .into_bytes(),
            );
        }
        _ => return None,
    };
    Some(literal.as_bytes().to_vec())
}
