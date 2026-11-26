use super::invocation::Invocation;
use super::{Completion, Error, Event, big, unsigned};
use crate::{
    Context, Origin,
    fate_chain::{
        self,
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
use ae_vm_fate::{
    Limits, abi, encoding,
    machine::{
        self, Executor,
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
    pub diagnostic_profile: ae_vm_fate::diagnostic::Profile,
}
#[allow(clippy::too_many_arguments)]
pub(super) fn execute<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: Invocation<'_>,
    calldata: &[u8],
    contract: &Contract,
    call: Call,
    allow_init: bool,
    trees: Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
    limits: Limits,
    maximum_steps: usize,
) -> Result<Completion<A, C>, Error> {
    execute_with_store_key(
        tx,
        calldata,
        contract,
        call,
        allow_init,
        trees,
        context,
        config,
        limits,
        maximum_steps,
        None,
    )
}
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_with_store_key<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: Invocation<'_>,
    calldata: &[u8],
    contract: &Contract,
    call: Call,
    allow_init: bool,
    trees: Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
    limits: Limits,
    maximum_steps: usize,
    store_key: Option<[u8; 32]>,
) -> Result<Completion<A, C>, Error> {
    execute_with_views(
        tx,
        calldata,
        contract,
        call,
        allow_init,
        trees,
        context,
        config,
        limits,
        maximum_steps,
        store_key,
        None,
    )
}
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_with_views<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: Invocation<'_>,
    calldata: &[u8],
    contract: &Contract,
    mut call: Call,
    allow_init: bool,
    trees: Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
    limits: Limits,
    maximum_steps: usize,
    store_key: Option<[u8; 32]>,
    onchain: Option<&Trees<A, C>>,
) -> Result<Completion<A, C>, Error> {
    let sanitized = onchain.is_none() && sanitize(context, allow_init);
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
    let code = decode_code(&wrapped, limits)?;
    match abi::decode_vm_calldata(calldata, limits) {
        Ok(_) => {}
        Err(ae_vm_fate::Error::ResourceLimit) => return Err(Error::ResourceLimit),
        Err(_) => {
            call.return_type = ReturnType::Error;
            call.gas_used = tx.gas().clone();
            call.return_value = if sanitized {
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
    if context.ga_transaction.is_some() {
        environment.auth_tx_hash = context.ga_transaction_hash.map(|h| h.0.to_vec());
    }
    let host = Host {
        name_claim_bid_timeout: context.name_claim_bid_timeout.clone(),
        contract_cache: Default::default(),
        auth_tx: context
            .ga_transaction
            .clone()
            .or_else(|| config.auth_tx.clone()),
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
        onchain: onchain.map(|t| {
            Box::new(fate_chain::remote::View {
                accounts: t.accounts.clone(),
                contracts: t.contracts.clone(),
                names: t.names.clone(),
                name_cache: Default::default(),
                oracles: t.oracles.clone(),
                oracle_reads: Default::default(),
            })
        }),
        environment,
        history: config.history.clone(),
    };
    let (store, limits) = if allow_init {
        // Init uses its supplied fresh store, even if an existing contract's
        // persisted subtree has entries left by a prior incarnation.
        let metadata = encoding::encode(&Value::Map(BTreeMap::new()), limits)
            .map_err(|e| Error::Vm(machine::Error::Data(e)))?;
        let mut remaining = limits;
        remaining.max_bytes = remaining
            .max_bytes
            .checked_sub(metadata.len())
            .ok_or(Error::ResourceLimit)?;
        remaining.max_nodes = remaining
            .max_nodes
            .checked_sub(1)
            .ok_or(Error::ResourceLimit)?;
        let mut store = ae_state::contracts::store::Store::new(ae_trie::Trie::new(
            OwnedReadView::new(host.contracts.tree().store().clone()),
        ));
        store.put(vec![0, 0], metadata);
        (Store(crate::fate_store::Backend(store)), remaining)
    } else if let Some(key) = store_key {
        let mut remaining = limits;
        remaining.max_bytes = remaining
            .max_bytes
            .checked_sub(32)
            .ok_or(Error::ResourceLimit)?;
        remaining.max_nodes = remaining
            .max_nodes
            .checked_sub(1)
            .ok_or(Error::ResourceLimit)?;
        let tree = host
            .contracts
            .tree()
            .owned_read_only_subtree(&ae_trie::Nibbles::from_bytes(
                &ae_state::contracts::store_id(key),
            ))
            .map_err(ae_state::contracts::Error::from)?;
        (
            Store(crate::fate_store::Backend(
                ae_state::contracts::store::Store::new(tree),
            )),
            remaining,
        )
    } else {
        host.contract_store_source(contract.public_key, limits)?
    };
    let spec = invocation::Spec {
        contract: contract.public_key,
        bytecode: &code.byte_code,
        calldata,
        vm_version: vm,
        gas: Value::Integer(big(tx.gas())),
        value: Value::Integer(if allow_init {
            0.into()
        } else {
            big(tx.amount())
        }),
        store,
        allow_init,
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
            let (output, remaining_limits) = if sanitized {
                (vec![], remaining_limits)
            } else {
                ae_vm_fate::diagnostic::reason(
                    &reason,
                    config.order,
                    config.diagnostic_profile,
                    remaining_limits,
                )
                .map_err(|e| Error::Vm(machine::Error::Data(e)))?
                .ok_or(Error::UnformattedDiagnostic(reason))?
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
            events: state
                .session
                .host
                .events
                .into_iter()
                .map(Event::Internal)
                .collect(),
        })
    } else {
        Ok(Completion {
            call,
            trees,
            events: vec![],
        })
    }
}
fn sanitize(context: &Context, allow_init: bool) -> bool {
    (allow_init || context.origin != Origin::Contract)
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
    let mut machine_context = config.context.clone();
    if host.onchain.is_some() {
        machine_context.on_chain = false;
    }
    let mut sources = BTreeMap::new();
    let out = (Executor {
        context: &machine_context,
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
            context: &machine_context,
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
                    context: &machine_context,
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

pub(crate) fn decode_code(wrapped: &[u8], limits: Limits) -> Result<SophiaCode, Error> {
    SophiaCode::decode_with_limits(
        wrapped,
        rlp::DecodeLimits {
            max_input_bytes: limits.max_bytes,
            max_items: limits.max_nodes,
            max_depth: limits.max_depth,
        },
    )
    .map_err(|e| match e {
        ae_codec::object::ObjectError::Rlp(rlp::DecodeError::ResourceLimit) => Error::ResourceLimit,
        ae_codec::object::ObjectError::ExpectedList => {
            let rlp::Rlp::Bytes(bytes) = rlp::decode_with_limits(
                wrapped,
                rlp::DecodeLimits {
                    max_input_bytes: limits.max_bytes,
                    max_items: limits.max_nodes,
                    max_depth: limits.max_depth,
                },
            )
            .expect("same bounded decoder") else {
                unreachable!()
            };
            use ae_vm_fate::native::{Class, Exception, Term};
            Error::NativeException(Box::new(Exception {
                class: Class::Error,
                reason: Term::tagged("badmatch", vec![Term::Binary(bytes.to_vec())]),
            }))
        }
        e => Error::State(ae_state::contracts::Error::Object(e)),
    })
}
