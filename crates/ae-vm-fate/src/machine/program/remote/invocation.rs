// aefa_fate:setup_engine/run and aefa_engine_state:new, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::*;
use crate::{
    code::CodeError,
    encoding::{self, DecodeException},
    store::Version,
};

#[derive(Debug, Clone)]
pub struct Spec<'a, S> {
    pub contract: [u8; 32],
    pub bytecode: &'a [u8],
    pub calldata: &'a [u8],
    pub vm_version: VmVersion,
    pub gas: Value,
    pub value: Value,
    pub store: S,
    pub allow_init: bool,
}
#[derive(Debug, Clone)]
pub struct Environment {
    pub protocol: u32,
    // Retain low-level missing/malformed callers; transaction admission is
    // separate. The original constructor wraps the payload without checking.
    pub caller: Option<ObjectPayload>,
    pub metering_floor: Option<u32>,
    pub debug: bool,
}

impl<S: CachedSubtreeBackend, O: MapOrder> Executor<'_, S, O> {
    // The host must represent a fresh invocation over the supplied input
    // trees. Authentication/chain/dry-run flags come from that trusted host.
    // No root publication, fee accounting or transaction admission occurs here.
    pub fn setup_invocation<H: Host<S::Error>>(
        &self,
        spec: &Spec<'_, S>,
        env: &Environment,
        host: &H,
        limits: Limits,
    ) -> Result<Outcome<S, H>, Error<S::Error>> {
        let (code, limits) = match Code::decode_bounded(spec.bytecode, limits) {
            Ok(code) => code,
            Err(CodeError::Data(DataError::ResourceLimit)) => {
                return Err(Error::Data(DataError::ResourceLimit));
            }
            // Original abort(bad_bytecode, no_state) fails while collecting
            // gas from the absent engine state; it is not a stateful abort.
            Err(_) => return Ok(Outcome::Exception(Exception::Native("function_clause"))),
        };
        let (call, limits) = match encoding::decode_for_vm_bounded(spec.calldata, limits) {
            Ok(call) => call,
            Err(e) => {
                return match e.exception {
                    None => Err(Error::Data(e.error)),
                    Some(DecodeException::Error(reason)) => {
                        Ok(Outcome::Exception(Exception::Native(reason)))
                    }
                    Some(DecodeException::Exit(reason)) => {
                        Ok(Outcome::Exception(Exception::Exit(reason)))
                    }
                };
            }
        };
        // setup_engine matches tuple tags/arity, but does not require that
        // Function is a string. The stricter ABI helper has another contract.
        let Value::Tuple(mut fields) = call else {
            return Ok(Outcome::Exception(Exception::Native("badmatch")));
        };
        if fields.len() != 2 {
            return Ok(Outcome::Exception(Exception::Native("badmatch")));
        }
        let Value::Tuple(arguments) = fields.pop().expect("two fields") else {
            return Ok(Outcome::Exception(Exception::Native("badmatch")));
        };
        let function = fields.pop().expect("one field");
        let mut limits = value::check_limits(&spec.gas, limits).map_err(Error::Data)?;
        limits = value::check_limits(&spec.value, limits).map_err(Error::Data)?;
        let (Value::Integer(gas), Value::Integer(amount)) = (&spec.gas, &spec.value) else {
            return Ok(Outcome::Exception(Exception::Native("bad_init_arg")));
        };
        if gas.sign() == Sign::Minus || amount.sign() == Sign::Minus {
            return Ok(Outcome::Exception(Exception::Native("bad_init_arg")));
        }
        let Some(caller) = &env.caller else {
            return Ok(Outcome::Exception(Exception::Native("badkey")));
        };
        let mut stores = Stores::new(Version::for_protocol(env.protocol));
        // The supplied store is present at entry. Its first use must not
        // trigger the chain host's contract-load cache effect a second time.
        stores.put_contract(spec.contract, spec.store.clone());
        let machine = MachineState {
            accumulator: stack::Accumulator {
                top: Value::Void,
                rest: Default::default(),
            },
            memory: BTreeMap::new(),
            stores,
            meter: gas::Meter {
                protocol: env.protocol,
                gas: gas.clone(),
                created_cells: 0.into(),
                metering_floor: if host.dry_run() {
                    env.metering_floor
                } else {
                    None
                },
            },
        };
        let context = Context {
            current_contract: spec.contract,
            vm_version: spec.vm_version,
            in_auth: host.environment().auth_tx_hash.is_some(),
            on_chain: host.is_on_chain(),
            reserved_gas: 0.into(),
            ..self.context.clone()
        };
        let executor = Executor {
            context: &context,
            order: self.order,
            overrides: self.overrides,
            contracts: self.contracts,
        };
        let mut result = executor.start_remote_program(
            LoadedCode {
                code: Arc::new(code),
                vm_version: spec.vm_version,
            },
            Value::Object(ObjectKind::Address, caller.clone()),
            host.clone(),
            &machine,
            &Entry {
                function,
                arguments,
                call_value: amount.clone(),
                allow_init: spec.allow_init,
            },
            limits,
        )?;
        // DEBUG_INFO initializes only after setup succeeds. Admission failures
        // still carry disabled debug state in the original debug build.
        if env.debug
            && let Outcome::Continue {
                state,
                remaining_limits,
            } = &mut result
        {
            let info = debug::Info::default();
            *remaining_limits = info.budget(*remaining_limits).map_err(Error::Data)?;
            state.program.cursor.debug = Some(info);
        }
        Ok(result)
    }

    pub fn run_invocation<H: finalize::StoreHost<S>>(
        &self,
        spec: &Spec<'_, S>,
        env: &Environment,
        host: &H,
        limits: Limits,
        max_steps: usize,
    ) -> Result<Outcome<S, H>, Error<S::Error>> {
        match self.setup_invocation(spec, env, host, limits)? {
            Outcome::Continue {
                state,
                remaining_limits,
            } => self.execute_remote_program(&state, remaining_limits, max_steps),
            terminal => Ok(terminal),
        }
    }
}
