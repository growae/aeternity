// Remote calls and protected recovery in aefa_fate / aefa_fate_op, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{State as ProgramState, *};
use crate::{
    types::Type,
    value::{ObjectKind, ObjectPayload},
};
use std::sync::Arc;
pub mod context;
pub mod deployment;
pub mod finalize;
pub mod hosts;
pub mod names;
pub mod oracle_write;
use context::{AddressTest, Environment, Log};

#[derive(Debug, Clone)]
pub struct LoadedCode {
    pub code: Arc<Code>,
    pub vm_version: VmVersion,
}
#[derive(Debug)]
pub enum HostFailure<E> {
    Unavailable(&'static str),
    AuthTransactionType(&'static str),
    Backend(E),
    ResourceLimit,
    Exception(Exception),
    Call(&'static str),
    Primop {
        operation: &'static str,
        reason: &'static str,
    },
    NameEncoding {
        operation: &'static str,
        error: ae_codec::idna::Error,
    },
}
// A cloned host is an isolated logical snapshot. Immutable backing nodes may
// be shared, but later mutations must not change an earlier snapshot's roots.
pub trait StoreAccess<E>: Clone {
    fn store_loaded(
        &mut self,
        contract: [u8; 32],
        limits: Limits,
    ) -> Result<Limits, HostFailure<E>>;
}
impl<E> StoreAccess<E> for () {
    fn store_loaded(&mut self, _: [u8; 32], limits: Limits) -> Result<Limits, HostFailure<E>> {
        Ok(limits)
    }
}
pub trait Host<E>: Clone + StoreAccess<E> {
    fn deploy(
        &mut self,
        creator: [u8; 32],
        source: &Value,
        amount: &Value,
        vm: VmVersion,
        protocol: u32,
        limits: Limits,
    ) -> Result<[u8; 32], HostFailure<E>>;
    fn remove_deployment(
        &mut self,
        contract: [u8; 32],
        current: [u8; 32],
        amount: &Value,
        limits: Limits,
    ) -> Result<(), HostFailure<E>>;
    fn auth_transaction(
        &mut self,
        vm: VmVersion,
        limits: Limits,
    ) -> Result<(BigInt, Value), HostFailure<E>>;
    fn code(
        &mut self,
        contract: &[u8],
        limits: Limits,
    ) -> Result<Option<LoadedCode>, HostFailure<E>>;
    fn transfer(
        &mut self,
        from: [u8; 32],
        to: [u8; 32],
        value: &BigInt,
    ) -> Result<(), HostFailure<E>>;
    fn height(&self) -> u64;
    fn spend(&mut self, from: [u8; 32], to: &[u8], value: &BigInt) -> Result<(), HostFailure<E>>;
    fn oracle_read(
        &mut self,
        request: hosts::OracleRead<'_>,
        limits: Limits,
    ) -> Result<Value, HostFailure<E>>;
    fn oracle_write(
        &mut self,
        request: &oracle_write::Request,
        protocol: u32,
        limits: Limits,
    ) -> Result<oracle_write::ResultValue, HostFailure<E>>;
    fn name_hash(
        &mut self,
        operation: &'static str,
        name: &[u8],
        limits: Limits,
    ) -> Result<[u8; 32], HostFailure<E>>;
    fn name_read(
        &mut self,
        name: &[u8],
        pointer: Option<&[u8]>,
        limits: Limits,
    ) -> Result<Option<names::Read>, HostFailure<E>>;
    fn name_write(
        &mut self,
        request: &names::Request,
        protocol: u32,
        vm: VmVersion,
        limits: Limits,
    ) -> Result<BigInt, HostFailure<E>>;
    fn verify_delegation(
        &mut self,
        signer: &[u8],
        message: &[u8],
        signature: &[u8; 64],
        vm: VmVersion,
    ) -> Result<bool, HostFailure<E>>;
    fn environment(&self) -> &Environment;
    fn balance(&mut self, key: &[u8]) -> Result<Option<BigInt>, HostFailure<E>>;
    fn creator(&mut self, key: [u8; 32]) -> Result<[u8; 32], HostFailure<E>>;
    fn address_test(&mut self, what: AddressTest, key: &[u8]) -> Result<bool, HostFailure<E>>;
    fn block_hash(
        &mut self,
        height: u64,
        vm: VmVersion,
        limits: Limits,
    ) -> Result<[u8; 32], HostFailure<E>>;
}
#[derive(Debug, Clone)]
pub struct RemoteFrame {
    pub local: LocalFrame,
    pub caller: Value,
    pub contract: [u8; 32],
    pub vm_version: VmVersion,
}
impl RemoteFrame {
    pub(super) fn budget(&self, mut limits: Limits) -> Result<Limits, DataError> {
        limits = value::check_limits(&self.caller, limits)?;
        limits = value::check_limits(&self.local.function, limits)?;
        limits = value::check_limits(&self.local.return_block, limits)?;
        limits = bindings_budget(&self.local.bindings, limits)?;
        limits = integer_budget(&self.local.call_value, limits)?;
        for v in &self.local.stack {
            limits = value::check_limits(v, limits)?;
        }
        memory_budget(&self.local.memory, limits)
    }
}
#[derive(Debug, Clone)]
pub struct ReturnCheck {
    pub returns: Type,
    pub bindings: Bindings,
    pub protected: bool,
    pub checkpoint: usize,
}
#[derive(Debug, Clone)]
pub struct Snapshot<S, H> {
    pub stores: Stores<S>,
    pub host: H,
}
#[derive(Debug, Clone)]
pub struct Session<S, H> {
    pub current_contract: [u8; 32],
    pub vm_version: VmVersion,
    pub caller: Value,
    // The first entry is the nearest caller.
    pub seen_contracts: Vec<[u8; 32]>,
    pub code_cache: BTreeMap<[u8; 32], LoadedCode>,
    pub host: H,
    pub snapshots: BTreeMap<usize, Snapshot<S, H>>,
    pub creator_cache: BTreeMap<[u8; 32], [u8; 32]>,
    pub logs: Vec<Log>,
}
#[derive(Debug, Clone)]
pub struct State<S, H> {
    pub program: ProgramState<S>,
    pub session: Session<S, H>,
}
#[derive(Debug, Clone)]
pub enum Outcome<S, H> {
    Break {
        state: State<S, H>,
        remaining_limits: Limits,
    },
    Continue {
        state: State<S, H>,
        remaining_limits: Limits,
    },
    Returned {
        state: State<S, H>,
        remaining_limits: Limits,
    },
    Abort {
        state: State<S, H>,
        reason: Reason,
        remaining_limits: Limits,
    },
    Revert {
        state: State<S, H>,
        value: Vec<u8>,
        remaining_limits: Limits,
    },
    Exception(Exception),
}
fn outcome<S, H>(out: super::Outcome<S>, session: Session<S, H>) -> Outcome<S, H> {
    match out {
        super::Outcome::Break {
            state,
            remaining_limits,
        } => Outcome::Break {
            state: State {
                program: state,
                session,
            },
            remaining_limits,
        },
        super::Outcome::Continue {
            state,
            remaining_limits,
        } => Outcome::Continue {
            state: State {
                program: state,
                session,
            },
            remaining_limits,
        },
        super::Outcome::Returned {
            state,
            remaining_limits,
        } => Outcome::Returned {
            state: State {
                program: state,
                session,
            },
            remaining_limits,
        },
        super::Outcome::Abort {
            state,
            reason,
            remaining_limits,
        } => Outcome::Abort {
            state: State {
                program: state,
                session,
            },
            reason,
            remaining_limits,
        },
        super::Outcome::Revert {
            state,
            value,
            remaining_limits,
        } => Outcome::Revert {
            state: State {
                program: state,
                session,
            },
            value,
            remaining_limits,
        },
        super::Outcome::Exception(e) => Outcome::Exception(e),
    }
}
fn address(key: [u8; 32]) -> Value {
    Value::Object(ObjectKind::Address, ObjectPayload::Bytes(key.to_vec()))
}
fn option(value: Option<Value>) -> Value {
    Value::Variant {
        arities: vec![0.into(), 1.into()],
        tag: usize::from(value.is_some()),
        values: value.into_iter().collect(),
    }
}
impl<S, H> Session<S, H> {
    fn budget(&self, mut limits: Limits) -> Result<Limits, DataError> {
        limits = value::check_limits(&self.caller, limits)?;
        let count = self
            .seen_contracts
            .len()
            .checked_add(self.code_cache.len())
            .and_then(|n| n.checked_add(self.snapshots.len()))
            .and_then(|n| n.checked_add(self.creator_cache.len().checked_mul(2)?))
            .ok_or(DataError::ResourceLimit)?;
        limits.max_nodes = limits
            .max_nodes
            .checked_sub(count)
            .ok_or(DataError::ResourceLimit)?;
        limits.max_bytes = limits
            .max_bytes
            .checked_sub(count.checked_mul(32).ok_or(DataError::ResourceLimit)?)
            .ok_or(DataError::ResourceLimit)?;
        for log in &self.logs {
            limits = log.budget(limits)?;
        }
        Ok(limits)
    }
    fn code<E>(&self) -> Result<Arc<Code>, Error<E>> {
        self.code_cache
            .get(&self.current_contract)
            .map(|c| Arc::clone(&c.code))
            .ok_or(Error::NeedCode(self.current_contract))
    }
}
impl<S: CachedSubtreeBackend, O: MapOrder> Executor<'_, S, O> {
    pub fn start_remote_program<H: Host<S::Error>>(
        &self,
        code: LoadedCode,
        caller: Value,
        host: H,
        machine: &MachineState<S>,
        entry: &Entry,
        limits: Limits,
    ) -> Result<Outcome<S, H>, Error<S::Error>> {
        let limits = host
            .environment()
            .budget(value::check_limits(&caller, limits).map_err(Error::Data)?)
            .map_err(Error::Data)?;
        let context = Context {
            vm_version: code.vm_version,
            ..self.context.clone()
        };
        let executor = Executor {
            context: &context,
            order: self.order,
            overrides: self.overrides,
            contracts: self.contracts,
        };
        let result = executor.start_program(&code.code, machine, entry, limits)?;
        let session = Session {
            current_contract: context.current_contract,
            vm_version: code.vm_version,
            caller,
            host,
            code_cache: BTreeMap::from([(context.current_contract, code)]),
            seen_contracts: vec![],
            snapshots: BTreeMap::new(),
            creator_cache: BTreeMap::new(),
            logs: vec![],
        };
        Ok(outcome(result, session))
    }

    pub fn advance_remote_program<H: Host<S::Error>>(
        &self,
        input: &State<S, H>,
        limits: Limits,
    ) -> Result<Outcome<S, H>, Error<S::Error>> {
        let limits = input
            .session
            .host
            .environment()
            .budget(limits)
            .map_err(Error::Data)?;
        let limits = input
            .program
            .cursor
            .budget(input.session.budget(limits).map_err(Error::Data)?)
            .map_err(Error::Data)?;
        let mut cursor = input.program.cursor.clone();
        let mut session = input.session.clone();
        let code = session.code()?;
        let context = Context {
            current_contract: session.current_contract,
            vm_version: session.vm_version,
            reserved_gas: cursor.reserved(),
            ..self.context.clone()
        };
        let executor = Executor {
            context: &context,
            order: self.order,
            overrides: self.overrides,
            contracts: self.contracts,
        };
        let instructions = block(&code, &cursor);
        let instruction = instructions.and_then(|is| is.get(cursor.instruction));
        let dispatched = instruction.is_some() && !cursor.returned;
        let (result, host) = if cursor.returned || cursor.suspended {
            executor.action_with_host(
                &input.program.machine,
                &[],
                limits,
                session.host.clone(),
                |_| Ok(()),
            )?
        } else if let Some(ins) = instruction {
            let supported = ops::supported(ins.opcode)
                || debug::supported(ins.opcode)
                || context::supported(ins.opcode)
                || hosts::supported(ins.opcode)
                || oracle_write::supported(ins.opcode)
                || names::supported(ins.opcode)
                || matches!(
                    ins.opcode,
                    Opcode::Create
                        | Opcode::Clone
                        | Opcode::CloneG
                        | Opcode::Call
                        | Opcode::CallT
                        | Opcode::Return
                        | Opcode::Returnr
                        | Opcode::CallValue
                        | Opcode::CallR
                        | Opcode::CallGr
                        | Opcode::CallPgr
                        | Opcode::Address
                        | Opcode::Caller
                );
            executor.step_with_host(
                &input.program.machine,
                ins,
                limits,
                supported,
                cursor.debug.is_some(),
                session.host.clone(),
                |step| {
                    step.defer_program_jumps = true;
                    let result = step
                        .remote_instruction(&code, &mut cursor, &mut session, ins)
                        .and_then(|()| step.debug_after_instruction(&mut cursor));
                    session.current_contract = step.context.current_contract;
                    session.vm_version = step.context.vm_version;
                    step.reserved_gas = cursor.reserved();
                    result
                },
            )?
        } else {
            executor.action_with_host(
                &input.program.machine,
                &[],
                limits,
                session.host.clone(),
                |step| {
                    if instructions.is_none() {
                        return Err(Fault::Abort(Reason::MissingBlock(cursor.block.clone())));
                    }
                    let Value::Integer(n) = &cursor.block else {
                        return Err(step.native("badarith"));
                    };
                    let target = Value::Integer(step.native_integer(n + 1)?);
                    step.jump_program(&code, &mut cursor, target)
                },
            )?
        };
        session.host = host;
        let result = outcome(super::outcome(result, cursor), session);
        let failed_limits = match &result {
            Outcome::Abort {
                remaining_limits, ..
            }
            | Outcome::Revert {
                remaining_limits, ..
            } => Some(*remaining_limits),
            _ => None,
        };
        let result = if dispatched
            && let Some(failed_limits) = failed_limits
            && !input.program.cursor.frames.is_empty()
        {
            // aefa_fate:step unwinds from the instruction-entry engine state,
            // not the state (or consumed gas) carried by the thrown error.
            let mut cursor = input.program.cursor.clone();
            let mut session = input.session.clone();
            let mut caught = false;
            let (unwound, host) = executor.action_with_host(
                &input.program.machine,
                &[],
                failed_limits,
                session.host.clone(),
                |step| {
                    step.defer_program_jumps = true;
                    step.reset_debug_on_jump = false;
                    let r = step
                        .unwind_protected(&mut cursor, &mut session)
                        .map(|handled| caught = handled);
                    session.current_contract = step.context.current_contract;
                    session.vm_version = step.context.vm_version;
                    step.reserved_gas = cursor.reserved();
                    r
                },
            )?;
            session.host = host;
            if !caught && matches!(unwound, crate::machine::Outcome::Continue { .. }) {
                // Reaching the end rethrows the original error with its original
                // state. An init continuation may instead throw a replacement.
                result
            } else {
                outcome(super::outcome(unwound, cursor), session)
            }
        } else {
            result
        };
        // Actual basic-block lookup happens in aefa_fate:jump, outside the
        // protected dispatch catch. A nonexistent jump target is not caught.
        if let Outcome::Continue {
            mut state,
            remaining_limits,
        } = result
        {
            if state.program.cursor.instruction == 0 {
                let code = state.session.code()?;
                let context = Context {
                    current_contract: state.session.current_contract,
                    vm_version: state.session.vm_version,
                    reserved_gas: state.program.cursor.reserved(),
                    ..self.context.clone()
                };
                let ex = Executor {
                    context: &context,
                    order: self.order,
                    overrides: self.overrides,
                    contracts: self.contracts,
                };
                let target = state.program.cursor.block.clone();
                let out = ex.action(&state.program.machine, &[], remaining_limits, |step| {
                    step.reset_debug_on_jump = false;
                    step.jump_program(&code, &mut state.program.cursor, target)
                })?;
                return Ok(outcome(
                    super::outcome(out, state.program.cursor),
                    state.session,
                ));
            }
            Ok(Outcome::Continue {
                state,
                remaining_limits,
            })
        } else {
            Ok(result)
        }
    }

    pub fn run_remote_program<H: Host<S::Error>>(
        &self,
        input: &State<S, H>,
        mut limits: Limits,
        max_steps: usize,
    ) -> Result<Outcome<S, H>, Error<S::Error>> {
        let mut next = None;
        for _ in 0..max_steps {
            match self.advance_remote_program(next.as_ref().unwrap_or(input), limits)? {
                Outcome::Continue {
                    state,
                    remaining_limits,
                } => {
                    next = Some(state);
                    limits = remaining_limits;
                }
                terminal => return Ok(terminal),
            }
        }
        Err(Error::Data(DataError::ResourceLimit))
    }
}
impl<S: CachedSubtreeBackend, O: MapOrder, H: Host<S::Error>> Step<'_, '_, S, O, H> {
    fn frame_room(&self, cursor: &Cursor) -> Result<(), Fault<S::Error>> {
        if cursor.frames.len() >= self.remaining.max_depth {
            Err(DataError::ResourceLimit.into())
        } else {
            Ok(())
        }
    }
    fn remote_instruction(
        &mut self,
        code: &Code,
        cursor: &mut Cursor,
        session: &mut Session<S, H>,
        ins: &Instruction,
    ) -> Result<(), Fault<S::Error>> {
        match ins.opcode {
            op if names::supported(op) => {
                self.name_instruction(session, ins)?;
                cursor.instruction += 1;
                Ok(())
            }
            op if oracle_write::supported(op) => {
                self.oracle_write(session, ins)?;
                cursor.instruction += 1;
                Ok(())
            }
            op if hosts::supported(op) => {
                self.host_instruction(session, ins)?;
                cursor.instruction += 1;
                Ok(())
            }
            op if context::supported(op) => {
                self.context_instruction(session, ins)?;
                cursor.instruction += 1;
                Ok(())
            }
            Opcode::Create | Opcode::Clone | Opcode::CloneG => {
                self.deploy_contract(cursor, session, ins)
            }
            Opcode::CallR | Opcode::CallGr | Opcode::CallPgr => {
                self.remote_call(cursor, session, ins)
            }
            Opcode::Return | Opcode::Returnr => {
                if ins.opcode == Opcode::Returnr {
                    let value = self.read(&ins.operands[0])?;
                    self.write(&Operand::Stack, value)?;
                }
                self.remote_return(cursor, session)
            }
            Opcode::Address | Opcode::Caller => {
                let value = if ins.opcode == Opcode::Address {
                    address(self.context.current_contract)
                } else {
                    session.caller.clone()
                };
                self.write(&ins.operands[0], value)?;
                cursor.instruction += 1;
                Ok(())
            }
            _ => self.program_instruction(code, cursor, ins),
        }
    }
    fn remote_call(
        &mut self,
        cursor: &mut Cursor,
        session: &mut Session<S, H>,
        ins: &Instruction,
    ) -> Result<(), Fault<S::Error>> {
        if ins.opcode == Opcode::CallPgr && self.context.vm_version < VmVersion::Two {
            return Err(Fault::Abort(Reason::Unavailable("call_pgr")));
        }
        let contract = self.read(&ins.operands[0])?;
        let arg_types = self.read(&ins.operands[2])?;
        let ret_type = self.read(&ins.operands[3])?;
        let value = self.read(&ins.operands[4])?;
        let cap = if ins.opcode != Opcode::CallR {
            Some(self.read(&ins.operands[5])?)
        } else {
            None
        };
        let protected = if ins.opcode == Opcode::CallPgr {
            match self.read(&ins.operands[6])? {
                Value::Boolean(b) => b,
                v => {
                    self.state = self.entry.clone();
                    self.host = self.entry_host.clone();
                    return Err(Fault::Abort(Reason::ValueType {
                        value: v,
                        expected: "bool",
                    }));
                }
            }
        } else {
            false
        };
        let Operand::Immediate(function) = &ins.operands[1] else {
            unreachable!()
        };
        if !self.remote_call_common(
            cursor,
            session,
            contract,
            function.clone(),
            arg_types,
            ret_type,
            value,
            cap,
            protected,
            None,
        )? {
            self.write(&Operand::Stack, option(None))?;
            cursor.instruction += 1;
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    fn remote_call_common(
        &mut self,
        cursor: &mut Cursor,
        session: &mut Session<S, H>,
        contract: Value,
        function: Value,
        arg_types: Value,
        ret_type: Value,
        value: Value,
        cap: Option<Value>,
        protected: bool,
        continuation: Option<deployment::InitFrame>,
    ) -> Result<bool, Fault<S::Error>> {
        let allow_init = continuation.is_some();
        let (Value::TypeRep(Type::Tuple(arg_types)), Value::TypeRep(ret_type)) =
            (arg_types, ret_type)
        else {
            return Err(self.native("function_clause"));
        };
        let n = arg_types.len();
        if n > self.remaining.max_nodes {
            return Err(DataError::ResourceLimit.into());
        }
        if n > self.state.accumulator.rest.len().saturating_add(1) && self.state.meter.protocol >= 5
        {
            return Err(Fault::Abort(Reason::EmptyStack));
        }
        let args: Vec<_> = std::iter::once(&self.state.accumulator.top)
            .chain(self.state.accumulator.rest.iter())
            .take(n)
            .cloned()
            .collect();
        self.state.accumulator.drop_n(n, &mut self.state.meter);
        self.native_meter()?;
        let Value::List(args) = self.unfold(Value::List(args), CostModel::Unfold)? else {
            unreachable!()
        };
        // Both popping and unfolding arguments precede the setup recovery.
        let recovery = protected.then(|| {
            (
                self.state.clone(),
                cursor.clone(),
                session.clone(),
                self.context.clone(),
                self.host.clone(),
            )
        });
        let result = (|| {
            self.frame_room(cursor)?;
            let Value::Integer(bb) = &cursor.block else {
                return Err(self.native("badarith"));
            };
            let return_block = Value::Integer(self.native_integer(bb + 1)?);
            let mut stack = std::mem::take(&mut self.state.accumulator.rest);
            let acc = std::mem::replace(&mut self.state.accumulator.top, Value::Void);
            if acc != Value::Void {
                stack.push_front(acc);
            }
            cursor.debug_push();
            cursor.frames.push(Frame::Remote(Box::new(RemoteFrame {
                local: LocalFrame {
                    function: cursor.function.clone(),
                    bindings: cursor.bindings.clone(),
                    return_block,
                    stack,
                    memory: std::mem::take(&mut self.state.memory),
                    call_value: cursor.call_value.clone(),
                },
                caller: session.caller.clone(),
                contract: self.context.current_contract,
                vm_version: self.context.vm_version,
            })));
            if let Some(cap) = cap {
                let Value::Integer(cap) = cap else {
                    return Err(Fault::Abort(Reason::ValueType {
                        value: cap,
                        expected: "integer",
                    }));
                };
                if cap <= BigInt::from(0) {
                    return Err(Fault::Abort(Reason::BadGasCap(cap)));
                }
                if cap < self.state.meter.gas {
                    self.frame_room(cursor)?;
                    let stored = self.native_integer(&self.state.meter.gas - &cap)?;
                    cursor.frames.push(Frame::Gas(stored));
                    self.state.meter.gas = cap;
                }
            }
            let Value::Object(ObjectKind::Contract, ObjectPayload::Bytes(key)) = &contract else {
                return Err(Fault::Abort(Reason::ValueType {
                    value: contract.clone(),
                    expected: "contract",
                }));
            };
            if key.as_slice() == self.context.current_contract
                || session.seen_contracts.iter().any(|k| key.as_slice() == k)
            {
                return Err(Fault::Abort(Reason::ReentrantCall));
            }
            session
                .seen_contracts
                .insert(0, self.context.current_contract);
            let from = self.context.current_contract;
            let payable = match &value {
                Value::Integer(n) => n > &BigInt::from(0),
                _ => true,
            };
            self.load_remote(
                session,
                cursor,
                key,
                function.clone(),
                address(from),
                payable,
                true,
                allow_init,
            )?;
            let code = session.code().map_err(Fault::Operational)?;
            let f = self.program_function(&code, &function)?;
            if args.len() != f.arguments.len() {
                return Err(Fault::Abort(Reason::FunctionArity {
                    given: args.len(),
                    expected: f.arguments.len(),
                }));
            }
            let expected = Type::Tuple(f.arguments.clone());
            cursor.bindings = types::check_type(
                &expected,
                &Value::Tuple(args.clone()),
                self.context.vm_version,
                self.remaining,
            )?
            .ok_or_else(|| {
                Fault::Abort(Reason::CallType {
                    arguments: args.clone(),
                    expected: f.arguments.clone(),
                })
            })?;
            if let Some(continuation) = continuation {
                self.frame_room(cursor)?;
                cursor.frames.push(Frame::Init(Box::new(continuation)));
            }
            let callee = Type::Tuple(
                std::iter::once(f.returns.clone())
                    .chain(f.arguments.iter().cloned())
                    .collect(),
            );
            let caller = Type::Tuple(std::iter::once(ret_type.clone()).chain(arg_types).collect());
            let callee = types::instantiate(&cursor.bindings, &callee, self.remaining)?;
            let bindings =
                types::match_type(&caller, &callee, self.context.vm_version, self.remaining)?
                    .ok_or(Fault::Abort(Reason::RemoteTypeMismatch))?;
            self.frame_room(cursor)?;
            let checkpoint = cursor.frames.len();
            session.snapshots.insert(
                checkpoint,
                Snapshot {
                    stores: self.state.stores.clone(),
                    host: self.host.clone(),
                },
            );
            cursor.frames.push(Frame::ReturnCheck(Box::new(ReturnCheck {
                returns: ret_type,
                bindings,
                protected,
                checkpoint,
            })));
            self.bind_arguments(args);
            let Value::Integer(amount) = value else {
                return Err(Fault::Abort(Reason::ValueType {
                    value,
                    expected: "integer",
                }));
            };
            if amount.sign() == Sign::Minus {
                return Err(Fault::Abort(Reason::CallError("negative_value")));
            }
            cursor.call_value = amount.clone();
            if amount != BigInt::from(0) {
                self.host
                    .transfer(from, self.context.current_contract, &amount)
                    .map_err(Self::host_fault)?;
            }
            self.jump_program(&code, cursor, Value::Integer(0.into()))
        })();
        match (result, recovery) {
            (
                Err(Fault::Abort(_) | Fault::Revert(_) | Fault::Exception(_)),
                Some((state, old_cursor, old_session, context, host)),
            ) => {
                self.state = state;
                *cursor = old_cursor;
                *session = old_session;
                self.host = host;
                self.context = context;
                Ok(false)
            }
            (result, _) => result.map(|()| true),
        }
    }
    fn code_fetch_gas(&mut self) -> Result<(), Fault<S::Error>> {
        self.spend(if self.state.meter.protocol >= 5 {
            4900.into()
        } else {
            0.into()
        })
    }
    fn fetch_code(
        &mut self,
        session: &mut Session<S, H>,
        key: &[u8],
    ) -> Result<Option<LoadedCode>, Fault<S::Error>> {
        let fixed = <&[u8; 32]>::try_from(key).ok().copied();
        let loaded = if let Some(c) = fixed.and_then(|k| session.code_cache.get(&k)) {
            c.clone()
        } else {
            let gas = self.state.meter.gas.clone();
            let host_before = self.host.clone();
            self.code_fetch_gas()?;
            let Some(c) = self
                .host
                .code(key, self.remaining)
                .map_err(Self::host_fault)?
            else {
                self.state.meter.gas = gas;
                self.host = host_before;
                return Ok(None);
            };
            let fixed = fixed
                .ok_or_else(|| Fault::Operational(Error::UnsupportedContractKey(key.to_vec())))?;
            session.code_cache.insert(fixed, c.clone());
            c
        };
        Ok(Some(loaded))
    }
    #[allow(clippy::too_many_arguments)]
    fn load_remote(
        &mut self,
        session: &mut Session<S, H>,
        cursor: &mut Cursor,
        key: &[u8],
        function: Value,
        caller: Value,
        payable: bool,
        private: bool,
        allow_init: bool,
    ) -> Result<(), Fault<S::Error>> {
        let loaded = self
            .fetch_code(session, key)?
            .ok_or_else(|| Fault::Abort(Reason::MissingContract(key.to_vec())))?;
        let fixed = <[u8; 32]>::try_from(key)
            .map_err(|_| Fault::Operational(Error::UnsupportedContractKey(key.to_vec())))?;
        self.context.current_contract = fixed;
        self.context.vm_version = loaded.vm_version;
        session.current_contract = fixed;
        session.vm_version = loaded.vm_version;
        session.caller = caller;
        if payable || private {
            let f = self.program_function(&loaded.code, &function)?;
            if payable && !f.attributes.contains(&Attribute::Payable) {
                return Err(Fault::Abort(Reason::NonPayableFunction(function)));
            }
            if private && f.attributes.contains(&Attribute::Private) {
                return Err(Fault::Abort(Reason::PrivateFunction(function)));
            }
        }
        self.set_function(&loaded.code, cursor, function, allow_init)
    }
    fn check_return(&mut self, check: &ReturnCheck) -> Result<(), Fault<S::Error>> {
        let value = self.state.accumulator.top.clone();
        self.simple_traversal(&value)?;
        let Some(bindings) = types::check_type(
            &check.returns,
            &value,
            self.context.vm_version,
            self.remaining,
        )?
        else {
            return Err(Fault::Abort(Reason::BadReturnType {
                value,
                expected: check.returns.clone(),
            }));
        };
        if !types::compatible_bindings(&bindings, &check.bindings, self.remaining)? {
            let expected = types::instantiate(&check.bindings, &check.returns, self.remaining)?;
            return Err(Fault::Abort(Reason::BadReturnType { value, expected }));
        }
        Ok(())
    }
    fn remote_return(
        &mut self,
        cursor: &mut Cursor,
        session: &mut Session<S, H>,
    ) -> Result<(), Fault<S::Error>> {
        while let Some(frame) = cursor.frames.pop() {
            match frame {
                Frame::Init(frame) => self.finish_init(session, &frame)?,
                Frame::Gas(n) => {
                    self.state.meter.gas = self.native_integer(&self.state.meter.gas + n)?
                }
                Frame::Local(frame) => return self.restore_local(cursor, session, *frame),
                Frame::Remote(frame) => return self.restore_remote(cursor, session, *frame, true),
                Frame::ReturnCheck(check) => {
                    let snapshot = session
                        .snapshots
                        .remove(&check.checkpoint)
                        .ok_or(Fault::Operational(Error::Data(DataError::InvalidValue)))?;
                    let before = check.protected.then(|| self.state.clone());
                    match self.check_return(&check) {
                        Ok(()) if check.protected => {
                            let v = option(Some(self.state.accumulator.top.clone()));
                            self.check(&v)?;
                            self.state.accumulator.top = v;
                        }
                        Ok(()) => {}
                        Err(Fault::Abort(_) | Fault::Revert(_) | Fault::Exception(_))
                            if check.protected =>
                        {
                            self.state = before.unwrap();
                            self.state.stores = snapshot.stores;
                            self.host = snapshot.host;
                            self.state.accumulator.top = option(None);
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        self.state.accumulator.top =
            self.unfold(self.state.accumulator.top.clone(), CostModel::Unfold)?;
        cursor.returned = true;
        Ok(())
    }
    fn restore_local(
        &mut self,
        cursor: &mut Cursor,
        session: &Session<S, H>,
        frame: LocalFrame,
    ) -> Result<(), Fault<S::Error>> {
        cursor.debug_pop();
        self.state.accumulator.rest = frame.stack;
        self.state.memory = frame.memory;
        cursor.call_value = frame.call_value;
        let code = session.code().map_err(Fault::Operational)?;
        self.set_function(&code, cursor, frame.function, true)?;
        cursor.bindings = frame.bindings;
        self.jump_program(&code, cursor, frame.return_block)
    }
    fn pop_remote(
        &mut self,
        session: &mut Session<S, H>,
        frame: &RemoteFrame,
    ) -> Result<bool, Fault<S::Error>> {
        if frame.contract == self.context.current_contract {
            return Ok(false);
        }
        let index = session
            .seen_contracts
            .iter()
            .position(|k| *k == frame.contract)
            .ok_or_else(|| self.native("badmatch"))?;
        session.seen_contracts.drain(..=index);
        if !(237000..=245000).contains(&self.host.height()) {
            self.context.current_contract = frame.contract;
        }
        self.context.vm_version = frame.vm_version;
        session.current_contract = self.context.current_contract;
        session.vm_version = self.context.vm_version;
        Ok(true)
    }
    fn restore_remote(
        &mut self,
        cursor: &mut Cursor,
        session: &mut Session<S, H>,
        frame: RemoteFrame,
        returning: bool,
    ) -> Result<(), Fault<S::Error>> {
        let callee = self.context.current_contract;
        cursor.debug_pop();
        let remote = self.pop_remote(session, &frame)?;
        self.state.accumulator.rest = frame.local.stack;
        self.state.memory = frame.local.memory;
        cursor.call_value = frame.local.call_value;
        if !returning {
            return Ok(());
        }
        if remote {
            let old = self.context.current_contract;
            self.context.current_contract = callee;
            self.state.accumulator.top =
                self.unfold(self.state.accumulator.top.clone(), CostModel::Unfold)?;
            self.context.current_contract = old;
            self.load_remote(
                session,
                cursor,
                &frame.contract,
                frame.local.function,
                frame.caller,
                false,
                false,
                true,
            )?;
        } else {
            let code = session.code().map_err(Fault::Operational)?;
            self.set_function(&code, cursor, frame.local.function, true)?;
        }
        cursor.bindings = frame.local.bindings;
        let code = session.code().map_err(Fault::Operational)?;
        self.jump_program(&code, cursor, frame.local.return_block)
    }
    fn unwind_protected(
        &mut self,
        cursor: &mut Cursor,
        session: &mut Session<S, H>,
    ) -> Result<bool, Fault<S::Error>> {
        while let Some(frame) = cursor.frames.pop() {
            match frame {
                Frame::Init(frame) => {
                    if self.context.vm_version < VmVersion::Three {
                        self.finish_init(session, &frame)?;
                    }
                }
                Frame::Gas(n) => {
                    self.state.meter.gas = self.native_integer(&self.state.meter.gas + n)?
                }
                Frame::Local(frame) => {
                    cursor.debug_pop();
                    self.state.accumulator.rest = frame.stack;
                    self.state.memory = frame.memory;
                    cursor.call_value = frame.call_value;
                }
                Frame::Remote(frame) => self.restore_remote(cursor, session, *frame, false)?,
                Frame::ReturnCheck(check) => {
                    let snapshot = session
                        .snapshots
                        .remove(&check.checkpoint)
                        .ok_or(Fault::Operational(Error::Data(DataError::InvalidValue)))?;
                    if check.protected {
                        self.state.stores = snapshot.stores;
                        self.host = snapshot.host;
                        self.state.accumulator.top = option(None);
                        return self.remote_return(cursor, session).map(|()| true);
                    }
                }
            }
        }
        Ok(false)
    }
}
