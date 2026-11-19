// aefa_fate / aefa_engine_state local execution, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{Outcome as InstructionOutcome, State as MachineState, *};
use crate::{
    code::{Attribute, Code, Function, INIT_ID},
    gas::CostModel,
    types::{self, Bindings},
};
use num_traits::ToPrimitive;
use std::collections::VecDeque;
pub mod debug;
pub mod remote;

#[derive(Debug, Clone)]
pub struct LocalFrame {
    pub function: Value,
    pub bindings: Bindings,
    pub return_block: Value,
    pub stack: VecDeque<Value>,
    pub memory: BTreeMap<Register, Value>,
    pub call_value: BigInt,
}
#[derive(Debug, Clone)]
pub enum Frame {
    Init(Box<remote::deployment::InitFrame>),
    Local(Box<LocalFrame>),
    Gas(BigInt),
    Remote(Box<remote::RemoteFrame>),
    ReturnCheck(Box<remote::ReturnCheck>),
}
#[derive(Debug, Clone)]
pub struct Cursor {
    pub function: Value,
    pub block: Value,
    pub instruction: usize,
    pub bindings: Bindings,
    // Last entry is the top of the original call stack.
    pub frames: Vec<Frame>,
    pub call_value: BigInt,
    pub returned: bool,
    pub debug: Option<debug::Info>,
    pub suspended: bool,
}
#[derive(Debug, Clone)]
pub struct State<S> {
    pub machine: MachineState<S>,
    pub cursor: Cursor,
}
#[derive(Debug, Clone)]
pub enum Outcome<S> {
    Break {
        state: State<S>,
        remaining_limits: Limits,
    },
    Continue {
        state: State<S>,
        remaining_limits: Limits,
    },
    // The result has been unfolded. Store finalization and final serialization
    // traversal are still required by the chain execution adapter.
    Returned {
        state: State<S>,
        remaining_limits: Limits,
    },
    Abort {
        state: State<S>,
        reason: Reason,
        remaining_limits: Limits,
    },
    Revert {
        state: State<S>,
        value: Vec<u8>,
        remaining_limits: Limits,
    },
    Exception(Exception),
}
pub struct Entry {
    pub function: Value,
    pub arguments: Vec<Value>,
    pub call_value: BigInt,
    pub allow_init: bool,
}
fn function<'a>(code: &'a Code, name: &Value) -> Option<&'a Function> {
    let Value::String(id) = name else {
        return None;
    };
    code.functions.get(id)
}
fn block<'a>(code: &'a Code, cursor: &Cursor) -> Option<&'a [Instruction]> {
    let f = function(code, &cursor.function)?;
    let Value::Integer(n) = &cursor.block else {
        return None;
    };
    f.blocks.get(&n.to_usize()?).map(Vec::as_slice)
}
fn bindings_budget(bindings: &Bindings, mut limits: Limits) -> Result<Limits, DataError> {
    for ty in bindings.values() {
        limits = value::check_type_limits(ty, limits)?;
    }
    Ok(limits)
}
fn memory_budget(
    memory: &BTreeMap<Register, Value>,
    mut limits: Limits,
) -> Result<Limits, DataError> {
    for (key, v) in memory {
        let (Register::Argument(k) | Register::Variable(k)) = key;
        limits = value::check_limits(k, limits)?;
        limits = value::check_limits(v, limits)?;
    }
    Ok(limits)
}
fn integer_budget(n: &BigInt, mut limits: Limits) -> Result<Limits, DataError> {
    limits.max_nodes = limits
        .max_nodes
        .checked_sub(1)
        .ok_or(DataError::ResourceLimit)?;
    let size =
        usize::try_from(n.magnitude().bits().div_ceil(8)).map_err(|_| DataError::ResourceLimit)?;
    limits.max_bytes = limits
        .max_bytes
        .checked_sub(size)
        .ok_or(DataError::ResourceLimit)?;
    Ok(limits)
}
impl Cursor {
    fn budget(&self, mut limits: Limits) -> Result<Limits, DataError> {
        if let Some(info) = &self.debug {
            limits = info.budget(limits)?;
        }
        if self.frames.len() > limits.max_depth {
            return Err(DataError::ResourceLimit);
        }
        limits = value::check_limits(&self.function, limits)?;
        limits = value::check_limits(&self.block, limits)?;
        limits = bindings_budget(&self.bindings, limits)?;
        limits = integer_budget(&self.call_value, limits)?;
        for frame in &self.frames {
            match frame {
                Frame::Init(f) => {
                    limits = value::check_limits(&f.source, limits)?;
                    limits = value::check_limits(&f.amount, limits)?;
                }
                Frame::Gas(n) => limits = integer_budget(n, limits)?,
                Frame::Local(f) => {
                    limits = value::check_limits(&f.function, limits)?;
                    limits = value::check_limits(&f.return_block, limits)?;
                    limits = bindings_budget(&f.bindings, limits)?;
                    limits = integer_budget(&f.call_value, limits)?;
                    for v in &f.stack {
                        limits = value::check_limits(v, limits)?;
                    }
                    limits = memory_budget(&f.memory, limits)?;
                }
                Frame::Remote(f) => {
                    limits = f.budget(limits)?;
                }
                Frame::ReturnCheck(f) => {
                    limits = bindings_budget(&f.bindings, limits)?;
                    limits = value::check_type_limits(&f.returns, limits)?;
                }
            }
        }
        Ok(limits)
    }
    fn reserved(&self) -> BigInt {
        self.frames
            .iter()
            .filter_map(|f| if let Frame::Gas(n) = f { Some(n) } else { None })
            .sum()
    }
}
fn outcome<S>(result: InstructionOutcome<S, ()>, cursor: Cursor) -> Outcome<S> {
    match result {
        InstructionOutcome::Continue {
            state,
            remaining_limits,
            ..
        } => {
            let returned = cursor.returned;
            let state = State {
                machine: state,
                cursor,
            };
            if state.cursor.suspended {
                Outcome::Break {
                    state,
                    remaining_limits,
                }
            } else if returned {
                Outcome::Returned {
                    state,
                    remaining_limits,
                }
            } else {
                Outcome::Continue {
                    state,
                    remaining_limits,
                }
            }
        }
        InstructionOutcome::Abort {
            state,
            reason,
            remaining_limits,
        } => Outcome::Abort {
            state: State {
                machine: state,
                cursor,
            },
            reason,
            remaining_limits,
        },
        InstructionOutcome::Revert {
            state,
            value,
            remaining_limits,
        } => Outcome::Revert {
            state: State {
                machine: state,
                cursor,
            },
            value,
            remaining_limits,
        },
        InstructionOutcome::Exception(e) => Outcome::Exception(e),
    }
}
impl<S: CachedSubtreeBackend, O: MapOrder> Executor<'_, S, O> {
    // Enter an already loaded contract; code retrieval, transaction admission,
    // remote calls and chain publication belong to the surrounding adapter.
    pub fn start_program(
        &self,
        code: &Code,
        machine: &MachineState<S>,
        entry: &Entry,
        mut limits: Limits,
    ) -> Result<Outcome<S>, Error<S::Error>> {
        limits = value::check_limits(&entry.function, limits).map_err(Error::Data)?;
        limits = integer_budget(&entry.call_value, limits).map_err(Error::Data)?;
        limits = integer_budget(&self.context.reserved_gas, limits).map_err(Error::Data)?;
        for v in &entry.arguments {
            limits = value::check_limits(v, limits).map_err(Error::Data)?;
        }
        let mut cursor = Cursor {
            function: Value::Void,
            block: Value::Integer(0.into()),
            instruction: 0,
            bindings: Bindings::new(),
            frames: vec![],
            call_value: entry.call_value.clone(),
            returned: false,
            debug: None,
            suspended: false,
        };
        // Reserved gas in the instruction executor's context models an outer
        // gas_store. Local execution retains that reservation until return.
        if self.context.reserved_gas != BigInt::from(0) {
            cursor
                .frames
                .push(Frame::Gas(self.context.reserved_gas.clone()));
        }
        let result = self.action(machine, &[], limits, |step| {
            let f = step.program_function(code, &entry.function)?;
            if entry.call_value > BigInt::from(0) && !f.attributes.contains(&Attribute::Payable) {
                return Err(Fault::Abort(Reason::NonPayableFunction(
                    entry.function.clone(),
                )));
            }
            if f.attributes.contains(&Attribute::Private) {
                return Err(Fault::Abort(Reason::PrivateFunction(
                    entry.function.clone(),
                )));
            }
            step.set_function(code, &mut cursor, entry.function.clone(), entry.allow_init)?;
            if entry.arguments.len() != f.arguments.len() {
                return Err(Fault::Abort(Reason::FunctionArity {
                    given: entry.arguments.len(),
                    expected: f.arguments.len(),
                }));
            }
            for ty in &f.arguments {
                step.remaining = value::check_type_limits(ty, step.remaining)?;
            }
            let expected = types::Type::Tuple(f.arguments.clone());
            let actual = Value::Tuple(entry.arguments.clone());
            cursor.bindings =
                types::check_type(&expected, &actual, self.context.vm_version, step.remaining)?
                    .ok_or_else(|| {
                        Fault::Abort(Reason::CallType {
                            arguments: entry.arguments.clone(),
                            expected: f.arguments.clone(),
                        })
                    })?;
            step.bind_arguments(entry.arguments.clone());
            Ok(())
        })?;
        Ok(outcome(result, cursor))
    }

    // One instruction, or a zero-cost fallthrough from an exhausted block.
    // Operational failure publishes no state, so the same input is retryable.
    pub fn advance_program(
        &self,
        code: &Code,
        input: &State<S>,
        limits: Limits,
    ) -> Result<Outcome<S>, Error<S::Error>> {
        let limits = input.cursor.budget(limits).map_err(Error::Data)?;
        let mut cursor = input.cursor.clone();
        let context = Context {
            reserved_gas: cursor.reserved(),
            ..self.context.clone()
        };
        let executor = Executor {
            context: &context,
            order: self.order,
            overrides: self.overrides,
            contracts: self.contracts,
        };
        let instructions = block(code, &cursor);
        let result = if cursor.returned || cursor.suspended {
            executor.action(&input.machine, &[], limits, |_| Ok(()))?
        } else if let Some(ins) = instructions.and_then(|is| is.get(cursor.instruction)) {
            let supported = ops::supported(ins.opcode)
                || debug::supported(ins.opcode)
                || matches!(
                    ins.opcode,
                    Opcode::Call
                        | Opcode::CallT
                        | Opcode::Return
                        | Opcode::Returnr
                        | Opcode::CallValue
                );
            executor.step_with(
                &input.machine,
                ins,
                limits,
                supported,
                cursor.debug.is_some(),
                |step| {
                    let result = step
                        .program_instruction(code, &mut cursor, ins)
                        .and_then(|()| step.debug_after_instruction(&mut cursor));
                    step.reserved_gas = cursor.reserved();
                    result
                },
            )?
        } else {
            executor.action(&input.machine, &[], limits, |step| {
                if instructions.is_none() {
                    return Err(Fault::Abort(Reason::MissingBlock(cursor.block.clone())));
                }
                let Value::Integer(n) = &cursor.block else {
                    return Err(step.native("badarith"));
                };
                let next = Value::Integer(step.native_integer(n + 1)?);
                step.jump_program(code, &mut cursor, next)
            })?
        };
        Ok(outcome(result, cursor))
    }

    // A bounded execution attempt. Exhaustion is operational, not out-of-gas;
    // callers can raise the work budget or resume using advance_program.
    pub fn run_program(
        &self,
        code: &Code,
        input: &State<S>,
        mut limits: Limits,
        max_steps: usize,
    ) -> Result<Outcome<S>, Error<S::Error>> {
        input.cursor.budget(limits).map_err(Error::Data)?;
        // Validate machine values before the first copy as in a single step.
        let mut next = None;
        for _ in 0..max_steps {
            let current = next.as_ref().unwrap_or(input);
            match self.advance_program(code, current, limits)? {
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
impl<S: CachedSubtreeBackend, O: MapOrder, H: StoreAccess<S::Error>> Step<'_, '_, S, O, H> {
    fn program_function<'c>(
        &self,
        code: &'c Code,
        name: &Value,
    ) -> Result<&'c Function, Fault<S::Error>> {
        function(code, name).ok_or_else(|| Fault::Abort(Reason::MissingFunction(name.clone())))
    }
    fn set_function(
        &mut self,
        code: &Code,
        cursor: &mut Cursor,
        name: Value,
        allow_init: bool,
    ) -> Result<(), Fault<S::Error>> {
        if !allow_init && name == Value::String(INIT_ID.to_vec()) {
            return Err(Fault::Abort(Reason::InvalidInitCall));
        }
        self.program_function(code, &name)?;
        cursor.function = name;
        cursor.block = Value::Integer(0.into());
        cursor.instruction = 0;
        Ok(())
    }
    fn jump_program(
        &mut self,
        code: &Code,
        cursor: &mut Cursor,
        target: Value,
    ) -> Result<(), Fault<S::Error>> {
        cursor.block = target;
        cursor.instruction = 0;
        if self.reset_debug_on_jump {
            cursor.debug_reset();
        }
        if !self.defer_program_jumps && block(code, cursor).is_none() {
            return Err(Fault::Abort(Reason::MissingBlock(cursor.block.clone())));
        }
        Ok(())
    }
    fn bind_arguments(&mut self, args: Vec<Value>) {
        self.state.memory = args
            .into_iter()
            .enumerate()
            .map(|(i, v)| (Register::Argument(Value::Integer(i.into())), v))
            .collect();
    }
    fn program_instruction(
        &mut self,
        code: &Code,
        cursor: &mut Cursor,
        ins: &Instruction,
    ) -> Result<(), Fault<S::Error>> {
        match ins.opcode {
            op if debug::supported(op) => self.debug_instruction(code, cursor, ins),
            Opcode::Call | Opcode::CallT => {
                let name = self.read(&ins.operands[0])?;
                let n = self.program_function(code, &name)?.arguments.len();
                if n > self.remaining.max_nodes {
                    return Err(DataError::ResourceLimit.into());
                }
                let available = self.state.accumulator.rest.len().saturating_add(1);
                if n > available && self.state.meter.protocol >= 5 {
                    return Err(Fault::Abort(Reason::EmptyStack(n)));
                }
                let args: Vec<_> = std::iter::once(&self.state.accumulator.top)
                    .chain(self.state.accumulator.rest.iter())
                    .take(n)
                    .cloned()
                    .collect();
                self.state.accumulator.drop_n(n, &mut self.state.meter);
                self.native_meter()?;
                if ins.opcode == Opcode::Call {
                    if cursor.frames.len() >= self.remaining.max_depth {
                        return Err(DataError::ResourceLimit.into());
                    }
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
                    cursor.frames.push(Frame::Local(Box::new(LocalFrame {
                        function: cursor.function.clone(),
                        bindings: cursor.bindings.clone(),
                        return_block,
                        stack,
                        memory: std::mem::take(&mut self.state.memory),
                        call_value: cursor.call_value.clone(),
                    })));
                }
                self.bind_arguments(args);
                self.set_function(
                    code,
                    cursor,
                    name,
                    self.context.vm_version == VmVersion::One,
                )?;
                self.jump_program(code, cursor, Value::Integer(0.into()))
            }
            Opcode::Return | Opcode::Returnr => {
                if ins.opcode == Opcode::Returnr {
                    let value = self.read(&ins.operands[0])?;
                    self.write(&Operand::Stack, value)?;
                }
                while let Some(frame) = cursor.frames.pop() {
                    match frame {
                        Frame::Gas(n) => {
                            self.state.meter.gas =
                                self.native_integer(&self.state.meter.gas + n)?;
                        }
                        Frame::Local(frame) => {
                            cursor.debug_pop();
                            self.state.accumulator.rest = frame.stack;
                            self.state.memory = frame.memory;
                            cursor.call_value = frame.call_value;
                            self.set_function(code, cursor, frame.function, true)?;
                            cursor.bindings = frame.bindings;
                            return self.jump_program(code, cursor, frame.return_block);
                        }
                        Frame::Remote(_) | Frame::ReturnCheck(_) | Frame::Init(_) => {
                            return Err(Fault::Operational(Error::Unsupported(ins.opcode)));
                        }
                    }
                }
                let value = self.unfold(self.state.accumulator.top.clone(), CostModel::Unfold)?;
                self.state.accumulator.top = value;
                cursor.returned = true;
                Ok(())
            }
            Opcode::CallValue => {
                self.write(&ins.operands[0], Value::Integer(cursor.call_value.clone()))?;
                cursor.instruction += 1;
                Ok(())
            }
            _ => match self.eval(ins)? {
                Control::Next => {
                    cursor.instruction += 1;
                    Ok(())
                }
                Control::Jump(target) => self.jump_program(code, cursor, target),
            },
        }
    }
}
