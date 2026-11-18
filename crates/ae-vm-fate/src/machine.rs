// aefa_gen_dispatch, aefa_fate and aefa_fate_op, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{
    Error as DataError, Limits,
    code::{Instruction, Operand},
    gas::{self, Meter},
    maps::{MapOrder, listing::Overrides},
    opcodes::Opcode,
    stack::{self, Accumulator},
    store::{Gas, RegisterRead, StoreError, Stores, read::CachedSubtreeBackend},
    types::Version as VmVersion,
    value::{self, Value},
};
use num_bigint::{BigInt, Sign};
use std::collections::BTreeMap;
mod ops;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Register {
    Argument(Value),
    Variable(Value),
}
#[derive(Debug, Clone)]
pub struct State<S> {
    pub accumulator: Accumulator,
    pub memory: BTreeMap<Register, Value>,
    pub meter: Meter,
    pub stores: Stores<S>,
}
// Reserved gas is the sum of gas_store entries in the caller's call stack.
// Authentication and chain mode must come from the VM invocation, not clients.
#[derive(Debug, Clone)]
pub struct Context {
    pub current_contract: [u8; 32],
    pub vm_version: VmVersion,
    pub in_auth: bool,
    pub on_chain: bool,
    pub reserved_gas: BigInt,
}
pub struct Executor<'a, S, O> {
    pub context: &'a Context,
    pub order: &'a O,
    pub overrides: &'a Overrides,
    // A fetched contract is inserted into the VM store only when the original
    // ensure_contract_store site is reached. Retrying a NeedContract result
    // with this source populated preserves all earlier failure checkpoints.
    pub contracts: &'a BTreeMap<[u8; 32], S>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reason {
    OutOfGas,
    EmptyStack,
    MissingMapKey,
    HeadEmpty,
    TailEmpty,
    Undefined(Register),
    Type {
        operation: &'static str,
        arguments: Vec<Value>,
    },
    ValueType {
        value: Value,
        expected: &'static str,
    },
    BadMap {
        operation: &'static str,
        value: Value,
    },
    InvalidTupleSize(Value),
    ElementBounds(Value),
    BadVariantSize(usize),
    BadVariantTag(usize),
    Unavailable(&'static str),
    NotAllowedInAuth(Opcode),
    NotAllowedOffchain(Opcode),
    UserExit(Vec<u8>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exception {
    UnknownInstruction,
    Native(&'static str),
    DetachedDuplicateIndex,
    // Malformed lower-level stores retain structured data failures. Mapping
    // every such failure to exact Erlang reason text belongs to the adapter.
    StoreData,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error<E> {
    Data(DataError),
    Backend(E),
    NeedContract([u8; 32]),
    ProtocolMismatch,
    Unsupported(Opcode),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    Next,
    Jump(Value),
}
#[derive(Debug, Clone)]
pub enum Outcome<S> {
    Continue {
        state: State<S>,
        control: Control,
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
enum Fault<E> {
    Abort(Reason),
    Revert(Vec<u8>),
    Exception(Exception),
    Operational(Error<E>),
}
impl<E> From<DataError> for Fault<E> {
    fn from(e: DataError) -> Self {
        Self::Operational(Error::Data(e))
    }
}
struct Step<'a, 'b, S, O> {
    state: State<S>,
    entry: State<S>,
    executor: &'a Executor<'b, S, O>,
    remaining: Limits,
}
impl<S: CachedSubtreeBackend, O: MapOrder> Executor<'_, S, O> {
    // An unimplemented opcode is an operational stop, never an invented VM
    // failure or successful no-op. The complete interpreter remains in scope.
    pub fn step(
        &self,
        state: &State<S>,
        instruction: &Instruction,
        limits: Limits,
    ) -> Result<Outcome<S>, Error<S::Error>> {
        let info = instruction.opcode.info();
        if instruction.operands.len() != info.arity
            || info
                .literal_operands
                .iter()
                .any(|&i| !matches!(instruction.operands[i], Operand::Immediate(_)))
        {
            return Ok(Outcome::Exception(Exception::UnknownInstruction));
        }
        if matches!(
            instruction.opcode,
            Opcode::DbgLoc | Opcode::DbgDef | Opcode::DbgUndef | Opcode::DbgContract
        ) {
            return Ok(Outcome::Exception(Exception::UnknownInstruction));
        }
        let mut remaining = limits;
        for value in std::iter::once(&state.accumulator.top).chain(state.accumulator.rest.iter()) {
            remaining = value::check_limits(value, remaining).map_err(Error::Data)?;
        }
        for (key, value) in &state.memory {
            let (Register::Argument(k) | Register::Variable(k)) = key;
            remaining = value::check_limits(k, remaining).map_err(Error::Data)?;
            remaining = value::check_limits(value, remaining).map_err(Error::Data)?;
        }
        for operand in &instruction.operands {
            if let Operand::Argument(v) | Operand::Variable(v) | Operand::Immediate(v) = operand {
                remaining = value::check_limits(v, remaining).map_err(Error::Data)?;
            }
        }
        let mut step = Step {
            state: state.clone(),
            entry: state.clone(),
            executor: self,
            remaining,
        };
        step.state
            .stores
            .vm_scope(state.meter.protocol, self.context.current_contract)
            .map_err(|_| Error::ProtocolMismatch)?;
        let result = (|| {
            if !info.offchain && !self.context.on_chain {
                return Err(Fault::Abort(Reason::NotAllowedOffchain(instruction.opcode)));
            }
            if !info.in_auth && self.context.in_auth {
                return Err(Fault::Abort(Reason::NotAllowedInAuth(instruction.opcode)));
            }
            if !ops::supported(instruction.opcode) {
                return Err(Fault::Operational(Error::Unsupported(instruction.opcode)));
            }
            step.spend(
                instruction
                    .opcode
                    .gas_at_protocol(state.meter.protocol)
                    .into(),
            )?;
            step.entry = step.state.clone();
            step.eval(instruction)
        })();
        match result {
            Ok(control) => Ok(Outcome::Continue {
                state: step.state,
                control,
                remaining_limits: step.remaining,
            }),
            Err(Fault::Abort(reason)) => {
                step.state.meter.gas = self.context.reserved_gas.clone();
                Ok(Outcome::Abort {
                    state: step.state,
                    reason,
                    remaining_limits: step.remaining,
                })
            }
            Err(Fault::Exception(e)) => Ok(Outcome::Exception(e)),
            Err(Fault::Revert(value)) => {
                step.state.meter.gas += &self.context.reserved_gas;
                Ok(Outcome::Revert {
                    state: step.state,
                    value,
                    remaining_limits: step.remaining,
                })
            }
            Err(Fault::Operational(e)) => Err(e),
        }
    }
}
impl<S: CachedSubtreeBackend, O: MapOrder> Step<'_, '_, S, O> {
    fn check(&mut self, v: &Value) -> Result<(), Fault<S::Error>> {
        self.remaining = value::check_limits(v, self.remaining)?;
        Ok(())
    }
    fn gas_fault(e: gas::Error) -> Fault<S::Error> {
        match e {
            gas::Error::OutOfGas => Fault::Abort(Reason::OutOfGas),
            gas::Error::Data(e) => e.into(),
            _ => unreachable!(),
        }
    }
    fn spend(&mut self, n: BigInt) -> Result<(), Fault<S::Error>> {
        self.state.meter.spend(n).map_err(Self::gas_fault)
    }
    fn cells(&mut self, n: BigInt) -> Result<(), Fault<S::Error>> {
        let delta = if n.sign() == Sign::Minus && self.state.meter.protocol >= 5 {
            -&n + 2
        } else {
            n.clone()
        };
        let result = self.state.meter.new_cells(n);
        if result.is_err() {
            self.state.meter.created_cells += delta;
        }
        result.map_err(Self::gas_fault)
    }
    fn stack_fault(e: stack::Error) -> Fault<S::Error> {
        match e {
            stack::Error::Data(e) => e.into(),
            stack::Error::EmptyStack => Fault::Abort(Reason::EmptyStack),
            stack::Error::OutOfGas => Fault::Abort(Reason::OutOfGas),
            stack::Error::InvalidDuplicateIndex => {
                Fault::Exception(Exception::DetachedDuplicateIndex)
            }
            stack::Error::ReferenceException(e) => Fault::Exception(Exception::Native(e)),
        }
    }
    fn store_fault(&self, e: StoreError<S::Error>) -> Fault<S::Error> {
        match e {
            StoreError::Data(DataError::ResourceLimit) => DataError::ResourceLimit.into(),
            StoreError::Data(DataError::InvalidOrderProfile) => {
                DataError::InvalidOrderProfile.into()
            }
            StoreError::InvalidOrder => {
                Fault::Operational(Error::Data(DataError::InvalidOrderProfile))
            }
            StoreError::Backend(e) => Fault::Operational(Error::Backend(e)),
            StoreError::ProtocolMismatch => Fault::Operational(Error::ProtocolMismatch),
            StoreError::MissingContract => {
                Fault::Operational(Error::NeedContract(self.executor.context.current_contract))
            }
            StoreError::OutOfGas => Fault::Abort(Reason::OutOfGas),
            _ => Fault::Exception(Exception::StoreData),
        }
    }
    fn ensure_store(&mut self) -> Result<(), Fault<S::Error>> {
        let key = self.executor.context.current_contract;
        if !self.state.stores.has_contract(&key) {
            let store = self
                .executor
                .contracts
                .get(&key)
                .ok_or(Fault::Operational(Error::NeedContract(key)))?;
            self.state.stores.put_contract(key, store.clone());
        }
        Ok(())
    }
    fn read(&mut self, operand: &Operand) -> Result<Value, Fault<S::Error>> {
        match operand {
            Operand::Stack => self
                .state
                .accumulator
                .pop(&mut self.state.meter)
                .map_err(Self::stack_fault),
            Operand::Immediate(v) => {
                self.check(v)?;
                Ok(v.clone())
            }
            Operand::Variable(Value::Integer(n)) if n.sign() == Sign::Minus => {
                self.ensure_store()?;
                let position = -n;
                let mut stores = self.state.stores.clone();
                let result = stores
                    .find_value(
                        &self.executor.context.current_contract,
                        &position,
                        Gas::Metered(self.state.meter.gas.clone()),
                        self.remaining,
                    )
                    .map_err(|e| self.store_fault(e))?;
                match result {
                    // Original abort/2 has no undefined_in_store clause.
                    RegisterRead::Missing => Err(self.native("function_clause")),
                    RegisterRead::Cached(v) => {
                        self.check(&v)?;
                        Ok(v)
                    }
                    RegisterRead::Loaded {
                        value,
                        bytes,
                        gas: Gas::Metered(gas),
                    } => {
                        self.state.meter.gas = gas;
                        if self.state.meter.protocol < 7 {
                            self.spend(
                                if self.state.meter.protocol >= 5 {
                                    2000
                                } else {
                                    0
                                }
                                .into(),
                            )?;
                            self.state.stores = stores;
                        } else {
                            self.state.stores = stores;
                            self.state
                                .meter
                                .store_read_floor(crate::store::read_gas_cost(bytes))
                                .map_err(Self::gas_fault)?;
                        }
                        self.check(&value)?;
                        Ok(value)
                    }
                    _ => unreachable!(),
                }
            }
            Operand::Argument(v) | Operand::Variable(v) => {
                let register = if matches!(operand, Operand::Argument(_)) {
                    Register::Argument(v.clone())
                } else {
                    Register::Variable(v.clone())
                };
                let v = self
                    .state
                    .memory
                    .get(&register)
                    .ok_or_else(|| Fault::Abort(Reason::Undefined(register.clone())))?;
                self.remaining = value::check_limits(v, self.remaining)?;
                Ok(v.clone())
            }
        }
    }
    fn write(&mut self, to: &Operand, value: Value) -> Result<(), Fault<S::Error>> {
        self.check(&value)?;
        match to {
            Operand::Stack => self
                .state
                .accumulator
                .push(value, &mut self.state.meter, self.remaining)
                .map_err(Self::stack_fault),
            Operand::Immediate(_) => Err(Fault::Exception(Exception::Native("function_clause"))),
            Operand::Variable(Value::Integer(n)) if n.sign() == Sign::Minus => {
                self.ensure_store()?;
                self.state
                    .stores
                    .put_value(&self.executor.context.current_contract, -n, value)
                    .map_err(|e| self.store_fault(e))
            }
            Operand::Argument(v) | Operand::Variable(v) => {
                let register = if matches!(to, Operand::Argument(_)) {
                    Register::Argument(v.clone())
                } else {
                    Register::Variable(v.clone())
                };
                self.state.memory.insert(register, value);
                Ok(())
            }
        }
    }
    fn type_error(&self, operation: &'static str, arguments: Vec<Value>) -> Fault<S::Error> {
        Fault::Abort(Reason::Type {
            operation,
            arguments,
        })
    }
    fn native(&self, reason: &'static str) -> Fault<S::Error> {
        Fault::Exception(Exception::Native(reason))
    }
    fn entry_abort(&mut self, reason: Reason) -> Fault<S::Error> {
        self.state = self.entry.clone();
        Fault::Abort(reason)
    }
}
