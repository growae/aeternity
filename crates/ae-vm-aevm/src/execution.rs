// Historical aevm_eeevm eval_code, fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{
    gas,
    maps::{self, Maps, Store},
    memory::Memory,
    opcode::{self, GasTable, Version},
    runtime_heap,
    word::{self, Word},
};
use num_bigint::{BigInt, BigUint};
use num_traits::{ToPrimitive, Zero};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Context {
    pub address: BigInt,
    pub creator: BigInt,
    pub origin: BigInt,
    pub caller: BigInt,
    pub value: BigInt,
    pub gas_price: BigInt,
    pub coinbase: BigInt,
    pub timestamp: BigInt,
    pub number: BigInt,
    pub difficulty: BigInt,
    pub gas_limit: BigInt,
}
impl Default for Context {
    fn default() -> Self {
        Self {
            address: 1.into(),
            creator: 0.into(),
            origin: 0.into(),
            caller: 0.into(),
            value: 0.into(),
            gas_price: 0.into(),
            coinbase: 0.into(),
            timestamp: 0.into(),
            number: 0.into(),
            difficulty: 0.into(),
            gas_limit: 0.into(),
        }
    }
}
impl Context {
    fn values(&self) -> [&BigInt; 11] {
        [
            &self.address,
            &self.creator,
            &self.origin,
            &self.caller,
            &self.value,
            &self.gas_price,
            &self.coinbase,
            &self.timestamp,
            &self.number,
            &self.difficulty,
            &self.gas_limit,
        ]
    }
    fn get(&self, op: u8) -> Option<&BigInt> {
        Some(match op {
            0x2f => &self.creator,
            0x30 => &self.address,
            0x32 => &self.origin,
            0x33 => &self.caller,
            0x34 => &self.value,
            0x3a => &self.gas_price,
            0x41 => &self.coinbase,
            0x42 => &self.timestamp,
            0x43 => &self.number,
            0x44 => &self.difficulty,
            0x45 => &self.gas_limit,
            _ => return None,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Log {
    pub address: Word,
    pub topics: Vec<Word>,
    pub data: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallCreate {
    pub data: Vec<u8>,
    pub destination: BigInt,
    pub gas_limit: BigUint,
    pub value: BigInt,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct State {
    pub version: Version,
    pub protocol: u32,
    pub call_stack: Vec<BigInt>,
    pub no_recursion: bool,
    pub call_creates: Vec<CallCreate>,
    pub auth_tx_hash: Option<Word>,
    pub solidity_storage: BTreeMap<BigUint, BigUint>,
    pub code: Vec<u8>,
    pub cp: usize,
    pub gas: BigUint,
    // Native order: the next value popped is at index zero.
    pub stack: Vec<Word>,
    pub memory: Memory,
    pub gas_table: GasTable,
    pub float_profile: gas::FloatProfile,
    pub data: Vec<u8>,
    pub return_data: Vec<u8>,
    pub jump_destinations: BTreeSet<usize>,
    pub context: Context,
    pub out_type: Option<runtime_heap::TypeRef>,
    pub output: Vec<u8>,
    pub logs: Vec<Log>,
    pub maps: Maps,
    pub map_store: Store,
}
impl State {
    pub fn new(
        version: Version,
        code: Vec<u8>,
        gas: BigUint,
        float_profile: gas::FloatProfile,
    ) -> Self {
        Self {
            version,
            protocol: 4,
            call_stack: Vec::new(),
            no_recursion: false,
            call_creates: Vec::new(),
            auth_tx_hash: None,
            solidity_storage: BTreeMap::new(),
            code,
            cp: 0,
            gas,
            stack: Vec::new(),
            memory: Memory::default(),
            gas_table: GasTable::default(),
            float_profile,
            data: Vec::new(),
            return_data: Vec::new(),
            jump_destinations: BTreeSet::new(),
            context: Context::default(),
            out_type: None,
            output: Vec::new(),
            logs: Vec::new(),
            maps: Maps::default(),
            map_store: Store::new(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Arithmetic,
    OutOfGas,
    OutOfStack,
    PopEmpty,
    DupEmpty,
    DupTooSmall,
    SwapEmpty,
    SwapTooSmall,
    IllegalInstruction(u8),
    InvalidJumpDestination(Word),
    Call(String),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Complete(Box<State>),
    Error { reason: Failure, gas: BigUint },
    Native(gas::Error),
    Revert { output: Vec<u8>, gas: BigUint },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    ResourceLimit,
    InvalidInitialState,
    UnimplementedInstruction(u8),
    UnimplementedPrimitiveEntry,
    UnimplementedPrimop(Word),
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_work: usize,
    pub max_bytes: usize,
    pub max_memory_words: usize,
    pub max_depth: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_work: 1_000_000,
            max_bytes: 64 * 1024 * 1024,
            max_memory_words: 1_000_000,
            max_depth: 256,
        }
    }
}
pub struct Budget {
    work: usize,
    bytes: usize,
    words: usize,
    pub(crate) depth: usize,
    active_calls: usize,
}
impl Budget {
    pub fn new(limits: Limits) -> Self {
        Self {
            work: limits.max_work,
            bytes: limits.max_bytes,
            words: limits.max_memory_words,
            depth: limits.max_depth,
            active_calls: 0,
        }
    }
    pub fn work(&mut self, n: usize) -> Result<(), Stop> {
        self.work = self.work.checked_sub(n).ok_or(Stop::ResourceLimit)?;
        Ok(())
    }
    pub(crate) fn enter_call(&mut self) -> Result<(), Stop> {
        if self.active_calls >= self.depth {
            return Err(Stop::ResourceLimit);
        }
        self.active_calls += 1;
        Ok(())
    }
    pub(crate) fn leave_call(&mut self) {
        self.active_calls -= 1;
    }
    pub(crate) fn memory(
        &mut self,
        memory: &Memory,
        address: &BigUint,
        size: usize,
    ) -> Result<(), Stop> {
        if size == 0 {
            return Ok(());
        }
        let offset = (address % 32u8).to_usize().expect("remainder below 32");
        let count = size
            .checked_add(offset)
            .ok_or(Stop::ResourceLimit)?
            .div_ceil(32);
        let low = address - offset;
        let high = &low + BigUint::from(count - 1) * 32u8;
        let reused = memory.words().range(low..=high.clone()).count();
        let total = memory
            .words()
            .len()
            .checked_add(count - reused)
            .ok_or(Stop::ResourceLimit)?;
        if total > self.words {
            return Err(Stop::ResourceLimit);
        }
        let bytes = usize::try_from(high.bits().div_ceil(8))
            .map_err(|_| Stop::ResourceLimit)?
            .checked_add(32)
            .ok_or(Stop::ResourceLimit)?;
        self.bytes(count.checked_mul(bytes).ok_or(Stop::ResourceLimit)?)
    }
    pub fn bytes(&mut self, n: usize) -> Result<(), Stop> {
        self.bytes = self.bytes.checked_sub(n).ok_or(Stop::ResourceLimit)?;
        Ok(())
    }
}
pub(crate) enum Fault {
    Stop(Stop),
    Vm(Failure),
    Native(gas::Error),
}
impl From<Stop> for Fault {
    fn from(e: Stop) -> Self {
        Self::Stop(e)
    }
}
impl From<Failure> for Fault {
    fn from(e: Failure) -> Self {
        Self::Vm(e)
    }
}
impl From<gas::Error> for Fault {
    fn from(e: gas::Error) -> Self {
        Self::Native(e)
    }
}
impl From<word::ArithmeticError> for Fault {
    fn from(_: word::ArithmeticError) -> Self {
        Self::Vm(Failure::Arithmetic)
    }
}
fn spend(state: &mut State, gas: BigUint) -> Result<(), Fault> {
    if state.gas < gas {
        return Err(Failure::OutOfGas.into());
    }
    state.gas -= gas;
    Ok(())
}
fn pop(s: &mut State) -> Result<Word, Fault> {
    if s.stack.is_empty() {
        Err(Failure::PopEmpty.into())
    } else {
        Ok(s.stack.remove(0))
    }
}
fn push(s: &mut State, budget: &mut Budget, word: Word) -> Result<(), Fault> {
    if s.stack.len() >= 1024 {
        return Err(Failure::OutOfStack.into());
    }
    budget.bytes(32)?;
    s.stack.insert(0, word);
    Ok(())
}
fn peek(s: &State, n: usize) -> Result<Word, Fault> {
    // aevm_gas catches native peek badarg and returns the state map; comparing
    // integer gas to that map is false, so spend_op_gas reports out_of_gas.
    s.stack.get(n).copied().ok_or(Failure::OutOfGas.into())
}
pub(crate) fn call_gas(s: &State) -> Result<(BigUint, BigUint), Fault> {
    let requested = peek(s, 0)?.unsigned();
    let extra = BigUint::from(s.gas_table.call)
        + if peek(s, 2)?.is_zero() {
            BigUint::zero()
        } else {
            s.gas_table.call_value.into()
        };
    let cap = if s.gas >= extra {
        gas::all_but_one_64th(&(&s.gas - &extra), s.float_profile)?.min(requested)
    } else {
        requested
    };
    Ok((cap.clone(), cap + extra))
}
fn dynamic(op: u8, s: &State) -> Result<BigUint, Fault> {
    Ok(match op {
        10 => BigUint::from(peek(s, 1)?.unsigned().bits().div_ceil(8)) * s.gas_table.exp_byte,
        0x37 | 0x39 => {
            gas::copy_words(&peek(s, 2)?.unsigned(), s.float_profile)? * s.gas_table.copy
        }
        0x20 => gas::copy_words(&peek(s, 1)?.unsigned(), s.float_profile)? * s.gas_table.sha3_word,
        0xa0..=0xa4 => peek(s, 1)?.unsigned() * s.gas_table.log_data,
        0xf4 => call_gas(s)?.1,
        _ => BigUint::zero(),
    })
}
fn area(s: &mut State, budget: &mut Budget, from: Word, size: Word) -> Result<Vec<u8>, Fault> {
    let n = size.unsigned().to_usize().ok_or(Stop::ResourceLimit)?;
    budget.bytes(n)?;
    budget.work(n)?;
    Ok(s.memory.area(&from.unsigned(), n, n)?)
}
fn sophia_return(
    s: &State,
    budget: &mut Budget,
    ty: Option<&runtime_heap::TypeRef>,
    ptr: Word,
) -> Result<(Vec<u8>, BigUint), Fault> {
    let maximum = gas::memory_limit(&s.gas, &s.gas_table, s.float_profile)? * 32u8;
    let ty = ty.ok_or(Failure::OutOfGas)?;
    let profile = if s.version == Version::Sophia4 {
        runtime_heap::MapSerialization::Corrected
    } else {
        runtime_heap::MapSerialization::Legacy
    };
    let output = runtime_heap::serialize(
        ty,
        ptr,
        &s.memory,
        runtime_heap::MapContext {
            maps: &s.maps,
            store: &s.map_store,
            profile,
        },
        &maximum,
        budget,
    )
    .map_err(heap_fault)?;
    let gas = gas::memory(&(output.len() / 32).into(), &s.gas_table, s.float_profile)?;
    Ok((output, gas))
}
fn heap_fault(error: runtime_heap::Error) -> Fault {
    match error {
        runtime_heap::Error::ResourceLimit => Fault::Stop(Stop::ResourceLimit),
        runtime_heap::Error::Stop(s) => Fault::Stop(s),
        _ => Fault::Vm(Failure::OutOfGas),
    }
}
fn implemented(op: u8) -> bool {
    matches!(op,0..=11|0x10..=0x1d|0x20|0x2f|0x30|0x32..=0x3a|0x3d|0x3e|0x41..=0x45|0x50..=0x53|0x56..=0x5b|0x60..=0xa4|0xf3|0xfd)
}
fn reserve_fault(e: runtime_heap::Error) -> Stop {
    match e {
        runtime_heap::Error::Stop(s) => s,
        _ => Stop::ResourceLimit,
    }
}
pub(crate) fn snapshot(
    state: &State,
    budget: &mut Budget,
    copy_jumps: bool,
) -> Result<State, Stop> {
    budget.work(state.code.len())?;
    budget.work(state.stack.len())?;
    budget.work(state.memory.words().len())?;
    budget.bytes(state.code.len())?;
    budget.bytes(state.data.len())?;
    budget.bytes(state.return_data.len())?;
    budget.bytes(state.output.len())?;
    for n in state.context.values() {
        budget.bytes(usize::try_from(n.bits().div_ceil(8)).map_err(|_| Stop::ResourceLimit)?)?;
    }
    budget.work(state.logs.len())?;
    for log in &state.logs {
        budget.bytes(32)?;
        budget.bytes(
            log.topics
                .len()
                .checked_mul(32)
                .ok_or(Stop::ResourceLimit)?,
        )?;
        budget.bytes(log.data.len())?;
    }
    if let Some(ty) = &state.out_type {
        runtime_heap::check_type(ty, budget)?;
    }
    maps::reserve_maps(&state.maps, budget).map_err(reserve_fault)?;
    maps::reserve_store(&state.map_store, budget).map_err(reserve_fault)?;
    budget.bytes(
        state
            .stack
            .len()
            .checked_mul(32)
            .ok_or(Stop::ResourceLimit)?,
    )?;
    budget.bytes(state.memory.allocated_bytes().ok_or(Stop::ResourceLimit)?)?;
    budget
        .bytes(usize::try_from(state.gas.bits().div_ceil(8)).map_err(|_| Stop::ResourceLimit)?)?;
    if state.memory.words().len() > budget.words {
        return Err(Stop::ResourceLimit);
    }
    budget.work(state.call_creates.len())?;
    for call in &state.call_creates {
        budget.bytes(call.data.len())?;
        for bits in [
            call.destination.bits(),
            call.value.bits(),
            call.gas_limit.bits(),
        ] {
            budget.bytes(usize::try_from(bits.div_ceil(8)).map_err(|_| Stop::ResourceLimit)?)?;
        }
    }
    // Evaluation rebuilds jump destinations; runtime finalization preserves them.
    budget.work(state.call_stack.len())?;
    for n in &state.call_stack {
        budget.bytes(usize::try_from(n.bits().div_ceil(8)).map_err(|_| Stop::ResourceLimit)?)?;
    }
    budget.work(state.solidity_storage.len())?;
    for (k, v) in &state.solidity_storage {
        budget.bytes(
            usize::try_from((k.bits().div_ceil(8)).saturating_add(v.bits().div_ceil(8)))
                .map_err(|_| Stop::ResourceLimit)?,
        )?;
    }
    if copy_jumps {
        budget.work(state.jump_destinations.len())?;
        budget.bytes(
            state
                .jump_destinations
                .len()
                .checked_mul(std::mem::size_of::<usize>())
                .ok_or(Stop::ResourceLimit)?,
        )?;
    }
    Ok(State {
        jump_destinations: if copy_jumps {
            state.jump_destinations.clone()
        } else {
            BTreeSet::new()
        },
        version: state.version,
        protocol: state.protocol,
        call_stack: state.call_stack.clone(),
        no_recursion: state.no_recursion,
        call_creates: state.call_creates.clone(),
        auth_tx_hash: state.auth_tx_hash,
        solidity_storage: state.solidity_storage.clone(),
        code: state.code.clone(),
        cp: state.cp,
        gas: state.gas.clone(),
        stack: state.stack.clone(),
        memory: state.memory.clone(),
        gas_table: state.gas_table.clone(),
        float_profile: state.float_profile,
        data: state.data.clone(),
        return_data: state.return_data.clone(),
        context: state.context.clone(),
        out_type: state.out_type.clone(),
        output: state.output.clone(),
        logs: state.logs.clone(),
        maps: state.maps.clone(),
        map_store: state.map_store.clone(),
    })
}
fn run<E>(
    state: &State,
    budget: &mut Budget,
    hosted: bool,
    call: &mut impl FnMut(u8, &State, &mut Budget) -> Result<CallStep, E>,
) -> Result<Outcome, RunFault<E>> {
    if state.context.address.is_zero() {
        return Err(Stop::UnimplementedPrimitiveEntry.into());
    }
    if state.stack.len() > 1024 {
        return Err(Stop::InvalidInitialState.into());
    }
    let mut s = snapshot(state, budget, false)?;
    let mut scan = 0usize;
    while scan < s.code.len() {
        let op = s.code[scan];
        if op == 0x5b {
            budget.bytes(std::mem::size_of::<usize>())?;
            s.jump_destinations.insert(scan);
        }
        scan = scan
            .checked_add(if (0x60..=0x7f).contains(&op) {
                usize::from(op - 0x5e)
            } else {
                1
            })
            .ok_or(Stop::ResourceLimit)?;
    }
    let mut pc = s.cp;
    loop {
        if pc >= s.code.len() {
            s.cp = pc;
            return Ok(Outcome::Complete(Box::new(s)));
        }
        budget.work(1)?;
        let op = s.code[pc];
        if !s.version.permits(op) {
            return Err(Failure::IllegalInstruction(op).into());
        }
        if !implemented(op) && !(hosted && matches!(op, 0xf1 | 0xf2 | 0xf4)) {
            return Err(Stop::UnimplementedInstruction(op).into());
        }
        let cost = if matches!(op, 0xf1 | 0xf2) {
            BigUint::zero()
        } else {
            opcode::metadata(op, &s.gas_table)
                .ok_or(Stop::UnimplementedInstruction(op))?
                .2
                + dynamic(op, &s)?
        };
        spend(&mut s, cost)?;
        let old_size = s.memory.size_in_words().clone();
        let mut next = pc.checked_add(1).ok_or(Stop::ResourceLimit)?;
        match op {
            0xf1 | 0xf2 | 0xf4 => match call(op, &s, budget).map_err(RunFault::Call)? {
                CallStep::Continue { value, state } => {
                    s = *state;
                    push(&mut s, budget, value)?;
                }
                CallStep::Halt(outcome) => return Ok(outcome),
            },
            0 => {
                s.cp = pc;
                return Ok(Outcome::Complete(Box::new(s)));
            }
            8 | 9 => {
                let a = pop(&mut s)?;
                let b = pop(&mut s)?;
                let c = pop(&mut s)?;
                let v = word::modular(op, a, b, c, s.version)?;
                push(&mut s, budget, v)?;
            }
            1..=7 | 10 | 11 | 0x10..=0x14 | 0x16..=0x18 | 0x1a..=0x1d => {
                let a = pop(&mut s)?;
                let b = pop(&mut s)?;
                if op == 10 {
                    budget.work(
                        usize::try_from(b.unsigned().bits()).map_err(|_| Stop::ResourceLimit)?,
                    )?;
                }
                let v = word::binary(op, a, b, s.version)?;
                push(&mut s, budget, v)?;
            }
            0x15 => {
                let a = pop(&mut s)?;
                push(&mut s, budget, Word::from(u64::from(a.is_zero())))?;
            }
            0x19 => {
                let a = pop(&mut s)?;
                push(&mut s, budget, Word(a.0.map(|b| !b)))?;
            }
            0x20 => {
                let from = pop(&mut s)?;
                let size = pop(&mut s)?;
                let bytes = area(&mut s, budget, from, size)?;
                push(&mut s, budget, Word(ae_crypto::keccak256(&bytes).0))?;
            }
            0x2f | 0x30 | 0x32..=0x34 | 0x3a | 0x41..=0x45 => {
                let value = Word::wrapping(
                    s.context
                        .get(op)
                        .ok_or(Stop::UnimplementedInstruction(op))?,
                );
                push(&mut s, budget, value)?;
            }
            0xa0..=0xa4 => {
                let from = pop(&mut s)?;
                let size = pop(&mut s)?;
                let mut topics = Vec::new();
                for _ in 0..op - 0xa0 {
                    topics.push(pop(&mut s)?);
                }
                budget.bytes(32 + topics.len() * 32)?;
                let data = area(&mut s, budget, from, size)?;
                s.logs.push(Log {
                    address: Word::wrapping(&s.context.address),
                    topics,
                    data,
                });
            }
            0xf3 | 0xfd => {
                let a = pop(&mut s)?;
                let b = pop(&mut s)?;
                if s.version == Version::Solidity1 {
                    s.output = area(&mut s, budget, a, b)?;
                } else {
                    let revert_type = std::sync::Arc::new(runtime_heap::Type::String);
                    let (bytes, gas) = sophia_return(
                        &s,
                        budget,
                        if op == 0xfd {
                            Some(&revert_type)
                        } else {
                            s.out_type.as_ref()
                        },
                        b,
                    )?;
                    spend(&mut s, gas)?;
                    s.output = bytes;
                }
                if op == 0xf3 {
                    return Ok(Outcome::Complete(Box::new(s)));
                }
                let expansion =
                    gas::memory(s.memory.size_in_words(), &s.gas_table, s.float_profile)?
                        - gas::memory(&old_size, &s.gas_table, s.float_profile)?;
                spend(&mut s, expansion)?;
                return Ok(Outcome::Revert {
                    output: s.output,
                    gas: s.gas,
                });
            }
            0x35 => {
                let from = pop(&mut s)?.unsigned();
                let mut word = [0; 32];
                if let Some(from) = from.to_usize().filter(|p| *p < s.data.len()) {
                    let n = 32.min(s.data.len() - from);
                    word[..n].copy_from_slice(&s.data[from..from + n]);
                }
                push(&mut s, budget, Word(word))?;
            }
            0x36 => {
                let n = Word::from_unsigned(&s.data.len().into());
                push(&mut s, budget, n)?;
            }
            0x3d => {
                let n = Word::from_unsigned(&s.return_data.len().into());
                push(&mut s, budget, n)?;
            }
            0x38 => {
                let n = Word::from_unsigned(&s.code.len().into());
                push(&mut s, budget, n)?;
            }
            0x37 | 0x39 | 0x3e => {
                let to = pop(&mut s)?.unsigned();
                let from = pop(&mut s)?.unsigned();
                let size = pop(&mut s)?.unsigned();
                let size = size.to_usize().ok_or(Stop::ResourceLimit)?;
                budget.bytes(size)?;
                budget.work(size)?;
                let source = match op {
                    0x37 => &s.data,
                    0x39 => &s.code,
                    _ => &s.return_data,
                };
                budget.memory(&s.memory, &to, size)?;
                let mut bytes = vec![0; size];
                if let Some(from) = from.to_usize().filter(|p| *p < source.len()) {
                    let n = size.min(source.len() - from);
                    bytes[..n].copy_from_slice(&source[from..from + n]);
                }
                s.memory.write_area(&to, &bytes);
            }
            0x50 => {
                pop(&mut s)?;
            }
            0x51 => {
                let a = pop(&mut s)?.unsigned();
                let v = s.memory.load(&a);
                push(&mut s, budget, v)?;
            }
            0x52 => {
                let a = pop(&mut s)?.unsigned();
                let v = pop(&mut s)?;
                budget.memory(&s.memory, &a, 32)?;
                s.memory.store(&a, v);
            }
            0x53 => {
                let a = pop(&mut s)?.unsigned();
                let v = pop(&mut s)?;
                budget.memory(&s.memory, &a, 1)?;
                s.memory.store8(&a, v.0[31]);
            }
            0x56 | 0x57 => {
                let dest = pop(&mut s)?;
                let take = op == 0x56 || !pop(&mut s)?.is_zero();
                if take {
                    next = dest
                        .unsigned()
                        .to_usize()
                        .filter(|d| s.jump_destinations.contains(d))
                        .ok_or(Failure::InvalidJumpDestination(dest))?;
                }
            }
            0x58 => {
                let p = Word::from_unsigned(&pc.into());
                push(&mut s, budget, p)?;
            }
            0x59 => {
                let n = Word::from_unsigned(&(s.memory.size_in_words() * 32u8));
                push(&mut s, budget, n)?;
            }
            0x5a => {
                let n = Word::from_unsigned(&s.gas);
                push(&mut s, budget, n)?;
            }
            0x5b => {}
            0x60..=0x7f => {
                let n = usize::from(op - 0x5f);
                let mut value = [0; 32];
                let available = n.min(s.code.len() - next);
                value[32 - n..32 - n + available].copy_from_slice(&s.code[next..next + available]);
                push(&mut s, budget, Word(value))?;
                next = next.checked_add(n).ok_or(Stop::ResourceLimit)?;
            }
            0x80..=0x8f => {
                if s.stack.is_empty() {
                    return Err(Failure::DupEmpty.into());
                }
                let n = usize::from(op - 0x80);
                let v = s.stack.get(n).copied().ok_or(Failure::DupTooSmall)?;
                push(&mut s, budget, v)?;
            }
            0x90..=0x9f => {
                if s.stack.is_empty() {
                    return Err(Failure::SwapEmpty.into());
                }
                let n = usize::from(op - 0x8f);
                if s.stack.len() <= n {
                    return Err(Failure::SwapTooSmall.into());
                }
                s.stack.swap(0, n);
            }
            _ => return Err(Stop::UnimplementedInstruction(op).into()),
        }
        if s.memory.words().len() > budget.words {
            return Err(Stop::ResourceLimit.into());
        }
        if *s.memory.size_in_words() != old_size {
            let expansion = gas::memory(s.memory.size_in_words(), &s.gas_table, s.float_profile)?
                - gas::memory(&old_size, &s.gas_table, s.float_profile)?;
            spend(&mut s, expansion)?;
        }
        pc = next;
    }
}
pub(crate) enum CallStep {
    Continue { value: Word, state: Box<State> },
    Halt(Outcome),
}
enum RunFault<E> {
    Core(Fault),
    Call(E),
}
impl<E> From<Fault> for RunFault<E> {
    fn from(e: Fault) -> Self {
        Self::Core(e)
    }
}
impl<E> From<Stop> for RunFault<E> {
    fn from(e: Stop) -> Self {
        Self::Core(e.into())
    }
}
impl<E> From<Failure> for RunFault<E> {
    fn from(e: Failure) -> Self {
        Self::Core(e.into())
    }
}
impl<E> From<gas::Error> for RunFault<E> {
    fn from(e: gas::Error) -> Self {
        Self::Core(e.into())
    }
}
impl<E> From<word::ArithmeticError> for RunFault<E> {
    fn from(e: word::ArithmeticError) -> Self {
        Self::Core(e.into())
    }
}
pub(crate) enum RunError<E> {
    Stop(Stop),
    Call(E),
}
pub(crate) fn evaluate_calls<E>(
    state: &State,
    budget: &mut Budget,
    hosted: bool,
    mut call: impl FnMut(u8, &State, &mut Budget) -> Result<CallStep, E>,
) -> Result<Outcome, RunError<E>> {
    match run(state, budget, hosted, &mut call) {
        Ok(outcome) => Ok(outcome),
        Err(RunFault::Core(Fault::Vm(reason))) => Ok(Outcome::Error {
            reason,
            gas: BigUint::zero(),
        }),
        Err(RunFault::Core(Fault::Native(e))) => Ok(Outcome::Native(e)),
        Err(RunFault::Core(Fault::Stop(e))) => Err(RunError::Stop(e)),
        Err(RunFault::Call(e)) => Err(RunError::Call(e)),
    }
}
// Every operational stop leaves the supplied snapshot untouched. Native errors
// are terminal VM outcomes; unfinished host instructions are operational stops.
pub fn evaluate(state: &State, limits: Limits) -> Result<Outcome, Stop> {
    evaluate_inner(state, &mut Budget::new(limits))
}
pub(crate) fn evaluate_inner(state: &State, budget: &mut Budget) -> Result<Outcome, Stop> {
    evaluate_calls(state, budget, false, |op, _, _| {
        Err(Stop::UnimplementedInstruction(op))
    })
    .map_err(|e| match e {
        RunError::Stop(e) | RunError::Call(e) => e,
    })
}
