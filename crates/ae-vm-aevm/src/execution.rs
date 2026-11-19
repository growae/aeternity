// Historical aevm_eeevm eval_code, fa2a6d7.
// Copyright (c) 2017, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{
    gas,
    memory::Memory,
    opcode::{self, GasTable, Version},
    word::{self, Word},
};
use num_bigint::BigUint;
use num_traits::{ToPrimitive, Zero};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct State {
    pub version: Version,
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
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Complete(Box<State>),
    Error { reason: Failure, gas: BigUint },
    Native(gas::Error),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    ResourceLimit,
    InvalidInitialState,
    UnimplementedInstruction(u8),
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_work: usize,
    pub max_bytes: usize,
    pub max_memory_words: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_work: 1_000_000,
            max_bytes: 64 * 1024 * 1024,
            max_memory_words: 1_000_000,
        }
    }
}
struct Budget {
    work: usize,
    bytes: usize,
    words: usize,
}
impl Budget {
    fn work(&mut self, n: usize) -> Result<(), Fault> {
        self.work = self.work.checked_sub(n).ok_or(Stop::ResourceLimit)?;
        Ok(())
    }
    fn memory(&mut self, memory: &Memory, address: &BigUint, size: usize) -> Result<(), Fault> {
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
            return Err(Stop::ResourceLimit.into());
        }
        let bytes = usize::try_from(high.bits().div_ceil(8))
            .map_err(|_| Stop::ResourceLimit)?
            .checked_add(32)
            .ok_or(Stop::ResourceLimit)?;
        self.bytes(count.checked_mul(bytes).ok_or(Stop::ResourceLimit)?)
    }
    fn bytes(&mut self, n: usize) -> Result<(), Fault> {
        self.bytes = self.bytes.checked_sub(n).ok_or(Stop::ResourceLimit)?;
        Ok(())
    }
}
enum Fault {
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
fn dynamic(op: u8, s: &State) -> Result<BigUint, Fault> {
    Ok(match op {
        10 => BigUint::from(peek(s, 1)?.unsigned().bits().div_ceil(8)) * s.gas_table.exp_byte,
        0x37 | 0x39 => {
            gas::copy_words(&peek(s, 2)?.unsigned(), s.float_profile)? * s.gas_table.copy
        }
        _ => BigUint::zero(),
    })
}
fn implemented(op: u8) -> bool {
    matches!(op,0..=11|0x10..=0x1d|0x35..=0x39|0x3d|0x3e|0x50..=0x53|0x56..=0x5b|0x60..=0x9f)
}
fn run(state: &State, limits: Limits) -> Result<State, Fault> {
    let mut budget = Budget {
        work: limits.max_work,
        bytes: limits.max_bytes,
        words: limits.max_memory_words,
    };
    budget.work(state.code.len())?;
    budget.work(state.stack.len())?;
    budget.work(state.memory.words().len())?;
    budget.bytes(state.code.len())?;
    budget.bytes(state.data.len())?;
    budget.bytes(state.return_data.len())?;
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
    if state.stack.len() > 1024 {
        return Err(Stop::InvalidInitialState.into());
    }
    if state.memory.words().len() > budget.words {
        return Err(Stop::ResourceLimit.into());
    }
    // Do not clone an untrusted previous jump table; derive it from code again.
    let mut s = State {
        jump_destinations: BTreeSet::new(),
        version: state.version,
        code: state.code.clone(),
        cp: state.cp,
        gas: state.gas.clone(),
        stack: state.stack.clone(),
        memory: state.memory.clone(),
        gas_table: state.gas_table.clone(),
        float_profile: state.float_profile,
        data: state.data.clone(),
        return_data: state.return_data.clone(),
    };
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
    loop {
        if s.cp >= s.code.len() {
            return Ok(s);
        }
        budget.work(1)?;
        let op = s.code[s.cp];
        if !s.version.permits(op) {
            return Err(Failure::IllegalInstruction(op).into());
        }
        if !implemented(op) {
            return Err(Stop::UnimplementedInstruction(op).into());
        }
        let cost = opcode::metadata(op, &s.gas_table)
            .ok_or(Stop::UnimplementedInstruction(op))?
            .2
            + dynamic(op, &s)?;
        spend(&mut s, cost)?;
        let old_size = s.memory.size_in_words().clone();
        let mut next = s.cp.checked_add(1).ok_or(Stop::ResourceLimit)?;
        match op {
            0 => return Ok(s),
            8 | 9 => {
                let a = pop(&mut s)?;
                let b = pop(&mut s)?;
                let c = pop(&mut s)?;
                let v = word::modular(op, a, b, c, s.version)?;
                push(&mut s, &mut budget, v)?;
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
                push(&mut s, &mut budget, v)?;
            }
            0x15 => {
                let a = pop(&mut s)?;
                push(&mut s, &mut budget, Word::from(u64::from(a.is_zero())))?;
            }
            0x19 => {
                let a = pop(&mut s)?;
                push(&mut s, &mut budget, Word(a.0.map(|b| !b)))?;
            }
            0x35 => {
                let from = pop(&mut s)?.unsigned();
                let mut word = [0; 32];
                if let Some(from) = from.to_usize().filter(|p| *p < s.data.len()) {
                    let n = 32.min(s.data.len() - from);
                    word[..n].copy_from_slice(&s.data[from..from + n]);
                }
                push(&mut s, &mut budget, Word(word))?;
            }
            0x36 => {
                let n = Word::from_unsigned(&s.data.len().into());
                push(&mut s, &mut budget, n)?;
            }
            0x3d => {
                let n = Word::from_unsigned(&s.return_data.len().into());
                push(&mut s, &mut budget, n)?;
            }
            0x38 => {
                let n = Word::from_unsigned(&s.code.len().into());
                push(&mut s, &mut budget, n)?;
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
                push(&mut s, &mut budget, v)?;
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
                let p = Word::from_unsigned(&s.cp.into());
                push(&mut s, &mut budget, p)?;
            }
            0x59 => {
                let n = Word::from_unsigned(&(s.memory.size_in_words() * 32u8));
                push(&mut s, &mut budget, n)?;
            }
            0x5a => {
                let n = Word::from_unsigned(&s.gas);
                push(&mut s, &mut budget, n)?;
            }
            0x5b => {}
            0x60..=0x7f => {
                let n = usize::from(op - 0x5f);
                let mut value = [0; 32];
                let available = n.min(s.code.len() - next);
                value[32 - n..32 - n + available].copy_from_slice(&s.code[next..next + available]);
                push(&mut s, &mut budget, Word(value))?;
                next = next.checked_add(n).ok_or(Stop::ResourceLimit)?;
            }
            0x80..=0x8f => {
                if s.stack.is_empty() {
                    return Err(Failure::DupEmpty.into());
                }
                let n = usize::from(op - 0x80);
                let v = s.stack.get(n).copied().ok_or(Failure::DupTooSmall)?;
                push(&mut s, &mut budget, v)?;
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
        s.cp = next;
    }
}
// Every operational stop leaves the supplied snapshot untouched. Native errors
// are terminal VM outcomes; unfinished host instructions are operational stops.
pub fn evaluate(state: &State, limits: Limits) -> Result<Outcome, Stop> {
    match run(state, limits) {
        Ok(s) => Ok(Outcome::Complete(Box::new(s))),
        Err(Fault::Vm(reason)) => Ok(Outcome::Error {
            reason,
            gas: BigUint::zero(),
        }),
        Err(Fault::Native(e)) => Ok(Outcome::Native(e)),
        Err(Fault::Stop(e)) => Err(e),
    }
}
