// aevm_eeevm_state init_vm/save_store and aevm_eeevm eval, fa2a6d7.
// Copyright (c) 2017-2018, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{
    execution::{self, Budget, Failure, Limits, State, Stop},
    gas,
    maps::{self, Data, Store},
    opcode::Version,
    runtime_heap::{self, Error as HeapError, Heap, TypeRef},
    store::{self, OrderProfile},
    word::Word,
};
use num_bigint::BigUint;
use num_traits::Zero;
use std::{collections::BTreeMap, convert::Infallible};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Native {
    BadMatchNone,
    BadMatchCodeReference(ae_types::Id),
    BadMatchChainError(String),
    CaseClauseNone,
    // case_clause {error, not_found} from aec_vm_chain:blockhash/2.
    BlockHashNoCommonAncestor,
    ChainException {
        class: &'static str,
        reason: &'static str,
    },
    IllegalStore(Store),
    MissingCallDataType,
    ReentrantCall,
    MapNotFound,
    BadStackPeek {
        index: usize,
        stack: Vec<Word>,
    },
    BadCallData,
    Heap(HeapError),
    Gas(gas::Error),
    OutOfGas,
    BadMatchBinary(Vec<u8>),
    BadPrimopData {
        reason: crate::heap::Error,
        data: Vec<u8>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault<E = Infallible> {
    Stop(Stop),
    Native(Native),
    Host(E),
}
impl Fault {
    pub(crate) fn widen<E>(self) -> Fault<E> {
        match self {
            Self::Stop(e) => Fault::Stop(e),
            Self::Native(e) => Fault::Native(e),
            Self::Host(e) => match e {},
        }
    }
}
impl<E> From<Stop> for Fault<E> {
    fn from(e: Stop) -> Self {
        Self::Stop(e)
    }
}
impl<E> From<HeapError> for Fault<E> {
    fn from(e: HeapError) -> Self {
        match e {
            HeapError::ResourceLimit => Self::Stop(Stop::ResourceLimit),
            HeapError::Stop(e) => Self::Stop(e),
            e => Self::Native(Native::Heap(e)),
        }
    }
}
impl<E> From<gas::Error> for Fault<E> {
    fn from(e: gas::Error) -> Self {
        Self::Native(Native::Gas(e))
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome<C> {
    Complete { state: Box<State>, chain: C },
    Error { reason: Failure, gas: BigUint },
    Revert { output: Vec<u8>, gas: BigUint },
}
// The host stages a new chain snapshot. It must not durably publish during this
// call: the original VM can fail its final gas check after invoking set_store.
pub trait StoreHost {
    type Chain: Clone;
    type Error;
    fn set_store(
        &self,
        store: Store,
        chain: &Self::Chain,
    ) -> Result<Self::Chain, Fault<Self::Error>>;
}
pub(crate) fn write_heap<E>(
    s: &mut State,
    heap: Heap,
    b: &mut Budget,
) -> Result<BigUint, Fault<E>> {
    b.memory(&s.memory, &heap.offset, heap.memory.len())?;
    b.work(heap.memory.len().div_ceil(32))?;
    maps::reserve_maps(&heap.maps, b)?;
    s.memory.write_area(&heap.offset, &heap.memory);
    s.maps.entries.extend(heap.maps.entries);
    s.maps.next_id = heap.maps.next_id;
    Ok(heap.pointer)
}
pub(crate) fn initialize_inner<E>(
    state: &State,
    call_data_type: Option<&TypeRef>,
    b: &mut Budget,
) -> Result<State, Fault<E>> {
    let mut s = execution::snapshot(state, b, false)?;
    s.cp = 0;
    s.stack.clear();
    s.call_creates.clear();
    s.logs.clear();
    s.output.clear();
    s.return_data.clear();
    if s.version == Version::Solidity1 {
        // contracts_store:contents excludes the empty subtree-root key.
        let pairs: Vec<_> = s.map_store.iter().filter(|(k, _)| !k.is_empty()).collect();
        let keys: Vec<_> = pairs.iter().map(|(k, _)| k.as_slice()).collect();
        b.bytes(keys.len().checked_mul(32).ok_or(Stop::ResourceLimit)?)?;
        let mut storage = BTreeMap::new();
        for i in store::binary_indices(&keys, true, b)? {
            let (k, v) = pairs[i];
            b.bytes(k.len() + v.len())?;
            let value = BigUint::from_bytes_be(v);
            // Native zero values skip insertion, including duplicate integer keys.
            if !value.is_zero() {
                storage.insert(BigUint::from_bytes_be(k), value);
            }
        }
        s.solidity_storage = storage;
        return Ok(s);
    }
    let t = call_data_type
        .filter(|_| s.out_type.is_some())
        .ok_or(Fault::Native(Native::MissingCallDataType))?;
    if s.call_stack.contains(&s.context.address) {
        return Err(Fault::Native(Native::ReentrantCall));
    }
    let pointer = match store::state_type_inner(&s.map_store, b)? {
        None => 0u8.into(),
        Some(_) => {
            let heap = store::load_inner(&s.map_store, b)?;
            write_heap(&mut s, heap, b)?
        }
    };
    b.memory(&s.memory, &0u8.into(), 32)?;
    s.memory.store(&0u8.into(), Word::from_unsigned(&pointer));
    let heap = runtime_heap::binary_to_heap_inner(
        t,
        &s.data,
        s.maps.next_id.clone(),
        s.memory.size_in_words() * 32u8,
        b,
    )
    .map_err(|e| match e {
        HeapError::ResourceLimit | HeapError::Stop(_) | HeapError::NativeFunctionClause => {
            Fault::from(e)
        }
        _ => Fault::Native(Native::BadCallData),
    })?;
    let ptr = write_heap(&mut s, heap, b)?;
    b.bytes(32)?;
    s.stack.push(Word::from_unsigned(&ptr));
    Ok(s)
}
pub fn initialize(
    state: &State,
    call_data_type: Option<&TypeRef>,
    limits: Limits,
) -> Result<State, Fault> {
    initialize_inner(state, call_data_type, &mut Budget::new(limits))
}
fn out_of_gas<C>() -> Outcome<C> {
    Outcome::Error {
        reason: Failure::OutOfGas,
        gas: BigUint::zero(),
    }
}
fn byte_size(heap: &Heap, b: &mut Budget) -> Result<BigUint, Stop> {
    let mut size = BigUint::from(heap.memory.len());
    for map in heap.maps.entries.values() {
        b.work(1)?;
        if let Data::Local(data) = &map.data {
            for (key, value) in data {
                b.work(1)?;
                if let Some(value) = value {
                    size += key.len();
                    size += value.len();
                }
            }
        }
    }
    Ok(size)
}
fn unsigned(n: &BigUint) -> Vec<u8> {
    if n.is_zero() {
        vec![0]
    } else {
        n.to_bytes_be()
    }
}
pub(crate) fn finalize_inner<H: StoreHost>(
    state: &State,
    chain: &H::Chain,
    host: &H,
    profile: OrderProfile,
    b: &mut Budget,
) -> Result<Outcome<H::Chain>, Fault<H::Error>> {
    let mut s = execution::snapshot(state, b, true)?;
    if s.version == Version::Solidity1 {
        let mut output = Store::new();
        for (key, value) in &s.solidity_storage {
            b.work(1)?;
            if !value.is_zero() {
                b.bytes(
                    usize::try_from(key.bits().div_ceil(8) + value.bits().div_ceil(8))
                        .map_err(|_| Stop::ResourceLimit)?,
                )?;
                output.insert(unsigned(key), unsigned(value));
            }
        }
        let chain = host.set_store(output, chain)?;
        return Ok(Outcome::Complete {
            state: Box::new(s),
            chain,
        });
    }
    // Native load returns a new memory value, but save_store ignores it.
    let addr = s.memory.peek(&0u8.into()).unsigned();
    if addr.is_zero() {
        return Ok(Outcome::Complete {
            state: Box::new(s),
            chain: chain.clone(),
        });
    }
    let ty = store::state_type_inner(&s.map_store, b)?;
    let ptr = s.memory.peek(&addr);
    let maximum = gas::memory_limit(&s.gas, &s.gas_table, s.float_profile)? * 32u8;
    let Some(ty) = ty else {
        return Ok(out_of_gas());
    };
    let heap = match runtime_heap::copy_memory(&ty, ptr, &s.memory, &s.maps, maximum, b) {
        Ok(h) => h,
        Err(e @ HeapError::ResourceLimit) | Err(e @ HeapError::Stop(_)) => return Err(e.into()),
        Err(_) => return Ok(out_of_gas()),
    };
    let memory_gas = gas::memory(
        &(heap.memory.len() / 32).into(),
        &s.gas_table,
        s.float_profile,
    )?;
    let store_gas = if s.protocol >= 4 {
        byte_size(&heap, b)? * 5u8
    } else {
        BigUint::zero()
    };
    let total = memory_gas + store_gas;
    let store = store::publish_inner(s.version, profile, &heap, &s.map_store, b)?;
    let chain = host.set_store(store, chain)?;
    if s.gas < total {
        if matches!(s.version, Version::Sophia1 | Version::Sophia2) {
            return Err(Fault::Native(Native::OutOfGas));
        }
        return Ok(out_of_gas());
    }
    s.gas -= total;
    Ok(Outcome::Complete {
        state: Box::new(s),
        chain,
    })
}
pub fn finalize<H: StoreHost>(
    state: &State,
    chain: &H::Chain,
    host: &H,
    profile: OrderProfile,
    limits: Limits,
) -> Result<Outcome<H::Chain>, Fault<H::Error>> {
    finalize_inner(state, chain, host, profile, &mut Budget::new(limits))
}
fn evaluate_inner<H: StoreHost>(
    state: &State,
    chain: &H::Chain,
    host: &H,
    profile: OrderProfile,
    b: &mut Budget,
) -> Result<Outcome<H::Chain>, Fault<H::Error>> {
    match execution::evaluate_inner(state, b)? {
        execution::Outcome::Complete(s) => finalize_inner(&s, chain, host, profile, b),
        execution::Outcome::Error { reason, gas } => Ok(Outcome::Error { reason, gas }),
        execution::Outcome::Revert { output, gas } => Ok(Outcome::Revert { output, gas }),
        execution::Outcome::Native(e) => Err(e.into()),
    }
}
pub fn evaluate<H: StoreHost>(
    state: &State,
    chain: &H::Chain,
    host: &H,
    profile: OrderProfile,
    limits: Limits,
) -> Result<Outcome<H::Chain>, Fault<H::Error>> {
    evaluate_inner(state, chain, host, profile, &mut Budget::new(limits))
}
pub fn invoke<H: StoreHost>(
    state: &State,
    call_data_type: Option<&TypeRef>,
    chain: &H::Chain,
    host: &H,
    profile: OrderProfile,
    limits: Limits,
) -> Result<Outcome<H::Chain>, Fault<H::Error>> {
    let mut b = Budget::new(limits);
    let s = initialize_inner(state, call_data_type, &mut b)?;
    evaluate_inner(&s, chain, host, profile, &mut b)
}
