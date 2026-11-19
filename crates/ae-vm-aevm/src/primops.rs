// aevm_ae_primops local map/crypto/auth calls, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{
    calls,
    execution::{self, Budget, Limits, State, Stop},
    gas,
    maps::{self, Data},
    opcode::Version,
    runtime::Fault,
    runtime_heap::{self, Error as HeapError, MapContext, MapSerialization, TypeRef},
    store,
    word::Word,
};
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use std::sync::Arc;
mod chain;
mod pure;
pub(crate) use chain::call_inner;
pub use chain::{AddressKind, ChainHost, HostResult, call};
pub use pure::call_local;
pub(crate) use pure::call_local_inner;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    IllegalCall,
    OutOfGas,
    BaseGas {
        op: Word,
        required: BigUint,
        state: Box<State>,
    },
    Heap(HeapError),
    Chain(String),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Complete {
        data: Vec<u8>,
        gas_spent: BigUint,
        state: Box<State>,
    },
    Error(Error),
}
pub trait MapHost {
    type Error;
    fn get_store(&self) -> Result<maps::Store, Self::Error>;
}
enum Problem<E> {
    Fault(Fault),
    Returned(Error),
    Host(E),
}
impl<E> From<Fault> for Problem<E> {
    fn from(e: Fault) -> Self {
        Self::Fault(e)
    }
}
impl<E> From<Stop> for Problem<E> {
    fn from(e: Stop) -> Self {
        Self::Fault(e.into())
    }
}
impl<E> From<HeapError> for Problem<E> {
    fn from(e: HeapError) -> Self {
        Self::Fault(e.into())
    }
}
impl<E> From<gas::Error> for Problem<E> {
    fn from(e: gas::Error) -> Self {
        Self::Fault(e.into())
    }
}
fn converted<E>(e: HeapError) -> Problem<E> {
    match e {
        HeapError::ResourceLimit | HeapError::Stop(_) => e.into(),
        _ => Problem::Returned(Error::Heap(e)),
    }
}
fn serialize<E>(
    t: &TypeRef,
    ptr: Word,
    s: &State,
    b: &mut Budget,
) -> Result<(Vec<u8>, BigUint), Problem<E>> {
    let maximum = gas::memory_limit(&s.gas, &s.gas_table, s.float_profile)? * 32u8;
    let profile = if s.version == Version::Sophia4 {
        MapSerialization::Corrected
    } else {
        MapSerialization::Legacy
    };
    runtime_heap::check_type(t, b)?;
    let bytes = runtime_heap::serialize(
        t,
        ptr,
        &s.memory,
        MapContext {
            maps: &s.maps,
            store: &s.map_store,
            profile,
        },
        &maximum,
        b,
    )
    .map_err(|e| {
        if e == HeapError::SizeLimit {
            Problem::Returned(Error::OutOfGas)
        } else {
            converted(e)
        }
    })?;
    let gas = gas::memory(&(bytes.len() / 32).into(), &s.gas_table, s.float_profile)?;
    Ok((bytes, gas))
}
fn append(out: &mut Vec<u8>, bytes: &[u8], b: &mut Budget) -> Result<(), Stop> {
    b.bytes(bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}
fn list<H: MapHost>(
    id: &BigUint,
    s: &State,
    host: &H,
    b: &mut Budget,
) -> Result<Vec<u8>, Problem<H::Error>> {
    let map = s.maps.get_map(id)?;
    let store = host.get_store().map_err(Problem::Host)?;
    maps::reserve_store(&store, b)?;
    let flat = maps::flatten_inner(id, map, &store, b)?;
    let Data::Local(data) = flat.data else {
        unreachable!()
    };
    let mut out = Vec::new();
    if data.is_empty() {
        append(&mut out, &[255; 32], b)?;
        return Ok(out);
    }
    append(&mut out, &Word::from(32).0, b)?;
    let count = data.len();
    let mut offset = BigUint::from(32u8);
    for (i, (k, v)) in data.into_iter().enumerate() {
        b.work(1)?;
        let v = v.ok_or(HeapError::InvalidValue)?;
        let header = v.get(..32).ok_or(HeapError::InvalidValue)?;
        let kh =
            runtime_heap::binary_to_heap_inner(&map.key_type, &k, 0u8.into(), &offset + 128u16, b)?;
        let vh = runtime_heap::copy_binary(
            &map.value_type,
            BigUint::from_bytes_be(header),
            &v[32..],
            &s.maps,
            32u8.into(),
            &offset + 128u16 + kh.memory.len(),
            None,
            b,
        )?;
        let tail = &offset + 128u16 + kh.memory.len() + vh.memory.len();
        for word in [
            Word::from_unsigned(&(&offset + 64u8)),
            if i + 1 == count {
                Word([255; 32])
            } else {
                Word::from_unsigned(&tail)
            },
            Word::from_unsigned(&kh.pointer),
            Word::from_unsigned(&vh.pointer),
        ] {
            append(&mut out, &word.0, b)?;
        }
        append(&mut out, &kh.memory, b)?;
        append(&mut out, &vh.memory, b)?;
        offset = tail;
    }
    Ok(out)
}
fn run<H: MapHost>(
    s: &State,
    data: &[u8],
    host: &H,
    b: &mut Budget,
) -> Result<Outcome, Problem<H::Error>> {
    let mut next = execution::snapshot(s, b, true)?;
    let op = calls::words(data, 1, b)?[0].unsigned().to_u16();
    let mut gas_spent = BigUint::from(0u8);
    let output = match op {
        Some(300) => {
            let args = calls::words(data, 3, b)?;
            let parent = Some(BigUint::from_bytes_be(&data[..32]));
            let key = store::parse_type(data, args[1].unsigned(), parent.clone(), b)?;
            let value = store::parse_type(data, args[2].unsigned(), parent, b)?;
            let (id, maps) = s.maps.empty_inner(key, value, b)?;
            next.maps = maps;
            Word::from_unsigned(&id).0.to_vec()
        }
        Some(op @ 301..=305) => {
            let id = calls::words(data, 2, b)?[1].unsigned();
            let map = s.maps.get_map(&id)?;
            match op {
                304 => Word::wrapping(&map.size).0.to_vec(),
                305 => list(&id, s, host, b)?,
                _ => {
                    let args = calls::words(data, if op == 302 { 4 } else { 3 }, b)?;
                    let (key, kg) = serialize(&map.key_type, args[2], s, b)?;
                    gas_spent = kg;
                    if op == 301 {
                        let mut out = Vec::new();
                        match s.maps.get_inner(&id, &key, &s.map_store, b)? {
                            None => {
                                append(&mut out, &Word::from(32).0, b)?;
                                append(&mut out, &[0; 32], b)?;
                            }
                            Some(value) => {
                                let ptr = value.get(..32).ok_or(HeapError::InvalidValue)?;
                                append(&mut out, &Word::from_unsigned(&value.len().into()).0, b)?;
                                append(&mut out, &value[32..], b)?;
                                append(&mut out, &Word::from(1).0, b)?;
                                append(&mut out, ptr, b)?;
                            }
                        }
                        out
                    } else {
                        let value = if op == 302 {
                            let maximum = gas::memory_limit(&s.gas, &s.gas_table, s.float_profile)?
                                * if s.version == Version::Sophia1 {
                                    1u8
                                } else {
                                    32u8
                                };
                            let h = runtime_heap::copy_memory(
                                &map.value_type,
                                args[3],
                                &s.memory,
                                &s.maps,
                                maximum,
                                b,
                            )
                            .map_err(converted)?;
                            gas_spent += gas::memory(
                                &(h.memory.len() / 32).into(),
                                &s.gas_table,
                                s.float_profile,
                            )?;
                            let mut bin = Vec::new();
                            append(&mut bin, &Word::from_unsigned(&h.pointer).0, b)?;
                            append(&mut bin, &h.memory, b)?;
                            Some(Arc::from(bin))
                        } else {
                            None
                        };
                        let (id, maps) =
                            s.maps
                                .update_inner(&id, Arc::from(key), value, &s.map_store, b)?;
                        next.maps = maps;
                        Word::from_unsigned(&id).0.to_vec()
                    }
                }
            }
        }
        _ => return Ok(Outcome::Error(Error::IllegalCall)),
    };
    b.bytes(output.len())?;
    Ok(Outcome::Complete {
        data: output,
        gas_spent,
        state: Box::new(next),
    })
}
// Map calls have zero base gas; the original helper ignores the separate call
// allowance and value. Heap conversion uses the gas in the caller state.
pub fn call_map<H: MapHost>(
    state: &State,
    data: &[u8],
    host: &H,
    limits: Limits,
) -> Result<Outcome, Fault<H::Error>> {
    call_map_inner(state, data, host, &mut Budget::new(limits))
}
pub(crate) fn call_map_inner<H: MapHost>(
    state: &State,
    data: &[u8],
    host: &H,
    b: &mut Budget,
) -> Result<Outcome, Fault<H::Error>> {
    match run(state, data, host, b) {
        Ok(out) => Ok(out),
        Err(Problem::Returned(e)) => Ok(Outcome::Error(e)),
        Err(Problem::Host(e)) => Err(Fault::Host(e)),
        Err(Problem::Fault(Fault::Stop(e))) => Err(Fault::Stop(e)),
        Err(Problem::Fault(Fault::Native(_))) => Ok(Outcome::Error(Error::IllegalCall)),
        Err(Problem::Fault(Fault::Host(e))) => match e {},
    }
}
