// aevm_data/aevm_data_v1 heap-to-binary conversion, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use crate::maps::{Maps, Store};
use crate::{
    execution::{Budget, Limits, Stop},
    memory::Memory,
    word::Word,
};
use num_bigint::BigUint;
use std::sync::Arc;
mod convert;

pub type TypeRef = Arc<Type>;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    Word,
    String,
    List(TypeRef),
    Tuple(Vec<TypeRef>),
    Variant(Vec<Vec<TypeRef>>),
    TypeRep,
    Map(TypeRef, TypeRef),
    Function,
    ContractBytearray,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidValue,
    MissingMap(BigUint),
    CircularReferences,
    AddressBeforeOffset,
    SizeLimit,
    BinaryTooShort,
    FunctionClause,
    NativeFunctionClause,
    ResourceLimit,
    Stop(Stop),
}
impl From<Stop> for Error {
    fn from(e: Stop) -> Self {
        if e == Stop::ResourceLimit {
            Self::ResourceLimit
        } else {
            Self::Stop(e)
        }
    }
}

pub(crate) fn check_type<'a>(ty: &'a Type, budget: &mut Budget) -> Result<(), Stop> {
    let mut pending = vec![(ty, 0usize)];
    while let Some((t, depth)) = pending.pop() {
        budget.work(1)?;
        if depth > budget.depth {
            return Err(Stop::ResourceLimit);
        }
        let mut add = |t: &'a TypeRef| {
            pending.push((t.as_ref(), depth + 1));
        };
        match t {
            Type::List(t) => add(t),
            Type::Map(a, b) => {
                add(a);
                add(b);
            }
            Type::Tuple(ts) => {
                budget.bytes(
                    ts.len()
                        .checked_mul(std::mem::size_of::<(&Type, usize)>())
                        .ok_or(Stop::ResourceLimit)?,
                )?;
                for t in ts {
                    add(t);
                }
            }
            Type::Variant(cs) => {
                budget.work(cs.len())?;
                for ts in cs {
                    budget.bytes(
                        ts.len()
                            .checked_mul(std::mem::size_of::<(&Type, usize)>())
                            .ok_or(Stop::ResourceLimit)?,
                    )?;
                    for t in ts {
                        add(t);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapSerialization {
    Legacy,
    Corrected,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heap {
    pub pointer: BigUint,
    pub offset: BigUint,
    pub memory: Vec<u8>,
    pub maps: Maps,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Format {
    Binary,
    Heap,
}

pub fn binary_to_heap(
    ty: &TypeRef,
    bin: &[u8],
    next_id: BigUint,
    offset: BigUint,
    limits: Limits,
) -> Result<Heap, Error> {
    if bin.is_empty() {
        return Err(Error::BinaryTooShort);
    }
    let pointer = binary_pointer(bin)?;
    let mut budget = Budget::new(limits);
    check_type(ty, &mut budget)?;
    let maps = Maps {
        next_id,
        ..Maps::default()
    };
    convert::run(
        ty,
        pointer,
        convert::Source::binary(bin, 32, 32u8.into()),
        &maps,
        &Store::new(),
        Format::Binary,
        Format::Heap,
        MapSerialization::Corrected,
        offset,
        None,
        &mut budget,
    )
}
pub fn heap_to_heap(
    ty: &TypeRef,
    heap: &Heap,
    offset: BigUint,
    maximum: Option<BigUint>,
    limits: Limits,
) -> Result<Heap, Error> {
    let mut budget = Budget::new(limits);
    check_type(ty, &mut budget)?;
    convert::run(
        ty,
        heap.pointer.clone(),
        convert::Source::binary(&heap.memory, 0, heap.offset.clone()),
        &heap.maps,
        &Store::new(),
        Format::Heap,
        Format::Heap,
        MapSerialization::Corrected,
        offset,
        maximum,
        &mut budget,
    )
}
pub fn heap_to_binary(
    ty: &TypeRef,
    heap: &Heap,
    store: &Store,
    profile: MapSerialization,
    maximum: Option<BigUint>,
    limits: Limits,
) -> Result<Vec<u8>, Error> {
    let mut budget = Budget::new(limits);
    check_type(ty, &mut budget)?;
    let h = convert::run(
        ty,
        heap.pointer.clone(),
        convert::Source::binary(&heap.memory, 0, heap.offset.clone()),
        &heap.maps,
        store,
        Format::Heap,
        Format::Binary,
        profile,
        32u8.into(),
        maximum,
        &mut budget,
    )?;
    join(h, &mut budget)
}
pub fn binary_to_binary(ty: &TypeRef, bin: &[u8], limits: Limits) -> Result<Vec<u8>, Error> {
    let pointer = binary_pointer(bin)?;
    let mut budget = Budget::new(limits);
    check_type(ty, &mut budget)?;
    let h = convert::run(
        ty,
        pointer,
        convert::Source::binary(bin, 32, 32u8.into()),
        &Maps::default(),
        &Store::new(),
        Format::Binary,
        Format::Binary,
        MapSerialization::Corrected,
        32u8.into(),
        None,
        &mut budget,
    )?;
    join(h, &mut budget)
}
fn binary_pointer(bin: &[u8]) -> Result<BigUint, Error> {
    Ok(BigUint::from_bytes_be(
        bin.get(..32).ok_or(Error::NativeFunctionClause)?,
    ))
}
fn join(heap: Heap, budget: &mut Budget) -> Result<Vec<u8>, Error> {
    budget.bytes(
        heap.memory
            .len()
            .checked_add(32)
            .ok_or(Error::ResourceLimit)?,
    )?;
    let mut out = Word::from_unsigned(&heap.pointer).0.to_vec();
    out.extend(heap.memory);
    Ok(out)
}
pub(crate) struct MapContext<'a> {
    pub maps: &'a Maps,
    pub store: &'a Store,
    pub profile: MapSerialization,
}
pub(crate) fn serialize(
    ty: &TypeRef,
    ptr: Word,
    memory: &Memory,
    context: MapContext<'_>,
    maximum: &BigUint,
    budget: &mut Budget,
) -> Result<Vec<u8>, Error> {
    let h = convert::run(
        ty,
        ptr.unsigned(),
        convert::Source::Sparse(memory),
        context.maps,
        context.store,
        Format::Heap,
        Format::Binary,
        context.profile,
        32u8.into(),
        Some(maximum.clone()),
        budget,
    )?;
    join(h, budget)
}
