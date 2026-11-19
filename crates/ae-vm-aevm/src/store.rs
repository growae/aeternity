// aevm_eeevm_store loading and aeb_heap type representations, fa2a6d7/009e036.
// Copyright (c) 2017-2018, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{
    execution::{Budget, Limits},
    maps::{Data, Map, Maps, Store},
    runtime_heap::{Error, Heap, Type, TypeRef},
    word::Word,
};
use num_bigint::BigUint;
use num_traits::{ToPrimitive, Zero};
use std::{
    collections::{BTreeSet, VecDeque},
    sync::Arc,
};
mod order;
mod references;
mod typerep;
mod write;
pub use order::OrderProfile;
pub(crate) use order::binary_indices;
pub use references::{has_maps, used_maps};
pub(crate) use write::publish_inner;
pub use write::{Plan, Update, initialize, plan, publish};

fn word(bytes: &[u8], ptr: &BigUint) -> Result<BigUint, Error> {
    let start = ptr.to_usize().ok_or(Error::InvalidValue)?;
    let end = start.checked_add(32).ok_or(Error::InvalidValue)?;
    Ok(BigUint::from_bytes_be(
        bytes.get(start..end).ok_or(Error::InvalidValue)?,
    ))
}
enum Task {
    Type(BigUint, usize),
    Types(BigUint, usize),
    Cases(BigUint, usize),
    Build(u8),
    Cons(bool),
    Leave(BigUint),
}
enum Value {
    Type(TypeRef),
    Types(VecDeque<TypeRef>),
    Cases(VecDeque<Vec<TypeRef>>),
}
fn pop_type(v: &mut Vec<Value>) -> Result<TypeRef, Error> {
    match v.pop() {
        Some(Value::Type(t)) => Ok(t),
        _ => Err(Error::InvalidValue),
    }
}
pub(crate) fn parse_type(
    bytes: &[u8],
    ptr: BigUint,
    parent: Option<BigUint>,
    b: &mut Budget,
) -> Result<TypeRef, Error> {
    let mut active = BTreeSet::new();
    if let Some(parent) = parent {
        active.insert(parent);
    }
    let mut tasks = vec![Task::Type(ptr, 0)];
    let mut values = Vec::new();
    while let Some(task) = tasks.pop() {
        b.work(1)?;
        let (ptr, depth, kind) = match task {
            Task::Leave(ptr) => {
                active.remove(&ptr);
                continue;
            }
            Task::Build(tag) => {
                let t = match tag {
                    2 => Type::List(pop_type(&mut values)?),
                    3 => {
                        let Some(Value::Types(ts)) = values.pop() else {
                            return Err(Error::InvalidValue);
                        };
                        Type::Tuple(ts.into_iter().collect())
                    }
                    4 => {
                        let Some(Value::Cases(cs)) = values.pop() else {
                            return Err(Error::InvalidValue);
                        };
                        Type::Variant(cs.into_iter().collect())
                    }
                    6 => {
                        let val = pop_type(&mut values)?;
                        let key = pop_type(&mut values)?;
                        Type::Map(key, val)
                    }
                    _ => return Err(Error::InvalidValue),
                };
                b.bytes(std::mem::size_of::<Type>())?;
                values.push(Value::Type(Arc::new(t)));
                continue;
            }
            Task::Cons(cases) => {
                b.bytes(std::mem::size_of::<TypeRef>())?;
                let tail = values.pop().ok_or(Error::InvalidValue)?;
                let head = values.pop().ok_or(Error::InvalidValue)?;
                match (cases, head, tail) {
                    (false, Value::Type(head), Value::Types(mut tail)) => {
                        tail.push_front(head);
                        values.push(Value::Types(tail));
                    }
                    (true, Value::Types(head), Value::Cases(mut tail)) => {
                        tail.push_front(head.into_iter().collect());
                        values.push(Value::Cases(tail));
                    }
                    _ => return Err(Error::InvalidValue),
                }
                continue;
            }
            Task::Type(p, d) => (p, d, 0),
            Task::Types(p, d) => (p, d, 1),
            Task::Cases(p, d) => (p, d, 2),
        };
        if depth > b.depth {
            return Err(Error::ResourceLimit);
        }
        if kind != 0 && ptr == Word([255; 32]).unsigned() {
            values.push(if kind == 1 {
                Value::Types(VecDeque::new())
            } else {
                Value::Cases(VecDeque::new())
            });
            continue;
        }
        if !active.insert(ptr.clone()) {
            return Err(Error::CircularReferences);
        }
        b.bytes(32 + 4 * std::mem::size_of::<Task>())?;
        tasks.push(Task::Leave(ptr.clone()));
        let first = word(bytes, &ptr)?;
        if kind != 0 {
            let tail = word(bytes, &(&ptr + 32u8))?;
            tasks.push(Task::Cons(kind == 2));
            if kind == 1 {
                tasks.push(Task::Types(tail, depth + 1));
                tasks.push(Task::Type(first, depth + 1));
            } else {
                tasks.push(Task::Cases(tail, depth + 1));
                tasks.push(Task::Types(first, depth + 1));
            }
            continue;
        }
        let tag = first.to_u8().ok_or(Error::InvalidValue)?;
        let arg = |i: u8| word(bytes, &(&ptr + BigUint::from(i) * 32u8));
        let simple = match tag {
            0 => Some(Type::Word),
            1 => Some(Type::String),
            5 => Some(Type::TypeRep),
            7 => Some(Type::Function),
            8 => Some(Type::ContractBytearray),
            2 => {
                tasks.push(Task::Build(2));
                tasks.push(Task::Type(arg(1)?, depth + 1));
                None
            }
            3 => {
                tasks.push(Task::Build(3));
                tasks.push(Task::Types(arg(1)?, depth + 1));
                None
            }
            4 => {
                tasks.push(Task::Build(4));
                tasks.push(Task::Cases(arg(1)?, depth + 1));
                None
            }
            6 => {
                tasks.push(Task::Build(6));
                tasks.push(Task::Type(arg(2)?, depth + 1));
                tasks.push(Task::Type(arg(1)?, depth + 1));
                None
            }
            _ => return Err(Error::InvalidValue),
        };
        if let Some(t) = simple {
            b.bytes(std::mem::size_of::<Type>())?;
            values.push(Value::Type(Arc::new(t)));
        }
    }
    if values.len() != 1 {
        return Err(Error::InvalidValue);
    }
    pop_type(&mut values)
}
pub fn state_type(store: &Store, limits: Limits) -> Result<Option<TypeRef>, Error> {
    state_type_inner(store, &mut Budget::new(limits))
}
pub(crate) fn state_type_inner(store: &Store, b: &mut Budget) -> Result<Option<TypeRef>, Error> {
    let Some(bytes) = store.get([1].as_slice()).filter(|b| !b.is_empty()) else {
        return Ok(None);
    };
    parse_type(bytes, word(bytes, &BigUint::zero())?, None, b).map(Some)
}
pub fn load(store: &Store, limits: Limits) -> Result<Heap, Error> {
    load_inner(store, &mut Budget::new(limits))
}
pub(crate) fn load_inner(store: &Store, b: &mut Budget) -> Result<Heap, Error> {
    let state = store.get([0].as_slice()).ok_or(Error::InvalidValue)?;
    let pointer = word(state, &BigUint::zero())?;
    b.bytes(state.len())?;
    let memory = state[32..].to_vec();
    let mut maps = Maps::default();
    for id in store
        .get([2].as_slice())
        .map_or(&[][..], Vec::as_slice)
        .chunks_exact(32)
    {
        b.work(1)?;
        b.bytes(32)?;
        let id = BigUint::from_bytes_be(id);
        let meta = store
            .get(Word::from_unsigned(&id).0.as_slice())
            .ok_or(Error::InvalidValue)?;
        if meta.len() < 96 {
            return Err(Error::InvalidValue);
        }
        let size = BigUint::from_bytes_be(&meta[64..96]).into();
        let types = &meta[96..];
        let ptr = word(types, &BigUint::zero())?;
        let key_type = parse_type(types, word(types, &ptr)?, Some(ptr.clone()), b)?;
        let value_type = parse_type(types, word(types, &(&ptr + 32u8))?, Some(ptr), b)?;
        maps.next_id = maps.next_id.max(&id + 1u8);
        maps.entries.insert(
            id,
            Map {
                key_type,
                value_type,
                parent: None,
                size,
                data: Data::Stored,
            },
        );
    }
    Ok(Heap {
        pointer,
        offset: 32u8.into(),
        memory,
        maps,
    })
}
