// aevm_data/aevm_data_v1 heap-to-binary conversion, fa2a6d7.
// Copyright (c) 2018, Aeternity Anstalt. ISC: repository LICENSE.
use crate::{
    execution::{Budget, Stop},
    memory::Memory,
    word::Word,
};
use num_bigint::BigUint;
use num_traits::{ToPrimitive, Zero};
use std::{collections::BTreeSet, sync::Arc};

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
}
#[derive(Debug)]
pub(crate) enum Error {
    InvalidValue,
    UnsupportedMap,
    Stop(Stop),
}
impl From<Stop> for Error {
    fn from(e: Stop) -> Self {
        Self::Stop(e)
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
#[derive(Clone, Copy)]
enum Plan<'a> {
    Native(&'a Type),
    Word,
    Rep,
    RepList,
    RepLists,
}
enum Task<'a> {
    Visit {
        plan: Plan<'a>,
        ptr: Word,
        dest: usize,
        depth: usize,
    },
    Leave(Word),
}
fn aligned(ptr: &BigUint) -> Result<(), Error> {
    if (ptr % 32u8).is_zero() {
        Ok(())
    } else {
        Err(Error::InvalidValue)
    }
}
fn read(memory: &Memory, ptr: &BigUint) -> Result<Word, Error> {
    aligned(ptr)?;
    Ok(memory.words().get(ptr).copied().unwrap_or(Word::ZERO))
}
fn allocate(
    out: &mut Vec<u8>,
    size: usize,
    maximum: &BigUint,
    budget: &mut Budget,
) -> Result<usize, Error> {
    let next = out.len().checked_add(size).ok_or(Stop::ResourceLimit)?;
    if BigUint::from(next - 32) > *maximum {
        return Err(Error::InvalidValue);
    }
    budget.bytes(size)?;
    budget.work(size.div_ceil(32))?;
    let base = out.len();
    out.resize(next, 0);
    Ok(base)
}
fn put(out: &mut [u8], dest: usize, value: Word) {
    out[dest..dest + 32].copy_from_slice(&value.0);
}

// Copies raw heap payloads instead of decoding/re-encoding normalized values.
// Native map conversion requires the historical map/store provider, not this
// sparse-memory-only boundary. Until supplied, it is an operational stop.
pub(crate) fn serialize(
    ty: &Type,
    ptr: Word,
    memory: &Memory,
    maximum: &BigUint,
    budget: &mut Budget,
) -> Result<Vec<u8>, Error> {
    budget.bytes(32)?;
    let mut out = vec![0; 32];
    let mut active = BTreeSet::new();
    let mut tasks = vec![Task::Visit {
        plan: Plan::Native(ty),
        ptr,
        dest: 0,
        depth: 0,
    }];
    while let Some(task) = tasks.pop() {
        budget.work(1)?;
        let Task::Visit {
            plan,
            ptr,
            dest,
            depth,
        } = task
        else {
            if let Task::Leave(ptr) = task {
                active.remove(&ptr);
            }
            continue;
        };
        if depth > budget.depth {
            return Err(Stop::ResourceLimit.into());
        }
        let address = ptr.unsigned();
        let fields: Vec<Plan<'_>> = match plan {
            Plan::Word | Plan::Native(Type::Word) => {
                put(&mut out, dest, ptr);
                continue;
            }
            Plan::Native(Type::String) => {
                let size = (read(memory, &address)?.unsigned() + 31u8) / 32u8 * 32u8 + 32u8;
                if &size > maximum || BigUint::from(out.len() - 32) + &size > *maximum {
                    return Err(Error::InvalidValue);
                }
                let size = size.to_usize().ok_or(Stop::ResourceLimit)?;
                let base = allocate(&mut out, size, maximum, budget)?;
                for i in 0..size / 32 {
                    let value = read(memory, &(&address + i * 32))?;
                    put(&mut out, base + i * 32, value);
                }
                put(&mut out, dest, Word::from_unsigned(&base.into()));
                continue;
            }
            Plan::Native(Type::List(item)) => {
                if ptr == Word([255; 32]) {
                    put(&mut out, dest, ptr);
                    continue;
                }
                vec![Plan::Native(item), plan]
            }
            Plan::RepList | Plan::RepLists => {
                if ptr == Word([255; 32]) {
                    put(&mut out, dest, ptr);
                    continue;
                }
                vec![
                    if matches!(plan, Plan::RepList) {
                        Plan::Rep
                    } else {
                        Plan::RepList
                    },
                    plan,
                ]
            }
            Plan::Native(Type::Tuple(ts)) => {
                if ts.is_empty() {
                    put(&mut out, dest, Word::ZERO);
                    continue;
                }
                budget.bytes(
                    ts.len()
                        .checked_mul(std::mem::size_of::<Plan<'_>>())
                        .ok_or(Stop::ResourceLimit)?,
                )?;
                ts.iter().map(|t| Plan::Native(t)).collect()
            }
            Plan::Native(Type::Variant(cs)) => {
                let tag = read(memory, &address)?
                    .unsigned()
                    .to_usize()
                    .ok_or(Error::InvalidValue)?;
                let ts = cs.get(tag).ok_or(Error::InvalidValue)?;
                budget.bytes(
                    ts.len()
                        .checked_add(1)
                        .and_then(|n| n.checked_mul(std::mem::size_of::<Plan<'_>>()))
                        .ok_or(Stop::ResourceLimit)?,
                )?;
                std::iter::once(Plan::Word)
                    .chain(ts.iter().map(|t| Plan::Native(t)))
                    .collect()
            }
            Plan::Native(Type::TypeRep) | Plan::Rep => {
                match read(memory, &address)?
                    .unsigned()
                    .to_u8()
                    .ok_or(Error::InvalidValue)?
                {
                    0 | 1 | 5 => vec![Plan::Word],
                    2 => vec![Plan::Word, Plan::Rep],
                    3 => vec![Plan::Word, Plan::RepList],
                    4 => vec![Plan::Word, Plan::RepLists],
                    6 => vec![Plan::Word, Plan::Rep, Plan::Rep],
                    _ => return Err(Error::InvalidValue),
                }
            }
            Plan::Native(Type::Map(..)) => return Err(Error::UnsupportedMap),
        };
        if active.contains(&ptr) {
            return Err(Error::InvalidValue);
        }
        aligned(&address)?;
        budget.bytes(32)?;
        budget.bytes(
            fields
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_mul(std::mem::size_of::<Task<'_>>()))
                .ok_or(Stop::ResourceLimit)?,
        )?;
        let base = allocate(
            &mut out,
            fields.len().checked_mul(32).ok_or(Stop::ResourceLimit)?,
            maximum,
            budget,
        )?;
        active.insert(ptr);
        put(&mut out, dest, Word::from_unsigned(&base.into()));
        tasks.push(Task::Leave(ptr));
        for (i, plan) in fields.into_iter().enumerate().rev() {
            let child = read(memory, &(&address + i * 32))?;
            tasks.push(Task::Visit {
                plan,
                ptr: child,
                dest: base + i * 32,
                depth: depth + 1,
            });
        }
    }
    Ok(out)
}
