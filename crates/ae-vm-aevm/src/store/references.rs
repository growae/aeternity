// aevm_data:has_maps/used_maps, fa2a6d7. ISC: repository LICENSE.
// Copyright (c) 2018, Aeternity Anstalt.
use super::{Budget, Error, Limits, Type, TypeRef, Word, word};
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use std::collections::BTreeSet;

pub fn has_maps(ty: &TypeRef, limits: Limits) -> Result<bool, Error> {
    contains(ty, &mut Budget::new(limits))
}
pub(super) fn contains(ty: &TypeRef, b: &mut Budget) -> Result<bool, Error> {
    let mut pending = vec![(ty, 0)];
    while let Some((t, depth)) = pending.pop() {
        b.work(1)?;
        if depth > b.depth {
            return Err(Error::ResourceLimit);
        }
        match t.as_ref() {
            Type::Map(_, _) => return Ok(true),
            Type::Word | Type::String | Type::TypeRep => {}
            Type::Function | Type::ContractBytearray => return Err(Error::NativeFunctionClause),
            Type::List(t) => pending.push((t, depth + 1)),
            Type::Tuple(ts) => {
                b.bytes(ts.len().checked_mul(32).ok_or(Error::ResourceLimit)?)?;
                pending.extend(ts.iter().rev().map(|t| (t, depth + 1)));
            }
            Type::Variant(cs) => {
                b.work(cs.len())?;
                for ts in cs.iter().rev() {
                    b.bytes(ts.len().checked_mul(32).ok_or(Error::ResourceLimit)?)?;
                    pending.extend(ts.iter().rev().map(|t| (t, depth + 1)));
                }
            }
        }
    }
    Ok(false)
}
pub fn used_maps(ty: &TypeRef, bytes: &[u8], limits: Limits) -> Result<BTreeSet<BigUint>, Error> {
    used(ty, bytes, &mut Budget::new(limits))
}
enum Task<'a> {
    Visit(&'a TypeRef, BigUint, usize),
    Read(&'a TypeRef, BigUint, usize),
    Leave(BigUint),
}
fn read(bytes: &[u8], ptr: &BigUint) -> Result<BigUint, Error> {
    if *ptr < BigUint::from(32u8) {
        return Err(Error::AddressBeforeOffset);
    }
    word(bytes, ptr)
}
pub(super) fn used(ty: &TypeRef, bytes: &[u8], b: &mut Budget) -> Result<BTreeSet<BigUint>, Error> {
    let head = bytes.get(..32).ok_or(Error::NativeFunctionClause)?;
    let mut tasks = vec![Task::Visit(ty, BigUint::from_bytes_be(head), 0)];
    let mut active = BTreeSet::new();
    let mut result = BTreeSet::new();
    while let Some(task) = tasks.pop() {
        b.work(1)?;
        let (t, ptr, depth) = match task {
            Task::Leave(p) => {
                active.remove(&p);
                continue;
            }
            Task::Visit(t, p, d) => (t, p, d),
            Task::Read(t, p, d) => (t, read(bytes, &p)?, d),
        };
        if depth > b.depth {
            return Err(Error::ResourceLimit);
        }
        if !contains(t, b)? {
            continue;
        }
        match t.as_ref() {
            Type::Map(_, _) => {
                b.bytes(32)?;
                result.insert(ptr);
            }
            Type::List(item) => {
                if ptr == Word([255; 32]).unsigned() {
                    continue;
                }
                enter(&mut active, &mut tasks, &ptr, b)?;
                tasks.push(Task::Read(t, &ptr + 32u8, depth + 1));
                tasks.push(Task::Read(item, ptr, depth + 1));
            }
            Type::Tuple(ts) => {
                enter(&mut active, &mut tasks, &ptr, b)?;
                b.bytes(ts.len().checked_mul(64).ok_or(Error::ResourceLimit)?)?;
                for (i, t) in ts.iter().enumerate().rev() {
                    tasks.push(Task::Read(t, &ptr + i * 32, depth + 1));
                }
            }
            Type::Variant(cs) => {
                let tag = read(bytes, &ptr)?.to_usize().ok_or(Error::InvalidValue)?;
                let ts = cs.get(tag).ok_or(Error::InvalidValue)?;
                // The selected tuple can be map-free even if another case has maps.
                let mut any = false;
                for t in ts {
                    if contains(t, b)? {
                        any = true;
                        break;
                    }
                }
                if any {
                    enter(&mut active, &mut tasks, &ptr, b)?;
                    b.bytes(ts.len().checked_mul(64).ok_or(Error::ResourceLimit)?)?;
                    for (i, t) in ts.iter().enumerate().rev() {
                        tasks.push(Task::Read(t, &ptr + (i + 1) * 32, depth + 1));
                    }
                }
            }
            _ => unreachable!("contains maps"),
        }
    }
    Ok(result)
}
fn enter(
    active: &mut BTreeSet<BigUint>,
    tasks: &mut Vec<Task<'_>>,
    ptr: &BigUint,
    b: &mut Budget,
) -> Result<(), Error> {
    b.bytes(32 + 3 * std::mem::size_of::<Task<'_>>())?;
    if !active.insert(ptr.clone()) {
        return Err(Error::CircularReferences);
    }
    tasks.push(Task::Leave(ptr.clone()));
    Ok(())
}
