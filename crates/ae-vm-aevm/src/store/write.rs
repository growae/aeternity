// aevm_eeevm_store and aevm_eeevm_store_vm3, fa2a6d7.
// Copyright (c) 2017-2018, Aeternity Anstalt. ISC: repository LICENSE.
use super::{
    Budget, Error, Heap, Limits, OrderProfile, TypeRef, order, parse_type, references, typerep,
    word,
};
use crate::{
    maps::{self, Bytes, Data, Maps, Store},
    opcode::Version,
    runtime_heap,
    word::Word,
};
use num_bigint::{BigInt, BigUint};
use num_traits::Zero;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Update {
    Copy {
        id: BigUint,
    },
    Reuse {
        id: BigUint,
        parent: BigUint,
        size: BigInt,
    },
    Put {
        id: BigUint,
        value_type: TypeRef,
        key: Bytes,
        value: Bytes,
    },
    Delete {
        id: BigUint,
        value_type: TypeRef,
        key: Bytes,
    },
    Collect {
        id: BigUint,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub counts: BTreeMap<BigUint, BigInt>,
    pub counts_are_deltas: bool,
    pub garbage: Vec<BigUint>,
    pub all_ids: Vec<BigUint>,
    pub updates: Vec<Update>,
}
fn corrected(version: Version) -> bool {
    version == Version::Sophia4
}
fn metadata<'a>(id: &BigUint, store: &'a Store) -> Result<&'a [u8], Error> {
    store
        .get(Word::from_unsigned(id).0.as_slice())
        .filter(|v| v.len() >= 96)
        .map(Vec::as_slice)
        .ok_or(Error::InvalidValue)
}
fn types(id: &BigUint, store: &Store, b: &mut Budget) -> Result<(TypeRef, TypeRef), Error> {
    let bytes = &metadata(id, store)?[96..];
    let ptr = word(bytes, &BigUint::zero())?;
    Ok((
        parse_type(bytes, word(bytes, &ptr)?, Some(ptr.clone()), b)?,
        parse_type(bytes, word(bytes, &(&ptr + 32u8))?, Some(ptr), b)?,
    ))
}
fn old_counts(store: &Store, b: &mut Budget) -> Result<BTreeMap<BigUint, BigInt>, Error> {
    let mut counts = BTreeMap::new();
    for chunk in store
        .get([2].as_slice())
        .map_or(&[][..], Vec::as_slice)
        .chunks_exact(32)
    {
        b.work(1)?;
        b.bytes(96)?;
        let id = BigUint::from_bytes_be(chunk);
        let n = BigUint::from_bytes_be(&metadata(&id, store)?[32..64]);
        counts.insert(id, n.into());
    }
    Ok(counts)
}
// Native store_to_list takes a physical prefix directly, without real_id lookup.
type StoredEntries = Vec<(Vec<u8>, Vec<u8>)>;
fn data(prefix: &BigUint, store: &Store, b: &mut Budget) -> Result<StoredEntries, Error> {
    let prefix = Word::from_unsigned(prefix).0;
    let mut out = Vec::new();
    for (k, v) in store.range(prefix.to_vec()..) {
        if !k.starts_with(&prefix) {
            break;
        }
        if k.len() == 32 {
            continue;
        }
        b.work(1)?;
        b.bytes(k.len() + v.len() + 64)?;
        out.push((k[32..].to_vec(), v.clone()));
    }
    Ok(out)
}
fn bump(
    counts: &mut BTreeMap<BigUint, BigInt>,
    id: BigUint,
    n: i32,
    required: bool,
    b: &mut Budget,
) -> Result<(), Error> {
    b.work(1)?;
    b.bytes(64)?;
    if required && !counts.contains_key(&id) {
        return Err(Error::NativeBadKey(id));
    }
    *counts.entry(id).or_default() += n;
    Ok(())
}
fn assignment(
    garbage: &[BigUint],
    maps: &Maps,
    only_local: bool,
    b: &mut Budget,
) -> Result<(Vec<BigUint>, BTreeMap<BigUint, BigUint>), Error> {
    b.work(garbage.len())?;
    b.bytes(garbage.len().checked_mul(64).ok_or(Error::ResourceLimit)?)?;
    let eligible: BTreeSet<_> = garbage.iter().collect();
    let mut parents = BTreeMap::new();
    for (id, map) in &maps.entries {
        b.work(1)?;
        if only_local && map.data == Data::Stored {
            continue;
        }
        if let Some(parent) = &map.parent
            && eligible.contains(parent)
        {
            b.bytes(128)?;
            parents.insert(parent.clone(), id.clone());
        }
    }
    let actual = garbage
        .iter()
        .filter(|id| !parents.contains_key(*id))
        .cloned()
        .collect();
    Ok((actual, parents))
}
fn planning(
    version: Version,
    profile: OrderProfile,
    maps: &Maps,
    store: &Store,
    b: &mut Budget,
) -> Result<Plan, Error> {
    let old = old_counts(store, b)?;
    b.bytes(old.len().checked_mul(128).ok_or(Error::ResourceLimit)?)?;
    let sorted: Vec<_> = old.keys().cloned().collect();
    let old_ids: Vec<_> = order::integers(&sorted, profile, false, b)?
        .into_iter()
        .map(|i| sorted[i].clone())
        .collect();
    let potential: Vec<_> = old_ids
        .into_iter()
        .filter(|id| !maps.entries.contains_key(id))
        .collect();
    let is_new = corrected(version);
    let mut counts = if is_new { BTreeMap::new() } else { old.clone() };
    for map in maps.entries.values() {
        b.work(1)?;
        let Data::Local(entries) = &map.data else {
            continue;
        };
        let keys: Vec<_> = entries.keys().map(|k| k.as_ref()).collect();
        let sorted: Vec<_> = entries.iter().collect();
        b.bytes(entries.len().checked_mul(32).ok_or(Error::ResourceLimit)?)?;
        for i in order::binaries(&keys, b)? {
            let (key, value) = sorted[i];
            if let Some(value) = value {
                for id in references::used(&map.value_type, value, b)? {
                    bump(&mut counts, id, 1, false, b)?;
                }
            }
            if !is_new
                && let Some(parent) = &map.parent
                && let Some(previous) = maps::stored_get(parent, key, store, b)?
            {
                for id in references::used(&map.value_type, previous, b)? {
                    bump(&mut counts, id, -1, false, b)?;
                }
            }
        }
    }
    let mut previous = BTreeSet::new();
    let garbage = loop {
        b.work(potential.len())?;
        b.bytes(
            potential
                .len()
                .checked_mul(64)
                .ok_or(Error::ResourceLimit)?,
        )?;
        let garbage: Vec<_> = potential
            .iter()
            .filter(|id| {
                let delta = counts.get(*id).cloned().unwrap_or_default();
                (delta
                    + if is_new {
                        old.get(*id).cloned().unwrap_or_default()
                    } else {
                        BigInt::zero()
                    })
                .is_zero()
            })
            .cloned()
            .collect();
        let (actual, _) = assignment(&garbage, maps, false, b)?;
        let new: Vec<_> = actual
            .iter()
            .filter(|id| !previous.contains(*id))
            .cloned()
            .collect();
        if new.is_empty() {
            break garbage;
        }
        for id in new {
            let (_, value_type) = types(&id, store, b)?;
            if references::contains(&value_type, b)? {
                for (_, value) in data(&id, store, b)? {
                    for id in references::used(&value_type, &value, b)? {
                        bump(&mut counts, id, -1, !is_new, b)?;
                    }
                }
            }
        }
        previous = actual.into_iter().collect();
    };
    let removed: BTreeSet<_> = garbage.iter().collect();
    let all_ids: Vec<_> = old
        .keys()
        .chain(maps.entries.keys())
        .filter(|id| !removed.contains(id))
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let (actual, parents) = assignment(&garbage, maps, true, b)?;
    let mut updates = Vec::new();
    for (id, map) in &maps.entries {
        b.work(1)?;
        if map.data == Data::Stored {
            continue;
        }
        if map.parent.as_ref().and_then(|p| parents.get(p)) != Some(id) {
            b.bytes(std::mem::size_of::<Update>() + 32)?;
            updates.push(Update::Copy { id: id.clone() });
        }
    }
    for (id, map) in &maps.entries {
        b.work(1)?;
        let Data::Local(entries) = &map.data else {
            continue;
        };
        let Some(parent) = &map.parent else { continue };
        if parents.get(parent) != Some(id) {
            continue;
        }
        b.bytes(std::mem::size_of::<Update>() + 96)?;
        updates.push(Update::Reuse {
            id: id.clone(),
            parent: parent.clone(),
            size: map.size.clone(),
        });
        for (key, value) in entries {
            b.work(1)?;
            b.bytes(std::mem::size_of::<Update>() + 32)?;
            updates.push(match value {
                Some(value) => Update::Put {
                    id: id.clone(),
                    value_type: map.value_type.clone(),
                    key: key.clone(),
                    value: value.clone(),
                },
                None => Update::Delete {
                    id: id.clone(),
                    value_type: map.value_type.clone(),
                    key: key.clone(),
                },
            });
        }
    }
    for id in actual {
        b.bytes(std::mem::size_of::<Update>() + 32)?;
        updates.push(Update::Collect { id });
    }
    Ok(Plan {
        counts,
        counts_are_deltas: is_new,
        garbage,
        all_ids,
        updates,
    })
}
pub fn plan(
    version: Version,
    profile: OrderProfile,
    maps: &Maps,
    store: &Store,
    limits: Limits,
) -> Result<Plan, Error> {
    let mut b = Budget::new(limits);
    maps::reserve_maps(maps, &mut b)?;
    maps::reserve_store(store, &mut b)?;
    planning(version, profile, maps, store, &mut b)
}
fn put(store: &mut Store, key: Vec<u8>, value: Vec<u8>, b: &mut Budget) -> Result<(), Error> {
    b.work(1)?;
    b.bytes(key.len() + value.len() + 64)?;
    store.insert(key, value);
    Ok(())
}
fn adjust(
    store: &mut Store,
    id: &BigUint,
    n: &BigInt,
    add: bool,
    b: &mut Budget,
) -> Result<(), Error> {
    let meta = metadata(id, store)?;
    b.bytes(meta.len())?;
    let mut meta = meta.to_vec();
    let count = if add {
        BigInt::from(BigUint::from_bytes_be(&meta[32..64])) + n
    } else {
        n.clone()
    };
    meta[32..64].copy_from_slice(&Word::wrapping(&count).0);
    put(store, Word::from_unsigned(id).0.to_vec(), meta, b)
}
fn apply(
    version: Version,
    profile: OrderProfile,
    maps: &Maps,
    store: &mut Store,
    b: &mut Budget,
) -> Result<(), Error> {
    let p = planning(version, profile, maps, store, b)?;
    b.bytes(
        p.all_ids
            .len()
            .checked_mul(32)
            .ok_or(Error::ResourceLimit)?,
    )?;
    let ids = p
        .all_ids
        .iter()
        .flat_map(|id| Word::from_unsigned(id).0)
        .collect();
    put(store, vec![2], ids, b)?;
    for update in &p.updates {
        b.work(1)?;
        match update {
            Update::Reuse { id, parent, size } => {
                let old = metadata(parent, store)?;
                b.bytes(old.len())?;
                let mut meta = old.to_vec();
                if version != Version::Sophia1 && version != Version::Solidity1 {
                    meta[64..96].copy_from_slice(&Word::wrapping(size).0);
                }
                let real = maps::real_id(parent, store)?.unsigned();
                if real != *parent {
                    put(store, Word::from_unsigned(parent).0.to_vec(), vec![], b)?;
                }
                put(store, Word::from_unsigned(id).0.to_vec(), meta, b)?;
            }
            Update::Put {
                id,
                value_type,
                key,
                ..
            }
            | Update::Delete {
                id,
                value_type,
                key,
            } => {
                let real = maps::real_id(id, store)?;
                b.bytes(key.len().checked_add(32).ok_or(Error::ResourceLimit)?)?;
                let mut full = real.0.to_vec();
                full.extend_from_slice(key);
                let removed = if corrected(version) {
                    match store.get(&full).filter(|v| !v.is_empty()) {
                        Some(v) => references::used(value_type, v, b)?,
                        None => BTreeSet::new(),
                    }
                } else {
                    BTreeSet::new()
                };
                let value = match update {
                    Update::Put { value, .. } => {
                        b.bytes(value.len())?;
                        value.to_vec()
                    }
                    _ => vec![],
                };
                put(store, full, value, b)?;
                for id in removed {
                    adjust(store, &id, &(-1).into(), true, b)?;
                }
            }
            Update::Copy { id } => {
                let original = maps.get_map(id)?;
                let flat = maps::flatten_inner(id, original, store, b)?;
                let Data::Local(entries) = flat.data else {
                    return Err(Error::InvalidValue);
                };
                if corrected(version) {
                    let copied: Vec<_> = entries
                        .iter()
                        .filter(|(k, _)| match &original.data {
                            Data::Local(d) => !d.contains_key(*k),
                            Data::Stored => true,
                        })
                        .collect();
                    b.bytes(copied.len().checked_mul(32).ok_or(Error::ResourceLimit)?)?;
                    let keys: Vec<_> = copied.iter().map(|(k, _)| k.as_ref()).collect();
                    for i in order::binaries(&keys, b)? {
                        let value = copied[i].1.as_ref().ok_or(Error::NativeFunctionClause)?;
                        for id in references::used(&flat.value_type, value, b)? {
                            adjust(store, &id, &1.into(), true, b)?;
                        }
                    }
                }
                let type_bytes = typerep::encode(&[flat.key_type, flat.value_type], true, b)?;
                b.bytes(96 + type_bytes.len())?;
                let mut meta = Word::from_unsigned(id).0.to_vec();
                meta.extend([0; 32]);
                meta.extend(Word::wrapping(&original.size).0);
                meta.extend(type_bytes);
                put(store, Word::from_unsigned(id).0.to_vec(), meta, b)?;
                for (key, value) in entries {
                    let value = value.ok_or(Error::NativeFunctionClause)?;
                    b.bytes(32 + key.len() + value.len())?;
                    let mut full = Word::from_unsigned(id).0.to_vec();
                    full.extend_from_slice(&key);
                    put(store, full, value.to_vec(), b)?;
                }
            }
            Update::Collect { id } => {
                let real = maps::real_id(id, store)?.unsigned();
                let entries = data(&real, store, b)?;
                put(store, Word::from_unsigned(id).0.to_vec(), vec![], b)?;
                put(store, Word::from_unsigned(&real).0.to_vec(), vec![], b)?;
                for (key, _) in entries {
                    b.bytes(32 + key.len())?;
                    let mut full = Word::from_unsigned(&real).0.to_vec();
                    full.extend(key);
                    put(store, full, vec![], b)?;
                }
            }
        }
    }
    let counts: Vec<_> = p
        .counts
        .iter()
        .filter(|(id, _)| p.all_ids.binary_search(id).is_ok())
        .collect();
    b.bytes(counts.len().checked_mul(64).ok_or(Error::ResourceLimit)?)?;
    let keys: Vec<_> = counts.iter().map(|(id, _)| (*id).clone()).collect();
    for i in order::integers(&keys, profile, corrected(version), b)? {
        let (id, n) = counts[i];
        adjust(store, id, n, corrected(version), b)?;
    }
    Ok(())
}
pub fn publish(
    version: Version,
    profile: OrderProfile,
    heap: &Heap,
    store: &Store,
    limits: Limits,
) -> Result<Store, Error> {
    let mut b = Budget::new(limits);
    maps::reserve_store(store, &mut b)?;
    maps::reserve_maps(&heap.maps, &mut b)?;
    b.bytes(
        heap.memory
            .len()
            .checked_add(32)
            .ok_or(Error::ResourceLimit)?,
    )?;
    let mut bytes = Word::from_unsigned(&heap.pointer).0.to_vec();
    bytes.extend_from_slice(&heap.memory);
    let mut result = store.clone();
    put(&mut result, vec![0], bytes, &mut b)?;
    apply(version, profile, &heap.maps, &mut result, &mut b)?;
    Ok(result)
}
pub fn initialize(
    version: Version,
    profile: OrderProfile,
    bytes: &[u8],
    limits: Limits,
) -> Result<Store, Error> {
    let mut b = Budget::new(limits);
    let root = word(bytes, &BigUint::zero())?;
    let ty = parse_type(bytes, word(bytes, &root)?, Some(root.clone()), &mut b)?;
    let ptr = word(bytes, &(&root + 32u8))?;
    b.bytes(bytes.len())?;
    let mut value = Word::from_unsigned(&ptr).0.to_vec();
    value.extend_from_slice(&bytes[32..]);
    let h = runtime_heap::binary_to_heap_inner(&ty, &value, 0u8.into(), 32u8.into(), &mut b)?;
    let type_bytes = typerep::encode(&[ty], false, &mut b)?;
    b.bytes(h.memory.len().checked_add(32).ok_or(Error::ResourceLimit)?)?;
    let mut value = Word::from_unsigned(&h.pointer).0.to_vec();
    value.extend(h.memory);
    let mut result = Store::new();
    put(&mut result, vec![1], type_bytes, &mut b)?;
    put(&mut result, vec![0], value, &mut b)?;
    apply(version, profile, &h.maps, &mut result, &mut b)?;
    Ok(result)
}
