// Historical store planning in aefa_stores_lima / ceres / aefa_stores, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::write::{
    GcCache, MapMetadata, Metadata, RawMap, SubtreeBackend, Update, WritePlan, Writer, Written,
};
use super::{StoreError, Version, map_data_key, store_key};
use crate::{
    Budget, Error, Limits,
    maps::{self, Map, MapError, MapOrder, RefCounts, Traversal},
    value::Value,
};
use num_bigint::BigInt;
use num_traits::Zero;
use std::collections::{BTreeMap, BTreeSet};

type IdSet = BTreeSet<BigInt>;
type Reuse = BTreeMap<BigInt, BigInt>;
type Allocated = BTreeMap<BigInt, Value>;
pub type Terms = BTreeMap<BigInt, (Value, bool)>;
pub const REUSE_ITERATIONS: usize = 100;

#[derive(Debug, Clone)]
pub struct Planned {
    pub plan: WritePlan,
    pub gas: BigInt,
    pub remaining_limits: Limits,
}
impl Planned {
    pub fn write<S: SubtreeBackend>(
        self,
        version: Version,
        store: &S,
        order: &impl MapOrder,
    ) -> Result<Written<S>, StoreError<S::Error>> {
        self.plan
            .write(version, store, self.gas, order, self.remaining_limits)
    }
}

// Plans from typed integer metadata and the complete register cache. Dirty
// filtering must happen after Lima's map traversal, including clean registers.
pub fn plan<S: SubtreeBackend>(
    version: Version,
    metadata: Metadata,
    terms: Terms,
    store: &S,
    gas: BigInt,
    order: &impl MapOrder,
    limits: Limits,
) -> Result<Planned, StoreError<S::Error>> {
    let mut work = Writer {
        version,
        store: store.clone(),
        metadata,
        gas,
        order,
        budget: Budget::new(limits),
        bytes: 0,
    };
    let old_metadata = std::mem::take(&mut work.metadata);
    work.metadata = clone_meta(&old_metadata, &mut work.budget, &mut work.bytes)?;
    let mut used = IdSet::new();
    for (id, meta) in &work.metadata {
        work.budget.visit(0)?;
        used.insert(id.clone());
        used.insert(meta.raw_id.clone().ok_or(StoreError::InvalidMetadata)?);
    }
    let positions = if version == Version::Lima {
        work.id_order(terms.keys(), Traversal::ToList)?
    } else {
        work.id_order(terms.keys(), Traversal::TermSorted)?
    };
    let mut terms = terms;
    let mut registers = vec![];
    let mut values = vec![];
    for position in positions {
        work.budget.visit(0)?;
        let (value, dirty) = terms.remove(&position).unwrap();
        if position > BigInt::zero() && dirty {
            registers.push(position);
            values.push(value);
        }
    }
    let allocation = maps::allocate_budgeted(
        used.into_iter().collect(),
        values,
        order,
        &mut work.budget,
        &mut work.bytes,
    )
    .map_err(map_error)?;
    let registers: Vec<_> = registers.into_iter().zip(allocation.values).collect();
    let allocated = allocation.maps;
    let mut counts = RefCounts::new();
    for (reg, value) in &registers {
        let mut new = work.count(value)?;
        let bytes = work
            .store
            .get(&store_key(reg)?)
            .map_err(StoreError::Backend)?;
        if !bytes.is_empty() {
            let old = work.decode(&bytes)?;
            new = maps::refcount_diff(new, work.count(&old)?);
        }
        counts = maps::refcount_union(counts, new);
    }
    for value in allocated.values() {
        let map_counts = match value {
            Value::Map(_) => work.count(value)?,
            Value::StoreMap { cache, .. } => {
                let mut out = RefCounts::new();
                for v in cache.values() {
                    maps::count(v, &mut out, &mut work.budget, 0)?;
                }
                out
            }
            _ => return Err(Error::InvalidValue.into()),
        };
        counts = maps::refcount_union(counts, map_counts);
    }
    let initial = update_counts(std::mem::take(&mut work.metadata), counts, &mut work.budget)?;
    let reuse = work.reuse(&allocated, &initial)?;
    let (garbage, metadata, gc_cache) = work.garbage(reuse.unused, &reuse.reuse, reuse.metadata)?;
    let mut updates: Vec<_> = registers
        .into_iter()
        .map(|(position, value)| Update::Push { position, value })
        .collect();
    let ids = if version == Version::Lima {
        work.id_order(allocated.keys(), Traversal::ToList)?
    } else {
        work.id_order(allocated.keys(), Traversal::TermSorted)?
    };
    let mut allocated = allocated;
    let mut in_place = vec![];
    for id in ids {
        let value = allocated.remove(&id).unwrap();
        if matches!(&value,Value::StoreMap{id:old_id,..}if reuse.reuse.get(old_id)==Some(&id)) {
            in_place.push(Update::InPlace { id, value });
        } else {
            updates.push(Update::Copy { id, value });
        }
    }
    updates.extend(in_place);
    updates.extend(garbage.into_iter().map(|raw_id| Update::Collect { raw_id }));
    Ok(Planned {
        plan: WritePlan {
            old_metadata,
            metadata,
            updates,
            gc_cache,
        },
        gas: work.gas.clone(),
        remaining_limits: work.limits(),
    })
}

struct ReuseResult {
    unused: IdSet,
    reuse: Reuse,
    metadata: Metadata,
}
enum Pass {
    Converged(ReuseResult),
    Exhausted(IdSet),
}
impl<S: SubtreeBackend, O: MapOrder> Writer<'_, S, O> {
    fn indices(
        &mut self,
        keys: &[&Value],
        traversal: Traversal,
    ) -> Result<Vec<usize>, StoreError<S::Error>> {
        maps::ordered_indices(keys, self.order, traversal, &mut self.budget).map_err(map_error)
    }
    fn id_order<'a>(
        &mut self,
        ids: impl Iterator<Item = &'a BigInt>,
        traversal: Traversal,
    ) -> Result<Vec<BigInt>, StoreError<S::Error>> {
        let mut keys = vec![];
        for id in ids {
            self.budget.visit(0)?;
            let bytes = usize::try_from(id.magnitude().bits().div_ceil(8))
                .map_err(|_| Error::ResourceLimit)?;
            self.add_bytes(bytes)?;
            keys.push(Value::Integer(id.clone()));
        }
        let indices = self.indices(&keys.iter().collect::<Vec<_>>(), traversal)?;
        Ok(indices
            .into_iter()
            .map(|i| {
                let Value::Integer(id) = &keys[i] else {
                    unreachable!()
                };
                id.clone()
            })
            .collect())
    }
    fn count(&mut self, value: &Value) -> Result<RefCounts, Error> {
        let mut counts = RefCounts::new();
        maps::count(value, &mut counts, &mut self.budget, 0)?;
        Ok(counts)
    }
    fn raw_values(
        &mut self,
        raw: &RawMap,
        without: Option<&BTreeSet<Vec<u8>>>,
    ) -> Result<RefCounts, StoreError<S::Error>> {
        let mut entries = vec![];
        for (k, v) in raw {
            self.budget.visit(0)?;
            if without.is_none_or(|set| !set.contains(k)) {
                entries.push((k, v));
            }
        }
        let mut keys = vec![];
        for (key, _) in &entries {
            self.budget.visit(0)?;
            self.add_bytes(key.len())?;
            keys.push(Value::String((*key).clone()));
        }
        let order = self.indices(&keys.iter().collect::<Vec<_>>(), Traversal::ToList)?;
        let mut values = vec![];
        for i in order {
            values.push(self.decode(entries[i].1)?);
        }
        Ok(self.count(&Value::List(values))?)
    }
    fn removed(&mut self, cache: &Map, raw: &BigInt) -> Result<RefCounts, StoreError<S::Error>> {
        let keys: Vec<_> = cache.keys().collect();
        let order = self.indices(&keys, Traversal::ToList)?;
        let mut values = vec![];
        for i in order {
            let key = map_data_key(raw, keys[i], self.limits())?;
            self.add_bytes(key.len())?;
            let bytes = self.store.get(&key).map_err(StoreError::Backend)?;
            self.read_charge(bytes.len())?;
            if !bytes.is_empty() {
                values.push(self.decode(&bytes)?);
            }
        }
        if self.version == Version::Arcus {
            values.reverse();
        }
        Ok(self.count(&Value::List(values))?)
    }
    fn inplace(&mut self, unused: &IdSet, maps: &Allocated) -> Result<Reuse, StoreError<S::Error>> {
        let mut reuse = Reuse::new();
        for id in self.id_order(maps.keys(), Traversal::Fold)? {
            if let Value::StoreMap { id: old, .. } = &maps[&id]
                && unused.contains(old)
            {
                reuse.insert(old.clone(), id);
            }
        }
        Ok(reuse)
    }
    fn adjustments(
        &mut self,
        meta: &Metadata,
        reuse: &Reuse,
        maps: &Allocated,
        only_reuse: bool,
        subtrees: &mut GcCache,
    ) -> Result<RefCounts, StoreError<S::Error>> {
        let mut counts = RefCounts::new();
        for id in self.id_order(maps.keys(), Traversal::Fold)? {
            let Value::StoreMap { id: old, cache } = &maps[&id] else {
                continue;
            };
            let reused = reuse.get(old) == Some(&id);
            if reused {
                if self.version != Version::Lima {
                    counts = maps::refcount_diff(counts, self.removed(cache, raw_id(meta, old)?)?);
                }
            } else if !only_reuse {
                let raw = raw_id(meta, old)?;
                let keys: Vec<_> = cache.keys().collect();
                let key_order = self.indices(&keys, Traversal::ToList)?;
                let mut overwritten = BTreeSet::new();
                for i in key_order {
                    overwritten.insert(self.encode(keys[i])?);
                }
                // Ceres and Arcus retain the subtree across fixpoint rounds.
                let stored;
                let full = if self.version == Version::Lima {
                    stored = self.subtree(raw)?;
                    &stored
                } else {
                    if !subtrees.contains_key(raw) {
                        subtrees.insert(raw.clone(), self.subtree(raw)?);
                    }
                    &subtrees[raw]
                };
                counts = maps::refcount_union(counts, self.raw_values(full, Some(&overwritten))?);
            }
        }
        Ok(counts)
    }
    fn reuse_loop(
        &mut self,
        mut unused: IdSet,
        maps: &Allocated,
        meta: &Metadata,
        only_reuse: bool,
        subtrees: &mut GcCache,
    ) -> Result<Pass, StoreError<S::Error>> {
        for _ in 0..REUSE_ITERATIONS {
            self.budget.visit(0)?;
            let reuse = self.inplace(&unused, maps)?;
            let adjustments = self.adjustments(meta, &reuse, maps, only_reuse, subtrees)?;
            // Each round applies its deltas to the original baseline metadata.
            let metadata = update_counts(
                clone_meta(meta, &mut self.budget, &mut self.bytes)?,
                adjustments,
                &mut self.budget,
            )?;
            let next = unused_maps(&metadata, &mut self.budget)?;
            if next == unused {
                return Ok(Pass::Converged(ReuseResult {
                    unused,
                    reuse,
                    metadata,
                }));
            }
            unused = next;
        }
        Ok(Pass::Exhausted(unused))
    }
    fn reuse(
        &mut self,
        maps: &Allocated,
        meta: &Metadata,
    ) -> Result<ReuseResult, StoreError<S::Error>> {
        let unused = unused_maps(meta, &mut self.budget)?;
        if self.version == Version::Lima {
            let reuse = self.inplace(&unused, maps)?;
            let delta = self.adjustments(meta, &reuse, maps, false, &mut GcCache::new())?;
            return Ok(ReuseResult {
                unused,
                reuse,
                metadata: update_counts(
                    clone_meta(meta, &mut self.budget, &mut self.bytes)?,
                    delta,
                    &mut self.budget,
                )?,
            });
        }
        let optimistic = self.reuse_loop(unused.clone(), maps, meta, true, &mut GcCache::new())?;
        let optimistic = match optimistic {
            Pass::Converged(out) => Some(out),
            Pass::Exhausted(u) if self.version == Version::Ceres => Some(ReuseResult {
                reuse: self.inplace(&u, maps)?,
                unused: u,
                metadata: clone_meta(meta, &mut self.budget, &mut self.bytes)?,
            }),
            Pass::Exhausted(_) => None,
        };
        if let Some(out) = optimistic {
            let dirty = maps
                .values()
                .filter(|v| matches!(v, Value::StoreMap { .. }))
                .count();
            if out.reuse.len() == dirty {
                let delta = self.adjustments(meta, &out.reuse, maps, false, &mut GcCache::new())?;
                return Ok(ReuseResult {
                    metadata: update_counts(
                        clone_meta(meta, &mut self.budget, &mut self.bytes)?,
                        delta,
                        &mut self.budget,
                    )?,
                    ..out
                });
            }
        }
        // Arcus carries gas already spent by the optimistic pass into this one.
        let mut subtrees = GcCache::new();
        match self.reuse_loop(unused, maps, meta, false, &mut subtrees)? {
            Pass::Converged(out) => Ok(out),
            Pass::Exhausted(_) if self.version == Version::Arcus => Err(StoreError::OutOfGas),
            Pass::Exhausted(unused) => {
                let reuse = self.inplace(&unused, maps)?;
                let delta = self.adjustments(meta, &reuse, maps, false, &mut subtrees)?;
                Ok(ReuseResult {
                    unused,
                    reuse,
                    metadata: update_counts(
                        clone_meta(meta, &mut self.budget, &mut self.bytes)?,
                        delta,
                        &mut self.budget,
                    )?,
                })
            }
        }
    }
    fn garbage(
        &mut self,
        mut unused: IdSet,
        reuse: &Reuse,
        mut meta: Metadata,
    ) -> Result<(Vec<BigInt>, Metadata, GcCache), StoreError<S::Error>> {
        let mut garbage = vec![];
        let mut gc_cache = GcCache::new();
        loop {
            self.budget.visit(0)?;
            let ids: Vec<_> = self
                .id_order(unused.iter(), Traversal::ToList)?
                .into_iter()
                .filter(|id| !reuse.contains_key(id))
                .collect();
            if ids.is_empty() {
                return Ok((garbage, meta, gc_cache));
            }
            let mut removed = RefCounts::new();
            for id in &ids {
                let raw = raw_id(&meta, id)?.clone();
                let data = self.subtree(&raw)?;
                removed = maps::refcount_union(removed, self.raw_values(&data, None)?);
                if self.version == Version::Arcus {
                    gc_cache.insert(raw.clone(), data);
                }
                garbage.push(raw);
            }
            meta = update_counts(
                meta,
                maps::refcount_diff(RefCounts::new(), removed),
                &mut self.budget,
            )?;
            for id in ids {
                meta.remove(&id);
            }
            unused = unused_maps(&meta, &mut self.budget)?;
        }
    }
}
fn update_counts(
    mut metadata: Metadata,
    counts: RefCounts,
    budget: &mut Budget,
) -> Result<Metadata, Error> {
    for (id, delta) in counts {
        budget.visit(0)?;
        metadata
            .entry(id)
            .or_insert_with(|| MapMetadata {
                raw_id: None,
                refcount: 0.into(),
                size: None,
            })
            .refcount += delta;
    }
    Ok(metadata)
}
fn unused_maps(meta: &Metadata, budget: &mut Budget) -> Result<IdSet, Error> {
    let mut unused = IdSet::new();
    for (id, m) in meta {
        budget.visit(0)?;
        if m.refcount.is_zero() {
            unused.insert(id.clone());
        }
    }
    Ok(unused)
}
fn clone_meta(meta: &Metadata, budget: &mut Budget, bytes: &mut usize) -> Result<Metadata, Error> {
    for (id, m) in meta {
        budget.visit(0)?;
        for n in [
            Some(id),
            m.raw_id.as_ref(),
            Some(&m.refcount),
            m.size.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            let size = usize::try_from(n.magnitude().bits().div_ceil(8))
                .map_err(|_| Error::ResourceLimit)?;
            *bytes = bytes.checked_add(size).ok_or(Error::ResourceLimit)?;
            if *bytes > budget.limits.max_bytes {
                return Err(Error::ResourceLimit);
            }
        }
    }
    Ok(meta.clone())
}
fn raw_id<'a, E>(meta: &'a Metadata, id: &BigInt) -> Result<&'a BigInt, StoreError<E>> {
    meta.get(id)
        .ok_or(StoreError::MissingMap)?
        .raw_id
        .as_ref()
        .ok_or(StoreError::InvalidMetadata)
}
fn map_error<E>(error: MapError) -> StoreError<E> {
    match error {
        MapError::Data(e) => StoreError::Data(e),
        MapError::InvalidOrder => StoreError::InvalidOrder,
        MapError::Lookup(never) => match never {},
    }
}
