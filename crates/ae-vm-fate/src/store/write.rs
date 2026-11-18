// aefa_stores_lima / aefa_stores_ceres / aefa_stores, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{Backend, StoreError, Version, map_prefix, read_gas_cost, store_key};
use crate::{
    Budget, Error, Limits, encoding,
    maps::{self, Map, MapError, MapOrder, RefCounts, Traversal},
    value::Value,
};
use num_bigint::{BigInt, Sign};
use num_traits::ToPrimitive;
use std::collections::BTreeMap;

pub type RawMap = BTreeMap<Vec<u8>, Vec<u8>>;
pub type Metadata = BTreeMap<BigInt, MapMetadata>;
pub type GcCache = BTreeMap<BigInt, RawMap>;

// None represents the original planner's undefined placeholder. It must be
// replaced by a copy/update before metadata can be serialized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapMetadata {
    pub raw_id: Option<BigInt>,
    pub refcount: BigInt,
    pub size: Option<BigInt>,
}

// A bounded subtree read must not change caches. None means the requested
// byte cap was exceeded, distinct from an underlying storage failure.
pub trait SubtreeBackend: Backend {
    fn subtree(
        &self,
        prefix: &[u8],
        maximum_bytes: usize,
    ) -> Result<Option<(RawMap, usize)>, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Update {
    Push { position: BigInt, value: Value },
    Copy { id: BigInt, value: Value },
    InPlace { id: BigInt, value: Value },
    Collect { raw_id: BigInt },
}

// This is the write stage of finalization. The caller supplies the historical
// allocation/refcount/reuse/GC plan, with copies before in-place updates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritePlan {
    pub old_metadata: Metadata,
    pub metadata: Metadata,
    pub updates: Vec<Update>,
    pub gc_cache: GcCache,
}
#[derive(Debug, Clone)]
pub struct Written<S> {
    pub store: S,
    pub gas: BigInt,
}

impl WritePlan {
    // All writes are private to a cloned backend until the complete result is
    // returned. Out-of-gas, data errors and resource limits publish nothing.
    pub fn write<S: SubtreeBackend>(
        self,
        version: Version,
        store: &S,
        gas: BigInt,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Written<S>, StoreError<S::Error>> {
        let mut work = Writer {
            version,
            store: store.clone(),
            metadata: self.metadata,
            gas,
            order,
            budget: Budget::new(limits),
            bytes: 0,
        };
        for update in self.updates {
            work.budget.visit(0)?;
            let bytes = work.update(update, &self.old_metadata, &self.gc_cache)?;
            work.charge(bytes * 5)?;
        }
        let mut meta = Map::new();
        for (id, entry) in std::mem::take(&mut work.metadata) {
            work.budget.visit(0)?;
            let raw = entry.raw_id.ok_or(StoreError::InvalidMetadata)?;
            let size = entry.size.ok_or(StoreError::InvalidMetadata)?;
            meta.insert(
                Value::Integer(id),
                Value::Tuple(vec![
                    Value::Integer(raw),
                    Value::Integer(entry.refcount),
                    Value::Integer(size),
                ]),
            );
        }
        let bytes = work.push(0.into(), Value::Map(meta))?;
        work.charge(bytes * 5)?;
        Ok(Written {
            store: work.store,
            gas: work.gas,
        })
    }
}

struct Writer<'a, S, O> {
    version: Version,
    store: S,
    metadata: Metadata,
    gas: BigInt,
    order: &'a O,
    budget: Budget,
    bytes: usize,
}
type BinData = Vec<(Vec<u8>, Option<Vec<u8>>)>;

impl<S: SubtreeBackend, O: MapOrder> Writer<'_, S, O> {
    fn limits(&self) -> Limits {
        Limits {
            max_nodes: self
                .budget
                .limits
                .max_nodes
                .saturating_sub(self.budget.nodes),
            max_bytes: self.budget.limits.max_bytes.saturating_sub(self.bytes),
            ..self.budget.limits
        }
    }
    fn add_bytes(&mut self, bytes: usize) -> Result<(), Error> {
        self.bytes = self.bytes.checked_add(bytes).ok_or(Error::ResourceLimit)?;
        if self.bytes > self.budget.limits.max_bytes {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
    fn encode(&mut self, value: &Value) -> Result<Vec<u8>, Error> {
        let (bytes, nodes) = encoding::encode_counted(value, self.limits())?;
        self.budget.nodes += nodes;
        self.add_bytes(bytes.len())?;
        Ok(bytes)
    }
    fn decode(&mut self, bytes: &[u8]) -> Result<Value, Error> {
        let (value, rest, nodes) = encoding::decode_one_counted(bytes, self.limits())?;
        if !rest.is_empty() {
            return Err(Error::TrailingBytes);
        }
        self.budget.nodes += nodes;
        self.add_bytes(bytes.len())?;
        Ok(value)
    }
    fn charge(&mut self, cost: BigInt) -> Result<(), StoreError<S::Error>> {
        self.gas -= cost;
        if self.gas.sign() == Sign::Minus {
            Err(StoreError::OutOfGas)
        } else {
            Ok(())
        }
    }
    fn read_charge(&mut self, bytes: usize) -> Result<(), StoreError<S::Error>> {
        if self.version == Version::Arcus {
            self.charge(read_gas_cost(bytes))?;
        }
        Ok(())
    }
    fn subtree(&mut self, raw_id: &BigInt) -> Result<RawMap, StoreError<S::Error>> {
        let operational = self.limits().max_bytes;
        let gas_cap = if self.version == Version::Arcus {
            if self.gas < BigInt::from(100) {
                return Err(StoreError::OutOfGas);
            }
            ((&self.gas - BigInt::from(100)) / BigInt::from(10))
                .to_usize()
                .unwrap_or(usize::MAX)
        } else {
            usize::MAX
        };
        let cap = operational.min(gas_cap);
        let Some((map, bytes)) = self
            .store
            .subtree(&map_prefix(raw_id), cap)
            .map_err(StoreError::Backend)?
        else {
            return Err(
                if self.version == Version::Arcus && gas_cap <= operational {
                    StoreError::OutOfGas
                } else {
                    Error::ResourceLimit.into()
                },
            );
        };
        self.read_charge(bytes)?;
        self.add_bytes(bytes)?;
        Ok(map)
    }
    fn cache_data(&mut self, cache: Map) -> Result<BinData, StoreError<S::Error>> {
        let traversal = if self.version == Version::Lima {
            Traversal::ToList
        } else {
            Traversal::TermSorted
        };
        let entries = maps::ordered::<std::convert::Infallible>(
            cache,
            self.order,
            traversal,
            &mut self.budget,
        )
        .map_err(|e| match e {
            MapError::Data(e) => StoreError::Data(e),
            MapError::InvalidOrder => StoreError::InvalidOrder,
            MapError::Lookup(never) => match never {},
        })?;
        entries
            .into_iter()
            .map(|(key, value)| {
                let key = self.encode(&key)?;
                let value = if value == Value::Tombstone {
                    None
                } else {
                    Some(self.encode(&value)?)
                };
                Ok((key, value))
            })
            .collect()
    }
    fn write_data(
        &mut self,
        raw_id: &BigInt,
        data: BinData,
    ) -> Result<BigInt, StoreError<S::Error>> {
        let mut bytes = BigInt::from(0);
        for (key, value) in data {
            self.budget.visit(0)?;
            let value = if let Some(value) = value {
                bytes += BigInt::from(key.len()) + BigInt::from(value.len());
                value
            } else {
                Vec::new()
            };
            self.store.put(raw_key(raw_id, &key), value);
        }
        Ok(bytes)
    }
    fn push(&mut self, position: BigInt, value: Value) -> Result<BigInt, StoreError<S::Error>> {
        let value = self.encode(&value)?;
        if position.sign() == Sign::Minus {
            return Err(Error::InvalidValue.into());
        }
        let key_bytes = position.magnitude().bits().div_ceil(8).max(1) + 1;
        if key_bytes > self.limits().max_bytes as u64 {
            return Err(Error::ResourceLimit.into());
        }
        let key = store_key(&position)?;
        self.add_bytes(key.len())?;
        let bytes = BigInt::from(key.len()) + BigInt::from(value.len());
        self.store.put(key, value);
        Ok(bytes)
    }
    fn update(
        &mut self,
        update: Update,
        old_meta: &Metadata,
        gc_cache: &GcCache,
    ) -> Result<BigInt, StoreError<S::Error>> {
        match update {
            Update::Push { position, value } => self.push(position, value),
            Update::Copy { id, value } => {
                let refcount = metadata(&self.metadata, &id)?.refcount.clone();
                let (size, data) = match value {
                    Value::Map(map) => (BigInt::from(map.len()), self.cache_data(map)?),
                    Value::StoreMap { id: old_id, cache } => {
                        let old = metadata(old_meta, &old_id)?;
                        let raw = old.raw_id.as_ref().ok_or(StoreError::InvalidMetadata)?;
                        let old_size = old.size.as_ref().ok_or(StoreError::InvalidMetadata)?;
                        let stored = self.subtree(raw)?;
                        let data = self.cache_data(cache)?;
                        let mut size = old_size.clone();
                        for (key, value) in &data {
                            size += size_change(stored.contains_key(key), value.is_none());
                        }
                        // Old binary keys are unique. Their write order cannot
                        // affect gas or the result; all precede cache entries.
                        let all = stored
                            .into_iter()
                            .map(|(k, v)| (k, Some(v)))
                            .chain(data)
                            .collect();
                        (size, all)
                    }
                    _ => return Err(Error::InvalidValue.into()),
                };
                self.metadata.insert(
                    id.clone(),
                    MapMetadata {
                        raw_id: Some(id.clone()),
                        refcount,
                        size: Some(size),
                    },
                );
                let bytes = self.write_data(&id, data)?;
                // The map prefix marker is deliberately not charged as data.
                self.store.put(map_prefix(&id).to_vec(), vec![0]);
                Ok(bytes)
            }
            Update::InPlace { id, value } => {
                let Value::StoreMap { id: old_id, cache } = value else {
                    return Err(Error::InvalidValue.into());
                };
                let refcount = metadata(&self.metadata, &id)?.refcount.clone();
                let old = metadata(&self.metadata, &old_id)?;
                let raw = old.raw_id.clone().ok_or(StoreError::InvalidMetadata)?;
                let mut size = old.size.clone().ok_or(StoreError::InvalidMetadata)?;
                let data = self.cache_data(cache)?;
                for (key, value) in &data {
                    let bytes = self
                        .store
                        .get(&raw_key(&raw, key))
                        .map_err(StoreError::Backend)?;
                    self.read_charge(bytes.len())?;
                    self.add_bytes(bytes.len())?;
                    size += size_change(!bytes.is_empty(), value.is_none());
                }
                // The original performs these refcount reads against the old
                // store, using the *unprefixed* serialized key. Preserve it.
                let old_store = self.store.clone();
                let keys: Vec<_> = data.iter().map(|(key, _)| key.clone()).collect();
                let bytes = self.write_data(&raw, data)?;
                self.metadata.remove(&old_id);
                self.metadata.insert(
                    id,
                    MapMetadata {
                        raw_id: Some(raw),
                        refcount,
                        size: Some(size),
                    },
                );
                let mut removed = RefCounts::new();
                for key in keys {
                    let bytes = old_store.get(&key).map_err(StoreError::Backend)?;
                    self.read_charge(bytes.len())?;
                    if !bytes.is_empty() {
                        let value = self.decode(&bytes)?;
                        maps::count(&value, &mut removed, &mut self.budget, 0)?;
                    }
                }
                for (id, count) in removed {
                    self.metadata
                        .entry(id)
                        .or_insert_with(|| MapMetadata {
                            raw_id: None,
                            refcount: 0.into(),
                            size: None,
                        })
                        .refcount -= count;
                }
                Ok(bytes)
            }
            Update::Collect { raw_id } => {
                let stored;
                let data = if self.version == Version::Arcus {
                    match gc_cache.get(&raw_id) {
                        Some(data) => data,
                        None => {
                            stored = self.subtree(&raw_id)?;
                            &stored
                        }
                    }
                } else {
                    stored = self.subtree(&raw_id)?;
                    &stored
                };
                self.store.put(map_prefix(&raw_id).to_vec(), vec![]);
                for key in data.keys() {
                    self.budget.visit(0)?;
                    self.add_bytes(key.len().checked_add(5).ok_or(Error::ResourceLimit)?)?;
                    self.store.put(raw_key(&raw_id, key), vec![]);
                }
                Ok(0.into())
            }
        }
    }
}
fn metadata<'a, E>(meta: &'a Metadata, id: &BigInt) -> Result<&'a MapMetadata, StoreError<E>> {
    meta.get(id).ok_or(StoreError::MissingMap)
}
fn raw_key(id: &BigInt, key: &[u8]) -> Vec<u8> {
    [map_prefix(id).as_slice(), key].concat()
}
fn size_change(present: bool, deleted: bool) -> i32 {
    match (present, deleted) {
        (true, true) => -1,
        (false, false) => 1,
        _ => 0,
    }
}
