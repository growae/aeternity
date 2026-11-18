// Historical aefa_stores_lima / aefa_stores_ceres / aefa_stores, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{Budget, Error, Limits, encoding, value::Value};
use num_bigint::{BigInt, Sign};
use num_traits::Zero;
use std::collections::BTreeMap;

pub mod plan;
pub mod read;
pub mod write;

// Backends keep their byte-read cache separate from FATE's decoded term cache.
// Cloning must isolate cache changes; reads must not publish persistent writes.
pub trait Backend: Clone {
    type Error;
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, Self::Error>;
    fn get_cached(&mut self, key: &[u8]) -> Result<Vec<u8>, Self::Error>;
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    Lima,
    Ceres,
    Arcus,
}
impl Version {
    pub fn for_protocol(protocol: u32) -> Self {
        match protocol {
            7.. => Self::Arcus,
            5.. => Self::Ceres,
            _ => Self::Lima,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError<E> {
    Backend(E),
    Data(Error),
    InvalidPosition,
    MissingContract,
    MissingMetadata,
    InvalidMetadata,
    InvalidOrder,
    MissingMap,
    OutOfGas,
}
impl<E> From<Error> for StoreError<E> {
    fn from(e: Error) -> Self {
        Self::Data(e)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Gas {
    Unmetered,
    Metered(BigInt),
}
impl Gas {
    fn charge<E>(self, cost: BigInt) -> Result<Self, StoreError<E>> {
        match self {
            Self::Unmetered => Ok(Self::Unmetered),
            Self::Metered(gas) => {
                let remaining = gas - cost;
                if remaining.sign() == Sign::Minus {
                    Err(StoreError::OutOfGas)
                } else {
                    Ok(Self::Metered(remaining))
                }
            }
        }
    }
}
pub fn read_gas_cost(bytes: usize) -> BigInt {
    BigInt::from(100) + BigInt::from(bytes) * 10
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterRead {
    Missing,
    Cached(Value),
    Loaded {
        value: Value,
        bytes: usize,
        gas: Gas,
    },
}
#[derive(Debug, Clone)]
pub struct Entry<S> {
    store: S,
    dirty: bool,
    terms: BTreeMap<BigInt, (Value, bool)>,
}
impl<S> Entry<S> {
    pub fn backend(&self) -> &S {
        &self.store
    }
    pub fn dirty(&self) -> bool {
        self.dirty
    }
    pub fn terms(&self) -> &BTreeMap<BigInt, (Value, bool)> {
        &self.terms
    }
}
#[derive(Debug, Clone)]
pub struct Stores<S> {
    version: Version,
    contracts: BTreeMap<[u8; 32], Entry<S>>,
}

#[derive(Debug, Clone)]
pub struct Finalized<S> {
    // Same reverse-fold order as the original finalize_stores publication list.
    pub stores: Vec<([u8; 32], S)>,
    pub gas: BigInt,
    pub remaining_limits: Limits,
}

impl<S: write::SubtreeBackend> Stores<S> {
    // Consumes VM-local caches. The caller retains its VM checkpoint for an
    // operational retry; no backend writes are published on any error.
    pub fn finalize(
        self,
        mut gas: BigInt,
        order: &impl crate::maps::MapOrder,
        limits: Limits,
    ) -> Result<Finalized<S>, StoreError<S::Error>> {
        use crate::maps::{self, MapError, Traversal};
        let mut budget = Budget::new(limits);
        let key_bytes = self
            .contracts
            .len()
            .checked_mul(32)
            .ok_or(Error::ResourceLimit)?;
        if key_bytes > limits.max_bytes || self.contracts.len() > limits.max_nodes {
            return Err(Error::ResourceLimit.into());
        }
        let keys: Vec<_> = self
            .contracts
            .keys()
            .map(|k| Value::String(k.to_vec()))
            .collect();
        let indices = maps::ordered_indices::<std::convert::Infallible>(
            &keys.iter().collect::<Vec<_>>(),
            order,
            Traversal::Fold,
            &mut budget,
        )
        .map_err(|e| match e {
            MapError::Data(e) => StoreError::Data(e),
            MapError::InvalidOrder => StoreError::InvalidOrder,
            MapError::Lookup(never) => match never {},
        })?;
        let mut remaining = Limits {
            max_nodes: limits.max_nodes - budget.nodes,
            max_bytes: limits.max_bytes - key_bytes,
            ..limits
        };
        let mut entries: Vec<_> = self.contracts.into_iter().map(Some).collect();
        let mut stores = Vec::new();
        for i in indices {
            let (key, mut entry) = entries[i].take().unwrap();
            if !entry.dirty {
                continue;
            }
            let decoded;
            let metadata = if let Some((v, _)) = entry.terms.get(&BigInt::zero()) {
                v
            } else {
                // Last metadata access; original find_meta_data_no_cache drops
                // both the decoded and byte-cache changes from this lookup.
                let mut reader = entry.store.clone();
                let bytes = read_bytes(&mut reader, self.version, &[0, 0])?;
                if bytes.is_empty() {
                    return Err(StoreError::MissingMetadata);
                }
                let (v, rest, nodes) = encoding::decode_one_counted(&bytes, remaining)?;
                if !rest.is_empty() {
                    return Err(Error::TrailingBytes.into());
                }
                remaining.max_nodes -= nodes;
                remaining.max_bytes -= bytes.len();
                decoded = v;
                &decoded
            };
            let (metadata, next_limits) = integer_metadata(metadata, remaining)?;
            let planned = plan::plan(
                self.version,
                metadata,
                std::mem::take(&mut entry.terms),
                &entry.store,
                gas,
                order,
                next_limits,
            )?;
            let written = planned.write(self.version, &entry.store, order)?;
            gas = written.gas;
            remaining = written.remaining_limits;
            stores.push((key, written.store));
        }
        stores.reverse();
        Ok(Finalized {
            stores,
            gas,
            remaining_limits: remaining,
        })
    }
}

// Canonical store metadata has integer IDs and three integer fields. Runtime
// planner placeholders are introduced after this boundary, never decoded here.
fn integer_metadata<E>(
    value: &Value,
    mut limits: Limits,
) -> Result<(write::Metadata, Limits), StoreError<E>> {
    let Value::Map(map) = value else {
        return Err(StoreError::InvalidMetadata);
    };
    let mut meta = write::Metadata::new();
    for (id, value) in map {
        if limits.max_nodes < 5 {
            return Err(Error::ResourceLimit.into());
        }
        limits.max_nodes -= 5;
        let (Value::Integer(id), Value::Tuple(fields)) = (id, value) else {
            return Err(StoreError::InvalidMetadata);
        };
        let [
            Value::Integer(raw),
            Value::Integer(count),
            Value::Integer(size),
        ] = fields.as_slice()
        else {
            return Err(StoreError::InvalidMetadata);
        };
        for field in [id, raw, count, size] {
            let bytes = usize::try_from(field.magnitude().bits().div_ceil(8))
                .map_err(|_| Error::ResourceLimit)?;
            limits.max_bytes = limits
                .max_bytes
                .checked_sub(bytes)
                .ok_or(Error::ResourceLimit)?;
        }
        meta.insert(
            id.clone(),
            write::MapMetadata {
                raw_id: Some(raw.clone()),
                refcount: count.clone(),
                size: Some(size.clone()),
            },
        );
    }
    Ok((meta, limits))
}
impl<S: Backend> Stores<S> {
    pub fn new(version: Version) -> Self {
        Self {
            version,
            contracts: BTreeMap::new(),
        }
    }
    pub fn put_contract(&mut self, key: [u8; 32], store: S) {
        self.contracts.insert(
            key,
            Entry {
                store,
                dirty: false,
                terms: BTreeMap::new(),
            },
        );
    }
    pub fn has_contract(&self, key: &[u8; 32]) -> bool {
        self.contracts.contains_key(key)
    }
    pub fn entry(&self, key: &[u8; 32]) -> Option<&Entry<S>> {
        self.contracts.get(key)
    }
    pub fn put_value(
        &mut self,
        key: &[u8; 32],
        position: BigInt,
        value: Value,
    ) -> Result<(), StoreError<S::Error>> {
        let entry = self
            .contracts
            .get_mut(key)
            .ok_or(StoreError::MissingContract)?;
        // The original write helper does not apply the read position guard.
        entry.terms.insert(position, (value, true));
        entry.dirty = true;
        Ok(())
    }
    fn staged<T>(
        &mut self,
        key: &[u8; 32],
        f: impl FnOnce(&mut Entry<S>, Version) -> Result<T, StoreError<S::Error>>,
    ) -> Result<T, StoreError<S::Error>> {
        let mut entry = self
            .contracts
            .get(key)
            .ok_or(StoreError::MissingContract)?
            .clone();
        let result = f(&mut entry, self.version)?;
        self.contracts.insert(*key, entry);
        Ok(result)
    }
    // Lower-level storage helper: pre-Arcus gas is charged by the VM caller.
    pub fn find_value(
        &mut self,
        key: &[u8; 32],
        position: &BigInt,
        gas: Gas,
        limits: Limits,
    ) -> Result<RegisterRead, StoreError<S::Error>> {
        valid_position(position)?;
        self.staged(key, |entry, version| {
            find_term(entry, version, position, gas, limits)
        })
    }
    // aefa_fate:lookup_in_store/2's successful-read pricing and charge order.
    pub fn load_register(
        &mut self,
        key: &[u8; 32],
        position: &BigInt,
        gas: Gas,
        limits: Limits,
    ) -> Result<RegisterRead, StoreError<S::Error>> {
        self.load_register_with_floor(key, position, gas, None, limits)
    }
    // Dry-run's activated-protocol floor applies only to an Arcus register
    // loaded from storage. Cached terms and map reads do not receive it.
    pub fn load_register_with_floor(
        &mut self,
        key: &[u8; 32],
        position: &BigInt,
        gas: Gas,
        metering_floor: Option<u32>,
        limits: Limits,
    ) -> Result<RegisterRead, StoreError<S::Error>> {
        valid_position(position)?;
        self.staged(key, |entry, version| {
            let result = find_term(entry, version, position, gas, limits)?;
            match result {
                RegisterRead::Loaded { value, bytes, gas } if version != Version::Arcus => {
                    Ok(RegisterRead::Loaded {
                        value,
                        bytes,
                        gas: gas.charge(if version == Version::Ceres { 2000 } else { 0 }.into())?,
                    })
                }
                RegisterRead::Loaded { value, bytes, gas } => {
                    let extra =
                        crate::gas::store_read_floor_cost(metering_floor, read_gas_cost(bytes));
                    let gas = if extra > BigInt::zero() {
                        gas.charge(extra)?
                    } else {
                        gas
                    };
                    Ok(RegisterRead::Loaded { value, bytes, gas })
                }
                other => Ok(other),
            }
        })
    }
    pub fn cache_metadata(
        &mut self,
        key: &[u8; 32],
        limits: Limits,
    ) -> Result<(), StoreError<S::Error>> {
        self.staged(key, |entry, version| {
            find_term(entry, version, &BigInt::zero(), Gas::Unmetered, limits)?;
            Ok(())
        })
    }
    pub fn map_size(
        &mut self,
        key: &[u8; 32],
        id: &BigInt,
        limits: Limits,
    ) -> Result<Value, StoreError<S::Error>> {
        self.staged(key, |entry, version| {
            Ok(map_metadata(entry, version, id, limits)?[2].clone())
        })
    }
    pub fn map_lookup(
        &mut self,
        key: &[u8; 32],
        id: &BigInt,
        map_key: &Value,
        gas: Gas,
        limits: Limits,
    ) -> Result<(Option<Value>, Gas), StoreError<S::Error>> {
        self.staged(key, |entry, version| {
            // Capture this before metadata lookup: the original replaces the
            // byte cache with this snapshot on a successful map read.
            let mut store = entry.store.clone();
            let meta = map_metadata(entry, version, id, limits)?;
            let Value::Integer(raw_id) = &meta[0] else {
                return Err(StoreError::InvalidMetadata);
            };
            let data_key = map_data_key(raw_id, map_key, limits)?;
            let bytes = read_bytes(&mut store, version, &data_key)?;
            let gas = charge_read(version, gas, bytes.len())?;
            if bytes.is_empty() {
                return Ok((None, gas));
            }
            let value = encoding::decode(&bytes, limits)?;
            if version != Version::Lima {
                entry.store = store;
            }
            Ok((Some(value), gas))
        })
    }
    pub fn map_member(
        &mut self,
        key: &[u8; 32],
        id: &BigInt,
        map_key: &Value,
        gas: Gas,
        limits: Limits,
    ) -> Result<(bool, Gas), StoreError<S::Error>> {
        self.staged(key, |entry, version| {
            let mut store = entry.store.clone();
            let meta = map_metadata(entry, version, id, limits)?;
            let Value::Integer(raw_id) = &meta[0] else {
                return Err(StoreError::InvalidMetadata);
            };
            let data_key = map_data_key(raw_id, map_key, limits)?;
            let bytes = read_bytes(&mut store, version, &data_key)?;
            let gas = charge_read(version, gas, bytes.len())?;
            let present = !bytes.is_empty();
            // Membership deliberately never decodes the stored value.
            if present && version != Version::Lima {
                entry.store = store;
            }
            Ok((present, gas))
        })
    }
}

pub fn initialize<S: Backend>(mut store: S, limits: Limits) -> Result<S, StoreError<S::Error>> {
    store.put(
        vec![0, 0],
        encoding::encode(&Value::Map(BTreeMap::new()), limits)?,
    );
    Ok(store)
}
fn valid_position<E>(position: &BigInt) -> Result<(), StoreError<E>> {
    if position <= &BigInt::zero() || position >= &((BigInt::from(1) << 256) - 1) {
        Err(StoreError::InvalidPosition)
    } else {
        Ok(())
    }
}
pub fn store_key(position: &BigInt) -> Result<Vec<u8>, Error> {
    if position.sign() == Sign::Minus {
        return Err(Error::InvalidValue);
    }
    let mut bytes = vec![0];
    bytes.extend(position.magnitude().to_bytes_be());
    Ok(bytes)
}
pub fn map_prefix(raw_id: &BigInt) -> [u8; 5] {
    // Erlang's fixed-width bit syntax retains the low 32 bits, even for negatives.
    let mut low = raw_id.magnitude().iter_u32_digits().next().unwrap_or(0);
    if raw_id.sign() == Sign::Minus {
        low = low.wrapping_neg();
    }
    let mut prefix = [1, 0, 0, 0, 0];
    prefix[1..].copy_from_slice(&low.to_be_bytes());
    prefix
}
pub fn map_data_key(raw_id: &BigInt, key: &Value, limits: Limits) -> Result<Vec<u8>, Error> {
    let mut bytes = map_prefix(raw_id).to_vec();
    bytes.extend(encoding::encode(key, limits)?);
    Ok(bytes)
}
fn charge_read<E>(version: Version, gas: Gas, bytes: usize) -> Result<Gas, StoreError<E>> {
    if version == Version::Arcus {
        gas.charge(read_gas_cost(bytes))
    } else {
        Ok(gas)
    }
}
fn read_bytes<S: Backend>(
    store: &mut S,
    version: Version,
    key: &[u8],
) -> Result<Vec<u8>, StoreError<S::Error>> {
    if version == Version::Lima {
        store.get(key)
    } else {
        store.get_cached(key)
    }
    .map_err(StoreError::Backend)
}
fn find_term<S: Backend>(
    entry: &mut Entry<S>,
    version: Version,
    position: &BigInt,
    gas: Gas,
    limits: Limits,
) -> Result<RegisterRead, StoreError<S::Error>> {
    if let Some((value, _)) = entry.terms.get(position) {
        return Ok(RegisterRead::Cached(value.clone()));
    }
    let mut store = entry.store.clone();
    let bytes = read_bytes(&mut store, version, &store_key(position)?)?;
    if bytes.is_empty() {
        return Ok(RegisterRead::Missing);
    }
    let gas = charge_read(version, gas, bytes.len())?;
    let value = encoding::decode(&bytes, limits)?;
    entry.terms.insert(position.clone(), (value.clone(), false));
    if version != Version::Lima {
        entry.store = store;
    }
    Ok(RegisterRead::Loaded {
        value,
        bytes: bytes.len(),
        gas,
    })
}
fn map_metadata<S: Backend>(
    entry: &mut Entry<S>,
    version: Version,
    id: &BigInt,
    limits: Limits,
) -> Result<Vec<Value>, StoreError<S::Error>> {
    let value = match find_term(entry, version, &BigInt::zero(), Gas::Unmetered, limits)? {
        RegisterRead::Missing => return Err(StoreError::MissingMetadata),
        RegisterRead::Cached(v) | RegisterRead::Loaded { value: v, .. } => v,
    };
    let Value::Map(meta) = value else {
        return Err(StoreError::InvalidMetadata);
    };
    let Some(Value::Tuple(fields)) = meta.get(&Value::Integer(id.clone())) else {
        return if meta.contains_key(&Value::Integer(id.clone())) {
            Err(StoreError::InvalidMetadata)
        } else {
            Err(StoreError::MissingMap)
        };
    };
    if fields.len() != 3 {
        return Err(StoreError::InvalidMetadata);
    }
    Ok(fields.clone())
}
