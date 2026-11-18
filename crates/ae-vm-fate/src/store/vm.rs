// aefa_engine_state:finalize and aefa_fate_op store-map helpers, fa2a6d7.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{
    Backend, Finalized, Gas, StoreError, Stores, Version, read::CachedSubtreeBackend,
    write::SubtreeBackend,
};
use crate::{
    Budget, Error, Limits,
    gas::{self, Charge, CostModel, Meter},
    maps::{self, Map, MapError, MapOrder, Traversal},
    value::Value,
};
use num_bigint::BigInt;
use std::convert::Infallible;

fn map_error<E>(error: MapError) -> StoreError<E> {
    match error {
        MapError::Data(e) => StoreError::Data(e),
        MapError::InvalidOrder => StoreError::InvalidOrder,
        MapError::Lookup(never) => match never {},
    }
}
fn gas_error<E>(error: gas::Error) -> StoreError<E> {
    match error {
        gas::Error::Data(e) => StoreError::Data(e),
        gas::Error::OutOfGas => StoreError::OutOfGas,
        gas::Error::InvalidOrder => StoreError::InvalidOrder,
        gas::Error::Host(never) => match never {},
    }
}
impl<S: Backend> Stores<S> {
    // Original maps:values order is captured by the ToList profile. Both key
    // sets include clean entries before filtering. Dirty zero/negative
    // registers still pay traversal, although planning skips their writes.
    pub fn charge_finalization(
        &self,
        mut gas: BigInt,
        order: &impl MapOrder,
        mut limits: Limits,
    ) -> Result<Charge, StoreError<S::Error>> {
        if self.version == Version::Lima {
            return Ok(Charge {
                gas,
                remaining_limits: limits,
            });
        }
        let bytes = self
            .contracts
            .len()
            .checked_mul(32)
            .ok_or(Error::ResourceLimit)?;
        limits.max_bytes = limits
            .max_bytes
            .checked_sub(bytes)
            .ok_or(Error::ResourceLimit)?;
        if self.contracts.len() > limits.max_nodes {
            return Err(Error::ResourceLimit.into());
        }
        let keys: Vec<_> = self
            .contracts
            .keys()
            .map(|k| Value::String(k.to_vec()))
            .collect();
        let mut budget = Budget::new(limits);
        let indices = maps::ordered_indices::<Infallible>(
            &keys.iter().collect::<Vec<_>>(),
            order,
            Traversal::ToList,
            &mut budget,
        )
        .map_err(map_error)?;
        limits.max_nodes -= budget.nodes;
        let entries: Vec<_> = self.contracts.values().collect();
        for i in indices {
            let entry = entries[i];
            if !entry.dirty {
                continue;
            }
            if entry.terms.len() > limits.max_nodes {
                return Err(Error::ResourceLimit.into());
            }
            for key in entry.terms.keys() {
                let bytes = usize::try_from(key.magnitude().bits().div_ceil(8))
                    .map_err(|_| Error::ResourceLimit)?;
                limits.max_bytes = limits
                    .max_bytes
                    .checked_sub(bytes)
                    .ok_or(Error::ResourceLimit)?;
            }
            let keys: Vec<_> = entry
                .terms
                .keys()
                .map(|n| Value::Integer(n.clone()))
                .collect();
            let mut budget = Budget::new(limits);
            let indices = maps::ordered_indices::<Infallible>(
                &keys.iter().collect::<Vec<_>>(),
                order,
                Traversal::ToList,
                &mut budget,
            )
            .map_err(map_error)?;
            limits.max_nodes -= budget.nodes;
            let terms: Vec<_> = entry.terms.values().collect();
            for j in indices {
                let (value, dirty) = terms[j];
                if !dirty {
                    continue;
                }
                // Ceres and Arcus store versions both use the Iris+ traversal
                // rule. Each term rounds separately, before any store writes.
                let result = gas::traversal(5, CostModel::Serial, value, gas, order, limits)
                    .map_err(gas_error)?;
                gas = result.gas;
                limits = result.remaining_limits;
            }
        }
        Ok(Charge {
            gas,
            remaining_limits: limits,
        })
    }
}
impl<S: SubtreeBackend> Stores<S> {
    pub fn finalize_with_traversal(
        self,
        gas: BigInt,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Finalized<S>, StoreError<S::Error>> {
        let charged = self.charge_finalization(gas, order, limits)?;
        self.finalize(charged.gas, order, charged.remaining_limits)
    }
}

// VM store operations require a contract already loaded by the chain host.
// The scope binds protocol pricing to the historical store implementation.
pub struct Scope<'a, S> {
    stores: &'a mut Stores<S>,
    current: [u8; 32],
    protocol: u32,
}
#[derive(Debug, Clone)]
pub struct Outcome<T> {
    pub value: T,
    pub gas: BigInt,
}
impl<S: Backend> Stores<S> {
    pub fn vm_scope(
        &mut self,
        protocol: u32,
        current: [u8; 32],
    ) -> Result<Scope<'_, S>, StoreError<S::Error>> {
        if Version::for_protocol(protocol) != self.version {
            return Err(StoreError::ProtocolMismatch);
        }
        Ok(Scope {
            stores: self,
            current,
            protocol,
        })
    }
}
fn metered(gas: Gas) -> BigInt {
    let Gas::Metered(gas) = gas else {
        unreachable!()
    };
    gas
}
fn store_values<E>(protocol: u32, gas: BigInt, count: &Value) -> Result<BigInt, StoreError<E>> {
    // The pre-Ceres helper does not inspect the argument at all.
    if protocol < 6 {
        return Ok(gas);
    }
    let Value::Integer(count) = count else {
        return Err(StoreError::InvalidMetadata);
    };
    let mut meter = Meter {
        protocol,
        gas,
        created_cells: 0.into(),
        metering_floor: None,
    };
    meter.store_values(count.clone()).map_err(gas_error)?;
    Ok(meter.gas)
}
impl<S: Backend> Scope<'_, S> {
    pub fn map_lookup(
        &mut self,
        cache: &Map,
        id: &BigInt,
        key: &Value,
        gas: BigInt,
        limits: Limits,
    ) -> Result<Outcome<Option<Value>>, StoreError<S::Error>> {
        if let Some(value) = cache.get(key) {
            return Ok(Outcome {
                value: (value != &Value::Tombstone).then(|| value.clone()),
                gas,
            });
        }
        if !self.stores.has_contract(&self.current) {
            return Err(StoreError::MissingContract);
        }
        let gas = store_values(self.protocol, gas, &Value::Integer(1.into()))?;
        let (value, gas) =
            self.stores
                .map_lookup(&self.current, id, key, Gas::Metered(gas), limits)?;
        Ok(Outcome {
            value,
            gas: metered(gas),
        })
    }
    pub fn map_member(
        &mut self,
        cache: &Map,
        id: &BigInt,
        key: &Value,
        gas: BigInt,
        limits: Limits,
    ) -> Result<Outcome<bool>, StoreError<S::Error>> {
        if let Some(value) = cache.get(key) {
            return Ok(Outcome {
                value: value != &Value::Tombstone,
                gas,
            });
        }
        if !self.stores.has_contract(&self.current) {
            return Err(StoreError::MissingContract);
        }
        let gas = store_values(self.protocol, gas, &Value::Integer(1.into()))?;
        let (value, gas) =
            self.stores
                .map_member(&self.current, id, key, Gas::Metered(gas), limits)?;
        Ok(Outcome {
            value,
            gas: metered(gas),
        })
    }
    pub fn map_size(
        &mut self,
        cache: &Map,
        id: &BigInt,
        gas: BigInt,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Outcome<Value>, StoreError<S::Error>> {
        let mut staged = self.stores.clone();
        let mut size = staged.map_size(&self.current, id, limits)?;
        let mut gas = store_values(self.protocol, gas, &Value::Integer(cache.len().into()))?;
        let mut budget = Budget::new(limits);
        if cache.len() > limits.max_nodes {
            return Err(Error::ResourceLimit.into());
        }
        let entries: Vec<_> = cache.iter().collect();
        let indices = maps::ordered_indices::<Infallible>(
            &entries.iter().map(|p| p.0).collect::<Vec<_>>(),
            order,
            Traversal::Fold,
            &mut budget,
        )
        .map_err(map_error)?;
        for i in indices {
            let (key, value) = entries[i];
            // Original membership caches are discarded. Tombstones read the
            // metadata-populated snapshot; other entries use the old snapshot.
            let tombstone = value == &Value::Tombstone;
            let mut probe = if tombstone {
                staged.clone()
            } else {
                self.stores.clone()
            };
            let (exists, next_gas) =
                probe.map_member(&self.current, id, key, Gas::Metered(gas), limits)?;
            gas = metered(next_gas);
            let delta = match (tombstone, exists) {
                (true, true) => -1,
                (false, false) => 1,
                _ => 0,
            };
            if delta != 0 {
                let Value::Integer(ref mut n) = size else {
                    return Err(StoreError::InvalidMetadata);
                };
                *n += delta;
            }
        }
        *self.stores = staged;
        Ok(Outcome { value: size, gas })
    }
}
impl<S: CachedSubtreeBackend> Scope<'_, S> {
    pub fn map_get_clean(
        &mut self,
        cache: &Map,
        id: &BigInt,
        gas: BigInt,
        order: &impl MapOrder,
        mut limits: Limits,
    ) -> Result<Outcome<Map>, StoreError<S::Error>> {
        let mut staged = self.stores.clone();
        let size = staged.map_size(&self.current, id, limits)?;
        let gas = store_values(self.protocol, gas, &size)?;
        limits.max_nodes = limits
            .max_nodes
            .checked_sub(cache.len())
            .ok_or(Error::ResourceLimit)?;
        let (list, gas) =
            staged.map_to_list(&self.current, id, Gas::Metered(gas), order, limits)?;
        // maps:from_list retains the last value of duplicate decoded keys.
        let mut value: Map = list.into_iter().collect();
        // Cache keys are unique and each overlay update is independent; their
        // iteration order has no observable effect on this result or host.
        for (key, entry) in cache {
            if entry == &Value::Tombstone {
                value.remove(key);
            } else {
                value.insert(key.clone(), entry.clone());
            }
        }
        *self.stores = staged;
        Ok(Outcome {
            value,
            gas: metered(gas),
        })
    }
}

struct Unfolder<'a, S, O> {
    stores: Stores<S>,
    metadata: Option<Value>,
    current: [u8; 32],
    order: &'a O,
    remaining: Limits,
}
fn subtract_work(mut limits: Limits, before: Limits, after: Limits) -> Result<Limits, Error> {
    limits.max_nodes = limits
        .max_nodes
        .checked_sub(before.max_nodes - after.max_nodes)
        .ok_or(Error::ResourceLimit)?;
    limits.max_bytes = limits
        .max_bytes
        .checked_sub(before.max_bytes - after.max_bytes)
        .ok_or(Error::ResourceLimit)?;
    Ok(limits)
}
impl<S: CachedSubtreeBackend, O: MapOrder> Unfolder<'_, S, O> {
    fn lookup(&mut self, id: &BigInt, limits: Limits) -> Result<Map, StoreError<S::Error>> {
        let limits = Limits {
            max_nodes: limits.max_nodes.min(self.remaining.max_nodes),
            max_bytes: limits.max_bytes.min(self.remaining.max_bytes),
            max_depth: limits.max_depth.min(self.remaining.max_depth),
        };
        let listed = self.stores.map_to_list_counted(
            &self.current,
            id,
            Gas::Unmetered,
            self.order,
            limits,
        )?;
        self.remaining = subtract_work(self.remaining, limits, listed.remaining_limits)?;
        Ok(listed.values.into_iter().collect())
    }
}
impl<S: CachedSubtreeBackend, O: MapOrder> gas::UnfoldHost for Unfolder<'_, S, O> {
    type Error = StoreError<S::Error>;
    fn map_size(&mut self, id: &BigInt, _: Limits) -> Result<BigInt, Self::Error> {
        // The original closure reads the metadata-prefetched Store1 snapshot,
        // discarding any returned cache. Only that frozen term is needed here.
        let Value::Map(meta) = self.metadata.as_ref().ok_or(StoreError::MissingMetadata)? else {
            return Err(StoreError::InvalidMetadata);
        };
        let Value::Tuple(fields) = meta
            .get(&Value::Integer(id.clone()))
            .ok_or(StoreError::MissingMap)?
        else {
            return Err(StoreError::InvalidMetadata);
        };
        let [_, _, Value::Integer(size)] = fields.as_slice() else {
            return Err(StoreError::InvalidMetadata);
        };
        Ok(size.clone())
    }
    fn unfold(&mut self, id: &BigInt, limits: Limits) -> Result<Value, Self::Error> {
        self.lookup(id, limits).map(Value::Map)
    }
}
impl<S: CachedSubtreeBackend> Scope<'_, S> {
    pub fn unfold(
        &mut self,
        value: Value,
        gas: BigInt,
        model: CostModel,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Outcome<Value>, StoreError<S::Error>> {
        let simple = gas::traversal(self.protocol, CostModel::Simple, &value, gas, order, limits)
            .map_err(gas_error)?;
        let mut budget = Budget::new(simple.remaining_limits);
        let mut counts = maps::RefCounts::new();
        maps::count(&value, &mut counts, &mut budget, 0)?;
        if counts.is_empty() {
            return Ok(Outcome {
                value,
                gas: simple.gas,
            });
        }
        let limits = Limits {
            max_nodes: budget.limits.max_nodes - budget.nodes,
            ..budget.limits
        };
        let mut staged = self.stores.clone();
        staged.cache_metadata(&self.current, limits)?;
        let metadata = staged
            .contracts
            .get(&self.current)
            .and_then(|e| e.terms.get(&0.into()))
            .map(|(v, _)| v.clone());
        let mut host = Unfolder {
            stores: staged,
            metadata,
            current: self.current,
            order,
            remaining: limits,
        };
        let charged = gas::traversal_with_host(
            self.protocol,
            model,
            &value,
            simple.gas,
            &mut host,
            order,
            limits,
        )
        .map_err(|e| match e {
            gas::Error::Data(e) => StoreError::Data(e),
            gas::Error::OutOfGas => StoreError::OutOfGas,
            gas::Error::InvalidOrder => StoreError::InvalidOrder,
            gas::Error::Host(e) => e,
        })?;
        // Account for traversal and decoding separately, then carry their
        // combined remainder into actual unfolding. Repeated lookups still
        // decode and charge operational work even when the byte cache is warm.
        let limits = subtract_work(charged.remaining_limits, limits, host.remaining)?;
        host.remaining = limits;
        let mut budget = Budget::new(limits);
        let value =
            maps::unfold_budgeted(value, order, &mut |id| host.lookup(id, limits), &mut budget)
                .map_err(|e| match e {
                    MapError::Data(e) => StoreError::Data(e),
                    MapError::InvalidOrder => StoreError::InvalidOrder,
                    MapError::Lookup(e) => e,
                })?;
        subtract_work(
            host.remaining,
            limits,
            Limits {
                max_nodes: limits.max_nodes - budget.nodes,
                ..limits
            },
        )?;
        *self.stores = host.stores;
        Ok(Outcome {
            value,
            gas: charged.gas,
        })
    }
}
