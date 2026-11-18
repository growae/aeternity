// aeb_fate_maps.erl, aebytecode 009e036, production threshold (no TEST).
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{Budget, Error, Limits, encoding, value::Value};
use num_bigint::BigInt;
use num_traits::Zero;
use std::{collections::BTreeMap, convert::Infallible};

pub type Map = BTreeMap<Value, Value>;
pub type RefCounts = BTreeMap<BigInt, BigInt>;
pub const STORE_MAP_THRESHOLD: usize = 100;
pub mod listing;
pub mod order;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Traversal {
    Fold,
    // The runtime's maps:to_list order, which can differ from maps:fold.
    ToList,
    // Erlang term comparison (lists:keysort), not FATE value comparison.
    TermSorted,
}

// Allocation IDs and lookup order depend on Erlang map traversal, which is
// distinct from FATE comparison and serialization order. A compatibility
// profile must supply it explicitly; there is intentionally no default sort.
pub trait MapOrder {
    fn indices(
        &self,
        keys: &[&Value],
        traversal: Traversal,
        limits: Limits,
    ) -> Result<Vec<usize>, Error>;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapError<E = Infallible> {
    Data(Error),
    InvalidOrder,
    Lookup(E),
}
impl<E> From<Error> for MapError<E> {
    fn from(e: Error) -> Self {
        Self::Data(e)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allocation {
    pub values: Vec<Value>,
    pub maps: BTreeMap<BigInt, Value>,
}

pub(crate) fn ordered<E>(
    map: Map,
    order: &impl MapOrder,
    traversal: Traversal,
    budget: &mut Budget,
) -> Result<Vec<(Value, Value)>, MapError<E>> {
    if map.len() > budget.limits.max_nodes.saturating_sub(budget.nodes) {
        return Err(Error::ResourceLimit.into());
    }
    let keys: Vec<_> = map.keys().collect();
    let indices = ordered_indices(&keys, order, traversal, budget)?;
    let mut entries: Vec<_> = map.into_iter().map(Some).collect();
    Ok(indices
        .into_iter()
        .map(|i| entries[i].take().unwrap())
        .collect())
}
pub(crate) fn ordered_indices<E>(
    keys: &[&Value],
    order: &impl MapOrder,
    traversal: Traversal,
    budget: &mut Budget,
) -> Result<Vec<usize>, MapError<E>> {
    if keys.len() > budget.limits.max_nodes.saturating_sub(budget.nodes) {
        return Err(Error::ResourceLimit.into());
    }
    let indices = order.indices(
        keys,
        traversal,
        Limits {
            max_nodes: budget.limits.max_nodes.saturating_sub(budget.nodes),
            ..budget.limits
        },
    )?;
    if indices.len() != keys.len() {
        return Err(MapError::InvalidOrder);
    }
    let mut seen = vec![false; keys.len()];
    for &i in &indices {
        budget.visit(0)?;
        let slot = seen.get_mut(i).ok_or(MapError::InvalidOrder)?;
        if *slot {
            return Err(MapError::InvalidOrder);
        }
        *slot = true;
    }
    Ok(indices)
}
pub fn allocate(
    used_ids: Vec<BigInt>,
    values: Vec<Value>,
    order: &impl MapOrder,
    limits: Limits,
) -> Result<Allocation, MapError> {
    allocate_budgeted(used_ids, values, order, &mut Budget::new(limits), &mut 0)
}
pub(crate) fn allocate_budgeted(
    used_ids: Vec<BigInt>,
    values: Vec<Value>,
    order: &impl MapOrder,
    budget: &mut Budget,
    bytes: &mut usize,
) -> Result<Allocation, MapError> {
    let mut state = Allocator {
        used_ids,
        maps: BTreeMap::new(),
        budget,
        bytes,
        order,
    };
    let values = values
        .into_iter()
        .map(|v| state.value(v, 0))
        .collect::<Result<_, _>>()?;
    Ok(Allocation {
        values,
        maps: state.maps,
    })
}
struct Allocator<'a, O> {
    used_ids: Vec<BigInt>,
    maps: BTreeMap<BigInt, Value>,
    budget: &'a mut Budget,
    bytes: &'a mut usize,
    order: &'a O,
}
impl<O: MapOrder> Allocator<'_, O> {
    fn next_id(&mut self) -> Result<BigInt, MapError> {
        let mut id = BigInt::zero();
        let mut index = 0;
        while let Some(used) = self.used_ids.get(index) {
            self.budget.visit(0)?;
            if &id < used {
                break;
            }
            if &id == used {
                id += 1;
            }
            index += 1;
        }
        self.budget.visit(0)?;
        self.used_ids.insert(index, id.clone());
        Ok(id)
    }
    fn map(&mut self, map: Map, depth: usize) -> Result<Map, MapError> {
        ordered(map, self.order, Traversal::Fold, self.budget)?
            .into_iter()
            .map(|(k, v)| Ok((k, self.value(v, depth)?)))
            .collect()
    }
    fn value(&mut self, value: Value, depth: usize) -> Result<Value, MapError> {
        self.budget.visit(depth)?;
        Ok(match value {
            Value::Tuple(vs) => Value::Tuple(self.values(vs, depth + 1)?),
            Value::List(vs) => Value::List(self.values(vs, depth + 1)?),
            Value::Variant {
                arities,
                tag,
                values,
            } => Value::Variant {
                arities,
                tag,
                values: self.values(values, depth + 1)?,
            },
            Value::Map(map) => {
                let value = Value::Map(self.map(map, depth + 1)?);
                let (bytes, nodes) = encoding::encode_counted(
                    &value,
                    Limits {
                        max_bytes: self.budget.limits.max_bytes.saturating_sub(*self.bytes),
                        max_nodes: self
                            .budget
                            .limits
                            .max_nodes
                            .saturating_sub(self.budget.nodes),
                        ..self.budget.limits
                    },
                )?;
                self.budget.nodes += nodes;
                *self.bytes += bytes.len();
                if bytes.len() < STORE_MAP_THRESHOLD {
                    value
                } else {
                    let id = self.next_id()?;
                    self.maps.insert(id.clone(), value);
                    Value::StoreMap {
                        id,
                        cache: Map::new(),
                    }
                }
            }
            Value::StoreMap { id, cache } if !cache.is_empty() => {
                // A modified persistent map gets its ID before maps in its
                // cache. An ordinary map gets its ID after its children.
                let new_id = self.next_id()?;
                let cache = self.map(cache, depth + 1)?;
                self.maps
                    .insert(new_id.clone(), Value::StoreMap { id, cache });
                Value::StoreMap {
                    id: new_id,
                    cache: Map::new(),
                }
            }
            Value::ContractBytearray(_) | Value::Void | Value::MaxTtl => {
                return Err(Error::InvalidValue.into());
            }
            other => other,
        })
    }
    fn values(&mut self, values: Vec<Value>, depth: usize) -> Result<Vec<Value>, MapError> {
        values.into_iter().map(|v| self.value(v, depth)).collect()
    }
}

pub fn refcount(value: &Value, limits: Limits) -> Result<RefCounts, Error> {
    let mut counts = RefCounts::new();
    count(value, &mut counts, &mut Budget::new(limits), 0)?;
    Ok(counts)
}
pub fn has_store_maps(value: &Value, limits: Limits) -> Result<bool, Error> {
    Ok(!refcount(value, limits)?.is_empty())
}
pub(crate) fn count(
    value: &Value,
    counts: &mut RefCounts,
    budget: &mut Budget,
    depth: usize,
) -> Result<(), Error> {
    budget.visit(depth)?;
    match value {
        Value::StoreMap { id, cache } => {
            if id.magnitude().bits().div_ceil(8) > budget.limits.max_bytes as u64 {
                return Err(Error::ResourceLimit);
            }
            *counts.entry(id.clone()).or_default() += 1;
            for value in cache.values() {
                count(value, counts, budget, depth + 1)?;
            }
        }
        Value::Map(map) => {
            for value in map.values() {
                count(value, counts, budget, depth + 1)?;
            }
        }
        Value::List(vs) | Value::Tuple(vs) | Value::Variant { values: vs, .. } => {
            for value in vs {
                count(value, counts, budget, depth + 1)?;
            }
        }
        Value::ContractBytearray(_) | Value::Void | Value::MaxTtl => {
            return Err(Error::InvalidValue);
        }
        _ => {}
    }
    Ok(())
}
// Zero and negative counts are retained: finalization uses exact zero to
// identify maps eligible for reuse or collection.
pub fn refcount_diff(mut new: RefCounts, old: RefCounts) -> RefCounts {
    for (id, n) in old {
        *new.entry(id).or_default() -= n;
    }
    new
}
pub fn refcount_union(mut a: RefCounts, b: RefCounts) -> RefCounts {
    for (id, n) in b {
        *a.entry(id).or_default() += n;
    }
    a
}

// The caller owns rollback of any effects in lookup (including host caches).
// Repeated references intentionally cause repeated lookups; no memoization.
pub fn unfold<E>(
    value: Value,
    order: &impl MapOrder,
    mut lookup: impl FnMut(&BigInt) -> Result<Map, E>,
    limits: Limits,
) -> Result<Value, MapError<E>> {
    unfold_budgeted(value, order, &mut lookup, &mut Budget::new(limits))
}
pub(crate) fn unfold_budgeted<E>(
    value: Value,
    order: &impl MapOrder,
    lookup: &mut impl FnMut(&BigInt) -> Result<Map, E>,
    budget: &mut Budget,
) -> Result<Value, MapError<E>> {
    unfold_value(value, order, lookup, budget, 0)
}
fn unfold_map<E>(
    map: Map,
    order: &impl MapOrder,
    lookup: &mut impl FnMut(&BigInt) -> Result<Map, E>,
    budget: &mut Budget,
    depth: usize,
) -> Result<Map, MapError<E>> {
    ordered(map, order, Traversal::Fold, budget)?
        .into_iter()
        .map(|(k, v)| Ok((k, unfold_value(v, order, lookup, budget, depth)?)))
        .collect()
}
fn unfold_value<E>(
    value: Value,
    order: &impl MapOrder,
    lookup: &mut impl FnMut(&BigInt) -> Result<Map, E>,
    budget: &mut Budget,
    depth: usize,
) -> Result<Value, MapError<E>> {
    budget.visit(depth)?;
    Ok(match value {
        Value::Tuple(vs) => Value::Tuple(
            vs.into_iter()
                .map(|v| unfold_value(v, order, lookup, budget, depth + 1))
                .collect::<Result<_, _>>()?,
        ),
        Value::List(vs) => Value::List(
            vs.into_iter()
                .map(|v| unfold_value(v, order, lookup, budget, depth + 1))
                .collect::<Result<_, _>>()?,
        ),
        Value::Variant {
            arities,
            tag,
            values,
        } => Value::Variant {
            arities,
            tag,
            values: values
                .into_iter()
                .map(|v| unfold_value(v, order, lookup, budget, depth + 1))
                .collect::<Result<_, _>>()?,
        },
        Value::Map(map) => Value::Map(unfold_map(map, order, lookup, budget, depth + 1)?),
        Value::StoreMap { id, cache } => {
            let stored = lookup(&id).map_err(MapError::Lookup)?;
            // Unfold the complete old map, including entries the cache will
            // delete or replace; skipping those changes failure/gas behavior.
            let Value::Map(mut stored) =
                unfold_value(Value::Map(stored), order, lookup, budget, depth + 1)?
            else {
                unreachable!()
            };
            let cache = unfold_map(cache, order, lookup, budget, depth + 1)?;
            for (key, value) in cache {
                if value == Value::Tombstone {
                    stored.remove(&key);
                } else {
                    stored.insert(key, value);
                }
            }
            Value::Map(stored)
        }
        Value::ContractBytearray(_) | Value::Void | Value::MaxTtl => {
            return Err(Error::InvalidValue.into());
        }
        other => other,
    })
}
