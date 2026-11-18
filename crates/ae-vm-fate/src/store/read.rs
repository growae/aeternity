// Historical store_map_to_list in aefa_stores_lima / ceres / aefa_stores.
// Copyright (c) 2019, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{
    Gas, StoreError, Stores, Version, charge_read, map_metadata, map_prefix,
    write::{RawMap, SubtreeBackend},
};
use crate::{
    Budget, Error, Limits, encoding,
    maps::{self, MapError, MapOrder, Traversal},
    value::Value,
};
use num_bigint::BigInt;

pub trait CachedSubtreeBackend: SubtreeBackend {
    fn subtree_cached(
        &mut self,
        prefix: &[u8],
        maximum_bytes: usize,
    ) -> Result<Option<(RawMap, usize)>, Self::Error>;
}
pub type MapList = Vec<(Value, Value)>;

impl<S: CachedSubtreeBackend> Stores<S> {
    pub fn map_to_list(
        &mut self,
        key: &[u8; 32],
        id: &BigInt,
        gas: Gas,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<(MapList, Gas), StoreError<S::Error>> {
        self.staged(key, |entry, version| {
            // As with single-key lookup, replace the byte cache captured before
            // metadata lookup, while retaining the decoded metadata term.
            let mut store = entry.store.clone();
            let meta = map_metadata(entry, version, id, limits)?;
            let Value::Integer(raw) = &meta[0] else {
                return Err(StoreError::InvalidMetadata);
            };
            let found = if version == Version::Lima {
                store.subtree(&map_prefix(raw), limits.max_bytes)
            } else {
                store.subtree_cached(&map_prefix(raw), limits.max_bytes)
            }
            .map_err(StoreError::Backend)?;
            let Some((subtree, bytes)) = found else {
                return Err(Error::ResourceLimit.into());
            };
            // The original reads first and charges before decoding, even on a
            // cache hit. The read cap above is operational, not a gas cap.
            let gas = charge_read(version, gas, bytes)?;
            let mut budget = Budget::new(limits);
            if subtree.len() > limits.max_nodes {
                return Err(Error::ResourceLimit.into());
            }
            let indices = if version == Version::Lima {
                let keys: Vec<_> = subtree.keys().map(|k| Value::String(k.clone())).collect();
                maps::ordered_indices::<std::convert::Infallible>(
                    &keys.iter().collect::<Vec<_>>(),
                    order,
                    Traversal::ToList,
                    &mut budget,
                )
                .map_err(|e| match e {
                    MapError::Data(e) => StoreError::Data(e),
                    MapError::InvalidOrder => StoreError::InvalidOrder,
                    MapError::Lookup(n) => match n {},
                })?
            } else {
                (0..subtree.len()).collect()
            };
            let entries: Vec<_> = subtree.into_iter().collect();
            let mut decoded_bytes = 0usize;
            let mut list = Vec::with_capacity(entries.len());
            for i in indices {
                budget.visit(0)?;
                let mut decode = |bytes: &[u8]| -> Result<Value, Error> {
                    let (v, rest, nodes) = encoding::decode_one_counted(
                        bytes,
                        Limits {
                            max_nodes: limits.max_nodes.saturating_sub(budget.nodes),
                            max_bytes: limits.max_bytes.saturating_sub(decoded_bytes),
                            ..limits
                        },
                    )?;
                    if !rest.is_empty() {
                        return Err(Error::TrailingBytes);
                    }
                    budget.nodes += nodes;
                    decoded_bytes += bytes.len();
                    Ok(v)
                };
                list.push((decode(&entries[i].0)?, decode(&entries[i].1)?));
            }
            if version != Version::Lima {
                entry.store = store;
            }
            Ok((list, gas))
        })
    }
}
