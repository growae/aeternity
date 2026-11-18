// aefa_fate_op:map_to_sorted_list/2, aec_fork_block_settings, fa2a6d7.
// Copyright (c) 2021, Aeternity Anstalt. ISC notice in repository LICENSE.
use super::{Map, MapError, MapOrder, Traversal};
use crate::{
    Budget, Error, Limits, encoding,
    value::{self, Value},
};
use std::{borrow::Cow, cmp::Ordering, collections::BTreeMap};

// The original table maps complete decoded terms to arbitrary decoded terms.
// Do not key this by map keys alone or require the replacement to be a list:
// the historical loader and make_list wrapper impose neither restriction.
#[derive(Debug, Clone, Default)]
pub struct Overrides {
    entries: BTreeMap<Value, Value>,
}
pub struct Listing<'a> {
    pub value: Cow<'a, Value>,
    pub remaining_limits: Limits,
}
impl Overrides {
    pub fn from_encoded<'a>(
        pairs: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
        mut limits: Limits,
    ) -> Result<Self, Error> {
        let mut entries = BTreeMap::new();
        for (key, value) in pairs {
            let mut decode = |bytes: &[u8]| {
                let (value, rest, nodes) = encoding::decode_one_counted(bytes, limits)?;
                if !rest.is_empty() {
                    return Err(Error::TrailingBytes);
                }
                limits.max_nodes -= nodes;
                limits.max_bytes -= bytes.len();
                Ok(value)
            };
            let key = decode(key)?;
            let value = decode(value)?;
            entries.insert(key, value);
        }
        Ok(Self { entries })
    }
    pub fn list<'a>(
        &'a self,
        map: Map,
        protocol: u32,
        order: &impl MapOrder,
        limits: Limits,
    ) -> Result<Listing<'a>, MapError> {
        let input = Value::Map(map);
        let limits = value::check_limits(&input, limits)?;
        if protocol < 5
            && let Some(replacement) = self.entries.get(&input)
        {
            let remaining_limits = value::check_limits(replacement, limits)?;
            return Ok(Listing {
                value: Cow::Borrowed(replacement),
                remaining_limits,
            });
        }
        let Value::Map(map) = input else {
            unreachable!()
        };
        let mut budget = Budget::new(limits);
        let rows = if protocol < 5 {
            super::ordered(map, order, Traversal::ToList, &mut budget)?
        } else {
            let mut rows: Vec<_> = map.into_iter().collect();
            let mut error = None;
            rows.sort_by(|(a, _), (b, _)| {
                if error.is_some() || a == b {
                    return Ordering::Equal;
                }
                match value::cmp(a, b, &mut budget, 0) {
                    Ok(c) => c,
                    Err(e) => {
                        error = Some(e);
                        Ordering::Equal
                    }
                }
            });
            if let Some(e) = error {
                return Err(e.into());
            }
            rows
        };
        budget.visit(0)?;
        let list = rows
            .into_iter()
            .map(|(k, v)| {
                budget.visit(1)?;
                Ok(Value::Tuple(vec![k, v]))
            })
            .collect::<Result<_, Error>>()?;
        Ok(Listing {
            value: Cow::Owned(Value::List(list)),
            remaining_limits: Limits {
                max_nodes: limits.max_nodes - budget.nodes,
                ..limits
            },
        })
    }
}
