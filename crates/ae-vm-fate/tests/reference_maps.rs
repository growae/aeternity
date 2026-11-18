use ae_vm_fate::{
    Error, Limits, encoding,
    maps::{self, MapError, MapOrder, RefCounts},
    types,
    value::Value,
};
use serde_json::Value as J;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::LazyLock,
};
#[path = "support/values.rs"]
mod values;
use values::{rows, string, ty, value};
static FIXTURE: LazyLock<J> =
    LazyLock::new(|| serde_json::from_str(include_str!("fixtures/erlang_fate_maps.json")).unwrap());

// These traces independently check the allocator/unfolder with the reference
// traversal as input. They do not attest a production Erlang-order profile.
struct RecordedOrder(Vec<Vec<Value>>);
impl RecordedOrder {
    fn new(j: &J) -> Self {
        Self(
            rows(j)
                .iter()
                .map(|r| rows(r).iter().map(value).collect())
                .collect(),
        )
    }
}
impl MapOrder for RecordedOrder {
    fn indices(&self, keys: &[&Value], _: Limits) -> Result<Vec<usize>, Error> {
        let set: BTreeSet<_> = keys.iter().copied().collect();
        let expected = self
            .0
            .iter()
            .find(|r| r.iter().collect::<BTreeSet<_>>() == set)
            .expect("missing reference order");
        Ok(expected
            .iter()
            .map(|key| keys.iter().position(|k| *k == key).unwrap())
            .collect())
    }
}
fn counts(j: &J) -> RefCounts {
    rows(j)
        .iter()
        .map(|p| {
            (
                string(&p[0]).parse().unwrap(),
                string(&p[1]).parse().unwrap(),
            )
        })
        .collect()
}
fn limits() -> Limits {
    Limits {
        max_depth: 512,
        ..Limits::default()
    }
}
#[test]
fn persistent_map_allocation_and_threshold_match_erlang_with_reference_order() {
    for (i, row) in rows(&FIXTURE["allocation"]).iter().enumerate() {
        let result = maps::allocate(
            rows(&row["used"])
                .iter()
                .map(|v| string(v).parse().unwrap())
                .collect(),
            rows(&row["values"]).iter().map(value).collect(),
            &RecordedOrder::new(&row["orders"]),
            limits(),
        );
        assert!(
            !matches!(result, Err(MapError::Data(Error::ResourceLimit))),
            "{i}"
        );
        if row["result"].is_null() {
            assert!(result.is_err(), "allocation {i}");
            continue;
        }
        let result = result.unwrap_or_else(|e| panic!("allocation {i}: {e:?}"));
        let expected = &row["result"];
        assert_eq!(
            result.values,
            rows(&expected["values"])
                .iter()
                .map(value)
                .collect::<Vec<_>>(),
            "allocation values {i}"
        );
        assert_eq!(
            result.maps,
            rows(&expected["maps"])
                .iter()
                .map(|p| (string(&p[0]).parse().unwrap(), value(&p[1])))
                .collect(),
            "allocation maps {i}"
        );
    }
}
#[test]
fn persistent_map_refcounts_runtime_encoding_and_inference_match_erlang() {
    for (i, row) in rows(&FIXTURE["refcounts"]).iter().enumerate() {
        let v = value(&row["value"]);
        let counted = maps::refcount(&v, limits());
        assert_ne!(counted.as_ref().err(), Some(&Error::ResourceLimit));
        assert_eq!(
            counted.ok(),
            if row["counts"].is_null() {
                None
            } else {
                Some(counts(&row["counts"]))
            },
            "count {i}"
        );
        assert_eq!(
            maps::has_store_maps(&v, limits()).ok(),
            row["has"].as_bool(),
            "has {i}"
        );
        assert_eq!(
            encoding::encode(&v, limits()).ok().map(hex::encode),
            row["encoded"].as_str().map(str::to_string),
            "encoding {i}"
        );
        assert_eq!(
            types::infer(&v, limits()).ok(),
            if row["inferred"].is_null() {
                None
            } else {
                Some(ty(&row["inferred"]))
            },
            "infer {i}"
        );
    }
    for row in rows(&FIXTURE["arithmetic"]) {
        assert_eq!(
            maps::refcount_union(counts(&row["a"]), counts(&row["b"])),
            counts(&row["union"])
        );
        assert_eq!(
            maps::refcount_diff(counts(&row["a"]), counts(&row["b"])),
            counts(&row["diff"])
        );
    }
}
#[test]
fn unfold_preserves_overwrites_tombstones_and_original_lookup_order() {
    for (i, row) in rows(&FIXTURE["unfold"]).iter().enumerate() {
        let mut lookups = vec![];
        let store: BTreeMap<_, _> = rows(&row["store"])
            .iter()
            .map(|p| (string(&p[0]).to_string(), value(&p[1])))
            .collect();
        let result = maps::unfold(
            value(&row["value"]),
            &RecordedOrder::new(&row["orders"]),
            |id| {
                lookups.push(id.to_string());
                let Some(Value::Map(map)) = store.get(&id.to_string()) else {
                    return Err(());
                };
                Ok(map.clone())
            },
            limits(),
        );
        assert!(
            !matches!(result, Err(MapError::Data(Error::ResourceLimit))),
            "{i}"
        );
        assert_eq!(
            result.ok(),
            if row["result"].is_null() {
                None
            } else {
                Some(value(&row["result"]))
            },
            "unfold {i}"
        );
        assert_eq!(
            lookups,
            rows(&row["lookups"])
                .iter()
                .map(|j| string(j).to_string())
                .collect::<Vec<_>>(),
            "lookups {i}"
        );
    }
}
#[test]
fn map_budgets_order_validation_and_cyclic_lookups_abort_distinctly() {
    struct Order(bool);
    impl MapOrder for Order {
        fn indices(&self, keys: &[&Value], _: Limits) -> Result<Vec<usize>, Error> {
            Ok(if self.0 {
                (0..keys.len()).collect()
            } else {
                vec![0; keys.len()]
            })
        }
    }
    let map = Value::Map(BTreeMap::from([
        (Value::Integer(0.into()), Value::String(vec![42; 120])),
        (Value::Integer(1.into()), Value::Boolean(true)),
    ]));
    assert_eq!(
        maps::allocate(vec![], vec![map.clone()], &Order(false), limits()),
        Err(MapError::InvalidOrder)
    );
    assert_eq!(
        maps::allocate(
            vec![],
            vec![map.clone()],
            &Order(true),
            Limits {
                max_nodes: 1,
                ..limits()
            }
        ),
        Err(MapError::Data(Error::ResourceLimit))
    );
    let allocated = maps::allocate(vec![], vec![map], &Order(true), limits()).unwrap();
    assert_eq!(allocated.maps.len(), 1);
    assert!(matches!(&allocated.values[0],Value::StoreMap {cache,..}if cache.is_empty()));

    let mut calls = 0;
    let result = maps::unfold(
        Value::StoreMap {
            id: 0.into(),
            cache: BTreeMap::new(),
        },
        &Order(true),
        |_| {
            calls += 1;
            Ok::<_, ()>(BTreeMap::from([(
                Value::Integer(0.into()),
                Value::StoreMap {
                    id: 0.into(),
                    cache: BTreeMap::new(),
                },
            )]))
        },
        Limits {
            max_depth: 8,
            ..limits()
        },
    );
    assert_eq!(result, Err(MapError::Data(Error::ResourceLimit)));
    assert!(calls <= 5);

    let mut deep = Value::StoreMap {
        id: 3.into(),
        cache: BTreeMap::new(),
    };
    for _ in 0..12 {
        deep = Value::Tuple(vec![deep]);
    }
    assert_eq!(
        maps::refcount(
            &deep,
            Limits {
                max_depth: 4,
                ..limits()
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        maps::refcount(&deep, limits()).unwrap(),
        BTreeMap::from([(3.into(), 1.into())])
    );
}
