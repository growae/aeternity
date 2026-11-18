use ae_execution::fate_store::Backend;
use ae_state::contracts::{Error as StateError, store::Store};
use ae_trie::{MemoryStore, Nibbles, Trie, Value as TrieValue};
use ae_vm_fate::{
    Error, Limits,
    maps::{MapOrder, Traversal},
    store::{
        StoreError, Version,
        plan::{self, Terms},
        write::{MapMetadata, Metadata, RawMap, Update},
    },
    value::Value,
};
use serde_json::{Value as J, json};
use std::{collections::BTreeSet, sync::LazyLock};
#[path = "../../ae-vm-fate/tests/support/values.rs"]
mod values;
use values::{bytes, rows, string, value};
static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_store_plans.json")).unwrap()
});
struct ReferenceOrder<'a>(&'a J);
impl MapOrder for ReferenceOrder<'_> {
    fn indices(&self, keys: &[&Value], mode: Traversal, _: Limits) -> Result<Vec<usize>, Error> {
        if keys.len() <= 1 {
            return Ok((0..keys.len()).collect());
        }
        let field = match mode {
            Traversal::Fold => "fold",
            Traversal::ToList => "list",
            Traversal::TermSorted => "sorted",
        };
        let set: BTreeSet<_> = keys.iter().copied().collect();
        for row in rows(self.0) {
            let expected: Vec<_> = rows(&row[field]).iter().map(value).collect();
            if expected.iter().collect::<BTreeSet<_>>() == set {
                return Ok(expected
                    .iter()
                    .map(|k| keys.iter().position(|v| *v == k).unwrap())
                    .collect());
            }
        }
        panic!("missing reference order {mode:?} {keys:?}");
    }
}
fn version(row: &J) -> Version {
    match string(&row["module"]) {
        "aefa_stores_lima" => Version::Lima,
        "aefa_stores_ceres" => Version::Ceres,
        "aefa_stores" => Version::Arcus,
        _ => unreachable!(),
    }
}
fn raw_map(j: &J) -> RawMap {
    rows(j)
        .iter()
        .map(|p| (bytes(&p[0]), bytes(&p[1])))
        .collect()
}
fn raw(row: &J) -> Backend {
    if row["mode"] == "tree" {
        let mut tree = Trie::new(MemoryStore::default());
        for p in rows(&row["base"]) {
            tree.put(
                &Nibbles::from_bytes(&bytes(&p[0])),
                TrieValue::Bytes(bytes(&p[1])),
            )
            .unwrap();
        }
        Backend(Store::new(tree))
    } else {
        let mut s = Store::default();
        s.put_map(raw_map(&row["base"]));
        Backend(s)
    }
}
fn meta(j: &J) -> Metadata {
    rows(j)
        .iter()
        .map(|p| {
            (
                string(&p[0]).parse().unwrap(),
                MapMetadata {
                    raw_id: p[1].as_str().map(|v| v.parse().unwrap()),
                    refcount: string(&p[2]).parse().unwrap(),
                    size: p[3].as_str().map(|v| v.parse().unwrap()),
                },
            )
        })
        .collect()
}
fn terms(j: &J) -> Terms {
    rows(j)
        .iter()
        .map(|p| {
            (
                string(&p[0]).parse().unwrap(),
                (value(&p[1]), p[2].as_bool().unwrap()),
            )
        })
        .collect()
}
fn updates(j: &J) -> Vec<Update> {
    rows(j)
        .iter()
        .map(|u| match string(&u[0]) {
            "push" => Update::Push {
                position: string(&u[1]).parse().unwrap(),
                value: value(&u[2]),
            },
            "copy" => Update::Copy {
                id: string(&u[1]).parse().unwrap(),
                value: value(&u[2]),
            },
            "inplace" => Update::InPlace {
                id: string(&u[1]).parse().unwrap(),
                value: value(&u[2]),
            },
            "gc" => Update::Collect {
                raw_id: string(&u[1]).parse().unwrap(),
            },
            _ => unreachable!(),
        })
        .collect()
}
fn pairs(m: &RawMap) -> J {
    json!(
        m.iter()
            .map(|(k, v)| [hex::encode(k), hex::encode(v)])
            .collect::<Vec<_>>()
    )
}
fn final_state(w: ae_vm_fate::store::write::Written<Backend>) -> J {
    let contents = w.store.0.contents().unwrap();
    let mut tree = Trie::new(MemoryStore::default());
    for (k, v) in &contents {
        if !v.is_empty() {
            tree.put(&Nibbles::from_bytes(k), TrieValue::Bytes(v.clone()))
                .unwrap();
        }
    }
    json!({"gas":w.gas.to_string(),"writes":pairs(w.store.0.write_cache()),"reads":pairs(w.store.0.read_cache()),"contents":pairs(&contents),"root":tree.root_hash().map(|h|hex::encode(h.0))})
}
fn error(e: StoreError<StateError>) -> J {
    match e {
        StoreError::OutOfGas => json!({"out_of_gas":true}),
        StoreError::InvalidOrder => panic!("invalid traversal"),
        StoreError::Data(Error::ResourceLimit) => panic!("unexpected operational limit"),
        _ => json!({"exception":true}),
    }
}
#[test]
fn all_historical_plans_and_complete_entry_writes_match_erlang() {
    for (i, row) in rows(&FIXTURE["cases"]).iter().enumerate() {
        let raw = raw(row);
        let before = raw.0.contents().unwrap();
        for run in rows(&row["runs"]) {
            let expected = &run["result"];
            let order = ReferenceOrder(&row["orders"]);
            let result = plan::plan(
                version(row),
                meta(&row["meta"]),
                terms(&row["terms"]),
                &raw,
                string(&run["gas"]).parse().unwrap(),
                &order,
                Limits::default(),
            );
            let actual = match result {
                Ok(planned) => {
                    assert!(
                        expected["plan"]["gas"].is_string(),
                        "case {i} plan unexpectedly succeeded: {expected}"
                    );
                    assert_eq!(
                        planned.plan.metadata,
                        meta(&expected["plan"]["metadata"]),
                        "case {i} metadata"
                    );
                    assert_eq!(
                        planned.plan.updates,
                        updates(&expected["plan"]["updates"]),
                        "case {i} updates"
                    );
                    assert_eq!(
                        planned.gas.to_string(),
                        string(&expected["plan"]["gas"]),
                        "case {i} planning gas"
                    );
                    assert_eq!(
                        planned.plan.gc_cache,
                        rows(&expected["plan"]["gc"])
                            .iter()
                            .map(|p| (string(&p[0]).parse().unwrap(), raw_map(&p[1])))
                            .collect(),
                        "case {i} GC cache"
                    );
                    match planned.write(version(row), &raw, &order) {
                        Ok(w) => final_state(w),
                        Err(e) => error(e),
                    }
                }
                Err(e) => {
                    let e = error(e);
                    assert_eq!(e, expected["plan"], "case {i} plan");
                    e
                }
            };
            assert_eq!(
                actual, expected["final"],
                "case {i} {} {} gas {}",
                row["module"], row["name"], run["gas"]
            );
            assert_eq!(
                raw.0.contents().unwrap(),
                before,
                "case {i} partial publication"
            );
        }
    }
}
#[test]
fn exhausted_reuse_changes_at_arcus_and_resource_limits_are_distinct() {
    for row in rows(&FIXTURE["cases"])
        .iter()
        .filter(|r| r["name"] == "reuse_cycle" && r["mode"] == "tree")
    {
        let raw = raw(row);
        let order = ReferenceOrder(&row["orders"]);
        let r = plan::plan(
            version(row),
            meta(&row["meta"]),
            terms(&row["terms"]),
            &raw,
            1_000_000_000.into(),
            &order,
            Limits::default(),
        );
        if version(row) == Version::Arcus {
            assert!(matches!(r, Err(StoreError::OutOfGas)));
        } else {
            assert!(r.is_ok());
        }
        let r = plan::plan(
            version(row),
            meta(&row["meta"]),
            terms(&row["terms"]),
            &raw,
            1_000_000_000.into(),
            &order,
            Limits {
                max_nodes: 1,
                ..Limits::default()
            },
        );
        assert!(matches!(r, Err(StoreError::Data(Error::ResourceLimit))));
        assert!(raw.0.write_cache().is_empty());
        assert!(raw.0.read_cache().is_empty());
    }
}
#[test]
fn planner_does_not_publish_partial_work_on_read_failure_and_retries() {
    use ae_vm_fate::store::{Backend as StoreBackend, write::SubtreeBackend};
    #[derive(Clone)]
    struct Fail(Backend);
    impl StoreBackend for Fail {
        type Error = StateError;
        fn get(&self, _: &[u8]) -> Result<Vec<u8>, StateError> {
            Err(StateError::InvalidValue)
        }
        fn get_cached(&mut self, _: &[u8]) -> Result<Vec<u8>, StateError> {
            Err(StateError::InvalidValue)
        }
        fn put(&mut self, k: Vec<u8>, v: Vec<u8>) {
            self.0.put(k, v);
        }
    }
    impl SubtreeBackend for Fail {
        fn subtree(&self, _: &[u8], _: usize) -> Result<Option<(RawMap, usize)>, StateError> {
            Err(StateError::InvalidValue)
        }
    }
    let row = rows(&FIXTURE["cases"])
        .iter()
        .find(|r| {
            r["module"] == "aefa_stores" && r["mode"] == "tree" && r["name"] == "nested_reuse"
        })
        .unwrap();
    let backend = raw(row);
    let before = backend.0.contents().unwrap();
    let order = ReferenceOrder(&row["orders"]);
    let r = plan::plan(
        Version::Arcus,
        meta(&row["meta"]),
        terms(&row["terms"]),
        &Fail(backend.clone()),
        1_000_000_000.into(),
        &order,
        Limits::default(),
    );
    assert!(matches!(
        r,
        Err(StoreError::Backend(StateError::InvalidValue))
    ));
    let r = plan::plan(
        Version::Arcus,
        meta(&row["meta"]),
        terms(&row["terms"]),
        &backend,
        1_000_000_000.into(),
        &order,
        Limits {
            max_bytes: 1,
            ..Limits::default()
        },
    );
    assert!(matches!(r, Err(StoreError::Data(Error::ResourceLimit))));
    plan::plan(
        Version::Arcus,
        meta(&row["meta"]),
        terms(&row["terms"]),
        &backend,
        1_000_000_000.into(),
        &order,
        Limits::default(),
    )
    .unwrap()
    .write(Version::Arcus, &backend, &order)
    .unwrap();
    assert_eq!(backend.0.contents().unwrap(), before);
}

#[test]
fn complete_finalization_matches_entry_and_multi_contract_reference() {
    use ae_vm_fate::store::{Stores, write::Written};
    let mut key = [0u8; 32];
    key[31] = 1;
    for row in rows(&FIXTURE["cases"]) {
        if rows(&row["terms"]).iter().any(|t| t[2] == false) {
            continue;
        }
        let backend = raw(row);
        let before = backend.0.contents().unwrap();
        let mut stores = Stores::new(version(row));
        stores.put_contract(key, backend.clone());
        for (pos, (v, _)) in terms(&row["terms"]) {
            stores.put_value(&key, pos, v).unwrap();
        }
        // The original helper fixture explicitly marks even empty caches dirty.
        // Exercise naturally dirty entries here; clean entries are covered below.
        if rows(&row["terms"]).is_empty() {
            continue;
        }
        for run in rows(&row["runs"]) {
            let actual = match stores.clone().finalize(
                string(&run["gas"]).parse().unwrap(),
                &ReferenceOrder(&row["orders"]),
                Limits::default(),
            ) {
                Ok(mut out) => {
                    assert_eq!(out.stores.len(), 1);
                    let (k, store) = out.stores.pop().unwrap();
                    assert_eq!(k, key);
                    final_state(Written {
                        store,
                        gas: out.gas,
                        remaining_limits: out.remaining_limits,
                    })
                }
                Err(e) => error(e),
            };
            assert_eq!(
                actual, run["result"]["final"],
                "entry {} {} gas {}",
                row["module"], row["name"], run["gas"]
            );
            assert_eq!(backend.0.contents().unwrap(), before);
        }
    }
    for row in rows(&FIXTURE["multi"]) {
        let mut stores = Stores::new(version(row));
        let mut original = vec![];
        for entry in rows(&row["entries"]) {
            let backend = raw(&json!({"mode":row["mode"],"base":entry["base"]}));
            let key: [u8; 32] = bytes(&entry["key"]).try_into().unwrap();
            original.push((backend.clone(), backend.0.contents().unwrap()));
            stores.put_contract(key, backend);
            for (pos, (v, _)) in terms(&entry["terms"]) {
                stores.put_value(&key, pos, v).unwrap();
            }
        }
        for run in rows(&row["runs"]) {
            let actual = match stores.clone().finalize(
                string(&run["gas"]).parse().unwrap(),
                &ReferenceOrder(&row["orders"]),
                Limits::default(),
            ) {
                Ok(out) => {
                    json!({"gas":out.gas.to_string(),"stores":out.stores.into_iter().map(|(k,s)|json!([hex::encode(k),final_state(Written{store:s,gas:0.into(),remaining_limits:Limits::default()})])).collect::<Vec<_>>()})
                }
                Err(e) => error(e),
            };
            assert_eq!(
                actual, run["result"],
                "multi {} {} gas {}",
                row["module"], row["name"], run["gas"]
            );
            for (backend, before) in &original {
                assert_eq!(&backend.0.contents().unwrap(), before);
            }
        }
        if row["name"] == "forty" {
            let result = stores.finalize(
                1_000_000_000.into(),
                &ReferenceOrder(&row["orders"]),
                Limits {
                    max_nodes: 200,
                    ..Limits::default()
                },
            );
            assert!(matches!(
                result,
                Err(StoreError::Data(Error::ResourceLimit))
            ));
            for (backend, before) in &original {
                assert_eq!(&backend.0.contents().unwrap(), before);
            }
        }
    }
}
