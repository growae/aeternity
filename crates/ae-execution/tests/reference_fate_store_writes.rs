use ae_execution::fate_store::Backend;
use ae_state::contracts::{Error as StateError, store::Store};
use ae_trie::{MemoryStore, Nibbles, Trie, Value as TrieValue};
use ae_vm_fate::{
    Error, Limits,
    maps::{MapOrder, Traversal},
    store::{
        Backend as StoreBackend, StoreError, Version,
        write::{GcCache, MapMetadata, Metadata, RawMap, SubtreeBackend, Update, WritePlan},
    },
    value::Value,
};
use serde_json::{Value as J, json};
use std::{collections::BTreeSet, sync::LazyLock};
#[path = "../../ae-vm-fate/tests/support/values.rs"]
mod values;
use values::{bytes, rows, string, value};
static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_store_writes.json")).unwrap()
});
struct ReferenceOrder<'a>(&'a J);
impl MapOrder for ReferenceOrder<'_> {
    fn indices(
        &self,
        keys: &[&Value],
        traversal: Traversal,
        _: Limits,
    ) -> Result<Vec<usize>, Error> {
        let field = match traversal {
            Traversal::ToList => "list",
            Traversal::TermSorted => "sorted",
            Traversal::Fold => panic!("write stage must not allocate"),
        };
        let set: BTreeSet<_> = keys.iter().copied().collect();
        for row in rows(self.0) {
            let recorded: Vec<_> = rows(&row[field]).iter().map(value).collect();
            if recorded.iter().collect::<BTreeSet<_>>() == set {
                return Ok(recorded
                    .iter()
                    .map(|v| keys.iter().position(|k| *k == v).unwrap())
                    .collect());
            }
        }
        panic!("missing original traversal");
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
    let mut store = Store::default();
    if row["mode"] == "tree" {
        let mut tree = Trie::new(MemoryStore::default());
        for pair in rows(&row["base"]) {
            tree.put(
                &Nibbles::from_bytes(&bytes(&pair[0])),
                TrieValue::Bytes(bytes(&pair[1])),
            )
            .unwrap();
        }
        store = Store::new(tree);
    } else {
        store.put_map(raw_map(&row["base"]));
    }
    Backend(store)
}
fn metadata(j: &J) -> Metadata {
    rows(j)
        .iter()
        .map(|row| {
            (
                string(&row[0]).parse().unwrap(),
                MapMetadata {
                    raw_id: row[1].as_str().map(|v| v.parse().unwrap()),
                    refcount: string(&row[2]).parse().unwrap(),
                    size: row[3].as_str().map(|v| v.parse().unwrap()),
                },
            )
        })
        .collect()
}
fn plan(row: &J) -> WritePlan {
    WritePlan {
        old_metadata: metadata(&row["old_meta"]),
        metadata: metadata(&row["meta"]),
        updates: rows(&row["updates"])
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
            .collect(),
        gc_cache: rows(&row["gc"])
            .iter()
            .map(|p| (string(&p[0]).parse().unwrap(), raw_map(&p[1])))
            .collect::<GcCache>(),
    }
}
fn pairs(map: &RawMap) -> J {
    json!(
        map.iter()
            .map(|(k, v)| [hex::encode(k), hex::encode(v)])
            .collect::<Vec<_>>()
    )
}
fn root(map: &RawMap) -> J {
    let mut tree = Trie::new(MemoryStore::default());
    for (key, value) in map {
        if !value.is_empty() {
            tree.put(&Nibbles::from_bytes(key), TrieValue::Bytes(value.clone()))
                .unwrap();
        }
    }
    json!(tree.root_hash().map(|h| hex::encode(h.0)))
}
#[test]
fn historical_write_plans_match_original_gas_caches_and_store_roots() {
    assert_eq!(FIXTURE["write_byte_gas"], 5);
    for (i, row) in rows(&FIXTURE["rows"]).iter().enumerate() {
        let backend = raw(row);
        let before = backend.0.contents().unwrap();
        let result = plan(row).write(
            version(row),
            &backend,
            string(&row["gas"]).parse().unwrap(),
            &ReferenceOrder(&row["orders"]),
            Limits::default(),
        );
        let actual = match result {
            Ok(written) => {
                let contents = written.store.0.contents().unwrap();
                json!({"gas":written.gas.to_string(),"writes":pairs(written.store.0.write_cache()),
                    "reads":pairs(written.store.0.read_cache()),"contents":pairs(&contents),"root":root(&contents)})
            }
            Err(StoreError::OutOfGas) => json!({"out_of_gas":true}),
            Err(StoreError::Data(Error::ResourceLimit)) => {
                panic!("unexpected resource limit at {i}")
            }
            Err(e) => {
                assert_ne!(e, StoreError::InvalidOrder, "{i}");
                json!({"exception":true})
            }
        };
        assert_eq!(
            actual, row["result"],
            "row {i}, {} {} gas {}",
            row["module"], row["name"], row["gas"]
        );
        assert_eq!(
            backend.0.contents().unwrap(),
            before,
            "input changed at {i}"
        );
    }
}
fn copy_row() -> &'static J {
    rows(&FIXTURE["rows"])
        .iter()
        .find(|r| {
            r["module"] == "aefa_stores"
                && r["mode"] == "tree"
                && r["name"] == "copy"
                && r["gas"] == "1000000000"
                && r["updates"][0][2][0] == "store_map"
                && r["result"]["gas"].is_string()
        })
        .unwrap()
}
#[test]
fn write_limits_abort_without_publishing_and_allow_larger_retry() {
    let row = copy_row();
    let backend = raw(row);
    let before = backend.0.contents().unwrap();
    for limits in [
        Limits {
            max_nodes: 1,
            ..Limits::default()
        },
        Limits {
            max_bytes: 1,
            ..Limits::default()
        },
    ] {
        let result = plan(row).write(
            Version::Arcus,
            &backend,
            1_000_000_000.into(),
            &ReferenceOrder(&row["orders"]),
            limits,
        );
        assert!(matches!(
            result,
            Err(StoreError::Data(Error::ResourceLimit))
        ));
        assert_eq!(backend.0.contents().unwrap(), before);
        assert!(backend.0.write_cache().is_empty());
        assert!(backend.0.read_cache().is_empty());
    }
    let result = plan(row)
        .write(
            Version::Arcus,
            &backend,
            1_000_000_000.into(),
            &ReferenceOrder(&row["orders"]),
            Limits::default(),
        )
        .unwrap();
    assert_eq!(result.gas.to_string(), string(&row["result"]["gas"]));
}
#[test]
fn arcus_bounds_subtree_reads_before_work_and_discards_staged_writes_on_failure() {
    use std::{cell::Cell, rc::Rc};
    #[derive(Clone)]
    struct Probe {
        inner: Backend,
        calls: Rc<Cell<usize>>,
        cap: Rc<Cell<usize>>,
        fail: bool,
    }
    impl StoreBackend for Probe {
        type Error = StateError;
        fn get(&self, key: &[u8]) -> Result<Vec<u8>, Self::Error> {
            self.inner.get(key)
        }
        fn get_cached(&mut self, key: &[u8]) -> Result<Vec<u8>, Self::Error> {
            self.inner.get_cached(key)
        }
        fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
            self.inner.put(key, value);
        }
    }
    impl SubtreeBackend for Probe {
        fn subtree(
            &self,
            prefix: &[u8],
            maximum: usize,
        ) -> Result<Option<(RawMap, usize)>, Self::Error> {
            self.calls.set(self.calls.get() + 1);
            self.cap.set(maximum);
            if self.fail {
                Err(StateError::InvalidValue)
            } else {
                self.inner.subtree(prefix, maximum)
            }
        }
    }
    let row = copy_row();
    let mut backend = Probe {
        inner: raw(row),
        calls: Rc::new(Cell::new(0)),
        cap: Rc::new(Cell::new(0)),
        fail: false,
    };
    let result = plan(row).write(
        Version::Arcus,
        &backend,
        99.into(),
        &ReferenceOrder(&row["orders"]),
        Limits::default(),
    );
    assert!(matches!(result, Err(StoreError::OutOfGas)));
    assert_eq!(backend.calls.get(), 0);
    let result = plan(row).write(
        Version::Arcus,
        &backend,
        109.into(),
        &ReferenceOrder(&row["orders"]),
        Limits::default(),
    );
    assert!(matches!(result, Err(StoreError::OutOfGas)));
    assert_eq!(backend.cap.get(), 0);
    backend.fail = true;
    let before = backend.inner.0.contents().unwrap();
    let mut p = plan(row);
    p.updates.insert(
        0,
        Update::Push {
            position: 1.into(),
            value: Value::Integer(99.into()),
        },
    );
    let result = p.write(
        Version::Arcus,
        &backend,
        1_000_000.into(),
        &ReferenceOrder(&row["orders"]),
        Limits::default(),
    );
    assert!(matches!(
        result,
        Err(StoreError::Backend(StateError::InvalidValue))
    ));
    assert_eq!(backend.inner.0.contents().unwrap(), before);
    assert!(backend.inner.0.write_cache().is_empty());
}
