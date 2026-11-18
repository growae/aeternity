use ae_execution::fate_store::Backend;
use ae_state::contracts::{Error as StateError, store::Store};
use ae_trie::{MemoryStore, Nibbles, Trie, Value as TrieValue};
use ae_vm_fate::{
    Error, Limits, encoding,
    maps::{MapOrder, Traversal},
    store::{
        self, Gas, StoreError, Stores, Version,
        read::CachedSubtreeBackend,
        write::{RawMap, SubtreeBackend},
    },
    value::Value,
};
use serde_json::{Value as J, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};
#[path = "../../ae-vm-fate/tests/support/values.rs"]
mod values;
use values::{bytes, rows, string, value};
static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_store_lists.json")).unwrap()
});
const KEY: [u8; 32] = [1; 32];
struct ReferenceOrder<'a>(&'a J);
impl MapOrder for ReferenceOrder<'_> {
    fn indices(&self, keys: &[&Value], mode: Traversal, _: Limits) -> Result<Vec<usize>, Error> {
        assert_eq!(mode, Traversal::ToList);
        if keys.len() <= 1 {
            return Ok((0..keys.len()).collect());
        }
        let set: BTreeSet<_> = keys.iter().copied().collect();
        for row in rows(self.0) {
            let expected: Vec<_> = rows(&row["list"]).iter().map(value).collect();
            if expected.iter().collect::<BTreeSet<_>>() == set {
                return Ok(expected
                    .iter()
                    .map(|k| keys.iter().position(|v| *v == k).unwrap())
                    .collect());
            }
        }
        panic!("missing reference order {keys:?}");
    }
}
fn raw_map(j: &J) -> RawMap {
    rows(j)
        .iter()
        .map(|p| (bytes(&p[0]), bytes(&p[1])))
        .collect()
}
fn pairs(m: &RawMap) -> J {
    json!(
        m.iter()
            .map(|(k, v)| [hex::encode(k), hex::encode(v)])
            .collect::<Vec<_>>()
    )
}
fn encode(v: &Value) -> String {
    hex::encode(encoding::encode(v, Limits::default()).unwrap())
}
fn gas(j: &J) -> Gas {
    j.as_str()
        .map(|s| Gas::Metered(s.parse().unwrap()))
        .unwrap_or(Gas::Unmetered)
}
fn raw(row: &J) -> Backend {
    let mut s = if row["mode"] == "tree" {
        let mut tree = Trie::new(MemoryStore::default());
        for (k, v) in raw_map(&row["base"]) {
            tree.put(&Nibbles::from_bytes(&k), TrieValue::Bytes(v))
                .unwrap();
        }
        Store::new(tree)
    } else {
        let mut s = Store::default();
        s.put_map(raw_map(&row["base"]));
        s
    };
    s.put_map(raw_map(&row["overlay"]));
    Backend(s)
}
fn stores(row: &J) -> Stores<Backend> {
    let version = match string(&row["module"]) {
        "aefa_stores_lima" => Version::Lima,
        "aefa_stores_ceres" => Version::Ceres,
        "aefa_stores" => Version::Arcus,
        _ => unreachable!(),
    };
    let mut s = Stores::new(version);
    s.put_contract(KEY, raw(row));
    s
}
fn snapshot(s: &Stores<Backend>) -> J {
    let e = s.entry(&KEY).unwrap();
    let b = &e.backend().0;
    json!({"dirty":e.dirty(),"terms":e.terms().iter().map(|(p,(v,d))| json!([p.to_string(),encode(v),d])).collect::<Vec<_>>(),
        "writes":pairs(b.write_cache()),"reads":pairs(b.read_cache()),
        "subtrees":b.cached_subtrees().iter().map(hex::encode).collect::<Vec<_>>(),"root":b.tree().root_hash().map(|h|hex::encode(h.0))})
}
fn outcome(r: Result<J, StoreError<StateError>>) -> J {
    match r {
        Ok(r) => r,
        Err(StoreError::OutOfGas) => json!({"out_of_gas":true}),
        Err(StoreError::Data(Error::ResourceLimit) | StoreError::InvalidOrder) => {
            panic!("unexpected operational failure")
        }
        Err(_) => json!({"exception":true}),
    }
}
fn list(
    s: &mut Stores<Backend>,
    id: &J,
    g: &J,
    order: &impl MapOrder,
) -> Result<J, StoreError<StateError>> {
    let (values, g) = s.map_to_list(
        &KEY,
        &string(id).parse().unwrap(),
        gas(g),
        order,
        Limits::default(),
    )?;
    Ok(
        json!({"list":values.iter().map(|(k,v)| [encode(k),encode(v)]).collect::<Vec<_>>(),"gas":match g { Gas::Unmetered => J::Null, Gas::Metered(n) => json!(n.to_string()) }}),
    )
}
#[test]
fn historical_listing_order_byte_gas_and_cache_transitions_match_erlang() {
    for case in rows(&FIXTURE["cases"]) {
        let order = ReferenceOrder(&case["orders"]);
        for r in rows(&case["boundaries"]) {
            let mut s = stores(case);
            assert_eq!(
                outcome(list(&mut s, &json!("0"), &r["gas"], &order)),
                r["result"],
                "{} {} {} {}",
                case["module"],
                case["mode"],
                case["name"],
                r["gas"]
            );
            assert_eq!(snapshot(&s), r["state"]);
        }
        let mut s = stores(case);
        for r in rows(&case["steps"]) {
            let op = &r["op"];
            let result = match string(&op[0]) {
                "list" => list(&mut s, &op[1], &op[2], &order),
                "reload" => {
                    s.put_contract(KEY, raw(case));
                    Ok(json!({"ok":true}))
                }
                "cache_meta" => s
                    .cache_metadata(&KEY, Limits::default())
                    .map(|()| json!({"ok":true})),
                "put" => s
                    .put_value(
                        &KEY,
                        string(&op[1]).parse().unwrap(),
                        encoding::decode(&bytes(&op[2]), Limits::default()).unwrap(),
                    )
                    .map(|()| json!({"ok":true})),
                _ => unreachable!(),
            };
            assert_eq!(
                outcome(result),
                r["result"],
                "{} {} {} {op}",
                case["module"],
                case["mode"],
                case["name"]
            );
            assert_eq!(
                snapshot(&s),
                r["state"],
                "{} {} {} {op}",
                case["module"],
                case["mode"],
                case["name"]
            );
        }
    }
}
#[test]
fn cached_subtree_caps_bound_masked_base_data_and_publish_no_partial_cache() {
    let key = vec![1, 0, 0, 0, 5, 0];
    let prefix = &key[..5];
    let mut tree = Trie::new(MemoryStore::default());
    tree.put(&Nibbles::from_bytes(prefix), TrieValue::Bytes(vec![0]))
        .unwrap();
    tree.put(&Nibbles::from_bytes(&key), TrieValue::Bytes(vec![42; 300]))
        .unwrap();
    let mut s = Store::new(tree);
    s.put(key.clone(), vec![1]);
    assert!(s.subtree_within_bytes(prefix, 2).is_ok());
    assert!(matches!(
        s.subtree_cached_within_bytes(prefix, 2),
        Err(StateError::ResourceLimit)
    ));
    assert!(s.read_cache().is_empty());
    assert!(s.cached_subtrees().is_empty());
    let (map, n) = s.subtree_cached_within_bytes(prefix, 301).unwrap();
    assert_eq!(n, 2);
    assert_eq!(map.get(&vec![0]), Some(&vec![1]));
    assert_eq!(s.read_cache().get(&key).unwrap().len(), 300);
    let before = s.read_cache().clone();
    let markers = s.cached_subtrees().clone();
    assert!(matches!(
        s.subtree_cached_within_bytes(prefix, 2),
        Err(StateError::ResourceLimit)
    ));
    assert_eq!(s.read_cache(), &before);
    assert_eq!(s.cached_subtrees(), &markers);
    s.put(key, vec![1; 400]);
    assert!(matches!(
        s.subtree_cached_within_bytes(&[1, 0, 0, 0, 5], 301),
        Err(StateError::ResourceLimit)
    ));
    assert_eq!(s.read_cache(), &before);
    assert_eq!(s.cached_subtrees(), &markers);
    assert_eq!(
        s.subtree_cached_within_bytes(&[1, 0, 0, 0, 5], 401)
            .unwrap()
            .1,
        401
    );
}
#[derive(Clone)]
struct Traced {
    inner: Backend,
    calls: Arc<AtomicUsize>,
    fail: bool,
}
impl store::Backend for Traced {
    type Error = StateError;
    fn get(&self, k: &[u8]) -> Result<Vec<u8>, StateError> {
        self.inner.0.get(k)
    }
    fn get_cached(&mut self, k: &[u8]) -> Result<Vec<u8>, StateError> {
        self.inner.0.get_cached(k)
    }
    fn put(&mut self, k: Vec<u8>, v: Vec<u8>) {
        self.inner.0.put(k, v);
    }
}
impl SubtreeBackend for Traced {
    fn subtree(&self, p: &[u8], max: usize) -> Result<Option<(RawMap, usize)>, StateError> {
        self.inner.subtree(p, max)
    }
}
impl CachedSubtreeBackend for Traced {
    fn subtree_cached(
        &mut self,
        p: &[u8],
        max: usize,
    ) -> Result<Option<(RawMap, usize)>, StateError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.fail {
            return Err(StateError::InvalidValue);
        }
        self.inner.subtree_cached(p, max)
    }
}
#[test]
fn listing_reads_before_charging_and_discards_failed_caches_before_retry() {
    let case = rows(&FIXTURE["cases"])
        .iter()
        .find(|c| c["name"] == "ordinary" && c["mode"] == "tree" && c["module"] == "aefa_stores")
        .unwrap();
    let order = ReferenceOrder(&case["orders"]);
    let calls = Arc::new(AtomicUsize::new(0));
    for fail in [true, false] {
        let mut s = Stores::new(Version::Arcus);
        s.put_contract(
            KEY,
            Traced {
                inner: raw(case),
                calls: calls.clone(),
                fail,
            },
        );
        let before = calls.load(Ordering::Relaxed);
        let result = s.map_to_list(
            &KEY,
            &0.into(),
            Gas::Metered(0.into()),
            &order,
            Limits::default(),
        );
        assert_eq!(calls.load(Ordering::Relaxed), before + 1);
        if fail {
            assert!(matches!(
                result,
                Err(StoreError::Backend(StateError::InvalidValue))
            ));
        } else {
            assert!(matches!(result, Err(StoreError::OutOfGas)));
        }
        let e = s.entry(&KEY).unwrap();
        assert!(e.terms().is_empty());
        assert!(e.backend().inner.0.read_cache().is_empty());
        assert!(e.backend().inner.0.cached_subtrees().is_empty());
    }
    let mut s = stores(case);
    let before = snapshot(&s);
    assert!(matches!(
        s.map_to_list(
            &KEY,
            &0.into(),
            Gas::Unmetered,
            &order,
            Limits {
                max_bytes: 1,
                ..Limits::default()
            }
        ),
        Err(StoreError::Data(Error::ResourceLimit))
    ));
    assert_eq!(snapshot(&s), before);
    let listed = s
        .map_to_list(&KEY, &0.into(), Gas::Unmetered, &order, Limits::default())
        .unwrap();
    assert_eq!(listed.0.len(), 3);
}
