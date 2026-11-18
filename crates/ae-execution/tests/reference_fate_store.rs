use ae_execution::fate_store::Backend;
use ae_state::contracts::{Error as StateError, store::Store};
use ae_trie::{MemoryStore, Nibbles, Trie, Value as TrieValue};
use ae_vm_fate::{
    Error, Limits, encoding,
    store::{self, Gas, RegisterRead, StoreError, Stores, Version},
    value::Value,
};
use serde_json::{Value as J, json};
use std::{collections::BTreeMap, sync::LazyLock};

static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_store_reads.json")).unwrap()
});
const KEY: [u8; 32] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
];
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn value(j: &J) -> Value {
    encoding::decode(&bytes(j), Limits::default()).unwrap()
}
fn encode(v: &Value) -> String {
    hex::encode(encoding::encode(v, Limits::default()).unwrap())
}
fn gas(j: &J) -> Gas {
    Gas::Metered(j.as_str().unwrap().parse().unwrap())
}
fn gas_json(g: &Gas) -> J {
    match g {
        Gas::Unmetered => J::Null,
        Gas::Metered(n) => json!(n.to_string()),
    }
}
fn pairs(m: &BTreeMap<Vec<u8>, Vec<u8>>) -> J {
    json!(
        m.iter()
            .map(|(k, v)| [hex::encode(k), hex::encode(v)])
            .collect::<Vec<_>>()
    )
}
fn raw(base: &J, mode: &str) -> Backend {
    let mut trie = Trie::new(MemoryStore::default());
    let mut writes = BTreeMap::new();
    for pair in base.as_array().unwrap().iter().rev() {
        if mode == "tree" {
            trie.put(
                &Nibbles::from_bytes(&bytes(&pair[0])),
                TrieValue::Bytes(bytes(&pair[1])),
            )
            .unwrap();
        } else {
            writes.insert(bytes(&pair[0]), bytes(&pair[1]));
        }
    }
    let mut store = Store::new(trie);
    store.put_map(writes);
    Backend(store)
}
fn snapshot(stores: &Stores<Backend>) -> J {
    let entry = stores.entry(&KEY).unwrap();
    let raw = &entry.backend().0;
    json!([{
        "key":hex::encode(KEY),"dirty":entry.dirty(),
        "terms":entry.terms().iter().map(|(n,(v,d))|json!([n.to_string(),encode(v),d])).collect::<Vec<_>>(),
        "writes":pairs(raw.write_cache()),"reads":pairs(raw.read_cache()),
        "root":raw.tree().root_hash().map(|h|hex::encode(h.0)),
    }])
}
fn execute(
    op: &J,
    stores: &mut Stores<Backend>,
    raw: &Backend,
    limits: Limits,
) -> Result<J, StoreError<StateError>> {
    let name = op[0].as_str().unwrap();
    Ok(match name {
        "find" | "load" => {
            let position = op[1].as_str().unwrap().parse().unwrap();
            let read = if name == "find" {
                stores.find_value(&KEY, &position, gas(&op[2]), limits)?
            } else {
                stores.load_register(&KEY, &position, gas(&op[2]), limits)?
            };
            match read {
                RegisterRead::Missing => json!({"missing":true}),
                RegisterRead::Cached(v) => json!({"cached":encode(&v)}),
                RegisterRead::Loaded { value, gas, .. } => {
                    json!({"value":encode(&value),"gas":gas_json(&gas)})
                }
            }
        }
        "lookup" => {
            let (v, g) = stores.map_lookup(
                &KEY,
                &op[1].as_str().unwrap().parse().unwrap(),
                &value(&op[2]),
                gas(&op[3]),
                limits,
            )?;
            json!({"value":v.as_ref().map(encode),"gas":gas_json(&g)})
        }
        "member" => {
            let (present, g) = stores.map_member(
                &KEY,
                &op[1].as_str().unwrap().parse().unwrap(),
                &value(&op[2]),
                gas(&op[3]),
                limits,
            )?;
            json!({"member":present,"gas":gas_json(&g)})
        }
        "size" => {
            json!({"size":encode(&stores.map_size(&KEY,&op[1].as_str().unwrap().parse().unwrap(),limits)?)})
        }
        "put" => {
            stores.put_value(
                &KEY,
                op[1].as_str().unwrap().parse().unwrap(),
                value(&op[2]),
            )?;
            json!({"ok":true})
        }
        "reload" => {
            stores.put_contract(KEY, raw.clone());
            json!({"ok":true})
        }
        "cache_meta" => {
            stores.cache_metadata(&KEY, limits)?;
            json!({"ok":true})
        }
        _ => panic!("{op}"),
    })
}
fn outcome(result: Result<J, StoreError<StateError>>) -> J {
    match result {
        Ok(j) => j,
        Err(StoreError::OutOfGas) => json!({"out_of_gas":true}),
        Err(StoreError::Data(Error::ResourceLimit)) => panic!("unexpected operational limit"),
        Err(_) => json!({"exception":true}),
    }
}
#[test]
fn fate_storage_keys_and_initial_metadata_match_erlang() {
    for row in FIXTURE["initial"].as_array().unwrap() {
        let initialized = store::initialize(Backend(Store::default()), Limits::default()).unwrap();
        assert_eq!(pairs(initialized.0.write_cache()), row["writes"]);
    }
    for row in FIXTURE["register_keys"].as_array().unwrap() {
        assert_eq!(
            json!(
                store::store_key(&row["position"].as_str().unwrap().parse().unwrap())
                    .ok()
                    .map(hex::encode)
            ),
            row["bytes"]
        );
    }
    for row in FIXTURE["map_keys"].as_array().unwrap() {
        assert_eq!(
            hex::encode(
                store::map_data_key(
                    &row["raw_id"].as_str().unwrap().parse().unwrap(),
                    &value(&row["key"]),
                    Limits::default()
                )
                .unwrap()
            ),
            row["bytes"]
        );
    }
    assert_eq!(
        store::read_gas_cost(0).to_string(),
        FIXTURE["gas"]["base"].to_string()
    );
    assert_eq!(
        (store::read_gas_cost(1) - store::read_gas_cost(0)).to_string(),
        FIXTURE["gas"]["byte"].to_string()
    );
}
#[test]
fn historical_register_map_and_cache_traces_match_original_modules() {
    for (i, trace) in FIXTURE["traces"].as_array().unwrap().iter().enumerate() {
        let backend = raw(&trace["base"], trace["mode"].as_str().unwrap());
        let mut stores = Stores::new(Version::for_protocol(
            trace["protocol"].as_u64().unwrap() as u32
        ));
        stores.put_contract(KEY, backend.clone());
        for (j, step) in trace["steps"].as_array().unwrap().iter().enumerate() {
            let result = outcome(execute(
                &step["op"],
                &mut stores,
                &backend,
                Limits::default(),
            ));
            assert_eq!(result, step["result"], "trace {i} step {j}: {}", step["op"]);
            assert_eq!(
                snapshot(&stores),
                step["state"],
                "state trace {i} step {j}: {}",
                step["op"]
            );
        }
    }
}
#[test]
fn gas_position_and_corrupt_value_boundaries_match_erlang() {
    for (i, row) in FIXTURE["boundaries"].as_array().unwrap().iter().enumerate() {
        let position = row["position"].as_str().unwrap().parse().unwrap();
        let key = store::store_key(&position).unwrap_or(vec![0, 1]);
        let base = json!([["0000", "2f00"], [hex::encode(key), row["bytes"]]]);
        let backend = raw(&base, "writes");
        let mut stores = Stores::new(Version::for_protocol(
            row["protocol"].as_u64().unwrap() as u32
        ));
        stores.put_contract(KEY, backend.clone());
        let op = json!(["load", row["position"], row["gas"]]);
        assert_eq!(
            outcome(execute(&op, &mut stores, &backend, Limits::default())),
            row["result"],
            "boundary {i}"
        );
        assert_eq!(snapshot(&stores), row["state"], "boundary state {i}");
    }
}
#[test]
fn operational_abort_and_backend_failure_leave_caches_retryable() {
    let trace = &FIXTURE["traces"][0];
    let backend = raw(&trace["base"], "tree");
    let mut stores = Stores::new(Version::Arcus);
    stores.put_contract(KEY, backend.clone());
    let before = snapshot(&stores);
    let op = json!(["lookup", "0", "00", "100000"]);
    assert_eq!(
        execute(
            &op,
            &mut stores,
            &backend,
            Limits {
                max_nodes: 1,
                ..Limits::default()
            }
        ),
        Err(StoreError::Data(Error::ResourceLimit))
    );
    assert_eq!(snapshot(&stores), before);
    let expected = execute(&op, &mut stores, &backend, Limits::default()).unwrap();
    assert!(expected["value"].is_string());
    let before = snapshot(&stores);
    // Metadata is now cached; fail while decoding the map value after its
    // byte read, and ensure that staged byte-cache changes are discarded.
    let nested = json!(["lookup", "0", "02", "100000"]);
    assert_eq!(
        execute(
            &nested,
            &mut stores,
            &backend,
            Limits {
                max_nodes: 1,
                ..Limits::default()
            }
        ),
        Err(StoreError::Data(Error::ResourceLimit))
    );
    assert_eq!(snapshot(&stores), before);
    assert!(
        execute(&nested, &mut stores, &backend, Limits::default()).unwrap()["value"].is_string()
    );
    // A separate contract has an independent decoded cache and dirty state.
    stores.put_contract([9; 32], backend.clone());
    stores
        .put_value(&[9; 32], 1.into(), Value::Integer(99.into()))
        .unwrap();
    assert!(!stores.entry(&KEY).unwrap().dirty());
    assert!(stores.entry(&[9; 32]).unwrap().dirty());
    assert_eq!(
        stores
            .find_value(&[9; 32], &1.into(), Gas::Unmetered, Limits::default())
            .unwrap(),
        RegisterRead::Cached(Value::Integer(99.into()))
    );

    #[derive(Clone)]
    struct Failing;
    impl store::Backend for Failing {
        type Error = ();
        fn get(&self, _: &[u8]) -> Result<Vec<u8>, ()> {
            Err(())
        }
        fn get_cached(&mut self, _: &[u8]) -> Result<Vec<u8>, ()> {
            Err(())
        }
        fn put(&mut self, _: Vec<u8>, _: Vec<u8>) {}
    }
    let mut stores = Stores::new(Version::Arcus);
    assert!(!stores.has_contract(&KEY));
    assert!(matches!(
        stores.find_value(&KEY, &1.into(), Gas::Unmetered, Limits::default()),
        Err(StoreError::MissingContract)
    ));
    stores.put_contract(KEY, Failing);
    assert_eq!(
        stores.find_value(&KEY, &1.into(), Gas::Unmetered, Limits::default()),
        Err(StoreError::Backend(()))
    );
    assert!(stores.entry(&KEY).unwrap().terms().is_empty());
    assert!(!stores.entry(&KEY).unwrap().dirty());
    stores
        .put_value(&KEY, 1.into(), Value::Boolean(true))
        .unwrap();
    assert_eq!(
        stores.find_value(
            &KEY,
            &1.into(),
            Gas::Metered((-1).into()),
            Limits::default()
        ),
        Ok(RegisterRead::Cached(Value::Boolean(true)))
    );
}

#[test]
fn activated_protocol_floor_applies_only_to_loaded_arcus_registers() {
    let fixture: J =
        serde_json::from_str(include_str!("fixtures/erlang_fate_store_floors.json")).unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        for run in case["runs"].as_array().unwrap() {
            let mut stores = Stores::new(Version::for_protocol(
                case["protocol"].as_u64().unwrap() as u32
            ));
            stores.put_contract(KEY, raw(&case["base"], case["mode"].as_str().unwrap()));
            for (gas, expected, state) in [
                (gas(&run["gas"]), &run["result"], &run["state"]),
                (
                    Gas::Metered((-1).into()),
                    &run["retry"],
                    &run["retry_state"],
                ),
            ] {
                let result = stores
                    .load_register_with_floor(
                        &KEY,
                        &1.into(),
                        gas,
                        case["floor"].as_u64().map(|n| n as u32),
                        Limits::default(),
                    )
                    .map(|read| match read {
                        RegisterRead::Missing => json!({"missing":true}),
                        RegisterRead::Cached(v) => json!({"cached":encode(&v)}),
                        RegisterRead::Loaded { value, gas, .. } => {
                            json!({"value":encode(&value),"gas":gas_json(&gas)})
                        }
                    });
                assert_eq!(
                    outcome(result),
                    *expected,
                    "protocol={} floor={} kind={} gas={}",
                    case["protocol"],
                    case["floor"],
                    case["kind"],
                    run["gas"]
                );
                assert_eq!(snapshot(&stores), *state);
            }
        }
    }
}
