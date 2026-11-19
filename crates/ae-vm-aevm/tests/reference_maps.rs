use ae_vm_aevm::{
    execution::Limits,
    maps::{Bytes, Data, Entries, Map, Maps, Store},
    runtime_heap::{self, Error, Heap, MapSerialization, Type, TypeRef},
    store,
};
use num_bigint::{BigInt, BigUint};
use serde_json::{Value as J, json};
use std::sync::{Arc, LazyLock};
fn expand(v: &J, pool: &[J], ceiling: usize) -> J {
    match v {
        J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
            let i = m["$ref"].as_u64().unwrap() as usize;
            assert!(i < ceiling);
            expand(&pool[i], pool, i)
        }
        J::Object(m) => J::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), expand(v, pool, ceiling)))
                .collect(),
        ),
        J::Array(a) => J::Array(a.iter().map(|v| expand(v, pool, ceiling)).collect()),
        _ => v.clone(),
    }
}
fn fixture(text: &str) -> J {
    let p: J = serde_json::from_str(text).unwrap();
    let pool = p["values"].as_array().unwrap();
    expand(&p["root"], pool, pool.len())
}
static DATA: LazyLock<J> = LazyLock::new(|| fixture(include_str!("fixtures/erlang_maps.json")));
static STORED: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_store_reads.json")));
static EXECUTION: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_map_execution.json")));
static BOUNDARIES: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_heap_boundaries.json")));
static NESTED: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_store_nested.json")));
static STORE_EXECUTION: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_store_execution.json")));
fn logical(mut v: J) -> J {
    if let Some(m) = v.as_object_mut() {
        for v in m.values_mut() {
            *v = logical(v.take());
        }
    }
    if let Some(a) = v.as_array_mut() {
        for v in a.iter_mut() {
            *v = logical(v.take());
        }
        if a.first().and_then(J::as_str) == Some("map") {
            a[1].as_array_mut().unwrap().sort_by_key(J::to_string);
        }
    }
    v
}
fn int(n: impl ToString) -> J {
    json!(["integer", n.to_string()])
}
fn atom(s: &str) -> J {
    json!(["atom", s])
}
fn binary(b: impl AsRef<[u8]>) -> J {
    json!(["binary", hex::encode(b)])
}
fn list(v: Vec<J>) -> J {
    json!(["list", v])
}
fn tuple(v: Vec<J>) -> J {
    json!(["tuple", v])
}
fn map(v: Vec<(J, J)>) -> J {
    json!([
        "map",
        v.into_iter()
            .map(|(k, v)| json!([k, v]))
            .collect::<Vec<_>>()
    ])
}
fn field<'a>(v: &'a J, key: &str) -> &'a J {
    &v[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom(key))
        .unwrap()[1]
}
fn uint(v: &J) -> BigUint {
    v[1].as_str().unwrap().parse().unwrap()
}
fn number(v: &J) -> BigInt {
    v[1].as_str().unwrap().parse().unwrap()
}
fn bytes(v: &J) -> Vec<u8> {
    assert_eq!(v[0], "binary");
    hex::decode(v[1].as_str().unwrap()).unwrap()
}
fn ty(v: &J) -> TypeRef {
    let t = if v[0] == "atom" {
        match v[1].as_str().unwrap() {
            "word" => Type::Word,
            "string" => Type::String,
            "typerep" => Type::TypeRep,
            "function" => Type::Function,
            "contract_bytearray" => Type::ContractBytearray,
            s => panic!("{s}"),
        }
    } else {
        let a = v[1].as_array().unwrap();
        match a[0][1].as_str().unwrap() {
            "list" => Type::List(ty(&a[1])),
            "map" => Type::Map(ty(&a[1]), ty(&a[2])),
            "tuple" => Type::Tuple(a[1][1].as_array().unwrap().iter().map(ty).collect()),
            "variant" => Type::Variant(
                a[1][1]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v[1].as_array().unwrap().iter().map(ty).collect())
                    .collect(),
            ),
            s => panic!("{s}"),
        }
    };
    Arc::new(t)
}
fn type_term(t: &Type) -> J {
    match t {
        Type::Word => atom("word"),
        Type::String => atom("string"),
        Type::TypeRep => atom("typerep"),
        Type::Function => atom("function"),
        Type::ContractBytearray => atom("contract_bytearray"),
        Type::List(t) => tuple(vec![atom("list"), type_term(t)]),
        Type::Map(k, v) => tuple(vec![atom("map"), type_term(k), type_term(v)]),
        Type::Tuple(ts) => tuple(vec![
            atom("tuple"),
            list(ts.iter().map(|t| type_term(t)).collect()),
        ]),
        Type::Variant(cs) => tuple(vec![
            atom("variant"),
            list(
                cs.iter()
                    .map(|ts| list(ts.iter().map(|t| type_term(t)).collect()))
                    .collect(),
            ),
        ]),
    }
}
fn pmap(v: &J) -> Map {
    let a = v[1].as_array().unwrap();
    assert_eq!(a[0], atom("pmap"));
    Map {
        key_type: ty(&a[1]),
        value_type: ty(&a[2]),
        parent: if a[3] == atom("none") {
            None
        } else {
            Some(uint(&a[3]))
        },
        size: number(&a[4]),
        data: if a[5] == atom("stored") {
            Data::Stored
        } else {
            Data::Local(
                a[5][1]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|p| {
                        (
                            Arc::from(bytes(&p[0])),
                            if p[1] == atom("tombstone") {
                                None
                            } else {
                                Some(Arc::from(bytes(&p[1])))
                            },
                        )
                    })
                    .collect(),
            )
        },
    }
}
fn pmap_term(m: &Map) -> J {
    tuple(vec![
        atom("pmap"),
        type_term(&m.key_type),
        type_term(&m.value_type),
        m.parent.as_ref().map_or(atom("none"), int),
        int(&m.size),
        match &m.data {
            Data::Stored => atom("stored"),
            Data::Local(d) => map(d
                .iter()
                .map(|(k, v)| (binary(k), v.as_ref().map_or(atom("tombstone"), binary)))
                .collect()),
        },
    ])
}
fn maps(v: &J) -> Maps {
    let a = v[1].as_array().unwrap();
    assert_eq!(a[0], atom("maps"));
    Maps {
        entries: a[1][1]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| (uint(&p[0]), pmap(&p[1])))
            .collect(),
        next_id: uint(&a[2]),
    }
}
fn maps_term(m: &Maps) -> J {
    tuple(vec![
        atom("maps"),
        map(m
            .entries
            .iter()
            .map(|(id, m)| (int(id), pmap_term(m)))
            .collect()),
        int(&m.next_id),
    ])
}
fn heap(v: &J) -> Heap {
    let a = v[1].as_array().unwrap();
    let h = &a[1][1];
    assert_eq!(h[0], atom("heap"));
    Heap {
        pointer: uint(&a[0]),
        maps: maps(&h[1]),
        offset: uint(&h[2]),
        memory: bytes(&h[3]),
    }
}
fn heap_term(h: &Heap) -> J {
    tuple(vec![
        int(&h.pointer),
        tuple(vec![
            atom("heap"),
            maps_term(&h.maps),
            int(&h.offset),
            binary(&h.memory),
        ]),
    ])
}
fn store(v: &J) -> Store {
    v[1].as_array()
        .unwrap()
        .iter()
        .map(|p| (bytes(&p[0]), bytes(&p[1])))
        .collect()
}
fn captured_bytes(v: &J) -> Vec<u8> {
    assert_eq!(v["kind"], "ok");
    let a = v["value"][1].as_array().unwrap();
    assert_eq!(a[0], atom("ok"));
    bytes(&a[1])
}
fn captured_heap(v: &J) -> Heap {
    assert_eq!(v["kind"], "ok");
    let a = v["value"][1].as_array().unwrap();
    assert_eq!(a[0], atom("ok"));
    heap(&a[1])
}
fn word(n: u64) -> Bytes {
    let mut b = vec![0; 32];
    b[24..].copy_from_slice(&n.to_be_bytes());
    Arc::from(b)
}

#[test]
fn native_binary_runtime_maps_and_relocations_match_original() {
    let rows = DATA["local"].as_array().unwrap();
    assert_eq!(rows.len(), 48);
    for row in rows {
        let t = ty(&row["type"]);
        let input = bytes(&row["input"]);
        let actual = runtime_heap::binary_to_heap(
            &t,
            &input,
            row["next_id"].as_u64().unwrap().into(),
            row["offset"].as_u64().unwrap().into(),
            Limits::default(),
        )
        .unwrap();
        assert_eq!(
            logical(heap_term(&actual)),
            logical(row["heap"].clone()),
            "{row}"
        );
        for moved in row["moved"].as_array().unwrap() {
            let result = runtime_heap::heap_to_heap(
                &t,
                &actual,
                moved["offset"].as_u64().unwrap().into(),
                None,
                Limits::default(),
            )
            .unwrap();
            assert_eq!(result, captured_heap(&moved["result"]));
        }
        for (profile, name) in [
            (MapSerialization::Legacy, "old_return"),
            (MapSerialization::Corrected, "new_return"),
        ] {
            assert_eq!(
                runtime_heap::heap_to_binary(
                    &t,
                    &actual,
                    &Store::new(),
                    profile,
                    None,
                    Limits::default()
                )
                .unwrap(),
                captured_bytes(&row[name])
            );
        }
        assert_eq!(
            runtime_heap::binary_to_binary(&t, &input, Limits::default()).unwrap(),
            captured_bytes(&row["new_return"])
        );
    }
}

#[test]
fn stored_map_loading_and_versioned_nested_serialization_match_original() {
    let rows = DATA["stored"].as_array().unwrap();
    assert_eq!(rows.len(), 32);
    let mut differences = 0;
    for row in rows {
        let t = ty(&row["type"]);
        let s = store(&row["store"]);
        let actual = store::load(&s, Limits::default()).unwrap();
        assert_eq!(actual, heap(&row["heap"]));
        assert_eq!(
            store::state_type(&s, Limits::default()).unwrap(),
            Some(ty(&row["stored_type"]))
        );
        let old = runtime_heap::heap_to_binary(
            &t,
            &actual,
            &s,
            MapSerialization::Legacy,
            None,
            Limits::default(),
        )
        .unwrap();
        let new = runtime_heap::heap_to_binary(
            &t,
            &actual,
            &s,
            MapSerialization::Corrected,
            None,
            Limits::default(),
        )
        .unwrap();
        assert_eq!(old, captured_bytes(&row["old_return"]));
        assert_eq!(new, captured_bytes(&row["new_return"]));
        differences += usize::from(old != new);
    }
    assert_eq!(differences, 8);
    let mut count = 0;
    for row in STORED["updates"]
        .as_array()
        .unwrap()
        .iter()
        .chain(STORED["competing_updates"].as_array().unwrap())
    {
        count += 1;
        assert_eq!(row["result"]["kind"], "ok");
        let v = &row["result"]["value"];
        let s = store(field(v, "contents"));
        let h = store::load(&s, Limits::default()).unwrap();
        assert_eq!(h, heap(field(v, "heap")));
        let t = store::state_type(&s, Limits::default()).unwrap().unwrap();
        for (profile, name) in [
            (MapSerialization::Legacy, "return_old"),
            (MapSerialization::Corrected, "return_new"),
        ] {
            let expected = field(v, name);
            let actual = runtime_heap::heap_to_binary(&t, &h, &s, profile, None, Limits::default());
            if expected[1][0] == atom("ok") {
                assert_eq!(actual.unwrap(), bytes(&expected[1][1]));
            } else {
                assert_eq!(expected[1][1], atom("function_clause"));
                assert_eq!(actual, Err(Error::FunctionClause));
            }
        }
    }
    assert_eq!(count, 36);
}
fn snapshot(id: &BigUint, m: &Maps, s: &Store) -> J {
    let map = m.get_map(id).unwrap();
    json!({"id":id.to_string().parse::<u64>().unwrap(),"maps":maps_term(m),"size":map.size.to_string().parse::<u64>().unwrap(),
        "type":tuple(vec![type_term(&map.key_type),type_term(&map.value_type)]),
        "reads":(0..4).map(|k|json!({"key":k,"value":m.get(id,&word(k),s,Limits::default()).unwrap().as_ref().map_or(atom("false"),binary)})).collect::<Vec<_>>(),
        "flattened":{"kind":"ok","value":pmap_term(&m.flatten(id,s,Limits::default()).unwrap())}})
}
#[test]
fn native_map_updates_keep_prior_ids_parent_overlays_and_tombstones() {
    let rows = DATA["updates"].as_array().unwrap();
    assert_eq!(rows.len(), 8);
    for row in rows {
        let mut m = maps(&row["initial"]["maps"]);
        let mut id = row["initial"]["id"].as_u64().unwrap().into();
        if row["mode"] == "fresh" {
            (id, m) = Maps::default()
                .empty(
                    Arc::new(Type::Word),
                    Arc::new(Type::Word),
                    Limits::default(),
                )
                .unwrap();
        }
        let s = if row["mode"] == "fresh" {
            Store::new()
        } else {
            store(
                &DATA["stored"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|r| r["id"] == "flat" && r["vm"] == row["vm"])
                    .unwrap()["store"],
            )
        };
        assert_eq!(
            logical(snapshot(&id, &m, &s)),
            logical(row["initial"].clone())
        );
        for step in row["trace"].as_array().unwrap() {
            let a = step["operation"][1].as_array().unwrap();
            let key = word(a[1][1].as_str().unwrap().parse().unwrap());
            let value = if a[0] == atom("delete") {
                None
            } else {
                Some(word(a[2][1].as_str().unwrap().parse().unwrap()))
            };
            (id, m) = m.update(&id, key, value, &s, Limits::default()).unwrap();
            assert_eq!(
                logical(snapshot(&id, &m, &s)),
                logical(step["snapshot"].clone())
            );
        }
    }
}
#[test]
fn map_conversion_and_overlay_resource_stops_preserve_inputs_and_retry() {
    let row = DATA["stored"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == "nested" && r["vm"] == 6)
        .unwrap();
    let s = store(&row["store"]);
    let t = ty(&row["type"]);
    let h = heap(&row["heap"]);
    let before = h.clone();
    let expected = captured_bytes(&row["new_return"]);
    for dimension in 0..3 {
        let mut stopped = false;
        let mut reached = false;
        for limit in [0, 1, 2, 4, 8, 32, 128, 512, 2048, 8192, 32768, 1_000_000] {
            let mut limits = Limits::default();
            match dimension {
                0 => limits.max_work = limit,
                1 => limits.max_bytes = limit,
                _ => limits.max_depth = limit,
            };
            match runtime_heap::heap_to_binary(
                &t,
                &h,
                &s,
                MapSerialization::Corrected,
                None,
                limits,
            ) {
                Err(Error::ResourceLimit) => stopped = true,
                Ok(out) => {
                    assert_eq!(out, expected);
                    reached = true;
                    break;
                }
                other => panic!("{other:?}"),
            }
            assert_eq!(h, before);
        }
        assert!(stopped && reached);
    }
    let before_store = s.clone();
    let original = h.maps.clone();
    let id = h.pointer.clone();
    let expected = h
        .maps
        .update(&id, word(4), Some(word(100)), &s, Limits::default())
        .unwrap();
    for dimension in 0..2 {
        let mut stopped = false;
        let mut reached = false;
        for limit in [0, 1, 8, 64, 512, 4096, 32768, 1_000_000] {
            let mut limits = Limits::default();
            if dimension == 0 {
                limits.max_work = limit
            } else {
                limits.max_bytes = limit
            }
            match h.maps.update(&id, word(4), Some(word(100)), &s, limits) {
                Err(Error::ResourceLimit) => stopped = true,
                Ok(out) => {
                    assert_eq!(out, expected);
                    reached = true;
                    break;
                }
                other => panic!("{other:?}"),
            }
            assert_eq!(h.maps, original);
            assert_eq!(s, before_store);
        }
        assert!(stopped && reached);
    }
    let (id, empty) = Maps::default()
        .empty(
            Arc::new(Type::Word),
            Arc::new(Type::Word),
            Limits::default(),
        )
        .unwrap();
    assert_eq!(id, 0u8.into());
    assert_eq!(empty.entries[&id].data, Data::Local(Entries::new()));
    assert_eq!(
        empty.merged(&h.maps, Limits::default()).unwrap().next_id,
        h.maps.next_id
    );
}

fn execution_state(row: &J, snapshot: &J) -> ae_vm_aevm::execution::State {
    use ae_vm_aevm::{
        execution::{Context, State},
        gas,
        opcode::Version,
        word::Word,
    };
    let mut s = State::new(
        Version::from_id(row["vm"].as_u64().unwrap() as u32).unwrap(),
        hex::decode(row["code"].as_str().unwrap()).unwrap(),
        uint(field(snapshot, "gas")),
        gas::FloatProfile::Otp26Word64,
    );
    s.cp = field(snapshot, "cp")[1].as_str().unwrap().parse().unwrap();
    s.stack = field(snapshot, "stack")[1]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| Word::from_unsigned(&uint(n)))
        .collect();
    let m = field(snapshot, "memory");
    for p in m[1].as_array().unwrap() {
        if p[0][0] == "integer" {
            s.memory
                .store(&uint(&p[0]), Word::from_unsigned(&uint(&p[1])));
        }
    }
    let size = uint(field(m, "mem_size"));
    if size != 0u8.into() {
        s.memory.load(&(size * 32u8 - 32u8));
    }
    s.output = bytes(field(snapshot, "out"));
    s.return_data = bytes(field(snapshot, "return_data"));
    assert_eq!(field(snapshot, "logs"), &list(vec![]));
    if let Some(v) = snapshot[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom("jumpdests"))
    {
        assert_eq!(v[1], map(vec![]));
    }
    let context = &row["context"];
    let get = |key| number(field(context, key));
    s.context = Context {
        address: number(field(snapshot, "address")),
        creator: get("creator"),
        origin: get("origin"),
        caller: get("caller"),
        value: get("value"),
        gas_price: get("gas_price"),
        coinbase: get("coinbase"),
        timestamp: get("timestamp"),
        number: get("number"),
        difficulty: get("difficulty"),
        gas_limit: get("gas_limit"),
    };
    s.out_type = Some(ty(&row["out_type"]));
    s.maps = maps(&row["maps"]);
    s.map_store = store(&row["store"]);
    s
}
#[test]
fn native_map_return_gas_boundaries_and_deep_copies_match_original_interpreter() {
    use ae_vm_aevm::execution::{self, Failure, Outcome, Stop};
    let rows = EXECUTION["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 848);
    let mut success = 0;
    let mut errors = 0;
    let mut deep = 0;
    for row in rows {
        let s = execution_state(row, &row["initial"]);
        if !s.map_store.is_empty() {
            let loaded = store::load(&s.map_store, Limits::default()).unwrap();
            assert_eq!(loaded.maps, s.maps);
            assert_eq!(
                store::state_type(&s.map_store, Limits::default()).unwrap(),
                s.out_type
            );
        }
        let before = s.clone();
        if row["case_id"].as_str().unwrap().contains("deep,300") {
            deep += 1;
            assert_eq!(
                execution::evaluate(&s, Limits::default()),
                Err(Stop::ResourceLimit)
            );
            assert_eq!(s, before);
        }
        let actual = execution::evaluate(
            &s,
            Limits {
                max_depth: 2048,
                ..Limits::default()
            },
        )
        .unwrap();
        let expected = if row["result"]["kind"] == "ok" {
            success += 1;
            let mut out = execution_state(row, &row["result"]["state"]);
            out.maps = maps(&row["result"]["maps"]);
            Outcome::Complete(Box::new(out))
        } else {
            errors += 1;
            assert_eq!(row["result"]["reason"], atom("out_of_gas"));
            Outcome::Error {
                reason: Failure::OutOfGas,
                gas: row["result"]["gas"].as_str().unwrap().parse().unwrap(),
            }
        };
        assert_eq!(actual, expected, "{} VM{}", row["case_id"], row["vm"]);
        assert_eq!(s, before);
    }
    assert_eq!(deep, 2);
    assert_eq!(success + errors, 848);
    assert!(success > 100 && errors > 100);
}
#[test]
fn native_map_transport_retains_raw_keys_padding_duplicates_and_large_offsets() {
    let rows = EXECUTION["binary"].as_array().unwrap();
    assert_eq!(rows.len(), 105);
    let mut offset_errors = 0;
    for row in rows {
        let t = ty(&row["type"]);
        let bin = bytes(&row["input"]);
        let offset = row["offset"].as_str().unwrap().parse().unwrap();
        let actual = runtime_heap::binary_to_heap(
            &t,
            &bin,
            row["next_id"].as_str().unwrap().parse().unwrap(),
            offset,
            Limits::default(),
        )
        .unwrap();
        assert_eq!(actual, captured_heap(&row["heap"]), "{}", row["id"]);
        assert_eq!(
            runtime_heap::binary_to_binary(&t, &bin, Limits::default()).unwrap(),
            captured_bytes(&row["binary"])
        );
        let back = runtime_heap::heap_to_binary(
            &t,
            &actual,
            &Store::new(),
            MapSerialization::Corrected,
            None,
            Limits::default(),
        );
        let expected = &row["returned"]["value"];
        if expected[1][0] == atom("ok") {
            assert_eq!(back.unwrap(), bytes(&expected[1][1]));
        } else {
            offset_errors += 1;
            assert_eq!(expected[1][1], atom("address_before_offset"));
            assert_eq!(back, Err(Error::AddressBeforeOffset));
        }
    }
    assert_eq!(offset_errors, 3);
    let rows = BOUNDARIES["cases"].as_array().unwrap();
    assert_eq!(rows.len(), 100);
    let mut native_exceptions = 0;
    let mut returned_errors = 0;
    let mut successes = 0;
    for row in rows {
        let t = ty(&row["type"]);
        let bin = bytes(&row["input"]);
        let actual = match row["operation"].as_str().unwrap() {
            "binary_to_heap" => {
                runtime_heap::binary_to_heap(&t, &bin, 0u8.into(), 32u8.into(), Limits::default())
                    .map(|h| heap_term(&h))
            }
            "binary_to_binary" => {
                runtime_heap::binary_to_binary(&t, &bin, Limits::default()).map(binary)
            }
            other => panic!("{other}"),
        };
        let expected = if row["result"]["kind"] == "native" {
            native_exceptions += 1;
            assert_eq!(row["result"]["class"], "error");
            assert_eq!(row["result"]["reason"], atom("function_clause"));
            Err(Error::NativeFunctionClause)
        } else {
            assert_eq!(row["result"]["kind"], "ok");
            let v = &row["result"]["value"][1];
            if v[0] == atom("ok") {
                successes += 1;
                Ok(v[1].clone())
            } else {
                returned_errors += 1;
                assert_eq!(v[0], atom("error"));
                Err(match v[1][1].as_str().unwrap() {
                    "binary_too_short" => Error::BinaryTooShort,
                    "function_clause" => Error::FunctionClause,
                    "address_before_offset" => Error::AddressBeforeOffset,
                    other => panic!("{other}"),
                })
            }
        };
        assert_eq!(actual, expected, "{row}");
    }
    assert_eq!(
        (native_exceptions, returned_errors, successes),
        (50, 34, 16)
    );
}

#[test]
fn native_store_types_are_decoded_iteratively_with_retryable_limits() {
    let rows = EXECUTION["type_reads"].as_array().unwrap();
    assert_eq!(rows.len(), 11);
    for row in rows {
        assert_eq!(row["result"]["kind"], "ok");
        let expected = Some(ty(&row["result"]["value"]));
        let s = Store::from([(vec![1], bytes(&row["input"]))]);
        let before = s.clone();
        let mut reached = false;
        let mut stopped = false;
        for depth in [0, 1, 4, 16, 256, 2048] {
            match store::state_type(
                &s,
                Limits {
                    max_depth: depth,
                    ..Limits::default()
                },
            ) {
                Err(Error::ResourceLimit) => stopped = true,
                Ok(t) => {
                    assert_eq!(t, expected);
                    reached = true;
                    break;
                }
                other => panic!("{other:?}"),
            }
            assert_eq!(s, before);
        }
        assert!(reached);
        if bytes(&row["input"]).len() > 20000 {
            assert!(stopped);
            assert_eq!(
                store::state_type(&s, Limits::default()),
                Err(Error::ResourceLimit)
            );
        }
    }
    let row = DATA["stored"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == "nested")
        .unwrap();
    let s = store(&row["store"]);
    let expected = heap(&row["heap"]);
    let before = s.clone();
    for dimension in 0..2 {
        let mut stopped = false;
        let mut reached = false;
        for allowance in [0, 1, 8, 32, 256, 2048, 16384, 1_000_000] {
            let mut limits = Limits::default();
            if dimension == 0 {
                limits.max_work = allowance
            } else {
                limits.max_bytes = allowance
            }
            match store::load(&s, limits) {
                Err(Error::ResourceLimit) => stopped = true,
                Ok(h) => {
                    assert_eq!(h, expected);
                    reached = true;
                    break;
                }
                other => panic!("{other:?}"),
            }
            assert_eq!(s, before);
        }
        assert!(stopped && reached);
    }
}

fn version(row: &J) -> ae_vm_aevm::opcode::Version {
    ae_vm_aevm::opcode::Version::from_id(row["vm"].as_u64().unwrap() as u32).unwrap()
}
const ORDER: store::OrderProfile = store::OrderProfile::Otp26Word64Crc32c;
fn plan_term(p: &store::Plan, maps: &Maps) -> J {
    use store::Update;
    let updates = p
        .updates
        .iter()
        .map(|u| match u {
            Update::Copy { id } => tuple(vec![atom("new"), int(id), pmap_term(&maps.entries[id])]),
            Update::Reuse { id, parent, size } => {
                tuple(vec![atom("new_inplace"), int(id), int(parent), int(size)])
            }
            Update::Put {
                id,
                value_type,
                key,
                value,
            } => {
                let mut ts = vec![atom("insert"), int(id)];
                if p.counts_are_deltas {
                    ts.push(type_term(value_type));
                }
                ts.extend([binary(key), binary(value)]);
                tuple(ts)
            }
            Update::Delete {
                id,
                value_type,
                key,
            } => {
                let mut ts = vec![atom("delete"), int(id)];
                if p.counts_are_deltas {
                    ts.push(type_term(value_type));
                }
                ts.push(binary(key));
                tuple(ts)
            }
            Update::Collect { id } => tuple(vec![atom("gc"), int(id)]),
        })
        .collect();
    map(vec![
        (
            atom(if p.counts_are_deltas {
                "deltas"
            } else {
                "counts"
            }),
            map(p.counts.iter().map(|(k, v)| (int(k), int(v))).collect()),
        ),
        (atom("garbage"), list(p.garbage.iter().map(int).collect())),
        (atom("updates"), list(updates)),
    ])
}
#[test]
fn native_nested_store_publication_plans_and_later_gc_match() {
    let rows = NESTED["cases"].as_array().unwrap();
    assert_eq!(rows.len(), 44);
    let mut native_errors = 0;
    for row in rows {
        let before = store(&row["before"]);
        let input = heap(&row["input_heap"]);
        let v = version(row);
        let t = ty(&row["type"]);
        assert_eq!(row["result"]["kind"], "ok");
        let expected = &row["result"]["value"];
        let p = store::plan(v, ORDER, &input.maps, &before, Limits::default()).unwrap();
        assert_eq!(
            logical(plan_term(&p, &input.maps)),
            logical(field(expected, "plan").clone()),
            "{} VM{}",
            row["scenario"],
            row["vm"]
        );
        let actual = store::publish(v, ORDER, &input, &before, Limits::default()).unwrap();
        assert_eq!(
            actual,
            store(field(expected, "dirty_contents")),
            "{} VM{}",
            row["scenario"],
            row["vm"]
        );
        let committed: Store = actual.into_iter().filter(|(_, v)| !v.is_empty()).collect();
        assert_eq!(committed, store(field(expected, "committed_contents")));
        let loaded = store::load(&committed, Limits::default()).unwrap();
        assert_eq!(loaded, heap(field(expected, "reloaded")));
        for (profile, key) in [
            (MapSerialization::Legacy, "return_old"),
            (MapSerialization::Corrected, "return_new"),
        ] {
            let actual = runtime_heap::heap_to_binary(
                &t,
                &loaded,
                &committed,
                profile,
                None,
                Limits::default(),
            );
            let expected = &field(expected, key)[1];
            if expected[0] == atom("ok") {
                assert_eq!(actual.unwrap(), bytes(&expected[1]));
            } else {
                if expected[1] == atom("function_clause") {
                    assert_eq!(actual, Err(Error::FunctionClause));
                } else {
                    assert_eq!(expected[1][1][0], atom("badmatch"));
                    assert_eq!(
                        logical(expected[1][1][1].clone()),
                        logical(maps_term(&loaded.maps)[1][1].clone())
                    );
                    let Err(Error::MissingMap(id)) = actual else {
                        panic!("{actual:?}")
                    };
                    assert!(!loaded.maps.entries.contains_key(&id));
                }
            }
        }
        let empty = Heap {
            pointer: ae_vm_aevm::word::Word([255; 32]).unsigned(),
            offset: 32u8.into(),
            memory: vec![],
            maps: Maps::default(),
        };
        let final_store = store::publish(v, ORDER, &empty, &committed, Limits::default());
        let drop = &field(expected, "drop_result")[1];
        if drop[0] == atom("ok") {
            assert_eq!(final_store.unwrap(), store(&drop[1]));
        } else {
            native_errors += 1;
            assert_eq!(drop[0], atom("native"));
            assert_eq!(drop[1], atom("error"));
            assert_eq!(drop[2][1][0], atom("badkey"));
            assert_eq!(final_store, Err(Error::NativeBadKey(uint(&drop[2][1][1]))));
        }
        assert_eq!(before, store(&row["before"]));
        assert_eq!(input, heap(&row["input_heap"]));
    }
    assert_eq!(native_errors, 9);
}
#[test]
fn native_flat_store_updates_keep_exact_metadata_and_tombstones() {
    let rows = STORED["updates"].as_array().unwrap();
    assert_eq!(rows.len(), 32);
    for row in rows {
        let before = store(&row["before"]);
        let loaded = store::load(&before, Limits::default()).unwrap();
        let keep = row["keep_parent"].as_bool().unwrap();
        let id = if keep {
            BigUint::from_bytes_be(&loaded.memory[..32])
        } else {
            loaded.pointer
        };
        let op = &row["operation"][1];
        let key = Arc::from(ae_vm_aevm::word::Word::from_unsigned(&uint(&op[1])).0);
        let value = if op[0] == atom("put") {
            Some(Arc::from(
                ae_vm_aevm::word::Word::from_unsigned(&uint(&op[2])).0,
            ))
        } else {
            None
        };
        let (new, mut maps) = loaded
            .maps
            .update(&id, key, value, &before, Limits::default())
            .unwrap();
        maps.entries.retain(|k, _| k == &new || keep && k == &id);
        let input = Heap {
            pointer: if keep { 32u8.into() } else { new.clone() },
            offset: 32u8.into(),
            memory: if keep {
                [
                    ae_vm_aevm::word::Word::from_unsigned(&id).0,
                    ae_vm_aevm::word::Word::from_unsigned(&new).0,
                ]
                .concat()
            } else {
                vec![]
            },
            maps,
        };
        let actual =
            store::publish(version(row), ORDER, &input, &before, Limits::default()).unwrap();
        assert_eq!(
            actual,
            store(field(&row["result"]["value"], "contents")),
            "{row}"
        );
    }
}
#[test]
fn store_publication_resource_stops_are_atomic_and_retryable() {
    for row in NESTED["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["scenario"] == "keep_old" || r["scenario"] == "two_inner_children")
    {
        let before = store(&row["before"]);
        let input = heap(&row["input_heap"]);
        let v = version(row);
        let expected = store::publish(v, ORDER, &input, &before, Limits::default()).unwrap();
        for dimension in 0..3 {
            let mut stopped = false;
            let mut reached = false;
            for limit in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000] {
                let mut limits = Limits::default();
                match dimension {
                    0 => limits.max_work = limit,
                    1 => limits.max_bytes = limit,
                    _ => limits.max_depth = limit,
                }
                match store::publish(v, ORDER, &input, &before, limits) {
                    Err(Error::ResourceLimit) => stopped = true,
                    Ok(actual) => {
                        assert_eq!(actual, expected);
                        reached = true;
                        break;
                    }
                    other => panic!("{other:?}"),
                }
                assert_eq!(before, store(&row["before"]));
                assert_eq!(input, heap(&row["input_heap"]));
            }
            assert!(stopped && reached);
        }
    }
}

#[test]
fn native_initial_sophia_stores_match_canonical_type_and_map_bytes() {
    let rows = DATA["stored"].as_array().unwrap();
    assert_eq!(rows.len(), 32);
    for row in rows {
        let bytes = bytes(&row["input"]);
        let actual = store::initialize(version(row), ORDER, &bytes, Limits::default()).unwrap();
        assert_eq!(
            actual,
            store(&row["store"]),
            "{} VM{}",
            row["id"],
            row["vm"]
        );
    }
}

fn check_publication(row: &J, h: &Heap, before: &Store, expected: &J) {
    let v = version(row);
    let p = store::plan(v, ORDER, &h.maps, before, Limits::default()).unwrap();
    assert_eq!(
        logical(plan_term(&p, &h.maps)),
        logical(field(expected, "plan").clone()),
        "{} VM{}",
        row["id"],
        row["vm"]
    );
    let actual = store::publish(v, ORDER, h, before, Limits::default()).unwrap();
    assert_eq!(
        actual,
        store(field(expected, "contents")),
        "{} VM{}",
        row["id"],
        row["vm"]
    );
}
#[test]
fn native_wide_store_gc_order_type_encodings_and_raw_metadata_match() {
    let rows = STORE_EXECUTION["initial"].as_array().unwrap();
    assert_eq!(rows.len(), 88);
    let mut native_errors = 0;
    for row in rows {
        assert_eq!(row["result"]["kind"], "ok", "{row}");
        let expected = &row["result"]["value"];
        let input = bytes(&row["input"]);
        let deep = row["id"] == "{deep,300}";
        if deep {
            assert_eq!(
                store::initialize(version(row), ORDER, &input, Limits::default()),
                Err(Error::ResourceLimit)
            );
        }
        let before = store::initialize(
            version(row),
            ORDER,
            &input,
            Limits {
                max_depth: 2048,
                ..Limits::default()
            },
        )
        .unwrap();
        assert_eq!(
            before,
            store(field(expected, "contents")),
            "{} VM{}",
            row["id"],
            row["vm"]
        );
        let h = store::load(&before, Limits::default()).unwrap();
        assert_eq!(h, heap(field(expected, "heap")));
        for (op, input) in [
            ("keep", h),
            (
                "drop",
                Heap {
                    pointer: 0u8.into(),
                    offset: 32u8.into(),
                    memory: vec![],
                    maps: Maps::default(),
                },
            ),
        ] {
            let result = &field(expected, op)[1];
            if result[0] == atom("ok") {
                check_publication(row, &input, &before, &result[1]);
            } else {
                native_errors += 1;
                assert_eq!(result[0], atom("native"));
                assert_eq!(result[1], atom("error"));
                assert_eq!(result[2], atom("function_clause"));
                assert_eq!(
                    store::publish(version(row), ORDER, &input, &before, Limits::default()),
                    Err(Error::NativeFunctionClause)
                );
            }
        }
    }
    assert_eq!(native_errors, 12);
    let rows = STORE_EXECUTION["custom"].as_array().unwrap();
    assert_eq!(rows.len(), 16);
    for row in rows {
        assert_eq!(row["result"]["kind"], "ok");
        check_publication(
            row,
            &heap(&row["input_heap"]),
            &store(&row["before"]),
            &row["result"]["value"],
        );
        let first = &row["result"]["value"];
        let before = store(field(first, "contents"));
        let empty = Heap {
            pointer: 0u8.into(),
            offset: 32u8.into(),
            memory: vec![],
            maps: Maps::default(),
        };
        check_publication(row, &empty, &before, field(first, "drop"));
    }
}
#[test]
fn native_used_map_unions_cycles_and_type_short_circuiting_match() {
    let rows = STORE_EXECUTION["used"].as_array().unwrap();
    assert_eq!(rows.len(), 24);
    for row in rows {
        let t = ty(&row["type"]);
        let b = bytes(&row["input"]);
        let actual = store::has_maps(&t, Limits::default());
        let expected = &row["has"];
        if expected["kind"] == "ok" {
            assert_eq!(actual.unwrap(), expected["value"] == atom("true"));
        } else {
            assert_eq!(expected["class"], "error");
            assert_eq!(expected["reason"], atom("function_clause"));
            assert_eq!(actual, Err(Error::NativeFunctionClause));
        }
        let actual = store::used_maps(&t, &b, Limits::default());
        let expected = &row["result"];
        if expected["kind"] == "ok" {
            assert_eq!(
                list(actual.unwrap().iter().map(int).collect()),
                expected["value"],
                "{}",
                row["id"]
            );
        } else {
            let error = match expected["reason"][1].as_str().unwrap() {
                "function_clause" => {
                    assert_eq!(expected["class"], "error");
                    Error::NativeFunctionClause
                }
                "circular_references" => {
                    assert_eq!(expected["class"], "exit");
                    Error::CircularReferences
                }
                "address_before_offset" => {
                    assert_eq!(expected["class"], "exit");
                    Error::AddressBeforeOffset
                }
                other => panic!("{other}"),
            };
            assert_eq!(actual, Err(error), "{}", row["id"]);
        }
    }
}

#[test]
fn initial_store_construction_limits_stop_and_retry_without_partial_results() {
    for row in STORE_EXECUTION["initial"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["id"] == "map_tuple" || r["id"] == "{wide,40}")
    {
        let input = bytes(&row["input"]);
        let original = input.clone();
        let expected = store::initialize(version(row), ORDER, &input, Limits::default()).unwrap();
        for dimension in 0..3 {
            let mut stopped = false;
            let mut reached = false;
            for allowance in [
                0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 262144, 1_000_000, 16_000_000,
            ] {
                let mut limits = Limits::default();
                match dimension {
                    0 => limits.max_work = allowance,
                    1 => limits.max_bytes = allowance,
                    _ => limits.max_depth = allowance,
                }
                match store::initialize(version(row), ORDER, &input, limits) {
                    Err(Error::ResourceLimit) => stopped = true,
                    Ok(actual) => {
                        assert_eq!(actual, expected);
                        reached = true;
                        break;
                    }
                    other => panic!("{other:?}"),
                }
                assert_eq!(input, original);
            }
            assert!(stopped && reached, "{} dimension{dimension}", row["id"]);
        }
    }
}
