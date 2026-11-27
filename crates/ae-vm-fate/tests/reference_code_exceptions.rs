use ae_vm_fate::{
    Limits,
    code::{Code, NativeFailure},
    diagnostic::{self, Term},
    maps::MapOrder,
    native::Class,
};
use serde_json::{Value as J, json};
#[path = "support/otp_order.rs"]
mod otp_order;

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
fn json_term(t: &Term<'_>) -> J {
    match t {
        Term::Atom(a) => json!(["atom", a]),
        Term::Integer(n) => json!(["integer", n.to_string()]),
        Term::FloatBits(_) => panic!("FATE code exception terms cannot contain floats"),
        Term::Binary(b) => json!(["binary", hex::encode(b)]),
        Term::Tuple(v) => json!(["tuple", v.iter().map(json_term).collect::<Vec<_>>()]),
        Term::List(v) => json!(["list", v.iter().map(json_term).collect::<Vec<_>>()]),
        Term::Map(v) => json!([
            "map",
            v.iter()
                .map(|(k, v)| json!([json_term(k), json_term(v)]))
                .collect::<Vec<_>>()
        ]),
    }
}
// Erlang maps compare by key/value contents; traversal is a formatting concern.
fn logical(mut t: J) -> J {
    if let Some(m) = t.as_object_mut() {
        for v in m.values_mut() {
            *v = logical(v.take());
        }
    }
    if let Some(a) = t.as_array_mut() {
        for v in a.iter_mut() {
            *v = logical(v.take());
        }
        if a.first().and_then(J::as_str) == Some("map") {
            a[1].as_array_mut().unwrap().sort_by_key(J::to_string);
        }
    }
    t
}
fn failure(e: NativeFailure, order: &impl MapOrder) -> J {
    let exception = e
        .exception
        .unwrap_or_else(|| panic!("operational: {:?}", e.error));
    let reason = diagnostic::native_term(&exception.reason, order, limits())
        .unwrap()
        .0;
    json!({"kind":"exception","class":match exception.class {Class::Error=>"error",Class::Exit=>"exit"},"reason":json_term(&reason)})
}
fn outcome(r: Result<Vec<u8>, NativeFailure>, order: &impl MapOrder) -> J {
    match r {
        Ok(v) => json!({"kind":"ok","value":hex::encode(v)}),
        Err(e) => failure(e, order),
    }
}
fn expected(mut v: J) -> J {
    v.as_object_mut().unwrap().remove("origin");
    logical(v)
}
fn limits() -> Limits {
    Limits {
        max_depth: 512,
        ..Limits::default()
    }
}
#[test]
fn native_bytecode_decode_reencoding_and_vm5_preparation_match_original_failure_terms() {
    let packed: J =
        serde_json::from_str(include_str!("fixtures/erlang_fate_code_exceptions.json")).unwrap();
    let pool = packed["values"].as_array().unwrap();
    let data = expand(&packed["root"], pool, pool.len());
    let profile: J = serde_json::from_str(include_str!("fixtures/erlang_fate_stack.json")).unwrap();
    let order = otp_order::profile(&profile["profile"]);
    let rows = data["cases"].as_array().unwrap();
    assert_eq!(rows.len(), 15076);
    for (i, row) in rows.iter().enumerate() {
        let bytes = hex::decode(row["bytes"].as_str().unwrap()).unwrap();
        match Code::decode_native_bounded(&bytes, &order, limits()) {
            Err(e) => assert_eq!(
                logical(failure(e, &order)),
                expected(row["result"].clone()),
                "decode {i}"
            ),
            Ok((mut code, _)) => {
                assert_eq!(row["result"]["kind"], "ok", "decode {i}");
                assert_eq!(
                    logical(outcome(code.encode_native(&order, limits()), &order)),
                    expected(row["result"]["canonical"].clone()),
                    "encode {i}"
                );
                let prepared = code
                    .strip_init_native(limits())
                    .and_then(|remaining| code.encode_native(&order, remaining));
                assert_eq!(
                    logical(outcome(prepared, &order)),
                    expected(row["result"]["vm5_prepared"].clone()),
                    "prepare {i}"
                );
            }
        }
    }
}

#[test]
fn native_function_map_order_metadata_precedence_and_partial_headers_match_original() {
    let data: J =
        serde_json::from_str(include_str!("fixtures/erlang_fate_code_order.json")).unwrap();
    let order = otp_order::profile(&data["profile"]);
    assert_eq!(data["cases"].as_array().unwrap().len(), 75);
    for row in data["cases"].as_array().unwrap() {
        let bytes = hex::decode(row["bytes"].as_str().unwrap()).unwrap();
        let e = Code::decode_native_bounded(&bytes, &order, limits()).unwrap_err();
        assert_eq!(
            logical(failure(e, &order)),
            expected(row["result"].clone()),
            "{}",
            row["case_id"]
        );
    }
}

#[test]
fn native_failure_payload_limits_and_deep_decoding_are_retryable() {
    let packed: J =
        serde_json::from_str(include_str!("fixtures/erlang_fate_code_exceptions.json")).unwrap();
    let pool = packed["values"].as_array().unwrap();
    let data = expand(&packed["root"], pool, pool.len());
    let profile: J = serde_json::from_str(include_str!("fixtures/erlang_fate_stack.json")).unwrap();
    let order = otp_order::profile(&profile["profile"]);
    let rows = data["cases"].as_array().unwrap();
    for category in [
        "badmatch",
        "bad_opcode",
        "none_unique_encoding",
        "map_as_key_in_map",
        "unknown_map_serialization_format",
        "too_large_tag_in_variant",
    ] {
        let row = rows
            .iter()
            .find(|r| {
                r["result"]["reason"][0] == "tuple"
                    && r["result"]["reason"][1][0][1] == category
                    && r["bytes"].as_str().unwrap().len() < 1024
            })
            .unwrap();
        let bytes = hex::decode(row["bytes"].as_str().unwrap()).unwrap();
        for byte_budget in [false, true] {
            let mut reached = false;
            for n in 0..8192 {
                let cap = if byte_budget {
                    Limits {
                        max_bytes: n,
                        ..limits()
                    }
                } else {
                    Limits {
                        max_nodes: n,
                        ..limits()
                    }
                };
                let e = Code::decode_native_bounded(&bytes, &order, cap).unwrap_err();
                if e.exception.is_none() {
                    assert_eq!(
                        e.error,
                        ae_vm_fate::code::CodeError::Data(ae_vm_fate::Error::ResourceLimit)
                    );
                } else {
                    assert_eq!(logical(failure(e, &order)), expected(row["result"].clone()));
                    reached = true;
                    break;
                }
            }
            assert!(reached, "{category}");
        }
    }
    // These eight inputs exceed the default operational depth; a larger
    // allowance reaches the original badarg without a stack-size override.
    for index in [3579, 3580, 5857, 5858, 14896, 14897, 14941, 14942] {
        let row = &rows[index];
        let bytes = hex::decode(row["bytes"].as_str().unwrap()).unwrap();
        let e = Code::decode_native_bounded(&bytes, &order, Limits::default()).unwrap_err();
        assert!(e.exception.is_none());
        assert_eq!(
            e.error,
            ae_vm_fate::code::CodeError::Data(ae_vm_fate::Error::ResourceLimit)
        );
        let e = Code::decode_native_bounded(&bytes, &order, limits()).unwrap_err();
        assert_eq!(logical(failure(e, &order)), expected(row["result"].clone()));
    }
}
