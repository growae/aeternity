use ae_stratum::{
    share::{Rejection, Response},
    wire::{
        json as wire_json,
        rpc::{self, Error, MaybeId, Options},
    },
};
use num_bigint::BigInt;
use serde_json::{Number, Value, json};
use std::str::FromStr;

fn normalized(t: &Value) -> Value {
    if let Some(s) = t.get("atom").and_then(Value::as_str) {
        return match s {
            "null" => Value::Null,
            "true" => json!(true),
            "false" => json!(false),
            _ => json!(s),
        };
    }
    if let Some(s) = t.get("binary").and_then(Value::as_str) {
        return json!(String::from_utf8(hex::decode(s).unwrap()).unwrap());
    }
    if let Some(s) = t.get("integer").and_then(Value::as_str) {
        return Value::Number(Number::from_str(s).unwrap());
    }
    if let Some(s) = t.get("float").and_then(Value::as_str) {
        return json!(f64::from_be_bytes(
            hex::decode(s).unwrap().try_into().unwrap()
        ));
    }
    if let Some(a) = t
        .get("list")
        .or_else(|| t.get("tuple"))
        .and_then(Value::as_array)
    {
        return Value::Array(a.iter().map(normalized).collect());
    }
    if let Some(a) = t.get("map").and_then(Value::as_array) {
        return Value::Object(
            a.iter()
                .map(|pair| {
                    (
                        normalized(&pair[0]).as_str().unwrap().to_owned(),
                        normalized(&pair[1]),
                    )
                })
                .collect(),
        );
    }
    panic!("unsupported native term {t}")
}
fn options(t: &Value) -> Options {
    let mut o = Options::default();
    let n = normalized(t);
    for (k, v) in n.as_object().unwrap() {
        o.set(k, BigInt::from_str(&v.to_string()).unwrap()).unwrap();
    }
    o
}
fn id(v: MaybeId) -> Value {
    match v {
        MaybeId::Null => Value::Null,
        MaybeId::Undefined => json!("undefined"),
        MaybeId::Integer(i) => serde_json::from_str(&i.to_string()).unwrap(),
    }
}
fn error(e: Error) -> Value {
    match e {
        Error::Parse => json!(["error", "parse_error"]),
        Error::InvalidMessage(i) => json!(["error", ["invalid_msg", id(i)]]),
        Error::InvalidMethod(i) => json!(["error", ["invalid_method", id(i)]]),
        Error::InvalidParam(p, i) => json!(["error", ["invalid_param", p, id(i)]]),
        Error::Internal(i) => json!(["error", ["internal_error", id(i)]]),
        Error::UnsupportedResponseMethod => json!(["exception", "error", "function_clause"]),
        other => panic!("unexpected {other:?}"),
    }
}
fn outcome(r: Result<Value, Error>) -> Value {
    match r {
        Ok(v) => json!(["ok", v]),
        Err(e) => error(e),
    }
}
#[test]
fn all_original_protocol_operations_match() {
    let corpus: Value =
        serde_json::from_str(include_str!("fixtures/erlang_stratum_wire.json")).unwrap();
    let rows = corpus.as_array().unwrap();
    assert_eq!(rows.len(), 1711);
    let mut counts = std::collections::BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        let args = row["args"]["list"].as_array().unwrap();
        let op = row["op"].as_str().unwrap();
        *counts.entry(op).or_insert(0) += 1;
        let mut expected = normalized(&row["result"]);
        let actual = match op {
            "decode" => outcome(rpc::decode(
                &hex::decode(args[0]["binary"].as_str().unwrap()).unwrap(),
                &options(&args[1]),
            )),
            "encode" => {
                if expected[0] == "ok" {
                    let bytes =
                        hex::decode(row["result"]["tuple"][1]["binary"].as_str().unwrap()).unwrap();
                    assert_eq!(bytes.last(), Some(&b'\n'));
                    expected[1] = wire_json::decode(&bytes, Default::default()).unwrap();
                }
                let o = options(&args[1]);
                match rpc::encode(&normalized(&args[0]), &o) {
                    Ok(bytes) => {
                        assert_eq!(bytes.last(), Some(&b'\n'));
                        json!(["ok", wire_json::decode(&bytes, o.limits).unwrap()])
                    }
                    Err(e) => error(e),
                }
            }
            "validate_rsp" => outcome(rpc::validate_response(
                normalized(&args[0]).as_str().unwrap(),
                &normalized(&args[1]),
                &options(&args[2]),
            )),
            "next_id" => {
                let n = BigInt::from_str(args[0]["integer"].as_str().unwrap()).unwrap();
                serde_json::from_str(&rpc::next_id(&n, &options(&args[1])).unwrap().to_string())
                    .unwrap()
            }
            "to_id" => rpc::to_id(&normalized(&args[0]), &options(&args[1]))
                .map_or(Value::Null, |n| {
                    serde_json::from_str(&n.to_string()).unwrap()
                }),
            _ => panic!("unknown op {op}"),
        };
        assert_eq!(
            actual, expected,
            "case {index}, {op}, {}, args {}",
            row["group"], row["args"]
        );
    }
    assert_eq!(
        counts,
        [
            ("decode", 1091),
            ("encode", 268),
            ("validate_rsp", 257),
            ("next_id", 44),
            ("to_id", 51)
        ]
        .into_iter()
        .collect()
    );
}

#[test]
fn work_responses_and_protocol_errors_encode_with_native_codes() {
    let o = Options::default();
    let id = BigInt::from(7);
    for (response, result, code) in [
        (Response::Result(true), Some(true), None),
        (Response::Result(false), Some(false), None),
        (
            Response::Error(Rejection::UnknownError, Some("invalid_solution")),
            None,
            Some(20),
        ),
        (
            Response::Error(Rejection::JobNotFound, None),
            None,
            Some(21),
        ),
        (
            Response::Error(Rejection::DuplicateShare, None),
            None,
            Some(22),
        ),
        (
            Response::Error(Rejection::LowDifficultyShare, None),
            None,
            Some(23),
        ),
        (
            Response::Error(Rejection::UnauthorizedWorker, None),
            None,
            Some(24),
        ),
    ] {
        let m = rpc::share_response(&id, response).unwrap();
        let b = rpc::encode(&m, &o).unwrap();
        let wire: Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(wire["id"], 7);
        if let Some(result) = result {
            assert_eq!(wire["result"], result);
            assert!(wire["error"].is_null());
        } else {
            assert_eq!(wire["error"][0], code.unwrap());
        }
        let raw = rpc::decode(&b, &o).unwrap();
        let checked = rpc::validate_response("submit", &raw, &o).unwrap();
        assert_eq!(rpc::encode(&checked, &o).unwrap(), b);
    }
    for (error, code, data, response_id) in [
        (Error::Parse, -32700, None, Value::Null),
        (
            Error::InvalidMessage(MaybeId::Undefined),
            -32000,
            None,
            Value::Null,
        ),
        (
            Error::InvalidMethod(MaybeId::Integer(7.into())),
            -32601,
            None,
            json!(7),
        ),
        (
            Error::InvalidParam("pow", MaybeId::Integer(7.into())),
            -32602,
            Some("pow"),
            json!(7),
        ),
        (Error::Internal(MaybeId::Null), -32603, None, Value::Null),
        (
            Error::InvalidMessage(MaybeId::Integer((-1).into())),
            -32000,
            None,
            Value::Null,
        ),
    ] {
        let m = rpc::error_response(&error, &o).unwrap();
        let b = rpc::encode(&m, &o).unwrap();
        let v: Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(v["id"], response_id);
        assert_eq!(v["error"][0], code);
        assert_eq!(v["error"][2], json!(data));
    }
    assert_eq!(rpc::error_response(&Error::Option, &o), Err(Error::Option));
}

#[test]
fn numeric_domains_options_and_resource_limits_are_explicit() {
    let mut o = Options::default();
    assert_eq!(o.set("unknown", 1.into()), Err(Error::Option));
    assert_eq!(
        o.set("id_max", BigInt::from(1) << 16384),
        Err(Error::Option)
    );
    assert_eq!(
        rpc::next_id(&(BigInt::from(1) << 16384), &o),
        Err(Error::Resource(wire_json::Error::Limit("integer")))
    );
    o.set("id_max", (-1).into()).unwrap();
    assert_eq!(
        rpc::next_id(&((BigInt::from(1) << 16384) - 1), &o),
        Err(Error::Resource(wire_json::Error::Limit("integer")))
    );
    o = Options::default();
    let floats = json!(vec![1.0; 42]);
    assert!(rpc::proof(&floats).is_none());
    assert_eq!(rpc::proof(&json!(vec![0; 42])), Some([0; 42]));
    assert!(rpc::proof(&json!(vec![-1; 42])).is_none());
    assert!(rpc::proof(&json!(vec![4294967296u64; 42])).is_none());
    assert!(rpc::proof(&json!([0])).is_none());
    let mut text_limits = Options::default();
    text_limits.limits.string_bytes = 64;
    text_limits.set("password_max_size", 60.into()).unwrap();
    let input = json!({"jsonrpc":"2.0","id":1,"method":"mining.authorize",
        "params":["ak_zrHosArjUGvPYvg9WoT5KNEoJmoJEsXfaDu7TQLHtod5mJS7Y.a", "İ".repeat(30)]});
    assert_eq!(
        rpc::decode(&serde_json::to_vec(&input).unwrap(), &text_limits),
        Err(Error::Resource(wire_json::Error::Limit("string_bytes")))
    );
    o.set("id_max", BigInt::from(1) << 64).unwrap();
    let n = rpc::to_id(&serde_json::from_str("18446744073709551616").unwrap(), &o).unwrap();
    assert_eq!(n, BigInt::from(1) << 64);
    assert!(rpc::to_id(&json!(1.0), &o).is_none());
    o.limits.bytes = 16;
    assert_eq!(
        rpc::decode(b"{'jsonrpc':'2.0','id':1}", &o),
        Err(Error::Resource(wire_json::Error::Limit("bytes")))
    );
    assert_eq!(
        rpc::encode(
            &json!({"type":"rsp","id":1,"method":"submit","result":true}),
            &o
        ),
        Err(Error::Resource(wire_json::Error::Limit("bytes")))
    );
    assert_eq!(
        rpc::validate_response("submit", &json!({"type":"rsp","id":1,"result":true}), &o),
        Err(Error::Resource(wire_json::Error::Limit("bytes")))
    );
    let o = Options::default();
    assert_eq!(
        rpc::validate_response(&"a".repeat(1025), &json!({}), &o),
        Err(Error::Resource(wire_json::Error::Limit("method")))
    );
}
