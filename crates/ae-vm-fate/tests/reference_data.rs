use ae_vm_fate::{
    Error, Limits, encoding,
    types::{self, Bindings, Type, Version},
    value::{self, ObjectKind, ObjectPayload, Value},
};
use serde_json::Value as Json;
use std::{cmp::Ordering, collections::BTreeMap, sync::LazyLock};
static FIXTURE: LazyLock<Json> =
    LazyLock::new(|| serde_json::from_str(include_str!("fixtures/erlang_fate_data.json")).unwrap());
fn rows(v: &Json) -> &[Json] {
    v.as_array().unwrap()
}
fn string(v: &Json) -> &str {
    v.as_str().unwrap()
}
fn bytes(v: &Json) -> Vec<u8> {
    hex::decode(string(v)).unwrap()
}
fn index(v: &Json) -> usize {
    v.as_u64().unwrap() as usize
}
fn kind(v: &str) -> ObjectKind {
    match v {
        "address" => ObjectKind::Address,
        "contract" => ObjectKind::Contract,
        "oracle" => ObjectKind::Oracle,
        "oracle_query" => ObjectKind::OracleQuery,
        "channel" => ObjectKind::Channel,
        _ => panic!("{v}"),
    }
}
fn ty(v: &Json) -> Type {
    if let Some(v) = v.as_str() {
        return match v {
            "integer" => Type::Integer,
            "boolean" => Type::Boolean,
            "bits" => Type::Bits,
            "string" => Type::String,
            "any" => Type::Any,
            "contract_bytearray" => Type::ContractBytearray,
            _ => Type::Object(kind(v)),
        };
    }
    let vs = rows(v);
    match string(&vs[0]) {
        "bytes" => Type::Bytes(vs[1].as_str().map(|s| s.parse().unwrap())),
        "variable" => Type::Variable(index(&vs[1]) as u8),
        "arity" => Type::Arity(index(&vs[1]) as u8),
        "list" => Type::List(Box::new(ty(&vs[1]))),
        "tuple" => Type::Tuple(rows(&vs[1]).iter().map(ty).collect()),
        "variant" => Type::Variant(rows(&vs[1]).iter().map(ty).collect()),
        "map" => Type::Map(Box::new(ty(&vs[1])), Box::new(ty(&vs[2]))),
        _ => panic!("{v}"),
    }
}
fn payload(v: &Json) -> ObjectPayload {
    if v.is_string() {
        ObjectPayload::Bytes(bytes(v))
    } else {
        ObjectPayload::List(rows(v).iter().map(payload).collect())
    }
}
fn value(v: &Json) -> Value {
    let vs = rows(v);
    match string(&vs[0]) {
        "integer" => Value::Integer(string(&vs[1]).parse().unwrap()),
        "bits" => Value::Bits(string(&vs[1]).parse().unwrap()),
        "boolean" => Value::Boolean(vs[1].as_bool().unwrap()),
        "string" => Value::String(bytes(&vs[1])),
        "bytes" => Value::Bytes(bytes(&vs[1])),
        "contract_bytearray" => Value::ContractBytearray(bytes(&vs[1])),
        "object" => Value::Object(kind(string(&vs[1])), payload(&vs[2])),
        "tuple" => Value::Tuple(rows(&vs[1]).iter().map(value).collect()),
        "list" => Value::List(rows(&vs[1]).iter().map(value).collect()),
        "map" => Value::Map(
            rows(&vs[1])
                .iter()
                .map(|r| (value(&r[0]), value(&r[1])))
                .collect(),
        ),
        "store_map" => Value::StoreMap(string(&vs[1]).parse().unwrap()),
        "type" => Value::TypeRep(ty(&vs[1])),
        "variant" => Value::Variant {
            arities: rows(&vs[1]).iter().map(|v| index(v) as u8).collect(),
            tag: index(&vs[2]),
            values: rows(&vs[3]).iter().map(value).collect(),
        },
        _ => panic!("{v}"),
    }
}
fn bindings(v: &Json) -> Option<Bindings> {
    if v.is_null() {
        None
    } else {
        Some(
            rows(v)
                .iter()
                .map(|r| (index(&r[0]) as u8, ty(&r[1])))
                .collect(),
        )
    }
}
fn version(v: &Json) -> Version {
    match index(v) {
        5 => Version::One,
        7 => Version::Two,
        8 => Version::Three,
        _ => panic!(),
    }
}
fn non_resource<T>(result: Result<T, Error>) -> Option<T> {
    assert_ne!(result.as_ref().err(), Some(&Error::ResourceLimit));
    result.ok()
}
#[test]
fn original_serialization_and_decoding() {
    let limits = Limits {
        max_depth: 512,
        ..Limits::default()
    };
    for row in rows(&FIXTURE["encode"]) {
        let result = non_resource(encoding::encode(&value(&row["value"]), limits));
        assert_eq!(
            result,
            row["encoded"].as_str().map(|s| hex::decode(s).unwrap()),
            "encode {row}"
        );
    }
    for row in rows(&FIXTURE["decode"]) {
        let raw = bytes(&row["bytes"]);
        let expected = &row["decoded"];
        let decoded = encoding::decode(&raw, limits);
        assert_ne!(
            decoded.as_ref().err(),
            Some(&Error::ResourceLimit),
            "decode budget {}",
            row["bytes"]
        );
        let result = non_resource(decoded);
        assert_eq!(
            result,
            if expected.is_null() {
                None
            } else {
                Some(value(&expected["value"]))
            },
            "decode {}",
            row["bytes"]
        );
        if let Some(result) = result {
            assert_eq!(
                non_resource(encoding::encode(&result, limits)),
                expected["canonical"]
                    .as_str()
                    .map(|s| hex::decode(s).unwrap()),
                "canonical {}",
                row["bytes"]
            );
            assert_eq!(
                non_resource(types::infer(&result, limits)),
                if expected["inferred"].is_null() {
                    None
                } else {
                    Some(ty(&expected["inferred"]))
                },
                "infer {}",
                row["bytes"]
            );
        }
        let expected = &row["one"];
        let result = non_resource(encoding::decode_one(&raw, limits));
        assert_eq!(
            result.map(|(v, r)| (v, r.to_vec())),
            if expected.is_null() {
                None
            } else {
                Some((value(&expected["value"]), bytes(&expected["rest"])))
            },
            "prefix {}",
            row["bytes"]
        );
    }
}
#[test]
fn original_type_serialization_and_decoding() {
    let limits = Limits::default();
    for row in rows(&FIXTURE["types"]) {
        let expected = ty(&row["type"]);
        let raw = bytes(&row["encoded"]);
        assert_eq!(encoding::encode_type(&expected, limits).unwrap(), raw);
        assert_eq!(encoding::decode_type(&raw, limits).unwrap(), expected);
    }
    for row in rows(&FIXTURE["type_decode"]) {
        let raw = bytes(&row["bytes"]);
        let expected = &row["decoded"];
        let result = non_resource(encoding::decode_type_one(&raw, limits));
        assert_eq!(
            result.as_ref().map(|(t, r)| (t.clone(), r.to_vec())),
            if expected.is_null() {
                None
            } else {
                Some((ty(&expected["type"]), bytes(&expected["rest"])))
            },
            "type prefix {row}"
        );
        if let Some((ty, rest)) = result {
            assert_eq!(
                encoding::encode_type(&ty, limits).unwrap(),
                bytes(&expected["canonical"])
            );
            assert_eq!(
                encoding::decode_type(&raw, limits),
                if rest.is_empty() {
                    Ok(ty)
                } else {
                    Err(Error::TrailingBytes)
                }
            );
        }
    }
}
#[test]
fn original_historical_inference_unification_and_instantiation() {
    let limits = Limits::default();
    let ts: Vec<_> = rows(&FIXTURE["types"])
        .iter()
        .map(|r| ty(&r["type"]))
        .collect();
    let vs: Vec<_> = rows(&FIXTURE["encode"])
        .iter()
        .map(|r| value(&r["value"]))
        .collect();
    for row in rows(&FIXTURE["checks"]) {
        assert_eq!(
            types::check_type(
                &ts[index(&row["type"])],
                &vs[index(&row["value"])],
                version(&row["version"]),
                limits
            )
            .unwrap(),
            bindings(&row["result"]),
            "check {row}"
        );
    }
    for row in rows(&FIXTURE["match"]) {
        assert_eq!(
            types::match_type(
                &ts[index(&row["expected"])],
                &ts[index(&row["actual"])],
                version(&row["version"]),
                limits
            )
            .unwrap(),
            bindings(&row["result"]),
            "match {row}"
        );
    }
    for row in rows(&FIXTURE["instantiate"]) {
        assert_eq!(
            non_resource(types::instantiate(
                &bindings(&row["bindings"]).unwrap(),
                &ts[index(&row["type"])],
                limits
            )),
            if row["result"].is_null() {
                None
            } else {
                Some(ty(&row["result"]))
            },
            "instantiate {row}"
        );
    }
}
#[test]
fn original_value_ordering() {
    let vs: Vec<_> = rows(&FIXTURE["encode"])
        .iter()
        .map(|r| value(&r["value"]))
        .collect();
    for row in rows(&FIXTURE["order"]) {
        let result = non_resource(value::compare(
            &vs[index(&row["a"])],
            &vs[index(&row["b"])],
            Limits::default(),
        ));
        assert_eq!(
            result.map(|o| o == Ordering::Less),
            row["result"].as_bool(),
            "order {row}"
        );
    }
}
#[test]
fn budgets_remain_distinct_from_malformed_data() {
    let limits = Limits {
        max_depth: 2,
        ..Limits::default()
    };
    let raw = [0x13, 0x13, 0x13, 0];
    assert_eq!(encoding::decode(&raw, limits), Err(Error::ResourceLimit));
    assert!(encoding::decode(&raw, Limits::default()).is_ok());
    let v = Value::List(vec![Value::List(vec![Value::List(vec![Value::Integer(
        0.into(),
    )])])]);
    assert_eq!(encoding::encode(&v, limits), Err(Error::ResourceLimit));
    assert_eq!(
        types::check_type(&Type::Any, &v, Version::Three, limits),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        encoding::decode(&[0x80], Limits::default()),
        Err(Error::InvalidEncoding)
    );
    assert_eq!(
        encoding::decode(
            &[0],
            Limits {
                max_nodes: 0,
                ..Limits::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        encoding::decode(
            &[0],
            Limits {
                max_bytes: 0,
                ..Limits::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        encoding::encode(
            &Value::String(vec![1; 64]),
            Limits {
                max_bytes: 64,
                ..Limits::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        encoding::encode_type(&Type::Tuple(vec![Type::Any; 256]), Limits::default()),
        Err(Error::InvalidType)
    );
    assert_eq!(
        encoding::encode_type(&Type::Arity(0), Limits::default()),
        Err(Error::InvalidType)
    );
    let mut map = BTreeMap::new();
    map.insert(Value::Integer(1.into()), Value::Integer(2.into()));
    assert_eq!(
        encoding::encode(
            &Value::Map(map),
            Limits {
                max_nodes: 1,
                ..Limits::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
}
