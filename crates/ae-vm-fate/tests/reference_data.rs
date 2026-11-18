use ae_vm_fate::{
    Error, Limits, encoding,
    types::{self, Bindings, Type, Version},
    value::{self, Value},
};
use serde_json::Value as Json;
use std::{cmp::Ordering, collections::BTreeMap, sync::LazyLock};
static FIXTURE: LazyLock<Json> =
    LazyLock::new(|| serde_json::from_str(include_str!("fixtures/erlang_fate_data.json")).unwrap());
#[path = "support/values.rs"]
mod values;
use values::*;
#[test]
fn variant_arity_expansion_obeys_node_budgets_and_retries_canonical_bytes() {
    let v = Value::Variant {
        arities: vec![0.into(); 1024],
        tag: 0,
        values: vec![],
    };
    let limits = Limits {
        max_nodes: 64,
        ..Limits::default()
    };
    assert_eq!(encoding::encode(&v, limits), Err(Error::ResourceLimit));
    assert_eq!(types::infer(&v, limits), Err(Error::ResourceLimit));
    let encoded = encoding::encode(&v, Limits::default()).unwrap();
    assert_eq!(
        encoding::decode(&encoded, limits),
        Err(Error::ResourceLimit)
    );
    assert_eq!(encoding::decode(&encoded, Limits::default()).unwrap(), v);
    assert_eq!(encoding::encode(&v, Limits::default()).unwrap(), encoded);
    assert!(types::infer(&v, Limits::default()).is_ok());
    let large = Value::Variant {
        arities: vec![0.into(), -(num_bigint::BigInt::from(1) << 8192_usize)],
        tag: 0,
        values: vec![],
    };
    assert_eq!(
        types::infer(
            &large,
            Limits {
                max_bytes: 128,
                ..Limits::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert!(types::infer(&large, Limits::default()).is_ok());
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
        encoding::encode_type(&Type::Arity(0.into()), Limits::default()),
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
