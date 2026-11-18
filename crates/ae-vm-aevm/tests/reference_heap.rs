use ae_vm_aevm::heap::{self, Error, Limits, Type, Value};
use num_bigint::{BigInt, BigUint};
use serde_json::Value as Json;
use std::collections::BTreeMap;

fn bytes(v: &Json) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn ty(t: &Json) -> Type {
    if let Some(s) = t.as_str() {
        return match s {
            "word" => Type::Word,
            "signed_word" => Type::SignedWord,
            "bool" => Type::Bool,
            "string" => Type::String,
            "typerep" => Type::TypeRep,
            "function" => Type::Function,
            "contract_bytearray" => Type::ContractBytearray,
            _ => panic!("unknown type {t}"),
        };
    }
    match t[0].as_str().unwrap() {
        "list" => Type::List(Box::new(ty(&t[1]))),
        "option" => Type::Option(Box::new(ty(&t[1]))),
        "tuple" => Type::Tuple(t[1].as_array().unwrap().iter().map(ty).collect()),
        "variant" => Type::Variant(
            t[1].as_array()
                .unwrap()
                .iter()
                .map(|c| c.as_array().unwrap().iter().map(ty).collect())
                .collect(),
        ),
        "map" => Type::Map(Box::new(ty(&t[1])), Box::new(ty(&t[2]))),
        _ => panic!("unknown type {t}"),
    }
}
fn value(v: &Json) -> Value {
    match v[0].as_str().unwrap() {
        "integer" => Value::Integer(v[1].as_str().unwrap().parse().unwrap()),
        "boolean" => Value::Boolean(v[1].as_bool().unwrap()),
        "string" => Value::String(bytes(&v[1])),
        "contract_bytearray" => Value::ContractBytearray(bytes(&v[1])),
        "tuple" => Value::Tuple(v[1].as_array().unwrap().iter().map(value).collect()),
        "list" => Value::List(v[1].as_array().unwrap().iter().map(value).collect()),
        "option" => Value::Option((!v[1].is_null()).then(|| Box::new(value(&v[1])))),
        "variant" => Value::Variant {
            tag: v[1].as_u64().unwrap() as usize,
            values: v[2].as_array().unwrap().iter().map(value).collect(),
        },
        "type" => Value::Type(ty(&v[1])),
        "map" => Value::Map(
            v[1].as_array()
                .unwrap()
                .iter()
                .map(|p| (value(&p[0]), value(&p[1])))
                .collect::<BTreeMap<_, _>>(),
        ),
        _ => panic!("unknown value {v}"),
    }
}
#[test]
fn heap_encoding_decoding_canonicality_and_base_addresses_match_original_erlang() {
    let corpus: Json = serde_json::from_str(include_str!("fixtures/erlang_heap.json")).unwrap();
    let limits = Limits::default();
    for r in corpus["encode"].as_array().unwrap() {
        let actual = heap::encode_at(
            &value(&r["value"]),
            &r["base"].as_str().unwrap().parse::<BigUint>().unwrap(),
            limits,
        );
        if r["accepted"] == true {
            assert_eq!(actual.unwrap(), bytes(&r["heap"]), "{r}");
        } else {
            assert!(actual.is_err(), "{r}");
        }
    }
    let (mut accepted, mut canonical) = (0, 0);
    for r in corpus["decode"].as_array().unwrap() {
        let t = ty(&r["type"]);
        let b = bytes(&r["heap"]);
        let expected = &r["decoded"];
        let actual = heap::decode(&t, &b, limits);
        if expected["accepted"] == true {
            accepted += 1;
            let v = actual.unwrap_or_else(|e| panic!("{r}: {e:?}"));
            assert_eq!(v, value(&expected["value"]), "{r}");
            let encoded = heap::encode(&v, limits);
            if expected["canonical"].is_null() {
                assert!(encoded.is_err(), "{r}");
            } else {
                assert_eq!(encoded.unwrap(), bytes(&expected["canonical"]), "{r}");
            }
            let is_canonical =
                !expected["canonical"].is_null() && bytes(&expected["canonical"]) == b;
            assert_eq!(
                heap::validate_value(&t, &b, limits).is_ok(),
                is_canonical,
                "{r}"
            );
            if t == Type::TypeRep {
                assert_eq!(heap::validate_type(&b, limits).is_ok(), is_canonical, "{r}");
            }
            canonical += usize::from(is_canonical);
        } else {
            assert!(actual.is_err(), "{r}");
            assert_ne!(
                actual,
                Err(Error::ResourceLimit),
                "bounded reference case {r}"
            );
            if expected["error"] == "circular_references" {
                assert_eq!(actual, Err(Error::CircularReferences), "{r}");
            }
        }
    }
    for r in corpus["bases"].as_array().unwrap() {
        let actual = heap::decode_at(
            &ty(&r["type"]),
            &bytes(&r["heap"]),
            r["base"].as_str().unwrap().parse().unwrap(),
            limits,
        );
        let expected = &r["decoded"];
        if expected["accepted"] == true {
            assert_eq!(actual.unwrap(), value(&expected["value"]), "{r}");
        } else {
            assert!(actual.is_err(), "{r}");
        }
    }
    assert_eq!((accepted, canonical), (536, 200));
    assert_eq!(corpus["encode"].as_array().unwrap().len(), 306);
    assert_eq!(corpus["decode"].as_array().unwrap().len(), 1324);
    assert_eq!(corpus["bases"].as_array().unwrap().len(), 245);
}
#[test]
fn resource_exhaustion_stays_distinct_from_malformed_data() {
    let value = Value::List((0..100).map(|n| Value::Integer(BigInt::from(n))).collect());
    let binary = heap::encode(&value, Limits::default()).unwrap();
    let limits = Limits {
        max_nodes: 10,
        ..Limits::default()
    };
    assert_eq!(
        heap::decode(&Type::List(Box::new(Type::Word)), &binary, limits),
        Err(Error::ResourceLimit)
    );
    assert_eq!(heap::encode(&value, limits), Err(Error::ResourceLimit));
    assert_eq!(
        heap::decode(&Type::Word, &[], Limits::default()),
        Err(Error::BinaryTooShort)
    );
    assert_eq!(
        heap::decode_at(&Type::Word, &binary, usize::MAX, Limits::default()),
        Err(Error::ResourceLimit)
    );
    let large = Value::String(vec![1; 1024]);
    assert_eq!(
        heap::encode(
            &large,
            Limits {
                max_bytes: 128,
                ..Limits::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        heap::decode(
            &Type::List(Box::new(Type::Word)),
            &binary,
            Limits::default()
        )
        .unwrap(),
        value
    );
}
