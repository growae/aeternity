use ae_codec::object::{FieldType, Value};
use ae_tx::{TransactionKind, WireTransaction};
use ae_types::{Id, Unsigned};
use serde_json::Value as Json;

fn value(kind: &FieldType, json: &Json) -> Value {
    match kind {
        FieldType::Integer => {
            Value::Integer(Unsigned::from_decimal(json.as_str().unwrap()).unwrap())
        }
        FieldType::Binary => Value::Binary(hex::decode(json.as_str().unwrap()).unwrap()),
        FieldType::Id => {
            Value::Id(Id::from_bytes(&hex::decode(json.as_str().unwrap()).unwrap()).unwrap())
        }
        FieldType::List(k) => Value::List(
            json.as_array()
                .unwrap()
                .iter()
                .map(|v| value(k, v))
                .collect(),
        ),
        FieldType::Tuple(ks) => Value::Tuple(
            ks.iter()
                .zip(json.as_array().unwrap())
                .map(|(k, v)| value(k, v))
                .collect(),
        ),
        _ => panic!("unexpected transaction field type"),
    }
}
#[test]
fn all_32_historical_schemas_match_original_erlang_serializers() {
    let fixtures: Json =
        serde_json::from_str(include_str!("fixtures/erlang_schemas.json")).unwrap();
    assert_eq!(fixtures.as_array().unwrap().len(), 96);
    for row in fixtures.as_array().unwrap() {
        let bytes = hex::decode(row["encoded"].as_str().unwrap()).unwrap();
        let tx = WireTransaction::decode(&bytes).unwrap();
        assert_eq!(tx.version(), row["version"].as_u64().unwrap());
        let schema = tx.kind().schema(tx.version()).unwrap();
        let expected: Vec<_> = schema
            .iter()
            .zip(row["fields"].as_array().unwrap())
            .map(|(field, v)| {
                assert_eq!(field.name, v[0].as_str().unwrap());
                (field.name.clone(), value(&field.kind, &v[2]))
            })
            .collect();
        assert_eq!(tx.fields(), expected);
        assert_eq!(tx.encode(), bytes);
        assert_eq!(
            WireTransaction::new(tx.kind(), tx.version(), expected).unwrap(),
            tx
        );
    }
    for kind in TransactionKind::ALL {
        assert!(kind.schema(0).is_none());
        assert!(kind.schema(3).is_none());
    }
}
