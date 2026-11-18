use ae_vm_fate::{
    types::Type,
    value::{ObjectKind, ObjectPayload, Value},
};
use serde_json::Value as Json;
pub fn rows(v: &Json) -> &[Json] {
    v.as_array().unwrap()
}
pub fn string(v: &Json) -> &str {
    v.as_str().unwrap()
}
pub fn bytes(v: &Json) -> Vec<u8> {
    hex::decode(string(v)).unwrap()
}
pub fn index(v: &Json) -> usize {
    v.as_u64().unwrap() as usize
}
pub fn kind(v: &str) -> ObjectKind {
    match v {
        "address" => ObjectKind::Address,
        "contract" => ObjectKind::Contract,
        "oracle" => ObjectKind::Oracle,
        "oracle_query" => ObjectKind::OracleQuery,
        "channel" => ObjectKind::Channel,
        _ => panic!("{v}"),
    }
}
pub fn ty(v: &Json) -> Type {
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
        "arity" => Type::Arity(
            vs[1]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| vs[1].to_string())
                .parse()
                .unwrap(),
        ),
        "list" => Type::List(Box::new(ty(&vs[1]))),
        "tuple" => Type::Tuple(rows(&vs[1]).iter().map(ty).collect()),
        "variant" => Type::Variant(rows(&vs[1]).iter().map(ty).collect()),
        "map" => Type::Map(Box::new(ty(&vs[1])), Box::new(ty(&vs[2]))),
        _ => panic!("{v}"),
    }
}
pub fn payload(v: &Json) -> ObjectPayload {
    if v.is_string() {
        ObjectPayload::Bytes(bytes(v))
    } else {
        ObjectPayload::List(rows(v).iter().map(payload).collect())
    }
}
pub fn value(v: &Json) -> Value {
    let vs = rows(v);
    match string(&vs[0]) {
        "tombstone" => Value::Tombstone,
        "void" => Value::Void,
        "max_ttl" => Value::MaxTtl,
        "integer" => Value::Integer(string(&vs[1]).parse().unwrap()),
        "bits" => Value::Bits(string(&vs[1]).parse().unwrap()),
        "bits_nil" => Value::BitsNil,
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
        "store_map" => Value::StoreMap {
            id: string(&vs[1]).parse().unwrap(),
            cache: vs
                .get(2)
                .map(|v| {
                    rows(v)
                        .iter()
                        .map(|p| (value(&p[0]), value(&p[1])))
                        .collect()
                })
                .unwrap_or_default(),
        },
        "type" => Value::TypeRep(ty(&vs[1])),
        "variant" => Value::Variant {
            arities: rows(&vs[1])
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| v.to_string())
                        .parse()
                        .unwrap()
                })
                .collect(),
            tag: index(&vs[2]),
            values: rows(&vs[3]).iter().map(value).collect(),
        },
        _ => panic!("{v}"),
    }
}
