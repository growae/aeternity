#![allow(dead_code)]
use ae_vm_aevm::{
    maps::{Bytes, Data, Map, Maps, Store},
    runtime_heap::{Heap, Type, TypeRef},
};
use num_bigint::{BigInt, BigUint};
use serde_json::{Value as J, json};
use std::sync::Arc;
pub fn expand(v: &J, pool: &[J], ceiling: usize) -> J {
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
pub fn fixture(text: &str) -> J {
    let p: J = serde_json::from_str(text).unwrap();
    let pool = p["values"].as_array().unwrap();
    expand(&p["root"], pool, pool.len())
}
pub fn logical(mut v: J) -> J {
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
pub fn int(n: impl ToString) -> J {
    json!(["integer", n.to_string()])
}
pub fn atom(s: &str) -> J {
    json!(["atom", s])
}
pub fn binary(b: impl AsRef<[u8]>) -> J {
    json!(["binary", hex::encode(b)])
}
pub fn list(v: Vec<J>) -> J {
    json!(["list", v])
}
pub fn tuple(v: Vec<J>) -> J {
    json!(["tuple", v])
}
pub fn map(v: Vec<(J, J)>) -> J {
    json!([
        "map",
        v.into_iter()
            .map(|(k, v)| json!([k, v]))
            .collect::<Vec<_>>()
    ])
}
pub fn field<'a>(v: &'a J, key: &str) -> &'a J {
    &v[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom(key))
        .unwrap()[1]
}
pub fn uint(v: &J) -> BigUint {
    v[1].as_str().unwrap().parse().unwrap()
}
pub fn number(v: &J) -> BigInt {
    v[1].as_str().unwrap().parse().unwrap()
}
pub fn bytes(v: &J) -> Vec<u8> {
    assert_eq!(v[0], "binary");
    hex::decode(v[1].as_str().unwrap()).unwrap()
}
pub fn ty(v: &J) -> TypeRef {
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
pub fn type_term(t: &Type) -> J {
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
pub fn pmap(v: &J) -> Map {
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
pub fn pmap_term(m: &Map) -> J {
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
pub fn maps(v: &J) -> Maps {
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
pub fn maps_term(m: &Maps) -> J {
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
pub fn heap(v: &J) -> Heap {
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
pub fn heap_term(h: &Heap) -> J {
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
pub fn store(v: &J) -> Store {
    v[1].as_array()
        .unwrap()
        .iter()
        .map(|p| (bytes(&p[0]), bytes(&p[1])))
        .collect()
}
pub fn captured_bytes(v: &J) -> Vec<u8> {
    assert_eq!(v["kind"], "ok");
    let a = v["value"][1].as_array().unwrap();
    assert_eq!(a[0], atom("ok"));
    bytes(&a[1])
}
pub fn captured_heap(v: &J) -> Heap {
    assert_eq!(v["kind"], "ok");
    let a = v["value"][1].as_array().unwrap();
    assert_eq!(a[0], atom("ok"));
    heap(&a[1])
}
pub fn word(n: u64) -> Bytes {
    let mut b = vec![0; 32];
    b[24..].copy_from_slice(&n.to_be_bytes());
    Arc::from(b)
}
