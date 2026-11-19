use ae_vm_aevm::{
    calls::{self, Return},
    execution::{Limits, State, Stop},
    gas::FloatProfile,
    heap,
    opcode::Version,
    runtime::{Fault, Native},
    runtime_heap::Error,
    word::Word,
};
use serde_json::Value as J;
use std::sync::LazyLock;
mod support;
use support::*;
static RETURNS: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_call_returns.json")));
fn state(vm: u32, snap: &J) -> State {
    let mut s = State::new(
        Version::from_id(vm).unwrap(),
        vec![0],
        uint(field(snap, "gas")),
        FloatProfile::Otp26Word64,
    );
    let memory = field(snap, "memory");
    for p in memory[1].as_array().unwrap() {
        if p[0][0] == "integer" {
            s.memory
                .store(&uint(&p[0]), Word::from_unsigned(&uint(&p[1])));
        }
    }
    let size = uint(field(memory, "mem_size"));
    if size != 0u8.into() {
        s.memory.load(&(size * 32u8 - 32u8));
    }
    s.maps = maps(field(snap, "maps"));
    if let Some(v) = snap[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom("auth_tx_hash") && p[1] != atom("undefined"))
    {
        s.auth_tx_hash = Some(Word(bytes(&v[1]).try_into().unwrap()));
    }
    s.cp = uint(field(snap, "cp")).try_into().unwrap();
    s.stack = field(snap, "stack")[1]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| Word::from_unsigned(&uint(v)))
        .collect();
    s.output = bytes(field(snap, "out"));
    s.return_data = bytes(field(snap, "return_data"));
    s
}
fn native(r: &J) -> Native {
    assert_eq!(r["kind"], "native");
    let reason = &r["reason"];
    if reason == &atom("function_clause") {
        return Native::Heap(Error::NativeFunctionClause);
    }
    let v = &reason[1];
    if v[0] == atom("aevm_eval_error") {
        assert_eq!(v[1], atom("out_of_gas"));
        assert_eq!(v[2], int(0));
        return Native::OutOfGas;
    }
    assert_eq!(v[0], atom("badmatch"));
    if v[1][0] == "binary" {
        return Native::BadMatchBinary(bytes(&v[1]));
    }
    if v[1] == tuple(vec![atom("error"), atom("not_found")]) {
        return Native::MapNotFound;
    }
    assert_eq!(v[1][1][0], atom("error"));
    let err = &v[1][1][1][1];
    assert_eq!(err[0], atom("binary_too_short"));
    Native::BadPrimopData {
        reason: heap::Error::BinaryTooShort,
        data: bytes(&err[1]),
    }
}
fn run(row: &J, kind: &str, limits: Limits) -> Result<(num_bigint::BigUint, State), Fault> {
    let vm = if kind == "solidity" {
        2
    } else {
        row["vm"].as_u64().unwrap() as u32
    };
    let input = if row["mode"] == "local" {
        hex::decode(format!(
            "{}{}",
            hex::encode(word(32)),
            hex::encode(word(301))
        ))
        .unwrap()
    } else {
        vec![]
    };
    let target = Word::from(if row["mode"] == "local" || kind == "solidity" {
        0
    } else {
        7
    });
    let t = if kind == "solidity" {
        None
    } else if kind == "malformed" {
        Some(ty(&atom("string")))
    } else {
        Some(ty(&row["type"]))
    };
    calls::return_result(
        &state(vm, &row["before"]),
        Return {
            target,
            input: &input,
            output_offset: Word::from(row["address"].as_u64().unwrap_or(7)),
            output_size: Word::from(row["size"].as_u64().unwrap_or(1)),
            data: &bytes(&row["return"]),
            ty: t.as_ref(),
        },
        limits,
    )
}
#[test]
fn original_call_results_preserve_heap_ids_offsets_gas_and_native_failures() {
    let mut success = 0;
    let mut native_count = 0;
    for (kind, n) in [("returns", 480), ("malformed", 24), ("solidity", 15)] {
        let rows = RETURNS[kind].as_array().unwrap();
        assert_eq!(rows.len(), n);
        for row in rows {
            let actual = run(row, kind, Limits::default());
            let result = &row["result"];
            if result["kind"] == "native" {
                native_count += 1;
                assert_eq!(actual, Err(Fault::Native(native(result))), "{row}");
            } else {
                success += 1;
                let v = &result["value"][1];
                let vm = if kind == "solidity" {
                    2
                } else {
                    row["vm"].as_u64().unwrap() as u32
                };
                assert_eq!(actual, Ok((uint(&v[0]), state(vm, &v[1]))), "{row}");
            }
        }
    }
    assert_eq!((success, native_count), (429, 90));
}
#[test]
fn original_local_primop_classification_and_malformed_decoding_match() {
    let rows = RETURNS["classify"].as_array().unwrap();
    assert_eq!(rows.len(), 14);
    for row in rows {
        let actual = calls::is_local_primop(&bytes(&row["input"]), Limits::default());
        let r = &row["result"];
        if r["kind"] == "native" {
            assert_eq!(actual, Err(Fault::Native(native(r))));
        } else {
            assert_eq!(actual, Ok(r["value"] == atom("true")));
        }
    }
}
#[test]
fn call_result_resource_limits_preserve_input_and_retry() {
    for row in RETURNS["returns"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["gas"] == 10000 && (r["id"] == "long_string" || r["id"] == "nested"))
    {
        let expected = run(row, "returns", Limits::default()).unwrap();
        for dim in 0..4 {
            let mut stopped = false;
            let mut reached = false;
            for n in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000] {
                let mut limits = Limits::default();
                match dim {
                    0 => limits.max_work = n,
                    1 => limits.max_bytes = n,
                    2 => limits.max_depth = n,
                    _ => limits.max_memory_words = n,
                }
                match run(row, "returns", limits) {
                    Err(Fault::Stop(Stop::ResourceLimit)) => stopped = true,
                    Ok(v) => {
                        assert_eq!(v, expected);
                        reached = true;
                        break;
                    }
                    x => panic!("{x:?}"),
                }
            }
            assert!(
                reached && (stopped || (dim == 2 && row["id"] == "long_string")),
                "{dim}: {row}"
            );
        }
    }
}

use ae_vm_aevm::{
    maps::Store,
    primops::{self, MapHost},
};
use std::cell::Cell;
static PRIMOPS: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_map_primops.json")));
struct MapSource<'a> {
    store: &'a Store,
    calls: Cell<usize>,
    fail: Cell<bool>,
}
impl MapHost for MapSource<'_> {
    type Error = &'static str;
    fn get_store(&self) -> Result<Store, Self::Error> {
        self.calls.set(self.calls.get() + 1);
        if self.fail.get() {
            Err("store read failed")
        } else {
            Ok(self.store.clone())
        }
    }
}
fn map_state(row: &J, snap: &J) -> State {
    let mut s = state(row["vm"].as_u64().unwrap() as u32, snap);
    s.map_store = store(field(snap, "storage"));
    s
}
fn primop_expected(row: &J) -> primops::Outcome {
    let r = &row["result"];
    assert_eq!(r["kind"], "ok");
    let v = &r["value"][1];
    if v[0] == atom("ok") {
        primops::Outcome::Complete {
            data: bytes(&v[1]),
            gas_spent: uint(&v[2]),
            state: Box::new(map_state(row, &v[3])),
        }
    } else {
        assert_eq!(v[0], atom("primop_error"));
        primops::Outcome::Error(match v[1][1].as_str().unwrap() {
            "illegal_primop_call" => primops::Error::IllegalCall,
            "out_of_gas" => primops::Error::OutOfGas,
            "max_size_exceeded" => primops::Error::Heap(Error::SizeLimit),
            s => panic!("{s}"),
        })
    }
}
#[test]
fn original_map_primops_preserve_versions_gas_results_and_store_reads() {
    let mut count = 0;
    let mut host_count = 0;
    for k in ["runs", "malformed"] {
        for row in PRIMOPS[k].as_array().unwrap() {
            let s = map_state(row, &row["before"]);
            let before = s.clone();
            let supplied = row
                .get("host_store")
                .map_or_else(|| s.map_store.clone(), store);
            let host = MapSource {
                store: &supplied,
                calls: Cell::new(0),
                fail: Cell::new(false),
            };
            let actual = primops::call_map(&s, &bytes(&row["data"]), &host, Limits::default());
            assert_eq!(actual, Ok(primop_expected(row)), "{row}");
            assert_eq!(
                host.calls.get(),
                row["host_calls"].as_u64().unwrap_or(0) as usize
            );
            host_count += host.calls.get();
            assert_eq!(s, before);
            count += 1;
        }
    }
    assert_eq!((count, host_count), (3872, 392));
}
#[test]
fn map_primop_limits_and_host_failures_are_operational_and_retryable() {
    for row in PRIMOPS["runs"].as_array().unwrap().iter().filter(|r| {
        r["kind"] == "nested"
            && r["stored"] == true
            && r["call_gas"] == 100
            && ["put", "get", "tolist"].iter().any(|op| r["op"] == *op)
    }) {
        let s = map_state(row, &row["before"]);
        if s.gas != 1000u16.into() {
            continue;
        }
        let before = s.clone();
        let expected = primop_expected(row);
        let data = bytes(&row["data"]);
        let host = MapSource {
            store: &s.map_store,
            calls: Cell::new(0),
            fail: Cell::new(false),
        };
        for dim in 0..3 {
            let mut stopped = false;
            let mut reached = false;
            for n in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000] {
                let mut l = Limits::default();
                match dim {
                    0 => l.max_work = n,
                    1 => l.max_bytes = n,
                    _ => l.max_depth = n,
                }
                match primops::call_map(&s, &data, &host, l) {
                    Err(Fault::Stop(Stop::ResourceLimit)) => stopped = true,
                    Ok(out) => {
                        assert_eq!(out, expected);
                        reached = true;
                        break;
                    }
                    other => panic!("{other:?}"),
                }
                assert_eq!(s, before);
            }
            assert!(stopped && reached);
        }
        if row["op"] == "tolist" {
            host.fail.set(true);
            assert_eq!(
                primops::call_map(&s, &data, &host, Limits::default()),
                Err(Fault::Host("store read failed"))
            );
            assert_eq!(s, before);
            host.fail.set(false);
            assert_eq!(
                primops::call_map(&s, &data, &host, Limits::default()),
                Ok(expected)
            );
        }
    }
}

use ae_vm_aevm::{
    calls::{Input, InputHost, Prepared},
    runtime_heap::TypeRef,
};
use std::cell::RefCell;
static INPUTS: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_call_inputs.json")));
struct TypesHost {
    calls: RefCell<Vec<(Word, Version, Word)>>,
    argument: Option<TypeRef>,
    fail: Cell<bool>,
}
impl InputHost for TypesHost {
    type Error = &'static str;
    fn function_types(
        &self,
        target: Word,
        version: Version,
        function: Word,
    ) -> Result<Option<(TypeRef, TypeRef)>, Self::Error> {
        self.calls.borrow_mut().push((target, version, function));
        if self.fail.get() {
            Err("type lookup failed")
        } else {
            Ok(self.argument.clone().map(|t| (t, ty(&atom("word")))))
        }
    }
}
fn prepared(row: &J, r: &J) -> Result<Prepared, Fault<&'static str>> {
    if r["kind"] == "native" {
        return Err(Fault::Native(native(r)));
    }
    let v = &r["value"][1];
    Ok(Prepared {
        data: bytes(&v[0]),
        output_type: (v[1] != atom("undefined")).then(|| ty(&v[1])),
        state: Box::new(map_state(row, &v[2])),
    })
}
#[test]
fn map_call_input_types_and_serialization_match_original() {
    let host = TypesHost {
        calls: RefCell::new(vec![]),
        argument: None,
        fail: Cell::new(true),
    };
    let mut count = 0;
    for row in PRIMOPS["runs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r.get("input_before").is_some())
    {
        let s = map_state(row, &row["input_before"]);
        let before = s.clone();
        let actual = calls::prepare_input(
            &s,
            Input {
                target: Word::ZERO,
                offset: Word::from_unsigned(
                    &row["input_pointer"].as_str().unwrap().parse().unwrap(),
                ),
                size: Word::from(777),
            },
            &host,
            Limits::default(),
        );
        assert_eq!(actual, prepared(row, &row["input_result"]), "{row}");
        assert_eq!(s, before);
        count += 1;
    }
    assert_eq!(count, 3840);
    assert!(host.calls.borrow().is_empty());
}
fn input_request(row: &J) -> Input {
    Input {
        target: Word::from(7),
        offset: Word::from_unsigned(&row["offset"].as_str().unwrap().parse().unwrap()),
        size: Word::from(row["size"].as_u64().unwrap_or(999)),
    }
}
#[test]
fn remote_and_solidity_input_preserve_auth_gas_and_host_order() {
    let mut count = 0;
    for k in ["runs", "solidity"] {
        for row in INPUTS[k].as_array().unwrap() {
            let s = map_state(row, &row["before"]);
            let before = s.clone();
            let arg = if k == "solidity" || row["mode"] == "missing" || row["mode"] == "host_error"
            {
                None
            } else {
                Some(ty(&row["type"]))
            };
            let host = TypesHost {
                calls: RefCell::new(vec![]),
                argument: arg,
                fail: Cell::new(false),
            };
            let result = calls::prepare_input(&s, input_request(row), &host, Limits::default());
            assert_eq!(result, prepared(row, &row["result"]), "{row}");
            let expected = row.get("host_calls").map_or(vec![], |v| {
                v[1].as_array()
                    .unwrap()
                    .iter()
                    .map(|v| {
                        let a = &v[1];
                        assert_eq!(field(&a[1], "abi"), &int(1));
                        (
                            Word(bytes(&a[0]).try_into().unwrap()),
                            Version::from_id(uint(field(&a[1], "vm")).try_into().unwrap()).unwrap(),
                            Word(bytes(&a[2]).try_into().unwrap()),
                        )
                    })
                    .collect()
            });
            assert_eq!(*host.calls.borrow(), expected);
            assert_eq!(s, before);
            count += 1;
        }
    }
    assert_eq!(count, 473);
}
#[test]
fn input_limits_and_lookup_failure_preserve_state_and_retry() {
    for row in INPUTS["runs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["mode"] == "nested" && r["gas"] == 1000 && r["auth"] == false)
    {
        let s = map_state(row, &row["before"]);
        let before = s.clone();
        let expected = prepared(row, &row["result"]).unwrap();
        let host = TypesHost {
            calls: RefCell::new(vec![]),
            argument: Some(ty(&row["type"])),
            fail: Cell::new(false),
        };
        for dim in 0..3 {
            let mut stopped = false;
            let mut reached = false;
            for n in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000] {
                let mut limits = Limits::default();
                match dim {
                    0 => limits.max_work = n,
                    1 => limits.max_bytes = n,
                    _ => limits.max_depth = n,
                }
                match calls::prepare_input(&s, input_request(row), &host, limits) {
                    Err(Fault::Stop(Stop::ResourceLimit)) => stopped = true,
                    Ok(out) => {
                        assert_eq!(out, expected);
                        reached = true;
                        break;
                    }
                    x => panic!("{x:?}"),
                }
                assert_eq!(s, before);
            }
            assert!(stopped && reached);
        }
        host.fail.set(true);
        assert_eq!(
            calls::prepare_input(&s, input_request(row), &host, Limits::default()),
            Err(Fault::Host("type lookup failed"))
        );
        assert_eq!(s, before);
        host.fail.set(false);
        assert_eq!(
            calls::prepare_input(&s, input_request(row), &host, Limits::default()),
            Ok(expected)
        );
    }
}
