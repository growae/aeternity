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
static PURE: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_hash_auth.json")));
static CRYPTO: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_crypto_auth.json")));
#[test]
fn original_signature_primitives_preserve_padding_gates_and_malformed_failures() {
    let mut count = 0;
    for group in ["runs", "malformed"] {
        for row in CRYPTO[group].as_array().unwrap() {
            let s = map_state(row, &row["before"]);
            let before = s.clone();
            let host = MapSource {
                store: &s.map_store,
                calls: Cell::new(0),
                fail: Cell::new(true),
            };
            let result = primops::call_local(
                &s,
                &row["call_gas"].as_u64().unwrap().into(),
                &bytes(&row["data"]),
                &host,
                Limits::default(),
            );
            assert_eq!(result, Ok(primop_expected(row)), "{row}");
            assert_eq!(s, before);
            assert_eq!(host.calls.get(), 0);
            count += 1;
        }
    }
    assert_eq!(count, 4270);
}
#[test]
fn original_crypto_auth_input_types_skip_dynamic_hash_checks() {
    let rows = CRYPTO["inputs"].as_array().unwrap();
    assert_eq!(rows.len(), 672);
    let host = TypesHost {
        calls: RefCell::new(vec![]),
        argument: None,
        fail: Cell::new(true),
    };
    for row in rows {
        let s = map_state(row, &row["before"]);
        let before = s.clone();
        let request = Input {
            target: Word::ZERO,
            offset: Word::from_unsigned(&row["input_pointer"].as_str().unwrap().parse().unwrap()),
            size: Word::from(777),
        };
        assert_eq!(
            calls::prepare_input(&s, request, &host, Limits::default()),
            prepared(row, &row["result"]),
            "{row}"
        );
        assert_eq!(s, before);
    }
    assert!(host.calls.borrow().is_empty());
}
#[test]
fn pure_primitive_budget_exhaustion_preserves_inputs_and_retries() {
    let mut rows: Vec<&J> = PURE["hashes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| {
            r["vm"] == 6 && r["call_gas"] == 1000 && field(&r["before"], "gas") == &int(1000)
        })
        .collect();
    rows.extend(
        CRYPTO["runs"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["vm"] == 6 && r["call_gas"] == 5000)
            .take(12),
    );
    assert!(!rows.is_empty());
    for row in rows {
        let s = map_state(row, &row["before"]);
        let before = s.clone();
        let host = MapSource {
            store: &s.map_store,
            calls: Cell::new(0),
            fail: Cell::new(true),
        };
        let gas = row["call_gas"].as_u64().unwrap().into();
        let data = bytes(&row["data"]);
        let expected = Ok(primop_expected(row));
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
                match primops::call_local(&s, &gas, &data, &host, limits) {
                    Err(Fault::Stop(Stop::ResourceLimit)) => stopped = true,
                    actual => {
                        assert_eq!(actual, expected);
                        reached = true;
                        break;
                    }
                }
                assert_eq!(s, before);
            }
            assert!(reached && (dim >= 2 || stopped));
        }
        assert_eq!(s, before);
        assert_eq!(host.calls.get(), 0);
    }
}
#[test]
fn original_generic_hashes_preserve_stored_map_versions_and_heap_failures() {
    let corpus = fixture(include_str!("fixtures/erlang_crypto_heaps.json"));
    let rows = corpus["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 720);
    for row in rows {
        let s = map_state(row, &row["before"]);
        let before = s.clone();
        let host = MapSource {
            store: &s.map_store,
            calls: Cell::new(0),
            fail: Cell::new(true),
        };
        assert_eq!(
            primops::call_local(
                &s,
                &row["call_gas"].as_u64().unwrap().into(),
                &bytes(&row["data"]),
                &host,
                Limits::default()
            ),
            Ok(primop_expected(row)),
            "{row}"
        );
        assert_eq!(s, before);
        assert_eq!(host.calls.get(), 0);
    }
}
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
        s.auth_tx_hash = Some(bytes(&v[1]));
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
    fn get_store(&self) -> Result<Store, Fault<Self::Error>> {
        self.calls.set(self.calls.get() + 1);
        if self.fail.get() {
            Err(Fault::Host("store read failed"))
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
        if v[1][0] == "tuple" {
            let payload = &v[1][1];
            if payload[0] == atom("badmatch") {
                let error = if payload[1][0] == "integer" {
                    Error::NativeBadAlignment(uint(&payload[1]).try_into().unwrap())
                } else {
                    assert_eq!(
                        logical(payload[1].clone()),
                        logical(field(&row["before"], "maps")[1][1].clone())
                    );
                    let data = bytes(&row["data"]);
                    let start: usize = num_bigint::BigUint::from_bytes_be(&data[..32])
                        .try_into()
                        .unwrap();
                    Error::MissingMap(num_bigint::BigUint::from_bytes_be(
                        &data[start + 64..start + 96],
                    ))
                };
                return primops::Outcome::Error(primops::Error::Heap(error));
            }
            assert_eq!(payload[0], atom("out_of_gas"));
            let charge = &payload[1][1];
            let resource = &charge[0][1];
            assert_eq!(resource[0], atom("primop"));
            let base = &resource[1][1];
            assert_eq!(base[0], atom("base"));
            for pair in row["before"][1].as_array().unwrap() {
                if pair[0] != atom("storage") {
                    assert_eq!(
                        logical(field(&payload[2], pair[0][1].as_str().unwrap()).clone()),
                        logical(pair[1].clone())
                    );
                }
            }
            return primops::Outcome::Error(primops::Error::BaseGas {
                op: Word::from_unsigned(&uint(&base[1])),
                required: uint(&charge[1]),
                state: Box::new(map_state(row, &row["before"])),
            });
        }
        primops::Outcome::Error(match v[1][1].as_str().unwrap() {
            "illegal_primop_call" => primops::Error::IllegalCall,
            "out_of_gas" => primops::Error::OutOfGas,
            "max_size_exceeded" => primops::Error::Heap(Error::SizeLimit),
            "function_clause" => primops::Error::Heap(Error::FunctionClause),
            "circular_references" => primops::Error::Heap(Error::CircularReferences),
            s => panic!("{s}"),
        })
    }
}
#[test]
fn original_hash_and_auth_primitives_match_gates_gas_bytes_and_failures() {
    let mut count = 0;
    for group in ["hashes", "strings", "auth"] {
        for row in PURE[group].as_array().unwrap() {
            let s = map_state(row, &row["before"]);
            let before = s.clone();
            let host = MapSource {
                store: &s.map_store,
                calls: Cell::new(0),
                fail: Cell::new(true),
            };
            let result = primops::call_local(
                &s,
                &row["call_gas"].as_u64().unwrap().into(),
                &bytes(&row["data"]),
                &host,
                Limits::default(),
            );
            assert_eq!(result, Ok(primop_expected(row)), "{row}");
            assert_eq!(s, before);
            assert_eq!(host.calls.get(), 0);
            count += 1;
        }
    }
    assert_eq!(count, 2220);
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

#[test]
fn map_store_native_failures_are_caught_but_operational_failures_escape() {
    struct FailedStore(Fault<&'static str>);
    impl MapHost for FailedStore {
        type Error = &'static str;
        fn get_store(&self) -> Result<Store, Fault<Self::Error>> {
            Err(self.0.clone())
        }
    }
    let row = PRIMOPS["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["kind"] == "nested"
                && r["stored"] == true
                && r["call_gas"] == 100
                && r["op"] == "tolist"
                && uint(field(&r["before"], "gas")) == 1000u16.into()
        })
        .unwrap();
    let s = map_state(row, &row["before"]);
    let before = s.clone();
    let data = bytes(&row["data"]);
    for fault in [
        Fault::Native(Native::ChainException {
            class: "error",
            reason: "function_clause",
        }),
        Fault::Host("store read failed"),
        Fault::Stop(Stop::ResourceLimit),
    ] {
        let expected = match &fault {
            Fault::Native(_) => Ok(primops::Outcome::Error(primops::Error::IllegalCall)),
            _ => Err(fault.clone()),
        };
        assert_eq!(
            primops::call_map(&s, &data, &FailedStore(fault), Limits::default()),
            expected
        );
        assert_eq!(s, before);
    }
    let host = MapSource {
        store: &s.map_store,
        calls: Cell::new(0),
        fail: Cell::new(false),
    };
    assert_eq!(
        primops::call_map(&s, &data, &host, Limits::default()),
        Ok(primop_expected(row))
    );
    assert_eq!(host.calls.get(), 1);
}

use ae_vm_aevm::{
    calls::{Input, InputHost, Prepared},
    runtime_heap::TypeRef,
};
use std::cell::RefCell;
static INPUTS: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_call_inputs.json")));
struct TypesHost {
    calls: RefCell<Vec<(Word, ae_vm_aevm::calls::ContractVersion, Word)>>,
    argument: Option<TypeRef>,
    fail: Cell<bool>,
}
impl InputHost for TypesHost {
    type Error = &'static str;
    fn function_types(
        &self,
        target: Word,
        version: ae_vm_aevm::calls::ContractVersion,
        function: Word,
        _budget: &mut ae_vm_aevm::execution::Budget,
    ) -> ae_vm_aevm::calls::FunctionTypes<Self::Error> {
        self.calls.borrow_mut().push((target, version, function));
        if self.fail.get() {
            Err(Fault::Host("type lookup failed"))
        } else {
            Ok(self
                .argument
                .clone()
                .map(|t| (t, ty(&atom("word"))))
                .ok_or(ae_vm_aevm::calls::TypeError::UnknownFunction))
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
                            ae_vm_aevm::calls::ContractVersion {
                                vm: Version::from_id(uint(field(&a[1], "vm")).try_into().unwrap())
                                    .unwrap(),
                                abi: 1,
                            },
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
