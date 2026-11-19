use ae_vm_aevm::{
    execution::{Limits, State, Stop},
    gas::FloatProfile,
    maps::{Maps, Store},
    opcode::Version,
    runtime::{self, Fault, Native, Outcome, StoreHost},
    runtime_heap::Error as HeapError,
    store::OrderProfile,
    word::Word,
};
use serde_json::Value as J;
use std::{
    cell::{Cell, RefCell},
    sync::LazyLock,
};
mod support;
use support::*;
static DATA: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_runtime_boundaries.json")));
static EXPANDED: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_runtime_execution.json")));
const ORDER: OrderProfile = OrderProfile::Otp26Word64Crc32c;
fn optional<'a>(v: &'a J, key: &str) -> Option<&'a J> {
    v[1].as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom(key))
        .map(|p| &p[1])
}
fn state(row: &J, snap: &J) -> State {
    let v = Version::from_id(row["vm"].as_u64().unwrap() as u32).unwrap();
    let mut s = State::new(
        v,
        optional(snap, "code").map_or(vec![0], bytes),
        uint(field(snap, "gas")),
        FloatProfile::Otp26Word64,
    );
    s.protocol = row["protocol"].as_u64().unwrap_or(4) as u32;
    s.context.address = number(field(snap, "address"));
    s.cp = field(snap, "cp")[1].as_str().unwrap().parse().unwrap();
    s.stack = field(snap, "stack")[1]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| Word::from_unsigned(&uint(n)))
        .collect();
    let mem = field(snap, "memory");
    for p in mem[1].as_array().unwrap() {
        if p[0][0] == "integer" {
            s.memory
                .store(&uint(&p[0]), Word::from_unsigned(&uint(&p[1])));
        }
    }
    let size = uint(field(mem, "mem_size"));
    if size != 0u8.into() {
        s.memory.load(&(size * 32u8 - 32u8));
    }
    s.maps = optional(snap, "maps").map_or_else(Maps::default, maps);
    if s.version == Version::Solidity1 {
        s.map_store = store(&row["store"]);
        s.solidity_storage = field(snap, "storage")[1]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| (uint(&p[0]), uint(&p[1])))
            .collect();
    } else {
        s.map_store = optional(snap, "storage").map_or_else(Store::new, store);
    }
    if let Some(jumps) = optional(snap, "jumpdests") {
        s.jump_destinations = jumps[1]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| {
                assert_eq!(p[1], atom("true"));
                p[0][1].as_str().unwrap().parse().unwrap()
            })
            .collect();
    }
    s.data = optional(snap, "data").map_or_else(Vec::new, bytes);
    s.return_data = bytes(field(snap, "return_data"));
    s.output = bytes(field(snap, "out"));
    assert_eq!(field(snap, "logs"), &list(vec![]));
    s.out_type = optional(snap, "out_type")
        .filter(|v| **v != atom("undefined"))
        .map(ty);
    s.call_stack = optional(snap, "call_stack").map_or_else(Vec::new, |v| {
        v[1].as_array().unwrap().iter().map(number).collect()
    });
    for (key, expected) in [("call", map(vec![])), ("callcreates", list(vec![]))] {
        if let Some(v) = optional(snap, key) {
            assert_eq!(*v, expected);
        }
    }
    s
}
#[derive(Default)]
struct Host {
    calls: RefCell<Vec<Store>>,
    fail: Cell<bool>,
}
impl StoreHost for Host {
    type Chain = Vec<Store>;
    type Error = &'static str;
    fn set_store(&self, store: Store, chain: &Self::Chain) -> Result<Self::Chain, Self::Error> {
        self.calls.borrow_mut().push(store.clone());
        let mut staged = chain.clone();
        staged.push(store);
        if self.fail.get() {
            Err("write failed")
        } else {
            Ok(staged)
        }
    }
}
fn native(r: &J) -> Native {
    assert_eq!(r["kind"], "native");
    let reason = &r["reason"];
    if reason == &atom("function_clause") {
        assert_eq!(r["class"], "error");
        return Native::Heap(HeapError::NativeFunctionClause);
    }
    let a = &reason[1];
    match a[0][1].as_str().unwrap() {
        "bad_vm_setup" => {
            assert_eq!(r["class"], "error");
            assert_eq!(a[1], atom("missing_call_data_type"));
            Native::MissingCallDataType
        }
        "init_error" => {
            assert_eq!(r["class"], "throw");
            match a[1][1].as_str().unwrap() {
                "reentrant_call" => Native::ReentrantCall,
                "bad_call_data" => Native::BadCallData,
                x => panic!("{x}"),
            }
        }
        "aevm_eval_error" => {
            assert_eq!(r["class"], "throw");
            assert_eq!(a[1], atom("out_of_gas"));
            assert_eq!(a[2], int(0));
            Native::OutOfGas
        }
        x => panic!("{x}"),
    }
}
fn setup(row: &J) -> State {
    let mut s = State::new(
        Version::from_id(row["vm"].as_u64().unwrap() as u32).unwrap(),
        row.get("code").map_or(vec![0], bytes),
        row["gas"].as_u64().unwrap().into(),
        FloatProfile::Otp26Word64,
    );
    s.protocol = row["protocol"].as_u64().unwrap_or(4) as u32;
    s.data = bytes(&row["data"]);
    s.map_store = store(&row["store"]);
    s.out_type = (row["out_type"] != atom("undefined")).then(|| ty(&row["out_type"]));
    if let Some(a) = row.get("address") {
        s.context.address = a.as_str().unwrap().parse().unwrap();
    }
    if let Some(stack) = row.get("call_stack") {
        s.call_stack = stack[1].as_array().unwrap().iter().map(number).collect();
    } else if row["mode"] == "reentrant" {
        s.call_stack = vec![1.into()];
    }
    if let Some(mem) = row.get("memory") {
        for p in mem[1].as_array().unwrap() {
            if p[0][0] == "integer" {
                s.memory
                    .store(&uint(&p[0]), Word::from_unsigned(&uint(&p[1])));
            }
        }
        let size = uint(field(mem, "mem_size"));
        if size != 0u8.into() {
            s.memory.load(&(size * 32u8 - 32u8));
        }
    }
    s
}

#[test]
fn native_runtime_setup_imports_stores_before_calldata_and_preserves_errors() {
    let rows = DATA["initialization"].as_array().unwrap();
    assert_eq!(rows.len(), 192);
    let mut ready = 0;
    for row in rows {
        let input = setup(row);
        let before = input.clone();
        let t = (row["type"] != atom("undefined")).then(|| ty(&row["type"]));
        let actual = runtime::initialize(&input, t.as_ref(), Limits::default());
        if row["result"]["kind"] == "ok" {
            ready += 1;
            assert_eq!(
                actual.unwrap(),
                state(row, &row["result"]["value"]),
                "{row}"
            );
        } else {
            assert_eq!(actual, Err(Fault::Native(native(&row["result"]))), "{row}");
        }
        assert_eq!(input, before);
    }
    assert_eq!(ready, 72);
}
fn check_finalization(row: &J, actual: Result<Outcome<Vec<Store>>, Fault<&'static str>>) {
    let expected = &row["result"];
    if expected["kind"] == "native" {
        assert_eq!(actual, Err(Fault::Native(native(expected))));
        return;
    }
    let result = &expected["value"][1];
    if result[0] == atom("ok") {
        let snap = &result[1];
        let chain = field(snap, "chain_state");
        let chain = optional(chain, "store").map_or_else(Vec::new, |v| vec![store(v)]);
        assert_eq!(
            actual,
            Ok(Outcome::Complete {
                state: Box::new(state(row, snap)),
                chain
            }),
            "VM{} {} gas{}",
            row["vm"],
            row["mode"],
            row["gas"]
        );
    } else {
        assert_eq!(result[0], atom("error"));
        assert_eq!(result[1], atom("out_of_gas"));
        assert_eq!(
            actual,
            Ok(Outcome::Error {
                reason: ae_vm_aevm::execution::Failure::OutOfGas,
                gas: 0u8.into()
            })
        );
    }
}
#[test]
fn native_finalization_preserves_gas_metadata_and_host_call_order() {
    let rows = DATA["finalization"].as_array().unwrap();
    assert_eq!(rows.len(), 250);
    let mut calls = 0;
    for row in rows {
        let input = state(row, &row["initial"]);
        let before = input.clone();
        let host = Host::default();
        let chain = vec![];
        let actual = runtime::finalize(&input, &chain, &host, ORDER, Limits::default());
        check_finalization(row, actual);
        let expected: Vec<_> = row["host_calls"][1]
            .as_array()
            .unwrap()
            .iter()
            .map(store)
            .collect();
        calls += expected.len();
        assert_eq!(*host.calls.borrow(), expected);
        assert_eq!(input, before);
        assert!(chain.is_empty());
    }
    assert_eq!(calls, 180);
}
#[test]
fn runtime_resource_and_host_failures_preserve_inputs_and_retry() {
    for row in DATA["initialization"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["mode"] == "map" && r["seed"] == "map" && r["gas"] == 100)
    {
        let input = setup(row);
        let t = ty(&row["type"]);
        let expected = runtime::initialize(&input, Some(&t), Limits::default()).unwrap();
        for dim in 0..4 {
            let mut stopped = false;
            let mut reached = false;
            for n in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000] {
                let mut l = Limits::default();
                match dim {
                    0 => l.max_work = n,
                    1 => l.max_bytes = n,
                    2 => l.max_depth = n,
                    _ => l.max_memory_words = n,
                }
                match runtime::initialize(&input, Some(&t), l) {
                    Err(Fault::Stop(Stop::ResourceLimit)) => stopped = true,
                    Ok(s) => {
                        assert_eq!(s, expected);
                        reached = true;
                        break;
                    }
                    other => panic!("{other:?}"),
                }
                assert_eq!(input, setup(row));
            }
            assert!(stopped && reached);
        }
    }
    for row in DATA["finalization"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["mode"] == "map" && r["gas"] == 1000)
    {
        let input = state(row, &row["initial"]);
        let expected =
            runtime::finalize(&input, &vec![], &Host::default(), ORDER, Limits::default()).unwrap();
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
                let host = Host::default();
                match runtime::finalize(&input, &vec![], &host, ORDER, l) {
                    Err(Fault::Stop(Stop::ResourceLimit)) => {
                        stopped = true;
                        assert!(host.calls.borrow().is_empty());
                    }
                    Ok(s) => {
                        assert_eq!(s, expected);
                        reached = true;
                        break;
                    }
                    other => panic!("{other:?}"),
                }
                assert_eq!(input, state(row, &row["initial"]));
            }
            assert!(stopped && reached);
        }
        let chain = vec![];
        let host = Host::default();
        host.fail.set(true);
        assert_eq!(
            runtime::finalize(&input, &chain, &host, ORDER, Limits::default()),
            Err(Fault::Host("write failed"))
        );
        assert_eq!(host.calls.borrow().len(), 1);
        assert!(chain.is_empty());
        assert_eq!(input, state(row, &row["initial"]));
        host.fail.set(false);
        assert_eq!(
            runtime::finalize(&input, &chain, &host, ORDER, Limits::default()).unwrap(),
            expected
        );
    }
}

#[test]
fn extended_setup_preserves_memory_offsets_native_errors_and_solidity_storage() {
    let rows = EXPANDED["initial"].as_array().unwrap();
    assert_eq!(rows.len(), 50);
    for row in rows {
        let input = setup(row);
        let before = input.clone();
        let t = (row["type"] != atom("undefined")).then(|| ty(&row["type"]));
        if row["mode"] == "deep_data" && input.version != Version::Solidity1 {
            assert_eq!(
                runtime::initialize(&input, t.as_ref(), Limits::default()),
                Err(Fault::Stop(Stop::ResourceLimit))
            );
        }
        let actual = runtime::initialize(
            &input,
            t.as_ref(),
            Limits {
                max_depth: 2048,
                ..Limits::default()
            },
        );
        if row["result"]["kind"] == "ok" {
            assert_eq!(
                actual.unwrap(),
                state(row, &row["result"]["value"]),
                "{row}"
            );
        } else {
            assert_eq!(actual, Err(Fault::Native(native(&row["result"]))), "{row}");
        }
        assert_eq!(input, before);
    }
}
fn check_run(row: &J, actual: Result<Outcome<Vec<Store>>, Fault<&'static str>>) {
    let r = &row["result"];
    if r["kind"] == "native" {
        assert_eq!(actual, Err(Fault::Native(native(r))));
        return;
    }
    let v = &r["value"][1];
    let expected = match v[0][1].as_str().unwrap() {
        "ok" => {
            let chain = optional(field(&v[1], "chain_state"), "store")
                .map_or_else(Vec::new, |v| vec![store(v)]);
            Outcome::Complete {
                state: Box::new(state(row, &v[1])),
                chain,
            }
        }
        "error" => {
            let reason = if v[1] == atom("out_of_gas") {
                ae_vm_aevm::execution::Failure::OutOfGas
            } else {
                assert_eq!(v[1][1][0], atom("illegal_instruction"));
                ae_vm_aevm::execution::Failure::IllegalInstruction(
                    v[1][1][1][1].as_str().unwrap().parse().unwrap(),
                )
            };
            Outcome::Error {
                reason,
                gas: uint(&v[2]),
            }
        }
        "revert" => Outcome::Revert {
            output: bytes(&v[1]),
            gas: uint(&v[2]),
        },
        other => panic!("{other}"),
    };
    assert_eq!(
        actual,
        Ok(expected),
        "VM{} {} gas{}",
        row["vm"],
        row["mode"],
        row["gas"]
    );
}
#[test]
fn complete_original_invocations_match_execution_finalization_and_host_traces() {
    let rows = EXPANDED["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 1326);
    let sol = EXPANDED["solidity"].as_array().unwrap();
    assert_eq!(sol.len(), 5);
    for row in rows.iter().chain(sol) {
        let input = setup(row);
        let before = input.clone();
        let t = (row["type"] != atom("undefined")).then(|| ty(&row["type"]));
        let ready = runtime::initialize(&input, t.as_ref(), Limits::default()).unwrap();
        assert_eq!(row["ready"]["kind"], "ok");
        assert_eq!(ready, state(row, &row["ready"]["value"]));
        let host = Host::default();
        let chain = vec![];
        check_run(
            row,
            runtime::invoke(&input, t.as_ref(), &chain, &host, ORDER, Limits::default()),
        );
        let expected: Vec<_> = row["host_calls"][1]
            .as_array()
            .unwrap()
            .iter()
            .map(store)
            .collect();
        assert_eq!(*host.calls.borrow(), expected);
        assert_eq!(input, before);
        assert!(chain.is_empty());
        let host = Host::default();
        check_run(
            row,
            runtime::evaluate(&ready, &chain, &host, ORDER, Limits::default()),
        );
        assert_eq!(*host.calls.borrow(), expected);
    }
}
#[test]
fn invocation_work_and_host_failures_never_publish_partial_chain_snapshots() {
    for row in EXPANDED["runs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["mode"] == "map_save" && r["gas"] == 1000)
    {
        let input = setup(row);
        let before = input.clone();
        let t = ty(&row["type"]);
        let chain = vec![];
        let expected = runtime::invoke(
            &input,
            Some(&t),
            &chain,
            &Host::default(),
            ORDER,
            Limits::default(),
        )
        .unwrap();
        for dim in 0..4 {
            let mut stopped = false;
            let mut reached = false;
            for n in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000] {
                let mut l = Limits::default();
                match dim {
                    0 => l.max_work = n,
                    1 => l.max_bytes = n,
                    2 => l.max_depth = n,
                    _ => l.max_memory_words = n,
                }
                let host = Host::default();
                match runtime::invoke(&input, Some(&t), &chain, &host, ORDER, l) {
                    Err(Fault::Stop(Stop::ResourceLimit)) => {
                        stopped = true;
                        assert!(host.calls.borrow().is_empty());
                    }
                    Ok(actual) => {
                        assert_eq!(actual, expected);
                        reached = true;
                        break;
                    }
                    other => panic!("{other:?}"),
                }
                assert_eq!(input, before);
                assert!(chain.is_empty());
            }
            assert!(stopped && reached);
        }
        let host = Host::default();
        host.fail.set(true);
        assert_eq!(
            runtime::invoke(&input, Some(&t), &chain, &host, ORDER, Limits::default()),
            Err(Fault::Host("write failed"))
        );
        assert_eq!(host.calls.borrow().len(), 1);
        assert_eq!(input, before);
        assert!(chain.is_empty());
        host.fail.set(false);
        assert_eq!(
            runtime::invoke(&input, Some(&t), &chain, &host, ORDER, Limits::default()).unwrap(),
            expected
        );
    }
}
