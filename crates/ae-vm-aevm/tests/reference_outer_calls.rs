use ae_vm_aevm::{
    execution::{Budget, CallCreate, Failure, Limits, State, Stop},
    gas::FloatProfile,
    maps::Store,
    opcode::Version,
    outer_call::{self, Fault, Host, Request, Response},
    runtime::{self, Outcome, StoreHost},
    runtime_heap::TypeRef,
    store::OrderProfile,
    word::Word,
};
use num_bigint::BigInt;
use serde_json::Value as J;
use std::{
    cell::{Cell, RefCell},
    sync::LazyLock,
};
mod outer_chain;
mod outer_oracle;
mod support;
use support::*;
static DATA: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_outer_calls.json")));
static EXECUTION: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_outer_call_execution.json")));
static INVOCATIONS: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_outer_call_invocations.json")));
static CRYPTO: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_crypto_auth.json")));
#[test]
fn original_outer_crypto_auth_calls_preserve_gas_imports_and_native_cases() {
    let rows = CRYPTO["outer"].as_array().unwrap();
    assert_eq!(rows.len(), 2800);
    for row in rows {
        let s = state(row, &row["before"]);
        let before = s.clone();
        let chain = logical(field(&row["before"], "chain_state").clone());
        let host = Source {
            mode: "crypto_auth",
            events: RefCell::new(vec![]),
            fail: Cell::new(None),
        };
        check(
            row,
            outer_call::evaluate(&s, &chain, &host, Limits::default()),
        );
        assert_eq!(
            logical(list(host.events.borrow().clone())),
            logical(row["host_calls"].clone()),
            "{row}"
        );
        assert_eq!(s, before);
    }
}
#[test]
fn outer_crypto_auth_budget_and_balance_failures_leave_inputs_retryable() {
    let mut count = 0;
    for row in CRYPTO["outer"].as_array().unwrap().iter().filter(|r| {
        r["vm"] == 6
            && field(&r["before"], "gas") == &int(5000)
            && field(&r["before"], "stack")[1][0] == int(1300)
            && field(&r["before"], "auth_tx_hash") == &binary(word(7))
            && r["result"]["kind"] == "ok"
            && r["result"]["value"][1][0] == atom("ok")
    }) {
        count += 1;
        let s = state(row, &row["before"]);
        let before = s.clone();
        let chain = logical(field(&row["before"], "chain_state").clone());
        let original = chain.clone();
        let host = Source {
            mode: "crypto_auth",
            events: RefCell::new(vec![]),
            fail: Cell::new(None),
        };
        let expected = outer_call::evaluate(&s, &chain, &host, Limits::default());
        check(row, expected.clone());
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
                match outer_call::evaluate(&s, &chain, &host, limits) {
                    Err(Fault::Runtime(runtime::Fault::Stop(Stop::ResourceLimit))) => {
                        stopped = true
                    }
                    actual => {
                        assert_eq!(actual, expected);
                        reached = true;
                        break;
                    }
                }
            }
            assert!(stopped && reached);
        }
        host.fail.set(Some("balance"));
        assert_eq!(
            outer_call::evaluate(&s, &chain, &host, Limits::default()),
            Err(Fault::Runtime(runtime::Fault::Host("balance")))
        );
        host.fail.set(None);
        assert_eq!(
            outer_call::evaluate(&s, &chain, &host, Limits::default()),
            expected
        );
        assert_eq!(s, before);
        assert_eq!(chain, original);
    }
    assert_eq!(count, 10);
}
fn optional<'a>(v: &'a J, key: &str) -> Option<&'a J> {
    v[1].as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom(key))
        .map(|p| &p[1])
}
fn vm_id(v: Version) -> u32 {
    match v {
        Version::Sophia1 => 1,
        Version::Solidity1 => 2,
        Version::Sophia2 => 3,
        Version::Sophia3 => 4,
        Version::Sophia4 => 6,
    }
}
fn state(row: &J, snap: &J) -> State {
    let mode = row["mode"].as_str().unwrap_or("normal");
    let mut s = State::new(
        Version::from_id(row["vm"].as_u64().unwrap() as u32).unwrap(),
        vec![row["opcode"].as_u64().unwrap_or(0) as u8, 0],
        uint(field(snap, "gas")),
        FloatProfile::Otp26Word64,
    );
    s.context.caller = 2.into();
    s.context.origin = 3.into();
    s.context.value = if mode.starts_with("local_") {
        0.into()
    } else {
        1.into()
    };
    s.no_recursion = mode == "no_recursion";
    s.call_stack = vec![
        9.into();
        match mode {
            "depth1023" => 1023,
            "depth1024" => 1024,
            _ => 0,
        }
    ];
    if mode == "auth" {
        s.auth_tx_hash = Some(Word::from(7).0.to_vec());
    }
    if let Some(code) = optional(snap, "code") {
        s.code = bytes(code);
    }
    if let Some(protocol) = optional(snap, "protocol_version") {
        s.protocol = uint(protocol).try_into().unwrap();
    }
    if let Some(data) = optional(snap, "data") {
        s.data = bytes(data);
    }
    if let Some(t) = optional(snap, "out_type") {
        s.out_type = Some(ty(t));
    }
    for (key, value) in [
        ("address", &mut s.context.address),
        ("caller", &mut s.context.caller),
        ("origin", &mut s.context.origin),
        ("value", &mut s.context.value),
    ] {
        if let Some(n) = optional(snap, key) {
            *value = number(n);
        }
    }
    if let Some(stack) = optional(snap, "call_stack") {
        s.call_stack = stack[1].as_array().unwrap().iter().map(number).collect();
    }
    if let Some(flag) = optional(snap, "no_recursion") {
        s.no_recursion = *flag == atom("true");
    }
    if let Some(hash) = optional(snap, "auth_tx_hash") {
        s.auth_tx_hash = (*hash != atom("undefined")).then(|| bytes(hash));
    }
    if let Some(storage) = optional(snap, "storage").filter(|v| v[0] == "map") {
        s.map_store = store(storage);
    }
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
    if let Some(abi) = optional(snap, "abi_version") {
        s.abi_version = uint(abi).try_into().unwrap();
    }
    s.cp = uint(field(snap, "cp")).try_into().unwrap();
    s.stack = field(snap, "stack")[1]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| Word::from_unsigned(&uint(n)))
        .collect();
    s.maps = maps(field(snap, "maps"));
    s.output = bytes(field(snap, "out"));
    s.return_data = bytes(field(snap, "return_data"));
    if let Some(j) = optional(snap, "jumpdests") {
        s.jump_destinations = j[1]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| uint(&p[0]).try_into().unwrap())
            .collect();
    }
    s.call_creates = field(snap, "callcreates")[1]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| CallCreate {
            data: bytes(field(c, "data")),
            destination: number(field(c, "destination")),
            gas_limit: uint(field(c, "gasLimit")),
            value: number(field(c, "value")),
        })
        .collect();
    s
}
struct Source<'a> {
    mode: &'a str,
    events: RefCell<Vec<J>>,
    fail: Cell<Option<&'static str>>,
}
impl Source<'_> {
    fn event(&self, j: J) {
        self.events.borrow_mut().push(j);
    }
    fn check(&self, name: &'static str) -> Result<(), &'static str> {
        if self.fail.get() == Some(name) {
            Err(name)
        } else {
            Ok(())
        }
    }
}
impl StoreHost for Source<'_> {
    type Chain = J;
    type Error = &'static str;
    fn set_store(&self, store: Store, chain: &J) -> Result<J, runtime::Fault<Self::Error>> {
        self.check("set_store").map_err(runtime::Fault::Host)?;
        let contents = map(store
            .into_iter()
            .map(|(k, v)| (binary(k), binary(v)))
            .collect());
        self.event(tuple(vec![atom("set_store"), contents.clone()]));
        Ok(put(chain, "store", contents))
    }
}
impl Host for Source<'_> {
    fn function_types(
        &self,
        target: Word,
        version: ae_vm_aevm::calls::ContractVersion,
        function: Word,
        _: &J,
        _budget: &mut ae_vm_aevm::execution::Budget,
    ) -> ae_vm_aevm::calls::FunctionTypes<Self::Error> {
        self.event(tuple(vec![
            atom("types"),
            binary(target.0),
            map(vec![
                (atom("vm"), int(vm_id(version.vm))),
                (atom("abi"), int(version.abi)),
            ]),
            binary(function.0),
        ]));
        self.check("types").map_err(runtime::Fault::Host)?;
        Ok((self.mode != "missing_function")
            .then(|| (ty(&atom("word")), ty(&atom("word"))))
            .ok_or(ae_vm_aevm::calls::TypeError::UnknownFunction))
    }
    fn balance(&self, address: Word, chain: &J) -> Result<BigInt, Self::Error> {
        self.event(tuple(vec![atom("balance"), binary(address.0)]));
        self.check("balance")?;
        Ok(number(field(chain, "balance")))
    }
    fn call(
        &self,
        request: Request<'_>,
        chain: &J,
        budget: &mut Budget,
    ) -> Result<(Response, J), Fault<Self::Error, J>> {
        self.event(tuple(vec![
            atom("call"),
            binary(request.target.0),
            int(request.gas),
            int(request.value),
            binary(request.data),
            list(request.call_stack.iter().map(int).collect()),
            binary(request.origin.0),
        ]));
        self.check("call")
            .map_err(|e| Fault::Runtime(runtime::Fault::Host(e)))?;
        if let Some(count) = optional(chain, "calls") {
            let count = uint(count);
            let next = put(chain, "calls", int(&count + 1u8));
            if self.mode == "nested" {
                return self.child(request, &next, budget);
            }
            let gas = BigInt::from(request.gas.clone());
            let response = match (self.mode, count == 1u8.into()) {
                ("sequence_fail", true) => Response::Exception {
                    reason: "out_of_gas".into(),
                    gas_spent: gas,
                },
                ("sequence_revert", true) => Response::Revert {
                    data: b"later".to_vec(),
                    gas_spent: gas.min(5.into()),
                },
                ("sequence_overspent", true) => Response::Complete {
                    data: word(42).to_vec(),
                    gas_spent: gas + 1,
                },
                _ => Response::Complete {
                    data: if self.mode == "sequence" {
                        Word::from_unsigned(&(count + 1u8)).0.to_vec()
                    } else {
                        word(42).to_vec()
                    },
                    gas_spent: 0.into(),
                },
            };
            return Ok((response, next));
        }
        let gas = BigInt::from(request.gas.clone());
        let spent = match self.mode {
            "spent_all" | "exception_all" => gas.clone(),
            "overspent" => &gas + 1,
            "spent_partial" | "exception_partial" | "unlimited_exception_partial" | "revert" => {
                gas.min(5.into())
            }
            _ => 0.into(),
        };
        let response = match self.mode {
            "exception_all" | "exception_partial" | "unlimited_exception_partial" => {
                Response::Exception {
                    reason: "out_of_gas".into(),
                    gas_spent: spent,
                }
            }
            "revert" => Response::Revert {
                data: b"reverted".to_vec(),
                gas_spent: spent,
            },
            _ => Response::Complete {
                data: word(42).to_vec(),
                gas_spent: spent,
            },
        };
        Ok((response, put(chain, "called", atom("true"))))
    }
}
fn put(chain: &J, key: &str, value: J) -> J {
    let mut next = chain.clone();
    let pairs = next[1].as_array_mut().unwrap();
    pairs.retain(|p| p[0] != atom(key));
    pairs.push(serde_json::json!([atom(key), value]));
    logical(next)
}
fn failure_name(reason: Failure) -> String {
    match reason {
        Failure::OutOfGas => "out_of_gas".into(),
        Failure::PopEmpty => "error_pop_empty_stack".into(),
        Failure::Call(name) => name,
        other => panic!("unexpected child failure {other:?}"),
    }
}
impl Source<'_> {
    fn child(
        &self,
        request: Request<'_>,
        chain: &J,
        budget: &mut Budget,
    ) -> Result<(Response, J), Fault<&'static str, J>> {
        let depth: usize = uint(field(chain, "depth")).try_into().unwrap();
        let version = Version::from_id(uint(field(chain, "vm")).try_into().unwrap()).unwrap();
        let address = request.target.unsigned();
        let code = if request.call_stack.len() < depth {
            let next: u8 = (&address + 1u8).try_into().unwrap();
            vec![
                80, 96, 0, 96, 0, 96, 0, 96, 32, 96, 0, 96, next, 90, 241, 96, 0, 243,
            ]
        } else {
            match field(chain, "leaf")[1].as_str().unwrap() {
                "return" => vec![80, 96, 42, 96, 0, 243],
                "revert" => vec![80, 96, 128, 96, 0, 253],
                "error" => vec![80, 80],
                other => panic!("{other}"),
            }
        };
        let mut s = State::new(
            version,
            code,
            request.gas.clone(),
            FloatProfile::Otp26Word64,
        );
        s.context.address = address.into();
        s.context.caller = request.call_stack[0].clone();
        s.context.origin = request.origin.unsigned().into();
        s.context.value = request.value.clone();
        s.data = request.data.to_vec();
        s.call_stack = request.call_stack.to_vec();
        s.out_type = Some(ty(&atom("word")));
        let input_type = ty(&tuple(vec![
            atom("tuple"),
            list(vec![atom("word"), atom("word")]),
        ]));
        let gas = BigInt::from(request.gas.clone());
        Ok(
            match outer_call::invoke_budgeted(
                &s,
                Some(&input_type),
                chain,
                self,
                OrderProfile::Otp26Word64Crc32c,
                budget,
            )? {
                Outcome::Complete { state, chain } => (
                    Response::Complete {
                        data: state.output,
                        gas_spent: gas - BigInt::from(state.gas),
                    },
                    chain,
                ),
                Outcome::Error { reason, gas: left } => (
                    Response::Exception {
                        reason: failure_name(reason),
                        gas_spent: gas - BigInt::from(left),
                    },
                    chain.clone(),
                ),
                Outcome::Revert { output, gas: left } => (
                    Response::Revert {
                        data: output,
                        gas_spent: gas - BigInt::from(left),
                    },
                    chain.clone(),
                ),
            },
        )
    }
}
fn check(row: &J, actual: Result<Outcome<J>, Fault<&'static str, J>>) {
    let r = &row["result"];
    if r["kind"] == "native" {
        if r["reason"] == atom("out_of_gas") {
            assert_eq!(
                actual,
                Err(Fault::Runtime(runtime::Fault::Native(
                    runtime::Native::OutOfGas
                )))
            );
            return;
        }
        let reason = &r["reason"][1];
        if reason[0] == atom("badarg") {
            assert_eq!(
                actual,
                Err(Fault::Runtime(runtime::Fault::Native(
                    runtime::Native::BadStackPeek {
                        index: uint(&reason[1]).try_into().unwrap(),
                        stack: reason[2][1]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|n| Word::from_unsigned(&uint(n)))
                            .collect(),
                    }
                )))
            );
            return;
        }
        assert_eq!(reason[0], atom("case_clause"));
        if reason[1][0] == "integer" {
            assert_eq!(
                actual,
                Err(Fault::VersionCase(
                    Version::from_id(uint(&reason[1]).try_into().unwrap()).unwrap()
                ))
            );
            return;
        }
        let v = &reason[1][1];
        assert_eq!(v[0], atom("ok"));
        assert_eq!(
            actual,
            Err(Fault::ResponseCase {
                response: Response::Complete {
                    data: bytes(&v[1]),
                    gas_spent: number(&v[2])
                },
                state: Box::new(state(row, &v[3])),
                chain: Box::new(logical(field(&v[3], "chain_state").clone()))
            }),
            "{row}"
        );
        return;
    }
    let v = &r["value"][1];
    let expected = match v[0][1].as_str().unwrap() {
        "ok" => Outcome::Complete {
            state: Box::new(state(row, &v[1])),
            chain: logical(field(&v[1], "chain_state").clone()),
        },
        "error" => {
            let Ok(Outcome::Error { reason, gas }) = actual else {
                panic!("{actual:?}: {row}")
            };
            let reason = match reason {
                Failure::IllegalInstruction(op) => {
                    tuple(vec![atom("illegal_instruction"), int(op)])
                }
                other => atom(&failure_name(other)),
            };
            assert_eq!(reason, v[1], "{row}");
            assert_eq!(gas, uint(&v[2]), "{row}");
            return;
        }
        "revert" => Outcome::Revert {
            output: bytes(&v[1]),
            gas: uint(&v[2]),
        },
        x => panic!("{x}"),
    };
    assert_eq!(actual, Ok(expected), "{row}");
}
#[test]
fn original_outer_invocations_finalize_only_after_successful_execution() {
    let rows = INVOCATIONS["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 225);
    let mut staged_then_failed = 0;
    for row in rows {
        let s = state(row, &row["before"]);
        let before = s.clone();
        let chain = logical(field(&row["before"], "chain_state").clone());
        let original = chain.clone();
        let host = Source {
            mode: row["mode"].as_str().unwrap(),
            events: RefCell::new(vec![]),
            fail: Cell::new(None),
        };
        let input = ty(&row["type"]);
        assert_eq!(
            runtime::initialize(&s, Some(&input), Limits::default()).unwrap(),
            state(row, &row["ready"])
        );
        let actual = outer_call::invoke(
            &s,
            Some(&input),
            &chain,
            &host,
            OrderProfile::Otp26Word64Crc32c,
            Limits::default(),
        );
        if !matches!(actual, Ok(Outcome::Complete { .. }))
            && host
                .events
                .borrow()
                .iter()
                .any(|e| e[0] == "tuple" && e[1][0] == atom("set_store"))
        {
            staged_then_failed += 1;
        }
        check(row, actual);
        assert_eq!(
            logical(list(host.events.borrow().clone())),
            logical(row["host_calls"].clone()),
            "{row}"
        );
        assert_eq!(s, before);
        assert_eq!(chain, original);
    }
    assert_eq!(staged_then_failed, 24);
}
#[test]
fn nested_calls_share_operational_budgets_and_unwind_on_failure() {
    let row = EXECUTION["nested"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            let chain = field(&r["before"], "chain_state");
            r["vm"] == 6
                && field(chain, "depth") == &int(3)
                && field(chain, "leaf") == &atom("return")
                && field(&r["before"], "gas") == &int(20000)
        })
        .unwrap();
    let s = state(row, &row["before"]);
    let before = s.clone();
    let chain = logical(field(&row["before"], "chain_state").clone());
    let original = chain.clone();
    let host = Source {
        mode: "nested",
        events: RefCell::new(vec![]),
        fail: Cell::new(None),
    };
    let expected = outer_call::evaluate(&s, &chain, &host, Limits::default());
    check(row, expected.clone());
    let mut stopped_after_child_entry = false;
    for dim in 0..4 {
        let mut stopped = false;
        let mut reached = false;
        for n in [
            0, 1, 2, 3, 4, 8, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000,
        ] {
            let mut limits = Limits::default();
            match dim {
                0 => limits.max_work = n,
                1 => limits.max_bytes = n,
                2 => limits.max_depth = n,
                _ => limits.max_memory_words = n,
            }
            host.events.borrow_mut().clear();
            match outer_call::evaluate(&s, &chain, &host, limits) {
                Err(Fault::Runtime(runtime::Fault::Stop(Stop::ResourceLimit))) => {
                    stopped = true;
                    stopped_after_child_entry |= host
                        .events
                        .borrow()
                        .iter()
                        .filter(|e| e[0] == "tuple" && e[1][0] == atom("call"))
                        .count()
                        >= 2;
                }
                actual => {
                    assert_eq!(actual, expected);
                    reached = true;
                    break;
                }
            }
        }
        assert!(stopped && reached);
    }
    assert!(stopped_after_child_entry);
    assert_eq!(s, before);
    assert_eq!(chain, original);
    let limits = Limits {
        max_depth: 3,
        ..Limits::default()
    };
    let mut budget = Budget::new(limits);
    let invocation = INVOCATIONS["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["vm"] == 6 && r["mode"] == "normal" && field(&r["before"], "gas") == &int(20000)
        })
        .unwrap();
    let s = state(invocation, &invocation["before"]);
    let input = ty(&invocation["type"]);
    host.fail.set(Some("call"));
    assert_eq!(
        outer_call::invoke_budgeted(
            &s,
            Some(&input),
            &chain,
            &host,
            OrderProfile::Otp26Word64Crc32c,
            &mut budget
        ),
        Err(Fault::Runtime(runtime::Fault::Host("call")))
    );
    host.fail.set(None);
    let shallow = State::new(s.version, vec![0], 100u8.into(), s.float_profile);
    let shallow_type = ty(&tuple(vec![atom("tuple"), list(vec![])]));
    let mut shallow = shallow;
    shallow.out_type = Some(ty(&atom("word")));
    shallow.data = word(0).to_vec();
    assert!(matches!(
        outer_call::invoke_budgeted(
            &shallow,
            Some(&shallow_type),
            &chain,
            &host,
            OrderProfile::Otp26Word64Crc32c,
            &mut budget
        ),
        Ok(Outcome::Complete { .. })
    ));
}
#[test]
fn full_call_invocations_preserve_inputs_through_host_and_budget_failures() {
    let mut checked = 0;
    for row in INVOCATIONS["runs"].as_array().unwrap().iter().filter(|r| {
        r["vm"] == 6
            && ["normal", "nested"].iter().any(|m| r["mode"] == *m)
            && field(&r["before"], "gas") == &int(20000)
            && field(&r["before"], "protocol_version") == &int(4)
    }) {
        checked += 1;
        let s = state(row, &row["before"]);
        let before = s.clone();
        let chain = logical(field(&row["before"], "chain_state").clone());
        let original = chain.clone();
        let input = ty(&row["type"]);
        let host = Source {
            mode: row["mode"].as_str().unwrap(),
            events: RefCell::new(vec![]),
            fail: Cell::new(None),
        };
        let run = |limits| {
            outer_call::invoke(
                &s,
                Some(&input),
                &chain,
                &host,
                OrderProfile::Otp26Word64Crc32c,
                limits,
            )
        };
        let expected = run(Limits::default());
        check(row, expected.clone());
        for stage in ["types", "balance", "call", "set_store"] {
            host.fail.set(Some(stage));
            assert_eq!(
                run(Limits::default()),
                Err(Fault::Runtime(runtime::Fault::Host(stage)))
            );
            host.fail.set(None);
            assert_eq!(run(Limits::default()), expected);
        }
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
                match run(limits) {
                    Err(Fault::Runtime(runtime::Fault::Stop(Stop::ResourceLimit))) => {
                        stopped = true
                    }
                    actual => {
                        assert_eq!(actual, expected);
                        reached = true;
                        break;
                    }
                }
            }
            assert!(stopped && reached);
        }
        assert_eq!(s, before);
        assert_eq!(chain, original);
    }
    assert_eq!(checked, 2);
}
#[test]
fn original_call_edges_sequences_contexts_and_nested_vms_match() {
    for (group, count, expected_events) in [
        ("edges", 810, 102),
        ("sequences", 100, 432),
        ("contexts", 45, 47),
        ("nested", 216, 768),
    ] {
        let rows = EXECUTION[group].as_array().unwrap();
        assert_eq!(rows.len(), count);
        let mut events = 0;
        for row in rows {
            let s = state(row, &row["before"]);
            let before = s.clone();
            let chain = logical(field(&row["before"], "chain_state").clone());
            let original = chain.clone();
            let host = Source {
                mode: row["mode"].as_str().unwrap(),
                events: RefCell::new(vec![]),
                fail: Cell::new(None),
            };
            check(
                row,
                outer_call::evaluate(&s, &chain, &host, Limits::default()),
            );
            assert_eq!(
                logical(list(host.events.borrow().clone())),
                logical(row["host_calls"].clone()),
                "{group}: {row}"
            );
            events += host.events.borrow().len();
            assert_eq!(s, before);
            assert_eq!(chain, original);
        }
        assert_eq!(events, expected_events);
    }
}
#[test]
fn original_outer_call_control_gas_state_and_host_order_match() {
    let rows = DATA["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 1696);
    let mut events = 0;
    for row in rows {
        let s = state(row, &row["before"]);
        let before = s.clone();
        let chain = logical(field(&row["before"], "chain_state").clone());
        let original = chain.clone();
        let host = Source {
            mode: row["mode"].as_str().unwrap(),
            events: RefCell::new(vec![]),
            fail: Cell::new(None),
        };
        check(
            row,
            outer_call::evaluate(&s, &chain, &host, Limits::default()),
        );
        assert_eq!(
            logical(list(host.events.borrow().clone())),
            logical(row["host_calls"].clone()),
            "{row}"
        );
        events += host.events.borrow().len();
        assert_eq!(s, before);
        assert_eq!(chain, original);
    }
    assert_eq!(events, 1950);
}
#[test]
fn outer_call_resource_and_host_failures_preserve_input_and_retry() {
    for row in DATA["runs"].as_array().unwrap().iter().filter(|r| {
        r["vm"] == 6
            && r["gas"] == 10000
            && r["requested"] == 50
            && ["normal", "local_get", "local_put", "local_tolist"]
                .iter()
                .any(|m| r["mode"] == *m)
    }) {
        let s = state(row, &row["before"]);
        let before = s.clone();
        let chain = logical(field(&row["before"], "chain_state").clone());
        let host = Source {
            mode: row["mode"].as_str().unwrap(),
            events: RefCell::new(vec![]),
            fail: Cell::new(None),
        };
        let expected = outer_call::evaluate(&s, &chain, &host, Limits::default());
        check(row, expected.clone());
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
                match outer_call::evaluate(&s, &chain, &host, l) {
                    Err(Fault::Runtime(runtime::Fault::Stop(Stop::ResourceLimit))) => {
                        stopped = true
                    }
                    result => {
                        assert_eq!(result, expected);
                        reached = true;
                        break;
                    }
                }
                assert_eq!(s, before);
            }
            assert!(stopped && reached);
        }
        for stage in ["types", "balance", "call", "store"] {
            let applicable = stage == "balance"
                || (row["mode"] == "normal" && ["types", "call"].contains(&stage))
                || (row["mode"] == "local_tolist" && stage == "store");
            if !applicable {
                continue;
            }
            host.fail.set(Some(stage));
            assert_eq!(
                outer_call::evaluate(&s, &chain, &host, Limits::default()),
                Err(Fault::Runtime(runtime::Fault::Host(stage)))
            );
            assert_eq!(s, before);
            host.fail.set(None);
            assert_eq!(
                outer_call::evaluate(&s, &chain, &host, Limits::default()),
                expected
            );
        }
    }
}

#[test]
fn suspended_invocations_match_original_states_native_failures_and_host_order() {
    let rows = INVOCATIONS["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 225);
    for row in rows {
        let s = state(row, &row["before"]);
        let before = s.clone();
        let chain = logical(field(&row["before"], "chain_state").clone());
        let original = chain.clone();
        let host = Source {
            mode: row["mode"].as_str().unwrap(),
            events: RefCell::new(vec![]),
            fail: Cell::new(None),
        };
        let input = ty(&row["type"]);
        let mut budget = Budget::new(Limits::default());
        budget.check_call_depth(1).unwrap();
        let mut progress = outer_call::start(
            &s,
            Some(&input),
            &chain,
            &host,
            OrderProfile::Otp26Word64Crc32c,
            &mut budget,
        );
        let actual = loop {
            match progress {
                Ok(outer_call::Progress::Complete(outcome)) => break Ok(outcome),
                Ok(outer_call::Progress::Call(call)) => {
                    let (response, next) =
                        match host.call(call.request(), call.chain(), &mut budget) {
                            Ok(outcome) => outcome,
                            Err(e) => break Err(e),
                        };
                    progress = outer_call::resume(
                        *call,
                        response,
                        next,
                        &host,
                        OrderProfile::Otp26Word64Crc32c,
                        &mut budget,
                    );
                }
                Err(e) => break Err(e),
            }
        };
        check(row, actual);
        assert_eq!(
            logical(list(host.events.borrow().clone())),
            logical(row["host_calls"].clone()),
            "{row}"
        );
        assert_eq!(s, before);
        assert_eq!(chain, original);
    }
}
