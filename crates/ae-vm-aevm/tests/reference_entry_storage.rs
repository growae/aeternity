use ae_vm_aevm::{
    execution::{self, Failure, Limits, State, Stop},
    gas::FloatProfile,
    maps::Store,
    opcode::Version,
    runtime::{self, Fault, Native, Outcome, StoreHost},
    runtime_heap::{Error as HeapError, Type},
    store::OrderProfile,
    word::Word,
};
use serde_json::{Value as J, json};
use std::{
    cell::{Cell, RefCell},
    sync::{Arc, LazyLock},
};
mod support;
use support::*;
static DATA: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_entry_storage.json")));
const ORDER: OrderProfile = OrderProfile::Otp26Word64Crc32c;
fn optional<'a>(v: &'a J, key: &str) -> Option<&'a J> {
    v[1].as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom(key))
        .map(|p| &p[1])
}
fn input(row: &J) -> State {
    let v = &row["before"];
    let mut s = State::new(
        Version::from_id(row["vm"].as_u64().unwrap() as u32).unwrap(),
        bytes(field(v, "code")),
        uint(field(v, "gas")),
        FloatProfile::Otp26Word64,
    );
    s.context.address = number(field(v, "address"));
    s.cp = uint(field(v, "cp")).try_into().unwrap();
    s.data = bytes(field(v, "data"));
    s.output = bytes(field(v, "out"));
    s.return_data = bytes(field(v, "return_data"));
    s.stack = field(v, "stack")[1]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| Word::from_unsigned(&uint(n)))
        .collect();
    s.jump_destinations = field(v, "jumpdests")[1]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| uint(&p[0]).try_into().unwrap())
        .collect();
    s.solidity_storage = field(v, "storage")[1]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| (uint(&p[0]), uint(&p[1])))
        .collect();
    let memory = field(v, "memory");
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
    s.self_destruct = optional(v, "selfdestruct").map(|n| Word::from_unsigned(&uint(n)));
    assert_eq!(field(v, "logs"), &list(vec![]));
    s
}
fn store_term(s: &Store) -> J {
    map(s.iter().map(|(k, v)| (binary(k), binary(v))).collect())
}
fn projection(s: &State, runtime: bool) -> J {
    let mut memory = s
        .memory
        .words()
        .iter()
        .map(|(k, v)| (int(k), int(v.unsigned())))
        .collect::<Vec<_>>();
    memory.push((atom("mem_size"), int(s.memory.size_in_words())));
    assert!(s.logs.is_empty() && s.call_creates.is_empty());
    let mut fields = vec![
        (atom("address"), int(&s.context.address)),
        (atom("code"), binary(&s.code)),
        (atom("cp"), int(s.cp)),
        (atom("gas"), int(&s.gas)),
        (atom("data"), binary(&s.data)),
        (
            atom("stack"),
            list(s.stack.iter().map(|w| int(w.unsigned())).collect()),
        ),
        (atom("memory"), map(memory)),
        (atom("logs"), list(vec![])),
        (atom("out"), binary(&s.output)),
        (atom("return_data"), binary(&s.return_data)),
        (atom("callcreates"), list(vec![])),
        (
            atom("jumpdests"),
            map(s
                .jump_destinations
                .iter()
                .map(|i| (int(i), atom("true")))
                .collect()),
        ),
        (
            atom("storage"),
            if runtime && s.version != Version::Solidity1 {
                store_term(&s.map_store)
            } else {
                map(s
                    .solidity_storage
                    .iter()
                    .map(|(k, v)| (int(k), int(v)))
                    .collect())
            },
        ),
    ];
    if let Some(value) = s.self_destruct {
        fields.push((atom("selfdestruct"), int(value.unsigned())));
    }
    map(fields)
}
fn failure(f: Failure) -> J {
    match f {
        Failure::IllegalInstruction(op) => tuple(vec![atom("illegal_instruction"), int(op)]),
        Failure::OutOfGas => atom("out_of_gas"),
        Failure::PopEmpty => atom("error_pop_empty_stack"),
        Failure::OutOfStack => atom("out_of_stack"),
        other => panic!("{other:?}"),
    }
}
fn native(n: Native) -> J {
    let (class, reason) = match n {
        Native::BadMatchBinary(b) => ("error", tuple(vec![atom("badmatch"), binary(b)])),
        Native::Gas(_) => ("error", atom("badarith")),
        Native::BadCallData => (
            "throw",
            tuple(vec![atom("init_error"), atom("bad_call_data")]),
        ),
        Native::Heap(HeapError::NativeFunctionClause) => ("error", atom("function_clause")),
        other => panic!("{other:?}"),
    };
    json!({"kind":"native","class":class,"reason":reason})
}
fn core_result(out: execution::Outcome) -> J {
    let value = match out {
        execution::Outcome::Complete(s) => tuple(vec![atom("ok"), projection(&s, false)]),
        execution::Outcome::Error { reason, gas } => {
            tuple(vec![atom("error"), failure(reason), int(gas)])
        }
        execution::Outcome::Revert { output, gas } => {
            tuple(vec![atom("revert"), binary(output), int(gas)])
        }
        execution::Outcome::Native(n) => {
            let Fault::<()>::Native(n) = Fault::from(n) else {
                panic!()
            };
            return native(n);
        }
    };
    json!({"kind":"ok","value":value})
}
#[test]
fn storage_creation_and_selfdestruct_match_original_and_sophia_gates() {
    let rows = DATA["core"].as_array().unwrap();
    assert_eq!(rows.len(), 3280);
    let mut counts = [0usize; 3];
    for row in rows {
        let s = input(row);
        let before = s.clone();
        let actual = execution::evaluate(&s, Limits::default()).unwrap();
        counts[match actual {
            execution::Outcome::Complete(_) => 0,
            execution::Outcome::Error { .. } => 1,
            execution::Outcome::Revert { .. } => 2,
            _ => panic!(),
        }] += 1;
        assert_eq!(
            logical(core_result(actual)),
            logical(row["result"].clone()),
            "{row}"
        );
        assert_eq!(s, before);
    }
    assert_eq!(counts, [293, 2981, 6]);
}
#[test]
fn address_zero_preserves_state_and_badmatch_prefix_before_code_or_stack_checks() {
    let rows = DATA["entry"].as_array().unwrap();
    assert_eq!(rows.len(), 210);
    let mut complete = 0;
    for row in rows {
        let s = input(row);
        let before = s.clone();
        let actual = execution::evaluate(&s, Limits::default()).unwrap();
        if let execution::Outcome::Complete(ref next) = actual {
            complete += 1;
            assert_eq!(**next, s);
        }
        assert_eq!(
            logical(core_result(actual)),
            logical(row["result"].clone()),
            "{row}"
        );
        assert_eq!(s, before);
    }
    assert_eq!(complete, 120);
}
#[derive(Default)]
struct Host {
    calls: RefCell<Vec<Store>>,
    fail: Cell<bool>,
}
impl StoreHost for Host {
    type Chain = Vec<Store>;
    type Error = &'static str;
    fn set_store(&self, s: Store, chain: &Self::Chain) -> Result<Self::Chain, Fault<Self::Error>> {
        self.calls.borrow_mut().push(s.clone());
        if self.fail.get() {
            return Err(Fault::Host("write failed"));
        }
        let mut next = chain.clone();
        next.push(s);
        Ok(next)
    }
}
fn runtime_input(row: &J) -> State {
    let mut s = State::new(
        Version::from_id(row["vm"].as_u64().unwrap() as u32).unwrap(),
        bytes(&row["code"]),
        row["gas"].as_u64().unwrap().into(),
        FloatProfile::Otp26Word64,
    );
    s.data = bytes(&row["data"]);
    s.context.address = number(&row["address"]);
    s.jump_destinations = row["jumpdests"][1]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| uint(&p[0]).try_into().unwrap())
        .collect();
    s.self_destruct = Some(Word::from(row["selfdestruct"].as_u64().unwrap()));
    s.map_store = store(&row["store"]);
    s.out_type = Some(Arc::new(Type::Word));
    s
}
fn runtime_result(out: Result<Outcome<Vec<Store>>, Fault<&str>>) -> J {
    let value = match out {
        Ok(Outcome::Complete { state, chain }) => {
            let mut s = projection(&state, true);
            let chain = map(chain
                .last()
                .map(|s| vec![(atom("store"), store_term(s))])
                .unwrap_or_default());
            s[1].as_array_mut()
                .unwrap()
                .push(json!([atom("chain_state"), chain]));
            tuple(vec![atom("ok"), s])
        }
        Ok(Outcome::Error { reason, gas }) => tuple(vec![atom("error"), failure(reason), int(gas)]),
        Ok(Outcome::Revert { output, gas }) => {
            tuple(vec![atom("revert"), binary(output), int(gas)])
        }
        Err(Fault::Native(n)) => return native(n),
        other => panic!("{other:?}"),
    };
    json!({"kind":"ok","value":value})
}
fn invoke(s: &State, h: &Host, limits: Limits) -> Result<Outcome<Vec<Store>>, Fault<&'static str>> {
    runtime::invoke(s, Some(&Arc::new(Type::Word)), &vec![], h, ORDER, limits)
}
#[test]
fn entry_and_solidity_instructions_compose_with_original_init_store_finalization() {
    let rows = DATA["runtime"].as_array().unwrap();
    assert_eq!(rows.len(), 420);
    for row in rows {
        let s = runtime_input(row);
        let before = s.clone();
        let host = Host::default();
        assert_eq!(
            logical(runtime_result(invoke(&s, &host, Limits::default()))),
            logical(row["result"].clone()),
            "{row}"
        );
        assert_eq!(
            logical(list(host.calls.borrow().iter().map(store_term).collect())),
            logical(row["host_calls"].clone()),
            "host calls: {row}"
        );
        assert_eq!(s, before);
    }
}
#[test]
fn core_limits_and_runtime_host_failures_leave_retryable_inputs() {
    for row in DATA["core"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["vm"] == 2 && field(&r["before"], "gas") == &int(100000))
    {
        let s = input(row);
        let before = s.clone();
        for dimension in 0..3 {
            for n in [0, 1, 16, 128, 1024, 16384] {
                let mut limits = Limits::default();
                match dimension {
                    0 => limits.max_work = n,
                    1 => limits.max_bytes = n,
                    _ => limits.max_memory_words = n,
                }
                match execution::evaluate(&s, limits) {
                    Err(Stop::ResourceLimit) => {}
                    Ok(result) => {
                        assert_eq!(logical(core_result(result)), logical(row["result"].clone()));
                        break;
                    }
                    other => panic!("{other:?}"),
                }
                assert_eq!(s, before);
            }
        }
        assert_eq!(
            logical(core_result(
                execution::evaluate(&s, Limits::default()).unwrap()
            )),
            logical(row["result"].clone())
        );
    }
    for row in DATA["entry"].as_array().unwrap() {
        let s = input(row);
        let before = s.clone();
        match execution::evaluate(
            &s,
            Limits {
                max_bytes: 0,
                ..Limits::default()
            },
        ) {
            Err(Stop::ResourceLimit) => {}
            Ok(result) => assert_eq!(logical(core_result(result)), logical(row["result"].clone())),
            other => panic!("{other:?}"),
        }
        assert_eq!(s, before);
    }
    for row in DATA["runtime"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| !r["host_calls"][1].as_array().unwrap().is_empty())
    {
        let s = runtime_input(row);
        let before = s.clone();
        let host = Host::default();
        host.fail.set(true);
        assert_eq!(
            invoke(&s, &host, Limits::default()),
            Err(Fault::Host("write failed"))
        );
        assert_eq!(s, before);
        host.fail.set(false);
        host.calls.borrow_mut().clear();
        assert_eq!(
            logical(runtime_result(invoke(&s, &host, Limits::default()))),
            logical(row["result"].clone())
        );
    }
}
