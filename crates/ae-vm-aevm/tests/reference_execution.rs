use ae_vm_aevm::{
    execution::{self, Failure, Limits, Outcome, State, Stop},
    gas,
    memory::Memory,
    opcode::{self, GasTable, Version},
    word::Word,
};
use num_bigint::BigUint;
use num_traits::{One, Zero};
use serde_json::{Value as J, json};
use std::sync::LazyLock;

fn expand(v: &J, pool: &[J], ceiling: usize) -> J {
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
static DATA: LazyLock<J> = LazyLock::new(|| {
    let p: J = serde_json::from_str(include_str!("fixtures/erlang_execution.json")).unwrap();
    let pool = p["values"].as_array().unwrap();
    expand(&p["root"], pool, pool.len())
});
static EXPANDED: LazyLock<J> = LazyLock::new(|| {
    let p: J =
        serde_json::from_str(include_str!("fixtures/erlang_execution_expanded.json")).unwrap();
    let pool = p["values"].as_array().unwrap();
    expand(&p["root"], pool, pool.len())
});
fn logical(mut v: J) -> J {
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
fn integer(n: impl ToString) -> J {
    json!(["integer", n.to_string()])
}
fn atom(s: &str) -> J {
    json!(["atom", s])
}
fn map(v: Vec<(J, J)>) -> J {
    json!([
        "map",
        v.into_iter()
            .map(|(k, v)| json!([k, v]))
            .collect::<Vec<_>>()
    ])
}
fn memory(m: &Memory) -> J {
    let mut pairs = m
        .words()
        .iter()
        .map(|(k, v)| (integer(k), integer(v.unsigned())))
        .collect::<Vec<_>>();
    pairs.push((atom("mem_size"), integer(m.size_in_words())));
    map(pairs)
}
fn memory_state(m: &Memory) -> J {
    map(vec![(atom("memory"), memory(m))])
}
fn state(s: &State) -> J {
    map(vec![
        (atom("memory"), memory(&s.memory)),
        (atom("cp"), integer(s.cp)),
        (atom("gas"), integer(&s.gas)),
        (
            atom("stack"),
            json!([
                "list",
                s.stack
                    .iter()
                    .map(|v| integer(v.unsigned()))
                    .collect::<Vec<_>>()
            ]),
        ),
        (
            atom("jumpdests"),
            map(s
                .jump_destinations
                .iter()
                .map(|i| (integer(i), atom("true")))
                .collect()),
        ),
        (atom("out"), json!(["binary", ""])),
        (
            atom("return_data"),
            json!(["binary", hex::encode(&s.return_data)]),
        ),
        (atom("logs"), json!(["list", []])),
    ])
}
fn failure(f: Failure) -> J {
    atom(match f {
        Failure::Call(name) => return atom(&name),
        Failure::Arithmetic => "arithmetic_error",
        Failure::OutOfGas => "out_of_gas",
        Failure::OutOfStack => "out_of_stack",
        Failure::PopEmpty => "error_pop_empty_stack",
        Failure::DupEmpty => "error_dup_empty_stack",
        Failure::DupTooSmall => "error_dup_too_small_stack",
        Failure::SwapEmpty => "error_swap_empty_stack",
        Failure::SwapTooSmall => "error_swap_too_small_stack",
        Failure::IllegalInstruction(op) => {
            return json!(["tuple", [atom("illegal_instruction"), integer(op)]]);
        }
        Failure::InvalidJumpDestination(w) => {
            return json!([
                "tuple",
                [["tuple", [atom("invalid_jumpdest"), integer(w.unsigned())]]]
            ]);
        }
    })
}
fn outcome(o: Outcome) -> J {
    match o {
        Outcome::Complete(s) => json!({"kind":"ok","state":state(&s)}),
        Outcome::Error { reason, gas } => {
            json!({"kind":"error","reason":failure(reason),"gas":gas.to_string()})
        }
        Outcome::Revert { output, gas } => {
            json!({"kind":"revert","message":hex::encode(output),"gas":gas.to_string()})
        }
        Outcome::Native(execution::Native::Gas(gas::Error::BadArithmetic)) => {
            json!({"kind":"native","class":"error","reason":atom("badarith")})
        }
        Outcome::Native(execution::Native::BadMatchBinary(b)) => {
            json!({"kind":"native","class":"error","reason":["tuple",[["atom","badmatch"],["binary",hex::encode(b)]]]})
        }
    }
}
fn number(v: &J) -> BigUint {
    v.as_str().unwrap().parse().unwrap()
}
fn input(row: &J) -> State {
    let mut s = State::new(
        Version::from_id(row["vm"].as_u64().unwrap() as u32).unwrap(),
        hex::decode(row["code"].as_str().unwrap()).unwrap(),
        number(&row["gas"]),
        gas::FloatProfile::Otp26Word64,
    );
    s.stack = row["stack"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| Word::from_unsigned(&number(v)))
        .collect();
    if let Some(data) = row["data"].as_str() {
        s.data = hex::decode(data).unwrap();
    }
    if let Some(data) = row["return_data"].as_str() {
        s.return_data = hex::decode(data).unwrap();
    }
    s
}

#[test]
fn historical_arithmetic_stack_memory_and_control_flow_match_original_interpreter() {
    let mut counts = [0usize; 2];
    for section in ["arithmetic", "boundaries", "programs"] {
        for (i, row) in DATA[section].as_array().unwrap().iter().enumerate() {
            let s = input(row);
            let before = s.clone();
            let result = execution::evaluate(&s, Limits::default()).unwrap();
            counts[usize::from(!matches!(&result, Outcome::Complete(_)))] += 1;
            assert_eq!(
                logical(outcome(result)),
                logical(row["result"].clone()),
                "{section}/{i}: {row}"
            );
            assert_eq!(s, before);
        }
    }
    assert_eq!(counts, [21665, 7200]);
}
#[test]
fn opcode_admission_and_original_metadata_match_all_historical_profiles() {
    for row in DATA["gates"].as_array().unwrap() {
        let vm = Version::from_id(row["vm"].as_u64().unwrap() as u32).unwrap();
        assert_eq!(
            vm.permits(row["opcode"].as_u64().unwrap() as u8),
            row["valid"].as_bool().unwrap(),
            "{row}"
        );
    }
    for row in DATA["metadata"].as_array().unwrap() {
        let actual =
            match opcode::metadata(row["opcode"].as_u64().unwrap() as u8, &GasTable::default()) {
                Some((a, b, c)) => {
                    json!({"kind":"ok","value":["tuple",[integer(a),integer(b),integer(c)]]})
                }
                None => json!({"kind":"native","class":"error","reason":atom("function_clause")}),
            };
        assert_eq!(actual, row["result"], "{row}");
    }
}
fn gas_result(r: Result<BigUint, gas::Error>) -> J {
    match r {
        Ok(n) => json!({"kind":"ok","value":integer(n)}),
        Err(gas::Error::BadArithmetic) => {
            json!({"kind":"native","class":"error","reason":atom("badarith")})
        }
    }
}
#[test]
fn native_float_gas_rounding_and_sparse_memory_traces_match_original_helpers() {
    for row in DATA["gas_helpers"].as_array().unwrap() {
        let size = number(&row["size"]);
        let table = GasTable::default();
        assert_eq!(
            gas_result(gas::memory(&size, &table, gas::FloatProfile::Otp26Word64)),
            row["memory"],
            "memory {size}"
        );
        assert_eq!(
            gas_result(gas::memory_limit(
                &size,
                &table,
                gas::FloatProfile::Otp26Word64
            )),
            row["limit"],
            "limit {size}"
        );
        assert_eq!(
            gas_result(gas::all_but_one_64th(&size, gas::FloatProfile::Otp26Word64)),
            row["cap"],
            "cap {size}"
        );
    }
    for row in DATA["memory_traces"].as_array().unwrap() {
        let a = number(&row["address"]);
        let mut m = Memory::default();
        let mut states = Vec::new();
        m.store(&a, Word(std::array::from_fn(|i| i as u8)));
        states.push(memory_state(&m));
        m.store8(&(&a + 31u8), 255);
        states.push(memory_state(&m));
        let loaded = m.load(&(&a + 1u8));
        states.push(memory_state(&m));
        let from = if a.is_zero() { a.clone() } else { &a - 1u8 };
        let area = m.area(&from, 34, 34).unwrap();
        states.push(memory_state(&m));
        m.store(&a, Word::ZERO);
        states.push(memory_state(&m));
        let zero = m.area(&(BigUint::one() << 300usize), 0, 0).unwrap();
        states.push(memory_state(&m));
        assert_eq!(
            logical(json!(states)),
            logical(row["states"].clone()),
            "memory {a}"
        );
        assert_eq!(loaded.unsigned(), number(&row["loaded"]));
        assert_eq!(hex::encode(area), row["area"]);
        assert_eq!(hex::encode(zero), row["zero"]);
    }
}
#[test]
fn native_limb_rounding_matches_expanded_gas_helpers() {
    assert_eq!(EXPANDED["gas_helpers"].as_array().unwrap().len(), 1254);
    let profile = gas::FloatProfile::Otp26Word64;
    let table = GasTable::default();
    for row in EXPANDED["gas_helpers"].as_array().unwrap() {
        let size = number(&row["size"]);
        assert_eq!(
            gas_result(gas::memory(&size, &table, profile)),
            row["memory"],
            "memory {size}"
        );
        assert_eq!(
            gas_result(gas::memory_limit(&size, &table, profile)),
            row["limit"],
            "limit {size}"
        );
        assert_eq!(
            gas_result(gas::all_but_one_64th(&size, profile)),
            row["cap"],
            "cap {size}"
        );
        assert_eq!(
            gas_result(gas::copy_words(&size, profile).map(|n| n * table.copy + table.very_low)),
            row["copy"],
            "copy {size}"
        );
    }
}
#[test]
fn expanded_native_data_padding_push_widths_and_random_words_match_original() {
    let mut count = 0;
    for section in ["data_programs", "pushes", "random"] {
        for (i, row) in EXPANDED[section].as_array().unwrap().iter().enumerate() {
            let s = input(row);
            let actual = execution::evaluate(&s, Limits::default()).unwrap();
            assert_eq!(
                logical(outcome(actual)),
                logical(row["result"].clone()),
                "{section}/{i}: {row}"
            );
            count += 1;
        }
    }
    assert_eq!(count, 7755);
}
#[test]
fn operational_limits_preserve_inputs_and_retry_to_native_outcomes() {
    let programs = [
        vec![0x60, 7, 0x60, 2, 10, 0],
        vec![0x60, 42, 0x60, 1, 0x52, 0x60, 1, 0x51, 0],
        vec![0x5b, 0x60, 0, 0x56],
        vec![0x60, 0x60, 0x60, 0, 0x60, 31, 0x39, 0],
    ];
    for code in programs {
        let s = State::new(
            Version::Sophia4,
            code,
            100u32.into(),
            gas::FloatProfile::Otp26Word64,
        );
        let before = s.clone();
        let expected = execution::evaluate(&s, Limits::default()).unwrap();
        for field in 0..3 {
            let mut stopped = 0;
            let mut reached = false;
            for allowance in 0..2000 {
                let mut limits = Limits::default();
                match field {
                    0 => limits.max_work = allowance,
                    1 => limits.max_bytes = allowance,
                    _ => limits.max_memory_words = allowance,
                };
                match execution::evaluate(&s, limits) {
                    Err(Stop::ResourceLimit) => stopped += 1,
                    Ok(result) => {
                        assert_eq!(result, expected);
                        reached = true;
                        break;
                    }
                    other => panic!("{other:?}"),
                }
                assert_eq!(s, before);
            }
            assert!(reached);
            if field < 2 {
                assert!(stopped > 0);
            }
            assert_eq!(
                execution::evaluate(&s, Limits::default()).unwrap(),
                expected
            );
        }
    }
    let s = State::new(
        Version::Sophia4,
        vec![0x31],
        0u8.into(),
        gas::FloatProfile::Otp26Word64,
    );
    assert_eq!(
        execution::evaluate(&s, Limits::default()),
        Err(Stop::UnimplementedInstruction(0x31))
    );
    let mut memory = Memory::default();
    memory.store(&1u8.into(), Word::from(42));
    let before = memory.clone();
    assert_eq!(
        memory.area(&1000u32.into(), 33, 32),
        Err(Stop::ResourceLimit)
    );
    assert_eq!(memory, before);
}
