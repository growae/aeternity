use ae_vm_aevm::{
    execution::{self, Context, Failure, Limits, Log, Outcome, State, Stop},
    gas,
    memory::Memory,
    opcode::Version,
    runtime_heap::{Type, TypeRef},
    word::Word,
};
use num_bigint::{BigInt, BigUint};
use num_traits::{ToPrimitive, Zero};
use serde_json::{Value as J, json};
use std::sync::{Arc, LazyLock};
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
fn fixture(text: &str) -> J {
    let p: J = serde_json::from_str(text).unwrap();
    let pool = p["values"].as_array().unwrap();
    expand(&p["root"], pool, pool.len())
}
static DATA: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_completion.json")));
static EXPANDED: LazyLock<J> =
    LazyLock::new(|| fixture(include_str!("fixtures/erlang_completion_expanded.json")));
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
fn int(n: impl ToString) -> J {
    json!(["integer", n.to_string()])
}
fn atom(s: &str) -> J {
    json!(["atom", s])
}
fn binary(b: impl AsRef<[u8]>) -> J {
    json!(["binary", hex::encode(b)])
}
fn list(v: Vec<J>) -> J {
    json!(["list", v])
}
fn tuple(v: Vec<J>) -> J {
    json!(["tuple", v])
}
fn map(v: Vec<(J, J)>) -> J {
    json!([
        "map",
        v.into_iter()
            .map(|(k, v)| json!([k, v]))
            .collect::<Vec<_>>()
    ])
}
fn field<'a>(v: &'a J, key: &str) -> Option<&'a J> {
    v[1].as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom(key))
        .map(|p| &p[1])
}
fn number(v: &J) -> BigInt {
    assert_eq!(v[0], "integer");
    v[1].as_str().unwrap().parse().unwrap()
}
fn bytes(v: &J) -> Vec<u8> {
    assert_eq!(v[0], "binary");
    hex::decode(v[1].as_str().unwrap()).unwrap()
}
fn word(v: &J) -> Word {
    Word::wrapping(&number(v))
}
fn ty(v: &J) -> Option<TypeRef> {
    let node = if v[0] == "atom" {
        match v[1].as_str().unwrap() {
            "word" => Type::Word,
            "string" => Type::String,
            "typerep" => Type::TypeRep,
            "undefined" => return None,
            s => panic!("{s}"),
        }
    } else {
        let a = v[1].as_array().unwrap();
        match a[0][1].as_str().unwrap() {
            "list" => Type::List(ty(&a[1]).unwrap()),
            "tuple" => Type::Tuple(
                a[1][1]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|t| ty(t).unwrap())
                    .collect(),
            ),
            "variant" => Type::Variant(
                a[1][1]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|ts| {
                        ts[1]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|t| ty(t).unwrap())
                            .collect()
                    })
                    .collect(),
            ),
            "map" => Type::Map(ty(&a[1]).unwrap(), ty(&a[2]).unwrap()),
            s => panic!("{s}"),
        }
    };
    Some(Arc::new(node))
}
fn input(row: &J) -> State {
    let initial = &row["initial"];
    let mut s = State::new(
        Version::from_id(row["vm"].as_u64().unwrap() as u32).unwrap(),
        hex::decode(row["code"].as_str().unwrap()).unwrap(),
        number(field(initial, "gas").unwrap()).to_biguint().unwrap(),
        gas::FloatProfile::Otp26Word64,
    );
    s.cp = number(field(initial, "cp").unwrap()).to_usize().unwrap();
    s.stack = field(initial, "stack").unwrap()[1]
        .as_array()
        .unwrap()
        .iter()
        .map(word)
        .collect();
    let m = field(initial, "memory").unwrap();
    for pair in m[1].as_array().unwrap() {
        if pair[0][0] == "integer" {
            s.memory
                .store(&number(&pair[0]).to_biguint().unwrap(), word(&pair[1]));
        }
    }
    let size = number(field(m, "mem_size").unwrap()).to_biguint().unwrap();
    if !size.is_zero() {
        s.memory.load(&(size * 32u8 - 32u8));
    }
    s.output = bytes(field(initial, "out").unwrap());
    s.return_data = bytes(field(initial, "return_data").unwrap());
    s.logs = field(initial, "logs").unwrap()[1]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let p = v[1].as_array().unwrap();
            Log {
                address: Word(bytes(&p[0]).try_into().unwrap()),
                topics: p[1][1]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|b| Word(bytes(b).try_into().unwrap()))
                    .collect(),
                data: bytes(&p[2]),
            }
        })
        .collect();
    let get = |key| number(field(&row["context"], key).unwrap());
    s.context = Context {
        address: number(field(initial, "address").unwrap()),
        creator: get("creator"),
        origin: get("origin"),
        caller: get("caller"),
        value: get("value"),
        gas_price: get("gas_price"),
        coinbase: get("coinbase"),
        timestamp: get("timestamp"),
        number: get("number"),
        difficulty: get("difficulty"),
        gas_limit: get("gas_limit"),
    };
    s.out_type = ty(&row["out_type"]);
    s
}
fn memory(m: &Memory) -> J {
    let mut pairs = m
        .words()
        .iter()
        .map(|(k, v)| (int(k), int(v.unsigned())))
        .collect::<Vec<_>>();
    pairs.push((atom("mem_size"), int(m.size_in_words())));
    map(pairs)
}
fn state(s: &State) -> J {
    map(vec![
        (atom("address"), int(&s.context.address)),
        (atom("memory"), memory(&s.memory)),
        (atom("cp"), int(s.cp)),
        (atom("gas"), int(&s.gas)),
        (
            atom("stack"),
            list(s.stack.iter().map(|v| int(v.unsigned())).collect()),
        ),
        (atom("out"), binary(&s.output)),
        (atom("return_data"), binary(&s.return_data)),
        (
            atom("logs"),
            list(
                s.logs
                    .iter()
                    .map(|l| {
                        tuple(vec![
                            binary(l.address.0),
                            list(l.topics.iter().map(|t| binary(t.0)).collect()),
                            binary(&l.data),
                        ])
                    })
                    .collect(),
            ),
        ),
        (
            atom("jumpdests"),
            map(s
                .jump_destinations
                .iter()
                .map(|p| (int(p), atom("true")))
                .collect()),
        ),
    ])
}
fn reason(f: Failure) -> J {
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
            return tuple(vec![atom("illegal_instruction"), int(op)]);
        }
        Failure::InvalidJumpDestination(p) => {
            return tuple(vec![tuple(vec![
                atom("invalid_jumpdest"),
                int(p.unsigned()),
            ])]);
        }
    })
}
fn result(out: Outcome) -> J {
    match out {
        Outcome::Complete(s) => json!({"kind":"ok","state":state(&s)}),
        Outcome::Error { reason: r, gas } => {
            json!({"kind":"error","reason":reason(r),"gas":gas.to_string()})
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
#[test]
fn native_context_hash_logs_and_terminal_state_match_original_interpreter() {
    let mut counts = [0; 3];
    for (i, row) in DATA["cases"].as_array().unwrap().iter().enumerate() {
        let s = input(row);
        let before = s.clone();
        let actual = execution::evaluate(&s, Limits::default()).unwrap();
        counts[match &actual {
            Outcome::Complete(_) => 0,
            Outcome::Error { .. } => 1,
            Outcome::Revert { .. } => 2,
            _ => panic!("unexpected native"),
        }] += 1;
        assert_eq!(
            logical(result(actual)),
            logical(row["result"].clone()),
            "{i}: {row}"
        );
        assert_eq!(s, before);
    }
    assert_eq!(counts, [2014, 3761, 65]);
}
#[test]
fn stopped_hash_log_and_return_attempts_preserve_snapshots_and_retry() {
    let mut selected = 0;
    for row in DATA["cases"].as_array().unwrap() {
        if row["vm"] != 6 || number(field(&row["initial"], "gas").unwrap()) != 10000.into() {
            continue;
        }
        if ![
            "[hash,31,34]",
            "[log,4,31,34]",
            "[return_string]",
            "[revert_string]",
            "[return_bad_string]",
        ]
        .contains(&row["case_id"].as_str().unwrap())
        {
            continue;
        }
        selected += 1;
        let s = input(row);
        let before = s.clone();
        let expected = execution::evaluate(&s, Limits::default()).unwrap();
        for field in 0..3 {
            let mut reached = false;
            let mut stopped = false;
            for allowance in 0..8192 {
                let mut limits = Limits::default();
                match field {
                    0 => limits.max_work = allowance,
                    1 => limits.max_bytes = allowance,
                    _ => limits.max_memory_words = allowance,
                };
                match execution::evaluate(&s, limits) {
                    Err(Stop::ResourceLimit) => stopped = true,
                    Ok(out) => {
                        assert_eq!(out, expected);
                        reached = true;
                        break;
                    }
                    other => panic!("{other:?}"),
                };
                assert_eq!(s, before);
            }
            assert!(reached && stopped);
        }
        assert_eq!(logical(result(expected)), logical(row["result"].clone()));
    }
    assert_eq!(selected, 5);
    let mut s = State::new(
        Version::Sophia4,
        vec![0xf3],
        BigUint::from(100u8),
        gas::FloatProfile::Otp26Word64,
    );
    s.stack = vec![Word::ZERO, Word::ZERO];
    s.out_type = Some(Arc::new(Type::Map(
        Arc::new(Type::Word),
        Arc::new(Type::Word),
    )));
    assert_eq!(
        execution::evaluate(&s, Limits::default()),
        Ok(Outcome::Error {
            reason: Failure::OutOfGas,
            gas: BigUint::zero()
        })
    );
}

#[test]
fn native_heap_graphs_padding_types_and_stack_widths_match_original() {
    let mut counts = [0; 2];
    for (i, row) in EXPANDED["cases"].as_array().unwrap().iter().enumerate() {
        let s = input(row);
        let before = s.clone();
        let actual = execution::evaluate(
            &s,
            Limits {
                max_depth: 2048,
                ..Limits::default()
            },
        )
        .unwrap();
        counts[match &actual {
            Outcome::Complete(_) => 0,
            Outcome::Error { .. } => 1,
            _ => panic!("unexpected terminal outcome"),
        }] += 1;
        assert_eq!(
            logical(result(actual)),
            logical(row["result"].clone()),
            "{i}: {row}"
        );
        assert_eq!(s, before);
    }
    assert_eq!(counts, [939, 3130]);
}

#[test]
fn deep_native_heap_copy_is_iterative_and_depth_stops_are_retryable() {
    let mut selected = 0;
    for row in EXPANDED["cases"].as_array().unwrap() {
        if !row["case_id"].as_str().unwrap().starts_with("[deep_list,") {
            continue;
        }
        selected += 1;
        let s = input(row);
        let before = s.clone();
        assert_eq!(
            execution::evaluate(&s, Limits::default()),
            Err(Stop::ResourceLimit)
        );
        assert_eq!(s, before);
        let actual = execution::evaluate(
            &s,
            Limits {
                max_depth: 2048,
                ..Limits::default()
            },
        )
        .unwrap();
        assert_eq!(logical(result(actual)), logical(row["result"].clone()));
        assert_eq!(s, before);
    }
    assert_eq!(selected, 8);
    let mut s = State::new(
        Version::Sophia4,
        vec![0],
        0u8.into(),
        gas::FloatProfile::Otp26Word64,
    );
    s.context.address = 0.into();
    assert_eq!(
        execution::evaluate(&s, Limits::default()),
        Ok(Outcome::Native(execution::Native::BadMatchBinary(vec![])))
    );
}
