use super::*;
use ae_vm_fate::{
    code::{Attribute, Code, Function},
    machine::program::{self, Entry, Frame},
    types::Bindings,
};
pub(super) fn code(j: &J) -> Code {
    let mut code = Code {
        functions: rows(&j["functions"])
            .iter()
            .map(|f| {
                (
                    bytes(&f["id"]),
                    Function {
                        attributes: rows(&f["attributes"])
                            .iter()
                            .map(|a| match string(a) {
                                "private" => Attribute::Private,
                                "payable" => Attribute::Payable,
                                _ => unreachable!(),
                            })
                            .collect(),
                        arguments: rows(&f["arguments"]).iter().map(values::ty).collect(),
                        returns: values::ty(&f["returns"]),
                        blocks: rows(&f["blocks"])
                            .iter()
                            .map(|b| {
                                (
                                    b[0].as_u64().unwrap() as usize,
                                    rows(&b[1]).iter().map(instruction).collect(),
                                )
                            })
                            .collect(),
                    },
                )
            })
            .collect(),
        ..Code::default()
    };
    if j["encoded"].is_string() {
        for f in code.functions.values_mut() {
            f.attributes.sort_by_key(|a| match a {
                Attribute::Payable => 0,
                Attribute::Private => 1,
            });
        }
        let decoded = Code::decode(&bytes(&j["encoded"]), Limits::default()).unwrap();
        assert_eq!(decoded, code);
        decoded
    } else {
        code
    }
}
pub(super) fn bindings(j: &J) -> Bindings {
    rows(j)
        .iter()
        .map(|p| (p[0].as_u64().unwrap() as u8, values::ty(&p[1])))
        .collect()
}
fn assert_cursor(cursor: &program::Cursor, expected: &J, check_pc: bool) {
    assert_eq!(cursor.function, value(&expected["function"]), "function");
    assert_eq!(cursor.block, value(&expected["block"]), "block");
    if check_pc {
        assert_eq!(
            cursor.instruction,
            expected["pc"].as_u64().unwrap() as usize,
            "instruction position"
        );
    }
    assert_eq!(cursor.returned, expected["returned"].as_bool().unwrap());
    assert_eq!(cursor.call_value, number(&expected["call_value"]));
    assert_eq!(cursor.bindings, bindings(&expected["bindings"]));
    let frames = rows(&expected["frames"]);
    assert_eq!(cursor.frames.len(), frames.len(), "frame count");
    for (actual, e) in cursor.frames.iter().rev().zip(frames) {
        match actual {
            Frame::Gas(n) => {
                assert_eq!(e[0], "gas");
                assert_eq!(*n, number(&e[1]));
            }
            Frame::Local(f) => {
                assert_eq!(e[0], "local");
                assert_eq!(f.function, value(&e[1]));
                assert_eq!(f.bindings, bindings(&e[2]));
                assert_eq!(f.return_block, value(&e[3]));
                assert_eq!(
                    f.stack.iter().cloned().collect::<Vec<_>>(),
                    rows(&e[4]).iter().map(value).collect::<Vec<_>>()
                );
                assert_eq!(f.memory, memory(&e[5]));
                assert_eq!(f.call_value, number(&e[6]));
            }
            _ => panic!("unexpected remote frame in local fixture"),
        }
    }
}
fn check(out: program::Outcome<Backend>, e: &J) -> Option<program::State<Backend>> {
    match out {
        program::Outcome::Continue { state, .. } => {
            assert_eq!(e["kind"], "next");
            assert_state(&state.machine, &e["state"]);
            assert_cursor(&state.cursor, &e["state"], true);
            Some(state)
        }
        program::Outcome::Returned { state, .. } => {
            assert_eq!(e["kind"], "returned");
            assert_state(&state.machine, &e["state"]);
            assert_cursor(&state.cursor, &e["state"], true);
            None
        }
        program::Outcome::Abort { state, reason, .. } => {
            assert_eq!(e["kind"], "abort");
            assert_state(&state.machine, &e["state"]);
            assert_cursor(&state.cursor, &e["state"], false);
            assert_reason(&reason, &e["reason"]);
            None
        }
        program::Outcome::Revert {
            state, value: v, ..
        } => {
            assert_eq!(e["kind"], "revert");
            assert_eq!(v, bytes(&e["value"]));
            assert_state(&state.machine, &e["state"]);
            assert_cursor(&state.cursor, &e["state"], false);
            None
        }
        program::Outcome::Exception(exception) => {
            assert_outcome(Outcome::<Backend>::Exception(exception), e);
            None
        }
    }
}
fn entry(j: &J) -> Entry {
    Entry {
        function: value(&j["entry"]),
        arguments: rows(&j["arguments"]).iter().map(value).collect(),
        call_value: number(&j["call_value"]),
        allow_init: j["allow_init"].as_bool().unwrap(),
    }
}
#[test]
fn local_program_traces_calls_returns_and_entry_checks_match_original_engine() {
    assert_eq!(PROGRAMS["base"], FIXTURE["base"]);
    let programs: Vec<_> = rows(&PROGRAMS["programs"]).iter().map(code).collect();
    let order = otp_order::profile(&PROGRAMS["profile"]);
    let overrides = Overrides::default();
    let contracts = BTreeMap::new();
    for (case, row) in rows(&PROGRAMS["cases"]).iter().enumerate() {
        let idx = row["program"].as_u64().unwrap() as usize;
        let code = &programs[idx];
        let definition = &PROGRAMS["programs"][idx];
        let entry = entry(definition);
        let mut ctx = context(row);
        ctx.reserved_gas = number(&row["reserved"]);
        let executor = Executor {
            context: &ctx,
            order: &order,
            overrides: &overrides,
            contracts: &contracts,
        };
        for run in rows(&row["runs"]) {
            let input = state(row, &run["gas"]);
            let result = std::panic::catch_unwind(|| {
                let start = executor
                    .start_program(code, &input, &entry, Limits::default())
                    .unwrap();
                let mut next = check(
                    start,
                    &PROGRAMS["results"][run["entry"].as_u64().unwrap() as usize],
                );
                if let Some(ref initial) = next {
                    check(
                        executor
                            .run_program(code, initial, Limits::default(), 10002)
                            .unwrap(),
                        &PROGRAMS["results"][run["terminal"].as_u64().unwrap() as usize],
                    );
                }
                for step in rows(&run["steps"]) {
                    let current = next.take().expect("nonterminal prefix");
                    let before = format!("{current:?}");
                    let out = executor
                        .advance_program(code, &current, Limits::default())
                        .unwrap();
                    assert_eq!(format!("{current:?}"), before);
                    next = check(out, &PROGRAMS["results"][step.as_u64().unwrap() as usize]);
                }
                assert!(next.is_none());
                let mut unchanged = row["initial"].clone();
                unchanged["gas"] = run["gas"].clone();
                assert_state(&input, &unchanged);
            });
            assert!(
                result.is_ok(),
                "case {case}, program {}, protocol {}, vm {}, gas {}, reserve {}",
                definition["name"],
                row["protocol"],
                row["vm"],
                run["gas"],
                row["reserved"]
            );
        }
    }
}

#[test]
fn program_work_limits_and_missing_store_fetch_preserve_retryable_inputs() {
    let row = rows(&PROGRAMS["cases"])
        .iter()
        .find(|r| {
            r["protocol"] == 8
                && r["vm"] == 8
                && r["reserved"] == "0"
                && PROGRAMS["programs"][r["program"].as_u64().unwrap() as usize]["name"]
                    == "loaded_return_map"
        })
        .unwrap();
    let definition = &PROGRAMS["programs"][row["program"].as_u64().unwrap() as usize];
    let code = code(definition);
    let mut ctx = context(row);
    ctx.reserved_gas = 0.into();
    let order = otp_order::profile(&PROGRAMS["profile"]);
    let overrides = Overrides::default();
    let empty = BTreeMap::new();
    let fetched = BTreeMap::from([(KEY, backend())]);
    let executor = Executor {
        context: &ctx,
        order: &order,
        overrides: &overrides,
        contracts: &empty,
    };
    let machine = state(row, &serde_json::json!("1000000"));
    let program::Outcome::Continue { mut state, .. } = executor
        .start_program(&code, &machine, &entry(definition), Limits::default())
        .unwrap()
    else {
        panic!()
    };
    state.machine.stores = Stores::new(Version::Arcus);
    let before = format!("{state:?}");
    assert!(matches!(
        executor.run_program(&code, &state, Limits::default(), 100),
        Err(Error::NeedContract(KEY))
    ));
    assert_eq!(format!("{state:?}"), before);
    let executor = Executor {
        contracts: &fetched,
        ..executor
    };
    assert!(matches!(
        executor.run_program(&code, &state, Limits::default(), 0),
        Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
    ));
    assert!(matches!(
        executor.run_program(
            &code,
            &state,
            Limits {
                max_nodes: 0,
                ..Limits::default()
            },
            100
        ),
        Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
    ));
    assert_eq!(format!("{state:?}"), before);
    assert!(matches!(
        executor.run_program(&code, &state, Limits::default(), 100),
        Ok(program::Outcome::Returned { .. })
    ));
    assert_eq!(format!("{state:?}"), before);
}

#[test]
fn recursive_program_frame_and_step_limits_are_operational_and_retryable() {
    let row = rows(&PROGRAMS["cases"])
        .iter()
        .find(|r| {
            r["protocol"] == 8
                && r["vm"] == 8
                && r["reserved"] == "0"
                && PROGRAMS["programs"][r["program"].as_u64().unwrap() as usize]["name"]
                    == "recursive_local"
        })
        .unwrap();
    let definition = &PROGRAMS["programs"][row["program"].as_u64().unwrap() as usize];
    let code = code(definition);
    let mut ctx = context(row);
    ctx.reserved_gas = 0.into();
    let order = otp_order::profile(&PROGRAMS["profile"]);
    let overrides = Overrides::default();
    let contracts = BTreeMap::new();
    let executor = Executor {
        context: &ctx,
        order: &order,
        overrides: &overrides,
        contracts: &contracts,
    };
    let machine = state(row, &serde_json::json!("1000000"));
    let program::Outcome::Continue { state, .. } = executor
        .start_program(&code, &machine, &entry(definition), Limits::default())
        .unwrap()
    else {
        panic!()
    };
    let before = format!("{state:?}");
    for _ in 0..2 {
        assert!(matches!(
            executor.run_program(&code, &state, Limits::default(), 4),
            Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
        ));
        assert!(matches!(
            executor.run_program(
                &code,
                &state,
                Limits {
                    max_depth: 2,
                    ..Limits::default()
                },
                100
            ),
            Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
        ));
        assert_eq!(format!("{state:?}"), before);
    }
    let high = rows(&row["runs"])
        .iter()
        .find(|r| r["gas"] == "1000000")
        .unwrap();
    check(
        executor
            .run_program(&code, &state, Limits::default(), 100)
            .unwrap(),
        &PROGRAMS["results"][high["terminal"].as_u64().unwrap() as usize],
    );
    assert_eq!(format!("{state:?}"), before);
}
