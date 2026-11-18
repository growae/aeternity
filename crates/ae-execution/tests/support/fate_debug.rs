use super::*;
use ae_execution::fate_chain::remote::{State as Chain, Store as RemoteStore};
use ae_vm_fate::machine::program::{
    debug::{Info, Location, Status},
    remote,
};
use remote_tests::small;

type DebugState = remote::State<RemoteStore, Chain>;
static DEBUG: LazyLock<J> = LazyLock::new(|| {
    let packed: J =
        serde_json::from_str(include_str!("../fixtures/erlang_fate_debug.json")).unwrap();
    let pool = rows(&packed["values"]);
    name_tests::expand(&packed["root"], pool, pool.len())
});
fn status(j: &J) -> Status {
    match j.as_str() {
        Some("continue") => Status::Continue,
        Some("break") => Status::Break,
        Some("stepin") => Status::StepIn,
        None => match string(&j[0]) {
            "stepover" => Status::StepOver(j[1].as_u64().unwrap() as usize),
            "stepout" => Status::StepOut(j[1].as_u64().unwrap() as usize),
            _ => panic!("{j}"),
        },
        _ => panic!("{j}"),
    }
}
fn location(j: &J) -> Option<Location> {
    (!j.is_null()).then(|| Location {
        file: value(&j[0]),
        line: value(&j[1]),
    })
}
fn info(j: &J) -> Option<Info> {
    if j.is_null() {
        return None;
    }
    Some(Info {
        status: status(&j["status"]),
        location: location(&j["location"]),
        breakpoints: rows(&j["breakpoints"])
            .iter()
            .map(|j| location(j).unwrap())
            .collect(),
        current_instruction: number(&j["instruction"]),
        variables: rows(&j["variables"])
            .iter()
            .map(|r| {
                (
                    value(&r[0]),
                    rows(&r[1])
                        .iter()
                        .map(|o| match string(&o[0]) {
                            "stack" => Operand::Stack,
                            "arg" => Operand::Argument(value(&o[1])),
                            "var" => Operand::Variable(value(&o[1])),
                            "immediate" => Operand::Immediate(value(&o[1])),
                            _ => panic!(),
                        })
                        .collect(),
                )
            })
            .collect(),
        call_stack: rows(&j["stack"]).iter().map(location).collect(),
        contract_names: rows(&j["names"])
            .iter()
            .map(|r| (remote_tests::key(&r[0]), value(&r[1])))
            .collect(),
    })
}
pub(super) fn assert_info(actual: &Option<Info>, expected: &J) {
    assert_eq!(*actual, info(expected), "debug info");
}
#[allow(clippy::too_many_arguments)]
fn segment<O: ae_vm_fate::maps::MapOrder>(
    build: &J,
    context: &Context,
    order: &O,
    overrides: &Overrides,
    sources: &mut BTreeMap<[u8; 32], RemoteStore>,
    mut state: DebugState,
    mut limits: Limits,
    segment: &J,
    local: bool,
) -> Option<(DebugState, Limits)> {
    let entry = &build["results"][segment["entry"].as_u64().unwrap() as usize];
    if state.program.cursor.debug.is_some() {
        let code = &state.session.code_cache[&state.session.current_contract].code;
        let before = format!("{state:?}");
        if let Err(e) = state.program.cursor.resume_debug(code) {
            assert_eq!(format!("{state:?}"), before);
            remote_tests::check::<MemoryStore, MemoryStore>(remote::Outcome::Exception(e), entry);
            assert!(rows(&segment["steps"]).is_empty());
            assert_eq!(segment["entry"], segment["terminal"]);
            return None;
        }
    }
    assert_eq!(entry["kind"], "next");
    remote_tests::assert_remote(&state, &entry["state"], true);
    for (index, step) in rows(&segment["steps"]).iter().enumerate() {
        let expected = &build["results"][step.as_u64().unwrap() as usize];
        let out = loop {
            let ex = Executor {
                context,
                order,
                overrides,
                contracts: sources,
            };
            let out = if local {
                ex.advance_program(
                    &state.session.code_cache[&state.session.current_contract].code,
                    &state.program,
                    limits,
                )
                .map(|out| local_outcome(out, &state))
            } else {
                ex.advance_remote_program(&state, limits)
            };
            match out {
                Ok(out) => break out,
                Err(Error::NeedContract(key)) => {
                    create_tests::fetch_pending(&state, key, sources, limits)
                }
                other => panic!("step {index}: {other:?}"),
            }
        };
        let paused = matches!(out, remote::Outcome::Break { .. });
        let result = remote_tests::check(out, expected);
        if paused || result.is_none() {
            assert_eq!(index + 1, rows(&segment["steps"]).len());
            assert_eq!(step, &segment["terminal"]);
            return result;
        }
        (state, limits) = result.unwrap();
    }
    panic!("missing terminal outcome")
}
fn local_outcome(
    out: ae_vm_fate::machine::program::Outcome<RemoteStore>,
    input: &DebugState,
) -> remote::Outcome<RemoteStore, Chain> {
    use ae_vm_fate::machine::program::Outcome as P;
    let state = |program| remote::State {
        program,
        session: input.session.clone(),
    };
    match out {
        P::Break {
            state: s,
            remaining_limits,
        } => remote::Outcome::Break {
            state: state(s),
            remaining_limits,
        },
        P::Continue {
            state: s,
            remaining_limits,
        } => remote::Outcome::Continue {
            state: state(s),
            remaining_limits,
        },
        P::Returned {
            state: s,
            remaining_limits,
        } => remote::Outcome::Returned {
            state: state(s),
            remaining_limits,
        },
        P::Abort {
            state: s,
            reason,
            remaining_limits,
        } => remote::Outcome::Abort {
            state: state(s),
            reason,
            remaining_limits,
        },
        P::Revert {
            state: s,
            value,
            remaining_limits,
        } => remote::Outcome::Revert {
            state: state(s),
            value,
            remaining_limits,
        },
        P::Exception(e) => remote::Outcome::Exception(e),
    }
}
#[test]
fn debug_instructions_breakpoints_frames_and_resumes_match_both_original_builds() {
    corpus(false);
}
#[test]
fn local_debug_calls_returns_fallthrough_and_resumes_match_original_engine() {
    corpus(true);
}
fn corpus(local: bool) {
    let overrides = Overrides::default();
    for build in rows(&DEBUG["builds"]) {
        assert_eq!(build["base"], FIXTURE["base"]);
        let order = otp_order::profile(&build["profile"]);
        for row in rows(&build["cases"]) {
            if local
                && !matches!(
                    string(&row["name"]),
                    "local" | "tail" | "fallthrough" | "empty_fallthrough" | "bad_jump"
                )
            {
                continue;
            }
            let mut context = context(row);
            context.current_contract = small(1);
            context.reserved_gas = 18.into();
            for run in rows(&row["runs"]) {
                let label = format!(
                    "build {} case {} {} protocol {} vm {} gas {}",
                    build["mode"],
                    row["case_id"],
                    row["name"],
                    row["protocol"],
                    row["vm"],
                    run["gas"]
                );
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut sources = remote_tests::sources();
                    let ex = Executor {
                        context: &context,
                        order: &order,
                        overrides: &overrides,
                        contracts: &sources,
                    };
                    let (mut state, limits) =
                        remote_tests::start(&ex, row, &run["gas"], remote_tests::chain(row));
                    if let Some(info) = info(&row["initial"]["debug"]) {
                        state.program.cursor.enable_debug(info, limits).unwrap();
                    }
                    let first = segment(
                        build,
                        &context,
                        &order,
                        &overrides,
                        &mut sources,
                        state,
                        limits,
                        run,
                        local,
                    );
                    for resume in rows(&run["resumes"]) {
                        let (mut paused, limits) =
                            first.clone().expect("resume requires breakpoint");
                        assert!(paused.program.cursor.suspended);
                        paused.program.cursor.debug.as_mut().unwrap().status =
                            status(&resume["status"]);
                        let next = segment(
                            build,
                            &context,
                            &order,
                            &overrides,
                            &mut sources,
                            paused,
                            limits,
                            &resume["segment"],
                            local,
                        );
                        if !resume["continue"].is_null() {
                            let (mut paused, limits) = next.expect("continue requires breakpoint");
                            paused.program.cursor.debug.as_mut().unwrap().status = Status::Continue;
                            segment(
                                build,
                                &context,
                                &order,
                                &overrides,
                                &mut sources,
                                paused,
                                limits,
                                &resume["continue"],
                                local,
                            );
                        }
                    }
                }))
                .unwrap_or_else(|e| {
                    eprintln!("{label}");
                    std::panic::resume_unwind(e)
                });
            }
        }
    }
}

fn selected(name: &str, stepping: bool) -> (&'static J, &'static J) {
    let build = rows(&DEBUG["builds"])
        .iter()
        .find(|b| b["mode"] == "enabled")
        .unwrap();
    let row = rows(&build["cases"])
        .iter()
        .find(|r| {
            r["name"] == name
                && r["protocol"] == 6
                && r["vm"] == 8
                && r["initial"]["debug"]["status"] == if stepping { "stepin" } else { "continue" }
        })
        .unwrap();
    (build, row)
}
fn high(row: &J) -> &J {
    rows(&row["runs"])
        .iter()
        .find(|r| r["gas"] == "30000")
        .unwrap()
}
#[test]
fn debugger_pause_resume_and_work_limits_preserve_retryable_states() {
    let (build, row) = selected("simple", true);
    let run = high(row);
    let order = otp_order::profile(&build["profile"]);
    let overrides = Overrides::default();
    let sources = remote_tests::sources();
    let mut context = context(row);
    context.current_contract = small(1);
    context.reserved_gas = 18.into();
    let ex = Executor {
        context: &context,
        order: &order,
        overrides: &overrides,
        contracts: &sources,
    };
    let (mut state, limits) = remote_tests::start(&ex, row, &run["gas"], remote_tests::chain(row));
    let before = format!("{state:?}");
    assert_eq!(
        state.program.cursor.enable_debug(
            info(&row["initial"]["debug"]).unwrap(),
            Limits {
                max_nodes: 0,
                ..limits
            }
        ),
        Err(ae_vm_fate::Error::ResourceLimit)
    );
    assert_eq!(format!("{state:?}"), before);
    state
        .program
        .cursor
        .enable_debug(info(&row["initial"]["debug"]).unwrap(), limits)
        .unwrap();
    let before = format!("{state:?}");
    assert!(matches!(
        ex.advance_remote_program(
            &state,
            Limits {
                max_bytes: 0,
                ..limits
            }
        ),
        Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
    ));
    assert_eq!(format!("{state:?}"), before);
    let terminal = &build["results"][run["terminal"].as_u64().unwrap() as usize];
    let (mut paused, rest) = remote_tests::check(
        ex.run_remote_program(&state, limits, 100).unwrap(),
        terminal,
    )
    .unwrap();
    assert!(paused.program.cursor.suspended);
    let before = format!("{paused:?}");
    remote_tests::check(ex.run_remote_program(&paused, rest, 100).unwrap(), terminal).unwrap();
    assert_eq!(format!("{paused:?}"), before);
    let code = &paused.session.code_cache[&paused.session.current_contract].code;
    let mut invalid = paused.program.cursor.clone();
    invalid.debug.as_mut().unwrap().current_instruction = BigInt::from(1) << 200;
    let before = format!("{invalid:?}");
    assert_eq!(
        invalid.resume_debug(code),
        Err(Exception::Native("function_clause"))
    );
    assert_eq!(format!("{invalid:?}"), before);
    paused.program.cursor.debug.as_mut().unwrap().status = Status::Continue;
    paused.program.cursor.resume_debug(code).unwrap();
    let resumed = rows(&run["resumes"])
        .iter()
        .find(|r| r["status"] == "continue")
        .unwrap();
    let expected = &build["results"][resumed["segment"]["terminal"].as_u64().unwrap() as usize];
    assert!(
        remote_tests::check(ex.run_remote_program(&paused, rest, 100).unwrap(), expected).is_none()
    );
}
#[test]
fn debugger_call_frames_are_not_published_when_remote_code_reads_fail() {
    let (build, row) = selected("remote", false);
    let run = high(row);
    let order = otp_order::profile(&build["profile"]);
    let overrides = Overrides::default();
    let sources = remote_tests::sources();
    let mut context = context(row);
    context.current_contract = small(1);
    context.reserved_gas = 18.into();
    let ex = Executor {
        context: &context,
        order: &order,
        overrides: &overrides,
        contracts: &sources,
    };
    let faults = oracle_write_tests::Faults::default();
    let (mut state, mut limits) = remote_tests::start(
        &ex,
        row,
        &run["gas"],
        oracle_write_tests::fault_host(remote_tests::chain(row), &faults),
    );
    state
        .program
        .cursor
        .enable_debug(info(&row["initial"]["debug"]).unwrap(), limits)
        .unwrap();
    for step in 0..3 {
        (state, limits) = remote_tests::check(
            ex.advance_remote_program(&state, limits).unwrap(),
            &build["results"][run["steps"][step].as_u64().unwrap() as usize],
        )
        .unwrap();
    }
    assert_eq!(state.program.cursor.instruction, 3);
    faults.fail_reads.set(true);
    let before = format!("{state:?}");
    assert!(matches!(
        ex.advance_remote_program(&state, limits),
        Err(Error::Backend(_))
    ));
    assert_eq!(format!("{state:?}"), before);
    faults.fail_reads.set(false);
    remote_tests::check(
        ex.advance_remote_program(&state, limits).unwrap(),
        &build["results"][run["steps"][3].as_u64().unwrap() as usize],
    )
    .unwrap();
}
