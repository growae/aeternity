use super::*;
use ae_execution::fate_chain::remote::{State as Chain, Store as RemoteStore};
use ae_vm_fate::machine::program::remote;
use remote_tests::small;

type Program<A = MemoryStore, C = MemoryStore> = remote::State<RemoteStore, Chain<A, C>>;
fn sources(row: &J) -> BTreeMap<[u8; 32], RemoteStore> {
    let mut tree = Trie::new(MemoryStore::default());
    for pair in rows(&row["base"]) {
        tree.put(
            &Nibbles::from_bytes(&bytes(&pair[0])),
            TrieValue::Bytes(bytes(&pair[1])),
        )
        .unwrap();
    }
    let source = RemoteStore(Backend(Store::new(tree)));
    (1..=3).map(|n| (small(n), source.clone())).collect()
}
static COMPLETION: LazyLock<J> = LazyLock::new(|| {
    let packed: J =
        serde_json::from_str(include_str!("../fixtures/erlang_fate_completion.json")).unwrap();
    assert_eq!(packed["format"], "interned-json-v1");
    let pool = rows(&packed["values"]);
    name_tests::expand(&packed["root"], pool, pool.len())
});
fn check<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    out: remote::Outcome<RemoteStore, Chain<A, C>>,
    expected: &J,
) {
    if let remote::Outcome::Returned { state, .. } = out {
        assert_eq!(expected["kind"], "returned");
        remote_tests::assert_remote(&state, &expected["state"], false);
    } else {
        assert!(remote_tests::check(out, expected).is_none());
    }
}
fn run<O: ae_vm_fate::maps::MapOrder>(
    context: &Context,
    order: &O,
    overrides: &Overrides,
    sources: &mut BTreeMap<[u8; 32], RemoteStore>,
    initial: &Program,
    mut limits: Limits,
) -> remote::Outcome<RemoteStore, Chain> {
    let mut state = initial.clone();
    for _ in 0..300 {
        let ex = Executor {
            context,
            order,
            overrides,
            contracts: sources,
        };
        match ex.advance_remote_program(&state, limits) {
            Ok(remote::Outcome::Continue {
                state: next,
                remaining_limits,
            }) => {
                state = next;
                limits = remaining_limits;
            }
            Ok(terminal) => return terminal,
            Err(Error::NeedContract(key)) => {
                create_tests::fetch_pending(&state, key, sources, limits)
            }
            other => panic!("{other:?}"),
        }
    }
    panic!("runaway program")
}
#[test]
fn full_execution_and_finalization_checkpoints_match_original_engine() {
    assert_eq!(COMPLETION["base"], FIXTURE["base"]);
    let order = otp_order::profile(&COMPLETION["profile"]);
    let overrides = Overrides::default();
    let mut programs = 0;
    let mut finalizations = 0;
    for row in rows(&COMPLETION["cases"]) {
        let mut context = context(row);
        context.current_contract = small(1);
        context.reserved_gas = 18.into();
        for sample in rows(&row["runs"]) {
            let label = format!(
                "{} protocol {} vm {} gas {}",
                row["name"], row["protocol"], row["vm"], sample["gas"]
            );
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut sources = sources(row);
                let ex = Executor {
                    context: &context,
                    order: &order,
                    overrides: &overrides,
                    contracts: &sources,
                };
                let (initial, limits) =
                    remote_tests::start(&ex, row, &sample["gas"], remote_tests::chain(row));
                let terminal = run(&context, &order, &overrides, &mut sources, &initial, limits);
                let ex = Executor {
                    context: &context,
                    order: &order,
                    overrides: &overrides,
                    contracts: &sources,
                };
                let completed = match terminal {
                    remote::Outcome::Returned {
                        state,
                        remaining_limits,
                    } => {
                        if sample["gas"] == "1000000000" {
                            let checkpoint = &row["finalization"];
                            remote_tests::assert_remote(&state, &checkpoint["initial"], false);
                            for probe in rows(&checkpoint["runs"]) {
                                let mut input = state.clone();
                                input.program.machine.meter.gas = number(&probe["gas"]);
                                let before = format!("{input:?}");
                                check(
                                    ex.finalize_remote_program(&input, remaining_limits)
                                        .unwrap(),
                                    &probe["result"],
                                );
                                assert_eq!(format!("{input:?}"), before);
                                finalizations += 1;
                            }
                        }
                        ex.finalize_remote_program(&state, remaining_limits)
                            .unwrap()
                    }
                    other => other,
                };
                check(completed, &sample["result"]);
                // The complete execution convenience path must use the same
                // completion boundary. Sources fetched above remain external
                // read inputs; it must still install each store at its demand.
                if sample["gas"] == "1000000000" {
                    check(
                        ex.execute_remote_program(&initial, limits, 300).unwrap(),
                        &sample["result"],
                    );
                }
                programs += 1;
            }))
            .unwrap_or_else(|e| {
                eprintln!("{label}");
                std::panic::resume_unwind(e)
            });
        }
    }
    assert_eq!(programs, 3202);
    assert_eq!(finalizations, 2203);
}

fn scalar_row() -> &'static J {
    rows(&COMPLETION["cases"])
        .iter()
        .find(|r| r["name"] == "store_scalar" && r["protocol"] == 6 && r["vm"] == 8)
        .unwrap()
}

fn altered(input: &Program, name: &str) -> Program {
    let mut state = input.clone();
    if name == "invalid_final_result" {
        state.program.machine.accumulator.top = Value::ContractBytearray(vec![1]);
        return state;
    }
    if name == "invalid_dirty_term" {
        state
            .program
            .machine
            .stores
            .put_value(&small(1), (-1).into(), Value::ContractBytearray(vec![1]))
            .unwrap();
        return state;
    }
    let original = &input.program.machine.stores;
    let mut stores = Stores::new(Version::for_protocol(input.program.machine.meter.protocol));
    if name != "empty_stores" {
        for key in original.keys() {
            let entry = original.entry(key).unwrap();
            let mut backend = entry.backend().clone();
            match name {
                "empty_write_key" => backend.0.0.put(vec![], vec![42]),
                "bad_metadata" => backend.0.0.put(vec![0, 0], vec![255, 255]),
                _ => {}
            }
            let key = if name == "missing_contract" {
                small(99)
            } else {
                *key
            };
            stores.put_contract(key, backend.clone());
            for (position, (value, dirty)) in entry.terms() {
                assert!(dirty);
                stores
                    .put_value(&key, position.clone(), value.clone())
                    .unwrap();
            }
            if name == "two_contracts" {
                stores.put_contract(small(2), backend.clone());
                stores
                    .put_value(&small(2), 3.into(), Value::Integer(77.into()))
                    .unwrap();
            }
            if name == "clean_missing_contract" {
                stores.put_contract(small(99), backend);
            }
        }
    }
    state.program.machine.stores = stores;
    state
}

#[test]
fn finalization_failure_order_and_chain_store_validation_match_original() {
    let order = otp_order::profile(&COMPLETION["profile"]);
    let overrides = Overrides::default();
    let mut count = 0;
    for row in rows(&COMPLETION["cases"])
        .iter()
        .filter(|r| r["name"] == "store_scalar")
    {
        let mut context = context(row);
        context.current_contract = small(1);
        context.reserved_gas = 18.into();
        let mut sources = sources(row);
        let ex = Executor {
            context: &context,
            order: &order,
            overrides: &overrides,
            contracts: &sources,
        };
        let (initial, limits) = remote_tests::start(
            &ex,
            row,
            &J::String("1000000000".into()),
            remote_tests::chain(row),
        );
        let remote::Outcome::Returned {
            state,
            remaining_limits,
        } = run(&context, &order, &overrides, &mut sources, &initial, limits)
        else {
            panic!()
        };
        let ex = Executor {
            context: &context,
            order: &order,
            overrides: &overrides,
            contracts: &sources,
        };
        for extra in rows(&row["extra_finalizations"]) {
            let mut input = altered(&state, string(&extra["name"]));
            remote_tests::assert_remote(&input, &extra["initial"], false);
            for probe in rows(&extra["runs"]) {
                input.program.machine.meter.gas = number(&probe["gas"]);
                let before = format!("{input:?}");
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    check(
                        ex.finalize_remote_program(&input, remaining_limits)
                            .unwrap(),
                        &probe["result"],
                    );
                }))
                .unwrap_or_else(|e| {
                    eprintln!(
                        "extra {} protocol {} vm {} gas {}",
                        extra["name"], row["protocol"], row["vm"], probe["gas"]
                    );
                    std::panic::resume_unwind(e)
                });
                assert_eq!(format!("{input:?}"), before);
                count += 1;
            }
        }
    }
    assert_eq!(count, 640);
}
#[test]
fn finalization_operational_errors_leave_the_return_checkpoint_retryable() {
    let row = scalar_row();
    let sample = rows(&row["runs"])
        .iter()
        .find(|r| r["gas"] == "1000000000")
        .unwrap();
    let order = otp_order::profile(&COMPLETION["profile"]);
    let overrides = Overrides::default();
    let mut sources = sources(row);
    let mut context = context(row);
    context.current_contract = small(1);
    context.reserved_gas = 18.into();
    let ex = Executor {
        context: &context,
        order: &order,
        overrides: &overrides,
        contracts: &sources,
    };
    let (initial, limits) = remote_tests::start(&ex, row, &sample["gas"], remote_tests::chain(row));
    assert!(matches!(
        ex.finalize_remote_program(&initial, limits),
        Err(Error::FinalizationRequiresReturn)
    ));
    let remote::Outcome::Returned {
        state,
        remaining_limits,
    } = run(&context, &order, &overrides, &mut sources, &initial, limits)
    else {
        panic!()
    };
    let ex = Executor {
        context: &context,
        order: &order,
        overrides: &overrides,
        contracts: &sources,
    };
    let before = format!("{state:?}");
    for limits in [
        Limits {
            max_nodes: 0,
            ..remaining_limits
        },
        Limits {
            max_bytes: 0,
            ..remaining_limits
        },
    ] {
        assert!(matches!(
            ex.finalize_remote_program(&state, limits),
            Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
        ));
        assert_eq!(format!("{state:?}"), before);
    }
    let faults = oracle_write_tests::Faults::default();
    let faulted = remote::State {
        program: state.program.clone(),
        session: remote::Session {
            host: oracle_write_tests::fault_host(state.session.host.clone(), &faults),
            snapshots: Default::default(),
            current_contract: state.session.current_contract,
            vm_version: state.session.vm_version,
            caller: state.session.caller.clone(),
            seen_contracts: state.session.seen_contracts.clone(),
            code_cache: state.session.code_cache.clone(),
            creator_cache: state.session.creator_cache.clone(),
            logs: state.session.logs.clone(),
        },
    };
    faults.fail_reads.set(true);
    let before = format!("{faulted:?}");
    assert!(matches!(
        ex.finalize_remote_program(&faulted, remaining_limits),
        Err(Error::Backend(_))
    ));
    assert_eq!(format!("{faulted:?}"), before);
    faults.fail_reads.set(false);
    check(
        ex.finalize_remote_program(&faulted, remaining_limits)
            .unwrap(),
        &sample["result"],
    );

    let extra = rows(&row["extra_finalizations"])
        .iter()
        .find(|r| r["name"] == "two_contracts")
        .unwrap();
    let expected = rows(&extra["runs"])
        .iter()
        .find(|r| r["gas"] == "1000000000")
        .unwrap();
    let mut multi = faulted.clone();
    multi.program = altered(&state, "two_contracts").program;
    multi.program.machine.meter.gas = 1_000_000_000.into();
    let mut failed = 0;
    let mut complete = false;
    for budget in 0..128 {
        faults.reads_left.set(Some(budget));
        let before = format!("{multi:?}");
        let out = ex.finalize_remote_program(&multi, remaining_limits);
        faults.reads_left.set(Some(budget));
        assert_eq!(format!("{multi:?}"), before);
        faults.reads_left.set(None);
        match out {
            Err(Error::Backend(_)) => failed += 1,
            Ok(out) => {
                check(out, &expected["result"]);
                complete = true;
                break;
            }
            other => panic!("{other:?}"),
        }
    }
    assert!(failed > 1 && complete);
    let remote::Outcome::Returned {
        state: completed, ..
    } = ex
        .finalize_remote_program(&multi, remaining_limits)
        .unwrap()
    else {
        panic!()
    };
    let base = backend().0;
    let overlaid = completed
        .session
        .host
        .contract_cache
        .overlay_store(&small(1), base.clone(), limits)
        .unwrap()
        .unwrap();
    assert_eq!(overlaid.get(&[0, 1]).unwrap(), base.get(&[0, 1]).unwrap());
    assert_eq!(
        overlaid.get(&[0, 3]).unwrap(),
        ae_vm_fate::encoding::encode(&Value::Integer(77.into()), limits).unwrap()
    );

    let mut paused = initial;
    paused
        .program
        .cursor
        .enable_debug(Default::default(), limits)
        .unwrap();
    paused.program.cursor.suspended = true;
    let before = format!("{paused:?}");
    assert!(matches!(
        ex.finalize_remote_program(&paused, limits),
        Err(Error::FinalizationRequiresReturn)
    ));
    let remote::Outcome::Break { state: after, .. } =
        ex.execute_remote_program(&paused, limits, 100).unwrap()
    else {
        panic!()
    };
    assert_eq!(format!("{after:?}"), before);
}
