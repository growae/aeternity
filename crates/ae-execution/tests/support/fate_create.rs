use super::*;
use ae_execution::fate_chain::remote::{State as Chain, Store as RemoteStore, View};
use ae_vm_fate::machine::program::remote;
use remote_tests::small;

static CREATE: LazyLock<J> = LazyLock::new(|| {
    let packed: J =
        serde_json::from_str(include_str!("../fixtures/erlang_fate_create.json")).unwrap();
    assert_eq!(packed["format"], "interned-json-v1");
    let pool = rows(&packed["values"]);
    name_tests::expand(&packed["root"], pool, pool.len())
});
fn chain(row: &J) -> Chain {
    let mut host = remote_tests::chain(row);
    if !row["onchain"].as_bool().unwrap() {
        host.onchain = Some(Box::new(View {
            accounts: host.accounts.clone(),
            contracts: host.contracts.clone(),
            names: host.names.clone(),
            name_cache: host.name_cache.clone(),
            oracles: host.oracles.clone(),
            oracle_reads: host.oracle_reads.clone(),
        }));
    }
    host
}
fn fetch_pending<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    state: &remote::State<RemoteStore, Chain<A, C>>,
    key: [u8; 32],
    sources: &mut BTreeMap<[u8; 32], RemoteStore>,
    limits: Limits,
) {
    assert!(!sources.contains_key(&key), "store requested twice");
    let store = state
        .session
        .host
        .contract_cache
        .initial_store(&key, MemoryStore::default(), limits)
        .unwrap()
        .unwrap_or_else(|| panic!("missing pending contract {}", hex::encode(key)));
    sources.insert(key, RemoteStore(Backend(store)));
}
#[test]
fn creation_cloning_init_continuations_and_cleanup_match_original_instruction_and_whole_engine() {
    assert_eq!(CREATE["base"], FIXTURE["base"]);
    assert_eq!(rows(&CREATE["cases"]).len(), 1088);
    let order = otp_order::profile(&CREATE["profile"]);
    let overrides = Overrides::default();
    let mut count = 0;
    for row in rows(&CREATE["cases"]) {
        let mut context = context(row);
        context.current_contract = small(1);
        context.reserved_gas = 18.into();
        let host = chain(row);
        for run in rows(&row["runs"]) {
            let mut sources = remote_tests::sources();
            let ex = Executor {
                context: &context,
                order: &order,
                overrides: &overrides,
                contracts: &sources,
            };
            let (mut state, mut limits) = remote_tests::start(&ex, row, &run["gas"], host.clone());
            for (index, step) in rows(&run["steps"]).iter().enumerate() {
                let expected = &CREATE["results"][step.as_u64().unwrap() as usize];
                let label = format!(
                    "case {} {} protocol {} vm {} gas {} step {}",
                    row["case_id"], row["name"], row["protocol"], row["vm"], run["gas"], index
                );
                let out = loop {
                    let ex = Executor {
                        context: &context,
                        order: &order,
                        overrides: &overrides,
                        contracts: &sources,
                    };
                    match ex.advance_remote_program(&state, limits) {
                        Ok(out) => break out,
                        Err(Error::NeedContract(key)) => {
                            fetch_pending(&state, key, &mut sources, limits)
                        }
                        Err(e) => panic!("{label}: {e:?}"),
                    }
                };
                let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    remote_tests::check(out, expected)
                }))
                .unwrap_or_else(|e| {
                    eprintln!("{label}");
                    std::panic::resume_unwind(e)
                });
                if let Some((next, budget)) = checked {
                    state = next;
                    limits = budget;
                } else {
                    assert_eq!(index + 1, rows(&run["steps"]).len());
                    assert_eq!(step, &run["terminal"]);
                    break;
                }
            }
            count += 1;
        }
    }
    assert_eq!(count, 15232);
}

fn selected(id: u64) -> &'static J {
    rows(&CREATE["cases"])
        .iter()
        .find(|r| r["case_id"] == id && r["protocol"] == 6 && r["vm"] == 8)
        .unwrap()
}
fn run(row: &J) -> &J {
    rows(&row["runs"])
        .iter()
        .find(|r| r["gas"] == "200000")
        .unwrap()
}
fn expected(row: &J, step: usize) -> &J {
    &CREATE["results"][run(row)["steps"][step].as_u64().unwrap() as usize]
}
fn retry_failures<O: ae_vm_fate::maps::MapOrder>(
    ex: &Executor<'_, RemoteStore, O>,
    state: &remote::State<
        RemoteStore,
        Chain<oracle_write_tests::FaultStore, oracle_write_tests::FaultStore>,
    >,
    limits: Limits,
    faults: &oracle_write_tests::Faults,
    expected: &J,
) {
    faults.fail_reads.set(true);
    let before = format!("{state:?}");
    assert!(matches!(
        ex.advance_remote_program(state, limits),
        Err(Error::Backend(_))
    ));
    assert_eq!(before, format!("{state:?}"));
    faults.fail_reads.set(false);
    let mut failures = 0;
    let mut reached_success = false;
    for budget in 0..128 {
        faults.writes_left.set(Some(budget));
        let before = format!("{state:?}");
        let out = ex.advance_remote_program(state, limits);
        faults.writes_left.set(Some(budget));
        assert_eq!(before, format!("{state:?}"));
        faults.writes_left.set(None);
        match out {
            Err(Error::Backend(_)) => {
                failures += 1;
                remote_tests::check(ex.advance_remote_program(state, limits).unwrap(), expected);
            }
            Ok(out) => {
                remote_tests::check(out, expected);
                reached_success = true;
                break;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(failures > 0 && reached_success);
}
#[test]
fn deployment_and_protected_init_cleanup_backend_failures_escape_recovery_and_retry() {
    let order = otp_order::profile(&CREATE["profile"]);
    let overrides = Overrides::default();
    let sources = remote_tests::sources();
    for id in [0, 8, 10, 14, 16, 35] {
        let row = selected(id);
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
        let (state, limits) = remote_tests::start(
            &ex,
            row,
            &run(row)["gas"],
            oracle_write_tests::fault_host(chain(row), &faults),
        );
        retry_failures(&ex, &state, limits, &faults, expected(row, 0));
        if matches!(id, 10 | 14) {
            let (state, limits) = remote_tests::check(
                ex.advance_remote_program(&state, limits).unwrap(),
                expected(row, 0),
            )
            .unwrap();
            assert_ne!(state.session.current_contract, small(1));
            retry_failures(&ex, &state, limits, &faults, expected(row, 1));
        }
    }
}
#[test]
fn init_frames_work_limits_and_pending_store_fetch_are_retryable_without_publication() {
    let row = selected(18);
    let order = otp_order::profile(&CREATE["profile"]);
    let overrides = Overrides::default();
    let mut sources = remote_tests::sources();
    let mut context = context(row);
    context.current_contract = small(1);
    context.reserved_gas = 18.into();
    let ex = Executor {
        context: &context,
        order: &order,
        overrides: &overrides,
        contracts: &sources,
    };
    let (state, limits) = remote_tests::start(&ex, row, &run(row)["gas"], chain(row));
    let (state, limits) = remote_tests::check(
        ex.advance_remote_program(&state, limits).unwrap(),
        expected(row, 0),
    )
    .unwrap();
    let before = format!("{state:?}");
    assert!(matches!(
        ex.advance_remote_program(
            &state,
            Limits {
                max_depth: 1,
                ..limits
            }
        ),
        Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
    ));
    let key = match ex.advance_remote_program(&state, limits) {
        Err(Error::NeedContract(k)) => k,
        other => panic!("{other:?}"),
    };
    assert_eq!(key, state.session.current_contract);
    assert!(matches!(
        state.session.host.contract_cache.initial_store(
            &key,
            MemoryStore::default(),
            Limits {
                max_bytes: 0,
                ..limits
            }
        ),
        Err(remote::HostFailure::ResourceLimit)
    ));
    assert_eq!(before, format!("{state:?}"));
    fetch_pending(&state, key, &mut sources, limits);
    let ex = Executor {
        context: &context,
        order: &order,
        overrides: &overrides,
        contracts: &sources,
    };
    let (mut state, mut limits) = (state, limits);
    for step in 1..rows(&run(row)["steps"]).len() {
        if let Some((next, rest)) = remote_tests::check(
            ex.advance_remote_program(&state, limits).unwrap(),
            expected(row, step),
        ) {
            state = next;
            limits = rest;
        }
    }
}
