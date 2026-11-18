use super::*;
use ae_vm_fate::machine::program::remote::{self, Host, HostFailure};
use oracle_write_tests::{Faults, fault_host};
use remote_tests::small;

static HOSTS: LazyLock<J> = LazyLock::new(|| {
    let packed: J =
        serde_json::from_str(include_str!("../fixtures/erlang_fate_auth.json")).unwrap();
    assert_eq!(packed["format"], "interned-json-v1");
    let pool = rows(&packed["values"]);
    name_tests::expand(&packed["root"], pool, pool.len())
});
fn unpack(row: &J) -> J {
    let mut row = row.clone();
    for field in rows(&HOSTS["input_fields"]) {
        let field = string(field);
        let index = row[field].as_u64().unwrap() as usize;
        row[field] = HOSTS["inputs"][index].clone();
    }
    row
}
fn selected(id: u64) -> J {
    unpack(
        rows(&HOSTS["cases"])
            .iter()
            .find(|r| r["case_id"] == id && r["protocol"] == 6 && r["vm"] == 8)
            .unwrap(),
    )
}
fn run(row: &J) -> &J {
    rows(&row["runs"])
        .iter()
        .find(|r| r["gas"] == "200000")
        .unwrap()
}
fn terminal(row: &J) -> &J {
    &HOSTS["results"][run(row)["terminal"].as_u64().unwrap() as usize]
}

#[test]
fn authentication_projection_limits_and_named_reads_are_retryable_without_publishing_caches() {
    for id in [0, 50, 78, 137, 213] {
        let row = selected(id);
        let mut host = context_tests::chain_base(&row);
        let before = format!("{host:?}");
        for limits in [
            Limits {
                max_bytes: 0,
                ..Limits::default()
            },
            Limits {
                max_nodes: 0,
                ..Limits::default()
            },
        ] {
            assert!(matches!(
                host.auth_transaction(VmVersion::Three, limits),
                Err(HostFailure::ResourceLimit)
            ));
            assert_eq!(format!("{host:?}"), before);
        }
        if id == 213 {
            assert!(matches!(
                host.auth_transaction(
                    VmVersion::Three,
                    Limits {
                        max_depth: 8,
                        ..Limits::default()
                    }
                ),
                Err(HostFailure::ResourceLimit)
            ));
            assert_eq!(format!("{host:?}"), before);
        }
        let (_, result) = host
            .auth_transaction(VmVersion::Three, Limits::default())
            .unwrap();
        let first = &HOSTS["results"][run(&row)["steps"][0].as_u64().unwrap() as usize];
        assert_eq!(result, value(&first["state"]["top"]));
        assert_eq!(format!("{host:?}"), before);
    }
    let faults = Faults::default();
    let mut host = fault_host(context_tests::chain_base(&selected(137)), &faults);
    faults.fail_reads.set(true);
    let before = format!("{host:?}");
    assert!(matches!(
        host.auth_transaction(VmVersion::Three, Limits::default()),
        Err(HostFailure::Backend(_))
    ));
    assert_eq!(format!("{host:?}"), before);
    faults.fail_reads.set(false);
    faults.writes_left.set(Some(0));
    let before = format!("{host:?}");
    host.auth_transaction(VmVersion::Three, Limits::default())
        .unwrap();
    assert_eq!(format!("{host:?}"), before);
    assert!(host.name_cache.names.is_empty());
    // A preexisting positive primitive cache is used, but no new cache is published.
    let name = host.names.name(small(20)).unwrap().unwrap();
    host.name_cache.names.insert(small(20), name);
    faults.fail_reads.set(true);
    let before = format!("{host:?}");
    host.auth_transaction(VmVersion::Three, Limits::default())
        .unwrap();
    assert_eq!(format!("{host:?}"), before);
}

#[test]
fn protected_authentication_storage_failures_escape_catches_and_retry_original_outcomes() {
    let order = otp_order::profile(&HOSTS["profile"]);
    let overrides = Overrides::default();
    let sources = remote_tests::sources();
    for id in 195..=198 {
        let row = selected(id);
        let mut context = context(&row);
        context.current_contract = small(1);
        context.reserved_gas = 18.into();
        let ex = Executor {
            context: &context,
            order: &order,
            overrides: &overrides,
            contracts: &sources,
        };
        let faults = Faults::default();
        let host = fault_host(context_tests::chain_base(&row), &faults);
        let (mut state, mut limits) =
            remote_tests::start(&ex, &row, &J::String("200000".into()), host);
        while state.session.current_contract == small(1) {
            let remote::Outcome::Continue {
                state: next,
                remaining_limits,
            } = ex.advance_remote_program(&state, limits).unwrap()
            else {
                panic!()
            };
            state = next;
            limits = remaining_limits;
        }
        faults.fail_reads.set(true);
        let before = format!("{state:?}");
        assert!(matches!(
            ex.advance_remote_program(&state, limits),
            Err(Error::Backend(_))
        ));
        assert_eq!(format!("{state:?}"), before);
        faults.fail_reads.set(false);
        assert!(
            remote_tests::check(
                ex.run_remote_program(&state, limits, 100).unwrap(),
                terminal(&row)
            )
            .is_none()
        );
    }
}

#[test]
fn runtime_infinite_ttl_hashes_match_original_and_require_an_explicit_atom_index() {
    let order = otp_order::profile(&HOSTS["profile"]);
    for row in rows(&HOSTS["runtime_atoms"]) {
        assert_eq!(
            order.hash(&value(&row[0]), Limits::default()).unwrap() as u64,
            row[1].as_u64().unwrap()
        );
    }
    let old = otp_order::profile(&FIXTURE["profile"]);
    assert_eq!(
        old.hash(&Value::MaxTtl, Limits::default()),
        Err(ae_vm_fate::Error::InvalidOrderProfile)
    );
    assert!(ae_vm_fate::value::check_limits(&Value::MaxTtl, Limits::default()).is_ok());
    assert_eq!(
        ae_vm_fate::encoding::encode(&Value::MaxTtl, Limits::default()),
        Err(ae_vm_fate::Error::InvalidValue)
    );
}
#[test]
fn authentication_transactions_wrappers_and_named_targets_match_original_instruction_and_whole_engine()
 {
    assert_eq!(HOSTS["base"], FIXTURE["base"]);
    assert_eq!(rows(&HOSTS["wire_versions"]).len(), 32);
    let order = otp_order::profile(&HOSTS["profile"]);
    let overrides = Overrides::default();
    let sources = remote_tests::sources();
    for packed in rows(&HOSTS["cases"]) {
        let row = unpack(packed);
        let mut context = context(&row);
        context.current_contract = small(1);
        context.reserved_gas = 18.into();
        let executor = Executor {
            context: &context,
            order: &order,
            overrides: &overrides,
            contracts: &sources,
        };
        let host = context_tests::chain_base(&row);
        for run in rows(&row["runs"]) {
            let (mut state, mut limits) =
                context_tests::start_with_host(&executor, &row, &run["gas"], host.clone());
            for (index, step) in rows(&run["steps"]).iter().enumerate() {
                let expected = &HOSTS["results"][step.as_u64().unwrap() as usize];
                let label = format!(
                    "case {} {} protocol {} vm {} gas {} step {}",
                    row["case_id"], row["name"], row["protocol"], row["vm"], run["gas"], index
                );
                let out = executor
                    .advance_remote_program(&state, limits)
                    .unwrap_or_else(|e| panic!("{label}: {e:?}"));
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
                    break;
                }
            }
        }
    }
}
