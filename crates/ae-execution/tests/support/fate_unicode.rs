use super::*;
static CLUSTERS: LazyLock<J> = LazyLock::new(|| {
    let packed: J = serde_json::from_str(include_str!(
        "../fixtures/erlang_fate_unicode_clusters.json"
    ))
    .unwrap();
    let pool = rows(&packed["values"]);
    name_tests::expand(&packed["root"], pool, pool.len())
});
#[test]
fn native_unicode_cluster_instructions_match_original_results_gas_and_storage() {
    assert_eq!(rows(&CLUSTERS["cases"]).len(), 13248);
    assert_eq!(
        rows(&CLUSTERS["cases"])
            .iter()
            .map(|r| rows(&r["runs"]).len())
            .sum::<usize>(),
        156432
    );
    check_runs(&CLUSTERS);
}

#[test]
fn unicode_cluster_contract_results_gas_and_roots_match_original() {
    let packed: J = serde_json::from_str(include_str!(
        "../fixtures/erlang_fate_unicode_contracts.json"
    ))
    .unwrap();
    contract_diagnostic_tests::check_profiles(packed, 576);
}
#[test]
fn unicode_cluster_instruction_limits_preserve_input_and_retry_native_outcomes() {
    let order = otp_order::profile(&CLUSTERS["profile"]);
    let overrides = Overrides::default();
    let contracts = BTreeMap::new();
    for op in ["STR_LENGTH", "STR_REVERSE", "STR_TO_LIST", "STR_FROM_LIST"] {
        let row = rows(&CLUSTERS["cases"])
            .iter()
            .find(|r| {
                r["protocol"] == 8
                    && r["vm"] == 8
                    && r["op"][0] == op
                    && CLUSTERS["inputs"][r["input_index"].as_u64().unwrap() as usize]
                        == serde_json::json!([99, 8204, 769])
            })
            .unwrap();
        let ctx = context(row);
        let input = state(row, &row["initial"]["gas"]);
        let ins = instruction(&row["op"]);
        let executor = Executor {
            context: &ctx,
            order: &order,
            overrides: &overrides,
            contracts: &contracts,
        };
        let high = rows(&row["runs"])
            .iter()
            .find(|r| r[0] == row["initial"]["gas"])
            .unwrap();
        let expected = &CLUSTERS["results"][high[1].as_u64().unwrap() as usize];
        assert_eq!(expected["kind"], "ok");
        for bytes in [false, true] {
            let mut success = false;
            for allowance in 0..10000 {
                let limits = if bytes {
                    Limits {
                        max_bytes: allowance,
                        ..Limits::default()
                    }
                } else {
                    Limits {
                        max_nodes: allowance,
                        ..Limits::default()
                    }
                };
                match executor.step(&input, &ins, limits) {
                    Err(Error::Data(ae_vm_fate::Error::ResourceLimit)) => {}
                    Ok(actual) => {
                        assert_outcome(actual, expected);
                        success = true;
                    }
                    Err(e) => panic!("{op}: {e:?}"),
                }
                assert_state(&input, &row["initial"]);
                if success {
                    break;
                }
            }
            assert!(success, "{op}");
        }
        assert_outcome(
            executor.step(&input, &ins, Limits::default()).unwrap(),
            expected,
        );
    }
}
