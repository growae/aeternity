use super::*;
use ae_execution::contracts::{self, fate::Config};
use ae_vm_fate::diagnostic::Profile;
use contract_call_tests::{chain, check, failure, transaction, trees, tx_context, vm_context};

#[test]
fn contract_error_records_match_original_release_and_test_builds() {
    let packed: J =
        serde_json::from_str(include_str!("../fixtures/erlang_contract_diagnostics.json")).unwrap();
    let pool = rows(&packed["values"]);
    let data = name_tests::expand(&packed["root"], pool, pool.len());
    let overrides = Overrides::default();
    for profile in rows(&data["profiles"]) {
        let diagnostic_profile = if profile["build"] == "release" {
            Profile::Release
        } else {
            Profile::Test
        };
        let order = otp_order::profile(&profile["profile"]);
        assert_eq!(rows(&profile["cases"]).len(), 648);
        for row in rows(&profile["cases"]) {
            let host = chain(row);
            let mut t = trees(row, &host);
            let context = tx_context(row);
            let vm = vm_context(row);
            let config = Config {
                context: &vm,
                order: &order,
                overrides: &overrides,
                environment: &host.environment,
                history: host.history.clone(),
                auth_tx: None,
                metering_floor: None,
                diagnostic_profile,
            };
            let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                match contracts::process_call(
                    &transaction(row),
                    &mut t,
                    &context,
                    &config,
                    Limits::default(),
                    10000,
                ) {
                    Ok(receipt) => check(row, &t, &receipt),
                    Err(e) => {
                        assert_eq!(failure(&e), row["result"]);
                        assert_eq!(publication_tests::snapshot(&t), row["initial"]);
                    }
                }
            }));
            assert!(
                checked.is_ok(),
                "{diagnostic_profile:?} protocol {} VM {} case {} dry {} context {}",
                row["protocol"],
                row["vm"],
                row["case_id"],
                row["dry_run"],
                row["context"]
            );
        }
    }
}
