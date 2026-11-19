use super::*;
use ae_execution::contracts::{self, fate::Config};
use ae_tx::CanonicalTransaction;
use ae_vm_fate::maps::MapOrder;
use ae_vm_fate::{
    diagnostic::{self, Term},
    native::Class,
};
use serde_json::json;

static CASES: LazyLock<J> = LazyLock::new(|| {
    let packed: J =
        serde_json::from_str(include_str!("../fixtures/erlang_fate_code_contracts.json")).unwrap();
    let pool = rows(&packed["values"]);
    name_tests::expand(&packed["root"], pool, pool.len())
});
fn json_term(t: &Term<'_>) -> J {
    match t {
        Term::Atom(a) => json!(["atom", a]),
        Term::Integer(n) => json!(["integer", n.to_string()]),
        Term::Binary(b) => json!(["binary", hex::encode(b)]),
        Term::Tuple(v) => json!(["tuple", v.iter().map(json_term).collect::<Vec<_>>()]),
        Term::List(v) => json!(["list", v.iter().map(json_term).collect::<Vec<_>>()]),
        Term::Map(v) => json!([
            "map",
            v.iter()
                .map(|(k, v)| json!([json_term(k), json_term(v)]))
                .collect::<Vec<_>>()
        ]),
    }
}
fn failure(e: &contracts::Error, order: &impl MapOrder) -> J {
    let (class, reason) = match e {
        contracts::Error::NativeException(e) => (
            match e.class {
                Class::Error => "error",
                Class::Exit => "exit",
            },
            json_term(
                &diagnostic::native_term(&e.reason, order, Limits::default())
                    .unwrap()
                    .0,
            ),
        ),
        contracts::Error::Exception(Exception::Native(s)) => ("error", json!(["atom", s])),
        contracts::Error::Exception(Exception::Exit(s)) => ("exit", json!(["atom", s])),
        e => return contract_call_tests::failure(e),
    };
    json!({"kind":"exception","class":class,"reason":reason})
}
fn check_case(
    row: &J,
    build: diagnostic::Profile,
    order: &impl MapOrder,
    limits: Limits,
    allow_resource: bool,
) -> bool {
    use contract_call_tests::{chain, check, trees, vm_context};
    let host = chain(row);
    let mut t = trees(row, &host);
    let before = format!("{t:?}");
    let context = contract_create_tests::tx_context(row);
    let vm = vm_context(row);
    let overrides = Overrides::default();
    let config = Config {
        context: &vm,
        order,
        overrides: &overrides,
        environment: &host.environment,
        history: host.history.clone(),
        auth_tx: None,
        metering_floor: None,
        diagnostic_profile: build,
    };
    let tx = ae_tx::ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
    match contracts::process_create(&tx, &mut t, &context, &config, limits, 10000) {
        Ok(receipt) => check(row, &t, &receipt),
        Err(e) => {
            assert_eq!(format!("{t:?}"), before);
            assert_eq!(publication_tests::snapshot(&t), row["initial"]);
            if allow_resource
                && matches!(
                    e,
                    contracts::Error::ResourceLimit
                        | contracts::Error::Vm(ae_vm_fate::machine::Error::Data(
                            ae_vm_fate::Error::ResourceLimit
                        ))
                )
            {
                return false;
            }
            assert_eq!(
                failure(&e, order),
                row["result"],
                "protocol {} VM {} case {} context {}",
                row["protocol"],
                row["vm"],
                row["case_id"],
                row["context"]
            );
        }
    }
    true
}
#[test]
fn malformed_bytecode_contract_creation_matches_original_admission_exceptions_and_rollback() {
    for profile in rows(&CASES["profiles"]) {
        assert_eq!(rows(&profile["cases"]).len(), 14112);
        let order = otp_order::profile(&profile["profile"]);
        let build = if profile["build"] == "release" {
            diagnostic::Profile::Release
        } else {
            diagnostic::Profile::Test
        };
        for row in rows(&profile["cases"]) {
            check_case(row, build, &order, Limits::default(), false);
        }
    }
}
#[test]
fn creation_native_exception_payload_limits_leave_all_trees_retryable() {
    let profile = &CASES["profiles"][0];
    let order = otp_order::profile(&profile["profile"]);
    for tag in ["bad_opcode", "badmap"] {
        let row = rows(&profile["cases"])
            .iter()
            .find(|r| r["result"]["reason"][0] == "tuple" && r["result"]["reason"][1][0][1] == tag)
            .unwrap();
        for byte_budget in [false, true] {
            let mut reached = false;
            for n in 0..8192 {
                let limits = if byte_budget {
                    Limits {
                        max_bytes: n,
                        ..Limits::default()
                    }
                } else {
                    Limits {
                        max_nodes: n,
                        ..Limits::default()
                    }
                };
                if check_case(row, diagnostic::Profile::Release, &order, limits, true) {
                    reached = true;
                    break;
                }
            }
            assert!(reached, "{tag}");
        }
        check_case(
            row,
            diagnostic::Profile::Release,
            &order,
            Limits::default(),
            false,
        );
    }
}
