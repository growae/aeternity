use super::wrapped::{aevm as aevm_config, environment, otp_order, vm};
use super::*;
use ae_execution::contracts::{self, aevm, fate};
use ae_vm_fate::{machine, maps::listing::Overrides};
use std::sync::Arc;
static AUTH: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_channel_payload_auth.json")));
fn execute<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    row: &J,
    t: &Trees<A, C>,
) -> Result<(), Error<A, C>> {
    execute_limits(
        row,
        t,
        SignatureLimits::default(),
        1_000_000,
        Default::default(),
        Default::default(),
    )
}
#[allow(clippy::too_many_arguments)]
fn execute_limits<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    row: &J,
    t: &Trees<A, C>,
    signature_limits: SignatureLimits,
    maximum_fate_steps: usize,
    aevm_limits: ae_vm_aevm::execution::Limits,
    fate_limits: ae_vm_fate::Limits,
) -> Result<(), Error<A, C>> {
    use ae_execution::channels::authentication::{self, Authorization};
    let network = NetworkProfile::mainnet();
    let vm = vm();
    let env = environment();
    let order = otp_order::profile(&AUTH["profile"]);
    let overrides = Overrides::default();
    let f = fate::Config {
        context: &vm,
        order: &order,
        overrides: &overrides,
        environment: &env,
        history: Arc::default(),
        auth_tx: None,
        metering_floor: None,
        diagnostic_profile: ae_vm_fate::diagnostic::Profile::Test,
    };
    let a = aevm_config();
    let config = Config {
        network: &network,
        fate: Some(&f),
        aevm: Some(&a),
        channel_limits: Default::default(),
        consensus: ae_protocol::ConsensusType::Pow,
        name_claim_bid_timeout: None,
        channel_state_atoms: None,
        signature_limits,
        oracle_limits: Default::default(),
        fate_limits,
        maximum_fate_steps,
        aevm_limits,
    };
    let signed = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
    let channel = ae_state::channels::Channel::deserialize(
        bytes(&row["channel_key"]).try_into().unwrap(),
        &bytes(&row["channel"]),
    )
    .unwrap();
    let mode = if row["type"] == "offchain" {
        Authorization::Offchain(&channel)
    } else {
        Authorization::Onchain
    };
    let c = Context::transaction(
        50,
        Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
    );
    let checked: Vec<_> = row["checked"][1]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| AccountPublicKey(bytes(k).try_into().unwrap()))
        .collect();
    authentication::verify(&signed, mode, t, &c, &checked, &config)
}
fn native(reason: J) -> J {
    json!({"kind":"native","class":"error","reason":reason})
}
fn native_term(t: &ae_vm_fate::native::Term) -> J {
    use ae_vm_fate::native::Term;
    match t {
        Term::Atom(a) => atom(a),
        Term::Integer(n) => json!(["integer", n.to_string()]),
        Term::Binary(b) => bin(b),
        Term::Tuple(v) => tuple(v.iter().map(native_term).collect()),
        Term::List(v) => json!(["list", v.iter().map(native_term).collect::<Vec<_>>()]),
        _ => panic!("unexpected native term {t:?}"),
    }
}
fn contract_failure(e: &contracts::Error) -> J {
    match e {
        contracts::Error::Rejected(s) => {
            json!({"kind":"ok","value":tuple(vec![atom("error"),atom(s)])})
        }
        contracts::Error::NativeException(e) => {
            assert_eq!(e.class, ae_vm_fate::native::Class::Error);
            native(native_term(&e.reason))
        }
        contracts::Error::Exception(machine::Exception::Native(s)) => native(atom(s)),
        contracts::Error::AuthenticationResultCase(n) => native(tuple(vec![
            atom("case_clause"),
            tuple(vec![atom("ok"), json!(["integer", n.to_string()])]),
        ])),
        e => panic!("unprojected contract failure {e:?}"),
    }
}
pub(super) fn failure<A, C>(e: &Error<A, C>) -> J {
    match e {
        Error::Contract(e) => contract_failure(e),
        Error::Aevm(e) => match e.as_ref() {
            ae_vm_aevm::outer_call::Fault::Runtime(ae_vm_aevm::runtime::Fault::Host(
                aevm::HostError::Transaction(e),
            )) => contract_failure(e),
            _ => panic!("unprojected AEVM error"),
        },
        _ => panic!("unprojected transaction error"),
    }
}
#[test]
fn channel_saved_and_current_authorization_match_both_original_vms_without_publication() {
    assert_eq!(AUTH["runs"].as_array().unwrap().len(), 1472);
    for (i, row) in AUTH["runs"].as_array().unwrap().iter().enumerate() {
        let trees = initial(row);
        let before = trees.roots();
        let actual = match execute(row, &trees) {
            Ok(()) => json!({"kind":"ok","value":atom("ok")}),
            Err(ref e) => failure(e),
        };
        assert_eq!(trees.roots(), before, "row {i}");
        if let Some(d) = difference(&normalize(&actual), &normalize(&row["result"]), "") {
            panic!(
                "row {i} protocol {} ABI {} {} {}: {d}; actual {actual}",
                row["protocol"], row["abi"], row["type"], row["mode"]
            );
        }
    }
}

fn fixture(abi: u64, kind: &str, mode: &str) -> &'static J {
    AUTH["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == 6 && r["abi"] == abi && r["type"] == kind && r["mode"] == mode)
        .unwrap()
}
#[test]
fn detached_payload_authentication_never_hides_backend_faults_or_publishes_vm_changes() {
    use super::failures::{Control, wrapped};
    use std::rc::Rc;
    let mut prefixes = 0;
    for abi in [1, 3] {
        for kind in ["offchain", "onchain"] {
            for mode in [
                "normal",
                "both",
                "repeated_saved",
                "hash_nested",
                "code_ref",
                if kind == "offchain" {
                    "saved_store"
                } else {
                    "current_store"
                },
            ] {
                let row = fixture(abi, kind, mode);
                let original = initial(row);
                let before = original.roots();
                let control = Rc::new(Control::default());
                let trees = wrapped(&original, &control);
                control.reset();
                execute(row, &trees).unwrap();
                let counts = [control.reads.get(), control.writes.get()];
                assert!(counts[0] > 0);
                for (dimension, count) in counts.into_iter().enumerate() {
                    for permitted in 0..=count {
                        control.reset();
                        if dimension == 0 {
                            control.read_limit.set(Some(permitted));
                        } else {
                            control.write_limit.set(Some(permitted));
                        }
                        match execute(row, &trees) {
                            Ok(()) => {
                                assert_eq!(permitted, count, "{abi} {kind} {mode} {dimension}")
                            }
                            Err(e) => {
                                assert!(permitted < count, "{e:?}");
                                assert!(
                                    format!("{e:?}").contains("injected backend failure"),
                                    "{abi} {kind} {mode} {dimension} {permitted}: {e:?}"
                                );
                            }
                        }
                        assert_eq!(trees.roots(), before);
                        assert_eq!(
                            trees.names.cache_root_hash(),
                            original.names.cache_root_hash()
                        );
                        assert_eq!(
                            trees.oracles.cache_root_hash(),
                            original.oracles.cache_root_hash()
                        );
                        control.reset();
                        execute(row, &trees).unwrap();
                        assert_eq!(trees.roots(), before);
                        prefixes += 1;
                    }
                }
            }
        }
    }
    eprintln!("channel payload authentication backend prefixes: {prefixes}");
    assert!(prefixes > 500);
}
#[test]
fn detached_payload_authentication_operational_limits_are_retryable() {
    let mut retries = 0;
    for abi in [1, 3] {
        for kind in ["offchain", "onchain"] {
            for mode in ["normal", "both", "repeated_saved", "hash_nested"] {
                let row = fixture(abi, kind, mode);
                let trees = initial(row);
                let roots = trees.roots();
                for dimension in 0..5 {
                    let mut sig = SignatureLimits::default();
                    let mut steps = 1_000_000;
                    let mut a = ae_vm_aevm::execution::Limits::default();
                    let mut f = ae_vm_fate::Limits::default();
                    match dimension {
                        0 => sig.max_layers = 0,
                        1 => sig.max_encoded_bytes = 0,
                        2 => sig.max_encoded_bytes = 1,
                        3 => {
                            if abi == 1 {
                                a.max_work = 0
                            } else {
                                steps = 0
                            }
                        }
                        _ => {
                            if abi == 1 {
                                a.max_bytes = 0
                            } else {
                                f.max_bytes = 0
                            }
                        }
                    }
                    let err = execute_limits(row, &trees, sig, steps, a, f).unwrap_err();
                    assert!(
                        format!("{err:?}").contains("ResourceLimit"),
                        "{abi} {kind} {mode} {dimension}: {err:?}"
                    );
                    assert_eq!(trees.roots(), roots);
                    execute(row, &trees).unwrap();
                    assert_eq!(trees.roots(), roots);
                    retries += 1;
                }
            }
        }
    }
    eprintln!("channel payload authentication budget retries: {retries}");
    assert_eq!(retries, 80);
}
