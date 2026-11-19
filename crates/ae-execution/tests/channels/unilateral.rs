use super::wrapped::{aevm as aevm_config, environment, otp_order, vm};
use super::*;
use ae_execution::contracts::fate;
use ae_vm_fate::maps::listing::Overrides;
use std::sync::Arc;
static UNILATERAL: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_channel_unilateral.json")));
struct Budgets {
    signature: SignatureLimits,
    channel: ae_execution::channels::unilateral::Limits,
    steps: usize,
    fate: ae_vm_fate::Limits,
    aevm: ae_vm_aevm::execution::Limits,
}
impl Default for Budgets {
    fn default() -> Self {
        Self {
            signature: Default::default(),
            channel: Default::default(),
            steps: 1_000_000,
            fate: Default::default(),
            aevm: Default::default(),
        }
    }
}
fn execute<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    row: &J,
    trees: &mut Trees<A, C>,
    limits: SignatureLimits,
) -> Result<Receipt, Error<A, C>> {
    execute_budgets(
        row,
        trees,
        Budgets {
            signature: limits,
            ..Default::default()
        },
    )
}
fn execute_budgets<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    row: &J,
    trees: &mut Trees<A, C>,
    budgets: Budgets,
) -> Result<Receipt, Error<A, C>> {
    let network = NetworkProfile::mainnet();
    let vm = vm();
    let environment = environment();
    let order = otp_order::profile(&UNILATERAL["profile"]);
    let overrides = Overrides::default();
    let f = fate::Config {
        context: &vm,
        order: &order,
        overrides: &overrides,
        environment: &environment,
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
        channel_limits: budgets.channel,
        channel_state_atoms: None,
        signature_limits: budgets.signature,
        oracle_limits: Default::default(),
        fate_limits: budgets.fate,
        maximum_fate_steps: budgets.steps,
        aevm_limits: budgets.aevm,
    };
    let signed = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
    let mut c = context(row);
    c.height = row["height"].as_u64().unwrap();
    if row["parent_tx"][0] == "binary" {
        c.ga_transaction = Some(Arc::new(
            ae_tx::WireTransaction::decode(&bytes(&row["parent_tx"])).unwrap(),
        ));
    }
    if option(row, "wrapped") == Some(&atom("true")) {
        transactions::execute_signed(&signed, trees, &c, &config)
    } else {
        let source = (option(row, "no_signed") != Some(&atom("true"))).then_some(&signed);
        transactions::process_with_signed_source(signed.transaction(), trees, &c, &config, source)
    }
}
fn failure(e: &Error) -> J {
    match e {
        Error::Contract(_) | Error::Aevm(_) => authentication::failure(e),
        Error::Signature(e) => verification(Err(e.clone())),
        _ => captured_error(e),
    }
}
fn compare<A: NodeReader, C: NodeReader>(row: &J, t: &Trees<A, C>, r: &Receipt) -> Option<String> {
    let signed = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
    let source = (option(row, "no_signed") != Some(&atom("true"))).then_some(&signed);
    let actual = snapshot(t, r, source);
    let expected = &row["result"]["value"][1][1];
    for k in [
        "root",
        "accounts",
        "channels",
        "contracts",
        "events",
        "context",
        "payer",
    ] {
        if let Some(d) = difference(
            &normalize(field(&actual, k)),
            &normalize(field(expected, k)),
            k,
        ) {
            return Some(d);
        }
    }
    if let Some(d) = difference(&entries(t.calls.tree()), field(expected, "calls"), "calls") {
        return Some(d);
    }
    if let Some(d) = difference(&number(&r.gas), &row["result"]["value"][1][2], "gas") {
        return Some(d);
    }
    let ids: std::collections::BTreeSet<_> = field(expected, "ga_auth_ids")[1]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| AccountPublicKey(bytes(b).try_into().unwrap()))
        .collect();
    assert_eq!(r.context.ga_authenticated, ids);
    for pair in field(expected, "ga_nonces")[1].as_array().unwrap() {
        let key = AccountPublicKey(bytes(&pair[1][0]).try_into().unwrap());
        let actual = r
            .context
            .ga_nonces
            .get(&key)
            .map(|n| tuple(vec![atom("value"), bin(n)]))
            .unwrap_or(atom("none"));
        assert_eq!(actual, pair[1][1]);
    }
    None
}
#[test]
fn four_unilateral_callbacks_match_original_state_proofs_authorization_and_order() {
    let rows = UNILATERAL["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 5136);
    for (i, row) in rows.iter().enumerate() {
        let mut trees = initial(row);
        let before = trees.roots();
        let result = execute(row, &mut trees, SignatureLimits::default());
        let d = match result {
            Ok(receipt) => {
                if row["result"]["kind"] != "ok" || row["result"]["value"][1][0] != atom("ok") {
                    Some(format!("unexpected success, expected {}", row["result"]))
                } else {
                    compare(row, &trees, &receipt)
                }
            }
            Err(ref e) => {
                assert_eq!(trees.roots(), before, "rollback row {i}");
                difference(&normalize(&failure(e)), &normalize(&row["result"]), "")
            }
        };
        if let Some(d) = d {
            panic!(
                "row {i} protocol {} {} {}: {d}",
                row["protocol"], row["operation"], row["label"]
            );
        }
    }
}
fn fixture(op: &str, label: J) -> &'static J {
    UNILATERAL["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == 6 && r["operation"] == op && r["label"] == label)
        .unwrap()
}
fn ga(abi: u64, mode: &str) -> J {
    tuple(vec![atom("ga"), number(&abi.into()), atom(mode)])
}
fn roots<A: NodeReader, C: NodeReader>(
    t: &Trees<A, C>,
) -> (
    ae_state::StateRoots,
    Option<ae_types::Hash256>,
    Option<ae_types::Hash256>,
) {
    (
        t.roots(),
        t.names.cache_root_hash(),
        t.oracles.cache_root_hash(),
    )
}
#[test]
fn unilateral_backend_failures_are_atomic_through_both_authorization_vms_and_payers() {
    use super::failures::{Control, wrapped};
    use std::rc::Rc;
    let mut prefixes = 0;
    for op in ["close", "slash", "snapshot", "delegates"] {
        let mut cases = vec![fixture(op, atom("normal"))];
        for abi in [1, 3] {
            for mode in [
                "both",
                "payer_wrapped",
                "wrapped_bad_proof",
                "wrapped_bad_delegate",
            ] {
                cases.push(fixture(op, ga(abi, mode)));
            }
        }
        for row in cases {
            let original = initial(row);
            let before = roots(&original);
            let control = Rc::new(Control::default());
            let base = wrapped(&original, &control);
            control.reset();
            execute(row, &mut base.clone(), SignatureLimits::default()).unwrap();
            let counts = [control.reads.get(), control.writes.get()];
            for (dimension, count) in counts.into_iter().enumerate() {
                assert!(count > 0);
                for permitted in 0..=count {
                    control.reset();
                    if dimension == 0 {
                        control.read_limit.set(Some(permitted));
                    } else {
                        control.write_limit.set(Some(permitted));
                    }
                    let mut t = base.clone();
                    match execute(row, &mut t, SignatureLimits::default()) {
                        Ok(receipt) => {
                            assert_eq!(permitted, count, "{op} {}", row["label"]);
                            control.reset();
                            assert!(compare(row, &t, &receipt).is_none());
                        }
                        Err(e) => {
                            assert!(permitted < count);
                            assert!(
                                format!("{e:?}").contains("injected backend failure"),
                                "{op} {}: {e:?}",
                                row["label"]
                            );
                            assert_eq!(roots(&t), before);
                            control.reset();
                            let receipt = execute(row, &mut t, SignatureLimits::default()).unwrap();
                            assert!(compare(row, &t, &receipt).is_none());
                            prefixes += 1;
                        }
                    }
                }
            }
        }
    }
    eprintln!("unilateral backend failure prefixes: {prefixes}");
    assert!(prefixes > 2000);
}
#[test]
fn unilateral_proof_payload_vm_and_envelope_budgets_remain_retryable() {
    let mut retries = 0;
    for op in ["close", "slash", "snapshot", "delegates"] {
        for abi in [1, 3] {
            for mode in ["normal", "payer_wrapped"] {
                let row = fixture(op, ga(abi, mode));
                for dimension in 0..7 {
                    if dimension == 6 && !["close", "slash"].contains(&op) {
                        continue;
                    }
                    let mut b = Budgets::default();
                    match dimension {
                        0 => b.signature.max_layers = 0,
                        1 => b.signature.max_encoded_bytes = 0,
                        2 => b.channel.transaction.max_layers = 0,
                        3 => b.channel.transaction.max_total_bytes = 1,
                        4 => {
                            if abi == 1 {
                                b.aevm.max_work = 0;
                            } else {
                                b.steps = 0;
                            }
                        }
                        5 => {
                            if abi == 1 {
                                b.aevm.max_bytes = 0;
                            } else {
                                b.fate.max_bytes = 0;
                            }
                        }
                        _ => b.channel.state.max_work_bytes = 0,
                    }
                    let mut t = initial(row);
                    let before = roots(&t);
                    let err = execute_budgets(row, &mut t, b).unwrap_err();
                    assert!(
                        format!("{err:?}").contains("ResourceLimit"),
                        "{op} {abi} {mode} {dimension}: {err:?}"
                    );
                    assert_eq!(roots(&t), before);
                    let receipt = execute(row, &mut t, SignatureLimits::default()).unwrap();
                    assert!(compare(row, &t, &receipt).is_none());
                    retries += 1;
                }
            }
        }
    }
    assert_eq!(retries, 104);
    eprintln!("unilateral budget retries: {retries}");
}
