use super::wrapped::{aevm as aevm_config, environment, otp_order, vm};
use super::*;
use ae_execution::contracts::fate;
use ae_tx::CanonicalTransaction;
use ae_vm_fate::maps::listing::Overrides;
use std::sync::Arc;
static FORCE: LazyLock<J> = LazyLock::new(|| {
    unpack(include_str!(
        "../fixtures/erlang_channel_force_progress.json"
    ))
});
struct Budgets {
    signature: SignatureLimits,
    channel: ae_execution::channels::unilateral::Limits,
    steps: usize,
    atoms: bool,
    fate: ae_vm_fate::Limits,
    aevm: ae_vm_aevm::execution::Limits,
}
impl Default for Budgets {
    fn default() -> Self {
        Self {
            signature: Default::default(),
            channel: Default::default(),
            steps: 1_000_000,
            atoms: true,
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
    let order = otp_order::profile(&FORCE["profile"]);
    let overrides = Overrides::default();
    let f = fate::Config {
        control: None,
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
        control: None,
        network: &network,
        fate: Some(&f),
        aevm: Some(&a),
        channel_limits: budgets.channel,
        consensus: ae_protocol::ConsensusType::Pow,
        name_claim_bid_timeout: None,
        channel_state_atoms: budgets.atoms.then_some(&*NAME_ATOMS),
        signature_limits: budgets.signature,
        oracle_limits: Default::default(),
        fate_limits: budgets.fate,
        maximum_fate_steps: budgets.steps,
        aevm_limits: budgets.aevm,
    };
    let signed = signed(row);
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
        Error::Contract(_) | Error::Aevm(_) => offchain::failure(e),
        Error::Signature(e) => verification(Err(e.clone())),
        _ => captured_error(e),
    }
}
fn compare<A: NodeReader, C: NodeReader>(row: &J, t: &Trees<A, C>, r: &Receipt) -> Option<String> {
    let signed = signed(row);
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
fn force_progress_matches_original_execution_settlement_and_admission() {
    let rows = FORCE["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 7280);
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

fn initial(row: &J) -> Trees {
    let mut t = super::initial(row);
    let c = tree(field(&row["before"], "calls"));
    t.calls = ae_state::calls::Calls::from_root(c.root_hash(), c.store().clone()).unwrap();
    t
}

static NAME_ATOMS: LazyLock<std::collections::BTreeSet<Vec<u8>>> =
    LazyLock::new(|| std::collections::BTreeSet::from([b"channel_fixture_status".to_vec()]));
fn signed(row: &J) -> SignedTransaction<Transaction> {
    SignedTransaction::decode_with_state_config(
        &bytes(&row["signed"]),
        Default::default(),
        ae_state::transport::Config {
            existing_name_status_atoms: Some(&NAME_ATOMS),
            ..Default::default()
        },
    )
    .unwrap()
}
#[test]
fn offchain_transactions_retain_original_onchain_rejections() {
    let rows = FORCE["offchain"].as_array().unwrap();
    assert_eq!(rows.len(), 96);
    let network = NetworkProfile::mainnet();
    let config = Config::primitives(&network);
    for row in rows {
        let tx = Transaction::decode(&bytes(&row["tx"])).unwrap();
        let mut c = Context::transaction(
            50,
            Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
        );
        c.origin = match row["context"][1].as_str().unwrap() {
            "aetx_contract" => Origin::Contract,
            "aetx_ga" => Origin::Generalized,
            _ => Origin::Transaction,
        };
        c.dry_run = row["dry_run"].as_bool().unwrap();
        let mut t = Trees {
            accounts: Default::default(),
            contracts: Default::default(),
            calls: Default::default(),
            channels: Default::default(),
            names: Default::default(),
            oracles: Default::default(),
        };
        let before = t.roots();
        let err = transactions::process(&tx, &mut t, &c, &config).unwrap_err();
        assert_eq!(t.roots(), before);
        assert_eq!(failure(&err), row["result"], "{row}");
    }
}
fn fixture(vm: u64, label: J) -> &'static J {
    FORCE["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == 6 && r["vm"] == vm && r["label"] == label)
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
fn expected<A: NodeReader, C: NodeReader>(row: &J, t: &Trees<A, C>, receipt: &Receipt) {
    if let Some(d) = compare(row, t, receipt) {
        panic!("VM{} {}: {d}", row["vm"], row["label"]);
    }
}
#[test]
fn force_progress_backend_failures_preserve_onchain_roots_and_retry() {
    use super::failures::{Control, wrapped};
    use std::rc::Rc;
    let mut prefixes = 0;
    for vm in [6, 8] {
        let mut labels = vec![
            atom("normal"),
            atom("state_hash"),
            atom("name_profile"),
            tuple(vec![atom("mode"), atom("nested")]),
        ];
        for abi in [1, 3] {
            for mode in ["payer_wrapped", "wrapped_reserve", "wrapped_name"] {
                labels.push(ga(abi, mode));
            }
        }
        for label in labels {
            let row = fixture(vm, label);
            let original = initial(row);
            let before = roots(&original);
            let control = Rc::new(Control::default());
            let base = wrapped(&original, &control);
            control.reset();
            let mut t = base.clone();
            let receipt = execute(row, &mut t, SignatureLimits::default()).unwrap();
            let counts = [control.reads.get(), control.writes.get()];
            control.reset();
            expected(row, &t, &receipt);
            for (dim, count) in counts.into_iter().enumerate() {
                for permitted in 0..count {
                    control.reset();
                    if dim == 0 {
                        control.read_limit.set(Some(permitted));
                    } else {
                        control.write_limit.set(Some(permitted));
                    }
                    let mut t = base.clone();
                    let err = execute(row, &mut t, SignatureLimits::default()).unwrap_err();
                    assert!(
                        format!("{err:?}").contains("injected backend failure"),
                        "VM{vm} {} {dim} {permitted}: {err:?}",
                        row["label"]
                    );
                    assert_eq!(roots(&t), before);
                    control.reset();
                    let receipt = execute(row, &mut t, SignatureLimits::default()).unwrap();
                    expected(row, &t, &receipt);
                    prefixes += 1;
                }
            }
        }
    }
    eprintln!("force-progress backend failure prefixes: {prefixes}");
    assert!(prefixes > 2000);
}
#[test]
fn force_progress_import_payload_and_vm_limits_remain_operational() {
    let mut retries = 0;
    for vm in [6, 8] {
        for label in [
            atom("normal"),
            atom("payload"),
            ga(1, "payer_wrapped"),
            ga(3, "payer_wrapped"),
        ] {
            let row = fixture(vm, label);
            for dimension in 0..6 {
                let mut b = Budgets::default();
                match dimension {
                    0 => b.signature.max_encoded_bytes = 1,
                    1 => b.channel.state.max_work_bytes = 0,
                    2 => b.channel.state.max_entries = 0,
                    3 => {
                        b.fate.max_bytes = 0;
                        b.aevm.max_bytes = 0;
                    }
                    4 => {
                        b.steps = 0;
                        b.aevm.max_work = 0;
                    }
                    _ => b.channel.state.max_input_bytes = 0,
                }
                let mut t = initial(row);
                let before = roots(&t);
                let err = execute_budgets(row, &mut t, b).unwrap_err();
                assert!(
                    format!("{err:?}").contains("ResourceLimit"),
                    "VM{vm} {} dim{dimension}: {err:?}",
                    row["label"]
                );
                assert_eq!(roots(&t), before);
                let receipt = execute(row, &mut t, SignatureLimits::default()).unwrap();
                expected(row, &t, &receipt);
                retries += 1;
            }
        }
        let row = fixture(vm, atom("name_profile"));
        let mut t = initial(row);
        let before = roots(&t);
        let err = execute_budgets(
            row,
            &mut t,
            Budgets {
                atoms: false,
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("MissingAtomProfile"), "{err:?}");
        assert_eq!(roots(&t), before);
        let receipt = execute(row, &mut t, SignatureLimits::default()).unwrap();
        expected(row, &t, &receipt);
        retries += 1;
    }
    eprintln!("force-progress budget/profile retries: {retries}");
    assert_eq!(retries, 50);
}
