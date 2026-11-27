use super::wrapped::{aevm as aevm_config, environment, otp_order, vm};
use super::*;
use ae_execution::{channels::offchain, contracts::fate};
use ae_vm_fate::maps::listing::Overrides;
use std::sync::Arc;
static OFFCHAIN: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_channel_offchain.json")));
fn trees(s: &J) -> Trees {
    let a = tree(field(s, "accounts"));
    let c = tree(field(s, "contracts"));
    let calls = tree(field(s, "calls"));
    Trees {
        accounts: Accounts::from_root(a.root_hash(), a.store().clone()).unwrap(),
        contracts: Contracts::from_root(c.root_hash(), c.store().clone()).unwrap(),
        calls: ae_state::calls::Calls::from_root(calls.root_hash(), calls.store().clone()).unwrap(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    }
}
fn snapshot<A: NodeReader, C: NodeReader>(t: &Trees<A, C>) -> J {
    json!([
        "map",
        [
            [atom("root"), bin(t.roots().hash().0)],
            [atom("accounts"), entries(t.accounts.tree())],
            [atom("contracts"), entries(t.contracts.tree())],
            [atom("calls"), entries(t.calls.tree())]
        ]
    ])
}
fn execute<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    r: &J,
    t: &mut Trees<A, C>,
    on: &Trees<A, C>,
) -> Result<Option<ae_state::calls::Call>, Error<A, C>> {
    execute_budgets(r, t, on, Budgets::default())
}
struct Budgets {
    fate: ae_vm_fate::Limits,
    steps: usize,
    aevm: ae_vm_aevm::execution::Limits,
}
impl Default for Budgets {
    fn default() -> Self {
        Self {
            fate: Default::default(),
            steps: 1_000_000,
            aevm: Default::default(),
        }
    }
}
fn execute_budgets<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    r: &J,
    t: &mut Trees<A, C>,
    on: &Trees<A, C>,
    budget: Budgets,
) -> Result<Option<ae_state::calls::Call>, Error<A, C>> {
    let network = NetworkProfile::mainnet();
    let vm = vm();
    let env = environment();
    let order = otp_order::profile(&OFFCHAIN["profile"]);
    let overrides = Overrides::default();
    let f = fate::Config {
        control: None,
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
        control: None,
        network: &network,
        fate: Some(&f),
        aevm: Some(&a),
        channel_limits: Default::default(),
        consensus: ae_protocol::ConsensusType::Pow,
        name_claim_bid_timeout: None,
        channel_state_atoms: None,
        signature_limits: Default::default(),
        oracle_limits: Default::default(),
        fate_limits: budget.fate,
        maximum_fate_steps: budget.steps,
        aevm_limits: budget.aevm,
    };
    let height = option(r, "height")
        .map(|v| v[1].as_str().unwrap().parse().unwrap())
        .unwrap_or(50);
    let mut c = Context::transaction(
        height,
        Protocol::try_from(r["protocol"].as_u64().unwrap() as u32).unwrap(),
    );
    c.dry_run = option(r, "dry_run") == Some(&atom("true"));
    if option(r, "context") == Some(&atom("aetx_contract")) {
        c.origin = Origin::Contract;
    }
    let u = ae_tx::channels::Update::decode(&bytes(&r["update"])).unwrap();
    let number = |v: &J| {
        Unsigned::from_field_bytes(
            &num_bigint::BigUint::parse_bytes(v.to_string().as_bytes(), 10)
                .unwrap()
                .to_bytes_be(),
        )
        .unwrap()
    };
    offchain::apply(
        &u,
        t,
        on,
        &c,
        &number(&r["round"]),
        &number(&r["reserve"]),
        &config,
    )
}
fn result<A: NodeReader, C: NodeReader>(
    t: &Trees<A, C>,
    call: Option<&ae_state::calls::Call>,
) -> J {
    let status = call
        .map(|c| {
            tuple(vec![
                atom(match c.return_type {
                    ae_state::calls::ReturnType::Ok => "ok",
                    ae_state::calls::ReturnType::Error => "error",
                    ae_state::calls::ReturnType::Revert => "revert",
                }),
                bin(c.serialize().unwrap()),
            ])
        })
        .unwrap_or(atom("ok"));
    json!({"kind":"ok", "value": tuple(vec![status, snapshot(t)])})
}
pub(super) fn failure(e: &Error) -> J {
    match e {
        Error::Aevm(e)
            if matches!(
                e.as_ref(),
                ae_vm_aevm::outer_call::Fault::Runtime(ae_vm_aevm::runtime::Fault::Native(_))
            ) =>
        {
            let ae_vm_aevm::outer_call::Fault::Runtime(ae_vm_aevm::runtime::Fault::Native(n)) =
                e.as_ref()
            else {
                unreachable!()
            };
            match n {
                ae_vm_aevm::runtime::Native::ChainException { class, reason } => {
                    json!({"kind":"native","class":class,"reason":atom(reason)})
                }
                ae_vm_aevm::runtime::Native::BadMatchBinary(b) => {
                    json!({"kind":"native","class":"error","reason":tuple(vec![atom("badmatch"),bin(b)])})
                }
                _ => panic!("unprojected AEVM native {n:?}"),
            }
        }
        Error::Contract(_) | Error::Aevm(_) => authentication::failure(e),
        _ => captured_error(e),
    }
}
#[test]
fn offchain_updates_match_both_original_vms_and_separate_chain_views() {
    let rows = OFFCHAIN["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 7000);
    for (i, r) in rows.iter().enumerate() {
        let mut t = trees(&r["before"]);
        let on = trees(&r["onchain"]);
        let before = t.roots();
        let on_before = on.roots();
        let actual = match execute(r, &mut t, &on) {
            Ok(call) => result(&t, call.as_ref()),
            Err(e) => {
                assert_eq!(t.roots(), before, "rollback row {i}");
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| failure(&e))) {
                    Ok(v) => v,
                    Err(_) => panic!(
                        "row {i} P{} VM{} {} {}: {e:?}",
                        r["protocol"], r["vm"], r["operation"], r["label"]
                    ),
                }
            }
        };
        assert_eq!(on.roots(), on_before);
        if let Some(d) = difference(&normalize(&actual), &normalize(&r["result"]), "") {
            panic!(
                "row {i} P{} VM{} {} {}: {d}",
                r["protocol"], r["vm"], r["operation"], r["label"]
            );
        }
    }
}
fn fixture(vm: u64, operation: &str, label: J) -> &'static J {
    OFFCHAIN["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["protocol"] == 6
                && r["vm"] == vm
                && r["operation"] == operation
                && r["label"] == label
        })
        .unwrap()
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
fn matches<A: NodeReader, C: NodeReader>(
    row: &J,
    t: &Trees<A, C>,
    call: Option<&ae_state::calls::Call>,
) {
    if let Some(d) = difference(&normalize(&result(t, call)), &normalize(&row["result"]), "") {
        panic!("{} VM{} {}: {d}", row["operation"], row["vm"], row["label"]);
    }
}
#[test]
fn offchain_backend_failures_preserve_both_views_and_retry() {
    use super::failures::{Control, wrapped};
    use std::rc::Rc;
    let mut cases = Vec::new();
    for operation in ["transfer", "deposit", "withdraw", "meta"] {
        cases.push(fixture(8, operation, atom("normal")));
    }
    for vm in [1, 5, 6, 8] {
        cases.push(fixture(vm, "create", atom("normal")));
        for label in [
            atom("normal"),
            atom("code_ref"),
            tuple(vec![atom("mode"), atom("nested")]),
            tuple(vec![atom("mode"), atom("onchain_balance")]),
            tuple(vec![
                atom("iris"),
                json!(["integer", "441444"]),
                atom("revert"),
            ]),
        ] {
            cases.push(fixture(vm, "call", label));
        }
    }
    let mut prefixes = 0;
    for row in cases {
        let original = trees(&row["before"]);
        let on_original = trees(&row["onchain"]);
        let control = Rc::new(Control::default());
        let base = wrapped(&original, &control);
        let on = wrapped(&on_original, &control);
        let before = roots(&base);
        let on_before = roots(&on);
        control.reset();
        let mut t = base.clone();
        let call = execute(row, &mut t, &on).unwrap();
        let counts = [control.reads.get(), control.writes.get()];
        control.reset();
        matches(row, &t, call.as_ref());
        for (dimension, count) in counts.into_iter().enumerate() {
            for permitted in 0..count {
                control.reset();
                if dimension == 0 {
                    control.read_limit.set(Some(permitted));
                } else {
                    control.write_limit.set(Some(permitted));
                }
                let mut t = base.clone();
                let err = execute(row, &mut t, &on).unwrap_err();
                assert!(
                    format!("{err:?}").contains("injected backend failure"),
                    "{} VM{} {} dim{dimension} prefix{permitted}: {err:?}",
                    row["operation"],
                    row["vm"],
                    row["label"]
                );
                assert_eq!(roots(&t), before);
                assert_eq!(roots(&on), on_before);
                control.reset();
                let call = execute(row, &mut t, &on).unwrap();
                matches(row, &t, call.as_ref());
                assert_eq!(roots(&on), on_before);
                prefixes += 1;
            }
        }
    }
    eprintln!("off-chain backend failure prefixes: {prefixes}");
    assert!(prefixes > 500);
}
#[test]
fn offchain_vm_budget_failures_never_publish_state_or_calls() {
    let mut retries = 0;
    for vm in [1, 5, 6, 8] {
        for (operation, label) in [
            ("create", atom("normal")),
            ("call", atom("normal")),
            ("call", tuple(vec![atom("mode"), atom("nested")])),
        ] {
            let row = fixture(vm, operation, label);
            let mut t = trees(&row["before"]);
            let on = trees(&row["onchain"]);
            let before = roots(&t);
            let on_before = roots(&on);
            for dimension in 0..2 {
                let mut b = Budgets::default();
                if [1, 6].contains(&vm) {
                    if dimension == 0 {
                        b.aevm.max_work = 0;
                    } else {
                        b.aevm.max_bytes = 0;
                    }
                } else if dimension == 0 {
                    b.steps = 0;
                } else {
                    b.fate.max_bytes = 0;
                }
                let err = execute_budgets(row, &mut t, &on, b).unwrap_err();
                assert!(
                    format!("{err:?}").contains("ResourceLimit"),
                    "{operation} VM{vm} dim{dimension}: {err:?}"
                );
                assert_eq!(roots(&t), before);
                assert_eq!(roots(&on), on_before);
                let call = execute(row, &mut t, &on).unwrap();
                matches(row, &t, call.as_ref());
                assert_eq!(roots(&on), on_before);
                t = trees(&row["before"]);
                retries += 1;
            }
        }
    }
    assert_eq!(retries, 24);
    eprintln!("off-chain budget retries: {retries}");
}
