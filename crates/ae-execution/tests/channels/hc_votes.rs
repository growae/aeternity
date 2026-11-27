use super::wrapped::{aevm, environment, otp_order, vm};
use super::*;
use ae_execution::contracts::{self, fate};
use ae_types::Hash256;
use ae_vm_fate::maps::listing::Overrides;
use std::sync::Arc;
static HC: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_hc_vote_wrapped.json")));
pub(super) fn execute<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    row: &J,
    s: &SignedTransaction<Transaction>,
    t: &mut Trees<A, C>,
    c: &Context,
    signature_limits: SignatureLimits,
    steps: usize,
) -> Result<Receipt, Error<A, C>> {
    let network = NetworkProfile::mainnet();
    let vm = vm();
    let env = environment();
    let order = otp_order::profile(&HC["profile"]);
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
    let a = aevm();
    let config = Config {
        control: None,
        network: &network,
        fate: Some(&f),
        aevm: Some(&a),
        channel_limits: Default::default(),
        consensus: if row["consensus"] == "pos" {
            ae_protocol::ConsensusType::Pos
        } else {
            ae_protocol::ConsensusType::Pow
        },
        name_claim_bid_timeout: None,
        channel_state_atoms: None,
        signature_limits,
        oracle_limits: Default::default(),
        fate_limits: Default::default(),
        maximum_fate_steps: steps,
        aevm_limits: ae_vm_aevm::execution::Limits {
            max_work: steps,
            ..Default::default()
        },
    };
    transactions::execute_signed(s, t, c, &config)
}
fn contract_error(e: &contracts::Error) -> J {
    let s = match e {
        contracts::Error::Rejected(s) => *s,
        contracts::Error::Primitive(e) => primitive(e),
        e => panic!("unprojected contract error {e:?}"),
    };
    json!({"kind":"ok","value":tuple(vec![atom("error"),atom(s)])})
}
fn failure(e: &Error) -> J {
    match e {
        Error::Contract(e) => contract_error(e),
        Error::Aevm(e) => match e.as_ref() {
            ae_vm_aevm::outer_call::Fault::Runtime(ae_vm_aevm::runtime::Fault::Host(
                contracts::aevm::HostError::Transaction(e),
            )) => contract_error(e),
            e => panic!("unprojected AEVM error {e:?}"),
        },
        Error::Signature(e) => verification(Err(e.clone())),
        e => captured_error(e),
    }
}
#[test]
fn hc_wrappers_execute_original_authorization_and_inner_rejection() {
    assert_eq!(HC["runs"].as_array().unwrap().len(), 320);
    for (i, r) in HC["runs"].as_array().unwrap().iter().enumerate() {
        let s = SignedTransaction::<Transaction>::decode(&bytes(&r["signed"])).unwrap();
        let c = Context::transaction(
            50,
            Protocol::try_from(r["protocol"].as_u64().unwrap() as u32).unwrap(),
        );
        let mut t = initial(r);
        let before = t.roots();
        let result = execute(r, &s, &mut t, &c, SignatureLimits::default(), 1_000_000);
        let expected = &r["result"];
        match result {
            Ok(receipt) => {
                if expected["kind"] != "ok" || expected["value"][1][0] != atom("ok") {
                    panic!(
                        "row {i} unexpected success: {} {} {}",
                        r["protocol"], r["operation"], r["mode"]
                    );
                }
                let state = &expected["value"][1][1];
                let actual = snapshot(&t, &receipt, Some(&s));
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
                        &normalize(field(state, k)),
                        k,
                    ) {
                        panic!(
                            "row {i} {} {} ABI {} protocol {}: {d}",
                            r["operation"], r["mode"], r["abi"], r["protocol"]
                        );
                    }
                }
                assert_eq!(
                    entries(t.calls.tree()),
                    *field(state, "calls"),
                    "calls row {i}"
                );
                assert_eq!(number(&receipt.gas), expected["value"][1][2], "gas row {i}");
                let ids: std::collections::BTreeSet<_> = field(state, "ga_auth_ids")[1]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|b| AccountPublicKey(bytes(b).try_into().unwrap()))
                    .collect();
                assert_eq!(receipt.context.ga_authenticated, ids, "auth IDs row {i}");
                for pair in field(state, "ga_nonces")[1].as_array().unwrap() {
                    let key = AccountPublicKey(bytes(&pair[1][0]).try_into().unwrap());
                    let expected = &pair[1][1];
                    let actual = receipt
                        .context
                        .ga_nonces
                        .get(&key)
                        .map(|n| tuple(vec![atom("value"), bin(n)]))
                        .unwrap_or(atom("none"));
                    assert_eq!(actual, *expected, "nonce row {i}");
                }
            }
            Err(e) => {
                assert_eq!(t.roots(), before, "rollback row {i}");
                if let Some(d) = difference(&normalize(&failure(&e)), &normalize(expected), "") {
                    panic!(
                        "row {i} {} {} ABI {} protocol {}: {d}",
                        r["operation"], r["mode"], r["abi"], r["protocol"]
                    );
                }
            }
        }
    }
}

fn roots<A: NodeReader, C: NodeReader>(
    t: &Trees<A, C>,
) -> (ae_state::StateRoots, Option<Hash256>, Option<Hash256>) {
    (
        t.roots(),
        t.names.cache_root_hash(),
        t.oracles.cache_root_hash(),
    )
}
#[test]
fn hc_wrapper_storage_failures_cannot_be_swallowed_by_ga() {
    use super::failures::{Control, wrapped};
    use std::rc::Rc;
    let mut prefixes = 0;
    for row in HC["runs"].as_array().unwrap().iter().filter(|r| {
        r["consensus"] == "pos"
            && (r["mode"] == "meta" || r["mode"] == "payer_meta")
            && (r["protocol"] == 4 || r["protocol"] == 6)
            && r["result"]["value"][1][0] == atom("ok")
    }) {
        let signed = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
        let c = Context::transaction(
            50,
            Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
        );
        let input = initial(row);
        let before = roots(&input);
        let control = Rc::new(Control::default());
        let base = wrapped(&input, &control);
        control.reset();
        let mut expected = base.clone();
        let receipt = execute(
            row,
            &signed,
            &mut expected,
            &c,
            SignatureLimits::default(),
            1_000_000,
        )
        .unwrap();
        let counts = [control.reads.get(), control.writes.get()];
        let expected_roots = roots(&expected);
        for (dimension, count) in counts.into_iter().enumerate() {
            assert!(count > 0);
            for allowed in 0..count {
                control.reset();
                if dimension == 0 {
                    control.read_limit.set(Some(allowed));
                } else {
                    control.write_limit.set(Some(allowed));
                }
                let mut state = base.clone();
                let error = execute(
                    row,
                    &signed,
                    &mut state,
                    &c,
                    SignatureLimits::default(),
                    1_000_000,
                )
                .unwrap_err();
                assert!(
                    format!("{error:?}").contains("injected backend failure"),
                    "{error:?}"
                );
                assert_eq!(roots(&state), before);
                control.reset();
                let r = execute(
                    row,
                    &signed,
                    &mut state,
                    &c,
                    SignatureLimits::default(),
                    1_000_000,
                )
                .unwrap();
                assert_eq!(roots(&state), expected_roots);
                assert_eq!(r.gas, receipt.gas);
                assert_eq!(r.events, receipt.events);
                assert_eq!(r.call, receipt.call);
                prefixes += 1;
            }
        }
    }
    eprintln!("HC backend failure prefixes: {prefixes}");
    assert!(prefixes > 100);
}
#[test]
fn hc_wrapper_vm_budgets_abort_without_publishing_authentication() {
    let mut retries = 0;
    for row in HC["runs"].as_array().unwrap().iter().filter(|r| {
        r["consensus"] == "pos"
            && (r["mode"] == "meta" || r["mode"] == "payer_meta")
            && r["result"]["value"][1][0] == atom("ok")
    }) {
        let signed = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
        let c = Context::transaction(
            50,
            Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
        );
        let mut state = initial(row);
        let before = roots(&state);
        execute(row, &signed, &mut state, &c, SignatureLimits::default(), 0).unwrap_err();
        assert_eq!(roots(&state), before);
        let receipt = execute(
            row,
            &signed,
            &mut state,
            &c,
            SignatureLimits::default(),
            1_000_000,
        )
        .unwrap();
        assert_eq!(
            bin(state.roots().hash().0),
            *field(&row["result"]["value"][1][1], "root")
        );
        assert_eq!(number(&receipt.gas), row["result"]["value"][1][2]);
        retries += 1;
    }
    eprintln!("HC VM budget retries: {retries}");
    assert!(retries >= 12);
}
