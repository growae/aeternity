use super::*;
static META: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_ga_meta_execution.json")));
fn execute<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    row: &J,
    t: &mut Trees<A, C>,
    signature_limits: SignatureLimits,
    maximum_fate_steps: usize,
    aevm_limits: ae_vm_aevm::execution::Limits,
    fate_limits: ae_vm_fate::Limits,
) -> Result<Receipt, Error<A, C>> {
    let network = NetworkProfile::mainnet();
    let vm = vm();
    let env = environment();
    let order = otp_order::profile(&META["profile"]);
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
    let a = aevm();
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
    let s = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
    let mut c = tx_context(row);
    let payer = field(&row["before"], "payer");
    if payer[0] == "binary" {
        c.payer = Some(AccountPublicKey(bytes(payer).try_into().unwrap()));
    }
    for k in field(&row["before"], "ga_auth_ids")[1].as_array().unwrap() {
        c.ga_authenticated
            .insert(AccountPublicKey(bytes(k).try_into().unwrap()));
    }
    for entry in field(&row["before"], "ga_nonces")[1].as_array().unwrap() {
        if entry[1][1][0] == "tuple" {
            c.ga_nonces.insert(
                AccountPublicKey(bytes(&entry[1][0]).try_into().unwrap()),
                bytes(&entry[1][1][1][1]),
            );
        }
    }
    transactions::execute_signed(&s, t, &c, &config)
}
fn run<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    row: &J,
    t: &mut Trees<A, C>,
) -> Result<Receipt, Error<A, C>> {
    execute(
        row,
        t,
        Default::default(),
        1_000_000,
        Default::default(),
        Default::default(),
    )
}
fn native(reason: J) -> J {
    json!({"kind":"native","class":"error","reason":reason})
}
fn native_term(t: &ae_vm_fate::native::Term) -> J {
    use ae_vm_fate::native::Term;
    match t {
        Term::Atom(a) => atom(a),
        Term::Integer(n) => int(n),
        Term::Binary(b) => bin(b),
        Term::Tuple(v) => tuple(v.iter().map(native_term).collect()),
        Term::List(v) => json!(["list", v.iter().map(native_term).collect::<Vec<_>>()]),
        _ => panic!("unexpected native term {t:?}"),
    }
}
fn contract_failure(e: &contracts::Error) -> J {
    match e {
        contracts::Error::Exception(machine::Exception::Native(s)) => native(atom(s)),
        contracts::Error::NativeException(e) => {
            assert_eq!(e.class, ae_vm_fate::native::Class::Error);
            native(native_term(&e.reason))
        }
        contracts::Error::AuthenticationResultCase(n) => native(tuple(vec![
            atom("case_clause"),
            tuple(vec![atom("ok"), int(n)]),
        ])),
        contracts::Error::Primitive(ae_execution::Error::GeneralizedAccountsUnavailable) => {
            json!({"kind":"ok","value":tuple(vec![atom("error"),atom("generalize_accounts_not_available_at_protocol")])})
        }
        _ => json!({"kind":"ok","value":tuple(vec![atom("error"),atom(contract_reason(e))])}),
    }
}
pub(super) fn failure<A, C>(e: &Error<A, C>) -> J {
    match e {
        Error::Contract(e) => contract_failure(e),
        Error::Aevm(e) => {
            use ae_vm_aevm::{
                outer_call::Fault,
                runtime::{Fault as R, Native},
            };
            match e.as_ref() {
                Fault::Runtime(R::Host(aevm::HostError::Transaction(e))) => contract_failure(e),
                Fault::Runtime(R::Native(Native::BadMatchBinary(b))) => {
                    native(tuple(vec![atom("badmatch"), bin(b)]))
                }
                Fault::Runtime(R::Native(Native::ChainException {
                    class: "error",
                    reason,
                })) => native(atom(reason)),
                _ => panic!("unexpected AEVM native result"),
            }
        }
        _ => super::failure(e),
    }
}
fn check_environment(r: &Receipt, expected: &J) {
    let ids: std::collections::BTreeSet<_> = field(expected, "ga_auth_ids")[1]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| AccountPublicKey(bytes(v).try_into().unwrap()))
        .collect();
    assert_eq!(r.context.ga_authenticated, ids);
    assert_eq!(
        *field(expected, "ga_tx_hash"),
        r.context
            .ga_transaction_hash
            .map(|h| bin(h.0))
            .unwrap_or_else(|| atom("undefined"))
    );
    assert_eq!(
        *field(expected, "ga_tx"),
        r.context
            .ga_transaction
            .as_ref()
            .map(|t| bin(t.encode()))
            .unwrap_or_else(|| atom("undefined"))
    );
    for entry in field(expected, "ga_nonces")[1].as_array().unwrap() {
        let key = AccountPublicKey(bytes(&entry[1][0]).try_into().unwrap());
        let actual = r
            .context
            .ga_nonces
            .get(&key)
            .map(|n| tuple(vec![atom("value"), bin(n)]))
            .unwrap_or_else(|| atom("none"));
        assert_eq!(entry[1][1], actual);
    }
}
#[test]
fn generalized_meta_authentication_inner_callbacks_and_historical_wrappers_match_original() {
    let rows = META["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 2152);
    let mut counts = [0; 3];
    for row in rows {
        let mut t = trees(&row["before"]);
        let roots = (
            t.roots(),
            t.names.cache_root_hash(),
            t.oracles.cache_root_hash(),
        );
        match run(row, &mut t) {
            Ok(r) => {
                counts[0] += 1;
                assert_eq!(row["result"]["kind"], "ok", "{row}");
                let v = &row["result"]["value"];
                assert_eq!(v[1][0], atom("ok"), "{row}");
                check(&t, &r, &v[1][1]);
                check_used_gas(&r.gas, &v[1][2]);
                assert_eq!(
                    transactions::accounting::used_gas(
                        signed(row).transaction().view(),
                        &50.into(),
                        tx_context(row).protocol,
                        &t
                    )
                    .unwrap(),
                    magnitude(&r.gas)
                );
                check_environment(&r, &v[1][1]);
                assert_eq!(
                    r.transaction_hash,
                    Some(
                        SignedTransaction::<Transaction>::decode(&bytes(&row["signed"]))
                            .unwrap()
                            .hash()
                    )
                );
            }
            Err(e) => {
                let actual = failure(&e);
                counts[if actual["kind"] == "native" { 2 } else { 1 }] += 1;
                assert_eq!(
                    actual, row["result"],
                    "{} {} {} {}: {e:?}",
                    row["protocol"], row["abi"], row["operation"], row["mode"]
                );
                assert_eq!(
                    (
                        t.roots(),
                        t.names.cache_root_hash(),
                        t.oracles.cache_root_hash()
                    ),
                    roots
                );
            }
        }
    }
    assert_eq!(counts, [1015, 1084, 53]);
}

fn fixture(abi: u64, op: &str, mode: &str, protocol: u64) -> &'static J {
    META["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["abi"] == abi
                && r["operation"] == op
                && r["mode"] == mode
                && r["protocol"] == protocol
                && r["dry_run"] == false
        })
        .unwrap()
}
#[test]
fn meta_auth_and_inner_backend_failures_never_publish_partial_charges_or_roots() {
    use super::failures::{Control, roots, wrapped};
    use std::rc::Rc;
    let mut prefixes = 0;
    for abi in [1, 3] {
        for (op, mode, protocol) in [
            ("spend", "normal", 6),
            ("spend", "inner_nonce", 6),
            ("spend", "nested_same", 6),
            ("spend", "nested_inner_signature", 6),
            ("spend", "nested_payer", 6),
            ("spend", "sponsored", 6),
            ("preclaim", "normal", 6),
            ("query", "normal", 6),
            ("fate_call", "normal", 6),
            ("aevm_call", "inner_bad_wrapper", 4),
        ] {
            let row = fixture(abi, op, mode, protocol);
            let input = trees(&row["before"]);
            let before = roots(&input);
            let control = Rc::new(Control::default());
            let base = wrapped(&input, &control);
            control.reset();
            run(row, &mut base.clone()).unwrap();
            let counts = [control.reads.get(), control.writes.get()];
            assert!(counts.iter().all(|n| *n > 0));
            for (dim, count) in counts.into_iter().enumerate() {
                for allowed in 0..=count {
                    control.reset();
                    if dim == 0 {
                        control.read_limit.set(Some(allowed))
                    } else {
                        control.write_limit.set(Some(allowed))
                    }
                    let mut t = base.clone();
                    match run(row, &mut t) {
                        Ok(r) => {
                            assert_eq!(allowed, count, "{abi} {op} {mode}");
                            control.reset();
                            check(&t, &r, &row["result"]["value"][1][1]);
                        }
                        Err(e) => {
                            assert!(allowed < count);
                            assert!(
                                format!("{e:?}").contains("injected backend failure"),
                                "{e:?}"
                            );
                            assert_eq!(roots(&t), before);
                        }
                    }
                    control.reset();
                    let mut retry = base.clone();
                    let r = run(row, &mut retry).unwrap();
                    check(&retry, &r, &row["result"]["value"][1][1]);
                    assert_eq!(roots(&base), before);
                    prefixes += 1;
                }
            }
        }
    }
    assert!(prefixes > 500);
}
#[test]
fn meta_vm_and_inner_operational_stops_preserve_authentication_and_allow_retry() {
    use super::failures::roots;
    for abi in [1, 3] {
        for (op, mode) in [
            ("spend", "nested_same"),
            ("spend", "sponsored"),
            ("fate_call", "normal"),
            ("aevm_call", "normal"),
        ] {
            let row = fixture(abi, op, mode, 6);
            let input = trees(&row["before"]);
            let before = roots(&input);
            for dimension in 0..5 {
                let mut t = input.clone();
                let mut a = ae_vm_aevm::execution::Limits::default();
                let mut f = ae_vm_fate::Limits::default();
                let mut s = SignatureLimits::default();
                let mut steps = 1_000_000;
                match dimension {
                    0 => s.max_encoded_bytes = 0,
                    1 => s.max_layers = 0,
                    2 => {
                        a.max_bytes = 0;
                        f.max_bytes = 0
                    }
                    3 => {
                        a.max_work = 0;
                        steps = 0
                    }
                    _ => {
                        a.max_depth = 0;
                        f.max_depth = 0
                    }
                }
                let e = execute(row, &mut t, s, steps, a, f).unwrap_err();
                assert!(
                    format!("{e:?}").contains("ResourceLimit"),
                    "{abi} {op} {mode}: {e:?}"
                );
                assert_eq!(roots(&t), before);
                let r = run(row, &mut t).unwrap();
                check(&t, &r, &row["result"]["value"][1][1]);
            }
        }
    }
    // Authentication succeeds in one VM before the other VM hits its local
    // limit. This must abort the enclosing meta, not admit a failed inner tx.
    for (abi, op) in [(1, "fate_call"), (3, "aevm_call")] {
        let row = fixture(abi, op, "normal", 6);
        let mut t = trees(&row["before"]);
        let before = roots(&t);
        let mut a = ae_vm_aevm::execution::Limits::default();
        let steps = if abi == 1 {
            0
        } else {
            a.max_work = 0;
            1_000_000
        };
        let e = execute(
            row,
            &mut t,
            Default::default(),
            steps,
            a,
            Default::default(),
        )
        .unwrap_err();
        assert!(format!("{e:?}").contains("ResourceLimit"));
        assert_eq!(roots(&t), before);
        let r = run(row, &mut t).unwrap();
        check(&t, &r, &row["result"]["value"][1][1]);
    }
}
#[test]
fn deep_meta_authorization_executes_iteratively_with_bounded_wrapper_work() {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let mut row = fixture(3, "spend", "normal", 8).clone();
            let original =
                SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
            let Wrapper::GaMeta(template) = original.transaction().layers()[0].wrapper() else {
                panic!()
            };
            let mut tx = Transaction::new(original.transaction().leaf().clone());
            for n in 0..256 {
                let mut m = template.clone();
                m.auth_data = ae_vm_fate::abi::create_calldata(
                    b"authorize",
                    vec![ae_vm_fate::value::Value::Integer(n.into())],
                    Default::default(),
                )
                .unwrap();
                tx = tx.wrap(Wrapper::GaMeta(m), vec![]);
            }
            row["signed"] = bin(SignedTransaction::new(tx, vec![]).encode());
            let mut input = trees(&row["before"]);
            let mut owner = input.accounts.lookup(template.account).unwrap().unwrap();
            owner.earn(&1_000_000_000_000_000_000u64.into());
            input.accounts.enter(&owner).unwrap();
            let before = super::failures::roots(&input);
            let limits = SignatureLimits {
                max_layers: 255,
                ..Default::default()
            };
            let mut limited = input.clone();
            assert!(
                execute(
                    &row,
                    &mut limited,
                    limits,
                    1_000_000,
                    Default::default(),
                    Default::default()
                )
                .is_err()
            );
            assert_eq!(super::failures::roots(&limited), before);
            let r = run(&row, &mut input).unwrap();
            assert_eq!(input.calls.tree().iter().count(), 256);
            assert_eq!(r.call.unwrap().return_type, ae_state::calls::ReturnType::Ok);
            assert!(r.context.ga_authenticated.is_empty());
            assert!(r.context.ga_nonces.is_empty());
            assert_eq!(
                input
                    .accounts
                    .lookup(template.account)
                    .unwrap()
                    .unwrap()
                    .nonce(),
                &5.into()
            );
            let recipient = ae_crypto::Ed25519SecretKey::from_seed(&small(3)).public_key();
            assert_eq!(
                input.accounts.lookup(recipient).unwrap().unwrap().balance(),
                &1_000_000_000_000_007u64.into()
            );
        })
        .unwrap()
        .join()
        .unwrap();
}
