use super::*;
static GA: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_ga_attach_execution.json")));
fn execute<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    row: &J,
    t: &mut Trees<A, C>,
    steps: usize,
    al: ae_vm_aevm::execution::Limits,
    fl: ae_vm_fate::Limits,
) -> Result<Receipt, Error<A, C>> {
    execute_mode(row, t, steps, al, fl, false)
}
fn execute_mode<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    row: &J,
    t: &mut Trees<A, C>,
    steps: usize,
    al: ae_vm_aevm::execution::Limits,
    fl: ae_vm_fate::Limits,
    raw: bool,
) -> Result<Receipt, Error<A, C>> {
    let network = NetworkProfile::mainnet();
    let vm = vm();
    let env = environment();
    let order = otp_order::profile(&GA["profile"]);
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
        channel_state_atoms: None,
        signature_limits: Default::default(),
        oracle_limits: Default::default(),
        fate_limits: fl,
        maximum_fate_steps: steps,
        aevm_limits: al,
    };
    let s = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
    let mut context = tx_context(row);
    let payer = field(&row["before"], "payer");
    if payer[0] == "binary" {
        context.payer = Some(AccountPublicKey(bytes(payer).try_into().unwrap()));
    }
    assert_eq!(s.encode(), bytes(&row["canonical_signed"]));
    if !raw {
        return transactions::execute_signed(&s, t, &context, &config);
    }
    let ae_tx::TransactionLeaf::GaAttach(g) = s.transaction().leaf() else {
        panic!()
    };
    let mut g1 = ae_tx::GaAttachTransaction::new(
        g.owner(),
        ae_protocol::contracts::Version {
            vm: row["vm"].as_u64().unwrap() as u16,
            abi: row["abi"].as_u64().unwrap() as u16,
        },
        g.code().to_vec(),
        g.auth_fun().to_vec(),
        g.call_data().to_vec(),
    );
    g1.set_nonce(g.nonce().clone());
    g1.set_fee(g.fee().clone());
    g1.set_ttl(g.ttl().clone());
    g1.set_gas(g.gas().clone());
    g1.set_gas_price(g.gas_price().clone());
    let mut tx = Transaction::new(ae_tx::TransactionLeaf::GaAttach(g1));
    for l in s.transaction().layers() {
        tx = tx.wrap(l.wrapper().clone(), l.inner_signatures().to_vec());
    }
    assert_eq!(
        tx.encode(),
        SignedTransaction::<Raw>::decode(&bytes(&row["signed"]))
            .unwrap()
            .transaction()
            .0
    );
    transactions::process(&tx, t, &context, &config)
}
fn run<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    row: &J,
    t: &mut Trees<A, C>,
) -> Result<Receipt, Error<A, C>> {
    execute(row, t, 1_000_000, Default::default(), Default::default())
}
fn native_contract(e: &contracts::Error) -> Option<J> {
    let r = match e {
        contracts::Error::Exception(machine::Exception::Native("function_clause")) => {
            atom("function_clause")
        }
        contracts::Error::IllegalContractVersion(v) => tuple(vec![
            atom("illegal"),
            atom("ct_version"),
            json!(["map", [[atom("abi"), int(v.abi)], [atom("vm"), int(v.vm)]]]),
        ]),
        contracts::Error::NativeException(e) => {
            use ae_vm_fate::native::{Class, Term};
            assert_eq!(e.class, Class::Error);
            assert_eq!(
                e.reason,
                Term::Tuple(vec![Term::Atom("badmatch"), Term::Binary(vec![])])
            );
            tuple(vec![atom("badmatch"), bin([])])
        }
        _ => return None,
    };
    Some(json!({"kind":"native","class":"error","reason":r}))
}
fn failure<A, C>(e: &Error<A, C>) -> J {
    let inner = match e {
        Error::Contract(e) => Some(e.as_ref()),
        Error::Aevm(e) => match e.as_ref() {
            ae_vm_aevm::outer_call::Fault::Runtime(ae_vm_aevm::runtime::Fault::Host(
                aevm::HostError::Transaction(e),
            )) => Some(e),
            _ => None,
        },
        _ => None,
    };
    if let Some(e) = inner {
        if let Some(r) = native_contract(e) {
            return r;
        }
        if matches!(
            e,
            contracts::Error::Primitive(ae_execution::Error::GeneralizedAccountsUnavailable)
        ) {
            return json!({"kind":"ok","value":tuple(vec![atom("error"),atom("generalize_accounts_not_available_at_protocol")])});
        }
    }
    super::failure(e)
}
#[test]
fn ga_attachment_matches_original_admission_both_vms_accounts_calls_and_fee_settlement() {
    let rows = GA["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 2116);
    let mut counts = [0; 3];
    for row in rows {
        let mut t = trees(&row["before"]);
        let before = (
            t.roots(),
            t.names.cache_root_hash(),
            t.oracles.cache_root_hash(),
        );
        match run(row, &mut t) {
            Ok(r) => {
                counts[0] += 1;
                assert_eq!(row["result"]["kind"], "ok", "{row}");
                let v = &row["result"]["value"];
                assert_eq!(
                    v[1][0],
                    atom("ok"),
                    "{} {} {}",
                    row["protocol"],
                    row["abi"],
                    row["mode"]
                );
                check(&t, &r, &v[1][1]);
                check_used_gas(&r.gas, &v[1][2]);
                let s = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
                assert_eq!(
                    transactions::accounting::used_gas(
                        s.transaction().view(),
                        &50.into(),
                        tx_context(row).protocol,
                        &t
                    )
                    .unwrap(),
                    magnitude(&r.gas)
                );
                assert_eq!(r.transaction_hash, Some(s.hash()));
            }
            Err(e) => {
                let actual = failure(&e);
                counts[if actual["kind"] == "native" { 2 } else { 1 }] += 1;
                assert_eq!(
                    actual, row["result"],
                    "{} {} {}: {e:?}",
                    row["protocol"], row["abi"], row["mode"]
                );
                assert_eq!(
                    (
                        t.roots(),
                        t.names.cache_root_hash(),
                        t.oracles.cache_root_hash()
                    ),
                    before
                );
            }
        }
    }
    assert_eq!(counts, [827, 1265, 24]);
}

#[test]
fn raw_ga_attachment_constructor_sizes_and_version_fields_match_original_callbacks() {
    for row in GA["runs"].as_array().unwrap() {
        let mut t = trees(&row["before"]);
        let before = (
            t.roots(),
            t.names.cache_root_hash(),
            t.oracles.cache_root_hash(),
        );
        match execute_mode(
            row,
            &mut t,
            1_000_000,
            Default::default(),
            Default::default(),
            true,
        ) {
            Ok(r) => {
                let v = &row["constructed_result"]["value"];
                assert_eq!(v[1][0], atom("ok"), "{row}");
                check(&t, &r, &v[1][1]);
                check_used_gas(&r.gas, &v[1][2]);
                assert!(r.transaction_hash.is_none());
            }
            Err(e) => {
                assert_eq!(
                    failure(&e),
                    row["constructed_result"],
                    "{} {} {}: {e:?}",
                    row["protocol"],
                    row["abi"],
                    row["mode"]
                );
                assert_eq!(
                    (
                        t.roots(),
                        t.names.cache_root_hash(),
                        t.oracles.cache_root_hash()
                    ),
                    before
                );
            }
        }
    }
}

fn fixture(abi: u64, mode: &str) -> &'static J {
    GA["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["protocol"] == 6 && r["abi"] == abi && r["mode"] == mode && r["dry_run"] == false
        })
        .unwrap()
}
#[test]
fn ga_attachment_backend_failures_never_publish_authorization_fees_or_child_effects() {
    use super::failures::{Control, roots, wrapped};
    use std::rc::Rc;
    let mut prefixes = 0;
    for (abi, mode) in [
        (1, "normal"),
        (1, "nested_store"),
        (1, "parent_revert"),
        (1, "spend"),
        (1, "sponsored"),
        (3, "normal"),
        (3, "nested_create"),
        (3, "nested_revert"),
        (3, "sponsored"),
    ] {
        let row = fixture(abi, mode);
        let input = trees(&row["before"]);
        let before = roots(&input);
        let c = Rc::new(Control::default());
        let base = wrapped(&input, &c);
        c.reset();
        run(row, &mut base.clone()).unwrap();
        let counts = [c.reads.get(), c.writes.get()];
        assert!(counts.iter().all(|n| *n > 0));
        for (dimension, count) in counts.into_iter().enumerate() {
            for allowed in 0..=count {
                c.reset();
                if dimension == 0 {
                    c.read_limit.set(Some(allowed))
                } else {
                    c.write_limit.set(Some(allowed))
                }
                let mut t = base.clone();
                match run(row, &mut t) {
                    Ok(r) => {
                        assert_eq!(allowed, count);
                        c.reset();
                        check(&t, &r, &row["result"]["value"][1][1]);
                    }
                    Err(e) => {
                        assert!(allowed < count);
                        assert!(
                            format!("{e:?}").contains("injected backend failure"),
                            "{abi} {mode}: {e:?}"
                        );
                        assert_eq!(roots(&t), before);
                    }
                }
                c.reset();
                let mut retry = base.clone();
                let r = run(row, &mut retry).unwrap();
                check(&retry, &r, &row["result"]["value"][1][1]);
                assert_eq!(roots(&base), before);
                prefixes += 1;
            }
        }
    }
    assert!(prefixes > 100);
}
#[test]
fn ga_auth_code_and_both_vm_operational_limits_allow_clean_retry() {
    use super::failures::roots;
    for abi in [1, 3] {
        let row = fixture(abi, "normal");
        let input = trees(&row["before"]);
        let before = roots(&input);
        for dim in 0..3 {
            let mut t = input.clone();
            let mut a = ae_vm_aevm::execution::Limits::default();
            let mut f = ae_vm_fate::Limits::default();
            let mut steps = 1_000_000;
            match dim {
                0 => {
                    a.max_bytes = 0;
                    f.max_bytes = 0
                }
                1 => {
                    a.max_work = 0;
                    steps = 0
                }
                _ => {
                    a.max_depth = 0;
                    f.max_depth = 0
                }
            }
            let e = execute(row, &mut t, steps, a, f).unwrap_err();
            assert!(
                format!("{e:?}").contains("ResourceLimit"),
                "{abi} {dim}: {e:?}"
            );
            assert_eq!(roots(&t), before);
            let r = run(row, &mut t).unwrap();
            check(&t, &r, &row["result"]["value"][1][1]);
        }
    }
}
