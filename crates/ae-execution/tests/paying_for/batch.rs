use super::*;
use ae_execution::transactions::{accounting, batch as b};
use num_bigint::BigInt;

static BATCH: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_block_batch.json")));
fn configured<T>(row: &J, f: impl FnOnce(&Context, &mut Config<'_>) -> T) -> T {
    let network = NetworkProfile::mainnet();
    let vm = vm();
    let env = environment();
    let order = otp_order::profile(&BATCH["profile"]);
    let overrides = Overrides::default();
    let fate = fate::Config {
        context: &vm,
        order: &order,
        overrides: &overrides,
        environment: &env,
        history: Arc::default(),
        auth_tx: None,
        metering_floor: None,
        diagnostic_profile: ae_vm_fate::diagnostic::Profile::Test,
    };
    let aevm = aevm();
    let mut config = Config {
        fate: Some(&fate),
        aevm: Some(&aevm),
        ..Config::primitives(&network)
    };
    let context = Context::transaction(
        50,
        Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
    );
    f(&context, &mut config)
}
fn txs(row: &J) -> Vec<SignedTransaction<Transaction>> {
    row["signed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| SignedTransaction::decode(&bytes(v)).unwrap())
        .collect()
}
fn options(row: &J) -> b::Options {
    b::Options {
        mode: if row["strict"] == true {
            b::Mode::Strict
        } else {
            b::Mode::SkipInvalid
        },
        signatures: if row["skip"] == true {
            b::Signatures::SkipOuter
        } else {
            b::Signatures::Verify
        },
        tx_events: row["events"] == true,
        gas_limit: (row["limit"][0] == "integer")
            .then(|| row["limit"][1].as_str().unwrap().parse().unwrap()),
        ..Default::default()
    }
}
fn normalize(v: J) -> J {
    match v {
        J::Array(mut a) if a.len() == 2 && a[0] == "map" => {
            let mut items: Vec<J> = a
                .pop()
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .cloned()
                .map(normalize)
                .collect();
            items.sort_by_key(|x| x[0].to_string());
            json!(["map", items])
        }
        J::Array(a) => J::Array(a.into_iter().map(normalize).collect()),
        v => v,
    }
}
fn failure(e: &b::Failure) -> J {
    match e {
        b::Failure::GasLimitExceeded => atom("gas_limit_exceeded"),
        b::Failure::Transaction(e) => {
            let v = ga_meta::failure(e);
            if v["kind"] == "native" {
                tuple(vec![
                    atom(v["class"].as_str().unwrap()),
                    v["reason"].clone(),
                ])
            } else {
                v["value"][1][1].clone()
            }
        }
        e => panic!("unexpected failure {e:?}"),
    }
}
fn event(v: &b::Event) -> J {
    let mut out = super::event(&v.value);
    if !matches!(
        v.value,
        Event::Contract(contracts::Event::Internal(_) | contracts::Event::Aevm(_))
    ) {
        for item in out[1][1][1].as_array_mut().unwrap() {
            if item[0] == atom("tx_hash") {
                item[1] = bin(v.transaction_hash.0);
            }
            if item[0] == atom("type") {
                item[1] = atom(match v.transaction_type {
                    TransactionKind::Spend => "spend_tx",
                    TransactionKind::PayingFor => "paying_for_tx",
                    TransactionKind::ContractCall => "contract_call_tx",
                    TransactionKind::ContractCreate => "contract_create_tx",
                    k => panic!("{k:?}"),
                });
            }
        }
    }
    out
}
#[test]
fn ordered_batches_gas_rejections_events_and_final_state_match_original() {
    let rows = BATCH["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2880);
    let mut accepted = 0;
    let mut rejected = 0;
    let mut crashes = 0;
    for (n, row) in rows.iter().enumerate() {
        let transactions = txs(row);
        let mut trees = trees(&row["before"]);
        let before = trees.roots();
        let result = configured(row, |context, config| {
            b::apply(&transactions, &mut trees, context, config, &options(row))
        });
        assert_eq!(row["result"]["kind"], "ok", "row {n}");
        let expected = &row["result"]["value"];
        match result {
            Ok(r) => {
                assert_eq!(expected[0], "map", "row {n}: {expected}");
                assert_eq!(
                    json!([
                        "list",
                        r.accepted
                            .iter()
                            .map(|(i, _)| bin(transactions[*i].encode()))
                            .collect::<Vec<_>>()
                    ]),
                    *field(expected, "valid"),
                    "row {n}"
                );
                let invalid: Vec<_> = r
                    .rejected
                    .iter()
                    .map(|(i, e)| {
                        tuple(vec![
                            bin(transactions[*i].encode()),
                            match e {
                                b::Rejection::Crash => {
                                    crashes += 1;
                                    atom("crash")
                                }
                                b::Rejection::Failure(e) => failure(e),
                            },
                        ])
                    })
                    .collect();
                assert_eq!(
                    json!(["list", invalid]),
                    *field(expected, "invalid"),
                    "row {n}"
                );
                assert_eq!(
                    normalize(json!([
                        "list",
                        r.events.iter().map(event).collect::<Vec<_>>()
                    ])),
                    normalize(field(expected, "events").clone()),
                    "row {n}"
                );
                let receipt = Receipt {
                    transaction_hash: None,
                    gas: 0.into(),
                    call: None,
                    events: vec![],
                    context: Context::transaction(50, Protocol::Roma),
                };
                check(&trees, &receipt, field(expected, "state"));
                for (j, (i, _)) in r.accepted.iter().enumerate() {
                    let p = Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap();
                    let gas = accounting::used_gas(
                        transactions[*i].transaction().view(),
                        &50.into(),
                        p,
                        &trees,
                    )
                    .unwrap();
                    let gas = Unsigned::from_field_bytes(&gas.to_bytes_be().1).unwrap();
                    check_used_gas(&gas, &field(expected, "gas")[1][j]);
                    let fee = accounting::deep_fee(transactions[*i].transaction().view(), &trees)
                        .unwrap();
                    let fee = Unsigned::from_field_bytes(&fee.to_bytes_be().1).unwrap();
                    check_used_gas(&fee, &field(expected, "fees")[1][j]);
                }
                if let Some(gas) = &r.remaining_gas {
                    assert!(gas >= &BigInt::ZERO);
                }
                accepted += r.accepted.len();
                rejected += r.rejected.len();
            }
            Err(e) => {
                assert_eq!(
                    tuple(vec![atom("error"), failure(&e.failure)]),
                    *expected,
                    "row {n}"
                );
                assert_eq!(trees.roots(), before, "row {n}");
            }
        }
    }
    eprintln!("batches: accepted {accepted}, rejected {rejected}, crashes {crashes}");
    assert!(accepted > 1000 && rejected > 1000 && crashes > 0);
}

fn row(scenario: &str) -> &'static J {
    BATCH["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["protocol"] == 6
                && r["scenario"] == scenario
                && r["strict"] == true
                && r["skip"] == false
                && r["gas_mode"] == "large"
                && r["events"] == true
        })
        .unwrap()
}

#[test]
fn strict_micro_batches_include_final_deep_fees_and_publish_atomically() {
    let rows = BATCH["micro_rows"].as_array().unwrap();
    assert_eq!(rows.len(), 72);
    let mut success = 0;
    for expected in rows {
        let row = BATCH["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| {
                r["protocol"] == expected["protocol"] && r["scenario"] == expected["scenario"]
            })
            .unwrap();
        let signed = txs(row);
        let mut t = trees(&row["before"]);
        let before = failures::roots(&t);
        let result = configured(row, |c, conf| {
            b::apply_micro(&signed, &mut t, c, conf, 6_000_000.into(), 123.into())
        });
        assert_eq!(expected["result"]["kind"], "ok");
        let expected = &expected["result"]["value"];
        match result {
            Ok(r) => {
                success += 1;
                assert_eq!(int(&r.fees), *field(expected, "fees"));
                assert_eq!(
                    normalize(json!([
                        "list",
                        r.transactions.events.iter().map(event).collect::<Vec<_>>()
                    ])),
                    normalize(field(expected, "events").clone())
                );
                let receipt = Receipt {
                    transaction_hash: None,
                    gas: 0.into(),
                    call: None,
                    events: vec![],
                    context: Context::transaction(50, Protocol::Roma),
                };
                check(&t, &receipt, field(expected, "state"));
                assert_eq!(r.transactions.accepted.len(), signed.len());
            }
            Err(e) => {
                assert_eq!(tuple(vec![atom("error"), failure(&e.failure)]), *expected);
                assert_eq!(failures::roots(&t), before);
            }
        }
    }
    eprintln!("successful strict micro batches: {success}");
    assert!(success > 10);
}

#[test]
fn batch_signature_vm_and_storage_stops_rollback_accepted_prefixes() {
    use std::rc::Rc;
    for scenario in ["fate_call", "aevm_call", "meta"] {
        let row = row(scenario);
        let input = trees(&row["before"]);
        let mut signed = txs(row);
        // A valid independent first transfer must also roll back if a later
        // transaction fails because the local VM or signature budget runs out.
        let prefix = txs(self::row("meta")).pop().unwrap();
        signed.insert(0, prefix);
        signed.truncate(2);
        for stop in 0..3 {
            let mut t = input.clone();
            let before = failures::roots(&t);
            let error = configured(row, |c, conf| {
                match stop {
                    0 => {
                        conf.maximum_fate_steps = 0;
                        conf.aevm_limits.max_work = 0;
                    }
                    1 => {
                        conf.fate = None;
                        conf.aevm = None;
                    }
                    _ => {
                        conf.signature_limits.max_encoded_bytes =
                            signed[0].transaction().view().wire_size()
                    }
                }
                b::apply(
                    &signed,
                    &mut t,
                    c,
                    conf,
                    &b::Options {
                        gas_limit: Some(6_000_000.into()),
                        ..Default::default()
                    },
                )
            })
            .unwrap_err();
            assert_eq!(error.index, 1, "{scenario} {stop}: {error:?}");
            assert!(error.failure.is_operational(), "{error:?}");
            assert_eq!(failures::roots(&t), before);
        }
        let signed = txs(row);
        let control = Rc::new(failures::Control::default());
        let base = failures::wrapped(&input, &control);
        control.reset();
        let mut good = base.clone();
        configured(row, |c, conf| {
            b::apply_micro(&signed, &mut good, c, conf, 6_000_000.into(), 123.into())
        })
        .unwrap();
        let counts = [control.reads.get(), control.writes.get()];
        for (dimension, count) in counts.into_iter().enumerate() {
            for permitted in 0..count {
                control.reset();
                if dimension == 0 {
                    control.read_limit.set(Some(permitted));
                } else {
                    control.write_limit.set(Some(permitted));
                }
                let mut t = base.clone();
                let error = configured(row, |c, conf| {
                    b::apply_micro(&signed, &mut t, c, conf, 6_000_000.into(), 123.into())
                })
                .unwrap_err();
                assert!(
                    error.failure.is_operational(),
                    "{scenario} {dimension} {permitted}: {error:?}"
                );
                assert_eq!(failures::roots(&t), failures::roots(&base));
            }
        }
        control.reset();
        let mut retry = base.clone();
        configured(row, |c, conf| {
            b::apply_micro(&signed, &mut retry, c, conf, 6_000_000.into(), 123.into())
        })
        .unwrap();
        assert_eq!(failures::roots(&retry), failures::roots(&good));
    }
}

#[test]
fn dry_run_still_checks_signatures_and_only_explicit_skip_bypasses_outer_signature() {
    let row = row("spends");
    let signed = txs(row);
    let invalid = SignedTransaction::new(signed[0].transaction().clone(), vec![]);
    let mut t = trees(&row["before"]);
    let roots = failures::roots(&t);
    configured(row, |context, conf| {
        let mut c = context.clone();
        c.dry_run = true;
        let r = b::apply(
            std::slice::from_ref(&invalid),
            &mut t,
            &c,
            conf,
            &b::Options::default(),
        )
        .unwrap();
        assert_eq!(r.rejected.len(), 1);
        assert_eq!(failures::roots(&t), roots);
        let r = b::apply(
            std::slice::from_ref(&invalid),
            &mut t,
            &c,
            conf,
            &b::Options {
                signatures: b::Signatures::SkipOuter,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(r.accepted.len(), 1);
    });
    let empty: Vec<SignedTransaction<Transaction>> = vec![];
    configured(row, |context, conf| {
        let r = b::apply(
            &empty,
            &mut t,
            context,
            conf,
            &b::Options::strict_block((-1).into()),
        )
        .unwrap();
        assert_eq!(r.remaining_gas, Some((-1).into()));
        let r = b::apply(
            &signed[..1],
            &mut trees(&row["before"]),
            context,
            conf,
            &b::Options {
                tx_events: true,
                ..Default::default()
            },
        )
        .unwrap();
        let initial: Vec<_> = r.events.into_iter().rev().collect();
        let r = b::apply(
            &empty,
            &mut t,
            context,
            conf,
            &b::Options {
                tx_events: true,
                initial_events: initial.clone(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(r.events, initial.into_iter().rev().collect::<Vec<_>>());
    });
}
