use super::*;
use ae_chain::execution as ex;
use ae_execution::{
    contracts::{self, aevm, fate},
    transactions::{self, Event},
};
use ae_state::{
    Trees, accounts::Accounts, calls::Calls, contracts::Contracts, names::Names, oracles::Oracles,
};
use ae_trie::{MemoryStore, NodeReader, NodeWriter, Trie};
use ae_tx::TransactionKind;
use ae_types::{AccountPublicKey, Unsigned};
use ae_vm_fate::{
    machine,
    maps::{listing::Overrides, order::WordSize},
};
use std::sync::Arc;
fn configured<T>(row: &J, f: impl FnOnce(&mut transactions::Config<'_>) -> T) -> T {
    let network = NetworkProfile::custom(
        b"ae_block_execution".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
        }],
    )
    .unwrap();
    let mut vm = vm();
    vm.in_auth = true;
    vm.on_chain = false;
    vm.reserved_gas = 123.into();
    let mut env = environment();
    env.auth_tx_hash = Some(vec![12; 32]);
    let order = otp_order::profile(&DATA["profile"]);
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
    let mut aevm = aevm();
    aevm.environment.auth_tx_hash = Some(vec![12; 32]);
    let mut config = transactions::Config {
        fate: Some(&fate),
        aevm: Some(&aevm),
        ..transactions::Config::primitives(&network)
    };
    f(&mut config)
}
fn with_parent<T>(row: &J, f: impl FnOnce(&mut ex::Parent<'_>) -> T) -> T {
    let previous = Header::decode(&bytes(&row["previous"]), HeaderMode::ProofOfWork).unwrap();
    let Header::Key(key) =
        Header::decode(&bytes(&row["previous_key"]), HeaderMode::ProofOfWork).unwrap()
    else {
        panic!()
    };
    let Header::Key(older) =
        Header::decode(&bytes(&row["older_key"]), HeaderMode::ProofOfWork).unwrap()
    else {
        panic!()
    };
    f(&mut ex::Parent {
        headers: ae_consensus::headers::Context {
            previous: &previous,
            previous_key: &key,
            recent_keys: &[],
        },
        previous_generation: ex::PreviousGeneration::Found(&older),
        fees: 123.into(),
        reported_fraud: row["mode"] == "double_report",
    })
}
fn block(row: &J) -> ae_chain::block::MicroBlock {
    let Block::Micro(m) = Block::decode(&bytes(&row["block"]), DecodeConfig::default()).unwrap()
    else {
        panic!()
    };
    m
}
fn rules() -> ex::Rules {
    ex::Rules {
        now_ms: 20000,
        micro_cycle_ms: 3000,
        configured_gas_limit: 6_000_000.into(),
        body_limits: Default::default(),
    }
}
fn run<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    row: &J,
    trees: &mut Trees<A, C>,
) -> Result<ex::ExecutedMicro, ex::Error<A, C>> {
    configured(row, |config| {
        let owned = transactions::owned::Owned::capture(config).unwrap();
        owned.with_config(|config| {
            with_parent(row, |parent| {
                ex::pow_micro(&block(row), trees, parent, config, &rules())
            })
        })
    })
}
fn check<A: NodeReader, C: NodeReader>(trees: &Trees<A, C>, r: &ex::ExecutedMicro, row: &J) {
    assert_eq!(row["result"]["kind"], "ok");
    let v = &row["result"]["value"];
    let state = field(v, "state");
    assert_eq!(bin(trees.roots().hash().0), *field(state, "root"));
    assert_eq!(r.state_hash(), trees.roots().hash());
    assert_eq!(r.hash(), Header::Micro(block(row).header).hash().unwrap());
    assert_eq!(int(r.fees()), *field(v, "fees"));
    assert_eq!(
        atom(if r.reported_fraud() { "true" } else { "false" }),
        *field(v, "fraud")
    );
    assert_eq!(
        normalize(json!([
            "list",
            r.events().iter().map(event).collect::<Vec<_>>()
        ])),
        normalize(field(v, "events").clone())
    );
    for (name, t) in [
        ("contracts", trees.contracts.tree()),
        ("calls", trees.calls.tree()),
        ("channels", trees.channels.tree()),
        ("names", trees.names.tree()),
        ("name_cache", trees.names.cache()),
        ("oracles", trees.oracles.tree()),
        ("oracle_cache", trees.oracles.cache()),
    ] {
        assert_eq!(entries(t), *field(state, name), "{name}");
    }
    assert_eq!(entries(trees.accounts.tree()), *field(state, "accounts"));
    if row["kind"] == "fate_env"
        || row["kind"].as_str().unwrap().starts_with("aevm_") && row["kind"] != "aevm_call"
    {
        let mut count = 0;
        for pair in trees.calls.tree().iter() {
            let (_, ae_trie::Value::Bytes(bytes)) = pair.unwrap() else {
                panic!()
            };
            let call = ae_state::calls::Call::deserialize([0; 32], &bytes).unwrap();
            assert_eq!(
                call.return_type,
                ae_state::calls::ReturnType::Ok,
                "{} {}",
                row["protocol"],
                row["kind"]
            );
            count += 1;
        }
        assert_eq!(count, 1);
    }
}
fn failure(e: &ex::Error, row: &J) -> J {
    assert!(!e.is_operational(), "{e:?}");
    let reason = match e {
        ex::Error::Admission(e) => {
            return match e {
                PowError::Body(b) => result(Err(PowError::Body(b.clone()))),
                PowError::Header(h) => result(Err(PowError::Header(h.clone()))),
            };
        }
        ex::Error::Header(ae_consensus::headers::Error::MicroSignature) => {
            return json!({"kind":"ok","value":tuple(vec![atom("error"),atom("signature_verification_failed")])});
        }
        ex::Error::WrongProofKey => {
            return json!({"kind":"ok","value":tuple(vec![atom("error"),atom("wrong_pubkey_in_pof")])});
        }
        ex::Error::Transactions(_) => atom("invalid_transactions_in_block"),
        ex::Error::StateHash { actual, expected } => tuple(vec![
            atom("root_hash_mismatch"),
            bin(actual.0),
            bin(expected.0),
        ]),
        ex::Error::DoubleReportedFraud => tuple(vec![
            atom("double_reported_fraud"),
            bin(Header::Micro(block(row).header).hash().unwrap().0),
        ]),
        e => panic!("unexpected {e:?}"),
    };
    json!({"kind":"native","class":"throw","reason":tuple(vec![atom("aec_chain_state_error"),reason])})
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
fn fixture(protocol: u32, kind: &str, parent: &str, mode: &str) -> &'static J {
    DATA["execution"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["protocol"] == protocol
                && r["kind"] == kind
                && r["parent_kind"] == parent
                && r["mode"] == mode
        })
        .unwrap()
}
#[test]
fn unavailable_context_and_local_execution_limits_preserve_every_root_and_allow_retry() {
    for (protocol, kind) in [
        (6, "spends"),
        (6, "fate_call"),
        (4, "aevm_call"),
        (6, "meta"),
    ] {
        let row = fixture(protocol, kind, "key", "normal");
        let input = trees(&row["before"]);
        let before = roots(&input);
        for mode in 0..5 {
            let mut t = input.clone();
            let result = configured(row, |config| {
                with_parent(row, |parent| {
                    let mut rules = rules();
                    match mode {
                        0 => config.signature_limits.max_encoded_bytes = 0,
                        1 => rules.body_limits.max_signed_bytes = 0,
                        2 => config.consensus = ae_protocol::ConsensusType::Pos,
                        3 => {
                            config.fate = None;
                            config.aevm = None;
                        }
                        _ => {
                            config.maximum_fate_steps = 0;
                            config.aevm_limits.max_work = 0;
                        }
                    }
                    ex::pow_micro(&block(row), &mut t, parent, config, &rules)
                })
            });
            if mode >= 3 && kind == "spends" {
                assert!(result.is_ok());
                continue;
            }
            let e = result.unwrap_err();
            assert!(e.is_operational(), "{e:?}");
            assert_eq!(roots(&t), before);
            let r = run(row, &mut t).unwrap();
            check(&t, &r, row);
        }
    }
    let row = fixture(6, "spends", "key", "proof");
    let input = trees(&row["before"]);
    let before = roots(&input);
    for mode in 0..4 {
        let mut t = input.clone();
        let r = configured(row, |config| {
            with_parent(row, |parent| {
                let mut wrong = parent.headers.previous_key.clone();
                wrong.common.height = 1;
                match mode {
                    0 => parent.previous_generation = ex::PreviousGeneration::Unavailable,
                    1 => parent.previous_generation = ex::PreviousGeneration::Missing,
                    2 => {
                        return ex::pow_micro(
                            &block(row),
                            &mut t,
                            &ex::Parent {
                                headers: ae_consensus::headers::Context {
                                    previous: parent.headers.previous,
                                    previous_key: parent.headers.previous_key,
                                    recent_keys: &[],
                                },
                                previous_generation: ex::PreviousGeneration::Found(&wrong),
                                fees: parent.fees.clone(),
                                reported_fraud: parent.reported_fraud,
                            },
                            config,
                            &rules(),
                        );
                    }
                    _ => t.accounts = Accounts::new(MemoryStore::default()),
                };
                ex::pow_micro(&block(row), &mut t, parent, config, &rules())
            })
        });
        let e = r.unwrap_err();
        assert_eq!(e.is_operational(), mode != 1, "{e:?}");
        if mode != 3 {
            assert_eq!(roots(&t), before);
        } else {
            assert!(t.accounts.root_hash().is_none());
        }
        let mut retry = input.clone();
        let r = run(row, &mut retry).unwrap();
        check(&retry, &r, row);
    }
}
#[test]
fn configured_cycle_and_previous_fraud_status_are_carried_into_micro_execution() {
    let row = fixture(6, "spends", "micro", "normal");
    let mut b = block(row);
    b.header.time = 10000;
    let signature = ae_crypto::Ed25519SecretKey::from_seed(&small(1))
        .sign(&Header::Micro(b.header.clone()).signature_bytes().unwrap());
    b.header.signature = signature.0;
    configured(row, |config| {
        with_parent(row, |parent| {
            parent.reported_fraud = true;
            parent.previous_generation = ex::PreviousGeneration::Unavailable;
            for cycle in [0, 1, 1500, 3000] {
                let mut t = trees(&row["before"]);
                let before = roots(&t);
                let mut rules = rules();
                rules.micro_cycle_ms = cycle;
                let result = ex::pow_micro(&b, &mut t, parent, config, &rules);
                if cycle == 0 {
                    let r = result.unwrap();
                    assert!(r.reported_fraud());
                    assert_eq!(
                        bin(r.state_hash().0),
                        *field(field(&row["result"]["value"], "state"), "root")
                    );
                } else {
                    assert!(matches!(
                        result,
                        Err(ex::Error::Admission(PowError::Header(
                            ae_consensus::headers::Error::MicroInterval
                        )))
                    ));
                    assert_eq!(roots(&t), before);
                }
            }
        })
    });
    let row = fixture(6, "spends", "micro", "proof");
    configured(row, |config| {
        with_parent(row, |parent| {
            parent.previous_generation = ex::PreviousGeneration::Unavailable;
            let mut t = trees(&row["before"]);
            let r = ex::pow_micro(&block(row), &mut t, parent, config, &rules()).unwrap();
            check(&t, &r, row);
        })
    });
}
#[test]
fn complete_pow_micro_execution_matches_original_state_fees_events_and_failure_order() {
    let rows = DATA["execution"].as_array().unwrap();
    assert_eq!(rows.len(), 656);
    let mut successes = 0;
    let mut failures = 0;
    for (i, row) in rows.iter().enumerate() {
        let mut t = trees(&row["before"]);
        let before = roots(&t);
        match run(row, &mut t) {
            Ok(r) => {
                check(&t, &r, row);
                successes += 1;
            }
            Err(e) => {
                assert_eq!(
                    failure(&e, row),
                    row["result"],
                    "row {i}: {} {} {} {} {e:?}",
                    row["protocol"],
                    row["kind"],
                    row["parent_kind"],
                    row["mode"]
                );
                assert_eq!(roots(&t), before);
                failures += 1;
            }
        }
    }
    assert_eq!((successes, failures), (254, 402));
}
#[path = "../../../ae-vm-fate/tests/support/otp_order.rs"]
mod otp_order;
fn term_bytes(v: &J) -> Vec<u8> {
    assert_eq!(v[0], "binary");
    bytes(&v[1])
}
fn int(n: impl ToString) -> J {
    json!(["integer", n.to_string()])
}
fn magnitude(n: &Unsigned) -> num_bigint::BigInt {
    num_bigint::BigInt::from_bytes_be(num_bigint::Sign::Plus, n.as_bytes())
}
fn tree(v: &J) -> Trie {
    let mut t = Trie::default();
    for pair in v[1].as_array().unwrap() {
        t.put(
            &ae_trie::Nibbles::from_bytes(&term_bytes(&pair[1][0])),
            ae_trie::Value::Bytes(term_bytes(&pair[1][1])),
        )
        .unwrap();
    }
    t
}
fn initial(v: &J) -> Trees {
    let a = tree(field(v, "accounts"));
    Trees {
        accounts: Accounts::from_root(a.root_hash(), a.store().clone()).unwrap(),
        contracts: Contracts::new(MemoryStore::default()),
        calls: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    }
}
fn entries<S: NodeReader>(t: &Trie<S>) -> J {
    json!([
        "list",
        t.iter()
            .map(|p| {
                let (k, v) = p.unwrap();
                let ae_trie::Value::Bytes(v) = v else {
                    panic!()
                };
                let k: Vec<_> = k
                    .as_slice()
                    .chunks_exact(2)
                    .map(|p| p[0] * 16 + p[1])
                    .collect();
                tuple(vec![bin(k), bin(v)])
            })
            .collect::<Vec<_>>()
    ])
}
fn primitive_event(e: &Event) -> J {
    if let Event::Contract(ae_execution::contracts::Event::Aevm(e)) = e {
        assert_eq!(e.context, ae_execution::aevm_chain::EventContext::None);
        return primitive_event(&Event::Primitive(e.event.clone()));
    }
    if let Event::Contract(ae_execution::contracts::Event::Internal(e)) = e {
        use ae_execution::fate_chain::TransferEvent;
        let (body, info) = match e {
            TransferEvent::DeploymentCall { info } => (
                tuple(vec![atom("internal_call_tx"), bin(info)]),
                atom("error"),
            ),
            TransferEvent::Delta {
                account,
                debit,
                amount,
            } => (
                tuple(vec![
                    atom("delta"),
                    tuple(vec![
                        bin(account.0),
                        int(if *debit {
                            -magnitude(amount)
                        } else {
                            magnitude(amount)
                        }),
                    ]),
                ]),
                bin("Spend.amount"),
            ),
            TransferEvent::InternalCall(t) => {
                let id = |tag, k| tuple(vec![atom("id"), atom(tag), bin(k)]);
                assert_eq!(t.recipient().tag, ae_types::IdTag::Account);
                let tx = tuple(vec![
                    atom("spend_tx"),
                    id("account", t.sender().0),
                    id("account", t.recipient().value),
                    int(magnitude(t.amount())),
                    int(magnitude(t.fee())),
                    int(magnitude(t.ttl())),
                    int(magnitude(t.nonce())),
                    bin(t.payload()),
                ]);
                (
                    tuple(vec![atom("internal_call_tx"), bin(t.payload())]),
                    tuple(vec![
                        atom("aetx"),
                        atom("spend_tx"),
                        atom("aec_spend_tx"),
                        int(t.wire_size()),
                        tx,
                    ]),
                )
            }
            e => panic!("unexpected internal event: {e:?}"),
        };
        return tuple(vec![
            body,
            json!([
                "map",
                [
                    [atom("info"), info],
                    [atom("type"), atom("non_tx")],
                    [atom("tx_hash"), bin([])]
                ]
            ]),
        ]);
    }
    use ae_execution::Event as P;
    let delta = |k: AccountPublicKey, n: &Unsigned, debit: bool| {
        tuple(vec![
            atom("delta"),
            tuple(vec![
                bin(k.0),
                int(if debit { -magnitude(n) } else { magnitude(n) }),
            ]),
        ])
    };
    let (body, info) = match e {
        Event::Primitive(P::Spend { from, to, amount }) => (
            tuple(vec![
                atom("spend"),
                tuple(vec![bin(from.0), bin(to.0), int(magnitude(amount))]),
            ]),
            "Spend.amount",
        ),
        Event::Primitive(P::ContractDelta {
            account,
            amount,
            debit,
            info,
        }) => (delta(*account, amount, *debit), *info),
        Event::Primitive(P::FeeDebit { account, amount }) => {
            (delta(*account, amount, true), "Chain.fee")
        }
        Event::Primitive(P::NameDebit { account, amount }) => {
            (delta(*account, amount, true), "Name.lock")
        }
        Event::Primitive(P::NameRefund { account, amount }) => {
            (delta(*account, amount, false), "Name.refund")
        }
        Event::Primitive(P::NameFeeLocked { amount }) => {
            (delta(AccountPublicKey::ZERO, amount, false), "Chain.lock")
        }
        Event::Primitive(P::OracleQueryFee { account, amount }) => {
            (delta(*account, amount, false), "Oracle.queryfee")
        }
        Event::Contract(ae_execution::contracts::Event::Delta {
            account,
            amount,
            debit,
            info,
        }) => (delta(*account, amount, *debit), *info),
        _ => panic!("unexpected {e:?}"),
    };
    tuple(vec![
        body,
        json!([
            "map",
            [
                [atom("info"), bin(info)],
                [atom("type"), atom("non_tx")],
                [atom("tx_hash"), bin([])]
            ]
        ]),
    ])
}
fn trees(v: &J) -> Trees {
    let mut t = initial(v);
    let c = tree(field(v, "contracts"));
    let calls = tree(field(v, "calls"));
    let n = tree(field(v, "names"));
    let nc = tree(field(v, "name_cache"));
    let o = tree(field(v, "oracles"));
    let oc = tree(field(v, "oracle_cache"));
    t.contracts = Contracts::from_root(c.root_hash(), c.into_store()).unwrap();
    t.calls = Calls::from_root(calls.root_hash(), calls.into_store()).unwrap();
    t.channels = tree(field(v, "channels")).into();
    t.names = Names::from_roots(
        n.root_hash(),
        nc.root_hash(),
        n.into_store(),
        nc.into_store(),
    )
    .unwrap();
    t.oracles = Oracles::from_roots(
        o.root_hash(),
        oc.root_hash(),
        o.into_store(),
        oc.into_store(),
    )
    .unwrap();
    assert_eq!(bin(t.roots().hash().0), *field(v, "root"));
    t
}
fn vm() -> machine::Context {
    machine::Context {
        current_contract: [0; 32],
        vm_version: ae_vm_fate::types::Version::Three,
        in_auth: false,
        on_chain: true,
        reserved_gas: 0.into(),
        arithmetic_word_size: WordSize::Bits64,
        unicode_profile: ae_vm_fate::unicode::Profile::Otp26Unicode15,
        bls_profile: ae_crypto::bls::Profile::Mcl11976beLlvm64VintArm64,
    }
}
fn small(n: u8) -> [u8; 32] {
    let mut b = [0; 32];
    b[31] = n;
    b
}
fn environment() -> machine::program::remote::context::Environment {
    machine::program::remote::context::Environment {
        auth_tx_hash: None,
        origin: [0; 32],
        gas_price: 1_000_000.into(),
        fee: 1_000_000_000_000u64.into(),
        beneficiary: small(5),
        timestamp: 12345.into(),
        difficulty: 7.into(),
        block_gas_limit: 6_000_000.into(),
        network_id: b"ae_mainnet".to_vec(),
        consensus: machine::program::remote::context::Consensus::Pow,
        key_hash: [0; 32],
    }
}
fn aevm() -> aevm::Config {
    aevm::Config {
        environment: aevm::Environment {
            genesis_height: 0,
            key_hash: [0; 32],
            history: Arc::default(),
            beneficiary: AccountPublicKey(small(5)),
            difficulty: 7.into(),
            timestamp_ms: 12345.into(),
            block_gas_limit: 6_000_000.into(),
            auth_tx_hash: None,
        },
        network_id: Arc::from(b"ae_mainnet".as_slice()),
        event_context: ae_execution::aevm_chain::EventContext::None,
        float_profile: ae_vm_aevm::gas::FloatProfile::Otp26Word64,
        store_order: ae_vm_aevm::store::OrderProfile::Otp26Word64Crc32c,
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
fn event(v: &transactions::batch::Event) -> J {
    let mut out = primitive_event(&v.value);
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

#[path = "storage.rs"]
mod storage;

#[path = "key.rs"]
mod key;
