use super::*;
use ae_execution::{
    Context, aevm_chain,
    contracts::{self, aevm},
};
use ae_state::{
    accounts::Accounts,
    calls::{Calls, ReturnType},
    contracts::Contracts,
};
use ae_stratum::payout;
use ae_trie::{MemoryStore, Nibbles, NodeReader, Trie, Value as TrieValue};
use ae_tx::{CanonicalTransaction, ContractTransaction, SignedTransaction};
use ae_types::{AccountPublicKey, Unsigned};
use ae_vm_aevm::{execution::Limits, gas::FloatProfile, store::OrderProfile};
use num_bigint::{BigInt, Sign};
use serde_json::{Value as J, json};
use std::sync::LazyLock;

static DATA: LazyLock<J> = LazyLock::new(|| {
    let p: J = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_stratum_payout_execution.json"
    )))
    .unwrap();
    expand(&p["root"], p["values"].as_array().unwrap())
});
fn expand(v: &J, pool: &[J]) -> J {
    if let Some(i) = v.get("$ref") {
        return expand(&pool[i.as_u64().unwrap() as usize], pool);
    }
    match v {
        J::Array(a) => J::Array(a.iter().map(|v| expand(v, pool)).collect()),
        J::Object(m) => J::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), expand(v, pool)))
                .collect(),
        ),
        _ => v.clone(),
    }
}
fn field<'a>(v: &'a J, k: &str) -> &'a J {
    &v[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom(k))
        .unwrap()[1]
}
fn atom(s: &str) -> J {
    json!(["atom", s])
}
fn bin(v: impl AsRef<[u8]>) -> J {
    json!(["binary", hex::encode(v)])
}
fn int(v: impl ToString) -> J {
    json!(["integer", v.to_string()])
}
fn tuple(v: Vec<J>) -> J {
    json!(["tuple", v])
}
fn list(v: Vec<J>) -> J {
    json!(["list", v])
}
fn bytes(v: &J) -> Vec<u8> {
    assert_eq!(v[0], "binary");
    hex::decode(v[1].as_str().unwrap()).unwrap()
}
fn big(v: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, v.as_bytes())
}
fn number(v: &str) -> BigInt {
    v.parse().unwrap()
}
fn unsigned(v: &str) -> Unsigned {
    Unsigned::from_field_bytes(&number(v).to_bytes_be().1).unwrap()
}
fn protocol(row: &J) -> Protocol {
    Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap()
}
fn context(row: &J) -> Context {
    Context::transaction(50, protocol(row))
}
// Cumulative cloning/work and list-depth budgets for the 1,000-recipient corpus.
// These operational limits do not alter transaction gas or block acceptance.
fn corpus_limits() -> Limits {
    Limits {
        max_work: 100_000_000,
        max_bytes: 2 << 30,
        max_depth: 2048,
        ..Default::default()
    }
}
fn vm_config(network: &[u8]) -> aevm::Config {
    aevm::Config {
        environment: aevm::Environment {
            genesis_height: 0,
            key_hash: [0; 32],
            history: Arc::default(),
            beneficiary: AccountPublicKey(key(5)),
            difficulty: 7.into(),
            timestamp_ms: 12345.into(),
            block_gas_limit: 6_000_000.into(),
            auth_tx_hash: None,
        },
        network_id: Arc::from(network),
        event_context: aevm_chain::EventContext::None,
        float_profile: FloatProfile::Otp26Word64,
        store_order: OrderProfile::Otp26Word64Crc32c,
    }
}
fn trie(v: &J) -> Trie<MemoryStore> {
    let mut t = Trie::new(MemoryStore::default());
    for p in v[1].as_array().unwrap() {
        t.put(
            &Nibbles::from_bytes(&bytes(&p[1][0])),
            TrieValue::Bytes(bytes(&p[1][1])),
        )
        .unwrap();
    }
    t
}
fn from_snapshot(v: &J) -> Trees {
    let a = trie(field(v, "accounts"));
    let c = trie(field(v, "contracts"));
    let calls = trie(field(v, "calls"));
    let t = Trees {
        accounts: Accounts::from_root(a.root_hash(), a.into_store()).unwrap(),
        contracts: Contracts::from_root(c.root_hash(), c.into_store()).unwrap(),
        calls: Calls::from_root(calls.root_hash(), calls.into_store()).unwrap(),
        names: Default::default(),
        oracles: Default::default(),
        channels: Default::default(),
    };
    assert_eq!(bin(t.roots().hash().0), *field(v, "root"));
    t
}
fn pairs<S: NodeReader>(t: &Trie<S>) -> J {
    list(
        t.iter()
            .map(|p| {
                let (k, v) = p.unwrap();
                let TrieValue::Bytes(v) = v else { panic!() };
                let k: Vec<_> = k
                    .as_slice()
                    .chunks_exact(2)
                    .map(|p| p[0] * 16 + p[1])
                    .collect();
                tuple(vec![bin(k), bin(v)])
            })
            .collect(),
    )
}
fn event(e: &contracts::Event) -> J {
    let (name, info) = match e {
        contracts::Event::Delta {
            account,
            amount,
            debit,
            info,
        } => (
            tuple(vec![
                atom("delta"),
                tuple(vec![
                    bin(account.0),
                    int(if *debit { -big(amount) } else { big(amount) }),
                ]),
            ]),
            info.as_bytes(),
        ),
        contracts::Event::Aevm(e) => {
            assert_eq!(e.context, aevm_chain::EventContext::None);
            match &e.event {
                ae_execution::Event::ContractDelta {
                    account,
                    amount,
                    debit,
                    info,
                } => (
                    tuple(vec![
                        atom("delta"),
                        tuple(vec![
                            bin(account.0),
                            int(if *debit { -big(amount) } else { big(amount) }),
                        ]),
                    ]),
                    info.as_bytes(),
                ),
                ae_execution::Event::Spend { from, to, amount } => (
                    tuple(vec![
                        atom("spend"),
                        tuple(vec![bin(from.0), bin(to.0), int(big(amount))]),
                    ]),
                    b"Spend.amount".as_slice(),
                ),
                ae_execution::Event::FeeDebit { account, amount } => (
                    tuple(vec![
                        atom("delta"),
                        tuple(vec![bin(account.0), int(-big(amount))]),
                    ]),
                    b"Chain.fee".as_slice(),
                ),
                other => panic!("unexpected payout event {other:?}"),
            }
        }
        other => panic!("unexpected payout receipt {other:?}"),
    };
    tuple(vec![
        name,
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
fn logical(v: J) -> J {
    if let Some(a) = v.as_array() {
        let mut a: Vec<_> = a.iter().cloned().map(logical).collect();
        if a.len() == 2 && a[0] == "map" {
            a[1].as_array_mut()
                .unwrap()
                .sort_by_key(ToString::to_string);
        }
        J::Array(a)
    } else {
        v
    }
}
fn assert_state(t: &Trees, r: &contracts::Receipt, row: &J) {
    assert_eq!(row["result"][1][0], atom("ok"));
    let expected = &row["result"][1][1];
    assert_eq!(
        bin(t.roots().hash().0),
        *field(expected, "root"),
        "{} {}",
        row["protocol"],
        row["case_name"]
    );
    assert_eq!(pairs(t.accounts.tree()), *field(expected, "accounts"));
    assert_eq!(pairs(t.contracts.tree()), *field(expected, "contracts"));
    assert_eq!(pairs(t.calls.tree()), *field(expected, "calls"));
    assert_eq!(
        logical(list(r.events.iter().map(event).collect())),
        logical(field(expected, "events").clone()),
        "events {} {}",
        row["protocol"],
        row["case_name"]
    );
}
fn transfers(row: &J) -> ae_stratum::rewards::Amounts {
    row["transfers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            (
                ae_codec::api::encode(
                    ae_codec::api::ApiType::AccountPubkey,
                    &hex::decode(v[0].as_str().unwrap()).unwrap(),
                )
                .into_bytes(),
                number(v[1].as_str().unwrap()),
            )
        })
        .collect()
}
fn environment(row: &J) -> payout::Environment {
    payout::Environment {
        caller: secret().public_key(),
        contract: ContractPublicKey(
            hex::decode(DATA["contract"].as_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap(),
        ),
        account_nonce: 10.into(),
        minimum_miner_gas_price: 1_000_000.into(),
        height: 50,
        protocol: protocol(row),
    }
}
fn prepared_payment(row: &J) -> core::Payment {
    let p = &row["prepared"];
    ae_stratum::rewards::Payment {
        id: (100, 0),
        total: number(p["gross"].as_str().unwrap()),
        relative: p["relative"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| {
                (
                    ae_codec::api::encode(
                        ae_codec::api::ApiType::AccountPubkey,
                        &hex::decode(v[0].as_str().unwrap()).unwrap(),
                    )
                    .into_bytes(),
                    Number::Float(u64::from_be_bytes(
                        hex::decode(v[1].as_str().unwrap())
                            .unwrap()
                            .try_into()
                            .unwrap(),
                    )),
                )
            })
            .collect(),
    }
    .into()
}
fn find(p: u32, name: &str) -> &'static J {
    DATA["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == p && r["case_name"] == name)
        .unwrap()
}

#[test]
fn original_deployed_aevm_payout_code_creates_the_same_state_and_executes_all_historical_profiles()
{
    let row = &DATA["create"];
    let mut trees = from_snapshot(&row["before"]);
    let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
    let r = aevm::process_create(
        &tx,
        &mut trees,
        &context(row),
        &vm_config(b"ae_mainnet"),
        corpus_limits(),
    )
    .unwrap();
    assert_eq!(r.call.return_type, ReturnType::Ok);
    assert_state(&trees, &r, row);
    let mut counts = [0, 0];
    for row in DATA["runs"].as_array().unwrap() {
        let mut trees = from_snapshot(&row["before"]);
        let tx = SignedTransaction::<ContractTransaction>::decode(&bytes(&row["signed"])).unwrap();
        assert_eq!(bin(tx.hash().0), row["hash"]);
        let r = aevm::execute_signed_call(
            &tx,
            &mut trees,
            &NetworkProfile::mainnet(),
            &context(row),
            &vm_config(b"ae_mainnet"),
            corpus_limits(),
        )
        .unwrap_or_else(|e| panic!("{} {}: {e:?}", row["protocol"], row["case_name"]));
        assert_eq!(r.transaction_hash, Some(tx.hash()));
        counts[usize::from(r.call.return_type != ReturnType::Ok)] += 1;
        assert_state(&trees, &r, row);
    }
    assert_eq!(counts, [96, 48]);
}

#[test]
fn native_payout_preparation_signatures_and_batch_gas_failures_reach_the_real_vm() {
    let mut prepared_count = 0;
    let mut calls = 0;
    for row in DATA["runs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["native_budget"] == true)
    {
        let env = environment(row);
        let expected = bytes(&row["transaction"]);
        let call = payout::call(&transfers(row), &env, Default::default()).unwrap();
        assert_eq!(call.transaction().encode(), expected);
        calls += 1;
        if !row["prepared"].is_null() {
            let p = payout::prepare(
                &prepared_payment(row),
                &env,
                Some(&unsigned("1000000000000000000000000000")),
                b"ae_mainnet",
                Default::default(),
            )
            .unwrap();
            assert_eq!(p.net(), &number(row["prepared"]["net"].as_str().unwrap()));
            assert_eq!(p.call().transaction().encode(), expected);
            let signed = p
                .attach_signature(secret().sign(p.signing_bytes()))
                .unwrap();
            assert_eq!(signed.encode(), bytes(&row["signed"]));
            prepared_count += 1;
        }
    }
    assert_eq!((calls, prepared_count), (54, 12));
    for p in 3..=8 {
        for case in ["thirty_three", "batch"] {
            let row = find(p, case);
            let mut t = from_snapshot(&row["before"]);
            let before = t.clone();
            let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
            let r = aevm::process_call(
                &tx,
                &mut t,
                &context(row),
                &vm_config(b"ae_mainnet"),
                corpus_limits(),
            )
            .unwrap();
            assert_eq!(r.call.return_type, ReturnType::Error);
            assert_eq!(&r.call.gas_used, tx.gas());
            assert_eq!(t.contracts.root_hash(), before.contracts.root_hash());
            for v in row["transfers"].as_array().unwrap() {
                let key = AccountPublicKey(
                    hex::decode(v[0].as_str().unwrap())
                        .unwrap()
                        .try_into()
                        .unwrap(),
                );
                assert_eq!(
                    t.accounts.lookup(key).unwrap(),
                    before.accounts.lookup(key).unwrap()
                );
            }
            assert_state(&t, &r, row);
        }
    }
}

#[test]
fn live_payout_actor_signs_and_submits_transactions_that_execute_the_deployed_contract() {
    for p in [3, 6, 8] {
        for case in ["prepared_one", "prepared_two"] {
            let row = find(p, case);
            let input = from_snapshot(&row["before"]);
            let env = environment(row);
            let temp = Temp::new();
            let profile = NetworkProfile::custom(
                b"ae_payout_contract_local".to_vec(),
                vec![Activation {
                    height: 0,
                    protocol: protocol(row),
                }],
            )
            .unwrap();
            let Header::Key(mut h) = genesis::header(Network::Uat).unwrap() else {
                panic!()
            };
            h.common.version = p;
            h.common.state_hash = input.roots().hash().0;
            if p >= 2 {
                h.info = Some([0; 4]);
            }
            let id = Identity::new(
                &profile,
                Header::Key(h.clone()).hash().unwrap(),
                Hash256::ZERO,
            );
            let db = Arc::new(Database::open(&temp.0, id).unwrap());
            db.commit(
                ledger::bootstrap(
                    &db.view().unwrap(),
                    &profile,
                    &h,
                    &input,
                    Default::default(),
                )
                .unwrap(),
            )
            .unwrap();
            seed(&db, prepared_payment(row));
            let s = signer();
            let mut config = ae_storage::pool::Config::default();
            config.policy.minimum_miner_gas_price = 1_000_000.into();
            let pool = txpool::Worker::spawn(
                db.clone(),
                profile.clone(),
                config,
                txpool::Settings {
                    origins_interval: None,
                    ..Default::default()
                },
            )
            .unwrap();
            let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
            let mut i = Inputs::new(
                durable::Config {
                    caller: env.caller,
                    contract: env.contract,
                    delay: 2,
                },
                s.client(),
                pool.client(),
            );
            // This compatibility case intentionally retains the original recipe.
            i.generation = payout::Generation::Native;
            let mut a =
                Worker::start(&mut w, db.clone(), Default::default(), i, Arc::new(Wall)).unwrap();
            a.request_check();
            let record = accepted(&mut a, &mut w);
            let signed = SignedTransaction::<ContractTransaction>::decode(
                &record.signed.as_ref().unwrap().transaction,
            )
            .unwrap();
            assert_eq!(signed.transaction().encode(), bytes(&row["transaction"]));
            let mut t = input;
            let r = aevm::execute_signed_call(
                &signed,
                &mut t,
                &profile,
                &context(row),
                &vm_config(profile.network_id()),
                Limits::default(),
            )
            .unwrap();
            assert_eq!(r.call.return_type, ReturnType::Ok);
            assert_state(&t, &r, row);
            a.finish(&mut w).unwrap();
            assert!(a.report().unsettled.is_none());
            drop(a);
            drop(w);
            assert!(pool.shutdown().is_ok());
            s.retire();
        }
    }
}

#[test]
fn large_payout_resource_stops_and_wrong_network_leave_all_input_roots_retryable() {
    let row = find(6, "batch");
    let mut trees = from_snapshot(&row["before"]);
    let before = trees.roots();
    let signed = SignedTransaction::<ContractTransaction>::decode(&bytes(&row["signed"])).unwrap();
    let cfg = vm_config(b"ae_mainnet");
    let failure = aevm::execute_signed_call(
        &signed,
        &mut trees,
        &NetworkProfile::mainnet(),
        &context(row),
        &cfg,
        Limits::default(),
    )
    .unwrap_err();
    assert!(matches!(
        failure,
        ae_vm_aevm::outer_call::Fault::Runtime(ae_vm_aevm::runtime::Fault::Stop(
            ae_vm_aevm::execution::Stop::ResourceLimit
        ))
    ));
    assert_eq!(trees.roots(), before);
    let wrong = NetworkProfile::custom(
        b"ae_wrong_payout_domain".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::Ceres,
        }],
    )
    .unwrap();
    assert!(
        aevm::execute_signed_call(
            &signed,
            &mut trees,
            &wrong,
            &context(row),
            &cfg,
            corpus_limits()
        )
        .is_err()
    );
    assert_eq!(trees.roots(), before);
    let receipt = aevm::execute_signed_call(
        &signed,
        &mut trees,
        &NetworkProfile::mainnet(),
        &context(row),
        &cfg,
        corpus_limits(),
    )
    .unwrap();
    assert_state(&trees, &receipt, row);
    let after = trees.roots();
    assert!(
        aevm::execute_signed_call(
            &signed,
            &mut trees,
            &NetworkProfile::mainnet(),
            &context(row),
            &cfg,
            corpus_limits()
        )
        .is_err()
    );
    assert_eq!(trees.roots(), after);
}

#[test]
fn payout_completion_total_is_strict_and_matches_the_captured_successful_calls() {
    for p in 3..=8 {
        for name in [
            "empty",
            "one",
            "two",
            "three",
            "ten",
            "prepared_one",
            "prepared_two",
        ] {
            let row = find(p, name);
            let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
            let after = from_snapshot(&row["result"][1][1]);
            let env = environment(row);
            let id = ae_state::calls::call_id(env.caller.0, tx.nonce(), env.contract.0).0;
            let call = after.calls.lookup(&env.contract.0, id).unwrap().unwrap();
            assert!(payout::confirms_total(tx.amount(), &call.return_value));
            assert!(!payout::confirms_total(
                &tx.amount().add(&1.into()),
                &call.return_value
            ));
            let mut trailing = call.return_value.clone();
            trailing.push(0);
            assert!(!payout::confirms_total(tx.amount(), &trailing));
            assert!(!payout::confirms_total(
                tx.amount(),
                &call.return_value[..31]
            ));
        }
    }
    assert!(!payout::confirms_total(
        &Unsigned::from_field_bytes(&[1; 33]).unwrap(),
        &[0; 64]
    ));
}

mod gas;
