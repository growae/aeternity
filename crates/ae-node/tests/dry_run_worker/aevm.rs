use super::*;
use ae_execution::{Context, contracts::aevm};
use ae_trie::{MemoryStore, Nibbles, Overlay, Trie, Value as TrieValue};
use ae_tx::{
    CanonicalTransaction, ContractOperation, ContractTransaction, Transaction, TransactionLeaf,
};
use ae_types::AccountPublicKey;
use ae_vm_aevm::word::Word;
use std::sync::Arc;
fn unpack(v: &J, pool: &[J]) -> J {
    if let Some(n) = v.get("$ref") {
        return unpack(&pool[n.as_u64().unwrap() as usize], pool);
    }
    match v {
        J::Array(a) => J::Array(a.iter().map(|x| unpack(x, pool)).collect()),
        J::Object(m) => J::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), unpack(v, pool)))
                .collect(),
        ),
        v => v.clone(),
    }
}
fn fixture(raw: &str) -> J {
    let v: J = serde_json::from_str(raw).unwrap();
    unpack(&v["root"], v["values"].as_array().unwrap())
}
fn field<'a>(v: &'a J, name: &str) -> &'a J {
    &v[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == json!(["atom", name]))
        .unwrap()[1]
}
fn binary(v: &J) -> Vec<u8> {
    assert_eq!(v[0], "binary");
    hex::decode(v[1].as_str().unwrap()).unwrap()
}
fn tree(v: &J) -> Trie<Overlay<MemoryStore>> {
    let mut t = Trie::new(Overlay::new(MemoryStore::default(), Default::default()));
    for p in v[1].as_array().unwrap() {
        t.put(
            &Nibbles::from_bytes(&binary(&p[1][0])),
            TrieValue::Bytes(binary(&p[1][1])),
        )
        .unwrap();
    }
    t
}
fn trees(v: &J) -> State {
    let a = tree(field(v, "accounts"));
    let c = tree(field(v, "contracts"));
    let calls = tree(field(v, "calls"));
    let t = ae_state::Trees {
        accounts: ae_state::accounts::Accounts::from_root(a.root_hash(), a.into_store()).unwrap(),
        contracts: ae_state::contracts::Contracts::from_root(c.root_hash(), c.into_store())
            .unwrap(),
        calls: ae_state::calls::Calls::from_root(calls.root_hash(), calls.into_store()).unwrap(),
        channels: ae_state::channels::Channels::new(Overlay::new(
            MemoryStore::default(),
            Default::default(),
        )),
        names: ae_state::names::Names::new(
            Overlay::new(MemoryStore::default(), Default::default()),
            Overlay::new(MemoryStore::default(), Default::default()),
        ),
        oracles: ae_state::oracles::Oracles::new(
            Overlay::new(MemoryStore::default(), Default::default()),
            Overlay::new(MemoryStore::default(), Default::default()),
        ),
    };
    assert_eq!(t.roots().hash().0.as_slice(), binary(field(v, "root")));
    t
}
#[tokio::test]
async fn isolated_aevm_preserves_native_calls_gas_stores_and_fork_history() {
    let data = fixture(include_str!(
        "../../../ae-execution/tests/fixtures/erlang_aevm_chain_read_transactions.json"
    ));
    let history = fixture(include_str!(
        "../../../ae-execution/tests/fixtures/erlang_aevm_chain_history.json"
    ));
    let history = Arc::new(ae_execution::chain_history::Snapshot {
        selected: history["history"]["selected"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| (p[0].as_u64().unwrap(), binary(&p[1]).try_into().unwrap()))
            .collect(),
        headers: history["history"]["headers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| {
                (
                    binary(&p["hash"]).try_into().unwrap(),
                    ae_execution::chain_history::HeaderLink {
                        height: p["height"].as_u64().unwrap(),
                        previous_key: binary(&p["previous"]).try_into().unwrap(),
                    },
                )
            })
            .collect(),
    });
    let p = process();
    let (client, owner) = admission::create(admission::Config {
        threads: 1,
        requests: 1,
        bytes: p.reservation(p.input_bytes).unwrap(),
    })
    .unwrap();
    let mut count = 0;
    for row in data["runs"].as_array().unwrap().iter().filter(|r| {
        r["dry_run"] == true
            && r["gas"] == 100000
            && matches!(
                r["case_name"].as_str().unwrap(),
                "store_hash" | "nested_hash" | "extcopy" | "balance_high"
            )
    }) {
        let t = trees(&row["before"]);
        let before = t.roots();
        let tx = ContractTransaction::decode(&binary(&row["transaction"])).unwrap();
        let ContractOperation::Call { contract, .. } = tx.operation() else {
            panic!()
        };
        let id = ae_state::calls::call_id(tx.signer().0, tx.nonce(), contract.value);
        let mut expected = trees(&row["result"]["value"][1][1]);
        let call = expected
            .calls
            .lookup(&contract.value, id.0)
            .unwrap()
            .unwrap();
        let call_body = http::transaction::calls::call(&call, Default::default()).unwrap();
        // Native transaction fixtures do not include dry-run's mandatory magic-account
        // funding. Adjust only that independently known balance in the native output;
        // contract execution, calls, stores and all other accounts come from Erlang.
        let mut magic = expected
            .accounts
            .lookup(ae_execution::dry_run::MAGIC_ACCOUNT)
            .unwrap()
            .unwrap();
        magic.earn(&Unsigned::from_decimal("1000000000000000000000").unwrap());
        expected.accounts.enter(&magic).unwrap();
        let mut context = Context::transaction(
            300,
            ae_protocol::Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
        );
        context.dry_run = true;
        let view = Arc::new(ae_hyperchain::chain::View {
            trees: Arc::new(t),
            context,
            environment: ae_vm_fate::machine::program::remote::context::Environment {
                auth_tx_hash: None,
                origin: [0; 32],
                gas_price: 0.into(),
                fee: 0.into(),
                beneficiary: Word::from(5).0,
                timestamp: 12345.into(),
                difficulty: 7.into(),
                block_gas_limit: 6_000_000.into(),
                network_id: b"ae_mainnet".to_vec(),
                consensus: ae_vm_fate::machine::program::remote::context::Consensus::Pow,
                key_hash: binary(&row["key_hash"]).try_into().unwrap(),
            },
            history: history.clone(),
            aevm: Some(aevm::Config {
                control: None,
                environment: aevm::Environment {
                    genesis_height: 0,
                    key_hash: binary(&row["key_hash"]).try_into().unwrap(),
                    history: history.clone(),
                    beneficiary: AccountPublicKey(Word::from(5).0),
                    difficulty: 7.into(),
                    timestamp_ms: 12345.into(),
                    block_gas_limit: 6_000_000.into(),
                    auth_tx_hash: None,
                },
                network_id: Arc::from(b"ae_mainnet".as_slice()),
                event_context: ae_execution::aevm_chain::EventContext::None,
                float_profile: ae_vm_aevm::gas::FloatProfile::Otp26Word64,
                store_order: ae_vm_aevm::store::OrderProfile::Otp26Word64Crc32c,
            }),
        });
        let network = ae_protocol::NetworkProfile::mainnet();
        let cfg = ae_execution::transactions::Config::primitives(&network);
        let ticket = worker::submit(
            &client,
            process(),
            view.clone(),
            worker::Request {
                inputs: vec![ae_execution::dry_run::prepare::Input::transaction(
                    Transaction::new(TransactionLeaf::Contract(tx)),
                )],
                funding: vec![],
                options: ae_execution::dry_run::Options {
                    profile: ae_execution::dry_run::Profile::Replay,
                    ..Default::default()
                },
            },
            parameters(&cfg, None),
            Timeout::After(Duration::from_secs(10)),
        )
        .unwrap();
        let retained = tokio::time::timeout(Duration::from_secs(15), ticket.wait_retained())
            .await
            .unwrap()
            .unwrap();
        let reply = worker::decode_reply(retained.value.result.as_ref().unwrap()).unwrap();
        let worker::Reply::Done(result) = reply else {
            panic!("{}: {reply:?}", row["case_name"])
        };
        assert_eq!(
            result.body["results"][0]["call_obj"], call_body,
            "{} protocol={} hash={}",
            row["case_name"], row["protocol"], row["key_hash"]
        );
        assert_eq!(result.roots, ae_chain::repository::Roots::of(&expected));
        assert_eq!(view.trees.roots(), before);
        count += 1;
    }
    assert_eq!(count, 108);
    owner.shutdown().await;
}
