use super::*;
use ae_tx::{CanonicalTransaction, SignedTransaction, Spend, Transaction, TransactionLeaf};
use ae_types::{Id, IdTag};

fn spend(open: &configured::Open, nonce: u64) -> Arc<SignedTransaction<Transaction>> {
    let mut tx = Spend::new(payer().public_key(), Id::new(IdTag::Account, [43; 32])).unwrap();
    tx.set_nonce(nonce.into());
    tx.set_amount(7.into());
    tx.set_fee(100_000_000_000_000u64.into());
    let mut raw = open.chain().settings().profile().signing_prefix(false);
    raw.extend(tx.encode());
    Arc::new(SignedTransaction::new(
        Transaction::new(TransactionLeaf::Spend(tx)),
        vec![payer().sign(&raw)],
    ))
}
async fn mine(h: &node::Handle, count: i64, kind: Kind) {
    let result = h
        .commands()
        .unwrap()
        .client
        .submit(Command::Blocks { count, kind })
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(
        result.result.as_ref().unwrap().command.result.is_ok(),
        "{result:?}"
    );
}
fn attach(open: &configured::Open, symbols: bool) -> Arc<SignedTransaction<Transaction>> {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../../ae-execution/tests/fixtures/erlang_ga_attach_execution.json"
    ))
    .unwrap();
    fn resolve<'a>(mut v: &'a Value, pool: &'a Value) -> &'a Value {
        while let Some(i) = v.get("$ref").and_then(Value::as_u64) {
            v = &pool[i as usize];
        }
        v
    }
    let values = &fixture["values"];
    let row = resolve(&fixture["root"]["runs"], values)
        .as_array()
        .unwrap()
        .iter()
        .map(|v| resolve(v, values))
        .find(|r| {
            r["protocol"] == 6
                && r["vm"] == 8
                && r["abi"] == 3
                && resolve(&r["result"], values)["kind"] == "ok"
                && r["context"] == "aetx_transaction"
        })
        .unwrap();
    let raw = resolve(&row["signed"], values);
    let old =
        SignedTransaction::<Transaction>::decode(&hex::decode(raw[1].as_str().unwrap()).unwrap())
            .unwrap();
    let TransactionLeaf::GaAttach(old) = old.transaction().leaf() else {
        panic!("attach fixture");
    };
    // Execution vectors deliberately omit symbol names. Add compiler-style
    // names for the successful description case without changing functions.
    let mut code = ae_codec::sophia::SophiaCode::decode(old.code()).unwrap();
    if symbols {
        let mut fate = ae_vm_fate::code::Code::decode(&code.byte_code, Default::default()).unwrap();
        assert_eq!(
            fate.insert_symbol(b"authorize").unwrap().as_slice(),
            &old.auth_fun()[..4]
        );
        fate.insert_symbol(b"init").unwrap();
        code.byte_code = fate.encode(Default::default()).unwrap();
    }
    let mut tx = ae_tx::GaAttachTransaction::new(
        payer().public_key(),
        old.version(),
        code.encode().unwrap(),
        old.auth_fun().to_vec(),
        old.call_data().to_vec(),
    );
    tx.set_nonce(1.into());
    tx.set_fee(100_000_000_000_000u64.into());
    tx.set_gas(old.gas().clone());
    tx.set_gas_price(1_000_000_000u64.into());
    let mut payload = open.chain().settings().profile().signing_prefix(false);
    payload.extend(tx.encode());
    Arc::new(SignedTransaction::new(
        Transaction::new(TransactionLeaf::GaAttach(tx)),
        vec![payer().sign(&payload)],
    ))
}

#[tokio::test]
async fn generalized_account_http_resolves_fate_names_and_skips_pool_with_or_without_symbols() {
    for symbols in [false, true] {
        let lab = Lab::new();
        let p = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
        let tx = attach(p.configured(), symbols);
        let mut running = p.launch(commands(&lab), || Ok(Default::default())).unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        let mut cfg = config();
        cfg.groups.insert("account".into());
        let http = running.listen_http(cfg).await.unwrap();
        let address = http.status().address;
        mine(&h, 1, Kind::Key).await;
        let pool = h
            .pow()
            .unwrap()
            .workers()
            .unwrap()
            .bindings()
            .admission
            .current()
            .unwrap()
            .client;
        pool.push(tx, ae_mempool::pool::Event::Created, false)
            .unwrap()
            .await
            .unwrap();
        mine(&h, 1, Kind::Micro).await;
        pool.refresh().unwrap().await.unwrap();
        let owner = payer().public_key();
        let path = format!(
            "/v3/accounts/{}",
            api::encode(ApiType::AccountPubkey, owner.as_ref())
        );
        let account = get(address, &path).await;
        if symbols {
            assert_eq!(account.0, 200, "{}", account.2);
            assert_eq!(account.2["kind"], "generalized");
            assert_eq!(account.2["auth_fun"], "authorize");
            let contract = ae_state::contracts::contract_id(
                owner,
                ae_state::contracts::Nonce::Number(&1.into()),
            );
            assert_eq!(
                account.2["contract_id"],
                api::encode(ApiType::ContractPubkey, contract.as_ref())
            );
        } else {
            assert_eq!(
                (account.0, account.2.clone()),
                (500, json!({"reason":"Internal server error"}))
            );
        }
        let mut held = vec![];
        while let Ok(ticket) = pool.pending_account(owner, Default::default()) {
            held.push(ticket.await.unwrap());
            assert!(held.len() < 32);
        }
        assert!(!held.is_empty());
        let usage = pool.account_query_usage();
        for strategy in ["max", "continuity"] {
            assert_eq!(
                get(address, &format!("{path}/next-nonce?strategy={strategy}"))
                    .await
                    .2,
                json!({"next_nonce":0})
            );
        }
        assert_eq!(pool.account_query_usage(), usage);
        assert_eq!(
            get(address, &format!("{path}/transactions/pending"))
                .await
                .0,
            503
        );
        assert_eq!(get(address, &path).await.2, account.2);
        drop(held);
        assert_eq!(pool.account_query_usage(), (0, 0));
        assert_eq!(
            get(address, &format!("{path}/transactions/pending"))
                .await
                .2,
            json!({"transactions":[]})
        );
        assert!(running.shutdown().await.unwrap().status().joined);
    }
}
#[tokio::test]
async fn account_http_queries_follow_pending_nonces_historical_key_micro_state_gc_and_replacement()
{
    let lab = Lab::new();
    let p = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = p.configured().chain().database().clone();
    let genesis = db.view().unwrap().status().tip.unwrap();
    let first = spend(p.configured(), 1);
    let third = spend(p.configured(), 3);
    let mut settings = commands(&lab);
    if let node::Settings::OnDemand(s) = &mut settings {
        s.config.workers.gc.as_mut().unwrap().policy.enabled = true;
    }
    let mut running = p.launch(settings, || Ok(Default::default())).unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut cfg = config();
    cfg.groups.insert("account".into());
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
    let owner = api::encode(ApiType::AccountPubkey, payer().public_key().as_ref());
    let contract = api::encode(ApiType::ContractPubkey, payer().public_key().as_ref());
    let path = format!("/v3/accounts/{owner}");
    let initial = get(address, &path).await;
    assert_eq!(initial.0, 200, "{}", initial.2);
    assert_eq!(initial.2["nonce"], 0);
    assert_eq!(initial.2["balance"], 1_000_000_000_000_000_000u64);
    assert_eq!(
        get(address, &format!("/v3/accounts/{contract}")).await.2,
        initial.2
    );
    assert_eq!(
        get(address, &format!("{path}/next-nonce")).await.2,
        json!({"next_nonce":1})
    );
    assert_eq!(
        get(address, &format!("{path}/transactions/pending"))
            .await
            .2,
        json!({"transactions":[]})
    );
    let workers = h.pow().unwrap().workers().unwrap();
    let pool = workers.bindings().admission.current().unwrap().client;
    pool.push(first.clone(), ae_mempool::pool::Event::Created, false)
        .unwrap()
        .await
        .unwrap();
    pool.push(third.clone(), ae_mempool::pool::Event::Received, false)
        .unwrap()
        .await
        .unwrap();
    for version in ["v2", "v3"] {
        let base = format!("/{version}/accounts/{owner}");
        assert_eq!(
            get(address, &format!("{base}/next-nonce")).await.2,
            json!({"next_nonce":4})
        );
        assert_eq!(
            get(address, &format!("{base}/next-nonce?strategy=continuity"))
                .await
                .2,
            json!({"next_nonce":2})
        );
        let pending = get(address, &format!("{base}/transactions/pending")).await;
        assert_eq!(pending.0, 200, "{}", pending.2);
        assert_eq!(
            pending.2["transactions"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["tx"]["nonce"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
    }
    assert_eq!(pool.account_query_usage(), (0, 0));
    assert_eq!(
        get(address, &format!("{path}/next-nonce?int-as-string=true"))
            .await
            .2,
        json!({"next_nonce":"4"})
    );
    assert_eq!(
        get(address, &format!("/v2/accounts/{owner}?int-as-string=true"))
            .await
            .2,
        initial.2
    );
    let missing = api::encode(ApiType::AccountPubkey, &[91; 32]);
    assert_eq!(
        get(address, &format!("/v3/accounts/{missing}/next-nonce"))
            .await
            .2,
        json!({"reason":"Account not found","error_code":"account_not_found"})
    );
    assert_eq!(
        get(address, &format!("/v3/accounts/{contract}/next-nonce"))
            .await
            .0,
        400
    );
    assert_eq!(
        get(address, &format!("{path}/height/99999")).await.2,
        json!({"reason":"Height not available","error_code":"chain_too_short"})
    );
    assert_eq!(
        get(
            address,
            &format!(
                "{path}/hash/{}",
                api::encode(ApiType::KeyBlockHash, &[91; 32])
            )
        )
        .await
        .2,
        json!({"reason":"Hash not available","error_code":"no_state_trees"})
    );
    mine(&h, 2, Kind::Any).await;
    pool.refresh().unwrap().await.unwrap();
    let (micro, key) = {
        let view = db.view().unwrap();
        let micro = view.location(first.hash()).unwrap().unwrap();
        let header = view.require_header(micro).unwrap();
        (micro, ae_types::Hash256(header.common().previous_key_hash))
    };
    let current = get(address, &path).await;
    assert_eq!(current.0, 200);
    assert_eq!(current.2["nonce"], 1);
    assert_eq!(current.2["balance"], 999_899_999_999_999_993u64);
    assert_eq!(get(address, &format!("{path}/height/1")).await.2, initial.2);
    assert_eq!(
        get(
            address,
            &format!(
                "{path}/hash/{}",
                api::encode(ApiType::KeyBlockHash, key.as_ref())
            )
        )
        .await
        .2,
        initial.2
    );
    assert_eq!(
        get(
            address,
            &format!(
                "{path}/hash/{}",
                api::encode(ApiType::MicroBlockHash, micro.as_ref())
            )
        )
        .await
        .2,
        current.2
    );
    assert_eq!(
        get(address, &format!("{path}/transactions/pending"))
            .await
            .2["transactions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    pool.delete(third.hash()).unwrap().await.unwrap();
    let generation = workers.status().admission_generation;
    let command_generation = h.commands().unwrap().number;
    workers
        .restart(ae_sync::runtime::workers::Child::Admission, generation)
        .unwrap()
        .wait()
        .await
        .unwrap();
    within(async {
        while !h.status().ready || h.commands().is_none_or(|c| c.number <= command_generation) {
            h.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(
        get(address, &format!("{path}/next-nonce?strategy=continuity"))
            .await
            .2,
        json!({"next_nonce":2})
    );
    let importer = h.importer().unwrap();
    let capacity = importer.available();
    let mut held = vec![];
    while let Ok(ticket) = importer.current() {
        held.push(ticket);
        assert!(held.len() < 64);
    }
    assert_eq!(get(address, "/v3/accounts/bad").await.0, 503);
    assert_eq!(get(address, "/v2/accounts/bad/height/-1").await.0, 400);
    drop(held);
    within(async {
        while importer.available() != capacity {
            tokio::task::yield_now().await;
        }
    })
    .await;
    db.rollback(
        ae_chain::repository::rollback::Target::Height(0),
        Default::default(),
    )
    .unwrap();
    assert_eq!(get(address, &path).await.2, initial.2);
    mine(&h, 2, Kind::Key).await;
    assert!(
        db.begin_gc(
            ae_chain::repository::retention::Selection::new(&[
                ae_chain::repository::retention::Tree::Calls
            ])
            .unwrap()
        )
        .unwrap()
    );
    assert_eq!(get(address, &format!("{path}/height/0")).await.0, 410);
    let (code, headers, body) = request(
        address,
        &format!("GET /v2/accounts/{owner}/height/0 HTTP/1.1\r\nHost: localhost\r\n\r\n"),
    )
    .await;
    assert_eq!(code, 500);
    assert!(body.is_empty());
    assert!(!headers.contains_key("x-ae-height"));
    // Account nodes were not selected for collection: hash lookup below the
    // global height floor still succeeds, as in the original partial-state read.
    assert_eq!(
        get(
            address,
            &format!(
                "{path}/hash/{}",
                api::encode(ApiType::KeyBlockHash, genesis.as_ref())
            )
        )
        .await
        .2,
        initial.2
    );
    let exit = running.shutdown().await.unwrap();
    assert!(exit.status().joined);
    assert!(http.status().joined);
}
