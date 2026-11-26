use super::*;
use ae_tx::{
    CanonicalTransaction, ContractTransaction, SignedTransaction, Transaction, TransactionLeaf,
};
use ae_types::{Id, IdTag};

fn contract(open: &configured::Open) -> Arc<SignedTransaction<Transaction>> {
    // Reuse the canonical original Ceres init transaction from the independent
    // execution corpus. Adapt owner, nonce, fee and miner gas price for this chain.
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../../ae-execution/tests/fixtures/erlang_contract_create.json"
    ))
    .unwrap();
    fn resolve<'a>(mut v: &'a Value, pool: &'a Value) -> &'a Value {
        while let Some(i) = v.get("$ref").and_then(Value::as_u64) {
            v = &pool[i as usize]
        }
        v
    }
    let pool = &fixture["values"];
    let row = resolve(&fixture["root"]["cases"], pool)
        .as_array()
        .unwrap()
        .iter()
        .map(|r| resolve(r, pool))
        .find(|r| {
            r["protocol"] == 6
                && r["case_id"] == 0
                && r["gas"] == 100000
                && resolve(&r["result"], pool)["kind"] == "ok"
        })
        .unwrap();
    let old =
        ContractTransaction::decode(&hex::decode(row["transaction"].as_str().unwrap()).unwrap())
            .unwrap();
    let mut tx = ContractTransaction::new(
        Id::new(IdTag::Account, payer().public_key().0),
        old.operation().clone(),
    )
    .unwrap();
    tx.set_nonce(1.into());
    tx.set_fee(100_000_000_000_000u64.into());
    tx.set_gas(old.gas().clone());
    tx.set_gas_price(1_000_000_000u64.into());
    let mut payload = open.chain().settings().profile().signing_prefix(false);
    payload.extend(tx.encode());
    Arc::new(SignedTransaction::new(
        Transaction::new(TransactionLeaf::Contract(tx)),
        vec![payer().sign(&payload)],
    ))
}
async fn mine(h: &node::Handle, count: i64, kind: Kind) {
    let done = h
        .commands()
        .unwrap()
        .client
        .submit(Command::Blocks { count, kind })
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(
        done.result.as_ref().unwrap().command.result.is_ok(),
        "{done:?}"
    );
}
#[tokio::test]
async fn http_transaction_info_executes_contract_and_survives_worker_replacement() {
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = prepared.configured().chain().database().clone();
    let tx = contract(prepared.configured());
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut cfg = config();
    cfg.groups.insert("transaction".into());
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
    let hash = api::encode(ApiType::TxHash, tx.hash().as_ref());
    for version in ["v2", "v3"] {
        let (code, _, body) = get(address, &format!("/{version}/transactions/{hash}/info")).await;
        assert_eq!(
            (code, body),
            (404, json!({"reason":"Transaction not found"}))
        );
        let (code, _, body) = get(address, &format!("/{version}/transactions/bad/info")).await;
        assert_eq!((code, body), (400, json!({"reason":"Invalid hash: hash"})));
    }
    mine(&h, 1, Kind::Key).await;
    let workers = h.pow().unwrap().workers().unwrap();
    workers
        .bindings()
        .admission
        .current()
        .unwrap()
        .client
        .push(tx.clone(), ae_mempool::pool::Event::Created, false)
        .unwrap()
        .await
        .unwrap();
    for version in ["v2", "v3"] {
        let (code, _, body) = get(address, &format!("/{version}/transactions/{hash}/info")).await;
        assert_eq!((code, body), (404, json!({"reason":"Tx not mined"})));
    }
    mine(&h, 2, Kind::Any).await;
    let view = db.view().unwrap();
    let block = view.location(tx.hash()).unwrap().unwrap();
    let height = view.require_header(block).unwrap().common().height;
    let contract = ae_state::contracts::contract_id(
        payer().public_key(),
        ae_state::contracts::Nonce::Number(&1.into()),
    );
    let mut expected = None;
    for version in ["v2", "v3"] {
        let (code, headers, body) =
            get(address, &format!("/{version}/transactions/{hash}/info")).await;
        assert_eq!(code, 200, "{body}");
        assert_eq!(
            headers["access-control-allow-origin"],
            "https://client.example"
        );
        let call = &body["call_info"];
        assert_eq!(
            call["caller_id"],
            api::encode(ApiType::AccountPubkey, payer().public_key().as_ref())
        );
        assert_eq!(
            call["contract_id"],
            api::encode(ApiType::ContractPubkey, contract.as_ref())
        );
        assert_eq!(call["caller_nonce"], 1);
        assert_eq!(call["height"], height);
        assert_eq!(call["return_type"], "ok");
        assert!(call["gas_used"].as_u64().unwrap() > 0);
        assert_eq!(call["log"], json!([]));
        if let Some(expected) = &expected {
            assert_eq!(&body, expected)
        } else {
            expected = Some(body)
        }
    }
    let mut strings = expected.clone().unwrap();
    for field in ["caller_nonce", "height", "gas_used", "gas_price"] {
        strings["call_info"][field] =
            json!(strings["call_info"][field].as_u64().unwrap().to_string());
    }
    assert_eq!(
        get(
            address,
            &format!("/v3/transactions/{hash}/info?int-as-string=true")
        )
        .await
        .2,
        strings
    );
    let generation = workers.status().admission_generation;
    let command_generation = h.commands().unwrap().number;
    workers
        .restart(ae_sync::runtime::workers::Child::Admission, generation)
        .unwrap()
        .wait()
        .await
        .unwrap();
    within(async {
        loop {
            assert!(h.status().failure.is_none());
            if h.status().ready && h.commands().is_some_and(|c| c.number > command_generation) {
                break;
            }
            h.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(
        get(address, &format!("/v3/transactions/{hash}/info"))
            .await
            .2,
        expected.unwrap()
    );
    drop(view);
    db.rollback(
        ae_chain::repository::rollback::Target::Height(0),
        Default::default(),
    )
    .unwrap();
    let (code, _, body) = get(address, &format!("/v3/transactions/{hash}/info")).await;
    assert_eq!(
        (code, body),
        (404, json!({"reason":"Transaction not found"}))
    );
    assert!(within(running.shutdown()).await.unwrap().status().joined);
    assert!(http.status().joined);
}
