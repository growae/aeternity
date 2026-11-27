use super::*;
use ae_protocol::oracles::Ttl;
use ae_tx::{
    CanonicalTransaction, OracleOperation, OracleTransaction, SignedTransaction, Transaction,
    TransactionLeaf,
};
use ae_types::{Id, IdTag};
fn signed(
    open: &configured::Open,
    nonce: u64,
    operation: OracleOperation,
) -> Arc<SignedTransaction<Transaction>> {
    let mut tx = OracleTransaction::new(payer().public_key(), operation).unwrap();
    tx.set_nonce(nonce.into());
    tx.set_fee(100_000_000_000_000u64.into());
    let mut bytes = open.chain().settings().profile().signing_prefix(false);
    bytes.extend(tx.encode());
    Arc::new(SignedTransaction::new(
        Transaction::new(TransactionLeaf::Oracle(tx)),
        vec![payer().sign(&bytes)],
    ))
}
async fn admit(h: &node::Handle, tx: Arc<SignedTransaction<Transaction>>) {
    h.pow()
        .unwrap()
        .workers()
        .unwrap()
        .bindings()
        .admission
        .current()
        .unwrap()
        .client
        .push(tx, ae_mempool::pool::Event::Created, false)
        .unwrap()
        .await
        .unwrap();
}
#[tokio::test]
async fn oracle_http_registers_queries_answers_paginates_and_tracks_replacement_and_rollback() {
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = prepared.configured().chain().database().clone();
    let oracle = Id::new(IdTag::Oracle, payer().public_key().0);
    let key = api::encode(ApiType::OraclePubkey, &oracle.value);
    let register = signed(
        prepared.configured(),
        1,
        OracleOperation::Register {
            query_format: b"question".to_vec(),
            response_format: b"answer".to_vec(),
            query_fee: 7.into(),
            oracle_ttl: Ttl::Delta(100.into()),
            abi_version: 0.into(),
        },
    );
    let queries: Vec<_> = (2..=4)
        .map(|nonce| {
            signed(
                prepared.configured(),
                nonce,
                OracleOperation::Query {
                    oracle,
                    query: vec![0, 255, nonce as u8],
                    query_fee: 7.into(),
                    query_ttl: Ttl::Delta(30.into()),
                    response_ttl: 10.into(),
                },
            )
        })
        .collect();
    let ids: Vec<_> = (2..=4)
        .map(|nonce| {
            ae_state::oracles::query_id(payer().public_key(), &(nonce as u64).into(), oracle.value)
                .0
        })
        .collect();
    let responses: Vec<_> = (0..2)
        .map(|n| {
            signed(
                prepared.configured(),
                5 + n as u64,
                OracleOperation::Response {
                    query_id: ids[n].to_vec(),
                    response: if n == 0 { vec![] } else { vec![0, 255, 42] },
                    response_ttl: 10.into(),
                },
            )
        })
        .collect();
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut cfg = config();
    cfg.groups = ["oracle".into()].into();
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
    let path = format!("/v3/oracles/{key}");
    assert_eq!(get(address, &path).await.0, 404);
    assert_eq!(
        get(address, &format!("{path}/queries")).await.2,
        json!({"oracle_queries":[]})
    );
    assert_eq!(
        get(
            address,
            &format!(
                "{path}/queries/{}",
                api::encode(ApiType::OracleQueryId, &ids[0])
            )
        )
        .await
        .0,
        404
    );
    info::mine(&h, 1, Kind::Key).await;
    admit(&h, register.clone()).await;
    info::mine(&h, 1, Kind::Micro).await;
    assert!(
        db.view()
            .unwrap()
            .location(register.hash())
            .unwrap()
            .is_some()
    );
    let metadata = get(address, &path).await;
    assert_eq!(metadata.0, 200, "{}", metadata.2);
    assert_eq!(
        metadata.2,
        json!({"id":key,"query_format":"question","response_format":"answer","query_fee":7,"ttl":101,"abi_version":0})
    );
    assert_eq!(
        metadata.1["access-control-allow-origin"],
        "https://client.example"
    );
    for tx in &queries {
        admit(&h, tx.clone()).await;
    }
    info::mine(&h, 1, Kind::Micro).await;
    let query_top = {
        let view = db.view().unwrap();
        for tx in &queries {
            assert!(view.location(tx.hash()).unwrap().is_some());
        }
        view.status().tip.unwrap()
    };
    let open = get(address, &format!("{path}/queries?type=open")).await;
    assert_eq!(open.0, 200, "{}", open.2);
    assert_eq!(open.2["oracle_queries"].as_array().unwrap().len(), 3);
    assert_eq!(
        get(address, &format!("{path}/queries?type=closed")).await.2,
        json!({"oracle_queries":[]})
    );
    for tx in &responses {
        admit(&h, tx.clone()).await;
    }
    info::mine(&h, 1, Kind::Micro).await;
    for tx in &responses {
        assert!(db.view().unwrap().location(tx.hash()).unwrap().is_some());
    }
    let all = get(address, &format!("{path}/queries")).await;
    assert_eq!(all.0, 200, "{}", all.2);
    let rows = all.2["oracle_queries"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(
        rows.iter()
            .map(|r| r["id"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>(),
        sorted
            .iter()
            .map(|id| api::encode(ApiType::OracleQueryId, id))
            .collect::<Vec<_>>()
    );
    for (n, id) in ids.iter().enumerate() {
        let result = get(
            address,
            &format!("{path}/queries/{}", api::encode(ApiType::OracleQueryId, id)),
        )
        .await;
        assert_eq!(result.0, 200, "{}", result.2);
        assert_eq!(result.2["sender_nonce"], n as u64 + 2);
        assert_eq!(
            api::decode_as(ApiType::OracleQuery, result.2["query"].as_str().unwrap()).unwrap(),
            [0, 255, n as u8 + 2]
        );
        let expected = if n == 1 { vec![0, 255, 42] } else { vec![] };
        assert_eq!(
            api::decode_as(
                ApiType::OracleResponse,
                result.2["response"].as_str().unwrap()
            )
            .unwrap(),
            expected
        );
    }
    let closed = get(address, &format!("{path}/queries?type=closed")).await;
    assert_eq!(closed.2["oracle_queries"].as_array().unwrap().len(), 2);
    let open = get(address, &format!("{path}/queries?type=open")).await;
    assert_eq!(open.2["oracle_queries"].as_array().unwrap().len(), 1);
    assert_eq!(
        open.2["oracle_queries"][0]["id"],
        api::encode(ApiType::OracleQueryId, &ids[2])
    );
    let mut cursor = String::new();
    let mut paged = vec![];
    for _ in 0..4 {
        let page = get(address, &format!("{path}/queries?limit=1{cursor}")).await;
        assert_eq!(page.0, 200, "{}", page.2);
        let page = page.2["oracle_queries"].as_array().unwrap();
        if page.is_empty() {
            break;
        }
        assert_eq!(page.len(), 1);
        cursor = format!("&from={}", page[0]["id"].as_str().unwrap());
        paged.push(page[0].clone());
    }
    assert_eq!(&paged, rows);
    assert_eq!(
        get(
            address,
            &format!("/v2/oracles/{key}/queries?int-as-string=true")
        )
        .await
        .2,
        all.2
    );
    let strings = get(address, &format!("{path}/queries?int-as-string=true")).await;
    assert_eq!(strings.0, 200, "{}", strings.2);
    for (converted, original) in strings.2["oracle_queries"]
        .as_array()
        .unwrap()
        .iter()
        .zip(rows)
    {
        assert_eq!(
            converted["sender_nonce"],
            original["sender_nonce"].to_string()
        );
        assert_eq!(converted["response_ttl"]["value"], "10");
    }
    assert_eq!(
        get(address, &format!("{path}?int-as-string=true")).await.2["ttl"],
        "101"
    );
    let workers = h.pow().unwrap().workers().unwrap();
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
    assert_eq!(get(address, &format!("{path}/queries")).await.2, all.2);
    let importer = h.importer().unwrap();
    let capacity = importer.available();
    let permit = importer.oracle_query(48 << 20).unwrap();
    assert!(importer.available().0 < capacity.0);
    drop(permit);
    assert_eq!(importer.available(), capacity);
    let held = importer
        .oracle_query(48 << 20)
        .unwrap()
        .submit(oracle.value, ae_chain::oracle::Kind::Metadata, |r| r)
        .unwrap()
        .wait_retained()
        .await
        .unwrap();
    assert!(matches!(
        held.value,
        Ok(ae_chain::oracle::Reply::Metadata(_))
    ));
    assert!(importer.available().0 < capacity.0);
    drop(held);
    assert_eq!(importer.available(), capacity);
    let mut held = vec![];
    while let Ok(ticket) = importer.current() {
        held.push(ticket);
        assert!(held.len() < 64);
    }
    for suffix in ["", "/queries", "/queries/bad"] {
        assert_eq!(
            get(address, &format!("/v3/oracles/bad{suffix}")).await.0,
            503
        );
    }
    assert_eq!(
        get(address, "/v3/oracles/bad/queries?limit=0&type=bad")
            .await
            .0,
        400
    );
    let bare = request(
        address,
        "GET /v3/oracles/bad/queries?limit HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(bare.0, 500);
    assert!(bare.2.is_empty());
    assert!(!bare.1.contains_key("x-ae-height"));
    drop(held);
    within(async {
        while importer.available() != capacity {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(
        get(address, &format!("{path}/queries?from=bad")).await.0,
        500
    );
    db.rollback(
        ae_chain::repository::rollback::Target::Micro(query_top),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        get(address, &format!("{path}/queries?type=closed")).await.2,
        json!({"oracle_queries":[]})
    );
    assert_eq!(
        get(address, &format!("{path}/queries?type=open")).await.2["oracle_queries"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    db.rollback(
        ae_chain::repository::rollback::Target::Height(0),
        Default::default(),
    )
    .unwrap();
    assert_eq!(get(address, &path).await.0, 404);
    assert_eq!(
        get(address, &format!("{path}/queries")).await.2,
        json!({"oracle_queries":[]})
    );
    assert_eq!(
        get(
            address,
            &format!(
                "{path}/queries/{}",
                api::encode(ApiType::OracleQueryId, &ids[0])
            )
        )
        .await
        .0,
        404
    );
    assert!(running.shutdown().await.unwrap().status().joined);
    assert!(http.status().joined);
}
