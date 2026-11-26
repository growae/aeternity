use super::*;

async fn post(address: SocketAddr, version: &str, body: &str, chunked: bool) -> (u16, Value) {
    let raw = if chunked {
        let split = body.len() / 2;
        format!(
            "POST /{version}/transactions HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json; charset=utf-8\r\nTransfer-Encoding: chunked\r\n\r\n{:x};test=yes\r\n{}\r\n{:x}\r\n{}\r\n0\r\nX-End: yes\r\n\r\n",
            split,
            &body[..split],
            body.len() - split,
            &body[split..]
        )
    } else {
        format!(
            "POST /{version}/transactions HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    };
    let (code, _, bytes) = request(address, &raw).await;
    (code, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test]
async fn http_post_transaction_admits_once_mines_and_uses_replacement_worker() {
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = prepared.configured().chain().database().clone();
    let tx = transfer(prepared.configured());
    let mut settings = commands(&lab);
    if let node::Settings::OnDemand(s) = &mut settings {
        s.config.workers.pool.policy.allow_reentry = true;
    }
    let mut running = prepared
        .launch(settings, || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut cfg = config();
    cfg.groups.insert("transaction".into());
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
    let hash = api::encode(ApiType::TxHash, tx.hash().as_ref());
    let body = json!({"tx":api::encode(ApiType::Transaction, &tx.encode())}).to_string();
    let workers = h.pow().unwrap().workers().unwrap();
    let pool = workers.bindings().admission.current().unwrap().client;
    let count = pool.info().submitted;
    assert_eq!(
        post(address, "v2", &body, false).await,
        (200, json!({"tx_hash":hash}))
    );
    assert_eq!(pool.info().submitted, count + 1);
    assert_eq!(
        get(address, &format!("/v3/transactions/{hash}")).await.2["block_height"],
        -1
    );
    assert_eq!(
        post(address, "v3", &body, true).await,
        (
            400,
            json!({"reason":"Invalid tx","error_code":"already_known"})
        )
    );
    assert_eq!(pool.info().submitted, count + 2);
    pool.delete(tx.hash()).unwrap().await.unwrap();
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
            assert!(h.status().running && h.status().failure.is_none());
            if h.status().ready && h.commands().is_some_and(|c| c.number > command_generation) {
                break;
            }
            h.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(
        post(address, "v3", &body, true).await,
        (200, json!({"tx_hash":hash}))
    );
    let done = h
        .commands()
        .unwrap()
        .client
        .submit(Command::Blocks {
            count: 2,
            kind: Kind::Any,
        })
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(done.result.as_ref().unwrap().command.result.is_ok());
    assert!(
        get(address, &format!("/v2/transactions/{hash}")).await.2["block_height"]
            .as_u64()
            .is_some()
    );
    assert_eq!(
        post(address, "v2", &body, false).await,
        (
            400,
            json!({"reason":"Invalid tx","error_code":"already_accepted"})
        )
    );
    db.rollback(
        ae_chain::repository::rollback::Target::Height(0),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        get(address, &format!("/v3/transactions/{hash}")).await.0,
        404
    );
    let stopped = within(running.shutdown()).await.unwrap();
    assert!(stopped.status().joined && http.status().joined);
}
#[tokio::test]
async fn http_post_rejects_invalid_schema_and_framing_before_pool_submission() {
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut cfg = config();
    cfg.groups.insert("transaction".into());
    cfg.body_bytes = 1024;
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
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
    let count = pool.info().submitted;
    assert_eq!(
        post(address, "v2", r#"{"tx":"no"}"#, false).await,
        (
            400,
            json!({"reason":"Invalid api encoding","error_code":"invalid_encoding"})
        )
    );
    assert_eq!(
        post(address, "v3", r#"{"tx":"no"}"#, true).await,
        (
            400,
            json!({"reason":"validation_error","parameter":"body","info":{"error":"no_match","data":"no","path":["tx"]}})
        )
    );
    for (headers, body, code) in [
        ("Content-Length: 2", "{}", 415),
        ("Content-Type: text/plain\r\nContent-Length: 2", "{}", 415),
        (
            "Content-Type: application/json\r\nExpect: unexpected\r\nContent-Length: 2",
            "{}",
            400,
        ),
        (
            "Content-Type: application/json\r\nTransfer-Encoding: chunked\r\nContent-Length: 2",
            "{}",
            400,
        ),
        (
            "Content-Type: application/json\r\nContent-Length: 2\r\nContent-Length: 2",
            "{}",
            400,
        ),
        (
            "Content-Type: application/json\r\nContent-Length: 1025",
            "",
            413,
        ),
        (
            "Content-Type: application/json\r\nTransfer-Encoding: chunked",
            "401\r\n",
            413,
        ),
        (
            "Content-Type: application/json\r\nTransfer-Encoding: chunked",
            "z\r\n",
            400,
        ),
        (
            "Content-Type: application/json\r\nTransfer-Encoding: chunked",
            "0\r\nContent-Length: 5\r\n\r\n",
            400,
        ),
    ] {
        assert_eq!(
            request(
                address,
                &format!(
                    "POST /v3/transactions HTTP/1.1\r\nHost: localhost\r\n{headers}\r\n\r\n{body}"
                )
            )
            .await
            .0,
            code,
            "{headers}"
        );
    }
    assert_eq!(
        request(
            address,
            "GET /v3/transactions HTTP/1.1\r\nHost: localhost\r\n\r\n"
        )
        .await
        .0,
        405
    );
    assert_eq!(pool.info().submitted, count);
    assert!(within(running.shutdown()).await.unwrap().status().joined);
    assert!(http.status().joined);
}

#[tokio::test]
async fn http_post_continue_pressure_cancellation_and_created_nonce_policy() {
    use ae_tx::CanonicalTransaction;
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let tx = transfer(prepared.configured());
    let ae_tx::TransactionLeaf::Spend(mut high) = tx.transaction().leaf().clone() else {
        unreachable!()
    };
    high.set_nonce(6.into());
    let mut signing = prepared
        .configured()
        .chain()
        .settings()
        .profile()
        .signing_prefix(false);
    signing.extend(high.encode());
    let high = Arc::new(ae_tx::SignedTransaction::new(
        ae_tx::Transaction::new(ae_tx::TransactionLeaf::Spend(high)),
        vec![payer().sign(&signing)],
    ));
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut cfg = config();
    cfg.groups.insert("transaction".into());
    cfg.submissions.requests = 1;
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
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
    let count = pool.info().submitted;
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(b"POST /v3/transactions HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 200\r\nExpect: 100-continue\r\n\r\n").await.unwrap();
    let mut interim = [0; 25];
    within(socket.read_exact(&mut interim)).await.unwrap();
    assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");
    assert_eq!(
        post(address, "v3", "{}", false).await,
        (503, json!({"reason":"Temporary overload"}))
    );
    assert_eq!(pool.info().submitted, count);
    drop(socket);
    within(async {
        loop {
            let response = post(address, "v3", "{}", false).await;
            if response.0 == 400 {
                break;
            }
            assert_eq!(response.0, 503);
            tokio::task::yield_now().await;
        }
    })
    .await;
    let high_body = json!({"tx":api::encode(ApiType::Transaction,&high.encode())}).to_string();
    assert_eq!(
        post(address, "v3", &high_body, false).await,
        (
            400,
            json!({"reason":"Invalid tx","error_code":"nonce_too_high"})
        )
    );
    // Same signed transaction is valid under peer-received policy. HTTP must
    // retain tx_created's stricter bound rather than using gossip admission.
    pool.push(high.clone(), ae_mempool::pool::Event::Received, false)
        .unwrap()
        .await
        .unwrap();
    pool.delete(high.hash()).unwrap().await.unwrap();
    let body = json!({"tx":api::encode(ApiType::Transaction,&tx.encode())}).to_string();
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(format!("POST /v2/transactions HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nExpect: 100-continue\r\n\r\n",body.len()).as_bytes()).await.unwrap();
    within(socket.read_exact(&mut interim)).await.unwrap();
    assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");
    for byte in body.bytes() {
        socket.write_all(&[byte]).await.unwrap();
    }
    let mut response = vec![];
    within(socket.read_to_end(&mut response)).await.unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert_eq!(
        get(
            address,
            &format!(
                "/v3/transactions/{}",
                api::encode(ApiType::TxHash, tx.hash().as_ref())
            )
        )
        .await
        .0,
        200
    );
    assert!(within(running.shutdown()).await.unwrap().status().joined);
    assert!(http.status().joined);
}
