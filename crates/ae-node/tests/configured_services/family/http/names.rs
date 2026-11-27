use super::*;
use ae_codec::name_pointer::{Pointer, PointerValue};
use ae_state::names::hashing;
use ae_tx::{
    CanonicalTransaction, NameOperation, NameTransaction, SignedTransaction, Transaction,
    TransactionLeaf,
};
use ae_types::{Id, IdTag, Unsigned};

fn signed(
    open: &configured::Open,
    nonce: u64,
    operation: NameOperation,
) -> Arc<SignedTransaction<Transaction>> {
    let mut tx = NameTransaction::new(payer().public_key(), operation).unwrap();
    tx.set_nonce(nonce.into());
    tx.set_fee(100_000_000_000_000u64.into());
    let mut bytes = open.chain().settings().profile().signing_prefix(false);
    bytes.extend(tx.encode());
    Arc::new(SignedTransaction::new(
        Transaction::new(TransactionLeaf::Name(tx)),
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
async fn configured_auction_timeout_changes_mined_state_and_binds_database_reopen() {
    for timeout in [0, 17] {
        let lab = Lab::new();
        lab.write("alloc.json",serde_json::to_vec(&json!({api::encode(ApiType::AccountPubkey,payer().public_key().as_ref()):10_000_000_000_000_000_000u64})).unwrap());
        let load = |value: Option<u64>| {
            let mut value_json = json!({"fork_management":{"network_id":"ae_auction_override"},"system":{"dev_mode":true,"custom_prefunded_accs_file":"alloc.json"},"chain":{"hard_forks":{"1":0,"6":1},"consensus":{"0":{"type":"on_demand"}}}});
            if let Some(value) = value {
                value_json["mining"] = json!({"name_claim_bid_timeout":value});
            }
            let cfg = Configuration::from_value(value_json).unwrap();
            let ctx = Context::new(Directories {
                home: lab.0.display().to_string(),
                data: "data".into(),
                log: "log".into(),
            })
            .unwrap();
            configured::Prepared::load(
                &cfg.resolve(&ctx).unwrap(),
                None,
                &lab.0,
                &empty(),
                &Specification::from_value(&profile::default_value()).unwrap(),
                Default::default(),
                &6_000_000.into(),
                Default::default(),
                Default::default(),
            )
            .unwrap()
        };
        let opened = load(Some(timeout))
            .open("chain", Default::default())
            .unwrap();
        let identity = opened.chain().identity().clone();
        let db = opened.chain().database().clone();
        let name = b"auctionquery.chain";
        let second = b"replacement-long-auction-name.chain";
        let fee = ae_protocol::names::claim_fee(
            (name.len() - 6).try_into().unwrap(),
            ae_protocol::Protocol::Ceres,
        );
        let claim = signed(
            &opened,
            1,
            NameOperation::Claim {
                name: name.to_vec(),
                salt: 0.into(),
                name_fee: Some(fee.clone()),
            },
        );
        let rebid = signed(
            &opened,
            2,
            NameOperation::Claim {
                name: name.to_vec(),
                salt: 0.into(),
                name_fee: Some(fee.mul_u64(2)),
            },
        );
        let next = signed(
            &opened,
            if timeout == 0 { 2 } else { 3 },
            NameOperation::Claim {
                name: second.to_vec(),
                salt: 0.into(),
                name_fee: Some(ae_protocol::names::claim_fee(
                    (second.len() - 6).try_into().unwrap(),
                    ae_protocol::Protocol::Ceres,
                )),
            },
        );
        let p = node::Prepared::new(opened, Default::default()).unwrap();
        let mut running = p.launch(commands(&lab), || Ok(Default::default())).unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        let mut cfg = config();
        cfg.groups = ["name_service".into()].into();
        let http = running.listen_http(cfg).await.unwrap();
        let address = http.status().address;
        info::mine(&h, 1, Kind::Key).await;
        admit(&h, claim.clone()).await;
        info::mine(&h, 1, Kind::Micro).await;
        assert!(db.view().unwrap().location(claim.hash()).unwrap().is_some());
        let path = format!(
            "/v3/{}/{}",
            if timeout == 0 { "names" } else { "auctions" },
            std::str::from_utf8(name).unwrap()
        );
        let first = get(address, &path).await;
        assert_eq!(first.0, 200, "{:?}", first.2);
        if timeout == 0 {
            assert_eq!(first.2["ttl"], 180001);
        } else {
            assert_eq!(first.2["ends_at"], 18);
            admit(&h, rebid.clone()).await;
            info::mine(&h, 1, Kind::Micro).await;
            assert!(db.view().unwrap().location(rebid.hash()).unwrap().is_some());
            assert_eq!(get(address, &path).await.2["ends_at"], 241);
        }
        let workers = h.pow().unwrap().workers().unwrap();
        let generation = workers.status().admission_generation;
        let command = h.commands().unwrap().number;
        workers
            .restart(ae_sync::runtime::workers::Child::Admission, generation)
            .unwrap()
            .wait()
            .await
            .unwrap();
        within(async {
            while !h.status().ready || h.commands().is_none_or(|c| c.number <= command) {
                h.changed().await.unwrap();
            }
        })
        .await;
        admit(&h, next.clone()).await;
        info::mine(&h, 1, Kind::Micro).await;
        assert!(db.view().unwrap().location(next.hash()).unwrap().is_some());
        let second_path = path.replace(
            std::str::from_utf8(name).unwrap(),
            std::str::from_utf8(second).unwrap(),
        );
        let second = get(address, &second_path).await;
        assert_eq!(second.0, 200);
        assert_eq!(
            second.2[if timeout == 0 { "ttl" } else { "ends_at" }],
            if timeout == 0 { 180001 } else { 18 }
        );
        let top = db.view().unwrap().status().tip;
        let exit = within(running.shutdown()).await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        drop((exit, h, http, workers, db));
        for changed in [None, Some(timeout + 1)] {
            assert!(matches!(
                load(changed).open("chain", Default::default()),
                Err(configured::Error::Genesis(
                    ae_node::bootstrap::genesis::Error::Database(
                        ae_node::bootstrap::Error::Repository(
                            ae_chain::repository::Error::Identity
                        )
                    )
                ))
            ));
        }
        let reopened = load(Some(timeout))
            .open("chain", Default::default())
            .unwrap();
        assert_eq!(reopened.chain().identity(), &identity);
        assert_eq!(
            reopened.chain().database().view().unwrap().status().tip,
            top
        );
    }
}
#[tokio::test]
async fn name_http_tracks_claim_update_revoke_auction_bids_replacement_pressure_and_rollback() {
    let lab = Lab::new();
    lab.write("alloc.json",serde_json::to_vec(&json!({api::encode(ApiType::AccountPubkey,payer().public_key().as_ref()):10_000_000_000_000_000_000u64})).unwrap());
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = prepared.configured().chain().database().clone();
    let name = b"http-name-compatibility-example.chain";
    let auction_name = b"auctionentry.chain";
    let hash = hashing::name_hash(name).0;
    let auction_hash = hashing::name_hash(auction_name).0;
    let name_id = api::encode(ApiType::Name, &hash);
    let auction_id = api::encode(ApiType::Name, &auction_hash);
    let owner = api::encode(ApiType::AccountPubkey, payer().public_key().as_ref());
    let fee = |label: usize| {
        ae_protocol::names::claim_fee(label.try_into().unwrap(), ae_protocol::Protocol::Ceres)
    };
    let bid = fee(12);
    let overbid = bid.mul_u64(2);
    let preclaim = signed(
        prepared.configured(),
        1,
        NameOperation::Preclaim {
            commitment: hashing::commitment_hash(name, &7.into()).unwrap().0,
        },
    );
    let claim = signed(
        prepared.configured(),
        2,
        NameOperation::Claim {
            name: name.to_vec(),
            salt: 7.into(),
            name_fee: Some(fee(name.len() - 6)),
        },
    );
    let auction = signed(
        prepared.configured(),
        3,
        NameOperation::Claim {
            name: auction_name.to_vec(),
            salt: 0.into(),
            name_fee: Some(bid.clone()),
        },
    );
    let pointer_key = vec![b'r', 0, 255];
    let pointer_data = vec![0, 255, 42];
    let update = signed(
        prepared.configured(),
        4,
        NameOperation::Update {
            name: hash,
            name_ttl: 100.into(),
            client_ttl: 7.into(),
            pointers: vec![
                Pointer {
                    key: b"account_pubkey".to_vec(),
                    value: PointerValue::Id(Id::new(IdTag::Account, payer().public_key().0)),
                },
                Pointer {
                    key: pointer_key.clone(),
                    value: PointerValue::Data(pointer_data.clone()),
                },
            ],
        },
    );
    let rebid = signed(
        prepared.configured(),
        5,
        NameOperation::Claim {
            name: auction_name.to_vec(),
            salt: 0.into(),
            name_fee: Some(overbid.clone()),
        },
    );
    let revoke = signed(
        prepared.configured(),
        6,
        NameOperation::Revoke { name: hash },
    );
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut cfg = config();
    cfg.groups = ["name_service".into()].into();
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
    let path = format!("/v3/names/{}", std::str::from_utf8(name).unwrap());
    let hash_path = format!("/v3/names/hash/{name_id}");
    let auction_path = format!(
        "/v3/auctions/{}",
        std::str::from_utf8(auction_name).unwrap()
    );
    for p in [&path, &hash_path, &auction_path] {
        assert_eq!(get(address, p).await.0, 404);
    }
    info::mine(&h, 1, Kind::Key).await;
    admit(&h, preclaim.clone()).await;
    info::mine(&h, 1, Kind::Micro).await;
    assert!(
        db.view()
            .unwrap()
            .location(preclaim.hash())
            .unwrap()
            .is_some()
    );
    info::mine(&h, 1, Kind::Key).await;
    for tx in [&claim, &auction] {
        admit(&h, tx.clone()).await;
    }
    info::mine(&h, 1, Kind::Micro).await;
    for tx in [&claim, &auction] {
        assert!(db.view().unwrap().location(tx.hash()).unwrap().is_some());
    }
    let initial = get(address, &path).await;
    assert_eq!(initial.0, 200, "{}", initial.2);
    assert_eq!(
        initial.2,
        json!({"id":name_id,"owner":owner,"ttl":180002,"pointers":[]})
    );
    assert_eq!(get(address, &hash_path).await.2, initial.2);
    assert_eq!(
        initial.1["access-control-allow-origin"],
        "https://client.example"
    );
    let number = |n: &Unsigned| {
        serde_json::from_str::<Value>(&num_bigint::BigUint::from_bytes_be(n.as_bytes()).to_string())
            .unwrap()
    };
    let initial_auction = get(address, &auction_path).await;
    assert_eq!(initial_auction.0, 200, "{}", initial_auction.2);
    assert_eq!(
        initial_auction.2,
        json!({"id":auction_id,"highest_bidder":owner,"highest_bid":number(&bid),"started_at":2,"ends_at":482})
    );
    assert_eq!(get(address, "/v3/names/auctionentry.chain").await.0, 404);
    admit(&h, update.clone()).await;
    info::mine(&h, 1, Kind::Micro).await;
    let update_top = {
        let view = db.view().unwrap();
        assert!(view.location(update.hash()).unwrap().is_some());
        view.status().tip.unwrap()
    };
    let updated = get(address, &path).await;
    assert_eq!(updated.0, 200, "{}", updated.2);
    assert_eq!(updated.2["ttl"], 102);
    assert_eq!(updated.2["pointers"][0]["id"], owner);
    assert_eq!(
        updated.2["pointers"][1]["encoded_key"],
        api::encode(ApiType::Bytearray, &pointer_key)
    );
    assert_eq!(
        updated.2["pointers"][1]["id"],
        api::encode(ApiType::Bytearray, &pointer_data)
    );
    assert!(updated.2.get("client_ttl").is_none());
    assert_eq!(get(address, &hash_path).await.2, updated.2);
    assert_eq!(get(address, &path.replace("v3", "v2")).await.2, updated.2);
    assert_eq!(
        get(address, &format!("{path}?int-as-string=true")).await.2["ttl"],
        "102"
    );
    assert_eq!(
        get(address, &format!("{auction_path}?int-as-string=true"))
            .await
            .2["highest_bid"],
        number(&bid).to_string()
    );
    info::mine(&h, 1, Kind::Key).await;
    for tx in [&rebid, &revoke] {
        admit(&h, tx.clone()).await;
    }
    info::mine(&h, 1, Kind::Micro).await;
    for tx in [&rebid, &revoke] {
        assert!(db.view().unwrap().location(tx.hash()).unwrap().is_some());
    }
    for p in [&path, &hash_path] {
        let result = get(address, p).await;
        assert_eq!(result.0, 404);
        assert_eq!(result.2["error_code"], "name_revoked");
    }
    let latest = get(address, &auction_path).await;
    assert_eq!(latest.0, 200, "{}", latest.2);
    assert_eq!(
        latest.2,
        json!({"id":auction_id,"highest_bidder":owner,"highest_bid":number(&overbid),"started_at":3,"ends_at":482})
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
    assert_eq!(get(address, &auction_path).await.2, latest.2);
    let importer = h.importer().unwrap();
    let capacity = importer.available();
    let permit = importer.name_query(48 << 20).unwrap();
    assert!(importer.available().0 < capacity.0);
    drop(permit);
    assert_eq!(importer.available(), capacity);
    assert!(matches!(
        importer.name_query(48 << 20).unwrap().submit(
            vec![0; 4097],
            ae_chain::name::Kind::Name,
            |r| r
        ),
        Err(ae_sync::chain::Error::Busy)
    ));
    assert_eq!(importer.available(), capacity);
    let held = importer
        .name_query(48 << 20)
        .unwrap()
        .submit(auction_name.to_vec(), ae_chain::name::Kind::Auction, |r| r)
        .unwrap()
        .wait_retained()
        .await
        .unwrap();
    assert!(matches!(held.value, Ok(ae_chain::name::Reply::Auction(_))));
    assert!(importer.available().0 < capacity.0);
    drop(held);
    assert_eq!(importer.available(), capacity);
    let mut held = vec![];
    while let Ok(ticket) = importer.current() {
        held.push(ticket);
        assert!(held.len() < 64);
    }
    for p in ["/v3/names/bad", "/v3/names/hash/bad", "/v3/auctions/bad"] {
        assert_eq!(get(address, p).await.0, 503);
    }
    let invalid = request(
        address,
        "GET /v3/names/bad?ignored=%GG HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(invalid.0, 400);
    assert!(invalid.2.is_empty());
    assert!(!invalid.1.contains_key("x-ae-height"));
    drop(held);
    within(async {
        while importer.available() != capacity {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(get(address, "/v3/names/hash/bad").await.0, 400);
    let absent = api::encode(ApiType::Name, &[1; 33]);
    assert_eq!(
        get(address, &format!("/v3/names/hash/{absent}")).await.0,
        404
    );
    let raw_auction = api::encode(ApiType::Name, &hashing::auction_hash(auction_hash));
    assert_eq!(
        get(address, &format!("/v3/names/hash/{raw_auction}"))
            .await
            .0,
        500
    );
    let v2hash = request(
        address,
        &format!(
            "GET /v2/names/hash/{name_id} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert_eq!(v2hash.0, 404);
    db.rollback(
        ae_chain::repository::rollback::Target::Micro(update_top),
        Default::default(),
    )
    .unwrap();
    assert_eq!(get(address, &path).await.2, updated.2);
    assert_eq!(get(address, &hash_path).await.2, updated.2);
    assert_eq!(get(address, &auction_path).await.2, initial_auction.2);
    db.rollback(
        ae_chain::repository::rollback::Target::Height(0),
        Default::default(),
    )
    .unwrap();
    for p in [&path, &hash_path, &auction_path] {
        assert_eq!(get(address, p).await.0, 404);
    }
    assert!(running.shutdown().await.unwrap().status().joined);
    assert!(http.status().joined);
}
