use super::*;
use ae_sync::chain::hyperchain::{configuration as startup, parent as utility};
fn number(n: &Unsigned) -> u64 {
    num_bigint::BigUint::from_bytes_be(n.as_bytes())
        .try_into()
        .unwrap()
}
fn configuration(node: &Node, chain: &str, enabled: bool) -> serde_json::Value {
    let rules = &node.config.rules.consensus;
    let stakers:Vec<_>=[12u8,13,14].into_iter().map(|n| {
        let mut seed=[0u8;32];seed[31]=n;
        let key=signer(n as u64).public_key();
        json!({"hyper_chain_account":{"pub":api::encode(ApiType::AccountPubkey,&key.0),"priv":hex::encode([seed.as_slice(),key.0.as_slice()].concat())}})
    }).collect();
    json!({"stakers":stakers,"child_block_time":number(&rules.child_block_time),"child_block_production_time":1,
        "election_contract":api::encode(ApiType::ContractPubkey,&node.config.profile.contracts.election),
        "parent_pin_sync_margin":number(&rules.pin_sync_margin),"default_pinning_behavior":enabled,
        "parent_chain":{"start_height":number(&rules.parent_start),"parent_epoch_length":number(&rules.parent_epoch_length),"finality":number(&rules.parent_finality),
            "consensus":{"type":chain,"network_id":"parent_network"}}})
}
fn parsed(v: &serde_json::Value) -> startup::Configuration {
    startup::Configuration::parse(&serde_json::to_vec(v).unwrap()).unwrap()
}
#[test]
fn configured_conductor_starts_empty_parent_all_callback_types_and_closes_utilities() {
    for (kind, chain, signer) in [
        (
            "AE2AE",
            parent_pins::Chain::Aeternity,
            utility::Signer::PresetKeys,
        ),
        ("AE2BTC", parent_pins::Chain::Bitcoin, utility::Signer::None),
        (
            "AE2DOGE",
            parent_pins::Chain::Dogecoin,
            utility::Signer::None,
        ),
    ] {
        let node = Node::new(0);
        let prepared = parsed(&configuration(&node, kind, false))
            .configure(
                node.config.clone(),
                config(&node, false),
                Default::default(),
            )
            .unwrap();
        assert_eq!(prepared.runtime().parent.hosts, 0);
        assert_eq!(
            prepared.runtime().parent.connector.fetch_interval,
            Some(Duration::from_millis(500))
        );
        let c = prepared
            .start_with_targets(node.db.clone(), Targets)
            .unwrap();
        assert!(c.pinning().is_none());
        let client = c.parent_utilities().unwrap();
        assert_eq!(
            client.metadata().unwrap(),
            utility::Metadata {
                chain,
                network: Arc::from(b"parent_network".as_slice()),
                signer
            }
        );
        assert!(
            !client
                .has_parent_account(super::signer(12).public_key())
                .unwrap()
        );
        assert!(matches!(
            client.pin_by_hash(b"hash"),
            Err(utility::Error::Parent(parent_pins::Error::NoParents))
        ));
        assert!(matches!(
            client.post(super::signer(12).public_key(), 1.into(), 1.into()),
            Err(utility::Error::MissingAccount(_))
        ));
        let s = shutdown(c);
        assert!(s.owner.errors.is_empty());
        drop(s);
        assert!(client.metadata().is_err());
        assert!(
            client
                .has_parent_account(super::signer(12).public_key())
                .is_err()
        );
    }
}
#[test]
fn configuration_rejects_each_consensus_binding_mismatch_before_starting_children() {
    let node = Node::new(0);
    let base = configuration(&node, "AE2AE", true);
    for field in [
        "child_block_time",
        "parent_pin_sync_margin",
        "start_height",
        "parent_epoch_length",
        "finality",
        "election_contract",
    ] {
        let mut v = base.clone();
        match field {
            "election_contract" => v[field] = json!(api::encode(ApiType::ContractPubkey, &[5; 32])),
            "child_block_time" | "parent_pin_sync_margin" => {
                v[field] = json!(v[field].as_u64().unwrap() + 1)
            }
            _ => v["parent_chain"][field] = json!(v["parent_chain"][field].as_u64().unwrap() + 1),
        }
        assert!(
            matches!(
                parsed(&v).configure(
                    node.config.clone(),
                    config(&node, false),
                    Default::default()
                ),
                Err(startup::Error::Binding)
            ),
            "{field}"
        );
    }
    let mut v = base.clone();
    v["parent_chain"]
        .as_object_mut()
        .unwrap()
        .remove("parent_epoch_length");
    assert!(matches!(
        parsed(&v).configure(
            node.config.clone(),
            config(&node, false),
            Default::default()
        ),
        Err(startup::Error::Binding)
    ));
    let c = parsed(&base)
        .configure(
            node.config.clone(),
            config(&node, false),
            Default::default(),
        )
        .unwrap()
        .start_with_targets(node.db.clone(), Targets)
        .unwrap();
    assert!(c.pinning().is_some());
    shutdown(c);
}
#[tokio::test]
async fn parent_utility_queries_selected_epoch_posts_mapped_account_and_retains_receipt() {
    let node = Node::new(0);
    let wallet = Wallet::new();
    let c = configured(&node, &wallet, |cfg| {
        cfg.parent.pins.as_mut().unwrap().agent = None
    });
    let client = c.parent_utilities().unwrap();
    assert!(client.has_parent_account(signer(12).public_key()).unwrap());
    assert!(!client.has_parent_account(signer(15).public_key()).unwrap());
    assert!(matches!(
        client.post(signer(15).public_key(), 1.into(), 2.into()),
        Err(utility::Error::MissingAccount(_))
    ));
    assert_eq!(wallet.requests.load(Ordering::Acquire), 0);
    let native: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/hc_configuration.json"
    )))
    .unwrap();
    let expected = &native["utilities"][0]["data"];
    for block in node.blocks().into_iter().take(6) {
        within(
            c.importer()
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
    }
    let data = client.pinning_data().unwrap();
    assert_eq!(
        data.pin.epoch,
        num_bigint::BigInt::from(expected["epoch"].as_i64().unwrap())
    );
    assert_eq!(
        data.pin.height,
        num_bigint::BigInt::from(expected["height"].as_i64().unwrap())
    );
    assert_eq!(
        data.pin.block_hash,
        node.db.view().unwrap().canonical_key(3).unwrap().unwrap().0
    );
    assert_eq!(
        ae_hyperchain::pin::decode_parent(&data.payload, 4096).unwrap(),
        data.pin
    );
    assert_eq!(
        data.selected_tip,
        node.db.view().unwrap().status().tip.unwrap()
    );
    assert_eq!(
        data.selected_revision,
        node.db.view().unwrap().status().revision
    );
    let mut receipt = client
        .post(signer(12).public_key(), 7.into(), 9.into())
        .unwrap();
    let mut outcome = None;
    wait(|| {
        outcome = receipt.try_result();
        outcome.is_some()
    });
    let Some(parent_pins::posting::Outcome::Submitted { hash, submission }) = outcome else {
        panic!()
    };
    let sent = wallet.posted.lock().unwrap()[0].clone();
    assert_eq!(sent.transaction().sender(), signer(99).public_key());
    assert_eq!(sent.transaction().amount(), &Unsigned::from(7));
    assert_eq!(sent.transaction().fee(), &Unsigned::from(9));
    assert_eq!(sent.transaction().payload(), data.payload);
    assert_eq!(submission.transaction.as_ref(), sent.encode());
    let mut lookup = client.pin_by_hash(&hash).unwrap();
    let mut found = None;
    wait(|| {
        found = lookup.try_result();
        found.is_some()
    });
    assert_eq!(found.unwrap().unwrap().unwrap().epoch, data.pin.epoch);
    // A micro at height four has no selected key four yet. Native Top has the
    // same failure; it must not silently substitute key three's epoch.
    within(
        c.importer()
            .import_block(node.blocks()[6].encode().unwrap(), 1_000_000)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    assert_eq!(
        client.pinning_data(),
        Err(utility::Error::State("current epoch"))
    );
    shutdown(c);
    assert!(client.pinning_data().is_err());
    assert_eq!(wallet.posted.lock().unwrap().len(), 1);
}
#[test]
fn parent_metadata_network_mismatch_fails_and_releases_constructor_ownership() {
    let node = Node::new(0);
    let wallet = Wallet::new();
    let mut cfg = super::configuration(&node, &wallet);
    cfg.parent.pins.as_mut().unwrap().network = b"wrong_network".to_vec();
    assert!(matches!(
        new(&node, cfg),
        Err(conductor::Error::Utilities(utility::Error::Configuration))
    ));
    shutdown(configured(&node, &wallet, |cfg| {
        cfg.parent.pins.as_mut().unwrap().agent = None
    }));
}
#[tokio::test]
async fn parsed_configuration_connects_real_parent_headers_and_signed_pin_posting() {
    let node = Node::new(1);
    let wallet = Wallet::new();
    let mut value = configuration(&node, "AE2AE", false);
    let mut seed = [0u8; 32];
    seed[31] = 99;
    let parent = signer(99).public_key();
    value["pinners"] = json!([{"parent_chain_account":{"pub":api::encode(ApiType::AccountPubkey,&parent.0),"priv":hex::encode([seed.as_slice(),parent.0.as_slice()].concat()),"owner":api::encode(ApiType::AccountPubkey,&signer(12).public_key().0)}}]);
    value["parent_chain"]["polling"] = json!({"nodes":[format!("{}ignored/path?query=1#fragment",wallet.node.origin())],"fetch_interval":10000});
    let prepared = parsed(&value)
        .configure(node.config.clone(), config(&node, true), Default::default())
        .unwrap();
    assert_eq!(prepared.runtime().parent.hosts, 1);
    let mut c = prepared.start(node.db.clone()).unwrap();
    // The live AE RPC adapter uses the parent's encoded kh_ hash as entropy.
    // The older native fixture uses raw hash bytes; produce against this real
    // configured backend so both production and admission use the same facts.
    let mut produced = 0;
    let mut wall = 1000;
    let mut clock = Instant::now();
    let end = clock + Duration::from_secs(25);
    while produced < 3 {
        assert!(Instant::now() < end, "{:?}", c.status());
        tick(&mut c, &mut wall, &mut clock);
        if let Some(output) = c.take_output() {
            match output {
                Output::Attempt(done) => {
                    if matches!(done.outcome, production::Outcome::Produced { .. }) {
                        produced += 1;
                    }
                }
                other => panic!("{other:?}"),
            }
        }
    }
    c.stop_production().unwrap();
    let client = c.parent_utilities().unwrap();
    let mut request = client
        .post(signer(12).public_key(), 3.into(), 5.into())
        .unwrap();
    let mut outcome = None;
    wait(|| {
        outcome = request.try_result();
        outcome.is_some()
    });
    assert!(matches!(
        outcome,
        Some(parent_pins::posting::Outcome::Submitted { .. })
    ));
    assert_eq!(wallet.posted.lock().unwrap().len(), 1);
    assert_eq!(
        wallet.posted.lock().unwrap()[0].transaction().sender(),
        parent
    );
    shutdown(c);
    assert!(client.metadata().is_err());
}
