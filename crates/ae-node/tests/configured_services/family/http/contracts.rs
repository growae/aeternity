use super::*;
use ae_state::transport::Proofs;

#[tokio::test]
async fn contract_http_executes_real_code_verifies_complete_proofs_and_tracks_rollback_and_replacement()
 {
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = prepared.configured().chain().database().clone();
    let tx = info::contract(prepared.configured());
    let key = ae_state::contracts::contract_id(
        payer().public_key(),
        ae_state::contracts::Nonce::Number(&1.into()),
    );
    let encoded = api::encode(ApiType::ContractPubkey, key.as_ref());
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let mut cfg = config();
    cfg.groups.insert("contract".into());
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
    for suffix in ["", "/code", "/poi"] {
        assert_eq!(
            get(address, &format!("/v3/contracts/{encoded}{suffix}"))
                .await
                .0,
            404
        );
    }
    info::mine(&h, 1, Kind::Key).await;
    let workers = h.pow().unwrap().workers().unwrap();
    workers
        .bindings()
        .admission
        .current()
        .unwrap()
        .client
        .push(tx, ae_mempool::pool::Event::Created, false)
        .unwrap()
        .await
        .unwrap();
    info::mine(&h, 1, Kind::Micro).await;
    let root = {
        let view = db.view().unwrap();
        view.require_header(view.status().tip.unwrap())
            .unwrap()
            .common()
            .state_hash
    };
    let mut expected = None;
    for version in ["v2", "v3"] {
        let path = format!("/{version}/contracts/{encoded}");
        let meta = get(address, &path).await;
        assert_eq!(meta.0, 200, "{}", meta.2);
        assert_eq!(meta.2["id"], encoded);
        assert_eq!(
            meta.2["owner_id"],
            api::encode(ApiType::AccountPubkey, &payer().public_key().0)
        );
        assert_eq!(
            meta.1["access-control-allow-origin"],
            "https://client.example"
        );
        let code = get(address, &format!("{path}/code")).await;
        assert_eq!(code.0, 200, "{}", code.2);
        let poi = get(address, &format!("{path}/poi")).await;
        assert_eq!(poi.0, 200, "{}", poi.2);
        let bytes = api::decode_as(ApiType::Poi, poi.2["poi"].as_str().unwrap()).unwrap();
        let proof = Proofs::deserialize(&bytes, Default::default()).unwrap();
        assert_eq!(proof.hash().0, root);
        let (contract, contents) = proof
            .lookup_contract(key.0, Default::default())
            .unwrap()
            .unwrap();
        assert!(!contents.is_empty());
        proof
            .verify_contract(key.0, &contract, &contents, Default::default())
            .unwrap();
        let account = proof
            .lookup_account(ae_types::AccountPublicKey(key.0), Default::default())
            .unwrap()
            .unwrap();
        proof
            .verify_account(account.public_key(), &account, Default::default())
            .unwrap();
        let ae_state::contracts::Code::Inline(stored) = contract.code else {
            panic!("inline create")
        };
        assert_eq!(
            api::decode_as(
                ApiType::ContractBytearray,
                code.2["bytecode"].as_str().unwrap()
            )
            .unwrap(),
            stored
        );
        assert_eq!(meta.2["vm_version"], contract.version.vm);
        assert_eq!(meta.2["abi_version"], contract.version.abi);
        let current = (meta.2, code.2, poi.2);
        if let Some(expected) = &expected {
            assert_eq!(&current, expected);
        } else {
            expected = Some(current);
        }
    }
    let expected = expected.unwrap();
    let path = format!("/v3/contracts/{encoded}");
    let strings = get(address, &format!("{path}?int-as-string=true")).await;
    assert_eq!(
        strings.2["vm_version"],
        expected.0["vm_version"].to_string()
    );
    assert_eq!(strings.2["deposit"], expected.0["deposit"].to_string());
    assert_eq!(strings.2["active"], expected.0["active"]);
    assert_eq!(
        get(
            address,
            &format!("/v2/contracts/{encoded}?int-as-string=true")
        )
        .await
        .2,
        expected.0
    );
    let generation = workers.status().admission_generation;
    let commands = h.commands().unwrap().number;
    workers
        .restart(ae_sync::runtime::workers::Child::Admission, generation)
        .unwrap()
        .wait()
        .await
        .unwrap();
    within(async {
        while !h.status().ready || h.commands().is_none_or(|c| c.number <= commands) {
            h.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(get(address, &format!("{path}/poi")).await.2, expected.2);
    let importer = h.importer().unwrap();
    let capacity = importer.available();
    let mut held = vec![];
    while let Ok(ticket) = importer.current() {
        held.push(ticket);
        assert!(held.len() < 64);
    }
    for suffix in ["", "/code", "/poi"] {
        assert_eq!(
            get(address, &format!("/v3/contracts/bad{suffix}")).await.0,
            503
        );
    }
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
    for suffix in ["", "/code", "/poi"] {
        assert_eq!(get(address, &format!("{path}{suffix}")).await.0, 404);
    }
    assert!(running.shutdown().await.unwrap().status().joined);
    assert!(http.status().joined);
}
