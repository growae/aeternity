use super::*;
use ae_sync::chain::pow::producer::mining;

#[tokio::test]
async fn configured_local_miner_searches_and_admits_the_same_explicit_small_graph() {
    let lab = Lab::new();
    let cfg = Configuration::from_value(json!({
        "fork_management":{"network_id":"ae_configured_services"},
        "system":{"dev_mode":true,"custom_prefunded_accs_file":"alloc.json"},
        "mining":{"cuckoo":{"edge_bits":15}},
        "chain":{"hard_forks":{"1":0,"6":1},"consensus":{"0":{"type":"pow_cuckoo"}}}
    }))
    .unwrap();
    let ctx = Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    let open = configured::Prepared::load(
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
    .open("chain", Default::default())
    .unwrap();
    let rules = open.pow_rules(3000, Default::default());
    assert_eq!(rules.admission.edge_bits.get(), 15);
    let prepared = Prepared::new(open, rules, Default::default(), Default::default()).unwrap();
    let db = prepared.configured().chain().database().clone();
    let snapshots = prepared.snapshots().clone();
    let mut config = root_config(&lab);
    let mut mining = mining::Config::new(payer().public_key());
    mining.edge_bits = ae_consensus::cuckoo::EdgeBits::new(15).unwrap();
    mining.instances[0].repeats = 64;
    mining.instances[0].worker.solver.memory_bytes = 16 * 1024 * 1024;
    config.production.mining = Some(mining);
    let running = root::launch(
        prepared,
        config,
        Arc::new(group::SystemClocks::default()),
        || Ok(Default::default()),
    )
    .unwrap();
    let h = running.handle();
    root_ready(&h, 1).await;
    h.set_enabled(true).unwrap();
    within(async {
        while db.view().unwrap().canonical_key(1).unwrap().is_none() {
            while h.try_output().is_some() {}
            assert!(h.status().failure.is_none(), "{:?}", h.status());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    h.set_enabled(false).unwrap();
    let exit = running.shutdown().await.unwrap();
    assert_root_joined(&exit);
    assert!(exit.status.failure.is_none(), "{:?}", exit.status);
    let view = db.view().unwrap();
    let first = view.canonical_key(1).unwrap().unwrap();
    let ae_chain::block::Block::Key(header) = view.require_block(first).unwrap() else {
        panic!()
    };
    assert_eq!(header.beneficiary, payer().public_key().0);
    assert!(
        ae_consensus::cuckoo::verify_key_seal_with_edge_bits(
            &header,
            ae_consensus::cuckoo::EdgeBits::new(15).unwrap()
        )
        .unwrap()
    );
    assert!(!ae_consensus::cuckoo::verify_key_seal(&header).unwrap());
    assert_eq!(snapshots.live_snapshots(), 0);
}
