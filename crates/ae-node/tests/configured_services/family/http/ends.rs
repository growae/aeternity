use super::*;
use ae_chain::{
    block::Block,
    repository::{self, pow::key},
};

async fn check(address: SocketAddr, expected: &[Hash256]) {
    let mut expected = expected.to_vec();
    expected.sort();
    let encoded = json!(
        expected
            .iter()
            .map(|h| api::encode(ApiType::KeyBlockHash, h.as_ref()))
            .collect::<Vec<_>>()
    );
    for path in [
        "/v2/status/chain-ends",
        "/v3/status/chain-ends",
        "/v3/status/chain-ends?int-as-string=true",
    ] {
        let r = get(address, path).await;
        assert_eq!(r.0, 200, "{path}: {}", r.2);
        assert_eq!(r.2, encoded, "{path}");
    }
}

#[tokio::test]
async fn chain_ends_http_tracks_real_forks_reorg_micro_blocks_rollback_and_reopen() {
    let lab = Lab::new();
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = prepared.configured().chain().database().clone();
    let identity = db.view().unwrap().identity().clone();
    let execution = prepared.configured().execution().execution().clone();
    let rules = prepared.pow().unwrap().rules().clone();
    let tx = transfer(prepared.configured());
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let importer = h.importer().unwrap();
    let mut cfg = config();
    cfg.groups = ["external".into()].into();
    let http = running.listen_http(cfg).await.unwrap();
    let address = http.status().address;
    check(address, &[identity.genesis]).await;
    info::mine(&h, 1, Kind::Key).await;
    let first = importer
        .current_header(0, |h| h)
        .unwrap()
        .wait()
        .await
        .unwrap()
        .selected
        .key;
    let miner = match db.view().unwrap().require_header(first).unwrap() {
        ae_codec::headers::Header::Key(k) => AccountPublicKey(k.miner),
        _ => panic!(),
    };
    h.pow()
        .unwrap()
        .workers()
        .unwrap()
        .bindings()
        .admission
        .current()
        .unwrap()
        .client
        .push(tx.clone(), ae_mempool::pool::Event::Created, false)
        .unwrap()
        .await
        .unwrap();
    info::mine(&h, 1, Kind::Micro).await;
    assert!(db.view().unwrap().location(tx.hash()).unwrap().is_some());
    check(address, &[first]).await;
    info::mine(&h, 2, Kind::Key).await;
    let canonical = db.view().unwrap().status().tip.unwrap();
    check(address, &[canonical]).await;
    let mut parent = identity.genesis;
    let mut fork = Vec::new();
    for height in 1..=4u64 {
        let time = height * 20_000;
        let block = {
            let view = db.view().unwrap();
            let candidate = execution
                .with_config(|execution| {
                    key::prepare(
                        &view,
                        &identity,
                        execution,
                        &rules,
                        key::Input {
                            parent,
                            production: ae_chain::key::candidate::Input {
                                miner,
                                beneficiary: AccountPublicKey([84; 32]),
                                calculation_time: time,
                                release_info: 0,
                            },
                        },
                        Default::default(),
                    )
                })
                .unwrap();
            let mut header = candidate.finish(time).unwrap();
            ae_consensus::on_demand::seal(&mut header);
            Block::Key(header)
        };
        let hash = block.hash().unwrap();
        importer
            .import_block(block.encode().unwrap(), u64::MAX - 600_000)
            .unwrap()
            .wait()
            .await
            .unwrap();
        parent = hash;
        fork.push(hash);
        check(address, &[canonical, hash]).await;
        if height < 3 {
            assert_eq!(db.view().unwrap().status().tip, Some(canonical));
        }
    }
    assert_eq!(db.view().unwrap().status().tip, Some(parent));
    assert!(db.view().unwrap().location(tx.hash()).unwrap().is_none());
    // Extending the selected branch does not erase the competing branch end.
    let held = importer.chain_ends(1, 0, |h| h).unwrap().wait().await;
    assert!(matches!(held, Err(ae_sync::chain::Error::Busy)));
    let capacity = importer.available();
    let pressure = importer.service_work(capacity.1).unwrap();
    let busy = get(address, "/v3/status/chain-ends").await;
    assert_eq!(busy.0, 503);
    assert_eq!(busy.1["retry-after"], "3");
    drop(pressure);
    let workers = h.pow().unwrap().workers().unwrap();
    let generation = workers.status().admission_generation;
    let command = h.commands().unwrap().number;
    workers
        .restart(workers::Child::Admission, generation)
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
    assert!(Arc::ptr_eq(&importer, &h.importer().unwrap()));
    drop(workers);
    check(address, &[canonical, parent]).await;
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(http.status().joined);
    drop(exit);
    drop(h);
    drop(importer);
    drop(db);
    let prepared = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = prepared.configured().chain().database().clone();
    let mut running = prepared
        .launch(commands(&lab), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let http = running.listen_http(config()).await.unwrap();
    check(http.status().address, &[canonical, parent]).await;
    db.rollback(repository::rollback::Target::Height(1), Default::default())
        .unwrap();
    check(http.status().address, &[first, fork[0]]).await;
    db.rollback(repository::rollback::Target::Height(0), Default::default())
        .unwrap();
    check(http.status().address, &[identity.genesis]).await;
    let exit = within(running.shutdown()).await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
}
