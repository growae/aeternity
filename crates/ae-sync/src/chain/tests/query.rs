use super::*;

#[tokio::test]
async fn header_queries_fit_beside_a_pending_generation_with_default_budgets() {
    let (_temp, importer) = setup(Default::default());
    let pending_download = importer.reserve().unwrap();
    assert!(matches!(importer.reserve(), Err(Error::Busy)));
    let current = importer.current().unwrap().wait().await.unwrap();
    assert_eq!(
        importer.top_key().unwrap().wait().await.unwrap().selected,
        current
    );
    drop(pending_download);
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn top_key_query_tracks_micro_tops_rollback_and_fixed_network_identity() {
    let (_temp, importer) = setup(Default::default());
    let initial = importer.top_key().unwrap().wait().await.unwrap();
    assert_eq!(
        initial.selected,
        importer.current().unwrap().wait().await.unwrap()
    );
    assert_eq!(initial.genesis, initial.selected.key);
    assert_eq!(initial.network, NetworkProfile::mainnet());
    assert_eq!(initial.algorithm, ae_protocol::ConsensusAlgorithm::Cuckoo);
    assert_eq!(
        Block::Key(initial.header.clone()).hash().unwrap(),
        initial.genesis
    );
    let blocks = fixtures::blocks(&NetworkProfile::mainnet());
    let Block::Key(expected) = &blocks[0] else {
        panic!("fixture begins with key")
    };
    assert!(matches!(&blocks[1], Block::Micro(_)));
    for block in &blocks[..2] {
        importer
            .import_block(block.encode().unwrap(), NOW)
            .unwrap()
            .wait()
            .await
            .unwrap();
        let key = importer.top_key().unwrap().wait().await.unwrap();
        assert_eq!(key.header, *expected);
        assert_eq!(key.selected.top, block.hash().unwrap());
        assert_eq!(key.selected.key, blocks[0].hash().unwrap());
        assert_eq!(
            key.selected,
            importer.current().unwrap().wait().await.unwrap()
        );
        assert_eq!(key.genesis, initial.genesis);
        assert_eq!(key.network, initial.network);
    }
    importer
        .database()
        .rollback(repository::rollback::Target::Height(0), Default::default())
        .unwrap();
    let rolled_back = importer.top_key().unwrap().wait().await.unwrap();
    assert_eq!(rolled_back.header, initial.header);
    assert_eq!(rolled_back.selected.key, initial.genesis);
    assert!(rolled_back.selected.revision > initial.selected.revision);
    importer.stop();
    assert!(matches!(importer.top_key(), Err(Error::Closed)));
    importer.join().await;
}

#[tokio::test]
async fn current_query_tracks_commits_and_rejects_retired_owner() {
    let (_temp, importer) = setup(Default::default());
    let initial = importer.current().unwrap().wait().await.unwrap();
    assert_eq!(initial.height, 0);
    assert_eq!(initial.key, initial.top);
    let block = fixtures::blocks(&NetworkProfile::mainnet()).remove(0);
    importer
        .import_block(block.encode().unwrap(), NOW)
        .unwrap()
        .wait()
        .await
        .unwrap();
    let current = importer.current().unwrap().wait().await.unwrap();
    assert!(current.revision > initial.revision);
    assert_eq!(current.top, block.hash().unwrap());
    assert_eq!(current.height, block.header().common().height);
    assert_eq!(current.key, current.top);
    assert_eq!(importer.cached_height(), Some(current.height));
    importer
        .database()
        .rollback(repository::rollback::Target::Height(0), Default::default())
        .unwrap();
    assert_eq!(importer.cached_height(), None);
    assert_eq!(importer.current().unwrap().wait().await.unwrap().height, 0);
    assert_eq!(importer.cached_height(), Some(0));
    importer.stop();
    assert_eq!(importer.cached_height(), None);
    assert!(matches!(importer.current(), Err(Error::Closed)));
    importer.join().await;
}

#[tokio::test]
async fn current_queries_share_capacity_cancel_queue_and_refuse_recovery() {
    let (_temp, importer) = setup(Config {
        jobs: 2,
        ..Default::default()
    });
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let active = importer
        .jobs
        .submit(1, move |_| {
            entered_tx.send(()).unwrap();
            blocked.recv().unwrap();
        })
        .unwrap();
    entered.await.unwrap();
    let queued = importer.current().unwrap();
    assert_eq!(importer.cached_height(), Some(0));
    assert!(matches!(importer.current(), Err(Error::Busy)));
    assert!(matches!(importer.top_key(), Err(Error::Busy)));
    drop(queued);
    release.send(()).unwrap();
    active.await.unwrap();
    importer.jobs.drain().await;
    assert_eq!(importer.current().unwrap().wait().await.unwrap().height, 0);
    assert_eq!(
        importer
            .top_key()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .selected
            .height,
        0
    );
    *importer.context.recovery.lock().unwrap() = Some(Error::RecoveryRequired);
    assert!(matches!(importer.current(), Err(Error::RecoveryRequired)));
    assert!(matches!(importer.top_key(), Err(Error::RecoveryRequired)));
    importer.stop();
    importer.join().await;
}
