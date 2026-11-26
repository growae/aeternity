use super::*;

#[tokio::test]
async fn chain_ends_keep_key_tips_across_micro_blocks_rollback_and_retained_reads() {
    let (_temp, importer) = setup(Config {
        jobs: 1,
        ..Default::default()
    });
    let genesis = importer.database().view().unwrap().identity().genesis;
    let read = |limit| importer.chain_ends(limit, 1024, |h| h).unwrap();
    assert_eq!(read(1).wait().await.unwrap(), vec![genesis]);
    assert!(matches!(read(0).wait().await, Err(Error::Busy)));
    assert!(matches!(
        importer.chain_ends(usize::MAX, 0, |h| h),
        Err(Error::Busy)
    ));
    let blocks = fixtures::blocks(&NetworkProfile::mainnet());
    let capacity = importer.available();
    let mut key = genesis;
    for block in &blocks[..2] {
        importer
            .import_block(block.encode().unwrap(), NOW)
            .unwrap()
            .wait()
            .await
            .unwrap();
        if matches!(block, Block::Key(_)) {
            key = block.hash().unwrap();
        }
        let held = read(1).wait_retained().await.unwrap();
        assert_eq!(held.value, vec![key]);
        assert_eq!(importer.available().0, 0);
        assert!(matches!(importer.chain_ends(1, 0, |h| h), Err(Error::Busy)));
        drop(held);
        assert_eq!(importer.available(), capacity);
    }
    importer
        .database()
        .rollback(repository::rollback::Target::Height(0), Default::default())
        .unwrap();
    assert_eq!(read(1).wait().await.unwrap(), vec![genesis]);
    *importer.context.recovery.lock().unwrap() = Some(Error::RecoveryRequired);
    assert!(matches!(
        importer.chain_ends(1, 0, |h| h),
        Err(Error::RecoveryRequired)
    ));
    importer.stop();
    importer.join().await;
    assert!(matches!(
        importer.chain_ends(1, 0, |h| h),
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn chain_ends_cancelled_render_retains_capacity_until_joined_shutdown() {
    let (_temp, importer) = setup(Config {
        jobs: 1,
        ..Default::default()
    });
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let ticket = importer
        .chain_ends(10, 1024, move |hashes| {
            started.send(()).unwrap();
            blocked.recv().unwrap();
            hashes
        })
        .unwrap();
    within(entered).await.unwrap();
    let held = importer.available();
    drop(ticket);
    assert_eq!(held.0, 0);
    assert_eq!(importer.available(), held);
    importer.stop();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), importer.join())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    within(importer.join()).await;
}

#[tokio::test]
async fn current_header_cancelled_render_keeps_capacity_until_joined_shutdown() {
    let (_temp, importer) = setup(Config {
        jobs: 1,
        ..Default::default()
    });
    let capacity = importer.available();
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let ticket = importer
        .current_header(1 << 20, move |head| {
            started.send(()).unwrap();
            blocked.recv().unwrap();
            head
        })
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    let held = importer.available();
    assert_eq!(held.0, 0);
    assert!(held.1 < capacity.1);
    drop(ticket);
    assert_eq!(importer.available(), held);
    importer.stop();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), importer.join())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), importer.join())
        .await
        .unwrap();
    assert!(matches!(
        importer.current_header(0, |h| h),
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn current_header_rendering_tracks_micro_tops_and_retains_bounded_snapshot_work() {
    let (_temp, importer) = setup(Config {
        jobs: 1,
        ..Default::default()
    });
    let original = importer
        .current_header(1 << 20, |head| head)
        .unwrap()
        .wait()
        .await
        .unwrap();
    let capacity = importer.available();
    assert!(matches!(
        importer.current_header(usize::MAX, |h| h),
        Err(Error::Busy)
    ));
    for block in &fixtures::blocks(&NetworkProfile::mainnet())[..2] {
        importer
            .import_block(block.encode().unwrap(), NOW)
            .unwrap()
            .wait()
            .await
            .unwrap();
        let held = importer
            .current_header(1 << 20, |h| h)
            .unwrap()
            .wait_retained()
            .await
            .unwrap();
        assert_eq!(held.value.version, block.header().common().version);
        assert_eq!(held.value.selected.top, block.hash().unwrap());
        assert_eq!(held.value.network, NetworkProfile::mainnet());
        assert!(matches!(
            importer.current_header(1, |h| h),
            Err(Error::Busy)
        ));
        drop(held);
        assert_eq!(importer.available(), capacity);
    }
    importer
        .database()
        .rollback(repository::rollback::Target::Height(0), Default::default())
        .unwrap();
    let rolled = importer
        .current_header(0, |h| h)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(rolled.version, original.version);
    assert_eq!(rolled.selected.top, original.selected.top);
    assert!(rolled.selected.revision > original.selected.revision);
    *importer.context.recovery.lock().unwrap() = Some(Error::RecoveryRequired);
    assert!(matches!(
        importer.current_header(0, |h| h),
        Err(Error::RecoveryRequired)
    ));
    importer.stop();
    importer.join().await;
    assert!(matches!(
        importer.current_header(0, |h| h),
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn transaction_queries_follow_mainnet_records_and_keep_reply_capacity_until_drop() {
    use crate::chain::query::transactions::{Reply, Request};
    let (_temp, importer) = setup(Config {
        jobs: 1,
        ..Default::default()
    });
    let blocks = fixtures::blocks(&NetworkProfile::mainnet());
    let Block::Micro(micro) = &blocks[1] else {
        panic!()
    };
    let transaction = micro.transactions[0].clone();
    let hash = transaction.hash();
    let query = |request| {
        importer
            .transaction_query(16 << 20)
            .unwrap()
            .submit(request, |reply| reply)
            .unwrap()
    };
    assert_eq!(
        query(Request::Hash(hash)).wait().await.unwrap(),
        Reply::MissingTransaction
    );
    assert_eq!(
        query(Request::Info(hash)).wait().await.unwrap(),
        Reply::MissingTransaction
    );
    for block in &blocks[..2] {
        importer
            .import_block(block.encode().unwrap(), NOW)
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    let block_hash = blocks[1].hash().unwrap();
    let held = query(Request::Hash(hash)).wait_retained().await.unwrap();
    assert_eq!(
        held.value,
        Reply::Transaction {
            transaction: Box::new(transaction.clone()),
            header: Some(blocks[1].header())
        }
    );
    assert_eq!(importer.available(), (0, 0));
    assert!(matches!(importer.current(), Err(Error::Busy)));
    drop(held.value);
    assert!(matches!(importer.transaction_query(1), Err(Error::Busy)));
    drop(held.credits);
    assert_eq!(importer.available(), (1, 128 << 20));
    let held = query(Request::Info(hash)).wait_retained().await.unwrap();
    assert_eq!(
        held.value,
        Reply::Info {
            kind: transaction.transaction().view().kind(),
            info: Err(ae_chain::transaction_info::Error::WithoutInfo)
        }
    );
    assert!(matches!(importer.transaction_query(1), Err(Error::Busy)));
    drop(held);
    assert_eq!(
        query(Request::Micro(block_hash)).wait().await.unwrap(),
        Reply::Transactions {
            transactions: micro.transactions.clone(),
            header: blocks[1].header()
        }
    );
    for index in [None, Some(0), Some(micro.transactions.len() + 1)] {
        assert_eq!(
            query(Request::MicroIndex {
                hash: block_hash,
                index
            })
            .wait()
            .await
            .unwrap(),
            Reply::InvalidIndex
        );
    }
    assert_eq!(
        query(Request::MicroIndex {
            hash: block_hash,
            index: Some(1)
        })
        .wait()
        .await
        .unwrap(),
        Reply::Transaction {
            transaction: Box::new(transaction),
            header: Some(blocks[1].header())
        }
    );
    assert_eq!(
        query(Request::Micro(blocks[0].hash().unwrap()))
            .wait()
            .await
            .unwrap(),
        Reply::MissingBlock
    );
    importer
        .database()
        .rollback(repository::rollback::Target::Height(0), Default::default())
        .unwrap();
    assert_eq!(
        query(Request::Hash(hash)).wait().await.unwrap(),
        Reply::MissingTransaction
    );
    assert_eq!(
        query(Request::Micro(block_hash)).wait().await.unwrap(),
        Reply::MissingBlock
    );
    importer.stop();
    assert!(matches!(importer.transaction_query(0), Err(Error::Closed)));
    importer.join().await;
}

#[tokio::test]
async fn transaction_reads_cancel_before_render_and_join_without_waiting_for_reply_credits() {
    use crate::chain::query::transactions::Request;
    let (_temp, importer) = setup(Config {
        jobs: 2,
        bytes: 256 << 20,
        ..Default::default()
    });
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let active = importer
        .jobs
        .submit(1, move |_| {
            entered_tx.send(()).unwrap();
            wait.recv().unwrap();
        })
        .unwrap();
    entered.await.unwrap();
    let rendered = Arc::new(AtomicBool::new(false));
    let flag = rendered.clone();
    let queued = importer
        .transaction_query(16 << 20)
        .unwrap()
        .submit(Request::Hash(Hash256::ZERO), move |_| {
            flag.store(true, Ordering::SeqCst)
        })
        .unwrap();
    assert!(matches!(importer.current(), Err(Error::Busy)));
    drop(queued);
    release.send(()).unwrap();
    active.await.unwrap();
    importer.jobs.drain().await;
    assert!(!rendered.load(Ordering::SeqCst));
    let held = importer
        .transaction_query(16 << 20)
        .unwrap()
        .submit(Request::Hash(Hash256::ZERO), |r| r)
        .unwrap()
        .wait_retained()
        .await
        .unwrap();
    importer.stop();
    within(importer.join()).await;
    // A completed socket response owns only permits, not a live worker job.
    drop(held);
    assert_eq!(importer.available(), (2, 256 << 20));
}

#[tokio::test]
async fn block_queries_follow_admitted_mainnet_keys_micros_and_rollback() {
    use crate::chain::query::blocks::{Kind, Missing, Reply, Request};
    let (_temp, importer) = setup(Default::default());
    let genesis = importer.current().unwrap().wait().await.unwrap().key;
    let blocks = fixtures::blocks(&NetworkProfile::mainnet());
    for block in &blocks[..2] {
        importer
            .import_block(block.encode().unwrap(), NOW)
            .unwrap()
            .wait()
            .await
            .unwrap();
        let hash = block.hash().unwrap();
        let Reply::Header(top) = importer.blocks(Request::Top).unwrap().wait().await.unwrap()
        else {
            panic!()
        };
        assert_eq!(top.header, block.header());
        assert_eq!(top.previous, Kind::Key);
        let Reply::Header(key) = importer
            .blocks(Request::CurrentKey)
            .unwrap()
            .wait()
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(key.header, blocks[0].header());
        let Reply::Generation { key, micros } = importer
            .blocks(Request::CurrentGeneration)
            .unwrap()
            .wait()
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(key.header, blocks[0].header());
        assert_eq!(
            micros,
            if matches!(block, Block::Micro(_)) {
                vec![hash]
            } else {
                vec![]
            }
        );
        assert_eq!(
            importer
                .blocks(Request::KeyHeight(1))
                .unwrap()
                .wait()
                .await
                .unwrap(),
            Reply::Header(key)
        );
        if let Block::Micro(m) = block {
            assert_eq!(
                importer
                    .blocks(Request::MicroCount(hash))
                    .unwrap()
                    .wait()
                    .await
                    .unwrap(),
                Reply::Count(m.transactions.len())
            );
            assert_eq!(
                importer
                    .blocks(Request::MicroHeader(hash))
                    .unwrap()
                    .wait()
                    .await
                    .unwrap(),
                Reply::Header(top)
            );
            assert_eq!(
                importer
                    .blocks(Request::KeyHash(hash))
                    .unwrap()
                    .wait()
                    .await
                    .unwrap(),
                Reply::Missing(Missing::Key)
            );
        }
    }
    for request in [
        Request::KeyHeight(u64::MAX as u128 + 1),
        Request::GenerationHeight(u64::MAX as u128 + 385),
    ] {
        assert_eq!(
            importer.blocks(request).unwrap().wait().await.unwrap(),
            Reply::Missing(Missing::ChainTooShort)
        );
    }
    assert_eq!(
        importer
            .blocks(Request::MicroHeader(genesis))
            .unwrap()
            .wait()
            .await
            .unwrap(),
        Reply::Missing(Missing::Micro)
    );
    assert_eq!(
        importer
            .blocks(Request::GenerationHash(Hash256::ZERO))
            .unwrap()
            .wait()
            .await
            .unwrap(),
        Reply::Missing(Missing::NotOnChain)
    );
    importer
        .database()
        .rollback(repository::rollback::Target::Height(0), Default::default())
        .unwrap();
    let Reply::Generation { key, micros } = importer
        .blocks(Request::CurrentGeneration)
        .unwrap()
        .wait()
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(key.header.hash().unwrap(), genesis);
    assert!(micros.is_empty());
    importer.stop();
    assert!(matches!(importer.blocks(Request::Top), Err(Error::Closed)));
    importer.join().await;
}

#[tokio::test]
async fn block_queries_cancel_queued_reads_share_capacity_and_apply_generation_limits() {
    use crate::chain::query::blocks::{Reply, Request};
    let (_temp, importer) = setup(Config {
        jobs: 2,
        blocks: 1,
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
    let queued = importer.blocks(Request::Top).unwrap();
    assert!(matches!(
        importer.blocks(Request::CurrentGeneration),
        Err(Error::Busy)
    ));
    drop(queued);
    release.send(()).unwrap();
    active.await.unwrap();
    importer.jobs.drain().await;
    for block in &fixtures::blocks(&NetworkProfile::mainnet())[..2] {
        importer
            .import_block(block.encode().unwrap(), NOW)
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    assert!(matches!(
        importer
            .blocks(Request::CurrentGeneration)
            .unwrap()
            .wait()
            .await,
        Err(Error::Storage(repository::Error::ResourceLimit))
    ));
    assert!(matches!(
        importer.blocks(Request::Top).unwrap().wait().await.unwrap(),
        Reply::Header(_)
    ));
    *importer.context.recovery.lock().unwrap() = Some(Error::RecoveryRequired);
    assert!(matches!(
        importer.blocks(Request::Top),
        Err(Error::RecoveryRequired)
    ));
    importer.stop();
    importer.join().await;
}

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
