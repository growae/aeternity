use super::*;
use ae_chain::repository::hyperchain::inputs::Limits;
use common::hyperchain::parents::parent;

#[tokio::test]
async fn received_block_waits_for_parent_finality_before_executing() {
    let pair = Pair::with_finality(false, vec![1.into()], 8);
    assert_eq!(
        pair.client.snapshot().cache.lookup(&1.into()),
        Err(parent_cache::LookupError::NotFinal)
    );
    let node = fixture::Node::new(0);
    let (importer, mut accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let ticket = importer
        .import_block(node.blocks()[0].encode().unwrap(), NOW)
        .unwrap();
    wait(|| pair.client.snapshot().lookup_calls >= 2);
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    pair.post(9, 1009);
    within(ticket.wait()).await.unwrap();
    node.check(0);
    assert!(
        accepted
            .take()
            .unwrap()
            .entries()
            .iter()
            .all(|e| matches!(e, Effect::Parent { .. }))
    );
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn unavailable_entropy_exhausts_native_poll_budget_once_and_retains_rejection_prefix() {
    let pair = Pair::with_targets(true, vec![16.into()]);
    let node = fixture::Node::new(0);
    let (importer, mut accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let result = within(
        importer
            .import_block(node.blocks()[0].encode().unwrap(), NOW)
            .unwrap()
            .wait(),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(pair.client.snapshot().lookup_calls, 101);
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    let effects = accepted.take().unwrap();
    assert!(!effects.entries().is_empty());
    assert!(
        effects
            .entries()
            .iter()
            .all(|e| matches!(e, Effect::RequestParent(h) if h == &BigInt::from(1)))
    );
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn child_writer_can_advance_during_entropy_wait_and_stale_snapshot_cannot_commit() {
    let pair = Pair::with_targets(true, vec![16.into()]);
    let node = fixture::Node::new(0);
    let (importer, mut accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let ticket = importer
        .import_block(node.blocks()[0].encode().unwrap(), NOW)
        .unwrap();
    wait(|| pair.client.snapshot().lookup_calls > 0);
    node.db
        .rollback(
            repo::rollback::Target::Key(node.config.identity.genesis),
            Default::default(),
        )
        .unwrap();
    assert_eq!(node.db.view().unwrap().status().revision, 2);
    pair.backend.missing.store(false, Ordering::Release);
    let result = within(ticket.wait()).await;
    // The importer may retry a storage conflict with a fresh owner snapshot.
    // Any acknowledged result must be based on the administrative revision.
    if result.is_ok() {
        assert_eq!(node.db.view().unwrap().status().revision, 3);
        node.check(0);
    } else {
        assert_eq!(node.db.view().unwrap().status().revision, 2);
        assert!(accepted.take().unwrap().entries().is_empty());
    }
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn cold_imports_acquire_entropy_for_every_native_chain_and_order_profile() {
    for profile in ["profile", "repeat_profile"] {
        for index in 0..7 {
            let pair = Pair::with_targets(false, vec![16.into()]);
            assert!(pair.client.snapshot().cache.lookup(&1.into()).is_err());
            assert!(pair.client.snapshot().cache.lookup(&6.into()).is_err());
            let node = fixture::Node::from_profile(&fixture::DATA, index, profile);
            let (importer, mut accepted) = Importer::new_hyperchain_scoped(
                node.db.clone(),
                node.config.clone(),
                pair.inputs(),
                Default::default(),
            )
            .unwrap();
            let mut epochs = vec![];
            for (i, block) in node.blocks().into_iter().enumerate() {
                within(
                    importer
                        .import_block(block.encode().unwrap(), NOW)
                        .unwrap()
                        .wait(),
                )
                .await
                .unwrap();
                node.check(i);
                for effect in accepted.take().unwrap().drain() {
                    let Effect::Parent { epoch, block } = effect else {
                        panic!("unresolved entropy")
                    };
                    let height = (epoch.clone() - BigInt::from(3)).max(0.into()) * 5 + 1;
                    let height = u64::try_from(height).unwrap();
                    assert_eq!(block, parent(height, height + 1000));
                    epochs.push(epoch);
                }
            }
            for epoch in 1..=4 {
                assert!(epochs.contains(&epoch.into()));
            }
            assert!(pair.client.snapshot().lookup_calls > 0);
            importer.stop();
            importer.join().await;
        }
    }
}

#[test]
fn captured_demand_is_isolated_bounded_and_preserves_explicit_misses() {
    let pair = Pair::with_targets(false, vec![16.into()]);
    let node = fixture::Node::new(0);
    let mut host = pair
        .inputs()
        .capture(
            &node.db.view().unwrap(),
            &node.blocks()[0],
            &Effects::default(),
        )
        .unwrap();
    assert!(
        matches!(host.entropy(&1.into()), Err(InputError::EntropyNotCaptured(h)) if h == 1.into())
    );
    assert!(host.effects().entries().is_empty());
    let mut other = host.clone();
    assert!(matches!(
        host.capture_entropy(1.into(), Some(parent(2, 1002))),
        Err(InputError::InvalidEntropy)
    ));
    host.capture_entropy(1.into(), Some(parent(1, 1001)))
        .unwrap();
    assert_eq!(host.entropy(&2.into()).unwrap(), Some(key(1001)));
    assert!(matches!(
        host.capture_entropy(1.into(), None),
        Err(InputError::InvalidEntropy)
    ));
    assert!(matches!(
        other.entropy(&1.into()),
        Err(InputError::EntropyNotCaptured(_))
    ));
    other.capture_entropy(1.into(), None).unwrap();
    pair.post(1, 9001);
    assert_eq!(other.entropy(&1.into()).unwrap(), None);
    assert_eq!(
        other.effects().entries(),
        &[Effect::RequestParent(1.into())]
    );
    assert_eq!(host.entropy(&1.into()).unwrap(), Some(key(1001)));

    let mut inputs = pair.inputs();
    inputs.limits = Limits {
        maximum_entropy_entries: 1,
        maximum_entropy_bytes: 4,
        ..Default::default()
    };
    let mut limited = inputs
        .capture(
            &node.db.view().unwrap(),
            &node.blocks()[0],
            &Effects::default(),
        )
        .unwrap();
    assert!(matches!(
        limited.capture_entropy(6.into(), Some(parent(6, 1006))),
        Err(InputError::ResourceLimit)
    ));
    limited.capture_entropy(6.into(), None).unwrap();
    assert!(matches!(
        limited.capture_entropy(11.into(), None),
        Err(InputError::ResourceLimit)
    ));
    assert!(matches!(
        limited.entropy(&5.into()),
        Err(InputError::ResourceLimit)
    ));
    assert_eq!(limited.entropy(&4.into()).unwrap(), None);
}

#[tokio::test]
async fn cancelling_entropy_wait_discards_staged_effects_and_releases_import_job() {
    let pair = Pair::with_targets(true, vec![16.into()]);
    let node = fixture::Node::new(0);
    let (importer, mut accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        chain::Config {
            jobs: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let before = node.db.view().unwrap().status();
    let bytes = node.blocks()[0].encode().unwrap();
    let ticket = importer.import_block(bytes.clone(), NOW).unwrap();
    wait(|| pair.client.snapshot().lookup_calls > 0);
    let start = std::time::Instant::now();
    drop(ticket);
    within(async {
        while importer.available().0 == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(node.db.view().unwrap().status(), before);
    assert!(accepted.take().unwrap().entries().is_empty());
    pair.backend.missing.store(false, Ordering::Release);
    within(importer.import_block(bytes, NOW).unwrap().wait())
        .await
        .unwrap();
    node.check(0);
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn acquisition_limits_fail_operationally_without_publishing_effects() {
    for limit in [
        Limits {
            maximum_entropy_entries: 0,
            ..Default::default()
        },
        Limits {
            maximum_entropy_bytes: 1,
            ..Default::default()
        },
    ] {
        let pair = Pair::with_targets(false, vec![16.into()]);
        let node = fixture::Node::new(0);
        let mut inputs = pair.inputs();
        inputs.limits = limit;
        let (importer, mut accepted) = Importer::new_hyperchain_scoped(
            node.db.clone(),
            node.config.clone(),
            inputs,
            Default::default(),
        )
        .unwrap();
        let error = within(
            importer
                .import_block(node.blocks()[0].encode().unwrap(), NOW)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error.error,
            chain::Error::Validation {
                operational: true,
                ..
            }
        ));
        assert_eq!(node.db.view().unwrap().status().revision, 1);
        assert!(accepted.take().unwrap().entries().is_empty());
        if limit.maximum_entropy_entries == 0 {
            assert_eq!(pair.client.snapshot().lookup_calls, 0);
        }
        importer.stop();
        importer.join().await;
    }
}

#[tokio::test]
async fn bad_header_precedes_entropy_acquisition() {
    let pair = Pair::with_targets(false, vec![16.into()]);
    let node = fixture::Node::new(0);
    let (importer, mut accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let mut block = node.blocks().remove(0);
    match &mut block {
        Block::Key(header) => header.time = NOW + 10_000_000,
        Block::Micro(block) => block.header.time = NOW + 10_000_000,
    }
    assert!(
        within(
            importer
                .import_block(block.encode().unwrap(), NOW)
                .unwrap()
                .wait()
        )
        .await
        .is_err()
    );
    assert_eq!(pair.client.snapshot().lookup_calls, 0);
    assert!(accepted.take().unwrap().entries().is_empty());
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    importer.stop();
    importer.join().await;
}
