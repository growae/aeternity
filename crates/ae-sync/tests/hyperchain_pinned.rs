mod common;
use ae_sync::chain;
use common::{
    hyperchain::{self as fixture, pinned},
    within,
};
use std::sync::atomic::Ordering;
const NOW: u64 = 1_000_000;
#[tokio::test]
async fn native_signed_pin_rewards_and_incorrect_proofs_commit_reopen_and_replay() {
    assert_eq!(pinned::DATA["rows"].as_array().unwrap().len(), 15);
    let mut total = 0;
    for profile in ["profile", "repeat_profile"] {
        for index in 0..15 {
            let mut node = fixture::Node::from_profile(&pinned::DATA, index, profile);
            let parent = pinned::Parent::new(index);
            let (importer, mut accepted) = parent.importer(&node);
            for (i, b) in node.blocks().iter().enumerate() {
                within(
                    importer
                        .import_block(b.encode().unwrap(), NOW)
                        .unwrap()
                        .wait(),
                )
                .await
                .unwrap();
                node.check(i);
                pinned::check_pin(&node, i);
                accepted.take().unwrap().drain();
                total += 1;
            }
            assert_eq!(parent.requests.load(Ordering::Acquire), 1);
            importer.stop();
            importer.join().await;
            drop(importer);
            drop(accepted);
            node = node.reopen();
            node.check(12);
            // Idempotent re-import must not publish another reward or revision.
            let before = node.db.view().unwrap().status();
            let (importer, mut accepted) = parent.importer(&node);
            let b = node.blocks().pop().unwrap();
            let reply = within(
                importer
                    .import_block(b.encode().unwrap(), NOW)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
            assert_eq!(reply.accepted.len(), 1);
            assert_eq!(reply.accepted[0].outcome, chain::Outcome::Present);
            assert_eq!(node.db.view().unwrap().status(), before);
            assert!(accepted.take().unwrap().entries().is_empty());
            importer.stop();
            importer.join().await;
        }
    }
    assert_eq!(total, 390);
}
#[tokio::test]
async fn changed_parent_proof_and_local_exhaustion_cannot_commit_the_signed_reward_root() {
    let node = fixture::Node::from_corpus(&pinned::DATA, 0);
    let parent = pinned::Parent::new(0);
    let (importer, mut accepted) = parent.importer(&node);
    let blocks = node.blocks();
    for b in &blocks[..10] {
        within(
            importer
                .import_block(b.encode().unwrap(), NOW)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        accepted.take().unwrap().drain();
    }
    let before = node.db.view().unwrap();
    let status = before.status();
    let checkpoint = before.hyperchain_checkpoint().unwrap();
    let root = before.require_record(status.tip.unwrap()).unwrap().roots;
    parent.mode.store(2, Ordering::Release);
    let error = within(
        importer
            .import_block(blocks[10].encode().unwrap(), NOW)
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
    assert_eq!(node.db.view().unwrap().status(), status);
    assert_eq!(
        node.db.view().unwrap().hyperchain_checkpoint().unwrap(),
        checkpoint
    );
    assert!(accepted.take().unwrap().entries().is_empty());
    parent.mode.store(1, Ordering::Release);
    let error = within(
        importer
            .import_block(blocks[10].encode().unwrap(), NOW)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error.error,
        chain::Error::Validation {
            operational: false,
            ..
        }
    ));
    assert_eq!(node.db.view().unwrap().status(), status);
    assert_eq!(
        node.db
            .view()
            .unwrap()
            .require_record(status.tip.unwrap())
            .unwrap()
            .roots,
        root
    );
    accepted.take().unwrap().drain(); // permitted native cache/effect prefix
    parent.mode.store(0, Ordering::Release);
    for (i, b) in blocks.iter().enumerate().skip(10) {
        within(
            importer
                .import_block(b.encode().unwrap(), NOW)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        node.check(i);
        pinned::check_pin(&node, i);
        accepted.take().unwrap().drain();
    }
    assert_eq!(before.status(), status);
    assert_eq!(parent.requests.load(Ordering::Acquire), 3);
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn retired_pin_service_cannot_redirect_old_importer_to_replacement() {
    let node = fixture::Node::from_corpus(&pinned::DATA, 0);
    let mut parent = pinned::Parent::new(0);
    let old_client = parent.client();
    let (old, mut accepted) = parent.importer(&node);
    let blocks = node.blocks();
    for b in &blocks[..10] {
        within(old.import_block(b.encode().unwrap(), NOW).unwrap().wait())
            .await
            .unwrap();
        accepted.take().unwrap().drain();
    }
    let before = node.db.view().unwrap().status();
    parent.close();
    let replacement = pinned::Parent::new(0);
    let error = within(
        old.import_block(blocks[10].encode().unwrap(), NOW)
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
    assert!(old_client.check().is_err());
    assert_eq!(node.db.view().unwrap().status(), before);
    assert_eq!(replacement.requests.load(Ordering::Acquire), 0);
    assert!(accepted.take().unwrap().entries().is_empty());
    old.stop();
    old.join().await;
    drop(old);
    assert!(matches!(accepted.take(), Err(chain::Error::Closed)));
    let (fresh, mut accepted) = replacement.importer(&node);
    for (i, b) in blocks.iter().enumerate().skip(10) {
        within(fresh.import_block(b.encode().unwrap(), NOW).unwrap().wait())
            .await
            .unwrap();
        node.check(i);
        pinned::check_pin(&node, i);
        accepted.take().unwrap().drain();
    }
    assert_eq!(replacement.requests.load(Ordering::Acquire), 1);
    fresh.stop();
    fresh.join().await;
}

#[tokio::test]
async fn rollback_across_pin_and_finalization_reacquires_proof_and_restores_native_root() {
    use ae_chain::repository::rollback::Target;
    for (index, selected) in [(6, 8), (6, 9), (1, 9)] {
        let node = fixture::Node::from_corpus(&pinned::DATA, index);
        let parent = pinned::Parent::new(index);
        let (importer, mut accepted) = parent.importer(&node);
        let blocks = node.blocks();
        for (i, b) in blocks.iter().enumerate() {
            within(
                importer
                    .import_block(b.encode().unwrap(), NOW)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
            node.check(i);
            pinned::check_pin(&node, i);
            accepted.take().unwrap().drain();
        }
        let before = node.db.view().unwrap();
        let status = before.status();
        let checkpoint = before.hyperchain_checkpoint().unwrap();
        let hash = blocks[selected].hash().unwrap();
        let target = if selected == 8 {
            Target::Key(hash)
        } else {
            Target::Micro(hash)
        };
        node.db.rollback(target, Default::default()).unwrap();
        assert_eq!(node.db.view().unwrap().status().tip, Some(hash));
        for (i, b) in blocks.iter().enumerate().skip(selected + 1) {
            within(
                importer
                    .import_block(b.encode().unwrap(), NOW)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
            node.check(i);
            pinned::check_pin(&node, i);
            accepted.take().unwrap().drain();
        }
        assert_eq!(before.status(), status);
        assert_eq!(before.hyperchain_checkpoint().unwrap(), checkpoint);
        assert_eq!(parent.requests.load(Ordering::Acquire), 2);
        drop(before);
        importer.stop();
        importer.join().await;
        drop(importer);
        drop(accepted);
        let node = node.reopen();
        node.check(12);
    }
}
