use super::*;
use common::{hyperchain::pinned, within};
use std::sync::atomic::Ordering;

fn configured(node: &Node, parent: &pinned::Parent) -> Conductor<Otp26> {
    let mut cfg = config(node, false);
    cfg.parent.pins = Some(conductor::PinConfig {
        nodes: vec![parent.node.clone()],
        service: ae_sync::parent_pins::Config {
            maximum_pending: 1,
            request_timeout: Duration::from_secs(2),
            lookup_timeout: Duration::from_secs(3),
            ..Default::default()
        },
    });
    let c = new(node, cfg).unwrap();
    c.parent().post_block(super::parent(8, 1008)).unwrap();
    c.parent().barrier(Duration::from_secs(5)).unwrap();
    c
}
async fn prefix(node: &Node, c: &Conductor<Otp26>, count: usize) {
    for (i, block) in node.blocks().into_iter().take(count).enumerate() {
        within(
            c.importer()
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        node.check(i);
    }
}

#[tokio::test]
async fn conductor_owned_pin_service_imports_all_native_proofs_and_closes_old_clients() {
    for profile in ["profile", "repeat_profile"] {
        for index in 0..15 {
            let node = Node::from_profile(&pinned::DATA, index, profile);
            let parent = pinned::Parent::new(index);
            let c = configured(&node, &parent);
            let client = c.parent_pins().unwrap();
            for (i, block) in node.blocks().into_iter().enumerate() {
                within(
                    c.importer()
                        .import_block(block.encode().unwrap(), 1_000_000)
                        .unwrap()
                        .wait(),
                )
                .await
                .unwrap();
                node.check(i);
                pinned::check_pin(&node, i);
            }
            assert_eq!(parent.requests.load(Ordering::Acquire), 1);
            assert!(shutdown(c).pins_joined);
            assert_eq!(client.check(), Err(ae_sync::parent_pins::Error::Closed));
        }
    }
}

#[tokio::test]
async fn pin_acquisition_follows_validation_and_cancellation_preserves_the_committed_prefix() {
    let node = Node::from_corpus(&pinned::DATA, 0);
    let parent = pinned::Parent::new(0);
    let c = configured(&node, &parent);
    prefix(&node, &c, 10).await;
    let before = node.db.view().unwrap().status();
    let blocks = node.blocks();
    let mut bad = blocks[10].clone();
    let Block::Key(ref mut key) = bad else {
        panic!()
    };
    key.time = 100_000_000;
    assert!(
        within(
            c.importer()
                .import_block(bad.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait()
        )
        .await
        .is_err()
    );
    assert_eq!(parent.requests.load(Ordering::Acquire), 0);
    parent.mode.store(3, Ordering::Release);
    let capacity = c.importer().available().0;
    let ticket = c
        .importer()
        .import_block(blocks[10].encode().unwrap(), 1_000_000)
        .unwrap();
    wait(|| parent.requests.load(Ordering::Acquire) == 1);
    let at = Instant::now();
    drop(ticket);
    let importer = c.importer();
    within(async {
        while importer.available().0 != capacity {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(at.elapsed() < Duration::from_secs(1));
    assert_eq!(node.db.view().unwrap().status(), before);
    parent.mode.store(0, Ordering::Release);
    within(
        importer
            .import_block(blocks[10].encode().unwrap(), 1_000_000)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    node.check(10);
    pinned::check_pin(&node, 10);
    shutdown(c);
}

async fn production(parent: &pinned::Parent) -> (Node, Conductor<Otp26>, u64) {
    let mut node = Node::from_corpus(&pinned::DATA, 0);
    // The retained block history uses 10s slots. Use that controlled producer
    // clock here; the pin request/reward logic and frozen prefix are unchanged.
    node.config.rules.consensus.child_block_time = 10000.into();
    let mut c = configured(&node, parent);
    prefix(&node, &c, 9).await;
    let Block::Micro(micro) = node.blocks().remove(9) else {
        panic!()
    };
    c.pool()
        .force_push(Arc::new(micro.transactions[0].clone()))
        .unwrap()
        .wait()
        .unwrap();
    c.start_production().unwrap();
    (node, c, 55003)
}

#[tokio::test]
async fn producer_acquires_pin_for_calculation_and_admission_and_awards_the_reward() {
    let parent = pinned::Parent::new(0);
    let (node, mut c, mut wall) = production(&parent).await;
    let mut clock = Instant::now();
    let end = clock + Duration::from_secs(20);
    loop {
        assert!(Instant::now() < end, "stalled {:?}", c.status());
        tick(&mut c, &mut wall, &mut clock);
        if let Some(output) = c.take_output() {
            let Output::Attempt(done) = output else {
                panic!("{output:?}")
            };
            if let production::Outcome::Produced { key, .. } = done.outcome {
                let view = node.db.view().unwrap();
                assert_eq!(view.status().tip, Some(key.hash));
                let events = view
                    .hyperchain_event(view.status().revision)
                    .unwrap()
                    .unwrap()
                    .events;
                assert!(events.iter().any(|e| matches!(e, ae_hyperchain::block::Event::PinAccepted { reward, .. } if reward == &BigInt::from(14))));
                assert_eq!(parent.requests.load(Ordering::Acquire), 2);
                break;
            }
        }
    }
    assert!(shutdown(c).pins_joined);
}

#[tokio::test]
async fn stop_and_shutdown_cancel_live_key_proof_work_and_join_before_releasing_ownership() {
    for stop_only in [true, false] {
        let parent = pinned::Parent::new(0);
        parent.mode.store(3, Ordering::Release);
        let (node, mut c, mut wall) = production(&parent).await;
        let mut clock = Instant::now();
        let end = clock + Duration::from_secs(20);
        while parent.requests.load(Ordering::Acquire) == 0 {
            assert!(Instant::now() < end, "stalled {:?}", c.status());
            tick(&mut c, &mut wall, &mut clock);
            assert!(c.take_output().is_none());
        }
        let before = node.db.view().unwrap().status();
        let at = Instant::now();
        if stop_only {
            c.stop_production().unwrap();
            assert!(at.elapsed() < Duration::from_millis(100));
            loop {
                assert!(Instant::now() < end);
                c.step(wall, clock).unwrap();
                if let Some(output) = c.take_output() {
                    let Output::Stopped(stopped) = output else {
                        panic!("{output:?}")
                    };
                    assert!(matches!(
                        stopped.key_preparation,
                        Some(Err(ae_sync::chain::Error::Cancelled))
                    ));
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        let old = c.parent_pins().unwrap();
        let report = shutdown(c);
        assert!(report.pins_joined);
        assert!(at.elapsed() < Duration::from_secs(1));
        assert_eq!(node.db.view().unwrap().status(), before);
        assert!(old.check().is_err());
        drop(report);
        parent.mode.store(0, Ordering::Release);
        let replacement = configured(&node, &parent);
        assert!(replacement.parent_pins().unwrap().check().is_ok());
        shutdown(replacement);
    }
}

#[test]
fn failed_pin_service_start_releases_all_constructor_leases() {
    let node = Node::new(0);
    let mut cfg = config(&node, false);
    cfg.parent.pins = Some(conductor::PinConfig {
        nodes: vec![],
        service: Default::default(),
    });
    assert!(matches!(new(&node, cfg), Err(conductor::Error::Parent(_))));
    let mut cfg = config(&node, false);
    cfg.parent.pins = Some(conductor::PinConfig {
        nodes: vec![],
        service: Default::default(),
    });
    let mut pins = repo::hyperchain::inputs::Pins::default();
    pins.insert(vec![1], None, Default::default()).unwrap();
    cfg.production.pins = Arc::new(pins);
    assert!(matches!(
        new(&node, cfg),
        Err(conductor::Error::Configuration)
    ));
    shutdown(new(&node, config(&node, false)).unwrap());
}

#[tokio::test]
async fn a_changed_pin_between_calculation_and_admission_cannot_commit_the_old_reward_root() {
    let parent = pinned::Parent::new(0);
    let (node, mut c, mut wall) = production(&parent).await;
    let mut clock = Instant::now();
    let end = clock + Duration::from_secs(20);
    let mut calculated = None;
    loop {
        assert!(Instant::now() < end, "stalled {:?}", c.status());
        let progress = tick(&mut c, &mut wall, &mut clock);
        if progress == Progress::Production(production::Progress::KeyPrepared) {
            assert_eq!(parent.requests.load(Ordering::Acquire), 1);
            calculated = Some(node.db.view().unwrap().status());
            parent.mode.store(1, Ordering::Release);
        }
        if let Some(output) = c.take_output() {
            let Output::Rejected { error, .. } = output else {
                panic!("{output:?}")
            };
            assert!(matches!(
                error,
                production::Error::Admission(ae_sync::chain::Failure {
                    error: ae_sync::chain::Error::Validation {
                        operational: false,
                        ..
                    },
                    ..
                })
            ));
            assert_eq!(node.db.view().unwrap().status(), calculated.unwrap());
            assert_eq!(parent.requests.load(Ordering::Acquire), 2);
            break;
        }
    }
    shutdown(c);
}

#[test]
fn asynchronous_pin_receipts_are_single_use_and_drop_cancels_active_lookup() {
    let mut parent = pinned::Parent::new(0);
    let hash = hex::decode(
        pinned::DATA["rows"][0]["parent_pin"]["transaction_hash"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let client = parent.client();
    let mut request = client.request(&hash).unwrap();
    let end = Instant::now() + Duration::from_secs(3);
    loop {
        assert!(Instant::now() < end);
        if let Some(result) = request.try_result() {
            assert!(result.unwrap().is_some());
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(request.try_result().is_none());
    parent.mode.store(3, Ordering::Release);
    let request = client.request(&hash).unwrap();
    wait(|| parent.requests.load(Ordering::Acquire) == 2);
    let at = Instant::now();
    drop(request);
    parent.close();
    assert!(at.elapsed() < Duration::from_secs(1));
    assert_eq!(client.check(), Err(ae_sync::parent_pins::Error::Closed));
}

#[tokio::test]
async fn pin_service_backpressure_retries_production_without_replacing_the_owner() {
    let parent = pinned::Parent::new(0);
    let (node, mut c, mut wall) = production(&parent).await;
    let hash = hex::decode(
        pinned::DATA["rows"][0]["parent_pin"]["transaction_hash"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let client = c.parent_pins().unwrap();
    parent.mode.store(3, Ordering::Release);
    let active = client.request(&hash).unwrap();
    wait(|| parent.requests.load(Ordering::Acquire) == 1);
    let queued = client.request(&hash).unwrap();
    let mut clock = Instant::now();
    let end = clock + Duration::from_secs(20);
    while c.status().retries == 0 {
        assert!(Instant::now() < end, "stalled {:?}", c.status());
        tick(&mut c, &mut wall, &mut clock);
        assert!(c.take_output().is_none());
    }
    assert!(c.status().failure.is_none());
    drop(queued);
    drop(active);
    parent.mode.store(0, Ordering::Release);
    loop {
        assert!(Instant::now() < end, "stalled {:?}", c.status());
        tick(&mut c, &mut wall, &mut clock);
        if let Some(output) = c.take_output() {
            let Output::Attempt(done) = output else {
                panic!("{output:?}")
            };
            let production::Outcome::Produced { key, .. } = done.outcome else {
                panic!("unexpected outcome")
            };
            assert_eq!(node.db.view().unwrap().status().tip, Some(key.hash));
            assert_eq!(parent.requests.load(Ordering::Acquire), 3);
            break;
        }
    }
    shutdown(c);
}
