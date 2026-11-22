mod common;
use ae_chain::repository::hyperchain::inputs::Effects;
use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::eoe::{self, coordinator, local_host, runtime};
use ae_sync::chain::{
    self, Importer,
    hyperchain::{
        delivery::{self, Delivery, Progress},
        scoped,
    },
};
use ae_trie::MemoryStore;
use ae_vm_fate::maps::order::Otp26;
use common::{
    hyperchain::{
        self as fixture,
        parents::{Pair, key},
        pinned,
    },
    within,
};
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};
const NOW: u64 = 1_000_000;
type Pump = Delivery<Otp26, MemoryStore, MemoryStore>;
fn runtime(
    node: &fixture::Node,
    owned: u64,
    config: coordinator::Config,
    limits: runtime::Config,
) -> runtime::Runtime {
    let host = local_host::LocalHost::new(
        local_host::Config {
            network: node.config.profile.network.clone(),
            election_contract: node.config.profile.contracts.election,
            parent_start_height: 1.into(),
            parent_epoch_length: 5.into(),
            limits: Default::default(),
        },
        local_host::SelectedTip {
            trees: None,
            protocol: node.config.profile.network.protocol_at(0).unwrap(),
            consensus: ae_protocol::ConsensusType::Pos,
        },
        (1..=owned)
            .map(|n| {
                let k = Ed25519SecretKey::from_seed(&key(n).try_into().unwrap());
                (k.public_key(), k)
            })
            .collect(),
    );
    runtime::Runtime::spawn(
        coordinator::Coordinator::new(host, 100_000.into(), config),
        fixture::order(&node.corpus["profile"]),
        limits,
    )
    .unwrap()
}
fn call(rt: &runtime::Runtime, command: runtime::Command) -> runtime::Call {
    match rt.client().call(command) {
        Ok(c) => c,
        Err(_) => panic!("EOE call refused"),
    }
}
fn pending(a: &mut scoped::Accepted<Otp26, Effects>) -> Effects {
    let mut effects = None;
    assert!(
        a.try_take_if(|e| {
            effects = Some(e.clone());
            false
        })
        .unwrap()
        .is_none()
    );
    effects.unwrap()
}
async fn drain(p: &mut Pump) {
    within(async {
        loop {
            if p.step().unwrap() == Progress::Idle {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
}
fn importer(node: &fixture::Node, pair: &Pair) -> (Importer, scoped::Accepted<Otp26, Effects>) {
    Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap()
}
async fn first(node: &fixture::Node, importer: &Importer) {
    within(
        importer
            .import_block(node.blocks()[0].encode().unwrap(), NOW)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    node.check(0);
}
#[tokio::test]
async fn signed_native_pin_chains_deliver_exact_accepted_effect_prefixes_to_live_eoe() {
    for index in [0, 1, 6] {
        let node = fixture::Node::from_corpus(&pinned::DATA, index);
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
        }
        let expected = pending(&mut accepted);
        assert!(!expected.entries().is_empty());
        let rt = runtime(&node, 0, Default::default(), Default::default());
        let mut pump = Delivery::new(accepted, parent.parents.client.clone(), rt.client());
        within(async {
            loop {
                let progress = pump.step().unwrap();
                let n = pump.acknowledged() as usize;
                if !pump.pending().is_empty() {
                    assert_eq!(
                        pump.pending().iter().collect::<Vec<_>>(),
                        expected.entries()[n..].iter().collect::<Vec<_>>()
                    );
                    assert_eq!(pump.retained_bytes(), expected.bytes());
                }
                if progress == Progress::Idle {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        assert_eq!(pump.acknowledged() as usize, expected.entries().len());
        assert_eq!(pump.retained_bytes(), 0);
        assert_eq!(rt.client().outstanding(), 0);
        node.check(12);
        importer.stop();
        importer.join().await;
        rt.shutdown().unwrap();
    }
}
#[tokio::test]
async fn cache_miss_delivery_requests_real_parent_and_native_block_retry_succeeds() {
    let pair = Pair::with_targets(true, vec![6.into()]);
    let node = fixture::Node::new(0);
    let (importer, mut accepted) = importer(&node, &pair);
    let block = node.blocks()[0].encode().unwrap();
    let error = within(importer.import_block(block.clone(), NOW).unwrap().wait())
        .await
        .unwrap_err();
    assert!(matches!(
        error.error,
        chain::Error::Validation {
            operational: false,
            ..
        }
    ));
    let effects = pending(&mut accepted);
    assert!(effects.entries().iter().any(|e| matches!(
        e,
        ae_chain::repository::hyperchain::inputs::Effect::RequestParent(_)
    )));
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    let rt = runtime(&node, 0, Default::default(), Default::default());
    let mut pump = Delivery::new(accepted, pair.client.clone(), rt.client());
    let calls = pair.client.snapshot().lookup_calls;
    pair.backend.missing.store(false, Ordering::Release);
    drain(&mut pump).await;
    assert!(pair.client.snapshot().lookup_calls > calls);
    within(async {
        while pair.client.snapshot().cache.lookup(&1.into()).is_err() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    within(importer.import_block(block, NOW).unwrap().wait())
        .await
        .unwrap();
    node.check(0);
    drain(&mut pump).await;
    assert!(pump.acknowledged() > effects.entries().len() as u64);
    importer.stop();
    importer.join().await;
    rt.shutdown().unwrap();
}
#[tokio::test]
async fn batch_limits_and_eoe_rejections_retain_effects_until_explicit_retry() {
    let pair = Pair::new(false);
    let node = fixture::Node::new(0);
    let (importer, mut accepted) = importer(&node, &pair);
    first(&node, &importer).await;
    let expected = pending(&mut accepted);
    let rt = runtime(
        &node,
        0,
        coordinator::Config {
            maximum_queued_events: 1,
            ..Default::default()
        },
        Default::default(),
    );
    let mut pump = Delivery::new(accepted, pair.client.clone(), rt.client());
    pump.retry(delivery::Config {
        maximum_bytes: 1,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(pump.step(), Err(delivery::Error::ResourceLimit));
    assert!(pump.pending().is_empty());
    pump.retry(Default::default()).unwrap();
    let service = delivery::Service::spawn(pump, Duration::from_millis(1)).unwrap();
    within(async {
        while service.status().failure.is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert_eq!(
        service.status().failure,
        Some(delivery::Error::Eoe(coordinator::Error::ResourceLimit))
    );
    let mut pump = service.shutdown().unwrap();
    assert_eq!(pump.acknowledged(), 0);
    assert_eq!(
        pump.pending().iter().collect::<Vec<_>>(),
        expected.entries().iter().collect::<Vec<_>>()
    );
    assert!(!pump.in_flight());
    assert_eq!(
        pump.step(),
        Err(delivery::Error::Eoe(coordinator::Error::ResourceLimit))
    );
    call(&rt, runtime::Command::Configure(Default::default()))
        .wait(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    pump.retry(Default::default()).unwrap();
    drain(&mut pump).await;
    assert_eq!(pump.acknowledged() as usize, expected.entries().len());
    node.check(0);
    importer.stop();
    importer.join().await;
    rt.shutdown().unwrap();
}
#[tokio::test]
async fn receipt_backpressure_and_cache_replacement_cannot_drop_or_redirect_pending_effects() {
    let mut pair = Pair::new(false);
    let node = fixture::Node::new(0);
    let (importer, mut accepted) = importer(&node, &pair);
    first(&node, &importer).await;
    let expected = pending(&mut accepted);
    let rt = runtime(
        &node,
        0,
        Default::default(),
        runtime::Config {
            command_capacity: 1,
            ..Default::default()
        },
    );
    let mut held = call(&rt, runtime::Command::Retry);
    within(async {
        while held.try_reply().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    let mut pump = Delivery::new(accepted, pair.client.clone(), rt.client());
    assert_eq!(pump.step(), Ok(Progress::Backpressured));
    assert_eq!(pump.pending().len(), expected.entries().len());
    drop(held);
    assert_eq!(pump.step(), Ok(Progress::Submitted));
    within(async {
        while pump.step().unwrap() != Progress::Acknowledged {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    // Acquire a fresh native effect if the first batch contained only one.
    within(
        importer
            .import_block(node.blocks()[1].encode().unwrap(), NOW)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    let mut held = call(&rt, runtime::Command::Retry);
    within(async {
        while held.try_reply().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert_eq!(pump.step(), Ok(Progress::Backpressured));
    let pending = pump.pending().clone();
    pair.cache.take().unwrap().shutdown().unwrap();
    let replacement = Pair::new(false);
    let before = replacement.client.snapshot().lookup_calls;
    assert_eq!(
        pump.step(),
        Err(delivery::Error::Closed(delivery::Target::Cache))
    );
    pump.retry(Default::default()).unwrap();
    assert_eq!(
        pump.step(),
        Err(delivery::Error::Closed(delivery::Target::Cache))
    );
    assert_eq!(pump.pending(), &pending);
    assert_eq!(replacement.client.snapshot().lookup_calls, before);
    drop(held);
    importer.stop();
    importer.join().await;
    rt.shutdown().unwrap();
}
#[tokio::test]
async fn lost_eoe_receipt_is_quarantined_and_cannot_be_retried_on_a_replacement() {
    let pair = Pair::new(false);
    let node = fixture::Node::new(0);
    let (importer, accepted) = importer(&node, &pair);
    first(&node, &importer).await;
    let rt = runtime(
        &node,
        3,
        Default::default(),
        runtime::Config {
            command_capacity: 2,
            output_capacity: 1,
            ..Default::default()
        },
    );
    let keys: Vec<_> = (1..=3)
        .map(|n| Ed25519SecretKey::from_seed(&key(n).try_into().unwrap()).public_key())
        .collect();
    call(
        &rt,
        runtime::Command::Negotiate(eoe::Negotiation {
            epoch: 1.into(),
            height: 1.into(),
            hash: key(99),
            leader: keys[0],
            validators: keys.iter().map(|k| (*k, 1.into())).collect(),
            seed: Some(key(1001)),
            current_length: 3.into(),
        }),
    )
    .wait(Duration::from_secs(2))
    .unwrap()
    .unwrap();
    assert!(matches!(
        rt.recv_timeout(Duration::from_secs(2)).unwrap(),
        runtime::Output::Service(coordinator::ServiceOutput::Publication(_))
    ));
    let mut pump = Delivery::new(accepted, pair.client.clone(), rt.client());
    assert_eq!(pump.step(), Ok(Progress::Submitted));
    assert_eq!(pump.step(), Ok(Progress::Waiting));
    let expected = pump.pending().clone();
    let service = delivery::Service::spawn(pump, Duration::from_millis(1)).unwrap();
    let mut pump = service.shutdown().unwrap();
    assert!(pump.in_flight());
    assert_eq!(pump.pending(), &expected);
    rt.shutdown().unwrap();
    let replacement = runtime(&node, 0, Default::default(), Default::default());
    let error = delivery::Error::ReceiptLost(delivery::Target::Eoe);
    assert_eq!(pump.step(), Err(error.clone()));
    assert_eq!(pump.retry(Default::default()), Err(error.clone()));
    assert_eq!(pump.step(), Err(error));
    assert_eq!(pump.pending(), &expected);
    assert_eq!(pump.acknowledged(), 0);
    assert_eq!(replacement.client().outstanding(), 0);
    importer.stop();
    importer.join().await;
    replacement.shutdown().unwrap();
}
#[tokio::test]
async fn delivery_worker_joins_while_pin_capture_holds_source_lock_and_resumes_same_outbox() {
    let node = fixture::Node::from_corpus(&pinned::DATA, 0);
    let parent = pinned::Parent::new(0);
    let (importer, accepted) = parent.importer(&node);
    let rt = runtime(&node, 0, Default::default(), Default::default());
    let pump = Delivery::new(accepted, parent.parents.client.clone(), rt.client());
    let service = delivery::Service::spawn(pump, Duration::from_millis(1)).unwrap();
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
    }
    within(async {
        while service.status().acknowledged == 0 || service.status().pending != 0 {
            assert!(service.status().failure.is_none());
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    let acknowledged = service.status().acknowledged;
    parent.mode.store(3, Ordering::Release);
    let pending = importer
        .import_block(blocks[10].encode().unwrap(), NOW)
        .unwrap();
    within(async {
        while parent.requests.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert!(service.status().running);
    let at = Instant::now();
    let pump = service.shutdown().unwrap();
    assert!(at.elapsed() < Duration::from_millis(500));
    assert_eq!(node.db.view().unwrap().status().revision, 11);
    parent.mode.store(0, Ordering::Release);
    within(pending.wait()).await.unwrap();
    node.check(10);
    pinned::check_pin(&node, 10);
    // Invalid startup returns the original owned outbox for a corrected retry.
    let pump = match delivery::Service::spawn(pump, Duration::ZERO) {
        Err(error) => error.delivery,
        Ok(_) => panic!("zero poll interval accepted"),
    };
    let service = delivery::Service::spawn(pump, Duration::from_millis(1)).unwrap();
    within(async {
        while service.status().acknowledged <= acknowledged || service.status().pending != 0 {
            assert!(service.status().failure.is_none());
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    let mut pump = service.shutdown().unwrap();
    drain(&mut pump).await;
    assert!(pump.acknowledged() > 0);
    assert!(pump.pending().is_empty());
    importer.stop();
    importer.join().await;
    drop(importer);
    assert_eq!(
        pump.step(),
        Err(delivery::Error::Source(chain::Error::Closed))
    );
    rt.shutdown().unwrap();
}
