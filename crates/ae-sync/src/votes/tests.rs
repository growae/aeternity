use super::*;
use crate::test_support::{Node, within};
use ae_tx::{CanonicalTransaction, Transaction, TransactionLeaf};
use ed25519_dalek::{Signer, SigningKey};
use std::time::Duration;

fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../ae-mempool/tests/fixtures/erlang_hc_vote.json"
    ))
    .unwrap()
}
fn normal() -> Vec<u8> {
    let f = fixture();
    hex::decode(
        f["signatures"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["label"] == "normal" && r["mode"] == "pos" && r["protocol"] == 1)
            .unwrap()["signed"]
            .as_str()
            .unwrap(),
    )
    .unwrap()
}
fn signed(epoch: u64, kind: u64) -> Vec<u8> {
    let key = SigningKey::from_bytes(&[91; 32]);
    let tx = Transaction::new(TransactionLeaf::HcVote(HcVote::new(
        ae_types::AccountPublicKey(key.verifying_key().to_bytes()),
        epoch.into(),
        kind.into(),
        Default::default(),
    )));
    let mut message = NetworkProfile::mainnet().network_id().to_vec();
    message.extend(tx.encode());
    Signed::new(tx, vec![ae_types::Signature(key.sign(&message).to_bytes())]).encode()
}
fn spawn(node: &Node, cache: GossipCache, config: Config, publications: &Publications) -> Worker {
    Worker::spawn(
        node.db.clone(),
        NetworkProfile::mainnet(),
        cache,
        config,
        publications.clone(),
    )
    .unwrap()
}
fn accepted(result: policy::PushOutcome, inserted: bool, event: policy::Event) {
    assert!(
        matches!(result, policy::PushOutcome::Accepted {inserted:i,event:e,..} if i==inserted && e==event)
    );
}
async fn push(client: &Client, bytes: Vec<u8>, event: policy::Event) -> policy::PushOutcome {
    within(client.push(bytes, event).unwrap()).await.unwrap()
}

#[tokio::test]
async fn native_signature_and_type_results_use_selected_protocol_and_cache_fast_path() {
    let node = Node::new();
    let cache = GossipCache::default();
    let source = Publications::new(&node.db, Config::default()).unwrap();
    let worker = spawn(&node, cache.clone(), Config::default(), &source);
    let client = worker.client();
    let mut events = source.listen().unwrap();
    for row in fixture()["signatures"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["mode"] == "pos" && r["protocol"] == 1)
    {
        cache.reset().unwrap();
        let bytes = hex::decode(row["signed"].as_str().unwrap()).unwrap();
        let result = within(client.push(bytes.clone(), policy::Event::Received).unwrap()).await;
        let reason = match result {
            Ok(policy::PushOutcome::Accepted {
                event: policy::Event::Received,
                ..
            }) => {
                let event = within(events.recv()).await.unwrap();
                assert_eq!(
                    event.transaction.encode(),
                    Signed::decode(&bytes).unwrap().encode()
                );
                assert_eq!(event.event, policy::Event::Received);
                "ok"
            }
            Err(Error::Pool(policy::Error::OnlyHcVoteAllowed)) => "only_hc_vote_tx_allowed",
            Err(Error::Pool(policy::Error::Signature(
                ae_execution::transactions::SignatureError::CheckFailed,
            ))) => "signature_check_failed",
            other => panic!("{}: {other:?}", row["label"]),
        };
        assert_eq!(reason, row["admission"], "{}", row["label"]);
        // Native first-level cache records misses even when type/signature checks fail.
        assert_eq!(
            push(&client, bytes, policy::Event::Created).await,
            policy::PushOutcome::Cached
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), events.recv())
            .await
            .is_err()
    );
    assert!(node.db.pool_view().unwrap().members(10).unwrap().is_empty());
    worker.shutdown().unwrap();
}

#[tokio::test]
async fn epochs_keep_first_equal_backward_and_clear_every_epoch_on_forward() {
    let node = Node::new();
    let source = Publications::new(&node.db, Config::default()).unwrap();
    let worker = spawn(&node, Default::default(), Config::default(), &source);
    let client = worker.client();
    for (epoch, kind) in [(7, 1), (7, 2), (999, 1)] {
        accepted(
            push(&client, signed(epoch, kind), policy::Event::Created).await,
            true,
            policy::Event::Created,
        );
    }
    let votes = within(client.peek(7.into(), None, 10).unwrap())
        .await
        .unwrap();
    assert_eq!(
        votes
            .iter()
            .map(|v| v.vote_type().clone())
            .collect::<Vec<_>>(),
        vec![2.into(), 1.into()]
    );
    for epoch in [8, 8, 2, 0, 9] {
        within(client.advance_epoch(epoch.into()).unwrap())
            .await
            .unwrap();
        let stats = within(client.stats().unwrap()).await.unwrap();
        assert_eq!(stats.epoch, epoch.into());
        assert_eq!(stats.entries, 3);
    }
    within(client.advance_epoch(10.into()).unwrap())
        .await
        .unwrap();
    let stats = within(client.stats().unwrap()).await.unwrap();
    assert_eq!(stats.entries, 0);
    assert_eq!(stats.retained_wire_bytes, 0);
    assert!(
        within(client.peek(999.into(), Some(1.into()), 10).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    worker.shutdown().unwrap();
}

#[tokio::test]
async fn publication_pressure_precedes_mutation_and_charges_delivered_events_until_drop() {
    let node = Node::new();
    let bytes = normal();
    let config = Config {
        events: 1,
        event_bytes: bytes.len() + 128,
        ..Default::default()
    };
    let source = Publications::new(&node.db, config).unwrap();
    let cache = GossipCache::default();
    let worker = spawn(&node, cache.clone(), config, &source);
    let client = worker.client();
    drop(client.push(bytes.clone(), policy::Event::Created).unwrap());
    assert_eq!(within(client.stats().unwrap()).await.unwrap().entries, 1);
    let other = signed(88, 2);
    assert!(matches!(
        client.push(other.clone(), policy::Event::Received),
        Err(Error::Worker(Fault::Busy))
    ));
    assert!(!cache.check(Signed::decode(&other).unwrap().hash()));
    let mut events = source.listen().unwrap();
    assert!(matches!(source.listen(), Err(Error::Worker(Fault::Busy))));
    let held = within(events.recv()).await.unwrap();
    assert_eq!(held.transaction.encode(), bytes);
    assert!(matches!(
        client.push(bytes.clone(), policy::Event::Received),
        Err(Error::Worker(Fault::Busy))
    ));
    drop(held);
    cache.reset().unwrap();
    accepted(
        push(&client, bytes.clone(), policy::Event::Received).await,
        false,
        policy::Event::Received,
    );
    drop(events);
    // A replacement consumer takes the queued publication; pool lifetime is separate.
    let mut events = source.listen().unwrap();
    let pending = within(events.recv()).await.unwrap();
    assert_eq!(pending.transaction.encode(), bytes);
    assert_eq!(pending.event, policy::Event::Received);
    worker.shutdown().unwrap();
    source.close();
    assert!(within(events.recv()).await.is_none());
}

#[tokio::test]
async fn accepted_queue_drains_on_shutdown_even_when_observer_is_dropped() {
    let node = Node::new();
    let config = Config {
        queue: QueueLimits {
            requests: 2,
            bytes: 2048,
        },
        ..Default::default()
    };
    let source = Publications::new(&node.db, config).unwrap();
    let worker = spawn(&node, Default::default(), config, &source);
    let client = worker.client();
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let blocked = client
        .inner
        .submit(0, move |_| {
            let _ = entered.send(());
            wait.recv_timeout(Duration::from_secs(5)).unwrap();
            Ok(())
        })
        .unwrap();
    within(waiting).await.unwrap();
    drop(client.push(normal(), policy::Event::Created).unwrap());
    assert!(matches!(
        client.advance_epoch(9.into()),
        Err(Error::Worker(Fault::Busy))
    ));
    worker.request_shutdown();
    assert!(matches!(client.stats(), Err(Error::Worker(Fault::Stopped))));
    release.send(()).unwrap();
    within(blocked).await.unwrap();
    worker.shutdown().unwrap();
    let mut events = source.listen().unwrap();
    assert_eq!(
        within(events.recv()).await.unwrap().transaction.encode(),
        normal()
    );
    assert!(client.info().joined);
}

#[tokio::test]
async fn unread_replies_keep_a_host_budget_across_worker_replacement() {
    let node = Node::new();
    let bytes = normal();
    let signed = Signed::decode(&bytes).unwrap();
    let TransactionLeaf::HcVote(vote) = signed.transaction().view().leaf() else {
        panic!()
    };
    let config = Config {
        reply_bytes: vote.wire_size() + 128,
        ..Default::default()
    };
    let source = Publications::new(&node.db, config).unwrap();
    let cache = GossipCache::default();
    let worker = spawn(&node, cache.clone(), config, &source);
    let client = worker.client();
    push(&client, bytes.clone(), policy::Event::Created).await;
    let unread = client.peek(7.into(), None, 1).unwrap();
    within(client.inner.submit(0, |_| Ok(())).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        within(client.peek(7.into(), None, 1).unwrap()).await,
        Err(Error::Worker(Fault::Busy))
    ));
    assert!(matches!(
        within(client.stats().unwrap()).await,
        Err(Error::Worker(Fault::Busy))
    ));
    worker.shutdown().unwrap();
    cache.reset().unwrap();
    let worker = spawn(&node, cache, config, &source);
    let client = worker.client();
    push(&client, bytes, policy::Event::Created).await;
    assert!(matches!(
        within(client.peek(7.into(), None, 1).unwrap()).await,
        Err(Error::Worker(Fault::Busy))
    ));
    let held = within(unread).await.unwrap();
    assert!(matches!(
        within(client.peek(7.into(), None, 1).unwrap()).await,
        Err(Error::Worker(Fault::Busy))
    ));
    drop(held);
    assert_eq!(
        within(client.peek(7.into(), None, 1).unwrap())
            .await
            .unwrap()
            .len(),
        1
    );
    worker.shutdown().unwrap();
}

#[tokio::test]
async fn cache_lookup_follows_cache_replacement_and_unavailable_cache_is_a_miss() {
    let node = Node::new();
    let (cache, mut controller) = GossipCache::managed(16, 16).unwrap();
    let source = Publications::new(&node.db, Config::default()).unwrap();
    let worker = spawn(&node, cache, Config::default(), &source);
    let client = worker.client();
    accepted(
        push(&client, normal(), policy::Event::Created).await,
        true,
        policy::Event::Created,
    );
    let first = controller.start().unwrap();
    accepted(
        push(&client, normal(), policy::Event::Received).await,
        false,
        policy::Event::Received,
    );
    assert_eq!(
        push(&client, normal(), policy::Event::Created).await,
        policy::PushOutcome::Cached
    );
    first.shutdown();
    let _second = controller.start().unwrap();
    accepted(
        push(&client, normal(), policy::Event::Created).await,
        false,
        policy::Event::Created,
    );
    worker.shutdown().unwrap();
}

#[tokio::test]
async fn panic_fences_old_clients_clears_pool_and_keeps_queued_publications() {
    let node = Node::new();
    let source = Publications::new(&node.db, Config::default()).unwrap();
    let cache = GossipCache::default();
    let worker = spawn(&node, cache.clone(), Config::default(), &source);
    let old = worker.client();
    push(&old, normal(), policy::Event::Created).await;
    assert!(matches!(
        Worker::spawn(
            node.db.clone(),
            NetworkProfile::mainnet(),
            cache.clone(),
            Config::default(),
            source.clone()
        ),
        Err(Error::Worker(Fault::Busy))
    ));
    old.fail();
    within(async {
        while old.info().running {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(matches!(
        worker.shutdown(),
        Err(Error::Worker(Fault::Panicked))
    ));
    assert!(matches!(old.stats(), Err(Error::Worker(Fault::Stopped))));
    let worker = spawn(&node, cache, Config::default(), &source);
    let client = worker.client();
    assert_eq!(within(client.stats().unwrap()).await.unwrap().entries, 0);
    assert_eq!(
        push(&client, normal(), policy::Event::Received).await,
        policy::PushOutcome::Cached
    );
    let mut events = source.listen().unwrap();
    assert_eq!(
        within(events.recv()).await.unwrap().transaction.encode(),
        normal()
    );
    worker.shutdown().unwrap();
}

#[tokio::test]
async fn exact_binding_waits_before_acceptance_and_never_retries_a_failed_ticket() {
    let node = Node::new();
    let source = Publications::new(&node.db, Config::default()).unwrap();
    let worker = spawn(&node, Default::default(), Config::default(), &source);
    let (owner, binding) = bindings::Owner::new(&node.db);
    let lookup = binding.clone();
    let task = tokio::spawn(async move { call(&lookup, |c| c.stats()).await });
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    owner.install(worker.client(), 1);
    assert_eq!(within(task).await.unwrap().unwrap().entries, 0);
    let client = worker.client();
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let blocked = client
        .inner
        .submit::<()>(0, move |_| {
            let _ = entered.send(());
            wait.recv_timeout(Duration::from_secs(5)).unwrap();
            panic!("failed with an accepted request behind it");
        })
        .unwrap();
    within(waiting).await.unwrap();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts = count.clone();
    let accepted = tokio::spawn(async move {
        call(&binding, |c| {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            c.stats()
        })
        .await
    });
    within(async {
        while client.info().pending != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    owner.withdraw();
    release.send(()).unwrap();
    assert!(matches!(
        within(blocked).await,
        Err(Error::Worker(Fault::Disconnected))
    ));
    assert!(matches!(
        worker.shutdown(),
        Err(Error::Worker(Fault::Panicked))
    ));
    let worker = spawn(&node, Default::default(), Config::default(), &source);
    owner.install(worker.client(), 2);
    assert!(matches!(
        within(accepted).await.unwrap(),
        Err(Error::Worker(Fault::Disconnected))
    ));
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    owner.close();
    worker.shutdown().unwrap();
}

#[tokio::test]
async fn genesis_fallback_checks_network_and_resource_configuration() {
    let node = Node::new();
    let identity = node.db.view().unwrap().identity().clone();
    let path = std::env::temp_dir().join(format!("ae-vote-genesis-{}", std::process::id()));
    let db = Arc::new(Database::open(&path, identity).unwrap());
    assert!(db.view().unwrap().status().tip.is_none());
    let config = Config::default();
    let source = Publications::new(&db, config).unwrap();
    assert!(matches!(
        Worker::spawn(
            db.clone(),
            NetworkProfile::uat(),
            Default::default(),
            config,
            source.clone()
        ),
        Err(Error::Worker(Fault::Configuration))
    ));
    assert!(matches!(
        Worker::spawn(
            node.db.clone(),
            NetworkProfile::mainnet(),
            Default::default(),
            config,
            source.clone()
        ),
        Err(Error::Worker(Fault::Configuration))
    ));
    assert!(matches!(
        Worker::spawn(
            db.clone(),
            NetworkProfile::mainnet(),
            Default::default(),
            Config {
                reply_bytes: config.reply_bytes - 1,
                ..config
            },
            source.clone()
        ),
        Err(Error::Worker(Fault::Configuration))
    ));
    let worker = Worker::spawn(
        db.clone(),
        NetworkProfile::mainnet(),
        Default::default(),
        config,
        source.clone(),
    )
    .unwrap();
    accepted(
        push(&worker.client(), normal(), policy::Event::Created).await,
        true,
        policy::Event::Created,
    );
    assert!(matches!(
        worker.client().push(
            vec![0; config.pool.maximum_encoded_bytes + 1],
            policy::Event::Created
        ),
        Err(Error::Pool(policy::Error::ResourceLimit))
    ));
    assert!(matches!(
        within(
            worker
                .client()
                .push(vec![0xff], policy::Event::Created)
                .unwrap()
        )
        .await,
        Err(Error::Protocol)
    ));
    worker.shutdown().unwrap();
    drop(source);
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn actual_native_process_lifecycle_matches_owned_worker_state() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_hc_vote_owner.json"
    ))
    .unwrap();
    assert_eq!(fixture["size_timeout"], true); // Rust stats intentionally repairs the native missing callback.
    let node = Node::new();
    let config = Config::default();
    let source = Publications::new(&node.db, config).unwrap();
    let cache = GossipCache::default();
    let mut worker = spawn(&node, cache.clone(), config, &source);
    for row in fixture["lifecycle"].as_array().unwrap() {
        let client = worker.client();
        match row["label"].as_str().unwrap() {
            "initial" | "after_size_timeout" => (),
            "inserted" | "before_stop" => {
                cache.reset().unwrap();
                accepted(
                    push(&client, normal(), policy::Event::Created).await,
                    true,
                    policy::Event::Created,
                );
            }
            "duplicate_published" => {
                cache.reset().unwrap();
                accepted(
                    push(&client, normal(), policy::Event::Received).await,
                    false,
                    policy::Event::Received,
                );
            }
            "cached_wrong_type" => {
                let wrong = crate::test_support::transaction();
                cache.check(wrong.hash());
                assert_eq!(
                    push(&client, wrong.encode(), policy::Event::Received).await,
                    policy::PushOutcome::Cached
                );
            }
            "future_inserted" => {
                accepted(
                    push(&client, signed(999, 3), policy::Event::Created).await,
                    true,
                    policy::Event::Created,
                );
            }
            "replacement_empty" => {
                worker.shutdown().unwrap();
                worker = spawn(&node, cache.clone(), config, &source);
            }
            label if label.starts_with("epoch_") => {
                within(
                    client
                        .advance_epoch(
                            Unsigned::from_decimal(row["epoch"].as_str().unwrap()).unwrap(),
                        )
                        .unwrap(),
                )
                .await
                .unwrap();
            }
            other => panic!("unhandled native row {other}"),
        }
        let stats = within(worker.client().stats().unwrap()).await.unwrap();
        assert_eq!(
            stats.epoch,
            Unsigned::from_decimal(row["epoch"].as_str().unwrap()).unwrap(),
            "{}",
            row["label"]
        );
        assert_eq!(
            stats.entries as u64,
            row["entries"].as_u64().unwrap(),
            "{}",
            row["label"]
        );
    }
    worker.shutdown().unwrap();
}
