mod common;
#[path = "common/expiry_metrics.rs"]
mod expiry_metrics;
use ae_chain::{
    block::Block,
    hyperchain::candidate as core,
    repository::{self as repo, hyperchain::candidate as build, ledger::Origin},
};
use ae_protocol::ConsensusType;
use ae_storage::{
    hyperchain::{self, candidate as owned},
    pool::{self, worker},
    snapshots,
};
use ae_sync::chain::hyperchain::packing::{self, Packing, Progress};
use ae_types::{AccountPublicKey, Hash256};
use ae_vm_fate::maps::order::Otp26;
use common::hyperchain::{Node, Parent, parents::Pair};
use serde_json::{Value as J, json};
use std::{
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};
static DATA: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_hc_pool_packing.json")).unwrap()
});
static METRICS: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_metrics_expiry.json")).unwrap()
});
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn hash(j: &J) -> Hash256 {
    Hash256(bytes(j).try_into().unwrap())
}
fn config() -> pool::Config {
    pool::Config {
        consensus: ConsensusType::Pos,
        policy: ae_mempool::pool::Config {
            minimum_miner_gas_price: 1.into(),
            tx_ttl: 20,
            future_nonce_ttl: 3,
            ..Default::default()
        },
        ..Default::default()
    }
}
fn worker(node: &Node, settings: worker::Settings) -> worker::Worker {
    worker::Worker::spawn(
        node.db.clone(),
        node.config.profile.network.clone(),
        config(),
        settings,
    )
    .unwrap()
}
fn prepare(
    node: &Node,
    owner: &hyperchain::Owner<Otp26, Parent>,
    snapshots: &snapshots::Owner,
    pair: &Pair,
    now: i128,
    limits: core::Limits,
) -> owned::Prepared<Otp26> {
    owner
        .prepare_candidate(
            snapshots,
            owned::Request {
                binding: repo::hyperchain::producer::Binding {
                    network: node.config.profile.network.clone(),
                    election: node.config.profile.contracts.election,
                    parent_start: 1.into(),
                    parent_length: 5.into(),
                    block_time: 4.into(),
                },
                input: owned::Input::Micro(build::Input {
                    parent: node.db.view().unwrap().status().tip.unwrap(),
                    now,
                    limits,
                }),
                parent: pair.client.snapshot().cache,
                pins: Arc::new(Default::default()),
                limits: Default::default(),
            },
            || Ok::<_, ()>(()),
        )
        .unwrap()
}
fn packing_config(c: &J) -> packing::Config {
    packing::Config {
        reserved_gas: c["offset"].as_i64().unwrap().into(),
        timeout: core::Timeout::Milliseconds(c["timeout"].as_u64().unwrap()),
        window_ms: c["window"].as_i64().unwrap(),
    }
}
fn wait<T>(ticket: worker::Ticket<T>) -> T {
    ticket.wait_timeout(Duration::from_secs(5)).unwrap()
}
fn pool_state(node: &Node, client: &worker::Client, expected: &J) {
    let mut members = node
        .db
        .pool_view()
        .unwrap()
        .members(100)
        .unwrap()
        .iter()
        .map(|h| hex::encode(h.0))
        .collect::<Vec<_>>();
    members.sort();
    assert_eq!(json!(members), expected["members"]);
    for (field, visited) in [("active", false), ("visited", true)] {
        for e in expected[field].as_array().unwrap() {
            let state = wait(client.inspect(hash(&e["hash"])).unwrap());
            assert_eq!(state.visited, visited);
            assert_eq!(state.failures, e["failures"].as_u64().unwrap());
        }
    }
    let info = client.info();
    assert_eq!(
        info.pool.active,
        expected["active"].as_array().unwrap().len()
    );
    assert_eq!(
        info.pool.visited,
        expected["visited"].as_array().unwrap().len()
    );
}
#[test]
fn original_pool_and_builder_match_after_selected_top_advance_in_both_profiles() {
    let pair = Pair::new(false);
    let mut count = 0;
    for profile in ["profile", "repeat_profile"] {
        for row in DATA["rows"].as_array().unwrap() {
            for parent in row["parents"].as_array().unwrap() {
                for c in parent["cases"].as_array().unwrap() {
                    let mut node = Node::new(row["corpus_index"].as_u64().unwrap() as usize);
                    let (metrics, _binding) = expiry_metrics::Expiry::bind(&node.db);
                    Arc::make_mut(&mut node.config.profile).order =
                        common::hyperchain::order(&DATA[profile]);
                    let owner = hyperchain::Owner::start(
                        node.db.clone(),
                        node.config.clone(),
                        Parent::default(),
                    )
                    .unwrap();
                    let blocks = node.blocks();
                    let n = parent["count"].as_u64().unwrap() as usize;
                    for b in &blocks[..n] {
                        owner.insert(b, 1_000_000, Origin::Gossip).unwrap();
                    }
                    let snapshots =
                        snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
                    let prepared = prepare(
                        &node,
                        &owner,
                        &snapshots,
                        &pair,
                        c["now"].as_i64().unwrap().into(),
                        Default::default(),
                    );
                    let effects = prepared.effects.clone();
                    let metadata = prepared.metadata.clone();
                    let pool = worker(
                        &node,
                        worker::Settings {
                            origins_interval: None,
                            ..Default::default()
                        },
                    );
                    let client = pool.client();
                    for tx in c["transactions"].as_array().unwrap() {
                        wait(
                            client
                                .force_push(Arc::new(
                                    ae_tx::SignedTransaction::decode(&bytes(tx)).unwrap(),
                                ))
                                .unwrap(),
                        );
                    }
                    pool_state(&node, &client, &c["initial"]);
                    let mut packing =
                        Packing::start(prepared, client.clone(), packing_config(c)).unwrap();
                    assert_eq!(metadata.status.tip, Some(hash(&c["parent"])));
                    owner.insert(&blocks[n], 1_000_000, Origin::Gossip).unwrap();
                    assert_eq!(
                        node.db.view().unwrap().status().tip,
                        Some(hash(&c["new_top"]))
                    );
                    if c["timeout"] != 0 {
                        std::thread::sleep(Duration::from_millis(3));
                    }
                    let end = Instant::now() + Duration::from_secs(10);
                    let mut selected = 0;
                    let mut reported = 0;
                    while !packing.ready() {
                        assert!(Instant::now() < end, "packing stalled: {}", c["name"]);
                        match packing.step().unwrap() {
                            Progress::Waiting => std::thread::sleep(Duration::from_millis(1)),
                            Progress::Selected => {
                                pool_state(&node, &client, &c["requests"][selected]["state"]);
                                selected += 1;
                            }
                            Progress::Reported { .. } => {
                                pool_state(&node, &client, &c["failures"][reported]["state"]);
                                reported += 1;
                            }
                            _ => {}
                        }
                    }
                    assert_eq!(selected, c["requests"].as_array().unwrap().len());
                    assert_eq!(reported, c["failures"].as_array().unwrap().len());
                    assert_eq!(packing.expired(), c["expired"].as_bool().unwrap());
                    let native = METRICS["rows"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|m| {
                            m["corpus_index"] == row["corpus_index"]
                                && m["parent_count"] == parent["count"]
                                && m["name"] == c["name"]
                        })
                        .unwrap();
                    let expected = native["events"].as_array().unwrap().len() as u64;
                    assert_eq!(metrics.count(), expected);
                    packing.retry().unwrap();
                    assert_eq!(packing.step(), Ok(Progress::Finished));
                    assert_eq!(metrics.count(), expected);
                    pool_state(&node, &client, &c["final"]);
                    let prepared = packing.finish().unwrap();
                    assert_eq!(prepared.effects, effects);
                    assert_eq!(prepared.metadata.status, metadata.status);
                    assert_eq!(prepared.metadata.source_id, metadata.source_id);
                    assert_eq!(prepared.metadata.resilience, metadata.resilience);
                    assert_eq!(prepared.metadata.cursor, metadata.cursor);
                    let owned::Candidate::Micro(p) = prepared.outcome.unwrap() else {
                        panic!()
                    };
                    let expected = &c["result"];
                    assert_eq!(
                        Block::Micro(p.candidate.block().clone()).encode().unwrap(),
                        bytes(&expected["block"]),
                        "{}",
                        c["name"]
                    );
                    let keys = ae_hyperchain::signing::Keys::new([12u64, 13, 14].map(|n| {
                        let mut seed = [0; 32];
                        seed[24..].copy_from_slice(&n.to_be_bytes());
                        ae_crypto::Ed25519SecretKey::from_seed(&seed)
                    }));
                    let signed = p
                        .candidate
                        .signed::<()>(
                            &keys,
                            AccountPublicKey(bytes(&c["leader"]).try_into().unwrap()),
                        )
                        .unwrap();
                    assert_eq!(
                        Block::Micro(signed).encode().unwrap(),
                        bytes(&expected["signed"])
                    );
                    assert_eq!(
                        hex::encode(p.candidate.trees().roots().hash().0),
                        expected["trees"]["state_root"]
                    );
                    for a in expected["trees"]["accounts"].as_array().unwrap() {
                        let k = AccountPublicKey(bytes(&a["key"]).try_into().unwrap());
                        assert_eq!(
                            p.candidate
                                .trees()
                                .accounts
                                .lookup(k)
                                .unwrap()
                                .unwrap()
                                .serialize()
                                .unwrap(),
                            bytes(&a["encoded"])
                        );
                    }
                    drop(p);
                    assert_eq!(snapshots.live_snapshots(), 0);
                    pool.shutdown().unwrap();
                    count += 1;
                }
            }
        }
    }
    assert_eq!(count, 112);
}
#[test]
fn stopped_pool_cannot_replay_selection_or_rebind_replacement_worker() {
    let node = Node::new(1);
    let pair = Pair::new(false);
    let owner =
        hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let prepared = prepare(&node, &owner, &snapshots, &pair, 999, Default::default());
    let pool = worker(&node, Default::default());
    let client = pool.client();
    pool.shutdown().unwrap();
    let mut packing = Packing::start(
        prepared,
        client.clone(),
        packing_config(&DATA["rows"][0]["parents"][0]["cases"][0]),
    )
    .unwrap();
    let expected = packing::Error::Pool {
        stage: packing::Stage::Selection,
        error: worker::Error::Stopped,
    };
    assert_eq!(packing.step(), Err(expected.clone()));
    assert_eq!(packing.retry(), Err(expected.clone()));
    let replacement = worker(&node, Default::default());
    for _ in 0..3 {
        assert_eq!(packing.step(), Err(expected.clone()));
    }
    assert_eq!(replacement.info().submitted, 0);
    assert_eq!(client.info().submitted, 0);
    let failed = packing.finish().err().unwrap();
    assert_eq!(failed.error, packing::Error::Sequence);
    drop(failed);
    assert_eq!(snapshots.live_snapshots(), 0);
    replacement.shutdown().unwrap();
}
#[test]
fn foreign_pool_returns_original_candidate_without_losing_its_snapshot_or_effects() {
    let node = Node::new(1);
    let foreign = Node::new(1);
    let pair = Pair::new(false);
    let owner =
        hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let prepared = prepare(&node, &owner, &snapshots, &pair, 999, Default::default());
    let effects = prepared.effects.clone();
    let pool = worker(&foreign, Default::default());
    let error = Packing::start(
        prepared,
        pool.client(),
        packing_config(&DATA["rows"][0]["parents"][0]["cases"][0]),
    )
    .err()
    .unwrap();
    assert_eq!(error.error, packing::Error::Binding);
    assert!(error.input.outcome.is_ok());
    assert_eq!(error.input.effects, effects);
    assert_eq!(snapshots.live_snapshots(), 1);
    drop(error);
    assert_eq!(snapshots.live_snapshots(), 0);
    assert_eq!(pool.info().submitted, 0);
    pool.shutdown().unwrap();
}

#[test]
fn execution_limit_retains_selected_receipt_and_never_replays_pool_visits_or_failure_reports() {
    let node = Node::new(1);
    let pair = Pair::new(false);
    let owner =
        hyperchain::Owner::start(node.db.clone(), node.config.clone(), Parent::default()).unwrap();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let prepared = prepare(
        &node,
        &owner,
        &snapshots,
        &pair,
        999,
        core::Limits {
            offered: 0,
            ..Default::default()
        },
    );
    let pool = worker(&node, Default::default());
    let client = pool.client();
    let c = &DATA["rows"][0]["parents"][0]["cases"][1];
    let tx = Arc::new(ae_tx::SignedTransaction::decode(&bytes(&c["transactions"][1])).unwrap());
    wait(client.force_push(tx.clone()).unwrap());
    let mut packing = Packing::start(prepared, client.clone(), packing_config(c)).unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < end);
        if packing.step().unwrap() == Progress::Selected {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let submitted = client.info().submitted;
    let expected = packing.step().unwrap_err();
    assert!(matches!(
        expected,
        packing::Error::Execution {
            retryable: true,
            ..
        }
    ));
    assert!(!packing.in_flight());
    assert_eq!(packing.step(), Err(expected.clone()));
    packing.retry().unwrap();
    assert_eq!(packing.step(), Err(expected));
    assert_eq!(client.info().submitted, submitted);
    let inspection = wait(client.inspect(tx.hash()).unwrap());
    assert!(inspection.visited);
    assert_eq!(inspection.failures, 0);
    drop(packing);
    assert_eq!(snapshots.live_snapshots(), 0);
    pool.shutdown().unwrap();
}
