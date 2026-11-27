use super::lifecycle::{Gate, GateClock, Release};
use super::*;

fn native() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../ae-node/tests/fixtures/erlang_stratum_identity.json"
    ))
    .unwrap()
}
fn start(node: &Node, enabled: bool, gate: &Arc<Gate>) -> producer::Worker {
    producer::Worker::spawn(
        &node.importer,
        node.generator(),
        node.keys(),
        producer::Config {
            enabled,
            external_key_work: true,
            ..node.config()
        },
        GateClock(gate.clone()),
    )
    .unwrap()
}
fn stop(worker: producer::Worker, gate: &Gate) -> producer::Report {
    worker.client().request_shutdown();
    gate.allow(usize::MAX);
    finish(worker)
}

#[test]
fn external_stratum_work_promotes_while_stopped_then_signs_real_microblocks_when_enabled() {
    let native = native();
    for enabled in [false, true] {
        let expected = native["promotion"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["running"] == enabled)
            .unwrap();
        assert_eq!(expected["miner_matches_block"], true);
        let node = Node::new(1, true);
        node.push(1);
        let candidate = node.keys().info().keys.candidate;
        let gate = Gate::new();
        let _release = Release(gate.clone());
        let worker = start(&node, enabled, &gate);
        let client = worker.client();
        gate.reach(&client, |s| {
            s.promotions == expected["promotions"].as_u64().unwrap()
        });
        assert_eq!(node.keys().info().keys.signing, candidate);
        assert_ne!(node.keys().info().keys.candidate, candidate);
        assert!(client.status().mining.is_none());
        if !enabled {
            assert_eq!(client.status().signatures, 0);
            assert_eq!(client.status().admissions, 0);
            assert!(!client.status().enabled);
            for _ in 0..3 {
                gate.step();
            }
            assert_eq!(client.status().admissions, 0);
            client.set_enabled(true).unwrap();
        }
        gate.allow(usize::MAX);
        let hash = admitted(&node, &client);
        assert_eq!(node.db.view().unwrap().status().tip, Some(hash));
        let report = finish(worker);
        assert!(report.status.failure.is_none());
        assert_eq!(report.status.promotions, 1);
    }
}

#[test]
fn disabling_external_production_retains_an_already_accepted_promotion() {
    let node = Node::new(1, true);
    node.push(1);
    let candidate = node.keys().info().keys.candidate;
    let gate = Gate::new();
    let _release = Release(gate.clone());
    let worker = start(&node, true, &gate);
    let client = worker.client();
    gate.reach(&client, |s| s.pending && s.promotions == 0);
    let revision = client.set_enabled(false).unwrap();
    gate.reach(&client, |s| {
        s.promotions == 1 && s.applied_revision == revision
    });
    assert_eq!(node.keys().info().keys.signing, candidate);
    assert_eq!(client.status().signatures, 0);
    assert_eq!(client.status().admissions, 0);
    let output = client.front().unwrap();
    assert!(
        matches!(&output.completion,producer::Completion::Promoted{expected,result:Ok(_)}if *expected==candidate)
    );
    assert!(client.acknowledge(&output));
    drop(output);
    client.set_enabled(true).unwrap();
    gate.allow(usize::MAX);
    admitted(&node, &client);
    assert!(finish(worker).status.failure.is_none());
}

#[test]
fn external_producer_shutdown_keeps_the_exact_accepted_promotion_receipt() {
    let node = Node::new(1, true);
    let candidate = node.keys().info().keys.candidate;
    let gate = Gate::new();
    let _release = Release(gate.clone());
    let worker = start(&node, false, &gate);
    let client = worker.client();
    gate.reach(&client, |s| s.pending && s.promotions == 0);
    let report = stop(worker, &gate);
    assert!(report.status.failure.is_none());
    assert_eq!(report.status.admissions, 0);
    assert!(
        matches!(report.completions.as_slice(),[producer::Completion::Promoted{expected,result:Ok(_)}]if *expected==candidate)
    );
    assert_eq!(node.keys().info().keys.signing, candidate);
}

#[test]
fn external_key_work_rejects_a_simultaneous_local_mining_owner() {
    let node = Node::new(1, false);
    let result = producer::Worker::spawn_with_mining(
        &node.importer,
        node.generator(),
        node.keys(),
        producer::Config {
            external_key_work: true,
            ..node.config()
        },
        producer::SystemClock,
        producer::mining::Inputs {
            snapshots: node._snapshots.clone(),
            config: super::mining::config(0),
            clock: Arc::new(generator::SystemClock),
        },
    );
    assert!(matches!(result, Err(producer::Error::Configuration)));
    assert!(node.keys().info().accepting);
    assert!(node.generator().status().accepting);
}

#[test]
fn external_candidate_identity_waits_for_matching_top_and_completed_promotion() {
    let node = Node::new(1, true);
    let parent = node.db.view().unwrap().status().tip.unwrap();
    let old = node.keys().info().keys.candidate;
    let gate = Gate::new();
    let _release = Release(gate.clone());
    let worker = start(&node, false, &gate);
    let client = worker.client();
    assert_eq!(client.candidate_key(parent).unwrap(), None);
    assert_eq!(client.candidate_key(Hash256::ZERO).unwrap(), None);
    gate.reach(&client, |s| s.pending && s.promotions == 0);
    assert_eq!(client.candidate_key(parent).unwrap(), None);
    gate.reach(&client, |s| s.promotions == 1 && !s.promotion_pending);
    let next = client.candidate_key(parent).unwrap().unwrap();
    assert_ne!(next, old);
    assert_eq!(next, node.keys().info().keys.candidate);
    assert_eq!(client.candidate_key(Hash256::ZERO).unwrap(), None);
    client.request_shutdown();
    assert_eq!(client.candidate_key(parent), Err(producer::Error::Closed));
    assert!(stop(worker, &gate).status.failure.is_none());
}
