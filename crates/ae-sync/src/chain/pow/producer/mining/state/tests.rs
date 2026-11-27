use super::*;
use crate::test_support::mining_metrics::MiningMetrics;
use crate::test_support::{Node, ledger_fixtures as fixtures};
use ae_protocol::NetworkProfile;
use ae_vm_fate::maps::order::Otp26;

const WALL: u64 = u64::MAX - 600_000;
struct Clock(u64);
impl generator::Clock for Clock {
    fn unix_millis(&self) -> i128 {
        self.0.into()
    }
}
fn until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "bounded mining coordinator wait");
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn importer(node: &Node) -> chain::Importer<Otp26> {
    chain::Importer::new(
        node.db.clone(),
        fixtures::execution(&NetworkProfile::mainnet()),
        fixtures::rules(node.db.view().unwrap().identity().genesis),
        Default::default(),
    )
    .unwrap()
}
fn context(header: &KeyHeader, enabled: bool, strict: bool) -> Context {
    Context {
        top: Hash256(header.common.previous_hash),
        miner: AccountPublicKey(header.miner),
        enabled,
        strict,
        wall: WALL,
        now: Instant::now(),
    }
}
fn configuration(header: &KeyHeader) -> Config {
    let mut config = Config::new(AccountPublicKey(header.beneficiary));
    config.instances[0].worker.solver.memory_bytes = 1;
    config.release_info = header.info.map(u32::from_be_bytes).unwrap_or(0);
    config
}

#[test]
fn mining_graph_must_match_the_importer_before_workers_or_preparation_start() {
    let node = Node::new();
    let importer = importer(&node);
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let mut config = Config::new(AccountPublicKey([1; 32]));
    config.edge_bits = cuckoo::EdgeBits::new(15).unwrap();
    assert!(matches!(
        Mining::new(
            &importer,
            Inputs {
                snapshots: snapshots.clone(),
                config,
                clock: Arc::new(Clock(WALL)),
            }
        ),
        Err(Error::Binding)
    ));
    assert_eq!(snapshots.live_snapshots(), 0);
}
fn ready(
    importer: &chain::Importer<Otp26>,
    snapshots: snapshots::Owner,
    header: &KeyHeader,
    config: Config,
) -> Mining<Otp26> {
    let mut mining = Mining::new(
        importer,
        Inputs {
            snapshots: snapshots.clone(),
            config,
            clock: Arc::new(Clock(header.time)),
        },
    )
    .unwrap();
    mining.act(importer, context(header, false, false)).unwrap();
    until(|| {
        mining
            .poll(
                Hash256(header.common.previous_hash),
                AccountPublicKey(header.miner),
                Instant::now(),
            )
            .unwrap();
        mining.output.is_some()
    });
    let Some(Completion::Prepared {
        result: Ok(candidate),
        used: true,
        ..
    }) = mining.take_output()
    else {
        panic!("expected a current candidate")
    };
    assert_eq!(candidate.header.seal, [0; 42]);
    assert_eq!(candidate.header.nonce, 0);
    let mut expected = header.clone();
    expected.seal = [0; 42];
    expected.nonce = 0;
    assert_eq!(candidate.header, expected);
    assert_eq!(snapshots.live_snapshots(), 0);
    mining.candidates[0].nonce = header.nonce.into();
    mining
}
fn prepare(
    importer: &chain::Importer<Otp26>,
    snapshots: snapshots::Owner,
    header: &KeyHeader,
) -> Mining<Otp26> {
    let mut mining = ready(importer, snapshots, header, configuration(header));
    mining.act(importer, context(header, true, false)).unwrap();
    assert_eq!(mining.status().active, 1);
    mining
}
// Replay an unchanged public proof into the coordinator's result handler.
// The real CPU receipt below is a resource failure, not a fresh mined proof.
// Canonical verification and ordinary durable admission remain fully enabled.
fn replay(mining: &Mining<Otp26>, header: &KeyHeader) -> miner::Completion {
    let actual = mining.cpus[0].job.as_ref().unwrap().ticket.blocking_wait();
    assert!(matches!(actual.result, Err(miner::Error::Solve { .. })));
    let mut result = actual.as_ref().clone();
    result.result = Ok(miner::Outcome::Found {
        nonce: header.nonce,
        proof: Box::new(header.seal.map(u64::from)),
    });
    result
}
fn retire(mining: Mining<Otp26>) -> Vec<Completion> {
    let mut retired = mining.retire();
    let mut outputs = vec![];
    until(|| retired.poll(&mut outputs));
    assert!(retired.is_settled());
    let count = outputs.len();
    for _ in 0..4 {
        assert!(retired.poll(&mut outputs));
    }
    assert_eq!(outputs.len(), count);
    outputs
}

#[tokio::test]
async fn public_proof_replay_uses_normal_admission_and_keeps_accepted_receipts_on_retirement() {
    let node = Node::new();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let importer = importer(&node);
    let mut count = 0;
    for block in fixtures::blocks(&NetworkProfile::mainnet()) {
        let Block::Key(header) = &block else {
            importer
                .import_block(block.encode().unwrap(), WALL)
                .unwrap()
                .wait()
                .await
                .unwrap();
            continue;
        };
        let mut mining = prepare(&importer, snapshots.clone(), header);
        let replay = replay(&mining, header);
        mining
            .searched(0, Arc::new(replay), Instant::now())
            .unwrap();
        assert!(matches!(
            mining.take_output(),
            Some(Completion::Searched { selected: true, .. })
        ));
        assert!(mining.candidates.is_empty());
        let hash = block.hash().unwrap();
        mining
            .act(&importer, context(header, true, count % 2 == 0))
            .unwrap();
        assert!(mining.admission.is_some());
        assert!(mining.solution.is_none());
        // Stopping after the importer accepts work cannot erase or retry it.
        mining.invalidate().unwrap();
        let outputs = retire(mining);
        assert_eq!(
            outputs
                .iter()
                .filter(|o| matches!(o, Completion::Admitted { .. }))
                .count(),
            1
        );
        let result = outputs
            .iter()
            .find_map(|o| match o {
                Completion::Admitted {
                    hash: observed,
                    result,
                    ..
                } => {
                    assert_eq!(*observed, hash);
                    Some(result)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(result.as_ref().unwrap().accepted.len(), 1);
        assert_eq!(result.as_ref().unwrap().accepted[0].hash, hash);
        assert_eq!(node.db.view().unwrap().status().tip, Some(hash));
        assert_eq!(
            node.db
                .view()
                .unwrap()
                .require_block(hash)
                .unwrap()
                .encode()
                .unwrap(),
            block.encode().unwrap()
        );
        assert_eq!(snapshots.live_snapshots(), 0);
        count += 1;
    }
    assert_eq!(count, 6);
}

#[test]
fn proof_binding_rejects_wrong_job_work_nonce_and_edges_before_admission() {
    let node = Node::new();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let importer = importer(&node);
    let Block::Key(header) = fixtures::blocks(&NetworkProfile::mainnet()).remove(0) else {
        panic!()
    };
    let mining = prepare(&importer, snapshots, &header);
    let good = replay(&mining, &header);
    let job = mining.cpus[0].job.as_ref().unwrap();
    assert_eq!(*seal(job, &good).unwrap().unwrap(), header);
    for fault in 0..8 {
        let mut bad = good.clone();
        match fault {
            0 => bad.id.sequence += 1,
            1 => bad.work.hash.0[0] ^= 1,
            2 => bad.work.target.0 ^= 1,
            3 => bad.work.edge_bits = 15,
            4 => bad.work.nonces = Nonces::reserve(header.nonce as u128 + 1, 1).unwrap(),
            _ => {
                let Ok(miner::Outcome::Found { nonce, proof }) = &mut bad.result else {
                    unreachable!()
                };
                match fault {
                    5 => *nonce = nonce.wrapping_add(1),
                    6 => proof[0] = u32::MAX as u64 + 1,
                    _ => proof.fill(0),
                }
            }
        }
        assert_eq!(seal(job, &bad), Err(Error::Binding), "fault {fault}");
    }
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    retire(mining);
}

#[tokio::test]
async fn strict_stale_replay_is_rejected_and_ordinary_duplicate_keeps_its_receipt() {
    for strict in [false, true] {
        let node = Node::new();
        let (metrics, _binding) = MiningMetrics::bind(node.db.instrumentation());
        let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
        let importer = importer(&node);
        let blocks = fixtures::blocks(&NetworkProfile::mainnet());
        let Block::Key(header) = &blocks[0] else {
            panic!()
        };
        let mut mining = prepare(&importer, snapshots, header);
        let replay = replay(&mining, header);
        mining
            .searched(0, Arc::new(replay), Instant::now())
            .unwrap();
        mining.take_output();
        assert_eq!(metrics.counts(), (1, 0));
        // A competing admission wins before this result is submitted.
        for block in &blocks[..2] {
            importer
                .import_block(block.encode().unwrap(), WALL)
                .unwrap()
                .wait()
                .await
                .unwrap();
        }
        let before = node.db.view().unwrap().status();
        mining
            .act(&importer, context(header, true, strict))
            .unwrap();
        let outputs = retire(mining);
        let result = outputs
            .iter()
            .find_map(|o| match o {
                Completion::Admitted { result, .. } => Some(result),
                _ => None,
            })
            .unwrap();
        if strict {
            assert!(matches!(
                result,
                Err(chain::Failure {
                    error: chain::Error::Stale,
                    ..
                })
            ));
        } else {
            assert!(result.is_ok(), "{result:?}");
        }
        assert_eq!(node.db.view().unwrap().status(), before);
        assert_eq!(metrics.counts(), (1, 0));
    }
}

mod external;
mod timeouts;

#[test]
fn native_retry_replies_follow_live_candidate_ownership_not_backoff_or_receipt_replay() {
    let rows: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_metrics_mining_retry.json"
    )))
    .unwrap();
    let node = Node::new();
    let importer = importer(&node);
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let Block::Key(header) = &fixtures::blocks(&NetworkProfile::mainnet())[0] else {
        panic!()
    };
    let (metrics, _binding) = MiningMetrics::bind(node.db.instrumentation());
    let mut expected = 0;
    for row in rows.as_array().unwrap() {
        let mut mining = prepare(&importer, snapshots.clone(), header);
        let mut result = replay(&mining, header);
        result.result = if row["reason"] == "no_solution" {
            Ok(miner::Outcome::NoSolution)
        } else {
            Err(miner::Error::External(miner::external::Error::Io {
                operation: miner::external::Operation::Spawn,
                kind: std::io::ErrorKind::NotFound,
            }))
        };
        match row["case_name"].as_str().unwrap() {
            "invalidated" => mining.invalidate().unwrap(),
            "missing" | "old_one" | "old_two" => {
                // Another real preparation supplies a distinct candidate id.
                // The outstanding result remains bound to the old CPU job.
                let mut fresh = ready(&importer, snapshots.clone(), header, configuration(header));
                if row["case_name"] == "missing" {
                    mining.candidates.clear();
                }
                mining.candidates.insert(0, fresh.candidates.remove(0));
                retire(fresh);
            }
            "current" | "current_stale" => (),
            _ => panic!("unexpected native case"),
        }
        let count: u64 = row["result"]["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["name"][3] == "mining" && m["name"][4] == "retries")
            .map(|m| m["value"].as_u64().unwrap())
            .sum();
        expected += count;
        mining
            .searched(0, Arc::new(result), Instant::now())
            .unwrap();
        assert_eq!(metrics.counts(), (0, expected), "{row:?}");
        retire(mining);
        assert_eq!(metrics.counts(), (0, expected));
    }
    assert_eq!(expected, 10);
}

#[test]
fn actual_resource_failure_and_retired_external_replies_are_not_native_retries() {
    let node = Node::new();
    let importer = importer(&node);
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let Block::Key(header) = &fixtures::blocks(&NetworkProfile::mainnet())[0] else {
        panic!()
    };
    let (metrics, _binding) = MiningMetrics::bind(node.db.instrumentation());
    let mut mining = prepare(&importer, snapshots.clone(), header);
    let actual = mining.cpus[0].job.as_ref().unwrap().ticket.blocking_wait();
    assert!(matches!(actual.result, Err(miner::Error::Solve { .. })));
    mining.searched(0, actual, Instant::now()).unwrap();
    retire(mining);
    let mut config = configuration(header);
    config.instances[0].external = Some(miner::external::Config::new(
        std::env::temp_dir().join(format!("missing-metrics-miner-{}", rand::random::<u128>())),
        std::env::temp_dir(),
    ));
    let mut mining = ready(&importer, snapshots, header, config);
    mining.act(&importer, context(header, true, false)).unwrap();
    let actual = mining.cpus[0].job.as_ref().unwrap().ticket.blocking_wait();
    assert!(matches!(actual.result, Err(miner::Error::External(_))));
    // Retirement joins and retains this exact reply without processing it
    // through the live conductor's retry boundary.
    retire(mining);
    assert_eq!(metrics.counts(), (0, 0));
}
