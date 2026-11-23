use super::*;
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
fn prepare(
    importer: &chain::Importer<Otp26>,
    snapshots: snapshots::Owner,
    header: &KeyHeader,
) -> Mining<Otp26> {
    let mut config = Config::new(AccountPublicKey(header.beneficiary));
    config.instances[0].worker.solver.memory_bytes = 1;
    config.release_info = header.info.map(u32::from_be_bytes).unwrap_or(0);
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
    }
}
