mod common;
use ae_chain::{block::Block, publication::Origin};
use ae_codec::headers::KeyHeader;
use ae_protocol::NetworkProfile;
use ae_storage::snapshots;
use ae_stratum::{
    candidate::{Active, Candidate, Sealed},
    ingress::{Input, Solution},
    nonce::Nonce,
};
use ae_sync::chain::{
    self,
    pow::{generator::Clock, preparation, stratum},
};
use ae_types::{AccountPublicKey, Hash256};
use common::{Node, ledger_fixtures::*};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const WALL: u64 = u64::MAX - 600_000;
struct Times(Mutex<std::vec::IntoIter<i128>>);
impl Clock for Times {
    fn unix_millis(&self) -> i128 {
        self.0.lock().unwrap().next().unwrap()
    }
}
fn times(h: &KeyHeader) -> Arc<dyn Clock> {
    Arc::new(Times(Mutex::new(
        vec![i128::from(h.time - 17), h.time.into()].into_iter(),
    )))
}
fn request(h: &KeyHeader) -> preparation::Request {
    preparation::Request {
        parent: Hash256(h.common.previous_hash),
        miner: AccountPublicKey(h.miner),
        beneficiary: AccountPublicKey(h.beneficiary),
        release_info: h.info.map(u32::from_be_bytes).unwrap_or(0),
    }
}
fn input(c: &Candidate, h: &KeyHeader) -> Input {
    Input {
        account: "ak_miner".into(),
        target: c.work().unwrap().block_target,
        hash: c.hash(),
        solution: Some(Solution {
            nonce: Nonce(h.nonce),
            proof: Box::new(h.seal.map(u64::from)),
        }),
    }
}
fn sealed(h: &KeyHeader) -> Sealed {
    let mut blank = h.clone();
    blank.nonce = 0;
    blank.seal = [0; 42];
    let c = Candidate::new(blank).unwrap();
    c.solution(&input(&c, h)).unwrap()
}
fn importer(node: &Node, jobs: usize) -> chain::Importer {
    chain::Importer::new(
        node.db.clone(),
        execution(&NetworkProfile::mainnet()),
        rules(node.db.view().unwrap().identity().genesis),
        chain::Config {
            jobs,
            // Polling retains its result even beyond this wait-only timeout.
            timeout: Duration::from_nanos(1),
            ..Default::default()
        },
    )
    .unwrap()
}
async fn settle(admission: &mut stratum::Admission) -> Result<chain::Report, chain::Failure> {
    let end = Instant::now() + Duration::from_secs(15);
    while admission.poll().is_none() {
        assert!(Instant::now() < end);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let result = admission.result().unwrap().clone();
    for _ in 0..3 {
        assert_eq!(admission.poll(), Some(&result));
    }
    result
}

#[tokio::test]
async fn prepared_stratum_work_reconstructs_and_admits_six_saved_mainnet_key_blocks() {
    let node = Node::new();
    let importer = importer(&node, 2);
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let builder = importer
        .stratum_builder(snapshots.clone(), Default::default())
        .unwrap();
    let mut active = Active::new(4).unwrap();
    let mut count = 0;
    for block in blocks(&NetworkProfile::mainnet()) {
        if let Block::Key(h) = &block {
            let completed = builder.start(request(h), times(h)).unwrap().wait().unwrap();
            let id = completed.id;
            assert_eq!(snapshots.live_snapshots(), 1);
            let prepared = stratum::Prepared::from_completion(completed).unwrap();
            assert_eq!(snapshots.live_snapshots(), 0);
            assert_eq!(prepared.id, id);
            assert_eq!(prepared.calculation_time, h.time - 17);
            assert_eq!(prepared.final_time, h.time);
            assert_eq!(prepared.request, request(h));
            assert_eq!(prepared.parent(), Hash256(h.common.previous_hash));
            assert!(
                active
                    .publish(prepared.candidate().clone(), prepared.parent())
                    .unwrap()
            );
            let c = active.claim(prepared.candidate().sealing_bytes()).unwrap();
            assert!(active.candidates().is_empty());
            let solution = c.solution(&input(&c, h)).unwrap();
            assert_eq!(solution.bytes(), block.encode().unwrap());
            assert_eq!(solution.hash(), block.hash().unwrap());
            let mut admitted = importer.import_stratum(&solution, WALL, true).unwrap();
            assert_eq!(admitted.solution(), &solution);
            let report = settle(&mut admitted).await.unwrap();
            assert_eq!(report.accepted.len(), 1);
            assert_eq!(report.accepted[0].hash, solution.hash());
            assert!(matches!(
                report.accepted[0].outcome,
                chain::Outcome::Committed { .. }
            ));
            assert_eq!(report.accepted[0].micro_transactions, None);
            assert_eq!(
                node.db
                    .view()
                    .unwrap()
                    .require_record(solution.hash())
                    .unwrap()
                    .roots
                    .state
                    .hash()
                    .0,
                h.common.state_hash
            );
            let events = node.db.events().after(&prepared.source.cursor, 16).unwrap();
            assert_eq!(events.records.len(), 1);
            assert_eq!(events.records[0].context().origin, Origin::Created);
            count += 1;
        } else {
            // The saved generations also contain real microblocks. Their
            // canonical insertion supplies the next candidate's actual parent.
            let mut ticket = importer
                .import_created_block(block.encode().unwrap(), WALL)
                .unwrap();
            let end = Instant::now() + Duration::from_secs(15);
            loop {
                if let Some(result) = ticket.try_result() {
                    result.unwrap();
                    break;
                }
                assert!(Instant::now() < end);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
    assert_eq!(count, 6);
    assert_eq!(snapshots.live_snapshots(), 0);
    assert!(!importer.recovery_required());
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn admission_retains_exact_outcomes_and_pressure_never_consumes_the_callers_solution() {
    let node = Node::new();
    let importer = importer(&node, 1);
    let Block::Key(header) = blocks(&NetworkProfile::mainnet()).remove(0) else {
        panic!()
    };
    let good = sealed(&header);
    let mut invalid = header.clone();
    invalid.seal = [0; 42];
    let invalid = sealed(&invalid);
    let initial = node.db.view().unwrap().status();
    let mut rejected = importer.import_stratum(&invalid, WALL, false).unwrap();
    assert!(matches!(
        importer.import_stratum(&good, WALL, false),
        Err(chain::Error::Busy)
    ));
    let error = settle(&mut rejected).await.unwrap_err();
    assert!(matches!(
        error.error,
        chain::Error::Validation {
            operational: false,
            ..
        }
    ));
    assert_eq!(error.progress, Some(chain::Report::default()));
    assert_eq!(rejected.solution(), &invalid);
    assert_eq!(node.db.view().unwrap().status(), initial);
    let mut admitted = importer.import_stratum(&good, WALL, false).unwrap();
    assert_eq!(
        settle(&mut admitted).await.unwrap().accepted[0].hash,
        good.hash()
    );
    assert_eq!(admitted.solution(), &good);
    let committed = node.db.view().unwrap().status();
    let mut stale = importer.import_stratum(&good, WALL, true).unwrap();
    assert_eq!(
        settle(&mut stale).await.unwrap_err().error,
        chain::Error::Stale
    );
    let mut present = importer.import_stratum(&good, WALL, false).unwrap();
    assert_eq!(
        settle(&mut present).await.unwrap().accepted[0].outcome,
        chain::Outcome::Present
    );
    assert_eq!(node.db.view().unwrap().status(), committed);
    importer.stop();
    assert!(matches!(
        importer.import_stratum(&good, WALL, false),
        Err(chain::Error::Closed)
    ));
    // Stopping the importer cannot overwrite a previously retained outcome.
    assert!(admitted.poll().unwrap().is_ok());
    assert!(rejected.poll().unwrap().is_err());
    importer.join().await;
}

#[test]
fn preparation_checks_the_database_and_receipt_before_releasing_its_snapshot() {
    let node = Node::new();
    let other = Node::new();
    let importer = importer(&node, 1);
    let foreign = snapshots::Owner::start(other.db.clone(), Default::default()).unwrap();
    assert!(
        importer
            .stratum_builder(foreign, Default::default())
            .is_err()
    );
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let builder = importer
        .stratum_builder(snapshots.clone(), Default::default())
        .unwrap();
    let Block::Key(h) = blocks(&NetworkProfile::mainnet()).remove(0) else {
        panic!()
    };
    for variant in 0..8 {
        let mut done = builder
            .start(request(&h), times(&h))
            .unwrap()
            .wait()
            .unwrap();
        match variant {
            0 => done.request.parent = Hash256::ZERO,
            1 => done.request.miner = AccountPublicKey([0; 32]),
            2 => done.calculation_time = None,
            3 => done.final_time = Some(0),
            4 => done.result.as_mut().unwrap().header.common.state_hash = [0; 32],
            5 => done.result.as_mut().unwrap().state.metadata.source_id ^= 1,
            6 => done.result.as_mut().unwrap().header.nonce = 1,
            _ => done.result = Err(preparation::Error::Cancelled),
        }
        assert!(stratum::Prepared::from_completion(done).is_err());
        assert!(!builder.busy());
        assert_eq!(snapshots.live_snapshots(), 0);
    }
}
