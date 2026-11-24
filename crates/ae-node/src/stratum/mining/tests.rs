use super::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(1);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "ae-stratum-mining-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
use ae_chain::{
    block::Block,
    publication::Origin,
    repository::{Identity, ledger},
};
use ae_codec::headers::{Header, KeyHeader};
use ae_protocol::NetworkProfile;
use ae_state::{
    Trees,
    genesis::{self, Network},
};
use ae_stratum::{ingress::Solution, nonce::Nonce, runtime::pool::Sink};
use ae_sync::chain::pow::{preparation, stratum::Prepared};
use ae_types::{AccountPublicKey, Hash256};
#[path = "../../../../ae-sync/tests/common/ledger_fixtures.rs"]
mod fixture;
const WALL: u64 = u64::MAX - 600_000;
struct ClockValue(i128);
impl Clock for ClockValue {
    fn unix_millis(&self) -> i128 {
        self.0
    }
}
fn open(t: &Temp) -> Arc<Database> {
    let profile = NetworkProfile::mainnet();
    let Header::Key(header) = genesis::header(Network::Mainnet).unwrap() else {
        panic!()
    };
    let identity = Identity::new(
        &profile,
        Header::Key(header.clone()).hash().unwrap(),
        Hash256::ZERO,
    );
    let db = Arc::new(Database::open(&t.0, identity).unwrap());
    if db.view().unwrap().status().tip.is_none() {
        let trees = Trees {
            accounts: genesis::state(Network::Mainnet).unwrap().0,
            contracts: Default::default(),
            channels: Default::default(),
            calls: Default::default(),
            names: Default::default(),
            oracles: Default::default(),
        };
        let plan = ledger::bootstrap(
            &db.view().unwrap(),
            &profile,
            &header,
            &trees,
            Default::default(),
        )
        .unwrap();
        db.commit(plan).unwrap();
    }
    db
}
fn importer(db: &Arc<Database>, jobs: usize) -> chain::Importer {
    chain::Importer::new(
        db.clone(),
        fixture::execution(&NetworkProfile::mainnet()),
        fixture::rules(db.view().unwrap().identity().genesis),
        chain::Config {
            jobs,
            ..Default::default()
        },
    )
    .unwrap()
}
fn inputs(importer: &chain::Importer) -> Inputs {
    Inputs {
        admitter: importer.stratum_admitter(),
        limits: Default::default(),
        strictly_follow_top: true,
        clock: Arc::new(ClockValue(WALL.into())),
    }
}
fn start(db: &Arc<Database>, importer: &chain::Importer) -> Accounting {
    Accounting::start_mining(
        db.clone(),
        Default::default(),
        Default::default(),
        Default::default(),
        inputs(importer),
    )
    .unwrap()
}
fn until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !condition() {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
}
fn settle(ticket: &mut chain::Ticket) -> Result<chain::Report, chain::Failure> {
    let mut result = None;
    until(|| {
        result = ticket.try_result();
        result.is_some()
    });
    result.unwrap()
}
fn stop(importer: chain::Importer) {
    importer.stop();
    runtime().block_on(importer.join());
}
fn first() -> KeyHeader {
    let Block::Key(header) = fixture::blocks(&NetworkProfile::mainnet()).remove(0) else {
        panic!()
    };
    header
}
fn candidate(h: &KeyHeader) -> Candidate {
    let mut blank = h.clone();
    blank.nonce = 0;
    blank.seal = [0; 42];
    Candidate::new(blank).unwrap()
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
fn effects(input: &Input) -> Vec<share::Effect> {
    let mut effects = vec![share::Effect::SubmitShare {
        account: input.account.clone(),
        target: input.target.clone(),
        hash: input.hash,
    }];
    if let Some(s) = &input.solution {
        effects.push(share::Effect::SubmitSolution {
            hash: input.hash,
            nonce: s.nonce,
            proof: s.proof.clone(),
        });
    }
    effects
}
fn date() -> Date {
    Date([2026, 11, 24, 0, 0, 0])
}
fn publish(client: &Client, candidate: Candidate) -> journal::Publication {
    let Reply::Published(receipt) = runtime()
        .block_on(client.publish(candidate, date()).unwrap().completion)
        .unwrap()
    else {
        panic!()
    };
    *receipt
}
fn done(client: &Client, id: Id) -> journal::Record {
    let mut record = None;
    until(|| {
        record = client.get(id).unwrap();
        record
            .as_ref()
            .is_some_and(|r| matches!(r.state, journal::State::Done(_)))
    });
    record.unwrap()
}
fn accepted(client: &super::super::Client, input: &Input) -> Id {
    let receipt = client.submit(&effects(input)).unwrap();
    let id = receipt.id;
    assert_eq!(runtime().block_on(receipt.completion).unwrap(), id);
    id
}
#[test]
fn six_saved_mainnet_keys_cross_preparation_journal_accounting_and_created_admission() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 2);
    let snapshots = ae_storage::snapshots::Owner::start(db.clone(), Default::default()).unwrap();
    let builder = importer
        .stratum_builder(snapshots.clone(), Default::default())
        .unwrap();
    let owner = start(&db, &importer);
    let accounting = owner.client();
    let client = accounting.mining().unwrap();
    let mut count = 0;
    for block in fixture::blocks(&NetworkProfile::mainnet()) {
        if let Block::Key(h) = &block {
            let request = preparation::Request {
                parent: Hash256(h.common.previous_hash),
                miner: AccountPublicKey(h.miner),
                beneficiary: AccountPublicKey(h.beneficiary),
                release_info: h.info.map(u32::from_be_bytes).unwrap_or(0),
            };
            let prepared = Prepared::from_completion(
                builder
                    .start(request, Arc::new(ClockValue(h.time.into())))
                    .unwrap()
                    .wait()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(snapshots.live_snapshots(), 0);
            let published = publish(&client, prepared.candidate().as_ref().clone());
            assert!(published.published);
            let id = accepted(&accounting, &input(&published.candidate, h));
            let record = done(&client, id);
            assert!(matches!(
                record.state,
                journal::State::Done(journal::Outcome::Accepted(journal::Accepted::Committed(_)))
            ));
            assert_eq!(record.attempts, 1);
            assert_eq!(record.accounted_key, Some(record.receipt.share_key));
            assert_eq!(
                record.solution().unwrap().unwrap().bytes(),
                block.encode().unwrap()
            );
            assert!(client.info().unwrap().active.is_empty());
            let view = db.view().unwrap();
            assert_eq!(
                view.require_record(block.hash().unwrap())
                    .unwrap()
                    .roots
                    .state
                    .hash()
                    .0,
                h.common.state_hash
            );
            let events = db.events().after(&prepared.source.cursor, 16).unwrap();
            assert_eq!(events.records.len(), 1);
            assert_eq!(events.records[0].context().origin, Origin::Created);
            drop(view);
            let reply = client.acknowledge(record).unwrap();
            assert_eq!(
                runtime().block_on(reply.completion).unwrap(),
                Reply::Acknowledged(id)
            );
            assert!(client.get(id).unwrap().is_none());
            assert!(
                db.stratum(Default::default())
                    .unwrap()
                    .ingress(Default::default())
                    .unwrap()
                    .last_closed()
            );
            count += 1;
        } else {
            settle(
                &mut importer
                    .import_created_block(block.encode().unwrap(), WALL)
                    .unwrap(),
            )
            .unwrap();
        }
    }
    assert_eq!(count, 6);
    assert_eq!(client.info().unwrap().counts, [0; 3]);
    let report = owner.shutdown();
    assert!(report.failure.is_none());
    assert!(report.admission.is_none());
    assert!(report.pending.is_empty());
    assert!(report.controls.is_empty());
    assert_eq!(accounting.retained(), (0, 0));
    stop(importer);
}
#[test]
fn busy_admission_survives_dropped_completions_shutdown_and_new_epochs_without_reaccounting() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    // Unread accepted ticket retains the only importer slot, even if finished.
    let mut pressure = importer.import_created_block(vec![0], WALL).unwrap();
    let owner = start(&db, &importer);
    let accounting = owner.client();
    let client = accounting.mining().unwrap();
    let h = first();
    let c = candidate(&h);
    drop(client.publish(c.clone(), date()).unwrap());
    let share = accounting.submit(&effects(&input(&c, &h))).unwrap();
    let id = share.id;
    drop(share);
    until(|| {
        client
            .get(id)
            .unwrap()
            .is_some_and(|r| r.attempts > 0 && r.state == journal::State::Ready)
    });
    let saved = client.get(id).unwrap().unwrap();
    let counts = db.stratum(Default::default()).unwrap().info().counts;
    let report = owner.shutdown();
    assert!(report.failure.is_none());
    assert!(report.admission.is_none());
    assert_eq!(accounting.retained(), (0, 0));
    assert!(matches!(
        client.publish(c.clone(), date()),
        Err(pool::Error::Closed)
    ));
    settle(&mut pressure).unwrap_err();
    let next = start(&db, &importer);
    let fresh = next.client().mining().unwrap();
    assert!(fresh.epoch() > client.epoch());
    let final_record = done(&fresh, id);
    assert_eq!(final_record.receipt, saved.receipt);
    assert_eq!(final_record.accounted_key, saved.accounted_key);
    assert!(matches!(
        final_record.state,
        journal::State::Done(journal::Outcome::Accepted(_))
    ));
    assert_eq!(
        db.stratum(Default::default()).unwrap().info().counts,
        counts
    );
    assert!(next.shutdown().failure.is_none());
    stop(importer);
}
#[test]
fn recovered_canonical_commit_is_observed_without_claiming_another_created_event() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let h = first();
    let c = candidate(&h);
    let input = input(&c, &h);
    let mut writer = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let epoch = writer.begin_mining(Default::default()).unwrap();
    let ingress = writer.begin_ingress(Default::default()).unwrap();
    writer
        .publish_work(&epoch, 1, c, date(), Default::default())
        .unwrap();
    let receipt = writer
        .accept_mining(
            &ingress,
            1,
            input,
            Default::default(),
            &epoch,
            Default::default(),
        )
        .unwrap();
    let record = db
        .stratum(Default::default())
        .unwrap()
        .mining(Default::default())
        .unwrap()
        .get(receipt.id)
        .unwrap()
        .unwrap();
    let submitting = writer
        .begin_submission(&record, Default::default())
        .unwrap();
    let solution = submitting.solution().unwrap().unwrap();
    let mut ticket = importer.import_stratum(&solution, WALL, true).unwrap();
    until(|| ticket.poll().is_some());
    ticket.result().unwrap().as_ref().unwrap();
    drop(ticket);
    let status = db.view().unwrap().status();
    drop(writer);
    let owner = start(&db, &importer);
    let client = owner.client().mining().unwrap();
    let recovered = done(&client, receipt.id);
    assert_eq!(
        recovered.state,
        journal::State::Done(journal::Outcome::Accepted(journal::Accepted::Observed(
            status.revision
        )))
    );
    assert_eq!(recovered.attempts, 1);
    assert_eq!(db.view().unwrap().status(), status);
    assert!(owner.shutdown().failure.is_none());
    stop(importer);
}
#[test]
fn worker_drop_joins_accepted_admission_before_another_writer_can_reconcile() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let mut writer = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut worker = Worker::start(
        &mut writer,
        db.clone(),
        Default::default(),
        Default::default(),
        inputs(&importer),
    )
    .unwrap();
    let ingress = writer.begin_ingress(Default::default()).unwrap();
    let h = first();
    let c = candidate(&h);
    worker
        .control(
            &mut writer,
            &Request::Publish {
                sequence: 1,
                candidate: c.clone(),
                date: date(),
            },
        )
        .unwrap();
    let receipt = worker
        .accept(&mut writer, &ingress, 1, input(&c, &h), Default::default())
        .unwrap();
    worker.tick(&mut writer, true).unwrap();
    assert!(worker.inflight.is_some());
    assert!(storage::Writer::start(db.clone(), Default::default()).is_err());
    drop(super::super::WorkerState {
        mining: Some(worker),
        writer,
    });
    // Slot credit is released only after the actual worker outcome is joined.
    assert_eq!(importer.available().0, 1);
    let owner = start(&db, &importer);
    let final_record = done(&owner.client().mining().unwrap(), receipt.id);
    assert!(matches!(
        final_record.state,
        journal::State::Done(journal::Outcome::Accepted(_))
    ));
    assert!(owner.shutdown().failure.is_none());
    stop(importer);
}
#[test]
fn rejection_is_durable_but_clock_and_importer_failures_keep_replayable_intents() {
    let t = Temp::new();
    let db = open(&t);
    let importer = importer(&db, 1);
    let owner = start(&db, &importer);
    let account = owner.client();
    let client = account.mining().unwrap();
    let h = first();
    let c = candidate(&h);
    assert!(publish(&client, c.clone()).published);
    let mut invalid = input(&c, &h);
    *invalid.solution.as_mut().unwrap().proof = [0; 42];
    let record = done(&client, accepted(&account, &invalid));
    assert!(matches!(
        record.state,
        journal::State::Done(journal::Outcome::Rejected {
            reason: journal::Rejection::Invalid,
            ..
        })
    ));
    assert_eq!(db.view().unwrap().status().tip, Some(c.parent()));
    assert!(owner.shutdown().failure.is_none());
    let mut config = inputs(&importer);
    config.clock = Arc::new(ClockValue(-1));
    let owner = Accounting::start_mining(
        db.clone(),
        Default::default(),
        Default::default(),
        Default::default(),
        config,
    )
    .unwrap();
    let account = owner.client();
    let client = account.mining().unwrap();
    assert!(publish(&client, c.clone()).published);
    let id = accepted(&account, &input(&c, &h));
    until(|| account.is_closed());
    let report = owner.shutdown();
    assert!(matches!(
        report.failure,
        Some(Error::Mining("clock outside u64"))
    ));
    assert!(report.admission.is_none());
    let record = client.get(id).unwrap().unwrap();
    assert_eq!(record.state, journal::State::Ready);
    assert_eq!(record.attempts, 0);
    importer.stop();
    let owner = start(&db, &importer);
    let account = owner.client();
    until(|| account.is_closed());
    let report = owner.shutdown();
    assert!(matches!(
        report.failure,
        Some(Error::Chain(chain::Error::Closed))
    ));
    assert_eq!(
        client.get(id).unwrap().unwrap().state,
        journal::State::Ready
    );
    stop(importer);
}
#[test]
fn admission_classification_preserves_unknown_operational_and_partial_results() {
    let h = first();
    let c = candidate(&h);
    let record = journal::Record {
        receipt: Receipt {
            id: Id {
                epoch: 1,
                sequence: 1,
            },
            input: input(&c, &h),
            share_key: u64::MAX,
        },
        epoch: 1,
        candidate: Some(c),
        accounted_key: Some(u64::MAX),
        attempts: 1,
        state: journal::State::Submitting,
    };
    for error in [
        chain::Error::Timeout,
        chain::Error::Cancelled,
        chain::Error::RecoveryRequired,
        chain::Error::WorkerPanicked,
        chain::Error::Validation {
            operational: true,
            reason: "temporary executor failure".into(),
        },
    ] {
        let failure = chain::Failure {
            progress: Some(Default::default()),
            error,
        };
        assert!(
            matches!(outcome(&record, Err(failure.clone())), Err(Error::Admission(e)) if e == failure)
        );
    }
    for error in [
        chain::Error::Stale,
        chain::Error::BlockedByWhitelist,
        chain::Error::Validation {
            operational: false,
            reason: "invalid seal".into(),
        },
    ] {
        let uncertain = chain::Failure {
            progress: None,
            error: error.clone(),
        };
        assert!(
            matches!(outcome(&record, Err(uncertain.clone())), Err(Error::Admission(e)) if e == uncertain)
        );
        assert!(matches!(
            outcome(
                &record,
                Err(chain::Failure {
                    progress: Some(Default::default()),
                    error
                })
            ),
            Ok(journal::Outcome::Rejected { .. })
        ));
    }
    assert!(outcome(&record, Ok(Default::default())).is_err());
}
#[test]
fn foreign_admitter_and_stale_control_requests_never_silently_change_the_owner() {
    let t = Temp::new();
    let db = open(&t);
    let u = Temp::new();
    let foreign = open(&u);
    let importer = importer(&foreign, 1);
    assert!(matches!(
        Accounting::start_mining(
            db.clone(),
            Default::default(),
            Default::default(),
            Default::default(),
            inputs(&importer)
        ),
        Err(Error::Configuration)
    ));
    assert_eq!(
        db.stratum(Default::default())
            .unwrap()
            .ingress(Default::default())
            .unwrap()
            .epoch(),
        0
    );
    stop(importer);
    let importer = self::importer(&db, 1);
    let owner = start(&db, &importer);
    let account = owner.client();
    let client = account.mining().unwrap();
    let mut h = first();
    h.common.previous_hash = [77; 32];
    assert!(!publish(&client, candidate(&h)).published);
    assert!(client.info().unwrap().active.is_empty());
    assert!(owner.shutdown().failure.is_none());
    stop(importer);
}
mod tcp;

mod recovery;
