use super::*;
use crate::test_support::ledger_fixtures as fixtures;
use ae_codec::headers::Header;
use ae_protocol::NetworkProfile;
use ae_state::{
    Trees,
    genesis::{self, Network},
};
use std::sync::{Mutex, atomic::AtomicU64};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temporary(std::path::PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn setup(config: Config) -> (Temporary, Importer) {
    let temp = Temporary(std::env::temp_dir().join(format!(
        "ae-import-owner-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    let network = NetworkProfile::mainnet();
    let Header::Key(header) = genesis::header(Network::Mainnet).unwrap() else {
        panic!()
    };
    let id = repository::Identity::new(
        &network,
        Block::Key(header.clone()).hash().unwrap(),
        ae_crypto::blake2b_256(b"import-owner-fault-tests"),
    );
    let db = Arc::new(Database::open(&temp.0, id.clone()).unwrap());
    let trees = Trees {
        accounts: genesis::state(Network::Mainnet).unwrap().0,
        contracts: Default::default(),
        channels: Default::default(),
        calls: Default::default(),
        oracles: Default::default(),
        names: Default::default(),
    };
    db.commit(
        ledger::bootstrap(
            &db.view().unwrap(),
            &network,
            &header,
            &trees,
            Default::default(),
        )
        .unwrap(),
    )
    .unwrap();
    let importer = Importer::new(
        db,
        fixtures::execution(&network),
        fixtures::rules(id.genesis),
        config,
    )
    .unwrap();
    (temp, importer)
}
fn packet(key: &Block, micros: &[Block], dir: Direction) -> Vec<u8> {
    Message::new(
        Kind::Generation,
        1,
        BTreeMap::from([
            ("key_block".into(), Value::Binary(key.encode().unwrap())),
            (
                "micro_blocks".into(),
                Value::List(
                    micros
                        .iter()
                        .map(|b| Value::Binary(b.encode().unwrap()))
                        .collect(),
                ),
            ),
            ("forward".into(), Value::Boolean(dir == Direction::Forward)),
        ]),
        Default::default(),
    )
    .unwrap()
    .encode_packet(Default::default())
    .unwrap()
}
const NOW: u64 = u64::MAX - 600_000;
async fn within<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), f)
        .await
        .expect("bounded test")
}

#[tokio::test]
async fn concurrent_policy_conflict_reprepares_against_new_view_before_committing() {
    let (_temp, mut importer) = setup(Default::default());
    let calls = Arc::new(AtomicU64::new(0));
    let count = calls.clone();
    Arc::get_mut(&mut importer.context).unwrap().commit_hook = Some(Arc::new(move |db, update| {
        if count.fetch_add(1, Ordering::SeqCst) == 0 {
            db.update_resilience(
                ae_chain::resilience::Event::Start,
                ae_chain::resilience::Config {
                    depth: 1,
                    from_start: true,
                },
            )?;
        }
        db.commit(update)
    }));
    let all = fixtures::blocks(&NetworkProfile::mainnet());
    let hash = all[0].hash().unwrap();
    let report = importer
        .import(
            packet(&all[0], &[], Direction::Backward),
            hash,
            Direction::Backward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(report.accepted.len(), 1);
    assert_eq!(importer.context.db.view().unwrap().status().tip, Some(hash));
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn uncertain_commit_before_or_after_durable_write_quarantines_the_owner() {
    for published in [false, true] {
        let (_temp, mut importer) = setup(Default::default());
        Arc::get_mut(&mut importer.context).unwrap().commit_hook =
            Some(Arc::new(move |db, update| {
                if published {
                    db.commit(update)?;
                }
                Err(repository::Error::Backend(
                    "injected uncertain acknowledgment".into(),
                ))
            }));
        let all = fixtures::blocks(&NetworkProfile::mainnet());
        let hash = all[0].hash().unwrap();
        let p = packet(&all[0], &[], Direction::Backward);
        let failure = importer
            .import(p.clone(), hash, Direction::Backward, NOW)
            .unwrap()
            .wait()
            .await
            .unwrap_err();
        assert!(matches!(failure.error,Error::CommitUncertain {hash:h,..} if h==hash));
        assert!(failure.progress.unwrap().accepted.is_empty());
        assert!(importer.recovery_required());
        assert_eq!(
            importer
                .context
                .db
                .view()
                .unwrap()
                .block(hash)
                .unwrap()
                .is_some(),
            published
        );
        assert!(matches!(
            importer.import(p, hash, Direction::Backward, NOW),
            Err(Error::RecoveryRequired)
        ));
        importer.stop();
        importer.join().await;
    }
}

#[tokio::test]
async fn panic_after_commit_has_unknown_progress_and_requires_recovery() {
    let (_temp, mut importer) = setup(Default::default());
    Arc::get_mut(&mut importer.context).unwrap().commit_hook = Some(Arc::new(|db, update| {
        db.commit(update)?;
        panic!("injected post-commit panic");
    }));
    let all = fixtures::blocks(&NetworkProfile::mainnet());
    let hash = all[0].hash().unwrap();
    let failure = importer
        .import(
            packet(&all[0], &[], Direction::Backward),
            hash,
            Direction::Backward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap_err();
    assert_eq!(failure.error, Error::WorkerPanicked);
    assert!(failure.progress.is_none());
    assert!(importer.recovery_required());
    assert!(
        importer
            .context
            .db
            .view()
            .unwrap()
            .block(hash)
            .unwrap()
            .is_some()
    );
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn timeout_after_commit_reports_unknown_outcome_and_joins_running_work() {
    let (_temp, mut importer) = setup(Config {
        timeout: Duration::from_millis(100),
        ..Default::default()
    });
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, gate) = std::sync::mpsc::channel();
    let gate = Mutex::new(gate);
    Arc::get_mut(&mut importer.context).unwrap().commit_hook = Some(Arc::new(move |db, update| {
        let committed = db.commit(update)?;
        entered.lock().unwrap().take().unwrap().send(()).unwrap();
        gate.lock().unwrap().recv().unwrap();
        Ok(committed)
    }));
    let all = fixtures::blocks(&NetworkProfile::mainnet());
    let hash = all[0].hash().unwrap();
    let ticket = importer
        .import(
            packet(&all[0], &[], Direction::Backward),
            hash,
            Direction::Backward,
            NOW,
        )
        .unwrap();
    within(entry).await.unwrap();
    let failure = ticket.wait().await.unwrap_err();
    assert_eq!(failure.error, Error::Timeout);
    assert!(failure.progress.is_none());
    assert!(
        importer
            .context
            .db
            .view()
            .unwrap()
            .block(hash)
            .unwrap()
            .is_some()
    );
    release.send(()).unwrap();
    importer.stop();
    within(importer.join()).await;
    assert!(!importer.recovery_required());
}

#[tokio::test]
async fn cancelled_queued_generation_never_commits_and_unread_result_keeps_credits() {
    let mut config = Config {
        jobs: 2,
        ..Default::default()
    };
    config.bytes = 2 * (config.messages.body_bytes + config.messages.work_bytes);
    let total = config.bytes;
    let (_temp, mut importer) = setup(config);
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, gate) = std::sync::mpsc::channel();
    let gate = Mutex::new(gate);
    Arc::get_mut(&mut importer.context).unwrap().commit_hook = Some(Arc::new(move |db, update| {
        if let Some(entered) = entered.lock().unwrap().take() {
            entered.send(()).unwrap();
            gate.lock().unwrap().recv().unwrap();
        }
        db.commit(update)
    }));
    let all = fixtures::blocks(&NetworkProfile::mainnet());
    let hash = all[0].hash().unwrap();
    let first = importer
        .import(
            packet(&all[0], &[], Direction::Backward),
            hash,
            Direction::Backward,
            NOW,
        )
        .unwrap();
    within(entry).await.unwrap();
    let queued = importer
        .import(
            packet(&all[0], &all[1..2], Direction::Forward),
            hash,
            Direction::Forward,
            NOW,
        )
        .unwrap();
    drop(queued);
    assert_eq!(importer.available(), (0, 0));
    release.send(()).unwrap();
    within(async {
        while importer.available().0 == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    // Only the cancelled job released its reservation; the completed first
    // result is deliberately unread and still owns one full reservation.
    assert_eq!(importer.available(), (1, total / 2));
    assert_eq!(first.wait().await.unwrap().accepted.len(), 1);
    assert!(
        importer
            .context
            .db
            .view()
            .unwrap()
            .block(all[1].hash().unwrap())
            .unwrap()
            .is_none()
    );
    importer.stop();
    importer.join().await;
    assert_eq!(importer.available(), (2, total));
}

#[tokio::test]
async fn shutdown_preserves_committed_micro_prefix_and_stops_the_next_block() {
    let (_temp, mut importer) = setup(Default::default());
    let all = fixtures::blocks(&NetworkProfile::mainnet());
    let hash = all[0].hash().unwrap();
    importer
        .import(
            packet(&all[0], &[], Direction::Backward),
            hash,
            Direction::Backward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, gate) = std::sync::mpsc::channel();
    let gate = Mutex::new(gate);
    Arc::get_mut(&mut importer.context).unwrap().commit_hook = Some(Arc::new(move |db, update| {
        let committed = db.commit(update)?;
        entered.lock().unwrap().take().unwrap().send(()).unwrap();
        gate.lock().unwrap().recv().unwrap();
        Ok(committed)
    }));
    let ticket = importer
        .import(
            packet(
                &all[0],
                &[all[1].clone(), all[1].clone()],
                Direction::Forward,
            ),
            hash,
            Direction::Forward,
            NOW,
        )
        .unwrap();
    within(entry).await.unwrap();
    importer.stop();
    release.send(()).unwrap();
    let failure = ticket.wait().await.unwrap_err();
    assert_eq!(failure.error, Error::Cancelled);
    let progress = failure.progress.unwrap();
    assert_eq!(progress.accepted.len(), 1);
    assert_eq!(progress.accepted[0].hash, all[1].hash().unwrap());
    assert_eq!(
        importer.context.db.view().unwrap().status().tip,
        Some(all[1].hash().unwrap())
    );
    within(importer.join()).await;
}
