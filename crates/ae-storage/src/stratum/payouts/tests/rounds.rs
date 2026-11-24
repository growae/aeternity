use super::super::super::rounds as callbacks;
use super::*;
fn round_config() -> callbacks::Config {
    callbacks::Config::from_percentages(
        [(b"pool".to_vec(), Number::Float(100.0f64.to_bits()))].into(),
        2,
        1000,
        Default::default(),
    )
    .unwrap()
}
fn setup(db: &Arc<Database>) {
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    w.begin_rounds(round_config(), Default::default()).unwrap();
    w.begin_payouts(config(), Default::default()).unwrap();
    commit(&mut w, vec![Command::Payment(pay((100, 0)))]);
    let e = stored(db).epoch;
    let r = prepared(&mut w, e);
    signed(&mut w, e, &r);
    set_top(db, 281, Protocol::Roma, Some(GROSS + 3), 0);
    set_top(db, 282, Protocol::Ceres, Some(GROSS + 7), 99);
    w.capture_rounds(Default::default()).unwrap();
}
fn round_info(db: &Database) -> callbacks::Info {
    db.stratum(Default::default())
        .unwrap()
        .rounds(Default::default())
        .unwrap()
        .unwrap()
}
fn rows(db: &Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.db
        .iterator(rocksdb::IteratorMode::Start)
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .filter(|(k, _)| k.starts_with(b"\0stratum") || k.first() == Some(&RECORDS))
        .collect()
}
#[test]
fn queued_key_context_uses_event_height_current_balance_and_captured_protocol_atomically() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    w.begin_rounds(round_config(), Default::default()).unwrap();
    let e = w.begin_payouts(config(), Default::default()).unwrap().epoch;
    commit(&mut w, vec![Command::Payment(pay((100, 0)))]);
    let r = prepared(&mut w, e);
    let signed = signed(&mut w, e, &r);
    set_top(&db, 281, Protocol::Roma, Some(GROSS + 3), 0);
    set_top(&db, 282, Protocol::Ceres, Some(GROSS + 7), 99);
    w.capture_rounds(Default::default()).unwrap();
    let snap = db.stratum(Default::default()).unwrap();
    let r = w.step_rounds(1, Default::default()).unwrap();
    assert_eq!(r.height, 281);
    assert_eq!(r.outcome, callbacks::Outcome::Top);
    let i = stored(&db);
    assert_eq!(i.context.height, 281);
    assert_eq!(i.context.balance, Some((GROSS + 7).into()));
    assert_eq!(i.context.protocol, Protocol::Roma);
    assert_eq!(i.pending, Some(signed.clone()));
    assert_eq!(round_info(&db).height(), 281);
    assert_eq!(
        snap.payouts(Default::default())
            .unwrap()
            .unwrap()
            .context
            .height,
        0
    );
    assert_eq!(
        snap.rounds(Default::default()).unwrap().unwrap().height(),
        0
    );
    drop(snap);
    assert_eq!(w.step_rounds(1, Default::default()).unwrap(), r);
    w.step_rounds(2, Default::default()).unwrap();
    assert_eq!(stored(&db).context.height, 282);
    assert_eq!(stored(&db).pending, Some(signed));
    assert_eq!(round_info(&db).height(), 282);
}
#[test]
fn callback_write_failure_recovers_both_mailbox_and_payout_context_as_one_batch() {
    for after in [false, true] {
        let t = Temp::new();
        let db = database(&t);
        setup(&db);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        w.begin_payouts(config(), Default::default()).unwrap();
        let before = rows(&db);
        let result = w.step_rounds_with(1, Default::default(), |stage, db, b| {
            assert_eq!(stage, callbacks::Stage::Finish);
            if after {
                write(db, b)?;
            }
            Err(Error::Backend("combined callback write uncertainty".into()))
        });
        assert!(result.is_err());
        assert!(w.recovery_required());
        if !after {
            assert_eq!(rows(&db), before);
        } else {
            assert_ne!(rows(&db), before);
        }
        w.recover().unwrap();
        w.step_rounds(1, Default::default()).unwrap();
        let completed = rows(&db);
        w.step_rounds(1, Default::default()).unwrap();
        assert_eq!(rows(&db), completed);
        assert_eq!(stored(&db).context.height, 281);
        assert_eq!(round_info(&db).height(), 281);
    }
}
fn perform(
    w: &mut Writer,
    write: impl FnMut(callbacks::Stage, &Database, WriteBatch) -> Result<(), Error>,
) -> Result<callbacks::Receipt, Failure> {
    w.begin_payouts(config(), Default::default())?;
    w.step_rounds_with(1, Default::default(), write)
}
#[test]
fn crash_worker() {
    use std::io::{BufRead, Write};
    let Some(path) = std::env::var_os("AE_PAYOUT_CALLBACK_CRASH_PATH") else {
        return;
    };
    let db = Arc::new(Database::open(path, identity()).unwrap());
    let mut w = Writer::start(db, Default::default()).unwrap();
    perform(&mut w, |stage, db, b| {
        assert_eq!(stage, callbacks::Stage::Finish);
        println!("AE_PAYOUT_CALLBACK_READY");
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().lock().read_line(&mut line).unwrap();
        assert_eq!(line, "commit\n");
        write(db, b)?;
        println!("AE_PAYOUT_CALLBACK_DURABLE");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    })
    .unwrap();
    panic!("crash boundary missing");
}
#[test]
fn killed_key_callbacks_never_separate_mailbox_progress_from_payout_context() {
    use std::{
        io::{BufRead, Write},
        process::{Command as Process, Stdio},
        sync::mpsc,
        time::{Duration, Instant},
    };
    struct Child {
        child: std::process::Child,
        reader: Option<std::thread::JoinHandle<()>>,
    }
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            if let Some(r) = self.reader.take() {
                r.join().unwrap();
            }
        }
    }
    let (before, after) = {
        let t = Temp::new();
        let db = database(&t);
        setup(&db);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let mut states = None;
        assert!(
            perform(&mut w, |_, db, b| {
                let before = rows(db);
                write(db, b)?;
                states = Some((before, rows(db)));
                Err(Error::Backend("capture golden callback".into()))
            })
            .is_err()
        );
        states.unwrap()
    };
    assert_ne!(before, after);
    for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
        let t = Temp::new();
        let db = database(&t);
        setup(&db);
        drop(db);
        let mut child = Process::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stratum::payouts::tests::rounds::crash_worker",
                "--nocapture",
            ])
            .env("AE_PAYOUT_CALLBACK_CRASH_PATH", &t.0)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for l in std::io::BufReader::new(stdout).lines() {
                if tx.send(l.unwrap()).is_err() {
                    break;
                }
            }
        });
        let mut child = Child {
            child,
            reader: Some(reader),
        };
        let wait = |marker: &str| {
            let end = Instant::now() + Duration::from_secs(30);
            loop {
                let line = rx
                    .recv_timeout(end.saturating_duration_since(Instant::now()))
                    .expect("bounded combined callback worker");
                if line.contains(marker) {
                    break;
                }
            }
        };
        wait("AE_PAYOUT_CALLBACK_READY");
        if phase != "before" {
            let input = child.child.stdin.as_mut().unwrap();
            input.write_all(b"commit\n").unwrap();
            input.flush().unwrap();
            if phase == "after" {
                wait("AE_PAYOUT_CALLBACK_DURABLE");
            } else {
                std::thread::sleep(Duration::from_millis(
                    phase.strip_prefix("race").unwrap().parse().unwrap(),
                ));
            }
        }
        child.child.kill().unwrap();
        child.child.wait().unwrap();
        drop(child);
        let db = database(&t);
        let actual = rows(&db);
        match phase {
            "before" => assert_eq!(actual, before),
            "after" => assert_eq!(actual, after),
            _ => assert!(actual == before || actual == after),
        };
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        w.begin_payouts(config(), Default::default()).unwrap();
        w.step_rounds(1, Default::default()).unwrap();
        let done = rows(&db);
        w.step_rounds(1, Default::default()).unwrap();
        assert_eq!(rows(&db), done);
        eprintln!("Stratum payout callback crash recovery {phase}: atomic context and mailbox");
    }
}
#[test]
fn scheduled_confirmation_waits_for_both_processed_height_and_canonical_depth() {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, e) = super::setup(&db);
    reward(&mut w, true);
    let r = prepared(&mut w, e);
    let r = signed(&mut w, e, &r);
    include(&db, &r, 100);
    set_top(&db, 280, Protocol::Ceres, Some(GROSS + 1), 0);
    assert_eq!(
        w.confirm_scheduled_payout(e, &r, Default::default())
            .unwrap(),
        Confirmation::Pending
    );
    w.refresh_payout_context(e, Default::default()).unwrap();
    assert_eq!(stored(&db).context.height, 280);
    set_top(&db, 279, Protocol::Ceres, Some(GROSS + 1), 0);
    assert_eq!(
        w.confirm_scheduled_payout(e, &r, Default::default())
            .unwrap(),
        Confirmation::Pending
    );
    set_top(&db, 280, Protocol::Ceres, Some(GROSS + 1), 0);
    assert!(matches!(
        w.confirm_scheduled_payout(e, &r, Default::default())
            .unwrap(),
        Confirmation::Confirmed(_)
    ));
    assert!(stored(&db).pending.is_none());
}
