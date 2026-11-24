use super::*;
use std::io::{BufRead, Write};

fn ready() {
    println!("AE_RECEIPT_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "commit\n");
}
fn durable() -> ! {
    println!("AE_RECEIPT_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
fn setup_case(db: &Arc<Database>, mode: &str) -> Record {
    let (w, _, r) = pending(db);
    include(db, &r, 100);
    set_top(db, 280, Protocol::Roma, Some(GROSS + 1), 0);
    if mode == "confirm" {
        db.begin_gc(Default::default()).unwrap();
        sweep(db);
    }
    drop(w);
    r
}
#[test]
fn crash_worker() {
    let Some(path) = std::env::var_os("AE_RECEIPT_CRASH_PATH") else {
        return;
    };
    let mode = std::env::var("AE_RECEIPT_CRASH_MODE").unwrap();
    let db = Arc::new(Database::open(path, identity()).unwrap());
    if mode == "gc" {
        ready();
        db.begin_gc(Default::default()).unwrap();
        durable();
    }
    assert_eq!(mode, "confirm");
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let info = w.begin_payouts(config(), Default::default()).unwrap();
    w.confirm_payout_with(
        info.epoch,
        &info.pending.unwrap(),
        Default::default(),
        |stage, db, batch| {
            assert_eq!(stage, Stage::Confirm);
            ready();
            write(db, batch)?;
            durable();
        },
    )
    .unwrap();
    panic!("receipt crash boundary not reached");
}
#[test]
fn killed_capture_and_confirmation_recover_complete_receipt_and_accounting_batches() {
    use std::{
        process::{Command as Process, Stdio},
        sync::mpsc,
        time::{Duration, Instant},
    };
    struct Worker {
        child: std::process::Child,
        reader: Option<std::thread::JoinHandle<()>>,
    }
    impl Drop for Worker {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            if let Some(reader) = self.reader.take() {
                reader.join().unwrap();
            }
        }
    }
    for mode in ["gc", "confirm"] {
        let (before, after) = {
            let temp = Temp::new();
            let db = database(&temp);
            let r = setup_case(&db, mode);
            if mode == "gc" {
                let before = rows(&db);
                db.begin_gc(Default::default()).unwrap();
                (before, rows(&db))
            } else {
                let mut w = Writer::start(db.clone(), Default::default()).unwrap();
                let info = w.begin_payouts(config(), Default::default()).unwrap();
                let before = rows(&db);
                w.confirm_payout(info.epoch, &r, Default::default())
                    .unwrap();
                (before, rows(&db))
            }
        };
        assert_ne!(before, after);
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let temp = Temp::new();
            let db = database(&temp);
            let r = setup_case(&db, mode);
            drop(db);
            let mut child = Process::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "stratum::payouts::tests::retention::recovery::crash_worker",
                    "--nocapture",
                ])
                .env("AE_RECEIPT_CRASH_PATH", &temp.0)
                .env("AE_RECEIPT_CRASH_MODE", mode)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let (tx, rx) = mpsc::channel();
            let reader = std::thread::spawn(move || {
                for line in std::io::BufReader::new(stdout).lines() {
                    if tx.send(line.unwrap()).is_err() {
                        break;
                    }
                }
            });
            let mut child = Worker {
                child,
                reader: Some(reader),
            };
            let wait = |marker: &str| {
                let end = Instant::now() + Duration::from_secs(30);
                loop {
                    let line = rx
                        .recv_timeout(end.saturating_duration_since(Instant::now()))
                        .expect("bounded receipt crash worker");
                    if line.contains(marker) {
                        break;
                    }
                }
            };
            wait("AE_RECEIPT_READY");
            if phase != "before" {
                let stdin = child.child.stdin.as_mut().unwrap();
                stdin.write_all(b"commit\n").unwrap();
                stdin.flush().unwrap();
                if phase == "after" {
                    wait("AE_RECEIPT_DURABLE");
                } else {
                    std::thread::sleep(Duration::from_millis(
                        phase.strip_prefix("race").unwrap().parse().unwrap(),
                    ));
                }
            }
            child.child.kill().unwrap();
            child.child.wait().unwrap();
            drop(child);
            let db = database(&temp);
            let recovered = rows(&db);
            match phase {
                "before" => assert_eq!(recovered, before),
                "after" => assert_eq!(recovered, after),
                _ => assert!(recovered == before || recovered == after),
            }
            if mode == "gc" {
                if db.gc_state().unwrap().epoch == 0 {
                    db.begin_gc(Default::default()).unwrap();
                }
                sweep(&db);
            }
            let mut w = Writer::start(db.clone(), Default::default()).unwrap();
            let info = w.begin_payouts(config(), Default::default()).unwrap();
            assert!(matches!(
                w.confirm_payout(info.epoch, &r, Default::default())
                    .unwrap(),
                Confirmation::Confirmed(_)
            ));
            assert!(db.db.get(archive::KEY).unwrap().is_none());
            let done = rows(&db);
            assert!(matches!(
                w.confirm_payout(info.epoch, &r, Default::default())
                    .unwrap(),
                Confirmation::Confirmed(_)
            ));
            assert_eq!(rows(&db), done);
            println!(
                "Stratum receipt crash recovery {mode}/{phase}: complete proof/floor/accounting batch"
            );
        }
    }
}
