use super::batches::{payments, prepare, setup_large};
use super::*;
fn rows(db: &Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.db
        .iterator(rocksdb::IteratorMode::Start)
        .map(|row| {
            let (key, value) = row.unwrap();
            (key.to_vec(), value.to_vec())
        })
        .filter(|(key, _)| key.starts_with(b"\0stratum") || key.first() == Some(&RECORDS))
        .collect()
}
#[test]
fn crash_worker() {
    use std::io::{BufRead, Write};
    let Some(path) = std::env::var_os("AE_STRATUM_BATCHES_CRASH_PATH") else {
        return;
    };
    let db = Arc::new(Database::open(path, identity()).unwrap());
    let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
    let epoch = writer
        .begin_payouts(config(), Default::default())
        .unwrap()
        .epoch;
    writer
        .prepare_generated_with(
            epoch,
            0.into(),
            payout::Generation::Bounded(Default::default()),
            Default::default(),
            |_, db, batch| {
                println!("AE_STRATUM_BATCHES_READY");
                std::io::stdout().flush().unwrap();
                let mut line = String::new();
                std::io::stdin().lock().read_line(&mut line).unwrap();
                assert_eq!(line, "commit\n");
                db.db.write_opt(batch, &write_options()).map_err(backend)?;
                println!("AE_STRATUM_BATCHES_DURABLE");
                std::io::stdout().flush().unwrap();
                loop {
                    std::thread::park();
                }
            },
        )
        .unwrap();
    panic!("target stage was not reached");
}
#[test]
fn killed_split_preparation_recovers_all_payments_and_exact_first_request_atomically() {
    use std::{
        io::{BufRead, Write},
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
    {
        let (before, after) = {
            let temp = Temp::new();
            let db = database(&temp);
            drop(setup_large(&db));
            let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
            let epoch = writer
                .begin_payouts(config(), Default::default())
                .unwrap()
                .epoch;
            let mut states = None;
            assert!(
                writer
                    .prepare_generated_with(
                        epoch,
                        0.into(),
                        payout::Generation::Bounded(Default::default()),
                        Default::default(),
                        |_, db, batch| {
                            let before = rows(db);
                            write(db, batch)?;
                            states = Some((before, rows(db)));
                            Err(Error::Backend("stop at split golden boundary".into()))
                        }
                    )
                    .is_err()
            );
            states.unwrap()
        };
        assert_ne!(before, after);
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let temp = Temp::new();
            let db = database(&temp);
            drop(setup_large(&db));
            drop(db);
            let mut child = Process::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "stratum::payouts::tests::batches_recovery::crash_worker",
                    "--nocapture",
                ])
                .env("AE_STRATUM_BATCHES_CRASH_PATH", &temp.0)
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
                        .expect("bounded payouts crash worker");
                    if line.contains(marker) {
                        break;
                    }
                }
            };
            wait("AE_STRATUM_BATCHES_READY");
            if phase != "before" {
                let input = child.child.stdin.as_mut().unwrap();
                input.write_all(b"commit\n").unwrap();
                input.flush().unwrap();
                if phase == "after" {
                    wait("AE_STRATUM_BATCHES_DURABLE");
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
            let actual = rows(&db);
            match phase {
                "before" => assert_eq!(actual, before),
                "after" => assert_eq!(actual, after),
                _ => assert!(actual == before || actual == after),
            }
            let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
            assert_eq!(
                writer.recover().unwrap(),
                Recovery::Verified(writer.version().unwrap())
            );
            let epoch = writer
                .begin_payouts(config(), Default::default())
                .unwrap()
                .epoch;
            let r = prepare(&mut writer, epoch);
            assert_eq!(r.sequence, 1);
            assert_eq!(payments(&db).len(), 2);
            let r = signed(&mut writer, epoch, &r);
            let complete = rows(&db);
            assert_eq!(prepare(&mut writer, epoch), r);
            assert_eq!(rows(&db), complete);
            eprintln!(
                "Stratum split crash recovery {phase}: exact original or complete split plus intent"
            );
        }
    }
}
