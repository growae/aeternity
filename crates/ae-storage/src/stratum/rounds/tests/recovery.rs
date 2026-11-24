use super::*;
fn stage(name: &str) -> Stage {
    match name {
        "initialize" => Stage::Initialize,
        "capture" => Stage::Capture,
        "round" => Stage::Round,
        "finish" => Stage::Finish,
        _ => panic!("unknown crash stage"),
    }
}
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
    let Some(path) = std::env::var_os("AE_STRATUM_ROUNDS_CRASH_PATH") else {
        return;
    };
    let at = stage(&std::env::var("AE_STRATUM_ROUNDS_CRASH_OPERATION").unwrap());
    let db = Arc::new(Database::open(path, identity()).unwrap());
    let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
    perform_stage(&mut writer, &db, at, |next, db, batch| {
        if next != at {
            return db.db.write_opt(batch, &write_options()).map_err(backend);
        }
        println!("AE_STRATUM_ROUNDS_READY");
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().lock().read_line(&mut line).unwrap();
        assert_eq!(line, "commit\n");
        db.db.write_opt(batch, &write_options()).map_err(backend)?;
        println!("AE_STRATUM_ROUNDS_DURABLE");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    })
    .unwrap();
    panic!("target stage was not reached");
}
#[test]
fn killed_round_callbacks_preserve_atomic_metadata_allocation_and_payments() {
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
    for name in ["initialize", "capture", "round", "finish"] {
        let at = stage(name);
        let (before, after) = {
            let temp = Temp::new();
            let db = database(&temp);
            setup_stage(&db, at);
            let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
            let mut states = None;
            assert!(
                perform_stage(&mut writer, &db, at, |next, db, batch| {
                    if next == at {
                        let before = rows(db);
                        db.db.write_opt(batch, &write_options()).map_err(backend)?;
                        states = Some((before, rows(db)));
                        Err(Error::Backend("stop at golden boundary".into()))
                    } else {
                        db.db.write_opt(batch, &write_options()).map_err(backend)
                    }
                })
                .is_err()
            );
            states.unwrap()
        };
        assert_ne!(before, after);
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let temp = Temp::new();
            let db = database(&temp);
            setup_stage(&db, at);
            drop(db);
            let mut child = Process::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "stratum::rounds::tests::recovery::crash_worker",
                    "--nocapture",
                ])
                .env("AE_STRATUM_ROUNDS_CRASH_PATH", &temp.0)
                .env("AE_STRATUM_ROUNDS_CRASH_OPERATION", name)
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
                        .expect("bounded rounds crash worker");
                    if line.contains(marker) {
                        break;
                    }
                }
            };
            wait("AE_STRATUM_ROUNDS_READY");
            if phase != "before" {
                let input = child.child.stdin.as_mut().unwrap();
                input.write_all(b"commit\n").unwrap();
                input.flush().unwrap();
                if phase == "after" {
                    wait("AE_STRATUM_ROUNDS_DURABLE");
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
            finish_stage(&mut writer, &db, at);
            let completed = rows(&db);
            finish_stage(&mut writer, &db, at);
            assert_eq!(rows(&db), completed);
            eprintln!(
                "Stratum rounds crash recovery {name}/{phase}: exact batch and idempotent replay"
            );
        }
    }
}
