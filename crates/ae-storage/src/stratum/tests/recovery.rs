use super::*;
fn seed(db: &Arc<Database>) {
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    commit(
        &mut w,
        vec![
            Command::Round,
            share(1),
            Command::Round,
            Command::Reward(reward()),
            Command::Payment(payment()),
        ],
    );
}
fn commands(op: &str) -> Vec<Command> {
    match op {
        "allocate" => vec![Command::Round, share(2)],
        "solution" => vec![Command::Solution {
            candidate: hash(1),
            block: hash(11),
        }],
        "prune" => vec![Command::DeleteRecords(71)],
        _ => panic!("invalid crash operation"),
    }
}
#[test]
fn crash_worker() {
    use std::io::{BufRead, Write};
    let Some(path) = std::env::var_os("AE_STRATUM_CRASH_PATH") else {
        return;
    };
    let op = std::env::var("AE_STRATUM_CRASH_OPERATION").unwrap();
    let db = Arc::new(Database::open(path, identity()).unwrap());
    let mut w = Writer::start(db, Default::default()).unwrap();
    println!("AE_STRATUM_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "commit\n");
    commit(&mut w, commands(&op));
    println!("AE_STRATUM_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn killed_writers_preserve_complete_batches_metadata_and_allocator() {
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
            if let Some(r) = self.reader.take() {
                r.join().unwrap();
            }
        }
    }
    for op in ["allocate", "solution", "prune"] {
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let temp = Temp::new();
            let db = database(&temp);
            seed(&db);
            let before = (rows(&db), db.stratum(Default::default()).unwrap().info());
            drop(db);
            let expected = {
                let probe = Temp::new();
                let db = database(&probe);
                seed(&db);
                let mut w = Writer::start(db.clone(), Default::default()).unwrap();
                commit(&mut w, commands(op));
                (rows(&db), db.stratum(Default::default()).unwrap().info())
            };
            let mut child = Process::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "stratum::tests::recovery::crash_worker",
                    "--nocapture",
                ])
                .env("AE_STRATUM_CRASH_PATH", &temp.0)
                .env("AE_STRATUM_CRASH_OPERATION", op)
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
                        .expect("bounded Stratum crash worker");
                    if line.contains(marker) {
                        break;
                    }
                }
            };
            wait("AE_STRATUM_READY");
            if phase != "before" {
                let input = child.child.stdin.as_mut().unwrap();
                input.write_all(b"commit\n").unwrap();
                input.flush().unwrap();
                if phase == "after" {
                    wait("AE_STRATUM_DURABLE");
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
            let actual = (rows(&db), db.stratum(Default::default()).unwrap().info());
            match phase {
                "before" => assert_eq!(actual, before),
                "after" => assert_eq!(actual, expected),
                _ => assert!(actual == before || actual == expected),
            }
            let mut w = Writer::start(db.clone(), Default::default()).unwrap();
            assert_eq!(
                w.recover().unwrap(),
                Recovery::Verified(w.version().unwrap())
            );
            assert_eq!(db.view().unwrap().status(), Default::default());
            let next = actual.1.next_key.unwrap();
            let result = commit(&mut w, vec![Command::Round]);
            assert_eq!(
                result.replies,
                [core::Reply::Record(Box::new(Record::Round(next)))]
            );
            eprintln!(
                "Stratum accounting crash recovery {op}/{phase}: revision {}",
                actual.1.revision
            );
        }
    }
}
