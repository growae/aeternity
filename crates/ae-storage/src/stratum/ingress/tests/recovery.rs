use super::*;
fn prepare(w: &mut Writer, op: &str) -> Option<Epoch> {
    (op != "epoch").then(|| w.begin_ingress(Default::default()).unwrap())
}
fn perform(w: &mut Writer, op: &str, epoch: Option<&Epoch>) {
    if op == "epoch" {
        w.begin_ingress(Default::default()).unwrap();
    } else {
        w.accept(
            epoch.unwrap(),
            1,
            input(op == "solution"),
            Default::default(),
        )
        .unwrap();
    }
}
#[test]
fn crash_worker() {
    use std::io::{BufRead, Write};
    let Some(path) = std::env::var_os("AE_STRATUM_INGRESS_CRASH_PATH") else {
        return;
    };
    let op = std::env::var("AE_STRATUM_INGRESS_CRASH_OPERATION").unwrap();
    let db = Arc::new(Database::open(path, identity()).unwrap());
    let mut w = Writer::start(db, Default::default()).unwrap();
    let epoch = prepare(&mut w, &op);
    println!("AE_STRATUM_INGRESS_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "commit\n");
    perform(&mut w, &op, epoch.as_ref());
    println!("AE_STRATUM_INGRESS_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn killed_ingress_preserves_epoch_receipt_solution_and_share_as_one_commit() {
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
    for op in ["epoch", "share", "solution"] {
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let temp = Temp::new();
            drop(open(&temp));
            let (before, expected) = {
                let probe = Temp::new();
                let db = open(&probe);
                let mut w = Writer::start(db.clone(), Default::default()).unwrap();
                let epoch = prepare(&mut w, op);
                let before = state(&db);
                perform(&mut w, op, epoch.as_ref());
                (before, state(&db))
            };
            let mut child = Process::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "stratum::ingress::tests::recovery::crash_worker",
                    "--nocapture",
                ])
                .env("AE_STRATUM_INGRESS_CRASH_PATH", &temp.0)
                .env("AE_STRATUM_INGRESS_CRASH_OPERATION", op)
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
            wait("AE_STRATUM_INGRESS_READY");
            if phase != "before" {
                let input = child.child.stdin.as_mut().unwrap();
                input.write_all(b"commit\n").unwrap();
                input.flush().unwrap();
                if phase == "after" {
                    wait("AE_STRATUM_INGRESS_DURABLE");
                } else {
                    std::thread::sleep(Duration::from_millis(
                        phase.strip_prefix("race").unwrap().parse().unwrap(),
                    ));
                }
            }
            child.child.kill().unwrap();
            child.child.wait().unwrap();
            drop(child);
            let db = open(&temp);
            let actual = state(&db);
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
            let next = actual.0.next_key.unwrap();
            let epoch = w.begin_ingress(Default::default()).unwrap();
            assert_eq!(epoch.value(), actual.2 + 1);
            assert_eq!(
                w.accept(&epoch, 1, input(false), Default::default())
                    .unwrap()
                    .share_key,
                next
            );
            eprintln!(
                "Stratum ingress crash recovery {op}/{phase}: revision {}",
                actual.0.revision
            );
        }
    }
}
