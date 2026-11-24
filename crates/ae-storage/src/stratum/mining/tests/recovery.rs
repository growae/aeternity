use super::*;
fn prepare(w: &mut Writer, op: &str) -> (Option<Epoch>, Option<ingress::Epoch>, Option<Record>) {
    if op == "epoch" {
        return (None, None, None);
    }
    let e = w.begin_mining(Default::default()).unwrap();
    let i = w.begin_ingress(Default::default()).unwrap();
    if op == "publish" {
        return (Some(e), Some(i), None);
    }
    w.publish_work(&e, 1, candidate(), date(), Default::default())
        .unwrap();
    if op == "accept" {
        return (Some(e), Some(i), None);
    }
    let receipt = if op == "legacy" {
        w.accept(&i, 1, input(), Default::default()).unwrap()
    } else {
        accept(w, &e, &i, 1)
    };
    if op == "legacy" {
        return (Some(e), Some(i), None);
    }
    let mut r = record(&w.db, receipt.id);
    if op != "submit" {
        r = w.begin_submission(&r, Default::default()).unwrap();
    }
    if op == "ack" {
        r = w
            .finish_submission(&r, reject(), Default::default())
            .unwrap();
    }
    (Some(e), Some(i), Some(r))
}
fn perform(
    w: &mut Writer,
    op: &str,
    e: Option<&Epoch>,
    i: Option<&ingress::Epoch>,
    r: Option<&Record>,
) {
    match op {
        "epoch" => {
            w.begin_mining(Default::default()).unwrap();
        }
        "publish" => {
            w.publish_work(e.unwrap(), 1, candidate(), date(), Default::default())
                .unwrap();
        }
        "accept" => {
            accept(w, e.unwrap(), i.unwrap(), 1);
        }
        "legacy" => {
            w.prepare_solution(
                e.unwrap(),
                Id {
                    epoch: i.unwrap().value(),
                    sequence: 1,
                },
                Default::default(),
            )
            .unwrap();
        }
        "submit" => {
            w.begin_submission(r.unwrap(), Default::default()).unwrap();
        }
        "finish" => {
            w.finish_submission(r.unwrap(), reject(), Default::default())
                .unwrap();
        }
        "reconcile" => {
            w.reconcile_settled_submission(r.unwrap(), Default::default())
                .unwrap();
        }
        "ack" => {
            w.acknowledge_solution(r.unwrap(), Default::default())
                .unwrap();
        }
        _ => unreachable!(),
    }
}
#[test]
fn crash_worker() {
    use std::io::{BufRead, Write};
    let Some(path) = std::env::var_os("AE_STRATUM_JOURNAL_CRASH_PATH") else {
        return;
    };
    let op = std::env::var("AE_STRATUM_JOURNAL_CRASH_OPERATION").unwrap();
    let db = Arc::new(Database::open(path, identity()).unwrap());
    let mut w = writer(&db);
    let (e, i, r) = prepare(&mut w, &op);
    println!("AE_STRATUM_JOURNAL_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "commit\n");
    perform(&mut w, &op, e.as_ref(), i.as_ref(), r.as_ref());
    println!("AE_STRATUM_JOURNAL_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn killed_journals_preserve_work_accounting_claim_attempt_and_closeout_atomically() {
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
    for op in [
        "epoch",
        "publish",
        "accept",
        "legacy",
        "submit",
        "finish",
        "reconcile",
        "ack",
    ] {
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let t = Temp::new();
            let db = open(&t);
            let chain = db.view().unwrap().status();
            drop(db);
            let (before, after) = {
                let probe = Temp::new();
                let db = open(&probe);
                let mut w = writer(&db);
                let (e, i, r) = prepare(&mut w, op);
                let before = rows(&db);
                perform(&mut w, op, e.as_ref(), i.as_ref(), r.as_ref());
                (before, rows(&db))
            };
            let mut child = Process::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "stratum::mining::tests::recovery::crash_worker",
                    "--nocapture",
                ])
                .env("AE_STRATUM_JOURNAL_CRASH_PATH", &t.0)
                .env("AE_STRATUM_JOURNAL_CRASH_OPERATION", op)
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
                        .expect("bounded journal crash worker");
                    if line.contains(marker) {
                        break;
                    }
                }
            };
            wait("AE_STRATUM_JOURNAL_READY");
            if phase != "before" {
                let stdin = child.child.stdin.as_mut().unwrap();
                stdin.write_all(b"commit\n").unwrap();
                stdin.flush().unwrap();
                if phase == "after" {
                    wait("AE_STRATUM_JOURNAL_DURABLE");
                } else {
                    std::thread::sleep(Duration::from_millis(
                        phase.strip_prefix("race").unwrap().parse().unwrap(),
                    ));
                }
            }
            child.child.kill().unwrap();
            child.child.wait().unwrap();
            drop(child);
            let db = open(&t);
            let actual = rows(&db);
            match phase {
                "before" => assert_eq!(actual, before),
                "after" => assert_eq!(actual, after),
                _ => assert!(actual == before || actual == after, "{op}/{phase}"),
            };
            assert_eq!(db.view().unwrap().status(), chain);
            let mut w = writer(&db);
            assert!(matches!(w.recover().unwrap(), Recovery::Verified(_)));
            db.stratum(Default::default()).unwrap().verify().unwrap();
            eprintln!(
                "Stratum journal crash recovery {op}/{phase}: revision {}",
                db.stratum(Default::default()).unwrap().info().revision
            );
        }
    }
}
