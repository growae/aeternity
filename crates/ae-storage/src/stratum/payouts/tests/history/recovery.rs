use super::*;
use std::io::{BufRead, Write};
fn setup_legacy(db: &Arc<Database>) {
    let (mut w, e) = setup(db);
    let r = prepared(&mut w, e);
    signed(&mut w, e, &r);
    drop(w);
    strip(db);
    old_marker(db);
}
#[test]
fn migration_crash_worker() {
    let Some(path) = std::env::var_os("AE_HISTORY_MIGRATION_CRASH_PATH") else {
        return;
    };
    println!("AE_HISTORY_READY");
    std::io::stdout().flush().unwrap();
    let mut command = String::new();
    std::io::stdin().lock().read_line(&mut command).unwrap();
    assert_eq!(command, "commit\n");
    let db = Database::open(path, identity()).unwrap();
    assert_eq!(history(&db).entries.len(), 1);
    println!("AE_HISTORY_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn killed_migration_publishes_the_schema_and_partial_baseline_as_one_batch() {
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
            if let Some(r) = self.reader.take() {
                r.join().unwrap();
            }
        }
    }
    let (before, after) = {
        let t = Temp::new();
        let db = database(&t);
        setup_legacy(&db);
        let before = rows(&db);
        drop(db);
        let db = database(&t);
        (before, rows(&db))
    };
    assert_ne!(before, after);
    for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
        let t = Temp::new();
        let db = database(&t);
        setup_legacy(&db);
        assert_eq!(rows(&db), before);
        drop(db);
        let mut child = Process::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stratum::payouts::tests::history::recovery::migration_crash_worker",
                "--nocapture",
            ])
            .env("AE_HISTORY_MIGRATION_CRASH_PATH", &t.0)
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
                    .expect("bounded history migration worker");
                if line.contains(marker) {
                    break;
                }
            }
        };
        wait("AE_HISTORY_READY");
        if phase != "before" {
            let input = child.child.stdin.as_mut().unwrap();
            input.write_all(b"commit\n").unwrap();
            input.flush().unwrap();
            if phase == "after" {
                wait("AE_HISTORY_DURABLE");
            } else {
                std::thread::sleep(Duration::from_millis(
                    phase.strip_prefix("race").unwrap().parse().unwrap(),
                ));
            }
        }
        child.child.kill().unwrap();
        child.child.wait().unwrap();
        drop(child);
        // Inspect before ordinary opening can finish an interrupted migration.
        let raw = rocksdb::DB::open_default(&t.0).unwrap();
        let recovered: Vec<_> = raw
            .iterator(rocksdb::IteratorMode::Start)
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.to_vec(), v.to_vec())
            })
            .collect();
        drop(raw);
        match phase {
            "before" => assert_eq!(recovered, before),
            "after" => assert_eq!(recovered, after),
            _ => assert!(recovered == before || recovered == after),
        };
        let db = database(&t);
        assert_eq!(rows(&db), after);
        let page = history(&db);
        assert!(page.summary.unwrap().partial);
        assert_eq!(page.entries.len(), 1);
        drop(db);
        let db = database(&t);
        assert_eq!(rows(&db), after);
        println!(
            "Stratum history migration crash recovery {phase}: complete schema and partial baseline"
        );
    }
}
