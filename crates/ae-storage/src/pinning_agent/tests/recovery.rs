use super::*;
use std::{
    io::{BufRead, Write},
    process::{Command, Stdio},
};
#[test]
fn crash_worker() {
    let Some(path) = std::env::var_os("AE_PINNING_CHECKPOINT_PATH") else {
        return;
    };
    let phase = std::env::var("AE_PINNING_CHECKPOINT_PHASE").unwrap();
    let db = Arc::new(Database::open(path, identity()).unwrap());
    let mut w = writer(db);
    println!("AE_CHECKPOINT_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "commit\n");
    transition(&mut w, &phase).unwrap();
    println!("AE_CHECKPOINT_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn killed_checkpoint_writers_recover_exact_event_progress_and_decision_prefix() {
    struct Worker(std::process::Child);
    impl Drop for Worker {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    fn marker(r: &mut impl BufRead, expected: &str) {
        loop {
            let mut line = String::new();
            assert_ne!(
                r.read_line(&mut line).unwrap(),
                0,
                "exited before {expected}"
            );
            if line.contains(expected) {
                break;
            }
        }
    }
    for operation in ["begin", "observe", "finish", "advance"] {
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let t = Temp::new();
            let (db, mut w) = setup(&t, operation);
            let before = w.checkpoint().unwrap().clone();
            transition(&mut w, operation).unwrap();
            let after = w.checkpoint().unwrap().clone();
            let mut batch = WriteBatch::default();
            batch.put(KEY, before.encode(Default::default()).unwrap());
            write(&db, batch).unwrap();
            drop(w);
            drop(db);
            let mut child = Worker(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "pinning_agent::tests::recovery::crash_worker",
                        "--nocapture",
                    ])
                    .env("AE_PINNING_CHECKPOINT_PATH", &t.0)
                    .env("AE_PINNING_CHECKPOINT_PHASE", operation)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .unwrap(),
            );
            let mut output = std::io::BufReader::new(child.0.stdout.take().unwrap());
            marker(&mut output, "AE_CHECKPOINT_READY");
            if phase != "before" {
                child
                    .0
                    .stdin
                    .as_mut()
                    .unwrap()
                    .write_all(b"commit\n")
                    .unwrap();
                child.0.stdin.as_mut().unwrap().flush().unwrap();
                if phase == "after" {
                    marker(&mut output, "AE_CHECKPOINT_DURABLE");
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(
                        phase.strip_prefix("race").unwrap().parse().unwrap(),
                    ));
                }
            }
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            let db = database(&t);
            let actual = db.pinning_agent(Default::default()).unwrap().unwrap();
            match phase {
                "before" => assert_eq!(actual, before),
                "after" => assert_eq!(actual, after),
                _ => assert!(actual == before || actual == after),
            };
            assert_eq!(*writer(db.clone()).recover().unwrap(), actual);
            assert_eq!(db.view().unwrap().status().revision, 1);
            eprintln!("Pinning checkpoint crash recovery {operation}/{phase}: complete prefix");
        }
    }
}
