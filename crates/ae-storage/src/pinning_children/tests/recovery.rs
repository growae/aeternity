use super::*;

#[test]
fn crash_worker() {
    use std::io::{BufRead, Write};
    let Some(path) = std::env::var_os("AE_CHILD_PIN_CRASH_PATH") else {
        return;
    };
    let phase = std::env::var("AE_CHILD_PIN_CRASH_OPERATION").unwrap();
    let db = Arc::new(Database::open(path, identity()).unwrap());
    let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
    println!("AE_CHILD_PIN_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "commit\n");
    if phase == "reserve" {
        writer.reserve(record(1)).unwrap();
    } else {
        writer.record_receipt(&record(1), receipt()).unwrap();
    }
    println!("AE_CHILD_PIN_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn killed_child_pin_writers_recover_complete_intents_and_receipts() {
    use std::io::{BufRead, Write};
    use std::process::{Command, Stdio};
    struct Worker(std::process::Child);
    impl Drop for Worker {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for operation in ["reserve", "receipt"] {
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let temp = Temp::new();
            let db = database(&temp);
            let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
            if operation == "receipt" {
                writer.reserve(record(1)).unwrap();
            }
            drop(writer);
            drop(db);
            let mut child = Worker(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "pinning_children::tests::recovery::crash_worker",
                        "--nocapture",
                    ])
                    .env("AE_CHILD_PIN_CRASH_PATH", &temp.0)
                    .env("AE_CHILD_PIN_CRASH_OPERATION", operation)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .unwrap(),
            );
            let mut output = std::io::BufReader::new(child.0.stdout.take().unwrap());
            let wait = |out: &mut std::io::BufReader<std::process::ChildStdout>, marker: &str| loop {
                let mut line = String::new();
                assert_ne!(
                    out.read_line(&mut line).unwrap(),
                    0,
                    "exited before {marker}"
                );
                if line.contains(marker) {
                    break;
                }
            };
            wait(&mut output, "AE_CHILD_PIN_READY");
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
                    wait(&mut output, "AE_CHILD_PIN_DURABLE");
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(
                        phase.strip_prefix("race").unwrap().parse().unwrap(),
                    ));
                }
            }
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            let db = database(&temp);
            let actual = rows(&db);
            let old = if operation == "reserve" {
                vec![]
            } else {
                vec![record(1)]
            };
            let mut expected = record(1);
            if operation == "receipt" {
                expected.receipt = Some(receipt());
            }
            let expected = vec![expected];
            match phase {
                "before" => assert_eq!(actual, old),
                "after" => assert_eq!(actual, expected),
                _ => assert!(actual == old || actual == expected),
            };
            assert_eq!(
                db.pinning_children().unwrap().revision(),
                u64::from(operation == "receipt") + u64::from(actual != old)
            );
            let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
            assert_eq!(writer.recover().unwrap(), actual);
            assert_eq!(db.view().unwrap().status(), Status::default());
            eprintln!(
                "Child pin crash recovery {operation}/{phase}: {} records",
                actual.len()
            );
        }
    }
}
