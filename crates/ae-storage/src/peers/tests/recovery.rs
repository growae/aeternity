use super::*;

#[test]
fn crash_worker() {
    use std::io::{BufRead, Write};
    let Some(path) = std::env::var_os("AE_PEER_STORE_CRASH_PATH") else {
        return;
    };
    let phase = std::env::var("AE_PEER_STORE_CRASH_OPERATION").unwrap();
    let db = Arc::new(Database::open(path, identity()).unwrap());
    let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
    let version = writer.version().unwrap();
    println!("AE_PEERS_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "commit\n");
    let changes = if phase == "replace" {
        vec![
            Change::Delete(peer(1).id),
            Change::Put(peer(2)),
            Change::Put(peer(3)),
        ]
    } else {
        vec![Change::Delete(peer(1).id)]
    };
    writer.commit(version, &changes).unwrap();
    println!("AE_PEERS_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn killed_peer_writers_recover_only_complete_records_and_metadata() {
    use std::io::{BufRead, Write};
    use std::process::{Command, Stdio};
    struct Worker(std::process::Child);
    impl Drop for Worker {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for operation in ["replace", "delete"] {
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let temp = Temp::new();
            let db = database(&temp);
            let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
            writer
                .commit(writer.version().unwrap(), &[Change::Put(peer(1))])
                .unwrap();
            drop(writer);
            drop(db);
            let mut child = Worker(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "peers::tests::recovery::crash_worker",
                        "--nocapture",
                    ])
                    .env("AE_PEER_STORE_CRASH_PATH", &temp.0)
                    .env("AE_PEER_STORE_CRASH_OPERATION", operation)
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
            wait(&mut output, "AE_PEERS_READY");
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
                    wait(&mut output, "AE_PEERS_DURABLE");
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
            let expected = if operation == "replace" {
                vec![peer(2), peer(3)]
            } else {
                vec![]
            };
            let old = vec![peer(1)];
            match phase {
                "before" => assert_eq!(actual, old),
                "after" => assert_eq!(actual, expected),
                _ => assert!(actual == old || actual == expected),
            };
            assert_eq!(
                db.peer_view().unwrap().version().revision(),
                if actual == old { 1 } else { 2 }
            );
            let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
            assert_eq!(writer.recover().unwrap().1, actual);
            assert_eq!(db.view().unwrap().status(), Status::default());
            eprintln!(
                "Peer store crash recovery {operation}/{phase}: {} records",
                actual.len()
            );
        }
    }
}
