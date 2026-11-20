use super::*;
use std::io::{BufRead, Write};

struct GateStore(Writer);
fn marker(s: &str) {
    println!("{s}");
    std::io::stdout().flush().unwrap();
}
fn release() {
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "continue\n");
}
impl Store for GateStore {
    fn recover(&mut self) -> Result<(Version, Vec<Record>), peers::Failure> {
        self.0.recover()
    }
    fn commit(&mut self, v: Version, w: &[peers::Change]) -> Result<Version, peers::Failure> {
        assert!(!w.is_empty());
        marker("AE_REGISTRY_PREPARED");
        release();
        let version = self.0.commit(v, w)?;
        marker("AE_REGISTRY_DURABLE");
        release();
        Ok(version)
    }
    fn uncertain(&self) -> bool {
        self.0.recovery_required()
    }
}
fn replacement() -> discovery::Peer {
    let mut p = peer(1, false);
    p.address = Ipv4Addr::new(99, 88, 77, 66);
    p.source = p.address;
    p.port = 5555;
    p
}

#[test]
fn crash_worker() {
    let Some(path) = std::env::var_os("AE_REGISTRY_CRASH_PATH") else {
        return;
    };
    let operation = std::env::var("AE_REGISTRY_CRASH_OPERATION").unwrap();
    let database = Arc::new(Database::open(path, identity()).unwrap());
    let cfg = config();
    let mut registry =
        Registry::with_seed(key(128), cfg.policy.clone(), &[1, 2, 3, 4], [7; 32]).unwrap();
    for c in [Command::Add(peer(1, false)), Command::Connect(Some(key(1)))] {
        let p = registry.prepare(0, c).unwrap();
        registry.commit(p).unwrap();
    }
    let command = if operation == "block" {
        Command::Block(policy::Info::from(&peer(1, false)))
    } else {
        let p = replacement();
        let prepared = registry
            .prepare(0, Command::ReserveInbound { address: p.address })
            .unwrap();
        let token = match registry.commit(prepared).unwrap().result {
            Outcome::Reserved(t) => t,
            _ => panic!(),
        };
        Command::Accept {
            token,
            peer: policy::Info::from(&p),
        }
    };
    let store = GateStore(Writer::start(database, cfg.storage).unwrap());
    let mut core = Core::start(registry, store, &cfg).unwrap();
    core.apply(1, command).unwrap();
    assert!(
        core.queue
            .iter()
            .any(|d| matches!(d.effect, Effect::Close(_)))
    );
    marker("AE_REGISTRY_PUBLISHED");
    loop {
        std::thread::park();
    }
}

#[test]
fn killed_owners_recover_complete_peer_records_across_storage_and_policy_publication() {
    use std::process::{Command as Process, Stdio};
    struct Worker(std::process::Child);
    impl Drop for Worker {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for operation in ["block", "replace"] {
        for phase in [
            "before",
            "durable",
            "published",
            "race0",
            "race1",
            "race5",
            "race10",
        ] {
            let temp = Temp::new();
            let database = db(&temp);
            let mut writer = Writer::start(database.clone(), Default::default()).unwrap();
            writer
                .commit(
                    writer.version().unwrap(),
                    &[peers::Change::Put(record(&peer(1, false)))],
                )
                .unwrap();
            drop(writer);
            drop(database);
            let mut child = Worker(
                Process::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "registry::tests::recovery::crash_worker",
                        "--nocapture",
                    ])
                    .env("AE_REGISTRY_CRASH_PATH", &temp.0)
                    .env("AE_REGISTRY_CRASH_OPERATION", operation)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .unwrap(),
            );
            let mut output = std::io::BufReader::new(child.0.stdout.take().unwrap());
            let wait = |output: &mut std::io::BufReader<std::process::ChildStdout>,
                        expected: &str| loop {
                let mut line = String::new();
                assert_ne!(
                    output.read_line(&mut line).unwrap(),
                    0,
                    "child exited before {expected}"
                );
                if line.contains(expected) {
                    break;
                }
            };
            let advance = |child: &mut Worker| {
                let input = child.0.stdin.as_mut().unwrap();
                input.write_all(b"continue\n").unwrap();
                input.flush().unwrap();
            };
            wait(&mut output, "AE_REGISTRY_PREPARED");
            match phase {
                "before" => {}
                "durable" => {
                    advance(&mut child);
                    wait(&mut output, "AE_REGISTRY_DURABLE");
                }
                "published" => {
                    advance(&mut child);
                    wait(&mut output, "AE_REGISTRY_DURABLE");
                    advance(&mut child);
                    wait(&mut output, "AE_REGISTRY_PUBLISHED");
                }
                race => {
                    advance(&mut child);
                    advance(&mut child);
                    std::thread::sleep(std::time::Duration::from_millis(
                        race.strip_prefix("race").unwrap().parse().unwrap(),
                    ));
                }
            }
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            let database = db(&temp);
            let s = service(database.clone(), config());
            let actual = s.saved_peers().cloned().collect::<Vec<_>>();
            let old = vec![record(&peer(1, false))];
            let new = if operation == "block" {
                vec![]
            } else {
                vec![record(&replacement())]
            };
            match phase {
                "before" => assert_eq!(actual, old),
                "durable" | "published" => assert_eq!(actual, new),
                _ => assert!(actual == old || actual == new),
            };
            assert_eq!(s.version().revision(), if actual == old { 1 } else { 2 });
            assert!(s.next_effect().unwrap().is_none());
            assert_eq!(s.policy().counts(), Default::default());
            assert_eq!(database.view().unwrap().status(), Default::default());
            eprintln!(
                "Registry owner crash recovery {operation}/{phase}: revision {}",
                s.version().revision()
            );
        }
    }
}
