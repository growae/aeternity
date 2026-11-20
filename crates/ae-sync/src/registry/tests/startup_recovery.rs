use super::*;
use crate::registry::startup::Configuration;
use ae_p2p::address;
use std::io::{BufRead, Write};

fn configuration() -> Configuration {
    let uri = |n| address::encode(key(n), b"127.0.0.1", &3015.into(), Default::default()).unwrap();
    Configuration {
        peers: vec![uri(3)],
        blocked_peers: vec![uri(1), uri(2)],
        include_default_peers: false,
        ..Default::default()
    }
}
fn marker(s: &str) {
    println!("{s}");
    std::io::stdout().flush().unwrap();
}
fn release() {
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "continue\n");
}
struct GateStore(Writer);
impl Store for GateStore {
    fn recover(&mut self) -> Result<(Version, Vec<Record>), peers::Failure> {
        self.0.recover()
    }
    fn commit(&mut self, v: Version, w: &[peers::Change]) -> Result<Version, peers::Failure> {
        assert_eq!(
            w,
            &[peers::Change::Delete(key(1)), peers::Change::Delete(key(2))]
        );
        marker("AE_STARTUP_PREPARED");
        release();
        let version = self.0.commit(v, w)?;
        marker("AE_STARTUP_DURABLE");
        release();
        Ok(version)
    }
    fn uncertain(&self) -> bool {
        self.0.recovery_required()
    }
}

#[test]
fn crash_worker() {
    let Some(path) = std::env::var_os("AE_STARTUP_CRASH_PATH") else {
        return;
    };
    let database = Arc::new(Database::open(path, identity()).unwrap());
    let cfg = config();
    let registry =
        Registry::with_seed(key(128), cfg.policy.clone(), &[1, 2, 3, 4], [7; 32]).unwrap();
    let store = GateStore(Writer::start(database, cfg.storage).unwrap());
    let mut core = Core::start(registry, store, &cfg).unwrap();
    assert!(
        core.configure_startup(0, b"ae_mainnet", configuration(), Default::default())
            .unwrap()
            .ready
    );
    assert_eq!(core.registry.blocked().count(), 2);
    assert_eq!(core.startup.as_ref().unwrap().status().protected_trusted, 1);
    marker("AE_STARTUP_PUBLISHED");
    loop {
        std::thread::park();
    }
}

#[test]
fn killed_startup_owners_recover_whole_block_batches_and_reinstall_intents_once() {
    use std::process::{Command as Process, Stdio};
    struct Worker(std::process::Child);
    impl Drop for Worker {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
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
        let old: Vec<_> = (1..=3).map(|n| record(&peer(n, false))).collect();
        let new = vec![old[2].clone()];
        let mut writer = Writer::start(database.clone(), Default::default()).unwrap();
        writer
            .commit(
                writer.version().unwrap(),
                &old.iter()
                    .cloned()
                    .map(peers::Change::Put)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        drop(writer);
        drop(database);
        let mut child = Worker(
            Process::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "registry::tests::startup_recovery::crash_worker",
                    "--nocapture",
                ])
                .env("AE_STARTUP_CRASH_PATH", &temp.0)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let mut output = std::io::BufReader::new(child.0.stdout.take().unwrap());
        let wait = |output: &mut std::io::BufReader<std::process::ChildStdout>, expected: &str| loop {
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
        wait(&mut output, "AE_STARTUP_PREPARED");
        match phase {
            "before" => {}
            "durable" => {
                advance(&mut child);
                wait(&mut output, "AE_STARTUP_DURABLE");
            }
            "published" => {
                advance(&mut child);
                wait(&mut output, "AE_STARTUP_DURABLE");
                advance(&mut child);
                wait(&mut output, "AE_STARTUP_PUBLISHED");
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
        let mut owner = service(database.clone(), config());
        let recovered = owner.saved_peers().cloned().collect::<Vec<_>>();
        match phase {
            "before" => assert_eq!(recovered, old),
            "durable" | "published" => assert_eq!(recovered, new),
            _ => assert!(recovered == old || recovered == new),
        }
        assert_eq!(
            owner.version().revision(),
            if recovered == old { 1 } else { 2 }
        );
        assert!(owner.next_effect().unwrap().is_none());
        assert_eq!(owner.policy().counts(), Default::default());
        let status = owner
            .configure_startup(0, configuration(), Default::default())
            .unwrap();
        assert!(status.ready);
        assert_eq!((status.queued, status.protected_trusted), (1, 1));
        assert_eq!(owner.version().revision(), 2);
        assert_eq!(owner.saved_peers().cloned().collect::<Vec<_>>(), new);
        assert_eq!(
            drain(&mut owner.core),
            vec![Effect::Blocked(key(1)), Effect::Blocked(key(2))]
        );
        owner.advance_startup(1, 1).unwrap();
        let effects = drain(&mut owner.core);
        assert_eq!(effects.len(), 1);
        assert!(matches!(&effects[0], Effect::Resolve { host, .. } if host == b"127.0.0.1"));
        assert_eq!(owner.recover().unwrap(), Recovery::Unchanged);
        assert!(owner.next_effect().unwrap().is_none());
        assert_eq!(database.view().unwrap().status(), Default::default());
        eprintln!(
            "Startup owner crash recovery {phase}: complete batch and exactly one reinstallation"
        );
    }
}
