use super::*;
use ae_chain::repository::Read;
use std::{
    io::{BufRead, Write},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

#[test]
fn hc_commit_crash_worker() {
    let Ok(path) = std::env::var("AE_HC_COMMIT_CRASH_PATH") else {
        return;
    };
    // The parent owns cleanup. A killed child must never delete its database.
    let path = std::mem::ManuallyDrop::new(Temp(path.into()));
    let index: usize = std::env::var("AE_HC_COMMIT_CRASH_INDEX")
        .unwrap()
        .parse()
        .unwrap();
    let (db, c) = setup(&path);
    let b = block(&DATA["rows"][0]["rows"][index]);
    let owner = Owner::start(db.clone(), c.clone(), Parent::default()).unwrap();
    let mut live = owner.live.lock().unwrap();
    let (mut state, mut host) = (live.state.clone(), live.host.clone());
    let plan = repo::prepare_hyperchain(
        &db.view().unwrap(),
        &b,
        &c,
        1_000_000,
        &mut state,
        &mut host,
    )
    .unwrap();
    println!("AE_HC_COMMIT_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "commit");
    owner.commit_staged(&mut live, plan, state, host).unwrap();
    println!("AE_HC_COMMIT_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
struct Worker(std::process::Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
fn killed_hc_commits_recover_atomic_micro_and_epoch_state() {
    for index in [0, 5] {
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let path = Temp::new();
            let (db, c) = setup(&path);
            let owner = Owner::start(db.clone(), c.clone(), Parent::default()).unwrap();
            for row in DATA["rows"][0]["rows"]
                .as_array()
                .unwrap()
                .iter()
                .take(index)
            {
                owner
                    .insert(&block(row), 1_000_000, Origin::Gossip)
                    .unwrap();
            }
            let view = db.view().unwrap();
            let before = view.source().prefix(&[], 100_000).unwrap();
            let revision = view.status().revision;
            let b = block(&DATA["rows"][0]["rows"][index]);
            let (_, mut state, mut host) = owner.snapshot().unwrap();
            let expected =
                repo::prepare_hyperchain(&view, &b, &c, 1_000_000, &mut state, &mut host).unwrap();
            drop(view);
            drop(owner);
            drop(db);
            let mut child = Worker(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "hyperchain::tests::recovery::hc_commit_crash_worker",
                        "--nocapture",
                    ])
                    .env("AE_HC_COMMIT_CRASH_PATH", &path.0)
                    .env("AE_HC_COMMIT_CRASH_INDEX", index.to_string())
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .unwrap(),
            );
            let output = child.0.stdout.take().unwrap();
            let (sender, receiver) = std::sync::mpsc::channel();
            let listener = std::thread::spawn(move || {
                for line in std::io::BufReader::new(output).lines() {
                    if sender.send(line.unwrap()).is_err() {
                        break;
                    }
                }
            });
            let wait = |marker: &str| {
                let deadline = Instant::now() + Duration::from_secs(60);
                loop {
                    let line = receiver
                        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                        .unwrap_or_else(|e| {
                            panic!("HC worker {index}/{phase} before {marker}: {e}")
                        });
                    if line == marker {
                        break;
                    }
                }
            };
            wait("AE_HC_COMMIT_READY");
            if phase != "before" {
                let input = child.0.stdin.as_mut().unwrap();
                input.write_all(b"commit\n").unwrap();
                input.flush().unwrap();
                if phase == "after" {
                    wait("AE_HC_COMMIT_DURABLE");
                } else {
                    std::thread::sleep(Duration::from_millis(
                        phase.strip_prefix("race").unwrap().parse().unwrap(),
                    ));
                }
            }
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            listener.join().unwrap();
            let db = Arc::new(
                Database::open_with_config(&path.0, c.identity.clone(), read_config()).unwrap(),
            );
            let view = db.view().unwrap();
            let actual = view.status().revision;
            assert!(actual == revision || actual == revision + 1);
            if phase == "before" {
                assert_eq!(actual, revision);
            }
            if phase == "after" {
                assert_eq!(actual, revision + 1);
            }
            if actual == revision {
                assert_eq!(view.source().prefix(&[], 100_000).unwrap(), before);
            } else {
                view.verify_tip().unwrap();
                assert_eq!(view.status().tip, Some(b.hash().unwrap()));
                assert_eq!(view.hyperchain_checkpoint().unwrap().state, state);
                assert_eq!(
                    view.hyperchain_event(actual).unwrap().unwrap().events,
                    expected.consensus_events()
                );
                let mut scan = view
                    .scan_state(b.hash().unwrap(), Default::default())
                    .unwrap();
                while !scan.is_complete() {
                    scan.next_page(Default::default()).unwrap();
                }
                let owner = Owner::start(db.clone(), c.clone(), Parent::default()).unwrap();
                assert_eq!(owner.snapshot().unwrap().1, state);
            }
            eprintln!("HC ledger crash recovery {index}/{phase}: revision {actual}");
        }
    }
}
