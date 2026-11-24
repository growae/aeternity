use super::*;
use std::io::{BufRead, Write};

fn reopen(path: &std::path::Path) -> Setup {
    let network = profile(3);
    let (trees, native) = input(3);
    let transaction = native.transaction().clone();
    let mut signing = network.signing_prefix(false);
    signing.extend(transaction.encode());
    let signed = ae_mempool::pool::Signed::new(transaction, vec![signer().sign(&signing)]);
    let Header::Key(mut genesis) = genesis::header(Network::Uat).unwrap() else {
        panic!()
    };
    genesis.common.version = 3;
    genesis.common.state_hash = trees.roots().hash().0;
    let id = repo::Identity::new(
        &network,
        Header::Key(genesis).hash().unwrap(),
        Hash256::ZERO,
    );
    let db = Arc::new(Database::open(path, id).unwrap());
    let info = stored(&db);
    let parent = db.view().unwrap().canonical_key(1).unwrap().unwrap();
    Setup {
        db,
        network,
        config: info.config,
        record: info.pending.unwrap(),
        signed,
        parent,
    }
}
#[test]
fn crash_worker() {
    let Some(path) = std::env::var_os("AE_RECEIPT_INSERT_CRASH_PATH") else {
        return;
    };
    let s = reopen(std::path::Path::new(&path));
    let block = micro(&s, s.parent, 20, vec![s.signed.clone()]);
    let update = plan(&s, &block, Default::default());
    println!("AE_RECEIPT_INSERT_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "commit\n");
    s.db.commit(update).unwrap();
    println!("AE_RECEIPT_INSERT_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn killed_block_commits_never_separate_receipts_from_executed_state_and_canonical_location() {
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
            if let Some(reader) = self.reader.take() {
                reader.join().unwrap();
            }
        }
    }
    let (before, after) = {
        let temp = Temp::new();
        let s = setup(&temp, 3);
        let block = micro(&s, s.parent, 20, vec![s.signed.clone()]);
        let before = rows(&s.db);
        s.db.commit(plan(&s, &block, Default::default())).unwrap();
        (before, rows(&s.db))
    };
    for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
        let temp = Temp::new();
        drop(setup(&temp, 3));
        let mut child = Process::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stratum::payouts::tests::retention::insertion::recovery::crash_worker",
                "--nocapture",
            ])
            .env("AE_RECEIPT_INSERT_CRASH_PATH", &temp.0)
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
                    .expect("bounded receipt insertion worker");
                if line.contains(marker) {
                    break;
                }
            }
        };
        wait("AE_RECEIPT_INSERT_READY");
        if phase != "before" {
            let stdin = child.child.stdin.as_mut().unwrap();
            stdin.write_all(b"commit\n").unwrap();
            stdin.flush().unwrap();
            if phase == "after" {
                wait("AE_RECEIPT_INSERT_DURABLE");
            } else {
                std::thread::sleep(Duration::from_millis(
                    phase.strip_prefix("race").unwrap().parse().unwrap(),
                ));
            }
        }
        child.child.kill().unwrap();
        child.child.wait().unwrap();
        drop(child);
        let s = reopen(&temp.0);
        let actual = rows(&s.db);
        match phase {
            "before" => assert_eq!(actual, before),
            "after" => assert_eq!(actual, after),
            _ => assert!(actual == before || actual == after),
        }
        let block = micro(&s, s.parent, 20, vec![s.signed.clone()]);
        if s.db
            .view()
            .unwrap()
            .block(block.hash().unwrap())
            .unwrap()
            .is_none()
        {
            s.db.commit(plan(&s, &block, Default::default())).unwrap();
        }
        assert!(!archived(&s.db).is_empty());
        assert_eq!(
            s.db.view().unwrap().location(s.signed.hash()).unwrap(),
            Some(block.hash().unwrap())
        );
        key_top(&s.db, 3, true);
        s.db.begin_gc(Default::default()).unwrap();
        sweep(&s.db);
        let mut w = Writer::start(s.db.clone(), Default::default()).unwrap();
        let info = w.begin_payouts(s.config, Default::default()).unwrap();
        assert!(matches!(
            w.confirm_payout(info.epoch, &s.record, Default::default())
                .unwrap(),
            Confirmation::Confirmed(_)
        ));
        assert!(s.db.db.get(archive::KEY).unwrap().is_none());
        println!(
            "Stratum receipt insertion crash recovery {phase}: complete block/state/proof batch"
        );
    }
}
