use super::*;
use std::{
    io::{BufRead, Write},
    process::{Command, Stdio},
};
fn checkpoint(name: &str) {
    println!("AE_STRATUM_OWNER_{name}");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "go\n");
}
#[test]
fn crash_worker() {
    let Some(path) = std::env::var_os("AE_STRATUM_OWNER_CRASH_PATH") else {
        return;
    };
    let t = Temp(PathBuf::from(path));
    let db = open(&t);
    let importer = importer(&db, 1);
    let h = first();
    let candidate = candidate(&h);
    let mut writer = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let epoch = writer.begin_mining(Default::default()).unwrap();
    let ingress = writer.begin_ingress(Default::default()).unwrap();
    writer
        .publish_work(&epoch, 1, candidate.clone(), date(), Default::default())
        .unwrap();
    let receipt = writer
        .accept_mining(
            &ingress,
            1,
            input(&candidate, &h),
            Default::default(),
            &epoch,
            Default::default(),
        )
        .unwrap();
    checkpoint("CLAIMED");
    let record = db
        .stratum(Default::default())
        .unwrap()
        .mining(Default::default())
        .unwrap()
        .get(receipt.id)
        .unwrap()
        .unwrap();
    let record = writer
        .begin_submission(&record, Default::default())
        .unwrap();
    checkpoint("SUBMITTING");
    let mut admission = importer
        .import_stratum(&record.solution().unwrap().unwrap(), WALL, true)
        .unwrap();
    until(|| admission.poll().is_some());
    let result = admission.result().unwrap().clone();
    result.as_ref().unwrap();
    checkpoint("COMMITTED");
    let record = writer
        .finish_submission(
            &record,
            outcome(&record, result).unwrap(),
            Default::default(),
        )
        .unwrap();
    checkpoint("FINISHED");
    writer
        .acknowledge_solution(&record, Default::default())
        .unwrap();
    checkpoint("ACKNOWLEDGED");
    panic!("crash worker must be killed");
}
#[test]
fn killed_owners_reconcile_canonical_commit_gaps_without_reaccounting_or_duplicate_publication() {
    struct Child {
        process: std::process::Child,
        reader: Option<thread::JoinHandle<()>>,
    }
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.process.kill();
            let _ = self.process.wait();
            if let Some(reader) = self.reader.take() {
                reader.join().unwrap();
            }
        }
    }
    for phase in [
        "CLAIMED",
        "SUBMITTING",
        "COMMITTED",
        "FINISHED",
        "ACKNOWLEDGED",
        "race0",
        "race5",
        "race10",
    ] {
        let t = Temp::new();
        let db = open(&t);
        let genesis = db.view().unwrap().status();
        drop(db);
        let mut process = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stratum::mining::tests::recovery::crash_worker",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("AE_STRATUM_OWNER_CRASH_PATH", &t.0)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = process.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let line = line.unwrap();
                if let Some(offset) = line.find("AE_STRATUM_OWNER_") {
                    let _ = send.send(line[offset..].to_owned());
                }
            }
        });
        let mut child = Child {
            process,
            reader: Some(reader),
        };
        for stage in [
            "CLAIMED",
            "SUBMITTING",
            "COMMITTED",
            "FINISHED",
            "ACKNOWLEDGED",
        ] {
            assert_eq!(
                receive.recv_timeout(Duration::from_secs(30)).unwrap(),
                format!("AE_STRATUM_OWNER_{stage}")
            );
            if stage == phase {
                break;
            }
            child
                .process
                .stdin
                .as_mut()
                .unwrap()
                .write_all(b"go\n")
                .unwrap();
            if stage == "SUBMITTING"
                && let Some(ms) = phase.strip_prefix("race")
            {
                thread::sleep(Duration::from_millis(ms.parse().unwrap()));
                break;
            }
        }
        child.process.kill().unwrap();
        child.process.wait().unwrap();
        drop(child);
        let db = open(&t);
        let before = db.stratum(Default::default()).unwrap().info();
        let importer = importer(&db, 1);
        let owner = start(&db, &importer);
        let client = owner.client().mining().unwrap();
        let id = Id {
            epoch: 1,
            sequence: 1,
        };
        if phase == "ACKNOWLEDGED" {
            assert!(client.get(id).unwrap().is_none());
        } else {
            let record = done(&client, id);
            assert!(
                matches!(
                    record.state,
                    journal::State::Done(journal::Outcome::Accepted(_))
                ),
                "{phase}: {record:?}"
            );
            assert_eq!(record.accounted_key, Some(record.receipt.share_key));
            if phase == "COMMITTED" {
                assert!(matches!(
                    record.state,
                    journal::State::Done(journal::Outcome::Accepted(journal::Accepted::Observed(
                        _
                    )))
                ));
            }
            if phase == "FINISHED" {
                assert!(matches!(
                    record.state,
                    journal::State::Done(journal::Outcome::Accepted(journal::Accepted::Committed(
                        _
                    )))
                ));
            }
        }
        assert!(owner.shutdown().failure.is_none());
        let after = db.stratum(Default::default()).unwrap().info();
        assert_eq!(before.counts, after.counts);
        assert_eq!(before.next_key, after.next_key);
        let header = first();
        let block = Block::Key(header.clone());
        let view = db.view().unwrap();
        assert_eq!(view.status().tip, Some(block.hash().unwrap()));
        assert_eq!(view.status().revision, genesis.revision + 1);
        assert_eq!(
            view.require_record(block.hash().unwrap())
                .unwrap()
                .roots
                .state
                .hash()
                .0,
            header.common.state_hash
        );
        drop(view);
        stop(importer);
        println!("Stratum owner crash recovery {phase}");
    }
}
