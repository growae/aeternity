use super::*;
use std::{
    io::{BufRead, Write},
    process::{Command, Stdio},
};
fn checkpoint(stage: &str) {
    println!("AE_STRATUM_PRODUCTION_{stage}");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line, "go\n");
}
#[test]
fn crash_worker() {
    let Some(path) = std::env::var_os("AE_STRATUM_PRODUCTION_CRASH_PATH") else {
        return;
    };
    let t = Temp(std::path::PathBuf::from(path));
    let db = open(&t);
    let importer = importer(&db, 1);
    let clock = Wall::new(first().time);
    let (send, receive) = mpsc::channel();
    *clock.gate.lock().unwrap() = Some(receive);
    let (config, _) = configuration(&db, &importer, clock.clone());
    let owner = owner(&db, config);
    let account = owner.client();
    let client = account.mining().unwrap();
    let services = services::Owner::new(services::Config {
        retained_reply_bytes: 2048,
        ..Default::default()
    })
    .unwrap();
    let service = services.client();
    let _connection = registered(&service);
    let mut views = vec![];
    while let Ok(view) = service.registered_connections() {
        views.push(view);
    }
    client.attach_production(service).unwrap();
    until(|| client.production_status().unwrap().preparing.is_some());
    checkpoint("PREPARING");
    send.send(()).unwrap();
    until(|| client.production_status().unwrap().waiting_broadcast);
    let publication = latest(&client, 1);
    assert_eq!(publication.candidate, candidate(&first()));
    checkpoint("STORED");
    drop(views);
    until(|| client.production_status().unwrap().broadcasts == 1);
    checkpoint("NOTIFIED");
    let mut pressure = importer.import_created_block(vec![0], WALL).unwrap();
    let id = accepted(&account, &input(&publication.candidate, &first()));
    until(|| {
        client
            .get(id)
            .unwrap()
            .is_some_and(|r| r.attempts > 0 && r.state == journal::State::Ready)
    });
    checkpoint("CLAIMED");
    clock.set(first().time + 60_000);
    settle(&mut pressure).unwrap_err();
    assert!(matches!(
        done(&client, id).state,
        journal::State::Done(journal::Outcome::Accepted(_))
    ));
    latest(&client, 2);
    checkpoint("REGENERATED");
    panic!("crash worker must be killed");
}
#[test]
fn killed_automatic_producers_rebind_new_epochs_without_replaying_share_accounting() {
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
        "PREPARING",
        "STORED",
        "NOTIFIED",
        "CLAIMED",
        "REGENERATED",
        "race0",
        "race5",
        "race10",
    ] {
        let t = Temp::new();
        drop(open(&t));
        let mut process = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stratum::mining::tests::production::recovery::crash_worker",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("AE_STRATUM_PRODUCTION_CRASH_PATH", &t.0)
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
                if let Some(index) = line.find("AE_STRATUM_PRODUCTION_") {
                    let _ = send.send(line[index..].to_owned());
                }
            }
        });
        let mut child = Child {
            process,
            reader: Some(reader),
        };
        for stage in ["PREPARING", "STORED", "NOTIFIED", "CLAIMED", "REGENERATED"] {
            assert_eq!(
                receive.recv_timeout(Duration::from_secs(30)).unwrap(),
                format!("AE_STRATUM_PRODUCTION_{stage}")
            );
            if phase == stage {
                break;
            }
            child
                .process
                .stdin
                .as_mut()
                .unwrap()
                .write_all(b"go\n")
                .unwrap();
            if stage == "PREPARING"
                && let Some(delay) = phase.strip_prefix("race")
            {
                thread::sleep(Duration::from_millis(delay.parse().unwrap()));
                break;
            }
        }
        child.process.kill().unwrap();
        child.process.wait().unwrap();
        drop(child);
        let db = open(&t);
        let before = db.stratum(Default::default()).unwrap().info();
        let old_epoch = db
            .stratum(Default::default())
            .unwrap()
            .mining(Default::default())
            .unwrap()
            .info()
            .epoch;
        let importer = importer(&db, 1);
        let clock = Wall::new(first().time + 60_000);
        let (config, snapshots) = configuration(&db, &importer, clock);
        let owner = owner(&db, config);
        let client = owner.client().mining().unwrap();
        let services = services::Owner::new(Default::default()).unwrap();
        client.attach_production(services.client()).unwrap();
        let fresh = latest(&client, 1);
        assert_eq!(client.epoch(), old_epoch + 1);
        assert_eq!(fresh.sequence, 1);
        assert_eq!(
            Some(fresh.candidate.parent()),
            db.view().unwrap().status().tip
        );
        assert_eq!(snapshots.live_snapshots(), 0);
        if phase == "CLAIMED" || phase == "REGENERATED" {
            let record = done(
                &client,
                Id {
                    epoch: 1,
                    sequence: 1,
                },
            );
            assert!(matches!(
                record.state,
                journal::State::Done(journal::Outcome::Accepted(_))
            ));
            assert_eq!(record.accounted_key, Some(record.receipt.share_key));
        }
        assert!(owner.shutdown().failure.is_none());
        let after = db.stratum(Default::default()).unwrap().info();
        assert_eq!(before.next_key, after.next_key);
        assert_eq!(before.counts[1], after.counts[1]);
        drop(services);
        stop(importer);
        println!("Stratum production crash recovery {phase}");
    }
}
