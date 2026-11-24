use super::*;
#[test]
fn crash_worker() {
    use std::io::Write;
    let Some(path) = std::env::var_os("AE_PAYOUT_RUNTIME_CRASH_PATH") else {
        return;
    };
    let t = Temp(PathBuf::from(path));
    let db = database(&t);
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    match std::env::var("AE_PAYOUT_RUNTIME_CRASH_STAGE")
        .unwrap()
        .as_str()
    {
        "prepared" => pending_sign(&mut a, &mut w),
        "signed" => {
            pending_sign(&mut a, &mut w);
            a.finish(&mut w).unwrap();
        }
        "submit" => pending_push(&mut a, &mut w),
        "receipt" => {
            a.request_check();
            accepted(&mut a, &mut w);
        }
        _ => panic!("runtime crash stage"),
    }
    println!("AE_PAYOUT_RUNTIME_READY");
    std::io::stdout().flush().unwrap();
    loop {
        thread::park();
    }
}
#[test]
fn killed_live_payout_requests_reopen_exact_bytes_and_never_replay_unknown_pushes() {
    use std::{
        io::BufRead,
        process::{Command, Stdio},
        sync::mpsc,
    };
    struct Child {
        child: std::process::Child,
        reader: Option<thread::JoinHandle<()>>,
    }
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            if let Some(r) = self.reader.take() {
                r.join().unwrap();
            }
        }
    }
    for stage in ["prepared", "signed", "submit", "receipt"] {
        for delay in [0, 1, 5, 10] {
            let t = Temp::new();
            let db = database(&t);
            seed(&db, payment());
            drop(db);
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "stratum::mining::payouts::tests::recovery::crash_worker",
                    "--nocapture",
                ])
                .env("AE_PAYOUT_RUNTIME_CRASH_PATH", &t.0)
                .env("AE_PAYOUT_RUNTIME_CRASH_STAGE", stage)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let (tx, rx) = mpsc::channel();
            let reader = thread::spawn(move || {
                for l in std::io::BufReader::new(stdout).lines() {
                    if tx.send(l.unwrap()).is_err() {
                        break;
                    }
                }
            });
            let mut child = Child {
                child,
                reader: Some(reader),
            };
            let end = Instant::now() + Duration::from_secs(30);
            loop {
                let line = rx
                    .recv_timeout(end.saturating_duration_since(Instant::now()))
                    .expect("bounded live payout worker");
                if line.contains("AE_PAYOUT_RUNTIME_READY") {
                    break;
                }
            }
            thread::sleep(Duration::from_millis(delay));
            child.child.kill().unwrap();
            child.child.wait().unwrap();
            drop(child);
            let db = database(&t);
            let before = stored(&db).pending.unwrap();
            let request = before.request.as_ref().unwrap();
            assert_eq!(
                request.prepared.request().environment.account_nonce,
                0.into()
            );
            assert_eq!(request.prepared.request().network, network().network_id());
            let expected = request
                .prepared
                .attach_signature(secret().sign(request.prepared.signing_bytes()))
                .unwrap()
                .encode();
            if let Some(s) = &before.signed {
                assert_eq!(s.transaction, expected);
            }
            let p = pool(&db, 1);
            let s = signer();
            let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
            let mut a = actor(&db, &mut w, &s, &p);
            if matches!(stage, "prepared" | "signed") {
                let after = accepted(&mut a, &mut w);
                assert_eq!(after.request, before.request);
                assert_eq!(after.signed.unwrap().transaction, expected);
                assert_eq!(p.client().info().submitted, 1);
            } else {
                tick(&mut a, &mut w);
                let after = stored(&db).pending.unwrap();
                assert_eq!(after.request, before.request);
                assert_eq!(after.signed.as_ref().unwrap().transaction, expected);
                assert_eq!(p.client().info().submitted, 0);
                match after.signed.as_ref().unwrap().dispatch {
                    Dispatch::Sending(1) => assert_eq!(
                        ae_storage::pool::Stored::load(db.view().unwrap())
                            .unwrap()
                            .location(after.hash().unwrap().unwrap())
                            .unwrap(),
                        ae_mempool::pool::Location::Unknown
                    ),
                    Dispatch::Done {
                        outcome:
                            durable::Outcome::Accepted { .. } | durable::Outcome::Observed { .. },
                        ..
                    } => assert_eq!(
                        ae_storage::pool::Stored::load(db.view().unwrap())
                            .unwrap()
                            .location(after.hash().unwrap().unwrap())
                            .unwrap(),
                        ae_mempool::pool::Location::Pool
                    ),
                    _ => panic!("retained submission phase"),
                }
            }
            a.finish(&mut w).unwrap();
            assert!(a.report().unsettled.is_none());
            p.shutdown().unwrap();
            s.retire();
            eprintln!(
                "Stratum payout runtime crash recovery {stage}/{delay}: exact request, no blind replay"
            );
        }
    }
}
