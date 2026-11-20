use super::*;

#[test]
fn coalesced_confirm_and_requeue_events_match_immediate_reconciliation() {
    for final_confirmation in [false, true] {
        let mut results = vec![];
        for delayed in [false, true] {
            let temp = Temp::new();
            let db = database(&temp);
            let all = blocks();
            for block in &all {
                db.commit(prepare(&db, block)).unwrap();
            }
            let mut service =
                Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
            let original = tx();
            let pending = Arc::new(ae_tx::SignedTransaction::new(
                original.transaction().clone(),
                vec![],
            ));
            service.force_push(pending.clone()).unwrap();
            service
                .candidate(1000000, &BTreeSet::new(), Default::default(), || 0)
                .unwrap();
            service
                .record_failure(pending.hash(), "insufficient_funds", false)
                .unwrap();
            for (index, confirmed) in [
                (5, Some(true)),
                (4, Some(false)),
                (6, final_confirmation.then_some(true)),
            ] {
                let old = db.view().unwrap().status();
                let hash = all[index].hash().unwrap();
                // The event stream is trusted input to this consumer. Reuse
                // already executed headers/state, and isolate fork membership
                // effects without claiming these fabricated forks pass PoW.
                let change = repo::Change {
                    revision: old.revision + 1,
                    inserted: Some(hash),
                    removed: vec![],
                    discarded: vec![],
                    old_tip: old.tip,
                    new_tip: hash,
                    ancestor: None,
                    detached: vec![],
                    attached: vec![],
                    requeue: if confirmed == Some(false) {
                        vec![pending.hash()]
                    } else {
                        vec![]
                    },
                    confirm: if confirmed == Some(true) {
                        vec![(pending.hash(), hash)]
                    } else {
                        vec![]
                    },
                };
                let mut writes = repo::Writes::new();
                writes.insert(
                    repo::key(repo::EVENTS, change.revision.to_be_bytes()),
                    Some(change.encode()),
                );
                writes.insert(
                    repo::STATUS.to_vec(),
                    Some(
                        Status {
                            revision: change.revision,
                            tip: Some(hash),
                        }
                        .encode(),
                    ),
                );
                if let Some(confirmed) = confirmed {
                    writes.insert(
                        repo::key(repo::TRANSACTIONS, pending.hash().0),
                        Some(ae_tx::storage::encode(&pending, Default::default()).unwrap()),
                    );
                    writes.insert(
                        repo::key(repo::LOCATIONS, pending.hash().0),
                        confirmed.then(|| hash.0.to_vec()),
                    );
                }
                {
                    let _guard = db.writer.lock().unwrap();
                    let pool = db.pool_chain_writes(&change, &writes).unwrap();
                    let mut batch = WriteBatch::default();
                    for (key, value) in writes.into_iter().chain(pool) {
                        match value {
                            Some(value) => batch.put(key, value),
                            None => batch.delete(key),
                        }
                    }
                    db.db.write_opt(batch, &write_options()).unwrap();
                }
                if !delayed {
                    service.refresh().unwrap();
                }
            }
            let final_state = service.refresh().unwrap();
            let peek = service.peek(10, None, None).unwrap();
            assert_eq!(peek.is_empty(), final_confirmation);
            if !final_confirmation {
                let inspection = service.inspect(pending.hash()).unwrap();
                assert_eq!(inspection.failures, 0);
                assert!(!inspection.visited);
                assert_eq!(inspection.expires, 26);
            }
            results.push((
                final_state,
                peek,
                db.pool_view().unwrap().members(10).unwrap(),
            ));
        }
        assert_eq!(results[0], results[1]);
    }
}

#[test]
fn confirmation_batch_failures_preserve_chain_status_record_and_pool_membership() {
    let temp = Temp::new();
    let db = database(&temp);
    let t = tx();
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    service.push(t.clone(), Event::Created, false).unwrap();
    db.commit(prepare(&db, &blocks()[0])).unwrap();
    let baseline = service.refresh().unwrap();
    let block = blocks().remove(1);
    let update = prepare(&db, &block);
    let count = update.writes().len()
        + db.pool_chain_writes(update.change(), update.writes())
            .unwrap()
            .len();
    let rows: Vec<_> = db
        .db
        .iterator(IteratorMode::Start)
        .map(Result::unwrap)
        .collect();
    for fail_at in 0..=count {
        let mut call = 0;
        assert!(matches!(
            db.commit_checked(prepare(&db, &block), || {
                let now = call;
                call += 1;
                if now == fail_at {
                    Err(Error::Backend("injected confirmation failure".into()))
                } else {
                    Ok(())
                }
            }),
            Err(Error::Backend(_))
        ));
        assert_eq!(
            db.db
                .iterator(IteratorMode::Start)
                .map(Result::unwrap)
                .collect::<Vec<_>>(),
            rows
        );
        assert_eq!(service.refresh().unwrap(), baseline);
    }
    db.commit(update).unwrap();
    assert_eq!(
        db.pool_view().unwrap().location(t.hash()).unwrap(),
        Location::Block(block.hash().unwrap())
    );
    assert_eq!(service.refresh().unwrap().active, 0);
}

#[test]
fn retention_gc_preserves_pending_records_and_delayed_owner_rebuilds_from_current_roots() {
    let temp = Temp::new();
    let db = database(&temp);
    let mut cfg = config();
    cfg.policy.tx_ttl = 100;
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), cfg).unwrap();
    // This real transaction is never submitted to the selected chain here.
    // A second local transaction with different signatures remains pending.
    let original = tx();
    let pending = Arc::new(ae_tx::SignedTransaction::new(
        original.transaction().clone(),
        vec![],
    ));
    service.force_push(pending.clone()).unwrap();
    let encoded = ae_tx::storage::encode(&pending, Default::default()).unwrap();
    for block in blocks() {
        db.commit(prepare(&db, &block)).unwrap();
    }
    assert!(db.begin_gc(Default::default()).unwrap());
    while db.gc_state().unwrap().active() {
        db.gc_step(Default::default()).unwrap();
    }
    assert!(matches!(
        db.view()
            .unwrap()
            .trees(identity().genesis, Default::default()),
        Err(Error::Pruned { .. })
    ));
    assert_eq!(
        db.pool_view().unwrap().location(pending.hash()).unwrap(),
        Location::Pool
    );
    assert_eq!(
        db.db.get(repo::key(RECORDS, pending.hash().0)).unwrap(),
        Some(encoded)
    );
    let info = service.refresh().unwrap();
    assert_eq!(info.chain, db.view().unwrap().status());
    assert_eq!(info.active, 1);
    assert_eq!(info.gc_height, 6);
    assert_eq!(service.peek(1, None, None).unwrap(), vec![pending]);
}

#[test]
fn crash_worker() {
    use std::io::{BufRead, Write};
    let Ok(path) = std::env::var("AE_POOL_CRASH_PATH") else {
        return;
    };
    let operation = std::env::var("AE_POOL_CRASH_OPERATION").unwrap();
    let db = Arc::new(Database::open(path, identity()).unwrap());
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    let t = tx();
    let update = (operation == "confirm").then(|| prepare(&db, &blocks()[1]));
    println!("AE_POOL_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "commit");
    match operation.as_str() {
        "admit" => {
            service.push(t, Event::Created, false).unwrap();
        }
        "delete" => {
            service.delete(t.hash()).unwrap();
        }
        "confirm" => {
            db.commit(update.unwrap()).unwrap();
        }
        "requeue" => {
            membership_change(&db, &t, false);
        }
        _ => panic!("unknown operation"),
    }
    println!("AE_POOL_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn killed_pool_owners_recover_complete_membership_records_and_canonical_locations() {
    use std::io::{BufRead, Write};
    use std::process::{Command, Stdio};
    struct Worker(std::process::Child);
    impl Drop for Worker {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for operation in ["admit", "delete", "confirm", "requeue"] {
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let temp = Temp::new();
            let db = database(&temp);
            let t = tx();
            let h = t.hash();
            let mut service =
                Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
            if operation != "admit" {
                service.push(t.clone(), Event::Created, false).unwrap();
            }
            if operation == "confirm" {
                db.commit(prepare(&db, &blocks()[0])).unwrap();
            }
            if operation == "requeue" {
                membership_change(&db, &t, true);
            }
            let old = db.pool_view().unwrap().location(h).unwrap();
            let expected = match operation {
                "admit" | "requeue" => Location::Pool,
                "delete" => Location::Deleted,
                "confirm" => Location::Block(blocks()[1].hash().unwrap()),
                _ => unreachable!(),
            };
            drop(service);
            drop(db);
            let mut child = Worker(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "pool::tests::recovery::crash_worker",
                        "--nocapture",
                    ])
                    .env("AE_POOL_CRASH_PATH", &temp.0)
                    .env("AE_POOL_CRASH_OPERATION", operation)
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
                    "worker exited before {marker}"
                );
                if line.contains(marker) {
                    break;
                }
            };
            wait(&mut output, "AE_POOL_READY");
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
                    wait(&mut output, "AE_POOL_DURABLE");
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(
                        phase.strip_prefix("race").unwrap().parse().unwrap(),
                    ));
                }
            }
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            let db = database(&temp);
            let stored = db.pool_view().unwrap();
            let current = stored.location(h).unwrap();
            match phase {
                "before" => assert_eq!(current, old),
                "after" => assert_eq!(current, expected),
                _ => assert!(current == old || current == expected),
            }
            assert_eq!(
                stored.members(10).unwrap().contains(&h),
                current == Location::Pool
            );
            assert_eq!(
                stored.transaction(h).unwrap().is_some(),
                current != Location::Unknown
            );
            if operation == "confirm" {
                assert_eq!(
                    db.view()
                        .unwrap()
                        .block(blocks()[1].hash().unwrap())
                        .unwrap()
                        .is_some(),
                    current == expected
                );
                db.view().unwrap().verify_tip().unwrap();
            }
            let mut service =
                Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
            assert_eq!(
                service.peek(10, None, None).unwrap().contains(&t),
                current == Location::Pool
            );
            eprintln!("Pool crash recovery {operation}/{phase}: {current:?}");
        }
    }
}
