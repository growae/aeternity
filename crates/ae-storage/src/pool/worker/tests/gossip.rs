use super::*;
use crate::pool::Signed;
use events::ReadError;

fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../ae-mempool/tests/fixtures/erlang_gossip.json"
    )))
    .unwrap()
}
fn unsigned() -> Arc<Signed> {
    Arc::new(
        Signed::decode(&hex::decode(fixture()["events"]["txs"][0].as_str().unwrap()).unwrap())
            .unwrap(),
    )
}

#[test]
fn received_cache_remembers_rejections_and_created_and_forced_bypass_it() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db.clone());
    let client = worker.client();
    let start = client.event_cursor();
    let invalid = unsigned();
    assert!(matches!(
        client.receive(invalid.clone()).unwrap().wait(),
        Err(Error::Service(Failure::Policy(_)))
    ));
    assert_eq!(
        client.receive(invalid).unwrap().wait().unwrap().outcome,
        PushOutcome::Cached
    );
    assert!(client.events_after(&start, 10).unwrap().events.is_empty());
    let accepted = client.receive(tx()).unwrap().wait().unwrap();
    assert!(matches!(
        accepted.outcome,
        PushOutcome::Accepted {
            event: Event::Received,
            inserted: true,
            ..
        }
    ));
    assert_eq!(
        client.receive(tx()).unwrap().wait().unwrap().outcome,
        PushOutcome::Cached
    );
    assert_eq!(
        client.push(tx(), Event::Created, false).unwrap().wait(),
        Err(Error::Service(Failure::Policy(policy::Error::AlreadyKnown)))
    );
    let stats = client.gossip_cache().stats().unwrap();
    assert_eq!(stats.hit, 2.into());
    assert_eq!(stats.miss, 2.into());
    let forced = client.force_push(tx()).unwrap().wait().unwrap();
    let batch = client.events_after(&start, 10).unwrap();
    assert_eq!(batch.events.len(), 2);
    for (e, s) in batch.events.iter().zip([accepted, forced]) {
        let PushOutcome::Accepted { event, .. } = s.outcome else {
            panic!()
        };
        assert_eq!(e.event, event);
        assert_eq!(e.transaction, tx());
        assert_eq!((e.chain, e.revision), (s.chain, s.revision));
        assert_eq!(
            db.pool_view()
                .unwrap()
                .location(e.transaction.hash())
                .unwrap(),
            Location::Pool
        );
    }
    assert_eq!(client.gossip_cache().stats().unwrap(), stats);
    worker.shutdown().unwrap();
    assert_eq!(client.receive(tx()).err(), Some(Error::Stopped));
}

#[test]
fn saturation_is_remembered_and_cache_lifetime_can_survive_worker_restart() {
    let temp = Temp::new();
    let db = database(&temp);
    let cache = GossipCache::new(2, 10).unwrap();
    let worker = Worker::spawn_with_cache(
        db.clone(),
        NetworkProfile::mainnet(),
        config(),
        Settings {
            requests: 1,
            ..settings()
        },
        cache.clone(),
    )
    .unwrap();
    let old = worker.client();
    let (release, busy) = gate(&old);
    assert_eq!(old.receive(tx()).err(), Some(Error::Saturated));
    assert_eq!(
        old.receive(tx()).unwrap().wait().unwrap().outcome,
        PushOutcome::Cached
    );
    assert_eq!(cache.stats().unwrap().miss, 1.into());
    release.send(()).unwrap();
    busy.wait().unwrap();
    assert!(old.peek(1, None, None).unwrap().wait().unwrap().is_empty());
    worker.shutdown().unwrap();
    let worker = Worker::spawn_with_cache(
        db.clone(),
        NetworkProfile::mainnet(),
        config(),
        settings(),
        cache.clone(),
    )
    .unwrap();
    let client = worker.client();
    assert_eq!(
        client.receive(tx()).unwrap().wait().unwrap().outcome,
        PushOutcome::Cached
    );
    cache.reconfigure(1).unwrap();
    cache.reset().unwrap();
    assert_eq!(cache.stats().unwrap(), Default::default());
    client.receive(tx()).unwrap().wait().unwrap();
    worker.shutdown().unwrap();
    let worker = spawn(db);
    let fresh = worker.client();
    assert_eq!(
        fresh.receive(tx()).unwrap().wait(),
        Err(Error::Service(Failure::Policy(policy::Error::AlreadyKnown)))
    );
    assert_eq!(
        fresh.receive(tx()).unwrap().wait().unwrap().outcome,
        PushOutcome::Cached
    );
    worker.shutdown().unwrap();
}

#[test]
fn committed_publications_survive_lost_replies_and_include_forced_visited_and_canonical_entries() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db.clone());
    let client = worker.client();
    let initial = client.event_cursor();
    drop(client.push(tx(), Event::Created, false).unwrap());
    let first = client
        .wait_for_events(&initial, 1, Duration::from_secs(10))
        .unwrap();
    assert_eq!(first.events.len(), 1);
    assert_eq!(
        db.pool_view().unwrap().location(tx().hash()).unwrap(),
        Location::Pool
    );
    assert_eq!(client.info().pool.revision, first.events[0].revision);
    client
        .candidate(1_000_000, BTreeSet::new(), Default::default())
        .unwrap()
        .wait()
        .unwrap();
    assert!(client.inspect(tx().hash()).unwrap().wait().unwrap().visited);
    client.force_push(tx()).unwrap().wait().unwrap();
    assert_eq!(
        (client.info().pool.active, client.info().pool.visited),
        (1, 1)
    );
    let next = client.events_after(&first.cursor, 10).unwrap();
    assert_eq!(next.events.len(), 1);
    client
        .record_failure(tx().hash(), "insufficient_funds".into(), false)
        .unwrap()
        .wait()
        .unwrap();
    for block in blocks().iter().take(2) {
        db.commit(prepare(&db, block)).unwrap();
    }
    client.refresh().unwrap().wait().unwrap();
    assert!(
        client
            .gossip_snapshot(10)
            .unwrap()
            .wait()
            .unwrap()
            .transactions
            .is_empty()
    );
    assert!(
        client
            .events_after(&next.cursor, 10)
            .unwrap()
            .events
            .is_empty()
    );
    let canonical = client.force_push(tx()).unwrap().wait().unwrap();
    let published = client.events_after(&next.cursor, 10).unwrap();
    assert_eq!(published.events.len(), 1);
    assert_eq!(published.events[0].event, Event::Created);
    assert_eq!(published.events[0].revision, canonical.revision);
    assert!(matches!(
        db.pool_view().unwrap().location(tx().hash()).unwrap(),
        Location::Block(_)
    ));
    assert_eq!(
        client.delete(tx().hash()).unwrap().wait(),
        Err(Error::Service(Failure::Policy(
            policy::Error::AlreadyAccepted
        )))
    );
    client.collect(true, 10).unwrap().wait().unwrap();
    client.recover(config()).unwrap().wait().unwrap();
    assert!(
        client
            .events_after(&published.cursor, 10)
            .unwrap()
            .events
            .is_empty()
    );
    worker.shutdown().unwrap();
}

#[test]
fn storage_failure_emits_nothing_and_cache_reset_allows_a_later_retry() {
    let temp = Temp::new();
    let db = database(&temp);
    let mut cfg = config();
    cfg.commit_bytes = 0;
    let worker = Worker::spawn(db.clone(), NetworkProfile::mainnet(), cfg, settings()).unwrap();
    let client = worker.client();
    let cursor = client.event_cursor();
    assert_eq!(
        client.receive(tx()).unwrap().wait(),
        Err(Error::Service(Failure::Repository(
            ae_chain::repository::Error::ResourceLimit
        )))
    );
    assert!(client.events_after(&cursor, 10).unwrap().events.is_empty());
    assert_eq!(
        db.pool_view().unwrap().location(tx().hash()).unwrap(),
        Location::Unknown
    );
    assert_eq!(
        client.receive(tx()).unwrap().wait().unwrap().outcome,
        PushOutcome::Cached
    );
    client.recover(config()).unwrap().wait().unwrap();
    client.gossip_cache().reset().unwrap();
    client.receive(tx()).unwrap().wait().unwrap();
    assert_eq!(client.events_after(&cursor, 10).unwrap().events.len(), 1);
    let after = client.event_cursor();
    client.delete(tx().hash()).unwrap().wait().unwrap();
    assert!(client.events_after(&after, 10).unwrap().events.is_empty());
    assert_eq!(
        db.pool_view().unwrap().location(tx().hash()).unwrap(),
        Location::Deleted
    );
    worker.shutdown().unwrap();
}

#[test]
fn bounded_history_reports_lag_and_drains_after_shutdown() {
    for byte_bound in [false, true] {
        let temp = Temp::new();
        let db = database(&temp);
        let bytes = BASE_WEIGHT
            + ae_tx::storage::encode(&tx(), ReadConfig::default().transaction_record())
                .unwrap()
                .len();
        let limits = if byte_bound {
            Settings {
                event_count: 10,
                event_bytes: 2 * bytes,
                ..settings()
            }
        } else {
            Settings {
                event_count: 2,
                ..settings()
            }
        };
        let worker = Worker::spawn(db, NetworkProfile::mainnet(), config(), limits).unwrap();
        let client = worker.client();
        let start = client.event_cursor();
        for _ in 0..3 {
            client.force_push(tx()).unwrap().wait().unwrap();
        }
        let resume = match client.events_after(&start, 10).unwrap_err() {
            ReadError::Lagged { missed: 1, resume } => resume,
            e => panic!("{e:?}"),
        };
        assert_eq!(resume, client.oldest_event_cursor());
        let next = client.events_after(&resume, 1).unwrap();
        assert_eq!(next.events[0].sequence, 2);
        assert!(!next.closed);
        let current = client.event_cursor();
        assert!(
            client
                .wait_for_events(&current, 1, Duration::from_millis(2))
                .unwrap()
                .events
                .is_empty()
        );
        assert_eq!(
            client.events_after(&current, 0).unwrap_err(),
            ReadError::InvalidLimit
        );
        worker.shutdown().unwrap();
        let last = client
            .wait_for_events(&next.cursor, 10, Duration::from_secs(10))
            .unwrap();
        assert!(last.closed);
        assert_eq!(last.events[0].sequence, 3);
        let empty = client
            .wait_for_events(&last.cursor, 10, Duration::from_secs(10))
            .unwrap();
        assert!(empty.closed && empty.events.is_empty());
    }
}

#[test]
fn disabled_or_oversized_history_never_changes_admission_and_snapshot_marks_its_boundary() {
    for limits in [
        Settings {
            event_count: 0,
            ..settings()
        },
        Settings {
            event_bytes: 1,
            ..settings()
        },
    ] {
        let temp = Temp::new();
        let db = database(&temp);
        let worker =
            Worker::spawn(db.clone(), NetworkProfile::mainnet(), config(), limits).unwrap();
        let client = worker.client();
        let start = client.event_cursor();
        client.receive(tx()).unwrap().wait().unwrap();
        assert!(matches!(
            client.events_after(&start, 10),
            Err(ReadError::Lagged { missed: 1, .. })
        ));
        assert_eq!(
            db.pool_view().unwrap().location(tx().hash()).unwrap(),
            Location::Pool
        );
        let partial = client.gossip_snapshot(0).unwrap().wait().unwrap();
        assert!(!partial.complete && partial.transactions.is_empty());
        let full = client.gossip_snapshot(10).unwrap().wait().unwrap();
        assert!(full.complete);
        assert_eq!(full.transactions, vec![tx()]);
        assert_eq!(full.cursor, client.event_cursor());
        worker.shutdown().unwrap();
        let worker = spawn(db);
        let new = worker.client();
        assert_eq!(
            new.events_after(&full.cursor, 10).unwrap_err(),
            ReadError::WrongStream
        );
        worker.shutdown().unwrap();
    }
}

#[test]
fn snapshot_and_cursor_are_atomic_with_queued_submissions() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db);
    let client = worker.client();
    assert_eq!(
        client.gossip_snapshot(settings().max_peek + 1).err(),
        Some(Error::RequestTooLarge)
    );
    let (release, busy) = gate(&client);
    let first = client.force_push(tx()).unwrap();
    let snapshot = client.gossip_snapshot(10).unwrap();
    let second = client.force_push(tx()).unwrap();
    release.send(()).unwrap();
    busy.wait().unwrap();
    first.wait().unwrap();
    let snapshot = snapshot.wait().unwrap();
    second.wait().unwrap();
    assert!(snapshot.complete);
    assert_eq!(snapshot.transactions, vec![tx()]);
    let after = client.events_after(&snapshot.cursor, 10).unwrap();
    assert_eq!(after.events.len(), 1);
    assert_eq!(after.events[0].sequence, 2);
    let reader = client.clone();
    let cursor = after.cursor.clone();
    let waiting = thread::spawn(move || {
        reader
            .wait_for_events(&cursor, 10, Duration::from_secs(10))
            .unwrap()
    });
    client.gossip_cache().reset().unwrap();
    assert!(client.receive(tx()).unwrap().wait().is_err());
    worker.shutdown().unwrap();
    let last = waiting.join().unwrap();
    assert!(last.closed && last.events.is_empty());
}
