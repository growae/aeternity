use super::*;
use ae_chain::publication::{Effect, Origin, Propagation};
use ae_storage::events::{self, Config};
fn insert(db: &Database, block: &Block, origin: ledger::Origin) -> ae_storage::Committed {
    let network = profile(Network::Mainnet);
    db.commit(
        ledger::prepare(
            &db.view().unwrap(),
            block,
            &transactions::Config::primitives(&network),
            &rules(Network::Mainnet),
            Limits {
                policy: ledger::Policy {
                    origin,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap(),
    )
    .unwrap()
}
#[test]
fn committed_event_payloads_preserve_real_execution_and_original_origin_rules() {
    for (input, output) in [
        (ledger::Origin::Gossip, Origin::Received),
        (ledger::Origin::Created, Origin::Created),
        (ledger::Origin::Sync, Origin::Synced),
    ] {
        let dir = Temp::new();
        let db = Database::open(&dir.0, identity(Network::Mainnet)).unwrap();
        bootstrap(&db, Network::Mainnet);
        let reader = db.events();
        let mut cursor = reader.cursor();
        let mut tx_events = 0;
        for block in blocks(Network::Mainnet) {
            let receipt = insert(&db, &block, input);
            let batch = reader.after(&cursor, 1).unwrap();
            assert_eq!(batch.records.len(), 1);
            let record = &batch.records[0];
            assert_eq!(record.change(), receipt.change());
            assert_eq!(record.context().origin, output);
            assert_eq!(record.context().block.unwrap().hash, block.hash().unwrap());
            assert_eq!(batch.cursor.revision(), cursor.revision() + 1);
            let events: Vec<_> = record
                .effects()
                .filter_map(|e| {
                    if let Effect::Transaction { event, .. } = e {
                        Some(event)
                    } else {
                        None
                    }
                })
                .collect();
            tx_events += receipt.update().events().len();
            if output == Origin::Synced {
                assert!(events.is_empty());
                assert!(record.body().is_none());
                assert!(record.effects().all(|e| matches!(e, Effect::TopChanged(_))));
            } else {
                assert_eq!(events, receipt.update().events().iter().collect::<Vec<_>>());
                assert_eq!(
                    record.body().unwrap().as_ref(),
                    match &block {
                        Block::Key(_) => block.encode().unwrap(),
                        Block::Micro(m) => ae_chain::block::Light::from_micro(m).encode().unwrap(),
                    }
                );
                assert!(record.effects().any(|e|matches!(e,Effect::BlockToPublish {origin,..} if origin == if output==Origin::Created {Propagation::Created}else{Propagation::Received})));
                assert_eq!(
                    record
                        .effects()
                        .any(|e| matches!(e, Effect::BlockCreated(_))),
                    output == Origin::Created
                );
            }
            cursor = batch.cursor;
        }
        assert!(tx_events > 0);
        assert!(reader.after(&cursor, 1).unwrap().records.is_empty());
    }
}
#[test]
fn snapshot_cursor_pins_tip_and_reopen_does_not_invent_transient_origin() {
    let dir = Temp::new();
    let id = identity(Network::Mainnet);
    let db = Database::open(&dir.0, id.clone()).unwrap();
    bootstrap(&db, Network::Mainnet);
    let reader = db.events();
    let (pinned, cursor) = db.event_snapshot().unwrap();
    let receipt = insert(&db, &blocks(Network::Mainnet)[0], ledger::Origin::Gossip);
    assert_eq!(pinned.status().revision, cursor.revision());
    assert_eq!(pinned.status().tip, Some(id.genesis));
    assert_eq!(
        reader.after(&cursor, 1).unwrap().records[0].change(),
        receipt.change()
    );
    drop(pinned);
    drop(db);
    let drained = reader.after(&cursor, 1).unwrap();
    assert!(drained.closed);
    assert_eq!(drained.records.len(), 1);
    let reopened = Database::open(&dir.0, id).unwrap();
    assert_eq!(
        reopened.events().cursor().revision(),
        receipt.change().revision
    );
    assert_eq!(reopened.events().info().records, 0);
    assert_eq!(
        reopened.events().after(&cursor, 1).unwrap_err(),
        events::Error::WrongSource
    );
    assert_eq!(
        reopened
            .view()
            .unwrap()
            .event(receipt.change().revision)
            .unwrap()
            .as_ref(),
        Some(receipt.change())
    );
}
#[test]
fn source_overflow_and_oversized_records_report_exact_gaps_without_rejecting_commits() {
    for bytes in [1, 16 << 20] {
        let dir = Temp::new();
        let db = Database::open_with_events(
            &dir.0,
            identity(Network::Mainnet),
            Default::default(),
            Default::default(),
            Config { records: 1, bytes },
        )
        .unwrap();
        bootstrap(&db, Network::Mainnet);
        let reader = db.events();
        let cursor = reader.cursor();
        let all = blocks(Network::Mainnet);
        insert(&db, &all[0], ledger::Origin::Gossip);
        let receipt = insert(&db, &all[1], ledger::Origin::Gossip);
        let events::Error::Gap { missed, resume } = reader.after(&cursor, 10).unwrap_err() else {
            panic!()
        };
        assert_eq!(missed, if bytes == 1 { 2 } else { 1 });
        let retained = reader.after(&resume, 10).unwrap();
        assert_eq!(retained.records.len(), usize::from(bytes != 1));
        assert_eq!(retained.cursor.revision(), receipt.change().revision);
        assert_eq!(
            db.view().unwrap().status().tip,
            Some(all[1].hash().unwrap())
        );
        assert!(reader.info().bytes <= bytes);
        if bytes == 1 {
            assert_eq!(reader.info().unretained, 3)
        } else {
            assert_eq!(reader.info().evicted, 2)
        };
    }
}
#[test]
fn rollback_records_reorg_change_without_republishing_detached_blocks() {
    let dir = Temp::new();
    let db = Database::open(&dir.0, identity(Network::Mainnet)).unwrap();
    bootstrap(&db, Network::Mainnet);
    let all = blocks(Network::Mainnet);
    for block in &all {
        insert(&db, block, ledger::Origin::Sync);
    }
    let reader = db.events();
    let cursor = reader.cursor();
    let receipt = db
        .rollback(
            repo::rollback::Target::Key(all[0].hash().unwrap()),
            Default::default(),
        )
        .unwrap();
    let batch = reader.after(&cursor, 1).unwrap();
    assert_eq!(batch.records.len(), 1);
    let record = &batch.records[0];
    assert_eq!(record.context().origin, Origin::Rollback);
    assert!(record.body().is_none());
    assert_eq!(
        record.effects().collect::<Vec<_>>(),
        vec![Effect::Reverted(receipt.change())]
    );
    assert!(!receipt.change().detached.is_empty());
    assert!(receipt.change().requeue.is_empty());
    assert!(!receipt.change().discarded.is_empty());
    insert(&db, &all[1], ledger::Origin::Gossip);
    assert_eq!(
        reader.after(&batch.cursor, 1).unwrap().records[0]
            .context()
            .origin,
        Origin::Received
    );
}
