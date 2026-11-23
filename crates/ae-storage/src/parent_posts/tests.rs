use super::*;
use crate::tests::{Temp, identity, plan};
mod recovery;
fn database(t: &Temp) -> Arc<Database> {
    Arc::new(Database::open(&t.0, identity()).unwrap())
}
fn record(n: u8) -> Record {
    Record {
        intent: Intent {
            chain: Chain::Aeternity,
            network: b"parent-network".to_vec(),
            account: AccountPublicKey([n; 32]),
            payload: vec![n; 80],
        },
        endpoint: b"https://parent.example/".to_vec(),
        transaction: vec![n; 150],
        response: None,
    }
}
fn rows(db: &Database) -> Vec<Record> {
    db.parent_posts()
        .unwrap()
        .records(Default::default())
        .unwrap()
}
#[test]
fn journal_reservation_deduplicates_independently_of_fee_nonce_or_endpoint() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let snapshot = db.parent_posts().unwrap();
    let first = record(1);
    assert_eq!(
        w.reserve(first.clone()).unwrap(),
        Reservation::New(first.clone())
    );
    let mut altered = first.clone();
    altered.endpoint = b"http://replacement.example:3013/".to_vec();
    altered.transaction = vec![2; 100];
    assert_eq!(
        w.reserve(altered).unwrap(),
        Reservation::Existing(first.clone())
    );
    assert_eq!(db.parent_posts().unwrap().revision(), 1);
    assert!(snapshot.records(Default::default()).unwrap().is_empty());
    for n in 0..4 {
        let mut other = first.clone();
        match n {
            0 => other.intent.network.push(b'2'),
            1 => other.intent.chain = Chain::Dogecoin,
            2 => other.intent.account.0[0] = 5,
            _ => other.intent.payload.push(5),
        }
        assert_ne!(other.intent.id(), first.intent.id());
        assert!(matches!(w.reserve(other), Ok(Reservation::New(_))));
    }
    assert_eq!(rows(&db).len(), 5);
    assert_eq!(db.view().unwrap().status(), Status::default());
}
#[test]
fn response_reconciliation_is_exact_monotonic_and_survives_reopen() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    assert!(matches!(
        Writer::start(db.clone(), Default::default()),
        Err(Failure::AlreadyRunning)
    ));
    let before = record(1);
    w.reserve(before.clone()).unwrap();
    let snapshot = db.parent_posts().unwrap();
    let after = w.record_response(&before, b"parent-hash".to_vec()).unwrap();
    assert_eq!(after.response.as_deref(), Some(b"parent-hash".as_slice()));
    assert!(matches!(
        w.record_response(&before, b"different".to_vec()),
        Err(Failure::Storage(Error::Conflict))
    ));
    assert_eq!(
        w.record_response(&after, b"parent-hash".to_vec()).unwrap(),
        after
    );
    assert_eq!(
        snapshot.records(Default::default()).unwrap(),
        vec![before.clone()]
    );
    drop(snapshot);
    drop(w);
    drop(db);
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    assert_eq!(
        w.reserve(before).unwrap(),
        Reservation::Existing(after.clone())
    );
    assert_eq!(w.recover().unwrap(), vec![after]);
    assert_eq!(db.parent_posts().unwrap().revision(), 2);
}
#[test]
fn ambiguous_intent_and_response_writes_require_recovery_without_replaying() {
    for operation in ["intent", "response"] {
        for persisted in [false, true] {
            let t = Temp::new();
            let db = database(&t);
            let mut w = Writer::start(db.clone(), Default::default()).unwrap();
            let first = record(1);
            if operation == "response" {
                w.reserve(first.clone()).unwrap();
            }
            let write = |db: &Database, batch: WriteBatch| {
                if persisted {
                    db.db.write_opt(batch, &write_options()).map_err(backend)?;
                }
                Err(Error::Backend("ambiguous journal write".into()))
            };
            let failed = if operation == "intent" {
                w.reserve_with(first.clone(), write).map(|_| ())
            } else {
                w.response_with(&first, b"hash".to_vec(), write).map(|_| ())
            };
            assert!(failed.is_err() && w.recovery_required());
            assert_eq!(w.reserve(first.clone()), Err(Failure::RecoveryRequired));
            assert_eq!(w.get(&first.intent), Err(Failure::RecoveryRequired));
            let actual = w.recover().unwrap();
            assert!(!w.recovery_required());
            if operation == "intent" && !persisted {
                assert!(actual.is_empty());
            } else {
                let mut expected = first.clone();
                if operation == "response" && persisted {
                    expected.response = Some(b"hash".to_vec())
                }
                assert_eq!(actual, vec![expected.clone()]);
                assert_eq!(w.reserve(first).unwrap(), Reservation::Existing(expected));
            }
        }
    }
}
#[test]
fn journal_bounds_fail_before_writes_and_never_evict_possible_submissions() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(
        db.clone(),
        Limits {
            records: 1,
            bytes: 4096,
            transaction_bytes: 150,
        },
    )
    .unwrap();
    let first = record(1);
    w.reserve(first.clone()).unwrap();
    assert!(matches!(
        w.reserve(record(2)),
        Err(Failure::Storage(Error::ResourceLimit))
    ));
    let mut large = first.clone();
    large.transaction.push(0);
    assert!(w.reserve(large).is_err());
    let mut secret = record(2);
    secret.endpoint = b"http://user:password@host/".to_vec();
    assert!(w.reserve(secret).is_err());
    assert_eq!(rows(&db), vec![first]);
    assert_eq!(db.parent_posts().unwrap().revision(), 1);
    assert!(
        db.parent_posts()
            .unwrap()
            .records(Limits {
                records: 1,
                bytes: 10,
                transaction_bytes: 150
            })
            .is_err()
    );
}
#[test]
fn schema_upgrade_validates_journal_and_preserves_canonical_pool_and_peer_metadata() {
    for with_journal in [false, true] {
        let t = Temp::new();
        let db = database(&t);
        db.commit(plan(&db)).unwrap();
        let status = db.view().unwrap().status();
        let roots = db
            .view()
            .unwrap()
            .require_record(status.tip.unwrap())
            .unwrap()
            .roots;
        let mut peers = crate::peers::Writer::start(db.clone(), Default::default()).unwrap();
        peers
            .commit(
                peers.version().unwrap(),
                &[crate::peers::Change::Put(crate::peers::Record {
                    id: ae_types::PeerPublicKey([9; 32]),
                    host: b"local".to_vec(),
                    address: "127.0.0.1".parse().unwrap(),
                    source: "127.0.0.1".parse().unwrap(),
                    port: 3015,
                    trusted: false,
                })],
            )
            .unwrap();
        drop(peers);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        if with_journal {
            w.reserve(record(1)).unwrap();
        }
        drop(w);
        let Value::List(mut fields) =
            Value::from_rlp(&ae_codec::rlp::decode(&db.identity.encode()).unwrap())
        else {
            panic!()
        };
        assert_eq!(fields[0], number(14));
        fields[0] = number(11);
        let legacy = Value::List(fields).encode();
        db.db.put(repo::IDENTITY, &legacy).unwrap();
        drop(db);
        let db = database(&t);
        assert_eq!(
            db.db.get(repo::IDENTITY).unwrap().unwrap(),
            identity().encode()
        );
        assert_eq!(db.view().unwrap().status(), status);
        assert_eq!(
            db.view()
                .unwrap()
                .require_record(status.tip.unwrap())
                .unwrap()
                .roots,
            roots
        );
        assert_eq!(
            db.peer_view()
                .unwrap()
                .records(Default::default())
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            rows(&db),
            if with_journal {
                vec![record(1)]
            } else {
                vec![]
            }
        );
    }
}

#[test]
fn missing_metadata_corruption_and_future_records_block_open_without_upgrading() {
    for kind in ["missing", "totals", "version", "key", "encoding"] {
        let t = Temp::new();
        let db = database(&t);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let first = record(1);
        w.reserve(first.clone()).unwrap();
        drop(w);
        let key = repo::key(RECORDS, first.intent.id().unwrap());
        match kind {
            "missing" => db.db.delete(METADATA).unwrap(),
            "totals" => {
                let mut m = Metadata::decode(db.db.get(METADATA).unwrap()).unwrap();
                m.bytes += 1;
                db.db.put(METADATA, m.encode()).unwrap();
            }
            "key" => {
                let raw = db.db.get(&key).unwrap().unwrap();
                db.db.delete(key).unwrap();
                db.db.put(repo::key(RECORDS, [99; 32]), raw).unwrap();
            }
            _ => {
                let raw = db.db.get(&key).unwrap().unwrap();
                let Value::List(mut fields) = flat(&raw, 4096, 10).unwrap() else {
                    panic!()
                };
                if kind == "version" {
                    fields[0] = number(2)
                } else {
                    fields[7] = number(0);
                    fields[8] = bytes(b"noncanonical")
                };
                db.db.put(key, Value::List(fields).encode()).unwrap();
            }
        }
        let Value::List(mut fields) = flat(&identity().encode(), 4096, 7).unwrap() else {
            panic!()
        };
        fields[0] = number(11);
        let legacy = Value::List(fields).encode();
        db.db.put(repo::IDENTITY, &legacy).unwrap();
        drop(db);
        assert!(Database::open(&t.0, identity()).is_err(), "{kind}");
        let raw = DB::open_default(&t.0).unwrap();
        assert_eq!(raw.get(repo::IDENTITY).unwrap().unwrap(), legacy);
    }
}
