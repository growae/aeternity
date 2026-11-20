use super::*;
use ae_protocol::NetworkProfile;
use ae_state::genesis::{self, Network};
use serde_json::Value as J;
use std::path::PathBuf;
mod recovery;
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "ae-peer-store-{}-{}",
            std::process::id(),
            NEXT_SOURCE.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn identity() -> Identity {
    Identity::new(
        &NetworkProfile::mainnet(),
        genesis::header(Network::Mainnet).unwrap().hash().unwrap(),
        Hash256::ZERO,
    )
}
fn database(t: &Temp) -> Arc<Database> {
    Arc::new(Database::open(&t.0, identity()).unwrap())
}
fn peer(n: u8) -> Record {
    Record {
        id: PeerPublicKey([n; 32]),
        host: format!("peer{n}").into_bytes(),
        address: IpAddr::from([1, 2, 3, n]),
        source: IpAddr::from([5, 6, 7, n]),
        port: 3015,
        trusted: false,
    }
}
fn rows(db: &Database) -> Vec<Record> {
    db.peer_view().unwrap().records(Default::default()).unwrap()
}
fn json_peer(v: &J) -> Record {
    let ip = |v: &J| {
        let a = v.as_array().unwrap();
        match a.len() {
            4 => IpAddr::from(std::array::from_fn::<_, 4, _>(|i| {
                a[i].as_u64().unwrap().try_into().unwrap()
            })),
            8 => IpAddr::from(std::array::from_fn::<_, 8, _>(|i| {
                a[i].as_u64().unwrap().try_into().unwrap()
            })),
            _ => panic!("address"),
        }
    };
    Record {
        id: PeerPublicKey(
            hex::decode(v["id"].as_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap(),
        ),
        host: hex::decode(v["host"].as_str().unwrap()).unwrap(),
        address: ip(&v["address"]),
        source: ip(&v["source"]),
        port: v["port"].as_u64().unwrap().try_into().unwrap(),
        trusted: v["trusted"].as_bool().unwrap(),
    }
}
fn json_change(v: &J) -> Change {
    let p = json_peer(&v["peer"]);
    match v["op"].as_str().unwrap() {
        "put" => Change::Put(p),
        "delete" => Change::Delete(p.id),
        _ => panic!("operation"),
    }
}

#[test]
fn original_mnesia_peer_records_updates_atomic_abort_and_restarts_match() {
    let fixture: J =
        serde_json::from_str(include_str!("../../tests/fixtures/erlang_peer_store.json")).unwrap();
    let mut steps = 0;
    for scenario in fixture["scenarios"].as_array().unwrap() {
        let temp = Temp::new();
        let mut db = database(&temp);
        let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
        for step in scenario["steps"].as_array().unwrap() {
            let version = writer.version().unwrap();
            match step["op"].as_str().unwrap() {
                "restart" => {
                    drop(writer);
                    drop(db);
                    db = database(&temp);
                    writer = Writer::start(db.clone(), Default::default()).unwrap();
                }
                "abort" => {
                    let changes: Vec<_> = step["changes"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(json_change)
                        .collect();
                    assert!(
                        writer
                            .commit_checked(
                                version,
                                &changes,
                                || Err(Error::Backend("abort".into())),
                                |_, _| panic!("must not write")
                            )
                            .is_err()
                    );
                }
                "batch" => {
                    let changes: Vec<_> = step["changes"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(json_change)
                        .collect();
                    writer.commit(version, &changes).unwrap();
                }
                _ => {
                    writer.commit(version, &[json_change(step)]).unwrap();
                }
            }
            assert_eq!(
                rows(&db),
                step["state"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(json_peer)
                    .collect::<Vec<_>>(),
                "{} step {steps}",
                scenario["name"]
            );
            assert_eq!(db.view().unwrap().status(), Status::default());
            steps += 1;
        }
    }
    assert_eq!(steps, 27);
    eprintln!("Original peer storage: 27 steps across three disk-backed Mnesia scenarios");
}

#[test]
fn exclusive_writer_snapshot_revision_and_resource_boundaries() {
    let temp = Temp::new();
    let db = database(&temp);
    let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
    assert!(matches!(
        Writer::start(db.clone(), Default::default()),
        Err(Failure::AlreadyRunning)
    ));
    let pinned = db.peer_view().unwrap();
    let old = pinned.version();
    let v = writer.commit(old, &[Change::Put(peer(1))]).unwrap();
    assert_eq!(v.revision(), 1);
    assert!(pinned.records(Default::default()).unwrap().is_empty());
    assert!(pinned.get(peer(1).id, 1024).unwrap().is_none());
    assert!(matches!(
        writer.commit(old, &[Change::Delete(peer(1).id)]),
        Err(Failure::Storage(Error::Conflict))
    ));
    let other_temp = Temp::new();
    let other = database(&other_temp);
    assert!(matches!(
        writer.commit(other.peer_view().unwrap().version(), &[]),
        Err(Failure::Storage(Error::Identity))
    ));
    assert_eq!(writer.commit(v, &[]).unwrap(), v);
    assert_eq!(
        db.peer_view().unwrap().get(peer(1).id, 1024).unwrap(),
        Some(peer(1))
    );
    assert!(matches!(
        db.peer_view().unwrap().records(Limits {
            bytes: 1,
            ..Default::default()
        }),
        Err(Error::ResourceLimit)
    ));
    assert!(matches!(
        db.peer_view().unwrap().get(peer(1).id, 1),
        Err(Error::ResourceLimit)
    ));
    drop(writer);
    assert!(matches!(
        Writer::start(
            db.clone(),
            Limits {
                host_bytes: 1,
                ..Default::default()
            }
        ),
        Err(Failure::Storage(Error::ResourceLimit))
    ));
    let mut writer = Writer::start(
        db.clone(),
        Limits {
            records: 1,
            changes: 3,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        writer.commit(v, &[Change::Put(peer(2))]),
        Err(Failure::Storage(Error::ResourceLimit))
    ));
    assert_eq!(writer.version().unwrap(), v);
    assert_eq!(rows(&db), vec![peer(1)]);
    let v = writer
        .commit(v, &[Change::Put(peer(2)), Change::Delete(peer(1).id)])
        .unwrap();
    assert_eq!(rows(&db), vec![peer(2)]);
    assert!(matches!(
        writer.commit(v, &vec![Change::Delete(peer(1).id); 4]),
        Err(Failure::Storage(Error::ResourceLimit))
    ));
    let mut long = peer(2);
    long.host = vec![1; 1025];
    assert!(
        writer
            .commit(v, &[Change::Put(long), Change::Delete(peer(2).id)])
            .is_err()
    );
    assert_eq!(writer.version().unwrap(), v);
    drop(writer);
    let mut writer = Writer::start(
        db.clone(),
        Limits {
            commit_bytes: 33,
            ..Default::default()
        },
    )
    .unwrap();
    // The deletion key fits, but the mandatory revision/count batch does not.
    assert!(matches!(
        writer.commit(v, &[Change::Delete(peer(2).id)]),
        Err(Failure::Storage(Error::ResourceLimit))
    ));
    assert_eq!(rows(&db), vec![peer(2)]);
    drop(writer);
    drop(pinned);
    drop(db);
    let db = database(&temp);
    let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
    assert!(matches!(
        writer.commit(v, &[]),
        Err(Failure::Storage(Error::Identity))
    ));
    assert_eq!(rows(&db), vec![peer(2)]);
}

#[test]
fn every_construction_failure_and_ambiguous_write_outcome_can_be_reconciled() {
    let temp = Temp::new();
    let db = database(&temp);
    let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
    let v = writer
        .commit(
            writer.version().unwrap(),
            &[Change::Put(peer(1)), Change::Put(peer(2))],
        )
        .unwrap();
    let changes = [
        Change::Delete(peer(1).id),
        Change::Put(peer(3)),
        Change::Put(peer(2)),
    ];
    // Three supplied effects, four final batch keys, then pre-publication check.
    for fail in 0..8 {
        let mut calls = 0;
        assert!(
            writer
                .commit_checked(
                    v,
                    &changes,
                    || {
                        let n = calls;
                        calls += 1;
                        if n == fail {
                            Err(Error::Backend("construction".into()))
                        } else {
                            Ok(())
                        }
                    },
                    |_, _| panic!("published failed batch")
                )
                .is_err()
        );
        assert_eq!(rows(&db), vec![peer(1), peer(2)]);
        assert_eq!(writer.version().unwrap(), v);
    }
    // Both possible outcomes of an I/O error are explicit; neither permits the
    // caller to continue mutating from the old in-memory state.
    for persisted in [false, true] {
        let before = writer.version().unwrap();
        let pinned = db.peer_view().unwrap();
        let mut replacement = peer(2);
        replacement.host = vec![u8::from(persisted); 32];
        assert!(matches!(
            writer.commit_checked(
                before,
                &[Change::Put(replacement.clone())],
                || Ok(()),
                |db, batch| {
                    if persisted {
                        db.db.write_opt(batch, &write_options()).map_err(backend)?;
                    }
                    Err(Error::Backend("ambiguous I/O".into()))
                }
            ),
            Err(Failure::Storage(Error::Backend(_)))
        ));
        assert!(matches!(
            writer.commit(before, &[]),
            Err(Failure::RestartRequired)
        ));
        let (current, records) = writer.recover().unwrap();
        assert_eq!(current.revision(), before.revision() + u64::from(persisted));
        assert_eq!(records[1], if persisted { replacement } else { peer(2) });
        assert_eq!(
            pinned.records(Default::default()).unwrap(),
            vec![peer(1), peer(2)]
        );
        writer.commit(current, &[]).unwrap();
    }
    let v = writer.version().unwrap();
    writer.commit(v, &changes).unwrap();
    assert_eq!(rows(&db), vec![peer(2), peer(3)]);
}

#[test]
fn corrupt_record_metadata_and_future_schema_never_silently_reset() {
    let record = peer(1);
    let key = repo::key(RECORDS, record.id.0);
    let raw = record.encode(MAX_HOST).unwrap();
    for size in [0, 1, 1024, MAX_HOST] {
        let mut p = record.clone();
        p.host = vec![255; size];
        assert_eq!(
            Record::decode(&key, &p.encode(MAX_HOST).unwrap(), MAX_HOST).unwrap(),
            p
        );
    }
    let Value::List(fields) = Value::decode(&raw).unwrap() else {
        panic!()
    };
    let mut corrupt = vec![
        vec![],
        vec![0],
        vec![0; MAX_HOST + 129],
        Value::List(vec![number(0); 65_536]).encode(),
        Value::List(vec![Value::List(vec![number(0)])]).encode(),
    ];
    for (index, value) in [
        (0, number(2)),
        (1, Value::Bytes(vec![2; 32])),
        (2, Value::List(vec![])),
        (3, Value::Bytes(vec![1; 5])),
        (4, Value::Bytes(vec![])),
        (5, number(65536)),
        (6, number(2)),
    ] {
        let mut changed = fields.clone();
        changed[index] = value;
        corrupt.push(Value::List(changed).encode());
    }
    for bytes in corrupt {
        assert!(Record::decode(&key, &bytes, MAX_HOST).is_err());
        let temp = Temp::new();
        let db = database(&temp);
        db.db.put(&key, &bytes).unwrap();
        db.db
            .put(
                METADATA,
                Metadata {
                    revision: 1,
                    records: 1,
                    bytes: (key.len() + bytes.len()) as u64,
                }
                .encode(),
            )
            .unwrap();
        assert!(db.peer_view().unwrap().records(Default::default()).is_err());
        assert!(Writer::start(db.clone(), Default::default()).is_err());
        assert_eq!(db.db.get(&key).unwrap().unwrap(), bytes);
    }
    for schema in 1..=8 {
        for bad in [
            vec![0],
            Value::List(vec![number(2), number(1), number(0), number(0)]).encode(),
        ] {
            let temp = Temp::new();
            let db = database(&temp);
            let Value::List(mut fields) = Value::decode(&identity().encode()).unwrap() else {
                panic!()
            };
            fields[0] = number(schema);
            let legacy = Value::List(fields).encode();
            db.db.put(repo::IDENTITY, &legacy).unwrap();
            db.db.put(METADATA, &bad).unwrap();
            drop(db);
            assert!(Database::open(&temp.0, identity()).is_err());
            let raw = DB::open_default(&temp.0).unwrap();
            assert_eq!(raw.get(repo::IDENTITY).unwrap().unwrap(), legacy);
            assert_eq!(raw.get(METADATA).unwrap().unwrap(), bad);
        }
    }
    let temp = Temp::new();
    let db = database(&temp);
    db.db.put(&key, &raw).unwrap();
    drop(db);
    assert!(matches!(
        Database::open(&temp.0, identity()),
        Err(Error::Corrupt("peer records without metadata"))
    ));
    let raw_db = DB::open_default(&temp.0).unwrap();
    raw_db
        .put(
            METADATA,
            Metadata {
                revision: 1,
                records: 2,
                bytes: (key.len() + raw.len()) as u64,
            }
            .encode(),
        )
        .unwrap();
    drop(raw_db);
    let db = database(&temp);
    assert!(matches!(
        db.peer_view().unwrap().records(Default::default()),
        Err(Error::Corrupt("peer metadata totals"))
    ));
    db.db
        .put(
            METADATA,
            Metadata {
                revision: u64::MAX,
                records: 1,
                bytes: (key.len() + raw.len()) as u64,
            }
            .encode(),
        )
        .unwrap();
    let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
    assert!(matches!(
        writer.commit(writer.version().unwrap(), &[Change::Delete(record.id)]),
        Err(Failure::Storage(Error::ResourceLimit))
    ));
    assert_eq!(rows(&db), vec![record]);
}

#[test]
fn peer_writes_coexist_with_canonical_bootstrap_pool_and_gc() {
    use ae_chain::repository::ledger;
    use ae_codec::headers::Header;
    use ae_tx::{SignedTransaction, Spend, Transaction, TransactionLeaf};
    use ae_types::{AccountPublicKey, Id, IdTag};
    let temp = Temp::new();
    let db = database(&temp);
    let Header::Key(header) = genesis::header(Network::Mainnet).unwrap() else {
        panic!()
    };
    let trees = ae_state::Trees {
        accounts: genesis::state(Network::Mainnet).unwrap().0,
        contracts: Default::default(),
        channels: Default::default(),
        calls: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    let plan = ledger::bootstrap(
        &db.view().unwrap(),
        &NetworkProfile::mainnet(),
        &header,
        &trees,
        Default::default(),
    )
    .unwrap();
    let pinned = db.peer_view().unwrap();
    let mut writer = Writer::start(db.clone(), Default::default()).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let worker_barrier = barrier.clone();
    let worker = std::thread::spawn(move || {
        worker_barrier.wait();
        let mut version = writer.version().unwrap();
        for n in 0..100 {
            let mut p = peer(1);
            p.source = IpAddr::from([9, 8, n, 7]);
            version = writer.commit(version, &[Change::Put(p)]).unwrap();
        }
        (writer, version)
    });
    barrier.wait();
    // A canonical plan prepared before peer mutations remains valid.
    db.commit(plan).unwrap();
    let mut pool =
        pool::Service::start(db.clone(), NetworkProfile::mainnet(), Default::default()).unwrap();
    let tx = Arc::new(SignedTransaction::new(
        Transaction::new(TransactionLeaf::Spend(
            Spend::new(AccountPublicKey([1; 32]), Id::new(IdTag::Account, [2; 32])).unwrap(),
        )),
        vec![],
    ));
    pool.force_push(tx.clone()).unwrap();
    assert!(db.begin_gc(Default::default()).unwrap());
    let mut pages = 0;
    while db.gc_state().unwrap().active() {
        db.gc_step(gc::Limits {
            rows: 64,
            bytes: 1 << 20,
        })
        .unwrap();
        pages += 1;
        assert!(pages < 1000);
    }
    let (mut writer, version) = worker.join().unwrap();
    assert_eq!(version.revision(), 100);
    let mut expected = peer(1);
    expected.source = IpAddr::from([9, 8, 99, 7]);
    assert_eq!(rows(&db), vec![expected]);
    assert!(pinned.records(Default::default()).unwrap().is_empty());
    assert_eq!(
        db.pool_view().unwrap().members(10).unwrap(),
        vec![tx.hash()]
    );
    let other_rows = || {
        db.db
            .iterator(IteratorMode::Start)
            .map(Result::unwrap)
            .filter(|(key, _)| key.first() != Some(&RECORDS) && key.as_ref() != METADATA)
            .collect::<Vec<_>>()
    };
    let canonical = other_rows();
    writer
        .commit(version, &[Change::Delete(peer(1).id), Change::Put(peer(2))])
        .unwrap();
    assert_eq!(other_rows(), canonical);
    db.view().unwrap().verify_tip().unwrap();
    assert_eq!(
        db.view().unwrap().status(),
        Status {
            revision: 1,
            tip: Some(identity().genesis)
        }
    );
    drop(writer);
    drop(pool);
    drop(pinned);
    drop(db);
    let db = database(&temp);
    assert_eq!(rows(&db), vec![peer(2)]);
    assert_eq!(
        db.pool_view().unwrap().members(10).unwrap(),
        vec![tx.hash()]
    );
    db.view().unwrap().verify_tip().unwrap();
}
