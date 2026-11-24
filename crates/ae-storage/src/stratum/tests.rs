use super::*;
use crate::tests::{Temp, identity, plan};
use ae_chain::repository as repo;
use ae_stratum::accounting::{Record, Table};
use std::collections::BTreeMap;
mod recovery;
mod reference;
fn database(t: &Temp) -> Arc<Database> {
    Arc::new(Database::open(&t.0, identity()).unwrap())
}
fn hash(n: u8) -> Hash256 {
    let mut h = [0; 32];
    h[31] = n;
    Hash256(h)
}
fn share(n: u8) -> Command {
    Command::Share {
        hash: hash(n),
        target: 100.into(),
        miner: vec![n],
    }
}
fn payment() -> core::Payment {
    ae_stratum::rewards::Payment {
        id: (71, 0),
        total: 100.into(),
        relative: [(b"a".to_vec(), Number::Float(1.0f64.to_bits()))].into(),
    }
    .into()
}
fn reward() -> core::Reward {
    core::Reward {
        height: 71,
        hash: hash(11),
        pool: core::Shares::Relative([(b"pool".to_vec(), Number::Float(1.0f64.to_bits()))].into()),
        miners: core::Shares::Relative(
            [(b"miner".to_vec(), Number::Float(1.0f64.to_bits()))].into(),
        ),
        amount: 1000.into(),
        round_key: u64::MAX - 2,
    }
}
fn rows(db: &Database) -> Vec<Record> {
    db.stratum(Default::default())
        .unwrap()
        .query(|tx| {
            let mut out = vec![];
            for t in Table::ALL {
                out.extend(tx.records(t)?);
            }
            Ok(out)
        })
        .unwrap()
}
fn commit(w: &mut Writer, c: Vec<Command>) -> Commit {
    w.commit(w.version().unwrap(), c).unwrap()
}
#[test]
fn allocation_atomic_updates_snapshots_and_exclusive_ownership_survive_reopen() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    assert!(matches!(
        Writer::start(db.clone(), Default::default()),
        Err(Failure::AlreadyRunning)
    ));
    let snapshot = db.stratum(Default::default()).unwrap();
    let start = snapshot.version();
    let done = commit(
        &mut w,
        vec![Command::Round, share(1), share(1), Command::Round],
    );
    assert_eq!(done.version.revision(), 1);
    assert!(
        snapshot
            .query(|t| t.records(Table::Shares))
            .unwrap()
            .is_empty()
    );
    assert_eq!(snapshot.info(), Info::default());
    drop(snapshot);
    assert_eq!(
        done.replies[0],
        core::Reply::Record(Box::new(Record::Round(u64::MAX)))
    );
    let stored = db.stratum(Default::default()).unwrap();
    assert_eq!(stored.info().next_key, Some(u64::MAX - 4));
    assert_eq!(stored.info().counts, [1, 2, 2, 0, 0, 0]);
    assert_eq!(
        stored.query(|tx| tx.hash(hash(1))).unwrap(),
        Some(u64::MAX - 2)
    );
    drop(stored);
    assert_eq!(
        w.commit(start, vec![share(2)]),
        Err(Failure::Storage(Error::Conflict))
    );
    commit(
        &mut w,
        vec![Command::Solution {
            candidate: hash(1),
            block: hash(11),
        }],
    );
    let old = rows(&db);
    let version = w.version().unwrap();
    assert_eq!(db.view().unwrap().status(), Default::default());
    drop(w);
    drop(db);
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    assert_eq!(rows(&db), old);
    assert_eq!(
        w.commit(version, vec![share(2)]),
        Err(Failure::Storage(Error::Conflict))
    );
    let done = commit(&mut w, vec![share(2)]);
    let core::Reply::Record(r) = &done.replies[0] else {
        panic!()
    };
    let Record::Share(s) = r.as_ref() else {
        panic!()
    };
    assert_eq!(s.key, u64::MAX - 4);
    let stored = db.stratum(Default::default()).unwrap();
    assert_eq!(stored.query(|t| t.hash(hash(1))).unwrap(), None);
    assert_eq!(
        stored.query(|t| t.hash(hash(11))).unwrap(),
        Some(u64::MAX - 2)
    );
    stored.verify().unwrap();
}
#[test]
fn failed_batches_preserve_allocator_revision_rows_and_no_partial_pruning() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let version = w.version().unwrap();
    assert_eq!(
        w.commit(
            version,
            vec![
                Command::Round,
                share(1),
                Command::Solution {
                    candidate: hash(9),
                    block: hash(11)
                }
            ]
        ),
        Err(Failure::Accounting(core::Error::MissingHash))
    );
    assert_eq!(w.version().unwrap(), version);
    assert!(rows(&db).is_empty());
    assert!(!w.recovery_required());
    assert_eq!(
        db.stratum(Default::default()).unwrap().info(),
        Info::default()
    );
    commit(
        &mut w,
        vec![
            Command::Round,
            share(1),
            Command::Round,
            share(2),
            Command::Reward(reward()),
            Command::StorePayments {
                reward: reward(),
                percent: Number::Float(1.5f64.to_bits()),
            },
        ],
    );
    let original = rows(&db);
    let before = db.stratum(Default::default()).unwrap().info();
    assert!(
        w.commit(
            w.version().unwrap(),
            vec![
                Command::DeleteRecords(71),
                Command::Solution {
                    candidate: hash(99),
                    block: hash(11)
                }
            ]
        )
        .is_err()
    );
    assert_eq!(rows(&db), original);
    assert_eq!(db.stratum(Default::default()).unwrap().info(), before);
    commit(&mut w, vec![Command::DeleteRecords(71)]);
    assert_eq!(
        db.stratum(Default::default()).unwrap().info().counts,
        [1, 1, 0, 0, 0, 0]
    );
    // Queries cannot accidentally publish staged mutations.
    assert_eq!(
        db.stratum(Default::default())
            .unwrap()
            .query(|t| t.apply(core::Operation::Round(1))),
        Err(Failure::MutationInQuery)
    );
    assert_eq!(
        db.stratum(Default::default()).unwrap().info().next_key,
        before.next_key
    );
}
#[test]
fn uncertain_writes_reconcile_exact_durable_results_without_replaying() {
    for persisted in [false, true] {
        for operation in ["share", "solution", "prune"] {
            let t = Temp::new();
            let db = database(&t);
            let mut w = Writer::start(db.clone(), Default::default()).unwrap();
            commit(
                &mut w,
                vec![
                    Command::Round,
                    share(1),
                    Command::Round,
                    Command::Reward(reward()),
                    Command::Payment(payment()),
                ],
            );
            let commands = match operation {
                "share" => vec![Command::Round, share(2)],
                "solution" => vec![Command::Solution {
                    candidate: hash(1),
                    block: hash(11),
                }],
                _ => vec![Command::DeleteRecords(71)],
            };
            let version = w.version().unwrap();
            let old = rows(&db);
            let old_info = db.stratum(Default::default()).unwrap().info();
            assert!(matches!(
                w.commit_with(version, commands.clone(), |db, batch| {
                    if persisted {
                        db.db.write_opt(batch, &write_options()).map_err(backend)?;
                    }
                    Err(Error::Backend("lost accounting acknowledgement".into()))
                }),
                Err(Failure::Storage(Error::Backend(_)))
            ));
            assert!(w.recovery_required());
            assert_eq!(w.version(), Err(Failure::RecoveryRequired));
            assert_eq!(
                w.commit(version, commands.clone()),
                Err(Failure::RecoveryRequired)
            );
            let recovered = w.recover().unwrap();
            assert!(!w.recovery_required());
            if persisted {
                let Recovery::Applied(done) = recovered else {
                    panic!()
                };
                assert_eq!(done.replies.len(), commands.len());
                assert_eq!(done.version.revision(), version.revision() + 1);
                assert_ne!(rows(&db), old);
                assert_eq!(
                    w.commit(version, commands),
                    Err(Failure::Storage(Error::Conflict))
                );
            } else {
                assert_eq!(recovered, Recovery::NotApplied(version));
                assert_eq!(rows(&db), old);
                assert_eq!(db.stratum(Default::default()).unwrap().info(), old_info);
                commit(&mut w, commands);
            }
            assert!(matches!(w.recover(), Ok(Recovery::Verified(_))));
        }
    }
}
#[test]
fn uncertain_recovery_rejects_valid_but_unexpected_row_substitution() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let p = payment();
    let version = w.version().unwrap();
    assert!(
        w.commit_with(version, vec![Command::Payment(p.clone())], |db, batch| {
            db.db.write_opt(batch, &write_options()).map_err(backend)?;
            Err(Error::Backend("lost acknowledgement".into()))
        })
        .is_err()
    );
    let mut wrong = p.clone();
    wrong.total = 101.into();
    let raw = Record::Payment(wrong).encode(Default::default()).unwrap();
    let original = Record::Payment(p).encode(Default::default()).unwrap();
    assert_eq!(raw.len(), original.len());
    let k = key(&core::payment_key((71, 0)));
    db.db.put(&k, raw).unwrap();
    assert_eq!(w.recover(), Err(Failure::Storage(Error::Conflict)));
    assert!(w.recovery_required());
    db.db.put(k, original).unwrap();
    assert!(matches!(w.recover(), Ok(Recovery::Applied(_))));
}
#[test]
fn quotas_failed_open_and_exhausted_counters_do_not_consume_keys() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(
        db.clone(),
        Limits {
            records: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let version = w.version().unwrap();
    assert!(w.commit(version, vec![share(1)]).is_err());
    assert_eq!(w.version().unwrap(), version);
    assert_eq!(
        db.stratum(Default::default()).unwrap().info(),
        Info::default()
    );
    commit(&mut w, vec![Command::Round]);
    drop(w);
    assert!(matches!(
        Writer::start(
            db.clone(),
            Limits {
                bytes: 1,
                ..Default::default()
            }
        ),
        Err(Failure::Storage(Error::ResourceLimit))
    ));
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    commit(&mut w, vec![share(1)]);
    assert_eq!(
        db.stratum(Default::default()).unwrap().info().next_key,
        Some(u64::MAX - 2)
    );
    drop(w);
    let mut info = db.stratum(Default::default()).unwrap().info();
    info.next_key = Some(0);
    db.db.put(METADATA, info.encode()).unwrap();
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let done = commit(&mut w, vec![Command::Round]);
    assert_eq!(
        done.replies,
        [core::Reply::Record(Box::new(Record::Round(0)))]
    );
    let before = rows(&db);
    assert!(
        w.commit(w.version().unwrap(), vec![Command::Round])
            .is_err()
    );
    assert_eq!(rows(&db), before);
    drop(w);
    let mut info = db.stratum(Default::default()).unwrap().info();
    info.revision = u64::MAX;
    db.db.put(METADATA, info.encode()).unwrap();
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    assert!(
        w.commit(w.version().unwrap(), vec![Command::Payment(payment())])
            .is_err()
    );
    assert_eq!(rows(&db), before);
}
#[test]
fn opening_rejects_missing_totals_malformed_records_and_unallocated_keys() {
    for corruption in [
        "missing_metadata",
        "count",
        "bytes",
        "next",
        "record",
        "key",
        "unknown_table",
        "orphan_metadata",
    ] {
        let t = Temp::new();
        let db = database(&t);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        commit(&mut w, vec![Command::Round, share(1)]);
        drop(w);
        let mut info = db.stratum(Default::default()).unwrap().info();
        match corruption {
            "missing_metadata" => db.db.delete(METADATA).unwrap(),
            "count" => {
                info.counts[0] += 1;
                db.db.put(METADATA, info.encode()).unwrap();
            }
            "bytes" => {
                info.bytes += 1;
                db.db.put(METADATA, info.encode()).unwrap();
            }
            "next" => {
                info.next_key = Some(u64::MAX);
                db.db.put(METADATA, info.encode()).unwrap();
            }
            "record" => db
                .db
                .put(
                    key(&core::key(Table::Shares, (u64::MAX - 1).to_be_bytes())),
                    [0],
                )
                .unwrap(),
            "key" => {
                let r = Record::Round(u64::MAX);
                db.db
                    .put(
                        key(&core::key(Table::Rounds, 7u64.to_be_bytes())),
                        r.encode(Default::default()).unwrap(),
                    )
                    .unwrap();
            }
            "unknown_table" => db.db.put([RECORDS, 99], [0]).unwrap(),
            "orphan_metadata" => {
                for r in rows(&db) {
                    db.db.delete(key(&r.key())).unwrap();
                }
            }
            _ => unreachable!(),
        }
        drop(db);
        assert!(Database::open(&t.0, identity()).is_err(), "{corruption}");
    }
}
#[test]
fn legacy_schema_upgrade_keeps_chain_identity_roots_and_operational_metadata() {
    for with_accounting in [false, true] {
        let t = Temp::new();
        let db = database(&t);
        db.commit(plan(&db)).unwrap();
        let chain = db.view().unwrap().status();
        let roots = db
            .view()
            .unwrap()
            .require_record(chain.tip.unwrap())
            .unwrap()
            .roots;
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        if with_accounting {
            commit(&mut w, vec![Command::Round, share(1)]);
        }
        drop(w);
        let old_rows = rows(&db);
        let info = db.stratum(Default::default()).unwrap().info();
        let Value::List(mut fields) = Value::decode(&db.identity.encode()).unwrap() else {
            panic!()
        };
        assert_eq!(fields[0], number(21));
        fields[0] = number(14);
        let legacy = Value::List(fields).encode();
        assert_eq!(repo::Identity::decode(&legacy).unwrap(), db.identity);
        let binding = db.identity.binding();
        db.db.put(repo::IDENTITY, legacy).unwrap();
        drop(db);
        let db = database(&t);
        assert_eq!(
            db.db.get(repo::IDENTITY).unwrap().unwrap(),
            db.identity.encode()
        );
        assert_eq!(db.identity.binding(), binding);
        assert_eq!(rows(&db), old_rows);
        assert_eq!(db.stratum(Default::default()).unwrap().info(), info);
        assert_eq!(db.view().unwrap().status(), chain);
        assert_eq!(
            db.view()
                .unwrap()
                .require_record(chain.tip.unwrap())
                .unwrap()
                .roots,
            roots
        );
    }
}
#[test]
fn reverse_scans_pages_and_read_budgets_match_fixed_snapshots() {
    use ae_stratum::accounting::Read;
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    commit(&mut w, (0..300).map(|_| Command::Round).collect());
    let snap = db.stratum(Default::default()).unwrap();
    commit(&mut w, vec![Command::Clear(Table::Rounds)]);
    for reverse in [false, true] {
        for n in [1, 17, 128, 300, 301] {
            let result = snap
                .scan(&Table::Rounds.prefix(), &Table::Rounds.end(), n, reverse)
                .unwrap();
            let mut expected: Vec<_> = (u64::MAX - 299..=u64::MAX).collect();
            if reverse {
                expected.reverse();
            }
            assert_eq!(
                result
                    .entries
                    .iter()
                    .map(|(k, v)| Record::decode(k, v, Default::default()).unwrap())
                    .collect::<Vec<_>>(),
                expected
                    .iter()
                    .take(n)
                    .map(|k| Record::Round(*k))
                    .collect::<Vec<_>>()
            );
            assert_eq!(result.more, n < 300);
        }
    }
    let bounded = Stored {
        source: snap.source.clone(),
        info: snap.info,
        limits: Limits {
            transaction: core::Limits {
                bytes: 10,
                ..Default::default()
            },
            ..Default::default()
        },
    };
    assert_eq!(
        bounded.scan(&Table::Rounds.prefix(), &Table::Rounds.end(), 1, false),
        Err(core::Error::Limit)
    );
    assert!(rows(&db).is_empty());
    assert_eq!(
        snap.query(|tx| tx.records(Table::Rounds)).unwrap().len(),
        300
    );
}

#[test]
fn replaced_uncertain_owner_verifies_state_without_fabricating_an_old_receipt() {
    for persisted in [false, true] {
        let t = Temp::new();
        let db = database(&t);
        let mut old = Writer::start(db.clone(), Default::default()).unwrap();
        let version = old.version().unwrap();
        assert!(
            old.commit_with(version, vec![Command::Round, share(1)], |db, batch| {
                if persisted {
                    db.db.write_opt(batch, &write_options()).map_err(backend)?;
                }
                Err(Error::Backend("owner lost acknowledgement".into()))
            })
            .is_err()
        );
        assert!(old.recovery_required());
        drop(old);
        let mut replacement = Writer::start(db.clone(), Default::default()).unwrap();
        // A replacement can prove durable contents, but it has no retained
        // request identity. It must not synthesize an Applied receipt.
        let current = replacement.version().unwrap();
        assert_eq!(replacement.recover().unwrap(), Recovery::Verified(current));
        assert_eq!(current.revision(), u64::from(persisted));
        assert_eq!(rows(&db).len(), if persisted { 3 } else { 0 });
        if persisted {
            assert_eq!(
                replacement.commit(version, vec![share(1)]),
                Err(Failure::Storage(Error::Conflict))
            );
        }
    }
}

#[test]
fn explicit_table_clearing_keeps_native_dangling_indices_and_never_reuses_keys() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    commit(
        &mut w,
        vec![Command::Round, share(1), Command::Clear(Table::Shares)],
    );
    let info = db.stratum(Default::default()).unwrap().info();
    assert_eq!(info.counts, [1, 0, 1, 0, 0, 0]);
    drop(w);
    drop(db);
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    assert_eq!(
        w.commit(
            w.version().unwrap(),
            vec![Command::Solution {
                candidate: hash(1),
                block: hash(11)
            }]
        ),
        Err(Failure::Accounting(core::Error::MissingShare))
    );
    assert_eq!(db.stratum(Default::default()).unwrap().info(), info);
    commit(&mut w, Table::ALL.into_iter().map(Command::Clear).collect());
    assert!(rows(&db).is_empty());
    assert_eq!(
        db.stratum(Default::default()).unwrap().info().next_key,
        info.next_key
    );
    let done = commit(&mut w, vec![Command::Round]);
    assert_eq!(
        done.replies,
        [core::Reply::Record(Box::new(Record::Round(
            info.next_key.unwrap()
        )))]
    );
}
