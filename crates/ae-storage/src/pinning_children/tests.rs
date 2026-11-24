use super::*;
use crate::tests::{Temp, identity, plan};
use ae_crypto::Ed25519SecretKey;
mod recovery;
fn database(t: &Temp) -> Arc<Database> {
    Arc::new(Database::open(&t.0, identity()).unwrap())
}
fn record(n: u8) -> Record {
    let signer = Ed25519SecretKey::from_seed(&[n; 32]);
    let leader = signer.public_key();
    let keys = ae_hyperchain::signing::Keys::new([signer]);
    let intent = Intent {
        network: identity().network,
        parent_chain: Chain::Aeternity,
        parent_network: b"parent".to_vec(),
        leader,
        kind: Kind::Note,
        parent_hash: vec![n; 32],
    };
    let tx = ae_hyperchain::pinning::note(ae_hyperchain::pinning::TransactionInput {
        leader,
        nonce: 1.into(),
        minimum_gas_price: 1_000_000_000u64.into(),
        parent_hash: &intent.parent_hash,
        network: &intent.network,
        keys: &keys,
        limits: Default::default(),
    })
    .unwrap();
    Record {
        intent,
        transaction: tx.encode(),
        receipt: None,
    }
}
fn receipt() -> Receipt {
    Receipt::Rejected("native policy rejection".into())
}
fn rows(db: &Database) -> Vec<Record> {
    db.pinning_children()
        .unwrap()
        .records(Default::default())
        .unwrap()
}
#[test]
fn child_intents_are_bound_signed_and_deduplicate_before_new_nonce_or_fee() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let first = record(1);
    let snapshot = db.pinning_children().unwrap();
    assert_eq!(
        w.reserve(first.clone()).unwrap(),
        Reservation::New(first.clone())
    );
    assert!(snapshot.records(Default::default()).unwrap().is_empty());
    let mut tx = first.signed().unwrap().transaction().clone();
    let ae_tx::TransactionLeaf::Spend(mut spend) = tx.view().leaf().clone() else {
        panic!()
    };
    spend.set_nonce(42.into());
    spend.set_fee(99.into());
    tx = ae_tx::Transaction::new(ae_tx::TransactionLeaf::Spend(spend));
    use ae_tx::CanonicalTransaction;
    let mut msg = first.intent.network.clone();
    msg.extend(tx.encode());
    let key = Ed25519SecretKey::from_seed(&[1; 32]);
    let mut changed = first.clone();
    changed.transaction = ae_tx::SignedTransaction::new(tx, vec![key.sign(&msg)]).encode();
    assert_eq!(
        w.reserve(changed).unwrap(),
        Reservation::Existing(first.clone())
    );
    for n in 0..5 {
        let mut other = first.intent.clone();
        match n {
            0 => other.network.push(1),
            1 => other.parent_chain = Chain::Dogecoin,
            2 => other.parent_network.push(1),
            3 => other.kind = Kind::Proof([7; 32]),
            _ => other.parent_hash.push(1),
        };
        assert_ne!(other.id(), first.intent.id());
    }
    let mut wrong = record(2);
    wrong.intent.leader = first.intent.leader;
    assert!(w.reserve(wrong).is_err());
    let mut wrong = record(2);
    wrong.transaction[20] ^= 1;
    assert!(w.reserve(wrong).is_err());
    let mut wrong = record(2);
    wrong.intent.network.push(1);
    assert!(w.reserve(wrong).is_err());
    assert_eq!(rows(&db), vec![first]);
    assert_eq!(db.view().unwrap().status(), Status::default());
}
#[test]
fn exact_receipts_are_monotonic_and_reopen_without_retaining_closed_owner() {
    let t = Temp::new();
    let db = database(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    assert!(matches!(
        Writer::start(db.clone(), Default::default()),
        Err(Failure::AlreadyRunning)
    ));
    let first = record(1);
    w.reserve(first.clone()).unwrap();
    let snap = db.pinning_children().unwrap();
    assert!(
        w.record_receipt(&first, Receipt::Rejected(String::new()))
            .is_err()
    );
    assert!(
        w.record_receipt(
            &first,
            Receipt::Submitted(pool::Submission {
                outcome: ae_mempool::pool::PushOutcome::Cached,
                chain: Status {
                    revision: 0,
                    tip: Some(Hash256::ZERO)
                },
                revision: 0
            })
        )
        .is_err()
    );
    assert!(
        w.record_receipt(
            &first,
            Receipt::Submitted(pool::Submission {
                outcome: ae_mempool::pool::PushOutcome::Accepted {
                    hash: Hash256::ZERO,
                    inserted: true,
                    event: ae_mempool::pool::Event::Received
                },
                chain: Status::default(),
                revision: 1
            })
        )
        .is_err()
    );
    let done = w.record_receipt(&first, receipt()).unwrap();
    assert_eq!(w.record_receipt(&done, receipt()).unwrap(), done);
    assert!(
        w.record_receipt(&first, Receipt::Rejected("different".into()))
            .is_err()
    );
    assert_eq!(
        snap.records(Default::default()).unwrap(),
        vec![first.clone()]
    );
    drop(snap);
    drop(w);
    drop(db);
    let db = database(&t);
    let mut w = Writer::start(db, Default::default()).unwrap();
    assert_eq!(
        w.reserve(first).unwrap(),
        Reservation::Existing(done.clone())
    );
    assert_eq!(w.observe(&done).unwrap(), Some(done));
}
#[test]
fn ambiguous_child_writes_quarantine_until_validated_recovery() {
    for op in ["reserve", "receipt"] {
        for persisted in [false, true] {
            let t = Temp::new();
            let db = database(&t);
            let mut w = Writer::start(db.clone(), Default::default()).unwrap();
            let first = record(1);
            if op == "receipt" {
                w.reserve(first.clone()).unwrap();
            }
            let write = |db: &Database, batch| {
                if persisted {
                    db.db.write_opt(batch, &write_options()).map_err(backend)?;
                }
                Err(Error::Backend("ambiguous child write".into()))
            };
            let result = if op == "reserve" {
                w.reserve_with(first.clone(), write).map(|_| ())
            } else {
                w.receipt_with(&first, receipt(), write).map(|_| ())
            };
            assert!(result.is_err());
            assert!(w.recovery_required());
            assert_eq!(w.get(&first.intent), Err(Failure::RecoveryRequired));
            assert_eq!(w.reserve(first.clone()), Err(Failure::RecoveryRequired));
            let actual = w.recover().unwrap();
            assert!(!w.recovery_required());
            let expected = if op == "reserve" && !persisted {
                vec![]
            } else {
                let mut r = first;
                if op == "receipt" && persisted {
                    r.receipt = Some(receipt());
                }
                vec![r]
            };
            assert_eq!(actual, expected);
        }
    }
}
#[test]
fn observation_uses_exact_stored_bytes_and_preserves_absent_or_deleted_status() {
    for member in [false, true] {
        let t = Temp::new();
        let db = database(&t);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let first = record(1);
        w.reserve(first.clone()).unwrap();
        assert_eq!(w.observe(&first).unwrap(), None);
        assert!(rows(&db)[0].receipt.is_none());
        let tx = first.signed().unwrap();
        let mut batch = WriteBatch::default();
        batch.put(
            repo::key(pool::RECORDS, tx.hash().0),
            ae_tx::storage::encode(&tx, db.config.transaction_record()).unwrap(),
        );
        batch.put(
            pool::REVISION,
            Value::List(vec![number(1), number(1)]).encode(),
        );
        if member {
            batch.put(repo::key(pool::MEMBERS, tx.hash().0), [1]);
        }
        db.db.write_opt(batch, &write_options()).unwrap();
        let done = w.observe(&first).unwrap().unwrap();
        assert_eq!(
            done.receipt,
            Some(Receipt::Observed {
                location: if member {
                    Location::Pool
                } else {
                    Location::Deleted
                },
                chain: Status::default(),
                revision: 1
            })
        );
        assert_eq!(done.transaction, first.transaction);
        drop(w);
        drop(db);
        let db = database(&t);
        assert_eq!(rows(&db), vec![done]);
    }
}
#[test]
fn bounds_fail_without_evicting_unresolved_children() {
    let t = Temp::new();
    let db = database(&t);
    let first = record(1);
    let size = first.encode(MAX_TRANSACTION).unwrap().len() + 33;
    let mut w = Writer::start(
        db.clone(),
        Limits {
            records: 1,
            bytes: size,
            transaction_bytes: 4096,
        },
    )
    .unwrap();
    w.reserve(first.clone()).unwrap();
    assert!(w.reserve(record(2)).is_err());
    assert!(w.record_receipt(&first, receipt()).is_err());
    assert_eq!(rows(&db), vec![first]);
    assert!(
        db.pinning_children()
            .unwrap()
            .records(Limits {
                records: 1,
                bytes: 1,
                transaction_bytes: 4096
            })
            .is_err()
    );
}
#[test]
fn schema_twelve_upgrades_preserving_roots_and_child_journal() {
    for with_children in [false, true] {
        let t = Temp::new();
        let db = database(&t);
        let id = identity();
        let update = plan(&db);
        db.commit(update).unwrap();
        let genesis = id.genesis;
        let roots = db.view().unwrap().require_record(genesis).unwrap().roots;
        if with_children {
            Writer::start(db.clone(), Default::default())
                .unwrap()
                .reserve(record(1))
                .unwrap();
        }
        let expected = rows(&db);
        let status = db.view().unwrap().status();
        let Value::List(mut fields) = Value::decode(&id.encode()).unwrap() else {
            panic!()
        };
        assert_eq!(fields[0], number(15));
        fields[0] = number(12);
        db.db
            .put(repo::IDENTITY, Value::List(fields).encode())
            .unwrap();
        drop(db);
        let db = database(&t);
        assert_eq!(db.view().unwrap().status(), status);
        assert_eq!(
            db.view().unwrap().require_record(genesis).unwrap().roots,
            roots
        );
        assert_eq!(rows(&db), expected);
        assert_eq!(db.db.get(repo::IDENTITY).unwrap(), Some(id.encode()));
    }
}
#[test]
fn corrupted_child_records_and_metadata_block_schema_upgrade() {
    for mode in 0..5 {
        let t = Temp::new();
        let db = database(&t);
        let first = record(1);
        Writer::start(db.clone(), Default::default())
            .unwrap()
            .reserve(first.clone())
            .unwrap();
        let key = repo::key(RECORDS, first.intent.id().unwrap());
        match mode {
            0 => db.db.delete(METADATA).unwrap(),
            1 => {
                let mut m = Metadata::decode(db.db.get(METADATA).unwrap()).unwrap();
                m.bytes += 1;
                db.db.put(METADATA, m.encode()).unwrap();
            }
            2 => {
                let Value::List(mut v) = Value::decode(&db.db.get(&key).unwrap().unwrap()).unwrap()
                else {
                    panic!()
                };
                v[0] = number(2);
                db.db.put(&key, Value::List(v).encode()).unwrap();
            }
            3 => {
                db.db
                    .put(
                        repo::key(RECORDS, Hash256([8; 32])),
                        db.db.get(&key).unwrap().unwrap(),
                    )
                    .unwrap();
                db.db.delete(&key).unwrap();
            }
            _ => {
                let Value::List(mut v) = Value::decode(&db.db.get(&key).unwrap().unwrap()).unwrap()
                else {
                    panic!()
                };
                v[7] = bytes(b"unsigned");
                db.db.put(&key, Value::List(v).encode()).unwrap();
            }
        }
        let Value::List(mut v) = Value::decode(&identity().encode()).unwrap() else {
            panic!()
        };
        v[0] = number(12);
        let old = Value::List(v).encode();
        db.db.put(repo::IDENTITY, &old).unwrap();
        drop(db);
        assert!(Database::open(&t.0, identity()).is_err());
        let raw = DB::open_default(&t.0).unwrap();
        assert_eq!(raw.get(repo::IDENTITY).unwrap(), Some(old));
    }
}
