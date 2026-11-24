use super::*;
use crate::tests::{Temp, identity};
use ae_stratum::{ingress::Solution, nonce::Nonce};
fn input(solution: bool) -> Input {
    Input {
        account: "ak_worker".into(),
        target: 7.into(),
        hash: Hash256([3; 32]),
        solution: solution.then(|| Solution {
            nonce: Nonce(91),
            proof: Box::new(std::array::from_fn(|n| n as u64)),
        }),
    }
}
fn open(t: &Temp) -> Arc<Database> {
    Arc::new(Database::open(&t.0, identity()).unwrap())
}
type State = (
    Info,
    Vec<core::Record>,
    u64,
    u64,
    Option<Receipt>,
    Vec<Receipt>,
);
fn state(db: &Database) -> State {
    let s = db.stratum(Default::default()).unwrap();
    let i = s.ingress(Default::default()).unwrap();
    (
        s.info(),
        s.query(|tx| tx.range(&[1], &[7], 100, false)).unwrap().0,
        i.epoch(),
        i.sequence(),
        i.last().cloned(),
        i.pending(None, 100).unwrap().0,
    )
}
#[test]
fn receipts_bind_epoch_sequence_exact_payload_and_single_allocation_across_reopening() {
    let t = Temp::new();
    let db = open(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let e = w.begin_ingress(Default::default()).unwrap();
    assert_eq!(e.value(), 1);
    let r = w.accept(&e, 1, input(true), Default::default()).unwrap();
    let original = state(&db);
    assert_eq!(r.share_key, u64::MAX);
    assert_eq!(original.0.counts, [1, 1, 0, 0, 0, 0]);
    assert_eq!(original.5.as_slice(), std::slice::from_ref(&r));
    assert_eq!(w.accept(&e, 1, input(true), Default::default()).unwrap(), r);
    assert_eq!(state(&db), original);
    let mut wrong = input(true);
    wrong.target = 8.into();
    assert!(w.accept(&e, 1, wrong, Default::default()).is_err());
    assert!(w.accept(&e, 3, input(false), Default::default()).is_err());
    assert_eq!(state(&db), original);
    let pinned = db
        .stratum(Default::default())
        .unwrap()
        .ingress(Default::default())
        .unwrap();
    let second = w.accept(&e, 2, input(false), Default::default()).unwrap();
    assert_eq!(second.share_key, u64::MAX - 1);
    assert_eq!(state(&db).0.counts, [1, 2, 0, 0, 0, 0]);
    assert_eq!(pinned.last(), Some(&r));
    assert!(w.accept(&e, 1, input(true), Default::default()).is_err());
    let newer = w.begin_ingress(Default::default()).unwrap();
    assert_eq!(newer.value(), 2);
    assert!(w.accept(&e, 3, input(false), Default::default()).is_err());
    let third = w
        .accept(&newer, 1, input(true), Default::default())
        .unwrap();
    assert_eq!(third.share_key, u64::MAX - 2);
    let (page, more) = db
        .stratum(Default::default())
        .unwrap()
        .ingress(Default::default())
        .unwrap()
        .pending(None, 1)
        .unwrap();
    assert_eq!(page, [r]);
    assert!(more);
    let (page, more) = db
        .stratum(Default::default())
        .unwrap()
        .ingress(Default::default())
        .unwrap()
        .pending(Some(page[0].id), 1)
        .unwrap();
    assert_eq!(page, [third]);
    assert!(!more);
    let final_state = state(&db);
    drop(pinned);
    drop(w);
    drop(db);
    let db = open(&t);
    assert_eq!(state(&db), final_state);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    assert!(
        w.accept(&newer, 2, input(false), Default::default())
            .is_err()
    );
    let current = w.begin_ingress(Default::default()).unwrap();
    assert_eq!(current.value(), 3);
    assert_eq!(
        w.accept(&current, 1, input(false), Default::default())
            .unwrap()
            .share_key,
        u64::MAX - 3
    );
    let foreign = Temp::new();
    let foreign = open(&foreign);
    let mut other = Writer::start(foreign, Default::default()).unwrap();
    let _ = other.begin_ingress(Default::default()).unwrap();
    assert!(
        other
            .accept(&current, 2, input(false), Default::default())
            .is_err()
    );
}
#[test]
fn uncertain_receipted_batches_reconcile_exact_metadata_share_and_solution_without_reallocation() {
    for persist in [false, true] {
        for solution in [false, true] {
            let t = Temp::new();
            let db = open(&t);
            let mut w = Writer::start(db.clone(), Default::default()).unwrap();
            let e = w.begin_ingress(Default::default()).unwrap();
            let old = state(&db);
            let error = w.accept_with(&e, 1, input(solution), Default::default(), |db, b| {
                if persist {
                    db.db.write_opt(b, &write_options()).map_err(backend)?;
                }
                Err(Error::Backend("lost ingress acknowledgement".into()))
            });
            assert!(error.is_err());
            assert!(w.recovery_required());
            assert_eq!(
                w.accept(&e, 1, input(solution), Default::default()),
                Err(Failure::RecoveryRequired)
            );
            let recovered = w.recover().unwrap();
            assert_eq!(matches!(recovered, Recovery::Applied(_)), persist);
            if !persist {
                assert_eq!(state(&db), old);
            }
            let receipt = w
                .accept(&e, 1, input(solution), Default::default())
                .unwrap();
            assert_eq!(receipt.share_key, u64::MAX);
            let saved = state(&db);
            assert_eq!(
                w.accept(&e, 1, input(solution), Default::default())
                    .unwrap(),
                receipt
            );
            assert_eq!(state(&db), saved);
            assert_eq!(saved.0.counts, [1, 1, 0, 0, 0, 0]);
            assert_eq!(saved.5.len(), usize::from(solution));
            drop(w);
            drop(db);
            assert_eq!(state(&open(&t)), saved);
        }
    }
    for persist in [false, true] {
        let t = Temp::new();
        let db = open(&t);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        assert!(
            w.begin_ingress_with(Default::default(), |db, b| {
                if persist {
                    db.db.write_opt(b, &write_options()).map_err(backend)?;
                }
                Err(Error::Backend("epoch reply lost".into()))
            })
            .is_err()
        );
        assert!(matches!(w.recover().unwrap(), Recovery::Applied(_)) == persist);
        assert_eq!(
            w.begin_ingress(Default::default()).unwrap().value(),
            if persist { 2 } else { 1 }
        );
    }
}
#[test]
fn receipt_reconciliation_detects_valid_same_size_metadata_substitution() {
    let t = Temp::new();
    let db = open(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let e = w.begin_ingress(Default::default()).unwrap();
    assert!(
        w.accept_with(&e, 1, input(false), Default::default(), |db, b| {
            db.db.write_opt(b, &write_options()).map_err(backend)?;
            Err(Error::Backend("lost receipt".into()))
        })
        .is_err()
    );
    let raw = db.db.get(META).unwrap().unwrap();
    let mut m = Metadata::decode(Some(&raw)).unwrap();
    m.last.as_mut().unwrap().input.target = 8.into();
    let wrong = m.encode().unwrap();
    assert_eq!(raw.len(), wrong.len());
    db.db.put(META, wrong).unwrap();
    assert_eq!(w.recover(), Err(Failure::Storage(Error::Conflict)));
    assert!(w.recovery_required());
    db.db.put(META, raw).unwrap();
    assert!(matches!(w.recover().unwrap(), Recovery::Applied(_)));
}
#[test]
fn solution_quotas_epoch_exhaustion_and_bad_inputs_leave_ordered_prefix_retryable() {
    let t = Temp::new();
    let db = open(&t);
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let limits = Limits {
        solutions: 1,
        ..Default::default()
    };
    let e = w.begin_ingress(limits).unwrap();
    w.accept(&e, 1, input(true), limits).unwrap();
    let original = state(&db);
    assert!(w.accept(&e, 2, input(true), limits).is_err());
    assert_eq!(state(&db), original);
    let mut negative = input(false);
    negative.target = (-1).into();
    assert!(w.accept(&e, 2, negative, limits).is_err());
    assert_eq!(state(&db), original);
    let mut bad = input(false);
    bad.account = "x".repeat(1025);
    assert!(w.accept(&e, 2, bad, limits).is_err());
    assert_eq!(state(&db), original);
    assert!(
        w.accept(
            &e,
            2,
            input(false),
            Limits {
                solutions: 1,
                bytes: 1
            }
        )
        .is_err()
    );
    assert_eq!(state(&db), original);
    assert_eq!(
        w.accept(&e, 2, input(false), limits).unwrap().share_key,
        u64::MAX - 1
    );
    let stored = db.stratum(Default::default()).unwrap();
    let inbox = stored.ingress(limits).unwrap();
    let mut m = inbox.metadata.clone();
    m.epoch = u64::MAX;
    m.last = None;
    db.db.put(META, m.encode().unwrap()).unwrap();
    drop(inbox);
    drop(stored);
    let before = state(&db);
    assert!(w.begin_ingress(limits).is_err());
    assert_eq!(state(&db), before);
}
#[test]
fn opening_rejects_orphan_receipts_wrong_totals_and_disagreeing_solution_payloads() {
    for corruption in 0..8 {
        let t = Temp::new();
        let db = open(&t);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let e = w.begin_ingress(Default::default()).unwrap();
        let r = w.accept(&e, 1, input(true), Default::default()).unwrap();
        drop(w);
        let raw = db.db.get(META).unwrap().unwrap();
        let mut m = Metadata::decode(Some(&raw)).unwrap();
        match corruption {
            0 => db.db.delete(META).unwrap(),
            1 => {
                m.solutions = 2;
                db.db.put(META, m.encode().unwrap()).unwrap();
            }
            2 => {
                m.bytes += 1;
                db.db.put(META, m.encode().unwrap()).unwrap();
            }
            3 => {
                m.last.as_mut().unwrap().input.target = 8.into();
                db.db.put(META, m.encode().unwrap()).unwrap();
            }
            4 => db.db.put(r.key(), [0xc0]).unwrap(),
            5 => db.db.delete(r.key()).unwrap(),
            6 => {
                let mut wrong = r.clone();
                wrong.share_key = 0;
                db.db.put(r.key(), wrong.encode().unwrap()).unwrap();
            }
            _ => {
                let mut k = r.key();
                k.push(0);
                db.db.delete(r.key()).unwrap();
                db.db.put(k, r.encode().unwrap()).unwrap();
            }
        }
        let marker = db.db.get(ae_chain::repository::IDENTITY).unwrap();
        drop(db);
        assert!(Database::open(&t.0, identity()).is_err());
        let raw = rocksdb::DB::open_default(&t.0).unwrap();
        assert_eq!(raw.get(ae_chain::repository::IDENTITY).unwrap(), marker);
    }
}
#[test]
fn pending_solution_pages_and_old_schema_upgrade_preserve_identity_and_snapshot_roots() {
    let t = Temp::new();
    let db = open(&t);
    let id = db.identity.clone();
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let e = w.begin_ingress(Default::default()).unwrap();
    for n in 1..=9 {
        w.accept(&e, n, input(n % 2 == 0), Default::default())
            .unwrap();
    }
    let saved = state(&db);
    let s = db.stratum(Default::default()).unwrap();
    let i = s.ingress(Default::default()).unwrap();
    assert!(!i.pending(None, 4).unwrap().1);
    assert!(i.pending(None, 3).unwrap().1);
    assert!(i.pending(None, 0).is_err());
    let low = s
        .ingress(Limits {
            solutions: 100,
            bytes: i.metadata.bytes as usize,
        })
        .unwrap();
    assert_eq!(low.pending(None, 100).unwrap().0.len(), 4);
    let Value::List(mut fields) = Value::decode(&id.encode()).unwrap() else {
        panic!()
    };
    fields[0] = number(15);
    db.db
        .put(ae_chain::repository::IDENTITY, Value::List(fields).encode())
        .unwrap();
    drop(low);
    drop(i);
    drop(s);
    drop(w);
    drop(db);
    let db = open(&t);
    assert_eq!(db.identity, id);
    assert_eq!(state(&db), saved);
    let Value::List(fields) =
        Value::decode(&db.db.get(ae_chain::repository::IDENTITY).unwrap().unwrap()).unwrap()
    else {
        panic!()
    };
    assert_eq!(fields[0], number(19));
}
mod recovery;
