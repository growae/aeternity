use super::*;
use crate::tests::{Temp, identity, plan};
use ae_stratum::{ingress::Solution, nonce::Nonce};
#[path = "../../../../ae-sync/tests/common/mining_metrics.rs"]
mod mining_metrics;
use mining_metrics::MiningMetrics;
fn open(t: &Temp) -> Arc<Database> {
    let db = Arc::new(Database::open(&t.0, identity()).unwrap());
    if db.view().unwrap().status().tip.is_none() {
        db.commit(plan(&db)).unwrap();
    }
    db
}
fn candidate() -> Candidate {
    let data: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../ae-stratum/tests/fixtures/erlang_stratum_candidates.json"
    ))
    .unwrap();
    let row = data["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == "ae_uat_1")
        .unwrap();
    Candidate::decode(&hex::decode(row["sealing"].as_str().unwrap()).unwrap()).unwrap()
}
fn input() -> Input {
    let data: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../ae-stratum/tests/fixtures/erlang_stratum_candidates.json"
    ))
    .unwrap();
    let row = data["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == "ae_uat_1")
        .unwrap();
    Input {
        account: "ak_miner".into(),
        target: 11.into(),
        hash: candidate().hash(),
        solution: Some(Solution {
            nonce: Nonce(row["nonce"].as_u64().unwrap()),
            proof: Box::new(std::array::from_fn(|i| row["proof"][i].as_u64().unwrap())),
        }),
    }
}
fn date() -> core::Date {
    core::Date([2026, 11, 24, 0, 0, 0])
}
fn writer(db: &Arc<Database>) -> Writer {
    Writer::start(db.clone(), Default::default()).unwrap()
}
fn record(db: &Database, id: Id) -> Record {
    db.stratum(Default::default())
        .unwrap()
        .mining(Default::default())
        .unwrap()
        .get(id)
        .unwrap()
        .unwrap()
}
fn rows(db: &Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.db
        .iterator(rocksdb::IteratorMode::Start)
        .map(|r| r.unwrap())
        .filter(|(k, _)| {
            k.starts_with(b"\0stratum")
                || matches!(k.first(), Some(22..=25))
                || k.as_ref() == repo::STATUS
                || k.as_ref() == repo::IDENTITY
        })
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect()
}
fn reject() -> Outcome {
    Outcome::Rejected {
        reason: Rejection::Invalid,
        detail: "explicit invalid seal test".into(),
    }
}
fn setup(w: &mut Writer) -> (Epoch, ingress::Epoch) {
    let e = w.begin_mining(Default::default()).unwrap();
    let i = w.begin_ingress(Default::default()).unwrap();
    assert!(
        w.publish_work(&e, 1, candidate(), date(), Default::default())
            .unwrap()
            .published
    );
    (e, i)
}
fn accept(w: &mut Writer, e: &Epoch, i: &ingress::Epoch, n: u64) -> Receipt {
    w.accept_mining(i, n, input(), Default::default(), e, Default::default())
        .unwrap()
}
#[test]
fn atomic_receipts_convert_the_current_share_and_consume_all_active_candidates() {
    let t = Temp::new();
    let db = open(&t);
    let (metrics, _binding) = MiningMetrics::bind(db.instrumentation());
    let status = db.view().unwrap().status();
    let mut w = writer(&db);
    let (e, i) = setup(&mut w);
    let p = w
        .publish_work(&e, 2, candidate(), date(), Default::default())
        .unwrap();
    let before = rows(&db);
    assert_eq!(
        w.publish_work(&e, 2, candidate(), date(), Default::default())
            .unwrap(),
        p
    );
    assert_eq!(rows(&db), before);
    assert_eq!(
        db.stratum(Default::default())
            .unwrap()
            .mining(Default::default())
            .unwrap()
            .info()
            .active
            .len(),
        2
    );
    let mut share = input();
    share.solution = None;
    let earlier = w
        .accept_mining(&i, 1, share, Default::default(), &e, Default::default())
        .unwrap();
    let receipt = accept(&mut w, &e, &i, 2);
    assert_eq!(metrics.counts(), (1, 0));
    let r = record(&db, receipt.id);
    let after = rows(&db);
    assert_eq!(r.state, State::Ready);
    assert_eq!(r.accounted_key, Some(receipt.share_key));
    assert_ne!(r.accounted_key, Some(earlier.share_key));
    assert_eq!(
        w.accept_mining(&i, 2, input(), Default::default(), &e, Default::default())
            .unwrap(),
        receipt
    );
    assert_eq!(rows(&db), after);
    let stored = db.stratum(Default::default()).unwrap();
    let j = stored.mining(Default::default()).unwrap();
    j.verify().unwrap();
    assert!(j.info().active.is_empty());
    assert_eq!(j.info().counts, [1, 0, 0]);
    assert_eq!(
        j.page(Phase::Ready, None, 1).unwrap(),
        (vec![r.clone()], false)
    );
    let hash = r.solution().unwrap().unwrap().hash();
    assert_eq!(
        stored.query(|tx| tx.hash(candidate().hash())).unwrap(),
        None
    );
    assert_eq!(
        stored.query(|tx| tx.hash(hash)).unwrap(),
        Some(receipt.share_key)
    );
    let old = stored
        .query(|tx| {
            tx.range(
                &core::key(core::Table::Shares, earlier.share_key.to_be_bytes()),
                &core::Table::Shares.end(),
                1,
                false,
            )
        })
        .unwrap()
        .0;
    assert!(matches!(&old[0],core::Record::Share(s)if s.hash==candidate().hash()));
    assert_eq!(db.view().unwrap().status(), status);
    assert_eq!(metrics.counts(), (1, 0));
    assert!(metrics.key_info().is_empty());
}
#[test]
fn missing_cache_and_inactive_work_keep_native_accounting_order_and_closeout_replays() {
    let t = Temp::new();
    let db = open(&t);
    let (metrics, _binding) = MiningMetrics::bind(db.instrumentation());
    let mut w = writer(&db);
    let (e, i) = setup(&mut w);
    let v = w.version().unwrap();
    w.commit(
        v,
        vec![Command::DeleteCandidatesBefore(core::Date([
            2027, 1, 1, 0, 0, 0,
        ]))],
    )
    .unwrap();
    let lost = accept(&mut w, &e, &i, 1);
    let lost = record(&db, lost.id);
    assert_eq!(lost.state, State::Done(Outcome::MissingCandidate));
    assert_eq!(metrics.counts(), (0, 0));
    assert!(lost.accounted_key.is_none());
    assert_eq!(
        db.stratum(Default::default())
            .unwrap()
            .mining(Default::default())
            .unwrap()
            .info()
            .active
            .len(),
        1
    );
    w.acknowledge_solution(&lost, Default::default()).unwrap();
    w.acknowledge_solution(&lost, Default::default()).unwrap();
    let stored = db.stratum(Default::default()).unwrap();
    let inbox = stored.ingress(Default::default()).unwrap();
    assert!(inbox.last_closed());
    assert!(inbox.pending(None, 1).unwrap().0.is_empty());
    drop(inbox);
    drop(stored);
    assert_eq!(accept(&mut w, &e, &i, 1), lost.receipt);
    w.publish_work(&e, 2, candidate(), date(), Default::default())
        .unwrap();
    let r = accept(&mut w, &e, &i, 2);
    let r = record(&db, r.id);
    assert!(w.acknowledge_solution(&r, Default::default()).is_err());
    let submitted = w.begin_submission(&r, Default::default()).unwrap();
    assert_eq!(
        w.begin_submission(&r, Default::default()).unwrap(),
        submitted
    );
    assert!(
        w.finish_submission(
            &submitted,
            Outcome::Accepted(Accepted::Present),
            Default::default()
        )
        .is_err()
    );
    let done = w
        .finish_submission(&submitted, reject(), Default::default())
        .unwrap();
    assert_eq!(done.attempts, 1);
    let late = accept(&mut w, &e, &i, 3);
    let late = record(&db, late.id);
    assert_eq!(late.state, State::Done(Outcome::Inactive));
    assert_eq!(metrics.counts(), (1, 0));
    assert_eq!(late.accounted_key, Some(late.receipt.share_key));
    w.acknowledge_solution(&done, Default::default()).unwrap();
    w.acknowledge_solution(&late, Default::default()).unwrap();
    db.stratum(Default::default()).unwrap().verify().unwrap();
    assert_eq!(accept(&mut w, &e, &i, 3), late.receipt);
    assert_eq!(
        db.stratum(Default::default())
            .unwrap()
            .mining(Default::default())
            .unwrap()
            .info()
            .counts,
        [0, 0, 0]
    );
    drop(w);
    drop(db);
    let db = open(&t);
    assert!(
        db.stratum(Default::default())
            .unwrap()
            .ingress(Default::default())
            .unwrap()
            .last_closed()
    );
}
#[test]
fn restarting_retires_active_work_and_delayed_solutions_record_the_actual_indexed_share() {
    let t = Temp::new();
    let db = open(&t);
    let mut w = writer(&db);
    let (e, i) = setup(&mut w);
    let first = w.accept(&i, 1, input(), Default::default()).unwrap();
    let mut share = input();
    share.solution = None;
    let last = w.accept(&i, 2, share, Default::default()).unwrap();
    let Value::List(mut old) = Value::decode(&db.db.get(ingress::META).unwrap().unwrap()).unwrap()
    else {
        panic!()
    };
    old[0] = number(1);
    old.pop();
    db.db.put(ingress::META, Value::List(old).encode()).unwrap();
    let Value::List(mut fields) = Value::decode(&db.identity.encode()).unwrap() else {
        panic!()
    };
    fields[0] = number(16);
    db.db
        .put(repo::IDENTITY, Value::List(fields).encode())
        .unwrap();
    drop(w);
    drop(db);
    let db = open(&t);
    let mut w = writer(&db);
    let newer = w.begin_mining(Default::default()).unwrap();
    assert_eq!(newer.value(), e.value() + 1);
    assert!(
        w.publish_work(&e, 2, candidate(), date(), Default::default())
            .is_err()
    );
    let r = w
        .prepare_solution(&newer, first.id, Default::default())
        .unwrap();
    assert_eq!(r.state, State::Done(Outcome::Inactive));
    assert_eq!(r.accounted_key, Some(last.share_key));
    assert_ne!(r.accounted_key, Some(first.share_key));
    let before = rows(&db);
    assert_eq!(
        w.prepare_solution(&newer, first.id, Default::default())
            .unwrap(),
        r
    );
    assert_eq!(rows(&db), before);
    w.acknowledge_solution(&r, Default::default()).unwrap();
    assert!(
        !db.stratum(Default::default())
            .unwrap()
            .ingress(Default::default())
            .unwrap()
            .last_closed()
    );
    db.stratum(Default::default()).unwrap().verify().unwrap();
}
#[test]
fn stale_publications_limits_and_wrong_attempts_do_not_mutate_existing_work() {
    let t = Temp::new();
    let db = open(&t);
    let mut w = writer(&db);
    let (e, i) = setup(&mut w);
    let mut header = candidate().header().clone();
    header.common.previous_hash = [42; 32];
    let stale = Candidate::new(header).unwrap();
    let p = w
        .publish_work(&e, 2, stale.clone(), date(), Default::default())
        .unwrap();
    assert!(!p.published);
    assert_eq!(
        w.publish_work(&e, 2, stale, date(), Default::default())
            .unwrap(),
        p
    );
    let before = rows(&db);
    let limits = Limits {
        active: 1,
        ..Default::default()
    };
    assert!(w.publish_work(&e, 3, candidate(), date(), limits).is_err());
    assert_eq!(rows(&db), before);
    assert!(
        w.publish_work(&e, 4, candidate(), date(), Default::default())
            .is_err()
    );
    assert_eq!(rows(&db), before);
    let tiny = Limits {
        bytes: 1,
        ..Default::default()
    };
    assert!(
        w.accept_mining(&i, 1, input(), Default::default(), &e, tiny)
            .is_err()
    );
    assert_eq!(rows(&db), before);
    let receipt = accept(&mut w, &e, &i, 1);
    let original = record(&db, receipt.id);
    let running = w.begin_submission(&original, Default::default()).unwrap();
    let retry = w
        .unaccepted_submission(&running, Default::default())
        .unwrap();
    let next = w.begin_submission(&retry, Default::default()).unwrap();
    assert_eq!(next.attempts, 2);
    let before = rows(&db);
    assert!(
        w.finish_submission(&running, reject(), Default::default())
            .is_err()
    );
    assert_eq!(rows(&db), before);
    // This test has no chain admission worker. Joining is therefore vacuous.
    let reconciled = w
        .reconcile_settled_submission(&next, Default::default())
        .unwrap();
    assert_eq!(reconciled.state, State::Ready);
    assert_eq!(reconciled.attempts, 2);
    let before = rows(&db);
    let v = w.version().unwrap();
    assert!(
        w.commit_checked(
            v,
            vec![],
            vec![],
            vec![(repo::STATUS.to_vec(), Some(vec![0]))],
            |_, _| panic!("must not write stale check")
        )
        .is_err()
    );
    assert_eq!(rows(&db), before);
}
#[test]
fn uncertain_atomic_intents_are_reconciled_without_repeating_accounting_or_active_claims() {
    for applied in [false, true] {
        let t = Temp::new();
        let db = open(&t);
        let (metrics, _binding) = MiningMetrics::bind(db.instrumentation());
        let mut w = writer(&db);
        let (e, i) = setup(&mut w);
        let before = rows(&db);
        let error = w.accept_maybe_mining(
            &i,
            1,
            input(),
            Default::default(),
            Some((&e, Default::default())),
            |db, b| {
                if applied {
                    db.db.write_opt(b, &write_options()).unwrap();
                }
                Err(Error::Backend("ambiguous test write".into()))
            },
        );
        assert!(error.is_err());
        assert!(w.recovery_required());
        if applied {
            assert!(matches!(w.recover().unwrap(), Recovery::Applied(_)));
        } else {
            assert!(matches!(w.recover().unwrap(), Recovery::NotApplied(_)));
            assert_eq!(rows(&db), before);
        }
        let receipt = accept(&mut w, &e, &i, 1);
        assert_eq!(metrics.counts(), (u64::from(!applied), 0));
        let r = record(&db, receipt.id);
        let after = rows(&db);
        assert_eq!(r.accounted_key, Some(receipt.share_key));
        assert_eq!(r.state, State::Ready);
        assert_eq!(accept(&mut w, &e, &i, 1), receipt);
        assert_eq!(metrics.counts(), (u64::from(!applied), 0));
        assert_eq!(rows(&db), after);
        let mut running = r.clone();
        running.state = State::Submitting;
        running.attempts = 1;
        assert!(
            w.change(
                &r,
                Some(&running),
                Default::default(),
                vec![],
                vec![],
                |db, b| {
                    db.db.write_opt(b, &write_options()).unwrap();
                    Err(Error::Backend("response lost".into()))
                }
            )
            .is_err()
        );
        assert!(matches!(w.recover().unwrap(), Recovery::Applied(_)));
        assert_eq!(w.begin_submission(&r, Default::default()).unwrap(), running);
        assert_eq!(record(&db, r.receipt.id), running);
        let mut done = running.clone();
        done.state = State::Done(reject());
        assert!(
            w.change(
                &running,
                Some(&done),
                Default::default(),
                vec![],
                vec![],
                |db, b| {
                    db.db.write_opt(b, &write_options()).unwrap();
                    Err(Error::Backend("response lost".into()))
                }
            )
            .is_err()
        );
        w.recover().unwrap();
        assert_eq!(
            w.finish_submission(&running, reject(), Default::default())
                .unwrap(),
            done
        );
        let stored = db.stratum(Default::default()).unwrap();
        let extras = stored
            .ingress(Default::default())
            .unwrap()
            .remove(&done.receipt)
            .unwrap();
        drop(stored);
        assert!(
            w.change(&done, None, Default::default(), vec![], extras, |db, b| {
                db.db.write_opt(b, &write_options()).unwrap();
                Err(Error::Backend("response lost".into()))
            })
            .is_err()
        );
        w.recover().unwrap();
        w.acknowledge_solution(&done, Default::default()).unwrap();
        assert_eq!(accept(&mut w, &e, &i, 1), receipt);
    }
}
#[test]
fn codecs_and_startup_reject_dangling_journal_indices_receipts_and_totals() {
    for corruption in [
        "index_missing",
        "orphan_index",
        "phase",
        "receipt",
        "future_epoch",
        "counts",
        "bytes",
        "record_key",
        "metadata",
    ] {
        let t = Temp::new();
        let db = open(&t);
        let mut w = writer(&db);
        let (e, i) = setup(&mut w);
        let receipt = accept(&mut w, &e, &i, 1);
        let r = record(&db, receipt.id);
        drop(w);
        match corruption {
            "index_missing" => db.db.delete(r.index()).unwrap(),
            "orphan_index" => db
                .db
                .put(
                    index(
                        Phase::Done,
                        Id {
                            epoch: 99,
                            sequence: 1,
                        },
                    ),
                    [],
                )
                .unwrap(),
            "phase" => {
                db.db.delete(r.index()).unwrap();
                db.db.put(index(Phase::Done, r.receipt.id), []).unwrap();
            }
            "receipt" => db.db.delete(r.receipt.key()).unwrap(),
            "future_epoch" => {
                let mut bad = r.clone();
                bad.epoch += 1;
                db.db.put(r.key(), bad.encode().unwrap()).unwrap();
            }
            "counts" | "bytes" => {
                let mut m = db
                    .stratum(Default::default())
                    .unwrap()
                    .mining(Default::default())
                    .unwrap()
                    .info()
                    .clone();
                if corruption == "counts" {
                    m.counts[0] += 1;
                } else {
                    m.bytes += 1;
                }
                db.db.put(META, m.encode().unwrap()).unwrap();
            }
            "record_key" => {
                db.db.delete(r.key()).unwrap();
                db.db.put([RECORDS, 0], r.encode().unwrap()).unwrap();
            }
            "metadata" => db.db.delete(META).unwrap(),
            _ => unreachable!(),
        }
        drop(db);
        assert!(Database::open(&t.0, identity()).is_err(), "{corruption}");
    }
}
mod recovery;
#[test]
fn uncertain_publication_recovers_the_same_work_cursor_as_its_durable_batch() {
    for applied in [false, true] {
        let t = Temp::new();
        let db = Arc::new(Database::open(&t.0, identity()).unwrap());
        let mut writer = writer(&db);
        let epoch = writer.begin_mining(Default::default()).unwrap();
        assert_eq!(writer.work_revision, Some(0));
        db.commit(plan(&db)).unwrap();
        let result = writer.publish_with(
            &epoch,
            1,
            candidate(),
            date(),
            Default::default(),
            |db, batch| {
                if applied {
                    db.db.write_opt(batch, &write_options()).unwrap();
                }
                Err(Error::Backend("publication response lost".into()))
            },
        );
        assert!(result.is_err());
        assert_eq!(writer.work_revision, Some(0));
        assert_eq!(writer.pending.as_ref().unwrap().work_after.unwrap().0, 1);
        writer.recover().unwrap();
        assert_eq!(writer.work_revision, Some(u64::from(applied)));
        let publication = writer
            .publish_work(&epoch, 1, candidate(), date(), Default::default())
            .unwrap();
        assert!(publication.published);
        let frame = writer.refresh_work(&epoch, Default::default()).unwrap();
        assert!(!frame.invalidated);
        assert_eq!(frame.info.active, vec![candidate()]);
        assert_eq!(writer.work_revision, Some(1));
    }
}
