use super::*;
use payout::{GasPolicy, Generation};
pub(super) fn large() -> core::Payment {
    let mut p = pay((100, 0));
    p.relative = (1..=201u32)
        .map(|n| {
            let mut k = [0; 32];
            k[28..].copy_from_slice(&n.to_be_bytes());
            (
                ae_codec::api::encode(ae_codec::api::ApiType::AccountPubkey, &k).into_bytes(),
                Number::Float((1.0 / 201.0f64).to_bits()),
            )
        })
        .collect();
    p
}
pub(super) fn setup_large(db: &Arc<Database>) -> (Writer, u64) {
    let (mut w, e) = setup(db);
    commit(&mut w, vec![Command::Payment(large())]);
    (w, e)
}
pub(super) fn payments(db: &Database) -> Vec<core::Payment> {
    db.stratum(Default::default())
        .unwrap()
        .query(|t| t.payments(100))
        .unwrap()
}
pub(super) fn prepare(w: &mut Writer, e: u64) -> Record {
    match w
        .prepare_payout_with_generation(
            e,
            0.into(),
            Generation::Bounded(Default::default()),
            Default::default(),
        )
        .unwrap()
    {
        Selection::Prepared(r) | Selection::Existing(r) => *r,
        _ => panic!(),
    }
}
fn rows(db: &Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.db
        .iterator(rocksdb::IteratorMode::Start)
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.to_vec(), v.to_vec())
        })
        .collect()
}
#[test]
fn split_selection_preserves_siblings_and_publishes_all_exact_entitlements_with_first_request() {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, e) = setup_large(&db);
    let mut sibling = pay((100, 10));
    sibling.total = 20.into();
    commit(&mut w, vec![Command::Payment(sibling.clone())]);
    let r = prepare(&mut w, e);
    let all = payments(&db);
    assert_eq!(
        all.iter().map(|p| p.id.1).collect::<Vec<_>>(),
        vec![0, 10, 11]
    );
    assert_eq!(all[1], sibling);
    assert_eq!(r.payment, all[0]);
    assert_eq!(
        r.request.as_ref().unwrap().prepared.request().gross,
        r.payment.gross
    );
    let allocations: ae_stratum::rewards::Amounts = all
        .iter()
        .filter_map(|p| p.gross.clone())
        .flatten()
        .collect();
    assert_eq!(
        allocations,
        ae_stratum::rewards::absolute(&large().relative, &large().total, Default::default())
            .unwrap()
    );
    let mut changed = r.payment.clone();
    let m = changed.gross.as_mut().unwrap();
    let keys: Vec<_> = m.keys().take(2).cloned().collect();
    *m.get_mut(&keys[0]).unwrap() += 1;
    *m.get_mut(&keys[1]).unwrap() -= 1;
    let before = rows(&db);
    assert!(
        w.commit(w.version().unwrap(), vec![Command::Payment(changed)])
            .is_err()
    );
    assert_eq!(rows(&db), before);
    let mut invalid = r.clone();
    let m = invalid.payment.gross.as_mut().unwrap();
    *m.get_mut(&keys[0]).unwrap() += 1;
    *m.get_mut(&keys[1]).unwrap() -= 1;
    assert!(
        invalid
            .encode(Default::default(), Default::default())
            .is_err()
    );
}
#[test]
fn split_failures_leave_all_rows_sequence_and_original_obligation_unchanged() {
    for case in 0..4 {
        let t = Temp::new();
        let db = database(&t);
        let (mut w, e) = setup_large(&db);
        let mut policy = GasPolicy::default();
        match case {
            0 => policy.block_gas_limit = 1,
            1 => {
                let mut p = pay((100, u32::MAX));
                p.total = 1.into();
                commit(&mut w, vec![Command::Payment(p)]);
            }
            2 => {
                let mut p = large();
                p.date = Some(core::Date([2026, 1, 1, 0, 0, 0]));
                commit(&mut w, vec![Command::Payment(p)]);
            }
            _ => {
                let mut p = large();
                p.total = 100.into();
                commit(&mut w, vec![Command::Payment(p)]);
            }
        }
        let before = rows(&db);
        assert!(
            w.prepare_generated_with(
                e,
                0.into(),
                Generation::Bounded(policy),
                Default::default(),
                |_, _, _| panic!("failed split wrote")
            )
            .is_err()
        );
        assert_eq!(rows(&db), before);
        assert_eq!(stored(&db).next, 1);
        assert!(stored(&db).pending.is_none());
    }
}
#[test]
fn ambiguous_split_writes_reopen_only_original_or_complete_split_and_never_replan_pending_request()
{
    for committed in [false, true] {
        let t = Temp::new();
        let db = database(&t);
        let (mut w, e) = setup_large(&db);
        let before = rows(&db);
        assert!(
            w.prepare_generated_with(
                e,
                0.into(),
                Generation::Bounded(Default::default()),
                Default::default(),
                |at, db, b| {
                    assert_eq!(at, Stage::Prepare);
                    if committed {
                        write(db, b)?;
                    }
                    Err(Error::Backend("split boundary ambiguity".into()))
                }
            )
            .is_err()
        );
        if !committed {
            assert_eq!(rows(&db), before);
        }
        drop(w);
        drop(db);
        let db = database(&t);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let i = w.begin_payouts(config(), Default::default()).unwrap();
        assert_eq!(i.pending.is_some(), committed);
        assert_eq!(payments(&db).len(), if committed { 2 } else { 1 });
        let r = prepare(&mut w, i.epoch);
        assert_eq!(r.sequence, 1);
        let r = signed(&mut w, i.epoch, &r);
        let before = rows(&db);
        for generation in [
            Generation::Native,
            Generation::Bounded(GasPolicy {
                max_transfers: 1,
                block_gas_limit: 1,
                ..Default::default()
            }),
        ] {
            let Selection::Existing(existing) = w
                .prepare_payout_with_generation(i.epoch, 999.into(), generation, Default::default())
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(*existing, r);
            assert_eq!(rows(&db), before);
        }
    }
}
#[test]
fn every_split_payment_must_confirm_before_reward_cleanup_and_native_mode_cannot_reinterpret_remaining_rows()
 {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, mut e) = setup_large(&db);
    reward(&mut w, true);
    let first = prepare(&mut w, e);
    let ids: Vec<_> = payments(&db).into_iter().map(|p| p.id).collect();
    assert_eq!(ids, vec![(100, 0), (100, 1)]);
    for (index, id) in ids.iter().enumerate() {
        let r = if index == 0 {
            first.clone()
        } else {
            let before = rows(&db);
            assert!(matches!(
                w.prepare_payout(e, 0.into(), Default::default()),
                Err(Failure::Payout(
                    payout::Error::ExactAllocationRequiresBounded
                ))
            ));
            assert_eq!(rows(&db), before);
            prepare(&mut w, e)
        };
        assert_eq!(r.payment.id, *id);
        let r = signed(&mut w, e, &r);
        let r = w
            .begin_payout_submission(e, &r, Default::default())
            .unwrap();
        include(&db, &r, 100);
        set_top(
            &db,
            280 + index as u64,
            Protocol::Roma,
            Some(GROSS + 1),
            index as u64 + 1,
        );
        let Confirmation::Confirmed(c) = w.confirm_payout(e, &r, Default::default()).unwrap()
        else {
            panic!()
        };
        assert_eq!(c.pruned, index + 1 == ids.len());
        assert_eq!(payments(&db).len(), ids.len() - index - 1);
        let counts = db.stratum(Default::default()).unwrap().info().counts;
        if index == 0 {
            assert_eq!(counts, [1, 1, 2, 1, 1, 0]);
        } else {
            assert_eq!(counts, [0; 6]);
        }
        drop(w);
        w = Writer::start(db.clone(), Default::default()).unwrap();
        e = w.begin_payouts(config(), Default::default()).unwrap().epoch;
    }
    assert!(matches!(
        w.prepare_payout_with_generation(
            e,
            0.into(),
            Generation::Bounded(Default::default()),
            Default::default()
        )
        .unwrap(),
        Selection::Deferred(Deferred::Empty)
    ));
}
