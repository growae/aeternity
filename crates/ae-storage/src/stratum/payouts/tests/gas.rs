use super::*;
use payout::{GasPolicy, Generation};
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
fn bounded_payout_intent_reopens_with_exact_signed_bytes_under_a_changed_generation_policy() {
    for prior in [Generation::Native, Generation::Bounded(Default::default())] {
        let t = Temp::new();
        let db = database(&t);
        let (mut w, e) = setup(&db);
        let Selection::Prepared(r) = w
            .prepare_payout_with_generation(e, 1.into(), prior, Default::default())
            .unwrap()
        else {
            panic!()
        };
        let r = signed(&mut w, e, &r);
        let raw = r
            .encode(Default::default(), core::Limits::default())
            .unwrap();
        set_top(&db, 281, Protocol::Ceres, Some(12), 999);
        drop(w);
        drop(db);
        let db = database(&t);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let i = w.begin_payouts(config(), Default::default()).unwrap();
        for next in [
            Generation::Native,
            Generation::Bounded(GasPolicy {
                initial: 999,
                per_transfer: 30000,
                max_transfers: 1,
                block_gas_limit: 1,
            }),
        ] {
            let before = rows(&db);
            let Selection::Existing(saved) = w
                .prepare_payout_with_generation(i.epoch, 99.into(), next, Default::default())
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(*saved, r);
            assert_eq!(
                saved
                    .encode(Default::default(), core::Limits::default())
                    .unwrap(),
                raw
            );
            assert_eq!(rows(&db), before);
        }
    }
}
#[test]
fn rejected_gas_policy_preserves_every_row_and_can_retry_without_consuming_sequence_or_nonce() {
    let t = Temp::new();
    let db = database(&t);
    let (mut w, e) = setup(&db);
    let before = rows(&db);
    for policy in [
        GasPolicy {
            max_transfers: 0,
            ..Default::default()
        },
        GasPolicy {
            block_gas_limit: 1,
            ..Default::default()
        },
    ] {
        assert!(
            w.prepare_generated_with(
                e,
                0.into(),
                Generation::Bounded(policy),
                Default::default(),
                |_, _, _| panic!("rejected plan must not write")
            )
            .is_err()
        );
        assert_eq!(rows(&db), before);
    }
    let Selection::Prepared(r) = w
        .prepare_payout_with_generation(
            e,
            0.into(),
            Generation::Bounded(Default::default()),
            Default::default(),
        )
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(r.sequence, 1);
    assert_eq!(
        r.request
            .as_ref()
            .unwrap()
            .prepared
            .call()
            .transaction()
            .nonce(),
        &Unsigned::from(1)
    );
    assert_eq!(
        r.request.as_ref().unwrap().prepared.generation(),
        Generation::Bounded(Default::default())
    );
}
#[test]
fn ambiguous_bounded_preparation_writes_recover_only_the_original_complete_request() {
    for committed in [false, true] {
        let t = Temp::new();
        let db = database(&t);
        let (mut w, e) = setup(&db);
        let before = rows(&db);
        assert!(
            w.prepare_generated_with(
                e,
                0.into(),
                Generation::Bounded(Default::default()),
                Default::default(),
                |stage, db, b| {
                    assert_eq!(stage, Stage::Prepare);
                    if committed {
                        write(db, b)?;
                    }
                    Err(Error::Backend("bounded preparation ambiguity".into()))
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
        let selection = w
            .prepare_payout_with_generation(
                i.epoch,
                0.into(),
                Generation::Bounded(Default::default()),
                Default::default(),
            )
            .unwrap();
        let r = match selection {
            Selection::Prepared(r) | Selection::Existing(r) => r,
            _ => panic!(),
        };
        assert_eq!(r.sequence, 1);
        assert_eq!(stored(&db).next, 2);
        signed(&mut w, i.epoch, &r);
    }
}
