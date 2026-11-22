use super::*;
use ae_chain::repository::Read;

#[test]
fn guarded_hc_insertion_discards_snapshots_at_each_cancellation_boundary() {
    for index in [0, 4, 5] {
        let mut calls = 0;
        // First count the complete guard schedule, then reject at entry,
        // capture, preparation, first batch entry and the final WAL gate.
        for stop in [None, Some(0), Some(1), Some(2), Some(3), Some(usize::MAX)] {
            let path = Temp::new();
            let (db, c) = setup_corpus(&path, &FINALIZED, 6);
            let owner = Owner::start(db.clone(), c, Parent::default()).unwrap();
            for row in FINALIZED["rows"][6]["rows"]
                .as_array()
                .unwrap()
                .iter()
                .take(index)
            {
                owner
                    .insert(&block(row), 1_000_000, Origin::Gossip)
                    .unwrap();
            }
            let b = block(&FINALIZED["rows"][6]["rows"][index]);
            let old = db.view().unwrap();
            let bytes = old.source().prefix(&[], 100_000).unwrap();
            let before = owner.snapshot().unwrap();
            let source = db.events();
            let cursor = source.cursor();
            let scope = db.policy_scope().unwrap();
            let stop = stop.map(|s| if s == usize::MAX { calls - 1 } else { s });
            let mut checked = 0;
            let mut captures = 0;
            let result = owner.insert_guarded(
                &b,
                1_000_000,
                Origin::Gossip,
                &scope,
                |view, _| {
                    assert_eq!(view.status(), old.status());
                    captures += 1;
                    Ok(Parent {
                        reads: vec![99.into()],
                        unavailable: false,
                    })
                },
                || {
                    let n = checked;
                    checked += 1;
                    if stop == Some(n) {
                        Err("cancelled")
                    } else {
                        Ok(())
                    }
                },
            );
            if stop.is_none() {
                result.unwrap();
                calls = checked;
                assert!(calls > 4);
                assert_eq!(captures, 1);
                assert_eq!(owner.snapshot().unwrap().2.reads[0], BigInt::from(99));
            } else {
                assert!(matches!(result, Err(Error::Guard("cancelled"))));
                assert_eq!(captures, usize::from(stop != Some(0)));
                assert_eq!(owner.snapshot().unwrap(), before);
                assert_eq!(
                    db.view().unwrap().source().prefix(&[], 100_000).unwrap(),
                    bytes
                );
                assert!(source.after(&cursor, 1).unwrap().records.is_empty());
                owner
                    .insert_guarded(
                        &b,
                        1_000_000,
                        Origin::Gossip,
                        &scope,
                        |_, _| Ok::<_, ()>(Parent::default()),
                        || Ok(()),
                    )
                    .unwrap();
            }
            assert_eq!(db.view().unwrap().status().tip, Some(b.hash().unwrap()));
        }
    }
}

#[test]
fn scoped_hc_conflicts_and_parent_replacement_never_install_staged_inputs() {
    let path = Temp::new();
    let (db, c) = setup(&path);
    let owner = Owner::start(db.clone(), c, Parent::default()).unwrap();
    let scope = db.policy_scope().unwrap();
    let before = owner.snapshot().unwrap();
    let b = block(&DATA["rows"][0]["rows"][0]);
    let result = owner.insert_guarded(
        &b,
        1_000_000,
        Origin::Gossip,
        &scope,
        |_, _| {
            db.update_resilience(
                ae_chain::resilience::Event::Start,
                ae_chain::resilience::Config {
                    depth: 1,
                    from_start: true,
                },
            )
            .unwrap();
            Ok::<_, ()>(Parent {
                reads: vec![99.into()],
                unavailable: false,
            })
        },
        || Ok(()),
    );
    assert!(matches!(
        result,
        Err(Error::Repository(repo::Error::Conflict))
    ));
    assert_eq!(owner.snapshot().unwrap(), before);
    let policy = crate::policy::Owner::new(db.clone()).unwrap();
    let lease = policy.store().acquire().unwrap();
    lease
        .update(ae_chain::resilience::Event::Start, Default::default())
        .unwrap();
    assert!(db.policy_ready());
    let result = owner.insert_guarded(
        &b,
        1_000_000,
        Origin::Gossip,
        &scope,
        |_, _| Ok::<_, ()>(Parent::default()),
        || Ok(()),
    );
    assert!(matches!(
        result,
        Err(Error::Repository(repo::Error::PolicyClosed))
    ));
    assert_eq!(owner.snapshot().unwrap(), before);
    let fresh = db.policy_scope().unwrap();
    owner
        .insert_guarded(
            &b,
            1_000_000,
            Origin::Gossip,
            &fresh,
            |_, _| Ok::<_, ()>(Parent::default()),
            || Ok(()),
        )
        .unwrap();
}

#[test]
fn guarded_hc_uncertain_writes_poison_without_publishing_staged_host_or_cache() {
    for failure in [1, 2] {
        let path = Temp::new();
        let (db, c) = setup(&path);
        let owner = Owner::start(db.clone(), c, Parent::default()).unwrap();
        let before = owner.snapshot().unwrap();
        let scope = db.policy_scope().unwrap();
        let b = block(&DATA["rows"][0]["rows"][0]);
        db.event_write_failure.store(failure, Ordering::Relaxed);
        let result = owner.insert_guarded(
            &b,
            1_000_000,
            Origin::Gossip,
            &scope,
            |_, _| {
                Ok::<_, ()>(Parent {
                    reads: vec![99.into()],
                    unavailable: false,
                })
            },
            || Ok(()),
        );
        assert!(matches!(
            result,
            Err(Error::Repository(repo::Error::Backend(_)))
        ));
        let live = owner.live.lock().unwrap();
        assert!(live.failed);
        assert_eq!((&live.state, &live.host), (&before.1, &before.2));
        drop(live);
        assert_eq!(
            db.view().unwrap().status().revision,
            if failure == 1 { 1 } else { 2 }
        );
        assert!(matches!(
            owner.insert_guarded(
                &b,
                1_000_000,
                Origin::Gossip,
                &scope,
                |_, h| Ok::<_, ()>(h.clone()),
                || Ok(())
            ),
            Err(Error::RecoveryRequired)
        ));
    }
}
