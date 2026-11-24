use super::*;
use crate::tests::{Temp, identity, plan};
mod recovery;
fn database(t: &Temp) -> Arc<Database> {
    Arc::new(Database::open(&t.0, identity()).unwrap())
}
fn writer(db: Arc<Database>) -> Writer {
    Writer::start(db, Hash256([7; 32]), Default::default()).unwrap()
}
fn batch() -> Batch {
    Batch {
        revision: 1,
        old_tip: None,
        tip: identity().genesis,
        source: Hash256([8; 32]),
        events: vec![core::Event::Epoch {
            epoch: 1.into(),
            first: 1.into(),
            last: 4.into(),
        }],
        next: 0,
        operation: Default::default(),
    }
}
fn unmapped() -> Operation {
    Operation {
        leader: Some((AccountPublicKey([1; 32]), false)),
        ..Default::default()
    }
}
fn setup(t: &Temp, phase: &str) -> (Arc<Database>, Writer) {
    let db = database(t);
    let mut w = writer(db.clone());
    db.commit(plan(&db)).unwrap();
    if phase != "begin" {
        w.begin(batch()).unwrap();
    }
    if matches!(phase, "finish" | "advance") {
        w.observe(unmapped()).unwrap();
    }
    if phase == "advance" {
        w.finish(&core::State::default()).unwrap();
    }
    (db, w)
}
fn transition(w: &mut Writer, phase: &str) -> Result<(), Failure> {
    match phase {
        "begin" => w.begin(batch()),
        "observe" => w.observe(unmapped()),
        "finish" => w.finish(&core::State::default()),
        "advance" => w.advance(),
        _ => panic!(),
    }
}
#[test]
fn checkpoints_reopen_with_exact_state_cursor_and_exclusive_ownership() {
    let t = Temp::new();
    let (db, mut w) = setup(&t, "begin");
    assert!(matches!(
        Writer::start(db.clone(), Hash256([7; 32]), Default::default()),
        Err(Failure::AlreadyRunning)
    ));
    for stage in ["begin", "observe", "finish", "advance"] {
        transition(&mut w, stage).unwrap();
    }
    let saved = w.checkpoint().unwrap().clone();
    assert_eq!(saved.consumed, db.view().unwrap().status());
    assert_eq!(saved.completed, 1);
    assert!(saved.pending.is_none());
    drop(w);
    drop(db);
    let db = database(&t);
    let w = writer(db.clone());
    assert_eq!(*w.checkpoint().unwrap(), saved);
    drop(w);
    let changed = Writer::start(db, Hash256([9; 32]), Default::default()).unwrap();
    assert_eq!(changed.checkpoint().unwrap().binding, Hash256([9; 32]));
    assert_eq!(changed.checkpoint().unwrap().consumed, saved.consumed);
}
#[test]
fn recorded_queries_cannot_change_or_finish_before_parent_outcome() {
    let t = Temp::new();
    let (db, mut w) = setup(&t, "begin");
    w.begin(batch()).unwrap();
    let leader = AccountPublicKey([1; 32]);
    let mut op = Operation {
        leader: Some((leader, true)),
        ..Default::default()
    };
    w.observe(op.clone()).unwrap();
    assert_eq!(w.finish(&core::State::default()), Err(Failure::Incomplete));
    op.post = Some(Post {
        pin: pin::Parent {
            epoch: 0.into(),
            height: 0.into(),
            block_hash: identity().genesis.0.to_vec(),
        },
        account: AccountPublicKey([2; 32]),
        amount: 1.into(),
        fee: 100.into(),
    });
    w.observe(op.clone()).unwrap();
    let mut changed = op.clone();
    changed.post.as_mut().unwrap().fee = 101.into();
    assert!(w.observe(changed).is_err());
    op.parent = Some(ParentResult::Submitted {
        hash: b"parent-hash".to_vec(),
        intent: Hash256([3; 32]),
    });
    w.observe(op.clone()).unwrap();
    assert!(matches!(
        w.finish(&core::State::default()),
        Err(Failure::Storage(Error::Conflict))
    ));
    let state = core::State {
        enabled: true,
        active: Some(core::Active {
            next_last: 3.into(),
            parent_hash: Some(b"parent-hash".to_vec()),
            child_note: false,
            leader,
        }),
    };
    w.finish(&state).unwrap();
    w.advance().unwrap();
    drop(w);
    assert!(matches!(
        Writer::start(db.clone(), Hash256([9; 32]), Default::default()),
        Err(Failure::Binding)
    ));
    assert_eq!(writer(db).checkpoint().unwrap().state, state);
}
#[test]
fn partial_note_then_proof_requires_both_recorded_decisions_before_state_advance() {
    for skipped in [false, true] {
        let t = Temp::new();
        let (db, mut w) = setup(&t, "begin");
        let mut initial = w.checkpoint().unwrap().clone();
        initial.state = core::State {
            enabled: true,
            active: Some(core::Active {
                next_last: 5.into(),
                parent_hash: Some(vec![1; 32]),
                child_note: false,
                leader: AccountPublicKey([1; 32]),
            }),
        };
        w.save(initial).unwrap();
        let mut b = batch();
        b.events = vec![core::Event::Top { height: 5.into() }];
        w.begin(b).unwrap();
        let mut op = Operation {
            included: Some(true),
            note: Some(if skipped {
                ChildResult::Skipped(core::Error::Signing)
            } else {
                ChildResult::Completed(Hash256([4; 32]))
            }),
            ..Default::default()
        };
        w.observe(op.clone()).unwrap();
        let mut end = w.checkpoint().unwrap().state.clone();
        end.enabled = false;
        end.active.as_mut().unwrap().child_note = true;
        assert_eq!(w.finish(&end), Err(Failure::Incomplete));
        assert!(
            !w.checkpoint()
                .unwrap()
                .state
                .active
                .as_ref()
                .unwrap()
                .child_note
        );
        op.proof = Some(ChildResult::Completed(Hash256([5; 32])));
        w.observe(op).unwrap();
        w.finish(&end).unwrap();
        w.advance().unwrap();
        assert_eq!(w.checkpoint().unwrap().state, end);
        assert_eq!(db.view().unwrap().status().revision, 1);
    }
}
#[test]
fn ambiguous_each_checkpoint_transition_requires_durable_recovery() {
    for phase in ["begin", "observe", "finish", "advance"] {
        for persisted in [false, true] {
            let t = Temp::new();
            let (db, mut w) = setup(&t, phase);
            let before = w.checkpoint().unwrap().clone();
            transition(&mut w, phase).unwrap();
            let after = w.checkpoint().unwrap().clone();
            db.db
                .put(KEY, before.encode(Default::default()).unwrap())
                .unwrap();
            w.current = before.clone();
            let result = w.save_with(Some(&before), after.clone(), |db, batch| {
                if persisted {
                    write(db, batch)?;
                }
                Err(Error::Backend("ambiguous checkpoint".into()))
            });
            assert!(result.is_err());
            assert_eq!(w.checkpoint(), Err(Failure::RecoveryRequired));
            assert!(transition(&mut w, phase).is_err());
            assert_eq!(
                *w.recover().unwrap(),
                if persisted { after } else { before }
            );
        }
    }
}
#[test]
fn quotas_corruption_and_schema_thirteen_upgrade_are_checked_before_publication() {
    let t = Temp::new();
    let db = database(&t);
    let w = writer(db.clone());
    let saved = w.checkpoint().unwrap().clone();
    drop(w);
    assert!(
        Writer::start(
            db.clone(),
            saved.binding,
            Limits {
                bytes: 1,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert_eq!(
        db.pinning_agent(Default::default()).unwrap(),
        Some(saved.clone())
    );
    let ae_trie::Value::List(mut fields) = ae_trie::Value::decode(&identity().encode()).unwrap()
    else {
        panic!()
    };
    assert_eq!(fields[0], ae_trie::Value::Bytes(vec![18]));
    fields[0] = ae_trie::Value::Bytes(vec![13]);
    let old = ae_trie::Value::List(fields).encode();
    db.db.put(repo::IDENTITY, &old).unwrap();
    drop(db);
    let db = database(&t);
    assert_eq!(db.pinning_agent(Default::default()).unwrap(), Some(saved));
    assert_eq!(
        db.db.get(repo::IDENTITY).unwrap(),
        Some(identity().encode())
    );
    db.db.put(KEY, b"invalid checkpoint").unwrap();
    db.db.put(repo::IDENTITY, &old).unwrap();
    drop(db);
    assert!(Database::open(&t.0, identity()).is_err());
    let raw = DB::open_default(&t.0).unwrap();
    assert_eq!(raw.get(repo::IDENTITY).unwrap(), Some(old));
}
