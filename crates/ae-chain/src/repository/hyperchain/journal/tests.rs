use super::*;
fn checkpoint() -> Checkpoint {
    let mut state = State::default();
    state
        .leaders
        .insert(
            1.into(),
            1.into(),
            2.into(),
            vec![
                ObjectPayload::Bytes(vec![7; 32]),
                ObjectPayload::List(vec![ObjectPayload::Bytes(vec![8])]),
            ],
            Default::default(),
        )
        .unwrap();
    state
        .cache_epoch_start(1.into(), 0.into(), 10.into())
        .unwrap();
    Checkpoint {
        revision: 4,
        first_revision: 1,
        state,
    }
}
fn mutate(raw: &[u8], f: impl FnOnce(&mut Vec<V>)) -> Vec<u8> {
    let V::Tuple(mut fields) = encoding::decode(raw, Default::default()).unwrap() else {
        panic!()
    };
    f(&mut fields);
    encoding::encode(&V::Tuple(fields), Default::default()).unwrap()
}
#[test]
fn checkpoint_rejects_duplicate_cache_keys_bad_versions_and_resource_excess() {
    let original = checkpoint();
    let raw = original.encode(Default::default()).unwrap();
    assert_eq!(
        Checkpoint::decode(&raw, Default::default()).unwrap(),
        original
    );
    for field in [4, 5] {
        let altered = mutate(&raw, |p| {
            let V::List(rows) = &mut p[field] else {
                panic!()
            };
            rows.push(rows[0].clone());
        });
        assert!(Checkpoint::decode(&altered, Default::default()).is_err());
    }
    for (field, value) in [(0, 2), (1, 0), (2, 0), (2, 5)] {
        assert!(
            Checkpoint::decode(
                &mutate(&raw, |p| p[field] = integer(value)),
                Default::default()
            )
            .is_err()
        );
    }
    for limits in [
        Limits {
            max_bytes: raw.len() - 1,
            ..Default::default()
        },
        Limits {
            max_nodes: 1,
            ..Default::default()
        },
        Limits {
            max_depth: 1,
            ..Default::default()
        },
    ] {
        assert_eq!(
            Checkpoint::decode(&raw, limits).unwrap_err(),
            Error::ResourceLimit
        );
    }
    assert!(Checkpoint::decode(&[raw, vec![0]].concat(), Default::default()).is_err());
}
#[test]
fn event_journal_preserves_native_values_and_rejects_invalid_contexts() {
    let r = Record {
        revision: 4,
        context: Context {
            origin: Origin::Received,
            block: Some(BlockInfo {
                hash: Hash256([1; 32]),
                kind: Kind::Key,
                previous: Hash256([2; 32]),
                height: 3,
            }),
        },
        events: vec![
            Event::NoPin,
            Event::IncorrectPin,
            Event::PinAccepted {
                reward: 7.into(),
                recipient: ObjectPayload::List(vec![ObjectPayload::Bytes(vec![2])]),
                epoch: integer(3),
                height: 8.into(),
            },
            Event::NewEpoch(EpochInfo {
                epoch: integer(4),
                first: 9.into(),
                length: 3.into(),
                last: 11.into(),
                seed: Some(vec![3; 32]),
                validators: Some(vec![Staker {
                    address: ObjectPayload::Bytes(vec![4; 32]),
                    stake: integer(100),
                }]),
            }),
        ],
    };
    let raw = r.encode(Default::default()).unwrap();
    assert_eq!(Record::decode(&raw, Default::default()).unwrap(), r);
    assert_eq!(r.emitted().count(), 4);
    let mut synced = r.clone();
    synced.context.origin = Origin::Synced;
    assert_eq!(synced.emitted().count(), 0);
    for origin in [Origin::Bootstrap, Origin::Rollback] {
        let mut bad = r.clone();
        bad.context.origin = origin;
        assert!(
            Record::decode(&bad.encode(Default::default()).unwrap(), Default::default()).is_err()
        );
    }
    let mut bad = r.clone();
    bad.context.block.as_mut().unwrap().kind = Kind::Micro;
    assert!(Record::decode(&bad.encode(Default::default()).unwrap(), Default::default()).is_err());
    let mut bad = r.clone();
    bad.context.block.as_mut().unwrap().height = 0;
    assert!(Record::decode(&bad.encode(Default::default()).unwrap(), Default::default()).is_err());
    assert!(Record::decode(&mutate(&raw, |p| p[2] = integer(99)), Default::default()).is_err());
    assert_eq!(
        Record::decode(
            &raw,
            Limits {
                max_nodes: 2,
                ..Default::default()
            }
        )
        .unwrap_err(),
        Error::ResourceLimit
    );
}
