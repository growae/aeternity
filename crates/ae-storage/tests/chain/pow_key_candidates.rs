use super::*;
use ae_chain::{key::candidate, repository::pow::key as build};
use ae_storage::pow::key as owned;
fn request(header: &KeyHeader) -> build::Input {
    build::Input {
        parent: Hash256(header.common.previous_hash),
        production: candidate::Input {
            miner: AccountPublicKey(header.miner),
            beneficiary: AccountPublicKey(header.beneficiary),
            calculation_time: header.time.saturating_sub(17),
            release_info: header.info.map(u32::from_be_bytes).unwrap_or(0),
        },
    }
}
#[test]
fn owned_key_candidates_reproduce_public_pow_headers_and_admit_original_seals() {
    let mut count = 0;
    for n in [Network::Mainnet, Network::Uat] {
        let temp = Temp::new();
        let db = Arc::new(Database::open(&temp.0, identity(n)).unwrap());
        bootstrap(&db, n);
        let owner = snapshots::Owner::start(db.clone(), Default::default()).unwrap();
        let net = profile(n);
        let execution = transactions::Config::primitives(&net);
        for block in blocks(n) {
            if let Block::Key(original) = &block {
                let prepared = owned::prepare(
                    &owner,
                    &identity(n),
                    &execution,
                    &rules(n),
                    request(original),
                    Default::default(),
                )
                .unwrap();
                assert_eq!(prepared.metadata.status, prepared.value.status);
                assert_eq!(prepared.metadata.source_id, prepared.value.source_id);
                assert_eq!(owner.live_snapshots(), 1);
                let candidate = prepared.value.finish(original.time).unwrap();
                assert_eq!(candidate.seal, [0; 42]);
                assert_eq!(candidate.nonce, 0);
                assert!(
                    ledger::prepare(
                        &db.view().unwrap(),
                        &Block::Key(candidate.clone()),
                        &execution,
                        &rules(n),
                        Default::default()
                    )
                    .is_err()
                );
                let mut reproduced = candidate;
                reproduced.seal = original.seal;
                reproduced.nonce = original.nonce;
                assert_eq!(
                    Block::Key(reproduced.clone()).encode().unwrap(),
                    block.encode().unwrap()
                );
                let before = prepared.value.view.status();
                let roots = prepared.value.candidate.trees().roots();
                insert(&db, n, &Block::Key(reproduced));
                assert_eq!(prepared.value.view.status(), before);
                assert!(
                    prepared
                        .value
                        .view
                        .block(block.hash().unwrap())
                        .unwrap()
                        .is_none()
                );
                assert_eq!(
                    db.view()
                        .unwrap()
                        .require_record(block.hash().unwrap())
                        .unwrap()
                        .roots
                        .state,
                    roots
                );
                count += 1;
            } else {
                insert(&db, n, &block);
            }
            assert_eq!(owner.live_snapshots(), 0);
        }
        let tip = db.view().unwrap().status().tip.unwrap();
        let key = genesis_key(n);
        let mut input = request(&key);
        input.parent = tip;
        let held = owned::prepare(
            &owner,
            &identity(n),
            &execution,
            &rules(n),
            input,
            Default::default(),
        )
        .unwrap();
        let bytes = Header::Key(held.value.finish(1234567890000).unwrap())
            .encode()
            .unwrap();
        drop(owner);
        drop(db);
        assert!(Database::open(&temp.0, identity(n)).is_err());
        held.value.view.verify_tip().unwrap();
        assert_eq!(
            Header::Key(held.value.finish(1234567890000).unwrap())
                .encode()
                .unwrap(),
            bytes
        );
        drop(held);
        let db = Arc::new(Database::open(&temp.0, identity(n)).unwrap());
        let owner = snapshots::Owner::start(db, Default::default()).unwrap();
        let again = owned::prepare(
            &owner,
            &identity(n),
            &execution,
            &rules(n),
            input,
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            Header::Key(again.value.finish(1234567890000).unwrap())
                .encode()
                .unwrap(),
            bytes
        );
    }
    assert_eq!(count, 12);
}
#[test]
fn key_candidate_binding_and_capacity_failures_release_owned_snapshots() {
    let n = Network::Mainnet;
    let temp = Temp::new();
    let db = Arc::new(Database::open(&temp.0, identity(n)).unwrap());
    bootstrap(&db, n);
    let owner = snapshots::Owner::start(
        db.clone(),
        snapshots::Config {
            maximum_snapshots: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let net = profile(n);
    let mut execution = transactions::Config::primitives(&net);
    let original = blocks(n).remove(0).header();
    let Header::Key(header) = original else {
        panic!()
    };
    let before = db.view().unwrap().status();
    for fault in 0..6 {
        let mut id = identity(n);
        let mut rules = rules(n);
        let mut limits = build::Limits::default();
        let mut input = request(&header);
        match fault {
            0 => id.consensus = ae_protocol::ConsensusType::Pos,
            1 => id.execution_profile = Hash256([1; 32]),
            2 => rules.genesis_hash = Hash256([1; 32]),
            3 => limits.history = 0,
            4 => input.parent = Hash256([99; 32]),
            _ => execution.consensus = ae_protocol::ConsensusType::Pos,
        }
        assert!(
            owned::prepare(&owner, &id, &execution, &rules, input, limits).is_err(),
            "fault {fault}"
        );
        assert_eq!(owner.live_snapshots(), 0);
        assert_eq!(db.view().unwrap().status(), before);
    }
    execution.consensus = ae_protocol::ConsensusType::Pow;
    let held = owned::prepare(
        &owner,
        &identity(n),
        &execution,
        &rules(n),
        request(&header),
        Default::default(),
    )
    .unwrap();
    assert!(matches!(
        owned::prepare(
            &owner,
            &identity(n),
            &execution,
            &rules(n),
            request(&header),
            Default::default()
        ),
        Err(owned::Error::Snapshot(snapshots::Error::ResourceLimit))
    ));
    drop(held);
    assert_eq!(owner.live_snapshots(), 0);
}
#[test]
fn key_candidate_missing_inputs_are_distinct_and_target_lookup_precedes_state_loading() {
    let n = Network::Mainnet;
    let temp = Temp::new();
    let db = Database::open(&temp.0, identity(n)).unwrap();
    bootstrap(&db, n);
    let first = blocks(n).remove(0);
    insert(&db, n, &first);
    let view = db.view().unwrap();
    let net = profile(n);
    let execution = transactions::Config::primitives(&net);
    let Header::Key(mut h) = first.header() else {
        panic!()
    };
    let mut input = request(&h);
    input.parent = first.hash().unwrap();
    let make = |rows| {
        repo::View::load_with_config(
            Faults {
                source: view.source().clone(),
                rows: Arc::new(rows),
            },
            view.config().clone(),
        )
        .unwrap()
    };
    let absent = Hash256([99; 32]);
    let mut missing = input;
    missing.parent = absent;
    assert!(
        matches!(build::prepare(&view,&identity(n),&execution,&rules(n),missing,Default::default()),Err(build::Error::BlockNotFound(x))if x==absent)
    );
    let state_missing = make(BTreeMap::from([(
        repo::key(repo::RECORDS, input.parent.0),
        None,
    )]));
    assert!(matches!(
        build::prepare(
            &state_missing,
            &identity(n),
            &execution,
            &rules(n),
            input,
            Default::default()
        ),
        Err(build::Error::Repository(_))
    ));
    // An explicit read-only malformed-history fixture, never admitted/stored.
    // It has no state record either, so the observed error proves lookup order.
    h.common.height = 18;
    h.common.previous_key_hash = absent.0;
    let fake = Block::Key(h);
    let hash = fake.hash().unwrap();
    let malformed = make(BTreeMap::from([(
        repo::key(repo::BLOCKS, hash.0),
        Some(fake.encode().unwrap()),
    )]));
    input.parent = hash;
    assert!(matches!(
        build::prepare(
            &malformed,
            &identity(n),
            &execution,
            &rules(n),
            input,
            Default::default()
        ),
        Err(build::Error::AdjustmentHistory(_))
    ));
    assert!(matches!(
        build::prepare(
            &malformed,
            &identity(n),
            &execution,
            &rules(n),
            input,
            build::Limits {
                history: 17,
                ..Default::default()
            }
        ),
        Err(build::Error::AdjustmentHistory(repo::Error::ResourceLimit))
    ));
    assert!(view.block(hash).unwrap().is_none());
    assert_eq!(view.status(), db.view().unwrap().status());
}
