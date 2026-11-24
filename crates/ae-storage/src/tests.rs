use super::*;
use ae_chain::repository::{Roots, ledger};
use ae_codec::headers::Header;
use ae_protocol::NetworkProfile;
use ae_state::{
    Trees,
    genesis::{self, Network},
};
use ae_trie::{NodeWriter, Overlay, OverlayLimits};
use std::path::PathBuf;
pub(crate) struct Temp(pub(crate) PathBuf);
impl Temp {
    pub(crate) fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "ae-store-unit-{}-{}",
            std::process::id(),
            NEXT_SOURCE.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub(crate) fn identity() -> Identity {
    Identity::new(
        &NetworkProfile::uat(),
        genesis::header(Network::Uat).unwrap().hash().unwrap(),
        Hash256::ZERO,
    )
}
pub(crate) fn plan(db: &Database) -> Update {
    let view = db.view().unwrap();
    let Header::Key(header) = genesis::header(Network::Uat).unwrap() else {
        panic!()
    };
    let trees = Trees {
        accounts: genesis::state(Network::Uat).unwrap().0,
        contracts: ae_state::contracts::Contracts::new(Default::default()),
        calls: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    ledger::bootstrap(
        &view,
        &NetworkProfile::uat(),
        &header,
        &trees,
        Default::default(),
    )
    .unwrap()
}

#[test]
fn full_state_scan_detects_deep_damage_and_keeps_the_original_rocksdb_snapshot() {
    use ae_trie::scan::PageLimits;
    let dir = Temp::new();
    let Header::Key(header) = genesis::header(Network::Mainnet).unwrap() else {
        panic!()
    };
    let id = Identity::new(
        &NetworkProfile::mainnet(),
        Header::Key(header.clone()).hash().unwrap(),
        Hash256::ZERO,
    );
    let db = Database::open(&dir.0, id.clone()).unwrap();
    let trees = Trees {
        accounts: genesis::state(Network::Mainnet).unwrap().0,
        contracts: ae_state::contracts::Contracts::new(Default::default()),
        calls: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    db.commit(
        ledger::bootstrap(
            &db.view().unwrap(),
            &NetworkProfile::mainnet(),
            &header,
            &trees,
            Default::default(),
        )
        .unwrap(),
    )
    .unwrap();
    let view = db.view().unwrap();
    let mut baseline = view.scan_state(id.genesis, Default::default()).unwrap();
    let mut expected = Vec::new();
    while !baseline.is_complete() {
        expected.extend(baseline.next_page(PageLimits::default()).unwrap());
    }
    assert!(expected.len() > 2);
    let target = &expected[2];
    let mut pinned = view.scan_state(id.genesis, Default::default()).unwrap();
    let mut actual = pinned
        .next_page(PageLimits {
            nodes: 1,
            ..Default::default()
        })
        .unwrap();
    let original_status = view.status();
    for bytes in [None, Some(vec![0])] {
        match bytes {
            None => db.db.delete(repo::key(repo::NODES, target.hash.0)).unwrap(),
            Some(b) => db.db.put(repo::key(repo::NODES, target.hash.0), b).unwrap(),
        }
        let damaged = db.view().unwrap();
        damaged.verify_tip().unwrap(); // shallow opening validation cannot see this leaf
        let mut scan = damaged.scan_state(id.genesis, Default::default()).unwrap();
        let before = scan.progress();
        assert!(scan.next_page(PageLimits::default()).is_err());
        assert_eq!(scan.progress(), before);
        assert!(!scan.is_complete());
        assert_eq!(damaged.status(), original_status);
    }
    while !pinned.is_complete() {
        actual.extend(pinned.next_page(PageLimits::default()).unwrap());
    }
    assert_eq!(actual, expected);
    db.db
        .put(repo::key(repo::NODES, target.hash.0), &target.bytes)
        .unwrap();
    drop(pinned);
    drop(baseline);
    drop(view);
    drop(db);
    let reopened = Database::open(&dir.0, id.clone()).unwrap();
    let mut scan = reopened
        .view()
        .unwrap()
        .scan_state(id.genesis, Default::default())
        .unwrap();
    let mut actual = Vec::new();
    while !scan.is_complete() {
        actual.extend(scan.next_page(PageLimits::default()).unwrap());
    }
    assert_eq!(actual, expected);
    assert_eq!(reopened.view().unwrap().status(), original_status);
}
#[test]
fn legacy_schema_upgrade_retains_canonical_transactions_and_all_existing_records() {
    for schema in 1..=9 {
        use ae_trie::Value;
        use ae_tx::{SignedTransaction, Spend, Transaction, TransactionLeaf};
        use ae_types::{AccountPublicKey, Id, IdTag};
        let dir = Temp::new();
        let id = identity();
        let db = Database::open(&dir.0, id.clone()).unwrap();
        db.commit(plan(&db)).unwrap();
        let tx = SignedTransaction::new(
            Transaction::new(TransactionLeaf::Spend(
                Spend::new(AccountPublicKey([1; 32]), Id::new(IdTag::Account, [2; 32])).unwrap(),
            )),
            vec![],
        );
        let Value::List(mut fields) = Value::decode(&id.encode()).unwrap() else {
            panic!()
        };
        fields[0] = Value::Bytes(vec![schema]);
        fields.truncate(5);
        let legacy = Value::List(fields).encode();
        let legacy_block = db
            .view()
            .unwrap()
            .require_block(id.genesis)
            .unwrap()
            .encode()
            .unwrap();
        if schema == 1 {
            db.db
                .put(repo::key(repo::BLOCKS, id.genesis.0), legacy_block)
                .unwrap();
        }
        // Earlier schema writers used the original ten-field insertion event.
        let event_key = repo::key(repo::EVENTS, 1u64.to_be_bytes());
        let Value::List(mut event) =
            Value::decode(&db.db.get(&event_key).unwrap().unwrap()).unwrap()
        else {
            panic!()
        };
        event[0] = Value::Bytes(vec![1]);
        event.truncate(10);
        db.db.put(&event_key, Value::List(event).encode()).unwrap();
        db.db.put(repo::IDENTITY, &legacy).unwrap();
        db.db
            .put(
                repo::key(repo::TRANSACTIONS, tx.hash().0),
                if schema == 1 {
                    tx.encode()
                } else {
                    ae_tx::storage::encode(&tx, Default::default()).unwrap()
                },
            )
            .unwrap();
        let before = db.view().unwrap().source().prefix(&[], 10000).unwrap();
        drop(db);
        let db = Database::open(&dir.0, id.clone()).unwrap();
        assert_eq!(db.db.get(repo::IDENTITY).unwrap().unwrap(), id.encode());
        assert_eq!(
            db.view().unwrap().transaction(tx.hash()).unwrap().unwrap(),
            tx
        );
        let after = db.view().unwrap().source().prefix(&[], 10000).unwrap();
        let without_identity = |pairs: repo::Entries| {
            pairs
                .into_iter()
                .filter(|(k, _)| k != repo::IDENTITY)
                .collect::<Vec<_>>()
        };
        assert_eq!(without_identity(before), without_identity(after));
        assert_eq!(db.view().unwrap().status().revision, 1);
    }
}

#[test]
fn failed_legacy_validation_never_advances_the_schema_marker() {
    for schema in 1..=9 {
        use ae_trie::Value;
        let dir = Temp::new();
        let id = identity();
        let db = Database::open(&dir.0, id.clone()).unwrap();
        db.commit(plan(&db)).unwrap();
        let root = db
            .view()
            .unwrap()
            .require_record(id.genesis)
            .unwrap()
            .roots
            .state
            .accounts
            .unwrap();
        let node = db.db.get(repo::key(repo::NODES, root.0)).unwrap().unwrap();
        let Value::List(mut fields) = Value::decode(&id.encode()).unwrap() else {
            panic!()
        };
        fields[0] = Value::Bytes(vec![schema]);
        fields.truncate(5);
        let legacy = Value::List(fields).encode();
        let legacy_block = db
            .view()
            .unwrap()
            .require_block(id.genesis)
            .unwrap()
            .encode()
            .unwrap();
        if schema == 1 {
            db.db
                .put(repo::key(repo::BLOCKS, id.genesis.0), legacy_block)
                .unwrap();
        }
        // Earlier schema writers used the original ten-field insertion event.
        let event_key = repo::key(repo::EVENTS, 1u64.to_be_bytes());
        let Value::List(mut event) =
            Value::decode(&db.db.get(&event_key).unwrap().unwrap()).unwrap()
        else {
            panic!()
        };
        event[0] = Value::Bytes(vec![1]);
        event.truncate(10);
        db.db.put(&event_key, Value::List(event).encode()).unwrap();
        db.db.put(repo::IDENTITY, &legacy).unwrap();
        db.db.delete(repo::key(repo::NODES, root.0)).unwrap();
        drop(db);
        assert!(Database::open(&dir.0, id.clone()).is_err());
        let raw = DB::open_default(&dir.0).unwrap();
        assert_eq!(raw.get(repo::IDENTITY).unwrap().unwrap(), legacy);
        raw.put(repo::key(repo::NODES, root.0), node).unwrap();
        drop(raw);
        let db = Database::open(&dir.0, id.clone()).unwrap();
        assert_eq!(db.db.get(repo::IDENTITY).unwrap().unwrap(), id.encode());
    }
}

#[test]
fn every_batch_construction_failure_keeps_the_database_unchanged_and_allows_retry() {
    let count = {
        let dir = Temp::new();
        let db = Database::open(&dir.0, identity()).unwrap();
        plan(&db).writes().len()
    };
    for prefix in 0..=count {
        let dir = Temp::new();
        let db = Database::open(&dir.0, identity()).unwrap();
        let before = db.view().unwrap();
        let mut left = prefix;
        let e = db
            .commit_checked(plan(&db), || {
                if left == 0 {
                    Err(Error::Backend("injected write failure".into()))
                } else {
                    left -= 1;
                    Ok(())
                }
            })
            .unwrap_err();
        assert!(matches!(e, Error::Backend(_)));
        let after = db.view().unwrap();
        assert_eq!(after.status(), Status::default());
        assert_eq!(
            before.source().prefix(&[], 10000).unwrap(),
            after.source().prefix(&[], 10000).unwrap()
        );
        db.commit(plan(&db)).unwrap();
        db.view().unwrap().verify_tip().unwrap();
        assert_eq!(before.status(), Status::default());
    }
    eprintln!("Atomic batch construction failure prefixes: {}", count + 1);
}
#[test]
fn corruption_and_unknown_schemas_are_reported_instead_of_reinitializing_state() {
    for mode in 0..9 {
        let dir = Temp::new();
        let id = identity();
        let db = Database::open(&dir.0, id.clone()).unwrap();
        db.commit(plan(&db)).unwrap();
        let record = db.view().unwrap().require_record(id.genesis).unwrap();
        let root = record.roots.state.accounts.unwrap();
        let mut bad = WriteBatch::default();
        match mode {
            0 => bad.delete(repo::key(repo::NODES, root.0)),
            1 => bad.put(repo::key(repo::NODES, root.0), b"corrupt"),
            2 => bad.delete(repo::key(repo::RECORDS, id.genesis.0)),
            3 => bad.put(repo::key(repo::RECORDS, id.genesis.0), b"corrupt"),
            4 => bad.delete(repo::key(repo::EVENTS, 1u64.to_be_bytes())),
            5 => bad.put(repo::key(repo::KEYS, 0u64.to_be_bytes()), [0; 32]),
            6 => bad.put(repo::STATUS, b"corrupt"),
            7 => bad.put(repo::IDENTITY, b"corrupt"),
            8 => bad.delete(repo::key(repo::BLOCKS, id.genesis.0)),
            _ => unreachable!(),
        }
        db.db.write_opt(bad, &write_options()).unwrap();
        drop(db);
        assert!(Database::open(&dir.0, id).is_err(), "mode {mode}");
    }
    let dir = Temp::new();
    let mut o = Options::default();
    o.create_if_missing(true);
    let db = DB::open(&o, &dir.0).unwrap();
    db.put_opt(b"unrecognized", b"preserve me", &write_options())
        .unwrap();
    drop(db);
    assert!(matches!(
        Database::open(&dir.0, identity()),
        Err(Error::Corrupt("unrecognized database"))
    ));
    let db = DB::open(&o, &dir.0).unwrap();
    assert_eq!(db.get(b"unrecognized").unwrap().unwrap(), b"preserve me");
}
#[test]
fn overlay_clones_isolate_pending_nodes_and_enforce_local_budgets() {
    let dir = Temp::new();
    let db = Database::open(&dir.0, identity()).unwrap();
    db.commit(plan(&db)).unwrap();
    let view = db.view().unwrap();
    let mut t = view.trees(identity().genesis, Default::default()).unwrap();
    let original = t.clone();
    let before = Roots::of(&original);
    let (account, _) = genesis::allocations(Network::Uat).unwrap().remove(0);
    let old = t.accounts.lookup(account).unwrap().unwrap();
    let mut changed = old.clone();
    changed.earn(&7.into());
    t.accounts.enter(&changed).unwrap();
    assert_ne!(Roots::of(&t), before);
    assert_eq!(Roots::of(&original), before);
    assert_eq!(original.accounts.lookup(account).unwrap(), Some(old));
    assert!(original.accounts.tree().store().pending().is_empty());
    assert!(!t.accounts.tree().store().pending().is_empty());
    assert_eq!(
        view.require_record(identity().genesis).unwrap().roots,
        before
    );
    let mut overlay = Overlay::new(view.source().clone(), OverlayLimits { nodes: 1, bytes: 3 });
    let first = vec![1, 2, 3];
    let hash = blake2b_256(&first);
    overlay.put_node(hash, first.clone()).unwrap();
    overlay.put_node(hash, first).unwrap();
    let frozen = overlay.clone();
    assert!(matches!(
        overlay.put_node(blake2b_256(b"4"), b"4".to_vec()),
        Err(ae_trie::Error::ResourceLimit)
    ));
    assert_eq!(frozen.pending(), overlay.pending());
    assert!(matches!(
        overlay.put_node(Hash256::ZERO, vec![]),
        Err(ae_trie::Error::HashMismatch(_))
    ));
    assert_eq!(overlay.node(&hash).unwrap(), Some(vec![1, 2, 3]));
}

#[test]
fn policy_changes_invalidate_plans_and_failed_effects_preserve_runtime_state() {
    use resilience::{Config, Event};
    let dir = Temp::new();
    let db = Database::open(&dir.0, identity()).unwrap();
    let old = db.view().unwrap();
    let stale = plan(&db);
    let config = Config {
        depth: 3,
        from_start: false,
    };
    db.update_resilience(Event::Start, config).unwrap();
    assert!(matches!(db.commit(stale), Err(Error::Conflict)));
    assert_eq!(old.resilience().state.configured_depth(), None);
    assert_eq!(
        db.view().unwrap().resilience().state.configured_depth(),
        Some(3)
    );
    db.commit(plan(&db)).unwrap();
    db.db
        .put_opt(
            repo::FINALIZED,
            repo::encode_finalized(9).unwrap(),
            &write_options(),
        )
        .unwrap();
    let before = db.view().unwrap();
    let bytes = before.source().prefix(&[], 10000).unwrap();
    assert!(
        db.update_resilience_checked(Event::Synced, config, || Err(Error::Backend(
            "injected control failure".into()
        )))
        .is_err()
    );
    assert_eq!(db.view().unwrap().resilience(), before.resilience());
    assert_eq!(
        db.view().unwrap().source().prefix(&[], 10000).unwrap(),
        bytes
    );
    db.update_resilience(Event::Synced, config).unwrap();
    assert_eq!(before.resilience().state.active_depth(), None);
    assert_eq!(
        db.view().unwrap().resilience().state.active_depth(),
        Some(3)
    );
    assert_eq!(db.view().unwrap().finalized_height().unwrap(), Some(9));
    db.clear_finalized_height().unwrap();
    assert_eq!(db.view().unwrap().finalized_height().unwrap(), None);
    assert_eq!(before.finalized_height().unwrap(), Some(9));
    drop(before);
    drop(old);
    drop(db);
    let db =
        Database::open_with_resilience(&dir.0, identity(), Default::default(), config).unwrap();
    assert_eq!(db.view().unwrap().resilience().state.active_depth(), None);
    assert!(!db.view().unwrap().resilience().state.synchronized());
}

#[test]
fn finalized_record_corruption_and_future_schema_never_reinitialize_storage() {
    use ae_trie::Value::{Bytes, List};
    let invalid = vec![
        vec![],
        vec![0xc0],
        vec![0x80],
        List(vec![Bytes(vec![1]), Bytes(vec![0])]).encode(),
        List(vec![Bytes(vec![2]), Bytes(vec![7])]).encode(),
        List(vec![Bytes(vec![1]), Bytes(vec![7]), Bytes(vec![])]).encode(),
        List(vec![Bytes(vec![1]), Bytes(vec![1, 0, 0, 0, 0, 0, 0, 0, 0])]).encode(),
        [repo::encode_finalized(7).unwrap(), vec![0]].concat(),
    ];
    for encoded in invalid {
        let dir = Temp::new();
        let id = identity();
        let db = Database::open(&dir.0, id.clone()).unwrap();
        db.commit(plan(&db)).unwrap();
        let pinned = db.view().unwrap();
        db.db
            .put_opt(repo::FINALIZED, &encoded, &write_options())
            .unwrap();
        assert!(db.view().unwrap().verify_tip().is_err());
        assert_eq!(pinned.finalized_height().unwrap(), None);
        drop(pinned);
        drop(db);
        assert!(Database::open(&dir.0, id).is_err());
        let raw = DB::open_default(&dir.0).unwrap();
        assert_eq!(raw.get(repo::FINALIZED).unwrap(), Some(encoded));
    }
    let dir = Temp::new();
    let id = identity();
    let db = Database::open(&dir.0, id.clone()).unwrap();
    let List(mut fields) = ae_trie::Value::decode(&id.encode()).unwrap() else {
        panic!()
    };
    let Bytes(version) = &fields[0] else { panic!() };
    let version = ae_types::Unsigned::from_field_bytes(version)
        .unwrap()
        .to_u64()
        .unwrap();
    fields[0] = Bytes(
        ae_types::Unsigned::from(version.checked_add(1).unwrap())
            .as_bytes()
            .to_vec(),
    );
    let encoded = List(fields).encode();
    db.db.put(repo::IDENTITY, &encoded).unwrap();
    drop(db);
    assert!(Database::open(&dir.0, id).is_err());
    let raw = DB::open_default(&dir.0).unwrap();
    assert_eq!(raw.get(repo::IDENTITY).unwrap(), Some(encoded));
}

#[test]
fn startup_community_recovery_is_atomic_bounded_and_idempotent() {
    use ae_chain::{
        block::Block,
        repository::{Change, Record, rollback},
    };
    use ae_state::StateRoots;
    // Seed already-admitted persistence directly: this tests startup recovery,
    // not proof of work or a historical mainnet chain replay.
    let Header::Key(mut g) = genesis::header(Network::Uat).unwrap() else {
        panic!()
    };
    g.common.state_hash = StateRoots::default().hash().0;
    let genesis = Block::Key(g.clone());
    let gh = genesis.hash().unwrap();
    let mut k = g.clone();
    k.common.height = rollback::COMMUNITY_FORK_HEIGHT;
    k.common.previous_hash = gh.0;
    k.common.previous_key_hash = gh.0;
    k.time = 1000;
    let checkpoint = Block::Key(k.clone());
    let ch = checkpoint.hash().unwrap();
    k.common.height += 1;
    k.common.previous_hash = ch.0;
    k.common.previous_key_hash = ch.0;
    k.time = 2000;
    let top = Block::Key(k);
    let th = top.hash().unwrap();
    let id = Identity::new(&NetworkProfile::mainnet(), gh, Hash256::ZERO);
    let dir = Temp::new();
    let db = Database::open(&dir.0, id.clone()).unwrap();
    let record = Record {
        roots: Roots {
            state: StateRoots::default(),
            names: None,
            oracles: None,
        },
        difficulty: 1.into(),
        fees: 0.into(),
        fraud: false,
        fork: gh,
    };
    let mut batch = WriteBatch::default();
    for b in [&genesis, &checkpoint, &top] {
        let h = b.hash().unwrap();
        let header = b.header();
        batch.put(repo::key(repo::BLOCKS, h.0), b.encode().unwrap());
        batch.put(repo::key(repo::RECORDS, h.0), record.encode());
        batch.put(repo::key(repo::CANONICAL, h.0), []);
        batch.put(
            repo::key(repo::KEYS, header.common().height.to_be_bytes()),
            h.0,
        );
        let mut indexed = header.common().height.to_be_bytes().to_vec();
        indexed.extend(h.0);
        batch.put(repo::key(repo::HEIGHTS, indexed), []);
        let mut child = header.common().previous_hash.to_vec();
        child.extend(h.0);
        batch.put(repo::key(repo::CHILDREN, child), []);
    }
    batch.put(repo::key(repo::ENDS, th.0), []);
    batch.put(
        repo::STATUS,
        Status {
            revision: 1,
            tip: Some(th),
        }
        .encode(),
    );
    batch.put(
        repo::FINALIZED,
        repo::encode_finalized(rollback::COMMUNITY_FORK_HEIGHT + 12).unwrap(),
    );
    batch.put(
        repo::key(repo::EVENTS, 1u64.to_be_bytes()),
        Change {
            revision: 1,
            inserted: Some(th),
            removed: vec![],
            discarded: vec![],
            old_tip: None,
            new_tip: th,
            ancestor: None,
            detached: vec![],
            attached: vec![th],
            requeue: vec![],
            confirm: vec![],
        }
        .encode(),
    );
    db.db.write_opt(batch, &write_options()).unwrap();
    let before = db.view().unwrap().source().prefix(&[], 100).unwrap();
    let size = rollback::prepare(
        &db.view().unwrap(),
        rollback::Target::Height(rollback::COMMUNITY_FORK_HEIGHT),
        Default::default(),
    )
    .unwrap()
    .writes()
    .len();
    for failure in 0..=size {
        let plan = rollback::prepare(
            &db.view().unwrap(),
            rollback::Target::Height(rollback::COMMUNITY_FORK_HEIGHT),
            Default::default(),
        )
        .unwrap();
        let mut checks = 0;
        assert!(
            db.commit_checked(plan, || {
                let step = checks;
                checks += 1;
                if step == failure {
                    Err(Error::Backend("injected rollback write".into()))
                } else {
                    Ok(())
                }
            })
            .is_err()
        );
        assert_eq!(
            db.view().unwrap().source().prefix(&[], 100).unwrap(),
            before
        );
    }
    eprintln!("Rollback failure prefixes {}", size + 1);
    drop(db);
    assert!(matches!(
        Database::open_with_recovery_limits(
            &dir.0,
            id.clone(),
            Default::default(),
            rollback::Limits {
                blocks: 0,
                ..Default::default()
            }
        ),
        Err(Error::ResourceLimit)
    ));
    let raw = DB::open_default(&dir.0).unwrap();
    assert_eq!(
        Status::decode(&raw.get(repo::STATUS).unwrap().unwrap())
            .unwrap()
            .tip,
        Some(th)
    );
    drop(raw);
    let db = Database::open(&dir.0, id.clone()).unwrap();
    let v = db.view().unwrap();
    v.verify_tip().unwrap();
    assert_eq!(
        v.status(),
        Status {
            revision: 2,
            tip: Some(ch)
        }
    );
    assert!(v.block(th).unwrap().is_none());
    assert_eq!(v.chain_ends(10).unwrap(), vec![ch]);
    assert_eq!(
        v.finalized_height().unwrap(),
        Some(rollback::COMMUNITY_FORK_HEIGHT + 12)
    );
    assert_eq!(v.event(2).unwrap().unwrap().removed, vec![th]);
    drop(v);
    drop(db);
    let db = Database::open(&dir.0, id).unwrap();
    assert_eq!(db.view().unwrap().status().revision, 2);
}

#[test]
fn persisted_consensus_binds_header_mode_without_changing_legacy_pow_archive_ownership() {
    use ae_codec::headers::HeaderMode;
    use ae_protocol::ConsensusType;
    use ae_trie::Value;
    let pow = identity();
    let Value::List(fields) = Value::decode(&pow.encode()).unwrap() else {
        panic!()
    };
    assert_eq!(fields.len(), 6);
    let mut legacy = fields[..5].to_vec();
    legacy[0] = Value::Bytes(vec![7]);
    assert_eq!(pow.binding(), Value::List(legacy.clone()).encode());
    for version in 1..=9 {
        legacy[0] = Value::Bytes(vec![version]);
        assert_eq!(
            Identity::decode(&Value::List(legacy.clone()).encode()).unwrap(),
            pow
        );
    }
    let pos = Identity::for_consensus(
        &NetworkProfile::uat(),
        pow.genesis,
        pow.execution_profile,
        ConsensusType::Pos,
    );
    assert_ne!(pos.binding(), pow.binding());
    assert_eq!(Identity::decode(&pos.encode()).unwrap(), pos);
    for malformed in [
        Value::List(Vec::new()),
        Value::List(fields[..5].to_vec()),
        Value::List([fields.clone(), vec![Value::Bytes(Vec::new())]].concat()),
        Value::List([vec![Value::Bytes(vec![9])], fields[1..].to_vec()].concat()),
    ] {
        assert!(Identity::decode(&malformed.encode()).is_err());
    }
    let temp = Temp::new();
    let config = ReadConfig {
        mode: HeaderMode::Hyperchain,
        ..Default::default()
    };
    assert!(matches!(
        Database::open_with_config(&temp.0, pow.clone(), config.clone()),
        Err(Error::Identity)
    ));
    assert!(!temp.0.exists());
    let db = Database::open(&temp.0, pow.clone()).unwrap();
    let original = db.db.get(repo::IDENTITY).unwrap().unwrap();
    drop(db);
    assert!(matches!(
        Database::open_with_config(&temp.0, pos, config),
        Err(Error::Identity)
    ));
    let db = Database::open(&temp.0, pow.clone()).unwrap();
    assert_eq!(db.db.get(repo::IDENTITY).unwrap().unwrap(), original);
    // Unknown consensus encodings are corruption, not a fallback to PoW.
    let Value::List(mut fields) = Value::decode(&pow.encode()).unwrap() else {
        panic!()
    };
    fields[5] = Value::Bytes(vec![2]);
    let bad = Value::List(fields).encode();
    assert!(Identity::decode(&bad).is_err());
    db.db.put(repo::IDENTITY, &bad).unwrap();
    drop(db);
    assert!(Database::open(&temp.0, pow).is_err());
    let raw = DB::open_default(&temp.0).unwrap();
    assert_eq!(raw.get(repo::IDENTITY).unwrap(), Some(bad));
}
