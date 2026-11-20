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
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
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
fn identity() -> Identity {
    Identity::new(
        &NetworkProfile::uat(),
        genesis::header(Network::Uat).unwrap().hash().unwrap(),
        Hash256::ZERO,
    )
}
fn plan(db: &Database) -> Update {
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
fn legacy_schema_upgrade_retains_canonical_transactions_and_all_existing_records() {
    for schema in 1..=3 {
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
    for schema in 1..=3 {
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
    fields[0] = Bytes(vec![5]);
    let encoded = List(fields).encode();
    db.db.put(repo::IDENTITY, &encoded).unwrap();
    drop(db);
    assert!(Database::open(&dir.0, id).is_err());
    let raw = DB::open_default(&dir.0).unwrap();
    assert_eq!(raw.get(repo::IDENTITY).unwrap(), Some(encoded));
}
