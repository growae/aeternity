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
