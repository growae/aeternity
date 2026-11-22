use super::*;
use ae_chain::{
    block::Block,
    repository::{Change, Record, Roots, ledger, retention::Tree},
};
use ae_codec::headers::Header;
use ae_protocol::NetworkProfile;
use ae_state::StateRoots;
use ae_state::{
    Trees,
    genesis::{self, Network},
};
use ae_trie::{Nibbles, Trie, Value};
use std::{collections::BTreeSet, path::PathBuf};
mod events;
pub(crate) mod service;

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "ae-gc-{}-{}",
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
fn setup(path: &Path) -> (Database, Vec<(Hash256, Roots)>) {
    let db = Database::open(path, identity()).unwrap();
    let Header::Key(mut header) = genesis::header(Network::Uat).unwrap() else {
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
    db.commit(
        ledger::bootstrap(
            &db.view().unwrap(),
            &NetworkProfile::uat(),
            &header,
            &trees,
            Default::default(),
        )
        .unwrap(),
    )
    .unwrap();
    let mut previous = identity().genesis;
    let mut roots = vec![(previous, Roots::of(&trees))];
    let mut tries: [Trie; 8] = std::array::from_fn(|_| Trie::default());
    for (height, sibling) in [
        (1, false),
        (2, false),
        (3, false),
        (4, false),
        (3, true),
        (4, true),
        (2, true),
    ] {
        for (i, trie) in tries.iter_mut().enumerate() {
            trie.put(&Nibbles::from_bytes(&[0xaa]), Value::Bytes(vec![99; 64]))
                .unwrap();
            trie.put(
                &Nibbles::from_bytes(&[0xbb]),
                Value::Bytes(vec![
                    i as u8
                        + height as u8 * 10
                        + if sibling { 100 } else { 0 };
                    64
                ]),
            )
            .unwrap();
        }
        let roots_at = Roots {
            state: StateRoots {
                accounts: tries[0].root_hash(),
                calls: tries[1].root_hash(),
                channels: tries[2].root_hash(),
                contracts: tries[3].root_hash(),
                names: tries[4].root_hash(),
                oracles: tries[5].root_hash(),
            },
            names: tries[6].root_hash(),
            oracles: tries[7].root_hash(),
        };
        header.common.height = height;
        let parent = if sibling {
            roots[height as usize - 1].0
        } else {
            previous
        };
        header.common.previous_hash = parent.0;
        header.common.previous_key_hash = parent.0;
        header.common.state_hash = roots_at.state.hash().0;
        let block = Block::Key(header.clone());
        let hash = block.hash().unwrap();
        let record = Record {
            roots: roots_at.clone(),
            difficulty: height.into(),
            fees: 0.into(),
            fraud: false,
            fork: identity().genesis,
        };
        // These headers and arbitrary leaf values are assumed admitted. This
        // tests durable GC, not consensus validity or domain-object decoding.
        let mut batch = WriteBatch::default();
        batch.put(repo::key(repo::BLOCKS, hash.0), block.encode().unwrap());
        batch.put(repo::key(repo::RECORDS, hash.0), record.encode());
        if !sibling {
            batch.put(repo::key(repo::CANONICAL, hash.0), []);
            batch.put(repo::key(repo::KEYS, height.to_be_bytes()), hash.0);
            let change = Change {
                revision: height + 1,
                inserted: Some(hash),
                removed: vec![],
                discarded: vec![],
                old_tip: Some(previous),
                new_tip: hash,
                ancestor: Some(previous),
                detached: vec![],
                attached: vec![hash],
                requeue: vec![],
                confirm: vec![],
            };
            batch.put(
                repo::STATUS,
                Status {
                    revision: height + 1,
                    tip: Some(hash),
                }
                .encode(),
            );
            batch.put(
                repo::key(repo::EVENTS, (height + 1).to_be_bytes()),
                change.encode(),
            );
        }
        for trie in &tries {
            for (h, b) in trie.store().nodes() {
                batch.put(repo::key(repo::NODES, h.0), b);
            }
        }
        db.db.write_opt(batch, &write_options()).unwrap();
        if !sibling {
            previous = hash;
        }
        roots.push((hash, roots_at));
    }
    let mut orphan = Trie::default();
    orphan
        .put(&Nibbles::from_bytes(&[0xff]), Value::Bytes(vec![255; 64]))
        .unwrap();
    for (h, b) in orphan.store().nodes() {
        db.db.put(repo::key(repo::NODES, h.0), b).unwrap();
    }
    db.view().unwrap().verify_tip().unwrap();
    (db, roots)
}
fn all(db: &Database) -> repo::Entries {
    db.view().unwrap().source().prefix(&[], 10000).unwrap()
}
fn node_set(db: &Database) -> BTreeSet<Hash256> {
    db.view()
        .unwrap()
        .source()
        .prefix(&[repo::NODES], 10000)
        .unwrap()
        .into_iter()
        .map(|(k, _)| Hash256::try_from(&k[1..]).unwrap())
        .collect()
}
fn expected(db: &Database, roots: &[(Hash256, Roots)], state: &State) -> BTreeSet<Hash256> {
    let view = db.view().unwrap();
    let mut hashes = BTreeSet::new();
    for (hash, root) in roots {
        let height = view.require_block(*hash).unwrap().header().common().height;
        for (i, h) in root.array().into_iter().enumerate() {
            if state.retains(i, height)
                && let Some(h) = h
            {
                hashes.extend(
                    Trie::from_root(Some(h), view.source().clone())
                        .unwrap()
                        .reachable_nodes()
                        .unwrap()
                        .nodes()
                        .keys()
                        .copied(),
                );
            }
        }
    }
    hashes
}
fn finish(db: &Database, rows: usize) -> State {
    for _ in 0..1000 {
        let state = db
            .gc_step(Limits {
                rows,
                ..Default::default()
            })
            .unwrap();
        if !state.active() {
            assert_eq!(state.phase, Phase::Complete);
            return state;
        }
    }
    panic!("GC did not complete")
}
#[test]
fn selective_collection_preserves_shared_nodes_anchors_and_pinned_snapshots() {
    for trees in [
        Selection::default(),
        Selection::new(&[Tree::Names]).unwrap(),
        Selection::new(&[Tree::Accounts, Tree::Contracts]).unwrap(),
        Selection::new(&[Tree::Oracles]).unwrap(),
    ] {
        for rows in [1, 7, 100] {
            let dir = Temp::new();
            let (db, roots) = setup(&dir.0);
            let before = node_set(&db);
            let pinned = db.view().unwrap();
            let mut old_scan = pinned.scan_state(roots[1].0, Default::default()).unwrap();
            assert!(db.begin_gc(trees).unwrap());
            assert!(matches!(db.begin_gc(trees), Err(Error::GcActive)));
            let required = expected(&db, &roots, &db.gc_state().unwrap());
            let state = finish(&db, rows);
            assert_eq!(node_set(&db), required);
            assert_eq!(state.removed as usize, before.len() - required.len());
            assert_eq!(state.nodes as usize, required.len());
            assert!(state.removed > 0);
            assert_eq!(state.records, roots.len() as u64);
            assert!(matches!(
                db.view().unwrap().trees(roots[3].0, Default::default()),
                Err(Error::Pruned {
                    height: 3,
                    floor: 4
                })
            ));
            assert!(
                db.view()
                    .unwrap()
                    .trees(roots[4].0, Default::default())
                    .is_ok()
            );
            while !old_scan.is_complete() {
                old_scan.next_page(Default::default()).unwrap();
            }
            assert_eq!(pinned.retention().unwrap().epoch, 0);
            db.validate_gc().unwrap();
            drop(old_scan);
            drop(pinned);
            drop(db);
            let db = Database::open(&dir.0, identity()).unwrap();
            assert_eq!(db.gc_state().unwrap(), state);
            assert_eq!(node_set(&db), required);
        }
    }
}
#[test]
fn every_gc_page_reopens_with_the_same_cursor_and_eventual_result() {
    let dir = Temp::new();
    let (mut db, roots) = setup(&dir.0);
    db.begin_gc(Selection::default()).unwrap();
    let required = expected(&db, &roots, &db.gc_state().unwrap());
    let mut pages = 0;
    loop {
        let state = db
            .gc_step(Limits {
                rows: 3,
                ..Default::default()
            })
            .unwrap();
        pages += 1;
        drop(db);
        db = Database::open(&dir.0, identity()).unwrap();
        assert_eq!(db.gc_state().unwrap(), state);
        if !state.active() {
            break;
        }
        assert!(pages < 200);
    }
    assert!(pages > 20);
    assert_eq!(node_set(&db), required);
}
#[test]
fn work_limits_missing_nodes_and_injected_failures_publish_nothing() {
    let dir = Temp::new();
    let (db, roots) = setup(&dir.0);
    let prior = all(&db);
    assert!(
        db.begin_gc_checked(Selection::default(), None, || Err(Error::Backend(
            "injected".into()
        )))
        .is_err()
    );
    assert_eq!(all(&db), prior);
    db.begin_gc(Selection::default()).unwrap();
    let prior = all(&db);
    for limits in [
        Limits {
            rows: 0,
            bytes: 1000,
        },
        Limits { rows: 1, bytes: 1 },
    ] {
        assert!(matches!(db.gc_step(limits), Err(Error::ResourceLimit)));
        assert_eq!(all(&db), prior);
    }
    assert!(
        db.gc_step_checked(Default::default(), || Err(Error::Backend(
            "injected".into()
        )))
        .is_err()
    );
    assert_eq!(all(&db), prior);
    let hash = roots[4].1.names.unwrap();
    let key = repo::key(repo::NODES, hash.0);
    let raw = db.db.get(&key).unwrap().unwrap();
    db.db.delete(&key).unwrap();
    let damaged = all(&db);
    assert!(db.gc_step(Default::default()).is_err());
    assert_eq!(all(&db), damaged);
    db.db.put(&key, raw).unwrap();
    finish(&db, 7);
}
#[test]
fn cancellation_keeps_retention_and_tree_selection_can_change_without_resurrecting_old_state() {
    let dir = Temp::new();
    let (db, roots) = setup(&dir.0);
    db.begin_gc(Selection::new(&[Tree::Names]).unwrap())
        .unwrap();
    let old = expected(&db, &roots, &db.gc_state().unwrap());
    finish(&db, 100);
    assert_eq!(node_set(&db), old);
    db.begin_gc(Selection::new(&[Tree::Accounts]).unwrap())
        .unwrap();
    let next = expected(&db, &roots, &db.gc_state().unwrap());
    let cancelled = db.cancel_gc().unwrap();
    assert_eq!(cancelled.phase, Phase::Cancelled);
    assert_eq!(cancelled.floors, [4, 0, 0, 0, 4, 0, 4, 0]);
    assert!(matches!(
        db.view().unwrap().require_retained(3),
        Err(Error::Pruned { .. })
    ));
    db.begin_gc(Selection::new(&[Tree::Accounts]).unwrap())
        .unwrap();
    assert_eq!(finish(&db, 3).epoch, 3);
    assert_eq!(node_set(&db), next);
}
#[test]
fn corrupt_gc_metadata_and_orphaned_work_are_rejected_on_open() {
    let dir = Temp::new();
    let (db, _) = setup(&dir.0);
    db.db.put(repo::GC_STATE, [0]).unwrap();
    drop(db);
    assert!(matches!(
        Database::open(&dir.0, identity()),
        Err(Error::Corrupt(_))
    ));
    let dir = Temp::new();
    let (db, _) = setup(&dir.0);
    db.db.put(repo::key(repo::GC_PENDING, [1; 32]), []).unwrap();
    drop(db);
    assert!(matches!(
        Database::open(&dir.0, identity()),
        Err(Error::Corrupt("orphaned GC work"))
    ));
}

#[test]
fn lost_pending_work_stops_before_deletion_and_cancellation_keeps_the_floor() {
    let dir = Temp::new();
    let (db, _) = setup(&dir.0);
    let before = node_set(&db);
    db.begin_gc(Selection::default()).unwrap();
    let pending = db
        .view()
        .unwrap()
        .source()
        .prefix(&[repo::GC_PENDING], 100)
        .unwrap();
    db.db.delete(&pending[0].0).unwrap();
    let mut rejected = false;
    for _ in 0..1000 {
        match db.gc_step(Limits {
            rows: 3,
            ..Default::default()
        }) {
            Ok(state) => assert_eq!(state.phase, Phase::Roots),
            Err(Error::Corrupt("GC pending count")) => {
                rejected = true;
                break;
            }
            other => panic!("unexpected GC result: {other:?}"),
        }
    }
    assert!(rejected);
    assert_eq!(node_set(&db), before);
    let state = db.cancel_gc().unwrap();
    assert_eq!(state.pending, 0);
    assert_eq!(state.height, 4);
    assert_eq!(state.phase, Phase::Cancelled);
    drop(db);
    assert_eq!(
        Database::open(&dir.0, identity())
            .unwrap()
            .gc_state()
            .unwrap(),
        state
    );
}

fn replace(db: &Database, rows: &repo::Entries) {
    let mut batch = WriteBatch::default();
    batch.delete_range(&[][..], &[255][..]);
    for (k, v) in rows {
        batch.put(k, v);
    }
    db.db.write_opt(batch, &write_options()).unwrap();
}
fn action(db: &Database, operation: &str) {
    match operation {
        "begin" => {
            db.begin_gc(Selection::default()).unwrap();
        }
        "cancel" => {
            db.cancel_gc().unwrap();
        }
        "mark" | "sweep" | "finish" => {
            db.gc_step(Limits {
                rows: 1,
                ..Default::default()
            })
            .unwrap();
        }
        _ => panic!("unknown GC action"),
    }
}
fn seed_action(db: &Database, operation: &str) {
    if operation == "begin" {
        return;
    }
    db.begin_gc(Selection::default()).unwrap();
    if operation == "mark" || operation == "cancel" {
        return;
    }
    for _ in 0..1000 {
        let before = all(db);
        let removed = db.gc_state().unwrap().removed;
        let after = db
            .gc_step(Limits {
                rows: 1,
                ..Default::default()
            })
            .unwrap();
        if (operation == "sweep" && after.removed > removed)
            || (operation == "finish" && after.phase == Phase::Complete)
        {
            replace(db, &before);
            return;
        }
    }
    panic!("failed to reach requested GC page");
}
#[test]
fn gc_crash_worker() {
    use std::io::{BufRead, Write};
    let Ok(path) = std::env::var("AE_GC_CRASH_PATH") else {
        return;
    };
    let operation = std::env::var("AE_GC_CRASH_OPERATION").unwrap();
    let db = Database::open(path, identity()).unwrap();
    println!("AE_GC_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "commit");
    action(&db, &operation);
    println!("AE_GC_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn killed_gc_processes_recover_exactly_one_complete_maintenance_batch() {
    use std::{
        io::{BufRead, Write},
        process::{Command, Stdio},
    };
    struct Worker(std::process::Child);
    impl Drop for Worker {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for operation in ["begin", "mark", "sweep", "finish", "cancel"] {
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let dir = Temp::new();
            let (db, _) = setup(&dir.0);
            seed_action(&db, operation);
            let before = all(&db);
            action(&db, operation);
            let committed = all(&db);
            assert_ne!(before, committed);
            replace(&db, &before);
            drop(db);
            let mut child = Worker(
                Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "gc::tests::gc_crash_worker", "--nocapture"])
                    .env("AE_GC_CRASH_PATH", &dir.0)
                    .env("AE_GC_CRASH_OPERATION", operation)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .unwrap(),
            );
            let mut output = std::io::BufReader::new(child.0.stdout.take().unwrap());
            let wait_for = |output: &mut std::io::BufReader<std::process::ChildStdout>,
                            marker: &str| loop {
                let mut line = String::new();
                assert_ne!(
                    output.read_line(&mut line).unwrap(),
                    0,
                    "GC worker ended before {marker}"
                );
                if line.contains(marker) {
                    break;
                }
            };
            wait_for(&mut output, "AE_GC_READY");
            if phase != "before" {
                child
                    .0
                    .stdin
                    .as_mut()
                    .unwrap()
                    .write_all(b"commit\n")
                    .unwrap();
                child.0.stdin.as_mut().unwrap().flush().unwrap();
                if phase == "after" {
                    wait_for(&mut output, "AE_GC_DURABLE");
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(
                        phase.strip_prefix("race").unwrap().parse().unwrap(),
                    ));
                }
            }
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            let db = Database::open(&dir.0, identity()).unwrap();
            let recovered = all(&db);
            match phase {
                "before" => assert_eq!(recovered, before),
                "after" => assert_eq!(recovered, committed),
                _ => assert!(recovered == before || recovered == committed),
            }
            if db.gc_state().unwrap().active() {
                finish(&db, 7);
            }
            db.view().unwrap().verify_tip().unwrap();
            let top = db.view().unwrap().status().tip.unwrap();
            let mut scan = db
                .view()
                .unwrap()
                .scan_state(top, Default::default())
                .unwrap();
            while !scan.is_complete() {
                scan.next_page(Default::default()).unwrap();
            }
            println!("GC crash recovery {operation}/{phase}: complete batch");
        }
    }
}
