use ae_chain::{
    block::{Block, MicroBlock},
    repository::{
        self as repo, Identity, Read, Roots, Status,
        ledger::{self, Limits},
    },
};
use ae_codec::{
    api::{self, ApiType},
    headers::{Header, HeaderMode, KeyHeader},
};
use ae_execution::transactions;
use ae_protocol::NetworkProfile;
use ae_state::{
    Trees,
    genesis::{self, Network},
};
use ae_storage::Database;
use ae_types::Hash256;
use serde_json::Value as J;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "ae-chain-store-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn profile(n: Network) -> NetworkProfile {
    match n {
        Network::Mainnet => NetworkProfile::mainnet(),
        Network::Uat => NetworkProfile::uat(),
    }
}
fn identity(n: Network) -> Identity {
    Identity::new(
        &profile(n),
        genesis::header(n).unwrap().hash().unwrap(),
        ae_crypto::blake2b_256(b"bounded-pow-replay"),
    )
}
fn genesis_trees(n: Network) -> Trees {
    Trees {
        accounts: genesis::state(n).unwrap().0,
        contracts: ae_state::contracts::Contracts::new(Default::default()),
        calls: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    }
}
fn genesis_key(n: Network) -> KeyHeader {
    let Header::Key(h) = genesis::header(n).unwrap() else {
        panic!()
    };
    h
}
fn rules(n: Network) -> ae_chain::key::Rules {
    ae_chain::key::Rules::new(
        ae_chain::admission::PowRules {
            now_ms: u64::MAX - 600_000,
            micro_cycle_ms: 3000,
            configured_gas_limit: 6_000_000.into(),
            body_limits: Default::default(),
        },
        identity(n).genesis,
    )
}
fn blocks(n: Network) -> Vec<Block> {
    let d: J = serde_json::from_str(include_str!("fixtures/public_bootstrap.json")).unwrap();
    let net = profile(n);
    let id = std::str::from_utf8(net.network_id()).unwrap();
    let mut blocks = vec![];
    for r in d["generations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["network"] == id)
    {
        let Header::Key(h) = Header::decode(
            &hex::decode(r["hex"].as_str().unwrap()).unwrap(),
            HeaderMode::ProofOfWork,
        )
        .unwrap() else {
            panic!()
        };
        blocks.push(Block::Key(h));
        for hash in r["micro_blocks"].as_array().unwrap() {
            let m = d["micros"]
                .as_array()
                .unwrap()
                .iter()
                .find(|m| m["hash"] == *hash)
                .unwrap();
            let Header::Micro(header) = Header::decode(
                &hex::decode(m["hex"].as_str().unwrap()).unwrap(),
                HeaderMode::ProofOfWork,
            )
            .unwrap() else {
                panic!()
            };
            let transactions = m["transactions"]["transactions"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    let b = api::decode_as(ApiType::Transaction, r["encoded_tx"].as_str().unwrap())
                        .unwrap();
                    let tx = ae_tx::SignedTransaction::decode(&b).unwrap();
                    assert_eq!(tx.encode(), b);
                    assert_eq!(
                        tx.hash().0.to_vec(),
                        api::decode_as(ApiType::TxHash, r["hash"].as_str().unwrap()).unwrap()
                    );
                    tx
                })
                .collect();
            let b = Block::Micro(MicroBlock {
                header,
                transactions,
                proof: None,
            });
            assert_eq!(
                b.hash().unwrap().0.to_vec(),
                api::decode_as(ApiType::MicroBlockHash, hash.as_str().unwrap()).unwrap()
            );
            blocks.push(b);
        }
    }
    blocks
}
fn bootstrap(db: &Database, n: Network) {
    let v = db.view().unwrap();
    let update = ledger::bootstrap(
        &v,
        &profile(n),
        &genesis_key(n),
        &genesis_trees(n),
        Limits::default(),
    )
    .unwrap();
    db.commit(update).unwrap();
}
#[test]
fn actual_public_generations_replay_persist_and_reopen_with_transaction_locations() {
    for n in [Network::Mainnet, Network::Uat] {
        let temp = Temp::new();
        let id = identity(n);
        let network = profile(n);
        let config = transactions::Config::primitives(&network);
        let all = blocks(n);
        let db = Database::open(&temp.0, id.clone()).unwrap();
        bootstrap(&db, n);
        let pinned = db.view().unwrap();
        let genesis_status = pinned.status();
        let genesis_roots = pinned.require_record(id.genesis).unwrap().roots;
        let mut last_tx = None;
        for (i, b) in all.iter().enumerate() {
            let view = db.view().unwrap();
            let before = view.status();
            let h = b.hash().unwrap();
            let plan = ledger::prepare(&view, b, &config, &rules(n), Default::default()).unwrap();
            let committed = db.commit(plan).unwrap();
            assert_eq!(committed.change().old_tip, before.tip);
            assert_eq!(committed.change().new_tip, h);
            assert_eq!(committed.change().attached, vec![h]);
            assert!(committed.change().detached.is_empty());
            assert!(view.block(h).unwrap().is_none());
            assert_eq!(view.status(), before);
            let after = db.view().unwrap();
            assert_eq!(
                after.status(),
                Status {
                    revision: i as u64 + 2,
                    tip: Some(h)
                }
            );
            assert_eq!(after.require_block(h).unwrap(), *b);
            assert_eq!(
                after.require_record(h).unwrap().roots.state.hash().0,
                b.header().common().state_hash
            );
            let t = after.trees(h, Default::default()).unwrap();
            assert_eq!(Roots::of(&t), after.require_record(h).unwrap().roots);
            let record = after.require_record(h).unwrap();
            if let Block::Micro(m) = b {
                for tx in &m.transactions {
                    assert_eq!(after.location(tx.hash()).unwrap(), Some(h));
                    last_tx = Some((tx.hash(), h));
                }
                assert_eq!(record.fees, 101014u64.into());
            } else if matches!(n, Network::Mainnet) && b.header().common().height == 2 {
                assert_eq!(record.fees, 101014u64.into());
            } else {
                assert_eq!(record.fees, 0.into());
            }
            assert!(!record.fraud);
            assert_eq!(record.fork, id.genesis);
            after.verify_tip().unwrap();
        }
        assert_eq!(pinned.status(), genesis_status);
        assert_eq!(
            pinned.require_record(id.genesis).unwrap().roots,
            genesis_roots
        );
        assert!(pinned.block(all[0].hash().unwrap()).unwrap().is_none());
        drop(pinned);
        drop(db);
        let db = Database::open(&temp.0, id).unwrap();
        let v = db.view().unwrap();
        let tip = all.last().unwrap().hash().unwrap();
        assert_eq!(v.status().tip, Some(tip));
        assert_eq!(v.chain_ends(100).unwrap(), vec![tip]);
        for revision in 1..=v.status().revision {
            assert_eq!(v.event(revision).unwrap().unwrap().revision, revision);
        }
        assert!(v.event(v.status().revision + 1).unwrap().is_none());
        if let Some((tx, block)) = last_tx {
            assert_eq!(v.location(tx).unwrap(), Some(block));
        }
    }
}
#[test]
fn incomplete_or_incompatible_genesis_cannot_be_published() {
    use ae_trie::{MemoryStore, Nibbles, NodeReader, NodeWriter, Trie, Value};
    let n = Network::Mainnet;
    let dir = Temp::new();
    let db = Database::open(&dir.0, identity(n)).unwrap();
    let v = db.view().unwrap();
    let mut trees = genesis_trees(n);
    let root = trees.accounts.root_hash().unwrap();
    let mut root_only = MemoryStore::default();
    root_only
        .put_node(
            root,
            trees.accounts.tree().store().node(&root).unwrap().unwrap(),
        )
        .unwrap();
    trees.accounts = ae_state::accounts::Accounts::from_root(Some(root), root_only).unwrap();
    assert!(matches!(
        ledger::bootstrap(&v, &profile(n), &genesis_key(n), &trees, Default::default()),
        Err(ledger::InsertError::Repository(repo::Error::Trie(
            ae_trie::Error::MissingNode(_)
        )))
    ));
    let mut trees = genesis_trees(n);
    let mut cache = Trie::new(MemoryStore::default());
    cache
        .put(&Nibbles::from_bytes(b"unexpected"), Value::Bytes(vec![1]))
        .unwrap();
    trees.names = ae_state::names::Names::from_roots(
        None,
        cache.root_hash(),
        MemoryStore::default(),
        cache.store().clone(),
    )
    .unwrap();
    assert!(matches!(
        ledger::bootstrap(&v, &profile(n), &genesis_key(n), &trees, Default::default()),
        Err(ledger::InsertError::WrongGenesis)
    ));
    assert_eq!(db.view().unwrap().status(), Status::default());
    let custom = NetworkProfile::custom(
        b"storage-test".to_vec(),
        vec![ae_protocol::Activation {
            height: 0,
            protocol: ae_protocol::Protocol::Roma,
        }],
    )
    .unwrap();
    for mode in 0..3 {
        let mut h = genesis_key(n);
        match mode {
            0 => h.hole = true,
            1 => h.end_of_epoch = true,
            _ => h.common.version = 2,
        }
        let id = Identity::new(
            &custom,
            Header::Key(h.clone()).hash().unwrap(),
            Hash256::ZERO,
        );
        let dir = Temp::new();
        let db = Database::open(&dir.0, id).unwrap();
        assert!(matches!(
            ledger::bootstrap(
                &db.view().unwrap(),
                &custom,
                &h,
                &genesis_trees(n),
                Default::default()
            ),
            Err(ledger::InsertError::WrongGenesis)
        ));
        assert_eq!(db.view().unwrap().status(), Status::default());
    }
    bootstrap(&db, n);
}
#[test]
fn stale_views_and_foreign_database_plans_cannot_publish() {
    let n = Network::Uat;
    let dir = Temp::new();
    let db = Database::open(&dir.0, identity(n)).unwrap();
    let v = db.view().unwrap();
    let one = ledger::bootstrap(
        &v,
        &profile(n),
        &genesis_key(n),
        &genesis_trees(n),
        Default::default(),
    )
    .unwrap();
    let two = ledger::bootstrap(
        &v,
        &profile(n),
        &genesis_key(n),
        &genesis_trees(n),
        Default::default(),
    )
    .unwrap();
    db.commit(one).unwrap();
    assert!(matches!(db.commit(two), Err(repo::Error::Conflict)));
    assert_eq!(v.status(), Status::default());
    let other_dir = Temp::new();
    let other = Database::open(&other_dir.0, identity(n)).unwrap();
    let update = ledger::bootstrap(
        &other.view().unwrap(),
        &profile(n),
        &genesis_key(n),
        &genesis_trees(n),
        Default::default(),
    )
    .unwrap();
    assert!(matches!(db.commit(update), Err(repo::Error::Identity)));
    let b = &blocks(n)[0];
    let network = profile(n);
    let config = transactions::Config::primitives(&network);
    let now = db.view().unwrap();
    let a = ledger::prepare(&now, b, &config, &rules(n), Default::default()).unwrap();
    let b = ledger::prepare(&now, b, &config, &rules(n), Default::default()).unwrap();
    std::thread::scope(|s| {
        let x = s.spawn(|| db.commit(a));
        let y = s.spawn(|| db.commit(b));
        let results = [x.join().unwrap(), y.join().unwrap()];
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Err(repo::Error::Conflict)))
                .count(),
            1
        );
    });
    assert_eq!(db.view().unwrap().status().revision, 2);
}
#[test]
fn network_profile_identity_lock_and_local_budgets_are_enforced() {
    let n = Network::Uat;
    let dir = Temp::new();
    let db = Database::open(&dir.0, identity(n)).unwrap();
    assert!(Database::open(&dir.0, identity(n)).is_err());
    let v = db.view().unwrap();
    let limits = Limits {
        commit_bytes: 1,
        ..Default::default()
    };
    assert!(matches!(
        ledger::bootstrap(&v, &profile(n), &genesis_key(n), &genesis_trees(n), limits),
        Err(ledger::InsertError::Repository(repo::Error::ResourceLimit))
    ));
    assert_eq!(db.view().unwrap().status(), Status::default());
    drop(v);
    drop(db);
    let mut wrong = identity(n);
    wrong.execution_profile = Hash256::ZERO;
    assert!(matches!(
        Database::open(&dir.0, wrong),
        Err(repo::Error::Identity)
    ));
    assert!(matches!(
        Database::open(&dir.0, identity(Network::Mainnet)),
        Err(repo::Error::Identity)
    ));
    let db = Database::open(&dir.0, identity(n)).unwrap();
    bootstrap(&db, n);
    let mut b = blocks(n).remove(0);
    let Block::Key(ref mut h) = b else { panic!() };
    h.nonce ^= 1;
    let network = profile(n);
    let config = transactions::Config::primitives(&network);
    let v = db.view().unwrap();
    let e = ledger::prepare(&v, &b, &config, &rules(n), Default::default()).unwrap_err();
    assert!(!e.is_operational());
    assert_eq!(db.view().unwrap().status().revision, 1);
    assert!(v.source().prefix(&[repo::NODES], 0).is_err());
}

#[test]
fn crash_worker() {
    use std::io::{BufRead, Write};
    let Ok(path) = std::env::var("AE_STORAGE_CRASH_PATH") else {
        return;
    };
    let operation = std::env::var("AE_STORAGE_CRASH_OPERATION").unwrap();
    let n = Network::Mainnet;
    let db = Database::open(path, identity(n)).unwrap();
    let view = db.view().unwrap();
    let network = profile(n);
    let config = transactions::Config::primitives(&network);
    let plan = if operation == "genesis" {
        ledger::bootstrap(
            &view,
            &network,
            &genesis_key(n),
            &genesis_trees(n),
            Default::default(),
        )
        .unwrap()
    } else {
        ledger::prepare(&view, &blocks(n)[1], &config, &rules(n), Default::default()).unwrap()
    };
    println!("AE_STORE_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "commit");
    db.commit(plan).unwrap();
    println!("AE_STORE_DURABLE");
    std::io::stdout().flush().unwrap();
    line.clear();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    panic!("worker must be killed by the parent");
}
#[test]
fn killed_processes_recover_only_complete_genesis_or_micro_commits() {
    use std::io::{BufRead, Write};
    use std::process::{Command, Stdio};
    struct Worker(std::process::Child);
    impl Drop for Worker {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let n = Network::Mainnet;
    let network = profile(n);
    let config = transactions::Config::primitives(&network);
    let all = blocks(n);
    for operation in ["genesis", "micro"] {
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let dir = Temp::new();
            let db = Database::open(&dir.0, identity(n)).unwrap();
            if operation == "micro" {
                bootstrap(&db, n);
                let plan = ledger::prepare(
                    &db.view().unwrap(),
                    &all[0],
                    &config,
                    &rules(n),
                    Default::default(),
                )
                .unwrap();
                db.commit(plan).unwrap();
            }
            let before = db.view().unwrap().status();
            drop(db);
            let mut child = Worker(
                Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "crash_worker", "--nocapture"])
                    .env("AE_STORAGE_CRASH_PATH", &dir.0)
                    .env("AE_STORAGE_CRASH_OPERATION", operation)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .unwrap(),
            );
            let mut output = std::io::BufReader::new(child.0.stdout.take().unwrap());
            let wait_for = |out: &mut std::io::BufReader<std::process::ChildStdout>,
                            marker: &str| {
                loop {
                    let mut line = String::new();
                    assert_ne!(
                        out.read_line(&mut line).unwrap(),
                        0,
                        "worker ended before {marker}"
                    );
                    if line.contains(marker) {
                        break;
                    }
                }
            };
            wait_for(&mut output, "AE_STORE_READY");
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
                    wait_for(&mut output, "AE_STORE_DURABLE");
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(
                        phase.strip_prefix("race").unwrap().parse().unwrap(),
                    ));
                }
            }
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            let db = Database::open(&dir.0, identity(n)).unwrap();
            let view = db.view().unwrap();
            let after = view.status();
            assert!(after.revision == before.revision || after.revision == before.revision + 1);
            if phase == "before" {
                assert_eq!(after, before);
            }
            if phase == "after" {
                assert_eq!(after.revision, before.revision + 1);
            }
            view.verify_tip().unwrap();
            let candidate = if operation == "genesis" {
                identity(n).genesis
            } else {
                all[1].hash().unwrap()
            };
            assert_eq!(
                view.block(candidate).unwrap().is_some(),
                after.revision != before.revision
            );
            assert_eq!(
                view.record(candidate).unwrap().is_some(),
                after.revision != before.revision
            );
            if after.revision != before.revision {
                let trees = view.trees(candidate, Default::default()).unwrap();
                assert_eq!(
                    trees.roots().hash().0,
                    view.require_block(candidate)
                        .unwrap()
                        .header()
                        .common()
                        .state_hash
                );
                for item in trees.accounts.tree().iter() {
                    item.unwrap();
                }
                assert_eq!(
                    view.event(after.revision).unwrap().unwrap().inserted,
                    candidate
                );
            }
            if operation == "micro" {
                let Block::Micro(m) = &all[1] else { panic!() };
                let location = view.location(m.transactions[0].hash()).unwrap();
                assert_eq!(
                    location,
                    if after.revision != before.revision {
                        Some(candidate)
                    } else {
                        None
                    }
                );
            }
            eprintln!(
                "Crash recovery {operation}/{phase}: revision {} -> {}",
                before.revision, after.revision
            );
        }
    }
}
