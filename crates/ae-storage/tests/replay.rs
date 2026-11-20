use ae_chain::{
    block::Block,
    interchange::{self, Chunk, Limits, Manifest},
    repository::{self as repo, Identity, ReadConfig, Roots, Update, View, ledger},
};
use ae_codec::headers::Header;
use ae_execution::transactions;
use ae_protocol::NetworkProfile;
use ae_state::{
    Trees,
    genesis::{self, Network},
};
use ae_storage::{
    Database, Snapshot,
    replay::{Archive, Failure, Phase, Replay},
};
use ae_trie::{Value, scan};
use serde_json::Value as J;
use std::{
    path::PathBuf,
    sync::{
        LazyLock,
        atomic::{AtomicU64, Ordering},
    },
};
static DATA: LazyLock<J> =
    LazyLock::new(|| serde_json::from_str(include_str!("fixtures/erlang_replay.json")).unwrap());
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "ae-replay-{}-{}",
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
fn bytes(v: &J) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
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
        ae_crypto::blake2b_256(b"replay-archive-validation"),
    )
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
fn prepare(n: Network, view: &View<Snapshot<'_>>, block: &Block) -> Result<Update, Failure> {
    let network = profile(n);
    if view.status().tip.is_none() {
        let Header::Key(header) = genesis::header(n).unwrap() else {
            unreachable!()
        };
        assert_eq!(block, &Block::Key(header.clone()));
        let trees = Trees {
            accounts: genesis::state(n).unwrap().0,
            contracts: ae_state::contracts::Contracts::new(Default::default()),
            calls: Default::default(),
            channels: Default::default(),
            names: Default::default(),
            oracles: Default::default(),
        };
        ledger::bootstrap(view, &network, &header, &trees, Default::default())
            .map_err(|e| Failure::Validation(format!("{e:?}")))
    } else {
        ledger::prepare(
            view,
            block,
            &transactions::Config::primitives(&network),
            &rules(n),
            Default::default(),
        )
        .map_err(|e| Failure::Validation(format!("{e:?}")))
    }
}
fn row(n: Network) -> &'static J {
    &DATA["networks"][match n {
        Network::Mainnet => 0,
        Network::Uat => 1,
    }]
}
fn manifest(n: Network) -> Manifest {
    Manifest::decode(&bytes(&row(n)["manifest"]), Limits::default()).unwrap()
}
fn open(t: &Temp, n: Network, m: &Manifest) -> Replay {
    Replay::open(
        &t.0,
        identity(n),
        m.clone(),
        ReadConfig::default(),
        Limits::default(),
    )
    .unwrap()
}
fn chunks(n: Network) -> Vec<Vec<u8>> {
    row(n)["chunks"]
        .as_array()
        .unwrap()
        .iter()
        .map(bytes)
        .collect()
}
fn replay_all(r: &mut Replay, n: Network) {
    let c = chunks(n);
    while r.progress().phase == Phase::Replay {
        r.apply_chunk(&c[r.progress().chunk as usize], usize::MAX, |v, b| {
            prepare(n, v, b)
        })
        .unwrap();
    }
}
fn verify_all(r: &mut Replay) {
    while r.progress().phase == Phase::Verify {
        r.verify_step(Default::default(), Default::default())
            .unwrap();
    }
}
fn value(bytes: &[u8]) -> Value {
    Value::from_rlp(&ae_codec::rlp::decode(bytes).unwrap())
}
fn descriptor(b: &[u8], count: u64) -> Chunk {
    Chunk {
        blocks: count,
        bytes: b.len() as u64,
        hash: ae_crypto::blake2b_256(b),
    }
}
#[test]
fn schema_seven_replay_ownership_and_partial_progress_resume_after_upgrade() {
    let n = Network::Mainnet;
    let m = manifest(n);
    let id = identity(n);
    let Value::List(mut fields) = Value::decode(&id.encode()).unwrap() else {
        panic!()
    };
    fields[0] = Value::Bytes(vec![7]);
    let legacy = Value::List(fields).encode();
    assert_eq!(id.binding(), legacy);
    for initialized in [false, true] {
        let temp = Temp::new();
        std::fs::create_dir_all(&temp.0).unwrap();
        let mut bound = legacy.clone();
        bound.extend(m.id().0);
        let marker = temp.0.join(format!(
            ".ae-replay-{}",
            hex::encode(ae_crypto::blake2b_256(&bound).0)
        ));
        std::fs::write(&marker, []).unwrap();
        let prior = if initialized {
            let mut replay = open(&temp, n, &m);
            replay
                .apply_chunk(&chunks(n)[0], 1, |v, b| prepare(n, v, b))
                .unwrap();
            let progress = replay.progress().clone();
            drop(replay);
            let raw = rocksdb::DB::open_default(&temp.0).unwrap();
            raw.put(repo::IDENTITY, &legacy).unwrap();
            drop(raw);
            Some(progress)
        } else {
            // A v7 importer can die after persisting ownership but before
            // RocksDB creates CURRENT. Unrelated contents must not block it.
            std::fs::write(temp.0.join("LOG"), b"interrupted v7 open").unwrap();
            None
        };
        let mut replay = open(&temp, n, &m);
        if let Some(prior) = prior {
            assert_eq!(replay.progress(), &prior);
        }
        replay_all(&mut replay, n);
        verify_all(&mut replay);
        drop(replay.into_database().unwrap());
        assert!(marker.is_file());
        let raw = rocksdb::DB::open_default(&temp.0).unwrap();
        assert_eq!(raw.get(repo::IDENTITY).unwrap(), Some(id.encode()));
    }
}
#[test]
fn original_mnesia_exports_resume_and_execute_the_next_real_block() {
    for n in [Network::Mainnet, Network::Uat] {
        let m = manifest(n);
        assert_eq!(m.encode(), bytes(&row(n)["manifest"]));
        assert_eq!(row(n)["restart_equal"], true);
        for batch in [1, 2, 99] {
            let t = Temp::new();
            let c = chunks(n);
            let mut r = open(&t, n, &m);
            while r.progress().phase == Phase::Replay {
                let before = r.progress().blocks;
                r.apply_chunk(&c[r.progress().chunk as usize], batch, |v, b| {
                    prepare(n, v, b)
                })
                .unwrap();
                assert!(r.progress().blocks > before);
                let expected = r.progress().clone();
                drop(r);
                assert!(matches!(
                    Database::open(&t.0, identity(n)),
                    Err(repo::Error::IncompleteImport)
                ));
                r = open(&t, n, &m);
                assert_eq!(r.progress(), &expected);
            }
            while r.progress().phase == Phase::Verify {
                r.verify_step(
                    Default::default(),
                    scan::PageLimits {
                        nodes: if batch == 1 { 7 } else { 256 },
                        ..Default::default()
                    },
                )
                .unwrap();
                let expected = r.progress().clone();
                drop(r);
                r = open(&t, n, &m);
                assert_eq!(r.progress(), &expected);
            }
            let db = r.into_database().unwrap();
            assert_eq!(
                db.view().unwrap().require_record(m.anchor).unwrap().roots,
                m.roots
            );
            let next =
                Block::decode(&bytes(&row(n)["next"]), ReadConfig::default().block()).unwrap();
            db.commit(prepare(n, &db.view().unwrap(), &next).unwrap())
                .unwrap();
            let v = db.view().unwrap();
            let roots = v.require_record(next.hash().unwrap()).unwrap().roots;
            let expected: Vec<_> = row(n)["next_roots"]
                .as_array()
                .unwrap()
                .iter()
                .map(bytes)
                .collect();
            assert_eq!(
                roots
                    .array()
                    .iter()
                    .map(|r| r.map_or(vec![], |h| h.0.to_vec()))
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                Roots::of(&v.trees(next.hash().unwrap(), Default::default()).unwrap()),
                roots
            );
            drop(v);
            drop(db);
            let db = Database::open(&t.0, identity(n)).unwrap();
            db.rollback(repo::rollback::Target::Height(4), Default::default())
                .unwrap();
            let all: Vec<_> = c
                .iter()
                .enumerate()
                .flat_map(|(i, b)| {
                    interchange::decode_chunk(
                        b,
                        i as u64,
                        &m.chunks[i],
                        &ReadConfig::default(),
                        Limits::default(),
                    )
                    .unwrap()
                })
                .collect();
            let key5 = all
                .iter()
                .find(|b| b.header().common().height == 5)
                .unwrap();
            db.commit(prepare(n, &db.view().unwrap(), key5).unwrap())
                .unwrap();
            db.commit(prepare(n, &db.view().unwrap(), &next).unwrap())
                .unwrap();
            let pinned = db.view().unwrap();
            let old = pinned.trees(m.genesis, Default::default()).unwrap();
            assert!(db.begin_gc(repo::retention::Selection::default()).unwrap());
            while db.gc_step(Default::default()).unwrap().active() {}
            assert_eq!(
                Roots::of(&pinned.trees(m.genesis, Default::default()).unwrap()),
                Roots::of(&old)
            );
            assert_eq!(
                db.view()
                    .unwrap()
                    .require_record(next.hash().unwrap())
                    .unwrap()
                    .roots,
                roots
            );
            drop(old);
            drop(pinned);
            drop(db);
            Database::open(&t.0, identity(n)).unwrap();
        }
    }
}
#[test]
fn all_native_transaction_families_and_internal_contexts_round_trip_losslessly() {
    let config = ReadConfig::default();
    let mut contexts = std::collections::BTreeSet::new();
    let mut count = 0;
    for r in DATA["wire"].as_array().unwrap() {
        let b = bytes(&r["chunk"]);
        let d = descriptor(&b, 1);
        let blocks = interchange::decode_chunk(&b, 0, &d, &config, Limits::default())
            .unwrap_or_else(|e| panic!("{r}: {e:?}"));
        let (actual, encoded) =
            interchange::encode_chunk(0, &blocks, &config, Limits::default()).unwrap();
        assert_eq!(
            d,
            actual,
            "name={} protocol={} diffs={:?}",
            r["name"],
            r["protocol"],
            b.iter()
                .zip(&encoded)
                .enumerate()
                .filter(|(_, (a, b))| a != b)
                .take(10)
                .collect::<Vec<_>>()
        );
        assert_eq!(encoded, b);
        if r["name"] == "context" {
            let records = interchange::transaction_records(&blocks[0], &config).unwrap();
            contexts.insert(records[0].1.clone());
        }
        count += 1;
    }
    assert_eq!(count, 308);
    assert_eq!(contexts.len(), 36);
}
#[test]
fn manifest_modes_limits_and_chunk_integrity_are_checked() {
    let m = manifest(Network::Mainnet);
    let raw = m.encode();
    for index in [0, 1, 2, 4, 11, 12, 13, 14] {
        let Value::List(mut f) = value(&raw) else {
            panic!()
        };
        f[index] = Value::Bytes(vec![]);
        assert!(Manifest::decode(&Value::List(f).encode(), Limits::default()).is_err());
    }
    for limits in [
        Limits {
            manifest_bytes: raw.len() - 1,
            ..Default::default()
        },
        Limits {
            chunks: 0,
            ..Default::default()
        },
        Limits {
            blocks: 1,
            ..Default::default()
        },
        Limits {
            blocks_per_chunk: 1,
            ..Default::default()
        },
        Limits {
            total_bytes: 1,
            ..Default::default()
        },
        Limits {
            chunk_bytes: 1,
            ..Default::default()
        },
    ] {
        assert!(Manifest::decode(&raw, limits).is_err());
    }
    let b = &chunks(Network::Mainnet)[0];
    let d = &m.chunks[0];
    for offset in [0, b.len() / 2, b.len() - 1] {
        let mut bad = b.clone();
        bad[offset] ^= 1;
        assert!(
            interchange::decode_chunk(&bad, 0, d, &ReadConfig::default(), Limits::default())
                .is_err()
        );
    }
    assert!(interchange::decode_chunk(b, 1, d, &ReadConfig::default(), Limits::default()).is_err());
    assert!(
        interchange::decode_chunk(
            &b[..b.len() - 1],
            0,
            d,
            &ReadConfig::default(),
            Limits::default()
        )
        .is_err()
    );
    // Updating the checksum cannot legitimize a malformed frame or missing metadata.
    let micro = &chunks(Network::Mainnet)[1];
    let Value::List(mut f) = value(micro) else {
        panic!()
    };
    let Value::List(frames) = &mut f[3] else {
        panic!()
    };
    let Value::List(frame) = &mut frames[0] else {
        panic!()
    };
    frame[1] = Value::List(vec![]);
    let bad = Value::List(f).encode();
    assert!(
        interchange::decode_chunk(
            &bad,
            1,
            &descriptor(&bad, 2),
            &ReadConfig::default(),
            Limits::default()
        )
        .is_err()
    );
}
#[test]
fn archive_files_are_bounded_and_source_directory_cannot_be_a_staging_database() {
    let t = Temp::new();
    std::fs::create_dir_all(&t.0).unwrap();
    let n = Network::Mainnet;
    let m = manifest(n);
    std::fs::write(t.0.join("manifest.rlp"), m.encode()).unwrap();
    let a = Archive::open(&t.0, Limits::default()).unwrap();
    assert_eq!(a.manifest(), &m);
    assert!(a.chunk(0).is_err());
    let chunk = chunks(n).remove(0);
    std::fs::write(t.0.join("00000000.rlp"), &chunk).unwrap();
    assert_eq!(a.chunk(0).unwrap(), chunk);
    assert!(a.chunk(99).is_err());
    assert!(
        Replay::open(
            &t.0,
            identity(n),
            m.clone(),
            Default::default(),
            Default::default()
        )
        .is_err()
    );
    assert!(!t.0.join("CURRENT").exists());
    assert_eq!(std::fs::read(t.0.join("manifest.rlp")).unwrap(), m.encode());
    std::fs::write(t.0.join("00000000.rlp"), &chunk[..10]).unwrap();
    assert!(a.chunk(0).is_err());
    assert!(
        Archive::open(
            &t.0,
            Limits {
                manifest_bytes: 1,
                ..Default::default()
            }
        )
        .is_err()
    );
}
#[test]
fn validation_failure_retains_exact_prefix_and_incomplete_database_is_not_published() {
    let n = Network::Mainnet;
    let t = Temp::new();
    let m = manifest(n);
    let c = chunks(n);
    let mut r = open(&t, n, &m);
    assert!(
        r.verify_step(Default::default(), Default::default())
            .is_err()
    );
    assert!(r.apply_chunk(&c[0], 0, |v, b| prepare(n, v, b)).is_err());
    assert!(
        r.apply_chunk(&c[0], 2, |v, b| if b.header().common().height == 1 {
            Err(Failure::Validation("injected".into()))
        } else {
            prepare(n, v, b)
        })
        .is_err()
    );
    assert_eq!(r.progress().blocks, 1);
    drop(r);
    assert!(matches!(
        Database::open(&t.0, identity(n)),
        Err(repo::Error::IncompleteImport)
    ));
    let mut wrong = m.clone();
    wrong.exporter_version.push('x');
    assert!(
        Replay::open(
            &t.0,
            identity(n),
            wrong,
            Default::default(),
            Default::default()
        )
        .is_err()
    );
    let mut r = open(&t, n, &m);
    assert_eq!(r.progress().blocks, 1);
    replay_all(&mut r, n);
    verify_all(&mut r);
    drop(r.into_database().unwrap());
}
#[test]
fn forged_anchor_or_invalid_pow_cannot_complete_even_with_correct_checksums() {
    let n = Network::Mainnet;
    for field in 0..5 {
        let t = Temp::new();
        let mut m = manifest(n);
        match field {
            0 => m.anchor.0[0] ^= 1,
            1 => m.anchor_key.0[0] ^= 1,
            2 => m.height += 1,
            3 => m.protocol += 1,
            _ => m.roots.names = Some(ae_crypto::blake2b_256(b"fake cache")),
        }
        let mut r = open(&t, n, &m);
        replay_all(&mut r, n);
        assert!(
            r.verify_step(Default::default(), Default::default())
                .is_err()
        );
        drop(r);
        assert!(matches!(
            Database::open(&t.0, identity(n)),
            Err(repo::Error::IncompleteImport)
        ));
    }
    let t = Temp::new();
    let mut m = manifest(n);
    let c = chunks(n);
    let mut blocks = interchange::decode_chunk(
        &c[0],
        0,
        &m.chunks[0],
        &ReadConfig::default(),
        Limits::default(),
    )
    .unwrap();
    let Block::Key(key) = &mut blocks[1] else {
        panic!()
    };
    key.nonce ^= 1;
    let (d, b) =
        interchange::encode_chunk(0, &blocks, &ReadConfig::default(), Limits::default()).unwrap();
    m.chunks[0] = d;
    let mut r = open(&t, n, &m);
    assert!(r.apply_chunk(&b, 2, |v, b| prepare(n, v, b)).is_err());
    assert_eq!(r.progress().blocks, 1);
}
#[test]
fn existing_chain_and_wrong_network_are_rejected_without_changing_logical_state() {
    let n = Network::Mainnet;
    let t = Temp::new();
    let m = manifest(n);
    let db = Database::open(&t.0, identity(n)).unwrap();
    let block = Block::Key(match genesis::header(n).unwrap() {
        Header::Key(h) => h,
        _ => panic!(),
    });
    db.commit(prepare(n, &db.view().unwrap(), &block).unwrap())
        .unwrap();
    let before = db.view().unwrap().status();
    drop(db);
    assert!(matches!(
        Replay::open(
            &t.0,
            identity(n),
            m.clone(),
            Default::default(),
            Default::default()
        ),
        Err(Failure::Repository(repo::Error::NotEmpty))
    ));
    let db = Database::open(&t.0, identity(n)).unwrap();
    assert_eq!(db.view().unwrap().status(), before);
    drop(db);
    assert!(
        Replay::open(
            &Temp::new().0,
            identity(Network::Uat),
            m,
            Default::default(),
            Default::default()
        )
        .is_err()
    );
}

#[test]
fn crash_worker() {
    use std::io::{BufRead, Write};
    let Ok(path) = std::env::var("AE_REPLAY_CRASH_PATH") else {
        return;
    };
    let operation = std::env::var("AE_REPLAY_CRASH_OPERATION").unwrap();
    let n = if matches!(operation.as_str(), "verify" | "complete") {
        Network::Mainnet
    } else {
        Network::Uat
    };
    let m = manifest(n);
    let mut replay = if operation == "init" {
        None
    } else {
        Some(
            Replay::open(
                &path,
                identity(n),
                m.clone(),
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        )
    };
    println!("REPLAY_READY");
    std::io::stdout().flush().unwrap();
    let mut command = String::new();
    std::io::stdin().lock().read_line(&mut command).unwrap();
    assert_eq!(command, "publish\n");
    match operation.as_str() {
        "init" => {
            replay = Some(
                Replay::open(
                    &path,
                    identity(n),
                    m,
                    Default::default(),
                    Default::default(),
                )
                .unwrap(),
            );
        }
        "block" => {
            let r = replay.as_mut().unwrap();
            let c = chunks(n);
            r.apply_chunk(&c[r.progress().chunk as usize], 1, |v, b| prepare(n, v, b))
                .unwrap();
        }
        "verify" => {
            replay
                .as_mut()
                .unwrap()
                .verify_step(
                    Default::default(),
                    scan::PageLimits {
                        nodes: 1,
                        ..Default::default()
                    },
                )
                .unwrap();
        }
        "complete" => {
            verify_all(replay.as_mut().unwrap());
        }
        _ => panic!(),
    }
    assert!(replay.is_some());
    println!("REPLAY_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn killed_importers_recover_a_complete_block_and_progress_pair() {
    use std::io::{BufRead, Write};
    use std::process::{Command, Stdio};
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for op in ["init", "block", "verify", "complete"] {
        let n = if matches!(op, "verify" | "complete") {
            Network::Mainnet
        } else {
            Network::Uat
        };
        let m = manifest(n);
        for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
            let t = Temp::new();
            let before = if op == "init" {
                None
            } else {
                let mut r = open(&t, n, &m);
                if op == "block" {
                    r.apply_chunk(&chunks(n)[0], 1, |v, b| prepare(n, v, b))
                        .unwrap();
                } else {
                    replay_all(&mut r, n);
                    if op == "complete" {
                        r.verify_step(
                            Default::default(),
                            scan::PageLimits {
                                nodes: 1,
                                ..Default::default()
                            },
                        )
                        .unwrap();
                    }
                }
                let before = r.progress().clone();
                drop(r);
                Some(before)
            };
            let mut child = Child(
                Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "crash_worker", "--nocapture"])
                    .env("AE_REPLAY_CRASH_PATH", &t.0)
                    .env("AE_REPLAY_CRASH_OPERATION", op)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .unwrap(),
            );
            let mut out = std::io::BufReader::new(child.0.stdout.take().unwrap());
            let wait = |out: &mut std::io::BufReader<std::process::ChildStdout>, marker: &str| loop {
                let mut line = String::new();
                assert_ne!(
                    out.read_line(&mut line).unwrap(),
                    0,
                    "child exited before {marker}"
                );
                if line.contains(marker) {
                    break;
                }
            };
            wait(&mut out, "REPLAY_READY");
            if phase != "before" {
                child
                    .0
                    .stdin
                    .as_mut()
                    .unwrap()
                    .write_all(b"publish\n")
                    .unwrap();
                child.0.stdin.as_mut().unwrap().flush().unwrap();
                if phase == "after" {
                    wait(&mut out, "REPLAY_DURABLE");
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(
                        phase.strip_prefix("race").unwrap().parse().unwrap(),
                    ));
                }
            }
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            let mut r = open(&t, n, &m);
            let current = r.progress().clone();
            if let Some(before) = before {
                if phase == "before" {
                    assert_eq!(current, before);
                }
                if op == "block" {
                    assert!([before.blocks, before.blocks + 1].contains(&current.blocks));
                    if phase == "after" {
                        assert_eq!(current.blocks, before.blocks + 1);
                    }
                } else {
                    assert_eq!(current.blocks, before.blocks);
                    if phase == "after" {
                        assert_ne!(current, before);
                    }
                }
            }
            if op == "verify" && phase == "after" {
                assert_eq!(current.phase, Phase::Verify);
            }
            if op == "complete" && phase == "after" {
                assert_eq!(current.phase, Phase::Complete);
            }
            drop(r);
            if current.phase != Phase::Complete {
                assert!(matches!(
                    Database::open(&t.0, identity(n)),
                    Err(repo::Error::IncompleteImport)
                ));
            }
            r = open(&t, n, &m);
            replay_all(&mut r, n);
            verify_all(&mut r);
            let db = r.into_database().unwrap();
            let v = db.view().unwrap();
            assert_eq!(v.status().tip, Some(m.anchor));
            assert_eq!(v.status().revision, m.blocks);
            assert_eq!(v.require_record(m.anchor).unwrap().roots, m.roots);
            v.verify_tip().unwrap();
            eprintln!("Replay crash recovery {op}/{phase}: {:?}", current.phase);
        }
    }
}
#[test]
fn corrupt_journal_or_missing_anchor_nodes_fail_closed() {
    let n = Network::Uat;
    let m = manifest(n);
    for case in 0..5 {
        let t = Temp::new();
        let mut r = open(&t, n, &m);
        replay_all(&mut r, n);
        drop(r);
        let raw = rocksdb::DB::open_default(&t.0).unwrap();
        match case {
            0 => raw.delete(b"\0replay-progress").unwrap(),
            1 => raw.delete(b"\0replay-manifest").unwrap(),
            2 => raw.put(b"\0replay-progress", b"invalid").unwrap(),
            3 => raw.put(b"\0replay-manifest", b"invalid").unwrap(),
            _ => {
                let hash = m.roots.state.accounts.unwrap();
                raw.delete(repo::key(repo::NODES, hash.0)).unwrap();
            }
        }
        drop(raw);
        assert!(Database::open(&t.0, identity(n)).is_err());
        let resumed = Replay::open(
            &t.0,
            identity(n),
            m.clone(),
            Default::default(),
            Default::default(),
        );
        if case == 4 {
            // Root corruption may be caught by startup before the deep scan.
            if let Ok(mut r) = resumed {
                assert!(
                    r.verify_step(Default::default(), Default::default())
                        .is_err()
                );
            }
        } else {
            assert!(resumed.is_err());
        }
    }
}
#[test]
fn validator_cannot_drop_exported_metering_metadata() {
    let n = Network::Mainnet;
    let t = Temp::new();
    let m = manifest(n);
    let c = chunks(n);
    let mut r = open(&t, n, &m);
    r.apply_chunk(&c[0], 2, |v, b| prepare(n, v, b)).unwrap();
    let before = r.progress().clone();
    let error = r
        .apply_chunk(&c[1], 1, |v, b| {
            let Block::Micro(mut micro) = b.clone() else {
                panic!()
            };
            let record =
                ae_tx::storage::encode(&micro.transactions[0], Default::default()).unwrap();
            let Value::List(mut f) = value(record.strip_prefix(ae_tx::storage::MAGIC).unwrap())
            else {
                panic!()
            };
            f[1] = Value::Bytes(vec![0]);
            let mut changed = ae_tx::storage::MAGIC.to_vec();
            changed.extend(Value::List(f).encode());
            micro.transactions[0] = ae_tx::storage::decode(&changed, Default::default()).unwrap();
            prepare(n, v, &Block::Micro(micro))
        })
        .unwrap_err();
    assert!(
        matches!(
            error,
            Failure::Repository(repo::Error::Corrupt("replay transaction metadata"))
        ),
        "{error:?}"
    );
    assert_eq!(r.progress(), &before);
    replay_all(&mut r, n);
    verify_all(&mut r);
    r.into_database().unwrap();
}
