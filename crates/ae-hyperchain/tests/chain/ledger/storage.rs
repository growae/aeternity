use super::*;
use ae_chain::{
    publication,
    repository::{self as repo, ledger::Origin},
};
use ae_storage::{Database, hyperchain::Owner};
struct Directory(std::path::PathBuf);
impl Directory {
    fn new(label: &str, index: usize) -> Self {
        Self(std::env::temp_dir().join(format!(
            "ae-hc-ledger-{label}-{}-{index}",
            std::process::id()
        )))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn config(s: &Step<'_>) -> repo::HyperchainConfig<Otp26> {
    repo::HyperchainConfig {
        identity: repo::Identity::for_consensus(
            &s.reader.profile.network,
            s.rules.genesis_hash,
            ae_crypto::blake2b_256(b"isolated HC ledger OTP26 profile"),
            ae_protocol::ConsensusType::Pos,
        ),
        profile: s.reader.profile.clone(),
        rules: s.rules.clone(),
        aevm: None,
        limits: Default::default(),
    }
}
fn open(path: &Directory, c: &repo::HyperchainConfig<Otp26>) -> Arc<Database> {
    Arc::new(
        Database::open_with_config(
            &path.0,
            c.identity.clone(),
            repo::ReadConfig {
                mode: HeaderMode::Hyperchain,
                ..Default::default()
            },
        )
        .unwrap(),
    )
}
fn bootstrap(db: &Database, c: &repo::HyperchainConfig<Otp26>, s: &Step<'_>) {
    db.commit(
        repo::bootstrap(
            &db.view().unwrap(),
            &c.profile.network,
            s.parent.key,
            &s.parent.view.trees,
            Default::default(),
        )
        .unwrap(),
    )
    .unwrap();
}
fn check_stored(db: &Database, b: &Block, expected: &J) {
    let view = db.view().unwrap();
    view.verify_tip().unwrap();
    let hash = b.hash().unwrap();
    assert_eq!(view.status().tip, Some(hash));
    let record = view.require_record(hash).unwrap();
    assert_eq!(number(&record.fees).to_string(), expected["fees"]);
    assert_eq!(number(&record.difficulty).to_string(), expected["score"]);
    check(
        &view.trees(hash, Default::default()).unwrap(),
        &expected["trees"],
    );
    assert_eq!(
        normalize(&block::cache(&view.hyperchain_checkpoint().unwrap().state)),
        normalize(&expected["cache"])
    );
    let event = view
        .hyperchain_event(view.status().revision)
        .unwrap()
        .unwrap();
    if matches!(b, Block::Key(_)) {
        assert_eq!(
            normalize(&block::events(event.events)),
            normalize(&expected["events"])
        );
    } else {
        assert!(event.events.is_empty());
    }
}
#[test]
fn native_hc_blocks_commit_and_reopen_with_atomic_roots_caches_and_events() {
    let mut blocks = 0;
    for (index, row) in LEDGER["rows"].as_array().unwrap().iter().enumerate() {
        let path = Directory::new("reopen", index);
        let mut first = true;
        walk("profile", row, |s| {
            let c = config(&s);
            let db = open(&path, &c);
            if first {
                bootstrap(&db, &c, &s);
                first = false;
            }
            let before = db.view().unwrap();
            let status = before.status();
            let before_checkpoint = before.hyperchain_checkpoint().unwrap();
            let owner = Owner::start(db.clone(), c.clone(), Host::default()).unwrap();
            assert!(Owner::start(db.clone(), c.clone(), Host::default()).is_err());
            let source = db.events();
            let cursor = source.cursor();
            let origin = match blocks % 3 {
                0 => Origin::Gossip,
                1 => Origin::Sync,
                _ => Origin::Created,
            };
            let receipt = owner
                .insert(s.block, s.rules.now_ms, origin)
                .unwrap_or_else(|e| panic!("{} {} {e:?}", row["protocol"], s.expected["kind"]));
            let hash = s.block.hash().unwrap();
            assert_eq!(receipt.change().new_tip, hash);
            assert_eq!(receipt.change().revision, status.revision + 1);
            assert_eq!(before.status(), status);
            assert!(before.block(hash).unwrap().is_none());
            assert_eq!(before.hyperchain_checkpoint().unwrap(), before_checkpoint);
            let (_, state, _) = owner.snapshot().unwrap();
            assert_eq!(
                normalize(&block::cache(&state)),
                normalize(&s.expected["cache"])
            );
            let messages = source.after(&cursor, 1).unwrap();
            assert_eq!(messages.records.len(), 1);
            let effects: Vec<_> = messages.records[0].effects().collect();
            let count = effects
                .iter()
                .filter(|e| matches!(e, publication::Effect::Consensus { .. }))
                .count();
            let emitted = !matches!(origin, Origin::Sync);
            assert_eq!(
                count,
                if emitted {
                    receipt.update().consensus_events().len()
                } else {
                    0
                }
            );
            if count > 0 {
                assert!(matches!(effects[0], publication::Effect::Consensus { .. }));
            }
            check_stored(&db, s.block, s.expected);
            assert!(owner.insert(s.block, s.rules.now_ms, origin).is_err());
            assert_eq!(db.view().unwrap().status().revision, status.revision + 1);
            drop(before);
            drop(owner);
            drop(db);
            let reopened = open(&path, &c);
            check_stored(&reopened, s.block, s.expected);
            let resumed = Owner::start(reopened.clone(), c, Host::default()).unwrap();
            assert_eq!(resumed.snapshot().unwrap().1, state);
            blocks += 1;
        });
    }
    assert_eq!(blocks, 77);
}

#[test]
fn hc_rollback_preserves_same_height_ancestors_and_replays_native_roots() {
    let row = LEDGER["rows"].as_array().unwrap().last().unwrap();
    for choice in 0..3 {
        let path = Directory::new("rollback", choice);
        let mut db = None;
        let mut owner = None;
        let mut cfg = None;
        let mut blocks = Vec::new();
        walk("profile", row, |s| {
            if db.is_none() {
                let c = config(&s);
                let database = open(&path, &c);
                bootstrap(&database, &c, &s);
                owner = Some(Owner::start(database.clone(), c.clone(), Host::default()).unwrap());
                db = Some(database);
                cfg = Some(c);
            }
            owner
                .as_ref()
                .unwrap()
                .insert(s.block, s.rules.now_ms, Origin::Gossip)
                .unwrap();
            blocks.push((s.block.clone(), s.expected.clone()));
        });
        let db = db.unwrap();
        let owner = owner.unwrap();
        let selected = if choice == 1 { 4 } else { 5 };
        let hash = blocks[selected].0.hash().unwrap();
        let target = match choice {
            0 => repo::rollback::Target::Key(hash),
            1 => repo::rollback::Target::Micro(hash),
            _ => repo::rollback::Target::Height(3),
        };
        let pinned = db.view().unwrap();
        let old = pinned.status();
        let receipt = db.rollback(target, Default::default()).unwrap();
        assert_eq!(receipt.change().new_tip, hash);
        assert_eq!(receipt.change().revision, old.revision + 1);
        assert_eq!(pinned.status(), old);
        assert_eq!(owner.snapshot().unwrap().1, cb::State::default());
        let after = db.view().unwrap();
        after.verify_tip().unwrap();
        for (block, _) in &blocks[..=selected] {
            assert!(after.block(block.hash().unwrap()).unwrap().is_some());
        }
        for revision in 1..=old.revision {
            assert!(after.hyperchain_event(revision).unwrap().is_some());
        }
        assert_eq!(
            after
                .hyperchain_event(old.revision + 1)
                .unwrap()
                .unwrap()
                .context
                .origin,
            publication::Origin::Rollback
        );
        for (b, expected) in &blocks[selected + 1..] {
            let receipt = owner.insert(b, 1_000_000, Origin::Gossip).unwrap();
            let view = db.view().unwrap();
            check(
                &view
                    .trees(receipt.change().new_tip, Default::default())
                    .unwrap(),
                &expected["trees"],
            );
            assert_eq!(
                number(&view.require_record(receipt.change().new_tip).unwrap().fees).to_string(),
                expected["fees"]
            );
        }
        drop(after);
        drop(pinned);
        drop(owner);
        drop(db);
        open(&path, &cfg.unwrap())
            .view()
            .unwrap()
            .verify_tip()
            .unwrap();
    }
}

#[test]
fn native_competing_hc_branches_preserve_side_events_and_reorganize_shared_transactions() {
    let corpus: J =
        serde_json::from_str(include_str!("../../fixtures/erlang_hc_durable.json")).unwrap();
    for (index, profile_name) in ["profile", "repeat_profile"].into_iter().enumerate() {
        let path = Directory::new("forks", index);
        let mut reader = genesis::reader(&genesis::DATA["rows"][0]);
        Arc::make_mut(&mut reader.profile).order = otp_order::profile(&corpus[profile_name]);
        let Header::Key(g) =
            Header::decode(&bytes(&corpus["genesis"]["header"]), HeaderMode::Hyperchain).unwrap()
        else {
            panic!()
        };
        let gh = Header::Key(g.clone()).hash().unwrap();
        let c = repo::HyperchainConfig {
            identity: repo::Identity::for_consensus(
                &reader.profile.network,
                gh,
                Hash256::ZERO,
                ae_protocol::ConsensusType::Pos,
            ),
            profile: reader.profile.clone(),
            rules: rules(gh),
            aevm: None,
            limits: Default::default(),
        };
        let db = open(&path, &c);
        db.commit(
            repo::bootstrap(
                &db.view().unwrap(),
                &c.profile.network,
                &g,
                &trees(&corpus["genesis"]["trees"]),
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let owner = Owner::start(db.clone(), c.clone(), Host::default()).unwrap();
        let mut side = 0;
        let mut reorganized = 0;
        let mut inserted = Vec::new();
        for row in corpus["rows"].as_array().unwrap() {
            let b = Block::decode(
                &bytes(&row["block"]),
                DecodeConfig {
                    mode: HeaderMode::Hyperchain,
                    ..Default::default()
                },
            )
            .unwrap();
            let source = db.events();
            let cursor = source.cursor();
            let receipt = owner.insert(&b, 1_000_000, Origin::Gossip).unwrap();
            assert_eq!(
                hex::encode(receipt.change().old_tip.unwrap().0),
                row["old_tip"]
            );
            assert_eq!(hex::encode(receipt.change().new_tip.0), row["new_tip"]);
            let v = db.view().unwrap();
            v.verify_tip().unwrap();
            let h = b.hash().unwrap();
            check(&v.trees(h, Default::default()).unwrap(), &row["trees"]);
            let record = v.require_record(h).unwrap();
            assert_eq!(number(&record.fees).to_string(), row["fees"]);
            assert_eq!(number(&record.difficulty).to_string(), row["score"]);
            let events = match &b {
                Block::Key(_) => block::events(receipt.update().consensus_events().to_vec()),
                Block::Micro(_) => list(receipt.update().events().iter().map(tx_event).collect()),
            };
            assert_eq!(normalize(&events), normalize(&row["events"]));
            assert_eq!(
                normalize(&block::cache(&v.hyperchain_checkpoint().unwrap().state)),
                normalize(&row["cache"])
            );
            let messages = source.after(&cursor, 1).unwrap();
            let effects: Vec<_> = messages.records[0].effects().collect();
            assert_eq!(
                effects
                    .iter()
                    .filter(|e| matches!(e, publication::Effect::Transaction { .. }))
                    .count(),
                receipt.update().events().len()
            );
            if receipt.change().old_tip == Some(receipt.change().new_tip) {
                side += 1;
                assert!(messages.records[0].body().is_none());
                assert!(
                    !effects
                        .iter()
                        .any(|e| matches!(e, publication::Effect::TopChanged(_)))
                );
            }
            if !receipt.change().detached.is_empty() {
                reorganized += 1;
            }
            inserted.push(b);
        }
        assert_eq!((side, reorganized), (2, 3));
        let v = db.view().unwrap();
        assert!(!v.is_canonical(inserted[3].hash().unwrap()).unwrap());
        assert!(v.is_canonical(inserted[5].hash().unwrap()).unwrap());
        let Block::Micro(bm) = &inserted[5] else {
            panic!()
        };
        for tx in &bm.transactions {
            assert_eq!(
                v.location(tx.hash()).unwrap(),
                Some(inserted[5].hash().unwrap())
            );
        }
        drop(v);
        drop(owner);
        drop(db);
        open(&path, &c).view().unwrap().verify_tip().unwrap();
    }
}
