mod common;
use ae_chain::block::Block;
use ae_execution::transactions::{self, owned};
use ae_protocol::NetworkProfile;
use ae_sync::chain::{Config, Direction, Error, Importer, Outcome};
use ae_types::Hash256;
use common::{Node, import::*, ledger_fixtures::*};

const NOW: u64 = u64::MAX - 600_000;
#[test]
fn original_generation_adjacency_and_conductor_order_match_all_native_traces() {
    use ae_codec::headers::HeaderMode;
    use ae_sync::chain::generation::Generation;
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/erlang_chain_import.json")).unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 216);
    let mut traces = 0;
    for row in rows {
        let mode = if row["mode"] == "pow" {
            HeaderMode::ProofOfWork
        } else {
            HeaderMode::Hyperchain
        };
        let direction = if row["direction"] == "forward" {
            Direction::Forward
        } else {
            Direction::Backward
        };
        let message = ae_p2p::message::Message::decode_packet(
            &hex::decode(row["packet"].as_str().unwrap()).unwrap(),
            Default::default(),
        )
        .unwrap();
        let hash = Hash256(
            hex::decode(row["hash"].as_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap(),
        );
        let config = ae_chain::block::DecodeConfig {
            mode,
            ..Default::default()
        };
        let generation = Generation::decode(&message, direction, config, 4096).unwrap();
        assert_eq!(
            generation.check(hash, mode).is_ok(),
            direction == Direction::Forward || row["consecutive"] == true
        );
        assert!(matches!(
            Generation::decode(&message, direction, config, 0),
            Err(ae_sync::chain::generation::Error::ResourceLimit)
        ));
        let blocks: Vec<_> = generation
            .blocks()
            .map(|b| hex::encode(b.hash().unwrap().0))
            .collect();
        for post in row["posts"].as_array().unwrap() {
            let fail = post["fail_at"].as_u64().unwrap() as usize;
            let stop = fail > 0 && fail <= blocks.len();
            let called = if stop { fail } else { blocks.len() };
            assert_eq!(serde_json::json!(&blocks[..called]), post["calls"]);
            assert_eq!(
                serde_json::json!(&blocks[..called - usize::from(stop)]),
                post["accepted"]
            );
            assert_eq!(post["result"], if stop { "refused" } else { "ok" });
            traces += 1;
        }
    }
    assert_eq!(traces, 852);
}
fn importer(node: &Node, config: Config) -> Importer {
    Importer::new(
        node.db.clone(),
        execution(&NetworkProfile::mainnet()),
        rules(node.db.view().unwrap().identity().genesis),
        config,
    )
    .unwrap()
}
#[test]
fn owned_execution_requires_both_vms_and_retains_explicit_profiles_after_sources_drop() {
    let network = NetworkProfile::mainnet();
    assert!(matches!(
        owned::Owned::capture(&transactions::Config::primitives(&network)),
        Err(owned::Error::MissingFate)
    ));
    let owned = execution(&network);
    owned.with_config(|c| {
        let mut missing = transactions::Config::primitives(&network);
        missing.fate = c.fate;
        assert!(matches!(
            owned::Owned::capture(&missing),
            Err(owned::Error::MissingAevm)
        ));
    });
    drop(network);
    owned.clone().with_config(|c| {
        assert_eq!(c.network.network_id(), b"ae_mainnet");
        assert_eq!(
            c.fate.unwrap().diagnostic_profile,
            ae_vm_fate::diagnostic::Profile::Test
        );
        assert_eq!(
            c.fate.unwrap().context.arithmetic_word_size,
            ae_vm_fate::maps::order::WordSize::Bits64
        );
        assert_eq!(
            c.aevm.unwrap().float_profile,
            ae_vm_aevm::gas::FloatProfile::Otp26Word64
        );
    });
}
#[tokio::test]
async fn actual_import_preserves_committed_prefix_duplicate_validation_and_unread_credits() {
    let node = Node::new();
    let config = Config {
        jobs: 1,
        ..Default::default()
    };
    let service = importer(&node, config);
    let all = blocks(&NetworkProfile::mainnet());
    let key = &all[0];
    let hash = key.hash().unwrap();
    let p = packet(key, &[], Direction::Backward);
    let ticket = service
        .import(p.clone(), hash, Direction::Backward, NOW)
        .unwrap();
    assert!(matches!(
        service.import(p.clone(), hash, Direction::Backward, NOW),
        Err(Error::Busy)
    ));
    let result = ticket.wait().await.unwrap();
    assert_eq!(result.accepted.len(), 1);
    assert!(matches!(
        result.accepted[0].outcome,
        Outcome::Committed { .. }
    ));
    let result = service
        .import(p, hash, Direction::Backward, NOW)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(result.accepted[0].outcome, Outcome::Present);
    let good = all[1].clone();
    let Block::Micro(mut invalid) = good.clone() else {
        panic!()
    };
    invalid.transactions.clear(); // Same signed header, invalid replacement body.
    let bad = Block::Micro(invalid);
    let failure = service
        .import(
            packet(key, &[good.clone(), bad.clone()], Direction::Forward),
            hash,
            Direction::Forward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap_err();
    assert_eq!(failure.progress.as_ref().unwrap().accepted.len(), 1);
    assert_eq!(
        failure.progress.as_ref().unwrap().accepted[0].hash,
        good.hash().unwrap()
    );
    assert!(
        matches!(&failure.error, Error::Validation { operational: false, reason } if reason.contains("TransactionsHash"))
    );
    assert_eq!(
        node.db
            .view()
            .unwrap()
            .require_block(good.hash().unwrap())
            .unwrap(),
        good
    );
    let failure = service
        .import(
            packet(key, &[bad], Direction::Forward),
            hash,
            Direction::Forward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap_err();
    assert!(failure.progress.as_ref().unwrap().accepted.is_empty());
    assert!(matches!(
        failure.error,
        Error::Validation {
            operational: false,
            ..
        }
    ));
    assert!(!service.recovery_required());
    service.stop();
    service.join().await;
    assert!(matches!(
        service.import(vec![], hash, Direction::Forward, NOW),
        Err(Error::Closed)
    ));
}
#[tokio::test]
async fn returned_key_is_validated_before_backward_hash_or_forward_insertion() {
    let node = Node::new();
    let service = importer(&node, Default::default());
    let all = blocks(&NetworkProfile::mainnet());
    let Block::Key(mut invalid) = all[0].clone() else {
        panic!()
    };
    invalid.nonce ^= 1;
    let bad = Block::Key(invalid);
    let initial = node.db.view().unwrap().status();
    for direction in [Direction::Forward, Direction::Backward] {
        let fail = service
            .import(
                packet(&bad, &[], direction),
                Hash256([99; 32]),
                direction,
                NOW,
            )
            .unwrap()
            .wait()
            .await
            .unwrap_err();
        assert!(matches!(
            fail.error,
            Error::Validation {
                operational: false,
                ..
            }
        ));
        assert!(fail.progress.as_ref().unwrap().accepted.is_empty());
        assert_eq!(node.db.view().unwrap().status(), initial);
    }
    let fail = service
        .import(
            packet(&all[0], &[], Direction::Backward),
            Hash256([99; 32]),
            Direction::Backward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap_err();
    assert_eq!(
        fail.error,
        Error::Generation(ae_sync::chain::generation::Error::HashMismatch)
    );
    // A valid forward key is checked but neither matched nor inserted.
    let report = service
        .import(
            packet(&all[0], &[], Direction::Forward),
            Hash256([99; 32]),
            Direction::Forward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(report.accepted.is_empty());
    assert_eq!(node.db.view().unwrap().status(), initial);
    service.stop();
    service.join().await;
}

#[tokio::test]
async fn configuration_whitelist_refusals_and_generation_failures_preserve_storage() {
    use ae_chain::repository::ledger;
    use ae_p2p::message::{Kind, Message, Value};
    let node = Node::new();
    let genesis = node.db.view().unwrap().identity().genesis;
    let mut config = Config::default();
    config.ledger.policy.origin = ledger::Origin::Gossip;
    assert!(matches!(
        Importer::new(
            node.db.clone(),
            execution(&NetworkProfile::mainnet()),
            rules(genesis),
            config
        ),
        Err(Error::Configuration)
    ));
    assert!(matches!(
        Importer::new(
            node.db.clone(),
            execution(&NetworkProfile::uat()),
            rules(genesis),
            Default::default()
        ),
        Err(Error::Storage(ae_chain::repository::Error::Identity))
    ));
    let all = blocks(&NetworkProfile::mainnet());
    let key = &all[0];
    let hash = key.hash().unwrap();
    let mut gated = rules(genesis);
    gated.checkpoints.insert(1, Hash256([99; 32]));
    let service = Importer::new(
        node.db.clone(),
        execution(&NetworkProfile::mainnet()),
        gated,
        Default::default(),
    )
    .unwrap();
    let before = node.db.view().unwrap().status();
    let fail = service
        .import(
            packet(key, &[], Direction::Forward),
            hash,
            Direction::Forward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap_err();
    assert_eq!(fail.error, Error::BlockedByWhitelist);
    assert_eq!(node.db.view().unwrap().status(), before);
    service.stop();
    service.join().await;
    let service = importer(&node, Default::default());
    let rejected = Message::response_error(
        Kind::Generation,
        b"block_not_found".to_vec(),
        Default::default(),
    )
    .unwrap()
    .encode_packet(Default::default())
    .unwrap();
    let failure = service
        .import(rejected, hash, Direction::Backward, NOW)
        .unwrap()
        .wait()
        .await
        .unwrap_err();
    assert_eq!(failure.error, Error::Refused(b"block_not_found".to_vec()));
    service
        .import(
            packet(key, &[], Direction::Backward),
            hash,
            Direction::Backward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap();
    let before = node.db.view().unwrap().status();
    let next = all
        .iter()
        .find(|b| matches!(b, Block::Key(k) if k.common.height==2))
        .unwrap();
    let next_hash = next.hash().unwrap();
    let duplicate = [all[1].clone(), all[1].clone()];
    let failure = service
        .import(
            packet(next, &duplicate, Direction::Backward),
            next_hash,
            Direction::Backward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap_err();
    assert_eq!(
        failure.error,
        Error::Generation(ae_sync::chain::generation::Error::NonConsecutive)
    );
    assert!(failure.progress.unwrap().accepted.is_empty());
    assert_eq!(node.db.view().unwrap().status(), before);
    let valid = message(next, &all[1..2], Direction::Backward);
    let mut fields = valid.fields().clone();
    let Value::List(micros) = fields.get_mut("micro_blocks").unwrap() else {
        panic!()
    };
    micros.push(Value::Binary(vec![0]));
    let malformed = Message::new(Kind::Generation, 1, fields, Default::default())
        .unwrap()
        .encode_packet(Default::default())
        .unwrap();
    let failure = service
        .import(malformed, next_hash, Direction::Backward, NOW)
        .unwrap()
        .wait()
        .await
        .unwrap_err();
    assert!(matches!(
        failure.error,
        Error::Generation(ae_sync::chain::generation::Error::Block(_))
    ));
    assert!(failure.progress.unwrap().accepted.is_empty());
    assert_eq!(node.db.view().unwrap().status(), before);
    service.stop();
    service.join().await;
}

#[tokio::test]
async fn real_public_generations_import_and_reopen_with_roots_and_transaction_locations() {
    use ae_chain::repository::{Identity, ledger};
    use ae_codec::headers::Header;
    use ae_state::{
        Trees,
        genesis::{self, Network},
    };
    use ae_storage::Database;
    use std::sync::Arc;
    struct Temporary(std::path::PathBuf);
    impl Drop for Temporary {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    for (n, network) in [
        (Network::Mainnet, NetworkProfile::mainnet()),
        (Network::Uat, NetworkProfile::uat()),
    ] {
        let temp = Temporary(std::env::temp_dir().join(format!(
            "ae-generation-import-{}-{}",
            std::process::id(),
            String::from_utf8_lossy(network.network_id())
        )));
        let Header::Key(genesis) = genesis::header(n).unwrap() else {
            panic!()
        };
        let id = Identity::new(
            &network,
            Block::Key(genesis.clone()).hash().unwrap(),
            ae_crypto::blake2b_256(b"import-reference-profiles"),
        );
        let db = Arc::new(Database::open(&temp.0, id.clone()).unwrap());
        let trees = Trees {
            accounts: genesis::state(n).unwrap().0,
            contracts: Default::default(),
            calls: Default::default(),
            channels: Default::default(),
            names: Default::default(),
            oracles: Default::default(),
        };
        let update = ledger::bootstrap(
            &db.view().unwrap(),
            &network,
            &genesis,
            &trees,
            Default::default(),
        )
        .unwrap();
        db.commit(update).unwrap();
        let service = Importer::new(
            db.clone(),
            execution(&network),
            rules(id.genesis),
            Default::default(),
        )
        .unwrap();
        let all = blocks(&network);
        let mut pending = vec![];
        let mut last_key = None;
        let mut imported = 0;
        for block in &all {
            match block {
                Block::Key(_) => {
                    let report = service
                        .import(
                            packet(block, &pending, Direction::Backward),
                            block.hash().unwrap(),
                            Direction::Backward,
                            NOW,
                        )
                        .unwrap()
                        .wait()
                        .await
                        .unwrap();
                    imported += report.accepted.len();
                    pending.clear();
                    last_key = Some(block);
                }
                Block::Micro(_) => pending.push(block.clone()),
            }
        }
        let last_key = last_key.unwrap();
        let report = service
            .import(
                packet(last_key, &pending, Direction::Forward),
                last_key.hash().unwrap(),
                Direction::Forward,
                NOW,
            )
            .unwrap()
            .wait()
            .await
            .unwrap();
        imported += report.accepted.len();
        assert_eq!(imported, all.len());
        service.stop();
        service.join().await;
        drop(service);
        drop(db);
        let reopened = Database::open(&temp.0, id).unwrap();
        let view = reopened.view().unwrap();
        assert_eq!(view.status().tip, Some(all.last().unwrap().hash().unwrap()));
        for block in &all {
            let hash = block.hash().unwrap();
            assert_eq!(view.require_block(hash).unwrap(), *block);
            assert_eq!(
                view.require_record(hash).unwrap().roots.state.hash().0,
                block.header().common().state_hash
            );
            if let Block::Micro(micro) = block {
                for tx in &micro.transactions {
                    assert_eq!(view.location(tx.hash()).unwrap(), Some(hash));
                }
            }
        }
    }
}

#[tokio::test]
async fn missing_ancestry_policy_rejection_and_pruned_state_remain_typed() {
    use ae_chain::{repository, resilience};
    let node = Node::new();
    let service = importer(&node, Default::default());
    let all = blocks(&NetworkProfile::mainnet());
    let second_key = all
        .iter()
        .find(|b| matches!(b, Block::Key(k) if k.common.height == 2))
        .unwrap();
    let failure = service
        .import(
            packet(second_key, &[], Direction::Backward),
            second_key.hash().unwrap(),
            Direction::Backward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap_err();
    assert_eq!(
        failure.error,
        Error::Storage(repository::Error::Missing(Hash256(
            second_key.header().common().previous_hash
        )))
    );
    assert!(failure.progress.unwrap().accepted.is_empty());
    let mut last_key = &all[0];
    for block in &all {
        let (key, micros, direction) = if matches!(block, Block::Key(_)) {
            last_key = block;
            (block, &[][..], Direction::Backward)
        } else {
            (last_key, std::slice::from_ref(block), Direction::Forward)
        };
        service
            .import(
                packet(key, micros, direction),
                key.hash().unwrap(),
                direction,
                NOW,
            )
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    let before = node.db.view().unwrap().status();
    let top = node
        .db
        .view()
        .unwrap()
        .require_block(before.tip.unwrap())
        .unwrap()
        .header()
        .common()
        .height;
    // An unknown old micro passes standalone body checks. Local policy/pruning
    // is consulted before contextual signature validation, as in the conductor.
    let Block::Micro(mut old) = all[1].clone() else {
        panic!()
    };
    old.header.signature[0] ^= 1;
    let old = Block::Micro(old);
    let height = old.header().common().height;
    assert!(
        node.db
            .view()
            .unwrap()
            .block(old.hash().unwrap())
            .unwrap()
            .is_none()
    );
    node.db
        .update_resilience(
            resilience::Event::Start,
            resilience::Config {
                depth: 1,
                from_start: true,
            },
        )
        .unwrap();
    let failure = service
        .import(
            packet(&all[0], std::slice::from_ref(&old), Direction::Forward),
            all[0].hash().unwrap(),
            Direction::Forward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap_err();
    assert_eq!(failure.error, Error::Policy { height, top });
    assert!(failure.progress.unwrap().accepted.is_empty());
    node.db
        .update_resilience(resilience::Event::Synced, resilience::Config::default())
        .unwrap();
    assert!(node.db.begin_gc(Default::default()).unwrap());
    let floor = node.db.gc_state().unwrap().height;
    let failure = service
        .import(
            packet(&all[0], &[old], Direction::Forward),
            all[0].hash().unwrap(),
            Direction::Forward,
            NOW,
        )
        .unwrap()
        .wait()
        .await
        .unwrap_err();
    assert_eq!(
        failure.error,
        Error::Storage(repository::Error::Pruned { height, floor })
    );
    assert!(failure.progress.unwrap().accepted.is_empty());
    assert_eq!(node.db.view().unwrap().status(), before);
    assert!(!service.recovery_required());
    node.db.cancel_gc().unwrap();
    service.stop();
    service.join().await;
}
