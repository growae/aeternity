use super::*;
use ae_chain::{
    block::Light,
    gossip::{self, Fragment, Precheck, PreparationError, Settings},
};
fn settings() -> Settings {
    Settings {
        now_ms: NOW,
        micro_cycle_ms: 3000,
        gossip_depth: 5,
    }
}

#[tokio::test]
async fn light_block_from_missing_or_local_pool_transaction_executes_and_commits() {
    for local in [false, true] {
        let node = crate::test_support::Node::new();
        let network = NetworkProfile::mainnet();
        let importer = Importer::new(
            node.db.clone(),
            fixtures::execution(&network),
            fixtures::rules(node.db.view().unwrap().identity().genesis),
            Default::default(),
        )
        .unwrap();
        let blocks = fixtures::blocks(&network);
        let Block::Micro(micro) = &blocks[1] else {
            panic!()
        };
        let light = Light::from_micro(micro);
        assert_eq!(
            gossip::precheck_stored(&node.db.view().unwrap(), &light.header, settings()),
            Err(PreparationError::Gossip(gossip::Error::PreviousKeyMissing))
        );
        within(
            importer
                .import_block(blocks[0].encode().unwrap(), NOW)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        if local {
            node.client()
                .push(
                    Arc::new(micro.transactions[0].clone()),
                    ae_mempool::pool::Event::Created,
                    false,
                )
                .unwrap()
                .wait()
                .unwrap();
        }
        let stored = node.db.pool_view().unwrap();
        assert_eq!(
            gossip::precheck_stored(stored.chain(), &light.header, settings()).unwrap(),
            Precheck::Assemble
        );
        let fragment = Fragment::lookup::<PreparationError>(light.clone(), |h| {
            stored
                .transaction(Hash256(h.try_into().unwrap()))
                .map_err(Into::into)
        })
        .unwrap();
        assert_eq!(fragment.missing().count(), usize::from(!local));
        let filled = fragment
            .fill(if local {
                vec![]
            } else {
                micro.transactions.clone()
            })
            .unwrap();
        assert_eq!(&filled, micro);
        drop(stored);
        let report = within(
            importer
                .import_block(Block::Micro(filled).encode().unwrap(), NOW)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        assert_eq!(report.accepted.len(), 1);
        assert!(matches!(
            report.accepted[0].outcome,
            Outcome::Committed { revision: 3 }
        ));
        let stored = node.db.pool_view().unwrap();
        assert_eq!(stored.chain().status().tip, Some(blocks[1].hash().unwrap()));
        assert_eq!(
            stored
                .transaction(micro.transactions[0].hash())
                .unwrap()
                .unwrap(),
            micro.transactions[0]
        );
        assert_eq!(
            gossip::precheck_stored(stored.chain(), &light.header, settings()).unwrap(),
            Precheck::Known
        );
        // Known headers do not authorize replacing a body that fails its root.
        let mut corrupt = micro.clone();
        corrupt.transactions.clear();
        drop(stored);
        let failure = within(
            importer
                .import_block(Block::Micro(corrupt).encode().unwrap(), NOW)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            failure.error,
            Error::Validation {
                operational: false,
                ..
            }
        ));
        assert_eq!(node.db.view().unwrap().status().revision, 3);
        importer.stop();
        importer.join().await;
    }
}
#[tokio::test]
async fn standalone_gossip_import_uses_gossip_policy_and_shared_job_quota() {
    let mut config = Config {
        jobs: 1,
        ..Default::default()
    };
    config.ledger.policy.gossip_depth = 0;
    let (_temp, importer) = setup(config);
    let blocks = fixtures::blocks(&NetworkProfile::mainnet());
    let ticket = importer
        .import_block(blocks[0].encode().unwrap(), NOW)
        .unwrap();
    assert_eq!(importer.available().0, 0);
    assert!(matches!(
        importer.import_block(blocks[0].encode().unwrap(), NOW),
        Err(Error::Busy)
    ));
    within(ticket.wait()).await.unwrap();
    for block in &blocks[1..3] {
        within(
            importer
                .import_block(block.encode().unwrap(), NOW)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
    }
    assert_eq!(importer.available(), (1, 128 << 20));
    let Block::Micro(mut stale) = blocks[1].clone() else {
        panic!()
    };
    stale.header.time += 1;
    let error = within(
        importer
            .import_block(Block::Micro(stale).encode().unwrap(), NOW)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error.error, Error::Policy { height: 1, top: 2 }));
    assert_eq!(importer.context.db.view().unwrap().status().revision, 4);
    importer.stop();
    importer.join().await;
    assert!(matches!(
        importer.import_block(vec![], NOW),
        Err(Error::Closed)
    ));
}
#[tokio::test]
async fn standalone_gossip_uncertain_commit_quarantines_generation_import_too() {
    let (_temp, mut importer) = setup(Default::default());
    Arc::get_mut(&mut importer.context).unwrap().commit_hook = Some(Arc::new(|db, update| {
        db.commit(update)?;
        Err(repository::Error::Backend(
            "lost gossip acknowledgment".into(),
        ))
    }));
    let blocks = fixtures::blocks(&NetworkProfile::mainnet());
    let error = within(
        importer
            .import_block(blocks[0].encode().unwrap(), NOW)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.error,
        Error::CommitUncertain {
            hash: blocks[0].hash().unwrap(),
            error: repository::Error::Backend("lost gossip acknowledgment".into())
        }
    );
    assert_eq!(importer.recovery_reason(), Some(error.error));
    assert_eq!(
        importer.context.db.view().unwrap().status().tip,
        Some(blocks[0].hash().unwrap())
    );
    assert!(matches!(
        importer.import(
            packet(&blocks[2], &[blocks[1].clone()], Direction::Backward),
            blocks[2].hash().unwrap(),
            Direction::Backward,
            NOW
        ),
        Err(Error::RecoveryRequired)
    ));
    importer.stop();
    importer.join().await;
}

#[tokio::test]
async fn resilience_lost_acknowledgment_quarantines_all_shared_import_paths() {
    use ae_chain::resilience::{Config, Event};
    for panic in [false, true] {
        let (_temp, mut importer) = setup(Default::default());
        Arc::get_mut(&mut importer.context).unwrap().policy_hook =
            Some(Arc::new(move |db, event, config| {
                db.update_resilience(event, config)?;
                if panic {
                    panic!("policy worker after publication");
                }
                Err(repository::Error::Backend(
                    "policy acknowledgment lost".into(),
                ))
            }));
        let config = Config {
            depth: 1,
            from_start: false,
        };
        let error = within(importer.policy(Event::Synced, config))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::WorkerPanicked | Error::PolicyUncertain { .. }
        ));
        assert!(
            importer
                .context
                .db
                .view()
                .unwrap()
                .resilience()
                .state
                .synchronized()
        );
        assert_eq!(importer.recovery_reason(), Some(error.clone()));
        let blocks = fixtures::blocks(&NetworkProfile::mainnet());
        assert!(matches!(
            importer.import_block(blocks[0].encode().unwrap(), NOW),
            Err(Error::RecoveryRequired)
        ));
        assert!(matches!(
            importer.import(
                packet(&blocks[0], &[], Direction::Backward),
                blocks[0].hash().unwrap(),
                Direction::Backward,
                NOW
            ),
            Err(Error::RecoveryRequired)
        ));
        assert_eq!(
            within(importer.policy(Event::ConfigChanged, config)).await,
            Err(Error::RecoveryRequired)
        );
        assert_eq!(importer.recovery_reason(), Some(error));
        importer.stop();
        importer.join().await;
    }
}
