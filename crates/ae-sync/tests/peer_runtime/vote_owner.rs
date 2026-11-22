use super::*;
use ae_sync::runtime::{connection as owner, workers};
use connection_owner::settings;

#[tokio::test]
async fn pos_connection_owner_preserves_the_unimplemented_importer_guard() {
    let mut node = Node::new();
    node.stop_worker();
    let mut config = worker_owner::worker_config();
    config.pool.consensus = ConsensusType::Pos;
    let (mut workers, service) = workers::create(
        node.db.clone(),
        NetworkProfile::mainnet(),
        config,
        Default::default(),
    )
    .unwrap();
    let worker_task = tokio::spawn(service.run());
    within(async {
        while !workers.status().ready {
            assert!(workers.status().running);
            workers.changed().await.unwrap();
        }
    })
    .await;
    let bindings = workers.bindings();
    assert!(matches!(
        owner::create_with_workers(
            worker_owner::inputs(&node, &bindings),
            settings(),
            || Ok(Default::default()),
            bindings.clone()
        ),
        Err(owner::Error::Configuration)
    ));
    let mut inputs = worker_owner::inputs(&node, &bindings);
    inputs.execution = common::ledger_fixtures::execution_with_consensus(
        &NetworkProfile::mainnet(),
        ConsensusType::Pos,
    );
    let (_handle, service) =
        owner::create_with_workers(inputs, settings(), || Ok(Default::default()), bindings)
            .unwrap();
    let status = within(service.run()).await;
    assert!(status.joined);
    assert!(
        matches!(status.error,Some(owner::Error::Startup(owner::Failure {child:owner::Child::Importer,detail})) if detail=="WrongConsensus")
    );
    workers.shutdown();
    assert!(within(worker_task).await.unwrap().joined);
}
