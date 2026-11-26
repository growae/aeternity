use super::*;
use crate::test_support::{Node, ledger_fixtures as fixtures};
use ae_p2p::noise;
use ae_protocol::NetworkProfile;

async fn setup() -> (
    Node,
    Arc<Context<Otp26>>,
    watch::Sender<bool>,
    network::Service,
) {
    let node = Node::new();
    let profile = NetworkProfile::mainnet();
    let importer = Importer::new(
        node.db.clone(),
        fixtures::execution(&profile),
        fixtures::rules(node.db.view().unwrap().identity().genesis),
        Default::default(),
    )
    .unwrap();
    let mut network_config = network::Config {
        listen: None,
        ..Default::default()
    };
    network_config.startup.include_default_peers = false;
    network_config.registry.policy.maximum_outbound = 0;
    network_config.registry.policy.maximum_probes = 0;
    let (network, _events, _pool, service) = network::create(
        node.db.clone(),
        node.client(),
        Arc::new(noise::Identity::from_secret([1; 32])),
        profile,
        network_config,
    )
    .await
    .unwrap();
    let config = Config {
        read_jobs: 2,
        read_threads: 1,
        read_bytes: 160 << 20,
        timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let reads = blocking::Pool::new(1, config.read_jobs, config.read_bytes).unwrap();
    let (stop, requested) = watch::channel(false);
    let context = Arc::new(Context {
        importer: Arc::new(importer),
        network,
        config,
        reads,
        policy_source: None,
        stopped: AtomicBool::new(false),
        requested,
        wake: watch::channel(()).0,
        shared: Mutex::new(shared::Book::new()),
        headers: Mutex::new(BTreeMap::new()),
    });
    (node, context, stop, service)
}
async fn finish(context: Arc<Context<Otp26>>, service: network::Service) {
    context.stopped.store(true, Ordering::Release);
    context.reads.stop();
    context.reads.join().await;
    context.importer.stop();
    context.importer.join().await;
    context.network.shutdown();
    assert!(service.run().await.status.joined);
}
#[tokio::test]
async fn live_queries_read_current_storage_and_retire_completed_unread_results() {
    let (_node, context, stop, service) = setup().await;
    let inactive = context.clone().progress().unwrap().wait().await.unwrap();
    assert_eq!(inactive, lifecycle::progress(&[], 0, 0).unwrap());
    assert_eq!(context.estimate(42), Ok(None));
    assert_eq!(context.clone().height().unwrap().wait().await, Ok(0));
    let block = fixtures::blocks(&NetworkProfile::mainnet()).remove(0);
    context
        .importer
        .import_block(block.encode().unwrap(), u64::MAX - 600_000)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(context.clone().height().unwrap().wait().await, Ok(1));
    let ((client, _events, driver), (remote, _events2, driver2)) =
        crate::test_support::pair(7, 8).await;
    let driver = tokio::spawn(driver.run());
    let driver2 = tokio::spawn(driver2.run());
    let header = fixtures::blocks(&NetworkProfile::mainnet())[2].header();
    let limits = coordination::Limits::default();
    let task = coordination::Chain::from_header(42, client.peer(), &header, limits).unwrap();
    context
        .shared
        .lock()
        .unwrap()
        .join(&task, &client, limits, 1)
        .unwrap();
    let progress = context.clone().progress().unwrap().wait().await.unwrap();
    assert_eq!(
        (progress.syncing, progress.task, progress.target),
        (true, Some(42), 2)
    );
    assert_eq!(progress.percent, 50.0);
    assert_eq!(
        context.estimate(42),
        Err(Error::Statistics(statistics::Error::UndefinedEstimate))
    );
    context
        .importer
        .database()
        .rollback(repository::rollback::Target::Height(0), Default::default())
        .unwrap();
    assert_eq!(
        context
            .clone()
            .progress()
            .unwrap()
            .wait()
            .await
            .unwrap()
            .percent,
        0.0
    );
    let unread = context.clone().height().unwrap();
    context.reads.drain().await;
    stop.send_replace(true);
    assert_eq!(unread.wait().await, Err(Error::Closed));
    assert!(matches!(context.clone().progress(), Err(Error::Closed)));
    assert_eq!(context.estimate(42), Err(Error::Closed));
    client.shutdown();
    remote.shutdown();
    driver.await.unwrap();
    driver2.await.unwrap();
    finish(context, service).await;
}
#[tokio::test]
async fn live_query_capacity_cancellation_and_recovery_belong_to_the_sync_owner() {
    let (_node, context, _stop, service) = setup().await;
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let active = context
        .reads
        .submit(1, move |_| {
            entered_tx.send(()).unwrap();
            blocked.recv().unwrap();
        })
        .unwrap();
    entered.await.unwrap();
    let queued = context.clone().height().unwrap();
    assert!(matches!(context.clone().progress(), Err(Error::Busy)));
    drop(queued);
    release.send(()).unwrap();
    active.await.unwrap();
    context.reads.drain().await;
    assert_eq!(context.reads.available(), (2, context.config.read_bytes));
    assert_eq!(context.clone().height().unwrap().wait().await, Ok(0));
    *context.importer.context.recovery.lock().unwrap() =
        Some(super::super::super::Error::RecoveryRequired);
    assert!(matches!(
        context.clone().progress(),
        Err(Error::Policy(super::super::super::Error::RecoveryRequired))
    ));
    finish(context, service).await;
}
