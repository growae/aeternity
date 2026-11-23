use super::*;
use ae_hyperchain::eoe::pool;
use ae_storage::pool::worker::GossipCache;
use ae_sync::votes::{
    self,
    eoe::{Owner, Routed},
    independent::Relay,
};

type OutputOwner = Owner<live::StateReader, live::StateReader>;
struct Connected {
    source: Arc<live::Source>,
    hub: pool::Hub,
    output: OutputOwner,
    worker: votes::Worker,
    epochs: live::Epochs,
    publications: votes::Publications,
    cache: GossipCache,
    config: votes::Config,
}
fn connected(node: &Node, pool_config: pool::Config, config: votes::Config) -> Connected {
    let source = source(node);
    let hub = pool::Hub::new(node.config.profile.network.clone(), pool_config).unwrap();
    let live::Connected { runtime, epochs } = source
        .connect_independent(
            super::config(node),
            BTreeMap::new(),
            node.config.profile.order.clone(),
            hub.clone(),
        )
        .unwrap();
    let publications = votes::Publications::new(&node.db, config).unwrap();
    let output =
        Owner::new(runtime, publications.clone()).unwrap_or_else(|e| panic!("{:?}", e.error));
    let cache = GossipCache::default();
    let worker = votes::Worker::spawn_independent(
        node.db.clone(),
        cache.clone(),
        config,
        output.relay().unwrap(),
        Default::default(),
    )
    .unwrap();
    Connected {
        source,
        hub,
        output,
        worker,
        epochs,
        publications,
        cache,
        config,
    }
}
async fn push(
    client: &votes::Client,
    signed: &SignedTransaction<Transaction>,
) -> Result<hc_votes::PushOutcome, votes::Error> {
    common::within(
        client
            .push(signed.encode(), hc_votes::Event::Received)
            .unwrap(),
    )
    .await
}
async fn publish(output: &mut OutputOwner) {
    common::within(async {
        loop {
            match output.route().unwrap() {
                Routed::Published => break,
                Routed::Idle => tokio::time::sleep(Duration::from_millis(1)).await,
                other => panic!("unexpected relay result {other:?}"),
            }
        }
    })
    .await
}
fn epochs_current(epochs: &mut live::Epochs, node: &Node) {
    let until = Instant::now() + Duration::from_secs(5);
    while epochs.cursor().revision() < node.db.events().cursor().revision() {
        assert!(Instant::now() < until);
        assert!(epochs.step().is_ok(), "{:?}", epochs.failure());
    }
}
#[tokio::test]
async fn peer_admission_publication_and_committed_epochs_remain_live_during_eoe_gap() {
    let node = Node::new(0);
    let mut c = connected(&node, Default::default(), Default::default());
    let client = c.worker.client();
    let mut events = c.publications.listen().unwrap();
    let first = vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13);
    assert!(matches!(
        push(&client, &first).await.unwrap(),
        hc_votes::PushOutcome::Accepted { inserted: true, .. }
    ));
    c.output.begin_retirement();
    assert!(c.output.is_finished());
    assert_eq!(c.source.snapshots().live_snapshots(), 0);
    publish(&mut c.output).await;
    assert_eq!(
        *common::within(events.recv()).await.unwrap().transaction,
        first
    );
    assert_eq!(
        common::within(client.stats().unwrap())
            .await
            .unwrap()
            .entries,
        1
    );
    assert!(c.output.take_retirement().is_some());
    // No fresh epoch feed may silently replace the live cursor during a gap.
    assert!(matches!(
        c.source.connect_independent(
            super::config(&node),
            BTreeMap::new(),
            node.config.profile.order.clone(),
            c.hub.clone()
        ),
        Err(live::Error::AlreadyRunning)
    ));
    assert!(matches!(
        c.source.connect(
            super::config(&node),
            BTreeMap::new(),
            node.config.profile.order.clone(),
        ),
        Err(live::Error::AlreadyRunning)
    ));
    let insert = inserted(&node);
    let blocks = node.blocks();
    for block in &blocks[..6] {
        insert.insert(block, 1_000_000, Origin::Gossip).unwrap();
    }
    epochs_current(&mut c.epochs, &node);
    let stats = common::within(client.stats().unwrap()).await.unwrap();
    assert_eq!(stats.epoch, 2.into());
    assert_eq!(stats.entries, 1);
    let second = vote(&node, eoe::Mode::Length, eoe::VoteType::Vote, 14);
    assert!(matches!(
        push(&client, &second).await.unwrap(),
        hc_votes::PushOutcome::Accepted { inserted: true, .. }
    ));
    publish(&mut c.output).await;
    assert_eq!(
        *common::within(events.recv()).await.unwrap().transaction,
        second
    );
    for block in &blocks[6..13] {
        insert.insert(block, 1_000_000, Origin::Gossip).unwrap();
    }
    epochs_current(&mut c.epochs, &node);
    let stats = common::within(client.stats().unwrap()).await.unwrap();
    assert_eq!(stats.epoch, 3.into());
    assert_eq!(stats.entries, 0);
    assert_eq!(c.epochs.acknowledged(), 2);
    assert!(client.info().accepting);
    c.worker.shutdown().unwrap();
    c.output.shutdown().unwrap();
}

#[tokio::test]
async fn independent_worker_requires_matching_database_and_runs_without_eoe() {
    let node = Node::new(0);
    let other = Node::new(0);
    let pow = common::Node::new();
    let config = votes::Config::default();
    let hub = pool::Hub::new(node.config.profile.network.clone(), Default::default()).unwrap();
    let publications = votes::Publications::new(&node.db, config).unwrap();
    let pow_publications = votes::Publications::new(&pow.db, config).unwrap();
    assert!(matches!(
        Relay::new(hub.clone(), pow_publications.clone()),
        Err(votes::Error::Worker(votes::Fault::Configuration))
    ));
    // Matching a PoW network still does not permit an independent PoS relay.
    let pow_hub =
        pool::Hub::new(ae_protocol::NetworkProfile::mainnet(), Default::default()).unwrap();
    assert!(matches!(
        Relay::new(pow_hub, pow_publications),
        Err(votes::Error::Worker(votes::Fault::Configuration))
    ));
    let mut relay = Relay::new(hub.clone(), publications.clone()).unwrap();
    assert!(matches!(
        votes::Worker::spawn_independent(
            other.db.clone(),
            GossipCache::default(),
            config,
            &relay,
            Default::default(),
        ),
        Err(votes::Error::Worker(votes::Fault::Configuration))
    ));
    assert!(hub.client().is_none());
    let worker = votes::Worker::spawn_independent(
        node.db.clone(),
        GossipCache::default(),
        config,
        &relay,
        Default::default(),
    )
    .unwrap();
    let tx = vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13);
    assert!(matches!(
        push(&worker.client(), &tx).await.unwrap(),
        hc_votes::PushOutcome::Accepted { inserted: true, .. }
    ));
    let mut events = publications.listen().unwrap();
    assert_eq!(
        relay.step().unwrap(),
        votes::independent::Progress::Published
    );
    assert_eq!(
        *common::within(events.recv()).await.unwrap().transaction,
        tx
    );
    worker.shutdown().unwrap();
}
#[tokio::test]
async fn pool_replacement_preserves_eoe_publications_cache_and_shared_reply_limits() {
    let node = Node::new(0);
    let mut c = connected(
        &node,
        Default::default(),
        votes::Config {
            reply_bytes: 2 * (Unsigned::from(0).as_bytes().len() + 128),
            ..Default::default()
        },
    );
    let original = c.output.client();
    let old = c.worker.client();
    let first = vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13);
    push(&old, &first).await.unwrap();
    let one = common::within(old.stats().unwrap()).await.unwrap();
    let two = common::within(old.stats().unwrap()).await.unwrap();
    c.worker.shutdown().unwrap();
    assert!(!old.info().accepting);
    assert!(!original.is_closed());
    assert!(c.hub.client().is_none());
    assert!(matches!(
        Relay::new(c.hub.clone(), c.publications.clone()),
        Err(votes::Error::Worker(votes::Fault::Busy))
    ));
    let replacement = votes::Worker::spawn_independent(
        node.db.clone(),
        c.cache.clone(),
        c.config,
        c.output.relay().unwrap(),
        Default::default(),
    )
    .unwrap();
    let client = replacement.client();
    assert!(original.same_runtime(&c.output.client()));
    assert!(matches!(
        common::within(client.stats().unwrap()).await,
        Err(votes::Error::Worker(votes::Fault::Busy))
    ));
    drop(one);
    let new = common::within(client.stats().unwrap()).await.unwrap();
    assert_eq!(new.entries, 0);
    assert_eq!(new.epoch, 0.into());
    drop((two, new));
    assert_eq!(
        push(&client, &first).await.unwrap(),
        hc_votes::PushOutcome::Cached
    );
    let mut events = c.publications.listen().unwrap();
    publish(&mut c.output).await;
    assert_eq!(
        *common::within(events.recv()).await.unwrap().transaction,
        first
    );
    c.cache.reset().unwrap();
    assert!(matches!(
        push(&client, &first).await.unwrap(),
        hc_votes::PushOutcome::Accepted { inserted: true, .. }
    ));
    publish(&mut c.output).await;
    assert_eq!(
        *common::within(events.recv()).await.unwrap().transaction,
        first
    );
    replacement.shutdown().unwrap();
    c.output.shutdown().unwrap();
}
#[tokio::test]
async fn independent_pressure_rolls_back_cache_miss_but_native_rejection_keeps_it() {
    let node = Node::new(0);
    let mut c = connected(
        &node,
        pool::Config {
            maximum_publications: 1,
            ..Default::default()
        },
        Default::default(),
    );
    let client = c.worker.client();
    let first = vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13);
    let second = vote(&node, eoe::Mode::Length, eoe::VoteType::Vote, 14);
    push(&client, &first).await.unwrap();
    for _ in 0..2 {
        assert_eq!(
            push(&client, &second).await,
            Err(votes::Error::Pool(hc_votes::Error::ResourceLimit))
        );
        assert_eq!(c.cache.stats().unwrap().miss, 1.into());
        assert_eq!(
            common::within(client.stats().unwrap())
                .await
                .unwrap()
                .entries,
            1
        );
    }
    publish(&mut c.output).await;
    assert!(matches!(
        push(&client, &second).await.unwrap(),
        hc_votes::PushOutcome::Accepted { inserted: true, .. }
    ));
    assert_eq!(c.cache.stats().unwrap().miss, 2.into());
    publish(&mut c.output).await;
    let invalid = SignedTransaction::new(
        first.transaction().clone(),
        vec![ae_types::Signature([0; 64])],
    );
    assert!(matches!(
        push(&client, &invalid).await,
        Err(votes::Error::Pool(hc_votes::Error::Signature(_)))
    ));
    assert_eq!(
        push(&client, &invalid).await.unwrap(),
        hc_votes::PushOutcome::Cached
    );
    assert_eq!(
        common::within(client.stats().unwrap())
            .await
            .unwrap()
            .entries,
        2
    );
    c.worker.shutdown().unwrap();
    c.output.shutdown().unwrap();
}
#[tokio::test]
async fn epoch_source_gap_closes_independent_authorities_but_keeps_accepted_publication() {
    let node = Node::with_events(ae_storage::events::Config {
        records: 1,
        bytes: 1 << 20,
    });
    let mut c = connected(&node, Default::default(), Default::default());
    let client = c.worker.client();
    let runtime = c.output.client();
    let tx = vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13);
    push(&client, &tx).await.unwrap();
    let insert = inserted(&node);
    for block in node.blocks().iter().take(2) {
        insert.insert(block, 1_000_000, Origin::Gossip).unwrap();
    }
    assert!(matches!(
        c.epochs.step(),
        Err(live::Error::Source(ae_storage::events::Error::Gap { .. }))
    ));
    assert!(c.hub.is_closed());
    assert!(runtime.is_closed());
    assert!(c.epochs.retry(Default::default()).is_err());
    c.output.begin_retirement();
    let mut events = c.publications.listen().unwrap();
    publish(&mut c.output).await;
    assert_eq!(
        *common::within(events.recv()).await.unwrap().transaction,
        tx
    );
    assert!(c.hub.client().is_none());
    assert_eq!(c.hub.new_epoch(99.into()), Err(pool::Error::Closed));
    assert!(matches!(
        c.hub.start_pool(Default::default()),
        Err(pool::Error::Closed)
    ));
    let _ = c.worker.shutdown();
    c.output.shutdown().unwrap();
}
#[tokio::test]
async fn relay_retains_exact_publications_under_peer_pressure_and_through_handoff() {
    let node = Node::new(0);
    let mut c = connected(
        &node,
        Default::default(),
        votes::Config {
            events: 1,
            ..Default::default()
        },
    );
    let client = c.worker.client();
    let first = vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13);
    let second = vote(&node, eoe::Mode::Length, eoe::VoteType::Vote, 14);
    push(&client, &first).await.unwrap();
    push(&client, &second).await.unwrap();
    publish(&mut c.output).await;
    let before = c.hub.retained_publication_bytes();
    // Pool output pressure does not prevent an EOE control completion.
    c.output
        .client()
        .call(runtime::Command::Retry)
        .unwrap_or_else(|_| panic!("control"))
        .wait(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    assert_eq!(c.output.route().unwrap(), Routed::Backpressured);
    assert_eq!(c.output.pending_publication().unwrap().signed(), &second);
    c.worker.shutdown().unwrap();
    c.output.begin_retirement();
    let retained = c.output.take_pending_publication().unwrap();
    assert_eq!(retained.signed(), &second);
    assert!(c.hub.retained_publication_bytes() > 0);
    assert!(c.hub.retained_publication_bytes() <= before);
    let mut events = c.publications.listen().unwrap();
    assert_eq!(
        *common::within(events.recv()).await.unwrap().transaction,
        first
    );
    c.output.shutdown().unwrap();
    drop(c.epochs);
    assert_eq!(retained.signed(), &second);
    drop(retained);
    assert_eq!(c.hub.retained_publication_bytes(), 0);
}

#[tokio::test]
async fn peer_publication_pressure_does_not_starve_eoe_supervision_output() {
    let node = Node::new(0);
    let mut c = connected(
        &node,
        Default::default(),
        votes::Config {
            events: 1,
            ..Default::default()
        },
    );
    let client = c.worker.client();
    let first = vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13);
    push(&client, &first).await.unwrap();
    publish(&mut c.output).await;
    let proposal = vote(&node, eoe::Mode::Fork, eoe::VoteType::Proposal, 13);
    let TransactionLeaf::HcVote(v) = proposal.transaction().leaf() else {
        panic!("vote")
    };
    let mut data = v.data().clone();
    data.insert(eoe::payload::SIGNATURE.to_vec(), vec![1]);
    let bad = finalization::sign(
        Transaction::new(TransactionLeaf::HcVote(ae_tx::HcVote::new(
            v.voter(),
            v.epoch().clone(),
            v.vote_type().clone(),
            data,
        ))),
        &signer(13),
        &node.config.profile.network,
    );
    push(&client, &bad).await.unwrap();
    common::within(async {
        loop {
            match c.output.route().unwrap() {
                Routed::Output(runtime::Output::Service(
                    coordinator::ServiceOutput::Restarted {
                        mode: eoe::Mode::Fork,
                        ..
                    },
                )) => break,
                Routed::Idle | Routed::Backpressured => {
                    tokio::time::sleep(Duration::from_millis(1)).await
                }
                other => panic!("unexpected output {other:?}"),
            }
        }
    })
    .await;
    assert_eq!(c.output.pending_publication().unwrap().signed(), &bad);
    let mut events = c.publications.listen().unwrap();
    assert_eq!(
        *common::within(events.recv()).await.unwrap().transaction,
        first
    );
    publish(&mut c.output).await;
    assert_eq!(
        *common::within(events.recv()).await.unwrap().transaction,
        bad
    );
    c.worker.shutdown().unwrap();
    c.output.shutdown().unwrap();
}
