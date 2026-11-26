use super::*;
use crate::{
    chain::hyperchain::eoe as live,
    test_support::hyperchain::{
        Node,
        parents::{ParentBackend, parent, wait},
    },
    votes,
};
use ae_hyperchain::{
    eoe::{local_host, runtime},
    parent_cache::{self, runtime as cache},
    parent_connector::runtime as connector,
};
use ae_storage::pool;
use ae_vm_fate::maps::order::Otp26;
use std::thread;

#[derive(Clone)]
struct Targets {
    hold: Arc<AtomicBool>,
    entered: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
}
impl cache::Targets for Targets {
    fn targets(&mut self, _: &ae_types::Unsigned) -> Result<Vec<BigInt>, cache::TargetError> {
        if self.hold.swap(false, Ordering::AcqRel) {
            self.entered.store(true, Ordering::Release);
            let end = Instant::now() + Duration::from_secs(5);
            while !self.release.load(Ordering::Acquire) {
                assert!(Instant::now() < end, "held parent callback expired");
                thread::sleep(Duration::from_millis(1));
            }
        }
        Ok(vec![1.into(), 6.into()])
    }
}
struct Parents {
    cache: Option<cache::Runtime>,
    connector: Option<connector::Runtime>,
    client: cache::Client,
    targets: Targets,
}
impl Parents {
    fn new(node: &Node) -> Self {
        let mailbox = cache::Mailbox::new(
            parent_cache::Cache::new(
                parent_cache::Config {
                    child_start_height: 0.into(),
                    max_size: 200.into(),
                    retry_interval: 1000.into(),
                    finality: 2.into(),
                },
                parent_cache::ChildTop {
                    hash: node.db.view().unwrap().status().tip.unwrap(),
                    height: 0.into(),
                },
            ),
            Default::default(),
        )
        .unwrap();
        let targets = Targets {
            hold: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(AtomicBool::new(false)),
            release: Arc::new(AtomicBool::new(false)),
        };
        let connector = connector::Runtime::spawn(
            Arc::new(ParentBackend {
                missing: AtomicBool::new(true),
            }),
            3,
            [0; 16],
            mailbox.sink(),
            connector::Config {
                fetch_interval: None,
                ..Default::default()
            },
        )
        .unwrap();
        let cache = mailbox
            .spawn(
                targets.clone(),
                cache::ConnectorLink::new(connector.client()),
                node.config.profile.order.clone(),
            )
            .unwrap();
        let client = cache.client();
        client.post_block(parent(8, 1008)).unwrap();
        client.barrier(Duration::from_secs(5)).unwrap();
        Self {
            cache: Some(cache),
            connector: Some(connector),
            client,
            targets,
        }
    }
    fn retire(&mut self) {
        self.client.request_stop();
        self.targets.release.store(true, Ordering::Release);
        self.cache.take().unwrap().retire().result.unwrap();
    }
}
impl Drop for Parents {
    fn drop(&mut self) {
        self.targets.release.store(true, Ordering::Release);
        if let Some(cache) = self.cache.take() {
            cache.shutdown().unwrap();
        }
        if let Some(connector) = self.connector.take() {
            connector.shutdown().unwrap();
        }
    }
}
struct Fixture {
    owner: Owner,
    importer: Importer<Otp26>,
    accepted: scoped::Accepted<Otp26, inputs::Effects>,
    pool: worker::Worker,
    parents: Parents,
}
fn fixture(node: &Node) -> Fixture {
    let parents = Parents::new(node);
    let (importer, accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        scoped::Cached {
            cache: parents.client.clone(),
            pins: Arc::new(Default::default()),
            parent_start: 1.into(),
            parent_length: 5.into(),
            limits: Default::default(),
        },
        Default::default(),
    )
    .unwrap();
    let source = live::Source::new(
        node.db.clone(),
        node.config.identity.clone(),
        node.config.profile.network.clone(),
        Default::default(),
    )
    .unwrap();
    let config = super::super::super::Config {
        eoe: live::Config {
            host: local_host::Config {
                network: node.config.profile.network.clone(),
                election_contract: node.config.profile.contracts.election,
                parent_epoch_length: 5.into(),
                parent_start_height: 1.into(),
                limits: Default::default(),
            },
            block_time: 4.into(),
            coordinator: Default::default(),
            runtime: runtime::Config {
                output_capacity: 1,
                ..Default::default()
            },
            epochs: Default::default(),
        },
        votes: Default::default(),
        pool: Default::default(),
        calls: Default::default(),
    };
    let owner = Owner::start(
        super::super::super::Inputs {
            source,
            database: node.db.clone(),
            publications: votes::Publications::new(&node.db, config.votes).unwrap(),
            gossip: worker::GossipCache::new(16, 128).unwrap(),
            parent: Some(parents.client.clone()),
        },
        config,
        Default::default(),
        node.config.profile.order.clone(),
    )
    .unwrap();
    let pool = worker::Worker::spawn(
        node.db.clone(),
        node.config.profile.network.clone(),
        pool::Config {
            consensus: ae_protocol::ConsensusType::Pos,
            ..Default::default()
        },
        worker::Settings {
            origins_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    Fixture {
        owner,
        importer,
        accepted,
        pool,
        parents,
    }
}
fn prepared(f: &mut Fixture, node: &Node, missing: bool) -> Attempt<Otp26> {
    let producer = f.accepted.producer();
    let mut attempt = f
        .owner
        .begin_production(producer.clone(), f.pool.client(), Default::default())
        .unwrap();
    // Assemble an actual cold-cache micro build in the production retirement
    // owner. Running next_producer first would warm the shared leader cache
    // and bypass the very parent reads this regression must exercise.
    let mut micro = f
        .owner
        .micro_candidate(
            producer,
            f.pool.client(),
            candidate::Input {
                parent: node.db.view().unwrap().status().tip.unwrap(),
                limits: Default::default(),
                pins: Arc::new(Default::default()),
                inputs: Default::default(),
                timeout: core::Timeout::Disabled,
                window_ms: 1000,
            },
        )
        .unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        f.owner.step().unwrap();
        if micro.step(&mut f.owner, 999).unwrap() == candidate::Progress::Prepared {
            break;
        }
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(micro.candidate().unwrap().outcome.is_err(), missing);
    if missing {
        assert_eq!(
            micro.pending_effects().unwrap(),
            &VecDeque::from(vec![inputs::Effect::RequestParent(1.into()); 2])
        );
    } else {
        let effects = micro.pending_effects().unwrap();
        assert!(effects.len() >= 2);
        assert!(
            effects
                .iter()
                .all(|effect| matches!(effect, inputs::Effect::Parent { .. }))
        );
    }
    attempt.micro = Some(micro);
    attempt.phase = Phase::Micro;
    attempt
}
fn submit(f: &mut Fixture, attempt: &mut Attempt<Otp26>) {
    assert_eq!(
        attempt
            .micro
            .as_mut()
            .unwrap()
            .step(&mut f.owner, 999)
            .unwrap(),
        candidate::Progress::Effects(delivery::Progress::Submitted)
    );
}
fn retire_eoe(f: &mut Fixture, retiring: &mut Retiring<Otp26>) {
    let old = f.owner.runtime_client();
    old.request_stop();
    wait(|| f.owner.runtime_finished());
    f.owner.begin_recovery().unwrap();
    retiring.retire_eoe(&old).unwrap();
}
async fn finish(f: Fixture) {
    assert!(f.owner.shutdown().errors.is_empty());
    f.pool.shutdown().unwrap();
    f.importer.stop();
    f.importer.join().await;
}

#[tokio::test]
async fn missing_key_retry_counts_after_native_sleep_and_cancellation_suppresses_the_reply() {
    use crate::test_support::mining_metrics::MiningMetrics;
    for cancel in [false, true] {
        let node = Node::new(1);
        let mut f = fixture(&node);
        let (metrics, _binding) = MiningMetrics::bind(node.db.instrumentation());
        let mut attempt = f
            .owner
            .begin_production(f.accepted.producer(), f.pool.client(), Default::default())
            .unwrap();
        // Exercise the sealing failure boundary with a real preset-key worker
        // whose key map is empty. No consensus admission or key is fabricated.
        let block = node
            .blocks()
            .into_iter()
            .find(|b| matches!(b, Block::Key(_)))
            .unwrap();
        attempt.block = Some(block);
        attempt.phase = Phase::Seal;
        let now = Instant::now();
        let end = now + Duration::from_secs(5);
        while attempt.deadline().is_none() {
            assert!(Instant::now() < end);
            f.owner.step().unwrap();
            attempt.step(&mut f.owner, &f.importer, 1000, now).unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        let deadline = attempt.deadline().unwrap();
        assert_eq!(deadline.duration_since(now), Duration::from_secs(1));
        assert_eq!(metrics.counts(), (0, 0));
        assert_eq!(
            attempt
                .step(
                    &mut f.owner,
                    &f.importer,
                    1999,
                    deadline - Duration::from_nanos(1)
                )
                .unwrap(),
            Progress::Waiting
        );
        if cancel {
            attempt.cancel();
            assert_eq!(
                attempt.step(&mut f.owner, &f.importer, 2000, deadline),
                Err(Error::Cancelled)
            );
            assert_eq!(metrics.counts(), (0, 0));
        } else {
            attempt
                .step(&mut f.owner, &f.importer, 2000, deadline)
                .unwrap();
            assert_eq!(metrics.counts(), (0, 1));
            attempt
                .step(&mut f.owner, &f.importer, 2000, deadline)
                .unwrap();
            assert_eq!(metrics.counts(), (0, 1));
        }
        drop(attempt);
        finish(f).await;
        assert_eq!(metrics.counts(), (0, u64::from(!cancel)));
    }
}

#[tokio::test]
async fn micro_cache_receipts_keep_success_rejection_and_unknown_after_join() {
    for index in [1, 3, 5] {
        for mode in 0..3 {
            let node = Node::new(index);
            let mut f = fixture(&node);
            let mut attempt = prepared(&mut f, &node, true);
            let before = node.db.view().unwrap().status();
            let old = f.parents.client.clone();
            old.barrier(Duration::from_secs(5)).unwrap();
            let reads = old.snapshot().lookup_calls;
            match mode {
                0 => {
                    old.post_block(parent(1, 1001)).unwrap();
                    old.barrier(Duration::from_secs(5)).unwrap();
                }
                1 => old.configure(cache::Limits {
                    cache: parent_cache::Limits {
                        maximum_bytes: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                }),
                _ => {
                    f.parents.targets.hold.store(true, Ordering::Release);
                    old.post_block(parent(9, 1009)).unwrap();
                    wait(|| f.parents.targets.entered.load(Ordering::Acquire));
                }
            }
            submit(&mut f, &mut attempt);
            if mode != 2 {
                old.barrier(Duration::from_secs(5)).unwrap();
            }
            let mut retiring = f.owner.retire_production(attempt).unwrap();
            assert_eq!(retiring.retire_cache(&old), Err(Error::Binding));
            f.parents.retire();
            retiring.retire_cache(&old).unwrap();
            // A second retirement with no matching receipt cannot erase the
            // first result, including an accepted call with unknown execution.
            retire_eoe(&mut f, &mut retiring);
            retiring.retire_cache(&old).unwrap();
            assert!(retiring.step(&f.owner, &f.importer).unwrap());
            let report = retiring.finish().unwrap();
            let candidate = report.candidate.unwrap();
            assert_eq!(candidate.acknowledged_effects, u64::from(mode == 0));
            assert_eq!(
                candidate.effect_error,
                match mode {
                    0 => None,
                    1 => Some(delivery::Error::Cache(cache::Error::Core(
                        parent_cache::Error::ResourceLimit
                    ))),
                    _ => Some(delivery::Error::ReceiptLost(delivery::Target::Cache)),
                }
            );
            assert_eq!(
                candidate.discarded_effects,
                vec![inputs::Effect::RequestParent(1.into()); if mode == 0 { 1 } else { 2 }]
            );
            assert_eq!(old.snapshot().lookup_calls, reads + u64::from(mode != 2));
            assert_eq!(node.db.view().unwrap().status(), before);
            assert_eq!(f.pool.info().submitted, 0);
            finish(f).await;
        }
    }
}

#[tokio::test]
async fn micro_eoe_unknown_receipt_survives_subsequent_cache_retirement() {
    for index in [1, 3, 5] {
        let node = Node::new(index);
        let mut f = fixture(&node);
        f.parents.client.post_block(parent(1, 1001)).unwrap();
        f.parents.client.barrier(Duration::from_secs(5)).unwrap();
        let mut attempt = prepared(&mut f, &node, false);
        let before = node.db.view().unwrap().status();
        let expected = attempt
            .micro
            .as_ref()
            .unwrap()
            .pending_effects()
            .unwrap()
            .clone();
        let old = f.owner.runtime_client();
        // Real entropy publications fill the output channel. The next call
        // holds the runtime there, leaving the candidate notification queued.
        for _ in 0..2 {
            assert!(matches!(
                old.call(runtime::Command::Entropy(3.into()))
                    .unwrap_or_else(|_| panic!("entropy admission"))
                    .wait(Duration::from_secs(5))
                    .unwrap()
                    .unwrap(),
                runtime::Applied::Entropy(Some(_))
            ));
        }
        let filler = old
            .call(runtime::Command::Entropy(3.into()))
            .unwrap_or_else(|_| panic!("filler admission"));
        submit(&mut f, &mut attempt);
        let mut retiring = f.owner.retire_production(attempt).unwrap();
        retire_eoe(&mut f, &mut retiring);
        f.parents.retire();
        retiring.retire_cache(&f.parents.client).unwrap();
        retiring.retire_eoe(&old).unwrap();
        assert!(retiring.step(&f.owner, &f.importer).unwrap());
        let candidate = retiring.finish().unwrap().candidate.unwrap();
        assert_eq!(candidate.acknowledged_effects, 0);
        assert_eq!(
            candidate.effect_error,
            Some(delivery::Error::ReceiptLost(delivery::Target::Eoe))
        );
        assert_eq!(candidate.discarded_effects, expected);
        assert_eq!(node.db.view().unwrap().status(), before);
        assert_eq!(f.pool.info().submitted, 0);
        drop(filler);
        finish(f).await;
    }
}
