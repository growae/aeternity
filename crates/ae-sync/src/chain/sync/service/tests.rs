use super::*;
use crate::{chain, gossip, test_support as common};
use ae_p2p::{noise, ping};
use ae_protocol::{ConsensusType, NetworkProfile};
use common::{Node, ledger_fixtures as fixtures, within};
use std::sync::atomic::AtomicU64;
use tokio::{net::TcpStream, task::JoinHandle};
mod recovery;

const WEIGHT: usize = 80 << 20;
fn importer(node: &Node) -> Importer {
    let network = NetworkProfile::mainnet();
    Importer::new(
        node.db.clone(),
        fixtures::execution(&network),
        fixtures::rules(node.db.view().unwrap().identity().genesis),
        chain::Config {
            jobs: 3,
            bytes: 3 * WEIGHT,
            ..Default::default()
        },
    )
    .unwrap()
}
async fn source() -> Node {
    seeded(Node::new()).await
}
async fn seeded(node: Node) -> Node {
    let importer = importer(&node);
    let blocks = fixtures::blocks(&NetworkProfile::mainnet());
    for (key, micros) in [(&blocks[0], &[][..]), (&blocks[2], &blocks[1..2])] {
        importer
            .import(
                common::import::packet(key, micros, chain::Direction::Backward),
                key.hash().unwrap(),
                chain::Direction::Backward,
                u64::MAX - 600_000,
            )
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    importer.stop();
    importer.join().await;
    node
}
struct Running {
    handle: Handle,
    context: Arc<Context<Otp26>>,
    _events: Option<Events>,
    task: JoinHandle<Status>,
    network: network::Handle,
    network_task: JoinHandle<network::Exit>,
    gossip: gossip::Handle,
    gossip_task: JoinHandle<gossip::Status>,
    application: peer::Handle,
    application_task: JoinHandle<peer::Status>,
    peers: Vec<(net::Client, JoinHandle<net::Exit>, JoinHandle<()>)>,
    identity: Arc<noise::Identity>,
}
impl Running {
    async fn new(node: &Node, source: &Node, importer: Importer) -> Self {
        let identity = Arc::new(noise::Identity::from_secret([1; 32]));
        let mut config = network::Config {
            listen: Some("127.0.0.1:0".parse().unwrap()),
            ..Default::default()
        };
        config.startup.include_default_peers = false;
        config.registry.policy.maximum_outbound = 0;
        config.registry.policy.maximum_probes = 0;
        let (mut network, scheduled, pool, service) = network::create(
            node.db.clone(),
            node.client(),
            identity.clone(),
            NetworkProfile::mainnet(),
            config,
        )
        .await
        .unwrap();
        let network_task = tokio::spawn(service.run());
        let (gossip, incoming, service) = gossip::create(
            node.db.clone(),
            node.client(),
            network.peers(),
            pool,
            NetworkProfile::mainnet(),
            ConsensusType::Pow,
            Default::default(),
        )
        .unwrap();
        let gossip_task = tokio::spawn(service.run());
        let (application, events, service) = peer::create(
            node.db.clone(),
            network.clone(),
            scheduled,
            incoming,
            Default::default(),
        )
        .unwrap();
        let application_task = tokio::spawn(service.run());
        let (handle, events, service) = create(
            importer,
            network.clone(),
            application.clone(),
            events,
            Config {
                read_bytes: 3 * WEIGHT,
                ..Default::default()
            },
        )
        .unwrap();
        let context = service.context.clone();
        let task = tokio::spawn(service.run());
        within(async {
            while !network.status().ready {
                network.changed().await.unwrap();
            }
        })
        .await;
        let mut running = Self {
            handle,
            context,
            _events: Some(events),
            task,
            network,
            network_task,
            gossip,
            gossip_task,
            application,
            application_task,
            peers: vec![],
            identity,
        };
        for n in 0..3 {
            running.connect(node, source, n).await;
        }
        running
    }
    async fn connect(&mut self, node: &Node, source: &Node, n: u8) {
        let id = noise::Identity::from_secret([n + 9; 32]);
        let binding = node.db.view().unwrap().identity().clone();
        let network = noise::Network::new(1, binding.genesis, &binding.network).unwrap();
        let socket = TcpStream::connect(self.network.status().listening.unwrap())
            .await
            .unwrap();
        let (client, mut events, driver) = net::establish(
            socket,
            &id,
            &network,
            noise::Role::Initiator(self.identity.public_key()),
            Default::default(),
        )
        .await
        .unwrap();
        let driver = tokio::spawn(driver.run());
        let remote = client.clone();
        let db = source.db.clone();
        let server = tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                if let net::Event::Message(received) = event {
                    let reply = peer::snapshot::serve(
                        &db.pool_view().unwrap(),
                        received.message(),
                        Default::default(),
                    )
                    .unwrap();
                    if let Some(reply) = reply {
                        let Ok(ticket) = remote.send(&reply) else {
                            break;
                        };
                        if ticket.await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        let local = peer::snapshot::local(&source.db.view().unwrap(), false).unwrap();
        let ping = ping::encode(
            &local,
            vec![],
            &[],
            ping::Encoding {
                version: 1,
                minimum_version: 1,
                port: 4000 + u16::from(n),
                share: 0.into(),
                limits: Default::default(),
            },
        )
        .unwrap();
        within(client.request(&ping, None).unwrap()).await.unwrap();
        self.peers.push((client, driver, server));
    }
    async fn shared(&mut self) {
        within(async {
            loop {
                let status = self.handle.status();
                assert_eq!(status.counts.failed, 0, "{status:?}");
                if status.active.iter().any(|a| a.progress.members == 3) {
                    break;
                }
                self.handle.changed().await.unwrap();
            }
        })
        .await;
    }
    async fn completed(&mut self) {
        within(async {
            loop {
                let status = self.handle.status();
                // Once the initial forward generation succeeds, later peers
                // can be accounted for by the global latch without a fetch.
                if status.counts.finished
                    + status.counts.failed
                    + status.lifecycle.suppressed_generations
                    >= 3
                    || !status.running
                {
                    break;
                }
                self.handle.changed().await.unwrap();
            }
        })
        .await;
    }
    async fn stop(self) -> Status {
        self.handle.shutdown();
        let status = within(self.task).await.unwrap();
        assert!(status.joined && !status.running);
        assert!(status.active.is_empty());
        assert_eq!(
            status.counts.started,
            status.counts.finished + status.counts.failed + status.counts.cancelled
        );
        assert_eq!(self.context.importer.available(), (3, 3 * WEIGHT));
        assert_eq!(Arc::strong_count(&self.context), 1);
        self.application.shutdown();
        assert!(within(self.application_task).await.unwrap().joined);
        self.gossip.shutdown();
        self.network.shutdown();
        assert!(!within(self.gossip_task).await.unwrap().running);
        assert!(within(self.network_task).await.unwrap().status.joined);
        for (_, driver, server) in self.peers {
            within(driver).await.unwrap();
            within(server).await.unwrap();
        }
        status
    }
}
struct Gate {
    entered: tokio::sync::oneshot::Receiver<()>,
    release: std::sync::mpsc::Sender<()>,
}
fn fault(importer: &mut Importer, published: bool, panic: bool) -> Gate {
    let (entered, entry) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let gate = Mutex::new(Some((entered, wait)));
    Arc::get_mut(&mut importer.context).unwrap().commit_hook = Some(Arc::new(move |db, update| {
        let gate = gate.lock().unwrap().take();
        let Some((entered, wait)) = gate else {
            return db.commit(update);
        };
        if published {
            db.commit(update)?;
        }
        let _ = entered.send(());
        let _ = wait.recv();
        if panic {
            panic!("injected owner post-commit panic");
        }
        Err(repository::Error::Backend(
            "injected owner acknowledgment".into(),
        ))
    }));
    Gate {
        entered: entry,
        release,
    }
}

#[tokio::test]
async fn uncertain_shared_post_preserves_exact_reason_and_reopens_before_resuming() {
    for published in [false, true] {
        let node = Node::new();
        let source = source().await;
        let blocks = fixtures::blocks(&NetworkProfile::mainnet());
        let hash = blocks[0].hash().unwrap();
        let mut owned = importer(&node);
        let gate = fault(&mut owned, published, false);
        let mut running = Running::new(&node, &source, owned).await;
        within(gate.entered).await.unwrap();
        running.shared().await;
        gate.release.send(()).unwrap();
        within(async {
            while running.handle.status().running {
                running.handle.changed().await.unwrap();
            }
        })
        .await;
        let status = running.stop().await;
        assert!(
            matches!(status.last_error,Some(Error::Import(chain::Failure { error:chain::Error::CommitUncertain { hash:h,.. },.. }))if h==hash),
            "{status:?}"
        );
        assert_eq!(
            node.db.view().unwrap().block(hash).unwrap().is_some(),
            published
        );
        let node = node.reopen();
        assert_eq!(
            node.db.view().unwrap().block(hash).unwrap().is_some(),
            published
        );
        let mut resumed = Running::new(&node, &source, importer(&node)).await;
        resumed.completed().await;
        let status = resumed.stop().await;
        assert_eq!(status.counts.failed, 0, "{status:?}");
        assert_eq!(status.counts.finished, 3);
        assert_eq!(
            node.db.view().unwrap().status().tip,
            Some(blocks[2].hash().unwrap())
        );
    }
}

#[tokio::test]
async fn shared_post_panic_retains_diagnostic_and_durable_prefix_after_reopen() {
    let node = Node::new();
    let source = source().await;
    let blocks = fixtures::blocks(&NetworkProfile::mainnet());
    let mut owned = importer(&node);
    let gate = fault(&mut owned, true, true);
    let mut running = Running::new(&node, &source, owned).await;
    within(gate.entered).await.unwrap();
    running.shared().await;
    gate.release.send(()).unwrap();
    within(async {
        while running.handle.status().running {
            running.handle.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(
        running.context.importer.recovery_reason(),
        Some(chain::Error::WorkerPanicked)
    );
    let status = running.stop().await;
    assert!(matches!(
        status.last_error,
        Some(Error::Import(chain::Failure {
            error: chain::Error::WorkerPanicked,
            progress: None
        }))
    ));
    let node = node.reopen();
    assert_eq!(
        node.db.view().unwrap().status().tip,
        Some(blocks[0].hash().unwrap())
    );
    let mut resumed = Running::new(&node, &source, importer(&node)).await;
    resumed.completed().await;
    assert_eq!(resumed.stop().await.counts.finished, 3);
    assert_eq!(
        node.db.view().unwrap().status().tip,
        Some(blocks[2].hash().unwrap())
    );
}

fn pause_after(importer: &mut Importer, number: u64) -> Gate {
    let (entered, entry) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let gate = Mutex::new(Some((entered, wait)));
    let calls = AtomicU64::new(0);
    Arc::get_mut(&mut importer.context).unwrap().commit_hook = Some(Arc::new(move |db, update| {
        let committed = db.commit(update)?;
        if calls.fetch_add(1, Ordering::SeqCst) + 1 == number {
            let (entered, wait) = gate.lock().unwrap().take().unwrap();
            let _ = entered.send(());
            let _ = wait.recv();
        }
        Ok(committed)
    }));
    Gate {
        entered: entry,
        release,
    }
}

async fn replace_poster(node: &Node, source: &Node, running: &mut Running) {
    let poster = running
        .context
        .shared
        .lock()
        .unwrap()
        .posting_client()
        .unwrap();
    let index = (0..3)
        .find(|n| noise::Identity::from_secret([n + 9; 32]).public_key() == poster.peer())
        .unwrap();
    poster.shutdown();
    within(poster.monitor().wait()).await;
    within(async {
        while running
            .network
            .connection(poster.clone())
            .unwrap()
            .wait()
            .await
            .unwrap()
            .is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    running.connect(node, source, index).await;
    within(async {
        while running.handle.status().counts.replaced == 0 {
            running.handle.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(running.handle.status().counts.cancelled, 1);
}

#[tokio::test]
async fn replacing_poster_during_micro_commit_requeues_only_unacknowledged_suffix() {
    let node = Node::new();
    let source = source().await;
    let blocks = fixtures::blocks(&NetworkProfile::mainnet());
    let mut owned = importer(&node);
    let gate = pause_after(&mut owned, 2);
    let mut running = Running::new(&node, &source, owned).await;
    within(gate.entered).await.unwrap();
    running.shared().await;
    assert_eq!(
        node.db.view().unwrap().status().tip,
        Some(blocks[1].hash().unwrap())
    );
    replace_poster(&node, &source, &mut running).await;
    gate.release.send(()).unwrap();
    running.completed().await;
    let status = running.stop().await;
    assert_eq!(status.counts.finished, 3, "{status:?}");
    assert_eq!(status.counts.failed, 0);
    assert_eq!(status.counts.cancelled, 1);
    assert_eq!(
        node.db.view().unwrap().status().tip,
        Some(blocks[2].hash().unwrap())
    );
    assert_eq!(
        node.db.view().unwrap().status().revision,
        source.db.view().unwrap().status().revision
    );
}

#[tokio::test]
async fn replacing_poster_after_generation_commit_before_book_ack_uses_actual_storage() {
    let node = Node::new();
    let source = source().await;
    let blocks = fixtures::blocks(&NetworkProfile::mainnet());
    let mut owned = importer(&node);
    let gate = pause_after(&mut owned, 2);
    let mut running = Running::new(&node, &source, owned).await;
    within(gate.entered).await.unwrap();
    running.shared().await;
    // Hold all read capacity after receipt inspection. The importer can finish
    // the generation, but the session cannot reread it and acknowledge the book.
    let (entered, entry) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let pending = within(running.context.reads.submit_wait(3 * WEIGHT, move |_| {
        let _ = entered.send(());
        let _ = wait.recv();
    }))
    .await
    .unwrap();
    within(entry).await.unwrap();
    gate.release.send(()).unwrap();
    within(async {
        while node.db.view().unwrap().status().tip != Some(blocks[2].hash().unwrap()) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let statistics_before_replacement =
        running.context.shared.lock().unwrap().statistics().tasks[0].total;
    replace_poster(&node, &source, &mut running).await;
    release.send(()).unwrap();
    within(pending).await.unwrap();
    running.completed().await;
    let status = running.stop().await;
    assert_eq!(
        status.statistics.last_terminated.as_ref().unwrap().total,
        statistics_before_replacement,
        "the cancelled unacknowledged posting batch must not publish statistics"
    );
    assert_eq!(status.counts.finished, 3, "{status:?}");
    assert_eq!(status.counts.failed, 0);
    assert_eq!(status.counts.cancelled, 1);
    assert_eq!(
        node.db.view().unwrap().status().revision,
        source.db.view().unwrap().status().revision
    );
}

#[tokio::test]
async fn borrowed_sync_shutdown_waits_for_inflight_commit_and_preserves_owner_recovery() {
    for uncertain in [false, true] {
        let node = Node::new();
        let source = source().await;
        let blocks = fixtures::blocks(&NetworkProfile::mainnet());
        let mut owned = importer(&node);
        let gate = if uncertain {
            fault(&mut owned, true, false)
        } else {
            pause_after(&mut owned, 1)
        };
        let running = Running::new(&node, &source, owned.borrowed()).await;
        within(gate.entered).await.unwrap();
        running.handle.shutdown();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let waited = !running.task.is_finished();
        gate.release.send(()).unwrap();
        assert!(waited, "borrowed shutdown skipped an accepted write");
        running.stop().await;
        assert!(!owned.jobs.stopped());
        assert!(
            node.db
                .view()
                .unwrap()
                .block(blocks[0].hash().unwrap())
                .unwrap()
                .is_some()
        );
        if uncertain {
            assert!(matches!(
                owned.recovery_reason(),
                Some(chain::Error::CommitUncertain { .. })
            ));
            assert_eq!(owned.check_open(), Err(chain::Error::RecoveryRequired));
        } else {
            owned.check_open().unwrap();
            owned
                .import_block(blocks[1].encode().unwrap(), u64::MAX - 600_000)
                .unwrap()
                .wait()
                .await
                .unwrap();
        }
        owned.stop();
        owned.join().await;
    }
}

#[tokio::test]
async fn sync_progress_timer_publishes_idle_without_observer_and_closes_on_shutdown() {
    use ae_chain::instrumentation::{self, Event, Gauge, Value};
    #[derive(Default)]
    struct Sink(Mutex<Vec<f64>>);
    impl instrumentation::Sink for Sink {
        fn event(&self, e: Event) {
            if let Event::Gauge(Gauge::SyncProgress, Value::Float(value)) = e {
                self.0.lock().unwrap().push(value);
            }
        }
        fn start(&self, _: instrumentation::Stage) -> Option<Box<dyn instrumentation::Span>> {
            None
        }
    }
    let node = source().await;
    let sink = Arc::new(Sink::default());
    let _binding = node.db.instrumentation().bind(sink.clone()).unwrap();
    for generation in 1..=2 {
        let mut running = Running::new(&node, &node, importer(&node)).await;
        let mut logs = running.handle.subscribe_logs(None).unwrap();
        within(async {
            while running.handle.status().metrics.ticks == 0 {
                running.handle.changed().await.unwrap();
            }
        })
        .await;
        let status = running.handle.status().metrics;
        assert_eq!(status.last, Some(100.0));
        assert_eq!(status.gauges, 1);
        assert_eq!(status.logs, 0);
        assert_eq!(sink.0.lock().unwrap().len(), generation);
        let stopped = running.stop().await;
        assert_eq!(stopped.metrics.next, None);
        assert_eq!(stopped.metrics.ticks, 1);
        assert!(within(logs.recv()).await.is_none());
    }
}

#[tokio::test]
async fn sync_progress_active_logs_reach_a_late_observer_and_close_with_owner() {
    use crate::observe;
    let node = Node::new();
    let source = source().await;
    let mut owned = importer(&node);
    let gate = fault(&mut owned, false, false);
    let mut running = Running::new(&node, &source, owned).await;
    within(gate.entered).await.unwrap();
    // If the 1ms initial jitter won the race with connection setup, wait for
    // the real 30s callback. Never shorten the production cadence for a test.
    tokio::time::timeout(Duration::from_secs(35), async {
        while running.handle.status().metrics.logs < 2 {
            running.handle.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(running.handle.status().metrics.last, Some(0.0));
    let mut source_logs = running.handle.subscribe_logs(None).unwrap();
    assert!(
        matches!(source_logs.recv().await, Some(crate::events::Delivery::Event(r)) if r.event == metrics::Log::Progress(0.0))
    );
    assert!(
        matches!(source_logs.recv().await, Some(crate::events::Delivery::Event(r)) if matches!(&r.event, metrics::Log::Task(t) if t.task.target > 0 && t.adding > 0))
    );
    let (mut observer, service) = observe::create(
        observe::Sources {
            database: node.db.clone(),
            network: running.network.clone(),
            peer: running.application.clone(),
            gossip: running.gossip.clone(),
            sync: running.handle.clone(),
            admission: node.client(),
        },
        running._events.take().unwrap(),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let mut observed = observer.subscribe(None).unwrap();
    let observer_task = tokio::spawn(service.run());
    within(async {
        while observer.status().sync_logs < 2 {
            observer.changed().await.unwrap();
        }
    })
    .await;
    let texts = within(async {
        let mut texts = vec![];
        while texts.len() < 2 {
            if let Some(observe::Delivery::Record(r)) = observed.recv().await {
                let value: serde_json::Value = serde_json::from_slice(r.json_line()).unwrap();
                if r.category == "sync" && value["data"]["type"] == "status" {
                    texts.push(value["data"]["text"].as_str().unwrap().to_owned());
                }
            }
        }
        texts
    })
    .await;
    assert_eq!(texts[0], "Sync progress: 0.0000%");
    assert!(texts[1].starts_with("SyncTask ("));
    gate.release.send(()).unwrap();
    observer.shutdown();
    assert!(within(observer_task).await.unwrap().joined);
    assert!(running.stop().await.joined);
    assert!(within(source_logs.recv()).await.is_none());
}
