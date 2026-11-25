use super::*;
use ae_sync::{
    chain::sync,
    observe::{self, metrics::Number},
};
use std::{
    future::Future,
    io::{self, Write},
    pin::Pin,
    sync::{Condvar, Mutex},
};

type Finish<'a> = Pin<Box<dyn Future<Output = sync::Status> + 'a>>;
struct Running<'a> {
    observer: observe::Handle,
    task: JoinHandle<observe::Status>,
    sync: sync::Handle,
    finish: Finish<'a>,
    remote: net::Client,
    events: net::Events,
    driver: JoinHandle<net::Exit>,
}
async fn start_monitor(
    node: &Node,
    outputs: observe::Outputs,
    config: observe::Config,
) -> Running<'_> {
    start_routed_monitor(node, outputs, config, None).await
}
async fn start_routed_monitor(
    node: &Node,
    outputs: observe::Outputs,
    config: observe::Config,
    routing: Option<ae_sync::routing::Client>,
) -> Running<'_> {
    let app = start(
        node,
        super::config(),
        peer::Config {
            // The burst can produce several control events per ping before
            // its consumer runs. Exercise diagnostic overflow below without
            // also triggering the control queue's intentional Busy shutdown.
            events: 256,
            ..Default::default()
        },
    )
    .await;
    let (remote, events, driver) = connect(node, &app).await;
    let network = app.network.clone();
    let peer = app.application.clone();
    let gossip = app.gossip.clone();
    let (sync, incoming, finish) = chain_sync::synchronize(
        node,
        app,
        sync::Config {
            forwarded: 256,
            ..Default::default()
        },
    );
    let sources = observe::Sources {
        database: node.db.clone(),
        network,
        peer,
        gossip,
        sync: sync.clone(),
        admission: node.client(),
    };
    let (observer, service) = match routing {
        Some(client) => observe::create_with_routing(sources, incoming, config, outputs, client),
        None => observe::create(sources, incoming, config, outputs),
    }
    .unwrap();
    Running {
        observer,
        task: tokio::spawn(service.run()),
        sync,
        finish: Box::pin(finish),
        remote,
        events,
        driver,
    }
}
fn compact() -> observe::Config {
    observe::Config {
        interval: Duration::from_millis(20),
        records: 4,
        history_bytes: 16 << 10,
        record_bytes: 8 << 10,
        sink_records: 2,
        sink_bytes: 16 << 10,
        ..Default::default()
    }
}
async fn await_status(
    handle: &mut observe::Handle,
    predicate: impl Fn(&observe::Status) -> bool,
) -> observe::Status {
    within(async {
        loop {
            let status = handle.status();
            if predicate(&status) {
                return status;
            }
            handle.changed().await.unwrap();
        }
    })
    .await
}
impl Running<'_> {
    async fn ping(&self, node: &Node) -> net::Received {
        let result = within(self.remote.request(&ping(node, 1, false), None).unwrap()).await;
        let Ok(net::Reply::Response(received)) = result else {
            panic!(
                "ping {result:?}; sync {:?}; observer {:?}",
                self.sync.status(),
                self.observer.status()
            );
        };
        received
    }
    async fn stop(self) -> observe::Status {
        self.observer.shutdown();
        let status = within(self.task).await.unwrap();
        assert!(status.joined);
        assert!(!status.running);
        for sink in [&status.log, &status.statsd].into_iter().flatten() {
            assert!(!sink.running);
            assert_eq!(sink.outstanding, 0);
            assert_eq!(sink.retained_bytes, 0);
        }
        self.sync.shutdown();
        assert!(within(self.finish).await.joined);
        within(self.driver).await.unwrap();
        status
    }
}
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<u8>>>);
impl Write for Log {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn peer_count_log_has_its_own_timer_and_uses_current_registry_counts_and_maxima() {
    for interval in [Duration::from_millis(50), Duration::ZERO] {
        let node = Node::new();
        let log = Log::default();
        let mut run = start_monitor(
            &node,
            observe::Outputs {
                log: Some(Box::new(log.clone())),
                statsd: None,
                metrics: None,
            },
            observe::Config {
                interval: Duration::from_secs(30),
                peer_count_interval: Some(interval),
                ..Default::default()
            },
        )
        .await;
        run.ping(&node).await;
        let mut feed = run.observer.subscribe(None).unwrap();
        let record = within(async {
            loop {
                if let Some(observe::Delivery::Record(record)) = feed.recv().await {
                    let value: serde_json::Value =
                        serde_json::from_slice(record.json_line()).unwrap();
                    if record.category == "peers" && value["data"]["inbound"] == 1 {
                        break value;
                    }
                }
            }
        })
        .await;
        let data = &record["data"];
        assert_eq!(data["maximum_inbound"], 100);
        assert_eq!(data["maximum_outbound"], 0);
        assert_eq!(data["outbound"], 0);
        assert_eq!(
            data["message"],
            format!(
                "Peer connections outbound: 0/0, inbound: 1/100, verified {}, unverified {}, standby {}",
                data["verified"], data["unverified"], data["standby"]
            )
        );
        await_status(&mut run.observer, |s| {
            s.peer_count_samples >= 3 && s.samples == 1
        })
        .await;
        let status = run.stop().await;
        assert_eq!(status.peer_count_errors, 0);
        assert_eq!(status.samples, 1);
        assert!(
            String::from_utf8(log.0.lock().unwrap().clone())
                .unwrap()
                .contains("Peer connections outbound:")
        );
    }
    let node = Node::new();
    let mut run = start_monitor(
        &node,
        Default::default(),
        observe::Config {
            peer_count_interval: None,
            ..compact()
        },
    )
    .await;
    await_status(&mut run.observer, |s| s.samples >= 3).await;
    assert_eq!(run.stop().await.peer_count_samples, 0);
}
#[tokio::test]
async fn diagnostic_overflow_never_blocks_encrypted_requests_and_statsd_uses_live_state() {
    let node = Node::new();
    let log = Log::default();
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut run = start_monitor(
        &node,
        observe::Outputs {
            log: Some(Box::new(log.clone())),
            statsd: Some(udp.local_addr().unwrap()),
            metrics: None,
        },
        compact(),
    )
    .await;
    let start = run.observer.cursor();
    for _ in 0..40 {
        assert_eq!(body(&run.ping(&node).await).kind(), Kind::Ping);
    }
    let status = await_status(&mut run.observer, |s| {
        s.events >= 40
            && s.samples > 0
            && s.metrics.as_ref().is_some_and(|m| {
                m.get("ae.epoch.aecore.peers.inbound.value") == Some(&Number::Integer(1))
            })
    })
    .await;
    assert!(status.evicted > 0);
    assert!(status.retained_records <= 4);
    assert!(status.retained_bytes <= 16 << 10);
    assert_eq!(
        status
            .metrics
            .as_ref()
            .unwrap()
            .get("ae.epoch.aecore.chain.height.value"),
        Some(&Number::Integer(0))
    );
    let mut feed = run.observer.subscribe(Some(start)).unwrap();
    assert!(matches!(feed.recv().await, Some(observe::Delivery::Gap { missed }) if missed > 0));
    let mut bytes = [0; 4096];
    within(async {
        loop {
            let n = udp.recv(&mut bytes).await.unwrap();
            if &bytes[..n] == b"ae.epoch.aecore.peers.inbound.value:1|g" {
                break;
            }
        }
    })
    .await;
    let observer = run.observer.clone();
    let status = run.stop().await;
    assert!(status.log.unwrap().written > 0);
    assert!(status.statsd.unwrap().written > 0);
    let mut count = 0;
    while let Some(delivery) = feed.recv().await {
        if let observe::Delivery::Record(record) = delivery {
            let row: serde_json::Value = serde_json::from_slice(record.json_line()).unwrap();
            assert_eq!(row["sequence"].as_u64().unwrap(), record.cursor.sequence());
            count += 1;
        }
    }
    assert!(count > 0);
    assert!(observer.subscribe(None).unwrap().recv().await.is_none());
    let log = log.0.lock().unwrap();
    for line in log.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        serde_json::from_slice::<serde_json::Value>(line).unwrap();
    }
}
struct Gate {
    released: Mutex<bool>,
    wake: Condvar,
    entered: tokio::sync::Notify,
}
impl Gate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}
struct Blocked(Arc<Gate>);
impl Write for Blocked {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut released = self.0.released.lock().unwrap();
        self.0.entered.notify_one();
        while !*released {
            released = self.0.wake.wait(released).unwrap();
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
#[tokio::test]
async fn blocked_log_counts_executing_job_and_udp_and_peers_keep_working() {
    let node = Node::new();
    let gate = Arc::new(Gate {
        released: Mutex::new(false),
        wake: Condvar::new(),
        entered: tokio::sync::Notify::new(),
    });
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut run = start_monitor(
        &node,
        observe::Outputs {
            log: Some(Box::new(Blocked(gate.clone()))),
            statsd: Some(udp.local_addr().unwrap()),
            metrics: None,
        },
        compact(),
    )
    .await;
    // Release before service fields are dropped even if a test assertion fails.
    let release = Release(gate.clone());
    within(gate.entered.notified()).await;
    for _ in 0..24 {
        run.ping(&node).await;
    }
    let status = await_status(&mut run.observer, |s| {
        s.events >= 24
            && s.log.as_ref().is_some_and(|l| l.dropped > 0)
            && s.statsd.as_ref().is_some_and(|u| u.written > 0)
    })
    .await;
    let log = status.log.unwrap();
    assert_eq!(log.outstanding, 2);
    assert_eq!(log.written, 0);
    assert!(log.retained_bytes <= 16 << 10);
    assert!(status.running);
    assert!(run.sync.status().running);
    drop(release);
    let status = run.stop().await;
    assert!(status.log.unwrap().written >= 2);
}
struct Broken(bool);
impl Write for Broken {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        assert!(!self.0, "deliberate diagnostic writer panic");
        Err(io::ErrorKind::PermissionDenied.into())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn sink_errors_and_panics_are_observable_without_terminating_peers() {
    for panic in [false, true] {
        let node = Node::new();
        let mut run = start_monitor(
            &node,
            observe::Outputs {
                log: Some(Box::new(Broken(panic))),
                statsd: Some("127.0.0.1:0".parse().unwrap()),
                metrics: None,
            },
            compact(),
        )
        .await;
        for _ in 0..12 {
            run.ping(&node).await;
        }
        let status = await_status(&mut run.observer, |s| {
            s.events >= 12 && s.log.as_ref().is_some_and(|l| l.failed > 0)
        })
        .await;
        assert!(status.running);
        assert!(status.statsd.is_none());
        let log = status.log.unwrap();
        assert_eq!(log.panicked, panic);
        if !panic {
            assert_eq!(log.last_error, Some(io::ErrorKind::PermissionDenied));
        }
        run.stop().await;
    }
}
#[tokio::test]
async fn observer_drains_sync_intents_while_importing_and_samples_committed_tip() {
    let node = Node::new();
    let source = Node::new();
    chain_sync::seed(&source, usize::MAX).await;
    let mut run = start_monitor(&node, observe::Outputs::default(), compact()).await;
    response(&run.remote, &ping(&source, 1, false)).await;
    chain_sync::serve_until_finished(
        &mut run.sync,
        &run.remote,
        &mut run.events,
        &source,
        false,
        false,
    )
    .await;
    assert_eq!(run.sync.status().counts.failed, 0);
    assert_eq!(run.sync.status().counts.finished, 1);
    let view = node.db.view().unwrap();
    let local = snapshot::local(&view, false).unwrap();
    assert_eq!(view.status().tip, source.db.view().unwrap().status().tip);
    let status = await_status(&mut run.observer, |s| {
        s.metrics.as_ref().is_some_and(|m| {
            m.chain_revision == view.status().revision
                && m.get("ae.rust.sync.completed") == Some(&Number::Integer(1))
        })
    })
    .await;
    let metrics = status.metrics.unwrap();
    assert_eq!(
        metrics.get("ae.epoch.aecore.chain.height.value"),
        Some(&Number::Integer(local.height.into()))
    );
    assert_eq!(
        metrics.get("ae.epoch.aecore.chain.total_difficulty"),
        Some(&Number::Unsigned(local.difficulty))
    );
    assert_eq!(
        metrics.get("ae.rust.sync.completed"),
        Some(&Number::Integer(1))
    );
    run.stop().await;
}
#[tokio::test]
async fn observer_rejects_foreign_event_owner_and_database_before_starting_workers() {
    let node = Node::new();
    let other = Node::new();
    for foreign_events in [false, true] {
        let app = start(&node, config(), Default::default()).await;
        let (sync, incoming, service) = sync::create(
            download_importer(&node, Default::default()),
            app.network.clone(),
            app.application.clone(),
            app.events,
            Default::default(),
        )
        .unwrap();
        let mut sources = observe::Sources {
            database: node.db.clone(),
            network: app.network.clone(),
            peer: app.application.clone(),
            gossip: app.gossip.clone(),
            sync,
            admission: node.client(),
        };
        let (incoming, extra) = if foreign_events {
            let second = start(&other, config(), Default::default()).await;
            let (handle, events, second_service) = sync::create(
                download_importer(&other, Default::default()),
                second.network.clone(),
                second.application.clone(),
                second.events,
                Default::default(),
            )
            .unwrap();
            drop(incoming);
            drop(handle);
            drop(second_service);
            (
                events,
                Some((
                    second.application,
                    second.application_task,
                    second.gossip,
                    second.gossip_task,
                    second.network,
                    second.network_task,
                )),
            )
        } else {
            sources.database = other.db.clone();
            (incoming, None)
        };
        assert!(matches!(
            observe::create(sources, incoming, compact(), observe::Outputs::default()),
            Err(observe::Error::Configuration)
        ));
        drop(service);
        app.application.shutdown();
        assert!(within(app.application_task).await.unwrap().joined);
        app.gossip.shutdown();
        app.network.shutdown();
        within(app.gossip_task).await.unwrap();
        within(app.network_task).await.unwrap();
        if let Some((p, pt, g, gt, n, nt)) = extra {
            p.shutdown();
            within(pt).await.unwrap();
            g.shutdown();
            n.shutdown();
            within(gt).await.unwrap();
            within(nt).await.unwrap();
        }
    }
}
#[tokio::test]
async fn aborted_observer_closes_subscriptions_and_releases_owned_workers() {
    let node = Node::new();
    let log = Log::default();
    let mut run = start_monitor(
        &node,
        observe::Outputs {
            log: Some(Box::new(log.clone())),
            statsd: None,
            metrics: None,
        },
        compact(),
    )
    .await;
    await_status(&mut run.observer, |s| {
        s.samples > 0 && s.log.as_ref().is_some_and(|l| l.written > 0)
    })
    .await;
    let mut subscriber = run.observer.subscribe(None).unwrap();
    run.task.abort();
    assert!(within(run.task).await.unwrap_err().is_cancelled());
    let status = run.observer.status();
    assert!(!status.running);
    assert!(!status.joined);
    assert_eq!(status.last_error, Some(observe::Error::Closed));
    let sink = status.log.unwrap();
    assert!(!sink.running);
    assert_eq!((sink.outstanding, sink.retained_bytes), (0, 0));
    within(async { while subscriber.recv().await.is_some() {} }).await;
    run.sync.shutdown();
    assert!(within(run.finish).await.joined);
    within(run.driver).await.unwrap();
    // The sink's writer was destroyed when the owned thread joined.
    assert_eq!(Arc::strong_count(&log.0), 1);
}
#[tokio::test]
async fn network_and_gossip_reject_admission_from_another_database() {
    let node = Node::new();
    let other = Node::new();
    assert!(matches!(
        network::create(
            node.db.clone(),
            other.client(),
            identities().0,
            NetworkProfile::mainnet(),
            config()
        )
        .await,
        Err(network::Error::Configuration)
    ));
    let (network, scheduled, pool, service) = network::create(
        node.db.clone(),
        node.client(),
        identities().0,
        NetworkProfile::mainnet(),
        config(),
    )
    .await
    .unwrap();
    assert!(matches!(
        gossip::create(
            node.db.clone(),
            other.client(),
            network.peers(),
            pool,
            NetworkProfile::mainnet(),
            ConsensusType::Pow,
            Default::default()
        ),
        Err(gossip::Error::Configuration)
    ));
    drop(scheduled);
    drop(service);
    drop(network);
}

#[tokio::test]
async fn observer_consumes_actual_gc_events_and_reports_history_gaps_with_progress() {
    for retained in [true, false] {
        let node = Node::with_notifications(ae_storage::Notifications {
            gc: ae_storage::gc::events::Config {
                records: if retained { 32 } else { 1 },
                ..Default::default()
            },
            ..Default::default()
        });
        let log = Log::default();
        let mut run = start_monitor(
            &node,
            observe::Outputs {
                log: Some(Box::new(log.clone())),
                statsd: None,
                metrics: None,
            },
            observe::Config {
                records: 32,
                sink_records: 32,
                ..compact()
            },
        )
        .await;
        node.db.begin_gc(Default::default()).unwrap();
        while node.db.gc_state().unwrap().active() {
            node.db.gc_step(Default::default()).unwrap();
        }
        let status = await_status(&mut run.observer, |s| {
            s.gc_events == if retained { 3 } else { 1 }
        })
        .await;
        assert_eq!(status.gc_missed, if retained { 0 } else { 2 });
        assert!(status.last_error.is_none(), "{status:?}");
        assert_eq!(body(&run.ping(&node).await).kind(), Kind::Ping);
        run.stop().await;
        let bytes = log.0.lock().unwrap();
        let records: Vec<serde_json::Value> = bytes
            .split(|b| *b == b'\n')
            .filter(|b| !b.is_empty())
            .map(|b| serde_json::from_slice(b).unwrap())
            .filter(|r: &serde_json::Value| r["category"] == "gc")
            .collect();
        let labels: Vec<_> = records
            .iter()
            .map(|r| r["data"]["event"].as_str().unwrap())
            .collect();
        assert_eq!(
            labels,
            if retained {
                vec!["gc_switch", "scans_complete", "collection_complete"]
            } else {
                vec!["history_gap", "collection_complete"]
            }
        );
        assert_eq!(
            records.last().unwrap()["data"]["state"]["phase"],
            "Complete"
        );
        if !retained {
            assert_eq!(records[0]["data"]["missed"], 2);
            assert_eq!(records[0]["data"]["snapshot_sequence"], 3);
        }
    }
}

#[tokio::test]
async fn metrics_routing_filters_real_outputs_and_preserves_snapshots_when_owner_closes() {
    use ae_sync::routing::{self, Destination};
    let node = Node::new();
    let log = Log::default();
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let rules = vec![
        routing::Rule {
            name: b"ae.epoch.aecore.chain.height".to_vec(),
            datapoints: b"value".to_vec(),
            actions: b"log".to_vec(),
            ..Default::default()
        },
        routing::Rule {
            name: b"ae.epoch.aecore.peers.count".to_vec(),
            datapoints: b"value".to_vec(),
            actions: b"send".to_vec(),
            ..Default::default()
        },
        routing::Rule {
            name: b"ae.**".to_vec(),
            actions: b"none".to_vec(),
            ..Default::default()
        },
    ];
    let owner = routing::Owner::new(routing::Setup {
        config: routing::Config {
            rules,
            ..Default::default()
        },
        ..Default::default()
    })
    .unwrap();
    let worker = routing::Worker::spawn(owner.store()).unwrap();
    let client = worker.client();
    let mut run = start_routed_monitor(
        &node,
        observe::Outputs {
            log: Some(Box::new(log.clone())),
            statsd: Some(udp.local_addr().unwrap()),
            metrics: None,
        },
        observe::Config {
            records: 128,
            sink_records: 128,
            ..compact()
        },
        Some(client.clone()),
    )
    .await;
    let mut subscription = run.observer.subscribe(None).unwrap();
    assert_eq!(body(&run.ping(&node).await).kind(), Kind::Ping);
    let status = await_status(&mut run.observer, |s| {
        s.samples >= 3 && s.statsd.as_ref().is_some_and(|i| i.written > 0)
    })
    .await;
    assert_eq!(status.routing_errors, 0);
    assert!(status.filtered_log > 0 && status.filtered_send > 0);
    assert!(status.metrics.as_ref().unwrap().metrics.len() > 20);
    let mut buffer = [0u8; 2048];
    let (n, _) = within(udp.recv_from(&mut buffer)).await.unwrap();
    assert!(
        std::str::from_utf8(&buffer[..n])
            .unwrap()
            .starts_with("ae.epoch.aecore.peers.count.value:")
    );
    within(async {
        loop {
            if let observe::Delivery::Record(record) = subscription.recv().await.unwrap()
                && record.category == "metrics"
            {
                let data: serde_json::Value = serde_json::from_slice(record.json_line()).unwrap();
                assert!(data["data"]["metrics"].as_object().unwrap().len() > 20);
                break;
            }
        }
    })
    .await;
    let key = observe::metrics::Metric {
        name: "ae.epoch.aecore.chain.height.value",
        value: Number::Integer(0),
    }
    .routing()
    .1;
    assert!(
        client
            .delete(key.clone(), Destination::Log)
            .unwrap()
            .await
            .unwrap()
    );
    client.add(key, Destination::Send).unwrap().await.unwrap();
    within(async {
        loop {
            let (n, _) = udp.recv_from(&mut buffer).await.unwrap();
            if std::str::from_utf8(&buffer[..n])
                .unwrap()
                .starts_with("ae.epoch.aecore.chain.height.value:")
            {
                break;
            }
        }
    })
    .await;
    let before = run.observer.status().samples;
    drop(owner);
    let status = await_status(&mut run.observer, |s| {
        s.routing_errors > 0 && s.samples >= before + 2
    })
    .await;
    assert!(status.running && status.metrics.as_ref().unwrap().metrics.len() > 20);
    assert!(matches!(
        status.last_error,
        Some(observe::Error::Routing(routing::Error::Closed))
    ));
    assert_eq!(body(&run.ping(&node).await).kind(), Kind::Ping);
    run.stop().await;
    worker.shutdown().unwrap();
    let lines = log.0.lock().unwrap();
    let records = lines
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_slice::<serde_json::Value>(l).unwrap())
        .filter(|r| r["category"] == "metrics")
        .collect::<Vec<_>>();
    assert!(!records.is_empty());
    for record in records {
        let values = record["data"]["metrics"].as_object().unwrap();
        assert_eq!(values.len(), 1);
        assert!(values.contains_key("ae.epoch.aecore.chain.height.value"));
    }
}
