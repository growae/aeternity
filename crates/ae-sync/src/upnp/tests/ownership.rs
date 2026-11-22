use super::*;
use std::collections::BTreeMap;
use tokio::{sync::watch, task::JoinHandle};

#[derive(Clone)]
struct Reservation {
    internal: u16,
    description: String,
}
struct RouterState {
    mappings: BTreeMap<u16, Reservation>,
    next: u16,
    adds: Vec<u16>,
    deleted: Vec<u16>,
    generic: usize,
    permanent: bool,
    force_new: bool,
    hold_add: bool,
    hold_delete: bool,
    lose_delete_reply: bool,
    lose_add_reply: bool,
    reject_adds: u8,
    add_requests: usize,
}
struct Router {
    state: Arc<Mutex<RouterState>>,
    settings: Settings,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}
fn field<'a>(node: roxmltree::Node<'a, 'a>, name: &str) -> &'a str {
    node.children()
        .find(|n| n.has_tag_name(name))
        .unwrap()
        .text()
        .unwrap_or("")
}
fn entry_body(port: u16, reservation: &Reservation, generic: bool) -> String {
    let client = if reservation.description == "foreign" {
        "printer.local"
    } else {
        "127.0.0.1"
    };
    let prefix = if generic {
        format!(
            "<NewRemoteHost></NewRemoteHost><NewExternalPort>{port}</NewExternalPort><NewProtocol>TCP</NewProtocol>"
        )
    } else {
        String::new()
    };
    format!(
        "{prefix}<NewInternalPort>{}</NewInternalPort><NewInternalClient>{client}</NewInternalClient><NewEnabled>1</NewEnabled><NewPortMappingDescription>{}</NewPortMappingDescription><NewLeaseDuration>7200</NewLeaseDuration>",
        reservation.internal, reservation.description
    )
}
impl Router {
    async fn start() -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let udp = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let mut settings = settings();
        settings.ssdp = address(udp.local_addr().unwrap());
        let location = format!("http://{}/root.xml", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(RouterState {
            mappings: BTreeMap::from([(
                4000,
                Reservation {
                    internal: 1234,
                    description: "foreign".into(),
                },
            )]),
            next: 5000,
            adds: vec![],
            deleted: vec![],
            generic: 0,
            permanent: false,
            force_new: false,
            hold_add: false,
            hold_delete: false,
            lose_delete_reply: false,
            lose_add_reply: false,
            reject_adds: 0,
            add_requests: 0,
        }));
        let (stop, mut stopped) = watch::channel(false);
        let udp_task = tokio::spawn(async move {
            loop {
                let mut packet = [0; 4096];
                tokio::select! {
                    _=stopped.changed()=>break,
                    result=udp.recv_from(&mut packet)=>{
                        let (n,from)=result.unwrap();
                        if std::str::from_utf8(&packet[..n]).unwrap().contains(&xml::target(2)) {
                            let reply=format!("HTTP/1.1 200 OK\r\nST: {}\r\nLocation: {location}\r\n\r\n",xml::target(2));
                            udp.send_to(reply.as_bytes(),from).await.unwrap();
                        }
                    },
                }
            }
        });
        let records = state.clone();
        let mut stopped = stop.subscribe();
        let http_task = tokio::spawn(async move {
            loop {
                let stream = tokio::select! {_=stopped.changed()=>break,result=listener.accept()=>result.unwrap().0};
                let raw = read_request(&stream).await;
                if raw.starts_with("GET ") {
                    answer(&stream, 200, &description(2)).await;
                    continue;
                }
                let body = raw.split_once("\r\n\r\n").unwrap().1;
                let doc = roxmltree::Document::parse(body).unwrap();
                let operation = doc
                    .descendants()
                    .find(|n| {
                        n.is_element() && n.tag_name().namespace() == Some(xml::service(2).as_str())
                    })
                    .unwrap();
                let name = operation.tag_name().name();
                let (status, body, hold, omit) = {
                    let mut s = records.lock().unwrap();
                    match name {
                        "GetNATRSIPStatus" => (
                            200,
                            response(2, name, "<NewNATEnabled>1</NewNATEnabled>"),
                            false,
                            false,
                        ),
                        "AddAnyPortMapping" => {
                            s.add_requests += 1;
                            let requested =
                                field(operation, "NewExternalPort").parse::<u16>().unwrap();
                            let internal =
                                field(operation, "NewInternalPort").parse::<u16>().unwrap();
                            let description =
                                field(operation, "NewPortMappingDescription").to_string();
                            if s.reject_adds > 0 {
                                s.reject_adds -= 1;
                                (500, fault(718), false, false)
                            } else if s.permanent && field(operation, "NewLeaseDuration") != "0" {
                                (500, fault(725), false, false)
                            } else {
                                let port = if !s.force_new
                                    && s.mappings.get(&requested).is_some_and(|m| {
                                        m.description == description && m.internal == internal
                                    }) {
                                    requested
                                } else {
                                    let p = s.next;
                                    s.next += 1;
                                    p
                                };
                                s.mappings.insert(
                                    port,
                                    Reservation {
                                        internal,
                                        description,
                                    },
                                );
                                s.adds.push(requested);
                                let hold = std::mem::take(&mut s.hold_add);
                                (
                                    200,
                                    response(
                                        2,
                                        name,
                                        &format!("<NewReservedPort>{port}</NewReservedPort>"),
                                    ),
                                    hold,
                                    std::mem::take(&mut s.lose_add_reply),
                                )
                            }
                        }
                        "GetSpecificPortMappingEntry" => {
                            let port = field(operation, "NewExternalPort").parse::<u16>().unwrap();
                            match s.mappings.get(&port) {
                                Some(m) => (
                                    200,
                                    response(2, name, &entry_body(port, m, false)),
                                    false,
                                    false,
                                ),
                                None => (500, fault(714), false, false),
                            }
                        }
                        "GetGenericPortMappingEntry" => {
                            s.generic += 1;
                            let index = field(operation, "NewPortMappingIndex")
                                .parse::<usize>()
                                .unwrap();
                            match s.mappings.iter().nth(index) {
                                Some((port, m)) => (
                                    200,
                                    response(2, name, &entry_body(*port, m, true)),
                                    false,
                                    false,
                                ),
                                None => (500, fault(713), false, false),
                            }
                        }
                        "DeletePortMapping" => {
                            let port = field(operation, "NewExternalPort").parse::<u16>().unwrap();
                            let owned = s.mappings.remove(&port).unwrap();
                            assert!(
                                owned.description.starts_with("aeternity:"),
                                "deleted someone else's mapping"
                            );
                            s.deleted.push(port);
                            let omit = std::mem::take(&mut s.lose_delete_reply);
                            let hold = std::mem::take(&mut s.hold_delete);
                            (200, response(2, name, ""), hold, omit)
                        }
                        _ => panic!("unexpected action {name}"),
                    }
                };
                if hold {
                    loop {
                        tokio::select! {_=stopped.changed()=>return,r=stream.readable()=>r.unwrap()};
                        match stream.try_read(&mut [0; 32]) {
                            Ok(0) => break,
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                            other => panic!("expected closed uncertain mutation: {other:?}"),
                        }
                    }
                } else if !omit {
                    answer(&stream, status, &body).await;
                }
            }
        });
        Self {
            state,
            settings,
            stop,
            tasks: vec![udp_task, http_task],
        }
    }
    fn config(&self) -> Config {
        Config {
            network: self.settings.clone(),
            system_gateways: false,
            ports: PortsSource::new(ports()).unwrap(),
            requests: 4,
            listener: None,
        }
    }
    fn spawn(&self, config: Config) -> Worker {
        Worker::with_timing(config, Duration::from_secs(60), RENEWAL_INTERVAL).unwrap()
    }
    async fn close(mut self) {
        self.stop.send_replace(true);
        for task in self.tasks.drain(..) {
            timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
impl Drop for Router {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn random_port_retries_are_bounded_and_reconcile_a_lost_assignment_before_retrying() {
    for (rejections, lost_reply) in [(2, false), (4, false), (0, true)] {
        let router = Router::start().await;
        {
            let mut state = router.state.lock().unwrap();
            state.reject_adds = rejections;
            state.lose_add_reply = lost_reply;
        }
        let config = router.config();
        config
            .ports
            .set(Ports {
                internal: 6000,
                external: 0,
            })
            .unwrap();
        let worker = router.spawn(config.clone());
        let client = worker.client();
        client.refresh().unwrap().await.unwrap();
        let info = wait_info(&client, |i| i.completed == 1).await;
        if rejections == 4 {
            assert_eq!(info.last_error, Some(Error::Soap(718)));
            assert!(info.mapping.is_none());
        } else {
            assert!(info.mapping.is_some(), "{info:?}");
            assert_eq!(info.last_error, None);
        }
        {
            let state = router.state.lock().unwrap();
            assert_eq!(state.add_requests, if lost_reply { 2 } else { 3 });
            if lost_reply {
                assert_eq!(state.deleted, vec![5000]);
                assert_eq!(info.mapping.unwrap().external, 5001);
            }
        }
        assert_eq!(client.prepare_stop().unwrap().await, Ok(()));
        worker.shutdown().unwrap();
        assert_eq!(config.ports.retained_mappings(), 0);
        assert_eq!(router.state.lock().unwrap().mappings.len(), 1);
        router.close().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn listener_gate_waits_and_survives_worker_replacement() {
    let router = Router::start().await;
    let gate = ListenerSource::default();
    let lease = gate.acquire().unwrap();
    assert!(matches!(gate.acquire(), Err(Error::Busy)));
    let mut config = router.config();
    config.listener = Some(gate.clone());
    let worker = router.spawn(config.clone());
    let client = worker.client();
    assert_eq!(client.refresh().unwrap().await, Err(Error::NotReady));
    assert_eq!(client.info().attempts, 0);
    assert!(router.state.lock().unwrap().adds.is_empty());
    lease.publish(Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6000)), 0);
    let first = wait_info(&client, |i| i.mapping.is_some()).await;
    assert_eq!(first.mapping.unwrap().internal, 6000);
    worker.shutdown().unwrap();
    let replacement = router.spawn(config);
    let next = replacement.client();
    next.refresh().unwrap().await.unwrap();
    assert_eq!(
        wait_info(&next, |i| i.mapping.is_some())
            .await
            .mapping
            .unwrap()
            .internal,
        6000
    );
    drop(lease);
    wait_info(&next, |i| i.mapping.is_none()).await;
    assert_eq!(next.refresh().unwrap().await, Err(Error::NotReady));
    wait_info(&next, |i| i.retirement_completions > 0).await;
    assert!(!next.info().active);
    assert_eq!(next.info().retained_mappings, 0);
    // A new connection owner acquires the same gate, without reviving the
    // old listener or changing the configured standalone port source.
    let lease = gate.acquire().unwrap();
    lease.publish(Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6001)), 0);
    assert_eq!(
        wait_info(&next, |i| i.mapping.is_some())
            .await
            .mapping
            .unwrap()
            .internal,
        6001
    );
    assert_eq!(next.prepare_stop().unwrap().await, Ok(()));
    lease.clear();
    lease.publish(Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6002)), 0);
    assert!(matches!(next.refresh(), Err(Error::Closed)));
    replacement.shutdown().unwrap();
    assert_eq!(router.state.lock().unwrap().mappings.len(), 1);
    router.close().await;
}
async fn mapped(worker: &Worker, completed: u64) -> Mapping {
    let info = wait_info(&worker.client(), |i| i.completed >= completed).await;
    assert_eq!(info.last_error, None);
    info.mapping.unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn assigned_permanent_mapping_survives_child_restart_and_config_change_for_cleanup() {
    let router = Router::start().await;
    router.state.lock().unwrap().permanent = true;
    let config = router.config();
    let source = config.ports.clone();
    let worker = router.spawn(config.clone());
    let old = worker.client();
    assert!(matches!(
        Worker::with_timing(config.clone(), Duration::from_secs(60), RENEWAL_INTERVAL),
        Err(Error::Busy)
    ));
    old.refresh().unwrap().await.unwrap();
    let mapping = mapped(&worker, 1).await;
    assert_eq!(mapping.external, 5000);
    assert_eq!(mapping.lifetime, None);
    worker.shutdown().unwrap();
    assert!(matches!(old.refresh(), Err(Error::Closed)));
    assert_eq!(source.retained_mappings(), 1);
    source
        .set(Ports {
            internal: 6000,
            external: 7000,
        })
        .unwrap();
    let replacement = router.spawn(config);
    assert_eq!(replacement.info().retained_mappings, 1);
    replacement.client().prepare_stop().unwrap().await.unwrap();
    replacement.shutdown().unwrap();
    assert_eq!(source.retained_mappings(), 0);
    {
        let s = router.state.lock().unwrap();
        assert_eq!(s.deleted, vec![5000]);
        assert_eq!(s.mappings.keys().copied().collect::<Vec<_>>(), vec![4000]);
    }
    router.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_add_recovers_unknown_assigned_port_without_deleting_foreign_entries() {
    let router = Router::start().await;
    router.state.lock().unwrap().hold_add = true;
    let config = router.config();
    let source = config.ports.clone();
    let worker = router.spawn(config.clone());
    worker.client().refresh().unwrap().await.unwrap();
    timeout(Duration::from_secs(2), async {
        while router.state.lock().unwrap().adds.is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(source.uncertain_mappings(), 1);
    worker.shutdown().unwrap();
    let replacement = router.spawn(config);
    replacement.client().prepare_stop().unwrap().await.unwrap();
    replacement.shutdown().unwrap();
    assert_eq!(source.retained_mappings(), 0);
    {
        let s = router.state.lock().unwrap();
        assert_eq!(s.deleted, vec![5000]);
        assert_eq!(s.generic, 3);
        assert!(s.mappings.contains_key(&4000));
    }
    router.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn renewal_prefers_assigned_port_and_tracks_a_router_that_allocates_another() {
    let router = Router::start().await;
    let config = router.config();
    let source = config.ports.clone();
    let worker = router.spawn(config);
    worker.client().refresh().unwrap().await.unwrap();
    assert_eq!(mapped(&worker, 1).await.external, 5000);
    router.state.lock().unwrap().force_new = true;
    worker.client().refresh().unwrap().await.unwrap();
    assert_eq!(mapped(&worker, 2).await.external, 5001);
    assert_eq!(source.uncertain_mappings(), 1);
    worker.client().prepare_stop().unwrap().await.unwrap();
    worker.shutdown().unwrap();
    {
        let s = router.state.lock().unwrap();
        assert_eq!(s.adds, vec![4015, 5000]);
        assert_eq!(s.deleted, vec![5000, 5001]);
        assert_eq!(s.generic, 4);
    }
    assert_eq!(source.retained_mappings(), 0);
    router.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn lost_delete_ack_is_retained_and_replacement_preserves_a_foreign_reused_port() {
    let router = Router::start().await;
    let config = router.config();
    let source = config.ports.clone();
    let worker = router.spawn(config.clone());
    worker.client().refresh().unwrap().await.unwrap();
    mapped(&worker, 1).await;
    router.state.lock().unwrap().lose_delete_reply = true;
    assert!(worker.client().prepare_stop().unwrap().await.is_err());
    worker.shutdown().unwrap();
    assert_eq!(source.retained_mappings(), 1);
    router.state.lock().unwrap().mappings.insert(
        5000,
        Reservation {
            internal: 3015,
            description: "another-owner".into(),
        },
    );
    let replacement = router.spawn(config);
    replacement.client().prepare_stop().unwrap().await.unwrap();
    replacement.shutdown().unwrap();
    assert_eq!(source.retained_mappings(), 0);
    {
        let s = router.state.lock().unwrap();
        assert_eq!(s.deleted, vec![5000]);
        assert_eq!(s.mappings[&5000].description, "another-owner");
    }
    router.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn retained_mapping_quota_rejects_before_wire_mutation_and_cleanup_frees_every_record() {
    let router = Router::start().await;
    let config = router.config();
    let source = config.ports.clone();
    // Exercise the ledger's hard safety cap directly. The worker retires
    // obsolete configurations before adding another mapping.
    let network = Network::new(config.network).unwrap();
    let gateway = network.discover().await.unwrap();
    for index in 0..32 {
        source
            .add_ports(
                network.clone(),
                gateway.clone(),
                Ports {
                    internal: 3015 + index,
                    external: 4015 + index,
                },
            )
            .await
            .unwrap();
    }
    assert_eq!(source.retained_mappings(), 32);
    assert_eq!(
        source
            .add_ports(
                network,
                gateway,
                Ports {
                    internal: 8000,
                    external: 9000,
                }
            )
            .await,
        Err(Error::Limit)
    );
    assert_eq!(router.state.lock().unwrap().adds.len(), 32);
    source.cleanup().await.unwrap();
    assert_eq!(source.retained_mappings(), 0);
    assert_eq!(router.state.lock().unwrap().deleted.len(), 32);
    router.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_configuration_changes_retire_permanent_mappings_before_adding_replacements() {
    let router = Router::start().await;
    router.state.lock().unwrap().permanent = true;
    let config = router.config();
    let source = config.ports.clone();
    let worker = router.spawn(config);
    for index in 0..36 {
        source
            .set(Ports {
                internal: 6000 + index,
                external: 7000 + index,
            })
            .unwrap();
        worker.client().refresh().unwrap().await.unwrap();
        let mapping = mapped(&worker, u64::from(index) + 1).await;
        assert_eq!(mapping.internal, 6000 + index);
        assert_eq!(mapping.lifetime, None);
        assert_eq!(source.retained_mappings(), 1);
        assert_eq!(source.retiring_mappings(), 0);
        let state = router.state.lock().unwrap();
        assert_eq!(state.mappings.len(), 2);
        assert!(state.mappings.contains_key(&4000));
        assert_eq!(state.deleted.len(), usize::from(index));
    }
    worker.client().prepare_stop().unwrap().await.unwrap();
    worker.shutdown().unwrap();
    assert_eq!(source.retained_mappings(), 0);
    assert_eq!(router.state.lock().unwrap().deleted.len(), 36);
    router.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_retirement_blocks_replacement_and_renewal_reconciles_a_reused_port() {
    let router = Router::start().await;
    let config = router.config();
    let source = config.ports.clone();
    let worker = router.spawn(config);
    worker.client().refresh().unwrap().await.unwrap();
    assert_eq!(mapped(&worker, 1).await.external, 5000);
    router.state.lock().unwrap().lose_delete_reply = true;
    source
        .set(Ports {
            internal: 6000,
            external: 7000,
        })
        .unwrap();
    worker.client().refresh().unwrap().await.unwrap();
    let info = wait_info(&worker.client(), |i| i.completed == 2).await;
    assert!(info.last_error.is_some() && info.mapping.is_none());
    assert_eq!(info.retiring_mappings, 1);
    assert_eq!(router.state.lock().unwrap().adds, vec![4015]);
    router.state.lock().unwrap().mappings.insert(
        5000,
        Reservation {
            internal: 3015,
            description: "another-owner".into(),
        },
    );
    // Reverting the configuration must not renew the deleted reservation or
    // steal the external port subsequently assigned to somebody else.
    source.set(ports()).unwrap();
    worker.client().refresh().unwrap().await.unwrap();
    assert_eq!(mapped(&worker, 3).await.external, 5001);
    assert_eq!(source.retiring_mappings(), 0);
    worker.client().prepare_stop().unwrap().await.unwrap();
    worker.shutdown().unwrap();
    {
        let state = router.state.lock().unwrap();
        assert_eq!(state.adds, vec![4015, 4015]);
        assert_eq!(state.deleted, vec![5000, 5001]);
        assert_eq!(state.mappings[&5000].description, "another-owner");
    }
    router.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn withdrawn_listener_cleanup_survives_rejected_refresh_and_worker_cancellation() {
    let router = Router::start().await;
    let gate = ListenerSource::default();
    let lease = gate.acquire().unwrap();
    let mut config = router.config();
    config.listener = Some(gate);
    config.network.request_timeout = Duration::from_secs(10);
    let source = config.ports.clone();
    let worker = router.spawn(config.clone());
    lease.publish(Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6000)), 7000);
    assert_eq!(mapped(&worker, 1).await.external, 5000);
    router.state.lock().unwrap().hold_delete = true;
    lease.clear();
    timeout(Duration::from_secs(2), async {
        while router.state.lock().unwrap().deleted.is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let client = worker.client();
    assert_eq!(client.refresh().unwrap().await, Err(Error::NotReady));
    assert!(client.info().retiring && client.info().active);
    worker.shutdown().unwrap();
    assert_eq!(source.retiring_mappings(), 1);
    router.state.lock().unwrap().mappings.insert(
        5000,
        Reservation {
            internal: 6000,
            description: "another-owner".into(),
        },
    );
    let replacement =
        Worker::with_timing(config, Duration::from_millis(1), RENEWAL_INTERVAL).unwrap();
    let info = wait_info(&replacement.client(), |i| i.retirement_completions > 0).await;
    assert_eq!(info.last_error, None);
    assert_eq!(info.attempts, 0);
    assert_eq!(source.retained_mappings(), 0);
    replacement.shutdown().unwrap();
    assert_eq!(
        router.state.lock().unwrap().mappings[&5000].description,
        "another-owner"
    );
    router.close().await;
}
