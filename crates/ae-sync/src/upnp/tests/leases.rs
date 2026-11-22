use super::*;
use std::time::Instant;
use tokio::sync::watch;

#[derive(Clone, Copy)]
struct Request {
    at: Instant,
    ports: Ports,
}
struct Control {
    external: u16,
    lifetime: u32,
    reject: usize,
    hold: bool,
    adds: Vec<Request>,
    deletes: usize,
}
struct Router {
    settings: Settings,
    control: Arc<Mutex<Control>>,
    stop: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
    _silent: UdpSocket,
}
impl Router {
    async fn start(lifetime: u32) -> Self {
        let gateway = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let settings = Settings {
            ssdp: address(silent.local_addr().unwrap()),
            gateways: vec![address(gateway.local_addr().unwrap())],
            request_timeout: Duration::from_secs(3),
            initial_retry: Duration::from_millis(250),
            ..settings()
        };
        let control = Arc::new(Mutex::new(Control {
            external: 8000,
            lifetime,
            reject: 0,
            hold: false,
            adds: vec![],
            deletes: 0,
        }));
        let state = control.clone();
        let (stop, mut stopped) = watch::channel(false);
        let task = tokio::spawn(async move {
            loop {
                let mut bytes = [0; 64];
                let (n, from) = tokio::select! { _=stopped.changed()=>break, r=gateway.recv_from(&mut bytes)=>r.unwrap() };
                let reply = if n == 2 {
                    assert_eq!(&bytes[..n], &[0, 0]);
                    vec![0, 128, 0, 0, 0, 0, 0, 1, 203, 0, 113, 1]
                } else {
                    assert_eq!(n, 12);
                    assert_eq!(&bytes[..4], &[0, 2, 0, 0]);
                    let ports = Ports {
                        internal: u16::from_be_bytes(bytes[4..6].try_into().unwrap()),
                        external: u16::from_be_bytes(bytes[6..8].try_into().unwrap()),
                    };
                    let deleting = bytes[8..12] == [0; 4];
                    let mut s = state.lock().unwrap();
                    if deleting {
                        assert_eq!(ports.external, 0);
                        s.deletes += 1;
                    } else {
                        s.adds.push(Request {
                            at: Instant::now(),
                            ports,
                        });
                        assert_eq!(
                            u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
                            MAPPING_LIFETIME
                        );
                        if s.hold {
                            continue;
                        }
                        if s.reject > 0 {
                            s.reject -= 1;
                            bytes[2..4].copy_from_slice(&2u16.to_be_bytes());
                        }
                    }
                    let mut reply = vec![0, 130, bytes[2], bytes[3], 0, 0, 0, 1];
                    reply.extend_from_slice(&ports.internal.to_be_bytes());
                    reply.extend_from_slice(&if deleting { 0 } else { s.external }.to_be_bytes());
                    reply.extend_from_slice(&if deleting { 0 } else { s.lifetime }.to_be_bytes());
                    reply
                };
                gateway.send_to(&reply, from).await.unwrap();
            }
        });
        Self {
            settings,
            control,
            stop,
            task: Some(task),
            _silent: silent,
        }
    }
    fn config(&self, gate: &ListenerSource) -> Config {
        Config {
            network: self.settings.clone(),
            system_gateways: false,
            listener: Some(gate.clone()),
            ..Default::default()
        }
    }
    async fn close(mut self) {
        self.stop.send_replace(true);
        timeout(Duration::from_secs(2), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
    }
}
impl Drop for Router {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
fn publish(lease: &super::super::listener::ListenerLease) {
    lease.publish(Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 6000)), 7000);
}
async fn confirmed(client: &Client, completed: u64, external: u16) {
    let info = wait_info(client, |i| i.completed >= completed).await;
    assert_eq!(info.last_error, None);
    assert_eq!(info.mapping.unwrap().external, external);
}

#[tokio::test(flavor = "current_thread")]
async fn automatic_renewal_follows_each_granted_lease_and_reuses_the_assigned_port() {
    let router = Router::start(1).await;
    let gate = ListenerSource::default();
    let lease = gate.acquire().unwrap();
    let worker = Worker::with_timing(
        router.config(&gate),
        Duration::from_secs(60),
        RENEWAL_INTERVAL,
    )
    .unwrap();
    let client = worker.client();
    publish(&lease);
    confirmed(&client, 1, 8000).await;
    {
        let mut s = router.control.lock().unwrap();
        s.external = 8001;
        s.lifetime = 2;
    }
    confirmed(&client, 2, 8001).await;
    {
        let mut s = router.control.lock().unwrap();
        s.external = 8002;
        s.lifetime = 7200;
    }
    confirmed(&client, 3, 8002).await;
    assert_eq!(gate.advertised_port(7000), 8002);
    {
        let s = router.control.lock().unwrap();
        assert_eq!(s.adds.len(), 3);
        assert_eq!(
            s.adds.iter().map(|r| r.ports.external).collect::<Vec<_>>(),
            vec![7000, 8000, 8001]
        );
        assert!(s.adds.iter().all(|r| r.ports.internal == 6000));
        assert!(s.adds[1].at.duration_since(s.adds[0].at) >= Duration::from_millis(400));
        assert!(s.adds[2].at.duration_since(s.adds[1].at) >= Duration::from_millis(900));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(client.info().attempts, 3);
    client.prepare_stop().unwrap().await.unwrap();
    worker.shutdown().unwrap();
    router.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_mapping_retries_back_off_and_success_resets_the_retry_delay() {
    let router = Router::start(7200).await;
    router.control.lock().unwrap().reject = 2;
    let gate = ListenerSource::default();
    let lease = gate.acquire().unwrap();
    let worker = Worker::with_timing(
        router.config(&gate),
        Duration::from_secs(60),
        RENEWAL_INTERVAL,
    )
    .unwrap();
    let client = worker.client();
    publish(&lease);
    let failed = wait_info(&client, |i| i.completed == 2).await;
    assert_eq!(failed.last_error, Some(Error::NatPmp(2)));
    assert_eq!(gate.advertised_port(7000), 7000);
    confirmed(&client, 3, 8000).await;
    {
        let s = router.control.lock().unwrap();
        assert_eq!(s.adds.len(), 3);
        assert!(s.adds[1].at.duration_since(s.adds[0].at) >= Duration::from_millis(900));
        assert!(s.adds[2].at.duration_since(s.adds[1].at) >= Duration::from_millis(1900));
    }
    router.control.lock().unwrap().reject = 1;
    client.refresh().unwrap().await.unwrap();
    confirmed(&client, 5, 8000).await;
    {
        let s = router.control.lock().unwrap();
        assert_eq!(s.adds.len(), 5);
        let gap = s.adds[4].at.duration_since(s.adds[3].at);
        assert!(
            gap >= Duration::from_millis(900) && gap < Duration::from_secs(3),
            "{gap:?}"
        );
    }
    client.prepare_stop().unwrap().await.unwrap();
    worker.shutdown().unwrap();
    router.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn short_lease_timer_does_not_cancel_its_pending_renewal_and_cleanup_quiesces_it() {
    let router = Router::start(1).await;
    let gate = ListenerSource::default();
    let lease = gate.acquire().unwrap();
    let worker = Worker::with_timing(
        router.config(&gate),
        Duration::from_secs(60),
        RENEWAL_INTERVAL,
    )
    .unwrap();
    let client = worker.client();
    publish(&lease);
    confirmed(&client, 1, 8000).await;
    router.control.lock().unwrap().hold = true;
    wait_info(&client, |i| i.attempts == 2 && i.active).await;
    assert_eq!(gate.advertised_port(7000), 7000);
    tokio::time::sleep(Duration::from_millis(650)).await;
    let pending = client.info();
    assert!(
        pending.active && pending.attempts == 2 && pending.cancelled == 0,
        "{pending:?}"
    );
    client.prepare_stop().unwrap().await.unwrap();
    let prepared = client.info();
    assert!(prepared.prepared && !prepared.active && prepared.cancelled == 1);
    assert_eq!(router.control.lock().unwrap().deletes, 1);
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(client.info().attempts, 2);
    worker.shutdown().unwrap();
    router.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn replacement_worker_does_not_inherit_a_retired_workers_lease_timer() {
    let router = Router::start(1).await;
    let gate = ListenerSource::default();
    let lease = gate.acquire().unwrap();
    let config = router.config(&gate);
    let worker =
        Worker::with_timing(config.clone(), Duration::from_secs(60), RENEWAL_INTERVAL).unwrap();
    publish(&lease);
    confirmed(&worker.client(), 1, 8000).await;
    worker.shutdown().unwrap();
    let replacement =
        Worker::with_timing(config, Duration::from_secs(60), RENEWAL_INTERVAL).unwrap();
    tokio::time::sleep(Duration::from_millis(650)).await;
    assert_eq!(replacement.info().attempts, 0);
    assert_eq!(gate.advertised_port(7000), 7000);
    replacement.client().refresh().unwrap().await.unwrap();
    confirmed(&replacement.client(), 1, 8000).await;
    confirmed(&replacement.client(), 2, 8000).await;
    replacement.client().prepare_stop().unwrap().await.unwrap();
    replacement.shutdown().unwrap();
    router.close().await;
}
