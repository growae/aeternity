use ae_sync::{analytics, runtime::workers, upnp};
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::UdpSocket, sync::watch, task::JoinHandle};

pub struct Services {
    records: Arc<Mutex<Vec<(u16, u16, u32)>>>,
    gateway: SocketAddrV4,
    silent: UdpSocket,
    stop: watch::Sender<bool>,
    server: Option<JoinHandle<()>>,
    clients: Mutex<Vec<(analytics::Client, upnp::Client)>>,
}
impl Services {
    pub async fn start() -> Self {
        let gateway = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let SocketAddr::V4(address) = gateway.local_addr().unwrap() else {
            unreachable!()
        };
        let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let records = Arc::new(Mutex::new(Vec::new()));
        let requests = records.clone();
        let (stop, mut stopped) = watch::channel(false);
        let server = tokio::spawn(async move {
            let mut bytes = [0; 64];
            loop {
                let (n, from) = tokio::select! {_=stopped.changed()=>break,packet=gateway.recv_from(&mut bytes)=>packet.unwrap()};
                let reply = if n == 2 {
                    assert_eq!(&bytes[..n], &[0, 0]);
                    vec![0, 128, 0, 0, 0, 0, 0, 1, 203, 0, 113, 1]
                } else {
                    assert_eq!(n, 12);
                    assert_eq!(&bytes[..4], &[0, 2, 0, 0]);
                    let internal = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
                    let external = u16::from_be_bytes(bytes[6..8].try_into().unwrap());
                    let lifetime = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
                    requests
                        .lock()
                        .unwrap()
                        .push((internal, external, lifetime));
                    let mut reply = vec![0, 130, 0, 0, 0, 0, 0, 1];
                    reply.extend_from_slice(&bytes[4..12]);
                    reply
                };
                gateway.send_to(&reply, from).await.unwrap();
            }
        });
        Self {
            records,
            gateway: address,
            silent,
            stop,
            server: Some(server),
            clients: Mutex::new(Vec::new()),
        }
    }
    pub fn configure(&self, config: &mut upnp::Config) {
        let SocketAddr::V4(ssdp) = self.silent.local_addr().unwrap() else {
            unreachable!()
        };
        config.system_gateways = false;
        config.network = upnp::Settings {
            bind: Ipv4Addr::LOCALHOST,
            ssdp,
            gateways: vec![self.gateway],
            discovery_timeout: Duration::from_millis(20),
            request_timeout: Duration::from_millis(500),
            initial_retry: Duration::from_millis(10),
            attempts: 2,
            ..Default::default()
        };
    }
    pub async fn mapped(&self, workers: &workers::Handle, port: u16, additions: usize) {
        let bindings = workers.bindings();
        let analytics = bindings.analytics.unwrap().current().unwrap();
        let client = bindings.upnp.unwrap().current().unwrap();
        self.clients
            .lock()
            .unwrap()
            .push((analytics, client.clone()));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let info = client.info();
                assert!(info.running, "{info:?}");
                if info
                    .mapping
                    .as_ref()
                    .is_some_and(|m| m.internal == port && m.external == 3456)
                    && self
                        .records
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|r| r.2 != 0)
                        .count()
                        >= additions
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            self.records
                .lock()
                .unwrap()
                .contains(&(port, 3456, upnp::MAPPING_LIFETIME))
        );
    }
    pub async fn no_listener(&self, workers: &workers::Handle) {
        let bindings = workers.bindings();
        let analytics = bindings.analytics.unwrap().current().unwrap();
        assert!(analytics.enabled() && analytics.info().running);
        assert!(
            analytics
                .snapshot()
                .unwrap()
                .await
                .unwrap()
                .stats
                .is_empty()
        );
        let mapping = bindings.upnp.unwrap().current().unwrap();
        self.clients
            .lock()
            .unwrap()
            .push((analytics, mapping.clone()));
        assert_eq!(mapping.refresh().unwrap().await, Err(upnp::Error::NotReady));
        assert_eq!(mapping.info().attempts, 0);
        assert!(self.records.lock().unwrap().is_empty());
    }
    pub async fn finish(mut self, workers: &workers::Handle, mapped: bool) {
        let status = workers.status();
        assert!(status.joined);
        {
            let clients = self.clients.lock().unwrap();
            assert!(!clients.is_empty());
            for (analytics, upnp) in clients.iter() {
                assert!(analytics.info().joined && upnp.info().joined);
            }
        }
        assert_eq!(status.upnp_cleanup_error, None);
        {
            let records = self.records.lock().unwrap();
            let additions: Vec<_> = records.iter().filter(|r| r.2 != 0).collect();
            assert_eq!(!additions.is_empty(), mapped);
            for (internal, external, lifetime) in additions {
                assert_ne!(*internal, 0);
                assert_eq!(*external, 3456);
                assert_eq!(*lifetime, upnp::MAPPING_LIFETIME);
                // NAT-PMP deletion identifies the internal TCP port and uses
                // external port zero, regardless of the granted public port.
                assert!(records.contains(&(*internal, 0, 0)), "{records:?}");
            }
        }
        self.stop.send_replace(true);
        self.server.take().unwrap().await.unwrap();
    }
}
impl Drop for Services {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
