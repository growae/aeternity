#![allow(dead_code)]
pub mod import;
pub mod ledger_fixtures;
use ae_chain::repository::{Identity, ledger};
use ae_codec::{
    api::{self, ApiType},
    headers::Header,
};
use ae_crypto::blake2b_256;
use ae_mempool::pool::Signed;
use ae_p2p::{noise, runtime as net};
use ae_protocol::NetworkProfile;
use ae_state::{
    Trees,
    genesis::{self, Network},
};
use ae_storage::{
    Database,
    pool::{self as storage, worker},
};
use ae_sync::pool::{self, Config, Connection, Handle};
use ae_types::{Hash256, Signature};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

static NEXT: AtomicU64 = AtomicU64::new(1);
struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        if !self.0.as_os_str().is_empty() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
pub struct Node {
    pub db: Arc<Database>,
    worker: Option<worker::Worker>,
    _temporary: Temporary,
}
impl Node {
    pub fn new() -> Self {
        Self::with_settings(worker::Settings {
            origins_interval: None,
            ..Default::default()
        })
    }
    pub fn with_settings(settings: worker::Settings) -> Self {
        Self::open(
            std::env::temp_dir().join(format!(
                "ae-pool-sync-live-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )),
            settings,
        )
    }
    pub fn open(path: PathBuf, settings: worker::Settings) -> Self {
        Self::open_with_notifications(path, settings, Default::default())
    }
    pub fn with_events(events: ae_storage::events::Config) -> Self {
        Self::with_notifications(ae_storage::Notifications {
            chain: events,
            ..Default::default()
        })
    }
    pub fn with_notifications(notifications: ae_storage::Notifications) -> Self {
        Self::open_with_notifications(
            std::env::temp_dir().join(format!(
                "ae-events-live-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )),
            worker::Settings {
                origins_interval: None,
                ..Default::default()
            },
            notifications,
        )
    }
    fn open_with_notifications(
        path: PathBuf,
        settings: worker::Settings,
        notifications: ae_storage::Notifications,
    ) -> Self {
        let temporary = Temporary(path);
        let profile = NetworkProfile::mainnet();
        let Header::Key(header) = genesis::header(Network::Mainnet).unwrap() else {
            panic!()
        };
        let identity = Identity::new(
            &profile,
            Header::Key(header.clone()).hash().unwrap(),
            blake2b_256(b"pool-sync-runtime"),
        );
        let db = Arc::new(
            Database::open_with_notifications(
                &temporary.0,
                identity,
                Default::default(),
                Default::default(),
                notifications,
            )
            .unwrap(),
        );
        let trees = Trees {
            accounts: genesis::state(Network::Mainnet).unwrap().0,
            contracts: Default::default(),
            channels: Default::default(),
            calls: Default::default(),
            names: Default::default(),
            oracles: Default::default(),
        };
        if db.view().unwrap().status().tip.is_none() {
            let plan = ledger::bootstrap(
                &db.view().unwrap(),
                &profile,
                &header,
                &trees,
                Default::default(),
            )
            .unwrap();
            db.commit(plan).unwrap();
        }
        let config = storage::Config {
            policy: ae_mempool::pool::Config {
                minimum_miner_gas_price: 1.into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let worker = worker::Worker::spawn(db.clone(), profile, config, settings).unwrap();
        Self {
            db,
            worker: Some(worker),
            _temporary: temporary,
        }
    }
    pub fn client(&self) -> worker::Client {
        self.worker.as_ref().unwrap().client()
    }
    pub fn stop_worker(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.shutdown().unwrap();
        }
    }
    pub fn persist(mut self) -> PathBuf {
        self.worker.take().unwrap().shutdown().unwrap();
        std::mem::take(&mut self._temporary.0)
    }
    pub fn reopen(self) -> Self {
        Self::open(
            self.persist(),
            worker::Settings {
                origins_interval: None,
                ..Default::default()
            },
        )
    }
    pub fn service(&self, config: Config) -> (Handle, pool::Events, JoinHandle<pool::Status>) {
        let (handle, events, service) =
            pool::create(self.db.clone(), self.client(), config).unwrap();
        (handle, events, tokio::spawn(service.run()))
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.shutdown().unwrap();
        }
    }
}
pub fn transaction() -> Arc<Signed> {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../ae-storage/tests/fixtures/public_bootstrap.json"
    ))
    .unwrap();
    let bytes = api::decode_as(
        ApiType::Transaction,
        fixture["micros"][0]["transactions"]["transactions"][0]["encoded_tx"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    Arc::new(Signed::decode(&bytes).unwrap())
}
pub fn invalid_signature(tx: &Signed) -> Arc<Signed> {
    Arc::new(Signed::new(
        tx.transaction().clone(),
        vec![Signature([0; 64])],
    ))
}
pub type Established = (net::Client, net::Events, net::Driver);
pub async fn pair(a: u8, b: u8) -> (Established, Established) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (second, _) = listener.accept().await.unwrap();
    let ia = noise::Identity::from_secret([a; 32]);
    let ib = noise::Identity::from_secret([b; 32]);
    let network = noise::Network::new(1, Hash256([0; 32]), b"ae_dev").unwrap();
    let (a, b) = tokio::join!(
        net::establish(
            first,
            &ia,
            &network,
            noise::Role::Initiator(ib.public_key()),
            net::Config::default()
        ),
        net::establish(
            second,
            &ib,
            &network,
            noise::Role::Responder,
            net::Config::default()
        )
    );
    (a.unwrap(), b.unwrap())
}
pub fn connection(established: Established) -> Connection {
    Connection::new(established.0, established.1, established.2).unwrap()
}
pub async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .expect("bounded tiny peer test")
}
