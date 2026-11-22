use ae_chain::repository::hyperchain::inputs::Pins;
use ae_hyperchain::{
    parent_block::ParentBlock,
    parent_cache::{self, Cache, ChildTop, runtime as cache},
    parent_connector::{
        fetch::{Backend, BackendError, Context, Query, Remote},
        runtime as connector,
    },
};
use ae_sync::chain::hyperchain::scoped;
use ae_types::{Hash256, Unsigned};
use num_bigint::BigInt;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
pub fn key(n: u64) -> Vec<u8> {
    let mut bytes = vec![0; 32];
    bytes[24..].copy_from_slice(&n.to_be_bytes());
    bytes
}
pub fn parent(height: u64, n: u64) -> ParentBlock {
    ParentBlock {
        hash: key(n),
        height: height.into(),
        previous_hash: Some(key(n - 1)),
        time: (height * 100).into(),
    }
}
#[derive(Clone)]
struct Targets(Vec<BigInt>);
impl cache::Targets for Targets {
    fn targets(&mut self, _: &Unsigned) -> Result<Vec<BigInt>, cache::TargetError> {
        Ok(self.0.clone())
    }
}
pub struct ParentBackend {
    pub missing: AtomicBool,
}
impl Backend for ParentBackend {
    fn fetch(
        &self,
        _: usize,
        query: &Query,
        _: &[u8; 16],
        _: &Context,
    ) -> Result<Remote, BackendError> {
        if self.missing.load(Ordering::Acquire) {
            return Ok(Remote::NotFound);
        }
        let height = match query {
            Query::Top => 8,
            Query::Height(h) => u64::try_from(h).map_err(|_| BackendError::ResourceLimit)?,
            Query::Hash(_) => return Ok(Remote::NotFound),
        };
        Ok(Remote::Block(parent(height, 1000 + height)))
    }
}
pub struct Pair {
    pub cache: Option<cache::Runtime>,
    pub connector: Option<connector::Runtime>,
    pub client: cache::Client,
    pub backend: Arc<ParentBackend>,
}
pub fn wait(mut check: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !check() {
        assert!(Instant::now() < end, "parent wait expired");
        thread::sleep(Duration::from_millis(1));
    }
}
impl Pair {
    pub fn new(missing: bool) -> Self {
        Self::with_targets(missing, vec![1.into(), 6.into()])
    }
    pub fn with_targets(missing: bool, targets: Vec<BigInt>) -> Self {
        let mailbox = cache::Mailbox::new(
            Cache::new(
                parent_cache::Config {
                    child_start_height: 0.into(),
                    max_size: 200.into(),
                    retry_interval: 1000.into(),
                    finality: 2.into(),
                },
                ChildTop {
                    height: 0.into(),
                    hash: Hash256::ZERO,
                },
            ),
            Default::default(),
        )
        .unwrap();
        let backend = Arc::new(ParentBackend {
            missing: AtomicBool::new(missing),
        });
        let connector = connector::Runtime::spawn(
            backend.clone(),
            3,
            [0; 16],
            mailbox.sink(),
            Default::default(),
        )
        .unwrap();
        let client = mailbox.client();
        let cache = mailbox
            .spawn(
                Targets(targets.clone()),
                cache::ConnectorLink::new(connector.client()),
                super::order(&super::DATA["profile"]),
            )
            .unwrap();
        let pair = Self {
            cache: Some(cache),
            connector: Some(connector),
            client,
            backend,
        };
        pair.post(8, 1008);
        if !missing {
            wait(|| {
                targets.iter().all(|h| {
                    matches!(
                        pair.client.snapshot().cache.blocks().get(h),
                        Some(parent_cache::Entry::Block(_))
                    )
                })
            });
        }
        pair
    }
    pub fn post(&self, h: u64, n: u64) {
        self.client.post_block(parent(h, n)).unwrap();
        self.client.barrier(Duration::from_secs(5)).unwrap();
    }
    pub fn inputs(&self) -> scoped::Cached<Arc<Pins>> {
        scoped::Cached {
            cache: self.client.clone(),
            pins: Arc::new(Pins::default()),
            parent_start: 1.into(),
            parent_length: 5.into(),
            limits: Default::default(),
        }
    }
}
impl Drop for Pair {
    fn drop(&mut self) {
        if let Some(cache) = self.cache.take() {
            cache.shutdown().unwrap();
        }
        if let Some(connector) = self.connector.take() {
            connector.shutdown().unwrap();
        }
    }
}
