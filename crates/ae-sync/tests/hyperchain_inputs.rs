mod common;
use ae_chain::{
    block::Block,
    repository::{
        self as repo,
        hyperchain::inputs::{Captured, Effect, Effects, InputError, Pins},
    },
};
use ae_hyperchain::{
    block::{Host, Pin},
    parent_block::ParentBlock,
    parent_cache::{self, Cache, ChildTop, runtime as cache},
    parent_connector::{
        fetch::{Backend, BackendError, Context, Query, Remote},
        runtime as connector,
    },
};
use ae_storage::Snapshot;
use ae_sync::chain::{
    self, Importer,
    hyperchain::scoped::{self, ParentInputs, PinInputs},
};
use ae_types::{Hash256, Unsigned};
use common::{hyperchain as fixture, within};
use num_bigint::BigInt;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
const NOW: u64 = 1_000_000;

fn key(n: u64) -> Vec<u8> {
    let mut bytes = vec![0; 32];
    bytes[24..].copy_from_slice(&n.to_be_bytes());
    bytes
}
fn parent(height: u64, n: u64) -> ParentBlock {
    ParentBlock {
        hash: key(n),
        height: height.into(),
        previous_hash: key(n - 1),
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
struct ParentBackend {
    missing: AtomicBool,
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
struct Pair {
    cache: Option<cache::Runtime>,
    connector: Option<connector::Runtime>,
    client: cache::Client,
    backend: Arc<ParentBackend>,
}
fn wait(mut check: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !check() {
        assert!(Instant::now() < end, "parent wait expired");
        thread::sleep(Duration::from_millis(1));
    }
}
impl Pair {
    fn new(missing: bool) -> Self {
        Self::with_targets(missing, vec![1.into(), 6.into()])
    }
    fn with_targets(missing: bool, targets: Vec<BigInt>) -> Self {
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
                fixture::order(&fixture::DATA["profile"]),
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
    fn post(&self, h: u64, n: u64) {
        self.client.post_block(parent(h, n)).unwrap();
        self.client.barrier(Duration::from_secs(5)).unwrap();
    }
    fn inputs(&self) -> scoped::Cached<Arc<Pins>> {
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

#[tokio::test]
async fn scoped_live_parent_snapshots_execute_all_native_finalized_chains() {
    let pair = Pair::new(false);
    for index in 0..7 {
        let node = fixture::Node::new(index);
        let (importer, mut accepted) = Importer::new_hyperchain_scoped(
            node.db.clone(),
            node.config.clone(),
            pair.inputs(),
            Default::default(),
        )
        .unwrap();
        let mut epochs = vec![];
        for (i, block) in node.blocks().into_iter().enumerate() {
            within(
                importer
                    .import_block(block.encode().unwrap(), NOW)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
            node.check(i);
            for effect in accepted.take().unwrap().drain() {
                let Effect::Parent { epoch, block } = effect else {
                    panic!("unexpected parent miss")
                };
                assert_eq!(
                    block.hash,
                    key(1000
                        + u64::try_from((epoch.clone() - BigInt::from(3)).max(0.into()) * 5 + 1)
                            .unwrap())
                );
                epochs.push(epoch);
            }
            assert!(accepted.take().unwrap().entries().is_empty());
        }
        assert!(
            epochs.contains(&1.into())
                && epochs.contains(&2.into())
                && epochs.contains(&3.into())
                && epochs.contains(&4.into())
        );
        importer.stop();
        importer.join().await;
        drop(importer);
        assert!(matches!(accepted.take(), Err(chain::Error::Closed)));
    }
}

#[tokio::test]
async fn captured_child_indexes_and_parent_blocks_remain_at_their_original_revisions() {
    let pair = Pair::new(false);
    let node = fixture::Node::new(0);
    let mut pins = Pins::default();
    let pin = Pin {
        epoch: 1.into(),
        child_height: 0.into(),
        child_hash: node.config.identity.genesis.0.to_vec(),
        parent_height: 7.into(),
    };
    pins.insert(key(77), Some(pin.clone()), Default::default())
        .unwrap();
    pins.insert(key(78), None, Default::default()).unwrap();
    let child = node.db.view().unwrap();
    let mut old = Captured::new(
        child,
        pair.client.snapshot().cache,
        Arc::new(pins),
        1.into(),
        5.into(),
        Effects::default(),
        Default::default(),
    )
    .unwrap();
    assert_eq!(old.pin(&key(77)).unwrap(), Some(pin.clone()));
    assert_eq!(
        old.key_hash(&pin.child_height).unwrap(),
        Some(pin.child_hash)
    );
    assert_eq!(old.pin(&key(78)).unwrap(), None);
    assert!(matches!(
        old.pin(&key(79)),
        Err(InputError::PinNotCaptured(_))
    ));
    assert_eq!(old.key_hash(&(-1).into()).unwrap(), None);
    assert_eq!(old.key_hash(&(BigInt::from(u64::MAX) + 1)).unwrap(), None);
    assert_eq!(old.key_hash(&1.into()).unwrap(), None);
    let (importer, mut accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let blocks = node.blocks();
    for b in &blocks[..2] {
        within(
            importer
                .import_block(b.encode().unwrap(), NOW)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
    }
    assert_eq!(old.revision(), 1);
    assert_eq!(old.key_hash(&1.into()).unwrap(), None);
    let mut fresh = pair
        .inputs()
        .capture(&node.db.view().unwrap(), &blocks[2], &Effects::default())
        .unwrap();
    assert_eq!(fresh.revision(), 3);
    assert_eq!(
        fresh.key_hash(&1.into()).unwrap(),
        Some(blocks[1].hash().unwrap().0.to_vec())
    );
    pair.post(1, 9001);
    assert_eq!(old.entropy(&1.into()).unwrap(), Some(key(1001)));
    assert_eq!(fresh.entropy(&1.into()).unwrap(), Some(key(1001)));
    let mut newer = pair
        .inputs()
        .capture(&node.db.view().unwrap(), &blocks[2], &Effects::default())
        .unwrap();
    assert_eq!(newer.entropy(&1.into()).unwrap(), Some(key(9001)));
    assert!(!accepted.take().unwrap().entries().is_empty());
    importer.stop();
    importer.join().await;
}

type Hook = Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>;
#[derive(Clone, Default)]
struct HookPins {
    hook: Hook,
}
impl PinInputs for HookPins {
    type Error = ();
    fn check(&self) -> Result<(), ()> {
        Ok(())
    }
    fn capture(&self, _: &repo::View<Snapshot<'_>>, _: &Block) -> Result<Arc<Pins>, ()> {
        let hook = self.hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
        Ok(Arc::new(Pins::default()))
    }
}
fn hooked(pair: &Pair, pins: HookPins) -> scoped::Cached<HookPins> {
    scoped::Cached {
        cache: pair.client.clone(),
        pins,
        parent_start: 1.into(),
        parent_length: 5.into(),
        limits: Default::default(),
    }
}

#[tokio::test]
async fn parent_cache_replacement_closes_old_scoped_owner_without_consuming_effects() {
    let mut pair = Pair::new(false);
    let node = fixture::Node::new(0);
    let pins = HookPins::default();
    let (importer, mut accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        hooked(&pair, pins.clone()),
        Default::default(),
    )
    .unwrap();
    let cache = pair.cache.take().unwrap();
    *pins.hook.lock().unwrap() = Some(Box::new(move || {
        cache.shutdown().unwrap();
    }));
    let b = node.blocks()[0].encode().unwrap();
    assert!(matches!(
        within(importer.import_block(b.clone(), NOW).unwrap().wait())
            .await
            .unwrap_err()
            .error,
        chain::Error::Validation {
            operational: true,
            ..
        }
    ));
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    assert!(accepted.take().unwrap().entries().is_empty());
    let replacement = Pair::new(false);
    assert!(
        within(importer.import_block(b.clone(), NOW).unwrap().wait())
            .await
            .is_err()
    );
    importer.stop();
    importer.join().await;
    drop(importer);
    let (next, mut outputs) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        replacement.inputs(),
        Default::default(),
    )
    .unwrap();
    within(next.import_block(b, NOW).unwrap().wait())
        .await
        .unwrap();
    node.check(0);
    assert!(!outputs.take().unwrap().entries().is_empty());
    assert!(matches!(accepted.take(), Err(chain::Error::Closed)));
    next.stop();
    next.join().await;
}

#[tokio::test]
async fn cancelled_scoped_capture_preserves_pending_effects_and_retries_with_fresh_inputs() {
    let pair = Pair::new(false);
    let node = fixture::Node::new(0);
    let pins = HookPins::default();
    let (importer, mut accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        hooked(&pair, pins.clone()),
        chain::Config {
            jobs: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let blocks = node.blocks();
    within(
        importer
            .import_block(blocks[0].encode().unwrap(), NOW)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    *pins.hook.lock().unwrap() = Some(Box::new(move || {
        entered.send(()).unwrap();
        resume.recv_timeout(Duration::from_secs(5)).unwrap();
    }));
    let ticket = importer
        .import_block(blocks[1].encode().unwrap(), NOW)
        .unwrap();
    within(ready).await.unwrap();
    drop(ticket);
    release.send(()).unwrap();
    within(async {
        while importer.available().0 == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(node.db.view().unwrap().status().revision, 2);
    let effects = accepted.take().unwrap();
    assert!(!effects.entries().is_empty());
    within(
        importer
            .import_block(blocks[1].encode().unwrap(), NOW)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    node.check(1);
    importer.stop();
    importer.join().await;
}

#[test]
fn captured_entropy_results_and_notifications_match_original_native_queries() {
    use serde_json::{Value as J, json};
    fn expand(v: &J, table: &[J]) -> J {
        match v {
            J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                expand(&table[v["$ref"].as_u64().unwrap() as usize], table)
            }
            J::Object(m) => J::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), expand(v, table)))
                    .collect(),
            ),
            J::Array(xs) => J::Array(xs.iter().map(|v| expand(v, table)).collect()),
            _ => v.clone(),
        }
    }
    let packed: J = serde_json::from_str(include_str!(
        "../../ae-hyperchain/tests/fixtures/erlang_hc_entropy.json"
    ))
    .unwrap();
    let data = expand(&packed["root"], packed["values"].as_array().unwrap());
    let pair = Pair::with_targets(false, vec![11.into(), 41.into()]);
    let node = fixture::Node::new(0);
    pair.post(11, 11);
    pair.post(41, 41);
    let mut notified = std::collections::BTreeMap::<BigInt, ParentBlock>::new();
    for row in data["direct"].as_array().unwrap() {
        match row["name"].as_str().unwrap() {
            "final" => pair.post(43, 43),
            "replaced" => pair.post(41, 99),
            _ => (),
        }
        let epoch: BigInt = row["epoch"].as_str().unwrap().parse().unwrap();
        let mut host = Captured::new(
            node.db.view().unwrap(),
            pair.client.snapshot().cache,
            Arc::new(Pins::default()),
            11.into(),
            5.into(),
            Effects::default(),
            Default::default(),
        )
        .unwrap();
        let actual = host.entropy(&epoch).unwrap();
        let reply = match &actual {
            Some(h) => json!(["tuple", [["atom", "ok"], ["binary", hex::encode(h)]]]),
            None => json!(["tuple", [["atom", "error"], ["atom", "not_in_cache"]]]),
        };
        assert_eq!(reply, row["reply"]);
        let effects = host.into_effects();
        assert_eq!(effects.entries().len(), 1);
        match &effects.entries()[0] {
            Effect::Parent { epoch, block } => {
                assert!(actual.is_some());
                notified.insert(epoch.clone(), block.clone());
            }
            Effect::RequestParent(height) => {
                assert!(actual.is_none());
                assert_eq!(height, &((epoch - BigInt::from(3)).max(0.into()) * 5 + 11));
            }
        }
        for worker in row["parents"][1].as_array().unwrap() {
            let parents = worker[1][1].as_array().unwrap();
            assert_eq!(parents.len(), notified.len());
            for entry in parents {
                let epoch: BigInt = entry[0][1].as_str().unwrap().parse().unwrap();
                let block = &notified[&epoch];
                assert_eq!(
                    entry[1],
                    json!([
                        "tuple",
                        [
                            ["atom", "block"],
                            ["binary", hex::encode(&block.hash)],
                            ["integer", parent_cache::integer(&block.height).to_string()],
                            ["binary", hex::encode(&block.previous_hash)],
                            ["integer", parent_cache::integer(&block.time).to_string()]
                        ]
                    ])
                );
            }
        }
    }
    // Capture compares one immutable lookup and its notification. Native
    // sleep/retry counts and asynchronous worker delivery are separate APIs.
}

#[test]
fn captured_pin_and_entropy_budgets_preserve_prior_inputs_and_effects() {
    use ae_chain::repository::hyperchain::inputs::Limits;
    let pair = Pair::new(false);
    let node = fixture::Node::new(0);
    let mut pins = Pins::default();
    pins.insert(key(1), None, Default::default()).unwrap();
    let wide = Pin {
        epoch: BigInt::from(1) << 9000usize,
        child_height: 0.into(),
        child_hash: key(1),
        parent_height: 0.into(),
    };
    assert!(matches!(
        pins.insert(key(1), Some(wide.clone()), Default::default()),
        Err(InputError::ResourceLimit)
    ));
    assert!(matches!(
        pins.insert(
            key(2),
            None,
            Limits {
                maximum_pin_entries: 1,
                ..Default::default()
            }
        ),
        Err(InputError::ResourceLimit)
    ));
    let mut host = Captured::new(
        node.db.view().unwrap(),
        pair.client.snapshot().cache,
        Arc::new(pins),
        1.into(),
        5.into(),
        Effects::default(),
        Limits {
            maximum_effect_bytes: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(host.pin(&key(1)).unwrap(), None);
    assert!(matches!(
        host.pin(&key(2)),
        Err(InputError::PinNotCaptured(_))
    ));
    assert!(matches!(
        host.entropy(&1.into()),
        Err(InputError::ResourceLimit)
    ));
    assert!(matches!(
        host.entropy(&wide.epoch),
        Err(InputError::ResourceLimit)
    ));
    assert!(host.effects().entries().is_empty());
    let mut pins = Pins::default();
    pins.insert(
        key(1),
        Some(wide),
        Limits {
            maximum_integer_bytes: 4096,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        Captured::new(
            node.db.view().unwrap(),
            pair.client.snapshot().cache,
            Arc::new(pins),
            1.into(),
            5.into(),
            Effects::default(),
            Default::default()
        ),
        Err(InputError::ResourceLimit)
    ));
}

#[tokio::test]
async fn missing_parent_requests_and_effect_limits_are_retryable_without_block_writes() {
    let pair = Pair::new(true);
    let node = fixture::Node::new(0);
    let (importer, mut accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    let b = node.blocks()[0].encode().unwrap();
    assert!(
        within(importer.import_block(b.clone(), NOW).unwrap().wait())
            .await
            .is_err()
    );
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    let effects = accepted.take().unwrap();
    assert!(!effects.entries().is_empty());
    pair.backend.missing.store(false, Ordering::Release);
    for effect in effects.entries() {
        let Effect::RequestParent(height) = effect else {
            panic!()
        };
        pair.client
            .call(parent_cache::Event::Get(height.clone()))
            .unwrap()
            .wait(Duration::from_secs(5))
            .unwrap();
    }
    wait(|| pair.client.snapshot().cache.lookup(&1.into()).is_ok());
    within(importer.import_block(b.clone(), NOW).unwrap().wait())
        .await
        .unwrap();
    node.check(0);
    importer.stop();
    importer.join().await;
    drop(importer);
    let node = fixture::Node::new(0);
    let mut inputs = pair.inputs();
    inputs.limits.maximum_effects = 0;
    let (limited, mut accepted) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        inputs,
        Default::default(),
    )
    .unwrap();
    assert!(matches!(
        within(limited.import_block(b.clone(), NOW).unwrap().wait())
            .await
            .unwrap_err()
            .error,
        chain::Error::Validation {
            operational: true,
            ..
        }
    ));
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    assert!(accepted.take().unwrap().entries().is_empty());
    limited.stop();
    limited.join().await;
    drop(limited);
    let (next, _) = Importer::new_hyperchain_scoped(
        node.db.clone(),
        node.config.clone(),
        pair.inputs(),
        Default::default(),
    )
    .unwrap();
    within(next.import_block(b, NOW).unwrap().wait())
        .await
        .unwrap();
    node.check(0);
    next.stop();
    next.join().await;
}

#[tokio::test]
async fn remote_pin_capture_replays_native_finalized_chains_without_unneeded_rpc() {
    use ae_sync::parent_pins::{Chain, Node, Service};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let service = Service::spawn(
        vec![
            Node::new(
                Chain::Aeternity,
                &format!("http://{}", listener.local_addr().unwrap()),
                None,
            )
            .unwrap(),
        ],
        Default::default(),
    )
    .unwrap();
    let pair = Pair::new(false);
    for index in 0..7 {
        let node = fixture::Node::new(index);
        let pins = scoped::RemotePins {
            execution: node.config.clone(),
            client: service.client(),
            limits: Default::default(),
        };
        let parents = scoped::Cached {
            cache: pair.client.clone(),
            pins,
            parent_start: 1.into(),
            parent_length: 5.into(),
            limits: Default::default(),
        };
        let (importer, mut accepted) = Importer::new_hyperchain_scoped(
            node.db.clone(),
            node.config.clone(),
            parents,
            Default::default(),
        )
        .unwrap();
        for (i, block) in node.blocks().into_iter().enumerate() {
            within(
                importer
                    .import_block(block.encode().unwrap(), NOW)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
            node.check(i);
            accepted.take().unwrap().drain();
        }
        importer.stop();
        importer.join().await;
    }
    assert!(listener.accept().is_err()); // all retained finalized fixtures have no pin
}
