use super::*;
use ae_sync::chain::pending::service;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicI64, AtomicUsize},
    mpsc,
};

struct ControlledClock {
    reads: AtomicUsize,
    time: AtomicI64,
    invalid: AtomicBool,
    panic: AtomicBool,
    gate: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
}
impl ControlledClock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            reads: AtomicUsize::new(0),
            time: AtomicI64::new(1_000_000),
            invalid: AtomicBool::new(false),
            panic: AtomicBool::new(false),
            gate: Mutex::new(None),
        })
    }
    fn pause(&self) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (entered, ready) = mpsc::sync_channel(1);
        let (release, wait) = mpsc::sync_channel(1);
        *self.gate.lock().unwrap() = Some((entered, wait));
        (ready, release)
    }
}
impl generator::Clock for ControlledClock {
    fn unix_millis(&self) -> i128 {
        self.reads.fetch_add(1, Ordering::Relaxed);
        if let Some((entered, wait)) = self.gate.lock().unwrap().take() {
            entered.send(()).unwrap();
            wait.recv_timeout(Duration::from_secs(15)).unwrap();
        }
        assert!(
            !self.panic.load(Ordering::Relaxed),
            "deliberate pending clock failure"
        );
        if self.invalid.load(Ordering::Relaxed) {
            return -1;
        }
        self.time.fetch_add(10_000, Ordering::Relaxed).into()
    }
}
fn settings(lab: &Lab, clock: Arc<ControlledClock>) -> node::Settings {
    let mut settings = commands(lab);
    let node::Settings::OnDemand(s) = &mut settings else {
        unreachable!()
    };
    s.clock = clock;
    s.config.pending_key.requests = 2;
    settings
}
async fn query(client: &service::Client) -> Arc<service::Completion> {
    within(client.get().unwrap()).await.unwrap()
}
async fn wait_until(mut condition: impl FnMut() -> bool) {
    within(async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
}

#[tokio::test]
async fn pending_http_preserves_cache_headers_and_bounds_before_construction() {
    use super::http::get;
    let lab = Lab::new();
    let p = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = p.configured().chain().database().clone();
    let snapshots = p.pow().unwrap().snapshots().clone();
    let initial = db.view().unwrap().status();
    let clock = ControlledClock::new();
    let mut running = p
        .launch(settings(&lab, clock.clone()), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let http = http_checks::start(&mut running).await;
    let address = http.status().address;
    let importer = h.importer().unwrap();
    let pending = h.pending_key().unwrap();
    let capacity = importer.available();
    let mut unread = vec![];
    while let Ok(ticket) = importer.current() {
        unread.push(ticket);
        assert!(unread.len() < 64);
    }
    let (code, headers, body) = get(address, "/v3/key-blocks/pending").await;
    assert_eq!((code, body), (503, json!({"reason":"Temporary overload"})));
    assert_eq!(headers["retry-after"], "3");
    assert_eq!(pending.status().accepted, 0);
    drop(unread);
    wait_until(|| importer.available() == capacity).await;
    let mut held = vec![];
    while let Ok(snapshot) = snapshots.view() {
        held.push(snapshot);
        assert!(held.len() <= 128);
    }
    assert_eq!(get(address, "/v3/key-blocks/pending").await.0, 503);
    assert_eq!(pending.status().last_error, Some(service::Error::Busy));
    assert!(pending.status().cached_parent.is_none());
    drop(held);
    wait_until(|| snapshots.live_snapshots() == 0).await;
    let reads = clock.reads.load(Ordering::Relaxed);
    let first = get(address, "/v2/key-blocks/pending").await;
    assert_eq!(first.0, 200);
    let receipt = query(&pending).await;
    let key = receipt.result.as_ref().unwrap().clone();
    let hash = ae_codec::headers::Header::Key(key.as_ref().clone())
        .hash()
        .unwrap();
    assert_eq!(
        first.2["hash"],
        api::encode(ApiType::KeyBlockHash, hash.as_ref())
    );
    assert_eq!(
        first.2["prev_hash"],
        api::encode(ApiType::KeyBlockHash, initial.tip.unwrap().as_ref())
    );
    assert_eq!(
        first.2["beneficiary"],
        api::encode(ApiType::AccountPubkey, &[42; 32])
    );
    assert_eq!(first.2["height"], 1);
    assert!(first.2.get("pow").is_none() && first.2.get("nonce").is_none());
    drop(receipt);
    for version in ["v2", "v3"] {
        assert_eq!(
            get(address, &format!("/{version}/key-blocks/pending"))
                .await
                .2,
            first.2
        );
        let strings = get(
            address,
            &format!("/{version}/key-blocks/pending?int-as-string=true"),
        )
        .await;
        assert_eq!(strings.0, 200);
        for field in ["height", "target", "time", "version"] {
            assert_eq!(
                strings.2[field],
                if version == "v3" {
                    json!(first.2[field].to_string())
                } else {
                    first.2[field].clone()
                }
            );
        }
        assert_eq!(strings.2["hash"], first.2["hash"]);
    }
    assert_eq!(clock.reads.load(Ordering::Relaxed), reads + 2);
    assert_eq!(db.view().unwrap().status(), initial);
    let receipts = (query(&pending).await, query(&pending).await);
    assert_eq!(get(address, "/v3/key-blocks/pending").await.0, 503);
    drop(receipts);
    importer
        .import_created_block(
            ae_chain::block::Block::Key(key.as_ref().clone())
                .encode()
                .unwrap(),
            key.time + 60_000,
        )
        .unwrap()
        .wait()
        .await
        .unwrap();
    let next = get(address, "/v3/key-blocks/pending").await;
    assert_eq!(next.0, 200);
    assert_eq!(next.2["height"], 2);
    assert_eq!(next.2["prev_hash"], first.2["hash"]);
    assert_ne!(next.2["hash"], first.2["hash"]);
    wait_until(|| importer.available() == capacity && pending.status().retained_requests == 0)
        .await;
    let exit = running.shutdown().await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(http.status().joined && pending.status().joined);
}

#[tokio::test]
async fn pending_http_timeout_keeps_accepted_work_owned_and_populates_the_cache() {
    use super::http::get;
    let lab = Lab::new();
    let p = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let clock = ControlledClock::new();
    let mut s = settings(&lab, clock.clone());
    let node::Settings::OnDemand(config) = &mut s else {
        unreachable!()
    };
    config.config.pending_key.requests = 1;
    let mut running = p.launch(s, || Ok(Default::default())).unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let http = running
        .listen_http(ae_node::http::Config {
            listen: ([127, 0, 0, 1], 0).into(),
            timeout: Duration::from_millis(100),
            ..Default::default()
        })
        .await
        .unwrap();
    let address = http.status().address;
    let pending = h.pending_key().unwrap();
    let (entered, release) = clock.pause();
    let first = tokio::spawn(get(address, "/v3/key-blocks/pending"));
    within(tokio::task::spawn_blocking(move || {
        entered.recv_timeout(Duration::from_secs(10)).unwrap()
    }))
    .await
    .unwrap();
    assert_eq!(within(first).await.unwrap().0, 500);
    assert_eq!(
        (pending.status().accepted, pending.status().completed),
        (1, 0)
    );
    assert_eq!(get(address, "/v2/key-blocks/pending").await.0, 503);
    assert_eq!(pending.status().accepted, 1);
    release.send(()).unwrap();
    wait_until(|| pending.status().completed == 1 && pending.status().retained_requests == 0).await;
    let reads = clock.reads.load(Ordering::Relaxed);
    assert_eq!(get(address, "/v3/key-blocks/pending").await.0, 200);
    assert_eq!(clock.reads.load(Ordering::Relaxed), reads);
    assert_eq!(pending.status().aborted, 0);
    let exit = running.shutdown().await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(pending.status().joined && http.status().joined);
}

#[tokio::test]
async fn pending_candidate_cache_admits_real_headers_and_survives_worker_replacement() {
    let lab = Lab::new();
    let p = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let db = p.configured().chain().database().clone();
    let snapshots = p.pow().unwrap().snapshots().clone();
    let initial = db.view().unwrap().status();
    let clock = ControlledClock::new();
    let running = p
        .launch(settings(&lab, clock.clone()), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let pending = h.pending_key().unwrap();
    let workers = h.pow().unwrap().workers().unwrap();
    let keys = workers.bindings().keys.unwrap().current().unwrap();
    let candidate = keys.candidate_public_key().unwrap().await.unwrap();
    let reads = clock.reads.load(Ordering::Relaxed);
    let first = query(&pending).await;
    let header = first.result.as_ref().unwrap().clone();
    let mut orphan = header.as_ref().clone();
    orphan.common.previous_hash = [99; 32];
    assert!(
        h.importer()
            .unwrap()
            .pending_header_query()
            .unwrap()
            .submit(Arc::new(orphan))
            .unwrap()
            .wait()
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(clock.reads.load(Ordering::Relaxed), reads + 2);
    assert_eq!(header.common.previous_hash, initial.tip.unwrap().0);
    assert_eq!(header.miner, candidate.0);
    assert_eq!(header.beneficiary, [42; 32]);
    assert_eq!(header.seal, [0; 42]);
    assert_eq!(header.nonce, 0);
    assert_eq!(snapshots.live_snapshots(), 0);
    assert_eq!(db.view().unwrap().status(), initial);
    let second = query(&pending).await;
    assert!(Arc::ptr_eq(&header, second.result.as_ref().unwrap()));
    assert_eq!(clock.reads.load(Ordering::Relaxed), reads + 2);
    assert!(matches!(pending.get(), Err(service::Error::Busy)));
    drop((first, second));
    keys.promote_candidate(candidate).unwrap().await.unwrap();
    assert!(Arc::ptr_eq(
        &header,
        query(&pending).await.result.as_ref().unwrap()
    ));
    let starts = h.pow().unwrap().status().production_starts;
    workers
        .restart(workers::Child::Keys, workers.status().keys_generation)
        .unwrap()
        .wait()
        .await
        .unwrap();
    wait_until(|| h.status().ready && h.pow().unwrap().status().production_starts > starts).await;
    assert!(pending.same_service(&h.pending_key().unwrap()));
    let current = workers.bindings().keys.unwrap().current().unwrap();
    assert!(!current.same_worker(&keys));
    let submitted = current.info().submitted;
    assert!(Arc::ptr_eq(
        &header,
        query(&pending).await.result.as_ref().unwrap()
    ));
    assert_eq!(current.info().submitted, submitted);
    assert_eq!(clock.reads.load(Ordering::Relaxed), reads + 2);
    let importer = h.importer().unwrap();
    importer
        .import_created_block(
            ae_chain::block::Block::Key(header.as_ref().clone())
                .encode()
                .unwrap(),
            header.time + 60_000,
        )
        .unwrap()
        .wait()
        .await
        .unwrap();
    let next = query(&pending).await;
    let next_header = next.result.as_ref().unwrap().clone();
    let parent = db.view().unwrap().status().tip.unwrap();
    assert_eq!(next_header.common.previous_hash, parent.0);
    assert_eq!(next_header.miner, current.info().keys.candidate.0);
    assert!(!Arc::ptr_eq(&header, &next_header));
    assert_eq!(snapshots.live_snapshots(), 0);
    drop(next);
    importer
        .import_created_block(
            ae_chain::block::Block::Key(next_header.as_ref().clone())
                .encode()
                .unwrap(),
            next_header.time + 60_000,
        )
        .unwrap()
        .wait()
        .await
        .unwrap();
    clock.invalid.store(true, Ordering::Relaxed);
    assert!(matches!(
        query(&pending).await.result,
        Err(service::Error::Preparation(_))
    ));
    assert_eq!(pending.status().cached_parent, Some(parent));
    db.rollback(
        ae_chain::repository::rollback::Target::Height(1),
        Default::default(),
    )
    .unwrap();
    let reads = clock.reads.load(Ordering::Relaxed);
    assert!(Arc::ptr_eq(
        &next_header,
        query(&pending).await.result.as_ref().unwrap()
    ));
    assert_eq!(clock.reads.load(Ordering::Relaxed), reads);
    assert_eq!(snapshots.live_snapshots(), 0);
    let exit = running.shutdown().await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    assert!(pending.status().joined);
    assert!(matches!(pending.get(), Err(service::Error::Closed)));
}

#[tokio::test]
async fn pending_candidate_shutdown_drains_dropped_requests_and_retains_response_capacity() {
    let lab = Lab::new();
    let p = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let snapshots = p.pow().unwrap().snapshots().clone();
    let clock = ControlledClock::new();
    let running = p
        .launch(settings(&lab, clock.clone()), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let pending = h.pending_key().unwrap();
    let (entered, release) = clock.pause();
    let first = pending.get().unwrap();
    within(tokio::task::spawn_blocking(move || {
        entered.recv_timeout(Duration::from_secs(10)).unwrap()
    }))
    .await
    .unwrap();
    drop(pending.get().unwrap());
    assert!(matches!(pending.get(), Err(service::Error::Busy)));
    let finish = tokio::spawn(running.shutdown());
    wait_until(|| !pending.status().accepting).await;
    assert!(!finish.is_finished());
    assert!(
        h.pow()
            .unwrap()
            .workers()
            .unwrap()
            .status()
            .keys
            .unwrap()
            .running
    );
    release.send(()).unwrap();
    let receipt = within(first).await.unwrap();
    assert!(receipt.result.is_ok());
    let exit = within(finish).await.unwrap().unwrap();
    assert!(exit.status().joined && exit.status().failure.is_none());
    let status = pending.status();
    assert!(status.joined && !status.running && !status.accepting);
    assert_eq!(
        (
            status.accepted,
            status.completed,
            status.aborted,
            status.pending,
            status.retained_requests
        ),
        (2, 2, 0, 0, 1)
    );
    assert_eq!(snapshots.live_snapshots(), 0);
    drop(receipt);
    assert_eq!(pending.status().retained_requests, 0);
}

#[tokio::test]
async fn pending_candidate_partial_startup_and_dropped_root_join_the_service() {
    for fail_startup in [false, true] {
        let lab = Lab::new();
        let p = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
        let mut settings = settings(&lab, ControlledClock::new());
        if fail_startup {
            let node::Settings::OnDemand(s) = &mut settings else {
                unreachable!()
            };
            s.config.production.controller.result_bytes = 0;
        }
        let running = p.launch(settings, || Ok(Default::default())).unwrap();
        let mut h = running.handle();
        if !fail_startup {
            ready(&mut h).await;
            drop(running);
        } else {
            wait_until(|| !h.status().running).await;
            let exit = running.shutdown().await.unwrap();
            assert!(exit.status().failure.is_some());
        }
        wait_until(|| h.status().joined).await;
        let pending = h.pending_key().expect("service started before production");
        assert!(pending.status().joined);
        assert!(matches!(pending.get(), Err(service::Error::Closed)));
    }
}

#[tokio::test]
async fn pending_candidate_panicked_builder_propagates_to_root() {
    let lab = Lab::new();
    let p = prepare(lab.services("on_demand", json!({"dev_mode":true}), Default::default()));
    let snapshots = p.pow().unwrap().snapshots().clone();
    let clock = ControlledClock::new();
    let running = p
        .launch(settings(&lab, clock.clone()), || Ok(Default::default()))
        .unwrap();
    let mut h = running.handle();
    ready(&mut h).await;
    let pending = h.pending_key().unwrap();
    clock.panic.store(true, Ordering::Relaxed);
    assert_eq!(query(&pending).await.result, Err(service::Error::Panicked));
    wait_until(|| h.status().joined).await;
    assert!(matches!(
        h.pow().unwrap().status().failure,
        Some(root::Error::PendingKey(service::Error::Panicked))
    ));
    let exit = running.shutdown().await.unwrap();
    assert!(exit.status().joined && exit.status().failure.is_some());
    assert!(pending.status().joined);
    assert_eq!(snapshots.live_snapshots(), 0);
}

#[tokio::test]
async fn pending_candidate_stopped_cuckoo_uses_configured_beneficiary_capability() {
    for configured in [false, true] {
        let lab = Lab::new();
        let p = prepare(lab.services("pow_cuckoo", json!({"dev_mode":true}), Default::default()));
        let mut s = cuckoo(&lab);
        let node::Settings::Cuckoo(settings) = &mut s else {
            unreachable!()
        };
        assert!(!settings.config.production.producer.enabled);
        if configured {
            let mut mining =
                ae_sync::chain::pow::producer::mining::Config::new(AccountPublicKey([42; 32]));
            mining.release_info = 0x01020304;
            settings.config.production.mining = Some(mining);
        }
        let running = p.launch(s, || Ok(Default::default())).unwrap();
        let mut h = running.handle();
        ready(&mut h).await;
        let pending = h.pending_key().unwrap();
        let reply = query(&pending).await;
        if configured {
            let header = reply.result.as_ref().unwrap();
            assert_eq!(header.beneficiary, [42; 32]);
            assert_eq!(header.info, Some([1, 2, 3, 4]));
            assert_eq!(header.seal, [0; 42]);
            assert!(Arc::ptr_eq(
                header,
                query(&pending).await.result.as_ref().unwrap()
            ));
        } else {
            assert_eq!(reply.result, Err(service::Error::BeneficiaryNotConfigured));
        }
        let exit = running.shutdown().await.unwrap();
        assert!(exit.status().joined && exit.status().failure.is_none());
        assert!(pending.status().joined);
    }
}
