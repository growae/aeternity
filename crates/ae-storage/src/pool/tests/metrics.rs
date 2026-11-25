use super::*;
use ae_chain::instrumentation::{self, Counter};

#[derive(Clone, Default)]
pub(in crate::pool) struct Events(Arc<std::sync::Mutex<Vec<instrumentation::Event>>>);
impl instrumentation::Sink for Events {
    fn event(&self, event: instrumentation::Event) {
        self.0.lock().unwrap().push(event);
    }
    fn start(&self, _: instrumentation::Stage) -> Option<Box<dyn instrumentation::Span>> {
        None
    }
}
impl Events {
    pub(in crate::pool) fn bind(&self, db: &Database) -> instrumentation::Binding {
        db.instrumentation().bind(Arc::new(self.clone())).unwrap()
    }
    pub(in crate::pool) fn take(&self) -> Vec<(Counter, u64)> {
        std::mem::take(&mut *self.0.lock().unwrap())
            .into_iter()
            .filter_map(|e| match e {
                instrumentation::Event::Counter(c, n) => Some((c, n)),
                _ => None,
            })
            .collect()
    }
}

#[test]
fn real_pool_observations_distinguish_admission_from_insert_and_recovery() {
    let temp = Temp::new();
    let db = database(&temp);
    let events = Events::default();
    let _binding = events.bind(&db);
    let mut cfg = config();
    cfg.limits.entries = 0;
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), cfg).unwrap();
    let good = tx();
    let bad = Arc::new(Signed::new(good.transaction().clone(), vec![]));
    assert_eq!(
        service
            .push(bad.clone(), Event::Received, true)
            .unwrap()
            .outcome,
        PushOutcome::Cached
    );
    assert!(events.take().is_empty());
    assert!(matches!(
        service.push(bad, Event::Created, true),
        Err(Failure::Policy(PolicyError::Signature(_)))
    ));
    assert_eq!(events.take(), vec![(Counter::PoolPushError, 1)]);
    assert_eq!(
        service.push(good.clone(), Event::Created, false),
        Err(Failure::Policy(PolicyError::ResourceLimit))
    );
    assert_eq!(events.take(), vec![(Counter::PoolPush, 1)]);
    assert_eq!(
        db.pool_view().unwrap().location(good.hash()).unwrap(),
        Location::Unknown
    );
    service.recover(config()).unwrap();
    service.push(good.clone(), Event::Created, false).unwrap();
    assert_eq!(events.take(), vec![(Counter::PoolPush, 1)]);
    assert_eq!(
        service.push(good.clone(), Event::Created, false),
        Err(Failure::Policy(PolicyError::AlreadyKnown))
    );
    assert_eq!(events.take(), vec![(Counter::PoolPushIgnore, 1)]);
    assert!(matches!(
        service.force_push(good.clone()).unwrap().outcome,
        PushOutcome::Accepted {
            inserted: false,
            ..
        }
    ));
    assert_eq!(events.take(), vec![(Counter::PoolPush, 1)]);
    service.delete(good.hash()).unwrap();
    assert_eq!(
        service.push(good.clone(), Event::Received, false),
        Err(Failure::Policy(PolicyError::AlreadyKnown))
    );
    assert_eq!(events.take(), vec![(Counter::PoolPushIgnore, 1)]);
    let mut cfg = config();
    cfg.policy.allow_reentry = true;
    service.reconfigure(cfg.policy, cfg.failures).unwrap();
    service.push(good, Event::Received, false).unwrap();
    assert_eq!(events.take(), vec![(Counter::PoolPush, 1)]);
    drop(service);
    let service = Service::start(db, NetworkProfile::mainnet(), config()).unwrap();
    assert_eq!(service.inspection().active, 1);
    assert!(events.take().is_empty());
}

#[test]
fn forced_pool_pushes_match_original_updates_without_admission_validation() {
    let data: J = serde_json::from_str(include_str!(
        "../../../../ae-mempool/tests/fixtures/erlang_metrics_pool.json"
    ))
    .unwrap();
    let mut count = 0;
    for row in data["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["label"] == "forced")
    {
        let temp = Temp::new();
        let db = database(&temp);
        let events = Events::default();
        let _binding = events.bind(&db);
        let mut service = Service::start(db, NetworkProfile::mainnet(), config()).unwrap();
        let tx =
            Arc::new(Signed::decode(&hex::decode(row["tx"].as_str().unwrap()).unwrap()).unwrap());
        service.force_push(tx).unwrap();
        assert_eq!(events.take(), vec![(Counter::PoolPush, 1)]);
        assert_eq!(
            row["events"],
            serde_json::json!([["update", "push", 1], ["server", 20]])
        );
        count += 1;
    }
    assert_eq!(count, 3);
}

#[test]
fn acknowledged_pool_cleanup_matches_original_batch_zero_and_per_origin_updates() {
    use ae_protocol::{Activation, Protocol};
    use ae_state::accounts::Account;
    let data: J = serde_json::from_str(include_str!(
        "../../../../ae-mempool/tests/fixtures/erlang_metrics_pool.json"
    ))
    .unwrap();
    let rows = data["gc"].as_array().unwrap();
    assert_eq!(rows.len(), 6);
    for row in rows {
        let label = row["label"].as_str().unwrap();
        let temp = Temp::new();
        let network = NetworkProfile::custom(
            b"ae_metrics_gc".to_vec(),
            vec![Activation {
                height: 0,
                protocol: Protocol::Roma,
            }],
        )
        .unwrap();
        let mut trees = Trees {
            accounts: Default::default(),
            contracts: Default::default(),
            channels: Default::default(),
            calls: Default::default(),
            names: Default::default(),
            oracles: Default::default(),
        };
        for a in row["accounts"].as_array().unwrap() {
            let key = ae_types::AccountPublicKey(
                hex::decode(a[0].as_str().unwrap())
                    .unwrap()
                    .try_into()
                    .unwrap(),
            );
            let mut account =
                Account::deserialize(key, &hex::decode(a[1].as_str().unwrap()).unwrap()).unwrap();
            // The native capture calls remove_txs_from_dbs after selection.
            // Set the on-chain nonce so the real Rust origin selector reaches
            // those same two removals before comparing the emitted updates.
            account.set_nonce(2.into());
            trees.accounts.enter(&account).unwrap();
        }
        let Header::Key(mut header) = genesis::header(Network::Mainnet).unwrap() else {
            panic!()
        };
        header.common.state_hash = trees.roots().hash().0;
        let id = Identity::new(
            &network,
            Header::Key(header.clone()).hash().unwrap(),
            blake2b_256(b"metrics-gc"),
        );
        let db = Arc::new(Database::open(&temp.0, id).unwrap());
        db.commit(
            ledger::bootstrap(
                &db.view().unwrap(),
                &network,
                &header,
                &trees,
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let txs: Vec<_> = row["txs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| Arc::new(Signed::decode(&hex::decode(v.as_str().unwrap()).unwrap()).unwrap()))
            .collect();
        let guarded = match label {
            "guarded" | "origin_guarded" => 1,
            "all_guarded" => 2,
            _ => 0,
        };
        for tx in txs.iter().take(guarded) {
            membership_change(&db, tx, true);
        }
        let events = Events::default();
        let _binding = events.bind(&db);
        let mut service = Service::start(db.clone(), network, config()).unwrap();
        for tx in &txs {
            service.force_push(tx.clone()).unwrap();
        }
        assert_eq!(events.take(), vec![(Counter::PoolPush, 1); 2]);
        let removed = if label.starts_with("origin") {
            service
                .note_origin(txs[0].transaction().view().origin().ok(), 2.into())
                .unwrap();
            service.collect(true, 0).unwrap()
        } else {
            service
                .collect_at(if label == "empty" { 0 } else { 30 }, false, 100)
                .unwrap()
        };
        let got: Vec<_> = events
            .take()
            .into_iter()
            .map(|(c, n)| {
                serde_json::json!([
                    "update",
                    match c {
                        Counter::PoolGarbageCollected => "gced",
                        Counter::PoolOriginGarbageCollected => "origin_gced",
                        _ => panic!("unexpected update"),
                    },
                    n
                ])
            })
            .collect();
        assert_eq!(serde_json::json!(got), row["events"], "{label}");
        assert_eq!(
            removed.len(),
            if label == "empty" { 0 } else { 2 - guarded },
            "{label}"
        );
        for hash in removed {
            assert_eq!(
                db.pool_view().unwrap().location(hash).unwrap(),
                Location::Deleted
            );
        }
    }
}
