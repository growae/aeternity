use super::*;
use ae_chain::{
    block::{Block, MicroBlock},
    repository::ledger,
};
use ae_codec::{
    api::{self, ApiType},
    headers::{Header, HeaderMode},
};
use ae_mempool::pool::{Error as PolicyError, Event, PushOutcome};
use ae_protocol::NetworkProfile;
use ae_state::{
    Trees,
    genesis::{self, Network},
};
use serde_json::Value as J;
use std::path::PathBuf;
mod lifecycle;
mod recovery;
pub(super) struct Temp(pub(super) PathBuf);
impl Temp {
    pub(super) fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "ae-durable-pool-{}-{}",
            std::process::id(),
            NEXT_SOURCE.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn identity() -> Identity {
    Identity::new(
        &NetworkProfile::mainnet(),
        genesis::header(Network::Mainnet).unwrap().hash().unwrap(),
        blake2b_256(b"durable-pool"),
    )
}
pub(super) fn database(t: &Temp) -> Arc<Database> {
    let db = Arc::new(Database::open(&t.0, identity()).unwrap());
    if db.view().unwrap().status().tip.is_none() {
        let Header::Key(header) = genesis::header(Network::Mainnet).unwrap() else {
            panic!()
        };
        let trees = Trees {
            accounts: genesis::state(Network::Mainnet).unwrap().0,
            contracts: Default::default(),
            channels: Default::default(),
            calls: Default::default(),
            names: Default::default(),
            oracles: Default::default(),
        };
        let plan = ledger::bootstrap(
            &db.view().unwrap(),
            &NetworkProfile::mainnet(),
            &header,
            &trees,
            Default::default(),
        )
        .unwrap();
        db.commit(plan).unwrap();
    }
    db
}
pub(super) fn config() -> Config {
    Config {
        policy: ae_mempool::pool::Config {
            minimum_miner_gas_price: 1.into(),
            tx_ttl: 20,
            future_nonce_ttl: 3,
            ..Default::default()
        },
        ..Default::default()
    }
}
fn blocks() -> Vec<Block> {
    let data: J =
        serde_json::from_str(include_str!("../../tests/fixtures/public_bootstrap.json")).unwrap();
    let mut out = vec![];
    for g in data["generations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|g| g["network"] == "ae_mainnet")
    {
        let Header::Key(header) = Header::decode(
            &hex::decode(g["hex"].as_str().unwrap()).unwrap(),
            HeaderMode::ProofOfWork,
        )
        .unwrap() else {
            panic!()
        };
        out.push(Block::Key(header));
        for h in g["micro_blocks"].as_array().unwrap() {
            let m = data["micros"]
                .as_array()
                .unwrap()
                .iter()
                .find(|m| m["hash"] == *h)
                .unwrap();
            let Header::Micro(header) = Header::decode(
                &hex::decode(m["hex"].as_str().unwrap()).unwrap(),
                HeaderMode::ProofOfWork,
            )
            .unwrap() else {
                panic!()
            };
            let transactions = m["transactions"]["transactions"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| {
                    Signed::decode(
                        &api::decode_as(ApiType::Transaction, t["encoded_tx"].as_str().unwrap())
                            .unwrap(),
                    )
                    .unwrap()
                })
                .collect();
            out.push(Block::Micro(MicroBlock {
                header,
                transactions,
                proof: None,
            }));
        }
    }
    out
}
pub(super) fn tx() -> Arc<Signed> {
    let Block::Micro(m) = blocks().remove(1) else {
        panic!()
    };
    Arc::new(m.transactions[0].clone())
}
fn prepare(db: &Database, block: &Block) -> Update {
    let net = NetworkProfile::mainnet();
    let rules = ae_chain::key::Rules::new(
        ae_chain::admission::PowRules {
            now_ms: u64::MAX - 600_000,
            micro_cycle_ms: 3000,
            configured_gas_limit: 6_000_000.into(),
            body_limits: Default::default(),
        },
        identity().genesis,
    );
    ledger::prepare(
        &db.view().unwrap(),
        block,
        &ae_execution::transactions::Config::primitives(&net),
        &rules,
        Default::default(),
    )
    .unwrap()
}
#[test]
fn membership_records_and_restart_restore_original_transient_lifecycle() {
    let temp = Temp::new();
    let db = database(&temp);
    let t = tx();
    let h = t.hash();
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    assert!(matches!(
        Service::start(db.clone(), NetworkProfile::mainnet(), config()),
        Err(Failure::AlreadyRunning)
    ));
    let before = db.pool_view().unwrap();
    assert_eq!(before.location(h).unwrap(), Location::Unknown);
    let result = service.push(t.clone(), Event::Created, false).unwrap();
    assert!(matches!(
        result.outcome,
        PushOutcome::Accepted { inserted: true, .. }
    ));
    assert_eq!(db.pool_view().unwrap().location(h).unwrap(), Location::Pool);
    assert_eq!(before.location(h).unwrap(), Location::Unknown);
    drop(before);
    assert_eq!(
        service.push(t.clone(), Event::Created, false),
        Err(Failure::Policy(PolicyError::AlreadyKnown))
    );
    let candidate = service
        .candidate(1000000, &BTreeSet::new(), Default::default(), || 0)
        .unwrap();
    assert_eq!(candidate.transactions, vec![t.clone()]);
    service
        .record_failure(h, "insufficient_funds", false)
        .unwrap();
    assert_eq!(service.inspect(h).unwrap().failures, 1);
    assert!(service.inspect(h).unwrap().visited);
    service.sync_target(100).unwrap();
    assert_eq!(service.refresh().unwrap().gc_height, 100);
    drop(service);
    drop(db);
    let db = database(&temp);
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    assert_eq!(
        service.inspect(h).unwrap(),
        ae_mempool::pool::Inspection {
            failures: 0,
            visited: false,
            expires: 20
        }
    );
    assert_eq!(service.refresh().unwrap().gc_height, 0);
    service.delete(h).unwrap();
    assert_eq!(
        db.pool_view().unwrap().location(h).unwrap(),
        Location::Deleted
    );
    assert_eq!(
        db.pool_view().unwrap().transaction(h).unwrap(),
        Some((*t).clone())
    );
    assert_eq!(
        service.push(t.clone(), Event::Received, false),
        Err(Failure::Policy(PolicyError::AlreadyKnown))
    );
    let mut c = config();
    c.policy.allow_reentry = true;
    service.reconfigure(c.policy, c.failures).unwrap();
    service.push(t.clone(), Event::Received, false).unwrap();
    for b in blocks().iter().take(2) {
        db.commit(prepare(&db, b)).unwrap();
    }
    assert!(matches!(
        db.pool_view().unwrap().location(h).unwrap(),
        Location::Block(_)
    ));
    assert!(db.pool_view().unwrap().members(100).unwrap().is_empty());
    assert_eq!(service.refresh().unwrap().active, 0);
    assert_eq!(
        service.inspect(h),
        Err(Failure::Policy(PolicyError::AlreadyAccepted))
    );
    service.force_push(t.clone()).unwrap();
    assert_eq!(service.peek(100, None, None).unwrap(), vec![t]);
    assert!(db.pool_view().unwrap().members(100).unwrap().is_empty());
    drop(service);
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    assert!(service.peek(100, None, None).unwrap().is_empty());
}
#[test]
fn pool_batches_reject_stale_views_and_leave_every_failed_prefix_unpublished() {
    let temp = Temp::new();
    let db = database(&temp);
    let t = tx();
    let h = t.hash();
    let stored = db.pool_view().unwrap();
    let mut writes = repo::Writes::new();
    stored.retain(&t, &mut writes).unwrap();
    let before: Vec<_> = db
        .db
        .iterator(IteratorMode::Start)
        .map(Result::unwrap)
        .collect();
    for failure in 0..=writes.len() + 1 {
        let mut n = 0;
        let result = db.commit_pool(&stored, writes.clone(), usize::MAX, || {
            let now = n;
            n += 1;
            if now == failure {
                Err(Error::ResourceLimit)
            } else {
                Ok(())
            }
        });
        assert_eq!(result, Err(Error::ResourceLimit), "prefix {failure}");
        assert_eq!(
            db.db
                .iterator(IteratorMode::Start)
                .map(Result::unwrap)
                .collect::<Vec<_>>(),
            before
        );
    }
    assert_eq!(
        db.commit_pool(&stored, writes.clone(), 0, || Ok(())),
        Err(Error::ResourceLimit)
    );
    db.commit_pool(&stored, writes.clone(), usize::MAX, || Ok(()))
        .unwrap();
    assert_eq!(stored.location(h).unwrap(), Location::Unknown);
    assert_eq!(
        db.commit_pool(&stored, writes, usize::MAX, || Ok(())),
        Err(Error::Conflict)
    );
    let current = db.pool_view().unwrap();
    db.commit(prepare(&db, &blocks()[0])).unwrap();
    assert_eq!(
        db.commit_pool(&current, Default::default(), usize::MAX, || Ok(())),
        Err(Error::Conflict)
    );
}
fn fixture_database(temp: &Temp, row: &J) -> (Arc<Database>, NetworkProfile) {
    use ae_protocol::{Activation, Protocol};
    use ae_state::accounts::Account;
    use ae_types::AccountPublicKey;
    let network = NetworkProfile::custom(
        b"ae_pool_storage".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
        }],
    )
    .unwrap();
    let Header::Key(header) = Header::decode(
        &hex::decode(row["header"].as_str().unwrap()).unwrap(),
        HeaderMode::ProofOfWork,
    )
    .unwrap() else {
        panic!()
    };
    let id = Identity::new(
        &network,
        Header::Key(header.clone()).hash().unwrap(),
        blake2b_256(b"pool-mnesia-reference"),
    );
    let db = Arc::new(Database::open(&temp.0, id).unwrap());
    let mut trees = Trees {
        accounts: Default::default(),
        contracts: Default::default(),
        channels: Default::default(),
        calls: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    for a in row["accounts"].as_array().unwrap() {
        let key = AccountPublicKey(
            hex::decode(a[0].as_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap(),
        );
        let account =
            Account::deserialize(key, &hex::decode(a[1].as_str().unwrap()).unwrap()).unwrap();
        trees.accounts.enter(&account).unwrap();
    }
    let plan = ledger::bootstrap(
        &db.view().unwrap(),
        &network,
        &header,
        &trees,
        Default::default(),
    )
    .unwrap();
    db.commit(plan).unwrap();
    (db, network)
}
fn snapshot(service: &mut Service, db: &Database, txs: &[Arc<Signed>]) -> J {
    use serde_json::json;
    let all = service.peek(10000, None, None).unwrap();
    let mut active = vec![];
    let mut visited = vec![];
    let mut expiry = std::collections::BTreeMap::new();
    for tx in &all {
        let info = service.inspect(tx.hash()).unwrap();
        let r = json!({"hash":hex::encode(tx.hash().0),"failures":info.failures});
        if info.visited {
            visited.push(r);
        } else {
            active.push(r);
        }
        expiry.insert(hex::encode(tx.hash().0), info.expires);
    }
    let origin = txs[0].transaction().view().origin().unwrap();
    let hash_list = |txs: Vec<Arc<Signed>>| {
        txs.iter()
            .map(|t| hex::encode(t.hash().0))
            .collect::<Vec<_>>()
    };
    let memory = json!({"peek":hash_list(all),"account":hash_list(service.peek(10000,Some(origin),None).unwrap()),"active":active,"visited":visited,"expiry":expiry.into_iter().map(|(h,t)|json!([h,t])).collect::<Vec<_>>(),"maximum":service.max_nonce(origin).unwrap().map(|n|n.to_u64().unwrap())});
    let stored = db.pool_view().unwrap();
    let hashes: BTreeSet<_> = txs.iter().map(|t| t.hash()).collect();
    let records: Vec<_> = hashes
        .into_iter()
        .map(|h| {
            let location = match stored.location(h).unwrap() {
                Location::Unknown => json!("not_found"),
                Location::Deleted => json!("none"),
                Location::Pool => json!("mempool"),
                Location::Block(b) => json!(["block", hex::encode(b.0)]),
            };
            let record = stored
                .transaction(h)
                .unwrap()
                .map(|t| hex::encode(ae_tx::storage::encode(&t, Default::default()).unwrap()));
            json!({"hash":hex::encode(h.0),"location":location,"record":record})
        })
        .collect();
    json!({"memory":memory,"members":stored.members(10000).unwrap().iter().map(|h|hex::encode(h.0)).collect::<Vec<_>>(),"records":records})
}
fn membership_change(db: &Database, tx: &Signed, confirmed: bool) {
    // Isolate the original DB operation, without claiming these synthetic
    // location changes have passed block consensus admission.
    let h = tx.hash();
    let mut block = [0; 32];
    block[31] = 99;
    let block = Hash256(block);
    let change = repo::Change {
        revision: 1,
        inserted: Some(block),
        removed: vec![],
        discarded: vec![],
        old_tip: None,
        new_tip: block,
        ancestor: None,
        detached: vec![],
        attached: vec![],
        requeue: if confirmed { vec![] } else { vec![h] },
        confirm: if confirmed { vec![(h, block)] } else { vec![] },
    };
    let mut writes = repo::Writes::new();
    writes.insert(
        repo::key(repo::TRANSACTIONS, h.0),
        Some(ae_tx::storage::encode(tx, Default::default()).unwrap()),
    );
    writes.insert(
        repo::key(repo::LOCATIONS, h.0),
        if confirmed {
            Some(block.0.to_vec())
        } else {
            None
        },
    );
    let _guard = db.writer.lock().unwrap();
    let pool = db.pool_chain_writes(&change, &writes).unwrap();
    let mut batch = WriteBatch::default();
    for (key, value) in writes.into_iter().chain(pool) {
        match value {
            Some(value) => batch.put(key, value),
            None => batch.delete(key),
        }
    }
    db.db.write_opt(batch, &write_options()).unwrap();
}
fn outcome<T>(result: Result<T, Failure>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(Failure::Policy(PolicyError::NonceTooLow)) => "nonce_too_low",
        Err(Failure::Policy(PolicyError::GeneralizedAccount)) => {
            "generalized_account_cant_sign_non_meta_tx"
        }
        Err(Failure::Policy(PolicyError::AlreadyKnown)) => "already_known",
        Err(Failure::Policy(PolicyError::NotFound)) => "not_found",
        Err(Failure::Policy(PolicyError::AlreadyAccepted)) => "already_accepted",
        e => panic!("unexpected outcome {:?}", e.err()),
    }
}
#[test]
fn original_mnesia_membership_deletion_and_restart_match_at_every_operation() {
    let data: J = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_durable_pool.json"
    ))
    .unwrap();
    for (i, row) in data["rows"].as_array().unwrap().iter().enumerate() {
        let temp = Temp::new();
        let (mut db, net) = fixture_database(&temp, row);
        let id = db.identity.clone();
        let mut cfg = config();
        let mut service = Service::start(db.clone(), net.clone(), cfg.clone()).unwrap();
        let txs: Vec<_> = row["txs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                Arc::new(
                    ae_tx::storage::decode(
                        &hex::decode(r.as_str().unwrap()).unwrap(),
                        Default::default(),
                    )
                    .unwrap(),
                )
            })
            .collect();
        for (j, step) in row["steps"].as_array().unwrap().iter().enumerate() {
            let tx = step["index"].as_u64().map(|n| txs[n as usize - 1].clone());
            let result = match step["op"].as_str().unwrap() {
                "push" => Some(outcome(service.push(tx.unwrap(), Event::Received, false))),
                "force" => Some(outcome(service.force_push(tx.unwrap()))),
                "delete" => Some(outcome(service.delete(tx.unwrap().hash()))),
                "candidate" => {
                    let result = service
                        .candidate(1000000, &BTreeSet::new(), Default::default(), || 0)
                        .unwrap();
                    assert_eq!(
                        serde_json::json!(
                            result
                                .transactions
                                .iter()
                                .map(|t| hex::encode(t.hash().0))
                                .collect::<Vec<_>>()
                        ),
                        step["hashes"],
                        "candidate {i}/{j}"
                    );
                    None
                }
                "failure" => {
                    service
                        .record_failure(tx.unwrap().hash(), "insufficient_funds", false)
                        .unwrap();
                    None
                }
                "restart" => {
                    drop(service);
                    drop(db);
                    db = Arc::new(Database::open(&temp.0, id.clone()).unwrap());
                    service = Service::start(db.clone(), net.clone(), cfg.clone()).unwrap();
                    None
                }
                "reentry" => {
                    cfg.policy.allow_reentry = true;
                    service
                        .reconfigure(cfg.policy.clone(), cfg.failures.clone())
                        .unwrap();
                    None
                }
                "gc" => {
                    for tx in service.peek(10000, None, None).unwrap() {
                        if service.inspect(tx.hash()).unwrap().expires
                            <= step["height"].as_u64().unwrap()
                        {
                            service.delete(tx.hash()).unwrap();
                        }
                    }
                    None
                }
                name @ ("confirm" | "requeue") => {
                    membership_change(&db, &tx.unwrap(), name == "confirm");
                    service.recover(cfg.clone()).unwrap();
                    None
                }
                name => panic!("{name}"),
            };
            if let Some(result) = result {
                assert_eq!(result, step["result"], "result {i}/{j}");
            }
            assert_eq!(
                snapshot(&mut service, &db, &txs),
                step["state"],
                "state {i}/{j} {}",
                step["op"]
            );
        }
    }
}
