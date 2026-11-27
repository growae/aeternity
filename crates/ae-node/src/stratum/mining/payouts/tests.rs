use super::super::tests::fixture;
use super::*;
use ae_chain::repository::{Identity, ledger};
use ae_codec::headers::Header;
use ae_crypto::Ed25519SecretKey;
use ae_protocol::{Activation, NetworkProfile, Protocol};
use ae_state::{
    Trees,
    accounts::Account,
    genesis::{self, Network},
};
use ae_stratum::{accounting as core, proof::Number};
use ae_types::{ContractPublicKey, Hash256};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(1);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "ae-payout-actor-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn until(mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < end, "bounded payout runtime");
        thread::sleep(Duration::from_millis(1));
    }
}
const GROSS: u64 = 1_000_000_000_000_000_000;
// The historical recipe declares more gas than its estimated run-fee reserve.
// A successful execution fixture must cover that declared maximum as well.
const BALANCE: u64 = GROSS + 1_000_000_000_000_000;
fn key(n: u8) -> [u8; 32] {
    let mut k = [0; 32];
    k[31] = n;
    k
}
fn secret() -> Ed25519SecretKey {
    Ed25519SecretKey::from_seed(&key(7))
}
fn config() -> durable::Config {
    durable::Config {
        caller: secret().public_key(),
        contract: execution::actor_contract(),
        delay: 180,
    }
}
fn network() -> NetworkProfile {
    NetworkProfile::custom(
        b"ae_payout_local".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::Salus,
        }],
    )
    .unwrap()
}
fn trees() -> Trees {
    let mut t = execution::actor_trees();
    t.accounts
        .enter(&Account::new(config().caller, BALANCE.into()))
        .unwrap();
    t
}
fn database(t: &Temp) -> Arc<Database> {
    database_with_trees(t, &trees())
}
fn database_with_trees(t: &Temp, trees: &Trees) -> Arc<Database> {
    let Header::Key(mut h) = genesis::header(Network::Uat).unwrap() else {
        panic!()
    };
    h.common.state_hash = trees.roots().hash().0;
    h.common.version = 8;
    h.info = Some([0; 4]);
    let id = Identity::new(
        &network(),
        Header::Key(h.clone()).hash().unwrap(),
        Hash256::ZERO,
    );
    let db = Arc::new(Database::open(&t.0, id).unwrap());
    if db.view().unwrap().status().tip.is_none() {
        db.commit(
            ledger::bootstrap(
                &db.view().unwrap(),
                &network(),
                &h,
                trees,
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap();
    }
    db
}
fn payment() -> core::Payment {
    ae_stratum::rewards::Payment {
        id: (100, 0),
        total: GROSS.into(),
        relative: [(
            ae_codec::api::encode(ae_codec::api::ApiType::AccountPubkey, &key(1)).into_bytes(),
            Number::Float(1.0f64.to_bits()),
        )]
        .into(),
    }
    .into()
}
fn seed(db: &Arc<Database>, p: core::Payment) {
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    w.commit(w.version().unwrap(), vec![storage::Command::Payment(p)])
        .unwrap();
}
fn pool(db: &Arc<Database>, price: u64) -> txpool::Worker {
    let mut c = ae_storage::pool::Config::default();
    c.policy.minimum_miner_gas_price = price.into();
    txpool::Worker::spawn(
        db.clone(),
        network(),
        c,
        txpool::Settings {
            origins_interval: None,
            ..Default::default()
        },
    )
    .unwrap()
}
fn signer() -> keys::Worker {
    keys::Worker::spawn(
        ae_hyperchain::signing::Keys::new([secret()]),
        Default::default(),
    )
    .unwrap()
}
struct Wall;
impl Clock for Wall {
    fn unix_millis(&self) -> i128 {
        1_700_000_000_000
    }
}
fn snapshots(db: &Arc<Database>) -> ae_storage::snapshots::Owner {
    ae_storage::snapshots::Owner::start(db.clone(), Default::default()).unwrap()
}
struct ExecutionOwner(chain::Importer);
impl std::ops::Deref for ExecutionOwner {
    type Target = chain::Importer;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Drop for ExecutionOwner {
    fn drop(&mut self) {
        self.0.stop();
        runtime().block_on(self.0.join());
    }
}
fn execution_owner(db: &Arc<Database>, network: &NetworkProfile) -> ExecutionOwner {
    ExecutionOwner(
        chain::Importer::new(
            db.clone(),
            fixture::execution(network),
            fixture::rules(db.view().unwrap().identity().genesis),
            Default::default(),
        )
        .unwrap(),
    )
}
struct Actor {
    worker: Worker,
    _execution: ExecutionOwner,
}
impl std::ops::Deref for Actor {
    type Target = Worker;
    fn deref(&self) -> &Self::Target {
        &self.worker
    }
}
impl std::ops::DerefMut for Actor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.worker
    }
}
fn inputs(db: &Arc<Database>, s: &keys::Worker, p: &txpool::Worker) -> Inputs {
    let mut inputs = Inputs::new(config(), s.client(), p.client(), snapshots(db));
    inputs.generation = ae_stratum::payout::Generation::Native;
    inputs
}
fn actor(
    db: &Arc<Database>,
    w: &mut storage::Writer,
    s: &keys::Worker,
    p: &txpool::Worker,
) -> Actor {
    let execution = execution_owner(db, &network());
    let worker = Worker::start(
        w,
        db.clone(),
        Default::default(),
        inputs(db, s, p),
        Arc::new(Wall),
        &execution.stratum_admitter(),
    )
    .unwrap();
    Actor {
        worker,
        _execution: execution,
    }
}
fn stored(db: &Database) -> durable::Info {
    db.stratum(Default::default())
        .unwrap()
        .payouts(Default::default())
        .unwrap()
        .unwrap()
}
fn tick(a: &mut Worker, w: &mut storage::Writer) {
    a.tick(w, true, Instant::now()).unwrap();
}
fn accepted(a: &mut Worker, w: &mut storage::Writer) -> Record {
    until(|| {
        tick(a, w);
        stored(&a.db).pending.as_ref().is_some_and(|r| {
            matches!(
                r.signed.as_ref().map(|s| &s.dispatch),
                Some(Dispatch::Done {
                    outcome: durable::Outcome::Accepted { .. },
                    ..
                })
            )
        })
    });
    stored(&a.db).pending.unwrap()
}
fn pending_sign(a: &mut Worker, w: &mut storage::Writer) {
    a.request_check();
    until(|| {
        tick(a, w);
        matches!(a.pending, Some(Pending::Signature { .. }))
    });
}
fn pending_push(a: &mut Worker, w: &mut storage::Writer) {
    a.request_check();
    until(|| {
        tick(a, w);
        matches!(a.pending, Some(Pending::Submission { .. }))
    });
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
#[test]
fn live_accounting_timer_uses_real_signer_and_pool_without_manual_submission() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = signer();
    let importer = chain::Importer::new(
        db.clone(),
        fixture::execution(&network()),
        fixture::rules(db.view().unwrap().identity().genesis),
        Default::default(),
    )
    .unwrap();
    let mut i = inputs(&db, &s, &p);
    i.payout_interval = Duration::from_millis(2);
    i.confirmation_interval = Duration::from_millis(2);
    let owner = Accounting::start_mining(
        db.clone(),
        Default::default(),
        Default::default(),
        Default::default(),
        super::super::Inputs {
            admitter: importer.stratum_admitter(),
            limits: Default::default(),
            strictly_follow_top: true,
            clock: Arc::new(Wall),
            production: None,
            rounds: Some(rounds::Inputs::new(
                storage::rounds::Config::from_percentages(
                    [(b"pool".to_vec(), Number::Float(100.0f64.to_bits()))].into(),
                    2,
                    180,
                    Default::default(),
                )
                .unwrap(),
            )),
            payouts: Some(i),
        },
    )
    .unwrap();
    let client = owner.client().mining().unwrap();
    until(|| {
        client
            .payout()
            .unwrap()
            .and_then(|i| i.pending)
            .is_some_and(|r| {
                matches!(
                    r.signed.as_ref().map(|s| &s.dispatch),
                    Some(Dispatch::Done {
                        outcome: durable::Outcome::Accepted { inserted: true, .. },
                        ..
                    })
                )
            })
    });
    let before = client.payout().unwrap().unwrap().pending.unwrap();
    let tx = Signed::decode(&before.signed.as_ref().unwrap().transaction).unwrap();
    assert_eq!(tx.hash(), before.hash().unwrap().unwrap());
    assert_eq!(
        before
            .request
            .as_ref()
            .unwrap()
            .prepared
            .request()
            .environment
            .protocol,
        Protocol::Salus
    );
    assert!(
        matches!(client.payout_status().unwrap().last, Some(Notice::Preflight(ref result)) if result.result.is_ok())
    );

    assert!(matches!(
        ae_storage::pool::Stored::load(db.view().unwrap())
            .unwrap()
            .location(tx.hash())
            .unwrap(),
        ae_mempool::pool::Location::Pool
    ));
    let report = owner.shutdown();
    assert!(report.failure.is_none(), "{:?}", report.failure);
    let report = report.payout.unwrap();
    assert!(report.status.stopped);
    assert!(report.unsettled.is_none());
    assert_eq!(stored(&db).pending, Some(before));
    importer.stop();
    runtime().block_on(importer.join());
    p.shutdown().unwrap();
    assert!(s.retire().joined);
}
#[test]
fn periodic_checks_wait_and_reward_notification_runs_the_same_pipeline() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    tick(&mut a, &mut w);
    assert_eq!(p.client().info().submitted, 0);
    assert!(stored(&db).pending.is_none());
    a.request_check();
    let r = accepted(&mut a, &mut w);
    let submitted = p.client().info().submitted;
    for _ in 0..4 {
        a.request_check();
        tick(&mut a, &mut w);
    }
    assert_eq!(p.client().info().submitted, submitted);
    assert_eq!(stored(&db).pending, Some(r));
    a.finish(&mut w).unwrap();
    assert!(a.report().unsettled.is_none());
    p.shutdown().unwrap();
    s.retire();
}
#[test]
fn shutdown_finishes_accepted_signature_without_dispatching_a_pool_push() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    pending_sign(&mut a, &mut w);
    let submitted = p.client().info().submitted;
    a.finish(&mut w).unwrap();
    assert!(a.report().unsettled.is_none());
    assert_eq!(p.client().info().submitted, submitted);
    let signed = stored(&db).pending.unwrap();
    assert!(matches!(
        signed.signed.as_ref().unwrap().dispatch,
        Dispatch::Ready(0)
    ));
    drop(a);
    drop(w);
    s.request_shutdown();
    until(|| s.is_finished());
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    let r = accepted(&mut a, &mut w);
    assert_eq!(
        r.signed.as_ref().unwrap().transaction,
        signed.signed.as_ref().unwrap().transaction
    );
    assert_eq!(p.client().info().submitted, submitted + 1);
    a.finish(&mut w).unwrap();
    p.shutdown().unwrap();
    s.retire();
}
#[test]
fn lost_pool_reply_is_reconciled_from_retention_without_second_push() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    pending_push(&mut a, &mut w);
    let report = a.report();
    assert!(matches!(
        report.unsettled,
        Some(Completion::Submission { result: Ok(_), .. })
    ));
    let before = stored(&db).pending.unwrap();
    assert!(matches!(
        before.signed.as_ref().unwrap().dispatch,
        Dispatch::Sending(1)
    ));
    let submitted = p.client().info().submitted;
    drop(a);
    drop(w);
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    tick(&mut a, &mut w);
    let after = stored(&db).pending.unwrap();
    assert!(matches!(
        after.signed.as_ref().unwrap().dispatch,
        Dispatch::Done {
            outcome: durable::Outcome::Observed {
                location: ae_mempool::pool::Location::Pool,
                ..
            },
            ..
        }
    ));
    assert_eq!(
        after.signed.as_ref().unwrap().transaction,
        before.signed.as_ref().unwrap().transaction
    );
    assert_eq!(p.client().info().submitted, submitted);
    a.finish(&mut w).unwrap();
    p.shutdown().unwrap();
    s.retire();
}
#[test]
fn unknown_submission_stays_retained_without_nonce_regeneration() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    pending_sign(&mut a, &mut w);
    a.finish(&mut w).unwrap();
    let signed = stored(&db).pending.unwrap();
    let pending = w
        .begin_payout_submission(a.epoch, &signed, Default::default())
        .unwrap();
    drop(a);
    drop(w);
    let submitted = p.client().info().submitted;
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    for _ in 0..4 {
        a.request_check();
        tick(&mut a, &mut w);
    }
    assert_eq!(stored(&db).pending, Some(pending));
    assert_eq!(p.client().info().submitted, submitted);
    a.finish(&mut w).unwrap();
    p.shutdown().unwrap();
    s.retire();
}
#[test]
fn consumed_pool_reply_survives_publication_failure_and_exact_retry() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    pending_push(&mut a, &mut w);
    a.pending.as_mut().unwrap().join();
    let submitted = p.client().info().submitted;
    let limits = a.inputs.limits;
    a.inputs.limits.bytes = 1;
    assert!(a.tick(&mut w, true, Instant::now()).is_err());
    assert!(matches!(
        a.pending,
        Some(Pending::Submission {
            result: Some(Ok(_)),
            ..
        })
    ));
    assert!(matches!(
        stored(&db).pending.unwrap().signed.unwrap().dispatch,
        Dispatch::Sending(1)
    ));
    a.inputs.limits = limits;
    a.finish(&mut w).unwrap();
    assert!(a.report().unsettled.is_none());
    assert!(matches!(
        stored(&db).pending.unwrap().signed.unwrap().dispatch,
        Dispatch::Done {
            outcome: durable::Outcome::Accepted { .. },
            ..
        }
    ));
    assert_eq!(p.client().info().submitted, submitted);
    p.shutdown().unwrap();
    s.retire();
}
#[test]
fn fresh_balance_equality_is_a_retryable_preparation_notice() {
    let temp = Temp::new();
    let db = database(&temp);
    let mut pay = payment();
    pay.total = BALANCE.into();
    seed(&db, pay.clone());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    a.request_check();
    until(|| {
        tick(&mut a, &mut w);
        matches!(
            a.shared.status().last,
            Some(Notice::Preparation(
                ae_stratum::payout::Error::InsufficientBalance { .. }
            ))
        )
    });
    assert!(stored(&db).pending.is_none());
    assert_eq!(
        db.stratum(Default::default())
            .unwrap()
            .query(|t| t.payments(100))
            .unwrap(),
        vec![pay]
    );
    a.finish(&mut w).unwrap();
    p.shutdown().unwrap();
    s.retire();
}
#[test]
fn signer_pressure_keeps_prepared_intent_and_retries_only_the_same_message() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = keys::Worker::spawn(
        ae_hyperchain::signing::Keys::new([secret()]),
        keys::Config {
            requests: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let held = s.client().is_ready().unwrap();
    assert_eq!(held.wait_timeout(Duration::from_secs(1)), Some(Ok(true)));
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    a.request_check();
    until(|| {
        tick(&mut a, &mut w);
        matches!(
            a.shared.status().last,
            Some(Notice::Signer(keys::Error::Busy))
        )
    });
    let before = stored(&db).pending.unwrap();
    assert!(before.signed.is_none());
    assert_eq!(p.client().info().submitted, 1);
    drop(held);
    a.request_check();
    let after = accepted(&mut a, &mut w);
    assert_eq!(after.request, before.request);
    assert_eq!(p.client().info().submitted, 2);
    a.finish(&mut w).unwrap();
    p.shutdown().unwrap();
    s.retire();
}
#[test]
fn missing_signing_key_keeps_prepared_intent_without_pool_submission() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = keys::Worker::spawn(Default::default(), Default::default()).unwrap();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    a.request_check();
    until(|| {
        tick(&mut a, &mut w);
        matches!(
            a.shared.status().last,
            Some(Notice::Signer(keys::Error::Signing(_)))
        )
    });
    let before = stored(&db).pending.unwrap();
    assert!(before.signed.is_none());
    assert_eq!(p.client().info().submitted, 1);
    a.finish(&mut w).unwrap();
    assert!(a.report().unsettled.is_none());
    p.shutdown().unwrap();
    s.retire();
}
#[test]
fn constructor_refuses_other_database_and_invalid_timers_before_external_work() {
    let t = Temp::new();
    let other = Temp::new();
    let db = database(&t);
    let execution = execution_owner(&db, &network());
    let elsewhere = database(&other);
    let p = pool(&elsewhere, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    assert!(matches!(
        Worker::start(
            &mut w,
            db.clone(),
            Default::default(),
            inputs(&db, &s, &p),
            Arc::new(Wall),
            &execution.stratum_admitter(),
        ),
        Err(Error::Configuration)
    ));
    assert!(
        db.stratum(Default::default())
            .unwrap()
            .payouts(Default::default())
            .unwrap()
            .is_none()
    );
    assert_eq!(p.client().info().submitted, 0);
    p.shutdown().unwrap();
    let p = pool(&db, 1);
    let mut i = inputs(&db, &s, &p);
    i.payout_interval = Duration::ZERO;
    assert!(matches!(
        Worker::start(
            &mut w,
            db.clone(),
            Default::default(),
            i,
            Arc::new(Wall),
            &execution.stratum_admitter()
        ),
        Err(Error::Configuration)
    ));
    assert!(
        db.stratum(Default::default())
            .unwrap()
            .payouts(Default::default())
            .unwrap()
            .is_none()
    );
    assert_eq!(p.client().info().submitted, 0);
    p.shutdown().unwrap();
    s.retire();
}
#[test]
fn known_pool_rejection_keeps_the_exact_signed_request() {
    let t = Temp::new();
    let db = database(&t);
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    pending_sign(&mut a, &mut w);
    a.finish(&mut w).unwrap();
    let before = stored(&db).pending.unwrap();
    drop(a);
    drop(w);
    p.shutdown().unwrap();
    let p = pool(&db, 1_000_000_001);
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    until(|| {
        tick(&mut a, &mut w);
        matches!(
            stored(&db)
                .pending
                .as_ref()
                .unwrap()
                .signed
                .as_ref()
                .unwrap()
                .dispatch,
            Dispatch::Done {
                outcome: durable::Outcome::Rejected(_),
                ..
            }
        )
    });
    let after = stored(&db).pending.unwrap();
    assert_eq!(after.request, before.request);
    assert_eq!(
        after.signed.as_ref().unwrap().transaction,
        before.signed.as_ref().unwrap().transaction
    );
    assert!(matches!(a.shared.status().last, Some(Notice::Pool(_))));
    for _ in 0..3 {
        a.request_check();
        tick(&mut a, &mut w);
    }
    assert_eq!(p.client().info().submitted, 1);
    a.finish(&mut w).unwrap();
    p.shutdown().unwrap();
    s.retire();
}
#[test]
fn refusal_before_ticket_restores_ready_and_never_claims_submission() {
    let t = Temp::new();
    let db = database(&t);
    let execution = execution_owner(&db, &network());
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    pending_sign(&mut a, &mut w);
    a.finish(&mut w).unwrap();
    let before = stored(&db).pending.unwrap();
    let mut next_inputs = Inputs::new(config(), s.client(), p.client(), a.inputs.snapshots.clone());
    next_inputs.generation = ae_stratum::payout::Generation::Native;
    drop(a);
    p.shutdown().unwrap();
    let mut a = Worker::start(
        &mut w,
        db.clone(),
        Default::default(),
        next_inputs,
        Arc::new(Wall),
        &execution.stratum_admitter(),
    )
    .unwrap();
    let mut failure = None;
    until(|| {
        failure = a.tick(&mut w, true, Instant::now()).err();
        failure.is_some()
    });
    assert!(matches!(
        failure,
        Some(Error::PayoutPool(txpool::Error::Stopped))
    ));
    let after = stored(&db).pending.unwrap();
    assert!(matches!(
        after.signed.as_ref().unwrap().dispatch,
        Dispatch::Ready(1)
    ));
    assert_eq!(
        after.signed.as_ref().unwrap().transaction,
        before.signed.as_ref().unwrap().transaction
    );
    assert!(matches!(
        a.report().unsettled,
        Some(Completion::Unaccepted {
            reason: Withdrawal::Pool(txpool::Error::Stopped),
            ..
        })
    ));
    s.retire();
}
#[test]
fn native_default_confirmation_tick_without_pending_payment_does_not_create_work() {
    let t = Temp::new();
    let db = database(&t);
    let p = pool(&db, 1);
    let s = signer();
    let i = inputs(&db, &s, &p);
    assert_eq!(i.payout_interval, Duration::from_secs(120));
    assert_eq!(i.confirmation_interval, Duration::from_secs(10));
    drop(i);
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = actor(&db, &mut w, &s, &p);
    let at = a.next_confirmation;
    a.tick(&mut w, true, at).unwrap();
    assert!(a.pending.is_none());
    assert!(stored(&db).pending.is_none());
    assert_eq!(p.client().info().submitted, 0);
    a.finish(&mut w).unwrap();
    p.shutdown().unwrap();
    s.retire();
}
mod recovery;
#[test]
fn close_during_signature_completion_joins_and_prevents_a_late_pool_dispatch() {
    struct BlockedWall {
        entered: std::sync::mpsc::SyncSender<()>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl Clock for BlockedWall {
        fn unix_millis(&self) -> i128 {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            1_700_000_000_000
        }
    }
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = signer();
    let importer = chain::Importer::new(
        db.clone(),
        fixture::execution(&network()),
        fixture::rules(db.view().unwrap().identity().genesis),
        Default::default(),
    )
    .unwrap();
    let (entered, observed) = std::sync::mpsc::sync_channel(1);
    let (release, waiting) = std::sync::mpsc::channel();
    let mut i = inputs(&db, &s, &p);
    i.payout_interval = Duration::from_millis(1);
    let owner = Accounting::start_mining(
        db.clone(),
        Default::default(),
        Default::default(),
        Default::default(),
        super::super::Inputs {
            admitter: importer.stratum_admitter(),
            limits: Default::default(),
            strictly_follow_top: true,
            clock: Arc::new(BlockedWall {
                entered,
                release: Mutex::new(waiting),
            }),
            production: None,
            rounds: Some(rounds::Inputs::new(
                storage::rounds::Config::from_percentages(
                    [(b"pool".to_vec(), Number::Float(100.0f64.to_bits()))].into(),
                    2,
                    180,
                    Default::default(),
                )
                .unwrap(),
            )),
            payouts: Some(i),
        },
    )
    .unwrap();
    observed.recv_timeout(Duration::from_secs(30)).unwrap();
    let client = owner.client();
    client.close();
    assert!(client.mining().unwrap().payout_status().unwrap().closing);
    let (tx, rx) = std::sync::mpsc::channel();
    let joined = thread::spawn(move || {
        tx.send(owner.shutdown()).unwrap();
    });
    assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());
    assert_eq!(p.client().info().submitted, 1);
    release.send(()).unwrap();
    let report = rx.recv_timeout(Duration::from_secs(30)).unwrap();
    joined.join().unwrap();
    assert!(report.failure.is_none(), "{:?}", report.failure);
    assert!(report.payout.unwrap().unsettled.is_none());
    assert_eq!(p.client().info().submitted, 1);
    assert!(matches!(
        stored(&db).pending.unwrap().signed.unwrap().dispatch,
        Dispatch::Ready(0)
    ));
    let w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    drop(w);
    importer.stop();
    runtime().block_on(importer.join());
    p.shutdown().unwrap();
    s.retire();
}

mod execution;
mod gating;
mod managed;

#[test]
fn failed_or_unavailable_confirmation_holds_real_dispatch_until_canonical_recheck() {
    for missing in [false, true] {
        let t = Temp::new();
        let db = database(&t);
        seed(&db, payment());
        let p = pool(&db, 1);
        let s = signer();
        let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
        let mut a = actor(&db, &mut w, &s, &p);
        pending_sign(&mut a, &mut w);
        a.finish(&mut w).unwrap();
        drop(a);
        drop(w);
        let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
        let mut a = actor(&db, &mut w, &s, &p);
        let r = stored(&db).pending.unwrap();
        let inclusion = durable::Inclusion {
            sequence: r.sequence,
            payment: r.payment.id,
            hash: r.hash().unwrap().unwrap(),
            block: Hash256(key(55)),
            height: 100,
            chain: db.view().unwrap().status(),
        };
        // The storage tests independently bind these outcomes to actual call
        // tries. Here they drive the real actor's response/dispatch boundary.
        let result = if missing {
            durable::Confirmation::Unavailable(durable::ReceiptUnavailable {
                inclusion,
                reason: durable::UnavailableReason::MissingCall,
            })
        } else {
            let tx = r.request.as_ref().unwrap().prepared.call().transaction();
            let mut call = ae_state::calls::Call::new(
                tx.actor(),
                tx.nonce().clone(),
                config().contract.0,
                None,
                100.into(),
                tx.gas_price().clone(),
            );
            call.return_type = ae_state::calls::ReturnType::Error;
            durable::Confirmation::Failed(Box::new(durable::ExecutionFailure {
                inclusion,
                reason: durable::FailureReason::ContractError,
                call,
            }))
        };
        assert!(!a.confirmation(result));
        a.next_confirmation = Instant::now() + Duration::from_secs(60);
        let before = p.client().info().submitted;
        for _ in 0..3 {
            a.request_check();
            tick(&mut a, &mut w);
        }
        assert!(a.confirmation_hold);
        assert_eq!(p.client().info().submitted, before);
        assert_eq!(stored(&db).pending, Some(r));
        assert!(!a.confirmation(durable::Confirmation::Pending));
        assert!(!a.confirmation_hold);
        assert!(a.shared.status().last.is_none());
        accepted(&mut a, &mut w);
        assert_eq!(p.client().info().submitted, before + 1);
        a.finish(&mut w).unwrap();
        p.shutdown().unwrap();
        s.retire();
    }
}

#[test]
fn default_live_payout_generation_uses_captured_execution_gas_and_restart_keeps_the_same_request() {
    let t = Temp::new();
    let db = database(&t);
    let execution = execution_owner(&db, &network());
    seed(&db, payment());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let i = Inputs::new(config(), s.client(), p.client(), snapshots(&db));
    assert_eq!(
        i.generation,
        ae_stratum::payout::Generation::Bounded(Default::default())
    );
    let mut a = Worker::start(
        &mut w,
        db.clone(),
        Default::default(),
        i,
        Arc::new(Wall),
        &execution.stratum_admitter(),
    )
    .unwrap();
    a.request_check();
    let r = accepted(&mut a, &mut w);
    let plan = &r.request.as_ref().unwrap().prepared;
    assert_eq!(plan.call().transaction().gas(), &Unsigned::from(22250));
    assert_eq!(
        plan.generation(),
        ae_stratum::payout::Generation::Bounded(Default::default())
    );
    let signed = r.signed.as_ref().unwrap().transaction.clone();
    a.finish(&mut w).unwrap();
    drop(a);
    drop(w);
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut i = Inputs::new(config(), s.client(), p.client(), snapshots(&db));
    i.generation = ae_stratum::payout::Generation::Native;
    let mut a = Worker::start(
        &mut w,
        db.clone(),
        Default::default(),
        i,
        Arc::new(Wall),
        &execution.stratum_admitter(),
    )
    .unwrap();
    assert_eq!(
        stored(&db).pending.unwrap().signed.unwrap().transaction,
        signed
    );
    a.finish(&mut w).unwrap();
    p.shutdown().unwrap();
    s.retire();
}

#[test]
fn default_payout_selection_splits_oversized_obligations_before_signing_and_pool_dispatch() {
    let t = Temp::new();
    let db = database(&t);
    let execution = execution_owner(&db, &network());
    let mut pay = payment();
    pay.relative = (1..=201)
        .map(|n| {
            (
                ae_codec::api::encode(ae_codec::api::ApiType::AccountPubkey, &key(n)).into_bytes(),
                Number::Float((1.0f64 / 201.0).to_bits()),
            )
        })
        .collect();
    seed(&db, pay.clone());
    let p = pool(&db, 1);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let i = Inputs::new(config(), s.client(), p.client(), snapshots(&db));
    let mut a = Worker::start(
        &mut w,
        db.clone(),
        Default::default(),
        i,
        Arc::new(Wall),
        &execution.stratum_admitter(),
    )
    .unwrap();
    a.request_check();
    let r = accepted(&mut a, &mut w);
    assert!(r.payment.gross.is_some());
    assert_eq!(r.request.as_ref().unwrap().prepared.absolute().len(), 101);
    assert_eq!(stored(&db).next, 2);
    let found = db
        .stratum(Default::default())
        .unwrap()
        .query(|t| t.payments(100))
        .unwrap();
    assert_eq!(found.len(), 2);
    let gross: ae_stratum::rewards::Amounts = found
        .iter()
        .flat_map(|p| p.gross.as_ref().unwrap().clone())
        .collect();
    assert_eq!(
        gross,
        ae_stratum::rewards::absolute(&pay.relative, &pay.total, Default::default()).unwrap()
    );
    assert_eq!(
        found.iter().map(|p| &p.total).sum::<num_bigint::BigInt>(),
        pay.total
    );
    assert_eq!(p.client().info().pool.active, 1);
    a.finish(&mut w).unwrap();
    p.shutdown().unwrap();
    s.retire();
}
