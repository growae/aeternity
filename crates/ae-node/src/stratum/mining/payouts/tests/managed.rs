use super::*;
use ae_sync::runtime::workers;

struct Managed {
    handle: workers::Handle,
    service: Option<workers::Service>,
    task: Option<std::thread::JoinHandle<workers::Status>>,
}
impl Managed {
    fn new(db: &Arc<Database>) -> Self {
        let mut config = workers::Config {
            pool_gc: None,
            resilience: None,
            capabilities: None,
            gc: None,
            poll: Duration::from_millis(1),
            ..Default::default()
        };
        config.pool.policy.minimum_miner_gas_price = 1.into();
        config.admission.origins_interval = None;
        let (handle, service) =
            workers::create(db.clone(), network(), config, Default::default()).unwrap();
        Self {
            handle,
            service: Some(service),
            task: None,
        }
    }
    fn start(&mut self) {
        let service = self.service.take().unwrap();
        self.task = Some(thread::spawn(move || runtime().block_on(service.run())));
        until(|| self.handle.status().ready);
    }
    fn pool(&self) -> txpool::Client {
        self.handle.bindings().admission.current().unwrap().client
    }
    fn restart(&self) {
        let number = self.handle.status().admission_generation;
        runtime().block_on(async {
            tokio::time::timeout(
                Duration::from_secs(10),
                self.handle
                    .restart(workers::Child::Admission, number)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap()
            .unwrap()
        });
        until(|| self.handle.bindings().admission.current().is_some());
    }
    fn actor(&self, db: &Arc<Database>, w: &mut storage::Writer, s: &keys::Worker) -> Actor {
        let execution = execution_owner(db, &network());
        let mut inputs = Inputs::new(
            config(),
            s.client(),
            self.handle.bindings().admission,
            snapshots(db),
        );
        inputs.generation = ae_stratum::payout::Generation::Native;
        let worker = Worker::start(
            w,
            db.clone(),
            Default::default(),
            inputs,
            Arc::new(Wall),
            &execution.stratum_admitter(),
        )
        .unwrap();
        Actor {
            worker,
            _execution: execution,
        }
    }
    fn close(&mut self) {
        self.handle.shutdown();
        if let Some(service) = self.service.take() {
            self.task = Some(thread::spawn(move || runtime().block_on(service.run())));
        }
        if let Some(task) = self.task.take() {
            let status = task.join().unwrap();
            assert!(status.joined && !status.running);
        }
    }
}
impl Drop for Managed {
    fn drop(&mut self) {
        self.close();
    }
}

#[test]
fn managed_payout_waits_for_initial_pool_and_keeps_the_original_price_ticket_across_replacement() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let mut managed = Managed::new(&db);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = managed.actor(&db, &mut w, &s);
    let before = stored(&db);
    a.request_check();
    for _ in 0..3 {
        tick(&mut a, &mut w);
    }
    assert!(a.shared.status().waiting_for_pool);
    assert_eq!(stored(&db), before);
    managed.start();
    let old = managed.pool();
    let policy = ae_mempool::pool::Config {
        minimum_miner_gas_price: 7.into(),
        ..Default::default()
    };
    old.reconfigure(policy, Default::default())
        .unwrap()
        .wait()
        .unwrap();
    until(|| {
        tick(&mut a, &mut w);
        matches!(a.pending, Some(Pending::Price { .. }))
    });
    managed.restart();
    assert!(old.info().joined && !old.same_worker(&managed.pool()));
    assert_eq!(managed.pool().info().submitted, 0);
    a.complete(&mut w, true).unwrap();
    let prepared = stored(&db).pending.unwrap().request.unwrap().prepared;
    assert_eq!(
        prepared.request().environment.minimum_miner_gas_price,
        7.into()
    );
    assert_eq!(managed.pool().info().submitted, 0);
    assert_eq!(
        managed
            .pool()
            .minimum_miner_gas_price()
            .unwrap()
            .wait()
            .unwrap(),
        1.into()
    );
    assert!(!a.shared.status().waiting_for_pool);
    a.finish(&mut w).unwrap();
    managed.close();
    s.retire();
}

#[test]
fn managed_payout_consumes_accepted_submission_from_retired_pool_without_repricing_or_replay() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let mut managed = Managed::new(&db);
    managed.start();
    let old = managed.pool();
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = managed.actor(&db, &mut w, &s);
    pending_push(&mut a, &mut w);
    let before = stored(&db).pending.unwrap();
    assert!(matches!(
        before.signed.as_ref().unwrap().dispatch,
        Dispatch::Sending(_)
    ));
    managed.restart();
    assert!(old.info().joined);
    assert_eq!(old.info().submitted, 2);
    assert_eq!(managed.pool().info().submitted, 0);
    assert!(matches!(
        a.pending,
        Some(Pending::Submission { result: None, .. })
    ));
    a.complete(&mut w, true).unwrap();
    let after = stored(&db).pending.unwrap();
    assert_eq!(after.request, before.request);
    assert_eq!(
        after.signed.as_ref().unwrap().transaction,
        before.signed.as_ref().unwrap().transaction
    );
    assert!(matches!(
        after.signed.as_ref().unwrap().dispatch,
        Dispatch::Done {
            outcome: durable::Outcome::Accepted { .. },
            ..
        }
    ));
    for _ in 0..3 {
        a.request_check();
        tick(&mut a, &mut w);
    }
    assert_eq!(managed.pool().info().submitted, 0);
    assert_eq!(managed.pool().info().pool.active, 1);
    assert!(a.report().unsettled.is_none());
    a.finish(&mut w).unwrap();
    managed.close();
    s.retire();
}

#[test]
fn managed_payout_keeps_signed_bytes_and_ready_attempt_unchanged_while_pool_is_unavailable() {
    let temp = Temp::new();
    let db = database(&temp);
    seed(&db, payment());
    let mut managed = Managed::new(&db);
    let s = signer();
    let mut w = storage::Writer::start(db.clone(), Default::default()).unwrap();
    let mut a = managed.actor(&db, &mut w, &s);
    w.prepare_payout_with_generation(
        a.epoch,
        1.into(),
        ae_stratum::payout::Generation::Native,
        Default::default(),
    )
    .unwrap();
    pending_sign(&mut a, &mut w);
    a.pending.as_mut().unwrap().join();
    a.complete(&mut w, true).unwrap();
    let before = stored(&db);
    for _ in 0..4 {
        tick(&mut a, &mut w);
    }
    assert!(a.shared.status().waiting_for_pool);
    assert_eq!(stored(&db), before);
    managed.start();
    let after = accepted(&mut a, &mut w);
    assert_eq!(
        after.signed.as_ref().unwrap().transaction,
        before.pending.unwrap().signed.unwrap().transaction
    );
    assert_eq!(managed.pool().info().submitted, 1);
    a.finish(&mut w).unwrap();
    managed.close();
    s.retire();
}
