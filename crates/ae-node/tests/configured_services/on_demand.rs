use super::*;
use ae_sync::chain::on_demand::{
    control::{Command, Kind},
    runtime, service,
};
use std::sync::{Mutex, mpsc};

fn config(lab: &Lab) -> root::Config<root::Commands> {
    let mut c = root::Config::with_production(root::Commands::new(AccountPublicKey([42; 32])));
    c.workers = lab.worker_config();
    c.connections = network_config();
    c.poll = Duration::from_millis(5);
    c.startup_timeout = Duration::from_secs(10);
    c
}
fn prepared(lab: &Lab, development: bool) -> Prepared {
    lab.services(
        "on_demand",
        json!({"dev_mode":development}),
        Default::default(),
    )
}
fn launch(lab: &Lab, development: bool) -> root::Running {
    root::launch_commands(
        prepared(lab, development),
        config(lab),
        Arc::new(Clock),
        || Ok(Default::default()),
    )
    .unwrap()
}

#[tokio::test]
async fn automatic_on_demand_root_seals_native_keys_toggles_and_reopens() {
    use ae_sync::chain::pow::producer::mining;
    for strict in [false, true] {
        let lab = Lab::new();
        let mut previous_height = 0;
        for round in 0..2 {
            let prepared = prepared(&lab, false);
            let db = prepared.configured().chain().database().clone();
            let snapshots = prepared.snapshots().clone();
            let mut config = root_config(&lab);
            config.outputs = 1;
            config.production.producer.enabled = round != 0;
            config.production.producer.strictly_follow_top = strict;
            let mut mining = mining::Config::new(AccountPublicKey([42; 32]));
            mining.instances = vec![mining::Instance::default(); 2];
            config.production.mining = Some(mining);
            let running = root::launch_on_demand(
                prepared,
                config,
                Arc::new(group::SystemClocks::default()),
                || Ok(Default::default()),
            )
            .unwrap();
            let h = running.handle();
            root_ready(&h, 1).await;
            assert!(h.commands().unwrap().production.is_some());
            assert!(matches!(
                h.importer()
                    .unwrap()
                    .stratum_builder(snapshots.clone(), Default::default()),
                Err(mining::Error::Binding)
            ));
            if round == 0 {
                within(async {
                    loop {
                        while h.try_output().is_some() {}
                        let generation = h.production().unwrap().current().unwrap();
                        let status = generation.producer.status();
                        assert!(!status.enabled);
                        if let Some(mining) = status.mining
                            && let Some(candidate) = mining.candidate
                        {
                            assert_eq!(
                                candidate.algorithm,
                                ae_protocol::ConsensusAlgorithm::OnDemand
                            );
                            assert_eq!(candidate.header.common.height, 1);
                            assert_eq!(mining.workers.len(), 2);
                            assert!(
                                mining
                                    .workers
                                    .iter()
                                    .all(|w| w.submitted == 0 && w.external.is_none())
                            );
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await;
                assert!(db.view().unwrap().canonical_key(1).unwrap().is_none());
                h.set_enabled(true).unwrap();
            }
            within(async {
                while db
                    .view()
                    .unwrap()
                    .canonical_key(previous_height + 2)
                    .unwrap()
                    .is_none()
                {
                    while h.try_output().is_some() {}
                    assert!(h.status().failure.is_none(), "{:?}", h.status());
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await;
            h.set_enabled(false).unwrap();
            // Shutdown must join even with the one-slot output queue unread.
            let exit = running.shutdown().await.unwrap();
            assert_root_joined(&exit);
            assert!(exit.status.failure.is_none(), "{:?}", exit.status);
            let view = db.view().unwrap();
            let mut height = 1;
            while let Some(hash) = view.canonical_key(height).unwrap() {
                let ae_chain::block::Block::Key(header) = view.require_block(hash).unwrap() else {
                    panic!()
                };
                assert_eq!((header.target, header.nonce), (1338, 1338));
                assert_eq!(header.seal, [0; 42]);
                assert_eq!(header.beneficiary, [42; 32]);
                assert_eq!(
                    view.header_record(hash).unwrap().unwrap().seal_present,
                    Some(true)
                );
                height += 1;
            }
            assert!(height > previous_height + 2);
            previous_height = height - 1;
            assert_eq!(snapshots.live_snapshots(), 0);
        }
    }
}
struct SharedClockGate {
    database: Arc<ae_storage::Database>,
    revision: u64,
    entered: mpsc::SyncSender<()>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
}
impl generator::Clock for SharedClockGate {
    fn unix_millis(&self) -> i128 {
        if self.database.view().unwrap().status().revision >= self.revision
            && let Some(release) = self.release.lock().unwrap().take()
        {
            self.entered.send(()).unwrap();
            release.recv().unwrap();
        }
        generator::SystemClock.unix_millis()
    }
}
struct SharedClocks {
    epoch: std::time::Instant,
    preparation: Arc<dyn generator::Clock>,
    commands: Arc<dyn generator::Clock>,
}
impl group::Clocks for SharedClocks {
    fn generator(&self, _: u64) -> Arc<dyn generator::Clock> {
        Arc::new(generator::SystemClock)
    }
    fn producer(&self, _: u64) -> Box<dyn ae_sync::chain::pow::producer::Clock> {
        Box::new(ae_sync::chain::pow::producer::SystemClock)
    }
    fn preparation(&self, _: u64) -> Arc<dyn generator::Clock> {
        self.preparation.clone()
    }
    fn commands(&self, _: u64) -> Arc<dyn generator::Clock> {
        self.commands.clone()
    }
    fn second(&self) -> u64 {
        self.epoch.elapsed().as_secs()
    }
}
fn shared_gate(
    db: Arc<ae_storage::Database>,
    revision: u64,
) -> (Arc<SharedClockGate>, mpsc::Receiver<()>, Release) {
    let (sent, entered) = mpsc::sync_channel(1);
    let (release, recv) = mpsc::channel();
    (
        Arc::new(SharedClockGate {
            database: db,
            revision,
            entered: sent,
            release: Mutex::new(Some(recv)),
        }),
        entered,
        Release(Some(release)),
    )
}
async fn shared_result(h: &root::Handle, ticket: &service::Ticket) -> Arc<service::Completion> {
    within(async {
        loop {
            while h.try_output().is_some() {}
            assert!(h.status().failure.is_none(), "{:?}", h.status());
            if let Some(completion) = ticket.try_result().unwrap() {
                return completion;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
}
#[tokio::test]
async fn shared_on_demand_commands_commit_while_automatic_preparation_is_inflight() {
    use ae_sync::chain::pow::producer::mining;
    let lab = Lab::new();
    let p = prepared(&lab, true);
    let tx = transaction(&p);
    let db = p.configured().chain().database().clone();
    let snapshots = p.snapshots().clone();
    let (gate, entered, mut release) = shared_gate(db.clone(), 1);
    let mut config = root_config(&lab);
    config.production.producer.enabled = true;
    config.production.mining = Some(mining::Config::new(AccountPublicKey([42; 32])));
    let running = root::launch_on_demand(
        p,
        config,
        Arc::new(SharedClocks {
            epoch: std::time::Instant::now(),
            preparation: gate,
            commands: Arc::new(generator::SystemClock),
        }),
        || Ok(Default::default()),
    )
    .unwrap();
    let h = running.handle();
    root_ready(&h, 1).await;
    within(async {
        while entered.try_recv().is_err() {
            while h.try_output().is_some() {}
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    let commands = h.commands().unwrap();
    let generation = commands.production.unwrap();
    assert!(
        h.production()
            .unwrap()
            .current()
            .unwrap()
            .producer
            .status()
            .enabled
    );
    h.workers()
        .unwrap()
        .bindings()
        .admission
        .current()
        .unwrap()
        .client
        .push(tx.clone(), ae_mempool::pool::Event::Created, false)
        .unwrap()
        .await
        .unwrap();
    let ticket = commands
        .client
        .submit(Command::Blocks {
            count: 2,
            kind: Kind::Any,
        })
        .unwrap();
    let completion = shared_result(&h, &ticket).await;
    let report = completion.result.as_ref().unwrap();
    assert!(report.command.result.is_ok(), "{report:?}");
    assert_eq!(report.admissions.len(), 2);
    assert!(report.promotions.is_empty());
    assert!(db.view().unwrap().location(tx.hash()).unwrap().is_some());
    let ae_chain::block::Block::Key(key) = report.admissions[0].block.as_ref() else {
        panic!()
    };
    assert_eq!(key.nonce, 0);
    let first = report.admissions[0].block.hash().unwrap();
    assert_eq!(
        db.view()
            .unwrap()
            .header_record(first)
            .unwrap()
            .unwrap()
            .seal_present,
        None
    );
    assert!(matches!(
        report.admissions[0].origin,
        ae_chain::repository::ledger::Origin::Sync
    ));
    assert!(matches!(
        report.admissions[1].origin,
        ae_chain::repository::ledger::Origin::Gossip
    ));
    assert_eq!(
        h.production()
            .unwrap()
            .current()
            .unwrap()
            .producer
            .status()
            .promotions,
        1
    );
    // The conductor's completed promotion must already be visible to the
    // native leader-dependent zero-micro command; it must not emit another key.
    let empty = commands
        .client
        .submit(Command::Blocks {
            count: 0,
            kind: Kind::Micro,
        })
        .unwrap();
    let empty = shared_result(&h, &empty).await;
    assert!(empty.result.as_ref().unwrap().command.result.is_ok());
    assert!(empty.result.as_ref().unwrap().admissions.is_empty());
    release.open();
    within(async {
        while db.view().unwrap().canonical_key(2).unwrap().is_none() {
            while h.try_output().is_some() {}
            assert!(h.status().failure.is_none(), "{:?}", h.status());
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    h.set_enabled(false).unwrap();
    within(async {
        loop {
            while h.try_output().is_some() {}
            let status = h.production().unwrap().current().unwrap().producer.status();
            if !status.enabled && status.applied_revision == status.request_revision {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    let hash = db.view().unwrap().canonical_key(2).unwrap().unwrap();
    assert_eq!(
        db.view()
            .unwrap()
            .header_record(hash)
            .unwrap()
            .unwrap()
            .seal_present,
        Some(true)
    );
    assert_eq!(h.commands().unwrap().production, Some(generation));
    let next = commands.client.submit(Command::Key).unwrap();
    assert!(
        shared_result(&h, &next)
            .await
            .result
            .as_ref()
            .unwrap()
            .command
            .result
            .is_ok()
    );
    let exit = running.shutdown().await.unwrap();
    assert_root_joined(&exit);
    assert!(exit.status.failure.is_none(), "{:?}", exit.status);
    assert!(exit.status.commands.as_ref().unwrap().joined);
    assert_eq!(exit.status.commands.as_ref().unwrap().completed, 3);
    assert!(!commands.client.status().accepting);
    assert_eq!(snapshots.live_snapshots(), 0);
}
#[tokio::test]
async fn shared_on_demand_replacement_waits_for_partial_command_and_never_replays_it() {
    use ae_sync::chain::pow::producer::mining;
    let lab = Lab::new();
    let p = prepared(&lab, true);
    let db = p.configured().chain().database().clone();
    let (gate, entered, mut release) = shared_gate(db.clone(), 2);
    let mut config = root_config(&lab);
    config.production.mining = Some(mining::Config::new(AccountPublicKey([42; 32])));
    let running = root::launch_on_demand(
        p,
        config,
        Arc::new(SharedClocks {
            epoch: std::time::Instant::now(),
            preparation: Arc::new(generator::SystemClock),
            commands: gate,
        }),
        || Ok(Default::default()),
    )
    .unwrap();
    let h = running.handle();
    root_ready(&h, 1).await;
    let old = h.commands().unwrap();
    let active = old
        .client
        .submit(Command::Blocks {
            count: 2,
            kind: Kind::Key,
        })
        .unwrap();
    let queued = old.client.submit(Command::Key).unwrap();
    within(async {
        while entered.try_recv().is_err() {
            while h.try_output().is_some() {}
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    let producer = h.production().unwrap().current().unwrap().producer;
    producer.request_shutdown();
    within(async {
        while producer.status().running {
            while h.try_output().is_some() {}
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    assert!(active.try_result().unwrap().is_none());
    assert!(ae_storage::pow::ProducerLease::acquire(&db).is_err());
    assert!(h.commands().is_none_or(|c| c.production == old.production));
    release.open();
    let result = shared_result(&h, &active).await;
    let report = result.result.as_ref().unwrap();
    assert!(report.command.result.is_err());
    assert_eq!(report.admissions.len(), 1);
    assert!(report.admissions[0].result.is_ok());
    assert!(matches!(
        shared_result(&h, &queued).await.result,
        Err(service::Error::Cancelled)
    ));
    let fresh = within(async {
        loop {
            while h.try_output().is_some() {}
            assert!(h.status().failure.is_none(), "{:?}", h.status());
            if let Some(next) = h.commands()
                && next.production != old.production
            {
                return next;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    assert_eq!(db.view().unwrap().status().revision, 2);
    assert_eq!(fresh.client.status().accepted, 0);
    assert!(old.client.submit(Command::Key).is_err());
    let next = fresh.client.submit(Command::Key).unwrap();
    assert!(
        shared_result(&h, &next)
            .await
            .result
            .as_ref()
            .unwrap()
            .command
            .result
            .is_ok()
    );
    assert_eq!(db.view().unwrap().status().revision, 3);
    let exit = running.shutdown().await.unwrap();
    assert_root_joined(&exit);
    assert!(exit.status.failure.is_none(), "{:?}", exit.status);
    assert!(exit.status.commands.as_ref().unwrap().joined);
}

#[tokio::test]
async fn shared_on_demand_root_rebinds_keys_and_pool_without_replaying_commands() {
    use ae_sync::chain::pow::producer::mining;
    let lab = Lab::new();
    let p = prepared(&lab, false);
    let db = p.configured().chain().database().clone();
    let mut config = root_config(&lab);
    config.production.mining = Some(mining::Config::new(AccountPublicKey([42; 32])));
    let running =
        root::launch_on_demand(p, config, Arc::new(group::SystemClocks::default()), || {
            Ok(Default::default())
        })
        .unwrap();
    let h = running.handle();
    root_ready(&h, 1).await;
    let importer = h.importer().unwrap();
    let network = h.connections().unwrap().current().unwrap().number;
    let workers = h.workers().unwrap();
    let mut receipts = Vec::new();
    for (number, child) in [(1, workers::Child::Keys), (2, workers::Child::Admission)] {
        let old = h.commands().unwrap();
        assert_eq!(old.number, number);
        assert!(old.production.is_some());
        let ticket = old.client.submit(Command::Key).unwrap();
        receipts.push(shared_result(&h, &ticket).await);
        within(workers.restart(child, 1).unwrap().wait())
            .await
            .unwrap();
        within(async {
            loop {
                while h.try_output().is_some() {}
                let s = h.status();
                assert!(s.failure.is_none(), "{s:?}");
                if s.ready && s.production_starts == number + 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        assert!(old.client.status().joined);
        assert!(old.client.submit(Command::Key).is_err());
        assert!(Arc::ptr_eq(&importer, &h.importer().unwrap()));
        assert_eq!(h.commands().unwrap().client.status().accepted, 0);
    }
    assert_eq!(db.view().unwrap().status().revision, 3);
    assert_eq!(h.connections().unwrap().current().unwrap().number, network);
    for receipt in &receipts {
        let report = receipt.result.as_ref().unwrap();
        assert!(report.command.result.is_ok());
        assert_eq!(report.admissions.len(), 1);
        assert!(report.promotions.is_empty());
    }
    let ticket = h.commands().unwrap().client.submit(Command::Key).unwrap();
    assert!(
        shared_result(&h, &ticket)
            .await
            .result
            .as_ref()
            .unwrap()
            .command
            .result
            .is_ok()
    );
    assert_eq!(db.view().unwrap().status().revision, 4);
    let exit = running.shutdown().await.unwrap();
    assert_root_joined(&exit);
    assert!(exit.status.failure.is_none(), "{:?}", exit.status);
    assert!(exit.status.commands.as_ref().unwrap().joined);
}

async fn sealed_candidate(h: &root::Handle, height: u64) -> ae_codec::headers::KeyHeader {
    within(async {
        loop {
            while h.try_output().is_some() {}
            assert!(h.status().failure.is_none(), "{:?}", h.status());
            if let Some(candidate) = h
                .production()
                .and_then(|p| p.current())
                .and_then(|p| p.producer.status().mining)
                .and_then(|m| m.candidate)
                && candidate.header.common.height == height
            {
                let mut header = candidate.header.clone();
                ae_consensus::on_demand::seal(&mut header);
                return header;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
}

#[tokio::test]
async fn on_demand_seal_presence_keeps_first_admission_and_survives_reopen_and_rollback() {
    use ae_chain::repository::rollback;
    use ae_sync::chain::{pow::producer::mining, query::blocks};
    for local_first in [false, true] {
        let lab = Lab::new();
        let p = prepared(&lab, false);
        let db = p.configured().chain().database().clone();
        let mut config = root_config(&lab);
        config.production.mining = Some(mining::Config::new(AccountPublicKey([42; 32])));
        let running =
            root::launch_on_demand(p, config, Arc::new(group::SystemClocks::default()), || {
                Ok(Default::default())
            })
            .unwrap();
        let h = running.handle();
        root_ready(&h, 1).await;
        let importer = h.importer().unwrap();
        let first = sealed_candidate(&h, 1).await;
        let block = ae_chain::block::Block::Key(first.clone());
        let hash = block.hash().unwrap();
        let wire = block.encode().unwrap();
        let ticket = if local_first {
            importer.import_created_key(first.clone(), first.time, None)
        } else {
            importer.import_synced_block(wire.clone(), first.time)
        }
        .unwrap();
        ticket.wait().await.unwrap();
        let revision = db.view().unwrap().status().revision;
        let ticket = if local_first {
            importer.import_synced_block(wire.clone(), first.time)
        } else {
            importer.import_created_key(first.clone(), first.time, None)
        }
        .unwrap();
        let duplicate = ticket.wait().await.unwrap();
        assert!(matches!(
            duplicate.accepted[0].outcome,
            ae_sync::chain::Outcome::Present
        ));
        assert_eq!(db.view().unwrap().status().revision, revision);
        let expected = local_first.then_some(true);
        let blocks::Reply::Header(reply) = importer
            .blocks(blocks::Request::CurrentKey)
            .unwrap()
            .wait()
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(reply.seal_present, expected);
        assert_eq!(
            db.view()
                .unwrap()
                .require_block(hash)
                .unwrap()
                .encode()
                .unwrap(),
            wire
        );
        let second = sealed_candidate(&h, 2).await;
        let second_hash = ae_codec::headers::Header::Key(second.clone())
            .hash()
            .unwrap();
        importer
            .import_created_key(second.clone(), second.time, Some(hash))
            .unwrap()
            .wait()
            .await
            .unwrap();
        let exit = running.shutdown().await.unwrap();
        assert_root_joined(&exit);
        assert!(exit.status.failure.is_none());
        drop(exit);
        drop(h);
        drop(importer);
        drop(db);
        // Shutdown closes the managed writer. Reopen before offline rollback.
        let reopened = prepared(&lab, false);
        let db = reopened.configured().chain().database().clone();
        assert_eq!(
            db.view()
                .unwrap()
                .header_record(second_hash)
                .unwrap()
                .unwrap()
                .seal_present,
            Some(true)
        );
        db.rollback(rollback::Target::Height(1), Default::default())
            .unwrap();
        assert!(
            db.view()
                .unwrap()
                .header_record(second_hash)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            db.view()
                .unwrap()
                .header_record(hash)
                .unwrap()
                .unwrap()
                .seal_present,
            expected
        );
        drop(db);
        drop(reopened);
        let reopened = prepared(&lab, false);
        assert_eq!(
            reopened
                .configured()
                .chain()
                .database()
                .view()
                .unwrap()
                .header_record(hash)
                .unwrap()
                .unwrap()
                .seal_present,
            expected
        );
        assert!(
            reopened
                .configured()
                .chain()
                .database()
                .view()
                .unwrap()
                .header_record(second_hash)
                .unwrap()
                .is_none()
        );
    }
}
async fn completed(c: &service::Client, n: u64) {
    within(async {
        while c.status().completed < n {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
}
fn transaction(p: &Prepared) -> Arc<ae_tx::SignedTransaction<ae_tx::Transaction>> {
    use ae_tx::CanonicalTransaction;
    let mut tx = ae_tx::Spend::new(
        payer().public_key(),
        ae_types::Id::new(ae_types::IdTag::Account, [43; 32]),
    )
    .unwrap();
    tx.set_nonce(1.into());
    tx.set_amount(7.into());
    tx.set_fee(100_000_000_000_000u64.into());
    let mut bytes = p
        .configured()
        .chain()
        .settings()
        .profile()
        .signing_prefix(false);
    bytes.extend(tx.encode());
    Arc::new(ae_tx::SignedTransaction::new(
        ae_tx::Transaction::new(ae_tx::TransactionLeaf::Spend(tx)),
        vec![payer().sign(&bytes)],
    ))
}

#[tokio::test]
async fn configured_command_root_commits_real_transfer_retains_unread_result_and_reopens() {
    for development in [false, true] {
        let lab = Lab::new();
        let p = prepared(&lab, development);
        let tx = transaction(&p);
        let db = p.configured().chain().database().clone();
        let running =
            root::launch_commands(p, config(&lab), Arc::new(Clock), || Ok(Default::default()))
                .unwrap();
        let h = running.handle();
        root_ready(&h, 1).await;
        assert_eq!(h.connections().is_none(), development);
        assert!(h.production().is_none());
        assert_eq!(h.set_enabled(true), Err(root::Error::UnsupportedControl));
        let commands = h.commands().unwrap();
        assert_eq!(commands.number, 1);
        h.workers()
            .unwrap()
            .bindings()
            .admission
            .current()
            .unwrap()
            .client
            .push(tx.clone(), ae_mempool::pool::Event::Created, false)
            .unwrap()
            .await
            .unwrap();
        let first = commands
            .client
            .submit(Command::Blocks {
                count: 2,
                kind: Kind::Any,
            })
            .unwrap()
            .wait()
            .await
            .unwrap();
        let r = first.result.as_ref().unwrap();
        assert!(r.command.result.is_ok(), "{r:?}");
        assert_eq!(r.admissions.len(), 2);
        assert!(r.admissions.iter().any(|a|matches!(a.block.as_ref(),ae_chain::block::Block::Micro(m) if m.transactions.iter().any(|t|t.hash()==tx.hash()))));
        let unread = commands.client.submit(Command::Key).unwrap();
        completed(&commands.client, 2).await;
        let tip = db.view().unwrap().status().tip;
        assert_eq!(db.view().unwrap().status().revision, 4);
        let exit = within(running.shutdown()).await.unwrap();
        assert_root_joined(&exit);
        assert!(exit.status.failure.is_none(), "{:?}", exit.status);
        assert!(exit.production.is_none());
        assert_eq!(
            exit.commands
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .status
                .completed,
            2
        );
        let result = unread.wait().await.unwrap();
        assert!(result.result.as_ref().unwrap().command.result.is_ok());
        assert_eq!(
            commands.client.submit(Command::Key).err().unwrap().error,
            service::Error::Closed
        );
        let mut ids = vec![];
        for o in &exit.retained {
            if let root::Output::Command {
                generation,
                completion,
            } = o
            {
                assert_eq!(*generation, 1);
                ids.push(completion.accepted.id);
            }
        }
        for o in &exit.commands.as_ref().unwrap().as_ref().unwrap().outputs {
            ids.push(o.accepted.id);
        }
        ids.sort();
        assert_eq!(ids, vec![1, 2]);
        drop(result);
        drop(first);
        drop(commands);
        drop(db);
        drop(h);
        drop(exit);
        assert_eq!(
            prepared(&lab, development)
                .configured()
                .chain()
                .database()
                .view()
                .unwrap()
                .status()
                .tip,
            tip
        );
    }
    println!(
        "Configured on-demand root: normal/development modes, real funded transfer, unread command completion through joined shutdown and canonical reopen"
    );
}

#[tokio::test]
async fn configured_command_root_replaces_managed_keys_and_pool_without_replaying_requests() {
    let lab = Lab::new();
    let running = launch(&lab, false);
    let h = running.handle();
    root_ready(&h, 1).await;
    let importer = h.importer().unwrap();
    let connections = h.connections().unwrap();
    let network = connections.current().unwrap().number;
    let w = h.workers().unwrap();
    let mut results = vec![];
    let mut observed = vec![];
    for (number, child) in [(1, workers::Child::Keys), (2, workers::Child::Admission)] {
        let generation = h.commands().unwrap();
        assert_eq!(generation.number, number);
        results.push(
            generation
                .client
                .submit(Command::Key)
                .unwrap()
                .wait()
                .await
                .unwrap(),
        );
        within(w.restart(child, 1).unwrap().wait()).await.unwrap();
        within(async {
            loop {
                while let Some(o) = h.try_output() {
                    observed.push(o);
                }
                let s = h.status();
                assert!(s.running && s.failure.is_none(), "{s:?}");
                if s.ready && s.production_starts == number + 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(generation.client.status().joined);
        assert_eq!(
            generation.client.submit(Command::Key).err().unwrap().error,
            service::Error::Closed
        );
        assert!(Arc::ptr_eq(&importer, &h.importer().unwrap()));
    }
    assert_eq!(h.commands().unwrap().number, 3);
    let result = h
        .commands()
        .unwrap()
        .client
        .submit(Command::Micro)
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(result.result.as_ref().unwrap().command.result.is_ok());
    assert_eq!(connections.current().unwrap().number, network);
    let exit = within(running.shutdown()).await.unwrap();
    assert_root_joined(&exit);
    assert!(exit.status.failure.is_none(), "{:?}", exit.status);
    observed.extend(exit.retained);
    let mut retired = vec![];
    let mut completed = vec![];
    for o in observed {
        match o {
            root::Output::Command {
                generation,
                completion,
            } => completed.push((generation, completion.accepted.id)),
            root::Output::CommandsRetired { generation, report } => {
                assert!(report.status.joined);
                retired.push(generation);
                for c in report.outputs {
                    completed.push((generation, c.accepted.id));
                }
            }
            _ => panic!("wrong production family"),
        }
    }
    for c in &exit.commands.as_ref().unwrap().as_ref().unwrap().outputs {
        completed.push((3, c.accepted.id));
    }
    retired.sort();
    completed.sort();
    assert_eq!(retired, vec![1, 2]);
    assert_eq!(completed, vec![(1, 1), (2, 1), (3, 1)]);
    assert_eq!(
        exit.services
            .as_ref()
            .unwrap()
            .configured()
            .chain()
            .database()
            .view()
            .unwrap()
            .status()
            .revision,
        4
    );
    assert_eq!(results.len(), 2);
    println!(
        "Configured on-demand root: managed key/pool replacement releases settled leases, preserves generation-tagged receipts and one importer, and never repeats accepted commands"
    );
}

#[tokio::test]
async fn configured_command_root_output_pressure_retains_old_and_new_generation_results() {
    for resume in [false, true] {
        let lab = Lab::new();
        let mut c = config(&lab);
        c.outputs = 1;
        c.production.service.requests = 2;
        c.startup_timeout = Duration::from_secs(2);
        let running = root::launch_commands(prepared(&lab, true), c, Arc::new(Clock), || {
            Ok(Default::default())
        })
        .unwrap();
        let h = running.handle();
        root_ready(&h, 1).await;
        let old = h.commands().unwrap();
        let first = old.client.submit(Command::Key).unwrap();
        completed(&old.client, 1).await;
        // Let the root move this completion into its sole observation slot.
        within(async {
            while h.status().commands.as_ref().unwrap().completed < 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        let second = old.client.submit(Command::Key).unwrap();
        completed(&old.client, 2).await;
        assert_eq!(
            old.client.submit(Command::Key).err().unwrap().error,
            service::Error::Busy
        );
        within(
            h.workers()
                .unwrap()
                .restart(workers::Child::Keys, 1)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        within(async {
            while h.status().phase != root::Phase::Rebinding {
                assert!(h.status().running, "{:?}", h.status());
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        tokio::time::sleep(Duration::from_millis(2100)).await;
        assert!(
            h.status().running && h.status().failure.is_none(),
            "{:?}",
            h.status()
        );
        assert_eq!(h.status().production_starts, 1);
        let mut observed = vec![];
        let mut latest = None;
        if resume {
            observed.push(h.try_output().unwrap());
            root_ready(&h, 2).await;
            let generation = h.commands().unwrap();
            latest = Some(generation.client.submit(Command::Key).unwrap());
            completed(&generation.client, 1).await;
        }
        let exit = within(running.shutdown()).await.unwrap();
        assert_root_joined(&exit);
        assert!(exit.status.failure.is_none(), "{:?}", exit.status);
        observed.extend(exit.retained);
        let mut ids = vec![];
        for o in observed {
            match o {
                root::Output::Command {
                    generation,
                    completion,
                } => ids.push((generation, completion.accepted.id)),
                root::Output::CommandsRetired { generation, report } => {
                    for c in report.outputs {
                        ids.push((generation, c.accepted.id));
                    }
                }
                _ => panic!("wrong production family"),
            }
        }
        let generation = if resume { 2 } else { 1 };
        for c in &exit.commands.as_ref().unwrap().as_ref().unwrap().outputs {
            ids.push((generation, c.accepted.id));
        }
        ids.sort();
        let expected = if resume {
            vec![(1, 1), (1, 2), (2, 1)]
        } else {
            vec![(1, 1), (1, 2)]
        };
        assert_eq!(ids, expected);
        assert!(
            first
                .wait()
                .await
                .unwrap()
                .result
                .as_ref()
                .unwrap()
                .command
                .result
                .is_ok()
        );
        assert!(
            second
                .wait()
                .await
                .unwrap()
                .result
                .as_ref()
                .unwrap()
                .command
                .result
                .is_ok()
        );
        if let Some(t) = latest {
            assert!(
                t.wait()
                    .await
                    .unwrap()
                    .result
                    .as_ref()
                    .unwrap()
                    .command
                    .result
                    .is_ok()
            );
        }
    }
    println!(
        "Configured on-demand root: bounded output pressure pauses replacement beyond startup timeout, then resume or shutdown preserves every old/new command result"
    );
}

struct Release(Option<mpsc::Sender<()>>);
impl Release {
    fn open(&mut self) {
        self.0.take().unwrap().send(()).unwrap();
    }
}
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(s) = self.0.take() {
            let _ = s.send(());
        }
    }
}
struct Gate {
    db: Arc<ae_storage::Database>,
    revision: u64,
    entered: mpsc::SyncSender<()>,
    release: Mutex<Option<mpsc::Receiver<()>>>,
    calls: AtomicU64,
    reverse: bool,
}
impl generator::Clock for Gate {
    fn unix_millis(&self) -> i128 {
        let n = self.calls.fetch_add(1, Ordering::Relaxed);
        if self.db.view().unwrap().status().revision >= self.revision
            && let Some(r) = self.release.lock().unwrap().take()
        {
            self.entered.send(()).unwrap();
            r.recv().unwrap();
        }
        if self.reverse {
            if n < 2 { 1_000_000 } else { 0 }
        } else {
            1_000_000 + i128::from(n) * 10_000
        }
    }
}
fn gated(lab: &Lab, reverse: bool) -> (root::Running, mpsc::Receiver<()>, Release) {
    let p = prepared(lab, true);
    let db = p.configured().chain().database().clone();
    let (sent, entered) = mpsc::sync_channel(1);
    let (release, recv) = mpsc::channel();
    let clock = Arc::new(Gate {
        db,
        revision: if reverse { 1 } else { 2 },
        entered: sent,
        release: Mutex::new(Some(recv)),
        calls: AtomicU64::new(0),
        reverse,
    });
    let running = root::launch_commands(p, config(lab), clock, || Ok(Default::default())).unwrap();
    (running, entered, Release(Some(release)))
}

#[tokio::test]
async fn configured_command_root_shutdown_and_drop_retain_partial_work_and_cancelled_queue() {
    for implicit in [false, true] {
        let lab = Lab::new();
        let (running, entered, mut release) = gated(&lab, false);
        let h = running.handle();
        let mut running = Some(running);
        root_ready(&h, 1).await;
        let commands = h.commands().unwrap();
        let active = commands
            .client
            .submit(Command::Blocks {
                count: 3,
                kind: Kind::Key,
            })
            .unwrap();
        tokio::task::spawn_blocking(move || entered.recv_timeout(Duration::from_secs(5)).unwrap())
            .await
            .unwrap();
        let queued = commands.client.submit(Command::Micro).unwrap();
        let queued2 = commands.client.submit(Command::Key).unwrap();
        if implicit {
            drop(running.take());
        } else {
            h.request_shutdown();
        }
        within(async {
            while commands.client.status().accepting {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        release.open();
        let exit = if let Some(running) = running {
            within(running.wait()).await.unwrap()
        } else {
            within(async {
                while h.status().running {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await;
            h.take_exit().unwrap()
        };
        assert_root_joined(&exit);
        assert!(exit.status.failure.is_none(), "{:?}", exit.status);
        assert!(h.take_exit().is_none());
        let active = active.wait().await.unwrap();
        let r = active.result.as_ref().unwrap();
        assert_eq!(r.admissions.len(), 1);
        assert!(r.admissions[0].result.is_ok());
        assert_eq!(r.command.produced.len(), 1);
        assert!(matches!(
            r.command.result,
            Err(on_demand::control::Error::Host(runtime::Error::Stopped))
        ));
        assert!(matches!(
            queued.wait().await.unwrap().result,
            Err(service::Error::Cancelled)
        ));
        assert!(matches!(
            queued2.wait().await.unwrap().result,
            Err(service::Error::Cancelled)
        ));
        assert_eq!(
            exit.commands
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .status
                .completed,
            3
        );
        assert_eq!(
            exit.services
                .as_ref()
                .unwrap()
                .configured()
                .chain()
                .database()
                .view()
                .unwrap()
                .status()
                .revision,
            2
        );
    }
    println!(
        "Configured on-demand root: shutdown or dropped root consumes an active committed prefix, cancels the untouched queue and retains a single joined receipt"
    );
}

#[tokio::test]
async fn configured_command_root_rejects_wrong_family_and_unwinds_partial_startup() {
    let lab = Lab::new();
    let failure = root::launch_commands(lab.public(), config(&lab), Arc::new(Clock), || {
        Ok(Default::default())
    })
    .err()
    .unwrap();
    assert_eq!(failure.error, root::Error::Configuration);
    let running = launch_root(&lab, failure.prepared);
    root_ready(&running.handle(), 1).await;
    assert_root_joined(&within(running.shutdown()).await.unwrap());
    let lab = Lab::new();
    let mut cfg = config(&lab);
    cfg.production.controller.event_catchup = 0;
    let running = root::launch_commands(prepared(&lab, false), cfg, Arc::new(Clock), || {
        Ok(Default::default())
    })
    .unwrap();
    let exit = within(running.wait()).await.unwrap();
    assert_root_joined(&exit);
    assert_eq!(
        exit.status.failure,
        Some(root::Error::Start("OnDemand(Configuration)".into()))
    );
    assert_eq!(exit.status.production_starts, 0);
    assert!(exit.status.connections.as_ref().unwrap().joined);
    assert!(exit.services.is_some());
    assert!(exit.commands.is_none());
}

#[tokio::test]
async fn configured_command_root_stops_after_failed_admission_without_replacement_or_replay() {
    let lab = Lab::new();
    let (running, entered, mut release) = gated(&lab, true);
    let h = running.handle();
    root_ready(&h, 1).await;
    let c = h.commands().unwrap();
    let failed = c.client.submit(Command::Key).unwrap();
    tokio::task::spawn_blocking(move || entered.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();
    let queued = c.client.submit(Command::Key).unwrap();
    release.open();
    let exit = within(running.wait()).await.unwrap();
    assert_root_joined(&exit);
    assert_eq!(
        exit.status.failure,
        Some(root::Error::Commands(service::Error::RecoveryRequired))
    );
    assert_eq!(exit.status.production_starts, 1);
    let failed = failed.wait().await.unwrap();
    let r = failed.result.as_ref().unwrap();
    assert_eq!(r.admissions.len(), 1);
    assert!(r.admissions[0].result.is_err());
    assert!(r.recovery_required);
    assert!(matches!(
        queued.wait().await.unwrap().result,
        Err(service::Error::Cancelled)
    ));
    assert_eq!(
        exit.services
            .as_ref()
            .unwrap()
            .configured()
            .chain()
            .database()
            .view()
            .unwrap()
            .status()
            .revision,
        1
    );
    println!(
        "Configured on-demand root: failed normal admission ends the root with its original journal and cancelled queued request, without command replay or automatic replacement"
    );
}
