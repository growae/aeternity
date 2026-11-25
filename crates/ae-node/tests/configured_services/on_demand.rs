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
