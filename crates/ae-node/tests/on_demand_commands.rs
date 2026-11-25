use ae_chain::{block::Block, publication, repository::ledger};
use ae_crypto::Ed25519SecretKey;
use ae_node::{
    bootstrap::Opened,
    configuration::{
        Configuration,
        chain::Settings,
        resolution::{Context, Directories},
    },
};
use ae_state::{Trees, accounts::Account, contracts::Contracts};
use ae_storage::{pool, snapshots};
use ae_sync::{
    chain::{
        Importer,
        on_demand::{
            control::{self, Command, Kind, Reply},
            runtime::{self, Controller},
        },
        pow::generator::Clock,
    },
    keys,
};
use ae_tx::{CanonicalTransaction, SignedTransaction, Spend, Transaction};
use ae_types::{AccountPublicKey, Hash256, Id, IdTag};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
#[path = "../../ae-sync/tests/common/ledger_fixtures.rs"]
mod execution_fixture;
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(std::env::temp_dir().join(format!(
            "ae-on-demand-commands-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        if self.0.exists() {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
}
fn settings() -> Settings {
    let cfg = Configuration::empty();
    let ctx = Context::new(Directories {
        home: "/release".into(),
        data: "/data".into(),
        log: "/log".into(),
    })
    .unwrap();
    Settings::resolve(&cfg.resolve(&ctx).unwrap(), Some(b"ae_dev")).unwrap()
}
fn trees() -> Trees {
    let mut trees = Trees {
        accounts: Default::default(),
        contracts: Contracts::new(Default::default()),
        calls: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    trees
        .accounts
        .enter(&Account::new(leader(), 1_000_000_000_000_000_000u64.into()))
        .unwrap();
    trees
}
fn secret() -> Ed25519SecretKey {
    Ed25519SecretKey::from_seed(&[7; 32])
}
fn leader() -> AccountPublicKey {
    secret().public_key()
}
fn fingerprint() -> Hash256 {
    ae_crypto::blake2b_256(b"explicit-on-demand-test-execution-profile")
}

struct Tick(AtomicU64);
impl Clock for Tick {
    fn unix_millis(&self) -> i128 {
        self.0.fetch_add(100, Ordering::Relaxed).into()
    }
}
struct Node {
    importer: Importer,
    pool: Option<pool::worker::Worker>,
    keys: Option<keys::Worker>,
    snapshots: snapshots::Owner,
    opened: Opened,
    _tmp: Temp,
}
impl Node {
    fn new() -> Self {
        Self::with_signature_budget(None)
    }
    fn with_signature_budget(budget: Option<usize>) -> Self {
        let tmp = Temp::new();
        let s = settings();
        let opened = Opened::open(
            tmp.0.join("chain"),
            s.clone(),
            &trees(),
            fingerprint(),
            Default::default(),
        )
        .unwrap();
        let db = opened.database().clone();
        let execution = execution_fixture::execution(s.profile());
        let execution = budget.map_or_else(
            || execution.clone(),
            |bytes| {
                execution.with_config(|base| {
                    ae_execution::transactions::owned::Owned::capture(
                        &ae_execution::transactions::Config {
                            signature_limits: ae_execution::transactions::SignatureLimits {
                                max_encoded_bytes: bytes,
                                ..base.signature_limits
                            },
                            ..*base
                        },
                    )
                    .unwrap()
                })
            },
        );
        let importer = Importer::new(
            db.clone(),
            execution,
            execution_fixture::rules(opened.identity().genesis),
            Default::default(),
        )
        .unwrap();
        let pool = pool::worker::Worker::spawn(
            db.clone(),
            s.profile().clone(),
            pool::Config {
                policy: ae_mempool::pool::Config {
                    minimum_miner_gas_price: 1.into(),
                    ..Default::default()
                },
                ..Default::default()
            },
            pool::worker::Settings {
                origins_interval: None,
                ..Default::default()
            },
        )
        .unwrap();
        let keys = keys::Worker::spawn(keys::Config::new(
            tmp.0.join("keys"),
            b"tiny local command test".to_vec(),
        ))
        .unwrap();
        let snapshots = snapshots::Owner::start(db, Default::default()).unwrap();
        Self {
            importer,
            pool: Some(pool),
            keys: Some(keys),
            snapshots,
            opened,
            _tmp: tmp,
        }
    }
    fn controller(&self, config: runtime::Config) -> Result<Controller, runtime::Error> {
        self.with_clock(config, Arc::new(Tick(AtomicU64::new(1000))))
    }
    fn with_clock(
        &self,
        config: runtime::Config,
        clock: Arc<dyn Clock>,
    ) -> Result<Controller, runtime::Error> {
        Controller::new(
            &self.importer,
            self.snapshots.clone(),
            self.pool.as_ref().unwrap().client(),
            self.keys.as_ref().unwrap().client(),
            clock,
            config,
        )
    }
    fn tx(&self, nonce: u64) -> SignedTransaction<Transaction> {
        let mut tx = Spend::new(leader(), Id::new(IdTag::Account, [21; 32])).unwrap();
        tx.set_nonce(nonce.into());
        tx.set_amount(17u64.into());
        tx.set_fee(100_000_000_000_000u64.into());
        let mut message = self.opened.settings().profile().signing_prefix(false);
        message.extend(tx.encode());
        SignedTransaction::new(
            Transaction::new(ae_tx::TransactionLeaf::Spend(tx)),
            vec![secret().sign(&message)],
        )
    }
    fn push(&self, nonce: u64) -> Hash256 {
        let tx = self.tx(nonce);
        let hash = tx.hash();
        self.pool
            .as_ref()
            .unwrap()
            .client()
            .push(Arc::new(tx), ae_mempool::pool::Event::Created, false)
            .unwrap()
            .wait()
            .unwrap();
        hash
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            pool.shutdown().unwrap();
        }
        if let Some(keys) = self.keys.take() {
            keys.shutdown().unwrap();
        }
    }
}
fn blocks(report: &runtime::Report) -> &[Arc<Block>] {
    let Reply::Blocks(blocks) = report.command.result.as_ref().unwrap() else {
        panic!("{report:?}")
    };
    blocks
}
fn kinds(blocks: &[Arc<Block>]) -> String {
    blocks
        .iter()
        .map(|b| {
            if matches!(b.as_ref(), Block::Key(_)) {
                'K'
            } else {
                'M'
            }
        })
        .collect()
}
#[test]
fn real_commands_rotate_keys_commit_transactions_and_preserve_publication_origins() {
    let node = Node::new();
    let mut controller = node.controller(runtime::Config::new(leader())).unwrap();
    let initial = controller.execute(Command::EmptyPool);
    assert_eq!(
        blocks(&initial)[0].hash().unwrap(),
        node.opened.identity().genesis
    );
    assert!(initial.admissions.is_empty());
    let cursor = node.opened.database().events().cursor();
    // Native count zero still emits an unreturned key when not the leader.
    let zero = controller.execute(Command::Blocks {
        count: 0,
        kind: Kind::Micro,
    });
    assert!(blocks(&zero).is_empty());
    assert_eq!(kinds(&zero.command.produced), "K");
    assert_eq!(zero.promotions.len(), 1);
    let first_signing = node.keys.as_ref().unwrap().info().keys.signing;
    let hash = node.push(1);
    let mined = controller.execute(Command::EmptyPool);
    assert_eq!(kinds(blocks(&mined)), "M");
    let Block::Micro(micro) = blocks(&mined)[0].as_ref() else {
        panic!()
    };
    assert_eq!(micro.transactions[0].hash(), hash);
    assert!(
        node.pool
            .as_ref()
            .unwrap()
            .client()
            .peek(1, None, None)
            .unwrap()
            .wait()
            .unwrap()
            .is_empty()
    );
    let hash = node.push(2);
    let encoded =
        ae_codec::api::encode(ae_codec::api::ApiType::TxHash, hash.as_bytes()).into_bytes();
    let until = controller.execute(Command::Until {
        hashes: vec![hash.0.to_vec(), encoded],
        maximum: 1,
    });
    assert_eq!(kinds(blocks(&until)), "K");
    assert_eq!(kinds(&until.command.produced), "MK");
    assert_ne!(
        node.keys.as_ref().unwrap().info().keys.signing,
        first_signing
    );
    let any = controller.execute(Command::Blocks {
        count: 5,
        kind: Kind::Any,
    });
    assert_eq!(kinds(blocks(&any)), "KMKMK");
    let direct = controller.execute(Command::Micro);
    assert!(direct.command.result.is_ok());
    let max = controller.execute(Command::Until {
        hashes: vec![vec![99; 32]],
        maximum: 1,
    });
    assert!(matches!(max.command.result, Ok(Reply::MaxReached)));
    assert_eq!(kinds(&max.command.produced), "MK");
    let events = node.opened.database().events().after(&cursor, 64).unwrap();
    let mut keys = 0;
    let mut micros = 0;
    for payload in events.records {
        let Some(block) = payload.context().block else {
            continue;
        };
        match block.kind {
            publication::Kind::Key => {
                keys += 1;
                assert_eq!(payload.context().origin, publication::Origin::Synced);
                assert!(!payload.effects().any(|e| matches!(
                    e,
                    publication::Effect::BlockToPublish { .. }
                        | publication::Effect::BlockCreated(_)
                )));
            }
            publication::Kind::Micro => {
                micros += 1;
                assert_eq!(payload.context().origin, publication::Origin::Received);
                assert!(payload.effects().any(|e| matches!(
                    e,
                    publication::Effect::BlockToPublish {
                        origin: publication::Propagation::Received,
                        ..
                    }
                )));
            }
        }
    }
    assert_eq!((keys, micros), (6, 6));
    assert!(
        zero.admissions
            .iter()
            .all(|a| matches!(a.origin, ledger::Origin::Sync))
    );
    assert!(
        mined
            .admissions
            .iter()
            .all(|a| matches!(a.origin, ledger::Origin::Gossip))
    );
    let db = node.opened.database();
    let view = db.view().unwrap();
    let state = view
        .trees(view.status().tip.unwrap(), Default::default())
        .unwrap();
    assert_eq!(
        state
            .accounts
            .lookup(AccountPublicKey([21; 32]))
            .unwrap()
            .unwrap()
            .balance(),
        &34u64.into()
    );
    println!(
        "On-demand runtime: actual owners, key rotation, empty-pool/until/any/direct commands, received/synced publication"
    );
}
#[test]
fn negative_counts_partial_effects_limits_and_exclusive_ownership() {
    let node = Node::new();
    let mut config = runtime::Config::new(leader());
    config.commands.blocks = 2;
    let mut controller = node.controller(config).unwrap();
    assert!(matches!(
        node.controller(runtime::Config::new(leader())),
        Err(runtime::Error::Snapshot(snapshots::Error::AlreadyRunning))
    ));
    let negative = controller.execute(Command::Blocks {
        count: -1,
        kind: Kind::Micro,
    });
    assert!(matches!(
        negative.command.result,
        Err(control::Error::FunctionClause)
    ));
    assert_eq!(kinds(&negative.command.produced), "K");
    let limited = controller.execute(Command::Blocks {
        count: 3,
        kind: Kind::Key,
    });
    assert!(matches!(
        limited.command.result,
        Err(control::Error::ResourceLimit)
    ));
    assert_eq!(kinds(&limited.command.produced), "KK");
    assert_eq!(limited.admissions.len(), 2);
    let unknown = controller.execute(Command::Blocks {
        count: 1,
        kind: Kind::Unknown(b"x".to_vec()),
    });
    assert!(matches!(
        unknown.command.result,
        Err(control::Error::UnknownKind { leader: true, .. })
    ));
    assert!(unknown.admissions.is_empty());
    let retired = controller.retire();
    assert!(node.controller(runtime::Config::new(leader())).is_err());
    drop(retired);
    let mut tiny = runtime::Config::new(leader());
    tiny.result_bytes = 1;
    let mut controller = node.controller(tiny).unwrap();
    let before = node.opened.database().view().unwrap().status();
    let result = controller.execute(Command::Key);
    assert!(matches!(
        result.command.result,
        Err(control::Error::Host(runtime::Error::ResourceLimit))
    ));
    assert!(result.admissions.is_empty());
    assert_eq!(node.opened.database().view().unwrap().status(), before);
}
#[test]
fn stop_during_preparation_consumes_the_task_without_publishing_or_restarting() {
    struct Gate {
        entered: std::sync::mpsc::SyncSender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        calls: AtomicU64,
    }
    impl Clock for Gate {
        fn unix_millis(&self) -> i128 {
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                self.entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
            }
            1000
        }
    }
    let node = Node::new();
    let before = node.opened.database().view().unwrap().status();
    let (sent, entered) = std::sync::mpsc::sync_channel(1);
    let (release, recv) = std::sync::mpsc::channel();
    let mut controller = node
        .with_clock(
            runtime::Config::new(leader()),
            Arc::new(Gate {
                entered: sent,
                release: std::sync::Mutex::new(recv),
                calls: AtomicU64::new(0),
            }),
        )
        .unwrap();
    let stop = controller.stop_handle();
    let task = std::thread::spawn(move || {
        let report = controller.execute(Command::Key);
        (controller, report)
    });
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    stop.request();
    release.send(()).unwrap();
    let (mut controller, report) = task.join().unwrap();
    assert!(matches!(
        report.command.result,
        Err(control::Error::Host(runtime::Error::Stopped))
    ));
    assert!(report.admissions.is_empty());
    assert_eq!(node.opened.database().view().unwrap().status(), before);
    assert!(matches!(
        controller.execute(Command::Key).command.result,
        Err(control::Error::Host(runtime::Error::Stopped))
    ));
}
#[test]
fn accepted_admission_failure_is_journaled_and_never_retried() {
    let node = Node::new();
    // First time is used for preparation, the final clock remains legal, but
    // admission sees an earlier instant and rejects its too-far future time.
    struct Reverse(AtomicU64);
    impl Clock for Reverse {
        fn unix_millis(&self) -> i128 {
            if self.0.fetch_add(1, Ordering::Relaxed) < 2 {
                1_000_000
            } else {
                0
            }
        }
    }
    let mut controller = node
        .with_clock(
            runtime::Config::new(leader()),
            Arc::new(Reverse(AtomicU64::new(0))),
        )
        .unwrap();
    let report = controller.execute(Command::Key);
    assert!(matches!(
        report.command.result,
        Err(control::Error::Host(runtime::Error::Admission(0)))
    ));
    assert_eq!(report.admissions.len(), 1);
    assert!(report.admissions[0].result.is_err());
    assert!(report.recovery_required);
    assert!(matches!(
        controller.execute(Command::Key).command.result,
        Err(control::Error::Host(runtime::Error::RecoveryRequired))
    ));
    assert_eq!(
        node.opened.database().view().unwrap().status().tip,
        Some(node.opened.identity().genesis)
    );
}

#[test]
fn micro_packing_preserves_explicit_transaction_limits() {
    let node = Node::with_signature_budget(Some(1));
    let mut controller = node.controller(runtime::Config::new(leader())).unwrap();
    assert!(controller.execute(Command::Key).command.result.is_ok());
    node.push(1);
    let before = node.opened.database().view().unwrap().status().tip;
    let report = controller.execute(Command::Micro);
    assert!(
        matches!(
            report.command.result,
            Err(control::Error::Host(runtime::Error::Packing(_)))
        ),
        "{report:?}"
    );
    assert!(report.admissions.is_empty());
    assert_eq!(node.opened.database().view().unwrap().status().tip, before);
    assert_eq!(
        node.pool
            .as_ref()
            .unwrap()
            .client()
            .peek(1, None, None)
            .unwrap()
            .wait()
            .unwrap()
            .len(),
        1
    );
}
#[test]
fn stopped_sources_and_wrong_databases_are_rejected() {
    let mut node = Node::new();
    let other = Node::new();
    let config = runtime::Config::new(leader());
    assert!(matches!(
        Controller::new(
            &node.importer,
            other.snapshots.clone(),
            node.pool.as_ref().unwrap().client(),
            node.keys.as_ref().unwrap().client(),
            Arc::new(Tick(AtomicU64::new(1000))),
            config.clone()
        ),
        Err(runtime::Error::Binding)
    ));
    assert!(matches!(
        Controller::new(
            &node.importer,
            node.snapshots.clone(),
            other.pool.as_ref().unwrap().client(),
            node.keys.as_ref().unwrap().client(),
            Arc::new(Tick(AtomicU64::new(1000))),
            config.clone()
        ),
        Err(runtime::Error::Binding)
    ));
    let mut controller = node.controller(config).unwrap();
    node.keys.take().unwrap().shutdown().unwrap();
    let report = controller.execute(Command::Key);
    assert!(matches!(
        report.command.result,
        Err(control::Error::Host(runtime::Error::Stopped))
    ));
    assert!(report.admissions.is_empty());
    node.importer.stop();
    assert!(controller.execute(Command::Key).admissions.is_empty());
}

#[test]
fn selected_external_key_is_promoted_once_and_controller_restart_keeps_leadership() {
    use ae_sync::chain::pow::preparation::{Builder, Config, Request};
    let node = Node::new();
    let mut controller = node.controller(runtime::Config::new(leader())).unwrap();
    let keys = node.keys.as_ref().unwrap();
    let selected = keys.info().keys.candidate;
    let builder = Builder::new(
        node.snapshots.clone(),
        Config {
            identity: node.opened.identity().clone(),
            execution: Arc::new(execution_fixture::execution(
                node.opened.settings().profile(),
            )),
            rules: Arc::new(execution_fixture::rules(node.opened.identity().genesis)),
            limits: Default::default(),
        },
    )
    .unwrap();
    let prepared = builder
        .start(
            Request {
                parent: node.opened.identity().genesis,
                miner: selected,
                beneficiary: leader(),
                release_info: 0,
            },
            Arc::new(Tick(AtomicU64::new(1))),
        )
        .unwrap()
        .wait()
        .unwrap()
        .result
        .unwrap();
    let block = Block::Key(prepared.header);
    let mut ticket = node
        .importer
        .import_synced_block(block.encode().unwrap(), 1000)
        .unwrap();
    loop {
        if let Some(result) = ticket.try_result() {
            result.unwrap();
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_ne!(keys.info().keys.signing, selected);
    let no_calls = controller.execute(Command::Until {
        hashes: vec![],
        maximum: 0,
    });
    assert!(matches!(no_calls.command.result, Ok(Reply::MaxReached)));
    assert!(no_calls.promotions.is_empty());
    let zero = controller.execute(Command::Blocks {
        count: 0,
        kind: Kind::Micro,
    });
    assert!(zero.admissions.is_empty());
    assert_eq!(zero.promotions.len(), 1);
    assert_eq!(keys.info().keys.signing, selected);
    let before = node.opened.database().view().unwrap().status();
    drop(controller.retire());
    let mut config = runtime::Config::new(leader());
    config.resume_current = true;
    let mut controller = node.controller(config).unwrap();
    let repeated = controller.execute(Command::Blocks {
        count: 0,
        kind: Kind::Micro,
    });
    assert!(repeated.admissions.is_empty());
    assert!(repeated.promotions.is_empty());
    assert_eq!(node.opened.database().view().unwrap().status(), before);
    assert_eq!(keys.info().promotions, 1);
}

#[test]
fn pinned_readers_keep_vm_limits_while_rebinding_block_environment() {
    use ae_execution::transactions::{Config, owned::Owned};
    use ae_hyperchain::{chain, consensus_contract::Contracts as Addresses};
    let settings = settings();
    let base = execution_fixture::execution(settings.profile());
    let atoms = [b"explicit-channel-state-atom".to_vec()]
        .into_iter()
        .collect();
    let owned = Arc::new(base.with_config(|base| {
        Owned::capture(&Config {
            signature_limits: ae_execution::transactions::SignatureLimits {
                max_layers: 7,
                max_encoded_bytes: 503,
            },
            aevm_limits: ae_vm_aevm::execution::Limits {
                max_work: 101,
                max_bytes: 102,
                max_memory_words: 103,
                max_depth: 9,
            },
            channel_state_atoms: Some(&atoms),
            maximum_fate_steps: 117,
            ..*base
        })
        .unwrap()
    }));
    let (profile, mut view) = owned.with_config(|base| {
        let f = base.fate.unwrap();
        let profile = chain::Profile {
            contracts: Addresses {
                owner: leader(),
                election: [0; 32],
                staking: [0; 32],
                rewards: [0; 32],
            },
            network: base.network.clone(),
            context: f.context.clone(),
            order: f.order.clone(),
            overrides: f.overrides.clone(),
            diagnostic: f.diagnostic_profile,
            limits: base.fate_limits,
            maximum_steps: 999,
            maximum_history_reads: 42,
        };
        let view = chain::View {
            trees: Arc::new(trees()),
            context: ae_execution::Context::transaction(1, ae_protocol::Protocol::Ceres),
            environment: f.environment.clone(),
            history: f.history.clone(),
            aevm: base.aevm.cloned(),
        };
        (profile, view)
    });
    view.environment.key_hash = [17; 32];
    view.environment.timestamp = 37.into();
    view.aevm.as_mut().unwrap().environment.key_hash = [18; 32];
    let reader = chain::Reader::new(
        chain::Selected::new(chain::Selection::default()),
        Arc::new(profile),
    )
    .with_owned_execution(owned);
    let pinned = reader.pin().unwrap();
    for reader in [
        reader.with_execution(&view, |cfg| {
            assert_eq!(cfg.fate.unwrap().environment.key_hash, [17; 32]);
            assert_eq!(cfg.fate.unwrap().environment.timestamp, 37.into());
            assert_eq!(cfg.aevm.unwrap().environment.key_hash, [18; 32]);
            (
                cfg.signature_limits.max_layers,
                cfg.signature_limits.max_encoded_bytes,
                cfg.aevm_limits.max_depth,
                cfg.maximum_fate_steps,
                cfg.channel_state_atoms.unwrap().clone(),
            )
        }),
        pinned.with_execution(&view, |cfg| {
            (
                cfg.signature_limits.max_layers,
                cfg.signature_limits.max_encoded_bytes,
                cfg.aevm_limits.max_depth,
                cfg.maximum_fate_steps,
                cfg.channel_state_atoms.unwrap().clone(),
            )
        }),
    ] {
        assert_eq!(reader, (7, 503, 9, 117, atoms.clone()));
    }
}
