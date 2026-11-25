use ae_chain::{
    block::Block,
    hyperchain::candidate::{Packing, Timeout},
    repository::{self, Identity, pow},
};
use ae_codec::headers::KeyHeader;
use ae_crypto::Ed25519SecretKey;
use ae_node::{
    bootstrap::Opened,
    configuration::{
        Configuration,
        chain::Settings,
        resolution::{Context, Directories},
    },
};
use ae_protocol::{ConsensusAlgorithm, ConsensusType};
use ae_state::{Trees, accounts::Account, contracts::Contracts};
use ae_storage::snapshots;
use ae_sync::chain::{
    Importer,
    pow::{
        generator::Clock,
        preparation::{Builder, Request},
    },
};
use ae_tx::{CanonicalTransaction, SignedTransaction, Spend, Transaction};
use ae_types::{AccountPublicKey, Hash256, Id, IdTag};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
#[path = "../../ae-sync/tests/common/ledger_fixtures.rs"]
mod execution_fixture;

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(std::env::temp_dir().join(format!(
            "ae-on-demand-{}-{}",
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
struct Fixed(i128);
impl Clock for Fixed {
    fn unix_millis(&self) -> i128 {
        self.0
    }
}
fn candidate(
    builder: &Builder<ae_vm_fate::maps::order::Otp26>,
    parent: Hash256,
    time: i128,
) -> KeyHeader {
    let done = builder
        .start(
            Request {
                parent,
                miner: leader(),
                beneficiary: leader(),
                release_info: 0,
            },
            Arc::new(Fixed(time)),
        )
        .unwrap()
        .wait()
        .unwrap()
        .result
        .unwrap();
    assert_eq!(done.header.target, 1338);
    assert_eq!(done.header.nonce, 0);
    assert_eq!(done.header.seal, [0; 42]);
    let temporary = done.state.value.candidate.temporary();
    let hash = ae_codec::headers::Header::Key(temporary.clone())
        .hash()
        .unwrap();
    let history = ae_execution::chain_history::select(
        &done.state.value.history,
        temporary.common.height - 1,
        hash.0,
        || Ok(()),
    )
    .unwrap();
    assert_eq!(history.branch, temporary.common.previous_key_hash);
    done.header
}
async fn accept(importer: &Importer, block: &Block) -> Hash256 {
    importer
        .import_created_block(block.encode().unwrap(), 1000)
        .unwrap()
        .wait()
        .await
        .unwrap();
    block.hash().unwrap()
}
async fn reject(importer: &Importer, db: &ae_storage::Database, block: Block) {
    let before = db.view().unwrap().status();
    assert!(
        importer
            .import_created_block(block.encode().unwrap(), 1000)
            .unwrap()
            .wait()
            .await
            .is_err()
    );
    assert_eq!(db.view().unwrap().status(), before);
}

#[test]
fn algorithm_identity_preserves_existing_bindings_and_rejects_cross_open() {
    use ae_trie::Value::{self, Bytes, List};
    let s = settings();
    let trees = trees();
    let tmp = Temp::new();
    let opened =
        Opened::open(&tmp.0, s.clone(), &trees, fingerprint(), Default::default()).unwrap();
    let id = opened.identity().clone();
    assert_eq!(id.algorithm, ConsensusAlgorithm::OnDemand);
    assert_eq!(id.consensus, ConsensusType::Pow);
    for (algorithm, schema, binding) in [
        (ConsensusAlgorithm::Cuckoo, 21, 7),
        (ConsensusAlgorithm::Hyperchain, 21, 10),
        (ConsensusAlgorithm::OnDemand, 22, 22),
    ] {
        let i = Identity::for_algorithm(s.profile(), id.genesis, fingerprint(), algorithm);
        assert_eq!(Identity::decode(&i.encode()).unwrap(), i);
        for (bytes, version) in [(i.encode(), schema), (i.binding(), binding)] {
            let List(fields) = Value::decode(&bytes).unwrap() else {
                panic!()
            };
            assert_eq!(fields[0], Bytes(vec![version]));
            assert_eq!(
                fields.len(),
                if version < 10 {
                    5
                } else if version < 22 {
                    6
                } else {
                    7
                }
            );
        }
    }
    let List(fields) = Value::decode(&id.encode()).unwrap() else {
        panic!()
    };
    for (field, value) in [
        (0, Bytes(vec![23])),
        (5, Bytes(vec![1])),
        (6, Bytes(vec![])),
        (6, Bytes(vec![2])),
        (1, Bytes(b"ae_mainnet".to_vec())),
        (1, Bytes(b"ae_uat".to_vec())),
    ] {
        let mut corrupt = fields.clone();
        corrupt[field] = value;
        assert!(Identity::decode(&List(corrupt).encode()).is_err());
    }
    let status = opened.database().view().unwrap().status();
    drop(opened);
    let ordinary = Identity::for_algorithm(
        s.profile(),
        id.genesis,
        fingerprint(),
        ConsensusAlgorithm::Cuckoo,
    );
    assert!(matches!(
        ae_storage::Database::open(&tmp.0, ordinary),
        Err(repository::Error::Identity)
    ));
    let reopened = Opened::open(&tmp.0, s, &trees, fingerprint(), Default::default()).unwrap();
    assert_eq!(reopened.database().view().unwrap().status(), status);
}

#[tokio::test]
async fn actual_candidates_imports_transactions_forks_and_reopen() {
    let tmp = Temp::new();
    let s = settings();
    let initial = trees();
    let opened = Opened::open(
        &tmp.0,
        s.clone(),
        &initial,
        fingerprint(),
        Default::default(),
    )
    .unwrap();
    let db = opened.database();
    let identity = opened.identity().clone();
    let owned = execution_fixture::execution(s.profile());
    let rules = execution_fixture::rules(identity.genesis);
    let snapshots = snapshots::Owner::start(db.clone(), Default::default()).unwrap();
    let builder = Builder::new(
        snapshots.clone(),
        ae_sync::chain::pow::preparation::Config {
            identity: identity.clone(),
            execution: Arc::new(owned.clone()),
            rules: Arc::new(rules.clone()),
            limits: Default::default(),
        },
    )
    .unwrap();
    let mut importer_rules = rules;
    // Native on-demand ignores the consensus-plugin checkpoint callback.
    importer_rules.checkpoints.insert(1, Hash256([255; 32]));
    let importer = Importer::new(
        db.clone(),
        owned.clone(),
        importer_rules,
        Default::default(),
    )
    .unwrap();
    assert!(matches!(
        importer.stratum_builder(snapshots.clone(), Default::default()),
        Err(ae_sync::chain::pow::producer::mining::Error::Binding)
    ));
    let completion = builder
        .start(
            Request {
                parent: identity.genesis,
                miner: leader(),
                beneficiary: leader(),
                release_info: 0,
            },
            Arc::new(Fixed(0)),
        )
        .unwrap()
        .wait()
        .unwrap();
    assert!(matches!(
        ae_sync::chain::pow::stratum::Prepared::from_completion(completion),
        Err(ae_sync::chain::pow::stratum::Error::Binding)
    ));
    assert_eq!(
        db.view()
            .unwrap()
            .require_record(identity.genesis)
            .unwrap()
            .difficulty,
        0.into()
    );
    let first = candidate(&builder, identity.genesis, 0);
    let mut wrong = first.clone();
    wrong.target = 0;
    reject(&importer, db, Block::Key(wrong)).await;
    let mut wrong = first.clone();
    wrong.common.version = 1;
    wrong.info = None;
    reject(&importer, db, Block::Key(wrong)).await;
    let mut wrong = first.clone();
    wrong.time = 541_000;
    reject(&importer, db, Block::Key(wrong)).await;
    let first_hash = accept(&importer, &Block::Key(first)).await;

    let config = owned.with_config(|c| {
        let f = c.fate.unwrap();
        pow::Config {
            identity: identity.clone(),
            profile: Arc::new(ae_hyperchain::chain::Profile {
                contracts: ae_hyperchain::consensus_contract::Contracts {
                    owner: AccountPublicKey([0; 32]),
                    election: [0; 32],
                    staking: [0; 32],
                    rewards: [0; 32],
                },
                network: c.network.clone(),
                context: f.context.clone(),
                order: f.order.clone(),
                overrides: f.overrides.clone(),
                diagnostic: f.diagnostic_profile,
                limits: c.fate_limits,
                maximum_steps: c.maximum_fate_steps,
                maximum_history_reads: 1024,
            }),
            execution: pow::Execution {
                aevm: c.aevm.cloned(),
                ..Default::default()
            },
            micro_cycle_ms: 3000,
        }
    });
    let prepared = ae_storage::pow::prepare_micro(
        &snapshots,
        &config,
        pow::Input {
            parent: first_hash,
            now: 1,
            limits: Default::default(),
        },
    )
    .unwrap();
    let p = prepared.value;
    assert_eq!(p.candidate.environment().difficulty, 1338.into());
    let view = p
        .reader
        .view(&ae_hyperchain::chain::RunEnv::Hash(first_hash.0.to_vec()))
        .unwrap();
    assert_eq!(view.environment.difficulty, 1338.into());
    assert_eq!(
        view.aevm.as_ref().unwrap().environment.difficulty,
        1338.into()
    );
    drop(view);
    let mut spend = Spend::new(leader(), Id::new(IdTag::Account, [8; 32])).unwrap();
    spend.set_amount(123.into());
    spend.set_nonce(1.into());
    spend.set_fee(30_000_000_000_000u64.into());
    let tx = Transaction::decode(&spend.encode()).unwrap();
    let mut bytes = s.profile().signing_prefix(false);
    bytes.extend(tx.encode());
    let tx = SignedTransaction::new(tx, vec![secret().sign(&bytes)]);
    let mut packing =
        Packing::new::<()>(p.candidate, 0.into(), 0, Timeout::Disabled, 3000).unwrap();
    assert!(packing.request::<()>(|| 0).unwrap().is_some());
    let applied = packing
        .selected::<(), _, _>(&[tx], false, &p.reader)
        .unwrap();
    assert!(applied.rejected.is_empty());
    assert!(packing.request::<()>(|| 0).unwrap().is_some());
    packing.selected::<(), _, _>(&[], false, &p.reader).unwrap();
    let micro = packing
        .finish::<()>()
        .unwrap()
        .signed::<()>(&ae_hyperchain::signing::Keys::new([secret()]), leader())
        .unwrap();
    assert_eq!(micro.transactions.len(), 1);
    let mut wrong = micro.clone();
    wrong.header.transactions_hash = [1; 32];
    reject(&importer, db, Block::Micro(wrong)).await;
    let mut wrong = micro.clone();
    wrong.header.common.state_hash = [1; 32];
    reject(&importer, db, Block::Micro(wrong)).await;
    let mut wrong = micro.clone();
    wrong.header.time = 0;
    reject(&importer, db, Block::Micro(wrong)).await;
    let mut wrong = micro.clone();
    wrong.transactions[0] =
        SignedTransaction::new(wrong.transactions[0].transaction().clone(), vec![]);
    wrong.header.transactions_hash = ae_trie::transaction_root(
        wrong
            .transactions
            .iter()
            .map(|t| t.encode())
            .collect::<Vec<_>>()
            .iter()
            .map(Vec::as_slice),
    )
    .unwrap()
    .0;
    reject(&importer, db, Block::Micro(wrong)).await;
    let micro_hash = accept(&importer, &Block::Micro(micro.clone())).await;
    let mut unsigned = micro;
    unsigned.header.signature = [0; 64];
    accept(&importer, &Block::Micro(unsigned)).await;
    assert_eq!(db.view().unwrap().status().tip, Some(micro_hash));
    drop(p.reader);
    let mut parent = micro_hash;
    let mut main = vec![identity.genesis, first_hash];
    for height in 2..=20 {
        // Equal/backwards key times are legal; no NG median or retarget window.
        parent = accept(&importer, &Block::Key(candidate(&builder, parent, 0))).await;
        main.push(parent);
        let record = db.view().unwrap().require_record(parent).unwrap();
        assert_eq!(record.difficulty, (height * 1338u64).into());
    }
    let top = parent;
    let mut fork = main[18];
    for (height, time) in [(19, 2000), (20, 2001), (21, 2002)] {
        fork = accept(&importer, &Block::Key(candidate(&builder, fork, time))).await;
        assert_eq!(
            db.view().unwrap().status().tip,
            Some(if height <= 20 { top } else { fork })
        );
    }
    let status = db.view().unwrap().status();
    let roots = db.view().unwrap().require_record(fork).unwrap().roots;
    assert_eq!(status.revision, 26);
    let state = db.view().unwrap().trees(fork, Default::default()).unwrap();
    assert_eq!(
        state.accounts.lookup(leader()).unwrap().unwrap().nonce(),
        &1.into()
    );
    assert_eq!(
        state
            .accounts
            .lookup(AccountPublicKey([8; 32]))
            .unwrap()
            .unwrap()
            .balance(),
        &123.into()
    );
    drop(state);
    importer.stop();
    importer.join().await;
    drop(importer);
    drop(builder);
    drop(snapshots);
    drop(opened);
    let reopened = Opened::open(&tmp.0, s, &initial, fingerprint(), Default::default()).unwrap();
    let view = reopened.database().view().unwrap();
    assert_eq!(view.status(), status);
    assert_eq!(view.require_record(fork).unwrap().roots, roots);
    view.verify_tip().unwrap();
    assert_eq!(
        view.require_record(fork).unwrap().difficulty,
        (21u64 * 1338).into()
    );
    println!(
        "On-demand durable pipeline: 23 key blocks, 2 micro blocks, 7 rejected blocks, transaction state, fork selection and reopen"
    );
}
