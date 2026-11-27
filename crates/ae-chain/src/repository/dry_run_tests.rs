use super::*;
use crate::{
    dry_run::{Error as SelectionError, Returned, Selector},
    repository as repo,
};
use ae_codec::headers::{CommonHeader, HeaderMode, KeyHeader, MicroHeader};
use ae_execution::{contracts::aevm, dry_run as dry};
use ae_protocol::ConsensusAlgorithm;
use ae_state::accounts::Account;
use ae_types::AccountPublicKey;
use num_bigint::BigInt;

const ACCOUNT: AccountPublicKey = AccountPublicKey([19; 32]);
fn network(algorithm: ConsensusAlgorithm) -> NetworkProfile {
    if algorithm == ConsensusAlgorithm::Cuckoo {
        NetworkProfile::mainnet()
    } else {
        NetworkProfile::custom(
            b"ae_dry_run_test".to_vec(),
            vec![ae_protocol::Activation {
                height: 0,
                protocol: ae_protocol::Protocol::Ceres,
            }],
        )
        .unwrap()
    }
}
// Authenticated repository fixtures. These exercise reader binding; the
// synthetic headers are not evidence of successful consensus admission.
fn insert(
    db: &mut Memory,
    height: u64,
    parents: (Hash256, Hash256),
    time: u64,
    balance: u64,
    micro: bool,
    config: ReadConfig,
) -> Hash256 {
    let (previous, leader) = parents;
    let mut accounts = Accounts::new(MemoryStore::default());
    accounts
        .enter(&Account::new(ACCOUNT, balance.into()))
        .unwrap();
    let record = Record {
        roots: Roots {
            state: StateRoots {
                accounts: accounts.root_hash(),
                ..Default::default()
            },
            names: None,
            oracles: None,
        },
        difficulty: 1.into(),
        fees: 0.into(),
        fraud: false,
        fork: Hash256::ZERO,
    };
    let common = CommonHeader {
        version: 6,
        height,
        previous_hash: previous.0,
        previous_key_hash: leader.0,
        state_hash: record.roots.state.hash().0,
    };
    let block = if micro {
        Block::Micro(crate::block::MicroBlock {
            header: MicroHeader {
                common,
                time,
                transactions_hash: [0; 32],
                proof_of_fraud: None,
                signature: [0; 64],
            },
            transactions: vec![],
            proof: None,
        })
    } else {
        Block::Key(KeyHeader {
            common,
            miner: [2; 32],
            beneficiary: [3; 32],
            target: 0x1f012345,
            seal: [0; 42],
            nonce: 0,
            time,
            info: Some([0; 4]),
            hole: false,
            end_of_epoch: false,
        })
    };
    let hash = block.hash().unwrap();
    let mut writes = Writes::new();
    store_block(&mut writes, &block, &record, &config).unwrap();
    for (h, b) in accounts.tree().store().nodes() {
        writes.insert(key(NODES, h.0), Some(b.clone()));
    }
    db.apply(writes);
    hash
}
fn snapshot(db: &Memory, id: &Identity, config: ReadConfig) -> repo::execution::Snapshot<Memory> {
    let aevm = aevm::Config {
        environment: aevm::Environment {
            genesis_height: 99,
            key_hash: [99; 32],
            history: Arc::default(),
            beneficiary: ACCOUNT,
            difficulty: 999.into(),
            timestamp_ms: 999.into(),
            block_gas_limit: 1.into(),
            auth_tx_hash: Some(vec![99; 32]),
        },
        network_id: Arc::from(&b"incorrect-template-network"[..]),
        event_context: ae_execution::aevm_chain::EventContext::None,
        float_profile: ae_vm_aevm::gas::FloatProfile::Otp26Word64,
        store_order: ae_vm_aevm::store::OrderProfile::Otp26Word64Jenkins,
    };
    repo::execution::snapshot(
        View::load_with_config(db.clone(), config).unwrap(),
        id,
        &network(id.algorithm),
        repo::execution::Config {
            aevm: Some(aevm),
            ..Default::default()
        },
    )
    .unwrap()
}
fn select(
    snapshot: &repo::execution::Snapshot<Memory>,
    selector: Selector,
    hash: Hash256,
    time: u64,
    balance: u64,
    difficulty: &BigInt,
) {
    let view = snapshot.dry_run_view(&selector).unwrap();
    assert_eq!(view.context.height, 1);
    assert_eq!(view.context.protocol, ae_protocol::Protocol::Ceres);
    assert!(!view.context.dry_run);
    assert_eq!(view.environment.timestamp, BigInt::from(time));
    assert_eq!(view.environment.key_hash, hash.0);
    assert_eq!(&view.environment.difficulty, difficulty);
    assert_eq!(view.environment.beneficiary, [3; 32]);
    assert_eq!(
        view.environment.network_id,
        snapshot.repository().identity().network
    );
    assert_eq!(
        view.trees
            .accounts
            .lookup(ACCOUNT)
            .unwrap()
            .unwrap()
            .balance(),
        &balance.into()
    );
    let aevm = view.aevm.as_ref().unwrap();
    assert_eq!(aevm.environment.genesis_height, 0);
    assert_eq!(aevm.environment.timestamp_ms, view.environment.timestamp);
    assert_eq!(aevm.environment.key_hash, view.environment.key_hash);
    assert_eq!(aevm.environment.beneficiary.0, view.environment.beneficiary);
    assert_eq!(aevm.environment.difficulty, view.environment.difficulty);
    assert_eq!(
        aevm.environment.block_gas_limit,
        view.environment.block_gas_limit
    );
    assert_eq!(aevm.network_id.as_ref(), view.environment.network_id);
    assert!(aevm.environment.auth_tx_hash.is_none());
    assert!(Arc::ptr_eq(&aevm.environment.history, &view.history));
    let before = view.trees.serialize_to_binary(Default::default()).unwrap();
    let setup = dry::setup(
        (*view.trees).clone(),
        &view.context,
        &[dry::Funding {
            account: ACCOUNT,
            amount: (-5).into(),
        }],
        &Default::default(),
    )
    .unwrap();
    assert_eq!(
        setup
            .trees()
            .accounts
            .lookup(ACCOUNT)
            .unwrap()
            .unwrap()
            .balance(),
        &(balance - 5).into()
    );
    assert_eq!(
        view.trees.serialize_to_binary(Default::default()).unwrap(),
        before
    );
}
#[test]
fn captured_repository_selectors_keep_roots_vm_environments_and_reorg_revision() {
    for algorithm in [
        ConsensusAlgorithm::Cuckoo,
        ConsensusAlgorithm::OnDemand,
        ConsensusAlgorithm::Hyperchain,
    ] {
        let config = ReadConfig {
            mode: if algorithm == ConsensusAlgorithm::Hyperchain {
                HeaderMode::Hyperchain
            } else {
                HeaderMode::ProofOfWork
            },
            ..Default::default()
        };
        let mut db = Memory::default();
        let g = insert(
            &mut db,
            0,
            (Hash256::ZERO, Hash256::ZERO),
            0,
            100,
            false,
            config.clone(),
        );
        let k = insert(&mut db, 1, (g, g), 1000, 200, false, config.clone());
        let m1 = insert(&mut db, 1, (k, k), 1100, 300, true, config.clone());
        let m2 = insert(&mut db, 1, (m1, k), 1200, 400, true, config.clone());
        let id = Identity::for_algorithm(&network(algorithm), g, Hash256::ZERO, algorithm);
        let values = Arc::make_mut(&mut db.kv);
        values.insert(IDENTITY.to_vec(), id.encode());
        values.insert(
            STATUS.to_vec(),
            Status {
                revision: 1,
                tip: Some(m2),
            }
            .encode(),
        );
        for h in [g, k, m1, m2] {
            values.insert(key(CANONICAL, h.0), vec![]);
        }
        for (height, h) in [(0u64, g), (1, k)] {
            values.insert(key(KEYS, height.to_be_bytes()), h.0.to_vec());
        }
        let original = db.kv.clone();
        let pinned = snapshot(&db, &id, config.clone());
        let difficulty = match algorithm {
            ConsensusAlgorithm::Cuckoo => BigInt::from_bytes_be(
                num_bigint::Sign::Plus,
                Target(0x1f012345).difficulty().unwrap().as_bytes(),
            ),
            ConsensusAlgorithm::OnDemand => BigInt::from(ae_consensus::on_demand::TAG),
            ConsensusAlgorithm::Hyperchain => BigInt::from(0x1f012345u32),
        };
        for (selector, time, balance) in [
            (Selector::Top, 1200, 400),
            (Selector::Height(1.into()), 1000, 200),
            (Selector::Hash(k), 1001, 200),
            (Selector::Hash(m1), 1100, 300),
            (Selector::InMicro(m1), 1100, 200),
            (Selector::InMicro(m2), 1200, 300),
        ] {
            select(&pinned, selector, k, time, balance, &difficulty);
        }
        assert_eq!(db.kv, original);
        let other = insert(&mut db, 1, (g, g), 2000, 500, false, config.clone());
        let values = Arc::make_mut(&mut db.kv);
        values.insert(
            STATUS.to_vec(),
            Status {
                revision: 2,
                tip: Some(other),
            }
            .encode(),
        );
        values.insert(key(KEYS, 1u64.to_be_bytes()), other.0.to_vec());
        values.insert(key(CANONICAL, other.0), vec![]);
        for h in [k, m1, m2] {
            values.remove(&key(CANONICAL, h.0));
        }
        let changed = snapshot(&db, &id, config.clone());
        select(&changed, Selector::Top, other, 2001, 500, &difficulty);
        select(
            &changed,
            Selector::Height(1.into()),
            other,
            2000,
            500,
            &difficulty,
        );
        select(&changed, Selector::Hash(k), k, 1001, 200, &difficulty);
        select(&pinned, Selector::Top, k, 1200, 400, &difficulty);
        select(
            &pinned,
            Selector::Height(1.into()),
            k,
            1000,
            200,
            &difficulty,
        );
        assert_eq!(pinned.repository().source().kv, original);
        let gc = repo::retention::State::default()
            .start(2, Default::default())
            .unwrap();
        Arc::make_mut(&mut db.kv).insert(GC_STATE.to_vec(), gc.encode());
        let pruned = snapshot(&db, &id, config);
        assert!(matches!(
            pruned.dry_run_view(&Selector::Hash(k)),
            Err(SelectionError::StateGarbageCollected)
        ));
        for selector in [Selector::Height(1.into()), Selector::InMicro(m2)] {
            assert!(matches!(
                pruned.dry_run_view(&selector),
                Err(SelectionError::Returned(Returned::StateGarbageCollected))
            ));
        }
        assert!(matches!(
            pruned.dry_run_view(&Selector::Hash(Hash256::ZERO)),
            Err(SelectionError::InvalidHash)
        ));
        select(&pinned, Selector::InMicro(m2), k, 1200, 300, &difficulty);
    }
}
#[test]
fn repository_selection_keeps_corruption_distinct_from_unavailable_state() {
    let config = ReadConfig::default();
    let mut db = Memory::default();
    let g = insert(
        &mut db,
        0,
        (Hash256::ZERO, Hash256::ZERO),
        0,
        100,
        false,
        config.clone(),
    );
    let id = Identity::new(&NetworkProfile::mainnet(), g, Hash256::ZERO);
    let values = Arc::make_mut(&mut db.kv);
    values.insert(IDENTITY.to_vec(), id.encode());
    values.insert(
        STATUS.to_vec(),
        Status {
            revision: 1,
            tip: Some(g),
        }
        .encode(),
    );
    let good = snapshot(&db, &id, config.clone());
    assert!(good.dry_run_view(&Selector::Top).is_ok());
    let record_key = key(RECORDS, g.0);
    let record = db.kv[&record_key].clone();
    Arc::make_mut(&mut db.kv).remove(&record_key);
    assert!(matches!(
        snapshot(&db, &id, config.clone()).dry_run_view(&Selector::Top),
        Err(SelectionError::StateGarbageCollected)
    ));
    let mut corrupt = Record::decode(&record).unwrap();
    corrupt.roots.state.accounts = None;
    Arc::make_mut(&mut db.kv).insert(record_key, corrupt.encode());
    assert!(matches!(
        snapshot(&db, &id, config).dry_run_view(&Selector::Top),
        Err(SelectionError::Source(
            ae_hyperchain::chain::LookupError::Backend(_)
        ))
    ));
    assert!(good.dry_run_view(&Selector::Top).is_ok());
}
