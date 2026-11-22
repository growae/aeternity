use super::*;
use crate::tests::Temp;
use ae_chain::repository::ledger::Origin;
use ae_codec::headers::{Header, HeaderMode};
use ae_hyperchain::{block as hc, chain, consensus_contract::Contracts};
use ae_protocol::{Activation, ConsensusType, NetworkProfile, Protocol};
use ae_state::{
    Trees,
    contracts::{Nonce, contract_id},
};
use ae_trie::{MemoryStore, Nibbles, Trie, Value as TrieValue};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use ae_vm_fate::{
    machine,
    maps::order::{Otp26, WordSize},
};
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use serde_json::Value as J;
use std::sync::{LazyLock, atomic::Ordering};
#[path = "../../../ae-vm-fate/tests/support/otp_order.rs"]
mod otp_order;
static DATA: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!(
        "../../../ae-hyperchain/tests/fixtures/erlang_hc_ledger.json"
    ))
    .unwrap()
});
mod recovery;
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Parent {
    reads: Vec<BigInt>,
    unavailable: bool,
}
impl Host for Parent {
    type Error = ();
    fn is_operational(_: &()) -> bool {
        true
    }
    fn entropy(&mut self, epoch: &BigInt) -> Result<Option<Vec<u8>>, ()> {
        self.reads.push(epoch.clone());
        if self.unavailable {
            return Err(());
        }
        let h: BigInt = (epoch - BigInt::from(3)).max(0.into()) * 5 + 1;
        Ok(Some(key(1000 + h.to_u64().unwrap()).to_vec()))
    }
    fn pin(&mut self, _: &[u8]) -> Result<Option<hc::Pin>, ()> {
        Ok(None)
    }
    fn key_hash(&mut self, _: &BigInt) -> Result<Option<Vec<u8>>, ()> {
        Ok(None)
    }
}
fn key(n: u64) -> [u8; 32] {
    let mut b = [0; 32];
    b[24..].copy_from_slice(&n.to_be_bytes());
    b
}
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn tree(j: &J) -> Trie<MemoryStore> {
    let mut t = Trie::new(MemoryStore::default());
    for row in j.as_array().unwrap() {
        t.put(
            &Nibbles::from_bytes(&bytes(&row["key"])),
            TrieValue::Bytes(bytes(&row["encoded"])),
        )
        .unwrap();
    }
    t
}
fn read_config() -> repo::ReadConfig {
    repo::ReadConfig {
        mode: HeaderMode::Hyperchain,
        ..Default::default()
    }
}
fn block(j: &J) -> Block {
    Block::decode(&bytes(&j["block"]), read_config().block()).unwrap()
}
fn setup(path: &Temp) -> (Arc<Database>, Config<Otp26>) {
    let row = &DATA["rows"][0];
    let network = NetworkProfile::custom(
        b"ae_hc_reference".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::Ceres,
        }],
    )
    .unwrap();
    let Header::Key(header) =
        Header::decode(&bytes(&row["genesis"]["header"]), HeaderMode::Hyperchain).unwrap()
    else {
        panic!()
    };
    let owner = AccountPublicKey(key(1));
    let staking = contract_id(owner, Nonce::Number(&Unsigned::from(1u64))).0;
    let election = contract_id(owner, Nonce::Number(&Unsigned::from(2u64))).0;
    let profile = chain::Profile {
        network: network.clone(),
        contracts: Contracts {
            owner,
            staking,
            election,
            rewards: election,
        },
        context: machine::Context {
            current_contract: election,
            vm_version: ae_vm_fate::types::Version::Three,
            in_auth: false,
            on_chain: true,
            reserved_gas: 0.into(),
            arithmetic_word_size: WordSize::Bits64,
            unicode_profile: ae_vm_fate::unicode::Profile::Otp26Unicode15,
            bls_profile: ae_crypto::bls::Profile::Mcl11976beLlvm64VintArm64,
        },
        order: otp_order::profile(&DATA["profile"]),
        overrides: Default::default(),
        diagnostic: ae_vm_fate::diagnostic::Profile::Release,
        limits: Default::default(),
        maximum_steps: 1_000_000,
        maximum_history_reads: 1000,
    };
    let hash = Header::Key(header.clone()).hash().unwrap();
    let mut rules = ae_chain::hyperchain::Rules::new(
        hash,
        hc::Config {
            parent_epoch_length: 5.into(),
            parent_start: 1.into(),
            parent_finality: 2.into(),
            pin_sync_margin: 1.into(),
            child_block_time: 4.into(),
            cache_limits: Default::default(),
        },
    );
    rules.reward_delay = 2;
    rules.fee_distribution = [10.into(), 70.into(), 20.into()];
    let config = Config {
        identity: repo::Identity::for_consensus(&network, hash, Hash256::ZERO, ConsensusType::Pos),
        profile: Arc::new(profile),
        rules,
        aevm: None,
        limits: Default::default(),
    };
    let db = Arc::new(
        Database::open_with_config(&path.0, config.identity.clone(), read_config()).unwrap(),
    );
    let t = &row["genesis"]["trees"];
    let a = tree(&t["accounts"]);
    let c = tree(&t["contracts"]);
    let calls = tree(&t["calls"]);
    let trees = Trees {
        accounts: ae_state::accounts::Accounts::from_root(a.root_hash(), a.store().clone())
            .unwrap(),
        contracts: ae_state::contracts::Contracts::from_root(c.root_hash(), c.store().clone())
            .unwrap(),
        calls: ae_state::calls::Calls::from_root(calls.root_hash(), calls.store().clone()).unwrap(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    if db.view().unwrap().status().tip.is_none() {
        db.commit(
            repo::bootstrap(
                &db.view().unwrap(),
                &network,
                &header,
                &trees,
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap();
    }
    (db, config)
}

#[test]
fn unacknowledged_hc_writes_never_install_live_caches_or_publish_success() {
    for mode in [1, 2] {
        let path = Temp::new();
        let (db, c) = setup(&path);
        let owner = Owner::start(db.clone(), c.clone(), Parent::default()).unwrap();
        let before = owner.snapshot().unwrap();
        let source = db.events();
        let cursor = source.cursor();
        db.event_write_failure.store(mode, Ordering::Relaxed);
        let b = block(&DATA["rows"][0]["rows"][0]);
        assert!(matches!(
            owner.insert(&b, 1_000_000, Origin::Gossip),
            Err(Error::Repository(repo::Error::Backend(_)))
        ));
        let live = owner.live.lock().unwrap();
        assert_eq!(
            (live.revision, &live.state, &live.host),
            (before.0, &before.1, &before.2)
        );
        assert!(live.failed);
        drop(live);
        assert!(matches!(
            owner.insert(&b, 1_000_000, Origin::Gossip),
            Err(Error::RecoveryRequired)
        ));
        assert_eq!(source.info().status.revision, 1);
        assert!(matches!(
            source.after(&cursor, 1),
            Err(crate::events::Error::Failed(
                crate::events::Failure::UncertainWrite
            ))
        ));
        drop(owner);
        assert!(Owner::start(db.clone(), c.clone(), Parent::default()).is_err());
        drop(db);
        let db = Arc::new(
            Database::open_with_config(&path.0, c.identity.clone(), read_config()).unwrap(),
        );
        assert_eq!(
            db.view().unwrap().status().revision,
            if mode == 1 { 1 } else { 2 }
        );
        let owner = Owner::start(db.clone(), c, Parent::default()).unwrap();
        if mode == 1 {
            owner.insert(&b, 1_000_000, Origin::Gossip).unwrap();
        }
        assert_eq!(db.view().unwrap().status().tip, Some(b.hash().unwrap()));
        assert!(!owner.snapshot().unwrap().1.leaders.schedule().is_empty());
        db.view().unwrap().verify_tip().unwrap();
    }
}

#[test]
fn hc_stale_preparation_and_operational_failure_leave_durable_state_unchanged() {
    let path = Temp::new();
    let (db, c) = setup(&path);
    let b = block(&DATA["rows"][0]["rows"][0]);
    let unavailable = Parent {
        unavailable: true,
        ..Default::default()
    };
    let owner = Owner::start(db.clone(), c.clone(), unavailable).unwrap();
    let before = owner.snapshot().unwrap();
    assert!(owner.insert(&b, 1_000_000, Origin::Gossip).is_err());
    assert_eq!(owner.snapshot().unwrap(), before);
    drop(owner);
    let view = db.view().unwrap();
    let checkpoint = view.hyperchain_checkpoint().unwrap();
    let mut state = checkpoint.state.clone();
    let mut host = Parent::default();
    let first = repo::prepare_hyperchain(&view, &b, &c, 1_000_000, &mut state, &mut host).unwrap();
    let second = repo::prepare_hyperchain(
        &view,
        &b,
        &c,
        1_000_000,
        &mut checkpoint.state.clone(),
        &mut Parent::default(),
    )
    .unwrap();
    db.commit(first).unwrap();
    assert!(matches!(db.commit(second), Err(repo::Error::Conflict)));
    assert_eq!(view.status().revision, 1);
    assert_eq!(view.hyperchain_checkpoint().unwrap(), checkpoint);
    assert_eq!(
        db.view().unwrap().hyperchain_checkpoint().unwrap().state,
        state
    );
    assert_eq!(db.events().info().status.revision, 2);
}

#[test]
fn legacy_hc_genesis_upgrade_preserves_an_explicit_empty_event_interval() {
    let path = Temp::new();
    let (db, c) = setup(&path);
    let TrieValue::List(mut fields) = TrieValue::decode(&c.identity.encode()).unwrap() else {
        panic!()
    };
    fields[0] = TrieValue::Bytes(vec![10]);
    let legacy = TrieValue::List(fields).encode();
    db.db.put(repo::IDENTITY, &legacy).unwrap();
    db.db.delete(repo::HC_STATE).unwrap();
    db.db
        .delete(repo::key(repo::HC_EVENTS, 1u64.to_be_bytes()))
        .unwrap();
    drop(db);
    for _ in 0..2 {
        let db = Database::open_with_config(&path.0, c.identity.clone(), read_config()).unwrap();
        assert_eq!(
            db.db.get(repo::IDENTITY).unwrap().unwrap(),
            c.identity.encode()
        );
        let v = db.view().unwrap();
        assert_eq!(v.hyperchain_checkpoint().unwrap().first_revision, 2);
        assert!(v.hyperchain_event(1).unwrap().is_none());
        v.verify_tip().unwrap();
    }
    let db =
        Arc::new(Database::open_with_config(&path.0, c.identity.clone(), read_config()).unwrap());
    let owner = Owner::start(db.clone(), c, Parent::default()).unwrap();
    owner
        .insert(&block(&DATA["rows"][0]["rows"][0]), 1_000_000, Origin::Sync)
        .unwrap();
    assert!(db.view().unwrap().hyperchain_event(2).unwrap().is_some());
}

#[test]
fn missing_or_corrupt_hc_metadata_never_reinitializes_storage() {
    for damage in 0..5 {
        let path = Temp::new();
        let (db, c) = setup(&path);
        let b = block(&DATA["rows"][0]["rows"][0]);
        if damage > 0 {
            let owner = Owner::start(db.clone(), c.clone(), Parent::default()).unwrap();
            owner.insert(&b, 1_000_000, Origin::Gossip).unwrap();
        }
        let rev = db.view().unwrap().status().revision;
        let k = repo::key(repo::HC_EVENTS, rev.to_be_bytes());
        match damage {
            0 | 1 => db.db.delete(repo::HC_STATE).unwrap(),
            2 => db.db.put(repo::HC_STATE, [255]).unwrap(),
            3 => db.db.delete(k).unwrap(),
            _ => {
                let mut e = db.view().unwrap().hyperchain_event(rev).unwrap().unwrap();
                e.context.block.as_mut().unwrap().height += 1;
                db.db.put(k, e.encode(Default::default()).unwrap()).unwrap();
            }
        }
        drop(db);
        assert!(
            Database::open_with_config(&path.0, c.identity, read_config()).is_err(),
            "damage {damage}"
        );
    }
}
