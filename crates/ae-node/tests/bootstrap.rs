use ae_chain::{
    block::Block,
    repository::{self, Roots, Status},
};
use ae_node::{
    bootstrap::{Config, Error, Opened},
    configuration::{
        Configuration,
        chain::Settings,
        resolution::{Context, Directories},
    },
};
use ae_protocol::ConsensusType;
use ae_state::{Trees, accounts::Account, contracts::Contracts};
use ae_types::{AccountPublicKey, Hash256};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(std::env::temp_dir().join(format!(
            "ae-node-bootstrap-{}-{}",
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
fn settings(value: Value, network: &[u8]) -> Settings {
    let cfg = Configuration::from_value(value).unwrap();
    let ctx = Context::new(Directories {
        home: "/release".into(),
        data: "/data".into(),
        log: "/log".into(),
    })
    .unwrap();
    Settings::resolve(&cfg.resolve(&ctx).unwrap(), Some(network)).unwrap()
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
        .enter(&Account::new(AccountPublicKey([1; 32]), 123456.into()))
        .unwrap();
    trees
}
fn execution() -> Hash256 {
    ae_crypto::blake2b_256(b"node-bootstrap-test-execution-profile")
}
#[test]
fn durable_genesis_reopen_preserves_state_and_revision_for_both_families() {
    for (value, family) in [
        (json!({}), ConsensusType::Pow),
        (
            json!({"chain":{"consensus":{"0":{"type":"on_demand"}}}}),
            ConsensusType::Pow,
        ),
        (
            json!({"chain":{"consensus":{"0":{"type":"hyperchain","config":{"genesis_start_time":1000}}},"hard_forks":{"7":0}}}),
            ConsensusType::Pos,
        ),
    ] {
        let tmp = Temp::new();
        let trees = trees();
        let s = settings(value, b"private");
        let opened =
            Opened::open(&tmp.0, s.clone(), &trees, execution(), Config::default()).unwrap();
        let identity = opened.identity().clone();
        assert_eq!(identity.consensus, family);
        let expected = Block::Key(opened.genesis().clone());
        assert_eq!(opened.settings().profile().network_id(), b"private");
        {
            let view = opened.database().view().unwrap();
            assert_eq!(
                view.status(),
                Status {
                    revision: 1,
                    tip: Some(identity.genesis)
                }
            );
            assert_eq!(view.require_block(identity.genesis).unwrap(), expected);
            assert_eq!(
                view.require_record(identity.genesis).unwrap().roots,
                Roots::of(&trees)
            );
            let restored = view.trees(identity.genesis, Default::default()).unwrap();
            assert_eq!(restored.roots(), trees.roots());
            view.verify_tip().unwrap();
        }
        drop(opened);
        let reopened = Opened::open(&tmp.0, s, &trees, execution(), Config::default()).unwrap();
        assert_eq!(reopened.identity(), &identity);
        assert_eq!(
            reopened.database().view().unwrap().status(),
            Status {
                revision: 1,
                tip: Some(identity.genesis)
            }
        );
    }
    println!("Node bootstrap: three durable consensus-algorithm genesis publications and reopens");
}
#[test]
fn identity_mismatch_never_replaces_an_existing_chain() {
    let tmp = Temp::new();
    let trees = trees();
    let s = settings(json!({}), b"private");
    let opened = Opened::open(&tmp.0, s.clone(), &trees, execution(), Config::default()).unwrap();
    let identity = opened.identity().clone();
    drop(opened);
    let mut changed = trees.clone();
    changed
        .accounts
        .enter(&Account::new(AccountPublicKey([2; 32]), 10.into()))
        .unwrap();
    for (s, t, e) in [
        (settings(json!({}), b"other-network"), &trees, execution()),
        (
            settings(json!({"chain":{"hard_forks":{"1":0,"6":2}}}), b"private"),
            &trees,
            execution(),
        ),
        (s.clone(), &changed, execution()),
        (s.clone(), &trees, Hash256([9; 32])),
        (
            settings(
                json!({"chain":{"hard_forks":{"7":0},"consensus":{"0":{"type":"hyperchain"}}}}),
                b"private",
            ),
            &trees,
            execution(),
        ),
    ] {
        assert!(matches!(
            Opened::open(&tmp.0, s, t, e, Config::default()),
            Err(Error::Repository(repository::Error::Identity))
        ));
    }
    let reopened = Opened::open(&tmp.0, s, &trees, execution(), Config::default()).unwrap();
    assert_eq!(reopened.identity(), &identity);
    assert_eq!(reopened.database().view().unwrap().status().revision, 1);
}
#[test]
fn unsupported_dispatch_and_bad_public_genesis_fail_before_creating_a_database() {
    let trees = trees();
    for (s, expected) in [
        (
            settings(
                json!({"chain":{"consensus":{"0":{"type":"pow_cuckoo"},"10":{"type":"hyperchain"}}}}),
                b"private",
            ),
            Error::UnsupportedSchedule,
        ),
        (
            settings(json!({}), b"ae_mainnet"),
            Error::Configuration(ae_node::configuration::chain::Error::Genesis),
        ),
        (
            settings(json!({}), b"ae_uat"),
            Error::Configuration(ae_node::configuration::chain::Error::Genesis),
        ),
    ] {
        let tmp = Temp::new();
        let error = Opened::open(&tmp.0, s, &trees, execution(), Config::default())
            .err()
            .unwrap();
        assert_eq!(error, expected);
        assert!(!tmp.0.exists());
    }
}
#[test]
fn bounded_bootstrap_failure_leaves_no_published_genesis_and_allows_retry() {
    let tmp = Temp::new();
    let trees = trees();
    let s = settings(json!({}), b"private");
    let identity = s.identity(trees.roots().hash(), execution()).unwrap();
    let mut config = Config::default();
    config.genesis.commit_bytes = 1;
    assert!(matches!(
        Opened::open(&tmp.0, s.clone(), &trees, execution(), config),
        Err(Error::Repository(repository::Error::ResourceLimit))
    ));
    {
        let db = ae_storage::Database::open(&tmp.0, identity).unwrap();
        assert_eq!(
            db.view().unwrap().status(),
            Status {
                revision: 0,
                tip: None
            }
        );
    }
    let opened = Opened::open(&tmp.0, s, &trees, execution(), Config::default()).unwrap();
    assert_eq!(opened.database().view().unwrap().status().revision, 1);
}
