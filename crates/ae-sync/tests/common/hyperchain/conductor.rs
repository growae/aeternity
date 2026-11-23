use super::{
    Node,
    parents::{ParentBackend, parent, wait},
};
use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::{
    eoe::local_host,
    parent_cache::{self, runtime as cache},
    parent_connector::runtime as connector,
};
use ae_storage::pool;
use ae_sync::chain::hyperchain::{
    conductor::{self, Conductor},
    eoe::{
        self as live,
        owner::{self as managed, production},
    },
};
use ae_types::Unsigned;
use ae_vm_fate::maps::order::Otp26;
use num_bigint::BigInt;
use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
pub fn signer(n: u64) -> Ed25519SecretKey {
    let mut seed = [0; 32];
    seed[24..].copy_from_slice(&n.to_be_bytes());
    Ed25519SecretKey::from_seed(&seed)
}
#[derive(Clone)]
struct Targets;
impl cache::Targets for Targets {
    fn targets(&mut self, _: &Unsigned) -> Result<Vec<BigInt>, cache::TargetError> {
        Ok(vec![1.into(), 6.into()])
    }
}
pub fn config(node: &Node, producing: bool) -> conductor::Config {
    let eoe = managed::Config {
        eoe: live::Config {
            host: local_host::Config {
                network: node.config.profile.network.clone(),
                election_contract: node.config.profile.contracts.election,
                parent_epoch_length: 5.into(),
                parent_start_height: 1.into(),
                limits: Default::default(),
            },
            block_time: node.config.rules.consensus.child_block_time.clone(),
            coordinator: Default::default(),
            runtime: Default::default(),
            epochs: Default::default(),
        },
        votes: Default::default(),
        pool: Default::default(),
        calls: Default::default(),
    };
    conductor::Config {
        eoe,
        production: production::Config {
            producer: managed::producer::Config {
                production_time: 1.into(),
                ..Default::default()
            },
            ..Default::default()
        },
        pool: pool::Config {
            consensus: ae_protocol::ConsensusType::Pos,
            policy: ae_mempool::pool::Config {
                minimum_miner_gas_price: 1.into(),
                ..Default::default()
            },
            ..Default::default()
        },
        pool_worker: pool::worker::Settings {
            origins_interval: None,
            ..Default::default()
        },
        admission: Default::default(),
        snapshots: Default::default(),
        resilience: None,
        gossip: Default::default(),
        parent: conductor::ParentConfig {
            pins: None,
            cache: parent_cache::Config {
                child_start_height: 0.into(),
                max_size: 200.into(),
                retry_interval: 1000.into(),
                finality: 2.into(),
            },
            cache_runtime: Default::default(),
            connector: connector::Config {
                fetch_interval: None,
                ..Default::default()
            },
            hosts: 3,
            seed: [0; 16],
        },
        events_per_step: 4,
        producing,
        block_producer: true,
    }
}
pub fn new(node: &Node, c: conductor::Config) -> Result<Conductor<Otp26>, conductor::Error> {
    Conductor::start_with_targets(
        node.db.clone(),
        node.config.clone(),
        [12, 13, 14]
            .into_iter()
            .map(|n| {
                let k = signer(n);
                (k.public_key(), k)
            })
            .collect(),
        c,
        Arc::new(ParentBackend {
            missing: AtomicBool::new(false),
        }),
        Targets,
    )
}
pub fn start(node: &Node, producing: bool) -> Conductor<Otp26> {
    let c = new(node, config(node, producing)).unwrap();
    c.parent().post_block(parent(8, 1008)).unwrap();
    c.parent().barrier(Duration::from_secs(5)).unwrap();
    wait(|| {
        [1, 6].into_iter().all(|h| {
            matches!(
                c.parent().snapshot().cache.blocks().get(&h.into()),
                Some(parent_cache::Entry::Block(_))
            )
        })
    });
    c
}
