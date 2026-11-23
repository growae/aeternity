mod common;
use ae_chain::{
    block::Block,
    hyperchain::candidate,
    repository::{self as repo, ledger, pow},
};
use ae_codec::headers::{Header, KeyHeader};
use ae_crypto::{Ed25519SecretKey, blake2b_256};
use ae_protocol::{Activation, NetworkProfile, Protocol};
use ae_state::{Trees, accounts::Account};
use ae_storage::{Database, pool, snapshots};
use ae_sync::{
    chain::{
        self,
        pow::{generator, packing, producer},
    },
    keys,
};
use ae_tx::{CanonicalTransaction, SignedTransaction, Spend, Transaction};
use ae_types::{AccountPublicKey, Hash256, Id, IdTag};
use ae_vm_fate::maps::order::Otp26;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

static NEXT: AtomicU64 = AtomicU64::new(1);
struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Node {
    importer: chain::Importer<Otp26>,
    generator: Option<generator::Worker<Otp26>>,
    pool: Option<pool::worker::Worker>,
    keys: Option<keys::Worker>,
    _snapshots: snapshots::Owner,
    db: Arc<Database>,
    network: NetworkProfile,
    genesis: KeyHeader,
    payer: Ed25519SecretKey,
    cycle: u64,
    _directory: Temporary,
}
impl Node {
    fn new(protocol: u32, candidate_key: bool) -> Self {
        let directory = Temporary(std::env::temp_dir().join(format!(
            "ae-pow-micro-producer-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )));
        let keys = keys::Worker::spawn(keys::Config::new(
            directory.0.join("keys"),
            b"isolated test".to_vec(),
        ))
        .unwrap();
        let miner = if candidate_key {
            keys.info().keys.candidate
        } else {
            keys.info().keys.signing
        };
        let payer = Ed25519SecretKey::from_seed(&[19; 32]);
        let mut trees = Trees {
            accounts: Default::default(),
            contracts: Default::default(),
            channels: Default::default(),
            calls: Default::default(),
            names: Default::default(),
            oracles: Default::default(),
        };
        trees
            .accounts
            .enter(&Account::new(
                payer.public_key(),
                1_000_000_000_000_000_000u64.into(),
            ))
            .unwrap();
        let Header::Key(mut genesis) =
            ae_state::genesis::header(ae_state::genesis::Network::Mainnet).unwrap()
        else {
            panic!()
        };
        genesis.common.version = protocol;
        genesis.common.state_hash = trees.roots().hash().0;
        genesis.miner = miner.0;
        genesis.beneficiary = miner.0;
        genesis.time = 1000;
        let network = NetworkProfile::custom(
            b"ae_pow_micro_producer".to_vec(),
            vec![Activation {
                height: 0,
                protocol: Protocol::try_from(protocol).unwrap(),
            }],
        )
        .unwrap();
        let hash = Header::Key(genesis.clone()).hash().unwrap();
        let identity = repo::Identity::new(
            &network,
            hash,
            blake2b_256(b"isolated PoW producer profile"),
        );
        let db = Database::open(directory.0.join("chain"), identity.clone()).unwrap();
        let update = ledger::bootstrap(
            &db.view().unwrap(),
            &network,
            &genesis,
            &trees,
            Default::default(),
        )
        .unwrap();
        db.commit(update).unwrap();
        let record = db.view().unwrap().require_record(hash).unwrap();
        drop(db);
        // A trusted, synthetic height-one parent avoids mining a 2^29-edge
        // Cuckoo graph in service tests. It has NO valid PoW seal. Only this
        // test fixture writes raw storage, before any service is started.
        // Every produced microblock still passes ordinary signature, body,
        // transaction, state-root and contextual admission. Public-key-block
        // admission is independently tested with unchanged mainnet fixtures.
        seed_parent(&directory.0.join("chain"), &genesis, record);
        let db = Arc::new(Database::open(directory.0.join("chain"), identity).unwrap());
        let cycle = 30;
        let mut rules = common::ledger_fixtures::rules(hash);
        rules.admission.micro_cycle_ms = cycle;
        let importer = chain::Importer::new(
            db.clone(),
            common::ledger_fixtures::execution(&network),
            rules,
            Default::default(),
        )
        .unwrap();
        let pool = pool::worker::Worker::spawn(
            db.clone(),
            network.clone(),
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
        let snapshots = snapshots::Owner::start(db.clone(), Default::default()).unwrap();
        let config = configuration(&db, &network, cycle);
        let generator = generator::Worker::spawn(
            db.clone(),
            snapshots.clone(),
            pool.client(),
            config,
            Arc::new(generator::SystemClock),
        )
        .unwrap();
        Self {
            importer,
            generator: Some(generator),
            pool: Some(pool),
            keys: Some(keys),
            _snapshots: snapshots,
            db,
            network,
            genesis,
            payer,
            cycle,
            _directory: directory,
        }
    }
    fn generator(&self) -> generator::Client<Otp26> {
        self.generator.as_ref().unwrap().client()
    }
    fn keys(&self) -> keys::Client {
        self.keys.as_ref().unwrap().client()
    }
    fn config(&self) -> producer::Config {
        producer::Config {
            enabled: true,
            resume_current: true,
            strictly_follow_top: true,
            micro_cycle_ms: self.cycle,
            poll: Duration::from_millis(2),
            ..Default::default()
        }
    }
    fn spawn(&self, config: producer::Config) -> producer::Worker {
        producer::Worker::spawn(
            &self.importer,
            self.generator(),
            self.keys(),
            config,
            producer::SystemClock,
        )
        .unwrap()
    }
    fn transaction(&self, nonce: u64) -> candidate::Signed {
        let mut spend =
            Spend::new(self.payer.public_key(), Id::new(IdTag::Account, [21; 32])).unwrap();
        spend.set_nonce(nonce.into());
        spend.set_amount(17u64.into());
        spend.set_fee(100_000_000_000_000u64.into());
        let mut message = self.network.signing_prefix(false);
        message.extend_from_slice(&spend.encode());
        SignedTransaction::new(
            Transaction::new(ae_tx::TransactionLeaf::Spend(spend)),
            vec![self.payer.sign(&message)],
        )
    }
    fn push(&self, nonce: u64) {
        self.pool
            .as_ref()
            .unwrap()
            .client()
            .push(
                Arc::new(self.transaction(nonce)),
                ae_mempool::pool::Event::Created,
                false,
            )
            .unwrap()
            .wait_timeout(Duration::from_secs(5))
            .unwrap();
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if let Some(generator) = self.generator.take() {
            drop(generator.shutdown().unwrap());
        }
        self.importer.stop();
        if let Some(pool) = self.pool.take() {
            pool.shutdown().unwrap();
        }
        if let Some(keys) = self.keys.take() {
            keys.shutdown().unwrap();
        }
    }
}
fn seed_parent(path: &std::path::Path, genesis: &KeyHeader, mut record: repo::Record) {
    let parent = Header::Key(genesis.clone()).hash().unwrap();
    let mut key = genesis.clone();
    key.common.height = 1;
    key.common.previous_hash = parent.0;
    key.common.previous_key_hash = parent.0;
    key.time += 1;
    let block = Block::Key(key);
    let hash = block.hash().unwrap();
    record.difficulty = record.difficulty.mul_u64(2);
    let event = repo::Change {
        revision: 2,
        inserted: Some(hash),
        old_tip: Some(parent),
        new_tip: hash,
        ancestor: Some(parent),
        attached: vec![hash],
        detached: vec![],
        removed: vec![],
        discarded: vec![],
        requeue: vec![],
        confirm: vec![],
    };
    let db = rocksdb::DB::open_default(path).unwrap();
    let mut batch = rocksdb::WriteBatch::default();
    batch.put(repo::key(repo::BLOCKS, hash.0), block.encode().unwrap());
    batch.put(repo::key(repo::RECORDS, hash.0), record.encode());
    batch.put(
        repo::key(
            repo::CHILDREN,
            [parent.0.as_slice(), hash.0.as_slice()].concat(),
        ),
        [],
    );
    batch.put(
        repo::key(
            repo::HEIGHTS,
            [1u64.to_be_bytes().as_slice(), hash.0.as_slice()].concat(),
        ),
        [],
    );
    batch.put(repo::key(repo::CANONICAL, hash.0), []);
    batch.put(repo::key(repo::KEYS, 1u64.to_be_bytes()), hash.0);
    batch.delete(repo::key(repo::ENDS, parent.0));
    batch.put(repo::key(repo::ENDS, hash.0), []);
    batch.put(repo::key(repo::EVENTS, 2u64.to_be_bytes()), event.encode());
    batch.put(
        repo::STATUS,
        repo::Status {
            revision: 2,
            tip: Some(hash),
        }
        .encode(),
    );
    db.write(batch).unwrap();
    db.flush().unwrap();
}
fn configuration(
    db: &Arc<Database>,
    network: &NetworkProfile,
    cycle: u64,
) -> generator::Config<Otp26> {
    let preparation = common::ledger_fixtures::execution(network).with_config(|c| {
        let fate = c.fate.unwrap();
        pow::Config {
            identity: db.view().unwrap().identity().clone(),
            profile: Arc::new(pow::Profile {
                contracts: ae_hyperchain::consensus_contract::Contracts {
                    owner: AccountPublicKey([0; 32]),
                    election: [0; 32],
                    staking: [0; 32],
                    rewards: [0; 32],
                },
                network: c.network.clone(),
                context: fate.context.clone(),
                order: fate.order.clone(),
                overrides: fate.overrides.clone(),
                diagnostic: fate.diagnostic_profile,
                limits: c.fate_limits,
                maximum_steps: c.maximum_fate_steps,
                maximum_history_reads: 1024,
            }),
            execution: pow::Execution {
                aevm: c.aevm.cloned(),
                ..Default::default()
            },
            micro_cycle_ms: cycle,
        }
    });
    generator::Config {
        preparation,
        candidate: Default::default(),
        packing: packing::Config {
            timeout: candidate::Timeout::Disabled,
            window_ms: cycle as i64,
        },
        controls: 8,
        publications: 4,
        queued_transactions: 1000,
        queued_bytes: 1 << 20,
        feed_batch: 16,
        poll_interval: Duration::from_millis(2),
    }
}
fn until(mut condition: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < end, "bounded producer wait");
        std::thread::sleep(Duration::from_millis(2));
    }
}
fn finish(worker: producer::Worker) -> producer::Report {
    let mut report = worker.shutdown().unwrap();
    until(|| report.poll());
    report
}
fn admitted(node: &Node, client: &producer::Client) -> Hash256 {
    let end = Instant::now() + Duration::from_secs(10);
    while client.front().is_none() && client.status().failure.is_none() {
        assert!(
            Instant::now() < end,
            "producer {:?}; generator {:?}",
            client.status(),
            node.generator().status()
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    loop {
        assert!(client.status().failure.is_none(), "{:?}", client.status());
        let output = client.front().unwrap();
        let hash = match &output.completion {
            producer::Completion::Admitted { hash, result, .. } => {
                assert!(result.is_ok(), "{result:?}");
                Some(*hash)
            }
            producer::Completion::Promoted { result, .. } => {
                assert!(result.is_ok(), "{result:?}");
                None
            }
            other => panic!("unexpected completion {other:?}"),
        };
        assert!(client.acknowledge(&output));
        if let Some(hash) = hash {
            return hash;
        }
        until(|| client.front().is_some() || client.status().failure.is_some());
    }
}

#[test]
fn actual_live_generator_signer_and_admission_produce_two_valid_microblocks() {
    let node = Node::new(1, false);
    let worker = node.spawn(node.config());
    let client = worker.client();
    node.push(1);
    let first = admitted(&node, &client);
    let Block::Micro(one) = node.db.view().unwrap().require_block(first).unwrap() else {
        panic!()
    };
    assert_ne!(one.header.signature, [0; 64]);
    assert_eq!(one.transactions, vec![node.transaction(1)]);
    node.push(2);
    let second = admitted(&node, &client);
    let Block::Micro(two) = node.db.view().unwrap().require_block(second).unwrap() else {
        panic!()
    };
    assert_eq!(two.header.common.previous_hash, first.0);
    assert!(two.header.time >= one.header.time + node.cycle);
    assert_eq!(node.db.view().unwrap().status().tip, Some(second));
    let report = finish(worker);
    assert!(report.status.failure.is_none(), "{:?}", report.status);
    assert!(report.is_settled());
}

#[test]
fn current_candidate_key_is_promoted_before_real_signing_and_admission() {
    let node = Node::new(6, true);
    let old = node.keys().info().keys;
    let worker = node.spawn(node.config());
    let client = worker.client();
    node.push(1);
    let hash = admitted(&node, &client);
    assert_ne!(old.signing, AccountPublicKey(node.genesis.miner));
    assert_eq!(node.keys().info().keys.signing, old.candidate);
    assert_ne!(node.keys().info().keys.candidate, old.candidate);
    assert_eq!(client.status().promotions, 1);
    assert_eq!(node.db.view().unwrap().status().tip, Some(hash));
    assert!(finish(worker).status.failure.is_none());
}

#[test]
fn primitive_transactions_produce_consecutive_microblocks_across_protocols_one_through_eight() {
    for protocol in 1..=8 {
        let node = Node::new(protocol, false);
        let mut cfg = node.config();
        cfg.events_per_step = 1;
        let worker = node.spawn(cfg);
        let client = worker.client();
        for nonce in 1..=4 {
            node.push(nonce);
            let hash = admitted(&node, &client);
            let Block::Micro(block) = node.db.view().unwrap().require_block(hash).unwrap() else {
                panic!()
            };
            assert_eq!(block.header.common.version, protocol);
            assert_eq!(block.transactions, vec![node.transaction(nonce)]);
            assert_eq!(node.db.view().unwrap().status().revision, nonce + 2);
        }
        assert!(finish(worker).status.failure.is_none());
    }
}

#[path = "pow_producer/group.rs"]
mod group;
#[path = "pow_producer/lifecycle.rs"]
mod lifecycle;
