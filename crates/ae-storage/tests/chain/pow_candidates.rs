use super::*;
use ae_chain::{hyperchain::candidate as core, repository::pow};
use ae_hyperchain::chain::{Profile, RunEnv};
use ae_storage::{pow as storage, snapshots};
use ae_types::AccountPublicKey;
use ae_vm_fate::{machine, maps};
use std::{collections::BTreeMap, convert::Infallible, sync::Arc};

// These deployed early-generation fixtures contain primitive transactions.
// Any accidental contract/map execution must fail instead of using a fake VM order.
#[derive(Clone)]
struct NoMaps;
impl maps::MapOrder for NoMaps {
    fn indices(
        &self,
        _: &[&ae_vm_fate::value::Value],
        _: maps::Traversal,
        _: ae_vm_fate::Limits,
    ) -> Result<Vec<usize>, ae_vm_fate::Error> {
        Err(ae_vm_fate::Error::InvalidOrderProfile)
    }
}
fn config(n: Network) -> pow::Config<NoMaps> {
    pow::Config {
        identity: identity(n),
        profile: Arc::new(Profile {
            contracts: ae_hyperchain::consensus_contract::Contracts {
                owner: AccountPublicKey([0; 32]),
                election: [0; 32],
                staking: [0; 32],
                rewards: [0; 32],
            },
            network: profile(n),
            context: machine::Context {
                current_contract: [0; 32],
                vm_version: ae_vm_fate::types::Version::Three,
                in_auth: false,
                on_chain: true,
                reserved_gas: 0.into(),
                arithmetic_word_size: maps::order::WordSize::Bits64,
                unicode_profile: ae_vm_fate::unicode::Profile::Otp26Unicode15,
                bls_profile: ae_crypto::bls::Profile::Mcl11976beLlvm64VintArm64,
            },
            order: NoMaps,
            overrides: Default::default(),
            diagnostic: ae_vm_fate::diagnostic::Profile::Test,
            limits: Default::default(),
            maximum_steps: 100_000,
            maximum_history_reads: 1_024,
        }),
        execution: Default::default(),
        micro_cycle_ms: 3_000,
    }
}
fn input(parent: Hash256, now: u64) -> pow::Input {
    pow::Input {
        parent,
        now: now.into(),
        limits: Default::default(),
    }
}
fn insert(db: &Database, n: Network, block: &Block) {
    let network = profile(n);
    db.commit(
        ledger::prepare(
            &db.view().unwrap(),
            block,
            &transactions::Config::primitives(&network),
            &rules(n),
            Default::default(),
        )
        .unwrap(),
    )
    .unwrap();
}
fn encoded<S: ae_trie::NodeWriter + Clone>(candidate: &core::Micro<S, S>) -> Vec<u8> {
    Block::Micro(candidate.block().clone()).encode().unwrap()
}

#[test]
fn pow_owned_candidates_reproduce_public_microblocks_and_pin_precommit_history() {
    let mut micros = 0;
    let mut keys = 0;
    for n in [Network::Mainnet, Network::Uat] {
        let temp = Temp::new();
        let db = Arc::new(Database::open(&temp.0, identity(n)).unwrap());
        bootstrap(&db, n);
        let owner = snapshots::Owner::start(db.clone(), Default::default()).unwrap();
        let cfg = config(n);
        for block in blocks(n) {
            if let Block::Micro(original) = &block {
                let prepared = storage::prepare_micro(
                    &owner,
                    &cfg,
                    input(
                        Hash256(original.header.common.previous_hash),
                        original.header.time,
                    ),
                )
                .unwrap();
                let metadata = prepared.metadata;
                let built = prepared.value;
                assert_eq!(built.status, metadata.status);
                assert_eq!(built.source_id, metadata.source_id);
                assert_eq!(owner.live_snapshots(), 1);
                let before = built.reader.selected.repository().status();
                let mut packing = core::Packing::new::<Infallible>(
                    built.candidate,
                    0.into(),
                    0,
                    core::Timeout::Disabled,
                    3000,
                )
                .unwrap();
                assert!(packing.request::<Infallible>(|| 0).unwrap().is_some());
                packing
                    .selected::<Infallible, _, _>(&original.transactions, false, &built.reader)
                    .unwrap();
                if packing.request::<Infallible>(|| 0).unwrap().is_some() {
                    packing
                        .selected::<Infallible, _, _>(&[], false, &built.reader)
                        .unwrap();
                }
                let candidate = packing.finish::<Infallible>().unwrap();
                let mut reproduced = candidate.block().clone();
                assert_eq!(reproduced.header.signature, [0; 64]);
                reproduced.header.signature = original.header.signature;
                let reproduced = Block::Micro(reproduced);
                assert_eq!(reproduced.encode().unwrap(), block.encode().unwrap());
                let roots = candidate.trees().roots();
                insert(&db, n, &reproduced);
                let latest = db.view().unwrap();
                assert_eq!(latest.status().revision, before.revision + 1);
                assert_eq!(
                    latest
                        .require_record(block.hash().unwrap())
                        .unwrap()
                        .roots
                        .state,
                    roots
                );
                let old = built.reader.selected.repository();
                assert_eq!(old.status(), before);
                assert!(old.block(block.hash().unwrap()).unwrap().is_none());
                assert_eq!(candidate.trees().roots(), roots);
                let parent = built
                    .reader
                    .view(&RunEnv::Hash(original.header.common.previous_hash.to_vec()))
                    .unwrap();
                assert_eq!(
                    parent.history.selected.get(&0),
                    Some(&cfg.identity.genesis.0)
                );
                assert_eq!(
                    parent.environment.consensus,
                    machine::program::remote::context::Consensus::Pow
                );
                assert_eq!(
                    candidate.environment().difficulty,
                    parent.environment.difficulty
                );
                micros += 1;
            } else {
                insert(&db, n, &block);
                let prepared =
                    storage::prepare_micro(&owner, &cfg, input(block.hash().unwrap(), 0)).unwrap();
                let Header::Key(key) = block.header() else {
                    panic!()
                };
                assert_eq!(prepared.value.candidate.block().header.time, key.time + 1);
                assert_eq!(
                    prepared.value.candidate.block().header.common.height,
                    key.common.height
                );
                keys += 1;
            }
            assert_eq!(owner.live_snapshots(), 0);
        }
        let tip = db.view().unwrap().status().tip.unwrap();
        let held = storage::prepare_micro(&owner, &cfg, input(tip, 0)).unwrap();
        let bytes = encoded(&held.value.candidate);
        drop(owner);
        drop(db);
        assert!(Database::open(&temp.0, identity(n)).is_err());
        assert_eq!(encoded(&held.value.candidate), bytes);
        held.value
            .reader
            .selected
            .repository()
            .verify_tip()
            .unwrap();
        drop(held);
        let db = Arc::new(Database::open(&temp.0, identity(n)).unwrap());
        let owner = snapshots::Owner::start(db, Default::default()).unwrap();
        let reopened = storage::prepare_micro(&owner, &cfg, input(tip, 0)).unwrap();
        assert_eq!(encoded(&reopened.value.candidate), bytes);
    }
    assert_eq!((micros, keys), (1, 12));
}

// Explicit fault injection over an immutable real snapshot. These altered
// metadata/proof rows are not admitted blocks and make no chain-validity claim.
#[derive(Clone, Debug)]
struct Faults<S> {
    source: S,
    rows: Arc<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
}
impl<S: Read> ae_trie::NodeReader for Faults<S> {
    fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.source.node(h)
    }
}
impl<S: Read> Read for Faults<S> {
    fn source_id(&self) -> u64 {
        self.source.source_id()
    }
    fn resilience(&self) -> ae_chain::resilience::Snapshot {
        self.source.resilience()
    }
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, repo::Error> {
        self.rows
            .get(k)
            .cloned()
            .map_or_else(|| self.source.get(k), Ok)
    }
    fn prefix(&self, p: &[u8], n: usize) -> Result<repo::Entries, repo::Error> {
        self.source.prefix(p, n)
    }
    fn range(&self, a: &[u8], b: &[u8], n: usize) -> Result<repo::Entries, repo::Error> {
        self.source.range(a, b, n)
    }
    fn visit_range(
        &self,
        a: &[u8],
        b: &[u8],
        n: usize,
        v: &mut repo::Visitor<'_>,
    ) -> Result<(), repo::Error> {
        self.source.visit_range(a, b, n, v)
    }
}
#[test]
fn pow_snapshot_proofs_use_previous_generation_and_require_intact_reporting_metadata() {
    let n = Network::Mainnet;
    let temp = Temp::new();
    let db = Database::open(&temp.0, identity(n)).unwrap();
    bootstrap(&db, n);
    let all = blocks(n);
    for b in &all {
        insert(&db, n, b);
    }
    let view = db.view().unwrap();
    let cfg = config(n);
    let block = all.iter().find(|b| matches!(b, Block::Micro(_))).unwrap();
    let Header::Micro(micro) = block.header() else {
        panic!()
    };
    let parent = block.hash().unwrap();
    let leader = Hash256(micro.common.previous_key_hash);
    let Block::Key(key) = view.require_block(leader).unwrap() else {
        panic!()
    };
    let mut second = micro.clone();
    second.time += 1;
    let proof = ae_chain::fraud::Proof {
        first: Header::Micro(micro),
        second: Header::Micro(second),
        public_key: key.miner.to_vec(),
    };
    let proof_key = repo::key(repo::PROOFS, key.common.previous_key_hash);
    let record_key = repo::key(repo::RECORDS, parent.0);
    let mut rows = BTreeMap::from([(proof_key.clone(), Some(proof.encode().unwrap()))]);
    // A different proof under the current generation must not be selected.
    rows.insert(repo::key(repo::PROOFS, leader.0), Some(vec![0]));
    let make = |rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>| {
        repo::View::load_with_config(
            Faults {
                source: view.source().clone(),
                rows: Arc::new(rows),
            },
            view.config().clone(),
        )
        .unwrap()
    };
    let with_proof = make(rows.clone());
    for predecessor in [leader, parent] {
        let candidate = pow::micro(&with_proof, &cfg, input(predecessor, 0)).unwrap();
        assert_eq!(candidate.candidate.block().proof.as_deref(), Some(&proof));
    }
    let mut record = view.require_record(parent).unwrap();
    record.fraud = true;
    rows.insert(record_key.clone(), Some(record.encode()));
    assert!(
        pow::micro(&make(rows.clone()), &cfg, input(parent, 0))
            .unwrap()
            .candidate
            .block()
            .proof
            .is_none()
    );
    // Each already-prepared snapshot remains independent of subsequent inputs.
    assert!(
        pow::micro(&with_proof, &cfg, input(parent, 0))
            .unwrap()
            .candidate
            .block()
            .proof
            .is_some()
    );
    for missing in [None, Some(vec![0])] {
        let mut broken = rows.clone();
        broken.insert(record_key.clone(), missing);
        assert!(pow::micro(&make(broken), &cfg, input(parent, 0)).is_err());
    }
    rows.insert(proof_key, Some(vec![0]));
    assert!(matches!(
        pow::micro(&make(rows), &cfg, input(parent, 0)),
        Err(pow::Error::Repository(repo::Error::Corrupt("stored proof")))
    ));
    assert!(!view.require_record(parent).unwrap().fraud);
    assert!(
        view.discovered_proof(Hash256(key.common.previous_key_hash))
            .unwrap()
            .is_none()
    );
}

#[test]
fn pow_candidate_failures_release_snapshot_capacity_and_keep_consensus_guards() {
    let n = Network::Mainnet;
    let temp = Temp::new();
    let db = Arc::new(Database::open(&temp.0, identity(n)).unwrap());
    bootstrap(&db, n);
    let first = blocks(n).remove(0);
    insert(&db, n, &first);
    let owner = snapshots::Owner::start(
        db.clone(),
        snapshots::Config {
            maximum_snapshots: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let cfg = config(n);
    let parent = first.hash().unwrap();
    let before = db.view().unwrap().status();
    for fault in 0..8 {
        let mut bad = cfg.clone();
        let mut request = input(parent, 0);
        match fault {
            0 => bad.identity.consensus = ae_protocol::ConsensusType::Pos,
            1 => bad.identity.execution_profile = Hash256([1; 32]),
            2 => Arc::make_mut(&mut bad.profile).network = NetworkProfile::uat(),
            3 => Arc::make_mut(&mut bad.profile).context.in_auth = true,
            4 => Arc::make_mut(&mut bad.profile).context.on_chain = false,
            5 => bad.execution.maximum_history_headers = 0,
            6 => request.parent = Hash256([99; 32]),
            _ => request.now = i128::MAX,
        }
        assert!(
            storage::prepare_micro(&owner, &bad, request).is_err(),
            "fault {fault}"
        );
        assert_eq!(owner.live_snapshots(), 0);
        assert_eq!(db.view().unwrap().status(), before);
    }
    assert!(matches!(
        repo::hyperchain::reader(
            db.view().unwrap(),
            &cfg.identity,
            cfg.profile.clone(),
            cfg.execution.clone()
        ),
        Err(repo::Error::Identity)
    ));
    let held = storage::prepare_micro(&owner, &cfg, input(parent, 0)).unwrap();
    assert!(matches!(
        storage::prepare_micro(&owner, &cfg, input(parent, 0)),
        Err(storage::Error::Snapshot(snapshots::Error::ResourceLimit))
    ));
    drop(held);
    assert_eq!(owner.live_snapshots(), 0);
    let held = storage::prepare_micro(&owner, &cfg, input(parent, 0)).unwrap();
    assert_eq!(held.metadata.status, before);
}

#[path = "pow_key_candidates.rs"]
mod key_candidates;
