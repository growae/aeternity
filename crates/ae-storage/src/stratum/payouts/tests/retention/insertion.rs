use super::*;
use ae_chain::block::MicroBlock;
use ae_execution::{Context, contracts::aevm, transactions};
use ae_trie::{MemoryStore, Nibbles, Trie};
use ae_tx::CanonicalTransaction;
use serde_json::Value as J;

fn expand(v: &J, pool: &[J]) -> J {
    if let Some(i) = v.get("$ref") {
        return expand(&pool[i.as_u64().unwrap() as usize], pool);
    }
    match v {
        J::Array(a) => J::Array(a.iter().map(|v| expand(v, pool)).collect()),
        J::Object(m) => J::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), expand(v, pool)))
                .collect(),
        ),
        _ => v.clone(),
    }
}
fn field<'a>(v: &'a J, name: &str) -> &'a J {
    &v[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == serde_json::json!(["atom", name]))
        .unwrap()[1]
}
fn bytes(v: &J) -> Vec<u8> {
    assert_eq!(v[0], "binary");
    hex::decode(v[1].as_str().unwrap()).unwrap()
}
fn trie(v: &J) -> Trie<MemoryStore> {
    let mut t = Trie::default();
    for p in v[1].as_array().unwrap() {
        t.put(
            &Nibbles::from_bytes(&bytes(&p[1][0])),
            Value::Bytes(bytes(&p[1][1])),
        )
        .unwrap();
    }
    t
}
fn input(protocol: u32) -> (Trees, ae_mempool::pool::Signed) {
    let packed: J = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../ae-node/tests/fixtures/erlang_stratum_payout_gas.json"
    )))
    .unwrap();
    let data = expand(&packed["root"], packed["values"].as_array().unwrap());
    let row = data["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == protocol && r["case_name"] == "prepared_two")
        .unwrap();
    let accounts = trie(field(&row["before"], "accounts"));
    let contracts = trie(field(&row["before"], "contracts"));
    let calls = trie(field(&row["before"], "calls"));
    let trees = Trees {
        accounts: ae_state::accounts::Accounts::from_root(
            accounts.root_hash(),
            accounts.into_store(),
        )
        .unwrap(),
        contracts: ae_state::contracts::Contracts::from_root(
            contracts.root_hash(),
            contracts.into_store(),
        )
        .unwrap(),
        calls: ae_state::calls::Calls::from_root(calls.root_hash(), calls.into_store()).unwrap(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    assert_eq!(
        trees.roots().hash().0.as_slice(),
        bytes(field(&row["before"], "root"))
    );
    (
        trees,
        ae_mempool::pool::Signed::decode(&bytes(&row["signed"])).unwrap(),
    )
}
fn profile(protocol: u32) -> NetworkProfile {
    NetworkProfile::custom(
        b"ae_receipt_commit_local".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::try_from(protocol).unwrap(),
        }],
    )
    .unwrap()
}
fn vm() -> aevm::Config {
    aevm::Config {
        environment: aevm::Environment {
            genesis_height: 0,
            key_hash: [0; 32],
            history: Arc::default(),
            beneficiary: AccountPublicKey(key(5)),
            difficulty: 1.into(),
            timestamp_ms: 10.into(),
            block_gas_limit: 6_000_000.into(),
            auth_tx_hash: None,
        },
        network_id: Arc::from(b"ae_receipt_commit_local".as_slice()),
        event_context: ae_execution::aevm_chain::EventContext::None,
        float_profile: ae_vm_aevm::gas::FloatProfile::Otp26Word64,
        store_order: ae_vm_aevm::store::OrderProfile::Otp26Word64Crc32c,
    }
}
fn rules(db: &Database) -> ae_chain::key::Rules {
    ae_chain::key::Rules::new(
        ae_chain::admission::PowRules {
            now_ms: u64::MAX - 600_000,
            micro_cycle_ms: 3000,
            configured_gas_limit: 6_000_000.into(),
            body_limits: Default::default(),
        },
        db.view().unwrap().identity().genesis,
    )
}
// Key blocks are admitted-state fixtures. Every tested microblock is
// signed, executed and prepared through the actual ledger and storage commit.
fn key_top(db: &Database, height: u64, prune_calls: bool) -> Hash256 {
    let parent = db.view().unwrap().status().tip.unwrap();
    key_at(db, parent, height, prune_calls, true)
}
fn key_at(
    db: &Database,
    parent: Hash256,
    height: u64,
    prune_calls: bool,
    selected: bool,
) -> Hash256 {
    let view = db.view().unwrap();
    let old = parent;
    let mut trees = view.trees(old, Default::default()).unwrap();
    if prune_calls {
        trees.calls.prune();
    }
    let previous = view.require_block(parent).unwrap().header();
    let previous_key = match previous {
        Header::Key(_) => parent,
        Header::Micro(h) => Hash256(h.common.previous_key_hash),
    };
    let Block::Key(mut header) = view.require_block(previous_key).unwrap() else {
        panic!()
    };
    header.common.height = height;
    header.common.previous_hash = old.0;
    header.common.previous_key_hash = previous_key.0;
    header.common.state_hash = trees.roots().hash().0;
    header.miner = signer().public_key().0;
    header.beneficiary = key(5);
    header.time = height * 10;
    let block = Block::Key(header);
    let hash = block.hash().unwrap();
    let mut record = view.require_record(old).unwrap();
    record.roots = Roots::of(&trees);
    record.difficulty = record.difficulty.add(&1.into());
    let change = repo::Change {
        revision: view.status().revision + 1,
        inserted: Some(hash),
        removed: vec![],
        discarded: vec![],
        old_tip: Some(old),
        new_tip: hash,
        ancestor: Some(old),
        detached: vec![],
        attached: vec![hash],
        requeue: vec![],
        confirm: vec![],
    };
    let mut batch = WriteBatch::default();
    batch.put(repo::key(repo::BLOCKS, hash.0), block.encode().unwrap());
    batch.put(repo::key(repo::RECORDS, hash.0), record.encode());
    if selected {
        batch.put(repo::key(repo::CANONICAL, hash.0), []);
        batch.put(repo::key(repo::KEYS, height.to_be_bytes()), hash.0);
    }
    batch.put(repo::key(repo::CHILDREN, [old.0, hash.0].concat()), []);
    batch.put(
        repo::key(
            repo::HEIGHTS,
            [height.to_be_bytes().as_slice(), hash.0.as_slice()].concat(),
        ),
        [],
    );
    if selected {
        batch.put(
            repo::key(repo::EVENTS, change.revision.to_be_bytes()),
            change.encode(),
        );
        batch.put(
            repo::STATUS,
            repo::Status {
                revision: change.revision,
                tip: Some(hash),
            }
            .encode(),
        );
    }
    write(db, batch).unwrap();
    hash
}
struct Setup {
    db: Arc<Database>,
    network: NetworkProfile,
    config: Config,
    record: Record,
    signed: ae_mempool::pool::Signed,
    parent: Hash256,
}
fn setup(temp: &Temp, protocol: u32) -> Setup {
    let network = profile(protocol);
    let (trees, signed) = input(protocol);
    let transaction = signed.transaction().clone();
    let mut signing = network.signing_prefix(false);
    signing.extend(transaction.encode());
    let signed = ae_mempool::pool::Signed::new(transaction, vec![signer().sign(&signing)]);
    let Header::Key(mut genesis) = genesis::header(Network::Uat).unwrap() else {
        panic!()
    };
    genesis.common.version = protocol;
    genesis.common.state_hash = trees.roots().hash().0;
    let id = repo::Identity::new(
        &network,
        Header::Key(genesis.clone()).hash().unwrap(),
        Hash256::ZERO,
    );
    let db = Arc::new(Database::open(&temp.0, id).unwrap());
    db.commit(
        ledger::bootstrap(
            &db.view().unwrap(),
            &network,
            &genesis,
            &trees,
            Default::default(),
        )
        .unwrap(),
    )
    .unwrap();
    let parent = key_top(&db, 1, false);
    let ae_tx::TransactionLeaf::Contract(tx) = signed.transaction().leaf() else {
        panic!()
    };
    let ae_tx::ContractOperation::Call { contract, .. } = tx.operation() else {
        panic!()
    };
    let config = Config {
        caller: tx.signer(),
        contract: ContractPublicKey(contract.value),
        delay: 1,
    };
    assert_eq!(config.caller, signer().public_key());
    let mut w = Writer::start(db.clone(), Default::default()).unwrap();
    let mut payment = pay((100, 0));
    payment.tx_hash = Some(signed.hash());
    commit(&mut w, vec![Command::Payment(payment)]);
    reward(&mut w, true);
    let info = w.begin_payouts(config.clone(), Default::default()).unwrap();
    Setup {
        db,
        network,
        config,
        record: info.pending.unwrap(),
        signed,
        parent,
    }
}
fn micro(
    s: &Setup,
    parent: Hash256,
    time: u64,
    transactions: Vec<ae_mempool::pool::Signed>,
) -> Block {
    let view = s.db.view().unwrap();
    let previous = view.require_block(parent).unwrap().header();
    let key = match &previous {
        Header::Key(_) => parent,
        Header::Micro(m) => Hash256(m.common.previous_key_hash),
    };
    let mut trees = view.trees(parent, Default::default()).unwrap();
    let vm = vm();
    let config = transactions::Config {
        aevm: Some(&vm),
        ..transactions::Config::primitives(&s.network)
    };
    transactions::batch::apply_micro(
        &transactions,
        &mut trees,
        &Context::transaction(
            previous.common().height,
            s.network.protocol_at(previous.common().height).unwrap(),
        ),
        &config,
        6_000_000.into(),
        0.into(),
    )
    .unwrap();
    let mut header = ae_codec::headers::MicroHeader {
        common: previous.common().clone(),
        transactions_hash: ae_trie::transaction_root(
            transactions
                .iter()
                .map(|t| t.encode())
                .collect::<Vec<_>>()
                .iter()
                .map(Vec::as_slice),
        )
        .unwrap()
        .0,
        time,
        proof_of_fraud: None,
        signature: [0; 64],
    };
    header.common.previous_hash = parent.0;
    header.common.previous_key_hash = key.0;
    header.common.state_hash = trees.roots().hash().0;
    header.signature = signer()
        .sign(&Header::Micro(header.clone()).signature_bytes().unwrap())
        .0;
    Block::Micro(MicroBlock {
        header,
        transactions,
        proof: None,
    })
}
fn plan(s: &Setup, block: &Block, limits: ledger::Limits) -> repo::Update {
    let vm = vm();
    let config = transactions::Config {
        aevm: Some(&vm),
        ..transactions::Config::primitives(&s.network)
    };
    ledger::prepare(&s.db.view().unwrap(), block, &config, &rules(&s.db), limits).unwrap()
}

#[test]
fn original_contract_inclusions_archive_with_real_commits_during_gc_and_side_branch_selection() {
    for protocol in [3, 6, 8] {
        for phase in [repo::retention::Phase::Roots, repo::retention::Phase::Sweep] {
            let temp = Temp::new();
            let s = setup(&temp, protocol);
            s.db.begin_gc(Default::default()).unwrap();
            while s.db.gc_state().unwrap().phase != phase {
                s.db.gc_step(crate::gc::Limits {
                    rows: 1,
                    ..Default::default()
                })
                .unwrap();
            }
            let first = micro(&s, s.parent, 20, vec![s.signed.clone()]);
            let first_hash = first.hash().unwrap();
            let update = plan(&s, &first, Default::default());
            assert_eq!(
                update.inserted_block(&s.db.view().unwrap()).unwrap(),
                Some(first.clone())
            );
            s.db.commit(update).unwrap();
            assert!(!archived(&s.db).is_empty());
            let second = micro(&s, s.parent, 21, vec![s.signed.clone()]);
            let second_hash = second.hash().unwrap();
            s.db.commit(plan(&s, &second, Default::default())).unwrap();
            assert_eq!(
                s.db.view().unwrap().location(s.signed.hash()).unwrap(),
                Some(first_hash)
            );
            // Microblock count cannot win an equal-work PoW fork. Supply an
            // admitted heavier key, then let real ledger selection attach it.
            let side_key = key_at(&s.db, second_hash, 2, true, false);
            let child = micro(&s, side_key, 30, vec![]);
            s.db.commit(plan(&s, &child, Default::default())).unwrap();
            assert_eq!(
                s.db.view().unwrap().location(s.signed.hash()).unwrap(),
                Some(second_hash)
            );
            sweep(&s.db);
            let root =
                s.db.view()
                    .unwrap()
                    .require_record(second_hash)
                    .unwrap()
                    .roots
                    .state
                    .calls
                    .unwrap();
            key_top(&s.db, 3, true);
            s.db.begin_gc(Default::default()).unwrap();
            sweep(&s.db);
            assert!(
                s.db.db
                    .get(repo::key(repo::NODES, root.0))
                    .unwrap()
                    .is_none()
            );
            let id = s.db.view().unwrap().identity().clone();
            let Setup {
                db, config, record, ..
            } = s;
            drop(db);
            let db = Arc::new(Database::open(&temp.0, id).unwrap());
            let mut w = Writer::start(db.clone(), Default::default()).unwrap();
            let info = w.begin_payouts(config, Default::default()).unwrap();
            let Confirmation::Confirmed(done) = w
                .confirm_payout(info.epoch, &record, Default::default())
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(done.block, second_hash);
            assert!(db.db.get(archive::KEY).unwrap().is_none());
        }
    }
    println!(
        "Retained payout insertion: six original-contract forks survive active GC, reorg and reopen"
    );
}

#[test]
fn receipt_bytes_count_toward_commit_limits_and_lost_acknowledgments_preserve_one_inclusion() {
    for failure in [1, 2] {
        let temp = Temp::new();
        let s = setup(&temp, 3);
        let block = micro(&s, s.parent, 20, vec![s.signed.clone()]);
        let update = plan(&s, &block, Default::default());
        let base = crate::pool::write_bytes(update.writes()).unwrap()
            + crate::pool::write_bytes(
                &s.db
                    .pool_chain_writes(update.change(), update.writes())
                    .unwrap(),
            )
            .unwrap();
        let before = rows(&s.db);
        let tight = plan(
            &s,
            &block,
            ledger::Limits {
                commit_bytes: base,
                ..Default::default()
            },
        );
        assert!(matches!(s.db.commit(tight), Err(Error::ResourceLimit)));
        assert_eq!(rows(&s.db), before);
        s.db.event_write_failure
            .store(failure, std::sync::atomic::Ordering::Relaxed);
        assert!(s.db.commit(update).is_err());
        if failure == 1 {
            assert_eq!(rows(&s.db), before);
        } else {
            assert_eq!(
                s.db.view().unwrap().location(s.signed.hash()).unwrap(),
                Some(block.hash().unwrap())
            );
            assert!(!archived(&s.db).is_empty());
        }
        let id = s.db.view().unwrap().identity().clone();
        let Setup {
            db,
            network,
            config,
            record,
            signed,
            parent,
        } = s;
        drop(db);
        let db = Arc::new(Database::open(&temp.0, id).unwrap());
        let s = Setup {
            db,
            network,
            config,
            record,
            signed,
            parent,
        };
        if failure == 1 {
            s.db.commit(plan(&s, &block, Default::default())).unwrap();
        } else {
            assert!(matches!(
                ledger::prepare(
                    &s.db.view().unwrap(),
                    &block,
                    &transactions::Config {
                        aevm: Some(&vm()),
                        ..transactions::Config::primitives(&s.network)
                    },
                    &rules(&s.db),
                    Default::default()
                ),
                Err(ledger::InsertError::Repository(Error::AlreadyPresent))
            ));
        }
        assert_eq!(
            s.db.view().unwrap().location(s.signed.hash()).unwrap(),
            Some(block.hash().unwrap())
        );
        assert!(!archived(&s.db).is_empty());
        assert_eq!(stored(&s.db).pending, Some(s.record));
    }
}

mod recovery;
