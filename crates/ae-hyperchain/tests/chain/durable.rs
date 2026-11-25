use super::{bytes, chain, genesis, trees};
use ae_chain::{
    block::{Block, MicroBlock},
    repository::{self as repo, Read, hyperchain as durable},
};
use ae_codec::headers::{Header, HeaderMode};
use ae_hyperchain::chain::{LookupError, RunEnv};
use ae_trie::NodeReader;
use ae_types::Hash256;
use num_bigint::BigInt;
use serde_json::{Value as J, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock},
};

static DATA: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../fixtures/erlang_hc_selected.json")).unwrap()
});

// Lookup fixtures, not admitted PoS blocks. Headers and contract roots come
// from native queries; the production importer remains explicitly PoW-only.
#[derive(Clone, Debug, Default)]
struct Memory {
    entries: Arc<BTreeMap<Vec<u8>, Vec<u8>>>,
    failure: Option<Vec<u8>>,
}
impl Memory {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        Arc::make_mut(&mut self.entries).insert(key, value);
    }
    fn remove(&mut self, key: &[u8]) {
        Arc::make_mut(&mut self.entries).remove(key);
    }
    fn view(&self) -> repo::View<Self> {
        repo::View::load_with_config(
            self.clone(),
            repo::ReadConfig {
                mode: HeaderMode::Hyperchain,
                ..Default::default()
            },
        )
        .unwrap()
    }
}
impl NodeReader for Memory {
    fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.get(&repo::key(repo::NODES, h.0))
            .map_err(|e| ae_trie::Error::Backend(e.to_string()))
    }
}
impl Read for Memory {
    fn source_id(&self) -> u64 {
        1
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, repo::Error> {
        if self.failure.as_deref() == Some(key) {
            return Err(repo::Error::Backend("injected lookup".into()));
        }
        Ok(self.entries.get(key).cloned())
    }
    fn prefix(&self, prefix: &[u8], limit: usize) -> Result<repo::Entries, repo::Error> {
        let v: Vec<_> = self
            .entries
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .take(limit.saturating_add(1))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if v.len() > limit {
            Err(repo::Error::ResourceLimit)
        } else {
            Ok(v)
        }
    }
    fn range(&self, start: &[u8], end: &[u8], limit: usize) -> Result<repo::Entries, repo::Error> {
        let v: Vec<_> = self
            .entries
            .range(start.to_vec()..end.to_vec())
            .take(limit.saturating_add(1))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if v.len() > limit {
            Err(repo::Error::ResourceLimit)
        } else {
            Ok(v)
        }
    }
    fn visit_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        visit: &mut repo::Visitor<'_>,
    ) -> Result<(), repo::Error> {
        for (k, v) in self.range(start, end, limit)? {
            visit(&k, &v)?;
        }
        Ok(())
    }
}
fn hash(j: &J) -> Hash256 {
    Hash256(bytes(j).try_into().unwrap())
}
fn state(row: &J) -> &J {
    genesis::DATA["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["protocol"] == row["protocol"])
        .unwrap()
}
fn prepare(row: &J, maximum: bool) -> (Memory, repo::Identity) {
    let g = state(row);
    let profile = genesis::reader(g).profile;
    let headers: Vec<&J> = if maximum {
        vec![&row["maximum_header"]]
    } else {
        row["blocks"].as_array().unwrap().iter().collect()
    };
    let gh = hash(&headers[0]["hash"]);
    let identity = repo::Identity::for_consensus(
        &profile.network,
        gh,
        ae_crypto::blake2b_256(b"HC immutable lookup fixture"),
        ae_protocol::ConsensusType::Pos,
    );
    let mut memory = Memory::default();
    memory.put(repo::IDENTITY.to_vec(), identity.encode());
    for b in &headers {
        let state = match b["state"].as_str().unwrap() {
            "genesis" => &g["stages"][5]["trees"],
            "key" => &g["lifecycle"][0]["trees"],
            "micro" => &g["lifecycle"][1]["trees"],
            other => panic!("{other}"),
        };
        let t = trees(state);
        let header = Header::decode(&bytes(&b["header"]), HeaderMode::Hyperchain).unwrap();
        assert_eq!(header.common().state_hash, t.roots().hash().0);
        let bh = hash(&b["hash"]);
        assert_eq!(header.hash().unwrap(), bh);
        let difficulty = match &header {
            Header::Key(k) => k.target,
            Header::Micro(_) => 0,
        };
        let height = header.common().height;
        let canonical = b["name"] != "side";
        if canonical {
            memory.put(repo::key(repo::CANONICAL, bh.0), vec![]);
            if matches!(header, Header::Key(_)) {
                memory.put(repo::key(repo::KEYS, height.to_be_bytes()), bh.0.to_vec());
            }
        }
        let block = match header {
            Header::Key(k) => Block::Key(k),
            Header::Micro(m) => Block::Micro(MicroBlock {
                header: m,
                transactions: vec![],
                proof: None,
            }),
        };
        memory.put(repo::key(repo::BLOCKS, bh.0), block.encode().unwrap());
        memory.put(
            repo::key(repo::RECORDS, bh.0),
            repo::Record {
                roots: repo::Roots::of(&t),
                difficulty: u64::from(difficulty).into(),
                fees: 0.into(),
                fraud: false,
                fork: gh,
            }
            .encode(),
        );
        for store in [
            t.accounts.tree().store(),
            t.calls.tree().store(),
            t.contracts.tree().store(),
            t.channels.tree().store(),
            t.names.tree().store(),
            t.names.cache().store(),
            t.oracles.tree().store(),
            t.oracles.cache().store(),
        ] {
            for (h, v) in store.nodes() {
                memory.put(repo::key(repo::NODES, h.0), v.clone());
            }
        }
    }
    let tip = if maximum {
        gh
    } else {
        hash(&headers[2]["hash"])
    };
    memory.put(
        repo::STATUS.to_vec(),
        repo::Status {
            revision: if maximum { 1 } else { 3 },
            tip: Some(tip),
        }
        .encode(),
    );
    (memory, identity)
}
fn env<A, C>(case: &J) -> RunEnv<A, C> {
    let v = &case["run_env"];
    match v[0].as_str().unwrap() {
        "atom" => {
            assert_eq!(v[1], "top");
            RunEnv::Top
        }
        "integer" => RunEnv::Height(v[1].as_str().unwrap().parse().unwrap()),
        "tuple" => RunEnv::Hash(bytes(&v[1][1][1])),
        other => panic!("{other}"),
    }
}
fn projection<S: NodeReader>(v: &chain::View<ae_trie::Overlay<S>, ae_trie::Overlay<S>>) -> J {
    json!({"height":v.context.height,"protocol":v.context.protocol as u32,"key_hash":hex::encode(v.environment.key_hash),"beneficiary":hex::encode(v.environment.beneficiary),"time":v.environment.timestamp.to_string(),"difficulty":v.environment.difficulty.to_string(),"root":hex::encode(v.trees.roots().hash().0)})
}
#[test]
fn original_key_micro_side_and_maximum_time_queries_match_persisted_views() {
    for row in DATA["rows"].as_array().unwrap() {
        for maximum in [false, true] {
            let (memory, id) = prepare(row, maximum);
            let before = memory.entries.clone();
            let reader = durable::reader(
                memory.view(),
                &id,
                genesis::reader(state(row)).profile,
                Default::default(),
            )
            .unwrap();
            let cases: Vec<_> = if maximum {
                vec![&row["maximum"]]
            } else {
                row["good"].as_array().unwrap().iter().collect()
            };
            for c in cases {
                let v = reader.view(&env(c)).unwrap();
                assert_eq!(
                    projection(&v),
                    c["projection"],
                    "protocol {} / {}",
                    row["protocol"],
                    c["label"]
                );
                genesis::queries_env(&reader, env(c), &c["queries"]);
                assert_eq!(memory.entries, before);
            }
        }
    }
}
#[test]
fn selected_reads_keep_old_roots_indexes_and_history_across_replacement_and_pruning() {
    let row = &DATA["rows"][2];
    let (mut memory, id) = prepare(row, false);
    let profile = genesis::reader(state(row)).profile;
    let old = durable::reader(memory.view(), &id, profile.clone(), Default::default()).unwrap();
    let key = hash(&row["blocks"][1]["hash"]);
    let micro = hash(&row["blocks"][2]["hash"]);
    let side = hash(&row["blocks"][3]["hash"]);
    let original = old.view(&RunEnv::Top).unwrap();
    let genesis_hash = hash(&row["blocks"][0]["hash"]);
    assert_eq!(
        original.history.selected,
        BTreeMap::from([(0, genesis_hash.0)])
    );
    assert!(original.history.headers.contains_key(&key.0));
    assert!(!original.history.headers.contains_key(&side.0));
    memory.put(repo::key(repo::KEYS, 1u64.to_be_bytes()), side.0.to_vec());
    memory.put(repo::key(repo::CANONICAL, side.0), vec![]);
    memory.remove(&repo::key(repo::CANONICAL, key.0));
    memory.remove(&repo::key(repo::CANONICAL, micro.0));
    memory.put(
        repo::STATUS.to_vec(),
        repo::Status {
            revision: 4,
            tip: Some(side),
        }
        .encode(),
    );
    let fresh = durable::reader(memory.view(), &id, profile.clone(), Default::default()).unwrap();
    let side_view = fresh.view(&RunEnv::Top).unwrap();
    assert_eq!(side_view.history.selected, original.history.selected);
    assert!(side_view.history.headers.contains_key(&side.0));
    assert!(!side_view.history.headers.contains_key(&key.0));
    let old_branch = fresh.view(&RunEnv::Hash(key.0.to_vec())).unwrap();
    assert!(old_branch.history.headers.contains_key(&side.0));
    assert!(old_branch.history.headers.contains_key(&key.0));
    let ancestry =
        ae_execution::chain_history::select(&old_branch.history, 0, key.0, || Ok(())).unwrap();
    assert_eq!(
        (ancestry.selected, ancestry.branch),
        (genesis_hash.0, genesis_hash.0)
    );
    assert_eq!(
        projection(&old.view(&RunEnv::Top).unwrap()),
        projection(&original)
    );
    assert!(!original.history.headers.contains_key(&side.0));
    assert_eq!(
        fresh.view(&RunEnv::Top).unwrap().environment.key_hash,
        side.0
    );
    assert_ne!(
        fresh.view(&RunEnv::Top).unwrap().trees.roots(),
        original.trees.roots()
    );
    // Explicit hashes still resolve stored side branches.
    assert_eq!(
        fresh
            .view(&RunEnv::Hash(key.0.to_vec()))
            .unwrap()
            .trees
            .roots(),
        original.trees.roots()
    );
    memory.remove(&repo::key(repo::RECORDS, key.0));
    let pruned = durable::reader(memory.view(), &id, profile, Default::default()).unwrap();
    let error = pruned.view(&RunEnv::Hash(key.0.to_vec())).unwrap_err();
    assert!(matches!(
        error,
        chain::Error::Lookup(LookupError::StateGarbageCollected)
    ));
    assert!(error.is_operational());
    assert_eq!(
        projection(&old.view(&RunEnv::Top).unwrap()),
        projection(&original)
    );
    assert_eq!(
        fresh
            .view(&RunEnv::Hash(key.0.to_vec()))
            .unwrap()
            .trees
            .roots(),
        original.trees.roots()
    );
    assert_eq!(
        row["pruned"]["queries"][0]["result"]["reason"],
        json!(["atom", "state_garbage_collected"])
    );
}

#[test]
fn corrupt_indexes_state_bindings_and_node_failures_never_fall_back_to_other_state() {
    let row = &DATA["rows"][2];
    let (memory, id) = prepare(row, false);
    let profile = genesis::reader(state(row)).profile;
    let key = hash(&row["blocks"][1]["hash"]);
    let micro = hash(&row["blocks"][2]["hash"]);
    let side = hash(&row["blocks"][3]["hash"]);
    let before = memory.entries.clone();
    for mode in 0..5 {
        let mut broken = memory.clone();
        match mode {
            0 => broken.put(repo::key(repo::KEYS, 1u64.to_be_bytes()), micro.0.to_vec()),
            1 => broken.remove(&repo::key(repo::CANONICAL, key.0)),
            2 => broken.put(
                repo::key(repo::RECORDS, key.0),
                memory
                    .get(&repo::key(repo::RECORDS, side.0))
                    .unwrap()
                    .unwrap(),
            ),
            3 => broken.put(
                repo::key(repo::BLOCKS, key.0),
                memory
                    .get(&repo::key(repo::BLOCKS, side.0))
                    .unwrap()
                    .unwrap(),
            ),
            4 => {
                let record = memory.view().require_record(key).unwrap();
                broken.failure = Some(repo::key(
                    repo::NODES,
                    record.roots.state.accounts.unwrap().0,
                ));
            }
            _ => unreachable!(),
        }
        let reader =
            durable::reader(broken.view(), &id, profile.clone(), Default::default()).unwrap();
        let error = reader.epoch(&RunEnv::Top).unwrap_err();
        assert!(error.is_operational(), "mode {mode}: {error:?}");
        if mode < 4 {
            assert!(matches!(
                error,
                chain::Error::Lookup(LookupError::Backend(_))
            ));
        }
        assert_eq!(memory.entries, before);
    }
    let reader = durable::reader(memory.view(), &id, profile.clone(), Default::default()).unwrap();
    assert!(reader.epoch(&RunEnv::Top).is_ok());
    let mut retired = memory.clone();
    retired.put(
        repo::GC_STATE.to_vec(),
        repo::retention::State {
            epoch: 1,
            height: 1,
            floors: [1; 8],
            phase: repo::retention::Phase::Complete,
            ..Default::default()
        }
        .encode(),
    );
    let r = durable::reader(retired.view(), &id, profile, Default::default()).unwrap();
    let error = r.epoch(&RunEnv::Height(0.into())).unwrap_err();
    assert!(error.is_operational());
    assert!(matches!(
        error,
        chain::Error::Lookup(LookupError::StateGarbageCollected)
    ));
    assert!(r.epoch(&RunEnv::Top).is_ok());
    assert!(reader.epoch(&RunEnv::Height(0.into())).is_ok());
}
#[test]
fn native_missing_input_errors_and_operational_storage_failures_remain_distinct() {
    let row = &DATA["rows"][0];
    let (memory, id) = prepare(row, false);
    let profile = genesis::reader(state(row)).profile;
    let reader = durable::reader(memory.view(), &id, profile.clone(), Default::default()).unwrap();
    for c in row["bad"].as_array().unwrap() {
        let e = reader.epoch(&env(c)).unwrap_err();
        assert!(!e.is_operational());
        match c["label"].as_str().unwrap() {
            "future" | "huge" => {
                assert!(matches!(e, chain::Error::ChainTooShort));
                assert_eq!(
                    c["queries"][0]["result"]["value"],
                    json!(["tuple", [["atom", "error"], ["atom", "chain_too_short"]]])
                );
                assert!(matches!(
                    reader.epoch_info(&env(c)),
                    Err(chain::Error::Native(chain::Native::BadmatchChainTooShort))
                ));
            }
            "negative" => {
                assert!(matches!(
                    e,
                    chain::Error::Native(chain::Native::FunctionClause)
                ));
                assert_eq!(
                    c["queries"][0]["result"]["reason"],
                    json!(["atom", "function_clause"])
                );
            }
            "unknown" | "short_hash" => {
                assert!(matches!(e, chain::Error::Lookup(LookupError::InvalidHash)));
                assert_eq!(
                    c["queries"][0]["result"]["reason"],
                    json!(["atom", "invalid_hash"])
                );
            }
            other => panic!("{other}"),
        }
    }
    let key = hash(&row["blocks"][1]["hash"]);
    for failure in [
        repo::key(repo::KEYS, 1u64.to_be_bytes()),
        repo::key(repo::BLOCKS, key.0),
        repo::key(repo::RECORDS, key.0),
    ] {
        let mut failed = memory.clone();
        failed.failure = Some(failure);
        let r = durable::reader(failed.view(), &id, profile.clone(), Default::default()).unwrap();
        let e = r.epoch(&RunEnv::Top).unwrap_err();
        assert!(e.is_operational());
        assert!(matches!(e, chain::Error::Lookup(LookupError::Backend(_))));
    }
    assert!(reader.epoch(&RunEnv::Top).is_ok());
    // HC micro blocks can advance the height before a key at that height
    // exists. Top then has no canonical key; a hash query still uses the
    // preceding key's context and the requested micro state/time.
    let mut open_generation = memory.clone();
    let advanced = hash(&row["blocks"][4]["hash"]);
    open_generation.put(
        repo::STATUS.to_vec(),
        repo::Status {
            revision: 4,
            tip: Some(advanced),
        }
        .encode(),
    );
    let open = durable::reader(
        open_generation.view(),
        &id,
        profile.clone(),
        Default::default(),
    )
    .unwrap();
    assert!(matches!(
        open.epoch(&RunEnv::Top),
        Err(chain::Error::ChainTooShort)
    ));
    assert!(open.epoch(&RunEnv::Hash(advanced.0.to_vec())).is_ok());
    assert_eq!(
        row["open_generation"]["queries"][0]["result"]["value"],
        json!(["tuple", [["atom", "error"], ["atom", "chain_too_short"]]])
    );
    let mut empty = memory.clone();
    empty.put(repo::STATUS.to_vec(), repo::Status::default().encode());
    let r = durable::reader(empty.view(), &id, profile, Default::default()).unwrap();
    assert!(matches!(
        r.epoch(&RunEnv::Top),
        Err(chain::Error::ChainTooShort)
    ));
    assert_eq!(
        row["empty"]["queries"][0]["result"]["value"],
        json!(["tuple", [["atom", "error"], ["atom", "chain_too_short"]]])
    );
}

#[test]
fn context_binding_history_and_write_limits_preserve_inputs_and_allow_retry() {
    use ae_execution::contracts::aevm;
    let row = &DATA["rows"][1];
    let (memory, id) = prepare(row, true);
    let profile = genesis::reader(state(row)).profile;
    let template = aevm::Config {
        environment: aevm::Environment {
            genesis_height: 99,
            key_hash: [99; 32],
            history: Default::default(),
            beneficiary: ae_types::AccountPublicKey([99; 32]),
            difficulty: (-1).into(),
            timestamp_ms: (-1).into(),
            block_gas_limit: 1.into(),
            auth_tx_hash: Some(vec![1; 32]),
        },
        network_id: Arc::from(b"unbound template".as_slice()),
        event_context: ae_execution::aevm_chain::EventContext::None,
        float_profile: ae_vm_aevm::gas::FloatProfile::Otp26Word64,
        store_order: ae_vm_aevm::store::OrderProfile::Otp26Word64Jenkins,
    };
    let configured = durable::Config {
        configured_block_gas_limit: 7_000_000.into(),
        aevm: Some(template.clone()),
        ..Default::default()
    };
    let reader = durable::reader(memory.view(), &id, profile.clone(), configured).unwrap();
    let v = reader.view(&RunEnv::Top).unwrap();
    let a = v.aevm.as_ref().unwrap();
    assert_eq!(a.environment.genesis_height, 0);
    assert_eq!(a.environment.key_hash, v.environment.key_hash);
    assert_eq!(a.environment.beneficiary.0, v.environment.beneficiary);
    assert_eq!(a.environment.timestamp_ms, BigInt::from(u64::MAX) + 1);
    assert_eq!(a.environment.difficulty, 0.into());
    assert_eq!(a.environment.block_gas_limit, 7_000_000.into());
    assert_eq!(a.environment.auth_tx_hash, None);
    assert_eq!(a.network_id.as_ref(), profile.network.network_id());
    assert!(Arc::ptr_eq(&a.environment.history, &v.history));
    assert_eq!(template.environment.timestamp_ms, (-1).into());
    let before = memory.entries.clone();
    for limits in [
        durable::Config {
            maximum_history_headers: 0,
            ..Default::default()
        },
        durable::Config {
            overlays: ae_trie::OverlayLimits { nodes: 0, bytes: 0 },
            ..Default::default()
        },
    ] {
        let limited = durable::reader(memory.view(), &id, profile.clone(), limits).unwrap();
        let e = limited.epoch(&RunEnv::Top).unwrap_err();
        assert!(e.is_operational(), "{e:?}");
        assert_eq!(memory.entries, before);
        assert!(reader.epoch(&RunEnv::Top).is_ok());
    }
    for mode in 0..4 {
        let mut bad = profile.clone();
        let p = Arc::make_mut(&mut bad);
        match mode {
            0 => p.context.in_auth = true,
            1 => p.context.on_chain = false,
            2 => p.context.reserved_gas = 1.into(),
            3 => p.network = ae_protocol::NetworkProfile::uat(),
            _ => unreachable!(),
        }
        assert!(matches!(
            durable::reader(memory.view(), &id, bad, Default::default()),
            Err(repo::Error::Identity)
        ));
    }
    let mut wrong = id.clone();
    wrong.consensus = ae_protocol::ConsensusType::Pow;
    assert!(matches!(
        durable::reader(memory.view(), &wrong, profile, Default::default()),
        Err(repo::Error::Identity)
    ));
}

#[test]
fn pin_preflight_uses_incoming_predecessor_even_when_selected_tip_changes() {
    // Lookup fixture assembled from original contract states, not an admitted
    // signed chain. Durable signed pin-success blocks remain separate evidence.
    fn extend(memory: &mut Memory, mut parent: Hash256) -> (Hash256, Vec<(u64, Hash256)>) {
        let view = memory.view();
        let h = view.require_block(parent).unwrap().header();
        let mut previous_key = match h {
            Header::Key(_) => parent,
            Header::Micro(m) => Hash256(m.common.previous_key_hash),
        };
        let Block::Key(mut k) = view.require_block(previous_key).unwrap() else {
            panic!()
        };
        let record = view.require_record(parent).unwrap();
        let mut chain = vec![(k.common.height, previous_key)];
        for height in (k.common.height + 1)..6 {
            k.common.height = height;
            k.common.previous_hash = parent.0;
            k.common.previous_key_hash = previous_key.0;
            k.common.state_hash = record.roots.state.hash().0;
            k.time += 1;
            let b = Block::Key(k.clone());
            parent = b.hash().unwrap();
            previous_key = parent;
            memory.put(repo::key(repo::BLOCKS, parent.0), b.encode().unwrap());
            memory.put(repo::key(repo::RECORDS, parent.0), record.encode());
            chain.push((height, parent));
        }
        (parent, chain)
    }
    let pinned = super::unpack(include_str!("../fixtures/erlang_hc_block.json"));
    for row in DATA["rows"].as_array().unwrap() {
        let (mut memory, id) = prepare(row, false);
        let pin_row = pinned["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["protocol"] == row["protocol"])
            .unwrap();
        let t = trees(&pin_row["pin_initial"]);
        let selected = memory.view().status().tip.unwrap();
        let native_side = row["blocks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|b| b["name"] == "side")
            .unwrap();
        let Header::Key(mut header) =
            Header::decode(&bytes(&native_side["header"]), HeaderMode::Hyperchain).unwrap()
        else {
            panic!()
        };
        header.common.state_hash = t.roots().hash().0;
        let parent = Block::Key(header.clone());
        let hash = parent.hash().unwrap();
        memory.put(repo::key(repo::BLOCKS, hash.0), parent.encode().unwrap());
        memory.put(
            repo::key(repo::RECORDS, hash.0),
            repo::Record {
                roots: repo::Roots::of(&t),
                difficulty: 1.into(),
                fees: 0.into(),
                fraud: false,
                fork: id.genesis,
            }
            .encode(),
        );
        for store in [
            t.accounts.tree().store(),
            t.contracts.tree().store(),
            t.calls.tree().store(),
        ] {
            for (h, v) in store.nodes() {
                memory.put(repo::key(repo::NODES, h.0), v.clone());
            }
        }
        let mut profile = genesis::reader(state(row)).profile.as_ref().clone();
        profile.maximum_steps = 1_000_000;
        let config = repo::HyperchainConfig {
            identity: id.clone(),
            profile: Arc::new(profile),
            rules: ae_chain::hyperchain::Rules::new(
                id.genesis,
                ae_hyperchain::block::Config {
                    parent_epoch_length: 5.into(),
                    parent_start: 1.into(),
                    parent_finality: 2.into(),
                    pin_sync_margin: 1.into(),
                    child_block_time: 4.into(),
                    cache_limits: Default::default(),
                },
            ),
            aevm: None,
            owned: None,
            limits: Default::default(),
        };
        let (hash, side_chain) = extend(&mut memory, hash);
        let (old_tip, _) = extend(&mut memory, selected);
        header.common.height = 6;
        header.common.previous_hash = hash.0;
        header.common.previous_key_hash = hash.0;
        let side = Block::Key(header.clone());
        let before = memory.view();
        let query = |view: &repo::View<Memory>, block: &Block| {
            repo::ledger::hyperchain_pin_request(view, block, &config).unwrap()
        };
        assert_eq!(query(&before, &side), Some(super::key(777).to_vec()));
        header.common.previous_hash = old_tip.0;
        header.common.previous_key_hash = old_tip.0;
        let old_branch = Block::Key(header);
        assert_eq!(query(&before, &old_branch), None);
        memory.put(
            repo::STATUS.to_vec(),
            repo::Status {
                revision: 4,
                tip: Some(hash),
            }
            .encode(),
        );
        for (height, hash) in &side_chain {
            memory.put(repo::key(repo::KEYS, height.to_be_bytes()), hash.0.to_vec());
        }
        assert_eq!(query(&memory.view(), &old_branch), None);
        assert_eq!(query(&memory.view(), &side), Some(super::key(777).to_vec()));
        assert_eq!(before.status().tip, Some(selected));
        memory.failure = Some(repo::key(repo::RECORDS, hash.0));
        let error =
            repo::ledger::hyperchain_pin_request(&memory.view(), &side, &config).unwrap_err();
        assert!(error.is_operational());
        assert_eq!(query(&before, &side), Some(super::key(777).to_vec()));
    }
}
