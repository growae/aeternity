// Fixtures from unchanged production aect_* modules at fa2a6d7.
use ae_protocol::{Protocol, contracts::Version};
use ae_state::{
    calls::{Call, Calls, LogEntry, ReturnType, call_id, ga_call_id},
    contracts::{
        Code, Contract, ContractProof, Contracts, Error, LoadOptions, Nonce, contract_id,
        store::{Contents, Store},
    },
};
use ae_trie::{MemoryStore, Nibbles, NodeReader, NodeWriter, Trie, Value};
use ae_types::{AccountPublicKey, Hash256, Id, IdTag, Unsigned};
use serde_json::{Value as J, json};

fn fixture() -> J {
    serde_json::from_str(include_str!("fixtures/erlang_contract_state.json")).unwrap()
}
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn key(j: &J) -> [u8; 32] {
    bytes(j).try_into().unwrap()
}
fn number(j: &J) -> Unsigned {
    Unsigned::from_decimal(j.as_str().unwrap()).unwrap()
}
fn decimal(n: &Unsigned) -> String {
    let mut n = n.clone();
    let mut digits = vec![];
    loop {
        let (q, r) = n.div_rem_u64(10).unwrap();
        digits.push((b'0' + r as u8) as char);
        n = q;
        if n == 0.into() {
            break;
        }
    }
    digits.into_iter().rev().collect()
}
fn small(n: u64) -> [u8; 32] {
    let mut k = [0; 32];
    k[24..].copy_from_slice(&n.to_be_bytes());
    k
}
fn pairs(j: &J) -> Contents {
    j.as_array()
        .unwrap()
        .iter()
        .map(|p| (bytes(&p[0]), bytes(&p[1])))
        .collect()
}
fn as_pairs(m: &Contents) -> J {
    json!(
        m.iter()
            .map(|(k, v)| [hex::encode(k), hex::encode(v)])
            .collect::<Vec<_>>()
    )
}
fn version(j: &J) -> Version {
    Version {
        vm: j["vm"].as_u64().unwrap() as u16,
        abi: j["abi"].as_u64().unwrap() as u16,
    }
}
fn contract(j: &J) -> Contract {
    Contract {
        public_key: key(&j["key"]),
        owner: AccountPublicKey(key(&j["owner"])),
        version: version(j),
        code: if j["code"]["inline"].is_string() {
            Code::Inline(bytes(&j["code"]["inline"]))
        } else {
            Code::Reference(Id::from_bytes(&bytes(&j["code"]["ref"])).unwrap())
        },
        log: bytes(&j["log"]),
        active: j["active"].as_bool().unwrap(),
        referrers: j["referrers"].as_array().unwrap().iter().map(key).collect(),
        deposit: number(&j["deposit"]),
        object_version: j["object_version"].as_u64().unwrap() as u8,
    }
}
fn as_contract(c: &Contract) -> J {
    json!({"key":hex::encode(c.public_key),"owner":hex::encode(c.owner.0),"vm":c.version.vm,"abi":c.version.abi,
        "code":match &c.code { Code::Inline(b)=>json!({"inline":hex::encode(b)}), Code::Reference(id)=>json!({"ref":hex::encode(id.to_bytes())}) },
        "log":hex::encode(&c.log),"active":c.active,"referrers":c.referrers.iter().map(hex::encode).collect::<Vec<_>>(),
        "deposit":decimal(&c.deposit),"object_version":c.object_version,"encoded":c.serialize().ok().map(hex::encode)})
}
fn call(j: &J) -> Call {
    Call {
        id: key(&j["id"]),
        caller: Id::from_bytes(&bytes(&j["caller"])).unwrap(),
        nonce: number(&j["nonce"]),
        height: number(&j["height"]),
        contract: key(&j["contract"]),
        call_target: j["target"].as_str().map(|s| hex::decode(s).unwrap()),
        gas_price: number(&j["gas_price"]),
        gas_used: number(&j["gas_used"]),
        return_value: bytes(&j["return_value"]),
        return_type: match j["return_type"].as_str().unwrap() {
            "ok" => ReturnType::Ok,
            "error" => ReturnType::Error,
            "revert" => ReturnType::Revert,
            _ => panic!(),
        },
        log: j["log"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| LogEntry {
                address: bytes(&l[0]),
                topics: l[1].as_array().unwrap().iter().map(bytes).collect(),
                data: bytes(&l[2]),
            })
            .collect(),
    }
}
fn mk_contract(k: u64, vm: u16, code: Code) -> Contract {
    let mut c = Contract::new(
        AccountPublicKey(small(1)),
        Nonce::Number(&1.into()),
        Version {
            vm,
            abi: if vm == 1 { 1 } else { 3 },
        },
        code,
        0.into(),
    )
    .unwrap();
    c.public_key = small(k);
    c
}
fn tree_entries<S: NodeReader>(tree: &Trie<S>) -> Contents {
    tree.iter()
        .map(|r| {
            let (k, v) = r.unwrap();
            let k = k
                .as_slice()
                .chunks_exact(2)
                .map(|p| p[0] << 4 | p[1])
                .collect();
            let Value::Bytes(v) = v else { panic!() };
            (k, v)
        })
        .collect()
}
fn root(j: &J) -> Option<Hash256> {
    j.as_str().map(|_| Hash256(key(j)))
}

#[test]
fn contract_records_and_decode_admission_match_erlang() {
    let f = fixture();
    for j in f["contracts"].as_array().unwrap() {
        let c = contract(j);
        assert_eq!(as_contract(&c), *j);
        let nonce = if matches!(c.code, Code::Reference(_)) {
            1.into()
        } else {
            c.deposit.clone()
        };
        let mut built = Contract::new(
            c.owner,
            Nonce::Number(&nonce),
            c.version,
            c.code.clone(),
            c.deposit.clone(),
        )
        .unwrap();
        built.active = c.active;
        built.log = c.log.clone();
        built.referrers = c.referrers.clone();
        assert_eq!(built, c);
        if j["encoded"].is_string() {
            assert_eq!(
                Contract::deserialize(c.public_key, &bytes(&j["encoded"])).unwrap(),
                c
            );
        }
    }
    for (i, j) in f["contract_decode"].as_array().unwrap().iter().enumerate() {
        let result = Contract::deserialize(small(9), &bytes(&j["bytes"]));
        let actual = result.as_ref().map(as_contract).unwrap_or(J::Null);
        assert_eq!(actual, j["result"], "contract decode {i}: {result:?}");
    }
}

#[test]
fn call_records_and_decode_admission_match_erlang() {
    let f = fixture();
    for j in f["calls"].as_array().unwrap() {
        let c = call(j);
        assert_eq!(c.serialize().unwrap(), bytes(&j["encoded"]));
        assert_eq!(Call::deserialize(c.id, &bytes(&j["encoded"])).unwrap(), c);
        let mut built = Call::new(
            c.caller,
            c.nonce.clone(),
            c.contract,
            c.call_target
                .as_ref()
                .map(|v| v.as_slice().try_into().unwrap()),
            c.height.clone(),
            c.gas_price.clone(),
        );
        built.gas_used = c.gas_used.clone();
        built.return_value = c.return_value.clone();
        built.return_type = c.return_type;
        built.log = c.log.clone();
        assert_eq!(built, c);
    }
    for (i, j) in f["call_decode"].as_array().unwrap().iter().enumerate() {
        let actual = Call::deserialize(small(9), &bytes(&j["bytes"]));
        if j["result"].is_null() {
            assert!(actual.is_err(), "call decode {i}: {actual:?}");
        } else {
            let expected = call(&j["result"]);
            assert_eq!(actual.unwrap(), expected, "call decode {i}");
            assert_eq!(
                expected.serialize().unwrap(),
                bytes(&j["result"]["encoded"])
            );
        }
    }
}

#[test]
fn contract_version_rules_identifiers_and_store_validation_match_erlang() {
    let f = fixture();
    for j in f["versions"].as_array().unwrap() {
        let v = version(j);
        assert_eq!(v.legal(), j["legal"]);
        assert_eq!(v.object_version(), j["object_version"]);
        assert_eq!(v.pack(), number(&j["packed"]));
        for p in j["protocols"].as_array().unwrap() {
            let protocol = Protocol::try_from(p["protocol"].as_u64().unwrap() as u32).unwrap();
            assert_eq!(
                v.can_create(protocol),
                p["create"],
                "create {v:?} {protocol:?}"
            );
            assert_eq!(v.can_call(protocol), p["call"], "call {v:?} {protocol:?}");
        }
    }
    for j in f["legal_calls"].as_array().unwrap() {
        let v = |k: &str| Version {
            vm: j[k][0].as_u64().unwrap() as u16,
            abi: j[k][1].as_u64().unwrap() as u16,
        };
        assert_eq!(v("to").can_call_from(v("from")), j["legal"], "{j}");
    }
    for j in f["packed"].as_array().unwrap() {
        let v = Version::unpack(&number(&j["packed"]));
        assert_eq!(v, version(j));
        assert_eq!(v.pack(), number(&j["canonical"]));
    }
    for j in f["hashes"].as_array().unwrap() {
        let n = number(&j["nonce"]);
        assert_eq!(
            contract_id(AccountPublicKey(small(1)), Nonce::Number(&n)).0,
            key(&j["contract"])
        );
        assert_eq!(call_id(small(1), &n, small(2)).0, key(&j["call"]));
    }
    for j in f["hash_bytes"].as_array().unwrap() {
        let n = key(&j["nonce"]);
        assert_eq!(
            contract_id(AccountPublicKey(small(1)), Nonce::Hash(n)).0,
            key(&j["contract"])
        );
        assert_eq!(ga_call_id(n, small(2)).0, key(&j["ga_call"]));
    }
    for j in f["store_validation"].as_array().unwrap() {
        let mut c = mk_contract(1, 7, Code::Inline(vec![]));
        c.version = version(j);
        assert_eq!(
            c.validate_writes(&pairs(&j["writes"])).is_ok(),
            j["valid"],
            "{j}"
        );
    }
}

#[test]
fn store_cache_prefixes_tombstones_and_bounds_match_erlang() {
    let f = fixture();
    for (i, trace) in f["store_traces"].as_array().unwrap().iter().enumerate() {
        let mut tree = Trie::new(MemoryStore::default());
        for (k, v) in pairs(&trace["base"]) {
            tree.put(&Nibbles::from_bytes(&k), Value::Bytes(v)).unwrap();
        }
        let mut store = Store::new(tree);
        for (s, step) in trace["steps"].as_array().unwrap().iter().enumerate() {
            let op = &step["op"];
            let k = bytes(&op[1]);
            let actual = match op[0].as_str().unwrap() {
                "get" => json!({"value":hex::encode(store.get_cached(&k).unwrap())}),
                "subtree" => json!({"map":as_pairs(&store.subtree_cached(&k).unwrap())}),
                "put" => {
                    store.put(k, bytes(&op[2]));
                    json!({})
                }
                _ => panic!(),
            };
            assert_eq!(actual, step["result"], "store {i} step {s}");
            assert_eq!(
                store.contents().unwrap(),
                pairs(&step["contents"]),
                "contents {i} {s}"
            );
            assert_eq!(store.size().unwrap() as u64, step["size"].as_u64().unwrap());
            assert_eq!(store.write_cache(), &pairs(&step["writes"]));
            for q in step["queries"].as_array().unwrap() {
                let prefix = bytes(&q["prefix"]);
                assert_eq!(
                    store.subtree(&prefix).unwrap(),
                    pairs(&q["map"]),
                    "subtree {i} {s} {prefix:?}"
                );
                for b in q["bounds"].as_array().unwrap() {
                    let maximum = b["limit"].as_u64().unwrap() as usize;
                    let actual = store.subtree_within_bytes(&prefix, maximum);
                    if b["result"].is_null() {
                        assert_eq!(
                            actual,
                            Err(Error::ResourceLimit),
                            "bounds {i} {s} {prefix:?} {maximum}"
                        );
                    } else {
                        assert_eq!(
                            actual.unwrap(),
                            (
                                pairs(&b["result"]["map"]),
                                b["result"]["bytes"].as_u64().unwrap() as usize
                            ),
                            "bounds {i} {s} {prefix:?} {maximum}"
                        );
                    }
                }
            }
        }
    }
}

fn contract_op<S: NodeWriter>(tree: &mut Contracts<S>, op: &J) -> Result<(), Error> {
    let k = op[1].as_u64().unwrap();
    match op[0].as_str().unwrap() {
        "insert" => tree.insert(
            &mk_contract(
                k,
                op[2].as_u64().unwrap() as u16,
                Code::Inline(bytes(&op[3])),
            ),
            &pairs(&op[4]),
        ),
        "enter" => tree.enter(
            &tree.record(small(k))?.ok_or(Error::NotPresent)?,
            &pairs(&op[2]),
        ),
        "enter_new" => tree.enter(&mk_contract(k, 7, Code::Inline(vec![1])), &Contents::new()),
        "clone" => tree.insert(
            &mk_contract(
                k,
                7,
                Code::Reference(Id::new(IdTag::Contract, small(op[2].as_u64().unwrap()))),
            ),
            &Contents::new(),
        ),
        "copy" => {
            let contents = tree.read_store(small(k))?.contents()?;
            tree.copy_contents(&contents, small(op[2].as_u64().unwrap()))
        }
        "delete" => tree.delete(small(k)),
        _ => panic!(),
    }
}
#[test]
fn contract_tree_roots_loading_copying_and_orphans_match_erlang() {
    let f = fixture();
    let mut tree = Contracts::default();
    for (i, row) in f["tree_trace"].as_array().unwrap().iter().enumerate() {
        let result = contract_op(&mut tree, &row["op"]);
        assert_eq!(result.is_ok(), row["accepted"], "operation {i}: {result:?}");
        let s = &row["state"];
        assert_eq!(tree.root_hash(), root(&s["root"]), "root {i}");
        assert_eq!(
            tree_entries(tree.tree()),
            pairs(&s["entries"]),
            "entries {i}"
        );
        for q in s["lookups"].as_array().unwrap() {
            let k = small(q["key"].as_u64().unwrap());
            let o = q["options"].as_u64().unwrap();
            let actual = match tree.lookup(
                k,
                LoadOptions {
                    no_store: o == 1,
                    full_store_cache: o == 2,
                },
            ) {
                Err(_) => J::Null,
                Ok(None) => json!(false),
                Ok(Some(c)) => {
                    json!({"contract":as_contract(&c.contract),"contents":as_pairs(&c.store.as_ref().map(|s|s.contents().unwrap()).unwrap_or_default()),"writes":as_pairs(&c.store.as_ref().map(|s|s.write_cache().clone()).unwrap_or_default())})
                }
            };
            assert_eq!(actual, q["result"], "lookup {i} {k:?} option {o}");
        }
        for q in s["code"].as_array().unwrap() {
            let actual = match tree.lookup_with_code(
                small(q["key"].as_u64().unwrap()),
                LoadOptions {
                    no_store: true,
                    full_store_cache: false,
                },
            ) {
                Err(_) => J::Null,
                Ok(None) => json!(false),
                Ok(Some((_, code))) => json!(hex::encode(code)),
            };
            assert_eq!(actual, q["result"], "code {i} {q}");
        }
    }
}

#[test]
fn call_tree_namespaces_auth_replacement_and_pruning_match_erlang() {
    let f = fixture();
    let mut tree = Calls::default();
    for (i, row) in f["call_tree_trace"].as_array().unwrap().iter().enumerate() {
        let result = match row["op"].as_str().unwrap() {
            "insert" => tree.insert(&call(&row["call"])),
            "auth" => tree.enter_auth(&call(&row["call"])),
            "clear" => {
                tree.prune();
                Ok(())
            }
            _ => panic!(),
        };
        assert_eq!(result.is_ok(), row["accepted"], "call tree {i}: {result:?}");
        assert_eq!(tree.root_hash(), root(&row["root"]), "call root {i}");
        assert_eq!(tree_entries(tree.tree()), pairs(&row["entries"]));
        for (k, v) in pairs(&row["entries"]) {
            let id = k[k.len() - 32..].try_into().unwrap();
            assert_eq!(
                tree.lookup(&k[..k.len() - 32], id)
                    .unwrap()
                    .unwrap()
                    .serialize()
                    .unwrap(),
                v
            );
        }
    }
}

#[test]
fn contract_proofs_match_erlang_and_require_complete_storage() {
    let rows: J =
        serde_json::from_str(include_str!("fixtures/erlang_contract_proofs.json")).unwrap();
    for (i, row) in rows.as_array().unwrap().iter().enumerate() {
        let mut raw = Trie::default();
        for (k, v) in pairs(&row["entries"]) {
            raw.put(&Nibbles::from_bytes(&k), Value::Bytes(v)).unwrap();
        }
        let tree = Contracts::from_root(raw.root_hash(), raw.into_store()).unwrap();
        let k = small(row["key"].as_u64().unwrap());
        let actual = tree.proof(k);
        if let Some(error) = row["error"].as_str() {
            assert_eq!(
                actual,
                Err(match error {
                    "not_present" => Error::NotPresent,
                    "wrong_root_hash" => Error::WrongRootHash,
                    _ => panic!(),
                }),
                "proof {i}"
            );
            continue;
        }
        let proof = actual.unwrap();
        assert_eq!(proof.root_hash(), root(&row["root"]).unwrap());
        let expected: Contents = pairs(&row["nodes"]);
        let actual: Contents = proof
            .nodes()
            .nodes()
            .iter()
            .map(|(k, v)| (k.0.to_vec(), v.clone()))
            .collect();
        assert_eq!(actual, expected, "proof nodes {i}");
        let full = contract(&row["contract"]);
        let contents = pairs(&row["contents"]);
        for check in row["checks"].as_array().unwrap() {
            let omitted = check["omitted"].as_str().unwrap();
            let mut nodes = MemoryStore::default();
            for (hash, bytes) in proof.nodes().nodes() {
                if omitted != "empty" && omitted != hex::encode(hash.0) {
                    nodes.put_node(*hash, bytes.clone()).unwrap();
                }
            }
            let variant = ContractProof::from_nodes(proof.root_hash(), nodes);
            let loaded = variant.lookup(k);
            if check["lookup"].is_object() {
                let (c, store) = loaded.unwrap().unwrap();
                assert_eq!(as_contract(&c), check["lookup"]["contract"]);
                assert_eq!(store, pairs(&check["lookup"]["contents"]));
            } else {
                assert!(
                    loaded.is_err() || matches!(loaded, Ok(None)),
                    "partial proof {i} {omitted}"
                );
            }
            assert_eq!(
                variant.verify(k, &full, &contents).is_ok(),
                check["verify"],
                "verify {i} {omitted}"
            );
            let mut changed = full.clone();
            changed.active = !changed.active;
            assert_eq!(
                variant.verify(k, &changed, &contents).is_ok(),
                check["changed_metadata"]
            );
            let mut changed = contents.clone();
            changed.insert(vec![255], vec![1]);
            assert_eq!(
                variant.verify(k, &full, &changed).is_ok(),
                check["changed_store"]
            );
            assert!(variant.verify(small(99), &full, &contents).is_err());
        }
        let mut wrong = ContractProof::new(Hash256(small(99)));
        let before = wrong.clone();
        assert_eq!(tree.add_to_proof(k, &mut wrong), Err(Error::WrongRootHash));
        assert_eq!(wrong, before);
        let mut accumulated = proof.clone();
        assert_eq!(
            tree.add_to_proof(small(99), &mut accumulated),
            Err(Error::NotPresent)
        );
        assert_eq!(accumulated, proof);
        tree.add_to_proof(k, &mut accumulated).unwrap();
        assert_eq!(accumulated, proof);
    }
}

#[test]
fn contract_and_call_root_updates_are_atomic_under_backend_failures() {
    use std::{cell::Cell, rc::Rc};
    struct FaultStore {
        inner: MemoryStore,
        remaining: Rc<Cell<usize>>,
    }
    impl NodeReader for FaultStore {
        fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            self.inner.node(h)
        }
    }
    impl NodeWriter for FaultStore {
        fn put_node(&mut self, h: Hash256, b: Vec<u8>) -> Result<(), ae_trie::Error> {
            let n = self.remaining.get();
            if n == 0 {
                return Err(ae_trie::Error::Backend("injected failure".into()));
            }
            self.remaining.set(n - 1);
            self.inner.put_node(h, b)
        }
    }
    let f = fixture();
    let mut base = Contracts::default();
    let mut failures = 0;
    for row in f["tree_trace"].as_array().unwrap() {
        let before = base.clone();
        let accepted = contract_op(&mut base, &row["op"]).is_ok();
        if !accepted {
            continue;
        }
        let mut successes = 0;
        for budget in 0..48 {
            let mut trial = Contracts::from_root(
                before.root_hash(),
                FaultStore {
                    inner: before.tree().store().clone(),
                    remaining: Rc::new(Cell::new(budget)),
                },
            )
            .unwrap();
            let result = contract_op(&mut trial, &row["op"]);
            let expected = if result.is_ok() {
                successes += 1;
                &base
            } else {
                assert!(matches!(
                    result,
                    Err(Error::Tree(ae_trie::Error::Backend(_)))
                ));
                failures += 1;
                &before
            };
            assert_eq!(
                trial.root_hash(),
                expected.root_hash(),
                "budget {budget} {}",
                row["op"]
            );
            assert_eq!(tree_entries(trial.tree()), tree_entries(expected.tree()));
        }
        assert!(successes > 0);
        let mut trial = before.clone();
        let result: Result<(), ()> = trial.transaction(|t| {
            contract_op(t, &row["op"]).unwrap();
            Err(())
        });
        assert!(result.is_err());
        assert_eq!(trial.root_hash(), before.root_hash());
        assert_eq!(tree_entries(trial.tree()), tree_entries(before.tree()));
    }
    assert!(failures > 0);
    let mut base = Calls::default();
    let mut call_failures = 0;
    fn call_op<S: NodeWriter>(t: &mut Calls<S>, row: &J) -> Result<(), Error> {
        match row["op"].as_str().unwrap() {
            "insert" => t.insert(&call(&row["call"])),
            "auth" => t.enter_auth(&call(&row["call"])),
            "clear" => {
                t.prune();
                Ok(())
            }
            _ => panic!(),
        }
    }
    for row in f["call_tree_trace"].as_array().unwrap() {
        let before = base.clone();
        if call_op(&mut base, row).is_err() {
            continue;
        }
        let mut successes = 0;
        for budget in 0..48 {
            let mut trial = Calls::from_root(
                before.root_hash(),
                FaultStore {
                    inner: before.tree().store().clone(),
                    remaining: Rc::new(Cell::new(budget)),
                },
            )
            .unwrap();
            let result = call_op(&mut trial, row);
            let expected = if result.is_ok() {
                successes += 1;
                &base
            } else {
                assert!(matches!(
                    result,
                    Err(Error::Tree(ae_trie::Error::Backend(_)))
                ));
                call_failures += 1;
                &before
            };
            assert_eq!(trial.root_hash(), expected.root_hash());
            assert_eq!(tree_entries(trial.tree()), tree_entries(expected.tree()));
        }
        assert!(successes > 0);
        let mut trial = before.clone();
        let result: Result<(), ()> = trial.transaction(|t| {
            call_op(t, row).unwrap();
            Err(())
        });
        assert!(result.is_err());
        assert_eq!(trial.root_hash(), before.root_hash());
        assert_eq!(tree_entries(trial.tree()), tree_entries(before.tree()));
    }
    assert!(call_failures > 0);
}
