use ae_state::{
    Trees,
    accounts::Accounts,
    calls::Calls,
    contracts::Contracts,
    names::Names,
    oracles::Oracles,
    transport::{Config, Error, Limits, Proofs},
};
use ae_trie::{MemoryStore, Nibbles, NodeReader, Trie, Value};
use ae_types::{AccountPublicKey, Hash256};
use serde_json::{Value as J, json};
use std::sync::LazyLock;
static DATA: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_state_transport.json")).unwrap()
});
fn rows(j: &J) -> &[J] {
    j.as_array().unwrap()
}
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn key(j: &J) -> [u8; 32] {
    bytes(j).try_into().unwrap()
}
fn small(n: u64) -> [u8; 32] {
    let mut x = [0; 32];
    x[24..].copy_from_slice(&n.to_be_bytes());
    x
}
fn tree(j: &J) -> Trie {
    let mut t = Trie::default();
    for r in rows(j) {
        t.put(
            &Nibbles::from_bytes(&bytes(&r[0])),
            Value::Bytes(bytes(&r[1])),
        )
        .unwrap();
    }
    t
}
fn initial(j: &J) -> Trees {
    let a = tree(&j["accounts"]);
    let c = tree(&j["contracts"]);
    let calls = tree(&j["calls"]);
    let n = tree(&j["names"]);
    let nc = tree(&j["name_cache"]);
    let o = tree(&j["oracles"]);
    let oc = tree(&j["oracle_cache"]);
    Trees {
        accounts: Accounts::from_root(a.root_hash(), a.into_store()).unwrap(),
        contracts: Contracts::from_root(c.root_hash(), c.into_store()).unwrap(),
        calls: Calls::from_root(calls.root_hash(), calls.into_store()).unwrap(),
        channels: tree(&j["channels"]).into(),
        names: Names::from_roots(
            n.root_hash(),
            nc.root_hash(),
            n.into_store(),
            nc.into_store(),
        )
        .unwrap(),
        oracles: Oracles::from_roots(
            o.root_hash(),
            oc.root_hash(),
            o.into_store(),
            oc.into_store(),
        )
        .unwrap(),
    }
}
fn entries<S: NodeReader>(t: &Trie<S>) -> J {
    json!(
        t.iter()
            .map(|r| {
                let (k, v) = r.unwrap();
                let k = k
                    .as_slice()
                    .chunks_exact(2)
                    .map(|p| (p[0] << 4) | p[1])
                    .collect::<Vec<_>>();
                let Value::Bytes(v) = v else { panic!() };
                [hex::encode(k), hex::encode(v)]
            })
            .collect::<Vec<_>>()
    )
}
fn snapshot<A: NodeReader, S: NodeReader>(t: &Trees<A, S>) -> J {
    json!({"client":t.serialize_to_client(Limits::default()).unwrap(),"encoded":hex::encode(t.serialize_to_binary(Limits::default()).unwrap()),"hash":hex::encode(t.roots().hash().0),"accounts":entries(t.accounts.tree()),"contracts":entries(t.contracts.tree()),"calls":entries(t.calls.tree()),"channels":entries(t.channels.tree()),"names":entries(t.names.tree()),"name_cache":entries(t.names.cache()),"oracles":entries(t.oracles.tree()),"oracle_cache":entries(t.oracles.cache()),"name_cache_root":t.names.cache_root_hash().map(|h|hex::encode(h.0)),"oracle_cache_root":t.oracles.cache_root_hash().map(|h|hex::encode(h.0))})
}
fn proof_row(p: &Proofs) -> J {
    let lookups=[1,2,5,17,99].map(|n|json!({"ok":match p.lookup_account(AccountPublicKey(small(n)),Limits::default()).unwrap(){Some(a)=>json!({"encoded":hex::encode(a.serialize().unwrap())}),None=>json!({"error":"not_found"})}}));
    json!({"encoded":hex::encode(p.serialize(Limits::default()).unwrap()),"hash":hex::encode(p.hash().0),"lookup":lookups})
}

#[test]
fn full_state_transport_preserves_bytes_roots_and_original_expiry_cache_rebuilds() {
    for r in rows(&DATA["states"]) {
        let t = initial(&r["initial"]);
        assert_eq!(snapshot(&t), r["initial"]);
        let got = Trees::deserialize_from_binary(
            &bytes(&r["encoded"]),
            MemoryStore::default(),
            MemoryStore::default(),
            Config::default(),
        );
        if r["result"]["ok"].is_object() {
            assert_eq!(snapshot(&got.unwrap()), r["result"]["ok"]);
        } else {
            assert_eq!(got.unwrap_err(), Error::InvalidValue);
        }
    }
}
#[test]
fn aggregate_proof_roots_accumulation_and_deferred_lookup_match_original() {
    for (r, state) in rows(&DATA["proofs"]).iter().zip(rows(&DATA["states"])) {
        let t = initial(&state["initial"]);
        let mut p = Proofs::from_trees(&t);
        assert_eq!(proof_row(&p), r["initial"]);
        for step in rows(&r["steps"]) {
            let before = p.clone();
            let result = p.add_account(
                AccountPublicKey(small(step["key"].as_u64().unwrap())),
                &t.accounts,
                Limits::default(),
            );
            match step["result"].as_str().unwrap() {
                "ok" => result.unwrap(),
                "not_present" => {
                    assert_eq!(result, Err(Error::NotPresent));
                    assert_eq!(p, before);
                }
                x => panic!("{x}"),
            };
            assert_eq!(proof_row(&p), step["proof"]);
            let q =
                Proofs::deserialize(&p.serialize(Limits::default()).unwrap(), Limits::default())
                    .unwrap();
            assert_eq!(q, p);
        }
    }
    for r in rows(&DATA["proof_decode"]) {
        let p = Proofs::deserialize(&bytes(&r["bytes"]), Limits::default());
        if r["result"]["ok"].is_object() {
            assert_eq!(proof_row(&p.unwrap()), r["result"]["ok"], "{}", r["name"]);
        } else {
            assert!(p.is_err(), "{}", r["name"]);
        }
    }
}
#[test]
fn aggregate_contract_proofs_include_full_store_and_account_paths() {
    let t = initial(&DATA["states"][5]["initial"]);
    let mut p = Proofs::from_trees(&t);
    for (i, r) in rows(&DATA["contract_proofs"]).iter().enumerate() {
        let k = key(&r["contract"]);
        match i {
            0 => {}
            1 => p.add_contract(k, &t.contracts, Limits::default()).unwrap(),
            2 => p
                .add_account(AccountPublicKey(small(1)), &t.accounts, Limits::default())
                .unwrap(),
            _ => panic!(),
        };
        assert_eq!(proof_row(&p), r["proof"]);
        let got = p.lookup_contract(k, Limits::default()).unwrap();
        let j = match got {
            None => json!({"error":"not_found"}),
            Some((c, s)) => {
                p.verify_contract(k, &c, &s, Limits::default()).unwrap();
                json!({"encoded":hex::encode(c.serialize().unwrap()),"store":s.iter().map(|(k,v)|[hex::encode(k),hex::encode(v)]).collect::<Vec<_>>()})
            }
        };
        assert_eq!(json!({"ok":j}), r["lookup"]);
    }
}
#[test]
fn transport_work_limits_are_retryable_and_do_not_modify_existing_proofs() {
    let t = initial(&DATA["states"][5]["initial"]);
    let raw = bytes(&DATA["states"][5]["encoded"]);
    for limits in [
        Limits {
            max_input_bytes: 1,
            ..Limits::default()
        },
        Limits {
            max_entries: 1,
            ..Limits::default()
        },
        Limits {
            max_work_bytes: 1,
            ..Limits::default()
        },
        Limits {
            max_key_bytes: 1,
            ..Limits::default()
        },
        Limits {
            max_items: 1,
            ..Limits::default()
        },
        Limits {
            max_depth: 1,
            ..Limits::default()
        },
    ] {
        assert_eq!(
            Trees::deserialize_from_binary(
                &raw,
                MemoryStore::default(),
                MemoryStore::default(),
                Config {
                    limits,
                    ..Config::default()
                }
            )
            .unwrap_err(),
            Error::ResourceLimit
        );
    }
    let mut p = Proofs::from_trees(&t);
    let before = p.clone();
    assert_eq!(
        p.add_account(
            AccountPublicKey(small(1)),
            &t.accounts,
            Limits {
                max_work_bytes: 1,
                ..Limits::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(p, before);
    p.add_account(AccountPublicKey(small(1)), &t.accounts, Limits::default())
        .unwrap();
    let b = p.serialize(Limits::default()).unwrap();
    assert_eq!(
        Proofs::deserialize(
            &b,
            Limits {
                max_items: 1,
                ..Limits::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        p.lookup_account(
            AccountPublicKey(small(1)),
            Limits {
                max_work_bytes: 1,
                ..Limits::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert!(
        p.lookup_account(AccountPublicKey(small(1)), Limits::default())
            .unwrap()
            .is_some()
    );
    let mut wrong = p.clone();
    if let Some(a) = wrong.accounts.as_mut() {
        *a = ae_state::transport::Proof::new(Hash256::ZERO);
    }
    let previous = wrong.clone();
    assert_eq!(
        wrong.add_account(AccountPublicKey(small(1)), &t.accounts, Limits::default()),
        Err(Error::WrongRootHash)
    );
    assert_eq!(wrong, previous);
}

#[test]
fn namespace_imports_and_name_atom_profiles_match_original() {
    let atoms = rows(&DATA["imports"])
        .iter()
        .filter(|r| r["existing_atom"] == true)
        .map(|r| bytes(&r["status"]))
        .collect();
    for r in rows(&DATA["imports"]) {
        let raw = bytes(&r["bytes"]);
        let got = Trees::deserialize_from_binary(
            &raw,
            MemoryStore::default(),
            MemoryStore::default(),
            Config {
                existing_name_status_atoms: Some(&atoms),
                ..Config::default()
            },
        );
        if r["result"]["ok"].is_object() {
            assert_eq!(
                snapshot(
                    &got.unwrap_or_else(|e| panic!("{} {}: {e:?}", r["namespace"], r["name"]))
                ),
                r["result"]["ok"]
            );
        } else {
            assert!(got.is_err(), "{} {}", r["namespace"], r["name"]);
        }
        if r["name"] == "status"
            && !matches!(bytes(&r["status"]).as_slice(), b"claimed" | b"revoked")
        {
            let got = Trees::deserialize_from_binary(
                &raw,
                MemoryStore::default(),
                MemoryStore::default(),
                Config::default(),
            );
            assert_eq!(
                got.unwrap_err(),
                if std::str::from_utf8(&bytes(&r["status"]))
                    .map_or(true, |s| s.chars().count() > 255)
                {
                    Error::InvalidExistingAtom(bytes(&r["status"]))
                } else {
                    Error::MissingAtomProfile(bytes(&r["status"]))
                }
            );
        }
    }
}
fn verify_result(result: Result<(), Error>) -> J {
    let e = match result {
        Ok(()) => return json!({"ok":{"atom":"ok"}}),
        Err(Error::NativeFunctionClause) => {
            return json!({"native":"error","reason":{"atom":"function_clause"}});
        }
        Err(Error::NativeBadarg) => return json!({"native":"error","reason":{"atom":"badarg"}}),
        Err(Error::BadProof) => json!({"atom":"bad_proof"}),
        Err(Error::EmptyAccountsProof) => json!({"atom":"empty_accounts_poi"}),
        Err(Error::EmptyContractsProof) => json!({"atom":"empty_contracts_poi"}),
        Err(Error::BadHash(h)) => {
            json!({"tuple":[{"atom":"bad_hash"},{"binary":hex::encode(h.0)}]})
        }
        Err(Error::BadValue(Value::Bytes(v))) => {
            json!({"tuple":[{"atom":"bad_value"},{"binary":hex::encode(v)}]})
        }
        Err(e) => panic!("unexpected proof result {e:?}"),
    };
    json!({"ok":{"tuple":[{"atom":"error"},e]}})
}
#[test]
fn proof_verification_preserves_absence_hash_and_contract_store_failures() {
    for r in rows(&DATA["proof_decode"]) {
        let Ok(p) = Proofs::deserialize(&bytes(&r["bytes"]), Limits::default()) else {
            continue;
        };
        for ((k, amount), expected) in [(1, 100u64), (1, 101), (99, 100)]
            .into_iter()
            .zip(rows(&r["verify"]["ok"]))
        {
            let k = AccountPublicKey(small(k));
            let a = ae_state::accounts::Account::new(k, amount.into());
            assert_eq!(
                verify_result(p.verify_account(k, &a, Limits::default())),
                json!({"ok":expected}),
                "{}",
                r["name"]
            );
        }
    }
    let t = initial(&DATA["states"][5]["initial"]);
    for (i, r) in rows(&DATA["contract_mutations"]).iter().enumerate() {
        let p = Proofs::deserialize(&bytes(&r["bytes"]), Limits::default()).unwrap();
        let k = key(&r["contract"]);
        let c = t.contracts.record(k).unwrap().unwrap();
        let contents = t.contracts.read_store(k).unwrap().contents().unwrap();
        for (store, expected) in [
            contents,
            Default::default(),
            [(vec![], vec![1])].into_iter().collect(),
            [
                (vec![0], vec![9]),
                (vec![1, 2], vec![3]),
                (vec![1, 3], vec![4]),
            ]
            .into_iter()
            .collect(),
            [
                (vec![0], vec![1]),
                (vec![1, 2], vec![9]),
                (vec![1, 3], vec![9]),
            ]
            .into_iter()
            .collect(),
        ]
        .into_iter()
        .zip(rows(&r["verify"]))
        {
            assert_eq!(
                verify_result(p.verify_contract(k, &c, &store, Limits::default())),
                *expected,
                "contract mutation {i} {}",
                r["name"]
            );
        }
        let got = match p.lookup_contract(k, Limits::default()) {
            Ok(None) => json!({"ok":{"error":"not_found"}}),
            Ok(Some((c, s))) => {
                json!({"ok":{"encoded":hex::encode(c.serialize().unwrap()),"store":s.iter().map(|(k,v)|[hex::encode(k),hex::encode(v)]).collect::<Vec<_>>()}})
            }
            Err(Error::NativeFunctionClause) => {
                json!({"native":"error","reason":{"atom":"function_clause"}})
            }
            Err(e) => panic!("lookup {i}: {e:?}"),
        };
        assert_eq!(got, r["lookup"], "contract lookup {i}");
    }
}

#[test]
fn proof_node_shapes_and_empty_child_verification_match_original() {
    for (i, r) in rows(&DATA["raw_proofs"]).iter().enumerate() {
        let root = Hash256(key(&r["root"]));
        let p = ae_state::transport::Proof::from_parts(
            root,
            [(
                root.0.to_vec(),
                rows(&r["node"]).iter().map(bytes).collect(),
            )],
        );
        assert_eq!(
            verify_result(p.verify(
                &bytes(&r["key"]),
                &Value::Bytes(bytes(&r["expected"])),
                Limits::default()
            )),
            r["verify"],
            "raw proof {i}"
        );
        let result = match p.lookup(&bytes(&r["key"]), Limits::default()) {
            Ok(None) => json!({"tuple":[{"atom":"error"},{"atom":"not_found"}]}),
            Ok(Some(Value::Bytes(b))) => json!({"tuple":[{"atom":"ok"},{"binary":hex::encode(b)}]}),
            Err(Error::NativeBadarg) => {
                assert_eq!(
                    r["lookup"],
                    json!({"native":"error","reason":{"atom":"badarg"}})
                );
                continue;
            }
            _ => panic!(),
        };
        assert_eq!(json!({"ok":result}), r["lookup"], "raw lookup {i}");
    }
}

#[test]
fn transport_backend_failure_prefixes_preserve_roots_and_proofs_for_retry() {
    use ae_trie::NodeWriter;
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };
    #[derive(Clone, Debug, Default)]
    struct Store {
        inner: Rc<RefCell<MemoryStore>>,
        count: Rc<Cell<usize>>,
        fail: Rc<Cell<Option<usize>>>,
    }
    impl Store {
        fn tick(&self) -> Result<(), ae_trie::Error> {
            let n = self.count.get();
            self.count.set(n + 1);
            if self.fail.get() == Some(n) {
                Err(ae_trie::Error::Backend("transport fault".into()))
            } else {
                Ok(())
            }
        }
    }
    impl NodeReader for Store {
        fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            self.tick()?;
            self.inner.borrow().node(h)
        }
    }
    impl NodeWriter for Store {
        fn put_node(&mut self, h: Hash256, b: Vec<u8>) -> Result<(), ae_trie::Error> {
            self.tick()?;
            self.inner.borrow_mut().put_node(h, b)
        }
    }
    let raw = bytes(&DATA["states"][5]["encoded"]);
    let k = key(&DATA["contract_proofs"][0]["contract"]);
    let build = || {
        let s = Store::default();
        let t =
            Trees::deserialize_from_binary(&raw, s.clone(), s.clone(), Config::default()).unwrap();
        s.count.set(0);
        (t, s)
    };
    for op in 0..4 {
        let run = |t: &Trees<Store, Store>, s: &Store, p: &mut Proofs| -> Result<(), Error> {
            match op {
                0 => Trees::deserialize_from_binary(&raw, s.clone(), s.clone(), Config::default())
                    .map(|_| ()),
                1 => t.serialize_to_binary(Limits::default()).map(|_| ()),
                2 => p.add_account(AccountPublicKey(small(1)), &t.accounts, Limits::default()),
                _ => p.add_contract(k, &t.contracts, Limits::default()),
            }
        };
        let (t, s) = build();
        let mut success = Proofs::from_trees(&t);
        run(&t, &s, &mut success).unwrap();
        let operations = s.count.get();
        assert!(operations > 0);
        for n in 0..operations {
            let (t, s) = build();
            let before = snapshot(&t);
            let mut p = Proofs::from_trees(&t);
            let prior = p.clone();
            s.count.set(0);
            s.fail.set(Some(n));
            let err = run(&t, &s, &mut p).unwrap_err();
            assert!(
                format!("{err:?}").contains("transport fault"),
                "{op}:{n}: {err:?}"
            );
            assert_eq!(p, prior);
            s.fail.set(None);
            assert_eq!(snapshot(&t), before);
            run(&t, &s, &mut p).unwrap();
            assert_eq!(p, success);
            assert_eq!(snapshot(&t), before);
        }
        eprintln!("transport operation {op} failure prefixes: {operations}");
    }
}
