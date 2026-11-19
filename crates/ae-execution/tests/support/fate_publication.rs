use super::*;
use ae_execution::fate_chain::{
    deployment::Pending,
    remote::{State as Chain, View},
};
use ae_state::{
    Trees,
    accounts::Account,
    calls::Calls,
    contracts::{Code as ContractCode, Contract},
    names::{Auction, Commitment, Name, Names},
    oracles::{Oracle, Oracles, Query},
};
use ae_trie::{NodeReader, NodeWriter};
use ae_types::{AccountPublicKey, Id, IdTag};
use ae_vm_fate::machine::program::remote::HostFailure;
use remote_tests::{key, small, tree, tree_entries};
use serde_json::json;

static PUBLICATION: LazyLock<J> = LazyLock::new(|| {
    let packed: J =
        serde_json::from_str(include_str!("../fixtures/erlang_fate_publication.json")).unwrap();
    assert_eq!(packed["format"], "interned-json-v1");
    let pool = rows(&packed["values"]);
    name_tests::expand(&packed["root"], pool, pool.len())
});
pub(super) fn chain(row: &J) -> Chain {
    let init = &row["initial"];
    let mut s = remote_tests::chain(
        &json!({"initial":{"chain":{"accounts":init["accounts"]}},"contracts":init["contracts"],"height":50}),
    );
    let n = tree(&init["names"]);
    let nc = tree(&init["name_expiry"]);
    s.names = Names::from_roots(
        n.root_hash(),
        nc.root_hash(),
        n.into_store(),
        nc.into_store(),
    )
    .unwrap();
    let o = tree(&init["oracles"]);
    let oc = tree(&init["oracle_expiry"]);
    s.oracles = Oracles::from_roots(
        o.root_hash(),
        oc.root_hash(),
        o.into_store(),
        oc.into_store(),
    )
    .unwrap();
    let initial = s.clone();
    for pending in rows(&row["pending"]) {
        let k: [u8; 32] = bytes(&pending["key"])[..32].try_into().unwrap();
        match string(&pending["kind"]) {
            "account" => s
                .accounts
                .enter(
                    &Account::deserialize(AccountPublicKey(k), &bytes(&pending["encoded"]))
                        .unwrap(),
                )
                .unwrap(),
            "contract" => {
                let contract = if pending["encoded"].is_string() {
                    Contract::deserialize(k, &bytes(&pending["encoded"])).unwrap()
                } else {
                    let version = ae_protocol::contracts::Version {
                        vm: pending["vm"].as_u64().unwrap() as u16,
                        abi: 3,
                    };
                    Contract {
                        public_key: k,
                        owner: AccountPublicKey(key(&pending["owner"])),
                        version,
                        code: ContractCode::Reference(Id::new(
                            IdTag::Contract,
                            key(&pending["code"][1]),
                        )),
                        log: vec![],
                        active: true,
                        referrers: vec![],
                        deposit: 0.into(),
                        object_version: version.object_version(),
                    }
                };
                s.contract_cache.pending.insert(
                    k,
                    Pending {
                        contract,
                        writes: pairs(&pending["writes"]),
                    },
                );
            }
            "name" => {
                s.name_cache.names.insert(
                    k,
                    Name::deserialize(k, &bytes(&pending["encoded"])).unwrap(),
                );
            }
            "commitment" => {
                s.name_cache.commitments.insert(
                    k,
                    Commitment::deserialize(k, &bytes(&pending["encoded"])).unwrap(),
                );
            }
            "auction" => {
                let k: [u8; 33] = bytes(&pending["key"]).try_into().unwrap();
                s.name_cache.auctions.insert(
                    k,
                    Auction::deserialize(k, &bytes(&pending["encoded"])).unwrap(),
                );
            }
            "oracle" => {
                s.oracle_reads.oracles.insert(
                    k,
                    Oracle::deserialize(k, &bytes(&pending["encoded"])).unwrap(),
                );
            }
            "query" => {
                s.oracle_reads.queries.insert(
                    (key(&pending["oracle"]), k),
                    Query::deserialize(k, &bytes(&pending["encoded"])).unwrap(),
                );
            }
            other => panic!("unknown cache kind {other}"),
        }
    }
    if row["offchain"] == true {
        let view = View {
            accounts: s.accounts,
            contracts: s.contracts,
            names: s.names,
            name_cache: s.name_cache,
            oracles: s.oracles,
            oracle_reads: s.oracle_reads,
        };
        s = initial;
        s.onchain = Some(Box::new(view));
    }
    s
}
pub(super) fn other_trees<S: NodeWriter + Clone>(
    row: &J,
    store: S,
) -> (Calls<S>, ae_state::channels::Channels<S>) {
    fn build<S: NodeWriter>(rows_json: &J, store: S) -> Trie<S> {
        let mut out = Trie::new(store);
        for item in rows(rows_json) {
            out.put(
                &Nibbles::from_bytes(&bytes(&item["key"])),
                TrieValue::Bytes(bytes(&item["encoded"])),
            )
            .unwrap();
        }
        out
    }
    let calls = build(&row["initial"]["calls"], store.clone());
    (
        Calls::from_root(calls.root_hash(), calls.into_store()).unwrap(),
        build(&row["initial"]["channels"], store).into(),
    )
}
pub(super) fn snapshot<A: NodeReader, C: NodeReader>(t: &Trees<A, C>) -> J {
    json!({"accounts":tree_entries(t.accounts.tree()),"contracts":tree_entries(t.contracts.tree()),"names":tree_entries(t.names.tree()),"name_expiry":tree_entries(t.names.cache()),"oracles":tree_entries(t.oracles.tree()),"oracle_expiry":tree_entries(t.oracles.cache()),"calls":tree_entries(t.calls.tree()),"channels":tree_entries(t.channels.tree()),
        "account_root":t.accounts.root_hash().map(|h|hex::encode(h.0)),"contract_root":t.contracts.root_hash().map(|h|hex::encode(h.0)),"name_root":t.names.root_hash().map(|h|hex::encode(h.0)),"name_expiry_root":t.names.cache_root_hash().map(|h|hex::encode(h.0)),"oracle_root":t.oracles.root_hash().map(|h|hex::encode(h.0)),"oracle_expiry_root":t.oracles.cache_root_hash().map(|h|hex::encode(h.0)),"call_root":t.calls.root_hash().map(|h|hex::encode(h.0)),"channel_root":t.channels.root_hash().map(|h|hex::encode(h.0)),"state_root":hex::encode(t.roots().hash().0)})
}
fn result<A: NodeReader, C: NodeReader>(
    r: Result<(Trees<A, C>, Limits), HostFailure<ae_execution::fate_chain::Error>>,
) -> J {
    match r {
        Ok((t, _)) => json!({"kind":"ok","trees":snapshot(&t)}),
        Err(HostFailure::Exception(Exception::Native(reason))) => {
            json!({"kind":"exception","class":"error","reason":reason})
        }
        other => panic!(
            "unexpected publication failure: {}",
            match other {
                Err(e) => format!("{e:?}"),
                _ => unreachable!(),
            }
        ),
    }
}
#[test]
fn full_invocation_tree_projection_matches_original_cache_write_through() {
    assert_eq!(rows(&PUBLICATION["cases"]).len(), 56);
    for row in rows(&PUBLICATION["cases"]) {
        let s = chain(row);
        let (calls, channels) = other_trees(row, MemoryStore::default());
        let before = format!("{s:?}{calls:?}{channels:?}");
        let after_contracts = s.contract_cache.budget(Limits::default()).unwrap();
        let contracts_only = Limits {
            max_nodes: Limits::default().max_nodes - after_contracts.max_nodes,
            ..Default::default()
        };
        assert!(s.contract_cache.budget(contracts_only).is_ok());
        if row["offchain"] != true {
            assert!(matches!(
                s.final_trees(&calls, &channels, contracts_only),
                Err(HostFailure::ResourceLimit)
            ));
        }
        for _ in 0..2 {
            assert_eq!(
                result(s.final_trees(&calls, &channels, Limits::default())),
                row["result"],
                "{} protocol {} VM {}",
                row["name"],
                row["protocol"],
                row["vm"]
            );
            assert_eq!(format!("{s:?}{calls:?}{channels:?}"), before);
        }
        if row["name"] == "dirty_records" {
            let mut s = s.clone();
            s.name_base = Some(s.names.clone());
            s.oracle_base = Some(s.oracles.clone());
            let mut n = s.name_cache.names[&small(40)].clone();
            n.expires_by = 999.into();
            s.names.enter_name(&n).unwrap();
            let mut o = s.oracle_reads.oracles[&small(1)].clone();
            o.expires_by = 999.into();
            s.oracles.enter_oracle(&o).unwrap();
            assert_eq!(
                result(s.final_trees(&calls, &channels, Limits::default())),
                row["result"],
                "intermediate TTLs must not leak"
            );
        }
    }
}
#[test]
fn publication_faults_and_combined_cache_limits_preserve_inputs_and_retry() {
    use oracle_write_tests::{Faults, fault_host};
    for name in [
        "dirty_records",
        "missing_expiry_cached_reads",
        "new_objects",
    ] {
        let row = rows(&PUBLICATION["cases"])
            .iter()
            .find(|r| r["name"] == name && r["protocol"] == 6 && r["vm"] == 8)
            .unwrap();
        let faults = Faults::default();
        let s = fault_host(chain(row), &faults);
        let (calls, channels) = other_trees(row, s.contracts.tree().store().clone());
        let before = format!("{s:?}{calls:?}{channels:?}");
        for limits in [
            Limits {
                max_nodes: 0,
                ..Default::default()
            },
            Limits {
                max_bytes: 0,
                ..Default::default()
            },
            Limits {
                max_nodes: Limits::default().max_nodes
                    - s.contract_cache
                        .budget(Limits::default())
                        .unwrap()
                        .max_nodes,
                ..Default::default()
            },
        ] {
            assert!(matches!(
                s.final_trees(&calls, &channels, limits),
                Err(HostFailure::ResourceLimit)
            ));
            assert_eq!(format!("{s:?}{calls:?}{channels:?}"), before);
        }
        for reads in [false, true] {
            let mut failures = 0;
            for allowance in 0..1024 {
                let counter = if reads {
                    &faults.reads_left
                } else {
                    &faults.writes_left
                };
                counter.set(Some(allowance));
                let out = s.final_trees(&calls, &channels, Limits::default());
                counter.set(None);
                assert_eq!(format!("{s:?}{calls:?}{channels:?}"), before);
                match out {
                    Err(HostFailure::Backend(_)) => failures += 1,
                    Ok(out) => {
                        assert_eq!(result(Ok(out)), row["result"]);
                        break;
                    }
                    other => panic!("unexpected {other:?}"),
                }
                assert_eq!(
                    result(s.final_trees(&calls, &channels, Limits::default())),
                    row["result"]
                );
                assert!(allowance < 1023, "never reached successful publication");
            }
            assert!(failures > 0, "did not inject any failures");
        }
    }
}

#[test]
fn store_sources_match_original_lookup_and_remain_independent_snapshots() {
    use oracle_write_tests::{Faults, fault_host};
    let mut compared = 0;
    for row in rows(&PUBLICATION["cases"]) {
        let s = chain(row);
        let before = format!("{s:?}");
        for sample in rows(&row["sources"]) {
            let actual = match s.contract_store_source(key(&sample["key"]), Limits::default()) {
                Ok((store, _)) => {
                    let pairs = |m: &BTreeMap<Vec<u8>, Vec<u8>>| {
                        m.iter()
                            .map(|(k, v)| json!([hex::encode(k), hex::encode(v)]))
                            .collect::<Vec<_>>()
                    };
                    json!({"kind":"ok","store":{"contents":pairs(&store.0.0.contents().unwrap()),"writes":pairs(store.0.0.write_cache())}})
                }
                Err(HostFailure::Exception(Exception::Native(reason))) => {
                    json!({"kind":"exception","class":"error","reason":reason})
                }
                other => panic!("unexpected store result {other:?}"),
            };
            assert_eq!(
                actual, sample["result"],
                "{} protocol {} VM {} store {}",
                row["name"], row["protocol"], row["vm"], sample["key"]
            );
            assert_eq!(format!("{s:?}"), before);
            compared += 1;
        }
    }
    assert_eq!(compared, 280);
    let row = rows(&PUBLICATION["cases"])
        .iter()
        .find(|r| r["name"] == "dirty_records" && r["protocol"] == 6 && r["vm"] == 8)
        .unwrap();
    let faults = Faults::default();
    let mut s = fault_host(chain(row), &faults);
    let before = format!("{s:?}");
    for limits in [
        Limits {
            max_nodes: 0,
            ..Default::default()
        },
        Limits {
            max_bytes: 0,
            ..Default::default()
        },
    ] {
        assert!(matches!(
            s.contract_store_source(small(1), limits),
            Err(HostFailure::ResourceLimit)
        ));
    }
    faults.fail_reads.set(true);
    assert!(matches!(
        s.contract_store_source(small(2), Limits::default()),
        Err(HostFailure::Backend(_))
    ));
    faults.fail_reads.set(false);
    assert_eq!(format!("{s:?}"), before);
    let (store, _) = s
        .contract_store_source(small(1), Limits::default())
        .unwrap();
    let contents = store.0.0.contents().unwrap();
    s.contract_cache
        .pending
        .get_mut(&small(1))
        .unwrap()
        .writes
        .clear();
    s.contracts.delete(small(1)).unwrap();
    drop(s);
    assert_eq!(store.0.0.contents().unwrap(), contents);
    faults.fail_reads.set(true);
    assert!(store.0.0.contents().is_err());
    faults.fail_reads.set(false);
    assert_eq!(store.0.0.contents().unwrap(), contents);
}

#[test]
fn loaded_store_sources_execute_and_publish_original_remote_and_creation_results() {
    use ae_vm_fate::machine::program::{Entry, remote};
    use ae_vm_fate::value::{ObjectKind, ObjectPayload};
    let fixture = &completion_tests::COMPLETION;
    let order = otp_order::profile(&fixture["profile"]);
    let overrides = Overrides::default();
    let mut compared = 0;
    let mut fetched = 0;
    for row in rows(&fixture["cases"]).iter().filter(|r| {
        [
            "store_nested_map",
            "remote_store",
            "remote_revert",
            "CREATE_store",
            "CLONE_store",
            "CLONE_G_store",
        ]
        .contains(&string(&r["name"]))
    }) {
        let sample = rows(&row["runs"])
            .iter()
            .find(|r| r["gas"] == "1000000000")
            .unwrap();
        if sample["result"]["kind"] != "returned" {
            continue;
        }
        let mut context = context(row);
        context.current_contract = small(1);
        context.reserved_gas = 18.into();
        let host = remote_tests::chain(row);
        let (root, _) = host
            .contract_store_source(small(1), Limits::default())
            .unwrap();
        let mut stores = Stores::new(Version::for_protocol(
            row["protocol"].as_u64().unwrap() as u32
        ));
        stores.put_contract(small(1), root);
        let machine = State {
            accumulator: Accumulator {
                top: Value::Void,
                rest: Default::default(),
            },
            memory: Default::default(),
            stores,
            meter: Meter {
                protocol: row["protocol"].as_u64().unwrap() as u32,
                gas: number(&sample["gas"]),
                created_cells: number(&row["initial"]["cells"]),
                metering_floor: None,
            },
        };
        let mut sources = BTreeMap::new();
        let initial = (Executor {
            context: &context,
            order: &order,
            overrides: &overrides,
            contracts: &sources,
        })
        .start_remote_program(
            remote::LoadedCode {
                code: std::sync::Arc::new(program_tests::code(&row["codes"][0])),
                vm_version: context.vm_version,
            },
            Value::Object(ObjectKind::Address, ObjectPayload::Bytes(small(9).to_vec())),
            host,
            &machine,
            &Entry {
                function: Value::String(vec![0, 0, 0, 1]),
                arguments: vec![],
                call_value: 0.into(),
                allow_init: false,
            },
            Limits::default(),
        )
        .unwrap();
        let remote::Outcome::Continue {
            mut state,
            remaining_limits: mut limits,
        } = initial
        else {
            panic!("failed entry")
        };
        let mut finished = false;
        for _ in 0..300 {
            match (Executor {
                context: &context,
                order: &order,
                overrides: &overrides,
                contracts: &sources,
            })
            .advance_remote_program(&state, limits)
            {
                Err(Error::NeedContract(key)) => {
                    assert!(!sources.contains_key(&key));
                    let before = format!("{state:?}");
                    let (store, remaining) = state
                        .session
                        .host
                        .contract_store_source(key, limits)
                        .unwrap();
                    assert_eq!(format!("{state:?}"), before);
                    sources.insert(key, store);
                    limits = remaining;
                    fetched += 1;
                }
                Ok(remote::Outcome::Continue {
                    state: next,
                    remaining_limits,
                }) => {
                    state = next;
                    limits = remaining_limits;
                    sources.clear();
                }
                Ok(remote::Outcome::Returned {
                    state,
                    remaining_limits,
                }) => {
                    let completed = (Executor {
                        context: &context,
                        order: &order,
                        overrides: &overrides,
                        contracts: &sources,
                    })
                    .finalize_remote_program(&state, remaining_limits)
                    .unwrap();
                    let remote::Outcome::Returned { state, .. } = completed else {
                        panic!("failed completion")
                    };
                    remote_tests::assert_remote(&state, &sample["result"]["state"], false);
                    finished = true;
                    compared += 1;
                    break;
                }
                other => panic!(
                    "{} protocol {} VM {}: {other:?}",
                    row["name"], row["protocol"], row["vm"]
                ),
            }
        }
        assert!(finished, "did not finish the program");
    }
    assert_eq!(compared, 33);
    assert_eq!(fetched, 25);
}
