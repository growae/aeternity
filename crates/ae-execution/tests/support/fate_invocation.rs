use super::*;
use ae_execution::fate_chain::remote::{State as Chain, Store as RemoteStore};
use ae_vm_fate::machine::program::remote::{self, invocation};
use ae_vm_fate::value::ObjectKind;
use remote_tests::small;

static INVOCATION: LazyLock<J> = LazyLock::new(|| {
    let packed: J =
        serde_json::from_str(include_str!("../fixtures/erlang_fate_invocation.json")).unwrap();
    assert_eq!(packed["format"], "interned-json-v1");
    let pool = rows(&packed["values"]);
    name_tests::expand(&packed["root"], pool, pool.len())
});
fn spec<'a>(
    row: &J,
    run: &J,
    bytecode: &'a [u8],
    calldata: &'a [u8],
) -> invocation::Spec<'a, RemoteStore> {
    let mut tree = Trie::new(MemoryStore::default());
    for pair in rows(&row["base"]) {
        tree.put(
            &Nibbles::from_bytes(&bytes(&pair[0])),
            TrieValue::Bytes(bytes(&pair[1])),
        )
        .unwrap();
    }
    invocation::Spec {
        contract: small(1),
        bytecode,
        calldata,
        vm_version: context(row).vm_version,
        gas: value(&run["gas"]),
        value: value(&row["value"]),
        store: RemoteStore(Backend(Store::new(tree))),
        allow_init: row["allow_init"].as_bool().unwrap(),
    }
}
fn environment(row: &J) -> invocation::Environment {
    invocation::Environment {
        protocol: row["protocol"].as_u64().unwrap() as u32,
        caller: if row["caller"].is_null() {
            None
        } else {
            let Value::Object(ObjectKind::Address, payload) = value(&row["caller"]) else {
                panic!()
            };
            Some(payload)
        },
        metering_floor: row["floor"].as_u64().map(|v| v as u32),
        debug: row["debug_mode"].as_bool().unwrap(),
    }
}
fn host(row: &J) -> Chain {
    let mut host = remote_tests::chain(row);
    host.authentication.dry_run = row["dry_run"].as_bool().unwrap();
    if row["auth"].as_bool().unwrap() {
        host.environment.auth_tx_hash = Some(small(17).to_vec());
    }
    if !row["onchain"].as_bool().unwrap() {
        host.onchain = Some(Box::new(ae_execution::fate_chain::remote::View {
            accounts: host.accounts.clone(),
            contracts: host.contracts.clone(),
            names: host.names.clone(),
            name_cache: host.name_cache.clone(),
            oracles: host.oracles.clone(),
            oracle_reads: host.oracle_reads.clone(),
        }));
    }
    host
}
fn check<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    out: remote::Outcome<RemoteStore, Chain<A, C>>,
    expected: &J,
) {
    match out {
        remote::Outcome::Returned { state, .. } => {
            assert_eq!(expected["kind"], "returned");
            remote_tests::assert_remote(&state, &expected["state"], false);
        }
        remote::Outcome::Exception(exception) => {
            assert_eq!(expected["kind"], "exception");
            match exception {
                Exception::Native(reason) => {
                    assert_eq!(expected["class"], "error");
                    assert_eq!(expected["reason"], reason);
                }
                Exception::Exit(reason) => {
                    assert_eq!(expected["class"], "exit");
                    assert_eq!(expected["reason"], reason);
                }
                Exception::UnknownInstruction => {
                    assert_eq!(expected["class"], "throw");
                    assert_eq!(expected["reason"], "unknown_instruction");
                }
                other => panic!("unexpected {other:?}"),
            }
        }
        other => {
            remote_tests::check(other, expected);
        }
    }
}

#[test]
fn serialized_invocation_setup_and_execution_match_original_engine() {
    let overrides = Overrides::default();
    let sources = BTreeMap::new();
    let mut count = 0;
    for build in rows(&INVOCATION["builds"]) {
        let order = otp_order::profile(&build["profile"]);
        for row in rows(&build["cases"]) {
            let mut context = context(row);
            context.in_auth = !context.in_auth;
            context.on_chain = !context.on_chain;
            // Deliberately retain the legacy helper's different current key and
            // reserved gas: a new invocation must derive fresh values from Spec.
            let executor = Executor {
                context: &context,
                order: &order,
                overrides: &overrides,
                contracts: &sources,
            };
            let bytecode = bytes(&row["bytecode"]);
            let calldata = bytes(&row["calldata"]);
            let env = environment(row);
            let host = host(row);
            for sample in rows(&row["runs"]) {
                let spec = spec(row, sample, &bytecode, &calldata);
                let label = format!(
                    "{} protocol {} vm {} gas {}",
                    row["name"], row["protocol"], row["vm"], sample["gas"]
                );
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let before = format!("{spec:?}{host:?}");
                    check(
                        executor
                            .setup_invocation(&spec, &env, &host, Limits::default())
                            .unwrap(),
                        &sample["setup"],
                    );
                    check(
                        executor
                            .run_invocation(&spec, &env, &host, Limits::default(), 100)
                            .unwrap(),
                        &sample["run"],
                    );
                    assert_eq!(format!("{spec:?}{host:?}"), before);
                }))
                .unwrap_or_else(|e| {
                    eprintln!("{label}");
                    std::panic::resume_unwind(e)
                });
                count += 1;
            }
        }
    }
    assert_eq!(count, 6768);
}

#[test]
fn invocation_work_and_backend_failures_preserve_input_and_retry() {
    let build = &INVOCATION["builds"][1];
    let row = rows(&build["cases"])
        .iter()
        .find(|r| r["name"] == "store" && r["protocol"] == 6 && r["vm"] == 8)
        .unwrap();
    let sample = rows(&row["runs"]).last().unwrap();
    let order = otp_order::profile(&build["profile"]);
    let overrides = Overrides::default();
    let sources = BTreeMap::new();
    let context = context(row);
    let ex = Executor {
        context: &context,
        order: &order,
        overrides: &overrides,
        contracts: &sources,
    };
    let bytecode = bytes(&row["bytecode"]);
    let calldata = bytes(&row["calldata"]);
    let spec = spec(row, sample, &bytecode, &calldata);
    let env = environment(row);
    let host = host(row);
    let before = format!("{spec:?}{host:?}");
    let (_, remaining) =
        ae_vm_fate::code::Code::decode_bounded(&bytecode, Limits::default()).unwrap();
    let shared = Limits {
        max_nodes: Limits::default().max_nodes - remaining.max_nodes + 1,
        ..Limits::default()
    };
    assert!(ae_vm_fate::code::Code::decode(&bytecode, shared).is_ok());
    assert!(ae_vm_fate::encoding::decode_for_vm(&calldata, shared).is_ok());
    assert!(matches!(
        ex.setup_invocation(&spec, &env, &host, shared),
        Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
    ));
    assert_eq!(format!("{spec:?}{host:?}"), before);
    for limits in [
        Limits {
            max_nodes: 0,
            ..Limits::default()
        },
        Limits {
            max_bytes: bytecode.len() + calldata.len() - 1,
            ..Limits::default()
        },
        Limits {
            max_depth: 0,
            ..Limits::default()
        },
    ] {
        assert!(matches!(
            ex.run_invocation(&spec, &env, &host, limits, 100),
            Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
        ));
        assert_eq!(format!("{spec:?}{host:?}"), before);
    }
    assert!(matches!(
        ex.run_invocation(&spec, &env, &host, Limits::default(), 0),
        Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
    ));
    assert_eq!(format!("{spec:?}{host:?}"), before);
    let faults = oracle_write_tests::Faults::default();
    let host = oracle_write_tests::fault_host(host, &faults);
    faults.fail_reads.set(true);
    let before = format!("{spec:?}{host:?}");
    // Entry uses supplied code/store, with no host contract lookup. The
    // backend fault is encountered only when completion installs the store.
    assert!(matches!(
        ex.setup_invocation(&spec, &env, &host, Limits::default())
            .unwrap(),
        remote::Outcome::Continue { .. }
    ));
    assert!(matches!(
        ex.run_invocation(&spec, &env, &host, Limits::default(), 100),
        Err(Error::Backend(_))
    ));
    assert_eq!(format!("{spec:?}{host:?}"), before);
    faults.fail_reads.set(false);
    check(
        ex.run_invocation(&spec, &env, &host, Limits::default(), 100)
            .unwrap(),
        &sample["run"],
    );
}
