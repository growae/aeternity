use super::*;
use ae_vm_fate::machine::program::remote;
use remote_tests::small;

static HOSTS: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../fixtures/erlang_fate_hosts.json")).unwrap()
});
fn unpack(row: &J) -> J {
    let mut row = row.clone();
    for field in rows(&HOSTS["input_fields"]) {
        let field = string(field);
        let index = row[field].as_u64().unwrap() as usize;
        row[field] = HOSTS["inputs"][index].clone();
    }
    row
}
fn selected(name: &str) -> J {
    unpack(
        rows(&HOSTS["cases"])
            .iter()
            .find(|r| r["name"] == name && r["protocol"] == 6 && r["vm"] == 8)
            .unwrap(),
    )
}

#[test]
fn oracle_read_failures_preserve_caches_and_can_be_retried() {
    use ae_execution::fate_chain::oracle::{self, Cache};
    use ae_state::oracles::Oracles;
    use ae_trie::NodeReader;
    use ae_vm_fate::types::Type;
    use remote::{
        HostFailure,
        hosts::{OracleOperation as Op, OracleRead},
    };
    use std::{cell::Cell, rc::Rc};
    struct Reader {
        inner: MemoryStore,
        fail: Rc<Cell<bool>>,
    }
    impl NodeReader for Reader {
        fn node(&self, h: &ae_types::Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            if self.fail.get() {
                Err(ae_trie::Error::Backend("oracle read failed".into()))
            } else {
                self.inner.node(h)
            }
        }
    }
    let host = context_tests::chain_base(&selected("oracle_auth"));
    let fail = Rc::new(Cell::new(false));
    let tree = Oracles::from_roots(
        host.oracles.root_hash(),
        None,
        Reader {
            inner: host.oracles.tree().store().clone(),
            fail: Rc::clone(&fail),
        },
        MemoryStore::default(),
    )
    .unwrap();
    let oracle = small(11);
    let query = small(1);
    let q = Value::TypeRep(Type::Integer);
    let r = Value::TypeRep(Type::Boolean);
    let request = OracleRead {
        operation: Op::Answer,
        oracle: &oracle,
        query: Some(&query),
        question_type: Some(&q),
        response_type: Some(&r),
    };
    let mut cache = Cache::default();
    fail.set(true);
    assert!(matches!(
        oracle::read(&tree, &mut cache, request, Limits::default()),
        Err(HostFailure::Backend(_))
    ));
    assert!(cache.oracles.is_empty() && cache.queries.is_empty());
    fail.set(false);
    // A positive oracle lookup may already be cached while its query is not.
    oracle::lookup(&tree, &mut cache, &oracle).unwrap();
    let before = format!("{cache:?}");
    fail.set(true);
    assert!(matches!(
        oracle::read(&tree, &mut cache, request, Limits::default()),
        Err(HostFailure::Backend(_))
    ));
    assert_eq!(format!("{cache:?}"), before);
    fail.set(false);
    for limits in [
        Limits {
            max_bytes: 0,
            ..Limits::default()
        },
        Limits {
            max_nodes: 0,
            ..Limits::default()
        },
    ] {
        assert!(matches!(
            oracle::read(&tree, &mut cache, request, limits),
            Err(HostFailure::ResourceLimit)
        ));
        assert_eq!(format!("{cache:?}"), before);
    }
    let answer = oracle::read(&tree, &mut cache, request, Limits::default()).unwrap();
    assert_eq!(
        answer,
        Value::Variant {
            arities: vec![0.into(), 1.into()],
            tag: 1,
            values: vec![Value::Boolean(true)]
        }
    );
    assert_eq!(cache.queries.len(), 1);
    // Once both objects are cached, a backing-store outage does not invalidate them.
    fail.set(true);
    assert_eq!(
        oracle::read(&tree, &mut cache, request, Limits::default()).unwrap(),
        answer
    );
    fail.set(false);
    let raw_oracle = small(10);
    let text = Value::TypeRep(Type::String);
    let raw = OracleRead {
        operation: Op::Question,
        oracle: &raw_oracle,
        question_type: Some(&text),
        response_type: Some(&text),
        ..request
    };
    let mut empty = Cache::default();
    assert!(matches!(
        oracle::read(
            &tree,
            &mut empty,
            raw,
            Limits {
                max_bytes: 1,
                ..Limits::default()
            }
        ),
        Err(HostFailure::ResourceLimit)
    ));
    assert!(empty.oracles.is_empty() && empty.queries.is_empty());
    assert_eq!(
        oracle::read(&tree, &mut empty, raw, Limits::default()).unwrap(),
        Value::String(b"question".to_vec())
    );
}

#[test]
fn protected_oracle_decode_and_cached_code_encoding_limits_are_retryable() {
    let order = otp_order::profile(&HOSTS["profile"]);
    let overrides = Overrides::default();
    let sources = remote_tests::sources();
    for name in ["protected_oracle_cache", "hash_cache"] {
        let row = selected(name);
        let run = rows(&row["runs"]).last().unwrap();
        let mut context = context(&row);
        context.current_contract = small(1);
        context.reserved_gas = 18.into();
        let ex = Executor {
            context: &context,
            order: &order,
            overrides: &overrides,
            contracts: &sources,
        };
        let (mut state, mut limits) =
            context_tests::start_with_host(&ex, &row, &run["gas"], context_tests::chain_base(&row));
        let restricted;
        if name == "protected_oracle_cache" {
            while state.session.current_contract == small(1) {
                let remote::Outcome::Continue {
                    state: next,
                    remaining_limits,
                } = ex.advance_remote_program(&state, limits).unwrap()
                else {
                    panic!()
                };
                state = next;
                limits = remaining_limits;
            }
            assert!(!state.session.snapshots.is_empty());
            // The failure occurs while decoding a real query response inside
            // the protected body, after its call checkpoint has been created.
            let mut answer = Value::Boolean(true);
            for _ in 0..80 {
                answer = Value::Tuple(vec![answer]);
            }
            let mut query = state
                .session
                .host
                .oracles
                .query(small(11), small(1))
                .unwrap()
                .unwrap();
            query.response =
                Some(ae_vm_fate::encoding::encode(&answer, Limits::default()).unwrap());
            state.session.host.oracles.enter_query(&query).unwrap();
            restricted = Limits {
                max_depth: 32,
                ..limits
            };
        } else {
            // Cached code is immutable/shared until serialization. Bound the
            // annotation copy in BYTECODE_HASH, even when no host fetch occurs.
            let mut code = program_tests::code(&row["codes"][1]);
            code.annotations = Value::String(vec![42; 10000]);
            state.session.code_cache.insert(
                small(2),
                remote::LoadedCode {
                    code: std::sync::Arc::new(code),
                    vm_version: VmVersion::Three,
                },
            );
            restricted = Limits {
                max_bytes: 4096,
                ..limits
            };
        }
        let before = format!("{state:?}");
        assert!(matches!(
            ex.advance_remote_program(&state, restricted),
            Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
        ));
        assert_eq!(format!("{state:?}"), before);
        if name == "protected_oracle_cache" {
            let expected = &HOSTS["results"][run["terminal"].as_u64().unwrap() as usize];
            // The later EXIT restores the original host snapshot, including
            // the original oracle tree, after a successful larger-budget retry.
            assert!(
                remote_tests::check(
                    ex.run_remote_program(&state, limits, 100).unwrap(),
                    expected
                )
                .is_none()
            );
        } else {
            assert!(matches!(
                ex.advance_remote_program(&state, limits).unwrap(),
                remote::Outcome::Continue { .. }
            ));
        }
    }
}

#[test]
fn spending_oracle_reads_and_hashes_match_original_instruction_and_whole_engine() {
    assert_eq!(HOSTS["base"], FIXTURE["base"]);
    let order = otp_order::profile(&HOSTS["profile"]);
    let overrides = Overrides::default();
    let sources = remote_tests::sources();
    for packed in rows(&HOSTS["cases"]) {
        let row = unpack(packed);
        let mut context = context(&row);
        context.current_contract = small(1);
        context.reserved_gas = 18.into();
        let executor = Executor {
            context: &context,
            order: &order,
            overrides: &overrides,
            contracts: &sources,
        };
        let host = context_tests::chain_base(&row);
        for run in rows(&row["runs"]) {
            let (mut state, mut limits) =
                context_tests::start_with_host(&executor, &row, &run["gas"], host.clone());
            for (index, step) in rows(&run["steps"]).iter().enumerate() {
                let expected = &HOSTS["results"][step.as_u64().unwrap() as usize];
                let label = format!(
                    "case {} {} protocol {} vm {} gas {} step {}",
                    row["case_id"], row["name"], row["protocol"], row["vm"], run["gas"], index
                );
                let out = executor
                    .advance_remote_program(&state, limits)
                    .unwrap_or_else(|e| panic!("{label}: {e:?}"));
                let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    remote_tests::check(out, expected)
                }))
                .unwrap_or_else(|e| {
                    eprintln!("{label}");
                    std::panic::resume_unwind(e)
                });
                if let Some((next, budget)) = checked {
                    state = next;
                    limits = budget;
                } else {
                    assert_eq!(index + 1, rows(&run["steps"]).len());
                    break;
                }
            }
        }
    }
}
