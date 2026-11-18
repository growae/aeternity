use super::*;
use ae_execution::fate_chain::{
    history::{HeaderLink, Snapshot},
    remote::{State as Chain, Store as RemoteStore, View},
};
use ae_state::{accounts::Accounts, contracts::Contracts, oracles::Oracles};
use ae_vm_fate::machine::program::remote::{
    self,
    context::{Consensus, Environment},
};
use remote_tests::{key, small};
use std::sync::Arc;
static CONTEXT: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../fixtures/erlang_fate_context.json")).unwrap()
});
pub(super) fn chain_base(row: &J) -> Chain {
    let mut c = remote_tests::chain(row);
    let t = remote_tests::tree(&row["oracles"]);
    let cache = row
        .get("oracle_cache")
        .map(remote_tests::tree)
        .unwrap_or_else(|| Trie::new(MemoryStore::default()));
    c.oracles = Oracles::from_roots(
        t.root_hash(),
        cache.root_hash(),
        t.into_store(),
        cache.into_store(),
    )
    .unwrap();
    if !row["onchain"].as_bool().unwrap() {
        let a = remote_tests::tree(&row["onchain_view"]["accounts"]);
        let o = remote_tests::tree(&row["onchain_view"]["oracles"]);
        c.onchain = Some(Box::new(View {
            oracle_reads: Default::default(),
            accounts: Accounts::from_root(a.root_hash(), a.into_store()).unwrap(),
            contracts: Contracts::default(),
            oracles: Oracles::from_roots(o.root_hash(), None, o.into_store(), Default::default())
                .unwrap(),
        }));
    }
    let e = &row["environment"];
    c.authentication.dry_run = e["dry_run"] == true;
    c.authentication.payer = e["payer"]
        .as_str()
        .map(|_| ae_types::AccountPublicKey(key(&e["payer"])));
    if e["ga_auth"] == true {
        c.authentication
            .ga_authenticated
            .insert(ae_types::AccountPublicKey(small(1)));
    }
    if !e["ga_nonce"].is_null() {
        c.authentication
            .ga_nonces
            .insert(ae_types::AccountPublicKey(small(1)), bytes(&e["ga_nonce"]));
    }
    c.environment = Environment {
        auth_tx_hash: e["auth_hash"].as_str().map(|s| hex::decode(s).unwrap()),
        origin: key(&e["origin"]),
        gas_price: number(&e["gas_price"]),
        fee: number(&e["fee"]),
        beneficiary: key(&e["beneficiary"]),
        timestamp: number(&e["timestamp"]),
        difficulty: number(&e["difficulty"]),
        block_gas_limit: number(&e["gas_limit"]),
        network_id: bytes(&e["network"]),
        consensus: if e["consensus"] == "pos" {
            Consensus::Pos
        } else {
            Consensus::Pow
        },
        key_hash: key(&e["key_hash"]),
    };

    c
}
static HISTORY: LazyLock<Arc<Snapshot>> = LazyLock::new(|| {
    Arc::new(Snapshot {
        selected: rows(&CONTEXT["history"]["selected"])
            .iter()
            .map(|p| (p[0].as_u64().unwrap(), key(&p[1])))
            .collect(),
        headers: rows(&CONTEXT["history"]["headers"])
            .iter()
            .map(|h| {
                (
                    key(&h["hash"]),
                    HeaderLink {
                        height: h["height"].as_u64().unwrap(),
                        previous_key: key(&h["previous"]),
                    },
                )
            })
            .collect(),
    })
});
fn chain(row: &J) -> Chain {
    let mut c = chain_base(row);
    c.history = Arc::clone(&HISTORY);
    c
}
fn start<O: ae_vm_fate::maps::MapOrder>(
    executor: &Executor<'_, RemoteStore, O>,
    row: &J,
    gas: &J,
) -> (remote::State<RemoteStore, Chain>, Limits) {
    start_with_host(executor, row, gas, chain(row))
}
pub(super) fn start_with_host<O: ae_vm_fate::maps::MapOrder>(
    executor: &Executor<'_, RemoteStore, O>,
    row: &J,
    gas: &J,
    host: Chain,
) -> (remote::State<RemoteStore, Chain>, Limits) {
    let (mut state, remaining_limits) = remote_tests::start(executor, row, gas, host);
    state.program.machine.accumulator.top = value(&row["initial"]["top"]);
    state.program.machine.accumulator.rest =
        rows(&row["initial"]["rest"]).iter().map(value).collect();
    (state, remaining_limits)
}
#[test]
fn context_queries_logs_and_fork_hashes_match_original_dispatch_and_whole_engine() {
    assert_eq!(CONTEXT["base"], FIXTURE["base"]);
    let order = otp_order::profile(&CONTEXT["profile"]);
    let overrides = Overrides::default();
    let sources = remote_tests::sources();
    for row in rows(&CONTEXT["cases"]) {
        let mut context = context(row);
        context.current_contract = small(1);
        context.reserved_gas = 18.into();
        let executor = Executor {
            context: &context,
            order: &order,
            overrides: &overrides,
            contracts: &sources,
        };
        for run in rows(&row["runs"]) {
            let (mut state, mut limits) = start(&executor, row, &run["gas"]);
            for (index, step) in rows(&run["steps"]).iter().enumerate() {
                let expected = &CONTEXT["results"][step.as_u64().unwrap() as usize];
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

#[test]
fn protected_history_reads_and_log_copy_limits_are_retryable_without_partial_results() {
    let order = otp_order::profile(&CONTEXT["profile"]);
    let overrides = Overrides::default();
    let sources = remote_tests::sources();
    for name in ["protected_history", "protected_log_success"] {
        let row = rows(&CONTEXT["cases"])
            .iter()
            .find(|r| r["name"] == name && r["protocol"] == 6 && r["vm"] == 8)
            .unwrap();
        let run = rows(&row["runs"]).last().unwrap();
        let mut context = context(row);
        context.current_contract = small(1);
        context.reserved_gas = 18.into();
        let ex = Executor {
            context: &context,
            order: &order,
            overrides: &overrides,
            contracts: &sources,
        };
        let (mut state, limits) = start(&ex, row, &run["gas"]);
        if name == "protected_history" {
            let history = Arc::clone(&state.session.host.history);
            Arc::make_mut(&mut state.session.host.history)
                .selected
                .remove(&299);
            let before = format!("{state:?}");
            assert!(matches!(
                ex.run_remote_program(&state, limits, 100),
                Err(Error::Backend(ae_execution::fate_chain::Error::NeedHeight(
                    299
                )))
            ));
            assert_eq!(format!("{state:?}"), before);
            state.session.host.history = Arc::clone(&history);
            let missing = state.session.host.environment.key_hash;
            Arc::make_mut(&mut state.session.host.history)
                .headers
                .remove(&missing);
            let before = format!("{state:?}");
            assert!(
                matches!(ex.run_remote_program(&state,limits,100),Err(Error::Backend(ae_execution::fate_chain::Error::NeedHeader(k))) if k==missing)
            );
            assert_eq!(format!("{state:?}"), before);
            state.session.host.history = history;
        } else {
            // Stop just after the callee emits its log; the next instruction
            // must bound existing logs before cloning them into another step.
            let mut remaining = limits;
            while state.session.logs.is_empty() {
                let remote::Outcome::Continue {
                    state: next,
                    remaining_limits,
                } = ex.advance_remote_program(&state, remaining).unwrap()
                else {
                    panic!()
                };
                state = next;
                remaining = remaining_limits;
            }
            let before = format!("{state:?}");
            assert!(matches!(
                ex.advance_remote_program(
                    &state,
                    Limits {
                        max_bytes: 1,
                        ..remaining
                    }
                ),
                Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
            ));
            assert_eq!(format!("{state:?}"), before);
        }
        let expected = &CONTEXT["results"][run["terminal"].as_u64().unwrap() as usize];
        assert!(
            remote_tests::check(
                ex.run_remote_program(&state, Limits::default(), 100)
                    .unwrap(),
                expected
            )
            .is_none()
        );
    }
}

#[test]
fn history_traversal_limits_and_cycles_are_operational_and_retryable() {
    use ae_execution::fate_chain::history::block_hash;
    use remote::HostFailure;
    let row = rows(&CONTEXT["cases"])
        .iter()
        .find(|r| r["name"] == "protected_history")
        .unwrap();
    let c = chain(row);
    let key_hash = c.environment.key_hash;
    assert!(matches!(
        block_hash(&c.history, 299, key_hash, VmVersion::Three, 2),
        Err(HostFailure::ResourceLimit)
    ));
    let expected = c.history.headers[&key_hash].previous_key;
    assert_eq!(
        block_hash(&c.history, 299, key_hash, VmVersion::Three, 100).ok(),
        Some(expected)
    );
    assert!(matches!(
        block_hash(&c.history, 299, key_hash, VmVersion::Two, 100),
        Err(HostFailure::Exception(Exception::Native("badmatch")))
    ));
    let mut cyclic = (*c.history).clone();
    cyclic.headers.get_mut(&key_hash).unwrap().previous_key = key_hash;
    assert!(matches!(
        block_hash(&cyclic, 299, key_hash, VmVersion::Three, 100),
        Err(HostFailure::ResourceLimit)
    ));
    assert_eq!(
        block_hash(&c.history, 299, key_hash, VmVersion::Three, 100).ok(),
        Some(expected)
    );
}

#[test]
fn context_tree_read_failures_do_not_become_absence_or_payability_defaults() {
    use ae_trie::{NodeReader, NodeWriter};
    use remote::{Host, HostFailure, context::AddressTest};
    use std::{cell::Cell, rc::Rc};
    #[derive(Debug, Clone)]
    struct Reader {
        inner: MemoryStore,
        fail: Rc<Cell<bool>>,
    }
    impl NodeReader for Reader {
        fn node(&self, h: &ae_types::Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            if self.fail.get() {
                Err(ae_trie::Error::Backend("context read failed".into()))
            } else {
                self.inner.node(h)
            }
        }
    }
    impl NodeWriter for Reader {
        fn put_node(&mut self, h: ae_types::Hash256, b: Vec<u8>) -> Result<(), ae_trie::Error> {
            self.inner.put_node(h, b)
        }
    }
    fn view(c: &Chain, fail: &Rc<Cell<bool>>) -> View<Reader, Reader> {
        let reader = |s: &MemoryStore| Reader {
            inner: s.clone(),
            fail: Rc::clone(fail),
        };
        View {
            oracle_reads: Default::default(),
            accounts: Accounts::from_root(
                c.accounts.root_hash(),
                reader(c.accounts.tree().store()),
            )
            .unwrap(),
            contracts: Contracts::from_root(
                c.contracts.root_hash(),
                reader(c.contracts.tree().store()),
            )
            .unwrap(),
            oracles: Oracles::from_roots(
                c.oracles.root_hash(),
                None,
                reader(c.oracles.tree().store()),
                reader(&MemoryStore::default()),
            )
            .unwrap(),
        }
    }
    let row = rows(&CONTEXT["cases"])
        .iter()
        .find(|r| r["name"] == "BALANCE")
        .unwrap();
    let c = chain(row);
    let fail = Rc::new(Cell::new(false));
    let v = view(&c, &fail);
    let mut host = ae_execution::fate_chain::remote::State {
        accounts: v.accounts,
        contracts: v.contracts,
        oracles: v.oracles,
        oracle_reads: Default::default(),
        onchain: Some(Box::new(view(&c, &fail))),
        authentication: Default::default(),
        oracle_base: None,
        height: c.height,
        events: vec![],
        environment: c.environment,
        history: c.history,
    };
    let before = format!("{host:?}");
    fail.set(true);
    assert!(matches!(
        host.balance(&small(1)),
        Err(HostFailure::Backend(
            ae_execution::fate_chain::Error::Accounts(_)
        ))
    ));
    assert!(matches!(
        host.creator(small(1)),
        Err(HostFailure::Backend(
            ae_execution::fate_chain::Error::Contracts(_)
        ))
    ));
    for what in [
        AddressTest::Oracle,
        AddressTest::Contract,
        AddressTest::Payable,
    ] {
        assert!(matches!(
            host.address_test(what, &small(1)),
            Err(HostFailure::Backend(_))
        ));
    }
    fail.set(false);
    assert_eq!(format!("{host:?}"), before);
    assert_eq!(host.balance(&small(1)).ok().flatten(), Some(0.into()));
    assert_eq!(host.creator(small(1)).ok(), Some(small(8)));
    assert_eq!(
        host.address_test(AddressTest::Oracle, &small(1)).ok(),
        Some(true)
    );
    assert_eq!(
        host.address_test(AddressTest::Contract, &small(1)).ok(),
        Some(true)
    );
    assert_eq!(
        host.address_test(AddressTest::Payable, &small(2)).ok(),
        Some(false)
    );
}
