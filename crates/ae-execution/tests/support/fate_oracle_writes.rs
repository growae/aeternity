use super::*;
use remote_tests::small;

static HOSTS: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../fixtures/erlang_fate_oracle_writes.json")).unwrap()
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
#[test]
fn oracle_writes_delegations_and_rollback_match_original_instruction_and_whole_engine() {
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
                if let remote::Outcome::Exception(Exception::Host { operation, detail }) = &out {
                    assert_eq!(*operation, "get_account", "{label}");
                    assert_eq!(detail, "account_not_found", "{label}");
                    assert_eq!(expected["reason"], "aeprimop_state", "{label}");
                }
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

use ae_execution::fate_chain::remote::State as Chain;
use ae_state::{accounts::Accounts, contracts::Contracts, oracles::Oracles};
use ae_trie::{NodeReader, NodeWriter};
use ae_vm_fate::machine::program::remote::{
    self, Host, HostFailure,
    oracle_write::{Request, Ttl},
};
use std::{cell::Cell, rc::Rc};

fn selected(id: u64) -> J {
    unpack(
        rows(&HOSTS["cases"])
            .iter()
            .find(|r| r["case_id"] == id && r["protocol"] == 6 && r["vm"] == 8)
            .unwrap(),
    )
}
fn terminal(row: &J) -> &J {
    let run = rows(&row["runs"])
        .iter()
        .find(|r| r["gas"] == "200000")
        .unwrap();
    &HOSTS["results"][run["terminal"].as_u64().unwrap() as usize]
}
#[derive(Clone, Debug, Default)]
pub(super) struct Faults {
    pub writes_left: Rc<Cell<Option<usize>>>,
    pub reads_left: Rc<Cell<Option<usize>>>,
    pub fail_reads: Rc<Cell<bool>>,
}
#[derive(Clone, Debug)]
pub(super) struct FaultStore {
    inner: MemoryStore,
    faults: Faults,
}
impl NodeReader for FaultStore {
    fn node(&self, hash: &ae_types::Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        if self.faults.fail_reads.get() {
            return Err(ae_trie::Error::Backend("injected oracle read".into()));
        }
        if let Some(left) = self.faults.reads_left.get() {
            if left == 0 {
                return Err(ae_trie::Error::Backend("injected read budget".into()));
            }
            self.faults.reads_left.set(Some(left - 1));
        }
        self.inner.node(hash)
    }
}
impl NodeWriter for FaultStore {
    fn put_node(&mut self, hash: ae_types::Hash256, bytes: Vec<u8>) -> Result<(), ae_trie::Error> {
        if let Some(left) = self.faults.writes_left.get() {
            if left == 0 {
                return Err(ae_trie::Error::Backend("injected oracle write".into()));
            }
            self.faults.writes_left.set(Some(left - 1));
        }
        self.inner.put_node(hash, bytes)
    }
}
pub(super) fn fault_host(host: Chain, faults: &Faults) -> Chain<FaultStore, FaultStore> {
    assert!(host.onchain.is_none() && host.oracle_base.is_none() && host.name_base.is_none());
    let wrap = |inner: &MemoryStore| FaultStore {
        inner: inner.clone(),
        faults: faults.clone(),
    };
    Chain {
        name_claim_bid_timeout: host.name_claim_bid_timeout.clone(),
        contract_cache: host.contract_cache.clone(),
        auth_tx: host.auth_tx.clone(),

        accounts: Accounts::from_root(
            host.accounts.root_hash(),
            wrap(host.accounts.tree().store()),
        )
        .unwrap(),
        contracts: Contracts::from_root(
            host.contracts.root_hash(),
            wrap(host.contracts.tree().store()),
        )
        .unwrap(),
        oracles: Oracles::from_roots(
            host.oracles.root_hash(),
            host.oracles.cache_root_hash(),
            wrap(host.oracles.tree().store()),
            wrap(host.oracles.cache().store()),
        )
        .unwrap(),
        height: host.height,
        authentication: host.authentication,
        events: host.events,
        oracle_reads: host.oracle_reads,
        oracle_base: None,
        name_base: None,
        name_cache: host.name_cache,
        names: ae_state::names::Names::from_roots(
            host.names.root_hash(),
            host.names.cache_root_hash(),
            wrap(host.names.tree().store()),
            wrap(host.names.cache().store()),
        )
        .unwrap(),
        onchain: None,
        environment: host.environment,
        history: host.history,
    }
}
fn request(id: u64) -> Request {
    use ae_vm_fate::{encoding, types::Type};
    let key = small(1).to_vec();
    let ttl = Ttl {
        relative: true,
        value: 10.into(),
    };
    let question_type = Value::TypeRep(Type::Integer);
    let response_type = Value::TypeRep(Type::String);
    match id {
        0 => Request::Register {
            key,
            ttl,
            fee: 7.into(),
            query_format: encoding::encode(&question_type, Limits::default()).unwrap(),
            response_format: encoding::encode(&response_type, Limits::default()).unwrap(),
        },
        29 => Request::Query {
            key,
            sender: small(1),
            content: Value::Integer(42.into()),
            fee: 7.into(),
            query_ttl: ttl,
            response_ttl: 5.into(),
            question_type,
            response_type,
        },
        58 => Request::Respond {
            key,
            query: small(7).to_vec(),
            content: Value::String(b"answer".to_vec()),
            question_type,
            response_type,
        },
        87 => Request::Extend { key, ttl },
        _ => unreachable!(),
    }
}
#[test]
fn failed_oracle_writes_leave_accounts_objects_expiry_cache_and_events_retryable() {
    for id in [0, 29, 58, 87] {
        let row = selected(id);
        let base = context_tests::chain_base(&row);
        let request = request(id);
        let faults = Faults::default();
        let mut failures = 0;
        let mut succeeded = false;
        // Move the failure through every persistent node write, including
        // account debits/rewards, query objects and final expiry publication.
        for allowed in 0..256 {
            faults.writes_left.set(None);
            let mut host = fault_host(base.clone(), &faults);
            faults.writes_left.set(Some(allowed));
            let before = format!("{host:?}");
            match host.oracle_write(&request, 6, Limits::default()) {
                Err(HostFailure::Backend(_)) => {
                    failures += 1;
                    faults.writes_left.set(Some(allowed));
                    assert_eq!(format!("{host:?}"), before, "case {id}, write {allowed}");
                    faults.writes_left.set(None);
                    host.oracle_write(&request, 6, Limits::default()).unwrap();
                    remote_tests::assert_chain(&host, &terminal(&row)["state"]["chain"]);
                }
                Ok(_) => {
                    succeeded = true;
                    break;
                }
                other => panic!("case {id}, write {allowed}: {other:?}"),
            }
        }
        assert!(succeeded && failures > 1, "case {id}: {failures} failures");
    }
}
#[test]
fn oracle_overlay_limits_and_delegation_read_failures_are_operational() {
    for id in [0, 29, 58, 87] {
        let row = selected(id);
        let mut host = context_tests::chain_base(&row);
        let before = format!("{host:?}");
        for limits in [
            Limits {
                max_bytes: 1,
                ..Limits::default()
            },
            Limits {
                max_nodes: 0,
                ..Limits::default()
            },
        ] {
            assert!(
                matches!(
                    host.oracle_write(&request(id), 6, limits),
                    Err(HostFailure::ResourceLimit)
                ),
                "case {id}"
            );
            assert_eq!(format!("{host:?}"), before);
        }
        host.oracle_write(&request(id), 6, Limits::default())
            .unwrap();
        remote_tests::assert_chain(&host, &terminal(&row)["state"]["chain"]);
        let before = format!("{host:?}");
        assert!(matches!(
            host.oracle_write(
                &Request::Extend {
                    key: small(1).to_vec(),
                    ttl: Ttl {
                        relative: true,
                        value: 1.into()
                    }
                },
                6,
                Limits {
                    max_bytes: 1,
                    ..Limits::default()
                }
            ),
            Err(HostFailure::ResourceLimit)
        ));
        assert_eq!(format!("{host:?}"), before);
    }
    let faults = Faults::default();
    let mut host = fault_host(context_tests::chain_base(&selected(0)), &faults);
    faults.fail_reads.set(true);
    let before = format!("{host:?}");
    assert!(matches!(
        host.verify_delegation(&small(1), b"message", &[0; 64], VmVersion::Three),
        Err(HostFailure::Backend(_))
    ));
    assert_eq!(format!("{host:?}"), before);
    faults.fail_reads.set(false);
    assert!(
        !host
            .verify_delegation(&small(1), b"message", &[0; 64], VmVersion::Three)
            .unwrap()
    );
}
#[test]
fn protected_oracle_write_storage_failures_escape_catch_and_retry_to_original_result() {
    let order = otp_order::profile(&HOSTS["profile"]);
    let overrides = Overrides::default();
    let sources = remote_tests::sources();
    for id in 202..=209 {
        let row = selected(id);
        let mut context = context(&row);
        context.current_contract = small(1);
        context.reserved_gas = 18.into();
        let executor = Executor {
            context: &context,
            order: &order,
            overrides: &overrides,
            contracts: &sources,
        };
        let faults = Faults::default();
        let host = fault_host(context_tests::chain_base(&row), &faults);
        let (mut state, mut limits) =
            remote_tests::start(&executor, &row, &J::String("200000".into()), host);
        while state.session.current_contract == small(1) {
            let remote::Outcome::Continue {
                state: next,
                remaining_limits,
            } = executor.advance_remote_program(&state, limits).unwrap()
            else {
                panic!()
            };
            state = next;
            limits = remaining_limits;
        }
        assert!(!state.session.snapshots.is_empty());
        faults.writes_left.set(Some(0));
        let before = format!("{state:?}");
        assert!(
            matches!(
                executor.advance_remote_program(&state, limits),
                Err(Error::Backend(_))
            ),
            "case {id}"
        );
        assert_eq!(format!("{state:?}"), before);
        faults.writes_left.set(None);
        assert!(
            remote_tests::check(
                executor.run_remote_program(&state, limits, 100).unwrap(),
                terminal(&row)
            )
            .is_none()
        );
    }
}
