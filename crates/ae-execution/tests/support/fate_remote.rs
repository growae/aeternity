use super::*;
use ae_execution::fate_chain::{
    TransferEvent,
    remote::{State as Chain, Store as RemoteStore},
};
use ae_state::{StateRoots, accounts::Accounts, contracts::Contracts};
use ae_tx::CanonicalTransaction;
use ae_vm_fate::machine::program::remote::Host;
use ae_vm_fate::{
    machine::program::{Entry, Frame, LocalFrame, remote},
    value::{ObjectKind, ObjectPayload},
};
use serde_json::json;
use std::sync::Arc;
type ChainState<A, C> = remote::State<RemoteStore, Chain<A, C>>;
static REMOTE: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../fixtures/erlang_fate_remote.json")).unwrap()
});

pub(super) fn start<H: Host<ae_execution::fate_chain::Error>, O: ae_vm_fate::maps::MapOrder>(
    executor: &Executor<'_, RemoteStore, O>,
    row: &J,
    gas: &J,
    host: H,
) -> (remote::State<RemoteStore, H>, Limits) {
    let mut stores = Stores::new(Version::for_protocol(
        row["protocol"].as_u64().unwrap() as u32
    ));
    stores.put_contract(small(1), RemoteStore(backend()));
    let machine = State {
        accumulator: Accumulator {
            top: Value::Void,
            rest: Default::default(),
        },
        memory: BTreeMap::new(),
        stores,
        meter: Meter {
            protocol: row["protocol"].as_u64().unwrap() as u32,
            gas: number(gas),
            created_cells: number(&row["initial"]["cells"]),
            metering_floor: None,
        },
    };
    let out = executor
        .start_remote_program(
            remote::LoadedCode {
                code: Arc::new(program_tests::code(&row["codes"][0])),
                vm_version: executor.context.vm_version,
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
        state,
        remaining_limits,
    } = out
    else {
        panic!("entry failed")
    };
    (state, remaining_limits)
}
fn selected(name: &str) -> &'static J {
    rows(&REMOTE["cases"])
        .iter()
        .find(|row| {
            row["name"] == name && row["protocol"] == 6 && row["vm"] == 8 && row["callee_vm"] == 8
        })
        .unwrap()
}
pub(super) fn sources() -> BTreeMap<[u8; 32], RemoteStore> {
    (1..=3)
        .map(|n| (small(n), RemoteStore(backend())))
        .collect()
}

#[test]
fn protected_remote_work_limits_and_store_fetches_are_retryable_without_partial_publication() {
    let row = selected("store_body");
    let run = rows(&row["runs"]).last().unwrap();
    let order = otp_order::profile(&REMOTE["profile"]);
    let overrides = Overrides::default();
    let sources = sources();
    let mut context = context(row);
    context.current_contract = small(1);
    context.reserved_gas = number(&row["reserved"]);
    let executor = Executor {
        context: &context,
        order: &order,
        overrides: &overrides,
        contracts: &sources,
    };
    let (state, limits) = start(&executor, row, &run["gas"], chain(row));
    let before = format!("{state:?}");
    assert!(matches!(
        executor.run_remote_program(&state, limits, 1),
        Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
    ));
    let remote::Outcome::Continue {
        state: at_call,
        remaining_limits,
    } = executor.advance_remote_program(&state, limits).unwrap()
    else {
        panic!()
    };
    let at_call_before = format!("{at_call:?}");
    assert!(matches!(
        executor.advance_remote_program(
            &at_call,
            Limits {
                max_depth: 2,
                ..remaining_limits
            }
        ),
        Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
    ));
    assert_eq!(format!("{at_call:?}"), at_call_before);
    let mut missing = sources.clone();
    missing.remove(&small(2));
    let limited = Executor {
        context: &context,
        order: &order,
        overrides: &overrides,
        contracts: &missing,
    };
    assert!(
        matches!(limited.run_remote_program(&state,limits,100),Err(Error::NeedContract(k)) if k==small(2))
    );
    assert_eq!(format!("{state:?}"), before);
    let terminal = &REMOTE["results"][run["terminal"].as_u64().unwrap() as usize];
    assert!(
        check(
            executor.run_remote_program(&state, limits, 100).unwrap(),
            terminal
        )
        .is_none()
    );
}

#[derive(Debug, Clone)]
struct FailingHost {
    inner: Chain,
    code_failure: bool,
    transfer_failure: bool,
}
impl Host<ae_execution::fate_chain::Error> for FailingHost {
    fn code(
        &mut self,
        key: &[u8],
        limits: Limits,
    ) -> Result<Option<remote::LoadedCode>, remote::HostFailure<ae_execution::fate_chain::Error>>
    {
        if self.code_failure {
            return Err(remote::HostFailure::Backend(
                ae_execution::fate_chain::Error::Contracts(ae_state::contracts::Error::Tree(
                    ae_trie::Error::Backend("injected code read".into()),
                )),
            ));
        }
        self.inner.code(key, limits)
    }
    fn transfer(
        &mut self,
        from: [u8; 32],
        to: [u8; 32],
        value: &BigInt,
    ) -> Result<(), remote::HostFailure<ae_execution::fate_chain::Error>> {
        self.inner.transfer(from, to, value)?;
        if self.transfer_failure {
            return Err(remote::HostFailure::Backend(
                ae_execution::fate_chain::Error::Accounts(ae_state::accounts::AccountError::Tree(
                    ae_trie::Error::Backend("injected transfer after write".into()),
                )),
            ));
        }
        Ok(())
    }
    fn spend(
        &mut self,
        f: [u8; 32],
        t: &[u8],
        v: &BigInt,
    ) -> Result<(), remote::HostFailure<ae_execution::fate_chain::Error>> {
        self.inner.spend(f, t, v)
    }
    fn oracle_read(
        &mut self,
        r: remote::hosts::OracleRead<'_>,
        l: Limits,
    ) -> Result<Value, remote::HostFailure<ae_execution::fate_chain::Error>> {
        self.inner.oracle_read(r, l)
    }
    fn oracle_write(
        &mut self,
        r: &remote::oracle_write::Request,
        p: u32,
        l: Limits,
    ) -> Result<
        remote::oracle_write::ResultValue,
        remote::HostFailure<ae_execution::fate_chain::Error>,
    > {
        self.inner.oracle_write(r, p, l)
    }
    fn verify_delegation(
        &mut self,
        k: &[u8],
        m: &[u8],
        s: &[u8; 64],
        v: VmVersion,
    ) -> Result<bool, remote::HostFailure<ae_execution::fate_chain::Error>> {
        self.inner.verify_delegation(k, m, s, v)
    }
    fn environment(&self) -> &remote::context::Environment {
        self.inner.environment()
    }
    fn balance(
        &mut self,
        k: &[u8],
    ) -> Result<Option<BigInt>, remote::HostFailure<ae_execution::fate_chain::Error>> {
        self.inner.balance(k)
    }
    fn creator(
        &mut self,
        k: [u8; 32],
    ) -> Result<[u8; 32], remote::HostFailure<ae_execution::fate_chain::Error>> {
        self.inner.creator(k)
    }
    fn address_test(
        &mut self,
        w: remote::context::AddressTest,
        k: &[u8],
    ) -> Result<bool, remote::HostFailure<ae_execution::fate_chain::Error>> {
        self.inner.address_test(w, k)
    }
    fn block_hash(
        &mut self,
        h: u64,
        v: VmVersion,
        l: Limits,
    ) -> Result<[u8; 32], remote::HostFailure<ae_execution::fate_chain::Error>> {
        self.inner.block_hash(h, v, l)
    }
    fn height(&self) -> u64 {
        self.inner.height
    }
}
#[test]
fn protected_call_does_not_swallow_host_failures_or_publish_a_mutated_host_attempt() {
    let row = selected("store_body");
    let run = rows(&row["runs"]).last().unwrap();
    let order = otp_order::profile(&REMOTE["profile"]);
    let overrides = Overrides::default();
    let sources = sources();
    let mut context = context(row);
    context.current_contract = small(1);
    context.reserved_gas = number(&row["reserved"]);
    let executor = Executor {
        context: &context,
        order: &order,
        overrides: &overrides,
        contracts: &sources,
    };
    for (code_failure, transfer_failure) in [(true, false), (false, true)] {
        let host = FailingHost {
            inner: chain(row),
            code_failure,
            transfer_failure,
        };
        let (mut state, limits) = start(&executor, row, &run["gas"], host);
        let before = format!("{state:?}");
        assert!(matches!(
            executor.run_remote_program(&state, limits, 100),
            Err(Error::Backend(_))
        ));
        assert_eq!(format!("{state:?}"), before);
        state.session.host.code_failure = false;
        state.session.host.transfer_failure = false;
        let remote::Outcome::Returned { state, .. } =
            executor.run_remote_program(&state, limits, 100).unwrap()
        else {
            panic!("retry did not return")
        };
        let expected = &REMOTE["results"][run["terminal"].as_u64().unwrap() as usize]["state"];
        assert_eq!(
            state.program.machine.accumulator.top,
            value(&expected["top"])
        );
        assert_eq!(state.program.machine.meter.gas, number(&expected["gas"]));
        assert_chain(&state.session.host.inner, &expected["chain"]);
        assert_stores(&state.program.machine.stores, &expected["stores"]);
        assert!(state.session.snapshots.is_empty());
        assert!(state.program.cursor.frames.is_empty());
    }
}
pub(super) fn key(j: &J) -> [u8; 32] {
    bytes(j).try_into().unwrap()
}
pub(super) fn small(n: u8) -> [u8; 32] {
    let mut key = [0; 32];
    key[31] = n;
    key
}
pub(super) fn tree(j: &J) -> Trie<MemoryStore> {
    let mut t = Trie::new(MemoryStore::default());
    for row in rows(j) {
        t.put(
            &Nibbles::from_bytes(&bytes(&row["key"])),
            TrieValue::Bytes(bytes(&row["encoded"])),
        )
        .unwrap();
    }
    t
}
pub(super) fn chain(row: &J) -> Chain {
    let a = tree(&row["initial"]["chain"]["accounts"]);
    let c = tree(&row["contracts"]);
    Chain {
        accounts: Accounts::from_root(a.root_hash(), a.into_store()).unwrap(),
        contracts: Contracts::from_root(c.root_hash(), c.into_store()).unwrap(),
        authentication: Default::default(),
        oracle_base: None,
        height: row["height"].as_u64().unwrap(),
        events: vec![],
        oracles: Default::default(),
        oracle_reads: Default::default(),
        onchain: None,
        history: Default::default(),
        environment: ae_vm_fate::machine::program::remote::context::Environment {
            auth_tx_hash: None,
            origin: small(9),
            gas_price: 0.into(),
            fee: 0.into(),
            beneficiary: small(0),
            timestamp: 0.into(),
            difficulty: 0.into(),
            block_gas_limit: 6000000.into(),
            network_id: b"ae_mainnet".to_vec(),
            consensus: ae_vm_fate::machine::program::remote::context::Consensus::Pow,
            key_hash: small(0),
        },
    }
}
fn oracle_cache(c: &ae_execution::fate_chain::oracle::Cache) -> J {
    let mut items = Vec::new();
    for (key, value) in &c.oracles {
        items.push(json!([
            "oracle",
            hex::encode(key),
            hex::encode(value.serialize().unwrap())
        ]));
    }
    for ((oracle, query), value) in &c.queries {
        items.push(json!([
            "oracle_query",
            [hex::encode(oracle), hex::encode(query)],
            hex::encode(value.serialize().unwrap())
        ]));
    }
    json!(items)
}
fn tree_entries<S: ae_trie::NodeReader>(tree: &Trie<S>) -> J {
    json!(
        tree.iter()
            .map(|r| {
                let (k, TrieValue::Bytes(v)) = r.unwrap() else {
                    panic!()
                };
                let k: Vec<_> = k
                    .as_slice()
                    .chunks_exact(2)
                    .map(|p| p[0] << 4 | p[1])
                    .collect();
                json!({"key":hex::encode(k),"encoded":hex::encode(v)})
            })
            .collect::<Vec<_>>()
    )
}
pub(super) fn assert_chain<A: ae_trie::NodeReader, C: ae_trie::NodeReader>(
    actual: &Chain<A, C>,
    expected: &J,
) {
    if expected.get("oracle_objects").is_some() {
        assert_eq!(
            tree_entries(actual.oracles.tree()),
            expected["oracle_objects"],
            "oracle objects"
        );
        assert_eq!(
            tree_entries(actual.oracles.cache()),
            expected["oracle_cache_entries"],
            "oracle expiry cache"
        );
        assert_eq!(
            json!(actual.oracles.cache_root_hash().map(|h| hex::encode(h.0))),
            expected["oracle_cache_root"],
            "oracle expiry cache root"
        );
    }
    if expected.get("oracle_reads").is_some() {
        assert_eq!(
            oracle_cache(&actual.oracle_reads),
            expected["oracle_reads"],
            "local oracle cache"
        );
        assert_eq!(
            actual
                .onchain
                .as_ref()
                .map(|c| oracle_cache(&c.oracle_reads))
                .unwrap_or(J::Null),
            expected["onchain_oracle_reads"],
            "onchain oracle cache"
        );
    }
    let accounts: Vec<_> = actual
        .accounts
        .tree()
        .iter()
        .map(|r| {
            let (k, TrieValue::Bytes(v)) = r.unwrap() else {
                panic!()
            };
            let key: Vec<_> = k
                .as_slice()
                .chunks_exact(2)
                .map(|p| p[0] << 4 | p[1])
                .collect();
            json!({"key":hex::encode(key),"encoded":hex::encode(v)})
        })
        .collect();
    assert_eq!(json!(accounts), expected["accounts"], "chain accounts");
    assert_eq!(
        json!(actual.accounts.root_hash().map(|h| hex::encode(h.0))),
        expected["root"],
        "account root"
    );
    assert_eq!(
        json!(hex::encode(
            StateRoots {
                accounts: actual.accounts.root_hash(),
                contracts: actual.contracts.root_hash(),
                oracles: actual.oracles.root_hash(),
                ..Default::default()
            }
            .hash()
            .0
        )),
        expected["tree_root"],
        "tree root"
    );
    let events:Vec<_>=actual.events.iter().map(|e|match e {
        TransferEvent::OracleCall {info,transaction}=>json!({"kind":"internal_call","info":info,"transaction":hex::encode(transaction.encode()),"type":"non_tx","tx_hash":""}),
        TransferEvent::OracleDelta {info,account,debit,amount}=>{let n=BigInt::from_bytes_be(num_bigint::Sign::Plus,amount.as_bytes());json!({"kind":"delta","key":hex::encode(account.0),"amount":if *debit {(-n).to_string()} else {n.to_string()},"info":info,"type":"non_tx","tx_hash":""})},
        TransferEvent::InternalCall(tx)=>json!({"kind":"internal_call","info":String::from_utf8_lossy(tx.payload()),"transaction":hex::encode(tx.encode()),"type":"non_tx","tx_hash":""}),
        TransferEvent::Delta {account,debit,amount}=>{let n=BigInt::from_bytes_be(num_bigint::Sign::Plus,amount.as_bytes());json!({"kind":"delta","key":hex::encode(account.0),"amount":if *debit {(-n).to_string()} else {n.to_string()},"info":"Spend.amount","type":"non_tx","tx_hash":""})}
    }).collect();
    assert_eq!(json!(events), expected["events"], "chain events");
}
pub(super) fn assert_stores(actual: &Stores<RemoteStore>, expected: &J) {
    let entries: BTreeMap<_, _> = rows(expected).iter().map(|e| (key(&e[0]), &e[1])).collect();
    for n in 1..=3 {
        let a = actual.entry(&small(n));
        let e = entries.get(&small(n));
        assert_eq!(a.is_some(), e.is_some(), "contract store {n}");
        if let (Some(a), Some(e)) = (a, e) {
            assert_eq!(a.dirty(), e["dirty"].as_bool().unwrap());
            let terms: BTreeMap<_, _> = rows(&e["terms"])
                .iter()
                .map(|t| (number(&t[0]), (value(&t[1]), t[2].as_bool().unwrap())))
                .collect();
            assert_eq!(a.terms(), &terms, "store terms {n}");
            assert_eq!(
                a.backend().0.0.read_cache(),
                &pairs(&e["reads"]),
                "read cache {n}"
            );
            assert_eq!(
                a.backend().0.0.write_cache(),
                &pairs(&e["writes"]),
                "write cache {n}"
            );
            assert_eq!(
                a.backend().0.0.cached_subtrees(),
                &rows(&e["subtrees"])
                    .iter()
                    .map(bytes)
                    .collect::<BTreeSet<_>>()
            );
        }
    }
}
fn assert_local(f: &LocalFrame, e: &J) {
    assert_eq!(f.function, value(&e[4]));
    assert_eq!(f.bindings, program_tests::bindings(&e[5]));
    assert_eq!(f.return_block, value(&e[6]));
    assert_eq!(
        f.stack.iter().cloned().collect::<Vec<_>>(),
        rows(&e[7]).iter().map(value).collect::<Vec<_>>()
    );
    assert_eq!(f.memory, memory(&e[8]));
    assert_eq!(f.call_value, number(&e[9]));
}
pub(super) fn assert_remote<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    actual: &ChainState<A, C>,
    expected: &J,
    pc: bool,
) {
    let m = &actual.program.machine;
    let c = &actual.program.cursor;
    let s = &actual.session;
    assert_eq!(m.accumulator.top, value(&expected["top"]), "top");
    assert_eq!(
        m.accumulator.rest.iter().cloned().collect::<Vec<_>>(),
        rows(&expected["rest"])
            .iter()
            .map(value)
            .collect::<Vec<_>>(),
        "rest"
    );
    assert_eq!(m.memory, memory(&expected["memory"]), "memory");
    assert_eq!(m.meter.gas, number(&expected["gas"]), "gas");
    assert_eq!(m.meter.created_cells, number(&expected["cells"]), "cells");
    assert_stores(&m.stores, &expected["stores"]);
    assert_chain(&s.host, &expected["chain"]);
    assert_eq!(c.function, value(&expected["function"]), "function");
    assert_eq!(c.block, value(&expected["block"]), "block");
    if pc {
        assert_eq!(
            c.instruction,
            expected["pc"].as_u64().unwrap() as usize,
            "pc"
        );
    }
    assert_eq!(c.returned, expected["returned"].as_bool().unwrap());
    assert_eq!(c.call_value, number(&expected["call_value"]));
    assert_eq!(c.bindings, program_tests::bindings(&expected["bindings"]));
    assert_eq!(s.current_contract, key(&expected["current"]));
    assert_eq!(s.vm_version as u64, expected["vm"].as_u64().unwrap());
    assert_eq!(s.caller, value(&expected["caller"]));
    assert_eq!(
        s.seen_contracts,
        rows(&expected["seen"]).iter().map(key).collect::<Vec<_>>()
    );
    assert_eq!(
        s.code_cache.keys().copied().collect::<Vec<_>>(),
        rows(&expected["code_cache"])
            .iter()
            .map(key)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        c.frames.len(),
        rows(&expected["frames"]).len(),
        "frame count"
    );
    if !expected["creator_cache"].is_null() {
        assert_eq!(
            s.creator_cache,
            rows(&expected["creator_cache"])
                .iter()
                .map(|p| (key(&p[0]), key(&p[1])))
                .collect()
        );
        let logs: Vec<_> = rows(&expected["logs"])
            .iter()
            .map(|l| remote::context::Log {
                contract: key(&l[0]),
                payload: bytes(&l[2]),
                topics: rows(&l[1])
                    .iter()
                    .map(|v| match value(v) {
                        Value::Object(_, p) => p,
                        _ => panic!(),
                    })
                    .collect(),
            })
            .collect();
        assert_eq!(s.logs, logs, "logs");
    }
    let (mut caller, mut contract, mut vm) = (s.caller.clone(), s.current_contract, s.vm_version);
    for (f, e) in c.frames.iter().rev().zip(rows(&expected["frames"])) {
        match f {
            Frame::Gas(n) => {
                assert_eq!(e[0], "gas");
                assert_eq!(*n, number(&e[1]));
            }
            Frame::Local(f) => {
                assert_eq!(e[0], "call");
                assert_eq!(caller, value(&e[1]));
                assert_eq!(contract, key(&e[2]));
                assert_eq!(vm as u64, e[3].as_u64().unwrap());
                assert_local(f, e);
            }
            Frame::Remote(f) => {
                assert_eq!(e[0], "call");
                assert_eq!(f.caller, value(&e[1]));
                assert_eq!(f.contract, key(&e[2]));
                assert_eq!(f.vm_version as u64, e[3].as_u64().unwrap());
                assert_local(&f.local, e);
                caller = f.caller.clone();
                contract = f.contract;
                vm = f.vm_version;
            }
            Frame::ReturnCheck(f) => {
                assert_eq!(e[0], "check");
                assert_eq!(f.bindings, program_tests::bindings(&e[1]));
                assert_eq!(f.protected, e[2].as_bool().unwrap());
                assert_eq!(f.returns, values::ty(&e[3]));
                let snap = s.snapshots.get(&f.checkpoint).unwrap();
                assert_stores(&snap.stores, &e[4]);
                assert_chain(&snap.host, &e[5]);
            }
        }
    }
}
pub(super) fn check<A: ae_trie::NodeWriter + Clone, C: ae_trie::NodeWriter + Clone>(
    out: remote::Outcome<RemoteStore, Chain<A, C>>,
    e: &J,
) -> Option<(ChainState<A, C>, Limits)> {
    match out {
        remote::Outcome::Continue {
            state,
            remaining_limits,
        } => {
            assert_eq!(e["kind"], "next");
            assert_remote(&state, &e["state"], true);
            Some((state, remaining_limits))
        }
        remote::Outcome::Returned { state, .. } => {
            assert_eq!(e["kind"], "returned");
            assert_remote(&state, &e["state"], true);
            None
        }
        remote::Outcome::Abort { state, reason, .. } => {
            assert_eq!(e["kind"], "abort");
            assert_reason(&reason, &e["reason"]);
            assert_remote(&state, &e["state"], false);
            None
        }
        remote::Outcome::Revert {
            state, value: v, ..
        } => {
            assert_eq!(e["kind"], "revert");
            assert_eq!(v, bytes(&e["value"]));
            assert_remote(&state, &e["state"], false);
            None
        }
        remote::Outcome::Exception(exception) => {
            assert_eq!(e["kind"], "exception");
            match exception {
                Exception::Native(s) => assert_eq!(s, string(&e["reason"])),
                Exception::Host { .. } => {}
                Exception::Exit(s) => {
                    assert_eq!(s, "op_not_implemented_yet");
                    assert_eq!(e["reason"], "exit_unimplemented");
                }
                Exception::DetachedValueType {
                    value: v,
                    expected: t,
                } => {
                    assert_eq!(e["reason"], "detached_value_type");
                    assert_eq!(v, value(&e["value"]));
                    assert_eq!(t, string(&e["expected"]));
                }
                e => panic!("unexpected {e:?}"),
            };
            None
        }
    }
}
#[test]
fn remote_programs_and_protected_recovery_match_original_instruction_and_whole_engine_traces() {
    assert_eq!(REMOTE["base"], FIXTURE["base"]);
    let order = otp_order::profile(&REMOTE["profile"]);
    let overrides = Overrides::default();
    let sources = BTreeMap::from([
        (small(1), RemoteStore(backend())),
        (small(2), RemoteStore(backend())),
        (small(3), RemoteStore(backend())),
    ]);
    for row in rows(&REMOTE["cases"]) {
        let mut context = context(row);
        context.current_contract = small(1);
        context.reserved_gas = number(&row["reserved"]);
        let executor = Executor {
            context: &context,
            order: &order,
            overrides: &overrides,
            contracts: &sources,
        };
        let code = remote::LoadedCode {
            code: Arc::new(program_tests::code(&row["codes"][0])),
            vm_version: context.vm_version,
        };
        for run in rows(&row["runs"]) {
            let label = format!(
                "case {} {} protocol {} vm {} callee {} gas {}",
                row["case_id"],
                row["name"],
                row["protocol"],
                row["vm"],
                row["callee_vm"],
                run["gas"]
            );
            let mut stores = Stores::new(Version::for_protocol(
                row["protocol"].as_u64().unwrap() as u32
            ));
            stores.put_contract(small(1), RemoteStore(backend()));
            let machine = State {
                accumulator: Accumulator {
                    top: Value::Void,
                    rest: Default::default(),
                },
                memory: BTreeMap::new(),
                stores,
                meter: Meter {
                    protocol: row["protocol"].as_u64().unwrap() as u32,
                    gas: number(&run["gas"]),
                    created_cells: number(&row["initial"]["cells"]),
                    metering_floor: None,
                },
            };
            let start = executor
                .start_remote_program(
                    code.clone(),
                    Value::Object(ObjectKind::Address, ObjectPayload::Bytes(small(9).to_vec())),
                    chain(row),
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
                mut remaining_limits,
            } = start
            else {
                panic!("entry failed")
            };
            for (index, step) in rows(&run["steps"]).iter().enumerate() {
                let e = &REMOTE["results"][step.as_u64().unwrap() as usize];
                let out = executor
                    .advance_remote_program(&state, remaining_limits)
                    .unwrap_or_else(|e| panic!("{label} step {index}: {e:?}"));
                let checked =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(out, e)))
                        .unwrap_or_else(|error| {
                            eprintln!("{label} step {index}");
                            std::panic::resume_unwind(error)
                        });
                if let Some((next, limits)) = checked {
                    state = next;
                    remaining_limits = limits;
                } else {
                    assert_eq!(index + 1, rows(&run["steps"]).len());
                    break;
                }
            }
        }
    }
}
