use super::invocations::heap_support as support;
use super::*;
use ae_execution::contracts::{self, aevm};
use ae_trie::NodeWriter;
use ae_trie::{Nibbles, NodeReader, Value as TrieValue};
use ae_tx::{CanonicalTransaction, ContractTransaction};
use ae_vm_aevm::{
    gas::FloatProfile,
    opcode::Version as VM,
    outer_call::{self, Host as _, Request, Response},
    store::OrderProfile,
};
static PREPARATION: LazyLock<J> = LazyLock::new(|| {
    support::fixture(include_str!(
        "../fixtures/erlang_aevm_recursive_chain_preparation.json"
    ))
});
static VECTORS: LazyLock<J> = LazyLock::new(|| {
    support::fixture(include_str!(
        "../fixtures/erlang_aevm_recursive_chain_vectors.json"
    ))
});
fn expanded_config() -> aevm::Config {
    let mut c = config();
    c.environment.beneficiary = AccountPublicKey(Word::from(5).0);
    c.environment.difficulty = 7.into();
    c.environment.timestamp_ms = 12345.into();
    c
}
fn expanded_state(row: &J) -> State {
    let mut s = state(row);
    if row["mode"] == "offchain" {
        let on = VECTORS["runs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| {
                r["mode"] == "onchain"
                    && r["protocol"] == row["protocol"]
                    && r["case_name"] == row["case_name"]
                    && r["gas"] == row["gas"]
            })
            .unwrap();
        s.onchain = Some(Arc::new(trees(&on["before"])));
    }
    s
}
fn trie(t: &J) -> Trie<MemoryStore> {
    let mut out = Trie::new(MemoryStore::default());
    for pair in t[1].as_array().unwrap() {
        out.put(
            &Nibbles::from_bytes(&bytes(&pair[1][0])),
            TrieValue::Bytes(bytes(&pair[1][1])),
        )
        .unwrap();
    }
    out
}
fn trees(snapshot: &J) -> Trees {
    let a = trie(field(snapshot, "accounts"));
    let c = trie(field(snapshot, "contracts"));
    let calls = trie(field(snapshot, "calls"));
    let t = Trees {
        accounts: Accounts::from_root(a.root_hash(), a.into_store()).unwrap(),
        contracts: Contracts::from_root(c.root_hash(), c.into_store()).unwrap(),
        calls: ae_state::calls::Calls::from_root(calls.root_hash(), calls.into_store()).unwrap(),
        ..trees_from(&map(vec![("accounts", list(vec![]))]))
    };
    assert_eq!(bin(t.roots().hash().0), *field(snapshot, "root"));
    t
}
fn state(row: &J) -> State {
    let t = trees(&row["before"]);
    State {
        onchain: if row["mode"] == "offchain" {
            Some(Arc::new(t.clone()))
        } else {
            None
        },
        trees: t,
        account: AccountPublicKey(Word::from(9).0),
        vm_version: VM::from_id(row["vm"].as_u64().unwrap() as u32).unwrap(),
        network_id: Arc::from(b"ae_mainnet".as_slice()),
        context: Arc::new(context(row)),
        event_context: EventContext::None,
        events: Arc::new(vec![]),
    }
}
fn config() -> aevm::Config {
    aevm::Config {
        environment: aevm::Environment {
            beneficiary: AccountPublicKey::ZERO,
            difficulty: 0.into(),
            timestamp_ms: 0.into(),
            block_gas_limit: 6_000_000.into(),
            auth_tx_hash: None,
        },
        network_id: Arc::from(b"ae_mainnet".as_slice()),
        event_context: EventContext::None,
        float_profile: FloatProfile::Otp26Word64,
        store_order: OrderProfile::Otp26Word64Crc32c,
    }
}
fn pairs<S: NodeReader>(t: &Trie<S>) -> J {
    list(
        t.iter()
            .map(|p| {
                let (k, v) = p.unwrap();
                let TrieValue::Bytes(v) = v else { panic!() };
                let k: Vec<_> = k
                    .as_slice()
                    .chunks_exact(2)
                    .map(|p| p[0] * 16 + p[1])
                    .collect();
                tuple(vec![bin(k), bin(v)])
            })
            .collect(),
    )
}
fn snapshot<A: NodeReader, C: NodeReader>(s: &State<A, C>) -> J {
    let events = s
        .events
        .iter()
        .map(|e| {
            let (name, info) = match &e.event {
                Event::ContractDelta {
                    account,
                    amount,
                    debit,
                    info,
                } => (
                    tuple(vec![
                        atom("delta"),
                        tuple(vec![
                            bin(account.0),
                            int(if *debit {
                                -unsigned(amount)
                            } else {
                                unsigned(amount)
                            }),
                        ]),
                    ]),
                    info.as_bytes(),
                ),
                Event::Spend { from, to, amount } => (
                    tuple(vec![
                        atom("spend"),
                        tuple(vec![bin(from.0), bin(to.0), int(unsigned(amount))]),
                    ]),
                    b"Spend.amount".as_slice(),
                ),
                Event::FeeDebit { account, amount } => (
                    tuple(vec![
                        atom("delta"),
                        tuple(vec![bin(account.0), int(-unsigned(amount))]),
                    ]),
                    b"Chain.fee".as_slice(),
                ),
                e => panic!("{e:?}"),
            };
            let (kind, hash) = match &e.context {
                EventContext::None => (atom("non_tx"), bin([])),
                EventContext::Transaction { kind, hash } => {
                    assert_eq!(*kind, ae_tx::TransactionKind::ContractCall);
                    (atom("contract_call_tx"), bin(hash.0))
                }
            };
            tuple(vec![
                name,
                map(vec![("info", bin(info)), ("type", kind), ("tx_hash", hash)]),
            ])
        })
        .collect();
    map(vec![
        ("root", bin(s.trees.roots().hash().0)),
        ("accounts", pairs(s.trees.accounts.tree())),
        ("contracts", pairs(s.trees.contracts.tree())),
        ("calls", pairs(s.trees.calls.tree())),
        ("events", list(events)),
    ])
}
fn result<A: NodeReader, C: NodeReader>(
    r: Result<(Response, State<A, C>), aevm::Error<A, C>>,
) -> J {
    match r {
        Ok((r, s)) => {
            let (v, g, t) = match r {
                Response::Complete { data, gas_spent } => (bin(data), gas_spent, "ok"),
                Response::Revert { data, gas_spent } => (bin(data), gas_spent, "revert"),
                Response::Exception { reason, gas_spent } => {
                    (atom(&reason), gas_spent, "exception")
                }
            };
            json!({"kind":"ok","value":tuple(vec![tuple(vec![atom("call_result"),v,int(g),atom(t)]),snapshot(&s)])})
        }
        Err(outer_call::Fault::Runtime(Fault::Native(Native::BadMatchNone))) => {
            json!({"kind":"native","class":"error","reason":tuple(vec![atom("badmatch"),atom("none")])})
        }
        Err(_) => panic!("unexpected recursive call fault"),
    }
}
#[test]
fn real_aevm_recursive_chain_callback_matches_original_preparation() {
    let mut counts = [0; 4];
    for row in PREPARATION["runs"].as_array().unwrap() {
        let s = state(row);
        let before = snapshot(&s);
        let cfg = config();
        let h = aevm::Host::new(&cfg);
        let data = bytes(&row["data"]);
        let gas = BigUint::from(row["gas"].as_u64().unwrap());
        let value = BigInt::from(row["amount"].as_u64().unwrap());
        let stack: Vec<_> = row["call_stack"][1]
            .as_array()
            .unwrap()
            .iter()
            .map(support::number)
            .collect();
        let r = h.call(
            Request {
                target: Word::from(1),
                gas: &gas,
                value: &value,
                data: &data,
                call_stack: &stack,
                origin: Word::from(3),
            },
            &s,
            &mut Budget::new(Limits::default()),
        );
        let actual = result(r);
        assert_eq!(
            support::logical(actual.clone()),
            support::logical(row["result"].clone()),
            "profile={}/{} mode={} case={}",
            row["protocol"],
            row["vm"],
            row["mode"],
            row["case_name"]
        );
        counts[if actual["kind"] == "native" {
            3
        } else {
            match actual["value"][1][0][1][3][1].as_str().unwrap() {
                "ok" => 0,
                "revert" => 1,
                _ => 2,
            }
        }] += 1;
        assert_eq!(snapshot(&s), before);
    }
    assert_eq!(counts, [22, 16, 122, 16]);
}

fn invoke<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    row: &J,
    s: &State<A, C>,
    cfg: &aevm::Config,
    limits: Limits,
) -> Result<(Response, State<A, C>), aevm::Error<A, C>> {
    let data = bytes(&row["data"]);
    let gas = BigUint::from(row["gas"].as_u64().unwrap());
    let value = BigInt::from(row["amount"].as_u64().unwrap());
    let stack: Vec<_> = row["call_stack"][1]
        .as_array()
        .unwrap()
        .iter()
        .map(support::number)
        .collect();
    aevm::Host::new(cfg).call(
        Request {
            target: Word::from(1),
            gas: &gas,
            value: &value,
            data: &data,
            call_stack: &stack,
            origin: Word::from(3),
        },
        s,
        &mut Budget::new(limits),
    )
}
fn check_callback<A: NodeReader, C: NodeReader>(
    row: &J,
    out: Result<(Response, State<A, C>), aevm::Error<A, C>>,
) {
    assert_eq!(
        support::logical(result(out)),
        support::logical(row["result"].clone()),
        "profile={}/{} mode={} case={} gas={}",
        row["protocol"],
        row["vm"],
        row["mode"],
        row["case_name"],
        row["gas"]
    );
}
#[test]
fn real_aevm_recursive_callbacks_match_original_nested_effects_and_rollback() {
    let rows = VECTORS["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 1728);
    for row in rows {
        let s = expanded_state(row);
        let before = snapshot(&s);
        let inner = s.onchain.as_ref().map(|t| t.roots());
        let out = invoke(row, &s, &expanded_config(), Limits::default());
        if let Ok((_, next)) = &out {
            assert_eq!(next.onchain.as_ref().map(|t| t.roots()), inner);
            assert_eq!(next.account, s.account);
            assert_eq!(next.vm_version, s.vm_version);
        }
        check_callback(row, out);
        assert_eq!(snapshot(&s), before);
        assert_eq!(s.onchain.as_ref().map(|t| t.roots()), inner);
    }
}
fn receipt_events(r: contracts::Receipt) -> Vec<aevm_chain::EnvironmentEvent> {
    r.events
        .into_iter()
        .map(|e| match e {
            contracts::Event::Delta {
                account,
                amount,
                debit,
                info,
            } => aevm_chain::EnvironmentEvent {
                event: Event::ContractDelta {
                    account,
                    amount,
                    debit,
                    info,
                },
                context: EventContext::None,
            },
            contracts::Event::Aevm(e) => e,
            contracts::Event::Internal(_) => panic!("FATE event"),
        })
        .collect()
}
fn transaction_context(row: &J) -> Context {
    let mut c = context(row);
    c.origin = Origin::Transaction;
    c.dry_run = row["dry_run"].as_bool().unwrap();
    c
}
#[test]
fn real_aevm_outer_transactions_match_original_calls_logs_gas_and_trees() {
    let rows = VECTORS["transactions"].as_array().unwrap();
    assert_eq!(rows.len(), 576);
    let mut counts = [0, 0];
    for row in rows {
        let mut s = state(row);
        let before = snapshot(&s);
        let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let r = aevm::process_call(
            &tx,
            &mut s.trees,
            &transaction_context(row),
            &expanded_config(),
            Limits::default(),
        );
        let actual = match r {
            Ok(r) => {
                counts[0] += 1;
                s.events = Arc::new(receipt_events(r));
                tuple(vec![atom("ok"), snapshot(&s)])
            }
            Err(outer_call::Fault::Runtime(Fault::Host(aevm::HostError::Transaction(e)))) => {
                counts[1] += 1;
                let reason = match e {
                    contracts::Error::Rejected(s) => s,
                    contracts::Error::Primitive(ae_execution::Error::AccountNotFound) => {
                        "account_not_found"
                    }
                    contracts::Error::Primitive(ae_execution::Error::InsufficientFunds) => {
                        "insufficient_funds"
                    }
                    e => panic!("{e:?}"),
                };
                assert_eq!(snapshot(&s), before);
                tuple(vec![atom("error"), atom(reason)])
            }
            Err(e) => panic!("{e:?}"),
        };
        assert_eq!(
            support::logical(actual),
            support::logical(row["result"]["value"].clone()),
            "profile={}/{} case={} dry={}",
            row["protocol"],
            row["vm"],
            row["case_name"],
            row["dry_run"]
        );
    }
    assert_eq!(counts, [528, 48]);
}

mod failures;

mod create;
mod depth;
