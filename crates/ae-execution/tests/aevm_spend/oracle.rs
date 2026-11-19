use super::*;
use ae_trie::{Nibbles, Value as TrieValue};
use ae_vm_aevm::{
    execution::Stop,
    heap::{Type, Value},
    opcode::Version,
    primops::{ChainHost, OracleRequest},
};
static DATA: LazyLock<J> = LazyLock::new(|| {
    let p: J =
        serde_json::from_str(include_str!("../fixtures/erlang_aevm_oracle_reads.json")).unwrap();
    expand(&p["root"], p["values"].as_array().unwrap())
});
static CALLS: LazyLock<J> = LazyLock::new(|| {
    let p: J =
        serde_json::from_str(include_str!("../fixtures/erlang_aevm_oracle_calls.json")).unwrap();
    expand(&p["root"], p["values"].as_array().unwrap())
});
fn trees(snapshot: &J) -> Trees {
    let make = |entries: &J| {
        let mut t = Trie::new(MemoryStore::default());
        for e in entries.as_array().unwrap() {
            t.put(
                &Nibbles::from_bytes(&bytes(&e["key"])),
                TrieValue::Bytes(bytes(&e["encoded"])),
            )
            .unwrap();
        }
        t
    };
    let t = make(&snapshot["oracles"]);
    let c = make(&snapshot["cache"]);
    let trees = Trees {
        accounts: Accounts::default(),
        contracts: Contracts::default(),
        calls: Default::default(),
        names: Default::default(),
        channels: Default::default(),
        oracles: Oracles::from_roots(t.root_hash(), c.root_hash(), t.into_store(), c.into_store())
            .unwrap(),
    };
    assert_eq!(bin(trees.roots().hash().0), snapshot["root"]);
    trees
}
fn state(row: &J) -> State {
    let on = trees(&row["onchain"]);
    let off = trees(&row["offchain"]);
    let (trees, onchain) = if row["mode"] == "offchain" {
        (off, Some(Arc::new(on)))
    } else {
        (on, None)
    };
    State {
        trees,
        onchain,
        account: AccountPublicKey(Word::from(1).0),
        vm_version: Version::from_id(row["vm"].as_u64().unwrap() as u32).unwrap(),
        network_id: Arc::from(b"ae_mainnet".as_slice()),
        context: Arc::new(context(row)),
        events: Arc::new(vec![]),
        event_context: EventContext::None,
    }
}
pub(super) fn ty(t: &J) -> Type {
    if t[0] == "atom" {
        return match t[1].as_str().unwrap() {
            "word" => Type::Word,
            "bool" => Type::Bool,
            "string" => Type::String,
            _ => panic!("{t}"),
        };
    }
    let a = &t[1];
    match a[0][1].as_str().unwrap() {
        "tuple" => Type::Tuple(a[1][1].as_array().unwrap().iter().map(ty).collect()),
        "list" => Type::List(Box::new(ty(&a[1]))),
        _ => panic!("{t}"),
    }
}
pub(super) fn type_term(t: &Type) -> J {
    match t {
        Type::Word => atom("word"),
        Type::Bool => atom("bool"),
        Type::String => atom("string"),
        Type::TypeRep => atom("typerep"),
        Type::Tuple(ts) => tuple(vec![
            atom("tuple"),
            list(ts.iter().map(type_term).collect()),
        ]),
        Type::List(t) => tuple(vec![atom("list"), type_term(t)]),
        Type::Variant(cs) => tuple(vec![
            atom("variant"),
            list(
                cs.iter()
                    .map(|ts| list(ts.iter().map(type_term).collect()))
                    .collect(),
            ),
        ]),
        Type::Map(k, v) => tuple(vec![atom("map"), type_term(k), type_term(v)]),
        _ => panic!("{t:?}"),
    }
}
pub(super) fn term(v: &Value) -> J {
    match v {
        Value::Integer(n) => int(n),
        Value::String(s) => bin(s),
        Value::Type(t) => type_term(t),
        Value::Tuple(vs) => tuple(vs.iter().map(term).collect()),
        Value::List(vs) => list(vs.iter().map(term).collect()),
        Value::Option(None) => atom("none"),
        Value::Option(Some(v)) => tuple(vec![atom("some"), term(v)]),
        Value::Variant { tag: 0, values } if values.len() == 1 => {
            tuple(vec![atom("delta"), term(&values[0])])
        }
        _ => panic!("{v:?}"),
    }
}
fn call(
    row: &J,
    s: &State,
    limits: Limits,
) -> ae_vm_aevm::primops::HostResult<Value, aevm_chain::Error> {
    let a = row["args"][1].as_array().unwrap();
    let k = |i| Word(bytes(&a[i]).try_into().unwrap());
    let types: Vec<_> = a.iter().filter(|a| a[0] != "binary").map(ty).collect();
    let request = match row["op"].as_str().unwrap() {
        "query_format" => OracleRequest::QueryFormat(k(0)),
        "response_format" => OracleRequest::ResponseFormat(k(0)),
        "query_fee" => OracleRequest::QueryFee(k(0)),
        "get_question" => OracleRequest::Question(k(0), k(1)),
        "get_answer" => OracleRequest::Answer(k(0), k(1)),
        "query_response_ttl" => OracleRequest::ResponseTtl(k(0), k(1)),
        "check" | "check_wrong" => OracleRequest::Check(k(0), &types[0], &types[1]),
        "check_query" => OracleRequest::CheckQuery(k(0), k(1), &types[0], &types[1]),
        _ => panic!("{row}"),
    };
    Host::default().oracle(request, s, &mut Budget::new(limits))
}
pub(super) fn result(r: ae_vm_aevm::primops::HostResult<Value, aevm_chain::Error>) -> J {
    match r {
        Ok(Ok(v)) => json!({"kind":"ok","value":tuple(vec![atom("ok"),term(&v)])}),
        Ok(Err(e)) => json!({"kind":"ok","value":tuple(vec![atom("error"),atom(&e)])}),
        Err(Fault::Native(n)) => {
            let reason = match n {
                Native::BadMatchNone => tuple(vec![atom("badmatch"), atom("none")]),
                Native::CaseClauseNone => tuple(vec![atom("case_clause"), atom("none")]),
                Native::BadMatchChainError(e) => {
                    tuple(vec![atom("badmatch"), tuple(vec![atom("error"), atom(&e)])])
                }
                Native::ChainException {
                    class: "error",
                    reason,
                } => atom(reason),
                _ => panic!("{n:?}"),
            };
            json!({"kind":"native","class":"error","reason":reason})
        }
        Err(e) => panic!("{e:?}"),
    }
}
#[test]
fn oracle_reads_match_original_formats_values_missing_records_and_native_failures() {
    let rows = DATA["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 180);
    let mut counts = [0; 3];
    for row in rows {
        let s = state(row);
        let before = s.trees.roots();
        let inner = s.onchain.as_ref().map(|t| t.roots());
        assert_eq!(bin(before.hash().0), row["visible_root"]);
        for read in row["reads"].as_array().unwrap() {
            let r = result(call(read, &s, Limits::default()));
            assert_eq!(r, read["result"], "{row}");
            let n = if r["kind"] == "native" {
                2
            } else if r["value"][1][0] == atom("error") {
                1
            } else {
                0
            };
            counts[n] += 1;
            assert_eq!(s.trees.roots(), before);
            assert_eq!(s.onchain.as_ref().map(|t| t.roots()), inner);
        }
    }
    assert_eq!(counts, [1228, 224, 168]);
}
#[test]
fn oracle_decoding_limits_are_operational_and_do_not_become_bad_formats_or_answers() {
    let row = DATA["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["vm"] == 6 && r["mode"] == "offchain" && r["case_name"] == "typed_tuple")
        .unwrap();
    let s = state(row);
    for read in row["reads"].as_array().unwrap().iter().filter(|r| {
        ["query_format", "get_answer", "get_question"].contains(&r["op"].as_str().unwrap())
    }) {
        for dim in 0..3 {
            let mut stopped = false;
            let mut done = false;
            for n in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536] {
                let mut l = Limits::default();
                match dim {
                    0 => l.max_work = n,
                    1 => l.max_bytes = n,
                    _ => l.max_depth = n,
                };
                match call(read, &s, l) {
                    Err(Fault::Stop(Stop::ResourceLimit)) => stopped = true,
                    r => {
                        assert_eq!(result(r), read["result"]);
                        done = true;
                        break;
                    }
                }
            }
            assert!(stopped && done);
            assert_eq!(result(call(read, &s, Limits::default())), read["result"]);
        }
    }
}

#[test]
fn real_oracle_outer_calls_match_original_and_preserve_all_tree_roots() {
    use ae_vm_aevm::{execution::Failure, outer_call, runtime::Outcome};
    let rows = CALLS["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 1320);
    let h = super::invocations::VmHost(Host::default());
    let mut counts = [0; 3];
    for row in rows {
        let s = state(row);
        let before = s.trees.roots();
        let inner = s.onchain.as_ref().map(|t| t.roots());
        let original = super::invocations::vm(row, &row["before"]);
        let actual = outer_call::evaluate(&original, &s, &h, Limits::default());
        let r = &row["result"];
        if r["kind"] == "native" {
            assert_eq!(r["reason"], atom("function_clause"));
            assert!(matches!(
                actual,
                Err(outer_call::Fault::Runtime(Fault::Native(
                    Native::ChainException {
                        class: "error",
                        reason: "function_clause"
                    }
                )))
            ));
            counts[2] += 1;
        } else {
            let expected = &r["value"][1];
            match actual.unwrap() {
                Outcome::Complete { state, chain } => {
                    assert_eq!(expected[0], atom("ok"));
                    assert_eq!(*state, super::invocations::vm(row, &expected[1]));
                    assert_eq!(chain.trees.roots(), before);
                    assert_eq!(chain.onchain.as_ref().map(|t| t.roots()), inner);
                    assert_eq!(
                        field(&expected[1], "chain_state"),
                        field(&row["before"], "chain_state")
                    );
                    counts[0] += 1;
                }
                Outcome::Error { reason, gas } => {
                    assert_eq!(reason, Failure::OutOfGas);
                    assert_eq!(expected[0], atom("error"));
                    assert_eq!(expected[1], atom("out_of_gas"));
                    assert_eq!(int(gas), expected[2]);
                    counts[1] += 1;
                }
                Outcome::Revert { .. } => panic!("unexpected revert"),
            }
        }
        assert_eq!(s.trees.roots(), before);
        assert_eq!(s.onchain.as_ref().map(|t| t.roots()), inner);
        assert_eq!(original, super::invocations::vm(row, &row["before"]));
    }
    assert!(counts.iter().all(|n| *n > 0));
    assert_eq!(counts.iter().sum::<usize>(), 1320);
}

#[test]
fn oracle_backend_failures_use_the_onchain_tree_and_never_become_missing_answers() {
    use ae_trie::NodeReader;
    use ae_types::Hash256;
    use std::{cell::Cell, rc::Rc};
    #[derive(Clone)]
    struct Backend(MemoryStore, Rc<Cell<bool>>);
    impl NodeReader for Backend {
        fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            if self.1.get() {
                Err(ae_trie::Error::Backend("offline".into()))
            } else {
                self.0.node(h)
            }
        }
    }
    let row = DATA["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["vm"] == 6 && r["mode"] == "offchain" && r["case_name"] == "typed_tuple")
        .unwrap();
    let s = state(row);
    let on = s.onchain.as_ref().unwrap();
    let inner = Rc::new(Cell::new(false));
    let outer = Rc::new(Cell::new(false));
    let wrap = |t: &Trees, flag: &Rc<Cell<bool>>| {
        let c = Contracts::from_root(
            t.contracts.root_hash(),
            Backend(t.contracts.tree().store().clone(), flag.clone()),
        )
        .unwrap();
        let o = Oracles::from_roots(
            t.oracles.root_hash(),
            t.oracles.cache_root_hash(),
            Backend(t.oracles.tree().store().clone(), flag.clone()),
            Backend(t.oracles.cache().store().clone(), flag.clone()),
        )
        .unwrap();
        (c, o)
    };
    let top = wrap(&s.trees, &outer);
    let inn = wrap(on, &inner);
    let reads = Reads {
        top: View {
            accounts: &s.trees.accounts,
            contracts: &top.0,
            oracles: &top.1,
        },
        onchain: Some(View {
            accounts: &on.accounts,
            contracts: &inn.0,
            oracles: &inn.1,
        }),
    };
    let request = OracleRequest::Answer(Word::from(5), Word::from(11));
    let expected = reads.oracle(
        request,
        Version::Sophia4,
        &mut Budget::new(Limits::default()),
    );
    assert!(matches!(expected, Ok(Ok(Value::Option(Some(_))))));
    outer.set(true);
    assert_eq!(
        reads.oracle(
            request,
            Version::Sophia4,
            &mut Budget::new(Limits::default())
        ),
        expected
    );
    inner.set(true);
    assert!(matches!(
        reads.oracle(
            request,
            Version::Sophia4,
            &mut Budget::new(Limits::default())
        ),
        Err(Fault::Host(aevm_chain::Error::OracleState(_)))
    ));
    inner.set(false);
    assert_eq!(
        reads.oracle(
            request,
            Version::Sophia4,
            &mut Budget::new(Limits::default())
        ),
        expected
    );
}
