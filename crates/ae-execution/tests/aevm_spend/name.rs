use super::*;
use ae_state::names::Names;
use ae_trie::{Nibbles, NodeReader, NodeWriter, Value as TV};
use ae_vm_aevm::{
    execution::{self, Stop},
    heap::{Type, Value},
    maps::Store,
    opcode::Version,
    outer_call,
    primops::{self, ChainHost, HostResult},
    runtime::{self, StoreHost},
    runtime_heap::TypeRef,
};
use std::cell::RefCell;
static READS: LazyLock<J> = LazyLock::new(|| {
    let p: J =
        serde_json::from_str(include_str!("../fixtures/erlang_aevm_name_reads.json")).unwrap();
    expand(&p["root"], p["values"].as_array().unwrap())
});
static CALLS: LazyLock<J> = LazyLock::new(|| {
    let p: J =
        serde_json::from_str(include_str!("../fixtures/erlang_aevm_name_resolution.json")).unwrap();
    expand(&p["root"], p["values"].as_array().unwrap())
});
fn trie(v: &J) -> Trie<MemoryStore> {
    let mut t = Trie::new(MemoryStore::default());
    for pair in v[1].as_array().unwrap() {
        let a = &pair[1];
        t.put(&Nibbles::from_bytes(&bytes(&a[0])), TV::Bytes(bytes(&a[1])))
            .unwrap();
    }
    t
}
fn trees(snap: &J) -> Trees {
    let n = trie(field(snap, "names"));
    let c = trie(field(snap, "cache"));
    let t = Trees {
        accounts: Accounts::default(),
        contracts: Contracts::default(),
        calls: Default::default(),
        oracles: Oracles::default(),
        names: Names::from_roots(n.root_hash(), c.root_hash(), n.into_store(), c.into_store())
            .unwrap(),
        channels: Trie::new(MemoryStore::default()),
    };
    assert_eq!(bin(t.roots().hash().0), *field(snap, "root"));
    t
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
        event_context: EventContext::None,
        events: Arc::new(vec![]),
    }
}
fn pairs<S: NodeReader>(t: &Trie<S>) -> J {
    list(
        t.iter()
            .map(|r| {
                let (k, v) = r.unwrap();
                let TV::Bytes(v) = v else { panic!() };
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
fn snapshot<A: NodeReader, C: NodeReader>(t: &Trees<A, C>) -> J {
    map(vec![
        ("root", bin(t.roots().hash().0)),
        ("names", pairs(t.names.tree())),
        ("cache", pairs(t.names.cache())),
    ])
}
#[test]
fn name_resolution_matches_original_tree_modes_types_ids_and_native_failures() {
    let rows = READS["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 150);
    let mut counts = [0; 3];
    for row in rows {
        let s = state(row);
        let before = snapshot(&s.trees);
        let inner = s.onchain.as_ref().map(|t| snapshot(t));
        for read in row["reads"].as_array().unwrap() {
            let got = oracle::result(Host::default().resolve_name(
                &bytes(&row["name"]),
                b"account_pubkey",
                &oracle::ty(&read["type"]),
                &s,
                &mut Budget::new(Limits::default()),
            ));
            assert_eq!(got, read["result"], "{row}");
            counts[if got["kind"] == "native" {
                2
            } else if got["value"][1][0] == atom("error") {
                1
            } else {
                0
            }] += 1;
        }
        assert_eq!(snapshot(&s.trees), before);
        assert_eq!(s.onchain.as_ref().map(|t| snapshot(t)), inner);
    }
    assert_eq!(counts, [420, 140, 40]);
}
struct Traced<A = MemoryStore, C = MemoryStore> {
    host: Host<A, C>,
    events: RefCell<Vec<J>>,
}
impl<A, C> Default for Traced<A, C> {
    fn default() -> Self {
        Self {
            host: Host::default(),
            events: RefCell::new(vec![]),
        }
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> StoreHost for Traced<A, C> {
    type Chain = State<A, C>;
    type Error = aevm_chain::Error;
    fn set_store(&self, _: Store, _: &Self::Chain) -> Result<Self::Chain, Fault<Self::Error>> {
        panic!("eval_code only; no finalization")
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> ChainHost for Traced<A, C> {
    type Spend = ();
    type Oracle = ();
    fn resolve_name(
        &self,
        name: &[u8],
        key: &[u8],
        ty: &Type,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Value, Self::Error> {
        self.events.borrow_mut().push(tuple(vec![
            atom("resolve"),
            bin(name),
            bin(key),
            oracle::type_term(ty),
        ]));
        self.host.resolve_name(name, key, ty, c, b)
    }
    fn get_store(&self, _: &Self::Chain) -> Result<Store, Fault<Self::Error>> {
        panic!("name resolution only")
    }
    fn prepare_spend(
        &self,
        _: Id,
        _: &BigInt,
        _: &Self::Chain,
        _: &mut Budget,
    ) -> HostResult<(), Self::Error> {
        panic!("name resolution only")
    }
    fn apply_spend(
        &self,
        _: (),
        _: &Self::Chain,
        _: &mut Budget,
    ) -> HostResult<Self::Chain, Self::Error> {
        panic!("name resolution only")
    }
    fn address(
        &self,
        _: AddressKind,
        _: Word,
        _: &Self::Chain,
        _: &mut Budget,
    ) -> HostResult<Word, Self::Error> {
        panic!("name resolution only")
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> outer_call::Host for Traced<A, C> {
    fn function_types(
        &self,
        _: Word,
        _: Version,
        _: Word,
        _: &Self::Chain,
    ) -> Result<Option<(TypeRef, TypeRef)>, Self::Error> {
        panic!("local primitive only")
    }
    fn balance(&self, a: Word, c: &Self::Chain) -> Result<BigInt, Self::Error> {
        self.events
            .borrow_mut()
            .push(tuple(vec![atom("balance"), bin(a.0)]));
        c.reads()
            .balance(AccountPublicKey(a.0))
            .map(|n| unsigned(&n))
    }
    fn call(
        &self,
        _: outer_call::Request<'_>,
        _: &Self::Chain,
        _: &mut Budget,
    ) -> outer_call::CallResult<Self::Error, Self::Chain> {
        panic!("local primitive only")
    }
}
fn direct<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    row: &J,
    s: &State<A, C>,
    h: &Traced<A, C>,
    l: Limits,
) -> Result<(primops::Outcome, State<A, C>), Fault<aevm_chain::Error>> {
    primops::call(
        &invocations::vm(row, &row["before"]),
        &row["call_gas"].as_u64().unwrap().into(),
        &7.into(),
        &bytes(&row["data"]),
        s,
        h,
        l,
    )
}
fn expected(row: &J) -> primops::Outcome {
    let r = &row["result"];
    assert_eq!(r["kind"], "ok");
    let v = &r["value"][1];
    if v[0] == atom("ok") {
        assert_eq!(
            field(&v[3], "chain_state"),
            field(&row["before"], "chain_state")
        );
        return primops::Outcome::Complete {
            data: bytes(&v[1]),
            gas_spent: v[2][1].as_str().unwrap().parse().unwrap(),
            state: Box::new(invocations::vm(row, &v[3])),
        };
    }
    assert_eq!(v[0], atom("primop_error"));
    primops::Outcome::Error(if v[1][0] == "tuple" {
        let p = &v[1][1];
        assert_eq!(p[0], atom("out_of_gas"));
        let charge = &p[1][1];
        assert_eq!(
            charge[0],
            tuple(vec![atom("primop"), tuple(vec![atom("base"), int(200)])])
        );
        assert_eq!(charge[1], int(12000));
        assert_eq!(p[2], row["before"]);
        primops::Error::BaseGas {
            op: Word::from(200),
            required: 12000u16.into(),
            state: Box::new(invocations::vm(row, &p[2])),
        }
    } else {
        match v[1][1].as_str().unwrap() {
            "illegal_primop_call" => primops::Error::IllegalCall,
            e => primops::Error::Chain(e.into()),
        }
    })
}
#[test]
fn original_name_primitives_preserve_base_gas_authentication_and_callback_order() {
    let rows = CALLS["runs"].as_array().unwrap();
    let mut counts = [0; 2];
    let mut callbacks = 0;
    for (i, row) in rows.iter().enumerate() {
        let s = state(row);
        let before = snapshot(&s.trees);
        let inner = s.onchain.as_ref().map(|t| snapshot(t));
        let h = Traced::default();
        let (actual, next) = direct(row, &s, &h, Limits::default()).unwrap();
        assert_eq!(actual, expected(row), "row {i}");
        counts[usize::from(matches!(actual, primops::Outcome::Error(_)))] += 1;
        callbacks += h.events.borrow().len();
        assert_eq!(list(h.events.into_inner()), row["host_calls"], "row {i}");
        assert_eq!(snapshot(&next.trees), before);
        assert_eq!(next.onchain.as_ref().map(|t| snapshot(t)), inner);
        assert_eq!(snapshot(&s.trees), before);
        assert!(next.events.is_empty());
    }
    assert_eq!(rows.len(), 2790);
    assert_eq!(counts, [1080, 1710]);
    assert_eq!(callbacks, 1380);
}
#[test]
fn original_name_outer_calls_preserve_type_hashes_results_and_complete_vm_state() {
    let rows = CALLS["outer"].as_array().unwrap();
    let mut counts = [0; 4];
    let mut callbacks = 0;
    for (i, row) in rows.iter().enumerate() {
        let s = state(row);
        let before = snapshot(&s.trees);
        let inner = s.onchain.as_ref().map(|t| snapshot(t));
        let h = Traced::default();
        let original = invocations::vm(row, &row["before"]);
        let actual = outer_call::evaluate(&original, &s, &h, Limits::default());
        let r = &row["result"];
        if r["kind"] == "native" {
            assert_eq!(r["reason"], atom("function_clause"), "row {i}");
            assert!(
                matches!(
                    actual,
                    Err(outer_call::Fault::Runtime(Fault::Native(
                        Native::ChainException {
                            class: "error",
                            reason: "function_clause"
                        }
                    )))
                ),
                "row {i}: {actual:?}"
            );
            counts[3] += 1;
        } else {
            let e = &r["value"][1];
            match actual.unwrap() {
                runtime::Outcome::Complete { state, chain } => {
                    assert_eq!(e[0], atom("ok"), "row {i}");
                    assert_eq!(*state, invocations::vm(row, &e[1]), "row {i}");
                    assert_eq!(
                        snapshot(&chain.trees),
                        logical(field(&e[1], "chain_state").clone())
                    );
                    assert_eq!(chain.onchain.as_ref().map(|t| snapshot(t)), inner);
                    assert!(chain.events.is_empty());
                    counts[0] += 1;
                }
                runtime::Outcome::Error { reason, gas } => {
                    assert_eq!(e[0], atom("error"), "row {i}");
                    let er = if e[1] == atom("out_of_gas") {
                        execution::Failure::OutOfGas
                    } else if e[1] == atom("error_pop_empty_stack") {
                        execution::Failure::PopEmpty
                    } else {
                        panic!("row {i}: {e}")
                    };
                    assert_eq!(reason, er, "row {i}");
                    assert_eq!(int(gas), e[2]);
                    counts[1] += 1;
                }
                runtime::Outcome::Revert { output, gas } => {
                    assert_eq!(e[0], atom("revert"));
                    assert_eq!(bin(output), e[1]);
                    assert_eq!(int(gas), e[2]);
                    counts[2] += 1;
                }
            }
        }
        callbacks += h.events.borrow().len();
        assert_eq!(list(h.events.into_inner()), row["host_calls"], "row {i}");
        assert_eq!(snapshot(&s.trees), before);
        assert_eq!(s.onchain.as_ref().map(|t| snapshot(t)), inner);
        assert_eq!(original, invocations::vm(row, &row["before"]));
    }
    assert_eq!(rows.len(), 1720);
    assert_eq!(counts, [864, 840, 16, 0]);
    assert_eq!(callbacks, 2288);
}
fn success(group: &str) -> &'static J {
    CALLS[group]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["vm"] == 6
                && r["mode"] == "offchain"
                && r["case_name"] == "normal"
                && r["type"] == atom("string")
                && r["result"]["value"][1][0] == atom("ok")
        })
        .unwrap()
}
#[test]
fn name_resource_stops_preserve_input_state_and_allow_retry() {
    for group in ["runs", "outer"] {
        let row = success(group);
        let s = state(row);
        let before = snapshot(&s.trees);
        let inner = s.onchain.as_ref().map(|t| snapshot(t));
        let original = invocations::vm(row, &row["before"]);
        for dim in 0..if group == "runs" { 3 } else { 4 } {
            let mut stopped = false;
            let mut done = false;
            for n in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000] {
                let mut limits = Limits::default();
                match dim {
                    0 => limits.max_work = n,
                    1 => limits.max_bytes = n,
                    2 => limits.max_depth = n,
                    _ => limits.max_memory_words = n,
                };
                let h = Traced::default();
                let r = if group == "runs" {
                    direct(row, &s, &h, limits).map(|(r, next)| {
                        assert_eq!(r, expected(row));
                        assert_eq!(snapshot(&next.trees), before);
                        assert_eq!(next.onchain.as_ref().map(|t| snapshot(t)), inner);
                    })
                } else {
                    outer_call::evaluate(&original, &s, &h, limits)
                        .map_err(|e| match e {
                            outer_call::Fault::Runtime(f) => f,
                            _ => panic!("{e:?}"),
                        })
                        .map(|r| {
                            let runtime::Outcome::Complete { state, chain } = r else {
                                panic!("{r:?}")
                            };
                            assert_eq!(*state, invocations::vm(row, &row["result"]["value"][1][1]));
                            assert_eq!(snapshot(&chain.trees), before);
                            assert_eq!(chain.onchain.as_ref().map(|t| snapshot(t)), inner);
                        })
                };
                match r {
                    Err(Fault::Stop(Stop::ResourceLimit)) => stopped = true,
                    Ok(()) => {
                        assert_eq!(list(h.events.into_inner()), row["host_calls"]);
                        done = true;
                    }
                    Err(e) => panic!("{group}, dim {dim}, {n}: {e:?}"),
                }
                assert_eq!(snapshot(&s.trees), before);
                assert_eq!(s.onchain.as_ref().map(|t| snapshot(t)), inner);
                assert_eq!(original, invocations::vm(row, &row["before"]));
                if done {
                    break;
                }
            }
            assert!(stopped && done, "{group}, dim {dim}");
        }
        assert_eq!(
            direct(success("runs"), &s, &Traced::default(), Limits::default())
                .unwrap()
                .0,
            expected(success("runs"))
        );
    }
}
#[test]
fn name_backend_failures_escape_and_encoded_ids_bypass_the_name_tree() {
    use invocations::failures::{Control, wrapped};
    use std::sync::atomic::Ordering::Relaxed;
    let row = success("runs");
    let s = state(row);
    let a = Arc::new(Control::default());
    let top = Arc::new(Control::default());
    let inner = Arc::new(Control::default());
    let w = State {
        trees: wrapped(&s.trees, &a, &top),
        onchain: Some(Arc::new(wrapped(s.onchain.as_ref().unwrap(), &a, &inner))),
        account: s.account,
        vm_version: s.vm_version,
        network_id: s.network_id.clone(),
        context: s.context.clone(),
        event_context: s.event_context.clone(),
        events: s.events.clone(),
    };
    let root = w.trees.roots();
    let inner_root = w.onchain.as_ref().unwrap().roots();
    top.read_error.store(true, Relaxed);
    assert_eq!(
        direct(row, &w, &Traced::default(), Limits::default())
            .unwrap()
            .0,
        expected(row)
    );
    inner.read_error.store(true, Relaxed);
    assert!(matches!(
        direct(row, &w, &Traced::default(), Limits::default()),
        Err(Fault::Host(aevm_chain::Error::Names(_)))
    ));
    let o = success("outer");
    assert!(matches!(
        outer_call::evaluate(
            &invocations::vm(o, &o["before"]),
            &w,
            &Traced::default(),
            Limits::default()
        ),
        Err(outer_call::Fault::Runtime(Fault::Host(
            aevm_chain::Error::Names(_)
        )))
    ));
    for case in ["encoded", "encoded_contract", "encoded_bad_type"] {
        let r = READS["runs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["case_name"] == case)
            .unwrap();
        for read in r["reads"].as_array().unwrap() {
            let got = oracle::result(Host::default().resolve_name(
                &bytes(&r["name"]),
                b"ignored_key",
                &oracle::ty(&read["type"]),
                &w,
                &mut Budget::new(Limits::default()),
            ));
            assert_eq!(got, read["result"]);
        }
    }
    inner.read_error.store(false, Relaxed);
    assert_eq!(
        direct(row, &w, &Traced::default(), Limits::default())
            .unwrap()
            .0,
        expected(row)
    );
    assert_eq!(w.trees.roots(), root);
    assert_eq!(w.onchain.as_ref().unwrap().roots(), inner_root);
    assert_eq!(a.writes.load(Relaxed), 0);
    assert_eq!(top.writes.load(Relaxed), 0);
    assert_eq!(inner.writes.load(Relaxed), 0);
}
