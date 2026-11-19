use super::*;
use ae_trie::{Nibbles, NodeReader, NodeWriter, Value as TV};
use ae_vm_aevm::{
    execution::{self, Stop},
    heap::Value,
    maps::Store,
    opcode::Version as VM,
    outer_call,
    primops::{self, ChainHost, HostResult, OracleRequest, OracleWrite},
    runtime::{self, StoreHost},
};
use std::cell::{Cell, RefCell};
static DATA: LazyLock<J> = LazyLock::new(|| {
    let p: J =
        serde_json::from_str(include_str!("../fixtures/erlang_aevm_oracle_writes.json")).unwrap();
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
fn state(row: &J) -> State {
    let snap = field(&row["before"], "chain_state");
    let a = trie(field(snap, "accounts"));
    let o = trie(field(snap, "oracles"));
    let c = trie(field(snap, "cache"));
    let trees = Trees {
        accounts: Accounts::from_root(a.root_hash(), a.into_store()).unwrap(),
        oracles: Oracles::from_roots(o.root_hash(), c.root_hash(), o.into_store(), c.into_store())
            .unwrap(),
        contracts: Contracts::default(),
        names: Default::default(),
        calls: Default::default(),
        channels: Trie::new(MemoryStore::default()),
    };
    let s = State {
        onchain: if row["mode"] == "offchain" {
            Some(Arc::new(trees.clone()))
        } else {
            None
        },
        trees,
        account: AccountPublicKey(Word::from(1).0),
        vm_version: VM::from_id(row["vm"].as_u64().unwrap() as u32).unwrap(),
        network_id: Arc::from(b"ae_mainnet".as_slice()),
        context: Arc::new(context(row)),
        event_context: EventContext::None,
        events: Arc::new(vec![]),
    };
    assert_eq!(snapshot(&s), logical(snap.clone()));
    s
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
fn snapshot<A: NodeReader, C: NodeReader>(s: &State<A, C>) -> J {
    let events = s
        .events
        .iter()
        .map(|e| {
            assert_eq!(e.context, EventContext::None);
            let (k, n, info) = match &e.event {
                Event::FeeDebit { account, amount } => {
                    (account, -unsigned(amount), b"Chain.fee".as_slice())
                }
                Event::OracleQueryFee { account, amount } => {
                    (account, unsigned(amount), b"Oracle.queryfee".as_slice())
                }
                _ => panic!("{:?}", e.event),
            };
            tuple(vec![
                tuple(vec![atom("delta"), tuple(vec![bin(k.0), int(n)])]),
                map(vec![
                    ("info", bin(info)),
                    ("type", atom("non_tx")),
                    ("tx_hash", bin([])),
                ]),
            ])
        })
        .collect();
    map(vec![
        ("root", bin(s.trees.roots().hash().0)),
        ("accounts", pairs(s.trees.accounts.tree())),
        ("oracles", pairs(s.trees.oracles.tree())),
        ("cache", pairs(s.trees.oracles.cache())),
        ("events", list(events)),
    ])
}
struct Traced<A = MemoryStore, C = MemoryStore> {
    host: Host<A, C>,
    events: RefCell<Vec<J>>,
    fail: Cell<Option<&'static str>>,
}
impl<A, C> Default for Traced<A, C> {
    fn default() -> Self {
        Self {
            host: Host::default(),
            events: RefCell::new(vec![]),
            fail: Cell::new(None),
        }
    }
}
impl<A, C> Traced<A, C> {
    fn record(&self, v: J) {
        self.events.borrow_mut().push(v)
    }
    fn check(&self, stage: &'static str) -> Result<(), Fault<aevm_chain::Error>> {
        if self.fail.get() == Some(stage) {
            Err(Fault::Host(aevm_chain::Error::Primitive(Box::new(
                ae_execution::Error::BadTransaction,
            ))))
        } else {
            Ok(())
        }
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> StoreHost for Traced<A, C> {
    type Chain = State<A, C>;
    type Error = aevm_chain::Error;
    fn set_store(&self, v: Store, c: &Self::Chain) -> Result<Self::Chain, Fault<Self::Error>> {
        self.host.set_store(v, c)
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> ChainHost for Traced<A, C> {
    type Spend = ae_tx::Spend;
    type Oracle = ae_tx::OracleTransaction;
    type Name = ();
    fn get_store(&self, c: &Self::Chain) -> Result<Store, Fault<Self::Error>> {
        self.host.get_store(c)
    }
    fn prepare_spend(
        &self,
        r: Id,
        v: &BigInt,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Self::Spend, Self::Error> {
        self.host.prepare_spend(r, v, c, b)
    }
    fn apply_spend(
        &self,
        t: Self::Spend,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Self::Chain, Self::Error> {
        self.host.apply_spend(t, c, b)
    }
    fn address(
        &self,
        k: AddressKind,
        w: Word,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Word, Self::Error> {
        self.host.address(k, w, c, b)
    }
    fn oracle(
        &self,
        r: OracleRequest<'_>,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Value, Self::Error> {
        self.record(match r {
            OracleRequest::QueryFormat(k) => tuple(vec![atom("query_format"), bin(k.0)]),
            OracleRequest::ResponseFormat(k) => tuple(vec![atom("response_format"), bin(k.0)]),
            OracleRequest::ResponseTtl(k, q) => {
                tuple(vec![atom("response_ttl"), bin(k.0), bin(q.0)])
            }
            _ => panic!("unused read"),
        });
        self.check("read")?;
        self.host.oracle(r, c, b)
    }
    fn oracle_height(
        &self,
        op: Word,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> Result<BigUint, Fault<Self::Error>> {
        self.record(atom("height"));
        self.check("height")?;
        self.host.oracle_height(op, c, b)
    }
    fn prepare_oracle(
        &self,
        r: OracleWrite<'_>,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<(Self::Oracle, usize), Self::Error> {
        self.record(tuple(vec![atom("prepare"), int(r.operation().unsigned())]));
        self.check("prepare")?;
        let x = self.host.prepare_oracle(r, c, b)?;
        if let Ok((t, _)) = &x {
            self.record(tuple(vec![atom("transaction"), bin(t.encode())]))
        }
        Ok(x)
    }
    fn apply_oracle(
        &self,
        op: Word,
        t: Self::Oracle,
        sig: Option<[u8; 64]>,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<(Value, Self::Chain), Self::Error> {
        self.record(tuple(vec![
            atom("apply"),
            int(op.unsigned()),
            sig.map_or_else(|| atom("undefined"), bin),
        ]));
        self.check("apply")?;
        self.host.apply_oracle(op, t, sig, c, b)
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> outer_call::Host for Traced<A, C> {
    fn function_types(
        &self,
        _: Word,
        _: ae_vm_aevm::calls::ContractVersion,
        _: Word,
        _: &Self::Chain,
        _budget: &mut ae_vm_aevm::execution::Budget,
    ) -> ae_vm_aevm::calls::FunctionTypes<Self::Error> {
        panic!("no remote calls")
    }
    fn balance(&self, k: Word, c: &Self::Chain) -> Result<BigInt, Self::Error> {
        self.record(tuple(vec![atom("balance"), bin(k.0)]));
        Ok(unsigned(&c.reads().balance(AccountPublicKey(k.0))?))
    }
    fn call(
        &self,
        _: outer_call::Request<'_>,
        _: &Self::Chain,
        _: &mut Budget,
    ) -> outer_call::CallResult<Self::Error, Self::Chain> {
        panic!("no remote calls")
    }
}
fn direct<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    row: &J,
    s: &State<A, C>,
    h: &Traced<A, C>,
    limits: Limits,
) -> Result<(primops::Outcome, State<A, C>), Fault<aevm_chain::Error>> {
    primops::call(
        &invocations::vm(row, &row["before"]),
        &row["call_gas"].as_u64().unwrap().into(),
        &row["value"].as_i64().unwrap().into(),
        &bytes(&row["data"]),
        s,
        h,
        limits,
    )
}
fn expected(row: &J) -> primops::Outcome {
    let r = &row["result"];
    assert_eq!(r["kind"], "ok");
    let v = &r["value"][1];
    if v[0] == atom("ok") {
        return primops::Outcome::Complete {
            data: bytes(&v[1]),
            gas_spent: v[2][1].as_str().unwrap().parse().unwrap(),
            state: Box::new(invocations::vm(row, &v[3])),
        };
    }
    assert_eq!(v[0], atom("primop_error"));
    let err = if v[1][0] == "tuple" {
        let p = &v[1][1];
        assert_eq!(p[0], atom("out_of_gas"));
        let charge = &p[1][1];
        let resource = &charge[0][1];
        assert_eq!(resource[0], atom("primop"));
        let r = &resource[1][1];
        let op = Word::from(row["op"].as_u64().unwrap());
        assert_eq!(r[1], int(op.unsigned()));
        let required = charge[1][1].as_str().unwrap().parse().unwrap();
        let state = Box::new(invocations::vm(row, &p[2]));
        assert_eq!(*state, invocations::vm(row, &row["before"]));
        assert_eq!(
            field(&p[2], "chain_state"),
            field(&row["before"], "chain_state")
        );
        if r[0] == atom("base") {
            primops::Error::BaseGas {
                op,
                required,
                state,
            }
        } else {
            assert_eq!(r[0], atom("dyn"));
            primops::Error::DynamicGas {
                op,
                required,
                state,
            }
        }
    } else {
        match v[1][1].as_str().unwrap() {
            "illegal_primop_call" => primops::Error::IllegalCall,
            "out_of_gas" => primops::Error::OutOfGas,
            e => primops::Error::Chain(e.into()),
        }
    };
    primops::Outcome::Error(err)
}
#[test]
fn original_oracle_writes_match_transactions_delegations_gas_events_and_trees() {
    let rows = DATA["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 4816);
    let boundaries = DATA["boundaries"].as_array().unwrap();
    assert_eq!(boundaries.len(), 318);
    let mut counts = [0; 2];
    for (i, row) in rows.iter().chain(boundaries).enumerate() {
        let s = state(row);
        let before = snapshot(&s);
        let inner = s.onchain.as_ref().map(|t| t.roots());
        let h = Traced::default();
        let (actual, next) = direct(row, &s, &h, Limits::default()).unwrap();
        assert_eq!(actual, expected(row), "row {i}: {row}");
        let desired = if matches!(actual, primops::Outcome::Complete { .. }) {
            counts[0] += 1;
            field(&row["result"]["value"][1][3], "chain_state")
        } else {
            counts[1] += 1;
            field(&row["before"], "chain_state")
        };
        assert_eq!(snapshot(&next), logical(desired.clone()), "row {i}");
        assert_eq!(list(h.events.into_inner()), row["host_calls"], "row {i}");
        assert_eq!(snapshot(&s), before);
        assert_eq!(next.onchain.as_ref().map(|t| t.roots()), inner);
    }
    assert_eq!(counts, [855, 4279]);
}
#[test]
fn original_outer_oracle_writes_preserve_call_failure_mapping_and_complete_state() {
    let rows = DATA["outer"].as_array().unwrap();
    assert_eq!(rows.len(), 960);
    let mut counts = [0; 3];
    for (i, row) in rows.iter().enumerate() {
        let s = state(row);
        let before = snapshot(&s);
        let vm = invocations::vm(row, &row["before"]);
        let h = Traced::default();
        let r = outer_call::evaluate(&vm, &s, &h, Limits::default()).unwrap();
        let e = &row["result"]["value"][1];
        assert_eq!(row["result"]["kind"], "ok");
        match r {
            runtime::Outcome::Complete { state, chain } => {
                counts[0] += 1;
                assert_eq!(e[0], atom("ok"));
                assert_eq!(*state, invocations::vm(row, &e[1]), "row {i}");
                assert_eq!(
                    snapshot(&chain),
                    logical(field(&e[1], "chain_state").clone()),
                    "row {i}"
                );
            }
            runtime::Outcome::Error { reason, gas } => {
                counts[1] += 1;
                assert_eq!(e[0], atom("error"));
                let expected = if e[1] == atom("out_of_gas") {
                    execution::Failure::OutOfGas
                } else if e[1] == atom("error_pop_empty_stack") {
                    execution::Failure::PopEmpty
                } else {
                    execution::Failure::Call(e[1][1].as_str().unwrap().into())
                };
                assert_eq!(reason, expected, "row {i}");
                assert_eq!(int(gas), e[2]);
            }
            runtime::Outcome::Revert { output, gas } => {
                counts[2] += 1;
                assert_eq!(e[0], atom("revert"));
                assert_eq!(bin(output), e[1]);
                assert_eq!(int(gas), e[2]);
            }
        }
        assert_eq!(list(h.events.into_inner()), row["host_calls"], "row {i}");
        assert_eq!(snapshot(&s), before);
    }
    assert_eq!(counts, [88, 856, 16]);
}

fn successes() -> impl Iterator<Item = &'static J> {
    DATA["runs"].as_array().unwrap().iter().filter(|r| {
        r["vm"] == 6
            && r["protocol"] == 4
            && r["mode"] == "onchain"
            && r["case_name"] == "tuple"
            && r["call_gas"] == 100000
            && r["result"]["value"][1][0] == atom("ok")
    })
}
#[test]
fn original_oracle_lifecycles_preserve_nonce_query_ids_rewards_and_repeated_writes() {
    let rows = DATA["sequences"].as_array().unwrap();
    assert_eq!(rows.len(), 21);
    let mut counts = [0; 2];
    for sequence in rows {
        let steps = sequence["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 6);
        let mut s = state(&steps[0]);
        for row in steps {
            let before = snapshot(&s);
            assert_eq!(
                before,
                logical(field(&row["before"], "chain_state").clone())
            );
            let h = Traced::default();
            let (v, next) = direct(row, &s, &h, Limits::default()).unwrap();
            assert_eq!(v, expected(row));
            let wanted = if matches!(v, primops::Outcome::Complete { .. }) {
                counts[0] += 1;
                field(&row["result"]["value"][1][3], "chain_state")
            } else {
                counts[1] += 1;
                field(&row["before"], "chain_state")
            };
            assert_eq!(snapshot(&next), logical(wanted.clone()));
            assert_eq!(snapshot(&s), before);
            assert_eq!(list(h.events.into_inner()), row["host_calls"]);
            s = next;
        }
    }
    assert_eq!(counts, [102, 24]);
}
#[test]
fn oracle_write_limits_and_callback_faults_preserve_inputs_and_retry() {
    let mut count = 0;
    for row in successes() {
        count += 1;
        let s = state(row);
        let before = snapshot(&s);
        let desired = logical(field(&row["result"]["value"][1][3], "chain_state").clone());
        let expected = expected(row);
        for dim in 0..3 {
            let mut stopped = false;
            let mut reached = false;
            for n in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000] {
                let mut limits = Limits::default();
                match dim {
                    0 => limits.max_work = n,
                    1 => limits.max_bytes = n,
                    _ => limits.max_depth = n,
                }
                let h = Traced::default();
                match direct(row, &s, &h, limits) {
                    Err(Fault::Stop(Stop::ResourceLimit)) => stopped = true,
                    Ok((v, next)) => {
                        assert_eq!(v, expected);
                        assert_eq!(snapshot(&next), desired);
                        reached = true;
                        break;
                    }
                    Err(e) => panic!("{e:?}"),
                };
                assert_eq!(snapshot(&s), before);
            }
            assert!(stopped && reached);
        }
        for stage in ["height", "prepare", "apply", "read"] {
            if stage == "read" && (row["op"] == 100 || row["op"] == 103) {
                continue;
            }
            let h = Traced::default();
            h.fail.set(Some(stage));
            assert!(matches!(
                direct(row, &s, &h, Limits::default()),
                Err(Fault::Host(_))
            ));
            assert_eq!(snapshot(&s), before);
            h.fail.set(None);
            let (v, next) = direct(row, &s, &h, Limits::default()).unwrap();
            assert_eq!(v, expected);
            assert_eq!(snapshot(&next), desired);
            assert_eq!(snapshot(&s), before);
        }
    }
    assert_eq!(count, 4);
}
#[test]
fn oracle_account_consensus_and_expiry_write_faults_leave_all_roots_retryable() {
    use invocations::failures::{Control, wrapped};
    use std::sync::atomic::Ordering::Relaxed;
    let mut count = 0;
    for row in successes() {
        count += 1;
        let input = state(row);
        let a = Arc::new(Control::default());
        let c = Arc::new(Control::default());
        let s = State {
            trees: wrapped(&input.trees, &a, &c),
            onchain: None,
            account: input.account,
            vm_version: input.vm_version,
            network_id: input.network_id,
            context: input.context,
            event_context: input.event_context,
            events: input.events,
        };
        let before = snapshot(&s);
        let h = Traced::default();
        let (_, next) = direct(row, &s, &h, Limits::default()).unwrap();
        let desired = snapshot(&next);
        let writes = [a.writes.load(Relaxed), c.writes.load(Relaxed)];
        assert!(writes.iter().all(|n| *n > 0));
        for (control, n) in [(&a, writes[0]), (&c, writes[1])] {
            for allowed in 0..=n {
                control.remaining.store(allowed, Relaxed);
                match direct(row, &s, &h, Limits::default()) {
                    Err(Fault::Host(_)) => assert!(allowed < n),
                    Ok((v, next)) => {
                        assert_eq!(allowed, n);
                        assert_eq!(v, expected(row));
                        assert_eq!(snapshot(&next), desired)
                    }
                    Err(e) => panic!("{e:?}"),
                };
                control.remaining.store(usize::MAX, Relaxed);
                assert_eq!(snapshot(&s), before);
                let (v, next) = direct(row, &s, &h, Limits::default()).unwrap();
                assert_eq!(v, expected(row));
                assert_eq!(snapshot(&next), desired);
            }
        }
        a.read_error.store(true, Relaxed);
        assert!(matches!(
            direct(row, &s, &h, Limits::default()),
            Err(Fault::Host(_))
        ));
        a.read_error.store(false, Relaxed);
        assert_eq!(snapshot(&s), before);
    }
    assert_eq!(count, 4);
}
