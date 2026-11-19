use super::*;
use ae_trie::{Nibbles, NodeReader, NodeWriter, Value as TV};
use ae_vm_aevm::{
    execution::{self, Stop},
    heap::Value,
    maps::Store,
    opcode::Version as VM,
    outer_call,
    primops::{self, ChainHost, HostResult, NameWrite},
    runtime::{self, StoreHost},
    runtime_heap::TypeRef,
};
use std::cell::{Cell, RefCell};
static PREPARATION: LazyLock<J> = LazyLock::new(|| {
    let p: J = serde_json::from_str(include_str!(
        "../fixtures/erlang_aevm_name_write_preparation.json"
    ))
    .unwrap();
    expand(&p["root"], p["values"].as_array().unwrap())
});
static DATA: LazyLock<J> = LazyLock::new(|| {
    let p: J =
        serde_json::from_str(include_str!("../fixtures/erlang_aevm_name_writes.json")).unwrap();
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
    let snap = if row["op"].is_string() {
        &row["before"]
    } else {
        field(&row["before"], "chain_state")
    };
    let a = trie(field(snap, "accounts"));
    let o = trie(field(snap, "names"));
    let c = trie(field(snap, "cache"));
    let trees = Trees {
        accounts: Accounts::from_root(a.root_hash(), a.into_store()).unwrap(),
        names: ae_state::names::Names::from_roots(
            o.root_hash(),
            c.root_hash(),
            o.into_store(),
            c.into_store(),
        )
        .unwrap(),
        contracts: Contracts::default(),
        oracles: Default::default(),
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
        context: Arc::new({
            let mut c = context(row);
            c.height = row["height"].as_u64().unwrap_or(50);
            c
        }),
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
                    (*account, -unsigned(amount), b"Chain.fee".as_slice())
                }
                Event::NameDebit { account, amount } => {
                    (*account, -unsigned(amount), b"Name.lock".as_slice())
                }
                Event::NameRefund { account, amount } => {
                    (*account, unsigned(amount), b"Name.refund".as_slice())
                }
                Event::NameFeeLocked { amount } => (
                    AccountPublicKey::ZERO,
                    unsigned(amount),
                    b"Chain.lock".as_slice(),
                ),
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
        ("names", pairs(s.trees.names.tree())),
        ("cache", pairs(s.trees.names.cache())),
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
    type Name = ae_tx::NameTransaction;
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
    fn name_hash(&self, op: Word, name: &[u8], b: &mut Budget) -> HostResult<Word, Self::Error> {
        self.check("hash")?;
        self.host.name_hash(op, name, b)
    }
    fn prepare_name(
        &self,
        r: NameWrite<'_>,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<(Self::Name, usize), Self::Error> {
        self.record(tuple(vec![atom("prepare"), int(r.operation().unsigned())]));
        self.check("prepare")?;
        let result = self.host.prepare_name(r, c, b)?;
        if let Ok((t, _)) = &result {
            self.record(tuple(vec![atom("transaction"), bin(t.encode())]));
        }
        Ok(result)
    }
    fn apply_name(
        &self,
        op: Word,
        t: Self::Name,
        sig: [u8; 64],
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Self::Chain, Self::Error> {
        self.record(tuple(vec![atom("apply"), int(op.unsigned()), bin(sig)]));
        self.check("apply")?;
        self.host.apply_name(op, t, sig, c, b)
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> outer_call::Host for Traced<A, C> {
    fn function_types(
        &self,
        _: Word,
        _: VM,
        _: Word,
        _: &Self::Chain,
    ) -> Result<Option<(TypeRef, TypeRef)>, Self::Error> {
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
        if p[0] == atom("illegal_aens_primop_call") {
            return primops::Outcome::Error(primops::Error::IllegalAens(Word::from(
                p[1][1].as_str().unwrap().parse::<u64>().unwrap(),
            )));
        }
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
fn original_name_writes_match_transactions_delegations_gas_events_and_trees() {
    let rows = DATA["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 5742);
    let boundaries = DATA["boundaries"].as_array().unwrap();
    assert_eq!(boundaries.len(), 432);
    assert_eq!(DATA["mismatches"].as_array().unwrap().len(), 6);
    let mut counts = [0; 2];
    for (i, row) in rows
        .iter()
        .chain(boundaries)
        .chain(DATA["mismatches"].as_array().unwrap())
        .enumerate()
    {
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
    assert_eq!(counts, [1132, 5048]);
}
#[test]
fn original_outer_name_writes_preserve_call_failure_mapping_and_complete_state() {
    let rows = DATA["outer"].as_array().unwrap();
    assert_eq!(rows.len(), 1272);
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
    assert_eq!(counts, [123, 1133, 16]);
}

fn successes() -> impl Iterator<Item = &'static J> {
    DATA["runs"].as_array().unwrap().iter().filter(|r| {
        r["vm"] == 6
            && r["protocol"] == 4
            && r["mode"] == "onchain"
            && (r["case_name"] == "normal" || (r["case_name"] == "auction_bid" && r["op"] == 202))
            && r["call_gas"] == 100000
            && r["result"]["value"][1][0] == atom("ok")
    })
}
#[test]
fn name_write_limits_and_callback_faults_preserve_inputs_and_retry() {
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
        for stage in ["hash", "prepare", "apply"] {
            if stage == "hash" && (row["op"] == 201 || row["op"] == 202) {
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
    assert_eq!(count, 5);
}
#[test]
fn name_account_consensus_and_expiry_write_faults_leave_all_roots_retryable() {
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
        for (control, reads_existing) in [(&a, true), (&c, s.trees.names.root_hash().is_some())] {
            control.read_error.store(true, Relaxed);
            let actual = direct(row, &s, &h, Limits::default());
            if reads_existing {
                assert!(matches!(actual, Err(Fault::Host(_))));
            } else {
                let (value, next) = actual.unwrap();
                assert_eq!(value, expected(row));
                assert_eq!(
                    next.trees.roots().hash().0,
                    bytes(field(&desired, "root")).as_slice()
                );
            }
            control.read_error.store(false, Relaxed);
            assert_eq!(snapshot(&s), before);
            let (value, next) = direct(row, &s, &h, Limits::default()).unwrap();
            assert_eq!(value, expected(row));
            assert_eq!(snapshot(&next), desired);
        }
    }
    assert_eq!(count, 5);
}
#[test]
fn name_transaction_callbacks_match_retained_original_preparation_and_application() {
    let rows = PREPARATION["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 1152);
    let mut preparation = [0; 3];
    let mut application = [0; 3];
    for (i, row) in rows.iter().enumerate() {
        let s = state(row);
        let before = snapshot(&s);
        let inner = s.onchain.as_ref().map(|t| t.roots());
        let account = Word(bytes(&row["actor"]).try_into().unwrap());
        let hash = Word(bytes(&row["hash"]).try_into().unwrap());
        let name = bytes(&row["name"]);
        let request = match row["op"].as_str().unwrap() {
            "preclaim" => NameWrite::Preclaim {
                account,
                commitment: Word::from(10),
            },
            "claim" => NameWrite::Claim {
                account,
                name: &name,
                salt: Word::from(1),
                fee: if row["vm"].as_u64().unwrap() >= 6 {
                    Some(Word::from(row["fee"].as_u64().unwrap()))
                } else {
                    None
                },
            },
            "transfer" => NameWrite::Transfer {
                account,
                recipient: Word::from(2),
                hash,
            },
            "revoke" => NameWrite::Revoke { account, hash },
            _ => panic!(),
        };
        let host = Host::default();
        let p = host.prepare_name(request, &s, &mut Budget::new(Limits::default()));
        let tx = p
            .as_ref()
            .ok()
            .and_then(|r| r.as_ref().ok())
            .map(|(t, _)| t.clone());
        let got = oracle::result(p.map(|r| {
            r.map(|(t, size)| {
                assert_eq!(size, t.encode().len());
                Value::String(t.encode())
            })
        }));
        assert_eq!(got, row["prepare"], "row {i}");
        preparation[if got["kind"] == "native" {
            2
        } else if got["value"][1][0] == atom("error") {
            1
        } else {
            0
        }] += 1;
        if let Some(tx) = tx {
            let sig = bytes(&row["signature"]).try_into().unwrap();
            let a = host.apply_name(
                request.operation(),
                tx,
                sig,
                &s,
                &mut Budget::new(Limits::default()),
            );
            let got = match a {
                Ok(Ok(next)) => {
                    assert_eq!(next.onchain.as_ref().map(|t| t.roots()), inner);
                    json!({"kind":"ok","value":tuple(vec![atom("ok"),snapshot(&next)])})
                }
                Ok(Err(e)) => oracle::result(Ok(Err(e))),
                Err(e) => oracle::result(Err(e)),
            };
            assert_eq!(got["kind"], row["apply"]["kind"], "row {i}");
            if got["kind"] == "ok" {
                assert_eq!(
                    logical(got["value"].clone()),
                    logical(row["apply"]["value"].clone()),
                    "row {i}"
                );
            } else {
                assert_eq!(got, row["apply"], "row {i}");
            }
            application[if got["kind"] == "native" {
                2
            } else if got["value"][1][0] == atom("error") {
                1
            } else {
                0
            }] += 1;
        } else {
            assert_eq!(row["apply"]["kind"], "not_applied");
        }
        assert_eq!(snapshot(&s), before);
        assert_eq!(s.onchain.as_ref().map(|t| t.roots()), inner);
    }
    assert_eq!(preparation, [540, 576, 36]);
    assert_eq!(application, [310, 212, 18]);
}
#[test]
fn name_lifecycles_preserve_height_nonce_ownership_and_event_order() {
    let rows = DATA["sequences"].as_array().unwrap();
    assert_eq!(rows.len(), 36);
    let mut counts = [0; 2];
    for sequence in rows {
        let steps = sequence["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 6);
        let mut s = state(&steps[0]);
        for row in steps {
            let mut context = (*s.context).clone();
            context.height = row["height"].as_u64().unwrap();
            s.context = Arc::new(context);
            let before = snapshot(&s);
            assert_eq!(
                before,
                logical(field(&row["before"], "chain_state").clone())
            );
            let h = Traced::default();
            let (value, next) = direct(row, &s, &h, Limits::default()).unwrap();
            assert_eq!(value, expected(row));
            let desired = if matches!(value, primops::Outcome::Complete { .. }) {
                counts[0] += 1;
                field(&row["result"]["value"][1][3], "chain_state")
            } else {
                counts[1] += 1;
                field(&row["before"], "chain_state")
            };
            assert_eq!(snapshot(&next), logical(desired.clone()));
            assert_eq!(snapshot(&s), before);
            assert_eq!(list(h.events.into_inner()), row["host_calls"]);
            s = next;
        }
    }
    assert_eq!(counts, [108, 108]);
}
