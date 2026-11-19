use super::*;
use ae_trie::{NodeReader, NodeWriter};
use ae_vm_aevm::{
    execution::{self, Failure},
    gas::FloatProfile,
    maps::Store,
    opcode::Version as VmVersion,
    outer_call,
    primops::{ChainHost, HostResult},
    runtime::{self, Outcome, StoreHost},
    runtime_heap::TypeRef,
    store::OrderProfile,
};
mod failures;
#[path = "../../../ae-vm-aevm/tests/support/mod.rs"]
mod heap_support;
static DATA: LazyLock<J> = LazyLock::new(|| {
    let p: J =
        serde_json::from_str(include_str!("../fixtures/erlang_aevm_account_adapter.json")).unwrap();
    expand(&p["root"], p["values"].as_array().unwrap())
});
fn base(row: &J, snap: &J) -> State {
    let mut trees = trees_from(snap);
    let writes = if field(snap, "store") == &atom("missing") {
        Store::new()
    } else {
        heap_support::store(field(snap, "store"))
    };
    for c in field(snap, "contracts")[1].as_array().unwrap() {
        let c = Contract::deserialize(
            bytes(field(c, "key")).try_into().unwrap(),
            &bytes(field(c, "encoded")),
        )
        .unwrap();
        trees.contracts.insert(&c, &writes).unwrap();
    }
    let s = State {
        trees,
        onchain: None,
        account: AccountPublicKey(Word::from(1).0),
        vm_version: ae_vm_aevm::opcode::Version::from_id(row["vm"].as_u64().unwrap() as u32)
            .unwrap(),
        context: Arc::new(context(row)),
        event_context: EventContext::None,
        events: Arc::new(vec![]),
    };
    assert_eq!(bin(s.trees.roots().hash().0), *field(snap, "root"));
    s
}
fn state(row: &J, group: &str) -> State {
    let snapshot = |r: &J| {
        if group == "invocations" {
            field(&r["before"], "chain_state").clone()
        } else {
            r["before"].clone()
        }
    };
    let mut s = base(row, &snapshot(row));
    if row["mode"] == "offchain" {
        let on = DATA[group]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| {
                r["mode"] == "onchain"
                    && r["vm"] == row["vm"]
                    && r["protocol"] == row["protocol"]
                    && r["case_name"] == row["case_name"]
            })
            .unwrap();
        s.onchain = Some(Arc::new(base(on, &snapshot(on)).trees));
    }
    s
}
fn chain_snapshot<A: NodeReader, C: NodeReader>(s: &State<A, C>) -> J {
    let common = snapshot_keys(s, &[1, 17]);
    let contracts = s
        .trees
        .contracts
        .record(Word::from(1).0)
        .unwrap()
        .into_iter()
        .map(|c| {
            map(vec![
                ("key", bin(c.public_key)),
                ("encoded", bin(c.serialize().unwrap())),
            ])
        })
        .collect();
    let store = Host::<A, C>::default().store(s).unwrap().map_or_else(
        || atom("missing"),
        |s| heap_support::map(s.into_iter().map(|(k, v)| (bin(k), bin(v))).collect()),
    );
    map(vec![
        ("root", field(&common, "root").clone()),
        ("accounts", field(&common, "accounts").clone()),
        ("events", field(&common, "events").clone()),
        ("contracts", list(contracts)),
        ("store", logical(store)),
    ])
}
pub(super) struct VmHost<A = MemoryStore, C = MemoryStore>(pub(super) Host<A, C>);
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> StoreHost for VmHost<A, C> {
    type Chain = State<A, C>;
    type Error = aevm_chain::Error;
    fn set_store(&self, s: Store, c: &Self::Chain) -> Result<Self::Chain, Fault<Self::Error>> {
        self.0.set_store(s, c)
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> ChainHost for VmHost<A, C> {
    type Spend = ae_tx::Spend;
    fn oracle(
        &self,
        r: ae_vm_aevm::primops::OracleRequest<'_>,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<ae_vm_aevm::heap::Value, Self::Error> {
        self.0.oracle(r, c, b)
    }
    fn get_store(&self, c: &Self::Chain) -> Result<Store, Fault<Self::Error>> {
        self.0.get_store(c)
    }
    fn prepare_spend(
        &self,
        r: Id,
        v: &BigInt,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Self::Spend, Self::Error> {
        self.0.prepare_spend(r, v, c, b)
    }
    fn apply_spend(
        &self,
        s: Self::Spend,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Self::Chain, Self::Error> {
        self.0.apply_spend(s, c, b)
    }
    fn address(
        &self,
        k: AddressKind,
        a: Word,
        c: &Self::Chain,
        b: &mut Budget,
    ) -> HostResult<Word, Self::Error> {
        self.0.address(k, a, c, b)
    }
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> outer_call::Host for VmHost<A, C> {
    fn function_types(
        &self,
        _: Word,
        _: VmVersion,
        _: Word,
        _: &Self::Chain,
    ) -> Result<Option<(TypeRef, TypeRef)>, Self::Error> {
        panic!("local primitive only")
    }
    fn balance(&self, a: Word, c: &Self::Chain) -> Result<BigInt, Self::Error> {
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
pub(super) fn vm(row: &J, snap: &J) -> execution::State {
    use heap_support::{number, uint};
    let mut s = execution::State::new(
        VmVersion::from_id(row["vm"].as_u64().unwrap() as u32).unwrap(),
        bytes(field(snap, "code")),
        uint(field(snap, "gas")),
        FloatProfile::Otp26Word64,
    );
    s.protocol = uint(field(snap, "protocol_version")).try_into().unwrap();
    s.cp = uint(field(snap, "cp")).try_into().unwrap();
    s.data = bytes(field(snap, "data"));
    s.out_type = Some(heap_support::ty(field(snap, "out_type")));
    s.map_store = heap_support::store(field(snap, "storage"));
    s.maps = heap_support::maps(field(snap, "maps"));
    s.output = bytes(field(snap, "out"));
    s.return_data = bytes(field(snap, "return_data"));
    for (k, v) in [
        ("address", &mut s.context.address),
        ("caller", &mut s.context.caller),
        ("origin", &mut s.context.origin),
        ("value", &mut s.context.value),
    ] {
        *v = number(field(snap, k));
    }
    let memory = field(snap, "memory");
    for p in memory[1].as_array().unwrap() {
        if p[0][0] == "integer" {
            s.memory
                .store(&uint(&p[0]), Word::from_unsigned(&uint(&p[1])));
        }
    }
    let size = uint(field(memory, "mem_size"));
    if size != 0u8.into() {
        s.memory.load(&(size * 32u8 - 32u8));
    }
    s.stack = field(snap, "stack")[1]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| Word::from_unsigned(&uint(n)))
        .collect();
    for k in ["call_stack", "callcreates", "logs"] {
        assert_eq!(field(snap, k), &list(vec![]));
    }
    assert_eq!(field(snap, "auth_tx_hash"), &atom("undefined"));
    assert_eq!(field(snap, "no_recursion"), &atom("false"));
    if let Some(p) = snap[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom("jumpdests"))
    {
        s.jump_destinations = p[1][1]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| uint(&p[0]).try_into().unwrap())
            .collect();
    }
    s
}
#[test]
fn original_contract_store_callbacks_preserve_roots_tombstones_and_native_errors() {
    let rows = DATA["stores"].as_array().unwrap();
    assert_eq!(rows.len(), 56);
    let host = Host::default();
    let mut counts = [0; 2];
    for row in rows {
        let s = state(row, "stores");
        let before = chain_snapshot(&s);
        assert_eq!(before, logical(row["before"].clone()));
        let writes = heap_support::store(&row["writes"]);
        let actual = host.set_store(writes.clone(), &s);
        let r = &row["result"];
        if r["kind"] == "native" {
            let expected = if r["reason"][1][0] == atom("illegal") {
                assert_eq!(r["reason"][1][1], atom("store"));
                assert_eq!(
                    logical(r["reason"][1][2].clone()),
                    logical(row["writes"].clone())
                );
                Native::IllegalStore(writes)
            } else {
                assert_eq!(r["reason"], tuple(vec![atom("case_clause"), atom("none")]));
                Native::CaseClauseNone
            };
            assert_eq!(actual.err(), Some(Fault::Native(expected)), "{row}");
            counts[1] += 1;
        } else {
            assert_eq!(
                chain_snapshot(&actual.unwrap()),
                logical(r["value"].clone()),
                "{row}"
            );
            counts[0] += 1;
        }
        assert_eq!(chain_snapshot(&s), before);
    }
    assert_eq!(counts, [32, 24]);
}
#[test]
fn full_original_calls_apply_real_account_transfers_and_publish_only_success() {
    let rows = DATA["invocations"].as_array().unwrap();
    assert_eq!(rows.len(), 432);
    let host = VmHost(Host::default());
    let mut counts = [0; 3];
    for row in rows {
        let s = state(row, "invocations");
        let before = chain_snapshot(&s);
        let original = vm(row, &row["before"]);
        let t = heap_support::ty(&row["type"]);
        assert_eq!(
            runtime::initialize(&original, Some(&t), Limits::default()).unwrap(),
            vm(row, &row["ready"])
        );
        let actual = outer_call::invoke(
            &original,
            Some(&t),
            &s,
            &host,
            OrderProfile::Otp26Word64Crc32c,
            Limits::default(),
        )
        .unwrap();
        let expected = &row["result"]["value"][1];
        assert_eq!(row["result"]["kind"], "ok");
        match actual {
            Outcome::Complete { state: next, chain } => {
                assert_eq!(expected[0], atom("ok"), "{row}");
                assert_eq!(*next, vm(row, &expected[1]), "{row}");
                assert_eq!(
                    chain_snapshot(&chain),
                    logical(field(&expected[1], "chain_state").clone()),
                    "{row}"
                );
                assert_eq!(
                    chain.onchain.as_ref().map(|t| t.roots()),
                    s.onchain.as_ref().map(|t| t.roots())
                );
                counts[0] += 1;
            }
            Outcome::Error { reason, gas } => {
                assert_eq!(expected[0], atom("error"));
                let reason = match reason {
                    Failure::OutOfGas => "out_of_gas".to_string(),
                    Failure::PopEmpty => "error_pop_empty_stack".to_string(),
                    Failure::Call(r) => r,
                    r => panic!("{r:?}"),
                };
                assert_eq!(atom(&reason), expected[1], "{row}");
                assert_eq!(int(gas), expected[2]);
                counts[1] += 1;
            }
            Outcome::Revert { output, gas } => {
                assert_eq!(expected[0], atom("revert"));
                assert_eq!(bin(output), expected[1]);
                assert_eq!(int(gas), expected[2]);
                counts[2] += 1;
            }
        }
        assert_eq!(chain_snapshot(&s), before);
        assert_eq!(original, vm(row, &row["before"]));
    }
    assert_eq!(counts, [100, 284, 48]);
}
#[test]
fn repeated_spends_preserve_contract_nonce_and_signed_event_context() {
    let rows = DATA["sequences"].as_array().unwrap();
    assert_eq!(rows.len(), 32);
    let host = Host::default();
    let mut count = 0;
    for row in rows {
        let mut s = state(row, "sequences");
        if row["signed"] == true {
            let tx = ae_tx::SignedTransaction::<ae_tx::Spend>::decode(&bytes(&row["signed_tx"]))
                .unwrap();
            s.event_context = EventContext::Transaction {
                kind: ae_tx::TransactionKind::Spend,
                hash: tx.hash(),
            };
        }
        for step in row["steps"].as_array().unwrap() {
            let original = chain_snapshot(&s);
            let mut budget = Budget::new(Limits::default());
            let tx = host
                .prepare(
                    Id::new(IdTag::Account, Word::from(17).0),
                    &step["amount"].as_u64().unwrap().into(),
                    &s,
                    &mut budget,
                )
                .unwrap()
                .unwrap();
            assert_eq!(tx.encode(), bytes(&step["tx"]));
            let next = host.apply(&tx, &s, &mut budget).unwrap().unwrap();
            assert_eq!(
                chain_snapshot(&next),
                logical(step["after_state"].clone()),
                "{row}"
            );
            assert_eq!(chain_snapshot(&s), original);
            s = next;
            count += 1;
        }
    }
    assert_eq!(count, 96);
}
