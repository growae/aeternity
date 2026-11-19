use super::invocations::heap_support as support;
use super::*;
use ae_trie::{Nibbles, NodeReader, NodeWriter, Value as TrieValue};
use ae_vm_aevm::{
    calls::{self, ContractVersion, FunctionTypes, InputHost, TypeError},
    execution::Stop,
    opcode::Version as VmVersion,
};
use std::cell::RefCell;
static PREPARATION: LazyLock<J> = LazyLock::new(|| {
    support::fixture(include_str!(
        "../fixtures/erlang_aevm_contract_type_preparation.json"
    ))
});
static DATA: LazyLock<J> =
    LazyLock::new(|| support::fixture(include_str!("../fixtures/erlang_aevm_contract_types.json")));
fn trees(snap: &J) -> Trees {
    let mut tree = Trie::new(MemoryStore::default());
    for pair in field(snap, "contracts")[1].as_array().unwrap() {
        tree.put(
            &Nibbles::from_bytes(&bytes(&pair[1][0])),
            TrieValue::Bytes(bytes(&pair[1][1])),
        )
        .unwrap();
    }
    let trees = Trees {
        contracts: Contracts::from_root(tree.root_hash(), tree.into_store()).unwrap(),
        ..trees_from(&map(vec![("accounts", list(vec![]))]))
    };
    assert_eq!(bin(trees.roots().hash().0), *field(snap, "root"));
    trees
}
fn caller(row: &J) -> ContractVersion {
    ContractVersion {
        vm: VmVersion::from_id(support::uint(field(&row["from"], "vm")).try_into().unwrap())
            .unwrap(),
        abi: support::uint(field(&row["from"], "abi"))
            .try_into()
            .unwrap(),
    }
}
fn state(row: &J) -> State {
    let on = trees(&row["onchain"]);
    let (trees, onchain) = if row["mode"] == "offchain" {
        (trees(&row["offchain"]), Some(Arc::new(on)))
    } else {
        (on, None)
    };
    State {
        trees,
        onchain,
        account: AccountPublicKey(Word::from(9).0),
        vm_version: caller(row).vm,
        network_id: Arc::from(b"ae_mainnet".as_slice()),
        context: Arc::new(context(row)),
        event_context: EventContext::None,
        events: Arc::new(vec![]),
    }
}
fn snapshot<A: NodeReader, C: NodeReader>(s: &State<A, C>) -> J {
    fn one<A: NodeReader, C: NodeReader>(t: &Trees<A, C>) -> J {
        let pairs = t
            .contracts
            .tree()
            .iter()
            .map(|p| {
                let (key, v) = p.unwrap();
                let TrieValue::Bytes(v) = v else { panic!() };
                let k: Vec<_> = key
                    .as_slice()
                    .chunks_exact(2)
                    .map(|p| p[0] * 16 + p[1])
                    .collect();
                tuple(vec![bin(k), bin(v)])
            })
            .collect();
        map(vec![
            ("root", bin(t.roots().hash().0)),
            ("contracts", list(pairs)),
        ])
    }
    json!([
        one(&s.trees),
        s.onchain.as_ref().map(|t| one(t)),
        s.events.len()
    ])
}
fn native(n: Native) -> J {
    let mut class = "error";
    let reason = match n {
        Native::BadMatchNone => tuple(vec![atom("badmatch"), atom("none")]),
        Native::BadMatchBinary(b) => tuple(vec![atom("badmatch"), bin(b)]),
        Native::BadMatchCodeReference(id) => tuple(vec![
            atom("badmatch"),
            tuple(vec![
                atom("ref"),
                tuple(vec![atom("id"), atom("contract"), bin(id.value)]),
            ]),
        ]),
        Native::OutOfGas => {
            class = "throw";
            tuple(vec![atom("aevm_eval_error"), atom("out_of_gas"), int(0)])
        }
        n => panic!("{n:?}"),
    };
    json!({"kind":"native","class":class,"reason":reason})
}
fn result(result: FunctionTypes<aevm_chain::Error>) -> J {
    match result {
        Ok(Ok((a, o))) => {
            json!({"kind":"ok","value":tuple(vec![atom("ok"),support::type_term(&a),support::type_term(&o)])})
        }
        Ok(Err(e)) => {
            let r = match e {
                TypeError::NoSuchContract(k) => tuple(vec![atom("no_such_contract"), bin(k.0)]),
                TypeError::WrongVmVersion { vm, abi } => tuple(vec![
                    atom("wrong_vm_version"),
                    map(vec![("vm", int(vm)), ("abi", int(abi))]),
                ]),
                TypeError::UnknownFunction => atom("unknown_function"),
                TypeError::BadTypeData => atom("bad_type_data"),
            };
            json!({"kind":"ok","value":tuple(vec![atom("error"),r])})
        }
        Err(Fault::Native(n)) => native(n),
        r => panic!("{r:?}"),
    }
}
#[test]
fn actual_contract_type_lookup_matches_original_versions_wrappers_references_and_tree_precedence() {
    for (data, total, expected) in [
        (&*PREPARATION, 742, [60, 634, 48]),
        (&*DATA, 882, [100, 734, 48]),
    ] {
        assert_eq!(data["runs"].as_array().unwrap().len(), total);
        let mut counts = [0; 3];
        for row in data["runs"].as_array().unwrap() {
            let s = state(row);
            let before = snapshot(&s);
            let got = result(s.function_types(
                Word::from(1),
                caller(row),
                Word(bytes(&row["hash"]).try_into().unwrap()),
                &mut Budget::new(Limits::default()),
            ));
            assert_eq!(
                support::logical(got.clone()),
                support::logical(row["result"].clone()),
                "from={} to={} mode={} case={}",
                row["from"],
                row["to"],
                row["mode"],
                row["case_name"]
            );
            counts[if got["kind"] == "native" {
                2
            } else if got["value"][1][0] == atom("error") {
                1
            } else {
                0
            }] += 1;
            assert_eq!(snapshot(&s), before);
        }
        assert_eq!(counts, expected);
    }
}
struct Traced<'a, A = MemoryStore, C = MemoryStore> {
    state: &'a State<A, C>,
    trace: RefCell<Vec<J>>,
}
impl<A: NodeWriter + Clone, C: NodeWriter + Clone> InputHost for Traced<'_, A, C> {
    type Error = aevm_chain::Error;
    fn function_types(
        &self,
        k: Word,
        v: ContractVersion,
        f: Word,
        b: &mut Budget,
    ) -> FunctionTypes<Self::Error> {
        self.trace.borrow_mut().push(tuple(vec![
            bin(k.0),
            map(vec![("vm", int(v.vm.id())), ("abi", int(v.abi))]),
            bin(f.0),
        ]));
        self.state.function_types(k, v, f, b)
    }
}
fn request(input: &J) -> calls::Input {
    calls::Input {
        target: Word::from(1),
        offset: Word::from_unsigned(&input["offset"].as_str().unwrap().parse().unwrap()),
        size: Word::from(999),
    }
}
fn prepared(row: &J, input: &J) -> Result<calls::Prepared, Fault<aevm_chain::Error>> {
    let r = &input["result"];
    if r["kind"] == "native" {
        let reason = &r["reason"][1];
        let n = if reason[0] == atom("aevm_eval_error") {
            Native::OutOfGas
        } else if reason[1] == atom("none") {
            Native::BadMatchNone
        } else if reason[1][0] == "binary" {
            Native::BadMatchBinary(bytes(&reason[1]))
        } else {
            Native::BadMatchCodeReference(Id {
                tag: IdTag::Contract,
                value: Word::from(3).0,
            })
        };
        assert_eq!(native(n.clone()), *r);
        return Err(Fault::Native(n));
    }
    let v = &r["value"][1];
    Ok(calls::Prepared {
        data: bytes(&v[0]),
        output_type: (v[1] != atom("undefined")).then(|| support::ty(&v[1])),
        state: Box::new(invocations::vm(row, &v[2])),
    })
}
#[test]
fn actual_contract_input_preserves_abi_gas_auth_order_native_errors_and_entire_vm_state() {
    let mut counts = [0; 2];
    let mut traces = 0;
    for row in DATA["runs"].as_array().unwrap() {
        let chain = state(row);
        let before = snapshot(&chain);
        for input in row["inputs"].as_array().unwrap() {
            let s = invocations::vm(row, &input["before"]);
            let unchanged = s.clone();
            let host = Traced {
                state: &chain,
                trace: RefCell::new(vec![]),
            };
            let got = calls::prepare_input(&s, request(input), &host, Limits::default());
            assert_eq!(
                got,
                prepared(row, input),
                "from={} to={} case={} input={}",
                row["from"],
                row["to"],
                row["case_name"],
                input["variant"]
            );
            counts[usize::from(got.is_err())] += 1;
            assert_eq!(
                logical(list(host.trace.borrow().clone())),
                logical(input["trace"].clone())
            );
            traces += host.trace.borrow().len();
            assert_eq!(s, unchanged);
            assert_eq!(snapshot(&chain), before);
        }
    }
    assert_eq!(counts, [830, 3580]);
    assert_eq!(traces, 2268);
}
#[test]
fn actual_contract_type_reads_bound_resources_and_preserve_backend_faults_for_retry() {
    use invocations::failures::{Control, wrapped};
    use std::sync::atomic::Ordering::Relaxed;
    let row = DATA["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["vm"] == 6
                && caller(r).abi == 1
                && r["case_name"] == "normal"
                && r["mode"] == "offchain"
                && support::uint(field(&r["to"], "vm")) == 6u8.into()
        })
        .unwrap();
    let chain = state(row);
    let unchanged = snapshot(&chain);
    let input = &row["inputs"][0];
    let s = invocations::vm(row, &input["before"]);
    let before = s.clone();
    let expected = prepared(row, input);
    for dim in 0..3 {
        let (mut stopped, mut reached) = (false, false);
        for n in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000] {
            let mut limits = Limits::default();
            match dim {
                0 => limits.max_work = n,
                1 => limits.max_bytes = n,
                _ => limits.max_depth = n,
            };
            match calls::prepare_input(&s, request(input), &chain, limits) {
                Err(Fault::Stop(Stop::ResourceLimit)) => stopped = true,
                r => {
                    assert_eq!(r, expected);
                    reached = true;
                    break;
                }
            }
            assert_eq!(s, before);
            assert_eq!(snapshot(&chain), unchanged);
        }
        assert!(stopped && reached);
    }
    let a = Arc::new(Control::default());
    let top = Arc::new(Control::default());
    let inner = Arc::new(Control::default());
    let faulty = State {
        trees: wrapped(&chain.trees, &a, &top),
        onchain: chain
            .onchain
            .as_ref()
            .map(|t| Arc::new(wrapped(t, &a, &inner))),
        account: chain.account,
        vm_version: chain.vm_version,
        network_id: chain.network_id.clone(),
        context: chain.context.clone(),
        event_context: chain.event_context.clone(),
        events: chain.events.clone(),
    };
    // Even a failing inner backend must not affect off-chain type lookup.
    inner.read_error.store(true, Relaxed);
    assert_eq!(
        calls::prepare_input(&s, request(input), &faulty, Limits::default()),
        expected
    );
    top.read_error.store(true, Relaxed);
    assert!(matches!(
        calls::prepare_input(&s, request(input), &faulty, Limits::default()),
        Err(Fault::Host(aevm_chain::Error::Contracts(_)))
    ));
    assert_eq!(s, before);
    top.read_error.store(false, Relaxed);
    inner.read_error.store(false, Relaxed);
    assert_eq!(snapshot(&faulty), unchanged);
    assert_eq!(
        calls::prepare_input(&s, request(input), &faulty, Limits::default()),
        expected
    );
    for c in [a, top, inner] {
        assert_eq!(c.writes.load(Relaxed), 0);
    }
}
