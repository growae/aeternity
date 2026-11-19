use super::*;
use ae_state::{calls::Calls, names::Names};
use ae_types::Hash256;
use ae_vm_aevm::execution::Stop;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};

pub(crate) struct Control {
    pub(crate) read_error: AtomicBool,
    pub(crate) remaining: AtomicUsize,
    pub(crate) writes: AtomicUsize,
}
impl Default for Control {
    fn default() -> Self {
        Self {
            read_error: AtomicBool::new(false),
            remaining: AtomicUsize::new(usize::MAX),
            writes: AtomicUsize::new(0),
        }
    }
}
#[derive(Clone)]
pub(crate) struct Backend {
    store: MemoryStore,
    control: Arc<Control>,
}
impl NodeReader for Backend {
    fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        if self.control.read_error.load(Relaxed) {
            return Err(ae_trie::Error::Backend("read failed".into()));
        }
        self.store.node(h)
    }
}
impl NodeWriter for Backend {
    fn put_node(&mut self, h: Hash256, bytes: Vec<u8>) -> Result<(), ae_trie::Error> {
        let left = self.control.remaining.load(Relaxed);
        if left == 0 {
            return Err(ae_trie::Error::Backend("write failed".into()));
        }
        self.control.remaining.store(left - 1, Relaxed);
        self.control.writes.fetch_add(1, Relaxed);
        self.store.put_node(h, bytes)
    }
}
pub(crate) fn wrapped(t: &Trees, a: &Arc<Control>, c: &Arc<Control>) -> Trees<Backend, Backend> {
    let wrap = |t: &Trie<MemoryStore>, control: &Arc<Control>| Backend {
        store: t.store().clone(),
        control: control.clone(),
    };
    Trees {
        accounts: Accounts::from_root(t.accounts.root_hash(), wrap(t.accounts.tree(), a)).unwrap(),
        contracts: Contracts::from_root(t.contracts.root_hash(), wrap(t.contracts.tree(), c))
            .unwrap(),
        names: Names::from_roots(
            t.names.root_hash(),
            t.names.cache_root_hash(),
            wrap(t.names.tree(), c),
            wrap(t.names.cache(), c),
        )
        .unwrap(),
        oracles: Oracles::from_roots(
            t.oracles.root_hash(),
            t.oracles.cache_root_hash(),
            wrap(t.oracles.tree(), c),
            wrap(t.oracles.cache(), c),
        )
        .unwrap(),
        calls: Calls::from_root(t.calls.root_hash(), wrap(t.calls.tree(), c)).unwrap(),
        channels: Trie::from_root(t.channels.root_hash(), wrap(&t.channels, c)).unwrap(),
    }
}
fn fixture() -> &'static J {
    DATA["invocations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["vm"] == 6
                && r["protocol"] == 4
                && r["mode"] == "offchain"
                && r["case_name"] == "two_spends"
                && field(&r["before"], "gas") == &int(60000)
        })
        .unwrap()
}
fn run<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    row: &J,
    s: &State<A, C>,
    limits: Limits,
) -> Result<Outcome<State<A, C>>, Fault<aevm_chain::Error>> {
    outer_call::invoke(
        &vm(row, &row["before"]),
        Some(&heap_support::ty(&row["type"])),
        s,
        &VmHost(Host::default()),
        OrderProfile::Otp26Word64Crc32c,
        limits,
    )
    .map_err(|e| match e {
        outer_call::Fault::Runtime(e) => e,
        _ => panic!("unexpected call transport fault"),
    })
}
fn complete<A: NodeReader, C: NodeReader>(out: Outcome<State<A, C>>, row: &J) {
    let Outcome::Complete { state, chain } = out else {
        panic!("expected success")
    };
    let expected = &row["result"]["value"][1][1];
    assert_eq!(*state, vm(row, expected));
    assert_eq!(
        chain_snapshot(&chain),
        logical(field(expected, "chain_state").clone())
    );
}
#[test]
fn real_chain_resource_stops_preserve_roots_events_and_allow_retry() {
    let row = fixture();
    let s = state(row, "invocations");
    let before = chain_snapshot(&s);
    let inner = s.onchain.as_ref().unwrap().roots();
    for dimension in 0..4 {
        let mut stopped = false;
        let mut reached = false;
        for n in [0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 1_000_000] {
            let mut l = Limits::default();
            match dimension {
                0 => l.max_work = n,
                1 => l.max_bytes = n,
                2 => l.max_depth = n,
                _ => l.max_memory_words = n,
            }
            match run(row, &s, l) {
                Err(Fault::Stop(Stop::ResourceLimit)) => stopped = true,
                Ok(out) => {
                    complete(out, row);
                    reached = true;
                }
                Err(e) => panic!("{e:?}"),
            }
            assert_eq!(chain_snapshot(&s), before);
            assert_eq!(s.onchain.as_ref().unwrap().roots(), inner);
            if reached {
                break;
            }
        }
        assert!(stopped && reached);
        complete(run(row, &s, Limits::default()).unwrap(), row);
    }
}
#[test]
fn real_account_and_contract_backend_failures_discard_partial_vm_changes() {
    let row = fixture();
    let input = state(row, "invocations");
    let a = Arc::new(Control::default());
    let c = Arc::new(Control::default());
    let s = State {
        trees: wrapped(&input.trees, &a, &c),
        onchain: input.onchain.as_ref().map(|t| Arc::new(wrapped(t, &a, &c))),
        account: input.account,
        vm_version: input.vm_version,
        network_id: input.network_id,
        context: input.context,
        event_context: input.event_context,
        events: input.events,
    };
    let before = chain_snapshot(&s);
    let inner = s.onchain.as_ref().unwrap().roots();
    complete(run(row, &s, Limits::default()).unwrap(), row);
    let writes = [a.writes.load(Relaxed), c.writes.load(Relaxed)];
    assert!(writes.iter().all(|n| *n > 1));
    for (control, count) in [(&a, writes[0]), (&c, writes[1])] {
        for allowed in 0..=count {
            control.remaining.store(allowed, Relaxed);
            match run(row, &s, Limits::default()) {
                Err(Fault::Host(_)) => assert!(allowed < count),
                Ok(out) => {
                    assert_eq!(allowed, count);
                    complete(out, row);
                }
                Err(e) => panic!("{e:?}"),
            }
            control.remaining.store(usize::MAX, Relaxed);
            assert_eq!(chain_snapshot(&s), before);
            assert_eq!(s.onchain.as_ref().unwrap().roots(), inner);
            complete(run(row, &s, Limits::default()).unwrap(), row);
        }
    }
    // The transaction has already staged tree writes when event accounting stops.
    let host = Host::default();
    let tx = host
        .prepare(
            Id::new(IdTag::Account, Word::from(17).0),
            &7.into(),
            &s,
            &mut Budget::new(Limits::default()),
        )
        .unwrap()
        .unwrap();
    let written = a.writes.load(Relaxed);
    let mut budget = Budget::new(Limits {
        max_work: 1,
        ..Limits::default()
    });
    assert!(matches!(
        host.apply(&tx, &s, &mut budget),
        Err(Fault::Stop(Stop::ResourceLimit))
    ));
    assert!(a.writes.load(Relaxed) > written);
    assert_eq!(chain_snapshot(&s), before);
    for control in [&a, &c] {
        control.read_error.store(true, Relaxed);
        if Arc::ptr_eq(control, &a) {
            assert!(matches!(
                host.prepare(
                    Id::new(IdTag::Account, Word::from(17).0),
                    &7.into(),
                    &s,
                    &mut Budget::new(Limits::default())
                ),
                Err(Fault::Host(aevm_chain::Error::Accounts(_)))
            ));
        } else {
            assert!(matches!(
                host.store(&s),
                Err(Fault::Host(aevm_chain::Error::Contracts(_)))
            ));
            assert!(matches!(
                host.set_store(Store::new(), &s),
                Err(Fault::Host(aevm_chain::Error::Contracts(_)))
            ));
        }
        control.read_error.store(false, Relaxed);
        assert_eq!(chain_snapshot(&s), before);
        complete(run(row, &s, Limits::default()).unwrap(), row);
    }
}
#[test]
fn missing_contract_store_is_a_native_failure_and_rolls_back_prior_spends() {
    let row = fixture();
    let mut s = state(row, "invocations");
    s.trees.contracts = Contracts::default();
    let before = chain_snapshot(&s);
    assert_eq!(Host::default().store(&s).unwrap(), None);
    assert_eq!(
        Host::default().get_store(&s),
        Err(Fault::Native(Native::ChainException {
            class: "error",
            reason: "function_clause",
        }))
    );
    assert!(matches!(
        run(row, &s, Limits::default()),
        Err(Fault::Native(Native::CaseClauseNone))
    ));
    assert_eq!(chain_snapshot(&s), before);
    complete(
        run(row, &state(row, "invocations"), Limits::default()).unwrap(),
        row,
    );
}
