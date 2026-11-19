use super::*;
use ae_state::{calls::Calls, names::Names};
use ae_types::Hash256;
use ae_vm_aevm::execution::Stop;
use std::sync::{
    RwLock,
    atomic::{AtomicUsize, Ordering::Relaxed},
};
static DEPTH: LazyLock<J> = LazyLock::new(|| {
    support::fixture(include_str!(
        "../../fixtures/erlang_aevm_recursive_depth_preparation.json"
    ))
});
struct Control {
    left: AtomicUsize,
    writes: AtomicUsize,
}
impl Default for Control {
    fn default() -> Self {
        Self {
            left: AtomicUsize::new(usize::MAX),
            writes: AtomicUsize::new(0),
        }
    }
}
// A shared append-only content-addressed backend models database handles: roots
// remain isolated while orphaned writes are harmless and snapshots are cheap.
#[derive(Clone)]
struct Shared {
    nodes: Arc<RwLock<MemoryStore>>,
    control: Arc<Control>,
}
impl NodeReader for Shared {
    fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.nodes.read().unwrap().node(h)
    }
}
impl NodeWriter for Shared {
    fn put_node(&mut self, h: Hash256, v: Vec<u8>) -> Result<(), ae_trie::Error> {
        let n = self.control.left.load(Relaxed);
        if n == 0 {
            return Err(ae_trie::Error::Backend("injected write failure".into()));
        }
        self.control.left.store(n - 1, Relaxed);
        self.control.writes.fetch_add(1, Relaxed);
        self.nodes.write().unwrap().put_node(h, v)
    }
}
fn chain(row: &J, control: &Arc<Control>) -> State<Shared, Shared> {
    let t = trees(&row["before"]);
    let wrap = |t: &Trie<MemoryStore>| Shared {
        nodes: Arc::new(RwLock::new(t.store().clone())),
        control: control.clone(),
    };
    let trees = Trees {
        accounts: Accounts::from_root(t.accounts.root_hash(), wrap(t.accounts.tree())).unwrap(),
        contracts: Contracts::from_root(t.contracts.root_hash(), wrap(t.contracts.tree())).unwrap(),
        names: Names::from_roots(
            t.names.root_hash(),
            t.names.cache_root_hash(),
            wrap(t.names.tree()),
            wrap(t.names.cache()),
        )
        .unwrap(),
        oracles: Oracles::from_roots(
            t.oracles.root_hash(),
            t.oracles.cache_root_hash(),
            wrap(t.oracles.tree()),
            wrap(t.oracles.cache()),
        )
        .unwrap(),
        calls: Calls::from_root(t.calls.root_hash(), wrap(t.calls.tree())).unwrap(),
        channels: ae_state::channels::Channels::from_root(
            t.channels.root_hash(),
            wrap(t.channels.tree()),
        )
        .unwrap(),
    };
    let mut context = Context::transaction(
        50,
        Protocol::try_from(row["protocol"].as_u64().unwrap_or(4) as u32).unwrap(),
    );
    context.origin = Origin::Contract;
    State {
        trees,
        onchain: None,
        account: AccountPublicKey(Word::from(9999).0),
        vm_version: VM::from_id(row["vm"].as_u64().unwrap_or(6) as u32).unwrap(),
        network_id: Arc::from(b"ae_mainnet".as_slice()),
        context: Arc::new(context),
        event_context: EventContext::None,
        events: Arc::new(vec![]),
    }
}
fn limits() -> Limits {
    Limits {
        max_work: 100_000_000,
        max_bytes: 1024 * 1024 * 1024,
        max_depth: 2048,
        ..Limits::default()
    }
}
fn run(
    row: &J,
    s: &State<Shared, Shared>,
    limits: Limits,
) -> Result<(Response, State<Shared, Shared>), aevm::Error<Shared, Shared>> {
    let cfg = expanded_config();
    let host = aevm::Host::new(&cfg);
    let data = bytes(&row["data"]);
    let gas: BigUint = row["gas"].as_str().unwrap().parse().unwrap();
    host.call(
        Request {
            target: Word::from(100),
            gas: &gas,
            value: &0.into(),
            data: &data,
            call_stack: &[],
            origin: Word::from(3),
        },
        s,
        &mut Budget::new(limits),
    )
}
fn checked(row: &J, out: Result<(Response, State<Shared, Shared>), aevm::Error<Shared, Shared>>) {
    assert_eq!(
        support::logical(result(out)),
        support::logical(row["result"].clone()),
        "depth={}",
        row["depth"]
    );
}
#[test]
fn deep_aevm_calls_match_original_gas_roots_and_stack_guard_on_small_native_stack() {
    // Parse outside the small worker: this check bounds VM driver stack usage,
    // independently of serde_json's fixture parsing implementation.
    assert_eq!(DEPTH["runs"].as_array().unwrap().len(), 14);
    std::thread::Builder::new()
        .stack_size(512 * 1024)
        .spawn(|| {
            for row in DEPTH["runs"].as_array().unwrap() {
                let control = Arc::new(Control::default());
                let s = chain(row, &control);
                let before = snapshot(&s);
                checked(row, run(row, &s, limits()));
                assert_eq!(snapshot(&s), before);
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
#[test]
fn deep_aevm_limits_and_backend_faults_remain_retryable_and_preserve_roots() {
    let rows = DEPTH["runs"].as_array().unwrap();
    let row = rows.iter().find(|r| r["depth"] == 128).unwrap();
    let control = Arc::new(Control::default());
    let s = chain(row, &control);
    let before = snapshot(&s);
    checked(row, run(row, &s, limits()));
    let writes = control.writes.load(Relaxed);
    assert!(writes > 128);
    for allowed in [0, 1, writes / 2, writes - 1] {
        control.left.store(allowed, Relaxed);
        assert!(matches!(
            run(row, &s, limits()),
            Err(outer_call::Fault::Runtime(Fault::Host(_)))
        ));
        control.left.store(usize::MAX, Relaxed);
        assert_eq!(snapshot(&s), before);
        checked(row, run(row, &s, limits()));
    }
    for (depth, cap) in [(128, 64), (1025, 1024), (1030, 1024)] {
        let row = rows.iter().find(|r| r["depth"] == depth).unwrap();
        let s = chain(row, &Arc::new(Control::default()));
        let before = snapshot(&s);
        assert!(matches!(
            run(
                row,
                &s,
                Limits {
                    max_depth: cap,
                    ..limits()
                }
            ),
            Err(outer_call::Fault::Runtime(Fault::Stop(Stop::ResourceLimit)))
        ));
        assert_eq!(snapshot(&s), before);
        checked(row, run(row, &s, limits()));
    }
}

static STACK: LazyLock<J> = LazyLock::new(|| {
    support::fixture(include_str!(
        "../../fixtures/erlang_aevm_recursive_stack_vectors.json"
    ))
});
#[test]
fn suspended_aevm_frames_preserve_historical_stores_returns_failures_and_repeated_calls() {
    let rows = STACK["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 360);
    for row in rows {
        let s = chain(row, &Arc::new(Control::default()));
        let before = snapshot(&s);
        let actual = result(run(row, &s, limits()));
        assert_eq!(
            support::logical(actual),
            support::logical(row["result"].clone()),
            "protocol={} vm={} case={} depth={}",
            row["protocol"],
            row["vm"],
            row["case_name"],
            row["depth"]
        );
        assert_eq!(snapshot(&s), before);
    }
}
