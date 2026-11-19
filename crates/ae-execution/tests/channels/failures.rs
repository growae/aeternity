use super::*;
use ae_state::{calls::Calls, names::Names, oracles::Oracles};
use ae_trie::{MemoryStore, NodeWriter};
use ae_types::Hash256;
use std::{cell::Cell, rc::Rc};
#[derive(Debug, Default)]
pub(super) struct Control {
    pub(super) reads: Cell<usize>,
    pub(super) writes: Cell<usize>,
    pub(super) read_limit: Cell<Option<usize>>,
    pub(super) write_limit: Cell<Option<usize>>,
}
impl Control {
    pub(super) fn reset(&self) {
        self.reads.set(0);
        self.writes.set(0);
        self.read_limit.set(None);
        self.write_limit.set(None);
    }
    fn visit(count: &Cell<usize>, limit: &Cell<Option<usize>>) -> Result<(), ae_trie::Error> {
        let n = count.get();
        if limit.get() == Some(n) {
            return Err(ae_trie::Error::Backend("injected backend failure".into()));
        }
        count.set(n + 1);
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub(super) struct Store {
    store: MemoryStore,
    control: Rc<Control>,
}
impl NodeReader for Store {
    fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        Control::visit(&self.control.reads, &self.control.read_limit)?;
        self.store.node(h)
    }
}
impl NodeWriter for Store {
    fn put_node(&mut self, h: Hash256, b: Vec<u8>) -> Result<(), ae_trie::Error> {
        Control::visit(&self.control.writes, &self.control.write_limit)?;
        self.store.put_node(h, b)
    }
}
pub(super) fn wrapped(t: &Trees, c: &Rc<Control>) -> Trees<Store, Store> {
    let wrap = |t: &Trie| Store {
        store: t.store().clone(),
        control: c.clone(),
    };
    Trees {
        accounts: Accounts::from_root(t.accounts.root_hash(), wrap(t.accounts.tree())).unwrap(),
        contracts: Contracts::from_root(t.contracts.root_hash(), wrap(t.contracts.tree())).unwrap(),
        calls: Calls::from_root(t.calls.root_hash(), wrap(t.calls.tree())).unwrap(),
        channels: ae_state::channels::Channels::from_root(
            t.channels.root_hash(),
            wrap(t.channels.tree()),
        )
        .unwrap(),
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
    }
}
fn roots<A: NodeReader, C: NodeReader>(
    t: &Trees<A, C>,
) -> (ae_state::StateRoots, Option<Hash256>, Option<Hash256>) {
    (
        t.roots(),
        t.names.cache_root_hash(),
        t.oracles.cache_root_hash(),
    )
}
fn check<A: NodeReader, C: NodeReader>(
    t: &Trees<A, C>,
    r: &Receipt,
    s: &SignedTransaction<Transaction>,
    expected: &J,
) {
    let actual = snapshot(t, r, Some(s));
    for key in [
        "root",
        "accounts",
        "channels",
        "contracts",
        "events",
        "context",
        "payer",
    ] {
        if let Some(d) = difference(
            &normalize(field(&actual, key)),
            &normalize(field(expected, key)),
            key,
        ) {
            panic!("{d}");
        }
    }
}
#[test]
fn channel_backend_failure_prefixes_keep_all_roots_atomic_and_retryable() {
    let network = NetworkProfile::mainnet();
    let config = Config::primitives(&network);
    let mut prefixes = 0;
    for op in ["create", "deposit", "withdraw", "mutual", "settle"] {
        // Direct ledger and both real authorization VMs; sponsored deposits,
        // withdrawals and closures also stage the outer payer's account.
        let mut cases: Vec<(&J, bool)> = DATA["runs"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["operation"] == op && r["protocol"] == 6 && r["label"] == atom("normal"))
            .map(|r| (r, false))
            .collect();
        for abi in [1, 3] {
            for mode in if op == "create" {
                &["both", "repeated", "copy_marker", "copy_entry"][..]
            } else if op == "settle" {
                &["initiator", "payer_ga"][..]
            } else {
                &["both", "payer_ga"][..]
            } {
                cases.push((
                    wrapped::WRAPPED["runs"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|r| {
                            r["operation"] == op
                                && r["protocol"] == 6
                                && r["abi"] == abi
                                && r["mode"] == *mode
                        })
                        .unwrap(),
                    true,
                ));
            }
        }
        for (row, is_wrapped) in cases {
            let signed = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
            let context = Context::transaction(50, Protocol::Ceres);
            let input = initial(row);
            let before = roots(&input);
            let control = Rc::new(Control::default());
            let base = wrapped(&input, &control);
            let execute = |t: &mut Trees<Store, Store>| {
                if is_wrapped {
                    wrapped::run(&signed, t, &context)
                } else {
                    transactions::execute_signed(&signed, t, &context, &config)
                }
            };
            control.reset();
            let expected = execute(&mut base.clone()).unwrap();
            let counts = [control.reads.get(), control.writes.get()];
            for (dimension, count) in counts.into_iter().enumerate() {
                assert!(count > 0);
                for permitted in 0..=count {
                    control.reset();
                    if dimension == 0 {
                        control.read_limit.set(Some(permitted))
                    } else {
                        control.write_limit.set(Some(permitted))
                    }
                    let mut t = base.clone();
                    match execute(&mut t) {
                        Ok(r) => {
                            assert_eq!(permitted, count, "{op} {} {}", row["abi"], row["mode"]);
                            control.reset();
                            check(&t, &r, &signed, &row["result"]["value"][1][1]);
                            assert_eq!(r.events, expected.events);
                        }
                        Err(e) => {
                            assert!(permitted < count);
                            assert!(
                                format!("{e:?}").contains("injected backend failure"),
                                "{op} {} {}: {e:?}",
                                row["abi"],
                                row["mode"]
                            );
                            assert_eq!(roots(&t), before);
                        }
                    }
                    control.reset();
                    let mut retry = base.clone();
                    let r = execute(&mut retry).unwrap_or_else(|e| panic!("retry {op}: {e:?}"));
                    check(&retry, &r, &signed, &row["result"]["value"][1][1]);
                    assert_eq!(roots(&base), before);
                    prefixes += 1;
                }
            }
        }
    }
    eprintln!("channel backend failure prefixes: {prefixes}");
    assert!(prefixes > 500);
}
#[test]
fn channel_envelope_budgets_abort_atomically_and_allow_larger_retries() {
    let network = NetworkProfile::mainnet();
    let mut retries = 0;
    for row in DATA["runs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["protocol"] == 6 && r["label"] == atom("normal"))
    {
        let s = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
        let context = context(row);
        let base = initial(row);
        let exact = s.encode().len();
        for limit in [0, s.transaction().view().wire_size(), exact - 1, exact] {
            let mut t = base.clone();
            let mut config = Config::primitives(&network);
            config.signature_limits.max_encoded_bytes = limit;
            let result = transactions::process_with_signed_source(
                s.transaction(),
                &mut t,
                &context,
                &config,
                Some(&s),
            );
            if limit < exact {
                assert!(
                    matches!(result,Err(Error::Channel(ref e)) if **e==ChannelError::ResourceLimit)
                );
                assert_eq!(roots(&t), roots(&base));
            } else {
                assert!(result.is_ok());
            }
            let mut t = base.clone();
            config.signature_limits.max_encoded_bytes = usize::MAX;
            let r = transactions::process_with_signed_source(
                s.transaction(),
                &mut t,
                &context,
                &config,
                Some(&s),
            )
            .unwrap();
            check(&t, &r, &s, &row["result"]["value"][1][1]);
            retries += 1;
        }
    }
    for row in wrapped::WRAPPED["runs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| {
            r["protocol"] == 6
                && [
                    if r["operation"] == "settle" {
                        "initiator"
                    } else {
                        "both"
                    },
                    "payer_ga",
                ]
                .contains(&r["mode"].as_str().unwrap())
        })
    {
        let s = SignedTransaction::<Transaction>::decode(&bytes(&row["signed"])).unwrap();
        let base = initial(row);
        let c = Context::transaction(50, Protocol::Ceres);
        for limit in [0, 1, 32] {
            let mut t = base.clone();
            let limits = SignatureLimits {
                max_encoded_bytes: limit,
                ..SignatureLimits::default()
            };
            assert!(wrapped::execute(&s, &mut t, &c, limits).is_err());
            assert_eq!(roots(&t), roots(&base));
            let r = wrapped::run(&s, &mut t, &c).unwrap();
            check(&t, &r, &s, &row["result"]["value"][1][1]);
            retries += 1;
        }
    }
    eprintln!("channel envelope budget retries: {retries}");
    assert_eq!(retries, 80);
}
