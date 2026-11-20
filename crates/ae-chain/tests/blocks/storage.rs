use super::*;
use ae_trie::NodeWriter;
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

#[test]
fn every_backend_failure_prefix_aborts_the_complete_micro_block_before_publication() {
    let mut prefixes = 0;
    for (protocol, kind) in [
        (6, "spends"),
        (6, "fate_call"),
        (4, "aevm_call"),
        (6, "meta"),
    ] {
        let row = super::fixture(protocol, kind, "micro", "proof");
        let input = trees(&row["before"]);
        let before = roots(&input);
        let control = Rc::new(Control::default());
        let base = wrapped(&input, &control);
        control.reset();
        let mut success = base.clone();
        let receipt = run(row, &mut success).unwrap();
        let counts = [control.reads.get(), control.writes.get()];
        control.reset();
        check(&success, &receipt, row);
        assert!(counts.iter().all(|n| *n > 0));
        for (dimension, count) in counts.into_iter().enumerate() {
            for permitted in 0..count {
                control.reset();
                if dimension == 0 {
                    control.read_limit.set(Some(permitted));
                } else {
                    control.write_limit.set(Some(permitted));
                }
                let mut t = base.clone();
                let e = run(row, &mut t).unwrap_err();
                assert!(e.is_operational(), "{kind} {dimension} {permitted}: {e:?}");
                assert_eq!(roots(&t), before);
                control.reset();
                let receipt = run(row, &mut t).unwrap();
                check(&t, &receipt, row);
                assert_eq!(roots(&base), before);
                prefixes += 1;
            }
        }
    }
    assert!(prefixes > 100);
    eprintln!("Whole micro-block storage failure prefixes: {prefixes}");
}
