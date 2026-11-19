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
fn fixture(op: &str) -> &'static J {
    DATA["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["operation"] == op
                && r["mode"] == "normal"
                && r["dry_run"] == false
                && if op == "aevm_create" {
                    r["protocol"] == 4 && r["context"] == "aetx_contract"
                } else {
                    r["protocol"] == 6 && r["context"] == "aetx_transaction"
                }
        })
        .unwrap()
}
pub(super) fn roots<A: NodeReader, C: NodeReader>(
    t: &Trees<A, C>,
) -> (ae_state::StateRoots, Option<Hash256>, Option<Hash256>) {
    (
        t.roots(),
        t.names.cache_root_hash(),
        t.oracles.cache_root_hash(),
    )
}
#[test]
fn every_read_and_write_failure_prefix_keeps_all_roots_atomic_and_retryable() {
    let mut prefixes = 0;
    for op in [
        "spend",
        "preclaim",
        "claim",
        "update",
        "transfer",
        "revoke",
        "register",
        "extend",
        "query",
        "response",
        "fate_create",
        "fate_call",
        "aevm_create",
        "aevm_call",
    ] {
        let row = fixture(op);
        let input = trees(&row["before"]);
        let before = roots(&input);
        let control = Rc::new(Control::default());
        let base = wrapped(&input, &control);
        control.reset();
        let receipt = run(row, &mut base.clone()).unwrap();
        let counts = [control.reads.get(), control.writes.get()];
        assert!(counts.iter().all(|n| *n > 0));
        assert_eq!(
            receipt.context.payer,
            Some(ae_crypto::Ed25519SecretKey::from_seed(&small(2)).public_key())
        );
        for (dimension, count) in counts.into_iter().enumerate() {
            for permitted in 0..=count {
                control.reset();
                if dimension == 0 {
                    control.read_limit.set(Some(permitted));
                } else {
                    control.write_limit.set(Some(permitted));
                }
                let mut t = base.clone();
                match run(row, &mut t) {
                    Ok(r) => {
                        assert_eq!(permitted, count, "{op} {dimension}");
                        control.reset();
                        check(&t, &r, &row["result"]["value"][1][1]);
                    }
                    Err(e) => {
                        assert!(permitted < count, "{op}: {e:?}");
                        assert!(
                            format!("{e:?}").contains("injected backend failure"),
                            "{op}: {e:?}"
                        );
                        assert_eq!(roots(&t), before, "{op} {dimension} {permitted}");
                    }
                }
                control.reset();
                let mut retry = base.clone();
                let r = run(row, &mut retry).unwrap();
                check(&retry, &r, &row["result"]["value"][1][1]);
                assert_eq!(roots(&base), before);
                prefixes += 1;
            }
        }
    }
    assert!(prefixes > 100);
}
#[test]
fn signature_and_both_vm_operational_stops_never_publish_payer_charges() {
    for op in ["fate_create", "fate_call", "aevm_create", "aevm_call"] {
        let row = fixture(op);
        let input = trees(&row["before"]);
        let before = roots(&input);
        for dimension in 0..3 {
            let mut t = input.clone();
            let mut sig = SignatureLimits::default();
            let mut steps = 1_000_000;
            let mut aevm = ae_vm_aevm::execution::Limits::default();
            match dimension {
                0 => sig.max_layers = 0,
                1 => sig.max_encoded_bytes = 0,
                _ => {
                    if op.starts_with("fate") {
                        steps = 0
                    } else {
                        aevm.max_work = 0
                    }
                }
            }
            let result = execute(row, &mut t, sig, steps, aevm);
            assert!(result.is_err(), "{op} {dimension}");
            if dimension < 2 {
                assert!(matches!(
                    result,
                    Err(Error::Signature(SignatureError::ResourceLimit))
                ));
            }
            assert_eq!(roots(&t), before);
            let r = run(row, &mut t).unwrap();
            check(&t, &r, &row["result"]["value"][1][1]);
        }
    }
}
#[test]
fn deep_signer_derivation_is_iterative_and_budgeted() {
    std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(|| {
            let s = SignedTransaction::<Transaction>::decode(&bytes(&fixture("spend")["signed"]))
                .unwrap();
            let mut tx = Transaction::new(s.transaction().leaf().clone());
            let payer = PayingFor {
                payer: AccountPublicKey::ZERO,
                nonce: 1.into(),
                fee: 1_000_000_000_000u64.into(),
            };
            for _ in 0..1024 {
                tx = tx.wrap(Wrapper::PayingFor(payer.clone()), vec![]);
            }
            let network = NetworkProfile::mainnet();
            let call = |n| {
                transactions::verify(
                    tx.view(),
                    &[],
                    &network,
                    Protocol::Ceres,
                    false,
                    SignatureLimits {
                        max_layers: n,
                        ..SignatureLimits::default()
                    },
                )
            };
            assert_eq!(call(1023), Err(SignatureError::ResourceLimit));
            assert_eq!(call(1024), Err(SignatureError::CheckFailed));
        })
        .unwrap()
        .join()
        .unwrap();
}
