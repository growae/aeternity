use ae_crypto::Ed25519SecretKey;
use ae_execution::{Context, Error, execute_signed_spend, process_spend};
use ae_protocol::{NetworkProfile, Protocol};
use ae_state::accounts::{Account, Accounts};
use ae_trie::{MemoryStore, NodeReader, NodeWriter, Trie};
use ae_tx::{CanonicalTransaction, SignedTransaction, Spend};
use ae_types::{AccountPublicKey, Hash256, Id, IdTag};
use std::{cell::Cell, rc::Rc};

struct FaultStore {
    inner: MemoryStore,
    remaining: Rc<Cell<usize>>,
}
impl NodeReader for FaultStore {
    fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.inner.node(h)
    }
}
impl NodeWriter for FaultStore {
    fn put_node(&mut self, h: Hash256, b: Vec<u8>) -> Result<(), ae_trie::Error> {
        if self.remaining.get() == 0 {
            return Err(ae_trie::Error::Backend("injected write failure".into()));
        }
        self.remaining.set(self.remaining.get() - 1);
        self.inner.put_node(h, b)
    }
}
#[test]
fn backend_failures_do_not_publish_partial_account_updates() {
    let sender = AccountPublicKey([1; 32]);
    let recipient = AccountPublicKey([2; 32]);
    let mut base = Accounts::default();
    base.enter(&Account::new(sender, 1_000_000_000_000u64.into()))
        .unwrap();
    base.enter(&Account::new(recipient, 7.into())).unwrap();
    let root = base.root_hash();
    let mut tx = Spend::new(sender, Id::new(IdTag::Account, recipient.0)).unwrap();
    tx.set_fee(100_000_000_000u64.into());
    tx.set_amount(17.into());
    tx.set_nonce(1.into());
    let mut failures = 0;
    let mut successes = 0;
    for budget in 0..20 {
        let remaining = Rc::new(Cell::new(budget));
        let store = FaultStore {
            inner: base.tree().store().clone(),
            remaining,
        };
        let mut accounts = Accounts::from_root(root, store).unwrap();
        let result = process_spend(
            &tx,
            &mut accounts,
            &Trie::default(),
            &Context::transaction(1, Protocol::Ceres),
        );
        if result.is_err() {
            failures += 1;
            assert_eq!(accounts.root_hash(), root);
            assert_eq!(
                accounts.lookup(sender).unwrap(),
                base.lookup(sender).unwrap()
            );
            assert_eq!(
                accounts.lookup(recipient).unwrap(),
                base.lookup(recipient).unwrap()
            );
        } else {
            successes += 1;
            assert_eq!(
                accounts.lookup(recipient).unwrap().unwrap().balance(),
                &24.into()
            );
        }
    }
    assert!(failures > 1 && successes > 1);
}
#[test]
fn signature_failure_and_signed_success_have_atomic_state_and_hash() {
    let key = Ed25519SecretKey::from_seed(&[7; 32]);
    let sender = key.public_key();
    let network = NetworkProfile::mainnet();
    let mut accounts = Accounts::default();
    accounts
        .enter(&Account::new(sender, 1_000_000_000_000u64.into()))
        .unwrap();
    let mut tx = Spend::new(sender, Id::new(IdTag::Account, [8; 32])).unwrap();
    tx.set_nonce(1.into());
    tx.set_amount(100.into());
    tx.set_fee(100_000_000_000u64.into());
    let mut payload = network.signing_prefix(false);
    payload.extend_from_slice(&tx.encode());
    let signed = SignedTransaction::new(tx.clone(), vec![key.sign(&payload)]);
    let context = Context::transaction(1, Protocol::Ceres);
    let root = accounts.root_hash();
    assert!(matches!(
        execute_signed_spend(
            &signed,
            &mut accounts,
            &Trie::default(),
            &NetworkProfile::uat(),
            &context
        ),
        Err(Error::Signature(_))
    ));
    assert_eq!(accounts.root_hash(), root);
    let receipt =
        execute_signed_spend(&signed, &mut accounts, &Trie::default(), &network, &context).unwrap();
    assert_eq!(receipt.transaction_hash, Some(signed.hash()));
    assert_eq!(
        accounts
            .lookup(AccountPublicKey([8; 32]))
            .unwrap()
            .unwrap()
            .balance(),
        &100.into()
    );
}
