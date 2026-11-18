use ae_execution::{Error, names::prune};
use ae_protocol::Protocol;
use ae_state::{
    accounts::{Account, Accounts},
    names::{Auction, Commitment, Name, Names},
};
use ae_trie::{MemoryStore, NodeReader, NodeWriter};
use ae_types::{AccountPublicKey, Hash256};
use std::{cell::Cell, rc::Rc};

struct FaultStore {
    inner: MemoryStore,
    remaining: Rc<Cell<usize>>,
}
impl NodeReader for FaultStore {
    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.inner.node(hash)
    }
}
impl NodeWriter for FaultStore {
    fn put_node(&mut self, hash: Hash256, bytes: Vec<u8>) -> Result<(), ae_trie::Error> {
        let n = self.remaining.get();
        if n == 0 {
            return Err(ae_trie::Error::Backend("injected write failure".into()));
        }
        self.remaining.set(n - 1);
        self.inner.put_node(hash, bytes)
    }
}
fn base() -> (Accounts, Names) {
    let mut accounts = Accounts::default();
    accounts
        .enter(&Account::new(AccountPublicKey::ZERO, 99.into()))
        .unwrap();
    let mut names = Names::default();
    names
        .insert_commitment(&Commitment::new(
            [1; 32],
            AccountPublicKey([2; 32]),
            10.into(),
            0.into(),
        ))
        .unwrap();
    names
        .enter_name(&Name::new([3; 32], AccountPublicKey([4; 32]), 10.into()))
        .unwrap();
    names
        .enter_auction(&Auction::new(
            [5; 32],
            AccountPublicKey([6; 32]),
            42.into(),
            10.into(),
            0.into(),
        ))
        .unwrap();
    (accounts, names)
}
#[test]
fn failures_in_each_backend_roll_back_accounts_objects_and_expiry_index() {
    let (original_accounts, original_names) = base();
    let (ar, nr, cr) = (
        original_accounts.root_hash(),
        original_names.root_hash(),
        original_names.cache_root_hash(),
    );
    let mut successes = 0;
    let mut failures = 0;
    for budget in 0..100 {
        let remaining = Rc::new(Cell::new(budget));
        let store = |inner| FaultStore {
            inner,
            remaining: remaining.clone(),
        };
        let mut accounts =
            Accounts::from_root(ar, store(original_accounts.tree().store().clone())).unwrap();
        let mut names = Names::from_roots(
            nr,
            cr,
            store(original_names.tree().store().clone()),
            store(original_names.cache().store().clone()),
        )
        .unwrap();
        let result = prune(&mut accounts, &mut names, 11, Protocol::Ceres);
        if result.is_err() {
            failures += 1;
            assert_eq!(
                (
                    accounts.root_hash(),
                    names.root_hash(),
                    names.cache_root_hash()
                ),
                (ar, nr, cr)
            );
            assert_eq!(
                accounts.lookup(AccountPublicKey::ZERO).unwrap(),
                original_accounts.lookup(AccountPublicKey::ZERO).unwrap()
            );
            assert_eq!(
                names.name([3; 32]).unwrap(),
                original_names.name([3; 32]).unwrap()
            );
            assert_eq!(
                names.tree().iter().collect::<Result<Vec<_>, _>>().unwrap(),
                original_names
                    .tree()
                    .iter()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
            );
            assert_eq!(
                names.cache().iter().collect::<Result<Vec<_>, _>>().unwrap(),
                original_names
                    .cache()
                    .iter()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
            );
        } else {
            successes += 1;
            assert_eq!(
                accounts
                    .lookup(AccountPublicKey::ZERO)
                    .unwrap()
                    .unwrap()
                    .balance(),
                &141.into()
            );
            assert_eq!(names.name([3; 32]).unwrap().unwrap().status, b"revoked");
            assert!(names.commitment([1; 32]).unwrap().is_none());
            assert_eq!(
                names.name([5; 32]).unwrap().unwrap().expires_by,
                180010.into()
            );
        }
    }
    assert!(failures > 10 && successes > 10);
}
#[test]
fn nested_rollbacks_keep_outer_changes_and_outer_failure_reverts_inner_success() {
    let (mut accounts, mut names) = base();
    let roots = (
        accounts.root_hash(),
        names.root_hash(),
        names.cache_root_hash(),
    );
    let result: Result<(), Error> = accounts.transaction(|accounts| {
        names.transaction(|names| {
            names.enter_name(&Name::new([7; 32], AccountPublicKey([8; 32]), 99.into()))?;
            let modified = names.root_hash();
            let inner: Result<(), Error> = names.transaction(|names| {
                names.delete_name([7; 32])?;
                Err(Error::AccountNotFound)
            });
            assert!(inner.is_err());
            assert_eq!(names.root_hash(), modified);
            assert_eq!(prune(accounts, names, 11, Protocol::Ceres)?.len(), 1);
            Err(Error::InsufficientFunds)
        })
    });
    assert_eq!(result, Err(Error::InsufficientFunds));
    assert_eq!(
        (
            accounts.root_hash(),
            names.root_hash(),
            names.cache_root_hash()
        ),
        roots
    );
    assert!(names.name([7; 32]).unwrap().is_none());
}
