use super::*;
use ae_trie::{MemoryStore, Value as TrieValue};
use ae_types::{AccountPublicKey, Hash256, Id, IdTag};

#[test]
fn imported_node_capacity_is_charged_before_backend_writes() {
    let mut bytes = Vec::with_capacity(4096);
    bytes.extend([0xc2, 0x20, 1]);
    let hash = ae_crypto::blake2b_256(&bytes);
    let required = bytes.capacity() + 128;
    let budget = Budget::new(Limits {
        max_work_bytes: required - 1,
        ..Default::default()
    });
    let mut store = WriteBudget {
        store: MemoryStore::default(),
        budget: &budget,
    };
    assert_eq!(
        store.put_node(hash, bytes),
        Err(ae_trie::Error::ResourceLimit)
    );
    assert!(store.store.nodes().is_empty());

    let budget = Budget::new(Limits {
        max_work_bytes: 4096,
        ..Default::default()
    });
    let mut tree = Trie::new(WriteBudget {
        store: MemoryStore::default(),
        budget: &budget,
    });
    let mut accepted = 0;
    for n in 0..64u8 {
        match tree.put(
            &Nibbles::from_bytes(&[n; 32]),
            TrieValue::Bytes(vec![n + 1; 256]),
        ) {
            Ok(()) => accepted += 1,
            Err(ae_trie::Error::ResourceLimit) => break,
            other => panic!("unexpected insert {other:?}"),
        }
    }
    assert!((1..64).contains(&accepted));
    let allocated: usize = tree
        .store()
        .store
        .nodes()
        .values()
        .map(|v| v.capacity() + 128)
        .sum();
    assert!(allocated <= 4096);
    assert!(budget.bytes.get() < 4096 - allocated);
}

struct Limited {
    store: MemoryStore,
    items: usize,
}
impl NodeReader for Limited {
    fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.store.node(hash)
    }
    fn decode_limits(&self) -> rlp::DecodeLimits {
        rlp::DecodeLimits {
            max_items: self.items,
            ..Default::default()
        }
    }
}

#[test]
fn typed_account_and_call_values_respect_the_source_decode_limits() {
    use crate::{
        accounts::{Account, AccountError, Accounts},
        calls::{Call, Calls, LogEntry},
        contracts,
    };
    let key = AccountPublicKey([7; 32]);
    let mut accounts = Accounts::default();
    let account = Account::new(key, 100.into());
    accounts.enter(&account).unwrap();
    assert_eq!(accounts.lookup(key).unwrap(), Some(account));
    let limited = Accounts::from_root(
        accounts.root_hash(),
        Limited {
            store: accounts.tree().store().clone(),
            items: 3,
        },
    )
    .unwrap();
    assert!(matches!(
        limited.lookup(key),
        Err(AccountError::Object(object::ObjectError::Rlp(
            rlp::DecodeError::ResourceLimit
        )))
    ));

    let mut calls = Calls::default();
    let mut call = Call::new(
        Id {
            tag: IdTag::Account,
            value: key.0,
        },
        1.into(),
        [3; 32],
        None,
        1.into(),
        1.into(),
    );
    call.log = vec![LogEntry {
        address: vec![3; 32],
        topics: vec![vec![]; 64],
        data: vec![],
    }];
    calls.insert(&call).unwrap();
    assert_eq!(calls.lookup(&[3; 32], call.id).unwrap(), Some(call.clone()));
    let limited = Calls::from_root(
        calls.root_hash(),
        Limited {
            store: calls.tree().store().clone(),
            items: 16,
        },
    )
    .unwrap();
    assert!(matches!(
        limited.lookup(&[3; 32], call.id),
        Err(contracts::Error::Object(object::ObjectError::Rlp(
            rlp::DecodeError::ResourceLimit
        )))
    ));
}
