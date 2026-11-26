use super::*;
use crate::{
    block::Block,
    repository::{Identity, Roots, Status, retention},
};
use ae_codec::headers::{CommonHeader, KeyHeader};
use ae_protocol::NetworkProfile;
use ae_state::{StateRoots, accounts::Accounts, calls::Calls};
use ae_trie::NodeReader;
use ae_types::Id;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
    sync::Arc,
};

#[derive(Clone, Default)]
struct Memory {
    data: Arc<BTreeMap<Vec<u8>, Vec<u8>>>,
    trace: Rc<RefCell<Vec<Vec<u8>>>>,
    copied: Rc<Cell<usize>>,
    fault: Option<Vec<u8>>,
}
impl Memory {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        Arc::make_mut(&mut self.data).insert(key, value);
    }
    fn remove(&mut self, key: &[u8]) {
        Arc::make_mut(&mut self.data).remove(key);
    }
    fn reader(&self, limits: Limits) -> Reader<Self> {
        let view = View::load(self.clone()).unwrap();
        let reader = Reader::new(&view, limits).unwrap();
        self.trace.borrow_mut().clear();
        self.copied.set(0);
        reader
    }
}
impl NodeReader for Memory {
    fn node(&self, _: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        panic!("unmetered node lookup")
    }
}
impl Read for Memory {
    fn source_id(&self) -> u64 {
        1
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, repo::Error> {
        self.get_bounded(key, usize::MAX)
    }
    fn get_bounded(&self, key: &[u8], limit: usize) -> Result<Option<Vec<u8>>, repo::Error> {
        self.trace.borrow_mut().push(key.to_vec());
        if self.fault.as_deref() == Some(key) {
            return Err(repo::Error::Backend("injected".into()));
        }
        self.data
            .get(key)
            .map(|v| {
                if v.len() > limit {
                    return Err(repo::Error::ResourceLimit);
                }
                self.copied.set(self.copied.get() + v.len());
                Ok(v.clone())
            })
            .transpose()
    }
    fn prefix(&self, _: &[u8], _: usize) -> Result<repo::Entries, repo::Error> {
        panic!("scan")
    }
    fn range(&self, _: &[u8], _: &[u8], _: usize) -> Result<repo::Entries, repo::Error> {
        panic!("scan")
    }
    fn visit_range(
        &self,
        _: &[u8],
        _: &[u8],
        _: usize,
        _: &mut repo::Visitor<'_>,
    ) -> Result<(), repo::Error> {
        panic!("scan")
    }
}
fn record(accounts: Option<Hash256>, calls: Option<Hash256>) -> Record {
    Record {
        roots: Roots {
            state: StateRoots {
                accounts,
                calls,
                ..Default::default()
            },
            names: None,
            oracles: None,
        },
        difficulty: 1.into(),
        fees: 0.into(),
        fraud: false,
        fork: Hash256::ZERO,
    }
}
struct Fixture {
    db: Memory,
    block: Hash256,
    top: Hash256,
    owner: AccountPublicKey,
    auth: Hash256,
    call: Call,
    ga: Call,
}
fn fixture() -> Fixture {
    let owner = AccountPublicKey([1; 32]);
    let auth = Hash256([2; 32]);
    let mut accounts = Accounts::default();
    let mut account = Account::new(owner, 100.into());
    account
        .attach_authorization(Id::new(IdTag::Contract, [3; 32]), [4; 32])
        .unwrap();
    accounts.enter(&account).unwrap();
    let mut calls = Calls::default();
    let mut call = Call::new(
        Id::new(IdTag::Account, owner.0),
        7.into(),
        [5; 32],
        None,
        77.into(),
        9.into(),
    );
    call.return_value = vec![10; 1024];
    calls.insert(&call).unwrap();
    let mut ga = call.clone();
    ga.contract = [3; 32];
    ga.id = ga_call_id(auth.0, ga.contract).0;
    calls.enter_auth(&ga).unwrap();
    // The historical account is absent; authorization is deliberately only in
    // the current top. These are real hashed MPT nodes, not mocked call objects.
    let historic = record(None, calls.root_hash());
    let current = record(accounts.root_hash(), None);
    let block = Block::Key(KeyHeader {
        common: CommonHeader {
            version: 1,
            height: 77,
            previous_hash: [0; 32],
            previous_key_hash: [0; 32],
            state_hash: historic.roots.state.hash().0,
        },
        miner: [0; 32],
        beneficiary: [0; 32],
        target: 0x2100ffff,
        seal: [0; 42],
        nonce: 0,
        time: 0,
        info: None,
        hole: false,
        end_of_epoch: false,
    });
    let hash = block.hash().unwrap();
    let top = Hash256([99; 32]);
    let mut db = Memory::default();
    db.put(
        repo::IDENTITY.to_vec(),
        Identity::new(&NetworkProfile::mainnet(), hash, Hash256::ZERO).encode(),
    );
    db.put(
        repo::STATUS.to_vec(),
        Status {
            revision: 1,
            tip: Some(top),
        }
        .encode(),
    );
    db.put(repo::key(repo::BLOCKS, hash.0), block.encode().unwrap());
    db.put(repo::key(repo::RECORDS, hash.0), historic.encode());
    db.put(repo::key(repo::RECORDS, top.0), current.encode());
    for (hash, bytes) in accounts
        .tree()
        .store()
        .nodes()
        .iter()
        .chain(calls.tree().store().nodes())
    {
        db.put(repo::key(repo::NODES, hash.0), bytes.clone());
    }
    Fixture {
        db,
        block: hash,
        top,
        owner,
        auth,
        call,
        ga,
    }
}
#[test]
fn stored_call_reads_pin_current_authorization_and_historical_call_roots() {
    let mut f = fixture();
    let mut held = f.db.reader(Default::default());
    assert_eq!(
        held.contract(f.call.contract, Hash256(f.call.id), f.block),
        Ok(f.call.clone())
    );
    assert_eq!(
        held.ga(f.owner, f.auth, f.block),
        Ga::from_call(f.ga.clone())
    );
    f.db.put(
        repo::key(repo::RECORDS, f.top.0),
        record(None, None).encode(),
    );
    // A newer immutable snapshot has no account; the held reader is unchanged.
    assert_eq!(
        f.db.reader(Default::default()).ga(f.owner, f.auth, f.block),
        Err(Error::InvalidObject)
    );
    assert_eq!(held.ga(f.owner, f.auth, f.block), Ga::from_call(f.ga));
}
#[test]
fn native_header_gc_state_and_call_absences_keep_their_read_order() {
    let f = fixture();
    for mode in 0..5 {
        let mut db = f.db.clone();
        let header = repo::key(repo::BLOCKS, f.block.0);
        let state = repo::key(repo::RECORDS, f.block.0);
        let expected = match mode {
            0 => {
                db.remove(&header);
                None
            }
            1 => {
                db.fault = Some(repo::GC_STATE.to_vec());
                None
            }
            2 => {
                db.put(
                    repo::GC_STATE.to_vec(),
                    retention::State::default()
                        .start(78, Default::default())
                        .unwrap()
                        .encode(),
                );
                Some(Error::GarbageCollected)
            }
            3 => {
                db.remove(&state);
                Some(Error::NoStateTrees)
            }
            _ => Some(Error::CallNotFound),
        };
        let result =
            db.reader(Default::default())
                .contract(f.call.contract, Hash256::ZERO, f.block);
        if let Some(error) = expected {
            assert_eq!(result, Err(error));
        } else {
            assert!(matches!(result, Err(Error::Backend(_))));
        }
        let trace = db.trace.borrow().clone();
        assert_eq!(trace[0], header);
        if mode == 0 {
            assert_eq!(trace.len(), 1)
        } else {
            assert_eq!(trace[1], repo::GC_STATE);
            if mode <= 2 {
                assert_eq!(trace.len(), 2)
            } else {
                assert_eq!(trace[2], state)
            }
        }
    }
    let mut db = f.db;
    db.remove(&repo::key(repo::BLOCKS, f.block.0));
    db.put(repo::GC_STATE.to_vec(), vec![0xff]);
    assert_eq!(
        db.reader(Default::default()).ga(f.owner, f.auth, f.block),
        Ga::from_call(f.ga)
    );
    assert!(
        !db.trace
            .borrow()
            .iter()
            .any(|k| k == repo::GC_STATE || k[0] == repo::BLOCKS)
    );
}
#[test]
fn call_reads_enforce_shared_credit_before_copy_and_reject_corrupt_nodes() {
    let f = fixture();
    let baseline =
        f.db.reader(Default::default())
            .contract(f.call.contract, Hash256(f.call.id), f.block)
            .unwrap();
    let reads = f.db.trace.borrow().len();
    let copied = f.db.copied.get();
    assert_eq!(baseline, f.call);
    for limits in [
        Limits {
            reads: 2,
            ..Default::default()
        },
        Limits {
            node_bytes: 1,
            ..Default::default()
        },
        Limits {
            read_bytes: 300,
            ..Default::default()
        },
    ] {
        let mut reader = f.db.reader(limits);
        assert!(matches!(
            reader.contract(f.call.contract, Hash256(f.call.id), f.block),
            Err(Error::Backend(_))
        ));
        assert!(f.db.copied.get() < copied);
    }
    let mut limited = f.db.reader(Limits {
        reads: reads + 2,
        ..Default::default()
    });
    assert_eq!(
        limited.contract(f.call.contract, Hash256(f.call.id), f.block),
        Ok(f.call.clone())
    );
    assert_eq!(
        limited.contract(f.call.contract, Hash256(f.call.id), f.block),
        Err(Error::Backend(repo::Error::ResourceLimit))
    );
    let mut bad = f.db.clone();
    let root = Record::decode(bad.data.get(&repo::key(repo::RECORDS, f.block.0)).unwrap())
        .unwrap()
        .roots
        .state
        .calls
        .unwrap();
    bad.put(repo::key(repo::NODES, root.0), vec![0xc0]);
    assert!(matches!(
        bad.reader(Default::default())
            .contract(f.call.contract, Hash256(f.call.id), f.block),
        Err(Error::Backend(repo::Error::Trie(_)))
    ));
    let mut bad = f.db;
    bad.put(
        repo::key(repo::RECORDS, f.block.0),
        record(None, None).encode(),
    );
    assert_eq!(
        bad.reader(Default::default())
            .contract(f.call.contract, Hash256(f.call.id), f.block),
        Err(Error::Backend(repo::Error::Corrupt("call state binding")))
    );
}
