use super::*;
use crate::{
    block::{Block, MicroBlock},
    repository::{Identity, Roots, Status, retention},
};
use ae_codec::{
    api,
    headers::{CommonHeader, KeyHeader, MicroHeader},
    sophia::{FunctionInfo, SophiaCode},
};
use ae_protocol::{NetworkProfile, contracts::Version};
use ae_state::{
    StateRoots,
    accounts::Accounts,
    contracts::{Contracts, Nonce},
};
use ae_trie::{MemoryStore, NodeReader};
use ae_types::{Id, Unsigned};
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
    fn nodes(&mut self, store: &MemoryStore) {
        for (hash, bytes) in store.nodes() {
            self.put(repo::key(repo::NODES, hash.0), bytes.clone());
        }
    }
    fn reader(&self, limits: Limits, gc_enabled: bool) -> Reader<Self> {
        let view = View::load(self.clone()).unwrap();
        let reader = Reader::new(&view, limits, gc_enabled).unwrap();
        self.trace.borrow_mut().clear();
        self.copied.set(0);
        reader
    }
    fn top(&mut self, hash: Hash256) {
        self.put(
            repo::STATUS.to_vec(),
            Status {
                revision: 1,
                tip: Some(hash),
            }
            .encode(),
        );
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
    fn get_bounded(&self, key: &[u8], bytes: usize) -> Result<Option<Vec<u8>>, repo::Error> {
        self.trace.borrow_mut().push(key.to_vec());
        if self.fault.as_deref() == Some(key) {
            return Err(repo::Error::Backend("injected".into()));
        }
        self.data
            .get(key)
            .map(|value| {
                if value.len() > bytes {
                    return Err(repo::Error::ResourceLimit);
                }
                self.copied.set(self.copied.get() + value.len());
                Ok(value.clone())
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
fn state(
    db: &mut Memory,
    height: u64,
    micro: bool,
    accounts: &[Account],
    contracts: &[Contract],
) -> Hash256 {
    let mut a = Accounts::default();
    for account in accounts {
        a.enter(account).unwrap();
    }
    let mut c = Contracts::default();
    for contract in contracts {
        c.insert(contract, &Default::default()).unwrap();
    }
    db.nodes(a.tree().store());
    db.nodes(c.tree().store());
    let record = Record {
        roots: Roots {
            state: StateRoots {
                accounts: a.root_hash(),
                contracts: c.root_hash(),
                ..Default::default()
            },
            names: None,
            oracles: None,
        },
        difficulty: 1.into(),
        fees: 0.into(),
        fraud: false,
        fork: Hash256::ZERO,
    };
    let common = CommonHeader {
        version: 1,
        height,
        previous_hash: [0; 32],
        previous_key_hash: [0; 32],
        state_hash: record.roots.state.hash().0,
    };
    let block = if micro {
        Block::Micro(MicroBlock {
            header: MicroHeader {
                common,
                transactions_hash: [0; 32],
                time: 1,
                proof_of_fraud: None,
                signature: [0; 64],
            },
            transactions: vec![],
            proof: None,
        })
    } else {
        Block::Key(KeyHeader {
            common,
            miner: [0; 32],
            beneficiary: [0; 32],
            target: 0x2100ffff,
            seal: [0; 42],
            nonce: 0,
            time: 0,
            info: None,
            hole: false,
            end_of_epoch: false,
        })
    };
    let hash = block.hash().unwrap();
    db.put(repo::key(repo::BLOCKS, hash.0), block.encode().unwrap());
    db.put(repo::key(repo::RECORDS, hash.0), record.encode());
    hash
}
fn initialize(db: &mut Memory, genesis: Hash256, top: Hash256) {
    db.put(
        repo::IDENTITY.to_vec(),
        Identity::new(&NetworkProfile::mainnet(), genesis, Hash256::ZERO).encode(),
    );
    db.top(top);
}
fn account(balance: u64) -> Account {
    Account::new(AccountPublicKey([1; 32]), balance.into())
}
fn contract(key: [u8; 32], code: Code, abi: u16) -> Contract {
    let mut contract = Contract::new(
        AccountPublicKey([2; 32]),
        Nonce::Number(&1.into()),
        Version {
            vm: if abi == 1 { 4 } else { 5 },
            abi,
        },
        code,
        0.into(),
    )
    .unwrap();
    contract.public_key = key;
    contract
}
fn code(name: &[u8]) -> Vec<u8> {
    SophiaCode::from_source(
        1,
        b"",
        vec![],
        vec![FunctionInfo {
            hash: vec![4; 32],
            name: name.into(),
            payable: None,
            arguments: vec![],
            returns: vec![],
        }],
    )
    .encode()
    .unwrap()
}

#[test]
fn account_reads_select_key_height_tip_and_noncanonical_hash_from_a_pinned_view() {
    let mut db = Memory::default();
    let key = state(&mut db, 7, false, &[account(100)], &[]);
    let top = state(&mut db, 7, true, &[account(90)], &[]);
    let fork = state(&mut db, 7, true, &[account(80)], &[]);
    initialize(&mut db, key, top);
    db.put(repo::key(repo::KEYS, 7u64.to_be_bytes()), key.0.to_vec());
    let held = db.reader(Default::default(), true);
    let owner = account(0).public_key();
    assert_eq!(held.account(owner, Selection::Height(7)), Ok(account(100)));
    assert_eq!(held.account(owner, Selection::Current), Ok(account(90)));
    assert_eq!(held.account(owner, Selection::Hash(fork)), Ok(account(80)));
    assert_eq!(
        held.account(owner, Selection::Height(u128::from(u64::MAX) + 1)),
        Err(Error::HeightNotAvailable)
    );
    db.top(fork);
    assert_eq!(held.account(owner, Selection::Current), Ok(account(90)));
    assert_eq!(
        db.reader(Default::default(), true)
            .account(owner, Selection::Current),
        Ok(account(80))
    );
    assert!(
        db.trace
            .borrow()
            .iter()
            .all(|key| !matches!(key[0], repo::TRANSACTIONS | repo::ENDS | repo::CANONICAL))
    );
}

#[test]
fn height_gc_precedes_lookup_but_hash_and_current_ignore_the_gc_floor() {
    let mut db = Memory::default();
    let hash = state(&mut db, 7, false, &[account(100)], &[]);
    initialize(&mut db, hash, hash);
    db.put(
        repo::GC_STATE.to_vec(),
        retention::State::default()
            .start(8, Default::default())
            .unwrap()
            .encode(),
    );
    let owner = account(0).public_key();
    let reader = db.reader(Default::default(), true);
    assert_eq!(
        reader.account(owner, Selection::Height(7)),
        Err(Error::GarbageCollected)
    );
    assert_eq!(*db.trace.borrow(), vec![repo::GC_STATE.to_vec()]);
    // This hash has no canonical-height index; native hash lookup accepts it.
    assert_eq!(
        reader.account(owner, Selection::Hash(hash)),
        Ok(account(100))
    );
    db.put(repo::GC_STATE.to_vec(), vec![0xff]);
    let reader = db.reader(Default::default(), true);
    assert_eq!(reader.account(owner, Selection::Current), Ok(account(100)));
    assert!(reader.account(owner, Selection::Height(7)).is_err());
    db.put(repo::key(repo::KEYS, 7u64.to_be_bytes()), hash.0.to_vec());
    assert_eq!(
        db.reader(Default::default(), false)
            .account(owner, Selection::Height(7)),
        Ok(account(100))
    );
    assert!(!db.trace.borrow().iter().any(|key| key == repo::GC_STATE));
}

#[test]
fn missing_state_account_and_corrupt_or_unavailable_nodes_stay_distinct() {
    let mut db = Memory::default();
    let hash = state(&mut db, 7, false, &[account(100)], &[]);
    initialize(&mut db, hash, hash);
    let owner = account(0).public_key();
    let reader = db.reader(Default::default(), true);
    assert_eq!(
        reader.account(owner, Selection::Hash(Hash256::ZERO)),
        Err(Error::NoStateTrees)
    );
    assert_eq!(
        reader.account(AccountPublicKey([9; 32]), Selection::Current),
        Err(Error::AccountNotFound)
    );
    let record_key = repo::key(repo::RECORDS, hash.0);
    let mut record = Record::decode(db.data.get(&record_key).unwrap()).unwrap();
    let root = record.roots.state.accounts.unwrap();
    for corrupt in [false, true] {
        let mut broken = db.clone();
        if corrupt {
            broken.put(repo::key(repo::NODES, root.0), vec![0xc0]);
        } else {
            broken.remove(&repo::key(repo::NODES, root.0));
        }
        assert!(matches!(
            broken
                .reader(Default::default(), true)
                .account(owner, Selection::Current),
            Err(Error::Backend(repo::Error::Trie(_)))
        ));
    }
    let mut broken = db.clone();
    broken.fault = Some(repo::key(repo::NODES, root.0));
    assert!(matches!(
        broken
            .reader(Default::default(), true)
            .account(owner, Selection::Current),
        Err(Error::Backend(_))
    ));
    record.roots.state.accounts = None;
    db.put(record_key, record.encode());
    assert_eq!(
        db.reader(Default::default(), true)
            .account(owner, Selection::Current),
        Err(Error::Backend(repo::Error::Corrupt(
            "account state binding"
        )))
    );
}

#[test]
fn generalized_history_resolves_current_code_once_and_preserves_snapshot_ownership() {
    let mut db = Memory::default();
    let mut ga = account(100);
    ga.attach_authorization(Id::new(IdTag::Contract, [3; 32]), [4; 32])
        .unwrap();
    let old = contract([3; 32], Code::Inline(code(b"old")), 1);
    let new = contract([3; 32], Code::Inline(code(b"current")), 1);
    let historic = state(&mut db, 7, false, std::slice::from_ref(&ga), &[old]);
    let top = state(&mut db, 8, false, &[], &[new]);
    initialize(&mut db, historic, top);
    let held = db.reader(Default::default(), true);
    let result = held
        .describe(
            held.account(ga.public_key(), Selection::Hash(historic))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(result.auth_fun, Some(b"current".to_vec()));
    assert_eq!(result.account, ga);
    let empty = state(&mut db, 9, false, &[], &[]);
    db.top(empty);
    let fresh = db.reader(Default::default(), true);
    // Lookup alone succeeds: nonce/pending consumers do not need GA code.
    assert_eq!(
        fresh.account(ga.public_key(), Selection::Hash(historic)),
        Ok(ga.clone())
    );
    assert_eq!(fresh.describe(ga.clone()), Err(Error::InvalidObject));
    db.remove(&repo::key(repo::RECORDS, empty.0));
    assert_eq!(
        db.reader(Default::default(), true).describe(ga.clone()),
        Err(Error::InvalidObject)
    );
    assert_eq!(
        held.describe(ga).unwrap().auth_fun,
        Some(b"current".to_vec())
    );
    let reader = db.reader(Default::default(), true);
    assert_eq!(reader.describe(account(5)).unwrap().auth_fun, None);
    assert!(db.trace.borrow().is_empty());
}

#[test]
fn generalized_contract_code_follows_one_reference_using_the_original_contract_abi() {
    let mut db = Memory::default();
    let mut ga = account(100);
    ga.attach_authorization(Id::new(IdTag::Contract, [3; 32]), [4; 32])
        .unwrap();
    let mut primary = contract(
        [3; 32],
        Code::Reference(Id::new(IdTag::Contract, [5; 32])),
        3,
    );
    // Stored version 2 permits a reference; ABI remains that of the primary.
    primary.version.abi = 1;
    primary.object_version = 2;
    let target = contract([5; 32], Code::Inline(code(b"referenced")), 3);
    let hash = state(&mut db, 7, false, &[], &[primary.clone(), target]);
    initialize(&mut db, hash, hash);
    assert_eq!(
        db.reader(Default::default(), true)
            .describe(ga.clone())
            .unwrap()
            .auth_fun,
        Some(b"referenced".to_vec())
    );
    let mut target = primary.clone();
    target.public_key = [5; 32];
    target.code = Code::Reference(Id::new(IdTag::Contract, [3; 32]));
    let hash = state(&mut db, 8, false, &[], &[primary, target]);
    db.top(hash);
    assert_eq!(
        db.reader(Default::default(), true).describe(ga),
        Err(Error::InvalidObject)
    );
}

#[test]
fn account_reads_bound_shared_bytes_nodes_and_rlp_before_copying() {
    let mut db = Memory::default();
    let hash = state(&mut db, 7, false, &[account(100)], &[]);
    initialize(&mut db, hash, hash);
    let owner = account(0).public_key();
    db.reader(Default::default(), true)
        .account(owner, Selection::Current)
        .unwrap();
    let normal_bytes = db.copied.get();
    let reads = db.trace.borrow().len();
    for reads_limit in [
        point::Limits {
            reads: 2,
            ..Default::default()
        },
        point::Limits {
            node_bytes: 1,
            ..Default::default()
        },
        point::Limits {
            read_bytes: 300,
            ..Default::default()
        },
        point::Limits {
            rlp: DecodeLimits {
                max_items: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    ] {
        let reader = db.reader(
            Limits {
                reads: reads_limit,
                ..Default::default()
            },
            true,
        );
        assert_eq!(
            reader.account(owner, Selection::Current),
            Err(Error::ResourceLimit)
        );
        assert!(db.copied.get() < normal_bytes);
    }
    let reader = db.reader(
        Limits {
            reads: point::Limits {
                reads: reads + 2,
                ..Default::default()
            },
            ..Default::default()
        },
        true,
    );
    assert_eq!(reader.account(owner, Selection::Current), Ok(account(100)));
    assert_eq!(
        reader.account(owner, Selection::Current),
        Err(Error::ResourceLimit)
    );
}

#[test]
fn original_account_objects_and_ga_names_match_all_selected_native_successes_and_missing_symbols() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_account_client.json"
    ))
    .unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 71);
    let mut counts = [0; 3];
    for row in rows {
        let mut owner = AccountPublicKey([0; 32]);
        owner.0[31] = 1;
        let account = Account::deserialize(
            owner,
            &hex::decode(row["account"].as_str().unwrap()).unwrap(),
        )
        .unwrap();
        let mut db = Memory::default();
        let old = state(&mut db, 7, false, std::slice::from_ref(&account), &[]);
        let contracts = account
            .authorization()
            .map(|auth| {
                let abi = if row["input"]["kind"] == "aevm" { 1 } else { 3 };
                vec![contract(
                    auth.contract.value,
                    Code::Inline(hex::decode(row["contract_code"].as_str().unwrap()).unwrap()),
                    abi,
                )]
            })
            .unwrap_or_default();
        let top = state(&mut db, 8, false, &[], &contracts);
        initialize(&mut db, old, top);
        let reader = db.reader(Default::default(), true);
        let account = reader.account(owner, Selection::Hash(old)).unwrap();
        let result = reader.describe(account);
        if row["code"] == 500 {
            assert_eq!(
                result,
                Err(Error::InvalidObject),
                "row {}",
                row["source_row"]
            );
            counts[2] += 1;
            continue;
        }
        let info = result.unwrap();
        let expected = &row["body"];
        assert_eq!(
            api::encode_id(Id::new(IdTag::Account, owner.0)),
            expected["id"]
        );
        assert_eq!(
            *info.account.balance(),
            Unsigned::from_decimal(&expected["balance"].to_string()).unwrap()
        );
        assert_eq!(
            *info.account.nonce(),
            Unsigned::from_decimal(&expected["nonce"].to_string()).unwrap()
        );
        assert_eq!(
            info.account.is_payable(),
            expected["payable"].as_bool().unwrap()
        );
        if let Some(auth) = info.account.authorization() {
            assert_eq!(expected["kind"], "generalized");
            assert_eq!(api::encode_id(auth.contract), expected["contract_id"]);
            assert_eq!(
                info.auth_fun.unwrap(),
                expected["auth_fun"].as_str().unwrap().as_bytes()
            );
            counts[1] += 1;
        } else {
            assert_eq!(expected["kind"], "basic");
            assert_eq!(info.auth_fun, None);
            counts[0] += 1;
        }
    }
    assert_eq!(counts, [35, 30, 6]);
}

#[test]
fn auth_function_decoding_preserves_hash_width_rules_and_operational_limits() {
    let bytes = code(b"authorize");
    assert_eq!(
        authentication_name(1, &[4; 32], &bytes, Default::default()),
        Ok(b"authorize".to_vec())
    );
    assert_eq!(
        authentication_name(1, &[4; 4], &bytes, Default::default()),
        Err(Error::InvalidObject)
    );
    assert_eq!(
        authentication_name(2, &[4; 32], &bytes, Default::default()),
        Err(Error::InvalidObject)
    );
    assert_eq!(
        authentication_name(
            1,
            &[4; 32],
            &bytes,
            Limits {
                reads: point::Limits {
                    value_bytes: 1,
                    ..Default::default()
                },
                ..Default::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    let mut fate = ae_vm_fate::code::Code::default();
    let symbol = fate.insert_symbol(b"authorize").unwrap();
    let bytes = SophiaCode::from_source(3, b"", fate.encode(Default::default()).unwrap(), vec![])
        .encode()
        .unwrap();
    let mut full = [9; 32];
    full[..4].copy_from_slice(&symbol);
    for function in [symbol.as_slice(), full.as_slice()] {
        assert_eq!(
            authentication_name(3, function, &bytes, Default::default()),
            Ok(b"authorize".to_vec())
        );
    }
    assert_eq!(
        authentication_name(3, &[9; 5], &bytes, Default::default()),
        Err(Error::InvalidObject)
    );
    assert_eq!(
        authentication_name(
            3,
            &symbol,
            &bytes,
            Limits {
                fate: ae_vm_fate::Limits {
                    max_nodes: 0,
                    ..Default::default()
                },
                ..Default::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
}
