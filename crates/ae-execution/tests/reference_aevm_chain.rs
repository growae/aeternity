use ae_execution::aevm_chain::{Reads, View};
use ae_protocol::contracts::Version;
use ae_state::{
    StateRoots,
    accounts::{Account, Accounts},
    contracts::{Code, Contract, Contracts, Nonce},
    oracles::{Oracle, Oracles},
};
use ae_types::AccountPublicKey;
use ae_vm_aevm::{primops::AddressKind, word::Word};
use serde_json::{Value as J, json};
fn expand(v: &J, pool: &[J]) -> J {
    if let Some(i) = v.get("$ref") {
        return expand(&pool[i.as_u64().unwrap() as usize], pool);
    }
    match v {
        J::Array(a) => J::Array(a.iter().map(|x| expand(x, pool)).collect()),
        J::Object(m) => J::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), expand(v, pool)))
                .collect(),
        ),
        _ => v.clone(),
    }
}
fn trees(off: bool) -> (Accounts, Contracts, Oracles) {
    let mut accounts = Accounts::default();
    for (n, b, np) in if off {
        vec![(1, 100, true), (3, 0, true)]
    } else {
        vec![(1, 10, false), (2, 20, true)]
    } {
        let mut a = Account::new(AccountPublicKey(Word::from(n).0), b.into());
        a.set_non_payable(np);
        accounts.enter(&a).unwrap();
    }
    let ck = if off { 6 } else { 4 };
    let mut c = Contract::new(
        AccountPublicKey(Word::from(1).0),
        Nonce::Number(&1.into()),
        Version { vm: 6, abi: 1 },
        Code::Inline(vec![0]),
        0.into(),
    )
    .unwrap();
    c.public_key = Word::from(ck).0;
    let mut contracts = Contracts::default();
    contracts.insert(&c, &Default::default()).unwrap();
    let mut oracles = Oracles::default();
    oracles
        .insert_oracle(&Oracle {
            public_key: Word::from(ck + 1).0,
            query_format: b"q".to_vec(),
            response_format: b"r".to_vec(),
            query_fee: 0.into(),
            expires_by: 100.into(),
            abi_version: 0.into(),
        })
        .unwrap();
    (accounts, contracts, oracles)
}
fn view(t: &(Accounts, Contracts, Oracles)) -> View<'_> {
    View {
        accounts: &t.0,
        contracts: &t.1,
        oracles: &t.2,
    }
}
fn root(t: &(Accounts, Contracts, Oracles)) -> J {
    json!([
        "binary",
        hex::encode(
            StateRoots {
                accounts: t.0.root_hash(),
                contracts: t.1.root_hash(),
                oracles: t.2.root_hash(),
                ..Default::default()
            }
            .hash()
            .0
        )
    ])
}
#[test]
fn balance_and_address_reads_match_original_onchain_and_offchain_trees() {
    let packed: J =
        serde_json::from_str(include_str!("fixtures/erlang_aevm_chain_reads.json")).unwrap();
    let data = expand(&packed["root"], packed["values"].as_array().unwrap());
    let on = trees(false);
    let off = trees(true);
    assert_eq!(root(&on), data["onchain_root"]);
    assert_eq!(root(&off), data["offchain_root"]);
    let rows = data["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 80);
    for row in rows {
        let offchain = row["mode"] == "offchain";
        let top = if offchain { &off } else { &on };
        let reads = Reads {
            top: view(top),
            onchain: offchain.then(|| view(&on)),
        };
        let key = Word::from(row["key"].as_u64().unwrap());
        let balance = reads.balance(AccountPublicKey(key.0)).unwrap();
        let amount = num_bigint::BigUint::from_bytes_be(balance.as_bytes()).to_string();
        assert_eq!(
            row["balance"],
            json!({"kind":"ok","value":["integer",amount]}),
            "{row}"
        );
        for (name, kind) in [
            ("payable", AddressKind::Payable),
            ("contract", AddressKind::Contract),
            ("oracle", AddressKind::Oracle),
        ] {
            let value = reads.address(kind, key).unwrap();
            assert_eq!(
                row[name],
                json!({"kind":"ok","value":["tuple",[["atom","ok"],["integer",value.unsigned().to_string()]]]}),
                "{row}"
            );
        }
        assert_eq!(root(top), row["visible_root"]);
    }
    assert_eq!(root(&on), data["onchain_root"]);
    assert_eq!(root(&off), data["offchain_root"]);
}

#[test]
fn failed_outer_account_reads_do_not_fall_back_to_onchain_data() {
    use ae_trie::{MemoryStore, NodeReader};
    use ae_types::Hash256;
    use std::{cell::Cell, rc::Rc};
    #[derive(Clone)]
    struct Toggle {
        store: MemoryStore,
        fail: Rc<Cell<bool>>,
    }
    impl NodeReader for Toggle {
        fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            if self.fail.get() {
                Err(ae_trie::Error::Backend("offline".into()))
            } else {
                self.store.node(hash)
            }
        }
    }
    let (on, contracts, oracles) = trees(false);
    let (off, _, _) = trees(true);
    let fail = Rc::new(Cell::new(false));
    let wrap = |a: &Accounts, flag| {
        Accounts::from_root(
            a.root_hash(),
            Toggle {
                store: a.tree().clone().into_store(),
                fail: flag,
            },
        )
        .unwrap()
    };
    let top = wrap(&off, fail.clone());
    let inner = wrap(&on, Rc::new(Cell::new(false)));
    let reads = Reads {
        top: View {
            accounts: &top,
            contracts: &contracts,
            oracles: &oracles,
        },
        onchain: Some(View {
            accounts: &inner,
            contracts: &contracts,
            oracles: &oracles,
        }),
    };
    let root = top.root_hash();
    let key = AccountPublicKey(Word::from(1).0);
    fail.set(true);
    assert!(matches!(
        reads.balance(key),
        Err(ae_execution::aevm_chain::Error::Accounts(
            ae_state::accounts::AccountError::Tree(ae_trie::Error::Backend(_))
        ))
    ));
    assert_eq!(
        reads.address(AddressKind::Payable, Word::from(1)).unwrap(),
        Word::from(1)
    );
    fail.set(false);
    assert_eq!(reads.balance(key).unwrap(), 100.into());
    assert_eq!(top.root_hash(), root);
}
