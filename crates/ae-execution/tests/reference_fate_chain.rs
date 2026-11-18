use ae_execution::fate_chain::{
    self as chain, CodeException, CodeLookup, Transfer, TransferEvent, TransferFailure,
};
use ae_state::{
    StateRoots,
    accounts::{Account, Accounts},
    contracts::Contracts,
};
use ae_trie::{MemoryStore, Nibbles, NodeReader, NodeWriter, Trie, Value};
use ae_tx::CanonicalTransaction;
use ae_types::{AccountPublicKey, Hash256, Id, IdTag, Unsigned};
use ae_vm_fate::{Limits, code::Code};
use num_bigint::BigUint;
use serde_json::{Value as J, json};
use std::{cell::Cell, rc::Rc, sync::LazyLock};

static CORPUS: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_chain.json")).unwrap()
});
fn rows(j: &J) -> &[J] {
    j.as_array().unwrap()
}
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn key(j: &J) -> [u8; 32] {
    bytes(j).try_into().unwrap()
}
fn number(j: &J) -> Unsigned {
    Unsigned::from_decimal(j.as_str().unwrap()).unwrap()
}
fn decimal(n: &Unsigned) -> String {
    BigUint::from_bytes_be(n.as_bytes()).to_string()
}
fn small(n: u8) -> [u8; 32] {
    let mut k = [0; 32];
    k[31] = n;
    k
}
fn tree(j: &J) -> Trie<MemoryStore> {
    let mut tree = Trie::new(MemoryStore::default());
    for row in rows(j) {
        tree.put(
            &Nibbles::from_bytes(&bytes(&row["key"])),
            Value::Bytes(bytes(&row["encoded"])),
        )
        .unwrap();
    }
    tree
}
fn accounts(j: &J) -> Accounts {
    let t = tree(&j["accounts"]);
    Accounts::from_root(t.root_hash(), t.into_store()).unwrap()
}
fn contracts() -> Contracts {
    let t = tree(&CORPUS["contracts"]);
    Contracts::from_root(t.root_hash(), t.into_store()).unwrap()
}
fn entries<S: NodeReader>(tree: &Trie<S>) -> J {
    json!(
        tree.iter()
            .map(|r| {
                let (k, Value::Bytes(v)) = r.unwrap() else {
                    panic!()
                };
                let k: Vec<_> = k
                    .as_slice()
                    .chunks_exact(2)
                    .map(|p| p[0] << 4 | p[1])
                    .collect();
                json!({"key":hex::encode(k),"encoded":hex::encode(v)})
            })
            .collect::<Vec<_>>()
    )
}
fn events(events: &[TransferEvent]) -> J {
    json!(events.iter().map(|e| match e {
        TransferEvent::NameCall { .. } | TransferEvent::NameDelta { .. } => panic!("unexpected name event"),
        TransferEvent::OracleCall {..} | TransferEvent::OracleDelta {..} => panic!("unexpected oracle event in transfer test"),
        TransferEvent::InternalCall(tx) => json!({"kind":"internal_call","info":"Call.amount","transaction":hex::encode(tx.encode()),"type":"non_tx","tx_hash":""}),
        TransferEvent::Delta { account, debit, amount } => {
            let n = decimal(amount);
            let n = if *debit && *amount != 0.into() { format!("-{n}") } else { n };
            json!({"kind":"delta","key":hex::encode(account.0),"amount":n,"info":"Spend.amount","type":"non_tx","tx_hash":""})
        }
    }).collect::<Vec<_>>())
}
fn snapshot<S: NodeReader>(a: &Accounts<S>, e: &[TransferEvent]) -> J {
    json!({"accounts":entries(a.tree()),"root":a.root_hash().map(|h| hex::encode(h.0)),
        "tree_root":hex::encode(StateRoots { accounts:a.root_hash(), ..Default::default() }.hash().0),"events":events(e)})
}

#[test]
fn contract_loading_and_reference_resolution_match_original_chain_api() {
    let contracts = contracts();
    let initial = entries(contracts.tree());
    for row in rows(&CORPUS["lookups"]) {
        let k = key(&row["key"]);
        let found = chain::contract_fate_bytecode(&contracts, k, Limits::default()).unwrap();
        match found {
            CodeLookup::Loaded(loaded) => {
                assert_eq!(row["code"]["kind"], "loaded", "{row}");
                assert_eq!(
                    u64::from(loaded.vm_version as u8),
                    row["code"]["vm"].as_u64().unwrap()
                );
                assert_eq!(
                    loaded.code,
                    Code::decode(&bytes(&row["code"]["encoded"]), Limits::default()).unwrap()
                );
                assert_eq!(
                    loaded.code.encode(Limits::default()).unwrap(),
                    bytes(&row["code"]["encoded"])
                );
            }
            CodeLookup::Unavailable => assert_eq!(row["code"]["kind"], "unavailable", "{row}"),
            CodeLookup::Exception(exception) => {
                assert_eq!(row["code"]["kind"], "exception", "{row}");
                assert_eq!(row["code"]["class"], "error");
                match exception {
                    CodeException::InvalidReferenceId(id) => {
                        assert_eq!(id, Id::new(IdTag::Account, small(1)));
                        assert_eq!(row["code"]["reason"], "function_clause");
                    }
                    CodeException::DeepReference(id) => {
                        assert_eq!(row["code"]["reason"], "found_deep_reference");
                        assert_eq!(
                            id,
                            Id::new(
                                IdTag::Contract,
                                if k == small(11) { small(1) } else { small(14) }
                            )
                        );
                    }
                    CodeException::MissingReference(id) => {
                        assert_eq!(id, Id::new(IdTag::Contract, small(99)));
                        assert_eq!(row["code"]["reason"], "badmatch");
                    }
                    CodeException::Sophia(_) => {
                        assert!((22..=27).contains(&k[31]));
                    }
                }
            }
        }
        assert_eq!(
            json!(chain::contract_vm_version(&contracts, k).unwrap()),
            row["vm"]
        );
        let final_ref = chain::contract_find_final_ref(&contracts, k, 32);
        if k == small(14) {
            assert_eq!(final_ref, Err(chain::Error::ResourceLimit));
            assert_eq!(row["final_ref"]["kind"], "cycle_unexecuted");
        } else if k == small(30) {
            assert_eq!(
                final_ref,
                Err(chain::Error::InvalidReferenceId(Id::new(
                    IdTag::Account,
                    small(1)
                )))
            );
            assert_eq!(row["final_ref"]["reason"], "function_clause");
        } else {
            let actual = final_ref
                .unwrap()
                .map(|(pk, vm)| json!({"key":hex::encode(pk),"vm":vm}));
            assert_eq!(json!(actual), row["final_ref"], "{row}");
        }
    }
    assert_eq!(initial, entries(contracts.tree()));
}

#[test]
fn transfer_value_matches_original_account_bytes_roots_and_internal_events() {
    for row in rows(&CORPUS["transfers"]) {
        let mut accounts = accounts(&row["initial"]);
        assert_eq!(snapshot(&accounts, &[]), row["initial"]);
        let result = chain::transfer_value(
            &mut accounts,
            AccountPublicKey(key(&row["from"])),
            AccountPublicKey(key(&row["to"])),
            &number(&row["amount"]),
        )
        .unwrap();
        let e = match result {
            Transfer::Transferred(events) => {
                assert_eq!(row["result"], "ok", "{row}");
                events.to_vec()
            }
            Transfer::Rejected(reason) => {
                assert_eq!(
                    row["result"],
                    match reason {
                        TransferFailure::AccountNotFound => "account_not_found",
                        TransferFailure::InsufficientFunds => "insufficient_funds",
                        TransferFailure::AccountNotPayable => "account_is_not_payable",
                    },
                    "{row}"
                );
                vec![]
            }
        };
        assert_eq!(snapshot(&accounts, &e), row["final"], "{row}");
    }
}

#[test]
fn balance_lookup_uses_local_zero_before_onchain_fallback() {
    for row in rows(&CORPUS["balances"]) {
        let local = accounts(&row["local"]);
        let onchain = accounts(&row["onchain"]);
        for lookup in rows(&row["values"]) {
            let value = chain::account_balance(
                &local,
                row["offchain"].as_bool().unwrap().then_some(&onchain),
                AccountPublicKey(key(&lookup["key"])),
            )
            .unwrap();
            assert_eq!(json!(value.as_ref().map(decimal)), lookup["value"]);
        }
        assert_eq!(snapshot(&local, &[]), row["local"]);
        assert_eq!(snapshot(&onchain, &[]), row["onchain"]);
    }
}

#[test]
fn transfer_sequences_preserve_event_order_and_rejected_call_state() {
    for row in rows(&CORPUS["traces"]) {
        let mut accounts = accounts(&row["initial"]);
        let mut all_events = vec![];
        for step in rows(&row["steps"]) {
            let result = chain::transfer_value(
                &mut accounts,
                AccountPublicKey(key(&step["from"])),
                AccountPublicKey(key(&step["to"])),
                &number(&step["amount"]),
            )
            .unwrap();
            match result {
                Transfer::Transferred(events) => {
                    assert_eq!(step["result"], "ok");
                    all_events.splice(0..0, *events);
                }
                Transfer::Rejected(reason) => {
                    assert_eq!(
                        step["result"],
                        match reason {
                            TransferFailure::AccountNotFound => "account_not_found",
                            TransferFailure::InsufficientFunds => "insufficient_funds",
                            TransferFailure::AccountNotPayable => "account_is_not_payable",
                        }
                    );
                }
            }
            assert_eq!(snapshot(&accounts, &all_events), step["final"]);
        }
    }
}

#[test]
fn lookup_budgets_are_retryable_and_do_not_become_missing_code() {
    let contracts = contracts();
    let before = entries(contracts.tree());
    for limits in [
        Limits {
            max_bytes: 1,
            ..Limits::default()
        },
        Limits {
            max_nodes: 1,
            ..Limits::default()
        },
        Limits {
            max_depth: 0,
            ..Limits::default()
        },
    ] {
        assert_eq!(
            chain::contract_fate_bytecode(&contracts, small(1), limits),
            Err(chain::Error::ResourceLimit)
        );
    }
    for maximum in 0..3 {
        assert_eq!(
            chain::contract_find_final_ref(&contracts, small(11), maximum),
            Err(chain::Error::ResourceLimit)
        );
    }
    assert_eq!(
        chain::contract_find_final_ref(&contracts, small(11), 3),
        Ok(Some((small(1), 5)))
    );
    assert!(matches!(
        chain::contract_fate_bytecode(&contracts, small(1), Limits::default()),
        Ok(CodeLookup::Loaded(_))
    ));
    assert_eq!(entries(contracts.tree()), before);

    // A wrapper larger than the old implicit 16 MiB RLP default can be retried
    // with a larger explicit budget; malformed bytecode remains unavailable.
    let mut wrapper =
        ae_codec::sophia::SophiaCode::from_source(3, b"", bytes(&CORPUS["code"]), vec![]);
    wrapper.source_hash = vec![42; 16 * 1024 * 1024 + 1];
    let raw = wrapper.encode().unwrap();
    assert!(matches!(
        ae_codec::sophia::SophiaCode::decode(&raw),
        Err(ae_codec::object::ObjectError::Rlp(
            ae_codec::rlp::DecodeError::ResourceLimit
        ))
    ));
    assert_eq!(
        ae_codec::sophia::SophiaCode::decode_with_limits(
            &raw,
            ae_codec::rlp::DecodeLimits {
                max_input_bytes: raw.len(),
                ..Default::default()
            }
        )
        .unwrap(),
        wrapper_with_compiler(wrapper)
    );
}
fn wrapper_with_compiler(
    mut wrapper: ae_codec::sophia::SophiaCode,
) -> ae_codec::sophia::SophiaCode {
    wrapper.compiler_version = Some(b"unknown".to_vec());
    wrapper
}

#[derive(Clone)]
struct FailingStore {
    inner: MemoryStore,
    remaining: Rc<Cell<Option<usize>>>,
}
impl NodeReader for FailingStore {
    fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
        self.inner.node(h)
    }
}
impl NodeWriter for FailingStore {
    fn put_node(&mut self, h: Hash256, bytes: Vec<u8>) -> Result<(), ae_trie::Error> {
        if let Some(n) = self.remaining.get() {
            if n == 0 {
                return Err(ae_trie::Error::Backend("injected write failure".into()));
            }
            self.remaining.set(Some(n - 1));
        }
        self.inner.put_node(h, bytes)
    }
}

#[test]
fn value_transfer_rolls_back_partial_storage_failures_and_supports_nested_snapshots() {
    let mut base = Accounts::default();
    for n in 1..=3 {
        base.enter(&Account::new(AccountPublicKey(small(n)), 100.into()))
            .unwrap();
    }
    let root = base.root_hash();
    let mut failures = 0;
    let mut successes = 0;
    for (spending, budget) in [false, true]
        .into_iter()
        .flat_map(|s| (0..12).map(move |b| (s, b)))
    {
        let remaining = Rc::new(Cell::new(Some(budget)));
        let store = FailingStore {
            inner: base.tree().store().clone(),
            remaining: remaining.clone(),
        };
        let mut trial = Accounts::from_root(root, store).unwrap();
        let transfer = if spending {
            chain::spend
        } else {
            chain::transfer_value
        };
        let result = transfer(
            &mut trial,
            AccountPublicKey(small(1)),
            AccountPublicKey(small(2)),
            &15.into(),
        );
        match result {
            Err(chain::Error::Accounts(ae_state::accounts::AccountError::Tree(
                ae_trie::Error::Backend(_),
            ))) => {
                failures += 1;
                assert_eq!(trial.root_hash(), root);
                assert_eq!(snapshot(&trial, &[]), snapshot(&base, &[]));
                remaining.set(None);
                assert!(matches!(
                    transfer(
                        &mut trial,
                        AccountPublicKey(small(1)),
                        AccountPublicKey(small(2)),
                        &15.into()
                    ),
                    Ok(Transfer::Transferred(_))
                ));
            }
            Ok(Transfer::Transferred(_)) => successes += 1,
            _ => panic!("unexpected transfer outcome: {result:?}"),
        }
    }
    assert!(failures > 1 && successes > 0);
    let result: Result<(), ()> = base.transaction(|outer| {
        assert!(matches!(
            chain::transfer_value(
                outer,
                AccountPublicKey(small(1)),
                AccountPublicKey(small(2)),
                &15.into()
            ),
            Ok(Transfer::Transferred(_))
        ));
        let outer_root = outer.root_hash();
        let inner: Result<(), ()> = outer.transaction(|inner| {
            assert!(matches!(
                chain::transfer_value(
                    inner,
                    AccountPublicKey(small(2)),
                    AccountPublicKey(small(3)),
                    &20.into()
                ),
                Ok(Transfer::Transferred(_))
            ));
            Err(())
        });
        assert_eq!(inner, Err(()));
        assert_eq!(outer.root_hash(), outer_root);
        Err(())
    });
    assert_eq!(result, Err(()));
    assert_eq!(base.root_hash(), root);
}

#[test]
fn lookup_backend_failures_are_not_converted_to_absent_contracts_or_balances() {
    struct Missing;
    impl NodeReader for Missing {
        fn node(&self, _: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            Err(ae_trie::Error::Backend("injected read failure".into()))
        }
    }
    // Start readable and fail only after from_root has validated the root.
    struct Toggle {
        inner: MemoryStore,
        fail: Rc<Cell<bool>>,
    }
    impl NodeReader for Toggle {
        fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            if self.fail.get() {
                Missing.node(h)
            } else {
                self.inner.node(h)
            }
        }
    }
    let contracts = contracts();
    let fail = Rc::new(Cell::new(false));
    let reader = Toggle {
        inner: contracts.tree().store().clone(),
        fail: fail.clone(),
    };
    let tree = Contracts::from_root(contracts.root_hash(), reader).unwrap();
    fail.set(true);
    assert!(matches!(
        chain::contract_fate_bytecode(&tree, small(1), Limits::default()),
        Err(chain::Error::Contracts(ae_state::contracts::Error::Tree(_)))
    ));
    assert!(matches!(
        chain::contract_find_final_ref(&tree, small(1), 32),
        Err(chain::Error::Contracts(ae_state::contracts::Error::Tree(_)))
    ));
    fail.set(false);
    assert!(matches!(
        chain::contract_fate_bytecode(&tree, small(1), Limits::default()),
        Ok(CodeLookup::Loaded(_))
    ));
    let local = accounts(&CORPUS["balances"][0]["local"]);
    let reader = Toggle {
        inner: local.tree().store().clone(),
        fail: fail.clone(),
    };
    let local = Accounts::from_root(local.root_hash(), reader).unwrap();
    let onchain = accounts(&CORPUS["balances"][0]["onchain"]);
    fail.set(true);
    assert!(matches!(
        chain::account_balance(&local, Some(&onchain), AccountPublicKey(small(1))),
        Err(chain::Error::Accounts(
            ae_state::accounts::AccountError::Tree(_)
        ))
    ));
    fail.set(false);
    assert_eq!(
        chain::account_balance(&local, Some(&onchain), AccountPublicKey(small(1))),
        Ok(Some(0.into()))
    );
}

#[test]
fn spending_checks_payability_after_debit_and_rejection_rolls_back() {
    let mut accounts = Accounts::default();
    let sender = AccountPublicKey(small(1));
    let recipient = AccountPublicKey(small(2));
    accounts.enter(&Account::new(sender, 100.into())).unwrap();
    let mut blocked = Account::new(recipient, 20.into());
    blocked.set_non_payable(true);
    accounts.enter(&blocked).unwrap();
    for (from, to, amount) in [
        (sender, recipient, 0),
        (sender, recipient, 30),
        (recipient, recipient, 0),
        (recipient, recipient, 10),
    ] {
        let before = snapshot(&accounts, &[]);
        assert_eq!(
            chain::spend(&mut accounts, from, to, &amount.into()).unwrap(),
            Transfer::Rejected(TransferFailure::AccountNotPayable)
        );
        assert_eq!(snapshot(&accounts, &[]), before);
    }
    assert!(matches!(
        chain::transfer_value(&mut accounts, sender, recipient, &10.into()),
        Ok(Transfer::Transferred(_))
    ));
    assert_eq!(
        accounts.lookup(sender).unwrap().unwrap().balance(),
        &90.into()
    );
    assert_eq!(
        accounts.lookup(recipient).unwrap().unwrap().balance(),
        &30.into()
    );
    let new = AccountPublicKey(small(3));
    let Transfer::Transferred(events) =
        chain::spend(&mut accounts, sender, new, &0.into()).unwrap()
    else {
        panic!()
    };
    assert_eq!(accounts.lookup(new).unwrap().unwrap().balance(), &0.into());
    let TransferEvent::InternalCall(tx) = &events[0] else {
        panic!()
    };
    assert_eq!(tx.payload(), b"Chain.spend");
}
