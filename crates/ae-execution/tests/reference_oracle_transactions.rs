use ae_execution::{
    Context, Error, Event, Origin,
    oracles::{self, Failure, abi},
};
use ae_state::{
    StateRoots,
    accounts::Accounts,
    names::{Error as NameError, Names},
    oracles::Oracles,
};
use ae_trie::{MemoryStore, Nibbles, NodeReader, Trie, Value};
use ae_tx::{CanonicalTransaction, OracleTransaction};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use serde_json::{Value as Json, json};
use std::sync::LazyLock;
static CORPUS: LazyLock<Json> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_oracle_transactions.json")).unwrap()
});
fn bytes(v: &Json) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn key(v: &Json) -> AccountPublicKey {
    AccountPublicKey(bytes(v).try_into().unwrap())
}
fn number(v: &Json) -> Unsigned {
    Unsigned::from_decimal(v.as_str().unwrap()).unwrap()
}
fn root(r: Option<Hash256>) -> Json {
    r.map_or(Json::Null, |h| json!(hex::encode(h.0)))
}
fn entries<S: NodeReader>(t: &Trie<S>) -> Json {
    json!(
        t.iter()
            .map(|r| {
                let (k, v) = r.unwrap();
                let k: Vec<_> = k
                    .as_slice()
                    .chunks_exact(2)
                    .map(|p| p[0] << 4 | p[1])
                    .collect();
                let Value::Bytes(v) = v else { panic!() };
                json!({"key":hex::encode(k),"encoded":hex::encode(v)})
            })
            .collect::<Vec<_>>()
    )
}
fn snapshot<S: NodeReader, O: NodeReader, C: NodeReader>(
    a: &Accounts<S>,
    o: &Oracles<O, C>,
    n: &Names,
) -> Json {
    json!({"root":hex::encode(StateRoots{accounts:a.root_hash(),oracles:o.root_hash(),names:n.root_hash(),..Default::default()}.hash().0),"accounts":entries(a.tree()),"oracles":entries(o.tree()),"cache":entries(o.cache()),"names":entries(n.tree()),"name_cache":entries(n.cache()),"oracle_root":root(o.root_hash()),"cache_root":root(o.cache_root_hash()),"name_cache_root":root(n.cache_root_hash())})
}
fn tree(v: &Json) -> Trie<MemoryStore> {
    let mut t = Trie::new(MemoryStore::default());
    for row in v.as_array().unwrap() {
        t.put(
            &Nibbles::from_bytes(&bytes(&row["key"])),
            Value::Bytes(bytes(&row["encoded"])),
        )
        .unwrap();
    }
    t
}
fn initial(v: &Json) -> (Accounts, Oracles, Names) {
    let a = tree(&v["accounts"]);
    let o = tree(&v["oracles"]);
    let c = tree(&v["cache"]);
    let n = tree(&v["names"]);
    let nc = tree(&v["name_cache"]);
    let a = Accounts::from_root(a.root_hash(), a.into_store()).unwrap();
    let o =
        Oracles::from_roots(o.root_hash(), c.root_hash(), o.into_store(), c.into_store()).unwrap();
    let n = Names::from_roots(
        n.root_hash(),
        nc.root_hash(),
        n.into_store(),
        nc.into_store(),
    )
    .unwrap();
    assert_eq!(snapshot(&a, &o, &n), *v);
    (a, o, n)
}
fn context(row: &Json) -> Context {
    let mut c = Context::transaction(
        row["height"].as_u64().unwrap(),
        (row["protocol"].as_u64().unwrap() as u32)
            .try_into()
            .unwrap(),
    );
    c.origin = match row["context"].as_str().unwrap() {
        "aetx_contract" => Origin::Contract,
        "aetx_ga" => Origin::Generalized,
        _ => Origin::Transaction,
    };
    c.dry_run = row["dry_run"].as_bool().unwrap();
    if !row["payer"].is_null() {
        c.payer = Some(key(&row["payer"]));
    }
    let mut actor = AccountPublicKey::ZERO;
    actor.0[31] = 1;
    if row["authenticated"] == true {
        c.ga_authenticated.insert(actor);
    }
    if !row["ga_nonce"].is_null() {
        c.ga_nonces.insert(actor, bytes(&row["ga_nonce"]));
    }
    c
}
fn reason(e: &Error) -> &str {
    match e {
        Error::TooLowFee => "too_low_fee",
        Error::TtlExpired => "ttl_expired",
        Error::InvalidAtProtocol => "invalid_at_protocol",
        Error::AccountNotFound => "account_not_found",
        Error::NotBasicAccount => "not_a_basic_account",
        Error::NotGeneralizedAccount => "not_a_generalized_account",
        Error::GeneralizedAccountsUnavailable => "generalize_accounts_not_available_at_protocol",
        Error::GaNonceMustBeZero => "nonce_in_ga_tx_should_be_zero",
        Error::BadGaEnvironment => "bad_ga_tx_env",
        Error::NonceAlreadyUsed => "tx_nonce_already_used_for_account",
        Error::NonceTooHigh => "tx_nonce_too_high_for_account",
        Error::InsufficientFunds => "insufficient_funds",
        Error::ReferenceException { reason, .. } => reason,
        Error::Name(NameError::NameNotFound) => "name_does_not_exist",
        Error::Name(NameError::NameRevoked) => "name_revoked",
        Error::Name(NameError::PointerNotFound) => "pointer_id_not_found",
        Error::OracleTransaction(f) => match f {
            Failure::TooLowAbsoluteTtl => "too_low_abs_ttl",
            Failure::TooLowHeight => "too_low_height",
            Failure::AlreadyOracle => "account_is_already_an_oracle",
            Failure::NotActiveOracle => "account_is_not_an_active_oracle",
            Failure::ZeroExtensionTtl => "zero_relative_oracle_extension_ttl",
            Failure::OracleDoesNotExist => "oracle_does_not_exist",
            Failure::QueryFeeTooLow => "query_fee_too_low",
            Failure::TooLongTtl => "too_long_ttl",
            Failure::NamedQueryUnavailable => "oracle_query_by_name_hash_not_available_at_protocol",
            Failure::QueryAlreadyPresent => "oracle_query_already_present",
            Failure::IllegalQuerySpec => "illegal_oracle_query_spec",
            Failure::NoMatchingQuery => "no_matching_oracle_query",
            Failure::WrongResponseTtl => "oracle_response_has_wrong_response_ttl",
            Failure::WrongOracle => "oracle_does_not_match_query_id",
            Failure::QueryClosed => "oracle_closed_for_response",
        },
        Error::OracleAbi(e) => match e {
            abi::Error::BadAbiVersion => "bad_abi_version",
            abi::Error::BadQueryFormat => "bad_query_format",
            abi::Error::BadResponseFormat => "bad_response_format",
            abi::Error::BadFormat => "bad_format",
            abi::Error::StoredFateFormat(ae_vm_fate::Error::TrailingBytes) => "badmatch",
            abi::Error::StoredFateFormat(_) => "function_clause",
            _ => panic!("unexpected ABI {e:?}"),
        },
        _ => panic!("unexpected error {e:?}"),
    }
}
fn event(row: &Json) -> Event {
    let account = key(&row["account"]);
    let amount =
        Unsigned::from_decimal(row["amount"].as_str().unwrap().trim_start_matches('-')).unwrap();
    match row["info"].as_str().unwrap() {
        "Chain.fee" => Event::FeeDebit { account, amount },
        "Oracle.queryfee" => Event::OracleQueryFee { account, amount },
        _ => panic!("unexpected {row}"),
    }
}
#[test]
fn oracle_callbacks_match_original_acceptance_events_and_all_state_roots() {
    assert_eq!(CORPUS.as_array().unwrap().len(), 2664);
    let (mut decoded, mut success) = (0, 0);
    for row in CORPUS.as_array().unwrap() {
        let tx = OracleTransaction::decode(&bytes(&row["transaction"]));
        if row["decoded"] == false {
            assert!(tx.is_err(), "{}", row["label"]);
            continue;
        }
        let tx = tx.unwrap();
        decoded += 1;
        assert_eq!(tx.encode(), bytes(&row["canonical"]));
        let ctx = context(row);
        assert_eq!(
            tx.valid_at_protocol(ctx.protocol),
            row["valid_at_protocol"].as_bool().unwrap()
        );
        assert_eq!(oracles::gas(&tx, &ctx), number(&row["gas"]));
        let (mut a, mut o, n) = initial(&row["initial"]);
        match oracles::process(&tx, &mut a, &mut o, &n, &ctx) {
            Ok(receipt) => {
                assert!(
                    row.get("final").is_some(),
                    "{} unexpected success, expected {}",
                    row["label"],
                    row["error"]
                );
                success += 1;
                assert_eq!(snapshot(&a, &o, &n), row["final"], "{}", row["label"]);
                assert_eq!(
                    receipt.events,
                    row["events"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(event)
                        .collect::<Vec<_>>(),
                    "{}",
                    row["label"]
                );
                assert_eq!(receipt.fee, *tx.fee());
                assert_eq!(receipt.gas, number(&row["gas"]));
            }
            Err(e) => {
                assert!(
                    row.get("final").is_none(),
                    "{} unexpected {e:?}",
                    row["label"]
                );
                assert_eq!(
                    reason(&e),
                    row["error"].as_str().unwrap(),
                    "{}: {e:?}",
                    row["label"]
                );
                let exceptional = matches!(
                    e,
                    Error::ReferenceException { .. }
                        | Error::OracleAbi(abi::Error::StoredFateFormat(_))
                );
                assert_eq!(
                    exceptional,
                    row.get("exception").is_some(),
                    "{}: {e:?}",
                    row["label"]
                );
                assert_eq!(
                    snapshot(&a, &o, &n),
                    row["initial"],
                    "rollback {}",
                    row["label"]
                );
            }
        }
    }
    assert_eq!((decoded, success), (2600, 1277));
}

#[test]
fn storage_faults_and_nested_transactions_restore_all_oracle_roots() {
    use ae_trie::NodeWriter;
    use std::{
        cell::Cell,
        collections::{BTreeMap, BTreeSet},
        rc::Rc,
    };
    struct FaultStore {
        inner: MemoryStore,
        remaining: Rc<Cell<usize>>,
        failed: Rc<Cell<Option<&'static str>>>,
        label: &'static str,
    }
    impl NodeReader for FaultStore {
        fn node(&self, h: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            self.inner.node(h)
        }
    }
    impl NodeWriter for FaultStore {
        fn put_node(&mut self, h: Hash256, b: Vec<u8>) -> Result<(), ae_trie::Error> {
            let n = self.remaining.get();
            if n == 0 {
                self.failed.set(Some(self.label));
                return Err(ae_trie::Error::Backend("injected write failure".into()));
            }
            self.remaining.set(n - 1);
            self.inner.put_node(h, b)
        }
    }
    let mut chosen = BTreeMap::new();
    for row in CORPUS.as_array().unwrap() {
        if row["protocol"] != 6 || row.get("final").is_none() {
            continue;
        }
        let label = match (
            row["operation"].as_str().unwrap(),
            row["events"].as_array().unwrap().len(),
        ) {
            ("query", 2) => "query_with_payer",
            ("response", 2) if row["initial"]["accounts"].as_array().unwrap().len() == 2 => {
                "response_create_account"
            }
            (op, _) => op,
        };
        chosen.entry(label).or_insert(row);
    }
    assert_eq!(chosen.len(), 6);
    let mut failed_backends = BTreeSet::new();
    for (label, row) in chosen {
        let (base_a, base_o, n) = initial(&row["initial"]);
        let tx = OracleTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let ctx = context(row);
        let (mut successes, mut failures) = (0, 0);
        for budget in 0..64 {
            let remaining = Rc::new(Cell::new(budget));
            let failed = Rc::new(Cell::new(None));
            let store = |inner, label| FaultStore {
                inner,
                label,
                remaining: remaining.clone(),
                failed: failed.clone(),
            };
            let mut a = Accounts::from_root(
                base_a.root_hash(),
                store(base_a.tree().store().clone(), "accounts"),
            )
            .unwrap();
            let mut o = Oracles::from_roots(
                base_o.root_hash(),
                base_o.cache_root_hash(),
                store(base_o.tree().store().clone(), "oracles"),
                store(base_o.cache().store().clone(), "expiry"),
            )
            .unwrap();
            match oracles::process(&tx, &mut a, &mut o, &n, &ctx) {
                Ok(_) => {
                    successes += 1;
                    assert_eq!(
                        snapshot(&a, &o, &n),
                        row["final"],
                        "{label} budget {budget}"
                    );
                }
                Err(_) => {
                    failures += 1;
                    failed_backends.insert(failed.get().expect("backend failure"));
                    assert_eq!(
                        snapshot(&a, &o, &n),
                        row["initial"],
                        "{label} budget {budget}"
                    );
                }
            }
        }
        assert!(successes > 0 && failures > 0, "{label}");
        let (mut a, mut o, n) = initial(&row["initial"]);
        let result: Result<(), Error> = a.transaction(|a| {
            o.transaction(|o| {
                oracles::process(&tx, a, o, &n, &ctx)?;
                assert_eq!(snapshot(a, o, &n), row["final"]);
                Err(Error::InsufficientFunds)
            })
        });
        assert_eq!(result, Err(Error::InsufficientFunds));
        assert_eq!(snapshot(&a, &o, &n), row["initial"]);
    }
    assert_eq!(
        failed_backends,
        BTreeSet::from(["accounts", "oracles", "expiry"])
    );
}

#[test]
fn vm_resource_failure_keeps_state_and_remains_distinct_from_bad_format() {
    use ae_tx::OracleOperation;
    let mut selected = std::collections::BTreeMap::new();
    for row in CORPUS.as_array().unwrap() {
        if row["protocol"] != 4 || row.get("final").is_none() {
            continue;
        }
        let tx = OracleTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let abi = match tx.operation() {
            OracleOperation::Register { abi_version, .. } => abi_version.clone(),
            OracleOperation::Query { .. } | OracleOperation::Response { .. } => {
                let (_, o, _) = initial(&row["initial"]);
                o.oracles(None, 1).unwrap()[0].abi_version.clone()
            }
            _ => continue,
        };
        if abi != 1.into() && abi != 3.into() {
            continue;
        }
        selected
            .entry((row["operation"].as_str().unwrap(), abi))
            .or_insert(row);
    }
    assert_eq!(selected.len(), 6);
    for (_, row) in selected {
        let (mut a, mut o, n) = initial(&row["initial"]);
        let tx = OracleTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let mut limits = abi::Limits::default();
        limits.aevm.max_nodes = 0;
        limits.fate.max_nodes = 0;
        assert_eq!(
            oracles::process_with_limits(&tx, &mut a, &mut o, &n, &context(row), limits),
            Err(Error::OracleAbi(abi::Error::ResourceLimit))
        );
        assert_eq!(snapshot(&a, &o, &n), row["initial"]);
        oracles::process(&tx, &mut a, &mut o, &n, &context(row)).unwrap();
        assert_eq!(snapshot(&a, &o, &n), row["final"]);
    }
}
