use ae_codec::{
    idna::{BadLabel, Error as AsciiError},
    name_pointer::Pointer,
    object::{self, ObjectTag},
};
use ae_execution::{
    Context, Error, Event, Origin,
    names::{self, Failure},
};
use ae_state::{
    StateRoots,
    accounts::{Account, Accounts},
    names::{Auction, Commitment, Error as NameError, Name, Names},
};
use ae_trie::{NodeReader, Trie, Value};
use ae_tx::{CanonicalTransaction, NameTransaction};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use serde_json::{Value as Json, json};

fn bytes(v: &Json) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn key(v: &Json) -> AccountPublicKey {
    AccountPublicKey::try_from(bytes(v).as_slice()).unwrap()
}
fn number(v: &Json) -> Unsigned {
    Unsigned::from_decimal(v.as_str().unwrap()).unwrap()
}
fn entries<S: NodeReader>(tree: &Trie<S>) -> Json {
    json!(
        tree.iter()
            .map(|r| {
                let (k, v) = r.unwrap();
                let k = k
                    .as_slice()
                    .chunks_exact(2)
                    .map(|p| p[0] << 4 | p[1])
                    .collect::<Vec<_>>();
                let Value::Bytes(v) = v else {
                    panic!("state value")
                };
                json!({"key":hex::encode(k),"encoded":hex::encode(v)})
            })
            .collect::<Vec<_>>()
    )
}
fn root(h: Option<Hash256>) -> Json {
    h.map_or(Json::Null, |h| json!(hex::encode(h.0)))
}
fn snapshot<S: NodeReader, N: NodeReader, C: NodeReader>(
    accounts: &Accounts<S>,
    names: &Names<N, C>,
) -> Json {
    json!({"root":hex::encode(StateRoots{accounts:accounts.root_hash(),names:names.root_hash(),..Default::default()}.hash().0),
        "accounts":entries(accounts.tree()),"names":entries(names.tree()),"cache":entries(names.cache()),"cache_root":root(names.cache_root_hash())})
}
fn initial(row: &Json) -> (Accounts, Names) {
    let (mut accounts, mut names) = (Accounts::default(), Names::default());
    for a in row["accounts"].as_array().unwrap() {
        accounts
            .enter(&Account::deserialize(key(&a["key"]), &bytes(&a["encoded"])).unwrap())
            .unwrap();
    }
    for n in row["names"].as_array().unwrap() {
        let k = bytes(&n["key"]);
        let raw = bytes(&n["encoded"]);
        match object::decode_envelope(&raw).unwrap().tag {
            ObjectTag::Name => names
                .enter_name(&Name::deserialize(k.try_into().unwrap(), &raw).unwrap())
                .unwrap(),
            ObjectTag::NameCommitment => names
                .insert_commitment(&Commitment::deserialize(k.try_into().unwrap(), &raw).unwrap())
                .unwrap(),
            ObjectTag::NameAuction => names
                .enter_auction(&Auction::deserialize(k.try_into().unwrap(), &raw).unwrap())
                .unwrap(),
            _ => panic!("unexpected naming object"),
        }
    }
    assert_eq!(snapshot(&accounts, &names), *row);
    (accounts, names)
}
fn reason(e: &Error) -> &str {
    match e {
        Error::TooLowFee => "too_low_fee",
        Error::TtlExpired => "ttl_expired",
        Error::InvalidAtProtocol => "invalid_at_protocol",
        Error::BadTransaction => "bad_transaction",
        Error::AccountNotFound => "account_not_found",
        Error::NotBasicAccount => "not_a_basic_account",
        Error::NotGeneralizedAccount => "not_a_generalized_account",
        Error::GeneralizedAccountsUnavailable => "generalize_accounts_not_available_at_protocol",
        Error::GaNonceMustBeZero => "nonce_in_ga_tx_should_be_zero",
        Error::BadGaEnvironment => "bad_ga_tx_env",
        Error::NonceAlreadyUsed => "tx_nonce_already_used_for_account",
        Error::NonceTooHigh => "tx_nonce_too_high_for_account",
        Error::InsufficientFunds => "insufficient_funds",
        Error::Name(NameError::NameNotFound) => "name_does_not_exist",
        Error::Name(NameError::NameRevoked) => "name_revoked",
        Error::Name(NameError::PointerNotFound) => "pointer_id_not_found",
        Error::Name(NameError::IllegalSalt) => "illegal_salt_value",
        Error::ReferenceException { reason, .. } => reason,
        Error::Naming(f) => match f {
            Failure::CommitmentAlreadyPresent => "commitment_already_present",
            Failure::CommitmentNotOwned => "commitment_not_owned",
            Failure::CommitmentDeltaTooSmall => "commitment_delta_too_small",
            Failure::NameNotPreclaimed => "name_not_preclaimed",
            Failure::NameAlreadyTaken => "name_already_taken",
            Failure::NameAlreadyInAuction => "name_already_in_auction",
            Failure::NameNotInAuction => "name_not_in_auction",
            Failure::InvalidNameFee => "invalid_name_fee",
            Failure::IllegalNameFee => "illegal_name_fee",
            Failure::NameFeeIncrementTooLow => "name_fee_increment_too_low",
            Failure::InvalidRegistrar => "invalid_registrar",
            Failure::NameNotOwned => "name_not_owned",
            Failure::TtlTooHigh => "ttl_too_high",
            Failure::TtlTooLow => "ttl_too_low",
            Failure::InvalidPointers => "invalid_pointers",
            Failure::Encoding(e) => match e {
                AsciiError::NoRegistrar => "no_registrar",
                AsciiError::RegistrarUnknown => "registrar_unknown",
                AsciiError::MultipleNamespaces => "multiple_namespaces",
                AsciiError::NoLabelInRegistrar => "no_label_in_registrar",
                AsciiError::InvalidCodepoint(_) => "invalid_codepoint",
                AsciiError::BadLabel(b) => match b {
                    BadLabel::Nfc => "nfc",
                    BadLabel::Hyphen => "hyphen",
                    BadLabel::InitialCombiner => "initial_combiner",
                    BadLabel::Context => "context",
                    BadLabel::ContextJ => "contextj",
                    BadLabel::ContextO => "contexto",
                    BadLabel::Bidi => "bidi",
                    BadLabel::Alabel => "alabel",
                    BadLabel::TooLong => "too_long",
                },
                _ => panic!("unexpected encoding error"),
            },
        },
        _ => panic!("unexpected error: {e:?}"),
    }
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
    let mut sender = AccountPublicKey::ZERO;
    sender.0[31] = 1;
    if row["authenticated"].as_bool().unwrap() {
        c.ga_authenticated.insert(sender);
    }
    if row["ga_nonce"].as_bool().unwrap() {
        c.ga_nonces.insert(sender, vec![42; 32]);
    }
    c
}
fn event(e: &Json) -> Event {
    let account = key(&e["account"]);
    let raw = e["amount"].as_str().unwrap();
    let amount = Unsigned::from_decimal(raw.strip_prefix('-').unwrap_or(raw)).unwrap();
    match e["info"].as_str().unwrap() {
        "Chain.fee" => Event::FeeDebit { account, amount },
        "Name.lock" => Event::NameDebit { account, amount },
        "Name.refund" => Event::NameRefund { account, amount },
        "Chain.lock" => {
            assert_eq!(account, AccountPublicKey::ZERO);
            Event::NameFeeLocked { amount }
        }
        _ => panic!("unexpected event"),
    }
}
#[test]
fn all_five_naming_callbacks_match_original_erlang_bytes_rules_events_and_roots() {
    let corpus: Json =
        serde_json::from_str(include_str!("fixtures/erlang_naming_transactions.json")).unwrap();
    let mut successes = 0;
    let mut invalid = 0;
    for row in corpus.as_array().unwrap() {
        let label = row["label"].as_str().unwrap();
        let decoded = NameTransaction::decode(&bytes(&row["transaction"]));
        if !row["decoded"].as_bool().unwrap() {
            assert!(decoded.is_err(), "{label}");
            invalid += 1;
            continue;
        }
        let tx = decoded.unwrap_or_else(|e| panic!("{label}: {e:?}"));
        let ctx = context(row);
        assert_eq!(tx.encode(), bytes(&row["canonical"]), "{label}");
        assert_eq!(
            tx.valid_at_protocol(ctx.protocol),
            row["valid_at_protocol"].as_bool().unwrap(),
            "{label}"
        );
        let (mut accounts, mut names) = initial(&row["initial"]);
        match names::process(&tx, &mut accounts, &mut names, &ctx) {
            Ok(receipt) => {
                assert!(
                    row.get("final").is_some(),
                    "{label}: expected error {}",
                    row["error"]
                );
                assert_eq!(snapshot(&accounts, &names), row["final"], "{label}");
                assert_eq!(receipt.gas, number(&row["gas"]), "{label}");
                assert_eq!(
                    receipt.events,
                    row["events"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(event)
                        .collect::<Vec<_>>(),
                    "{label}"
                );
                successes += 1;
            }
            Err(e) => {
                assert_eq!(Some(reason(&e)), row["error"].as_str(), "{label}: {e:?}");
                let class = match e {
                    Error::ReferenceException { class, .. } => Some(class),
                    _ => None,
                };
                assert_eq!(class, row["exception"].as_str(), "{label}");
                assert_eq!(snapshot(&accounts, &names), row["initial"], "{label}");
            }
        }
    }
    assert_eq!(corpus.as_array().unwrap().len(), 1864);
    assert_eq!((successes, invalid), (698, 144));
}

#[test]
fn optional_update_fields_and_fixed_ttls_match_original_primops() {
    let corpus: Json =
        serde_json::from_str(include_str!("fixtures/erlang_naming_updates.json")).unwrap();
    let mut successes = 0;
    for row in corpus.as_array().unwrap() {
        let label = row["label"].as_str().unwrap();
        let (accounts, mut names) = initial(&row["initial"]);
        let ctx = Context::transaction(
            row["height"].as_u64().unwrap(),
            (row["protocol"].as_u64().unwrap() as u32)
                .try_into()
                .unwrap(),
        );
        let update = names::Update {
            ttl: if row["ttl"].is_null() {
                None
            } else {
                let value = number(&row["ttl"]["value"]);
                Some(match row["ttl"]["kind"].as_str().unwrap() {
                    "relative_ttl" => names::Expiration::Relative(value),
                    "fixed_ttl" => names::Expiration::Fixed(value),
                    _ => panic!("unexpected TTL kind"),
                })
            },
            client_ttl: (!row["client_ttl"].is_null()).then(|| number(&row["client_ttl"])),
            pointers: row["pointers"].as_array().map(|ps| {
                ps.iter()
                    .map(|p| {
                        Pointer::decode(
                            object::Value::Tuple(vec![
                                object::Value::Binary(bytes(&p["key"])),
                                object::Value::Binary(bytes(&p["value"])),
                            ]),
                            2,
                        )
                        .unwrap()
                    })
                    .collect()
            }),
        };
        let mut owner = AccountPublicKey::ZERO;
        owner.0[31] = 1;
        let mut hash = [0; 32];
        hash[31] = 10;
        match names::update_name(&mut names, owner, hash, &update, &ctx) {
            Ok(()) => {
                assert_eq!(snapshot(&accounts, &names), row["final"], "{label}");
                successes += 1;
            }
            Err(e) => {
                assert_eq!(Some(reason(&e)), row["error"].as_str(), "{label}: {e:?}");
                assert_eq!(snapshot(&accounts, &names), row["initial"], "{label}");
            }
        }
    }
    assert_eq!(corpus.as_array().unwrap().len(), 864);
    assert_eq!(successes, 301);
}

#[test]
fn backend_failures_roll_back_every_naming_transaction_and_auction_refund() {
    use ae_trie::{MemoryStore, NodeWriter};
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
            if self.remaining.get() == 0 {
                self.failed.set(Some(self.label));
                return Err(ae_trie::Error::Backend("injected write failure".into()));
            }
            self.remaining.set(self.remaining.get() - 1);
            self.inner.put_node(h, b)
        }
    }
    let corpus: Json =
        serde_json::from_str(include_str!("fixtures/erlang_naming_transactions.json")).unwrap();
    let mut chosen = BTreeMap::new();
    for row in corpus.as_array().unwrap() {
        if row["protocol"] != 6 || row.get("final").is_none() {
            continue;
        }
        let tx = NameTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let label = match tx.operation() {
            ae_tx::NameOperation::Preclaim { .. } => "preclaim",
            ae_tx::NameOperation::Update { .. } => "update",
            ae_tx::NameOperation::Transfer { .. } => "transfer",
            ae_tx::NameOperation::Revoke { .. } => "revoke",
            ae_tx::NameOperation::Claim { .. } => {
                if row["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|e| e["info"] == "Name.refund")
                {
                    "refund"
                } else if row["final"]["names"].as_array().unwrap().iter().any(|n| {
                    object::decode_envelope(&bytes(&n["encoded"])).unwrap().tag
                        == ObjectTag::NameAuction
                }) {
                    "auction"
                } else {
                    "claim"
                }
            }
        };
        chosen.entry(label).or_insert(row);
    }
    assert_eq!(chosen.len(), 7);
    let mut failed_backends = BTreeSet::new();
    for (label, row) in chosen {
        let (base_accounts, base_names) = initial(&row["initial"]);
        let tx = NameTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let ctx = context(row);
        let (mut failures, mut successes) = (0, 0);
        for budget in 0..64 {
            let remaining = Rc::new(Cell::new(budget));
            let failed = Rc::new(Cell::new(None));
            let store = |inner, label| FaultStore {
                inner,
                label,
                remaining: remaining.clone(),
                failed: failed.clone(),
            };
            let mut accounts = Accounts::from_root(
                base_accounts.root_hash(),
                store(base_accounts.tree().store().clone(), "accounts"),
            )
            .unwrap();
            let mut names = Names::from_roots(
                base_names.root_hash(),
                base_names.cache_root_hash(),
                store(base_names.tree().store().clone(), "names"),
                store(base_names.cache().store().clone(), "expiry"),
            )
            .unwrap();
            if names::process(&tx, &mut accounts, &mut names, &ctx).is_err() {
                failures += 1;
                failed_backends.insert(failed.get().expect("backend failure"));
                assert_eq!(
                    snapshot(&accounts, &names),
                    row["initial"],
                    "{label}, budget {budget}"
                );
            } else {
                successes += 1;
                assert_eq!(
                    snapshot(&accounts, &names),
                    row["final"],
                    "{label}, budget {budget}"
                );
            }
        }
        assert!(failures > 0 && successes > 0, "{label}");
    }
    assert_eq!(
        failed_backends,
        BTreeSet::from(["accounts", "names", "expiry"])
    );
}
