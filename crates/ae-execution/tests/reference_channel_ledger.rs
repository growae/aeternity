use ae_execution::{
    Context, Origin,
    channels::{Error as ChannelError, NativeFailure},
    transactions::{self, Config, Error, Event, Receipt, SignatureError, SignatureLimits},
};
use ae_protocol::{NetworkProfile, Protocol};
use ae_state::{Trees, accounts::Accounts, channels::Channels, contracts::Contracts};
use ae_trie::{NodeReader, Trie};
use ae_tx::{SignedTransaction, Transaction, TransactionKind};
use ae_types::{AccountPublicKey, Unsigned};
use serde_json::{Value as J, json};
use std::sync::LazyLock;
fn unpack(text: &str) -> J {
    fn u(v: &J, pool: &[J]) -> J {
        match v {
            J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                u(&pool[m["$ref"].as_u64().unwrap() as usize], pool)
            }
            J::Object(m) => J::Object(m.iter().map(|(k, v)| (k.clone(), u(v, pool))).collect()),
            J::Array(a) => J::Array(a.iter().map(|v| u(v, pool)).collect()),
            _ => v.clone(),
        }
    }
    let v: J = serde_json::from_str(text).unwrap();
    if v["format"] == "interned-json-v1" {
        u(&v["root"], v["values"].as_array().unwrap())
    } else {
        v
    }
}
static DATA: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("fixtures/erlang_channel_ledger.json")));
fn atom(s: &str) -> J {
    json!(["atom", s])
}
fn bin(b: impl AsRef<[u8]>) -> J {
    json!(["binary", hex::encode(b)])
}
fn tuple(v: Vec<J>) -> J {
    json!(["tuple", v])
}
fn number(n: &Unsigned) -> J {
    json!([
        "integer",
        num_bigint::BigUint::from_bytes_be(n.as_bytes()).to_string()
    ])
}
fn bytes(v: &J) -> Vec<u8> {
    assert_eq!(v[0], "binary");
    hex::decode(v[1].as_str().unwrap()).unwrap()
}
fn field<'a>(v: &'a J, key: &str) -> &'a J {
    assert_eq!(v[0], "map");
    &v[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom(key))
        .unwrap()[1]
}
fn option<'a>(r: &'a J, key: &str) -> Option<&'a J> {
    r["config"][1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom(key))
        .map(|p| &p[1])
}
fn tree(v: &J) -> Trie {
    let mut t = Trie::default();
    for p in v[1].as_array().unwrap() {
        t.put(
            &ae_trie::Nibbles::from_bytes(&bytes(&p[1][0])),
            ae_trie::Value::Bytes(bytes(&p[1][1])),
        )
        .unwrap();
    }
    t
}
fn initial(r: &J) -> Trees {
    let b = &r["before"];
    let a = tree(field(b, "accounts"));
    let c = tree(field(b, "contracts"));
    Trees {
        accounts: Accounts::from_root(a.root_hash(), a.store().clone()).unwrap(),
        channels: Channels::from(tree(field(b, "channels"))),
        contracts: Contracts::from_root(c.root_hash(), c.store().clone()).unwrap(),
        calls: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    }
}
fn entries(t: &Trie<impl NodeReader>) -> J {
    json!([
        "list",
        t.iter()
            .map(|p| {
                let (k, v) = p.unwrap();
                let ae_trie::Value::Bytes(v) = v else {
                    panic!()
                };
                let k: Vec<_> = k
                    .as_slice()
                    .chunks_exact(2)
                    .map(|p| p[0] * 16 + p[1])
                    .collect();
                tuple(vec![bin(k), bin(v)])
            })
            .collect::<Vec<_>>()
    ])
}
fn name(k: TransactionKind) -> &'static str {
    match k {
        TransactionKind::ChannelForceProgress => "channel_force_progress_tx",
        TransactionKind::ChannelCreate => "channel_create_tx",
        TransactionKind::ChannelDeposit => "channel_deposit_tx",
        TransactionKind::ChannelWithdraw => "channel_withdraw_tx",
        TransactionKind::ChannelCloseMutual => "channel_close_mutual_tx",
        TransactionKind::ChannelSettle => "channel_settle_tx",
        TransactionKind::ChannelCloseSolo => "channel_close_solo_tx",
        TransactionKind::ChannelSlash => "channel_slash_tx",
        TransactionKind::ChannelSnapshotSolo => "channel_snapshot_solo_tx",
        TransactionKind::ChannelSetDelegates => "channel_set_delegates_tx",
        TransactionKind::PayingFor => "paying_for_tx",
        TransactionKind::HcVote => "hc_vote_tx",
        _ => panic!("{k:?}"),
    }
}
fn source_kind(s: &SignedTransaction<Transaction>) -> TransactionKind {
    let mut t = s.transaction().view();
    while matches!(
        t.layer().map(|l| l.wrapper()),
        Some(ae_tx::Wrapper::GaMeta(_))
    ) {
        t = t.inner().unwrap();
    }
    t.kind()
}
fn event(e: &Event, source: Option<&SignedTransaction<Transaction>>) -> J {
    if let Event::Channel {
        channel,
        transaction_hash,
        transaction_type,
    } = e
    {
        return tuple(vec![
            tuple(vec![atom("channel"), bin(channel)]),
            json!([
                "map",
                [
                    [atom("type"), atom(name(*transaction_type))],
                    [atom("tx_hash"), bin(transaction_hash.0)]
                ]
            ]),
        ]);
    }
    let (key, amount, debit, info) = match e {
        Event::Primitive(ae_execution::Event::FeeDebit { account, amount }) => {
            (*account, amount, true, "Chain.fee")
        }
        Event::Primitive(ae_execution::Event::ContractDelta {
            account,
            amount,
            debit,
            info,
        }) => (*account, amount, *debit, *info),
        Event::Contract(ae_execution::contracts::Event::Delta {
            account,
            amount,
            debit,
            info,
        }) => (*account, amount, *debit, *info),
        _ => panic!("{e:?}"),
    };
    let mut n = num_bigint::BigInt::from_bytes_be(num_bigint::Sign::Plus, amount.as_bytes());
    if debit {
        n = -n;
    }
    let metadata = json!([
        "map",
        [
            [atom("info"), bin(info)],
            [
                atom("type"),
                source
                    .map(|s| atom(name(source_kind(s))))
                    .unwrap_or(atom("non_tx"))
            ],
            [
                atom("tx_hash"),
                source.map(|s| bin(s.hash().0)).unwrap_or(bin([]))
            ]
        ]
    ]);
    tuple(vec![
        tuple(vec![
            atom("delta"),
            tuple(vec![bin(key.0), json!(["integer", n.to_string()])]),
        ]),
        metadata,
    ])
}
fn snapshot<A: NodeReader, C: NodeReader>(
    t: &Trees<A, C>,
    r: &Receipt,
    source: Option<&SignedTransaction<Transaction>>,
) -> J {
    json!([
        "map",
        [
            [atom("root"), bin(t.roots().hash().0)],
            [
                atom("context"),
                atom(match r.context.origin {
                    Origin::Transaction => "aetx_transaction",
                    Origin::Generalized => "aetx_ga",
                    Origin::Contract => "aetx_contract",
                })
            ],
            [
                atom("events"),
                json!([
                    "list",
                    r.events
                        .iter()
                        .map(|e| event(e, source))
                        .collect::<Vec<_>>()
                ])
            ],
            [
                atom("payer"),
                r.context
                    .payer
                    .map(|p| bin(p.0))
                    .unwrap_or(atom("undefined"))
            ],
            [atom("accounts"), entries(t.accounts.tree())],
            [atom("channels"), entries(t.channels.tree())],
            [atom("contracts"), entries(t.contracts.tree())]
        ]
    ])
}
fn primitive(e: &ae_execution::Error) -> &'static str {
    match e {
        ae_execution::Error::OracleTransaction(
            ae_execution::oracles::Failure::TooLowAbsoluteTtl,
        ) => "too_low_abs_ttl",
        ae_execution::Error::InvalidAtProtocol => "invalid_at_protocol",
        ae_execution::Error::TooLowFee => "too_low_fee",
        ae_execution::Error::TtlExpired => "ttl_expired",
        ae_execution::Error::AccountNotFound => "account_not_found",
        ae_execution::Error::InsufficientFunds => "insufficient_funds",
        ae_execution::Error::NotBasicAccount => "not_a_basic_account",
        ae_execution::Error::NotGeneralizedAccount => "not_a_generalized_account",
        ae_execution::Error::GeneralizedAccountsUnavailable => {
            "generalize_accounts_not_available_at_protocol"
        }
        ae_execution::Error::GaNonceMustBeZero => "nonce_in_ga_tx_should_be_zero",
        ae_execution::Error::BadGaEnvironment => "bad_ga_tx_env",
        ae_execution::Error::NonceAlreadyUsed => "tx_nonce_already_used_for_account",
        ae_execution::Error::NonceTooHigh => "tx_nonce_too_high_for_account",
        e => panic!("{e:?}"),
    }
}
fn captured_error(e: &Error) -> J {
    let reason = match e {
        Error::Primitive(e) => atom(primitive(e)),
        Error::Channel(e) => match e.as_ref() {
            ChannelError::Primitive(e) => atom(primitive(e)),
            ChannelError::Rejected(s) => atom(s),
            ChannelError::Native(n) => {
                let reason = match n {
                    NativeFailure::MissingSignedTransaction => {
                        tuple(vec![atom("badmatch"), atom("none")])
                    }
                    NativeFailure::ChannelKey(s) => {
                        tuple(vec![atom("badmatch"), tuple(vec![atom("error"), atom(s)])])
                    }
                    NativeFailure::NonceCase(n) => tuple(vec![atom("case_clause"), number(n)]),
                    NativeFailure::CounterpartyKind {
                        generalized,
                        authenticated,
                    } => tuple(vec![
                        atom("case_clause"),
                        tuple(vec![
                            atom(if *generalized { "generalized" } else { "basic" }),
                            atom(if *authenticated { "true" } else { "false" }),
                        ]),
                    ]),
                    NativeFailure::FunctionClause => atom("function_clause"),
                    NativeFailure::Badarg => atom("badarg"),
                    NativeFailure::ForceProgressCatch(b) => {
                        tuple(vec![atom("badmatch_trees"), bin(b)])
                    }
                    NativeFailure::OffchainUpdate(s) => {
                        tuple(vec![atom("off_chain_update_error"), atom(s)])
                    }
                    NativeFailure::IllegalContractVersion(v) => tuple(vec![
                        atom("illegal"),
                        atom("ct_version"),
                        json!([
                            "map",
                            [
                                [atom("vm"), number(&u64::from(v.vm).into())],
                                [atom("abi"), number(&u64::from(v.abi).into())]
                            ]
                        ]),
                    ]),
                    NativeFailure::ProofAccount { value, error } => {
                        assert_eq!(
                            *error,
                            ae_state::accounts::AccountError::Object(
                                ae_codec::object::ObjectError::ExpectedList
                            )
                        );
                        let ae_trie::Value::Bytes(b) = value else {
                            panic!("{value:?}");
                        };
                        tuple(vec![atom("badmatch"), bin(b)])
                    }
                    NativeFailure::MissingAuthenticationStore => tuple(vec![
                        atom("badmatch"),
                        tuple(vec![atom("error"), atom("no_such_subtree")]),
                    ]),
                    NativeFailure::AuthenticationStoreAlreadyPresent(k) => {
                        tuple(vec![atom("already_present"), bin(k)])
                    }
                    NativeFailure::LegacyDelegates {
                        initiator,
                        responder,
                    } => {
                        let ids = |v: &[ae_types::Id]| {
                            json!([
                                "list",
                                v.iter()
                                    .map(|id| tuple(vec![
                                        atom("id"),
                                        atom("account"),
                                        bin(id.value)
                                    ]))
                                    .collect::<Vec<_>>()
                            ])
                        };
                        let map = json!([
                            "map",
                            [
                                [atom("initiator"), ids(initiator)],
                                [atom("responder"), ids(responder)]
                            ]
                        ]);
                        let schema = json!(["list", [atom("id")]]);
                        tuple(vec![
                            atom("illegal_field"),
                            atom("delegate_ids"),
                            schema.clone(),
                            map.clone(),
                            schema,
                            map,
                        ])
                    }
                };
                return json!({"kind":"native","class":"error","reason":reason});
            }
            e => panic!("{e:?}"),
        },
        e => panic!("{e:?}"),
    };
    json!({"kind":"ok","value":tuple(vec![atom("error"),reason])})
}
fn verification(r: Result<(), SignatureError>) -> J {
    let v = match r {
        Ok(()) => atom("ok"),
        Err(SignatureError::CheckFailed) => {
            tuple(vec![atom("error"), atom("signature_check_failed")])
        }
        Err(SignatureError::VerificationFailed) => {
            tuple(vec![atom("error"), atom("signature_verification_failed")])
        }
        Err(e) => panic!("{e:?}"),
    };
    json!({"kind":"ok","value":v})
}
fn context(r: &J) -> Context {
    let mut c = Context::transaction(
        50,
        Protocol::try_from(r["protocol"].as_u64().unwrap() as u32).unwrap(),
    );
    c.dry_run = option(r, "dry_run") == Some(&atom("true"));
    c.origin = match field(&r["before"], "context")[1].as_str().unwrap() {
        "aetx_transaction" => Origin::Transaction,
        "aetx_ga" => Origin::Generalized,
        "aetx_contract" => Origin::Contract,
        _ => panic!(),
    };
    let p = field(&r["before"], "payer");
    if p[0] == "binary" {
        c.payer = Some(AccountPublicKey(bytes(p).try_into().unwrap()));
    }
    for k in r["authenticated"][1].as_array().unwrap() {
        c.ga_authenticated
            .insert(AccountPublicKey(bytes(k).try_into().unwrap()));
    }
    for p in r["ga_nonces"][1].as_array().unwrap() {
        if p[1][1][0] == "tuple" {
            c.ga_nonces.insert(
                AccountPublicKey(bytes(&p[1][0]).try_into().unwrap()),
                bytes(&p[1][1][1][1]),
            );
        }
    }
    c
}
fn normalize(v: &J) -> J {
    if let Some(a) = v.as_array() {
        if a.len() == 2 && a[0] == "map" {
            let mut pairs: Vec<_> = a[1].as_array().unwrap().iter().map(normalize).collect();
            pairs.sort_by_key(|p| p[0].to_string());
            return json!(["map", pairs]);
        }
        return J::Array(a.iter().map(normalize).collect());
    }
    if let Some(m) = v.as_object() {
        return J::Object(m.iter().map(|(k, v)| (k.clone(), normalize(v))).collect());
    }
    v.clone()
}
fn difference(a: &J, b: &J, path: &str) -> Option<String> {
    if a == b {
        return None;
    }
    match (a, b) {
        (J::Array(a), J::Array(b)) if a.len() == b.len() => a
            .iter()
            .zip(b)
            .enumerate()
            .find_map(|(i, (a, b))| difference(a, b, &format!("{path}/{i}"))),
        (J::Object(a), J::Object(b)) if a.keys().eq(b.keys()) => a
            .iter()
            .find_map(|(k, a)| difference(a, &b[k], &format!("{path}/{k}"))),
        _ => Some(format!("{path}: actual {a}, expected {b}")),
    }
}
#[test]
fn five_channel_ledger_callbacks_match_original_roots_events_fees_and_failures() {
    let network = NetworkProfile::mainnet();
    let config = Config::primitives(&network);
    assert_eq!(DATA["runs"].as_array().unwrap().len(), 3784);
    for (i, r) in DATA["runs"].as_array().unwrap().iter().enumerate() {
        let signed = SignedTransaction::<Transaction>::decode(&bytes(&r["signed"])).unwrap();
        let mut t = initial(r);
        let before = t.roots();
        let c = context(r);
        let verified = transactions::verify_with_trees(
            signed.transaction().view(),
            signed.signatures(),
            &network,
            &c,
            SignatureLimits::default(),
            &t.channels,
        );
        assert_eq!(
            verification(verified.clone()),
            r["verification"],
            "verify row {i}"
        );
        let original_source =
            SignedTransaction::<Transaction>::decode(&bytes(&r["source_signed"])).unwrap();
        let source = (option(r, "no_signed") != Some(&atom("true"))).then_some(&original_source);
        let result = transactions::process_with_signed_source(
            signed.transaction(),
            &mut t,
            &c,
            &config,
            source,
        );
        let actual = match result {
            Ok(ref receipt) => {
                json!({"kind":"ok","value":tuple(vec![atom("ok"),snapshot(&t,receipt,source),number(&receipt.gas)])})
            }
            Err(ref e) => {
                assert_eq!(t.roots(), before, "rollback row {i}");
                captured_error(e)
            }
        };
        if let Some(d) = difference(&normalize(&actual), &normalize(&r["result"]), "") {
            panic!(
                "row {i} {} {} protocol {}: {d}",
                r["operation"], r["label"], r["protocol"]
            );
        }
        if source.is_some() && original_source.encode() == signed.encode() {
            let mut input = initial(r);
            let executed =
                transactions::execute_signed_with_context(&signed, &mut input, &c, &config);
            if verified.is_ok() {
                assert_eq!(input.roots(), t.roots(), "signed row {i}");
                assert_eq!(executed.is_ok(), result.is_ok());
            } else {
                assert!(executed.is_err());
                assert_eq!(input.roots(), before);
            }
        }
    }
}

#[path = "channels/wrapped.rs"]
mod wrapped;

#[path = "channels/failures.rs"]
mod failures;

#[path = "channels/authentication.rs"]
mod authentication;

#[path = "channels/unilateral.rs"]
mod unilateral;

#[path = "channels/offchain.rs"]
mod offchain;

#[path = "channels/force_progress.rs"]
mod force_progress;

#[path = "channels/hc_votes.rs"]
mod hc_votes;
