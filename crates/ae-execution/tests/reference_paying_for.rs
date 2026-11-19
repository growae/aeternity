use ae_codec::object::Value;
use ae_execution::{
    Context,
    transactions::{self, Config, Error, Event, Receipt, SignatureError, SignatureLimits},
};
use ae_protocol::{NetworkProfile, Protocol};
use ae_state::{Trees, accounts::Accounts, contracts::Contracts};
use ae_trie::{MemoryStore, NodeReader, Trie};
use ae_tx::{
    CanonicalTransaction, GaMeta, PayingFor, SignedTransaction, Transaction, TransactionKind,
    WireTransaction, Wrapper,
};
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
static PREP: LazyLock<J> = LazyLock::new(|| {
    unpack(include_str!(
        "fixtures/erlang_paying_for_execution_preparation.json"
    ))
});
fn bytes(v: &J) -> Vec<u8> {
    assert_eq!(v[0], "binary");
    hex::decode(v[1].as_str().unwrap()).unwrap()
}
fn field<'a>(v: &'a J, k: &str) -> &'a J {
    assert_eq!(v[0], "map");
    &v[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p[0] == atom(k))
        .unwrap()[1]
}
fn atom(s: &str) -> J {
    json!(["atom", s])
}
fn bin(b: impl AsRef<[u8]>) -> J {
    json!(["binary", hex::encode(b)])
}
fn tuple(v: Vec<J>) -> J {
    json!(["tuple", v])
}
fn int(n: impl ToString) -> J {
    json!(["integer", n.to_string()])
}
fn magnitude(n: &Unsigned) -> num_bigint::BigInt {
    num_bigint::BigInt::from_bytes_be(num_bigint::Sign::Plus, n.as_bytes())
}
fn check_used_gas(actual: &Unsigned, captured: &J) {
    // The inner capture is itself serialized by the outer Erlang term encoder.
    assert_eq!(field(captured, "kind"), &bin("ok"));
    let value = field(captured, "value");
    assert_eq!(value[0], "list");
    let fields = value[1].as_array().unwrap();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0], bin("integer"));
    assert_eq!(magnitude(actual).to_string().as_bytes(), bytes(&fields[1]));
}
fn tree(v: &J) -> Trie {
    let mut t = Trie::default();
    for pair in v[1].as_array().unwrap() {
        t.put(
            &ae_trie::Nibbles::from_bytes(&bytes(&pair[1][0])),
            ae_trie::Value::Bytes(bytes(&pair[1][1])),
        )
        .unwrap();
    }
    t
}
fn initial(v: &J) -> Trees {
    let a = tree(field(v, "accounts"));
    Trees {
        accounts: Accounts::from_root(a.root_hash(), a.store().clone()).unwrap(),
        contracts: Contracts::new(MemoryStore::default()),
        calls: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    }
}
fn entries<S: NodeReader>(t: &Trie<S>) -> J {
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
fn event(e: &Event) -> J {
    if let Event::Contract(ae_execution::contracts::Event::Aevm(e)) = e {
        assert_eq!(e.context, ae_execution::aevm_chain::EventContext::None);
        return event(&Event::Primitive(e.event.clone()));
    }
    if let Event::Contract(ae_execution::contracts::Event::Internal(e)) = e {
        use ae_execution::fate_chain::TransferEvent;
        let (body, info) = match e {
            TransferEvent::DeploymentCall { info } => (
                tuple(vec![atom("internal_call_tx"), bin(info)]),
                atom("error"),
            ),
            TransferEvent::Delta {
                account,
                debit,
                amount,
            } => (
                tuple(vec![
                    atom("delta"),
                    tuple(vec![
                        bin(account.0),
                        int(if *debit {
                            -magnitude(amount)
                        } else {
                            magnitude(amount)
                        }),
                    ]),
                ]),
                bin("Spend.amount"),
            ),
            TransferEvent::InternalCall(t) => {
                let id = |tag, k| tuple(vec![atom("id"), atom(tag), bin(k)]);
                assert_eq!(t.recipient().tag, ae_types::IdTag::Account);
                let tx = tuple(vec![
                    atom("spend_tx"),
                    id("account", t.sender().0),
                    id("account", t.recipient().value),
                    int(magnitude(t.amount())),
                    int(magnitude(t.fee())),
                    int(magnitude(t.ttl())),
                    int(magnitude(t.nonce())),
                    bin(t.payload()),
                ]);
                (
                    tuple(vec![atom("internal_call_tx"), bin(t.payload())]),
                    tuple(vec![
                        atom("aetx"),
                        atom("spend_tx"),
                        atom("aec_spend_tx"),
                        int(t.wire_size()),
                        tx,
                    ]),
                )
            }
            e => panic!("unexpected internal event: {e:?}"),
        };
        return tuple(vec![
            body,
            json!([
                "map",
                [
                    [atom("info"), info],
                    [atom("type"), atom("non_tx")],
                    [atom("tx_hash"), bin([])]
                ]
            ]),
        ]);
    }
    use ae_execution::Event as P;
    let delta = |k: AccountPublicKey, n: &Unsigned, debit: bool| {
        tuple(vec![
            atom("delta"),
            tuple(vec![
                bin(k.0),
                int(if debit { -magnitude(n) } else { magnitude(n) }),
            ]),
        ])
    };
    let (body, info) = match e {
        Event::Primitive(P::Spend { from, to, amount }) => (
            tuple(vec![
                atom("spend"),
                tuple(vec![bin(from.0), bin(to.0), int(magnitude(amount))]),
            ]),
            "Spend.amount",
        ),
        Event::Primitive(P::ContractDelta {
            account,
            amount,
            debit,
            info,
        }) => (delta(*account, amount, *debit), *info),
        Event::Primitive(P::FeeDebit { account, amount }) => {
            (delta(*account, amount, true), "Chain.fee")
        }
        Event::Primitive(P::NameDebit { account, amount }) => {
            (delta(*account, amount, true), "Name.lock")
        }
        Event::Primitive(P::NameRefund { account, amount }) => {
            (delta(*account, amount, false), "Name.refund")
        }
        Event::Primitive(P::NameFeeLocked { amount }) => {
            (delta(AccountPublicKey::ZERO, amount, false), "Chain.lock")
        }
        Event::Primitive(P::OracleQueryFee { account, amount }) => {
            (delta(*account, amount, false), "Oracle.queryfee")
        }
        Event::Contract(ae_execution::contracts::Event::Delta {
            account,
            amount,
            debit,
            info,
        }) => (delta(*account, amount, *debit), *info),
        _ => panic!("unexpected {e:?}"),
    };
    tuple(vec![
        body,
        json!([
            "map",
            [
                [atom("info"), bin(info)],
                [atom("type"), atom("non_tx")],
                [atom("tx_hash"), bin([])]
            ]
        ]),
    ])
}
fn check_snapshot<A: NodeReader, C: NodeReader>(t: &Trees<A, C>, r: &Receipt, v: &J) {
    assert_eq!(bin(t.roots().hash().0), *field(v, "root"));
    assert_eq!(entries(t.accounts.tree()), *field(v, "accounts"));
    assert_eq!(
        json!(["list", r.events.iter().map(event).collect::<Vec<_>>()]),
        *field(v, "events")
    );
    assert_eq!(
        r.context
            .payer
            .map(|k| bin(k.0))
            .unwrap_or(atom("undefined")),
        *field(v, "payer")
    );
    assert_eq!(
        atom(match r.context.origin {
            ae_execution::Origin::Transaction => "aetx_transaction",
            ae_execution::Origin::Generalized => "aetx_ga",
            ae_execution::Origin::Contract => "aetx_contract",
        }),
        *field(v, "context")
    );
}
fn reason<A, C>(e: &Error<A, C>) -> &'static str {
    match e {
        Error::Signature(SignatureError::CheckFailed) => "signature_check_failed",
        Error::Signature(SignatureError::VerificationFailed) => "signature_verification_failed",
        Error::RecursivePayer => "not_allowed_to_pay_for_recursively",
        Error::Primitive(e) => match e {
            ae_execution::Error::InvalidAtProtocol => "invalid_at_protocol",
            ae_execution::Error::TooLowFee => "too_low_fee",
            ae_execution::Error::TtlExpired => "ttl_expired",
            ae_execution::Error::AccountNotFound => "account_not_found",
            ae_execution::Error::InsufficientFunds => "insufficient_funds",
            ae_execution::Error::NotBasicAccount => "not_a_basic_account",
            ae_execution::Error::GaNonceMustBeZero => "nonce_in_ga_tx_should_be_zero",
            ae_execution::Error::NonceAlreadyUsed => "tx_nonce_already_used_for_account",
            ae_execution::Error::NonceTooHigh => "tx_nonce_too_high_for_account",
            e => panic!("{e:?}"),
        },
        _ => panic!("unexpected execution error"),
    }
}
// Only the recursive-constructor corpus bypasses the public decoder. Each
// wrapper is reconstructed as an original constructor object; canonical bytes
// must still match. All ordinary fixtures exercise the actual signed decoder.
#[derive(Clone)]
struct Raw(Vec<u8>);
impl CanonicalTransaction for Raw {
    fn decode(b: &[u8]) -> Result<Self, ae_tx::Error> {
        Ok(Self(b.to_vec()))
    }
    fn encode(&self) -> Vec<u8> {
        self.0.clone()
    }
}
fn reconstructed(raw: &[u8]) -> Transaction {
    let w = WireTransaction::decode(raw).unwrap();
    let uint = |n| {
        let Value::Integer(v) = w.field(n).unwrap() else {
            panic!()
        };
        v.clone()
    };
    let account = |n| {
        let Value::Id(v) = w.field(n).unwrap() else {
            panic!()
        };
        AccountPublicKey(v.value)
    };
    let binary = |n| {
        let Value::Binary(v) = w.field(n).unwrap() else {
            panic!()
        };
        v.clone()
    };
    let wrapper = match w.kind() {
        TransactionKind::PayingFor => Wrapper::PayingFor(PayingFor {
            payer: account("payer_id"),
            nonce: uint("nonce"),
            fee: uint("fee"),
        }),
        TransactionKind::GaMeta => Wrapper::GaMeta(GaMeta {
            account: account("ga_id"),
            auth_data: binary("auth_data"),
            abi_version: uint("abi_version"),
            fee: uint("fee"),
            gas: uint("gas"),
            gas_price: uint("gas_price"),
            ttl: (w.version() == 1).then(|| uint("ttl")),
        }),
        _ => return Transaction::decode(raw).unwrap(),
    };
    let inner = SignedTransaction::<Raw>::decode(&binary("tx")).unwrap();
    let tx = reconstructed(&inner.transaction().0).wrap(wrapper, inner.signatures().to_vec());
    assert_eq!(tx.encode(), raw);
    tx
}
fn signed(row: &J) -> SignedTransaction<Transaction> {
    let b = bytes(&row["signed"]);
    if row["scenario"]
        .as_str()
        .is_some_and(|s| s.starts_with("recursive"))
    {
        assert_eq!(
            SignedTransaction::<Transaction>::decode(&b).unwrap_err(),
            ae_tx::Error::RecursivePayer
        );
        let raw = SignedTransaction::<Raw>::decode(&b).unwrap();
        let s = SignedTransaction::new(
            reconstructed(&raw.transaction().0),
            raw.signatures().to_vec(),
        );
        assert_eq!(s.encode(), b);
        s
    } else {
        SignedTransaction::decode(&b).unwrap()
    }
}
fn context(row: &J) -> Context {
    let mut c = Context::transaction(
        50,
        Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
    );
    c.dry_run = row["dry_run"].as_bool().unwrap();
    c
}
#[test]
fn original_fee_payer_signature_admission_nonce_and_state_matrix() {
    let network = NetworkProfile::mainnet();
    let config = Config::primitives(&network);
    let rows = PREP.as_array().unwrap();
    assert_eq!(rows.len(), 432);
    let mut counts = [0; 3];
    for row in rows {
        let s = signed(row);
        let c = context(row);
        let v = transactions::verify(
            s.transaction().view(),
            s.signatures(),
            &network,
            c.protocol,
            false,
            SignatureLimits::default(),
        );
        let actual = match v {
            Ok(()) => {
                counts[0] += 1;
                atom("ok")
            }
            Err(SignatureError::CheckFailed) => {
                tuple(vec![atom("error"), atom("signature_check_failed")])
            }
            e => panic!("{e:?}"),
        };
        assert_eq!(row["verify"]["kind"], "ok");
        assert_eq!(actual, row["verify"]["value"], "{row}");
        for (index, label) in ["process", "verified_process"].into_iter().enumerate() {
            let mut t = initial(&row["before"]);
            let roots = t.roots();
            assert_eq!(bin(roots.hash().0), *field(&row["before"], "root"));
            let outcome = if index == 0 {
                transactions::process(s.transaction(), &mut t, &c, &config)
            } else {
                transactions::execute_signed(&s, &mut t, &c, &config)
            };
            assert_eq!(row[label]["kind"], "ok");
            let expected = &row[label]["value"];
            match outcome {
                Ok(r) => {
                    counts[index + 1] += 1;
                    assert_eq!(expected[1][0], atom("ok"), "{row}");
                    check_snapshot(&t, &r, &expected[1][1]);
                    assert_eq!(r.transaction_hash, (index == 1).then(|| s.hash()));
                    assert_eq!(
                        magnitude(&r.gas),
                        s.transaction()
                            .view()
                            .pricing(&50.into(), c.protocol)
                            .gas_limit
                    );
                }
                Err(e) => {
                    assert_eq!(
                        tuple(vec![atom("error"), atom(reason(&e))]),
                        *expected,
                        "{} {} {label}: {e:?}",
                        row["protocol"],
                        row["scenario"]
                    );
                    assert_eq!(t.roots(), roots);
                    assert_eq!(
                        entries(t.accounts.tree()),
                        *field(&row["before"], "accounts")
                    );
                }
            }
        }
    }
    assert_eq!(counts, [346, 48, 40]);
}

#[path = "paying_for/expanded.rs"]
mod expanded;
