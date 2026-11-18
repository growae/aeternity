use ae_execution::{Context, Error, Event, Origin, process_spend};
use ae_protocol::Protocol;
use ae_state::{
    StateRoots,
    accounts::{Account, Accounts},
    names::{self, Name},
};
use ae_trie::{Nibbles, Trie, Value};
use ae_tx::{CanonicalTransaction, Spend};
use ae_types::{AccountPublicKey, Unsigned};
use serde_json::Value as Json;

fn bytes(v: &Json) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn key(v: &Json) -> AccountPublicKey {
    AccountPublicKey::try_from(bytes(v).as_slice()).unwrap()
}
fn number(v: &Json) -> Unsigned {
    Unsigned::from_decimal(v.as_str().unwrap()).unwrap()
}
fn root(accounts: &Accounts, names: &Trie) -> String {
    hex::encode(
        StateRoots {
            accounts: accounts.root_hash(),
            names: names.root_hash(),
            ..Default::default()
        }
        .hash()
        .0,
    )
}
fn reason(e: &Error) -> &str {
    match e {
        Error::TooLowFee => "too_low_fee",
        Error::TtlExpired => "ttl_expired",
        Error::AccountNotFound => "account_not_found",
        Error::NotBasicAccount => "not_a_basic_account",
        Error::NotGeneralizedAccount => "not_a_generalized_account",
        Error::GeneralizedAccountsUnavailable => "generalize_accounts_not_available_at_protocol",
        Error::GaNonceMustBeZero => "nonce_in_ga_tx_should_be_zero",
        Error::BadGaEnvironment => "bad_ga_tx_env",
        Error::NonceAlreadyUsed => "tx_nonce_already_used_for_account",
        Error::NonceTooHigh => "tx_nonce_too_high_for_account",
        Error::InsufficientFunds => "insufficient_funds",
        Error::AccountNotPayable => "account_is_not_payable",
        Error::Name(names::Error::NameNotFound) => "name_does_not_exist",
        Error::Name(names::Error::NameRevoked) => "name_revoked",
        Error::Name(names::Error::PointerNotFound) => "pointer_id_not_found",
        Error::ReferenceException {
            class: "error",
            reason: "function_clause",
        } => "function_clause",
        _ => panic!("unexpected error {e:?}"),
    }
}
#[test]
fn spend_transitions_errors_events_and_state_roots_match_original_erlang() {
    let cases: Json = serde_json::from_str(include_str!("fixtures/erlang_spends.json")).unwrap();
    assert_eq!(cases.as_array().unwrap().len(), 376);
    let mut successes = 0;
    for case in cases.as_array().unwrap() {
        let mut accounts = Accounts::default();
        let mut names = Trie::default();
        for a in case["accounts"].as_array().unwrap() {
            accounts
                .enter(&Account::deserialize(key(&a["key"]), &bytes(&a["encoded"])).unwrap())
                .unwrap();
        }
        for n in case["names"].as_array().unwrap() {
            let raw = bytes(&n["encoded"]);
            let hash: [u8; 32] = bytes(&n["hash"]).try_into().unwrap();
            let name = Name::deserialize(hash, &raw).unwrap();
            assert_eq!(name.serialize().unwrap(), raw);
            names
                .put(&Nibbles::from_bytes(&hash), Value::Bytes(raw))
                .unwrap();
        }
        assert_eq!(
            root(&accounts, &names),
            case["initial_root"].as_str().unwrap(),
            "{}",
            case["label"]
        );
        let before_root = accounts.root_hash();
        let mut ctx = Context::transaction(
            case["height"].as_u64().unwrap(),
            Protocol::try_from(case["protocol"].as_u64().unwrap() as u32).unwrap(),
        );
        ctx.origin = match case["context"].as_str().unwrap() {
            "aetx_transaction" => Origin::Transaction,
            "aetx_contract" => Origin::Contract,
            "aetx_ga" => Origin::Generalized,
            _ => unreachable!(),
        };
        ctx.dry_run = case["dry_run"].as_bool().unwrap();
        if !case["payer"].is_null() {
            ctx.payer = Some(key(&case["payer"]));
        }
        let tx = Spend::decode(&bytes(&case["transaction"])).unwrap();
        if case["authenticated"].as_bool().unwrap() {
            ctx.ga_authenticated.insert(tx.sender());
        }
        if case["ga_nonce"].as_bool().unwrap() {
            ctx.ga_nonces.insert(tx.sender(), vec![42; 32]);
        }
        let result = process_spend(&tx, &mut accounts, &names, &ctx);
        if let Some(expected) = case.get("error").or_else(|| case.get("crash")) {
            assert_eq!(
                reason(&result.unwrap_err()),
                expected.as_str().unwrap(),
                "{}",
                case["label"]
            );
            assert_eq!(accounts.root_hash(), before_root, "{}", case["label"]);
        } else {
            let receipt = result.unwrap_or_else(|e| panic!("{}: {e:?}", case["label"]));
            successes += 1;
            assert_eq!(
                root(&accounts, &names),
                case["final_root"].as_str().unwrap(),
                "{}",
                case["label"]
            );
            assert_eq!(receipt.gas, number(&case["gas"]));
            for a in case["final_accounts"].as_array().unwrap() {
                assert_eq!(
                    accounts
                        .lookup(key(&a["key"]))
                        .unwrap()
                        .unwrap()
                        .serialize()
                        .unwrap(),
                    bytes(&a["encoded"]),
                    "{}",
                    case["label"]
                );
            }
            let expected: Vec<_> = case["events"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| match e["kind"].as_str().unwrap() {
                    "spend" => {
                        assert_eq!(e["info"], "Spend.amount");
                        Event::Spend {
                            from: key(&e["from"]),
                            to: key(&e["to"]),
                            amount: number(&e["amount"]),
                        }
                    }
                    "fee" => {
                        assert_eq!(e["info"], "Chain.fee");
                        Event::FeeDebit {
                            account: key(&e["account"]),
                            amount: number(&e["amount"]),
                        }
                    }
                    _ => unreachable!(),
                })
                .collect();
            assert_eq!(receipt.events, expected, "{}", case["label"]);
        }
    }
    assert_eq!(successes, 154);
}
