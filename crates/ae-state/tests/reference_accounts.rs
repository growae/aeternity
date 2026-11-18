// Produced by unchanged aec_accounts.erl at fa2a6d7 and aeserialization a1204df.
use ae_state::accounts::Account;
use ae_types::{AccountPublicKey, Id, IdTag, Unsigned};

#[test]
fn historical_account_versions_and_payability_match_erlang() {
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/erlang_accounts.json")).unwrap();
    let mut key = [0; 32];
    key[31] = 1;
    let key = AccountPublicKey(key);
    let mut contract = [0; 32];
    contract[31] = 2;
    let mut function = [0; 32];
    function[31] = 3;
    let large = Unsigned::from_decimal("10000000000000000000000000000000").unwrap();
    for row in rows {
        let name = row["name"].as_str().unwrap();
        let flagged = name == "non_payable" || name == "generalized_non_payable";
        let mut account = Account::new(key, if flagged { 0.into() } else { large.clone() });
        account.set_non_payable(flagged);
        if name == "basic_large" || name == "generalized" {
            account.set_nonce(Unsigned::from_decimal("999999999999999999999999999999").unwrap());
        }
        if name.starts_with("generalized") {
            account
                .attach_authorization(Id::new(IdTag::Contract, contract), function)
                .unwrap();
        }
        if name == "set_true" {
            account.set_payable(true);
        }
        if name == "set_false" {
            account = Account::new(key, 0.into());
            account.set_non_payable(true);
            account.set_payable(false);
        }
        let expected = hex::decode(row["encoded"].as_str().unwrap()).unwrap();
        assert_eq!(account.serialize().unwrap(), expected, "{name}");
        assert_eq!(
            account.is_payable(),
            row["payable"].as_bool().unwrap(),
            "{name}"
        );
        assert_eq!(Account::deserialize(key, &expected).unwrap(), account);
    }
}
