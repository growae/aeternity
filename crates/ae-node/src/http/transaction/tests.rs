use super::*;
use ae_tx::{CanonicalTransaction, GaMeta, Spend, TransactionLeaf};
use ae_types::{AccountPublicKey, IdTag};
use serde_json::json;

fn input(row: &Value) -> Result<SignedTransaction<Transaction>, ae_tx::Error> {
    let wire = hex::decode(row["wire"].as_str().unwrap()).unwrap();
    if row["kind"] == "signed" {
        return SignedTransaction::decode(&wire);
    }
    let tx = Transaction::decode(&wire)?;
    let mut one = Signature([0; 64]);
    one.0[63] = 1;
    let mut two = Signature([0; 64]);
    two.0[63] = 2;
    Ok(SignedTransaction::new(
        tx,
        if row["kind"] == "pair" {
            vec![two, one, two]
        } else {
            vec![]
        },
    ))
}
#[test]
fn original_transaction_clients_cover_all_types_versions_and_signed_views() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_transaction_client.json"
    ))
    .unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    let rejections = fixture["rejections"].as_array().unwrap();
    assert_eq!(rows.len(), 1699);
    assert_eq!(rejections.len(), 243);
    let mut types = std::collections::BTreeSet::new();
    let mut versions = std::collections::BTreeSet::new();
    let (_, block) = api::decode(fixture["mined"]["block"].as_str().unwrap()).unwrap();
    let location = Location::Mined {
        height: fixture["mined"]["height"].as_u64().unwrap(),
        block: Hash256(block.try_into().unwrap()),
    };
    for row in rows {
        let id = row["id"].as_str().unwrap();
        let tx = input(row).unwrap_or_else(|e| panic!("{id}: {e:?}"));
        assert_eq!(
            tx.encode(),
            hex::decode(row["canonical"].as_str().unwrap()).unwrap(),
            "{id}"
        );
        assert_eq!(
            tx.hash().as_ref(),
            hex::decode(row["hash"].as_str().unwrap()).unwrap(),
            "{id}"
        );
        assert_eq!(
            unsigned(tx.transaction().view(), Limits::default()).unwrap(),
            row["inner"]["tx"],
            "unsigned {id}"
        );
        assert_eq!(
            signed(&tx, Location::Inner, Limits::default()).unwrap(),
            row["inner"],
            "inner {id}"
        );
        let mut expected = row["inner"].clone();
        expected["hash"] = row["metadata"]["hash"].clone();
        expected["encoded_tx"] = row["metadata"]["encoded_tx"].clone();
        expected["block_height"] = json!(-1);
        expected["block_hash"] = json!("none");
        assert_eq!(
            signed(&tx, Location::Pending, Limits::default()).unwrap(),
            expected,
            "pending {id}"
        );
        expected["block_height"] = fixture["mined"]["height"].clone();
        expected["block_hash"] = fixture["mined"]["block"].clone();
        assert_eq!(
            signed(&tx, location, Limits::default()).unwrap(),
            expected,
            "mined {id}"
        );
        types.insert(row["inner"]["tx"]["type"].as_str().unwrap());
        versions.insert((
            row["inner"]["tx"]["type"].as_str().unwrap(),
            row["inner"]["tx"]["version"].as_u64().unwrap(),
        ));
    }
    assert_eq!(types.len(), 27);
    assert_eq!(versions.len(), 31);
    for row in rejections {
        assert!(input(row).is_err(), "native rejection {}", row["id"]);
    }
}

#[test]
fn transaction_client_limits_reject_without_changing_canonical_data() {
    let mut spend =
        Spend::new(AccountPublicKey([1; 32]), Id::new(IdTag::Account, [2; 32])).unwrap();
    spend.set_amount(
        Unsigned::from_decimal("1606938044258990275541962092341162602522202993782792835301376")
            .unwrap(),
    );
    spend.set_payload(vec![0xff; 4096]);
    let mut tx = Transaction::new(TransactionLeaf::Spend(spend));
    let wrapper = Wrapper::GaMeta(GaMeta {
        account: AccountPublicKey([3; 32]),
        auth_data: vec![0; 16],
        abi_version: 3.into(),
        fee: 4.into(),
        gas: 5.into(),
        gas_price: 6.into(),
        ttl: None,
    });
    for _ in 0..8 {
        tx = tx.wrap(wrapper.clone(), vec![]);
    }
    let tx = SignedTransaction::new(tx, vec![]);
    let canonical = tx.encode();
    for limits in [
        Limits {
            layers: 7,
            ..Limits::default()
        },
        Limits {
            json_bytes: 1,
            ..Limits::default()
        },
        Limits {
            work_bytes: 1,
            ..Limits::default()
        },
    ] {
        assert_eq!(
            signed(&tx, Location::Pending, limits),
            Err(Error::ResourceLimit)
        );
    }
    let output = signed(&tx, Location::Pending, Limits::default()).unwrap();
    assert!(!output.as_object().unwrap().contains_key("signatures"));
    assert_eq!(canonical, tx.encode());
    let encoded = serde_json::to_vec(&output).unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap(), output);
}
