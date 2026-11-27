use ae_execution::{Context, Origin, oracles};
use ae_protocol::oracles::abi_available;
use ae_tx::{CanonicalTransaction, OracleTransaction, Transaction, TransactionLeaf};
use ae_types::Unsigned;
use serde_json::Value as Json;

fn bytes(v: &Json) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn number(v: &Json) -> Unsigned {
    Unsigned::from_decimal(v.as_str().unwrap()).unwrap()
}
#[test]
fn typed_oracle_envelopes_historical_admission_state_rent_and_fee_sizes_match_erlang() {
    let corpus: Json =
        serde_json::from_str(include_str!("fixtures/erlang_oracle_wire.json")).unwrap();
    let (mut decoded, mut rejected, mut prices) = (0, 0, 0);
    for row in corpus["transactions"].as_array().unwrap() {
        let raw = bytes(&row["transaction"]);
        let result = OracleTransaction::decode(&raw);
        if row["decoded"] == false {
            assert!(result.is_err(), "{row}");
            rejected += 1;
            continue;
        }
        let tx = result.unwrap_or_else(|e| panic!("{row}: {e:?}"));
        decoded += 1;
        let canonical = bytes(&row["canonical"]);
        assert_eq!(tx.encode(), canonical, "{row}");
        assert_eq!(tx.wire_size(), raw.len());
        let mut built = OracleTransaction::new(tx.account(), tx.operation().clone()).unwrap();
        built.set_nonce(tx.nonce().clone());
        built.set_fee(tx.fee().clone());
        built.set_ttl(tx.ttl().clone());
        assert_eq!(built.encode(), canonical);
        assert_eq!(built.wire_size(), canonical.len());
        for r in row["prices"].as_array().unwrap() {
            let mut context = Context::transaction(
                r["height"].as_u64().unwrap(),
                (r["protocol"].as_u64().unwrap() as u32).try_into().unwrap(),
            );
            assert_eq!(
                tx.valid_at_protocol(context.protocol),
                r["valid"].as_bool().unwrap(),
                "{row}"
            );
            assert_eq!(oracles::gas(&tx, &context), number(&r["gas"]), "{row}");
            assert_eq!(
                Transaction::new(TransactionLeaf::Oracle(tx.clone()))
                    .view()
                    .gas_limit(&Unsigned::from(context.height), context.protocol)
                    .to_string(),
                r["gas"].as_str().unwrap(),
                "gas preflight {} at {}",
                row["label"],
                context.height
            );
            assert_eq!(
                oracles::minimum_fee(&tx, &context),
                number(&r["minimum_fee"]),
                "{row}"
            );
            context.origin = Origin::Generalized;
            assert_eq!(
                oracles::minimum_fee(&tx, &context),
                number(&r["ga_minimum_fee"]),
                "{row}"
            );
            prices += 1;
        }
    }
    for r in corpus["abi"].as_array().unwrap() {
        assert_eq!(
            abi_available(
                &number(&r["abi"]),
                (r["protocol"].as_u64().unwrap() as u32).try_into().unwrap()
            ),
            r["accepted"].as_bool().unwrap(),
            "{r}"
        );
    }
    assert_eq!((decoded, rejected, prices), (76, 23, 1824));
    assert_eq!(corpus["abi"].as_array().unwrap().len(), 48);
}
