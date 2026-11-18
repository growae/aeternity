use ae_crypto::BlsField;
use num_bigint::BigUint;
use serde_json::Value;

#[test]
fn field_encoding_matches_pinned_native_mcl_montgomery_bytes() {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/erlang_bls_fields.json")).unwrap();
    for row in rows {
        let field = match row["field"].as_str().unwrap() {
            "fr" => BlsField::Scalar,
            "fp" => BlsField::Base,
            _ => unreachable!(),
        };
        let n: BigUint = row["integer"].as_str().unwrap().parse().unwrap();
        let expected = match row["result"]["kind"].as_str().unwrap() {
            "ok" => Some(hex::decode(row["result"]["value"].as_str().unwrap()).unwrap()),
            "exception" => {
                assert_eq!(row["result"]["reason"], "badmatch");
                None
            }
            _ => unreachable!(),
        };
        assert_eq!(field.encode_integer(&n), expected, "{row}");
    }
}
