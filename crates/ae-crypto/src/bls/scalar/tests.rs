use super::*;
#[test]
fn scalar_decode_and_vint_glv_split_match_original() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_bls_scalar_parameters.json"
    ))
    .unwrap();
    fn integer(value: &serde_json::Value) -> BigInt {
        BigInt::parse_bytes(value.as_str().unwrap().as_bytes(), 16).unwrap()
    }
    for row in fixture["samples"].as_array().unwrap() {
        let raw = hex::decode(row["raw"].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let n = decode(&raw);
        assert_eq!(BigInt::from(n.clone()), integer(&row["decoded"]));
        for (name, actual) in [("g1", Fp::split(n.clone())), ("g2", Fp2::split(n))] {
            let expected: Vec<_> = row[name].as_array().unwrap().iter().map(integer).collect();
            assert_eq!(actual, expected, "{name} split, raw {raw:?}");
        }
    }
}
