use ae_crypto::poseidon_bls12_381;
use num_bigint::BigUint;
use serde_json::Value;

fn bytes(value: &Value) -> [u8; 32] {
    let n: BigUint = value.as_str().unwrap().parse().unwrap();
    let b = n.to_bytes_be();
    let mut out = [0; 32];
    out[32 - b.len()..].copy_from_slice(&b);
    out
}

#[test]
fn two_input_poseidon_matches_original_erlang_rounds() {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/erlang_poseidon.json")).unwrap();
    for row in rows {
        assert_eq!(
            poseidon_bls12_381(&bytes(&row["left"]), &bytes(&row["right"])),
            Some(bytes(&row["result"])),
            "{row}"
        );
    }
    let q = bytes(&Value::String(
        "52435875175126190479447740508185965837690552500527637822603658699938581184513".into(),
    ));
    assert_eq!(poseidon_bls12_381(&q, &[0; 32]), None);
    assert_eq!(poseidon_bls12_381(&[0; 32], &[255; 32]), None);
}
