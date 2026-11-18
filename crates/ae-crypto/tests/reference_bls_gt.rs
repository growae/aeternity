use ae_crypto::bls::{self, Profile};
use serde_json::Value;
const PROFILE: Profile = Profile::Mcl11976beLlvm64VintArm64;
fn bytes<const N: usize>(value: &Value) -> [u8; N] {
    hex::decode(value.as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap()
}
fn gt(value: &Value) -> bls::Gt {
    std::array::from_fn(|i| bytes(&value[i]))
}
#[test]
fn raw_gt_multiplication_and_power_match_original_native() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/erlang_bls_gt.json")).unwrap();
    let points: Vec<_> = fixture["points"]
        .as_array()
        .unwrap()
        .iter()
        .map(gt)
        .collect();
    let scalars: Vec<bls::Fr> = fixture["scalars"]
        .as_array()
        .unwrap()
        .iter()
        .map(bytes)
        .collect();
    for row in fixture["mul"].as_array().unwrap() {
        let a = row["a"].as_u64().unwrap() as usize;
        let b = row["b"].as_u64().unwrap() as usize;
        assert_eq!(
            bls::gt_multiply(PROFILE, &points[a], &points[b]),
            gt(&row["result"]),
            "GT mul {a} {b}"
        );
    }
    for row in fixture["pow"].as_array().unwrap() {
        let p = row["point"].as_u64().unwrap() as usize;
        let k = row["scalar"].as_u64().unwrap() as usize;
        assert_eq!(
            bls::gt_power(PROFILE, &points[p], &scalars[k]),
            gt(&row["result"]),
            "GT pow {p} {k}"
        );
    }
}
