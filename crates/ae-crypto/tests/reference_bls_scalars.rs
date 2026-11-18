use ae_crypto::bls::{self, Profile};
use serde_json::Value;
const PROFILE: Profile = Profile::Mcl11976beLlvm64VintArm64;
fn bytes<const N: usize>(v: &Value) -> [u8; N] {
    hex::decode(v.as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap()
}
fn g1(v: &Value) -> bls::G1 {
    std::array::from_fn(|i| bytes(&v[i]))
}
fn g2(v: &Value) -> bls::G2 {
    std::array::from_fn(|i| std::array::from_fn(|j| bytes(&v[i][j])))
}
#[test]
fn raw_bls_scalar_multiplication_matches_original_native() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_bls_scalars.json")).unwrap();
    let scalars: Vec<bls::Fr> = fixture["scalars"]
        .as_array()
        .unwrap()
        .iter()
        .map(bytes)
        .collect();
    for kind in ["g1", "g2"] {
        let group = &fixture[kind];
        for row in group["mul"].as_array().unwrap() {
            let i = row["point"].as_u64().unwrap() as usize;
            let j = row["scalar"].as_u64().unwrap() as usize;
            let p = &group["points"][i];
            if kind == "g1" {
                assert_eq!(
                    bls::g1_multiply(PROFILE, &scalars[j], &g1(p)),
                    g1(&row["result"]),
                    "G1 mul {i} {j}"
                );
            } else {
                assert_eq!(
                    bls::g2_multiply(PROFILE, &scalars[j], &g2(p)),
                    g2(&row["result"]),
                    "G2 mul {i} {j}"
                );
            }
        }
    }
}
