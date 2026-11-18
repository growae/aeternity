use ae_crypto::bls::{self, Profile};
use serde_json::Value;
const PROFILE: Profile = Profile::Mcl11976beLlvm64VintArm64;
fn bytes(value: &Value) -> [u8; 48] {
    hex::decode(value.as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap()
}
fn g1(value: &Value) -> bls::G1 {
    std::array::from_fn(|i| bytes(&value[i]))
}
fn g2(value: &Value) -> bls::G2 {
    std::array::from_fn(|i| std::array::from_fn(|j| bytes(&value[i][j])))
}
fn gt(value: &Value) -> bls::Gt {
    std::array::from_fn(|i| bytes(&value[i]))
}
#[test]
fn raw_final_exponentiation_and_pairing_match_original_native() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_bls_pairing.json")).unwrap();
    for (i, x) in fixture["gt"].as_array().unwrap().iter().enumerate() {
        assert_eq!(
            bls::final_exponentiation(PROFILE, &gt(x)),
            gt(&fixture["final_exp"][i]),
            "final exponentiation {i}"
        );
    }
    let ps: Vec<_> = fixture["g1"].as_array().unwrap().iter().map(g1).collect();
    let qs: Vec<_> = fixture["g2"].as_array().unwrap().iter().map(g2).collect();
    for row in fixture["pairs"].as_array().unwrap() {
        let p = row["p"].as_u64().unwrap() as usize;
        let q = row["q"].as_u64().unwrap() as usize;
        assert_eq!(
            bls::miller_loop(PROFILE, &ps[p], &qs[q]),
            gt(&row["miller"]),
            "Miller loop {p} {q}"
        );
        assert_eq!(
            bls::pairing(PROFILE, &ps[p], &qs[q]),
            gt(&row["pairing"]),
            "pairing {p} {q}"
        );
    }
}
