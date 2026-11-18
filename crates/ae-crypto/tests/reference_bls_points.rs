use ae_crypto::bls::{self, G1, G2, Gt, Profile};
use serde_json::Value;
const PROFILE: Profile = Profile::Mcl11976beLlvm64VintArm64;
fn fp(j: &Value) -> [u8; 48] {
    hex::decode(j.as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap()
}
fn g1(j: &Value) -> G1 {
    std::array::from_fn(|i| fp(&j[i]))
}
fn g2(j: &Value) -> G2 {
    std::array::from_fn(|i| std::array::from_fn(|k| fp(&j[i][k])))
}
fn gt(j: &Value) -> Gt {
    std::array::from_fn(|i| fp(&j[i]))
}
#[test]
fn raw_bls_points_and_target_field_operations_match_original_native() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_bls_points.json")).unwrap();
    let f = &fixture["g1"];
    let points: Vec<_> = f["points"].as_array().unwrap().iter().map(g1).collect();
    for (i, j) in f["unary"].as_array().unwrap().iter().enumerate() {
        assert_eq!(
            bls::g1_negate(PROFILE, &points[i]),
            g1(&j["neg"]),
            "G1 neg {i}"
        );
        assert_eq!(
            bls::g1_normalize(PROFILE, &points[i]),
            g1(&j["norm"]),
            "G1 norm {i}"
        );
        assert_eq!(
            bls::g1_valid(PROFILE, &points[i]),
            j["valid"].as_bool().unwrap(),
            "G1 valid {i}"
        );
        assert_eq!(
            bls::g1_zero(PROFILE, &points[i]),
            j["zero"].as_bool().unwrap(),
            "G1 zero {i}"
        );
    }
    for row in f["add"].as_array().unwrap() {
        let a = row["a"].as_u64().unwrap() as usize;
        let b = row["b"].as_u64().unwrap() as usize;
        assert_eq!(
            bls::g1_add(PROFILE, &points[a], &points[b]),
            g1(&row["result"]),
            "G1 add {a} {b}"
        );
    }
    let f = &fixture["g2"];
    let points: Vec<_> = f["points"].as_array().unwrap().iter().map(g2).collect();
    for (i, j) in f["unary"].as_array().unwrap().iter().enumerate() {
        assert_eq!(
            bls::g2_negate(PROFILE, &points[i]),
            g2(&j["neg"]),
            "G2 neg {i}"
        );
        assert_eq!(
            bls::g2_normalize(PROFILE, &points[i]),
            g2(&j["norm"]),
            "G2 norm {i}"
        );
        assert_eq!(
            bls::g2_valid(PROFILE, &points[i]),
            j["valid"].as_bool().unwrap(),
            "G2 valid {i}"
        );
        assert_eq!(
            bls::g2_zero(PROFILE, &points[i]),
            j["zero"].as_bool().unwrap(),
            "G2 zero {i}"
        );
    }
    for row in f["add"].as_array().unwrap() {
        let a = row["a"].as_u64().unwrap() as usize;
        let b = row["b"].as_u64().unwrap() as usize;
        assert_eq!(
            bls::g2_add(PROFILE, &points[a], &points[b]),
            g2(&row["result"]),
            "G2 add {a} {b}"
        );
    }
    let f = &fixture["gt"];
    let values: Vec<_> = f["points"].as_array().unwrap().iter().map(gt).collect();
    for (i, j) in f["unary"].as_array().unwrap().iter().enumerate() {
        assert_eq!(
            bls::gt_unitary_inverse(PROFILE, &values[i]),
            gt(&j["inv"]),
            "GT inv {i}"
        );
        assert_eq!(
            bls::gt_is_one(PROFILE, &values[i]),
            j["one"].as_bool().unwrap(),
            "GT one {i}"
        );
    }
    for row in f["add"].as_array().unwrap() {
        let a = row["a"].as_u64().unwrap() as usize;
        let b = row["b"].as_u64().unwrap() as usize;
        assert_eq!(
            bls::gt_add(PROFILE, &values[a], &values[b]),
            gt(&row["result"]),
            "GT add {a} {b}"
        );
    }
}
