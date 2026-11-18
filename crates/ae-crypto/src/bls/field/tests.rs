use super::*;
use serde_json::Value;
fn fp(v: &Value) -> Fp {
    Fp::read(
        &hex::decode(v.as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap(),
    )
}
fn fp2(v: &Value) -> Fp2 {
    Fp2([fp(&v[0]), fp(&v[1])])
}
#[test]
fn noncanonical_raw_field_arithmetic_matches_original_llvm_montgomery() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_bls_arithmetic.json"
    ))
    .unwrap();
    for row in fixture["fp"].as_array().unwrap() {
        let a = fp(&row["a"]);
        let b = fp(&row["b"]);
        for (name, actual) in [
            ("neg", a.neg()),
            ("add", a.add(&b)),
            ("sub", a.sub(&b)),
            ("mul", a.mul(&b)),
            ("sqr", a.square()),
        ] {
            assert_eq!(actual, fp(&row[name]), "{name} {row}");
        }
    }
    for row in fixture["fp2"].as_array().unwrap() {
        let a = fp2(&row["a"]);
        let b = fp2(&row["b"]);
        for (name, actual) in [
            ("neg", a.neg()),
            ("add", a.add(&b)),
            ("sub", a.sub(&b)),
            ("mul", a.mul(&b)),
            ("sqr", a.square()),
        ] {
            assert_eq!(actual, fp2(&row[name]), "{name} {row}");
        }
    }
    for row in fixture["inverse"].as_array().unwrap() {
        assert_eq!(fp(&row["input"]).inverse(), fp(&row["output"]), "{row}");
    }
    for row in fixture["inverse2"].as_array().unwrap() {
        assert_eq!(fp2(&row["input"]).inverse(), fp2(&row["output"]), "{row}");
    }
}
