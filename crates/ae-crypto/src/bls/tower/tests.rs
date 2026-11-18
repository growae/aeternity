use super::*;
#[test]
fn raw_fp12_square_matches_original_native() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../tests/fixtures/erlang_bls_gt.json")).unwrap();
    fn read(value: &serde_json::Value) -> Gt {
        std::array::from_fn(|i| {
            hex::decode(value[i].as_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap()
        })
    }
    for (i, p) in fixture["points"].as_array().unwrap().iter().enumerate() {
        assert_eq!(
            Fp12::read(&read(p)).square().bytes(),
            read(&fixture["square"][i]),
            "Fp12 square {i}"
        );
    }
}
