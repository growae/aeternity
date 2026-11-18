use ae_crypto::bls::{self, IntegerConversionError as Error, Profile};
use num_bigint::BigUint;
use serde_json::Value;

#[test]
fn field_to_integer_matches_native_defined_results_and_identifies_undefined_writes() {
    let rows: Value =
        serde_json::from_str(include_str!("fixtures/erlang_bls_reverse.json")).unwrap();
    let mut successes = 0;
    let mut rejections = 0;
    let mut unresolved = 0;
    for row in rows.as_array().unwrap() {
        let raw = hex::decode(row["raw"].as_str().unwrap()).unwrap();
        let profile = Profile::Mcl11976beLlvm64VintArm64;
        let result = match row["field"].as_str().unwrap() {
            "fr" => bls::fr_to_integer(profile, &raw.try_into().unwrap()),
            "fp" => bls::fp_to_integer(profile, &raw.try_into().unwrap()),
            _ => unreachable!(),
        };
        let expected = if row["boundary_byte"] == 0 {
            // Original C++ with oversized backing storage observes the NUL
            // write at the advertised boundary. No NIF result is fabricated.
            assert!(row["native"].is_null());
            unresolved += 1;
            Err(Error::UndefinedDecimalBufferWrite)
        } else if row["native"]["kind"] == "exception" {
            assert_eq!(row["native"]["reason"], "badmatch");
            assert_eq!(row["bounded_return"], 0);
            rejections += 1;
            Err(Error::DecimalBufferRejected)
        } else {
            assert_eq!(row["native"]["kind"], "ok");
            assert_eq!(row["native"]["value"], row["decoded"]);
            successes += 1;
            Ok(row["decoded"].as_str().unwrap().parse::<BigUint>().unwrap())
        };
        assert_eq!(result, expected, "{row}");
    }
    assert!(successes > 1000 && rejections > 1000 && unresolved > 10);
}
