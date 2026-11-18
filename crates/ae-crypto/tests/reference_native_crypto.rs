use ae_crypto::{blake2b_256, keccak256, recover_secp256k1_address, sha256, verify_secp256k1};
use serde_json::Value as J;
use std::sync::LazyLock;
static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_native_crypto.json")).unwrap()
});
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn array<const N: usize>(j: &J) -> [u8; N] {
    bytes(j).try_into().unwrap()
}

#[test]
fn keccak_sha256_and_blake2b_match_original_native_modules() {
    for row in FIXTURE["hashes"].as_array().unwrap() {
        let input = bytes(&row["input"]);
        assert_eq!(keccak256(&input).0, array::<32>(&row["keccak"]));
        assert_eq!(sha256(&input).0, array::<32>(&row["sha256"]));
        assert_eq!(blake2b_256(&input).0, array::<32>(&row["blake2b"]));
    }
}
#[test]
fn verification_and_recovery_match_original_openssl_and_pinned_secp256k1() {
    for (i, row) in FIXTURE["verify"].as_array().unwrap().iter().enumerate() {
        let result = verify_secp256k1(
            &array(&row["key"]),
            &array(&row["digest"]),
            &array(&row["signature"]),
        );
        let expected = &row["result"];
        if expected["kind"] == "exception" {
            assert_eq!(expected["reason"], "badarg");
            assert!(result.is_err(), "verify {i}");
        } else {
            assert_eq!(result.unwrap(), expected["value"], "verify {i}");
        }
    }
    for (i, row) in FIXTURE["recover"].as_array().unwrap().iter().enumerate() {
        let result = recover_secp256k1_address(&array(&row["digest"]), &array(&row["signature"]));
        assert_eq!(row["result"]["kind"], "ok");
        let expected = &row["result"]["value"];
        assert_eq!(
            result,
            (!expected.is_null()).then(|| array(expected)),
            "recover {i}"
        );
    }
}
