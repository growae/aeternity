use ae_crypto::verify_ed25519;
use ae_types::{AccountPublicKey, Signature};

#[test]
fn libsodium_signature_acceptance_corpus() {
    let rows: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/libsodium_ed25519.json")).unwrap();
    for row in rows.as_array().unwrap() {
        let key = AccountPublicKey::try_from(
            hex::decode(row["public_key"].as_str().unwrap())
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let sig = Signature::try_from(
            hex::decode(row["signature"].as_str().unwrap())
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let msg = hex::decode(row["message"].as_str().unwrap()).unwrap();
        assert_eq!(
            verify_ed25519(&key, &msg, &sig),
            row["valid"].as_bool().unwrap(),
            "{}",
            row["name"]
        );
    }
}
