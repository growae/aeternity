use ae_protocol::{Activation, NetworkProfile, Protocol};
use ae_tx::{SignedTransaction, Spend};

#[test]
fn original_erlang_signed_spend_acceptance_hashes_and_normalization() {
    let cases: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/erlang_signed.json")).unwrap();
    assert_eq!(cases.as_array().unwrap().len(), 154);
    let protocols = [
        Protocol::Roma,
        Protocol::Minerva,
        Protocol::Fortuna,
        Protocol::Lima,
        Protocol::Iris,
        Protocol::Ceres,
        Protocol::Arcus,
        Protocol::Salus,
    ];
    for case in cases.as_array().unwrap() {
        let bytes = hex::decode(case["encoded"].as_str().unwrap()).unwrap();
        let result = SignedTransaction::<Spend>::decode(&bytes);
        assert_eq!(
            result.is_ok(),
            case["decoded"].as_bool().unwrap(),
            "{}",
            case["name"]
        );
        let Ok(signed) = result else { continue };
        assert_eq!(
            hex::encode(signed.encode()),
            case["canonical"].as_str().unwrap(),
            "{}",
            case["name"]
        );
        assert_eq!(
            hex::encode(signed.hash().0),
            case["hash"].as_str().unwrap(),
            "{}",
            case["name"]
        );
        let network = match case["network"].as_str().unwrap() {
            "ae_mainnet" => NetworkProfile::mainnet(),
            "ae_uat" => NetworkProfile::uat(),
            id => NetworkProfile::custom(
                id.as_bytes().to_vec(),
                vec![Activation {
                    height: 0,
                    protocol: Protocol::Roma,
                }],
            )
            .unwrap(),
        };
        let protocol = protocols[case["protocol"].as_u64().unwrap() as usize - 1];
        assert_eq!(
            signed
                .verify(&network, protocol, case["inner"].as_bool().unwrap())
                .is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}",
            case["name"]
        );
    }
}
