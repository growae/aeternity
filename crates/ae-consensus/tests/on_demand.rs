use ae_codec::headers::{CommonHeader, Header, HeaderMode, KeyHeader};
use ae_consensus::{headers, on_demand};
use ae_protocol::{Activation, NetworkProfile, Protocol};
use serde_json::Value;
#[test]
fn original_target_sealing_and_canonical_header_bytes() {
    let data: Value = serde_json::from_str(include_str!("fixtures/erlang_on_demand.json")).unwrap();
    let mut accepted = 0;
    let mut rejected = 0;
    for row in data["blocks"].as_array().unwrap() {
        let version = row["version"].as_u64().unwrap() as u32;
        let height = row["height"].as_u64().unwrap();
        let mut key = KeyHeader {
            common: CommonHeader {
                version,
                height,
                previous_hash: [1; 32],
                previous_key_hash: [2; 32],
                state_hash: [42; 32],
            },
            miner: [3; 32],
            beneficiary: [4; 32],
            target: row["target"].as_u64().unwrap() as u32,
            seal: [0; 42],
            nonce: 17,
            time: 1000,
            info: row["info"].as_u64().map(|n| (n as u32).to_be_bytes()),
            hole: false,
            end_of_epoch: false,
        };
        let network = NetworkProfile::custom(
            b"ae_dev".to_vec(),
            vec![Activation {
                height: 0,
                protocol: Protocol::try_from(version).unwrap(),
            }],
        )
        .unwrap();
        if row["result"]["ok"] == false {
            // Native header construction rejects these target words; Rust's
            // typed encoding boundary leaves that check to on-demand admission.
            key.common.height = 1;
            assert!(matches!(
                on_demand::validate_standalone(&Header::Key(key), &network, 1000),
                Err(headers::Error::WrongTarget { .. })
            ));
            rejected += 1;
            continue;
        }
        let expected = &row["result"]["value"];
        assert_eq!(
            hex::encode_upper(Header::Key(key.clone()).encode().unwrap()),
            expected["before_hex"].as_str().unwrap()
        );
        assert_eq!(
            hex::encode_upper(on_demand::sealing_input(&key)),
            expected["sealing_input"].as_str().unwrap()
        );
        on_demand::seal(&mut key);
        assert_eq!(key.nonce, expected["nonce"].as_u64().unwrap());
        let encoded = Header::Key(key.clone()).encode().unwrap();
        assert_eq!(
            hex::encode_upper(&encoded),
            expected["sealed_hex"].as_str().unwrap()
        );
        assert_eq!(
            Header::decode(&encoded, HeaderMode::ProofOfWork)
                .unwrap()
                .encode()
                .unwrap(),
            encoded
        );
        if height != 0 {
            assert!(on_demand::validate_standalone(&Header::Key(key), &network, 1000).is_ok());
        }
        accepted += 1;
    }
    assert_eq!((accepted, rejected), (24, 48));
    println!("On-demand headers: 24 native sealed-byte comparisons and 48 target rejections");
}
