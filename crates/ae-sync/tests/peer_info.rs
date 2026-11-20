use ae_p2p::message::Limits;
use ae_sync::peer::info::{self, Metadata};
use ae_types::Unsigned;
use serde_json::Value;

#[test]
fn node_info_packets_and_disabled_silence_match_original_callbacks() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_peer_info.json")).unwrap();
    let mut counts = [0; 2];
    for row in fixture["rows"].as_array().unwrap() {
        let meta = &row["metadata"];
        let bytes = |key| hex::decode(meta[key].as_str().unwrap()).unwrap();
        let metadata = Metadata {
            version: bytes("version"),
            revision: bytes("revision"),
            vendor: bytes("vendor"),
            os: bytes("os"),
        };
        let enabled = row["config"] != false;
        let verified = Unsigned::from_decimal(row["verified"].as_str().unwrap()).unwrap();
        let unverified = Unsigned::from_decimal(row["unverified"].as_str().unwrap()).unwrap();
        let response = info::response(
            enabled.then_some(&metadata),
            &bytes("network"),
            &verified,
            &unverified,
            Default::default(),
        )
        .unwrap();
        if let Some(response) = response {
            assert_eq!(
                hex::encode(response.encode_packet(Default::default()).unwrap()),
                row["response"].as_str().unwrap()
            );
            assert_eq!(row["calls"].as_array().unwrap().len(), 8);
            assert!(
                info::response(
                    Some(&metadata),
                    &bytes("network"),
                    &verified,
                    &unverified,
                    Limits {
                        body_bytes: 1,
                        ..Default::default()
                    }
                )
                .is_err()
            );
            counts[0] += 1;
        } else {
            assert!(row["response"].is_null());
            assert_eq!(row["calls"], serde_json::json!(["config"]));
            assert!(
                info::response(
                    None,
                    &[],
                    &verified,
                    &unverified,
                    Limits {
                        body_bytes: 0,
                        work_bytes: 0,
                        items: 0,
                        depth: 0
                    }
                )
                .unwrap()
                .is_none()
            );
            counts[1] += 1;
        }
    }
    assert_eq!(counts, [24, 12]);
    let metadata = Metadata::current();
    assert_eq!(metadata.version, env!("CARGO_PKG_VERSION").as_bytes());
    assert_eq!(
        metadata.revision,
        option_env!("AETERNITY_REVISION").unwrap_or("").as_bytes()
    );
    assert_eq!(metadata.vendor, b"GrowAE Rust node");
    assert_eq!(
        metadata.os,
        format!("{}:{}", std::env::consts::FAMILY, std::env::consts::OS).as_bytes()
    );
}
