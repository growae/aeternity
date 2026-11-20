use ae_codec::api::{self, ApiType, SafeDecodeError};
use ae_p2p::address::{self, Address, Error, Limits, Port};
use ae_types::PeerPublicKey;
use serde_json::{Value, json};
fn generous() -> Limits {
    Limits {
        bytes: 65536,
        host_bytes: 16384,
        userinfo_bytes: 16384,
        port_digits: 4096,
    }
}
fn key() -> PeerPublicKey {
    let mut k = [0; 32];
    k[31] = 7;
    PeerPublicKey(k)
}
fn decode_hex(v: &Value) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn compare(input: &[u8], expected: &Value) {
    let result = address::parse(input, generous());
    match expected["kind"].as_str().unwrap() {
        "ok" => {
            let a = result.unwrap_or_else(|e| panic!("{}: {e:?}", String::from_utf8_lossy(input)));
            assert_eq!(
                json!({"kind":"ok","key":hex::encode(a.id.as_bytes()),"host":hex::encode(a.host),"port":a.port.map(|p|p.as_decimal().to_owned())}),
                *expected,
                "{}",
                String::from_utf8_lossy(input)
            );
        }
        "error" => {
            let reason = String::from_utf8(decode_hex(&expected["reason_hex"])).unwrap();
            let e = match reason.as_str() {
                "invalid_encoding" => Error::Key(SafeDecodeError::InvalidEncoding),
                "invalid_prefix" => Error::Key(SafeDecodeError::InvalidPrefix),
                _ => Error::InvalidUri,
            };
            assert_eq!(
                result,
                Err(e),
                "{}: {reason}",
                String::from_utf8_lossy(input)
            );
        }
        "exception" => assert_eq!(
            result,
            Err(Error::InvalidUri),
            "{}",
            String::from_utf8_lossy(input)
        ),
        kind => panic!("unknown native result {kind}"),
    }
}
#[test]
fn original_peer_uri_parsing_and_raw_encoding_match_without_network_access() {
    let data: Value =
        serde_json::from_str(include_str!("fixtures/erlang_peer_address.json")).unwrap();
    let parses = data["parses"].as_array().unwrap();
    for row in parses {
        assert_eq!(row["binary"], row["list"]);
        compare(&decode_hex(&row["input"]), &row["binary"]);
    }
    let encodes = data["encodes"].as_array().unwrap();
    for row in encodes {
        let host = decode_hex(&row["host"]);
        let port = Port::from_decimal(row["port"].as_str().unwrap()).unwrap();
        let encoded = address::encode(key(), &host, &port, generous()).unwrap();
        assert_eq!(encoded, decode_hex(&row["encoded"]));
        compare(&encoded, &row["parsed"]);
    }
    assert!(!data["full_node_started"].as_bool().unwrap());
    assert!(!data["network_access"].as_bool().unwrap());
    println!(
        "Original peer addresses: {} binary/list parses and {} raw encodes",
        parses.len(),
        encodes.len()
    );
}
#[test]
fn endpoint_admission_retains_lossless_parse_data_and_rejects_unusable_ports_and_families() {
    let k = api::encode(ApiType::PeerPubkey, key().as_bytes());
    for (host, port, error) in [
        ("host", "", Error::MissingPort),
        ("host", "0", Error::UnusablePort),
        ("host", "65536", Error::UnusablePort),
        ("host", "18446744073709551616", Error::UnusablePort),
        ("", "1", Error::UnusableHost),
        ("[::1]", "1", Error::UnsupportedFamily),
        ("[::1]abc", "1", Error::UnsupportedFamily),
    ] {
        let a =
            address::parse(format!("aenode://{k}@{host}:{port}").as_bytes(), generous()).unwrap();
        let saved = a.clone();
        assert_eq!(a.registry_info(), Err(error));
        assert_eq!(a, saved);
    }
    for host in ["Example.COM.", "127.0.0.1", "a%2fb"] {
        let a = address::parse(
            format!("aenode://{k}@{host}:003015/path?x#y").as_bytes(),
            generous(),
        )
        .unwrap();
        let info = a.registry_info().unwrap();
        assert_eq!(info.host, host.as_bytes());
        assert_eq!(info.port, 3015);
        assert_eq!(info.id, key());
        assert_eq!(
            address::encode(a.id, &a.host, a.port.as_ref().unwrap(), generous()).unwrap(),
            format!("aenode://{k}@{host}:3015").into_bytes()
        );
    }
    assert_eq!(Port::from_decimal("-000").unwrap().as_decimal(), "0");
    assert_eq!(Port::from_decimal("-001").unwrap().as_decimal(), "-1");
    assert_eq!(
        Port::from_decimal("-1").unwrap().transport(),
        Err(Error::UnusablePort)
    );
    assert_eq!(Port::from_decimal("+1"), Err(Error::InvalidInteger));
    let huge = Address {
        id: key(),
        host: vec![b'a'; 65537],
        port: Some(1u16.into()),
    };
    assert_eq!(huge.registry_info(), Err(Error::ResourceLimit));
    let opaque = Address {
        id: key(),
        host: vec![0, 255],
        port: Some(1u16.into()),
    };
    assert_eq!(opaque.registry_info(), Err(Error::UnusableHost));
}
#[test]
fn independent_work_limits_and_hard_ceilings_are_checked_before_allocating_outputs() {
    let host = b"abc";
    let port = Port::from_decimal("3015").unwrap();
    let input = address::encode(key(), host, &port, generous()).unwrap();
    let full = Limits {
        bytes: input.len(),
        host_bytes: 3,
        userinfo_bytes: api::encode(ApiType::PeerPubkey, key().as_bytes()).len(),
        port_digits: 4,
    };
    assert!(address::parse(&input, full).is_ok());
    assert_eq!(address::encode(key(), host, &port, full).unwrap(), input);
    for limits in [
        Limits {
            bytes: full.bytes - 1,
            ..full
        },
        Limits {
            host_bytes: 2,
            ..full
        },
        Limits {
            userinfo_bytes: full.userinfo_bytes - 1,
            ..full
        },
        Limits {
            port_digits: 3,
            ..full
        },
    ] {
        assert_eq!(address::parse(&input, limits), Err(Error::ResourceLimit));
        assert_eq!(
            address::encode(key(), host, &port, limits),
            Err(Error::ResourceLimit)
        );
    }
    for limits in [
        Limits { bytes: 0, ..full },
        Limits {
            bytes: (1 << 20) + 1,
            ..full
        },
        Limits {
            host_bytes: 65537,
            ..full
        },
        Limits {
            userinfo_bytes: 0,
            ..full
        },
        Limits {
            userinfo_bytes: 16385,
            ..full
        },
        Limits {
            port_digits: 0,
            ..full
        },
        Limits {
            port_digits: 4097,
            ..full
        },
    ] {
        assert_eq!(address::parse(&input, limits), Err(Error::Configuration));
        assert_eq!(
            address::encode(key(), host, &port, limits),
            Err(Error::Configuration)
        );
    }
    assert_eq!(
        Port::from_decimal(&"0".repeat(4097)),
        Err(Error::ResourceLimit)
    );
    let big = Port::from_decimal(&"9".repeat(4096)).unwrap();
    assert_eq!(big.as_decimal().len(), 4096);
    assert_eq!(big.transport(), Err(Error::UnusablePort));
    let b = address::encode(key(), host, &big, generous()).unwrap();
    assert_eq!(address::parse(&b, generous()).unwrap().port, Some(big));
    assert_eq!(
        address::parse(&b, Limits::default()),
        Err(Error::ResourceLimit)
    );
}
