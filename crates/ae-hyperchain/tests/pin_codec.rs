use ae_hyperchain::pin::{self, Error, Parent};
use serde_json::Value;
fn bytes(v: &Value) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn parent(v: &Value) -> Parent {
    Parent {
        epoch: v["epoch"].as_str().unwrap().parse().unwrap(),
        height: v["height"].as_str().unwrap().parse().unwrap(),
        block_hash: bytes(&v["hash"]),
    }
}
#[test]
fn original_erlang_pin_payloads() {
    let data: Value = serde_json::from_str(include_str!("fixtures/pin_codec.json")).unwrap();
    let mut count = 0;
    for row in data["encoded"].as_array().unwrap() {
        let encoded = bytes(&row["encoded"]);
        assert_eq!(pin::encode_parent(&parent(row), 4096).unwrap(), encoded);
        let expected = if row["decoded"].is_null() {
            Err(Error::ParentPayload)
        } else {
            Ok(parent(&row["decoded"]))
        };
        assert_eq!(pin::decode_parent(&encoded, 4096), expected);
        count += 1;
    }
    for row in data["decoded"].as_array().unwrap() {
        let expected = if row["result"].is_null() {
            Err(Error::ParentPayload)
        } else {
            Ok(parent(&row["result"]))
        };
        assert_eq!(
            pin::decode_parent(&bytes(&row["input"]), 4096),
            expected,
            "{row}"
        );
        count += 1;
    }
    for row in data["children"].as_array().unwrap() {
        assert_eq!(
            pin::encode_child(&bytes(&row["hash"]), 4096).unwrap(),
            bytes(&row["encoded"])
        );
        assert_eq!(
            pin::decode_child(&bytes(&row["encoded"]), 4096).unwrap(),
            bytes(&row["hash"])
        );
        count += 1;
    }
    for row in data["invalid_children"].as_array().unwrap() {
        assert_eq!(
            pin::decode_child(&bytes(&row["input"]), 4096).is_ok(),
            row["valid"].as_bool().unwrap()
        );
        count += 1;
    }
    assert_eq!(count, 200);
}
#[test]
fn codec_limits_are_operational_and_accept_exact_boundary() {
    let p = Parent {
        epoch: (-256).into(),
        height: 16.into(),
        block_hash: vec![42; 32],
    };
    let b = pin::encode_parent(&p, 4096).unwrap();
    assert_eq!(pin::encode_parent(&p, b.len()).unwrap(), b);
    assert_eq!(
        pin::encode_parent(&p, b.len() - 1),
        Err(Error::ResourceLimit)
    );
    assert_eq!(pin::decode_parent(&b, b.len()).unwrap(), p);
    assert_eq!(
        pin::decode_parent(&b, b.len() - 1),
        Err(Error::ResourceLimit)
    );
    assert_eq!(pin::encode_child(b"", 3).unwrap(), b"pin");
    assert_eq!(pin::encode_child(b"", 2), Err(Error::ResourceLimit));
    assert_eq!(pin::decode_child(b"pin", 2), Err(Error::ResourceLimit));
}
