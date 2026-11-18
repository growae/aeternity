use ae_codec::headers::{Header, HeaderMode};
use ae_consensus::cuckoo::{sealing_bytes, verify_key_seal};

#[test]
fn captured_mainnet_headers_have_valid_cuckoo29_proofs() {
    let rows: serde_json::Value = serde_json::from_str(include_str!(
        "../../ae-codec/tests/fixtures/mainnet_headers.json"
    ))
    .unwrap();
    let mut checked = 0;
    for row in rows.as_array().unwrap() {
        let bytes = hex::decode(row["expected_hex"].as_str().unwrap()).unwrap();
        let Header::Key(header) = Header::decode(&bytes, HeaderMode::ProofOfWork).unwrap() else {
            panic!("key header")
        };
        if header.common.height == 0 {
            continue;
        } // Genesis is trusted by hash, without mining.
        assert!(
            verify_key_seal(&header).unwrap(),
            "height {}",
            header.common.height
        );
        let sealed = sealing_bytes(&header).unwrap();
        assert_ne!(
            sealed,
            Header::Key(header.clone()).signature_bytes().unwrap()
        );
        let mut changed = header.clone();
        changed.nonce ^= 1;
        assert!(!verify_key_seal(&changed).unwrap());
        changed = header.clone();
        changed.seal.swap(0, 1);
        assert!(!verify_key_seal(&changed).unwrap());
        changed = header.clone();
        changed.seal[1] = changed.seal[0];
        assert!(!verify_key_seal(&changed).unwrap());
        changed = header.clone();
        changed.target = 0x01010000;
        assert!(!verify_key_seal(&changed).unwrap());
        checked += 1;
    }
    assert_eq!(checked, 11);
}
