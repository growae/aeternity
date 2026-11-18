// Bounded public samples captured 2026-11-18 from mainnet.aeternity.io/v3.
// Expected bytes independently reconstructed with Python hashlib in Goal 1.
use ae_codec::{
    api::{self, ApiType},
    headers::{Header, HeaderError, HeaderMode},
};
use serde_json::Value;

fn samples() -> Vec<Value> {
    serde_json::from_str(include_str!("fixtures/mainnet_headers.json")).unwrap()
}
fn api_hash(value: &Value) -> Vec<u8> {
    api::decode(value.as_str().unwrap()).unwrap().1
}

#[test]
fn genesis_and_both_sides_of_every_fork_match_external_bytes_and_hashes() {
    let samples = samples();
    assert_eq!(samples.len(), 12);
    for sample in samples {
        let data = &sample["api"];
        let bytes = hex::decode(sample["expected_hex"].as_str().unwrap()).unwrap();
        let header = Header::decode(&bytes, HeaderMode::ProofOfWork).unwrap();
        let Header::Key(key) = &header else {
            panic!("key fixture expected")
        };
        assert_eq!(key.common.height, data["height"].as_u64().unwrap());
        assert_eq!(
            u64::from(key.common.version),
            data["version"].as_u64().unwrap()
        );
        assert_eq!(
            key.common.previous_hash.as_slice(),
            api_hash(&data["prev_hash"])
        );
        assert_eq!(
            key.common.previous_key_hash.as_slice(),
            api_hash(&data["prev_key_hash"])
        );
        assert_eq!(
            key.common.state_hash.as_slice(),
            api_hash(&data["state_hash"])
        );
        assert_eq!(key.miner.as_slice(), api_hash(&data["miner"]));
        assert_eq!(key.beneficiary.as_slice(), api_hash(&data["beneficiary"]));
        assert_eq!(u64::from(key.target), data["target"].as_u64().unwrap());
        assert_eq!(key.time, data["time"].as_u64().unwrap());
        assert_eq!(key.nonce, data["nonce"].as_u64().unwrap_or(0));
        if let Some(pow) = data["pow"].as_array() {
            assert_eq!(
                key.seal.iter().map(|x| u64::from(*x)).collect::<Vec<_>>(),
                pow.iter().map(|x| x.as_u64().unwrap()).collect::<Vec<_>>()
            );
        } else {
            assert_eq!(key.seal, [0; 42]);
        }
        assert_eq!(header.encode().unwrap(), bytes);
        let hash = header.hash().unwrap();
        assert_eq!(hash.as_ref(), api_hash(&data["hash"]));
        assert_eq!(
            api::encode(ApiType::KeyBlockHash, hash.as_ref()),
            data["hash"].as_str().unwrap()
        );
    }
}

#[test]
fn key_header_shape_is_not_contextual_consensus_validation() {
    let samples = samples();
    let raw = hex::decode(samples[0]["expected_hex"].as_str().unwrap()).unwrap();
    for len in 0..raw.len() {
        assert!(Header::decode(&raw[..len], HeaderMode::ProofOfWork).is_err());
    }
    let mut extra = raw.clone();
    extra.push(0);
    assert_eq!(
        Header::decode(&extra, HeaderMode::ProofOfWork),
        Err(HeaderError::Malformed)
    );
    for bit in 0..28 {
        let mut bad = raw.clone();
        let flags = u32::from_be_bytes(bad[4..8].try_into().unwrap()) | 1 << bit;
        bad[4..8].copy_from_slice(&flags.to_be_bytes());
        assert_eq!(
            Header::decode(&bad, HeaderMode::ProofOfWork),
            Err(HeaderError::ReservedFlags)
        );
    }
    for bit in [28, 29] {
        let mut hc = raw.clone();
        hc[4] |= 1 << (bit - 24);
        assert_eq!(
            Header::decode(&hc, HeaderMode::ProofOfWork),
            Err(HeaderError::HyperchainFlags)
        );
        let header = Header::decode(&hc, HeaderMode::Hyperchain).unwrap();
        assert_eq!(header.encode().unwrap(), hc);
    }
    let mut roma_info = raw.clone();
    roma_info[..4].copy_from_slice(&1u32.to_be_bytes());
    assert_eq!(
        Header::decode(&roma_info, HeaderMode::ProofOfWork),
        Err(HeaderError::VersionInfo)
    );
}
