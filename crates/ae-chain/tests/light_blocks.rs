use ae_chain::{
    block::{Block, DecodeConfig, Light},
    gossip::{self, Environment, Fragment, Precheck},
};
use ae_codec::headers::{Header, HeaderMode};
use ae_tx::{SignedTransaction, Transaction};
use serde_json::Value as J;
use std::sync::LazyLock;
static DATA: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_light_blocks.json")).unwrap()
});
fn bytes(v: &J) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn header(v: &J) -> Header {
    Header::decode(&bytes(v), HeaderMode::ProofOfWork).unwrap()
}
fn signed(v: &J) -> SignedTransaction<Transaction> {
    SignedTransaction::decode(&bytes(v)).unwrap()
}
fn list(v: &J) -> Vec<Vec<u8>> {
    v.as_array().unwrap().iter().map(bytes).collect()
}

#[test]
fn original_light_transport_preserves_all_transaction_families_and_proofs() {
    let rows = DATA["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 352);
    for row in rows {
        let Block::Micro(block) =
            Block::decode(&bytes(&row["input"]), DecodeConfig::default()).unwrap()
        else {
            panic!()
        };
        let light = Light::from_micro(&block);
        assert_eq!(light.encode().unwrap(), bytes(&row["light"]));
        assert_eq!(
            Light::decode(&bytes(&row["light"]), DecodeConfig::default()).unwrap(),
            light
        );
        assert_eq!(light.header, header(&row["header"]));
        assert_eq!(light.hashes, list(&row["hashes"]));
        assert_eq!(
            light
                .proof
                .iter()
                .map(|p| p.encode().unwrap())
                .collect::<Vec<_>>(),
            list(&row["proof"])
        );
    }
}
#[test]
fn original_light_envelopes_do_not_constrain_version_or_hash_width() {
    let rows = DATA["mutations"].as_array().unwrap();
    assert_eq!(rows.len(), 60);
    for row in rows {
        let decoded = Light::decode(&bytes(&row["input"]), DecodeConfig::default());
        assert_eq!(decoded.is_ok(), row["result"]["ok"].as_bool().unwrap());
        if let Ok(light) = decoded {
            assert_eq!(light.header, header(&row["result"]["header"]));
            assert_eq!(
                num_bigint::BigUint::from_bytes_be(light.version.as_bytes()).to_string(),
                row["version"].as_str().unwrap()
            );
            assert_eq!(light.hashes, list(&row["hashes"]));
        }
    }
    let raw = bytes(&DATA["rows"][0]["light"]);
    assert!(
        Light::decode(
            &raw,
            DecodeConfig {
                max_block_bytes: raw.len() - 1,
                ..Default::default()
            }
        )
        .unwrap_err()
        .is_operational()
    );
    let raw = bytes(&DATA["assembly"][0]["block"]);
    let Block::Micro(block) = Block::decode(&raw, DecodeConfig::default()).unwrap() else {
        panic!()
    };
    assert!(
        Light::decode(
            &Light::from_micro(&block).encode().unwrap(),
            DecodeConfig {
                max_transactions: 0,
                ..Default::default()
            }
        )
        .unwrap_err()
        .is_operational()
    );
}
#[test]
fn original_local_lookup_and_ordered_reply_assembly_match() {
    let rows = DATA["assembly"].as_array().unwrap();
    assert_eq!(rows.len(), 108);
    for row in rows {
        let Block::Micro(block) =
            Block::decode(&bytes(&row["block"]), DecodeConfig::default()).unwrap()
        else {
            panic!()
        };
        let known: Vec<_> = row["known"]
            .as_array()
            .unwrap()
            .iter()
            .map(signed)
            .collect();
        let fragment = Fragment::lookup::<gossip::Error>(Light::from_micro(&block), |h| {
            Ok(known.iter().find(|t| t.hash().0.as_slice() == h).cloned())
        })
        .unwrap();
        assert_eq!(
            fragment.missing().map(<[u8]>::to_vec).collect::<Vec<_>>(),
            list(&row["missing"])
        );
        assert_eq!(fragment.missing().next().is_none(), row["state"] == "all");
        let replies = row["replies"]
            .as_array()
            .unwrap()
            .iter()
            .map(signed)
            .collect();
        let result = fragment.fill(replies);
        if row["filled"].is_null() {
            assert_eq!(result, Err(gossip::Error::Transactions));
        } else {
            let filled = result.unwrap();
            assert_eq!(filled.header, block.header);
            assert_eq!(filled.proof, block.proof);
            assert_eq!(
                filled
                    .transactions
                    .iter()
                    .map(|t| t.encode())
                    .collect::<Vec<_>>(),
                list(&row["filled"])
            );
        }
    }
}
fn reason(result: Result<Precheck, gossip::Error>) -> &'static str {
    use gossip::Error::*;
    match result {
        Ok(Precheck::Known) => "known",
        Ok(Precheck::Assemble) => "ok",
        Err(PreviousKeyMissing) => "prev_key_block_not_found",
        Err(Version) => "protocol_version_mismatch",
        Err(Interval) => "bad_micro_block_interval",
        Err(Future) => "block_from_the_future",
        Err(Orphan) => "orphan_blocks_not_allowed",
        Err(TooOld) => "too_far_below_top",
        Err(Height) => "wrong_prev_key_block_height",
        Err(PreviousKeyHash) => "wrong_prev_key_hash",
        Err(Signature) => "signature_verification_failed",
        other => panic!("{other:?}"),
    }
}
#[test]
fn original_preassembly_validation_order_and_height_boundaries_match() {
    let rows = DATA["precheck"].as_array().unwrap();
    assert_eq!(rows.len(), 480);
    for row in rows {
        let previous = (!row["previous"].is_null()).then(|| header(&row["previous"]));
        let key = (!row["previous_key"].is_null()).then(|| header(&row["previous_key"]));
        let key = key.as_ref().map(|h| {
            let Header::Key(k) = h else { panic!() };
            k
        });
        let checked = gossip::precheck(
            &header(&row["header"]),
            Environment {
                known: row["known"].as_bool().unwrap(),
                previous: previous.as_ref(),
                previous_key: key,
                connected: row["connected"].as_bool().unwrap(),
                top_height: row["top"].as_u64(),
                gossip_depth: row["depth"].as_u64().unwrap(),
                now_ms: row["now"].as_u64().unwrap(),
                micro_cycle_ms: row["cycle"].as_u64().unwrap(),
                hyperchain: row["hyperchain"].as_bool().unwrap(),
            },
        );
        assert_eq!(reason(checked), row["result"].as_str().unwrap(), "{row}");
    }
    let rows = DATA["gates"].as_array().unwrap();
    assert_eq!(rows.len(), 42);
    for row in rows {
        assert_eq!(
            gossip::allows_height(row["height"].as_u64().unwrap(), row["sync"].as_u64()),
            row["result"].as_bool().unwrap()
        );
    }
}
