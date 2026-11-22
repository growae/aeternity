use super::*;

#[test]
fn hyperchain_header_responses_preserve_hole_and_epoch_flags() {
    let row = &crate::test_support::hyperchain::DATA["rows"][0]["rows"];
    for index in [3, 5, 10] {
        let block = crate::test_support::hyperchain::block(&row[index]);
        let header = block.header();
        let message = Message::new(
            Kind::Header,
            1,
            BTreeMap::from([("hdr".into(), Value::Binary(header.encode().unwrap()))]),
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            wire::header(
                &message,
                &BTreeMap::new(),
                ae_codec::headers::HeaderMode::Hyperchain
            )
            .unwrap(),
            header
        );
        assert!(
            wire::header(
                &message,
                &BTreeMap::new(),
                ae_codec::headers::HeaderMode::ProofOfWork
            )
            .is_err()
        );
    }
}
use agreement::{Probe, Search, Step};
use serde_json::Value as Json;
fn fixture() -> Json {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_chain_sync.json"
    ))
    .unwrap()
}
fn hash(j: &Json) -> Hash256 {
    Hash256(
        hex::decode(j.as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap(),
    )
}
fn point(j: &Json) -> Point {
    Point {
        height: j["height"].as_u64().unwrap(),
        hash: hash(&j["hash"]),
    }
}
fn checkpoints(row: &Json) -> BTreeMap<u64, Hash256> {
    row["blocked_at"]
        .as_u64()
        .map(|n| {
            (
                n,
                Hash256({
                    let mut b = [0; 32];
                    b[31] = 255;
                    b
                }),
            )
        })
        .into_iter()
        .collect()
}
#[test]
fn original_ancestor_agreement_matches_queries_hash_checks_and_errors() {
    let fixture = fixture();
    let rows = fixture["agreement"].as_array().unwrap();
    assert_eq!(rows.len(), 1405);
    for (index, row) in rows.iter().enumerate() {
        let target = Point {
            height: row["remote_height"].as_u64().unwrap(),
            hash: hash(&row["remote_hash"]),
        };
        let mut search = Search::new(
            row["local_height"].as_u64().unwrap(),
            target,
            hash(&row["genesis"]),
        );
        let mut queries = row["queries"].as_array().unwrap().iter();
        let mut replies = row["replies"].as_array().unwrap().iter();
        let mut checks = row["checks"].as_array().unwrap().iter();
        let result = loop {
            let probe = match search.next() {
                Step::Agreed(p) => break Ok(p),
                Step::Probe(Probe::Known(p)) => p.hash,
                Step::Probe(Probe::Header(h)) => {
                    let query = queries.next().expect("native header query");
                    assert_eq!(query["height"], h, "row {index}");
                    assert_eq!(hash(&query["top"]), target.hash);
                    if row["fail_at"].as_u64() == Some(h) {
                        break Err("injected_fetch_failure");
                    }
                    let reply = replies.next().unwrap();
                    assert_eq!(reply["height"], h);
                    let message = Message::new(
                        Kind::Header,
                        1,
                        BTreeMap::from([(
                            "hdr".into(),
                            Value::Binary(hex::decode(reply["header"].as_str().unwrap()).unwrap()),
                        )]),
                        Default::default(),
                    )
                    .unwrap();
                    match wire::header(
                        &message,
                        &checkpoints(row),
                        ae_codec::headers::HeaderMode::ProofOfWork,
                    ) {
                        Ok(header) => header.hash().unwrap(),
                        Err(Error::BlockedByWhitelist) => break Err("blocked_by_whitelist"),
                        other => panic!("unexpected header result: {other:?}"),
                    }
                }
            };
            let check = checks.next().expect("native hash check");
            assert_eq!(probe, hash(&check["hash"]), "row {index}");
            search
                .observe(probe, check["connected"].as_bool().unwrap())
                .unwrap();
        };
        match result {
            Ok(p) => {
                assert_eq!(row["result"]["kind"], "ok");
                assert_eq!(p, point(&row["result"]));
            }
            Err(e) => {
                assert_eq!(row["result"]["kind"], "error");
                assert_eq!(row["result"]["reason"], e);
            }
        }
        assert!(queries.next().is_none());
        assert!(replies.next().is_none());
        assert!(checks.next().is_none());
    }
}
#[test]
fn original_initial_headers_and_successor_gates_preserve_native_semantics() {
    let f = fixture();
    assert_eq!(f["initial"].as_array().unwrap().len(), 8);
    for row in f["initial"].as_array().unwrap() {
        let message = Message::new(
            Kind::Header,
            1,
            BTreeMap::from([(
                "hdr".into(),
                Value::Binary(hex::decode(row["header"].as_str().unwrap()).unwrap()),
            )]),
            Default::default(),
        )
        .unwrap();
        let header = wire::header(
            &message,
            &BTreeMap::new(),
            ae_codec::headers::HeaderMode::ProofOfWork,
        )
        .unwrap();
        assert_eq!(wire::target(&header).unwrap(), point(&row["target"]));
        // These synthetic native headers have no valid PoW: announcement
        // parsing is not full block admission. Genesis also skips whitelist.
        if header.common().height == 0 {
            assert!(
                wire::header(
                    &message,
                    &BTreeMap::from([(0, Hash256([255; 32]))]),
                    ae_codec::headers::HeaderMode::ProofOfWork
                )
                .is_ok()
            );
        }
    }
    let rows = f["successors"].as_array().unwrap();
    assert_eq!(rows.len(), 27);
    for row in rows {
        let message = Message::decode_packet(
            &hex::decode(row["packet"].as_str().unwrap()).unwrap(),
            Default::default(),
        )
        .unwrap();
        let result = wire::successors(&message, Hash256([0; 32]), 4096, &checkpoints(row));
        match row["result"].as_str().unwrap() {
            "ok" | "empty" => assert!(result.is_ok()),
            "blocked" => assert_eq!(result, Err(Error::BlockedByWhitelist)),
            "nonconsecutive" => assert_eq!(result, Err(Error::NonConsecutive)),
            other => panic!("{other}"),
        }
    }
}
#[test]
fn original_successor_callback_filters_short_rows_and_local_limits_bound_invalid_hashes() {
    let f = fixture();
    let rows = f["raw_successors"].as_array().unwrap();
    assert_eq!(rows.len(), 9);
    for row in rows {
        let fields = BTreeMap::from([(
            "header_hashes".into(),
            Value::List(
                row["input"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|s| Value::Binary(hex::decode(s.as_str().unwrap()).unwrap()))
                    .collect(),
            ),
        )]);
        let message = Message::new(Kind::HeaderHashes, 1, fields, Default::default()).unwrap();
        let result = wire::successors(&message, Hash256([0; 32]), 4096, &BTreeMap::new());
        if row["decoded"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["hash"].as_str().unwrap().len() != 64)
        {
            assert_eq!(result, Err(Error::Hash));
        } else {
            assert_eq!(
                result.unwrap(),
                row["decoded"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(point)
                    .collect::<Vec<_>>()
            );
        }
        if !row["input"].as_array().unwrap().is_empty() {
            assert_eq!(
                wire::successors(&message, Hash256([0; 32]), 0, &BTreeMap::new()),
                Err(Error::ResourceLimit)
            );
        }
    }
    let mut search = Search::new(
        0,
        Point {
            height: 0,
            hash: Hash256([1; 32]),
        },
        Hash256([0; 32]),
    );
    assert_eq!(
        search.observe(Hash256([1; 32]), false),
        Err(agreement::Error::NoProgress)
    );
}
