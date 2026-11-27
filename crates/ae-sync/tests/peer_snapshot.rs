mod common;
use ae_chain::{
    block::{Block, MicroBlock},
    repository::ledger,
};
use ae_codec::{
    api::{self, ApiType},
    headers::{Header, HeaderMode},
};
use ae_p2p::message::{Limits, Message};
use ae_protocol::NetworkProfile;
use ae_sync::peer::snapshot;
use common::Node;

fn blocks() -> [Block; 2] {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../ae-storage/tests/fixtures/public_bootstrap.json"
    ))
    .unwrap();
    let Header::Key(key) = Header::decode(
        &hex::decode(fixture["generations"][0]["hex"].as_str().unwrap()).unwrap(),
        HeaderMode::ProofOfWork,
    )
    .unwrap() else {
        panic!()
    };
    let row = &fixture["micros"][0];
    let Header::Micro(header) = Header::decode(
        &hex::decode(row["hex"].as_str().unwrap()).unwrap(),
        HeaderMode::ProofOfWork,
    )
    .unwrap() else {
        panic!()
    };
    let transactions = row["transactions"]["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| {
            ae_tx::SignedTransaction::decode(
                &api::decode_as(ApiType::Transaction, t["encoded_tx"].as_str().unwrap()).unwrap(),
            )
            .unwrap()
        })
        .collect();
    [
        Block::Key(key),
        Block::Micro(MicroBlock {
            header,
            transactions,
            proof: None,
        }),
    ]
}
#[test]
fn one_snapshot_keeps_genesis_while_validated_public_key_and_micro_change_selected_top() {
    let node = Node::new();
    let pinned = node.db.pool_view().unwrap();
    let initial = snapshot::local(pinned.chain(), false).unwrap();
    assert_eq!(initial.height, 0);
    assert_eq!(initial.best, initial.genesis);
    let network = NetworkProfile::mainnet();
    let config = ae_execution::transactions::Config::primitives(&network);
    let rules = ae_chain::key::Rules::new(
        ae_chain::admission::PowRules {
            edge_bits: Default::default(),
            now_ms: u64::MAX - 600_000,
            micro_cycle_ms: 3000,
            configured_gas_limit: 6_000_000.into(),
            body_limits: Default::default(),
        },
        initial.genesis,
    );
    let blocks = blocks();
    let key = blocks[0].hash().unwrap();
    for block in &blocks {
        let plan = ledger::prepare(
            &node.db.view().unwrap(),
            block,
            &config,
            &rules,
            Default::default(),
        )
        .unwrap();
        node.db.commit(plan).unwrap();
        let view = node.db.view().unwrap();
        let local = snapshot::local(&view, true).unwrap();
        assert_eq!(local.best, key);
        assert_eq!(local.height, 1);
        assert!(local.temporary);
        assert_eq!(
            local.difficulty,
            view.require_record(key).unwrap().difficulty
        );
        assert_eq!(snapshot::local(pinned.chain(), false).unwrap(), initial);
    }
    assert_ne!(node.db.view().unwrap().status().tip, Some(key));
    let stored = node.db.pool_view().unwrap();
    let native: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/erlang_peer_read.json")).unwrap();
    let rows = native["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 37);
    for row in rows {
        let request = Message::decode_packet(
            &hex::decode(row["request"].as_str().unwrap()).unwrap(),
            Limits::default(),
        )
        .unwrap();
        let response = snapshot::serve(&stored, &request, Limits::default())
            .unwrap()
            .unwrap();
        assert_eq!(
            hex::encode(response.encode_packet(Limits::default()).unwrap()),
            row["response"].as_str().unwrap(),
            "{request:?}"
        );
        if request.kind() == ae_p2p::message::Kind::GetHeaderByHash
            && request.fields()["hash"] == ae_p2p::message::Value::Binary(key.0.to_vec())
        {
            assert!(matches!(
                snapshot::serve(
                    &stored,
                    &request,
                    Limits {
                        body_bytes: 16,
                        ..Default::default()
                    }
                ),
                Err(snapshot::Error::Message(_))
            ));
        }
    }
}

#[test]
fn public_bootstrap_generations_include_stored_transactions_and_preserve_snapshot_selection() {
    use ae_p2p::message::{Details, Kind, Value};
    let node = Node::new();
    let pinned = node.db.pool_view().unwrap();
    let genesis = pinned.chain().identity().genesis;
    let network = NetworkProfile::mainnet();
    let config = ae_execution::transactions::Config::primitives(&network);
    let rules = ae_chain::key::Rules::new(
        ae_chain::admission::PowRules {
            edge_bits: Default::default(),
            now_ms: u64::MAX - 600_000,
            micro_cycle_ms: 3000,
            configured_gas_limit: 6_000_000.into(),
            body_limits: Default::default(),
        },
        genesis,
    );
    let blocks = blocks();
    for block in &blocks {
        let plan = ledger::prepare(
            &node.db.view().unwrap(),
            block,
            &config,
            &rules,
            Default::default(),
        )
        .unwrap();
        node.db.commit(plan).unwrap();
    }
    let stored = node.db.pool_view().unwrap();
    let key = blocks[0].hash().unwrap();
    let request = Message::new(
        Kind::GetGeneration,
        1,
        std::collections::BTreeMap::from([
            ("hash".into(), Value::Binary(key.0.to_vec())),
            ("forward".into(), Value::Boolean(true)),
        ]),
        Default::default(),
    )
    .unwrap();
    let response = snapshot::serve(&stored, &request, Default::default())
        .unwrap()
        .unwrap();
    let Details::Response {
        message: Some(Ok(body)),
        ..
    } = response.details()
    else {
        panic!()
    };
    assert_eq!(
        body.fields()["key_block"],
        Value::Binary(blocks[0].encode().unwrap())
    );
    assert_eq!(
        body.fields()["micro_blocks"],
        Value::List(vec![Value::Binary(blocks[1].encode().unwrap())])
    );
    let old = snapshot::serve(&pinned, &request, Default::default())
        .unwrap()
        .unwrap();
    assert_eq!(
        old.fields()["reason"],
        Value::Binary(b"block_not_found".to_vec())
    );
    let request = Message::new(
        Kind::GetSuccessors,
        2,
        std::collections::BTreeMap::from([
            ("from_hash".into(), Value::Binary(genesis.0.to_vec())),
            (
                "target_hash".into(),
                Value::Binary(blocks[1].hash().unwrap().0.to_vec()),
            ),
            ("n".into(), Value::Integer(u64::MAX.into())),
        ]),
        Default::default(),
    )
    .unwrap();
    let response = snapshot::serve(&stored, &request, Default::default())
        .unwrap()
        .unwrap();
    let Details::Response {
        message: Some(Ok(body)),
        ..
    } = response.details()
    else {
        panic!()
    };
    let mut entry = 1u64.to_be_bytes().to_vec();
    entry.extend(key.0);
    assert_eq!(
        body.fields()["header_hashes"],
        Value::List(vec![Value::Binary(entry)])
    );
    let old = snapshot::serve(&pinned, &request, Default::default())
        .unwrap()
        .unwrap();
    assert_eq!(
        old.fields()["reason"],
        Value::Binary(b"not_on_chain".to_vec())
    );
}
