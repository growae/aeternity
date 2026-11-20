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
