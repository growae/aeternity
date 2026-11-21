use super::*;
use serde_json::{Value, json};
fn hash(n: u8) -> Hash256 {
    let mut h = [0; 32];
    h[31] = n;
    Hash256(h)
}
#[test]
fn publication_order_and_origins_match_original_conductor_callbacks() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_committed_events.json"
    ))
    .unwrap();
    let cases = corpus["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 24);
    for case in cases {
        let origin = match case["origin"].as_str().unwrap() {
            "synced" => Origin::Synced,
            "received" => Origin::Received,
            "created" => Origin::Created,
            _ => panic!(),
        };
        let kind = if case["block_type"] == "key" {
            Kind::Key
        } else {
            Kind::Micro
        };
        let changed = case["changed"].as_bool().unwrap();
        let payload = Payload {
            context: Context {
                origin,
                block: Some(BlockInfo {
                    hash: hash(9),
                    kind,
                    previous: hash(7),
                    height: 2,
                }),
            },
            change: Change {
                revision: 2,
                inserted: Some(hash(9)),
                removed: vec![],
                discarded: vec![],
                old_tip: Some(hash(8)),
                new_tip: hash(if changed { 9 } else { 8 }),
                ancestor: None,
                detached: vec![],
                attached: vec![],
                requeue: vec![],
                confirm: vec![],
            },
            events: if origin == Origin::Synced {
                vec![]
            } else {
                (1..=case["event_count"].as_u64().unwrap() as u8)
                    .map(|n| batch::Event {
                        transaction_hash: hash(n + 100),
                        transaction_type: ae_tx::TransactionKind::ChannelCreate,
                        value: transactions::Event::Channel {
                            channel: hash(n).0,
                            transaction_hash: hash(n + 100),
                            transaction_type: ae_tx::TransactionKind::ChannelCreate,
                        },
                    })
                    .collect()
            },
            body: None,
            bytes: 0,
        };
        let effects: Vec<Value> = payload.effects().map(|e|match e {
            Effect::Transaction {block,origin,event} => {
                let transactions::Event::Channel {channel,..} = event.value else {panic!()};
                json!({"kind":"transaction","channel":channel[31],"block_hash":hex::encode(block.hash.0),"tx_hash":hex::encode(event.transaction_hash.0),"origin":if origin == Origin::Created {"micro_block_created"} else {"micro_block_received"}})
            },
            Effect::BlockCreated(b)=>json!({"kind":"created","hash":hex::encode(b.hash.0),"block_type":case["block_type"]}),
            Effect::BlockToPublish {block,origin}=>json!({"kind":"publish","hash":hex::encode(block.hash.0),"origin":if origin==Propagation::Created {"created"}else{"received"}}),
            Effect::TopChanged(b)=>json!({"kind":"top","hash":hex::encode(b.hash.0),"block_type":case["block_type"],"previous":hex::encode(b.previous.0),"height":b.height}),
            Effect::Reverted(_)=>panic!(),
        }).collect();
        assert_eq!(json!(effects), case["effects"], "{case}");
    }
}
