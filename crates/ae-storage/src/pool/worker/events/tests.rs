use super::*;
use serde_json::{Value, json};

fn submission(tx: &Signed, event: Event) -> Submission {
    Submission {
        outcome: PushOutcome::Accepted {
            hash: tx.hash(),
            event,
            inserted: true,
        },
        chain: Status {
            revision: 5,
            tip: None,
        },
        revision: 7,
    }
}

#[test]
fn native_private_put_trace_keeps_duplicate_events_and_original_event_kinds() {
    let data: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../ae-mempool/tests/fixtures/erlang_gossip.json"
    )))
    .unwrap();
    let txs: Vec<_> = data["events"]["txs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| Arc::new(Signed::decode(&hex::decode(s.as_str().unwrap()).unwrap()).unwrap()))
        .collect();
    let mut feed = Feed::new(32, 100000);
    let mut cursor = feed.cursor();
    for step in data["events"]["steps"].as_array().unwrap() {
        if step["op"] == "put" && step["result"] == "ok" {
            let tx = txs[step["args"][0].as_u64().unwrap() as usize - 1].clone();
            let event = match step["args"][1].as_str().unwrap() {
                "tx_created" => Event::Created,
                "tx_received" => Event::Received,
                other => panic!("{other}"),
            };
            feed.publish(tx.clone(), &submission(&tx, event), 128);
            assert_eq!(step["log"][0]["kind"], "db");
            assert_eq!(step["log"][1]["kind"], "event");
        }
        let batch = feed.read(&cursor, 32, false).unwrap();
        let actual:Vec<_>=batch.events.iter().map(|e|json!({"kind":"event","hash":hex::encode(e.transaction.hash().0),"event":match e.event { Event::Created=>"tx_created",Event::Received=>"tx_received" }})).collect();
        let expected: Vec<_> = step["log"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|l| l["kind"] == "event")
            .cloned()
            .collect();
        assert_eq!(actual, expected);
        cursor = batch.cursor;
    }
    let mut cached = submission(&txs[0], Event::Received);
    cached.outcome = PushOutcome::Cached;
    feed.publish(txs[0].clone(), &cached, 128);
    assert_eq!(cursor, feed.cursor());
    assert_eq!(cursor.sequence, 4);
}

#[test]
fn oversize_creates_an_explicit_gap_and_sequence_rollover_changes_stream() {
    let tx = crate::pool::tests::tx();
    let accepted = submission(&tx, Event::Created);
    let mut feed = Feed::new(10, 256);
    let start = feed.cursor();
    feed.publish(tx.clone(), &accepted, 128);
    feed.publish(tx.clone(), &accepted, 257);
    feed.publish(tx.clone(), &accepted, 128);
    let resume = match feed.read(&start, 10, false).unwrap_err() {
        ReadError::Lagged { missed: 2, resume } => resume,
        other => panic!("{other:?}"),
    };
    assert_eq!(feed.read(&resume, 10, false).unwrap().events[0].sequence, 3);
    let mut ahead = feed.cursor();
    ahead.sequence += 1;
    assert_eq!(feed.read(&ahead, 10, false).unwrap_err(), ReadError::Ahead);
    feed.latest = u64::MAX;
    let old = feed.cursor();
    feed.publish(tx, &accepted, 128);
    assert_eq!(
        feed.read(&old, 10, false).unwrap_err(),
        ReadError::WrongStream
    );
    let batch = feed.read(&feed.oldest(), 10, false).unwrap();
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.events[0].sequence, 1);
}
