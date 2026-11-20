use ae_p2p::{
    message::{self, ChainPoi, Message, Peer},
    ping::{self, Effect},
};
use ae_types::{Hash256, Unsigned};
use serde_json::{Value as J, json};
fn data() -> J {
    serde_json::from_str(include_str!("fixtures/erlang_peer_ping.json")).unwrap()
}
fn decimal(n: &Unsigned) -> String {
    let mut n = n.clone();
    let mut digits = vec![];
    loop {
        let (q, r) = n.div_rem_u64(10).unwrap();
        digits.push(char::from(b'0' + r as u8));
        n = q;
        if n == 0.into() {
            break;
        }
    }
    digits.iter().rev().collect()
}
fn hash(n: u8) -> Hash256 {
    let mut bytes = [0; 32];
    bytes[31] = n;
    Hash256(bytes)
}
fn local(temporary: bool) -> ping::Local {
    ping::Local {
        genesis: hash(0),
        best: hash(1),
        height: 101,
        difficulty: 1000.into(),
        temporary,
    }
}
fn peer(p: &Peer) -> J {
    json!({"host":hex::encode(&p.host),"port":decimal(&p.port),"key":hex::encode(&p.pubkey)})
}
fn effect(e: &Effect) -> J {
    match e {
        Effect::Status {
            temporary,
            genesis,
            best,
            difficulty,
        } => {
            json!({"action":"status","temporary":temporary,"genesis":hex::encode(genesis),"best":hex::encode(best),"difficulty":decimal(difficulty)})
        }
        Effect::Generation(h) => json!({"action":"generation","hash":hex::encode(h)}),
        Effect::SyncDone => json!({"action":"done"}),
        Effect::Sync { best, difficulty } => {
            json!({"action":"sync","hash":hex::encode(best),"difficulty":decimal(difficulty)})
        }
        Effect::Peers(ps) => {
            json!({"action":"peers","source":"127.1.2.3","peers":ps.iter().map(peer).collect::<Vec<_>>()})
        }
        Effect::Capabilities(cs) => {
            json!({"action":"capabilities","wire":hex::encode(message::encode_capabilities(cs,Default::default()).unwrap())})
        }
        Effect::Pool => json!({"action":"pool"}),
    }
}
fn sample() -> Vec<Peer> {
    [
        (b"z.example".as_slice(), 3000, 3),
        (b"a.example".as_slice(), 3015, 4),
        (b"a.example".as_slice(), 1, 5),
    ]
    .into_iter()
    .map(|(h, p, k)| Peer {
        host: h.to_vec(),
        port: p.into(),
        pubkey: hash(k).0.to_vec(),
    })
    .collect()
}
fn caps() -> Vec<ChainPoi> {
    [1, 2]
        .into_iter()
        .map(|n| ChainPoi {
            height: u64::from(n).into(),
            root_hash: hash(n).0.to_vec(),
            genesis: hash(0).0.to_vec(),
            top: hash(1).0.to_vec(),
            poi: vec![n],
        })
        .collect()
}
#[test]
fn original_ping_validation_effect_order_and_upgrade_suppression_match() {
    let data = data();
    let rows = data["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2160);
    let mut crashes = 0;
    for (index, row) in rows.iter().enumerate() {
        let message = Message::decode_packet(
            &hex::decode(row["packet"].as_str().unwrap()).unwrap(),
            Default::default(),
        )
        .unwrap();
        let negotiated = row["negotiated"].as_u64().unwrap();
        let decision = ping::evaluate(
            &local(row["temporary"].as_bool().unwrap()),
            &message,
            (negotiated != 0).then_some(negotiated as u16),
            row["top_target"].as_u64().unwrap(),
            ping::Config {
                sync_start: row["sync_start"].as_i64().unwrap(),
                ..Default::default()
            },
        )
        .unwrap();
        let result = match decision.response {
            Some(Ok(())) => json!({"kind":"ok"}),
            Some(Err(e)) => {
                json!({"kind":"error","reason":std::str::from_utf8(e.reason()).unwrap()})
            }
            None => {
                crashes += 1;
                json!({"kind":"crash","class":"error","reason":"function_clause"})
            }
        };
        assert_eq!(result, row["result"], "row {index}: {row}");
        assert_eq!(
            json!(decision.effects.iter().map(effect).collect::<Vec<_>>()),
            row["effects"],
            "row {index}: {row}"
        );
    }
    assert!(crashes > 0);
    println!(
        "Original ping policy: {} complete decisions, {crashes} malformed-capability effect prefixes",
        rows.len()
    );
}
#[test]
fn original_local_ping_bytes_peer_sorting_shares_and_response_exclusions_match() {
    let data = data();
    let objects = data["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 32);
    for row in objects {
        let share = Unsigned::from_decimal(row["share"].as_str().unwrap()).unwrap();
        let wire = ping::encode(
            &local(row["temporary"].as_bool().unwrap()),
            sample(),
            &caps(),
            ping::Encoding {
                version: row["version"].as_u64().unwrap() as u16,
                minimum_version: row["minimum"].as_u64().unwrap() as u16,
                port: 3015,
                share: share.clone(),
                limits: Default::default(),
            },
        )
        .unwrap()
        .encode_packet(Default::default())
        .unwrap();
        assert_eq!(hex::encode(wire), row["packet"].as_str().unwrap());
        if share != 0.into() {
            assert_eq!(row["sample"]["maximum"], decimal(&share));
            assert_eq!(row["sample"]["percent"], 10);
        } else {
            assert!(row["sample"].is_null());
        }
    }
    let responses = data["responses"].as_array().unwrap();
    assert_eq!(responses.len(), 10);
    for row in responses {
        let share = Unsigned::from_decimal(row["share"].as_str().unwrap()).unwrap();
        let message = ping::encode(
            &local(false),
            sample(),
            &caps(),
            ping::Encoding {
                version: row["version"].as_u64().unwrap() as u16,
                minimum_version: 1,
                port: 3015,
                share: share.clone(),
                limits: Default::default(),
            },
        )
        .unwrap();
        assert_eq!(
            hex::encode(message.encode_packet(Default::default()).unwrap()),
            row["packet"].as_str().unwrap()
        );
        if share != 0.into() {
            let mut their = sample();
            their.truncate(1);
            their[0].pubkey = hash(6).0.to_vec();
            let incoming = ping::encode(
                &local(false),
                their,
                &[],
                ping::Encoding {
                    version: 1,
                    minimum_version: 1,
                    port: 4000,
                    share: 32.into(),
                    limits: Default::default(),
                },
            )
            .unwrap();
            let exclusions = ping::exclusions(&hash(2).0, Some(&incoming)).unwrap();
            assert_eq!(
                json!(exclusions.iter().map(hex::encode).collect::<Vec<_>>()),
                row["sample"]["exclude"]
            );
            assert_eq!(row["sample"]["maximum"], decimal(&share));
        } else {
            assert!(row["sample"].is_null());
        }
    }
    println!(
        "Original ping encoding: 32 local snapshots and 10 replies including arbitrary-precision share"
    );
}
#[test]
fn ping_encoding_limits_are_cumulative_and_configuration_is_checked() {
    let encoding = ping::Encoding {
        version: 2,
        minimum_version: 1,
        port: 3015,
        share: 32.into(),
        limits: Default::default(),
    };
    assert!(matches!(
        ping::Config {
            share: 33,
            ..Default::default()
        }
        .validate(),
        Err(ping::Error::Configuration)
    ));
    assert!(matches!(
        ping::encode(
            &local(false),
            sample(),
            &[],
            ping::Encoding {
                minimum_version: 0,
                ..encoding.clone()
            }
        ),
        Err(ping::Error::Configuration)
    ));
    let baseline = ping::encode(&local(false), sample(), &caps(), encoding.clone()).unwrap();
    for limits in [
        message::Limits {
            items: 1,
            ..Default::default()
        },
        message::Limits {
            body_bytes: 32,
            ..Default::default()
        },
        message::Limits {
            work_bytes: 32,
            ..Default::default()
        },
    ] {
        assert!(matches!(
            ping::encode(
                &local(false),
                sample(),
                &caps(),
                ping::Encoding {
                    limits,
                    ..encoding.clone()
                }
            ),
            Err(ping::Error::Message(message::Error::ResourceLimit))
        ));
    }
    assert_eq!(
        ping::encode(&local(false), sample(), &caps(), encoding).unwrap(),
        baseline
    );
    assert!(!ping::pool_ready(u64::MAX, 0, -100));
    assert!(ping::pool_ready(0, 1, i64::MIN));
    assert!(!ping::pool_ready(101, 202, -100));
    assert!(ping::pool_ready(102, 202, -100));
}
