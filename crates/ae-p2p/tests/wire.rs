use ae_p2p::{
    framing,
    message::{self, Details, Error, Kind, Limits, Message, Value},
};
use ae_types::Unsigned;
use serde_json::{Value as J, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
fn data() -> J {
    serde_json::from_str(include_str!("fixtures/erlang_peer_wire.json")).unwrap()
}
fn bytes(j: &J) -> Vec<u8> {
    hex::decode(j.as_str().unwrap()).unwrap()
}
fn decimal(n: &Unsigned) -> String {
    let mut n = n.clone();
    let mut out = vec![];
    loop {
        let (q, r) = n.div_rem_u64(10).unwrap();
        out.push(char::from(b'0' + r as u8));
        n = q;
        if n == 0.into() {
            break;
        }
    }
    out.iter().rev().collect()
}
fn value(v: &J) -> Value {
    if let Some(n) = v["integer"].as_str() {
        Value::Integer(Unsigned::from_decimal(n).unwrap())
    } else if let Some(b) = v["binary"].as_str() {
        Value::Binary(hex::decode(b).unwrap())
    } else if let Some(b) = v.as_bool() {
        Value::Boolean(b)
    } else if let Some(xs) = v.as_array() {
        Value::List(xs.iter().map(value).collect())
    } else if let Some(xs) = v["tuple"].as_array() {
        Value::Tuple(xs.iter().map(value).collect())
    } else {
        Value::Record(
            v.as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), value(v)))
                .collect(),
        )
    }
}
fn norm(v: &Value) -> J {
    match v {
        Value::Integer(n) => json!({"integer":decimal(n)}),
        Value::Boolean(b) => json!(b),
        Value::Binary(b) => json!({"binary":hex::encode(b)}),
        Value::List(xs) => json!(xs.iter().map(norm).collect::<Vec<_>>()),
        Value::Tuple(xs) => json!({"tuple":xs.iter().map(norm).collect::<Vec<_>>()}),
        Value::Record(xs) => json!(
            xs.iter()
                .map(|(k, v)| (k.clone(), norm(v)))
                .collect::<BTreeMap<_, _>>()
        ),
        Value::Id(_) => panic!("peer schemas have no ID field"),
    }
}
fn caps(c: &Result<Vec<message::ChainPoi>, Error>) -> J {
    match c {
        Err(_) => json!({"error":true}),
        Ok(c) => json!(
            c.iter()
                .map(|p| json!({"tuple":[{"atom":"chain_poi"},[norm(&Value::Record(p.fields()))]]}))
                .collect::<Vec<_>>()
        ),
    }
}
fn normalized(result: Result<&Message, &Error>) -> J {
    let m = match result {
        Ok(m) => m,
        Err(_) => return json!({"error":true}),
    };
    let mut fields = norm(&Value::Record(m.fields().clone()));
    match m.details() {
        Details::Ping {
            peers,
            capabilities,
        } => {
            fields["peers"] = json!(
                peers
                    .iter()
                    .map(|p| norm(&Value::Record(p.fields())))
                    .collect::<Vec<_>>()
            );
            if let Some(c) = capabilities {
                fields["capabilities"] = caps(c);
            }
        }
        Details::Response { kind, message } => {
            fields = json!({"result":m.fields()["result"]==Value::Boolean(true),"type":{"atom":kind.name()}});
            if let Some(m) = message {
                fields["msg"] = normalized(m.as_ref().map(Box::as_ref));
            } else {
                fields["reason"] = norm(&m.fields()["reason"]);
            }
        }
        Details::None => {}
    }
    if m.kind() == Kind::NodeInfo {
        let obj = fields.as_object_mut().unwrap();
        let version = obj.remove("version").unwrap();
        let verified = obj.remove("verified_peers").unwrap();
        let unverified = obj.remove("unverified_peers").unwrap();
        obj.insert("node_version".into(), version);
        obj.insert(
            "peers".into(),
            json!({"verified":verified,"unverified":unverified}),
        );
    }
    json!({"type":m.kind().name(),"version":m.version(),"fields":fields})
}
fn compare_decode(row: &J) {
    let raw = bytes(&row["encoded"]);
    let decoded = Message::decode(row["tag"].as_u64().unwrap() as u16, &raw, Limits::default());
    assert_eq!(
        normalized(decoded.as_ref()),
        row["decoded"],
        "tag {} bytes {}",
        row["tag"],
        row["encoded"]
    );
}
#[test]
fn every_native_message_and_legacy_version_matches_wire_and_decoded_fields() {
    let d = data();
    let mut legacy = 0;
    for row in d["roundtrip"].as_array().unwrap() {
        let tag = row["tag"].as_u64().unwrap() as u16;
        let kind = Kind::from_tag(tag).unwrap();
        let version = row["version"].as_u64().unwrap() as u16;
        let Value::Record(fields) = value(&row["fields"]) else {
            panic!()
        };
        let m = Message::new(kind, version, fields, Limits::default()).unwrap();
        let expected = bytes(&row["encoded"]);
        assert_eq!(m.encode(Limits::default()).unwrap(), expected);
        compare_decode(row);
        let packet = m.encode_packet(Limits::default()).unwrap();
        assert_eq!(&packet[..2], tag.to_be_bytes());
        assert_eq!(
            Message::decode_packet(&packet, Limits::default()).unwrap(),
            m
        );
        if row["public_encoder"]["error"] == true {
            legacy += 1;
            assert!(version == 1 && matches!(kind, Kind::GetHeaderByHeight | Kind::GetSuccessors));
        } else {
            assert_eq!(bytes(&row["public_encoder"]["binary"]), expected);
        }
    }
    assert_eq!(legacy, 8);
    assert_eq!(Kind::ALL.len(), 21);
    for row in d["versions"].as_array().unwrap() {
        let result = json!([{"protocol":{"binary":hex::encode(b"ping")},"vsns":message::ping_versions(row["minimum"].as_u64().unwrap()).iter().map(|v|json!({"integer":v.to_string()})).collect::<Vec<_>>()}]);
        assert_eq!(result, row["supported"]);
    }
    assert!(Kind::Ping.latest(2).is_err());
    assert_eq!(Kind::Ping.latest(1).unwrap(), 2);
    assert!(Kind::from_tag(2).is_err());
    assert!(Kind::from_tag(0).is_err());
}
#[test]
fn malformed_bodies_nested_responses_and_capability_errors_match_original() {
    let d = data();
    for row in d["decode"]
        .as_array()
        .unwrap()
        .iter()
        .chain(d["responses"].as_array().unwrap())
    {
        compare_decode(row);
    }
    for (i, row) in d["capabilities"].as_array().unwrap().iter().enumerate() {
        let raw = bytes(&row["encoded"]);
        let c = message::decode_capabilities(&raw, Limits::default());
        assert_eq!(caps(&c), row["decoded"]);
        compare_decode(&row["ping"]);
        if i < 3 {
            assert_eq!(
                message::encode_capabilities(c.as_ref().unwrap(), Limits::default()).unwrap(),
                raw
            );
        }
    }
}
#[test]
fn original_fragment_boundaries_and_valid_assembly_match_exact_bytes() {
    let d = data();
    for row in d["chunks"].as_array().unwrap() {
        let n = row["bytes"].as_u64().unwrap() as usize;
        let original = (0..n).map(|n| (n % 251) as u8).collect::<Vec<_>>();
        let chunks = framing::fragment(&original, Default::default()).unwrap();
        assert_eq!(chunks.len(), row["chunks"].as_array().unwrap().len());
        for (b, expected) in chunks.iter().zip(row["chunks"].as_array().unwrap()) {
            assert_eq!(
                json!({"bytes":b.len(),"sha256":hex::encode(Sha256::digest(b)),"prefix":hex::encode(&b[..8.min(b.len())])}),
                *expected
            );
        }
        if n >= 2 {
            let mut a = framing::Assembler::new(Default::default());
            let mut result = None;
            for p in chunks {
                assert!(result.is_none());
                result = a.push(&p).unwrap();
            }
            assert_eq!(result, Some(original));
            assert!(!a.is_pending());
        }
    }
    for case in d["fragments"].as_array().unwrap() {
        if !["ordered", "reset", "empty"].contains(&case["name"].as_str().unwrap()) {
            continue;
        }
        let mut a = framing::Assembler::new(Default::default());
        for row in case["steps"].as_array().unwrap() {
            let p = fragment(row);
            let result = a.push(&p).unwrap();
            assert_eq!(json!(result.map(hex::encode)), row["delivered"]);
            assert_eq!(
                a.buffered_bytes(),
                row["stored"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|b| bytes(b).len())
                    .sum::<usize>()
            );
        }
    }
}
fn fragment(row: &J) -> Vec<u8> {
    let mut p = vec![0, 0];
    p.extend_from_slice(&(row["n"].as_u64().unwrap() as u16).to_be_bytes());
    p.extend_from_slice(&(row["m"].as_u64().unwrap() as u16).to_be_bytes());
    p.extend(bytes(&row["payload"]));
    p
}
#[test]
fn invalid_fragment_sequences_close_partial_assemblies_instead_of_inheriting_native_bugs() {
    let d = data();
    for case in d["fragments"].as_array().unwrap() {
        if ["ordered", "reset", "empty"].contains(&case["name"].as_str().unwrap()) {
            continue;
        }
        let mut a = framing::Assembler::new(Default::default());
        let mut rejected = false;
        for row in case["steps"].as_array().unwrap() {
            let r = a.push(&fragment(row));
            if r.is_err() {
                assert_eq!(r, Err(framing::Error::InvalidSequence));
                rejected = true;
                assert!(!a.is_pending());
            }
        }
        assert!(rejected, "{}", case["name"]);
    }
    let first = [0, 0, 0, 1, 0, 2, 7];
    let final_part = [0, 0, 0, 2, 0, 2, 8];
    let mut a = framing::Assembler::new(Default::default());
    a.push(&first).unwrap();
    assert_eq!(a.push(&[0, 20, 193, 1]).unwrap(), Some(vec![0, 20, 193, 1]));
    assert_eq!(a.buffered_bytes(), 1);
    assert_eq!(a.push(&final_part).unwrap(), Some(vec![7, 8]));
    a.push(&first).unwrap();
    assert_eq!(a.push(&[0]), Err(framing::Error::Truncated));
    assert!(!a.is_pending());
    a.push(&first).unwrap();
    a.reset();
    assert!(!a.is_pending());
    let mut a = framing::Assembler::new(framing::Limits {
        message_bytes: 1,
        fragments: 2,
    });
    a.push(&first).unwrap();
    assert_eq!(a.push(&final_part), Err(framing::Error::ResourceLimit));
    assert!(!a.is_pending());
    assert_eq!(
        framing::fragment(
            &vec![0; 65518],
            framing::Limits {
                fragments: 1,
                ..Default::default()
            }
        ),
        Err(framing::Error::ResourceLimit)
    );
    assert_eq!(
        framing::fragment(
            &[0; 2],
            framing::Limits {
                message_bytes: 1,
                ..Default::default()
            }
        ),
        Err(framing::Error::ResourceLimit)
    );
}
#[test]
fn shared_nested_decode_budgets_and_constructor_bounds_are_explicit_and_retryable() {
    let d = data();
    let row = d["roundtrip"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["tag"] == 1
                && r["version"] == 2
                && r["fields"]["peers"].as_array().unwrap().len() == 2
        })
        .unwrap();
    let body = bytes(&row["encoded"]);
    for limits in [
        Limits {
            body_bytes: body.len() - 1,
            ..Default::default()
        },
        Limits {
            items: 1,
            ..Default::default()
        },
        Limits {
            depth: 1,
            ..Default::default()
        },
        Limits {
            work_bytes: body.len(),
            ..Default::default()
        },
    ] {
        assert_eq!(Message::decode(1, &body, limits), Err(Error::ResourceLimit));
    }
    assert!(Message::decode(1, &body, Limits::default()).is_ok());
    let init = Message::new(Kind::PoolInit, 1, BTreeMap::new(), Limits::default()).unwrap();
    let response = Message::response_ok(&init, Limits::default()).unwrap();
    assert!(matches!(
        response.details(),
        Details::Response {
            kind: Kind::PoolInit,
            message: Some(Ok(_))
        }
    ));
    let error = Message::response_error(
        Kind::Transactions,
        b"no_active_sync".to_vec(),
        Limits::default(),
    )
    .unwrap();
    assert!(matches!(
        error.details(),
        Details::Response {
            kind: Kind::Transactions,
            message: None
        }
    ));
    let mut nested = init.encode(Limits::default()).unwrap();
    let mut tag = 20;
    for _ in 0..70 {
        use ae_codec::rlp::{self, Rlp};
        nested = rlp::encode(&Rlp::List(vec![
            Rlp::Bytes(&[1]),
            Rlp::Bytes(&[1]),
            Rlp::Bytes(&[tag]),
            Rlp::Bytes(&[]),
            Rlp::Bytes(&nested),
        ]));
        tag = 100;
    }
    assert_eq!(
        Message::decode(100, &nested, Limits::default()),
        Err(Error::ResourceLimit)
    );
    assert!(
        Message::decode(
            100,
            &nested,
            Limits {
                depth: 128,
                ..Default::default()
            }
        )
        .is_ok()
    );
    for _ in 0..100 {
        use ae_codec::rlp::{self, Rlp};
        nested = rlp::encode(&Rlp::List(vec![
            Rlp::Bytes(&[1]),
            Rlp::Bytes(&[1]),
            Rlp::Bytes(&[100]),
            Rlp::Bytes(&[]),
            Rlp::Bytes(&nested),
        ]));
    }
    assert_eq!(
        Message::decode(
            100,
            &nested,
            Limits {
                depth: usize::MAX,
                ..Default::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    let mut fields = BTreeMap::new();
    fields.insert("txs".into(), Value::List(vec![Value::Binary(vec![0; 64])]));
    assert_eq!(
        Message::new(
            Kind::Transactions,
            1,
            fields.clone(),
            Limits {
                work_bytes: 32,
                ..Default::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    let m = Message::new(Kind::Transactions, 1, fields, Limits::default()).unwrap();
    assert_eq!(
        m.encode(Limits {
            body_bytes: 1,
            ..Default::default()
        }),
        Err(Error::ResourceLimit)
    );
    assert!(m.encode(Limits::default()).is_ok());
    let peer = message::Peer {
        host: b"localhost".to_vec(),
        port: Unsigned::from_decimal("18446744073709551616").unwrap(),
        pubkey: vec![1; 32],
    };
    assert!(peer.encode(Limits::default()).is_ok());
    assert_eq!(
        peer.encode(Limits {
            items: 1,
            ..Default::default()
        }),
        Err(Error::ResourceLimit)
    );
    let poi = message::ChainPoi {
        height: 1.into(),
        root_hash: vec![1; 32],
        genesis: vec![],
        top: vec![],
        poi: vec![],
    };
    assert_eq!(
        message::encode_capabilities(
            &[poi],
            Limits {
                work_bytes: 16,
                ..Default::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
}

#[test]
fn pool_requests_use_native_response_tags_and_preserve_opaque_payloads_and_errors() {
    use ae_p2p::pool::{Reply, Request, ResponseError};
    for row in data()["roundtrip"].as_array().unwrap() {
        let tag = row["tag"].as_u64().unwrap() as u16;
        if !(20..=23).contains(&tag) {
            continue;
        }
        let m = Message::decode(tag, &bytes(&row["encoded"]), Limits::default()).unwrap();
        let r = Request::from_message(&m).unwrap();
        assert_eq!(r.clone().into_message(Limits::default()).unwrap(), m);
        let response = match &r {
            Request::Init => Reply::Init,
            Request::Unfold(v) => Reply::Unfold(v.clone()),
            Request::Get(v) => Reply::Transactions(v.clone()),
            Request::Finish(v) => Reply::Finished(*v),
        };
        let encoded = response.clone().into_response(Limits::default()).unwrap();
        assert_eq!(r.response(&encoded), Ok(response));
        assert!(
            matches!(encoded.details(),Details::Response{kind,..} if *kind==r.expected_response())
        );
        if matches!(r, Request::Get(_)) {
            assert_eq!(r.expected_response(), Kind::Transactions);
        }
        let error = Message::response_error(r.expected_response(), vec![0, 255], Limits::default())
            .unwrap();
        assert_eq!(r.response(&error), Err(ResponseError::Peer(vec![0, 255])));
    }
    let req = Request::Unfold(vec![]);
    let not_ready = Message::response_error(
        Kind::PoolUnfold,
        b"tree_not_ready".to_vec(),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(
        req.response(&not_ready),
        Err(ResponseError::Peer(b"tree_not_ready".to_vec()))
    );
    assert_eq!(
        req.response(&Reply::Init.into_response(Limits::default()).unwrap()),
        Err(ResponseError::UnexpectedType)
    );
    assert_eq!(
        Request::Get(vec![])
            .response(&Message::response_error(Kind::PoolGet, vec![], Limits::default()).unwrap()),
        Err(ResponseError::UnexpectedType)
    );
    let malformed = Message::new(
        Kind::Response,
        1,
        BTreeMap::from([
            ("result".into(), Value::Boolean(true)),
            ("type".into(), Value::Integer(21.into())),
            ("reason".into(), Value::Binary(vec![])),
            ("object".into(), Value::Binary(vec![])),
        ]),
        Limits::default(),
    )
    .unwrap();
    assert!(matches!(
        req.response(&malformed),
        Err(ResponseError::Protocol(_))
    ));
    assert_eq!(
        Request::Get(vec![vec![]; 2]).into_message(Limits {
            items: 1,
            ..Default::default()
        }),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        Request::Get(vec![vec![0; 32]]).into_message(Limits {
            work_bytes: 1,
            ..Default::default()
        }),
        Err(Error::ResourceLimit)
    );
}
