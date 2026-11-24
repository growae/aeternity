use ae_stratum::wire::json::{self, Error, Frames, Limits};
use serde_json::{Value, json};

fn term(v: &Value) -> Value {
    match v {
        Value::Null => json!({"atom":"null"}),
        Value::Bool(true) => json!({"atom":"true"}),
        Value::Bool(false) => json!({"atom":"false"}),
        Value::String(s) => json!({"binary":hex::encode(s.as_bytes())}),
        Value::Number(n) => {
            let text = n.to_string();
            if text.contains(['.', 'e', 'E']) {
                json!({"float":hex::encode(n.as_f64().unwrap().to_be_bytes())})
            } else {
                json!({"integer":text})
            }
        }
        Value::Array(a) => json!({"list":a.iter().map(term).collect::<Vec<_>>()}),
        Value::Object(m) => {
            let mut entries = m.iter().collect::<Vec<_>>();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            json!({"map":entries.into_iter().map(|(k,v)|json!([{"binary":hex::encode(k)},term(v)])).collect::<Vec<_>>()})
        }
    }
}

#[test]
fn original_jsx_values_errors_and_encoder_interoperability() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_stratum_json.json")).unwrap();
    let rows = fixture.as_array().unwrap();
    assert_eq!(rows.len(), 1608);
    let mut accepted = 0;
    for (index, row) in rows.iter().enumerate() {
        let bytes = hex::decode(row["hex"].as_str().unwrap()).unwrap();
        let native = &row["result"];
        let result = json::decode(&bytes, Limits::default());
        if native["status"] == "ok" {
            accepted += 1;
            let value =
                result.unwrap_or_else(|e| panic!("case {index} {} {bytes:?}: {e:?}", row["group"]));
            assert_eq!(term(&value), native["value"], "case {index} {bytes:?}");
            let encoded = json::encode(&value, Limits::default()).unwrap();
            assert_eq!(encoded.last(), Some(&b'\n'));
            assert_eq!(encoded.iter().filter(|&&b| b == b'\n').count(), 1);
            // Native JSX output is accepted with identical types, binary values
            // and float bits, as is Rust output through a separate strict parser.
            let native_bytes = hex::decode(native["encoded"].as_str().unwrap()).unwrap();
            assert_eq!(
                term(&json::decode(&native_bytes, Limits::default()).unwrap()),
                native["roundtrip"],
                "native encode {index}"
            );
            assert_eq!(
                term(&json::decode(&encoded, Limits::default()).unwrap()),
                native["value"],
                "Rust roundtrip {index}"
            );
            let strict: Value = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(term(&strict), native["value"], "strict parser {index}");
        } else {
            assert_eq!(
                result,
                Err(Error::Parse),
                "case {index} {bytes:?}; native {native}"
            );
        }
    }
    assert_eq!(accepted, 1127);
}

#[test]
fn line_framer_handles_fragmentation_errors_and_backpressure() {
    let packet = b"{'text':'\xf0\x9d\x84\x9e','id':18446744073709551616}\r\n";
    for split in 0..packet.len() {
        let mut frames = Frames::new(Limits::default()).unwrap();
        assert_eq!(frames.next(&mut &packet[..split]).unwrap(), None);
        assert_eq!(frames.pending_bytes(), split);
        let value = frames.next(&mut &packet[split..]).unwrap().unwrap();
        assert_eq!(value["text"], "𝄞");
        assert_eq!(value["id"].to_string(), "18446744073709551616");
        assert_eq!(frames.pending_bytes(), 0);
        frames.finish().unwrap();
        assert_eq!(frames.next(&mut &b"{}\n"[..]), Err(Error::Closed));
    }
    let mut frames = Frames::new(Limits::default()).unwrap();
    let mut input = &b"oops\ntrue\nfalse\n"[..];
    assert_eq!(frames.next(&mut input), Err(Error::Parse));
    assert_eq!(input, b"true\nfalse\n");
    assert_eq!(frames.next(&mut input).unwrap(), Some(json!(true)));
    assert_eq!(input, b"false\n");
    assert_eq!(frames.next(&mut input).unwrap(), Some(json!(false)));
    assert!(input.is_empty());
    frames.finish().unwrap();
    let mut frames = Frames::new(Limits::default()).unwrap();
    assert_eq!(frames.next(&mut &b"true"[..]).unwrap(), None);
    assert_eq!(frames.finish(), Err(Error::IncompleteFrame));
    assert_eq!(frames.pending_bytes(), 0);
    assert_eq!(frames.finish(), Err(Error::Closed));
}

#[test]
fn resource_limits_bound_decoding_encoding_and_pending_frames() {
    let base = Limits::default();
    for limits in [
        Limits {
            bytes: 1_048_577,
            ..base
        },
        Limits { depth: 65, ..base },
        Limits {
            values: 65537,
            ..base
        },
        Limits {
            string_bytes: 1_048_577,
            ..base
        },
        Limits {
            number_bytes: 4097,
            ..base
        },
    ] {
        assert_eq!(json::decode(b"null", limits), Err(Error::Limits));
        assert_eq!(json::encode(&Value::Null, limits), Err(Error::Limits));
        assert!(matches!(Frames::new(limits), Err(Error::Limits)));
    }
    let examples: &[(&[u8], Limits, Error)] = &[
        (b"null", Limits { bytes: 3, ..base }, Error::Limit("bytes")),
        (b"[[]]", Limits { depth: 1, ..base }, Error::Limit("depth")),
        (
            b"[0,1]",
            Limits { values: 2, ..base },
            Error::Limit("values"),
        ),
        (
            b"{'a':0,'a':1}",
            Limits { values: 2, ..base },
            Error::Limit("values"),
        ),
        (
            b"'abc'",
            Limits {
                string_bytes: 2,
                ..base
            },
            Error::Limit("string_bytes"),
        ),
        (
            b"'\xff'",
            Limits {
                string_bytes: 2,
                ..base
            },
            Error::Limit("string_bytes"),
        ),
        (
            b"100",
            Limits {
                number_bytes: 2,
                ..base
            },
            Error::Limit("number_bytes"),
        ),
        (
            b"/* /* */ */0",
            Limits { depth: 1, ..base },
            Error::Limit("depth"),
        ),
    ];
    for (input, limits, error) in examples {
        assert_eq!(json::decode(input, *limits), Err(error.clone()));
    }
    assert_eq!(
        json::decode(
            b"[0]",
            Limits {
                values: 2,
                depth: 1,
                ..base
            }
        )
        .unwrap(),
        json!([0])
    );
    assert_eq!(
        json::encode(&json!([[]]), Limits { depth: 1, ..base }),
        Err(Error::Limit("depth"))
    );
    assert_eq!(
        json::encode(&json!([0, 1]), Limits { values: 2, ..base }),
        Err(Error::Limit("values"))
    );
    assert_eq!(
        json::encode(
            &json!({"abc":0}),
            Limits {
                string_bytes: 2,
                ..base
            }
        ),
        Err(Error::Limit("string_bytes"))
    );
    assert_eq!(
        json::encode(
            &json!(100),
            Limits {
                number_bytes: 2,
                ..base
            }
        ),
        Err(Error::Limit("number_bytes"))
    );
    assert_eq!(
        json::encode(&Value::Null, Limits { bytes: 4, ..base }),
        Err(Error::Limit("bytes"))
    );
    assert_eq!(
        json::encode(&Value::Null, Limits { bytes: 5, ..base }).unwrap(),
        b"null\n"
    );
    let huge_float: Value = serde_json::from_str("1e999").unwrap();
    assert_eq!(json::encode(&huge_float, base), Err(Error::Parse));
    let mut frames = Frames::new(Limits { bytes: 5, ..base }).unwrap();
    let mut input = &b"null\ntrue\n"[..];
    assert_eq!(frames.next(&mut input).unwrap(), Some(Value::Null));
    assert_eq!(input, b"true\n");
    assert_eq!(frames.next(&mut input).unwrap(), Some(json!(true)));
    let mut frames = Frames::new(Limits { bytes: 5, ..base }).unwrap();
    assert_eq!(frames.next(&mut &b"false"[..]).unwrap(), None);
    let mut last = &b"\n"[..];
    assert_eq!(frames.next(&mut last), Err(Error::Limit("bytes")));
    assert_eq!(last, b"\n");
    assert_eq!(frames.pending_bytes(), 0);
    assert_eq!(frames.next(&mut last), Err(Error::Closed));
}
