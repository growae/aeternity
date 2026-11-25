use super::*;
use serde_json::Value as Json;
fn atom(s: &str) -> Part {
    Part::atom(s)
}
fn text(s: &str) -> Part {
    Part::List(s.chars().map(u32::from).collect())
}
fn base() -> Key {
    Key {
        name: vec![atom("ae"), atom("test")],
        datapoint: atom("value"),
    }
}
fn case(id: &str) -> Option<(Options, Key, Value, Extra)> {
    let mut o = Options::default();
    let mut k = base();
    let mut v = Value::Integer(42.into());
    let mut e = Extra::List(vec![]);
    let mut rule = |head, tail, kind: &str| {
        o.type_map.push(TypeRule {
            head,
            tail,
            kind: kind.into(),
        })
    };
    match id {
        "default" => e = Extra::Undefined,
        "prefix_string" => o.prefix = text("node"),
        "prefix_binary" => o.prefix = Part::Binary(b"node".to_vec()),
        "prefix_atom" => o.prefix = atom("node"),
        "prefix_integer" => o.prefix = Part::Integer(7.into()),
        "empty_binary_prefix" => o.prefix = Part::Binary(vec![]),
        "empty_metric" => k.name = vec![],
        "mixed_parts" => {
            k.name = vec![
                atom("ae"),
                Part::Binary(b"bytes".to_vec()),
                text("text"),
                Part::Integer((-2).into()),
            ];
            k.datapoint = Part::Integer(99.into());
        }
        "separator_in_part" => k.name = vec![atom("a.b"), Part::Binary(b"c:d".to_vec())],
        "unicode_binary" => k.name = vec![Part::Binary(vec![206, 187])],
        "unicode_charlist" => k.name = vec![Part::List(vec![955])],
        "unsupported_name" => return None,
        "large_integer" => v = Value::Integer(BigInt::from(1) << 256),
        "negative_integer" => v = Value::Integer((-42).into()),
        "float_tie" => v = Value::Float(1.0625),
        "negative_zero" => v = Value::Float(-0.0),
        "nonnumeric_value" => v = Value::Other,
        "explicit_counter" => e = Extra::List(vec![OptionValue::ReportType("counter".into())]),
        "first_type_option" => {
            e = Extra::List(vec![
                OptionValue::ReportType("counter".into()),
                OptionValue::ReportType("gauge".into()),
            ])
        }
        "unknown_type" => e = Extra::List(vec![OptionValue::ReportType("unknown".into())]),
        "exact_type_map" => rule(
            vec![atom("ae"), atom("test"), atom("value")],
            false,
            "timer",
        ),
        "wildcard_part" => rule(
            vec![atom("ae"), atom("_"), atom("value")],
            false,
            "histogram",
        ),
        "wildcard_tail" => rule(vec![atom("ae")], true, "meter"),
        "wildcard_all" => rule(vec![], true, "set"),
        "ordered_type_map" => {
            rule(vec![], true, "counter");
            rule(
                vec![atom("ae"), atom("test"), atom("value")],
                false,
                "gauge",
            );
        }
        "extra_overrides_map" => {
            rule(vec![], true, "counter");
            e = Extra::List(vec![OptionValue::ReportType("timer".into())]);
        }
        "nonmatching_type_map" => rule(vec![atom("ae"), atom("value")], false, "counter"),
        "tags_atoms" => {
            e = Extra::List(vec![OptionValue::Tags(vec![
                Tag::Text(atom("host")),
                Tag::Text(atom("zone")),
            ])])
        }
        "tags_binary_string" => {
            e = Extra::List(vec![OptionValue::Tags(vec![
                Tag::Text(Part::Binary(b"host".to_vec())),
                Tag::Text(text("zone")),
            ])])
        }
        "tags_numeric_pairs" => {
            e = Extra::List(vec![OptionValue::Tags(vec![
                Tag::Pair(Value::Integer(1.into()), Value::Integer(2.into())),
                Tag::Pair(Value::Float(3.5), Value::Float(-4.5)),
            ])])
        }
        "tags_text_pairs" => {
            e = Extra::List(vec![OptionValue::Tags(vec![Tag::Pair(
                Value::Other,
                Value::Other,
            )])])
        }
        "empty_tags" => e = Extra::List(vec![OptionValue::Tags(vec![])]),
        "first_tags_option" => {
            e = Extra::List(vec![
                OptionValue::Tags(vec![]),
                OptionValue::Tags(vec![Tag::Text(atom("host"))]),
            ])
        }
        "invalid_tag" => {
            e = Extra::List(vec![OptionValue::Tags(vec![Tag::Text(Part::Integer(
                12.into(),
            ))])])
        }
        "invalid_tags_container" => e = Extra::List(vec![OptionValue::NonListTags]),
        "invalid_extra" => e = Extra::Other,
        other => panic!("unclassified original case {other}"),
    }
    Some((o, k, v, e))
}
#[test]
fn original_names_report_types_values_and_tag_iolists_keep_exact_bytes_and_failure_stage() {
    let native: Json = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_metrics_format.json"
    ))
    .unwrap();
    let mut compared = 0;
    let mut typed_boundary = 0;
    for row in native["cases"].as_array().unwrap() {
        let id = row["id"].as_str().unwrap();
        let result = &row["result"];
        let Some((o, k, v, e)) = case(id) else {
            assert_eq!(id, "unsupported_name");
            assert_eq!(
                result["report_error"],
                serde_json::json!(["error", "function_clause"])
            );
            typed_boundary += 1;
            continue;
        };
        let event = o.event(k, v, e, Limits::default()).unwrap();
        assert_eq!(event.kind, result["type"]);
        let line = event.line(Limits::default());
        if let Some(hex) = result["line_hex"].as_str() {
            assert_eq!(line.unwrap(), hex::decode(hex).unwrap(), "{id}");
        } else if result.get("byte_adapter_error").is_some() {
            assert_eq!(line, Err(Error::InvalidIolist));
        } else {
            assert_eq!(
                result["format_error"],
                serde_json::json!(["error", "function_clause"])
            );
            let expected = match id {
                "unknown_type" => Error::UnsupportedType,
                "invalid_tag" => Error::InvalidTag,
                "invalid_extra" => Error::InvalidExtra,
                _ => panic!("unexpected original error {id}"),
            };
            assert_eq!(line, Err(expected));
        }
        compared += 1;
    }
    assert_eq!((compared, typed_boundary), (35, 1));
}
#[test]
fn event_limits_reject_before_output_and_keep_native_scalar_kinds_distinct() {
    let l = Limits::default();
    let o = Options::default();
    let e = o
        .event(base(), Value::Float(-0.0), Extra::Undefined, l)
        .unwrap();
    let bytes = e.retained_bytes(l).unwrap();
    assert_eq!(
        e.retained_bytes(Limits {
            bytes: bytes - 1,
            ..l
        }),
        Err(Error::Limit)
    );
    assert_eq!(
        o.event(base(), Value::Float(f64::NAN), Extra::Undefined, l),
        Err(Error::InvalidNumber)
    );
    assert_eq!(
        o.event(
            base(),
            Value::Integer(BigInt::from(1) << 4096),
            Extra::Undefined,
            l
        ),
        Err(Error::Limit)
    );
    assert_eq!(
        o.event(
            base(),
            Value::Other,
            Extra::List(vec![OptionValue::Other; 65]),
            l
        ),
        Err(Error::Limit)
    );
    let rule = Options {
        prefix: Part::List(vec![]),
        type_map: vec![TypeRule {
            head: vec![atom("ae"), Part::Binary(b"test".to_vec()), atom("value")],
            tail: false,
            kind: "counter".into(),
        }],
    };
    assert_eq!(
        rule.event(base(), Value::Integer(1.into()), Extra::Undefined, l)
            .unwrap()
            .kind,
        "gauge"
    );
    let mut key = base();
    key.name[1] = Part::Binary(b"test".to_vec());
    assert_eq!(
        rule.event(key, Value::Integer(1.into()), Extra::Undefined, l)
            .unwrap()
            .kind,
        "counter"
    );
}
