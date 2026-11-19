use ae_vm_fate::{
    Error, Limits,
    encoding::{self, DecodeException, DecodingFailure},
};
use serde_json::{Value, json};

fn outcome<T>(result: Result<T, DecodingFailure>) -> Value {
    match result {
        Ok(_) => Value::Null,
        Err(e) => {
            assert_ne!(e.error, Error::ResourceLimit);
            match e.exception.expect("native exception") {
                DecodeException::Error(reason) => json!(["error", reason]),
                DecodeException::Exit(reason) => json!(["exit", reason]),
            }
        }
    }
}

#[test]
fn native_decoder_failure_classes_and_precedence_match_original() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_fate_decode_failures.json")).unwrap();
    assert_eq!(fixture["format"], "grouped-fate-decode-v1");
    let limits = Limits {
        max_depth: 512,
        ..Limits::default()
    };
    let mut counts = [0, 0];
    for row in fixture["decode"].as_array().unwrap() {
        for input in row["inputs"].as_array().unwrap() {
            let bytes = hex::decode(input.as_str().unwrap()).unwrap();
            assert_eq!(
                outcome(encoding::decode_for_vm(&bytes, limits)),
                row["whole"],
                "whole {input}"
            );
            assert_eq!(
                outcome(encoding::decode_one_for_vm(&bytes, limits)),
                row["prefix"],
                "prefix {input}"
            );
            counts[0] += 1;
        }
    }
    for row in fixture["type_decode"].as_array().unwrap() {
        for input in row["inputs"].as_array().unwrap() {
            let bytes = hex::decode(input.as_str().unwrap()).unwrap();
            assert_eq!(
                outcome(encoding::decode_type_one_for_vm(&bytes, limits)),
                row["type"],
                "type {input}"
            );
            counts[1] += 1;
        }
    }
    assert!(counts[0] > 65_536 && counts[1] > 65_536);
}

#[test]
fn decoder_resource_exhaustion_has_no_native_exception_and_can_retry() {
    let bytes = encoding::encode(
        &ae_vm_fate::value::Value::List(vec![ae_vm_fate::value::Value::String(vec![42; 128]); 12]),
        Limits::default(),
    )
    .unwrap();
    let expected = encoding::decode_for_vm(&bytes, Limits::default()).unwrap();
    for limits in [
        Limits {
            max_bytes: 100,
            ..Limits::default()
        },
        Limits {
            max_nodes: 2,
            ..Limits::default()
        },
        Limits {
            max_depth: 0,
            ..Limits::default()
        },
    ] {
        let e = encoding::decode_for_vm(&bytes, limits).unwrap_err();
        assert_eq!(
            e,
            DecodingFailure {
                error: Error::ResourceLimit,
                exception: None
            }
        );
        assert_eq!(
            encoding::decode_for_vm(&bytes, Limits::default()).unwrap(),
            expected
        );
    }
}
