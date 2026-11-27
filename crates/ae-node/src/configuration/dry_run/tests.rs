use super::*;
use crate::configuration::{
    Configuration,
    resolution::{Context, Directories},
};
use std::sync::LazyLock;

static DATA: LazyLock<Value> = LazyLock::new(|| {
    fn unpack(v: &Value, pool: &[Value]) -> Value {
        match v {
            Value::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                unpack(&pool[m["$ref"].as_u64().unwrap() as usize], pool)
            }
            Value::Object(m) => Value::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), unpack(v, pool)))
                    .collect(),
            ),
            Value::Array(a) => Value::Array(a.iter().map(|v| unpack(v, pool)).collect()),
            v => v.clone(),
        }
    }
    let v: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_dry_run_worker.json"
    ))
    .unwrap();
    unpack(&v["root"], v["values"].as_array().unwrap())
});
#[test]
fn native_phase_heap_policy_keeps_disabled_values_separate_from_service_capacity() {
    for value in [
        json!(0),
        json!(-1),
        json!(false),
        Value::Null,
        json!(1.0),
        json!("4096"),
    ] {
        assert_eq!(native_heap_bytes(&value, 8), Ok(None));
    }
    assert_eq!(native_heap_bytes(&json!(4096), 8), Ok(Some(32768)));
    assert_eq!(native_heap_bytes(&json!(4096), 4), Ok(Some(16384)));
    assert_eq!(
        native_heap_bytes(&json!(0), 16),
        Err(Error::Field("dry_run_heap_budget"))
    );
    let huge = serde_json::from_str(&(BigInt::from(1) << 100_usize).to_string()).unwrap();
    assert_eq!(
        native_heap_bytes(&huge, 8),
        Err(Error::Field("max_heap_words"))
    );
}
fn json_term(v: &Value) -> Value {
    if let Some(b) = v.get("binary") {
        return json!(String::from_utf8(hex::decode(b.as_str().unwrap()).unwrap()).unwrap());
    }
    if let Some(a) = v.get("atom") {
        return match a.as_str().unwrap() {
            "true" => json!(true),
            "false" => json!(false),
            "null" => Value::Null,
            s => json!(s),
        };
    }
    assert!(v.is_number());
    v.clone()
}
// Internal Rust callers have one typed profile/override, not an Erlang
// proplist. Project the original first-property semantics into that API.
fn first(opts: &Value, key: &str) -> Value {
    for v in opts.as_array().unwrap() {
        if v.get("atom") == Some(&json!(key)) {
            return json!({"atom":"true"});
        }
        if let Some(pair) = v.get("tuple").and_then(Value::as_array)
            && pair.len() == 2
            && pair[0]["atom"] == key
        {
            return pair[1].clone();
        }
    }
    json!({"atom":"undefined"})
}
#[test]
fn native_heap_words_select_process_cap_without_disabling_service_backstop() {
    use ae_worker_heap::{Limit, Source};
    assert_eq!(
        heap_limit(&json!(4096), 8, 65536).unwrap(),
        Limit {
            bytes: 32768,
            source: Source::Native
        }
    );
    assert_eq!(
        heap_limit(&json!(4096), 4, 65536).unwrap(),
        Limit {
            bytes: 16384,
            source: Source::Native
        }
    );
    assert_eq!(
        heap_limit(&json!(4096), 8, 32768).unwrap(),
        Limit {
            bytes: 32768,
            source: Source::Native
        }
    );
    let service = Limit {
        bytes: 65536,
        source: Source::Service,
    };
    for raw in [
        json!(0),
        json!(-1),
        json!(false),
        Value::Null,
        json!(1.0),
        json!("4096"),
        json!(256000000),
    ] {
        assert_eq!(heap_limit(&raw, 8, 65536).unwrap(), service, "{raw}");
    }
    let huge: Value = serde_json::from_str(&(BigInt::from(1) << 100_usize).to_string()).unwrap();
    assert!(matches!(
        heap_limit(&huge, 8, 65536),
        Err(Error::Field("max_heap_words"))
    ));
    for (word, cap) in [(0, 65536), (16, 65536), (8, 0), (8, usize::MAX)] {
        assert!(matches!(
            heap_limit(&json!(0), word, cap),
            Err(Error::Field("dry_run_heap_budget"))
        ));
    }
}
#[test]
fn original_timeout_profile_and_worker_setting_accessors() {
    let rows = DATA["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1680);
    let mut errors = 0;
    for (i, row) in rows.iter().enumerate() {
        let mut context = Context::new(Directories {
            home: "/release".into(),
            data: "data".into(),
            log: "log".into(),
        })
        .unwrap();
        for (app, settings) in row["config"]["applications"].as_object().unwrap() {
            for (key, value) in settings.as_object().unwrap() {
                context
                    .set(app.clone(), key.clone(), value.clone())
                    .unwrap();
            }
        }
        let configuration = Configuration {
            value: row["config"]["user"].clone(),
        };
        let resolved = configuration.resolve(&context).unwrap();
        let profile = match first(&row["options"], "dry_run_profile")["atom"]
            .as_str()
            .unwrap()
        {
            "public" => Profile::Public,
            "internal" => Profile::Internal,
            "replay" => Profile::Replay,
            "includability" => Profile::Includability,
            "undefined" => Profile::Unspecified,
            _ => Profile::Other,
        };
        assert_eq!(
            profile
                .metering(ae_protocol::Protocol::Ceres, true)
                .1
                .is_some(),
            row["force"]["ok"]["atom"] == "true"
        );
        let explicit = first(&row["options"], "timeout_ms");
        let explicit_value = (explicit["atom"] != "undefined").then(|| json_term(&explicit));
        let actual = timeout(&resolved, profile, explicit_value.as_ref());
        if row["resolved"].get("ok").is_some() {
            let expected = &row["resolved"]["ok"]["tuple"][0];
            let expected = if expected["atom"] == "infinity" {
                Timeout::Unbounded
            } else {
                Timeout::Milliseconds(json_term(expected))
            };
            assert_eq!(actual, Ok(expected), "row {i}");
        } else {
            errors += 1;
            assert_eq!(actual, Err(Error::Field("timeout_ms")), "row {i}");
            assert_eq!(row["resolved"]["class"], "error");
            assert_eq!(
                row["resolved"]["reason"]["tuple"],
                json!([{"atom":"case_clause"},explicit])
            );
        }
        assert_eq!(
            max_heap_words(&resolved).unwrap(),
            json_term(&row["heap"]["ok"]),
            "heap row {i}"
        );
        assert_eq!(
            store_read_gas_metering(&resolved).unwrap(),
            json_term(&row["metering"]["ok"]),
            "metering row {i}"
        );
    }
    assert_eq!(errors, 350);
}
#[test]
fn explicit_zero_is_a_deadline_and_only_positive_integer_heap_guards_apply() {
    let context = Context::new(Directories {
        home: "/release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    let configuration = Configuration {
        value: json!({"http":{"dry_run":{"timeout_ms":0,"replay_timeout_ms":0}}}),
    };
    let resolved = configuration.resolve(&context).unwrap();
    for profile in [Profile::Public, Profile::Replay] {
        assert_eq!(
            timeout(&resolved, profile, None)
                .unwrap()
                .runtime()
                .unwrap(),
            ae_sync::dry_run::Timeout::Unbounded
        );
        assert_eq!(
            timeout(&resolved, profile, Some(&json!(0)))
                .unwrap()
                .runtime()
                .unwrap(),
            ae_sync::dry_run::Timeout::After(Duration::ZERO)
        );
    }
    for value in [
        json!(-1),
        json!(1.0),
        json!("infinity"),
        Value::Null,
        json!(false),
        serde_json::from_str("18446744073709551616").unwrap(),
    ] {
        assert_eq!(
            Timeout::Milliseconds(value).runtime(),
            Err(Error::Field("timeout_ms"))
        );
    }
    for row in DATA["workers"].as_array().unwrap() {
        let configured = heap_guard_words(&json_term(&row["heap"])).unwrap_or_default();
        let size = row["heap_info"]["map"]
            .as_array()
            .unwrap()
            .iter()
            .find(|pair| pair[0]["atom"] == "size")
            .unwrap()[1]
            .to_string()
            .parse::<BigInt>()
            .unwrap();
        assert_eq!(configured, size);
    }
}
