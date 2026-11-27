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
