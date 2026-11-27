use super::*;

pub(crate) fn corpus() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_surface.json"
    ))
    .unwrap()
}

#[test]
fn original_cors_and_height_headers_match_preflight_and_normal_requests() {
    let corpus = corpus();
    let rows = corpus["cors"].as_array().unwrap();
    assert_eq!(rows.len(), 192);
    for row in rows {
        let mut config = Config::default();
        let native = &row["config"];
        if let Some(domains) = native["allow_domains"].as_array() {
            config.cors.domains = domains
                .iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect();
        }
        if let Some(methods) = native["allow_methods"].as_array() {
            config.cors.methods = methods
                .iter()
                .map(|v| v.as_str().unwrap())
                .collect::<Vec<_>>()
                .join(", ");
        }
        config.cors.headers = native["allow_headers"].as_str().map(str::to_owned);
        if let Some(age) = native["max_age"].as_u64() {
            config.cors.max_age = age.into();
        }
        assert!(config.cors.valid());
        let headers = row["request_headers"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
            .collect();
        let mut response = Response::empty(200);
        response.stamp(&config, &headers, row["height"].as_u64());
        assert_eq!(
            serde_json::to_value(response.headers).unwrap(),
            row["headers"],
            "{row}"
        );
        assert_eq!(
            row["action"],
            if row["method"] == "OPTIONS" {
                "stop"
            } else {
                "continue"
            }
        );
    }
}

#[test]
fn original_recursive_integer_conversion_preserves_floats_booleans_and_null() {
    for row in corpus()["converted"].as_array().unwrap() {
        let mut value = row["input"].clone();
        integers_as_strings(&mut value);
        assert_eq!(value, row["output"]);
    }
}
