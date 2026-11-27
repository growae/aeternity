use super::Startup;
use crate::configuration::{
    Configuration,
    http::startup,
    resolution::{Context, Directories},
};
use crate::http::{ConfiguredLimits as Limits, Target};
use serde_json::{Value, json};
use std::time::Duration;
use std::{collections::BTreeMap, net::IpAddr};

#[test]
fn native_http_startup_getters_groups_addresses_and_cors_match() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_http_startup.json"
    ))
    .unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 130);
    for row in rows {
        let label = row["name"].as_str().unwrap();
        // The native harness caches its input directly before calling the
        // getters. Schema/file validation is a separate boundary.
        let configuration = Configuration {
            value: row["user"].clone(),
        };
        let mut context = Context::new(Directories {
            home: "/release".into(),
            data: "data".into(),
            log: "log".into(),
        })
        .unwrap();
        for (key, value) in row["application"].as_object().unwrap() {
            context
                .set("aehttp".into(), key.clone(), value.clone())
                .unwrap();
        }
        let resolved = configuration.resolve(&context).unwrap();
        let expected = &row["output"];
        let groups = startup::groups(&resolved);
        assert_eq!(
            groups.is_ok(),
            expected["groups"]["kind"] == "ok",
            "{label}"
        );
        if let Ok(groups) = groups {
            assert_eq!(json!(groups), expected["groups"]["value"], "{label}");
        }
        let actual = Startup::resolve(&resolved);
        assert_eq!(
            actual.is_ok(),
            expected["startup"]["kind"] == "ok",
            "{label}: {actual:?}"
        );
        if let Ok(actual) = actual {
            let values: Vec<_> = actual.endpoints.iter().map(|e| {
                let address = match e.address { IpAddr::V4(a) => json!(a.octets()), IpAddr::V6(a) => json!(a.segments()) };
                let target = match e.target { Target::External => "external", Target::Internal => "internal", Target::Rosetta => "rosetta", Target::RosettaOffline => "rosetta_offline" };
                json!({"target":target,"address":address,"port":e.port,"acceptors":e.acceptors,"idle_timeout":actual.idle_timeout_ms,"max_skip_body_length":actual.max_skip_body_length})
            }).collect();
            assert_eq!(json!(values), expected["startup"]["value"], "{label}");
            let admissible = actual
                .max_skip_body_length
                .as_number()
                .is_some_and(|n| n.to_string().parse::<num_bigint::BigInt>().is_ok())
                && actual.endpoints.iter().all(|e| {
                    e.port.as_u64().is_some_and(|p| p <= 65535)
                        && e.acceptors
                            .as_u64()
                            .is_some_and(|a| (1..=4096).contains(&a))
                });
            assert_eq!(
                actual.configs(&Limits::default()).is_ok(),
                admissible,
                "{label}"
            );
        }
        // CORS resolves even when a different startup getter failed.
        let cors = startup::cors(&resolved).unwrap();
        assert!(cors.valid(), "{label}");
        for (key, origin) in [
            ("cors_allowed", "https://allowed.example"),
            ("cors_other", "https://other.example"),
        ] {
            let request = BTreeMap::from([
                ("origin".into(), origin.into()),
                (
                    "access-control-request-headers".into(),
                    "x-requested".into(),
                ),
            ]);
            let mut headers = BTreeMap::new();
            cors.apply(&request, &mut headers);
            assert_eq!(json!(headers), expected[key]["value"], "{label} {key}");
        }
    }
}

#[test]
fn configured_http_service_budgets_do_not_reinterpret_native_protocol_settings() {
    let configuration = Configuration::from_value(json!({"http":{"protocol_options":{"max_skip_body_length":23},"external":{"request_timeout":17,"connect_timeout":19}}})).unwrap();
    let context = Context::new(Directories {
        home: "/release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    let startup = Startup::resolve(&configuration.resolve(&context).unwrap()).unwrap();
    let limits = Limits {
        timeout: Duration::from_secs(7),
        ..Default::default()
    };
    let configs = startup.configs(&limits).unwrap();
    assert_eq!(startup.idle_timeout_ms, 480_000);
    assert_eq!(startup.max_skip_body_length, json!(23));
    for config in configs {
        assert_eq!(config.timeout, limits.timeout);
        assert_eq!(config.body_bytes, limits.body_bytes);
        assert_eq!(config.protocol.request_timeout, Duration::from_secs(5));
        assert_eq!(
            config.protocol.idle_timeout,
            Duration::from_millis(startup.idle_timeout_ms)
        );
        assert_eq!(config.protocol.max_skip_body_length, 23.into());
        assert_eq!(config.protocol.max_keepalive, 1000);
    }
    assert!(
        startup
            .configs(&Limits {
                connections: 0,
                ..limits
            })
            .is_err()
    );
    let mut large = startup;
    large.cors.max_age = num_bigint::BigInt::from(1) << 4097;
    assert!(large.configs(&Limits::default()).is_err());
}
