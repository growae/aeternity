use ae_node::configuration::{
    Configuration,
    metrics::{Error, Metrics},
    resolution::{Context, Directories},
};
use ae_sync::{
    observe::{self, statsd},
    routing,
};
use serde_json::{Value, json};
use std::time::Duration;
fn context(app: &Value) -> Context {
    let mut context = Context::new(Directories {
        home: "release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    for (k, v) in app.as_object().unwrap() {
        context.set("aecore".into(), k.clone(), v.clone()).unwrap();
    }
    context
}
fn rule(value: &Value) -> routing::Rule {
    let bytes = |key, default: &str| {
        value
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or(default)
            .as_bytes()
            .to_vec()
    };
    routing::Rule {
        name: bytes("name", "*"),
        kind: bytes("type", "*"),
        datapoints: bytes("datapoints", "default"),
        actions: bytes("actions", "log,send"),
    }
}
#[test]
fn original_getters_and_raw_rule_defaults_match_with_separate_udp_consumer_bounds() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_metrics_config.json")).unwrap();
    let schema: Value =
        serde_json::from_str(include_str!("fixtures/erlang_metrics_schema.json")).unwrap();
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 18);
    assert_eq!(schema["cases"].as_array().unwrap().len(), 18);
    for row in schema["cases"].as_array().unwrap() {
        assert_eq!(
            row["schema_result"],
            json!({"status":"ok","unchanged":true})
        );
        Configuration::from_value(row["user"].clone()).unwrap();
    }
    for row in fixture["cases"].as_array().unwrap() {
        let cfg = Configuration::from_value(row["user"].clone()).unwrap();
        let ctx = context(&row["application"]);
        let m = Metrics::resolve(&cfg.resolve(&ctx).unwrap()).unwrap();
        let result = &row["result"];
        assert_eq!(
            json!(match &m.host {
                statsd::Host::System => "$SYSTEM_HOSTNAME",
                statsd::Host::Name(h) => h,
            }),
            result["host"]
        );
        assert_eq!(json!(m.port), result["port"]);
        assert_eq!(json!(m.reconnect_ms), result["reconnect_interval"]);
        assert_eq!(
            m.rules,
            result["rules"]
                .as_array()
                .unwrap()
                .iter()
                .map(rule)
                .collect::<Vec<_>>()
        );
        let mut routing = None;
        let mut observer = observe::Config::default();
        if m.port > u16::MAX as u64 {
            assert!(matches!(
                m.apply(&mut routing, &mut observer),
                Err(Error::Range("port"))
            ));
            assert!(routing.is_none());
        } else {
            let output = m.apply(&mut routing, &mut observer).unwrap();
            assert_eq!(u64::from(output.target.port), m.port);
            assert_eq!(
                output.target.reconnect_interval,
                Duration::from_millis(m.reconnect_ms)
            );
            assert_eq!(routing.unwrap().config.rules, m.rules);
            assert!(observer.statsd.is_none());
        }
    }
}
#[test]
fn metrics_resolution_preserves_caller_resources_and_applies_atomically() {
    let defaults = Configuration::from_value(json!({})).unwrap();
    for host in [Value::Null, json!(false), json!([])] {
        let invalid = context(&json!({"metrics_host": host}));
        assert!(matches!(
            Metrics::resolve(&defaults.resolve(&invalid).unwrap()),
            Err(Error::Field("host"))
        ));
    }
    let ctx = context(&json!({}));
    let cfg = Configuration::from_value(
        json!({"metrics":{"host":"127.0.0.1","port":0,"reconnect_interval":7,"rules":[{}]}}),
    )
    .unwrap();
    let metrics = Metrics::resolve(&cfg.resolve(&ctx).unwrap()).unwrap();
    let mut setup = routing::Setup::default();
    setup.config.limits.requests = 13;
    let mut routing = Some(setup);
    let mut observer = observe::Config {
        interval: Duration::from_secs(20),
        sink_records: 3,
        sink_bytes: 8192,
        record_bytes: 1024,
        ..Default::default()
    };
    let output = metrics.apply(&mut routing, &mut observer).unwrap();
    assert_eq!(output.records, 3);
    assert_eq!(output.bytes, 8192);
    assert_eq!(routing.as_ref().unwrap().config.limits.requests, 13);
    assert_eq!(observer.interval, Duration::from_secs(10));
    assert_eq!(output.target.reconnect_interval, Duration::from_millis(7));
    let mut invalid = metrics.clone();
    invalid.rules = vec![Default::default(); 2];
    routing.as_mut().unwrap().config.limits.rules = 1;
    observer.interval = Duration::from_secs(20);
    assert!(matches!(
        invalid.apply(&mut routing, &mut observer),
        Err(Error::Routing(routing::Error::Limit))
    ));
    assert_eq!(routing.unwrap().config.rules, metrics.rules);
    assert_eq!(observer.interval, Duration::from_secs(20));
}
