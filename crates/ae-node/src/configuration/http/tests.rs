use super::*;
use crate::configuration::{
    Configuration,
    resolution::{Context, Directories},
    runtime::Core,
};
use std::path::Path;

#[test]
fn original_gas_reporting_overrides_and_scalar_floor_use_nested_precedence() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_gas_reporting.json"
    ))
    .unwrap();
    let rows = fixture.as_array().unwrap();
    assert_eq!(rows.len(), 44);
    let integer = |v: &Value| v.as_number().unwrap().as_str().parse::<BigInt>().unwrap();
    for r in rows {
        let mut context = Context::new(Directories {
            home: "/release".into(),
            data: "data".into(),
            log: "log".into(),
        })
        .unwrap();
        let app: Vec<Value> = r["application"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| json!([k, v]))
            .collect();
        context
            .set("aehttp".into(), "gas_price".into(), Value::Array(app))
            .unwrap();
        let config = Configuration {
            value: json!({"http":{"gas_price":r["user"]}}),
        };
        let resolved = config.resolve(&context).unwrap();
        let reporting = GasReporting::resolve(&resolved).unwrap();
        let expected = &r["result"]["value"];
        assert_eq!(
            reporting_floor(&resolved)
                .unwrap()
                .map(|v| BigInt::from_bytes_be(num_bigint::Sign::Plus, v.as_bytes())),
            (!expected["floor"].is_null()).then(|| integer(&expected["floor"])),
            "{r}"
        );
        assert_eq!(
            reporting.price,
            (!expected["floor"].is_null()).then(|| integer(&expected["floor"])),
            "{r}"
        );
        assert_eq!(
            Some(u64::from(reporting.utilization)),
            if reporting.price.is_some() {
                expected["utilization"].as_u64()
            } else {
                Some(0)
            },
            "{r}"
        );
        for sample in expected["samples"].as_array().unwrap() {
            let mut row = ae_chain::gas_prices::Summary {
                minutes: 1,
                minimum: integer(&sample["observed"]),
                utilization: integer(&sample["used"]),
            };
            assert_eq!(
                reporting.apply_price(row.minimum.clone()),
                integer(&sample["scalar_price"])
            );
            reporting.apply(&mut row);
            assert_eq!(row.minimum, integer(&sample["pair_price"]));
            assert_eq!(row.utilization, integer(&sample["pair_utilization"]));
        }
    }
}

#[test]
fn scalar_reporting_floor_skips_malformed_utilization_even_when_enabled() {
    let mut context = Context::new(Directories {
        home: "/release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    context
        .set(
            "aehttp".into(),
            "gas_price".into(),
            json!([
                ["min_relay_gas_price", 2_000_000],
                ["reporting_utilization_override"]
            ]),
        )
        .unwrap();
    let configuration = Configuration::empty();
    let resolved = configuration.resolve(&context).unwrap();
    assert_eq!(
        reporting_floor(&resolved).unwrap(),
        Some(2_000_000u64.into())
    );
    assert_eq!(GasReporting::resolve(&resolved), Err(Error::Lookup));
}

#[test]
fn disabled_price_reporting_skips_malformed_unused_application_utilization() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_gas_reporting_gates.json"
    ))
    .unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 16);
    for r in rows {
        let mut context = Context::new(Directories {
            home: "/release".into(),
            data: "data".into(),
            log: "log".into(),
        })
        .unwrap();
        context
            .set(
                "aehttp".into(),
                "gas_price".into(),
                r["application_pairs"].clone(),
            )
            .unwrap();
        let config = Configuration {
            value: json!({"http":{"gas_price":r["user"]}}),
        };
        let reporting = GasReporting::resolve(&config.resolve(&context).unwrap());
        if r["first"]["code"] == 500 {
            assert_eq!(reporting, Err(Error::Lookup), "{r}");
        } else {
            let reporting = reporting.unwrap();
            for expected in r["first"]["body"].as_array().unwrap() {
                let mut summary = ae_chain::gas_prices::Summary {
                    minutes: expected["minutes"].as_u64().unwrap() as u32,
                    minimum: 1000.into(),
                    utilization: 17.into(),
                };
                reporting.apply(&mut summary);
                assert_eq!(
                    summary.minimum.to_string(),
                    expected["min_gas_price"].to_string(),
                    "{r}"
                );
                assert_eq!(
                    summary.utilization.to_string(),
                    expected["utilization"].to_string(),
                    "{r}"
                );
            }
        }
        assert_eq!(r["first"]["code"], r["second"]["code"]);
        assert_eq!(r["first"]["body"], r["second"]["body"]);
    }
}

#[test]
fn original_protocol_parameter_micro_cycle_lookup_matches_native_precedence() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_protocol_parameters.json"
    ))
    .unwrap();
    let mut compared = 0;
    for row in fixture["rows"].as_array().unwrap() {
        if row["code"] != 200 {
            continue;
        }
        let mut context = Context::new(Directories {
            home: "/release".into(),
            data: "data".into(),
            log: "log".into(),
        })
        .unwrap();
        for (app, values) in row["applications"].as_object().unwrap() {
            for (key, value) in values.as_object().unwrap() {
                context
                    .set(app.clone(), key.clone(), value.clone())
                    .unwrap();
            }
        }
        let configuration = Configuration {
            value: row["user"].clone(),
        };
        let resolved = configuration.resolve(&context).unwrap();
        assert_eq!(
            crate::configuration::chain::micro_block_cycle(&resolved).unwrap(),
            row["body"]["micro_block_cycle"],
            "{}",
            row["name"]
        );
        compared += 1;
    }
    assert_eq!(compared, 138);
    for (value, expected) in [
        (json!(0), Some(0)),
        (json!(17), Some(17)),
        (json!(86_400_000), Some(86_400_000)),
        (json!(86_400_001), None),
        (json!(-1), None),
        (json!(false), None),
        (json!("17"), None),
        (Value::Null, None),
    ] {
        let mut context = Context::new(Directories {
            home: "/release".into(),
            data: "data".into(),
            log: "log".into(),
        })
        .unwrap();
        context
            .set("aecore".into(), "micro_block_cycle".into(), value.clone())
            .unwrap();
        let config = Configuration::empty();
        let resolved = config.resolve(&context).unwrap();
        let settings = crate::configuration::chain::Settings::resolve(&resolved, None);
        assert_eq!(
            settings.as_ref().ok().map(|s| s.micro_cycle_ms()),
            expected,
            "{value}"
        );
        if expected.is_none() {
            assert_eq!(
                settings.unwrap_err(),
                crate::configuration::chain::Error::MicroCycle
            );
        }
    }
}

#[test]
fn original_settings_lookup_matches_nested_application_precedence_and_typed_pool_policy() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_node_settings.json"
    ))
    .unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 149);
    let mut dry_compared = 0;
    let mut pool_compared = 0;
    for row in rows {
        if row["failure"] != "none" {
            continue;
        }
        let mut context = Context::new(Directories {
            home: "/release".into(),
            data: "data".into(),
            log: "log".into(),
        })
        .unwrap();
        for (app, values) in row["applications"].as_object().unwrap() {
            for (key, value) in values.as_object().unwrap() {
                context
                    .set(app.clone(), key.clone(), value.clone())
                    .unwrap();
            }
        }
        // The native getter corpus injects values after configuration validation.
        // Keep the same boundary here; typed operational policy rejects invalid
        // values separately, rather than inventing valid policy for them.
        let configuration = Configuration {
            value: row["user"].clone(),
        };
        let resolved = configuration.resolve(&context).unwrap();
        let dry = external_dry_run_gas_limit(&resolved);
        if row["code"] == 200 {
            assert_eq!(dry.unwrap(), row["body"]["dry_run_gas_limit"], "{row}");
            dry_compared += 1;
            if let Ok(core) = Core::resolve(&resolved, Path::new("/release")) {
                let policy = core.pool;
                assert_eq!(
                    policy.minimum_miner_gas_price,
                    ae_types::Unsigned::from_decimal(
                        row["body"]["min_miner_gas_price"].as_str().unwrap()
                    )
                    .unwrap()
                );
                for (actual, field) in [
                    (policy.maximum_auth_gas, "max_auth_fun_gas"),
                    (policy.nonce_offset, "mempool_nonce_offset"),
                ] {
                    assert_eq!(
                        actual,
                        ae_types::Unsigned::from_decimal(
                            row["body"][field].as_number().unwrap().as_str()
                        )
                        .unwrap(),
                        "{row}"
                    );
                }
                assert_eq!(Some(policy.tx_ttl), row["body"]["mempool_tx_ttl"].as_u64());
                pool_compared += 1;
            }
        } else if row["name"] == "external-shape" {
            assert_eq!(dry, Err(Error::Lookup));
        }
    }
    assert_eq!(dry_compared, 140);
    assert!(pool_compared > 90, "{pool_compared}");
}

#[test]
fn nested_application_paths_preserve_first_tuple_expansion_and_missing_shape() {
    let mut context = Context::new(Directories {
        home: "/release".into(),
        data: "data".into(),
        log: "log".into(),
    })
    .unwrap();
    context
        .set(
            "aehttp".into(),
            "external".into(),
            json!([["nested", [["gas_limit", "$DATA_DIR"], ["gas_limit", 2]]]]),
        )
        .unwrap();
    assert_eq!(
        context
            .application_path("aehttp", &["external", "nested", "gas_limit"])
            .unwrap(),
        Some(json!("data"))
    );
    assert_eq!(
        context
            .application_path("aehttp", &["external", "missing"])
            .unwrap(),
        None
    );
    assert_eq!(context.application_path("aehttp", &[]), Err(Error::Lookup));
    assert_eq!(
        context.application_path("aehttp", &vec!["x"; 65]),
        Err(Error::Limit)
    );
}
