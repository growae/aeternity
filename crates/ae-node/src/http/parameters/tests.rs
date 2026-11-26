use super::*;

#[test]
fn original_protocol_parameter_dispatcher_preserves_tables_forks_and_governance_values() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_protocol_parameters.json"
    ))
    .unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 141);
    let mut counts = [0usize; 3];
    for row in rows {
        let network = row["network"].as_str().unwrap().as_bytes();
        let value = |key: &str, default: Value| {
            row["user"]["mining"]
                .as_object()
                .and_then(|o| o.get(key))
                .or_else(|| {
                    row["applications"]["aecore"]
                        .as_object()
                        .and_then(|o| o.get(key))
                })
                .cloned()
                .unwrap_or(default)
        };
        let result = if row["failure"] != "none" {
            counts[2] += 1;
            Err(Response::unavailable(row["failure"] == "overload"))
        } else {
            let schedule = crate::configuration::protocols::Protocols::select(
                network,
                row["user"]["chain"]
                    .as_object()
                    .and_then(|o| o.get("hard_forks")),
            );
            match schedule {
                Err(_) => {
                    counts[1] += 1;
                    Err(Response::internal())
                }
                Ok(schedule) => {
                    let fork = &row["applications"]["aecore"]["fork"];
                    let signalled = fork["version"]
                        .as_u64()
                        .map(|v| (v.into(), fork["signalling_end_height"].clone()));
                    let child =
                        &row["user"]["chain"]["consensus"]["0"]["config"]["child_block_time"];
                    let interval = if row["consensus"] == "pow" {
                        Some((
                            "expected_block_mine_rate",
                            value("expected_mine_rate", json!(180000)),
                        ))
                    } else {
                        child
                            .as_number()
                            .and_then(|n| n.as_str().parse::<BigInt>().ok())
                            .map(|_| ("child_block_time", child.clone()))
                    };
                    let auction = row["user"]["mining"]
                        .as_object()
                        .and_then(|o| o.get("name_claim_bid_timeout"))
                        .or_else(|| {
                            row["applications"]["aecore"]
                                .as_object()
                                .and_then(|o| o.get("name_claim_bid_timeout"))
                        })
                        .cloned();
                    counts[0] += 1;
                    body(
                        (row["kind"] != "undefined")
                            .then(|| row["version"].as_u64().unwrap() as u32),
                        Tables {
                            network: network.to_vec(),
                            schedule: schedule.entries().iter().cloned().collect(),
                            signalled,
                            genesis: row["genesis"].as_u64().unwrap() as u32,
                        },
                        Values {
                            micro_cycle: value("micro_block_cycle", json!(3000)),
                            interval,
                            auction,
                        },
                    )
                }
            }
        };
        let response = match result {
            Ok(body) => Response::json(200, body),
            Err(r) => r,
        };
        assert_eq!(
            u64::from(response.status),
            row["code"].as_u64().unwrap(),
            "{}",
            row["name"]
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap(),
            row["body"],
            "{} user={} app={}",
            row["name"],
            row["user"],
            row["applications"]
        );
    }
    assert_eq!(counts, [138, 1, 2]);
}

#[test]
fn bound_parameter_rendering_limits_arbitrary_integer_work_and_uses_explicit_values() {
    let settings = Bound {
        micro_cycle_ms: 17,
        interval: Interval::Pos(23.into()),
        auction: Some(0.into()),
    };
    let values = settings.values().unwrap();
    assert_eq!(values.micro_cycle, 17);
    assert_eq!(values.interval, Some(("child_block_time", json!(23))));
    assert_eq!(values.auction, Some(json!(0)));
    let huge = Unsigned::from_field_bytes(&vec![1; 8193]).unwrap();
    let settings = Bound {
        micro_cycle_ms: 17,
        interval: Interval::Pos(1.into()),
        auction: Some(huge),
    };
    assert_eq!(settings.values().err().unwrap().status, 503);
}
