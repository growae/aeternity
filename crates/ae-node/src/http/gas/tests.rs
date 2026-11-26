use super::*;

fn summaries() -> [Summary; 4] {
    [1, 5, 15, 60].map(|minutes| Summary {
        minutes,
        minimum: 1000.into(),
        utilization: 17.into(),
    })
}

#[test]
fn original_recent_gas_dispatcher_reporting_matches_all_configuration_rows() {
    let f: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_recent_gas.json"
    ))
    .unwrap();
    let resolved: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_gas_reporting.json"
    ))
    .unwrap();
    let mut count = 0;
    for row in f["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["scenario"] == "observed")
    {
        // Resolution itself is independently checked in configuration/http.
        // Use its original resolved values to check the rendered dispatcher body.
        let native = &resolved[count];
        assert_eq!(row["user"], native["user"]);
        assert_eq!(row["application"], native["application"]);
        let value = &native["result"]["value"];
        let reporting = GasReporting {
            price: value["floor"]
                .as_number()
                .map(|n| n.as_str().parse().unwrap()),
            utilization: value["utilization"].as_u64().unwrap() as u8,
        };
        let actual = body(summaries(), &reporting).unwrap();
        assert_eq!(actual, row["first"]["body"], "{}", row["name"]);
        assert_eq!(actual, row["second"]["body"], "{}", row["name"]);
        count += 1;
    }
    assert_eq!(count, 44);
}

#[test]
fn gas_response_bounds_errors_and_owner_close_preserve_distinct_failures() {
    let reporting = GasReporting {
        price: Some(BigInt::from(1u8) << (INTEGER_BYTES * 8)),
        utilization: 90,
    };
    assert_eq!(body(summaries(), &reporting).unwrap_err().status, 503);
    let mut rows = summaries();
    rows[2].utilization = BigInt::from(1u8) << (INTEGER_BYTES * 8);
    assert_eq!(
        body(rows, &GasReporting::default()).unwrap_err().status,
        503
    );
    let mut response = failure(QueryError::Observation(gas::Error::MissingHeader(
        ae_types::Hash256::ZERO,
    )));
    response.stamp(
        &Config::default(),
        &response::Headers::from([("origin".into(), "https://client.example".into())]),
        Some(99),
    );
    assert_eq!(response.status, 500);
    assert!(response.body.is_empty() && response.headers.is_empty());
    for error in [
        gas::Error::MissingState(ae_types::Hash256::ZERO),
        gas::Error::Arithmetic,
        gas::Error::InvalidObject,
    ] {
        let response = failure(QueryError::Observation(error));
        assert_eq!(response.status, 500);
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap(),
            json!({"reason":"Internal server error"})
        );
    }
    assert_eq!(
        failure(QueryError::Observation(gas::Error::ResourceLimit)).status,
        503
    );
    assert_eq!(failure(QueryError::Cache(CacheError::Timeout)).status, 500);
    let cache = Arc::new(Cache::new(Default::default()).unwrap());
    let owner = Owner(cache.clone());
    drop(owner);
    assert!(matches!(
        cache.get_or_fill(
            ae_types::Hash256::ZERO,
            || Ok(()),
            |_| panic!("closed cache cannot fill")
        ),
        Err(CacheError::Closed)
    ));
}
