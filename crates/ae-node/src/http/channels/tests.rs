use super::*;
fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../../ae-chain/tests/fixtures/erlang_http_channels.json"
    ))
    .unwrap()
}
fn state(row: &Value, key: [u8; 32]) -> Result<Channel, Error> {
    match row["input"]["scenario"].as_str().unwrap() {
        "no_state" | "no_top" => return Err(Error::NoStateTrees),
        "storage_error" => {
            return Err(Error::Backend(ae_chain::repository::Error::Backend(
                "injected".into(),
            )));
        }
        _ => (),
    }
    let entry = row["channels"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| hex::decode(p[0].as_str().unwrap()).unwrap() == key)
        .ok_or(Error::NotPresent)?;
    Channel::deserialize(key, &hex::decode(entry[1].as_str().unwrap()).unwrap()).map_err(Into::into)
}
#[test]
fn original_channel_dispatcher_matches_all_historical_fields_and_failure_precedence() {
    let f = fixture();
    let rows = f["dispatcher"].as_array().unwrap();
    assert_eq!(rows.len(), 328);
    for row in rows {
        let op = Operation {
            key: hex::decode(row["params_hex"]["pubkey"].as_str().unwrap()).unwrap(),
        };
        let r = match row["input"]["scenario"].as_str().unwrap() {
            "overload" => Response::unavailable(true),
            "unstable" => Response::unavailable(false),
            _ => op
                .decode()
                .and_then(|key| state(row, key).map_err(failure))
                .and_then(body)
                .map(|v| Response::json(200, v))
                .unwrap_or_else(|r| r),
        };
        assert_eq!(
            u64::from(r.status),
            row["code"].as_u64().unwrap(),
            "{}",
            row["input"]
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&r.body).unwrap(),
            row["body"],
            "{}",
            row["input"]
        );
        assert_eq!(
            r.headers.get("retry-after").map(String::as_str),
            row["headers"]["retry-after"].as_str()
        );
    }
}
fn channel() -> Channel {
    let f = fixture();
    let row = &f["dispatcher"][0];
    state(
        row,
        Operation {
            key: row["params"]["pubkey"]
                .as_str()
                .unwrap()
                .as_bytes()
                .to_vec(),
        }
        .decode()
        .unwrap(),
    )
    .unwrap()
}
#[test]
fn channel_response_bounds_aggregate_integer_work_delegate_count_and_state_encoding() {
    let mut c = channel();
    c.channel_amount = ae_types::Unsigned::from_field_bytes(&vec![255; 6000]).unwrap();
    assert!(body(c.clone()).is_ok());
    c.round = c.channel_amount.clone();
    assert_eq!(body(c).unwrap_err().status, 503);
    let mut c = channel();
    c.state_hash = vec![0; JSON_BYTES / 2];
    assert_eq!(body(c).unwrap_err().status, 503);
    let mut c = channel();
    c.delegates = Delegates::Separate {
        initiator: vec![ae_types::Id::new(ae_types::IdTag::Account, [1; 32]); 2048],
        responder: vec![ae_types::Id::new(ae_types::IdTag::Contract, [2; 32]); 2049],
    };
    assert_eq!(body(c).unwrap_err().status, 503);
    let mut c = channel();
    c.delegates = Delegates::Shared(vec![]);
    assert_eq!(body(c).unwrap_err().status, 500);
    assert_eq!(failure(Error::ResourceLimit).status, 503);
}
#[test]
fn channel_routes_limit_identifiers_and_require_channel_or_external_groups() {
    for group in ["channel", "external"] {
        let cfg = Config {
            groups: [group.into()].into(),
            ..Default::default()
        };
        for v in ["v2", "v3"] {
            assert!(response::route(&format!("/{v}/channels/bad"), &cfg).is_some());
        }
    }
    let cfg = Config {
        groups: ["name_service".into()].into(),
        ..Default::default()
    };
    assert!(response::route("/v3/channels/bad", &cfg).is_none());
    assert_eq!(
        Operation {
            key: vec![b'a'; 4097]
        }
        .decode()
        .unwrap_err()
        .status,
        503
    );
    for n in [0, 1, 31, 33, 64] {
        assert_eq!(
            Operation {
                key: api::encode(ApiType::Channel, &vec![1; n]).into_bytes()
            }
            .decode()
            .unwrap_err()
            .status,
            400
        );
    }
}
