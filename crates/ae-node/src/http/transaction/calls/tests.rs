use super::*;
use ae_chain::transaction_info as selected;
use ae_state::calls::LogEntry;
use ae_types::IdTag;

fn key(n: u8) -> [u8; 32] {
    let mut k = [0; 32];
    k[31] = n;
    k
}
fn constructed(input: &Value) -> Result<(Ga, Info), selected::Error> {
    let height = Unsigned::from_decimal(input["height"].as_str().unwrap()).unwrap();
    let gas = Unsigned::from_decimal(input["gas"].as_str().unwrap()).unwrap();
    let result = match input["return_type"].as_str().unwrap() {
        "ok" => ReturnType::Ok,
        "error" => ReturnType::Error,
        "revert" => ReturnType::Revert,
        _ => panic!(),
    };
    let mut auth = Call::new(
        Id::new(IdTag::Account, key(1)),
        0.into(),
        key(2),
        None,
        height.clone(),
        gas.clone(),
    );
    auth.gas_used = gas.add(&1.into());
    auth.return_type = result;
    auth.return_value = vec![0, 255, 128];
    let auth = Ga::from_call(auth)?;
    let inner = match input["inner"].as_str().unwrap() {
        "undefined" => Info::Undefined,
        "spend_tx" => Info::Type(TransactionKind::Spend),
        "ga_meta_tx" => selected::inner(
            TransactionKind::GaMeta,
            Info::Ga {
                call: auth.clone(),
                inner: Box::new(Info::Type(TransactionKind::Spend)),
            },
        )?,
        kind => {
            let mut call = Call::new(
                Id::new(IdTag::Account, key(1)),
                3.into(),
                key(4),
                None,
                height,
                gas.clone(),
            );
            call.gas_used = gas.add(&1.into());
            call.return_type = result;
            call.return_value = vec![255, 0];
            call.log = vec![LogEntry {
                address: key(5).to_vec(),
                topics: vec![key(0).to_vec(), key(255).to_vec()],
                data: vec![0, 255],
            }];
            selected::inner(
                match kind {
                    "contract_call_tx" => TransactionKind::ContractCall,
                    "contract_create_tx" => TransactionKind::ContractCreate,
                    "paying_for_tx" => TransactionKind::PayingFor,
                    "ga_attach_tx" => TransactionKind::GaAttach,
                    _ => panic!(),
                },
                Info::Call(Box::new(call)),
            )?
        }
    };
    Ok((auth, inner))
}
#[test]
fn original_call_and_ga_clients_match_all_successes_and_rejections() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_call_clients.json"
    ))
    .unwrap();
    let mut counts = [0; 4];
    for row in fixture["rows"].as_array().unwrap() {
        let input = &row["input"];
        let expected = &row["result"];
        if input["kind"] == "call" {
            let id = hex::decode(input["call_id"].as_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap();
            let wire = hex::decode(input["wire"].as_str().unwrap()).unwrap();
            let parsed = Call::deserialize(id, &wire);
            if expected.get("decode_error").is_some() {
                assert!(parsed.is_err(), "{input}");
                counts[1] += 1;
            } else {
                let parsed = parsed.unwrap();
                assert_eq!(
                    parsed.serialize().unwrap(),
                    hex::decode(expected["canonical"].as_str().unwrap()).unwrap()
                );
                assert_eq!(
                    call(&parsed, Limits::default()).unwrap(),
                    expected["client"]["body"],
                    "{input}"
                );
                counts[0] += 1;
            }
        } else {
            let parsed = constructed(input);
            if expected.get("error").is_some() {
                assert_eq!(parsed, Err(selected::Error::InvalidObject), "{input}");
                counts[3] += 1;
            } else {
                let (a, i) = parsed.unwrap();
                assert_eq!(
                    ga(&a, &i, Limits::default()).unwrap(),
                    expected["body"],
                    "{input}"
                );
                counts[2] += 1;
            }
        }
    }
    assert_eq!(counts, [221, 869, 90, 78]);
}
#[test]
fn call_client_limits_and_outer_envelopes_preserve_stored_call_data() {
    let mut value = Call::new(
        Id::new(IdTag::Contract, key(1)),
        3.into(),
        key(4),
        Some(key(9)),
        77.into(),
        5.into(),
    );
    value.log = vec![LogEntry {
        address: vec![],
        topics: vec![vec![], vec![1], key(0).to_vec(), key(255).to_vec()],
        data: vec![255; 4096],
    }];
    let canonical = value.serialize().unwrap();
    let body = call(&value, Limits::default()).unwrap();
    assert_eq!(body["log"][0]["topics"], serde_json::json!([0, 255]));
    assert!(body.get("call_target").is_none());
    for limits in [
        Limits {
            json_bytes: 1,
            ..Default::default()
        },
        Limits {
            work_bytes: 1,
            ..Default::default()
        },
    ] {
        assert_eq!(call(&value, limits), Err(Error::ResourceLimit));
    }
    let mut tree = Info::Type(TransactionKind::Spend);
    for _ in 0..8 {
        tree = Info::Ga {
            call: Ga::from_call(value.clone()).unwrap(),
            inner: Box::new(tree),
        };
    }
    assert_eq!(
        info(
            &tree,
            TransactionKind::GaMeta,
            Limits {
                layers: 7,
                ..Default::default()
            }
        ),
        Err(Error::ResourceLimit)
    );
    assert_eq!(
        info(&tree, TransactionKind::PayingFor, Limits::default()),
        Err(Error::InvalidInfo)
    );
    assert!(info(&tree, TransactionKind::GaMeta, Limits::default()).is_ok());
    assert_eq!(canonical, value.serialize().unwrap());
}
