use super::*;
use ae_chain::transaction_info::{self as info, Ga, Lookup};
use ae_state::calls::{Call, ReturnType};
use ae_tx::{SignedTransaction, Transaction};
use ae_types::{AccountPublicKey, Hash256, Id, IdTag};

fn key(n: u8) -> [u8; 32] {
    let mut k = [0; 32];
    k[31] = n;
    k
}
struct Provider<'a> {
    scenario: &'a str,
    trace: Vec<Value>,
}
impl Provider<'_> {
    fn before(&mut self, method: &str, args: &[[u8; 32]]) -> Result<(), info::Error> {
        self.trace.push(
            json!({"method":method,"args":args.iter().map(hex::encode_upper).collect::<Vec<_>>()}),
        );
        match self.scenario {
            "call_not_found" => Err(info::Error::CallNotFound),
            "garbage_collected" => Err(info::Error::GarbageCollected),
            "no_state_trees" => Err(info::Error::NoStateTrees),
            "call_fault" => Err(info::Error::Backend(ae_chain::repository::Error::Backend(
                "injected_call_read".into(),
            ))),
            _ => Ok(()),
        }
    }
}
impl Lookup for Provider<'_> {
    fn contract(
        &mut self,
        contract: [u8; 32],
        id: Hash256,
        block: Hash256,
    ) -> Result<Call, info::Error> {
        self.before("get_contract_call", &[contract, id.0, block.0])?;
        let mut c = Call::new(
            Id::new(IdTag::Account, key(1)),
            3.into(),
            contract,
            None,
            77.into(),
            5.into(),
        );
        c.id = id.0;
        c.gas_used = 6.into();
        c.return_value = vec![0, 255];
        Ok(c)
    }
    fn ga(
        &mut self,
        owner: AccountPublicKey,
        auth: Hash256,
        block: Hash256,
    ) -> Result<Ga, info::Error> {
        self.before("get_ga_call", &[owner.0, auth.0, block.0])?;
        let mut c = Call::new(
            Id::new(IdTag::Account, owner.0),
            0.into(),
            auth.0,
            None,
            77.into(),
            5.into(),
        );
        c.gas_used = 6.into();
        c.return_value = vec![0, 255];
        c.return_type = match self.scenario {
            "ga_error" => ReturnType::Error,
            "ga_revert" => ReturnType::Revert,
            _ => ReturnType::Ok,
        };
        Ga::from_call(c)
    }
}
#[test]
fn native_transaction_info_selection_identifiers_and_dispatch_errors_match() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_transaction_info.json"
    ))
    .unwrap();
    let transactions = fixture["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            SignedTransaction::<Transaction>::decode(
                &hex::decode(x["canonical"].as_str().unwrap()).unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(transactions.len(), 145);
    let mut selected = 0;
    for row in fixture["rows"].as_array().unwrap() {
        let i = &row["input"];
        let scenario = i["scenario"].as_str().unwrap();
        // Queue/stability and absent REST bindings are covered by live/parser
        // tests. This comparison executes real hash parsing and selection.
        if matches!(scenario, "overload" | "unstable") || i["hash_kind"] == "absent" {
            continue;
        }
        let tx = &transactions[i["transaction"].as_u64().unwrap() as usize];
        let op = Operation::Info(row["params"]["hash"].as_str().unwrap().as_bytes().to_vec());
        let mut trace = vec![];
        let response = match op.request() {
            Err(r) => r,
            Ok(Request::Info(hash)) => {
                trace.push(
                    json!({"method":"find_tx_with_location","args":[hex::encode_upper(hash.0)]}),
                );
                let reply = match scenario {
                    "missing" => Reply::MissingTransaction,
                    "mempool" => Reply::NotMined,
                    "storage" => {
                        assert_eq!(row["code"], 500);
                        assert_eq!(trace, *row["calls"].as_array().unwrap());
                        continue;
                    }
                    _ => {
                        let mut provider = Provider {
                            scenario,
                            trace: vec![],
                        };
                        let result =
                            info::select(tx, Hash256(key(9)), &mut provider, Default::default());
                        trace.extend(provider.trace);
                        Reply::Info {
                            kind: tx.transaction().view().kind(),
                            info: result,
                        }
                    }
                };
                // Dispatcher comparison is before HTTP response-schema validation.
                match body(&op, reply) {
                    Ok(body) => Response::json(200, body),
                    Err(r) => r,
                }
            }
            _ => panic!(),
        };
        assert_eq!(
            u64::from(response.status),
            row["code"].as_u64().unwrap(),
            "{i}"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap(),
            row["body"],
            "{i}"
        );
        assert_eq!(trace, *row["calls"].as_array().unwrap(), "{i}");
        selected += 1;
    }
    assert_eq!(selected, 1310);
}
#[test]
fn all_native_transaction_info_http_schemas_match_exact_integer_and_gc_behavior() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_transaction_info_schemas.json"
    ))
    .unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 8276);
    let mut path_rejections = 0;
    for r in rows {
        let input = &fixture["inputs"][r["id"].as_u64().unwrap() as usize];
        if r["logic_reads"] == 0 {
            assert!(input["bindings"].get("hash").is_none());
            path_rejections += 1;
            continue;
        }
        let op = Operation::Info(vec![]);
        let v3 = r["module"] == "oas_endpoints";
        let strings = v3 && r["query"]["id"] == "true";
        let mut body = input["body"].clone();
        if strings {
            response::integers_as_strings(&mut body)
        }
        let code = input["code"].as_u64().unwrap() as u16;
        let mut response = if code == 200 {
            finish(&op, input["body"].clone(), v3, strings)
        } else if code == 410 {
            render(
                &op,
                Reply::Info {
                    kind: ae_tx::TransactionKind::ContractCall,
                    info: Err(info::Error::GarbageCollected),
                },
                v3,
                strings,
            )
        } else {
            Response::json(code, body)
        };
        let expected = &r["result"]["response"];
        assert_eq!(
            u64::from(response.status),
            expected["code"].as_u64().unwrap(),
            "input {} v3 {v3} strings {strings}",
            r["id"]
        );
        if r["result"]["kind"] == "stream_error" {
            assert!(response.body.is_empty());
            response.stamp(
                &Config::default(),
                &std::collections::BTreeMap::from([(
                    "origin".into(),
                    "https://client.example".into(),
                )]),
                Some(7),
            );
            assert!(response.headers.is_empty());
        } else {
            assert_eq!(
                serde_json::from_slice::<Value>(&response.body).unwrap(),
                expected["body"]
            )
        }
    }
    assert_eq!(path_rejections, 12);
}

#[test]
fn transaction_info_limits_reject_before_lookup_and_routes_use_transaction_group() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_transaction_info.json"
    ))
    .unwrap();
    let tx = fixture["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            SignedTransaction::<Transaction>::decode(
                &hex::decode(x["canonical"].as_str().unwrap()).unwrap(),
            )
            .unwrap()
        })
        .find(|t| !t.transaction().view().layers().is_empty())
        .unwrap();
    for limits in [
        info::Limits {
            layers: 0,
            ..Default::default()
        },
        info::Limits {
            work_bytes: 0,
            ..Default::default()
        },
    ] {
        let mut provider = Provider {
            scenario: "present",
            trace: vec![],
        };
        assert_eq!(
            info::select(&tx, Hash256::ZERO, &mut provider, limits),
            Err(info::Error::ResourceLimit)
        );
        assert!(provider.trace.is_empty());
    }
    let operation = route(&[b"transactions", b"bad", b"info"]).unwrap();
    assert_eq!(operation.group(), "transaction");
    assert_eq!(operation.name(), "GetTransactionInfoByHash");
    assert!(route(&[b"transactions", b"bad", b"info", b"extra"]).is_none());
}
