use super::*;
use ae_state::oracles::Oracles;
use ae_trie::{MemoryStore, Nibbles, Trie};
fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../../ae-chain/tests/fixtures/erlang_http_oracles.json"
    ))
    .unwrap()
}
fn operation(row: &Value) -> Operation {
    let p = &row["params"];
    let selection = match row["input"]["operation"].as_str().unwrap() {
        "GetOracleByPubkey" => Selection::Metadata,
        "GetOracleQueryByPubkeyAndQueryId" => {
            Selection::Query(p["query-id"].as_str().unwrap().as_bytes().to_vec())
        }
        _ => Selection::Queries {
            cursor: (p["from"] != "undefined")
                .then(|| Some(p["from"].as_str().unwrap().as_bytes().to_vec())),
            filter: match p["type"].as_str().unwrap() {
                "open" => QueryFilter::Open,
                "closed" => QueryFilter::Closed,
                _ => QueryFilter::All,
            },
            limit: p["limit"].as_u64().unwrap() as usize,
        },
    };
    Operation {
        key: p["pubkey"].as_str().unwrap().as_bytes().to_vec(),
        selection,
    }
}
fn state(row: &Value) -> Oracles {
    let mut tree = Trie::default();
    for pair in row["oracles"].as_array().unwrap() {
        tree.put(
            &Nibbles::from_bytes(&hex::decode(pair[0].as_str().unwrap()).unwrap()),
            ae_trie::Value::Bytes(hex::decode(pair[1].as_str().unwrap()).unwrap()),
        )
        .unwrap();
    }
    Oracles::from_roots(
        tree.root_hash(),
        None,
        tree.store().clone(),
        MemoryStore::default(),
    )
    .unwrap()
}
#[test]
fn original_oracle_dispatcher_matches_all_encoded_fields_queries_and_failure_precedence() {
    let f = fixture();
    let rows = f["dispatcher"].as_array().unwrap();
    assert_eq!(rows.len(), 668);
    for row in rows {
        let op = operation(row);
        let response = (|| {
            let scenario = row["input"]["scenario"].as_str().unwrap();
            if scenario == "overload" {
                return Response::unavailable(true);
            }
            if scenario == "unstable" {
                return Response::unavailable(false);
            }
            let (key, kind) = match op.decode() {
                Ok(v) => v,
                Err(r) => return r,
            };
            if matches!(scenario, "no_state" | "no_top") {
                return failure(&op.selection, Error::NoStateTrees);
            }
            if scenario == "storage_error" {
                return failure(
                    &op.selection,
                    Error::Backend(ae_chain::repository::Error::Backend("injected".into())),
                );
            }
            let tree = state(row);
            let reply = match kind {
                Kind::Metadata => tree
                    .oracle(key)
                    .unwrap()
                    .map(Reply::Metadata)
                    .ok_or(Error::NotPresent),
                Kind::Query(id) => tree
                    .query(key, id)
                    .unwrap()
                    .map(Reply::Query)
                    .ok_or(Error::NotPresent),
                Kind::Queries {
                    after,
                    filter,
                    limit,
                } => Ok(Reply::Queries(
                    tree.queries(key, after, filter, limit).unwrap(),
                )),
            };
            reply
                .map_err(|e| failure(&op.selection, e))
                .and_then(body)
                .map(|v| Response::json(200, v))
                .unwrap_or_else(|r| r)
        })();
        assert_eq!(
            u64::from(response.status),
            row["code"].as_u64().unwrap(),
            "{}",
            row["input"]
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap(),
            row["body"],
            "{}",
            row["input"]
        );
    }
}
fn query_record() -> Query {
    Query::new(
        ae_types::Id::new(ae_types::IdTag::Oracle, [1; 32]),
        ae_types::AccountPublicKey([2; 32]),
        1.into(),
        vec![],
        1.into(),
        100.into(),
        10.into(),
    )
    .unwrap()
}
#[test]
fn oracle_response_budgets_reject_whole_pages_and_sum_decimal_work_before_rendering() {
    let mut q = query_record();
    q.query = vec![0; 4096];
    assert_eq!(body(Reply::Queries(vec![q; 1000])).unwrap_err().status, 503);
    let mut q = query_record();
    q.fee = Unsigned::from_field_bytes(&vec![255; 6000]).unwrap();
    assert!(body(Reply::Query(q.clone())).is_ok());
    assert_eq!(body(Reply::Queries(vec![q; 2])).unwrap_err().status, 503);
    assert_eq!(
        body(Reply::Queries(vec![query_record(); 1001]))
            .unwrap_err()
            .status,
        503
    );
    let mut q = query_record();
    q.response = Some(vec![0; JSON_BYTES / 2]);
    assert_eq!(body(Reply::Query(q)).unwrap_err().status, 503);
    let o = Oracle {
        public_key: [1; 32],
        query_format: vec![0; JSON_BYTES / 6],
        response_format: vec![],
        query_fee: 1.into(),
        expires_by: 1.into(),
        abi_version: 0.into(),
    };
    assert_eq!(body(Reply::Metadata(o)).unwrap_err().status, 503);
}
#[test]
fn oracle_routes_identifier_limits_and_missing_state_keep_distinct_errors() {
    let mut cfg = Config::default();
    cfg.groups.clear();
    for group in ["oracle", "external"] {
        cfg.groups.insert(group.into());
        for v in ["v2", "v3"] {
            for suffix in ["", "/queries", "/queries/bad"] {
                assert!(response::route(&format!("/{v}/oracles/bad{suffix}"), &cfg).is_some());
            }
        }
        cfg.groups.clear();
    }
    assert!(response::route("/v3/oracles/bad", &cfg).is_none());
    let key = api::encode(ApiType::OraclePubkey, &[1; 32]).into_bytes();
    let mut op = Operation {
        key: key.clone(),
        selection: Selection::Queries {
            cursor: Some(None),
            filter: QueryFilter::All,
            limit: 20,
        },
    };
    assert_eq!(op.decode().unwrap_err().status, 500);
    op.key = api::encode(ApiType::AccountPubkey, &[1; 32]).into_bytes();
    assert_eq!(op.decode().unwrap_err().status, 400);
    for selection in [
        Selection::Metadata,
        Selection::Query(vec![b'a'; 4097]),
        Selection::Queries {
            cursor: Some(Some(vec![b'a'; 4097])),
            filter: QueryFilter::All,
            limit: 20,
        },
    ] {
        let op = Operation {
            key: if selection == Selection::Metadata {
                vec![b'a'; 4097]
            } else {
                key.clone()
            },
            selection,
        };
        assert_eq!(op.decode().unwrap_err().status, 503);
    }
    let selection = Selection::Queries {
        cursor: None,
        filter: QueryFilter::All,
        limit: 20,
    };
    assert_eq!(failure(&selection, Error::NoStateTrees).status, 200);
    assert_eq!(failure(&selection, Error::InvalidObject).status, 500);
    assert_eq!(
        failure(
            &selection,
            Error::Backend(ae_chain::repository::Error::Backend("fault".into()))
        )
        .status,
        500
    );
    assert_eq!(failure(&selection, Error::ResourceLimit).status, 503);
    assert_eq!(
        failure(&Selection::Metadata, Error::NoStateTrees).status,
        404
    );
}
