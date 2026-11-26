use super::*;
use ae_mempool::pool::{Pool, accounts::Limits as PoolLimits};
use ae_state::accounts::Account;
use ae_tx::{SignedTransaction, Spend, Transaction, TransactionLeaf};
use ae_types::{Id, IdTag};

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_http_accounts.json"
    ))
    .unwrap()
}
fn operation(name: &str, bindings: &Value) -> Operation {
    let key = bindings["pubkey"].as_str().unwrap().as_bytes().to_vec();
    match name {
        "GetAccountByPubkey" => Operation::Current(key),
        "GetAccountByPubkeyAndHeight" => Operation::Height {
            key,
            height: bindings["height"].as_str().unwrap().as_bytes().to_vec(),
        },
        "GetAccountByPubkeyAndHash" => Operation::Hash {
            key,
            hash: bindings["hash"].as_str().unwrap().as_bytes().to_vec(),
        },
        "GetPendingAccountTransactionsByPubkey" => Operation::Pending(key),
        "GetAccountNextNonce" => Operation::Nonce {
            key,
            strategy: if bindings["strategy"] == "continuity" {
                Strategy::Continuity
            } else {
                Strategy::Max
            },
        },
        _ => panic!("operation"),
    }
}
fn tx(nonce: Unsigned) -> Arc<SignedTransaction<Transaction>> {
    let mut sender = [0; 32];
    sender[31] = 1;
    let mut recipient = [0; 32];
    recipient[31] = 2;
    let mut tx = Spend::new(AccountPublicKey(sender), Id::new(IdTag::Account, recipient)).unwrap();
    tx.set_nonce(nonce);
    tx.set_amount(1.into());
    tx.set_fee(1_000_000_000_000u64.into());
    Arc::new(SignedTransaction::new(
        Transaction::new(TransactionLeaf::Spend(tx)),
        vec![],
    ))
}

#[test]
fn all_native_account_schema_outcomes_preserve_coercion_response_types_and_int_strings() {
    let f = fixture();
    let inputs = f["inputs"].as_array().unwrap();
    let rows = f["schema"].as_array().unwrap();
    assert_eq!(inputs.len(), 572);
    assert_eq!(rows.len(), 6864);
    for row in rows {
        let input = &inputs[row["id"].as_u64().unwrap() as usize];
        let mut op = operation(input["operation"].as_str().unwrap(), &input["bindings"]);
        let v3 = row["module"] == "oas_endpoints";
        let query = row["query"]["query"].as_str().unwrap();
        let base = input["query_base"].as_str().unwrap();
        let query = if base.is_empty() {
            query.to_owned()
        } else {
            format!("{base}&{query}")
        };
        let response = (|| {
            let strings = if v3 {
                super::super::transport::strings(&query)?
            } else {
                false
            };
            op.validate(v3, &query)?;
            let code = input["code"].as_u64().unwrap() as u16;
            Ok::<_, Response>(if code == 200 {
                finish(&op, input["body"].clone(), v3, strings)
            } else {
                finish_error(Response::json(code, input["body"].clone()), v3)
            })
        })()
        .unwrap_or_else(|r| r);
        let expected = &row["result"]["response"];
        assert_eq!(
            u64::from(response.status),
            expected["code"].as_u64().unwrap(),
            "{} {} {query}",
            row["id"],
            row["module"]
        );
        if row["result"]["kind"] == "stream_error" {
            assert!(response.body.is_empty(), "{row}");
        } else {
            assert_eq!(
                serde_json::from_slice::<Value>(&response.body).unwrap(),
                expected["body"],
                "{} {} {query}",
                row["id"],
                row["module"]
            );
        }
    }
}

#[test]
fn native_account_dispatcher_prefixes_errors_fields_pending_and_nonce_strategies_match() {
    let f = fixture();
    let rows = f["dispatcher"].as_array().unwrap();
    assert_eq!(rows.len(), 519);
    // The last seven cases bypass the native HTTP request schema. The actual
    // wrapper rejects their out-of-range/coercion inputs in the test above.
    for row in &rows[..512] {
        let input = &row["input"];
        let op = operation(input["operation"].as_str().unwrap(), &row["params"]);
        let scenario = input["scenario"].as_str().unwrap();
        let response = (|| {
            if scenario == "overload" {
                return Response::unavailable(true);
            }
            if scenario == "unstable" {
                return Response::unavailable(false);
            }
            let request = match op.request(false) {
                Ok(r) => r,
                Err(r) => return r,
            };
            let error = match scenario {
                "missing" => Some(account::Error::AccountNotFound),
                "no_state_trees" => Some(account::Error::NoStateTrees),
                "storage" => Some(account::Error::Backend(
                    ae_chain::repository::Error::Backend("injected".into()),
                )),
                "chain_too_short" if matches!(op, Operation::Height { .. }) => {
                    Some(account::Error::HeightNotAvailable)
                }
                "garbage_collected" if matches!(op, Operation::Height { .. }) => {
                    Some(account::Error::GarbageCollected)
                }
                _ => None,
            };
            if let Some(e) = error {
                return state_error(&op, e);
            }
            let account = Account::deserialize(
                request.account,
                &hex::decode(row["account"].as_str().unwrap()).unwrap(),
            )
            .unwrap();
            let reply = match request.kind {
                Kind::State(_) => {
                    if scenario == "ga_missing_code" {
                        return Response::internal();
                    }
                    let auth_fun = if let Some(auth) = account.authorization() {
                        let abi = if input["kind"] == "aevm" { 1 } else { 3 };
                        match account::authentication_name(
                            abi,
                            &auth.function,
                            &hex::decode(row["contract_code"].as_str().unwrap()).unwrap(),
                            Default::default(),
                        ) {
                            Ok(name) => Some(name),
                            Err(_) => return Response::internal(),
                        }
                    } else {
                        None
                    };
                    Reply::Account(account::Info { account, auth_fun })
                }
                Kind::Nonce(_) if account.authorization().is_some() => Reply::Nonce(0.into()),
                kind => {
                    let nonces = input.get("pool").cloned().unwrap_or(json!([6, 8]));
                    let transactions: Vec<_> = nonces
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|n| tx(Unsigned::from_decimal(&n.to_string()).unwrap()))
                        .collect();
                    if let Kind::Nonce(strategy) = kind {
                        let mut pool = Pool::new(Default::default(), Default::default(), 0);
                        for tx in transactions {
                            pool.force_push(tx, Default::default()).unwrap();
                        }
                        Reply::Nonce(
                            pool.account_nonce(
                                request.account,
                                account.nonce(),
                                strategy,
                                PoolLimits::default(),
                            )
                            .unwrap(),
                        )
                    } else {
                        Reply::Pending(transactions)
                    }
                }
            };
            match reply {
                // Dispatcher emits 410 before the v2 wrapper rejects it.
                Reply::StateError(e) => state_error(&op, e),
                reply => body(&op, reply)
                    .map(|body| Response::json(200, body))
                    .unwrap_or_else(|r| r),
            }
        })();
        assert_eq!(
            u64::from(response.status),
            row["code"].as_u64().unwrap(),
            "{input}"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap(),
            row["body"],
            "{input}"
        );
    }
}

#[test]
fn account_paths_groups_decode_priority_and_render_limits_are_explicit() {
    let config = Config {
        groups: std::collections::BTreeSet::from(["account".into()]),
        ..Default::default()
    };
    for v in ["v2", "v3"] {
        for suffix in [
            "",
            "/height/1",
            "/hash/bad",
            "/transactions/pending",
            "/next-nonce",
        ] {
            assert!(response::route(&format!("/{v}/accounts/bad{suffix}"), &config).is_some());
            assert!(
                response::route(&format!("/{v}/accounts/bad{suffix}"), &Config::default())
                    .is_none()
            );
        }
    }
    let op = Operation::Hash {
        key: b"bad".to_vec(),
        hash: b"bad".to_vec(),
    };
    assert_eq!(
        serde_json::from_slice::<Value>(&op.request(true).unwrap_err().body).unwrap()["reason"],
        "Invalid public key"
    );
    assert_eq!(
        Operation::Current(vec![b'a'; 4097])
            .request(true)
            .unwrap_err()
            .status,
        503
    );
    let mut op = Operation::Nonce {
        key: b"bad".to_vec(),
        strategy: Strategy::Max,
    };
    op.validate(true, "strategy=continuity&strategy=max")
        .unwrap();
    assert!(matches!(
        op,
        Operation::Nonce {
            strategy: Strategy::Continuity,
            ..
        }
    ));
    assert_eq!(
        number(&Unsigned::from_field_bytes(&vec![1; 8193]).unwrap())
            .unwrap_err()
            .status,
        503
    );
}
