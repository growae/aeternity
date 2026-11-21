use ae_mempool::pool::{
    self, AccountInfo, CandidateRequest, Config, Key, Limits, Location, Pool, Reconcile,
    SelectOptions, Signed,
};
use ae_protocol::Protocol;
use ae_tx::SignedTransaction;
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use serde_json::{Value as J, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, LazyLock},
};
static DATA: LazyLock<J> =
    LazyLock::new(|| serde_json::from_str(include_str!("fixtures/erlang_pool.json")).unwrap());
fn tx(v: &J) -> Arc<Signed> {
    Arc::new(SignedTransaction::decode(&hex::decode(v.as_str().unwrap()).unwrap()).unwrap())
}
fn decimal(n: &Unsigned) -> String {
    num_bigint::BigUint::from_bytes_be(n.as_bytes()).to_string()
}
fn hashes(t: &[Arc<Signed>]) -> J {
    json!(
        t.iter()
            .map(|t| hex::encode(t.hash().0))
            .collect::<Vec<_>>()
    )
}
fn config() -> Config {
    Config {
        minimum_miner_gas_price: 1.into(),
        tx_ttl: 20,
        future_nonce_ttl: 3,
        ..Default::default()
    }
}
fn snapshot(pool: &Pool, account: AccountPublicKey) -> J {
    let all = pool.peek(10000, None, None);
    let mut active = vec![];
    let mut visited = vec![];
    let mut expiry = BTreeMap::new();
    for tx in &all {
        let hash = tx.hash();
        let inspect = pool.inspect(hash, Location::Pool).unwrap();
        let r = json!({"hash":hex::encode(hash.0),"failures":inspect.failures});
        if inspect.visited {
            visited.push(r)
        } else {
            active.push(r)
        }
        expiry.insert(hex::encode(hash.0), inspect.expires);
    }
    json!({"peek":hashes(&all),"account":hashes(&pool.peek(10000,Some(account),None)),"active":active,"visited":visited,"expiry":expiry.into_iter().map(|(h,t)|json!([h,t])).collect::<Vec<_>>(),"maximum":pool.max_nonce(account).map(|n|n.to_u64().unwrap())})
}
fn lookup_for(
    row: &J,
    txs: &[Arc<Signed>],
) -> (AccountPublicKey, BTreeMap<AccountPublicKey, AccountInfo>) {
    let first = txs[0].transaction().view().origin().unwrap();
    if let Some(entries) = row["accounts"].as_array() {
        let accounts = entries
            .iter()
            .map(|a| {
                let key = AccountPublicKey(
                    hex::decode(a[0].as_str().unwrap())
                        .unwrap()
                        .try_into()
                        .unwrap(),
                );
                let account = ae_state::accounts::Account::deserialize(
                    key,
                    &hex::decode(a[1].as_str().unwrap()).unwrap(),
                )
                .unwrap();
                (key, AccountInfo::from(account))
            })
            .collect();
        return (first, accounts);
    }
    let second = txs
        .iter()
        .map(|t| t.transaction().view().origin().unwrap())
        .find(|a| *a != first)
        .unwrap();
    let mut accounts = BTreeMap::from([(
        second,
        AccountInfo {
            nonce: 0.into(),
            generalized: false,
        },
    )]);
    if row["account_type"] != "missing" {
        accounts.insert(
            first,
            AccountInfo {
                nonce: row["nonce"].as_u64().unwrap().into(),
                generalized: row["account_type"] == "generalized",
            },
        );
    }
    (first, accounts)
}
#[test]
fn original_fee_and_nonce_selection_matches_after_each_pool_mutation() {
    for (i, row) in DATA["selection"].as_array().unwrap().iter().enumerate() {
        let txs: Vec<_> = row["txs"].as_array().unwrap().iter().map(tx).collect();
        let (account, accounts) = lookup_for(row, &txs);
        let mut p = Pool::new(config(), Limits::default(), 10);
        p.reconcile(
            Reconcile {
                key_top: false,
                requeue: &txs,
                confirmed: &[],
            },
            |k| Ok(accounts.get(&k).cloned()),
            Default::default(),
        )
        .unwrap();
        assert_eq!(snapshot(&p, account), row["initial"], "initial {i}");
        let mode = row["mode"].as_str().unwrap();
        let opts = match mode {
            "none" => SelectOptions::default(),
            "deadline" => SelectOptions {
                deadline: Some(0),
                ..Default::default()
            },
            "packed" => SelectOptions {
                deadline: Some(0),
                packed: true,
                ..Default::default()
            },
            "ceiling" => SelectOptions {
                ceiling: Some(0),
                ..Default::default()
            },
            _ => panic!(),
        };
        let mut ignore = BTreeSet::new();
        for field in ["first", "second", "revisited"] {
            if field == "revisited" {
                p.revisit();
                ignore.clear();
            }
            let c = p
                .candidate(
                    CandidateRequest {
                        block: Hash256([42; 32]),
                        height: 10,
                        protocol: Protocol::try_from(row["protocol"].as_u64().unwrap() as u32)
                            .unwrap(),
                        max_gas: row["gas"].as_u64().unwrap(),
                        ignore: &ignore,
                        options: if field == "revisited" {
                            SelectOptions::default()
                        } else {
                            opts
                        },
                    },
                    |k| Ok(accounts.get(&k).cloned()),
                    || 1,
                )
                .unwrap();
            assert_eq!(
                hashes(&c.transactions),
                row[field]["hashes"],
                "selected {i}/{field}"
            );
            assert_eq!(c.expired, row[field]["expired"], "expired {i}/{field}");
            assert_eq!(
                snapshot(&p, account),
                row[field]["state"],
                "state {i}/{field}"
            );
            ignore = c.transactions.iter().map(|t| t.hash()).collect();
        }
    }
}
#[test]
fn all_transaction_pool_keys_match_original_deep_fee_and_outer_envelope() {
    for (i, r) in DATA["keys"].as_array().unwrap().iter().enumerate() {
        let tx = tx(&r["tx"]);
        if r.get("error").is_some() {
            assert!(matches!(Key::of(&tx), Err(pool::Error::NoOrigin)));
            continue;
        }
        let k = Key::of(&tx).unwrap();
        assert_eq!(decimal(&k.fee.0), r["fee"], "fee {i}");
        assert_eq!(decimal(&k.gas_price.0), r["price"]);
        assert_eq!(
            k.origin.map_or(String::new(), |p| hex::encode(p.0)),
            r["origin"]
        );
        assert_eq!(decimal(&k.nonce), r["nonce"]);
        assert_eq!(hex::encode(k.hash.0), r["hash"]);
    }
}
#[test]
fn resource_and_lookup_failures_leave_selection_and_reconciliation_retryable() {
    let row = &DATA["selection"][2];
    let txs: Vec<_> = row["txs"].as_array().unwrap().iter().map(tx).collect();
    let (account, accounts) = lookup_for(row, &txs);
    for work in [0, 1, 5, 20] {
        let mut p = Pool::new(
            config(),
            Limits {
                work,
                ..Default::default()
            },
            10,
        );
        let before = snapshot(&p, account);
        let result = p.reconcile(
            Reconcile {
                key_top: false,
                requeue: &txs,
                confirmed: &[],
            },
            |k| Ok(accounts.get(&k).cloned()),
            Default::default(),
        );
        if result.is_err() {
            assert_eq!(snapshot(&p, account), before);
            continue;
        }
        let before = snapshot(&p, account);
        let result = p.candidate(
            CandidateRequest {
                block: Hash256([0; 32]),
                height: 10,
                protocol: Protocol::Roma,
                max_gas: 1000000,
                ignore: &BTreeSet::new(),
                options: Default::default(),
            },
            |k| Ok(accounts.get(&k).cloned()),
            || 0,
        );
        assert!(matches!(result, Err(pool::Error::ResourceLimit)));
        assert_eq!(snapshot(&p, account), before);
    }
    let mut p = Pool::new(config(), Default::default(), 10);
    p.reconcile(
        Reconcile {
            key_top: false,
            requeue: &txs,
            confirmed: &[],
        },
        |k| Ok(accounts.get(&k).cloned()),
        Default::default(),
    )
    .unwrap();
    let before = snapshot(&p, account);
    assert!(
        p.candidate(
            CandidateRequest {
                block: Hash256([0; 32]),
                height: 10,
                protocol: Protocol::Roma,
                max_gas: 1000000,
                ignore: &BTreeSet::new(),
                options: Default::default(),
            },
            |_| Err(pool::Error::ResourceLimit),
            || 0
        )
        .is_err()
    );
    assert_eq!(snapshot(&p, account), before);
    p.candidate(
        CandidateRequest {
            block: Hash256([0; 32]),
            height: 10,
            protocol: Protocol::Roma,
            max_gas: 1000000,
            ignore: &BTreeSet::new(),
            options: Default::default(),
        },
        |k| Ok(accounts.get(&k).cloned()),
        || 0,
    )
    .unwrap();
}

fn state(row: &J) -> ae_state::Trees {
    let mut trees = ae_state::Trees {
        accounts: Default::default(),
        calls: Default::default(),
        channels: ae_state::channels::Channels::new(Default::default()),
        contracts: ae_state::contracts::Contracts::new(Default::default()),
        names: Default::default(),
        oracles: Default::default(),
    };
    for a in row["accounts"].as_array().unwrap() {
        let key = AccountPublicKey(
            hex::decode(a[0].as_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap(),
        );
        trees
            .accounts
            .enter(
                &ae_state::accounts::Account::deserialize(
                    key,
                    &hex::decode(a[1].as_str().unwrap()).unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    for n in row["names"].as_array().unwrap() {
        let key = hex::decode(n[0].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        trees
            .names
            .enter_name(
                &ae_state::names::Name::deserialize(
                    key,
                    &hex::decode(n[1].as_str().unwrap()).unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    trees
}
fn error_name(e: &pool::Error) -> &str {
    use pool::Error::*;
    match e {
        NonceTooLow => "nonce_too_low",
        NonceTooHigh => "nonce_too_high",
        GeneralizedAccount => "generalized_account_cant_sign_non_meta_tx",
        TooMuchAuthGas => "too_much_gas_for_auth_function",
        InvalidAtProtocol => "invalid_at_protocol",
        Signature(ae_execution::transactions::SignatureError::CheckFailed) => {
            "signature_check_failed"
        }
        TooLowFee => "too_low_fee",
        TooLowGasPrice => "too_low_gas_price",
        TooLowMinerGasPrice => "too_low_gas_price_for_miner",
        TtlExpired => "ttl_expired",
        NotPayable => "account_is_not_payable",
        UnresolvedRecipient => "could_not_resolve_recipient",
        AlreadyKnown => "already_known",
        AlreadyAccepted => "already_accepted",
        _ => panic!("unexpected error {e:?}"),
    }
}
#[test]
fn admission_matches_original_signatures_nonces_fees_payability_and_expiry() {
    use pool::{Admission, Event, PushOptions, PushOutcome};
    let network = ae_protocol::NetworkProfile::mainnet();
    for (i, row) in DATA["admission"].as_array().unwrap().iter().enumerate() {
        let t = tx(&row["tx"]);
        let trees = state(row);
        let env = Admission {
            trees: &trees,
            network: &network,
            protocol: Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
            consensus: ae_protocol::ConsensusType::Pow,
            height: 10,
            record: Default::default(),
        };
        let event = if row["event"] == "tx_created" {
            Event::Created
        } else {
            Event::Received
        };
        let mut p = Pool::new(config(), Default::default(), 10);
        let result = p.push(
            t.clone(),
            PushOptions {
                event,
                location: Location::Unknown,
                gossip_cached: false,
            },
            &env,
        );
        let name = match &result {
            Ok(_) => "ok",
            Err(e) => error_name(e),
        };
        assert_eq!(name, row["result"], "admission {i}: {result:?}");
        if name == "ok" {
            assert_eq!(
                result.unwrap(),
                PushOutcome::Accepted {
                    hash: t.hash(),
                    inserted: true,
                    event
                }
            );
            let ttl = t
                .transaction()
                .view()
                .ttl()
                .map(|n| n.to_u64().unwrap())
                .unwrap_or(u64::MAX);
            assert_eq!(
                p.inspect(t.hash(), Location::Pool).unwrap().expires,
                i128::from(ttl.min(10 + row["residency"].as_u64().unwrap())),
                "expiry {i}"
            );
            assert_eq!(p.len(), 1);
        } else {
            assert!(p.is_empty());
            assert_eq!(p.retained_record_bytes(), 0);
        }
    }
}
#[test]
fn failure_limits_match_original_schema_defaults_and_overrides() {
    use pool::FailureSettings;
    let defaults = FailureSettings::default();
    let disabled = FailureSettings {
        enabled: false,
        ..defaults.clone()
    };
    let overrides = FailureSettings {
        enabled: true,
        common: [("fallback".into(), 17), ("insufficient_funds".into(), 3)].into(),
        transactions: [(
            "spend_tx".into(),
            [("fallback".into(), 7), ("crash".into(), 2)].into(),
        )]
        .into(),
    };
    for (i, row) in DATA["failures"].as_array().unwrap().iter().enumerate() {
        let settings = match row["settings"].as_str().unwrap() {
            "defaults" => &defaults,
            "disabled" => &disabled,
            "overrides" => &overrides,
            _ => panic!(),
        };
        let reason = settings.reason(
            row["reason"].as_str().unwrap(),
            row["tuple"].as_bool().unwrap(),
        );
        assert_eq!(
            settings.limit(row["kind"].as_str().unwrap(), reason),
            row["limit"].as_u64(),
            "limit {i}"
        );
    }
}

#[test]
fn mutations_reorgs_and_garbage_collection_match_original_after_every_step() {
    for (i, row) in DATA["maintenance"].as_array().unwrap().iter().enumerate() {
        let txs: Vec<_> = row["txs"].as_array().unwrap().iter().map(tx).collect();
        let (account, mut accounts) = lookup_for(row, &txs);
        let mut p = Pool::new(config(), Default::default(), 10);
        p.reconcile(
            Reconcile {
                key_top: false,
                requeue: &txs,
                confirmed: &[],
            },
            |k| Ok(accounts.get(&k).cloned()),
            Default::default(),
        )
        .unwrap();
        assert_eq!(snapshot(&p, account), row["initial"], "initial {i}");
        let mut accepted = BTreeSet::new();
        for (j, step) in row["steps"].as_array().unwrap().iter().enumerate() {
            let indices = |field: &str| -> Vec<Arc<Signed>> {
                step[field]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| txs[v.as_u64().unwrap() as usize - 1].clone())
                    .collect()
            };
            if step.get("nonce").is_some() {
                if step["account_type"] == "missing" {
                    accounts.remove(&account);
                } else {
                    accounts.insert(
                        account,
                        AccountInfo {
                            nonce: step["nonce"].as_u64().unwrap().into(),
                            generalized: step["account_type"] == "generalized",
                        },
                    );
                }
            }
            match step["op"].as_str().unwrap() {
                "candidate" => {
                    let c = p
                        .candidate(
                            CandidateRequest {
                                block: Hash256([42; 32]),
                                height: 10,
                                protocol: Protocol::try_from(
                                    row["protocol"].as_u64().unwrap() as u32
                                )
                                .unwrap(),
                                max_gas: 1000000,
                                ignore: &BTreeSet::new(),
                                options: Default::default(),
                            },
                            |k| Ok(accounts.get(&k).cloned()),
                            || 0,
                        )
                        .unwrap();
                    assert_eq!(hashes(&c.transactions), step["hashes"], "selection {i}/{j}");
                }
                "failure" => {
                    let settings = pool::FailureSettings {
                        enabled: !step["disabled"].as_bool().unwrap_or(false),
                        ..Default::default()
                    };
                    let limit = settings.limit("spend_tx", step["reason"].as_str().unwrap());
                    for tx in indices("indices") {
                        p.record_failure(tx.hash(), limit).unwrap();
                    }
                }
                "reconcile" => {
                    let requeue = indices("requeue");
                    let confirmed = indices("confirmed");
                    for t in &requeue {
                        accepted.remove(&t.hash());
                    }
                    for t in &confirmed {
                        accepted.insert(t.hash());
                    }
                    p.reconcile(
                        Reconcile {
                            key_top: step["key_top"].as_bool().unwrap(),
                            requeue: &requeue,
                            confirmed: &confirmed,
                        },
                        |k| Ok(accounts.get(&k).cloned()),
                        Default::default(),
                    )
                    .unwrap();
                }
                "origin_gc" => {
                    p.collect_origins(
                        |k| Ok(accounts.get(&k).cloned()),
                        |h| Ok(!accepted.contains(&h)),
                    )
                    .unwrap();
                }
                "expiry" => {
                    for t in indices("guarded") {
                        accepted.insert(t.hash());
                    }
                    p.collect_expired(step["height"].as_u64().unwrap(), usize::MAX, |h| {
                        Ok(!accepted.contains(&h))
                    })
                    .unwrap();
                }
                other => panic!("unknown operation {other}"),
            }
            assert_eq!(
                snapshot(&p, account),
                step["state"],
                "state {i}/{j} {}",
                step["op"]
            );
        }
    }
}

#[test]
fn admission_cache_reentry_force_and_reconfiguration_preserve_pool_semantics() {
    use pool::{Admission, Event, PushOptions, PushOutcome};
    let row = DATA["admission"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["result"] == "ok"
                && r["protocol"] == 5
                && r["account_type"] == "basic"
                && r["residency"] == 3
                && tx(&r["tx"]).transaction().view().ttl().is_none()
        })
        .unwrap();
    let t = tx(&row["tx"]);
    let account = t.transaction().view().origin().unwrap();
    let bad = Arc::new(SignedTransaction::new(t.transaction().clone(), vec![]));
    let trees = state(row);
    let network = ae_protocol::NetworkProfile::mainnet();
    let env = Admission {
        trees: &trees,
        network: &network,
        protocol: Protocol::Iris,
        consensus: ae_protocol::ConsensusType::Pow,
        height: 10,
        record: Default::default(),
    };
    let options = PushOptions {
        event: Event::Received,
        location: Location::Unknown,
        gossip_cached: false,
    };
    let mut p = Pool::new(config(), Default::default(), 10);
    assert_eq!(
        p.push(
            bad.clone(),
            PushOptions {
                gossip_cached: true,
                ..options
            },
            &env
        )
        .unwrap(),
        PushOutcome::Cached
    );
    assert!(p.is_empty());
    assert!(matches!(
        p.push(
            bad.clone(),
            PushOptions {
                gossip_cached: true,
                event: Event::Created,
                ..options
            },
            &env
        ),
        Err(pool::Error::Signature(_))
    ));
    for (location, expected) in [
        (Location::Deleted, pool::Error::AlreadyKnown),
        (Location::Pool, pool::Error::AlreadyKnown),
        (
            Location::Block(Hash256([9; 32])),
            pool::Error::AlreadyAccepted,
        ),
    ] {
        assert_eq!(
            p.push(
                bad.clone(),
                PushOptions {
                    location,
                    ..options
                },
                &env
            ),
            Err(expected)
        );
        assert!(p.is_empty());
    }
    p.reconfigure(Config {
        allow_reentry: true,
        ..config()
    });
    p.push(
        bad.clone(),
        PushOptions {
            location: Location::Deleted,
            ..options
        },
        &env,
    )
    .unwrap();
    assert_eq!(p.inspect(bad.hash(), Location::Pool).unwrap().expires, 13);
    p.record_failure(bad.hash(), None).unwrap();
    let before = snapshot(&p, account);
    p.force_push(bad.clone(), Default::default()).unwrap();
    assert_eq!(snapshot(&p, account), before); // Existing active entry keeps attempts and expiry.
    assert_eq!(
        p.inspect(bad.hash(), Location::Block(Hash256([9; 32]))),
        Err(pool::Error::AlreadyAccepted)
    );
    assert_eq!(
        p.delete(bad.hash(), Location::Block(Hash256([9; 32]))),
        Err(pool::Error::AlreadyAccepted)
    );
    p.delete(bad.hash(), Location::Pool).unwrap();
    p.force_push(t.clone(), Default::default()).unwrap();
    assert_eq!(p.inspect(t.hash(), Location::Pool).unwrap().expires, 30);
    p.reconfigure(Config {
        tx_ttl: 1,
        future_nonce_ttl: 10,
        ..config()
    });
    p.update_gc_height(9, 12);
    p.update_gc_height(1, 1);
    assert_eq!(p.gc_height(), 12);
    assert_eq!(p.inspect(t.hash(), Location::Pool).unwrap().expires, 30);
    p.delete(t.hash(), Location::Pool).unwrap();
    p.push(t.clone(), options, &env).unwrap();
    assert_eq!(p.inspect(t.hash(), Location::Pool).unwrap().expires, 13);
}
#[test]
fn admission_and_garbage_collection_failures_are_atomic_and_retryable() {
    use pool::{Admission, Event, PushOptions};
    let row = DATA["admission"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["result"] == "ok" && r["account_type"] == "basic" && r["protocol"] == 5)
        .unwrap();
    let t = tx(&row["tx"]);
    let trees = state(row);
    let network = ae_protocol::NetworkProfile::mainnet();
    let env = Admission {
        trees: &trees,
        network: &network,
        protocol: Protocol::Iris,
        consensus: ae_protocol::ConsensusType::Pow,
        height: 10,
        record: Default::default(),
    };
    let options = PushOptions {
        event: Event::Received,
        location: Location::Unknown,
        gossip_cached: false,
    };
    for limits in [
        Limits {
            entries: 0,
            ..Default::default()
        },
        Limits {
            retained_record_bytes: 0,
            ..Default::default()
        },
        Limits {
            record_bytes: 0,
            ..Default::default()
        },
        Limits {
            signatures: ae_execution::transactions::SignatureLimits {
                max_encoded_bytes: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    ] {
        let mut p = Pool::new(config(), limits, 10);
        assert!(
            p.push(t.clone(), options, &env)
                .unwrap_err()
                .is_operational()
        );
        assert!(p.is_empty());
        assert_eq!(p.retained_record_bytes(), 0);
    }
    let mut p = Pool::new(config(), Default::default(), u64::MAX);
    assert_eq!(
        p.force_push(t, Default::default()),
        Err(pool::Error::ResourceLimit)
    );
    assert!(p.is_empty());
    let row = &DATA["selection"][2];
    let txs: Vec<_> = row["txs"].as_array().unwrap().iter().map(tx).collect();
    let (account, accounts) = lookup_for(row, &txs);
    let mut p = Pool::new(config(), Default::default(), 10);
    p.reconcile(
        Reconcile {
            key_top: false,
            requeue: &txs,
            confirmed: &[],
        },
        |k| Ok(accounts.get(&k).cloned()),
        Default::default(),
    )
    .unwrap();
    let before = snapshot(&p, account);
    let bytes = p.retained_record_bytes();
    let mut n = 0;
    assert_eq!(
        p.collect_expired(30, usize::MAX, |_| {
            n += 1;
            if n == 2 {
                Err(pool::Error::ResourceLimit)
            } else {
                Ok(true)
            }
        }),
        Err(pool::Error::ResourceLimit)
    );
    assert_eq!(snapshot(&p, account), before);
    assert_eq!(p.retained_record_bytes(), bytes);
    p.reconcile(
        Reconcile {
            key_top: false,
            requeue: &[],
            confirmed: &[txs[1].clone()],
        },
        |k| Ok(accounts.get(&k).cloned()),
        Default::default(),
    )
    .unwrap();
    let before = snapshot(&p, account);
    let mut n = 0;
    let updated = AccountInfo {
        nonce: 9.into(),
        generalized: false,
    };
    assert_eq!(
        p.collect_origins(
            |_| Ok(Some(updated.clone())),
            |_| {
                n += 1;
                if n == 2 {
                    Err(pool::Error::ResourceLimit)
                } else {
                    Ok(true)
                }
            }
        ),
        Err(pool::Error::ResourceLimit)
    );
    assert_eq!(snapshot(&p, account), before);
    assert!(
        !p.collect_origins(|_| Ok(Some(updated.clone())), |_| Ok(true))
            .unwrap()
            .is_empty()
    );
    p.collect_expired(30, usize::MAX, |_| Ok(true)).unwrap();
    assert!(p.is_empty());
    assert_eq!(p.retained_record_bytes(), 0);
}
