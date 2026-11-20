use super::*;
#[test]
fn native_nonce_policy_handles_missing_and_generalized_accounts() {
    let rows: serde_json::Value =
        serde_json::from_str(include_str!("../../tests/fixtures/erlang_pool.json")).unwrap();
    for (i, r) in rows["nonces"].as_array().unwrap().iter().enumerate() {
        let kind = r["kind"].as_str().unwrap();
        let account = match kind {
            "missing" | "unavailable" => None,
            _ => Some(AccountInfo {
                nonce: r["account_nonce"].as_u64().unwrap().into(),
                generalized: kind == "generalized",
            }),
        };
        let cfg = Config {
            nonce_offset: r["offset"].as_u64().unwrap().into(),
            ..Default::default()
        };
        let result = nonce_check(
            account.as_ref(),
            &r["nonce"].as_u64().unwrap().into(),
            r["created"].as_bool().unwrap(),
            &cfg,
        );
        let actual = match result {
            Ok(()) => "ok",
            Err(Error::NonceTooHigh) => "nonce_too_high",
            Err(Error::NonceTooLow) => "nonce_too_low",
            Err(Error::GeneralizedAccount) => "generalized_account_cant_sign_non_meta_tx",
            other => panic!("{other:?}"),
        };
        assert_eq!(actual, r["result"], "nonce {i}");
    }
}

#[test]
fn forced_reentry_preserves_both_queues_until_revisit_and_height_is_monotonic() {
    use serde_json::{Value as J, json};
    let data: J =
        serde_json::from_str(include_str!("../../tests/fixtures/erlang_pool.json")).unwrap();
    let snapshot = |p: &Pool, account: AccountPublicKey| {
        let hashes = |xs: Vec<Arc<Signed>>| {
            xs.iter()
                .map(|t| hex::encode(t.hash().0))
                .collect::<Vec<_>>()
        };
        let fields = |m: &BTreeMap<Key, Entry>| {
            m.iter()
                .map(|(k, e)| json!({"hash":hex::encode(k.hash.0),"failures":e.failures}))
                .collect::<Vec<_>>()
        };
        json!({"peek":hashes(p.peek(10000,None,None)),"account":hashes(p.peek(10000,Some(account),None)),
            "active":fields(&p.active),"visited":fields(&p.visited),
            "expiry":p.gc.iter().map(|(h,(ttl,_))|json!([hex::encode(h.0),ttl])).collect::<Vec<_>>(),
            "maximum":p.max_nonce(account).map(|n|n.to_u64().unwrap())})
    };
    for r in data["forced"].as_array().unwrap() {
        let t = Arc::new(Signed::decode(&hex::decode(r["tx"].as_str().unwrap()).unwrap()).unwrap());
        let origin = t.transaction().view().origin().unwrap();
        let mut p = Pool::new(
            Config {
                minimum_miner_gas_price: 1.into(),
                tx_ttl: 20,
                future_nonce_ttl: 3,
                ..Default::default()
            },
            Default::default(),
            10,
        );
        for s in r["steps"].as_array().unwrap() {
            match s["op"].as_str().unwrap() {
                "force" => {
                    p.force_push(t.clone(), Default::default()).unwrap();
                }
                "failure" => {
                    p.record_failure(t.hash(), Some(5)).unwrap();
                }
                "revisit" => p.revisit(),
                "candidate" => {
                    p.candidate(
                        CandidateRequest {
                            block: Hash256([42; 32]),
                            height: 10,
                            protocol: Protocol::try_from(r["protocol"].as_u64().unwrap() as u32)
                                .unwrap(),
                            max_gas: 1000000,
                            ignore: &BTreeSet::new(),
                            options: Default::default(),
                        },
                        |_| {
                            Ok(Some(AccountInfo {
                                nonce: 0.into(),
                                generalized: false,
                            }))
                        },
                        || 0,
                    )
                    .unwrap();
                }
                other => panic!("{other}"),
            }
            assert_eq!(
                snapshot(&p, origin),
                s["state"],
                "force {} {}",
                r["protocol"],
                s["op"]
            );
            assert_eq!(
                p.bytes,
                p.active
                    .values()
                    .chain(p.visited.values())
                    .map(|e| e.bytes)
                    .sum::<usize>()
            );
        }
    }
    for r in data["gc_heights"].as_array().unwrap() {
        let mut p = Pool::new(
            Default::default(),
            Default::default(),
            r["previous"].as_u64().unwrap(),
        );
        p.update_gc_height(r["local"].as_u64().unwrap(), r["target"].as_u64().unwrap());
        assert_eq!(p.gc_height(), r["result"]);
    }
}
