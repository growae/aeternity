use super::*;
use serde_json::Value;
fn fixture() -> Value {
    serde_json::from_str(include_str!("../../../tests/fixtures/erlang_pool_gc.json")).unwrap()
}
fn transaction() -> Arc<Signed> {
    let v = fixture();
    let row = v["origins"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["count"] == 1)
        .unwrap();
    Arc::new(Signed::decode(&hex::decode(row["txs"][0].as_str().unwrap()).unwrap()).unwrap())
}
#[test]
fn gc_ttl_minimum_signed_adjustment_and_height_gate_match_native() {
    let tx = transaction();
    for r in fixture()["ttl"].as_array().unwrap() {
        let initial = r["initial"].as_u64().unwrap();
        let mut pool = Pool::new(
            Config {
                tx_ttl: initial,
                ..Default::default()
            },
            Default::default(),
            0,
        );
        pool.force_push(tx.clone(), Default::default()).unwrap();
        let result = pool
            .shorten_ttl(tx.hash(), r["offered"].as_i64().unwrap().into())
            .unwrap();
        assert_eq!(result, i128::from(r["minimum"].as_i64().unwrap()));
        pool.adjust_ttl(r["difference"].as_i64().unwrap().into())
            .unwrap();
        assert_eq!(
            pool.ttl(tx.hash()).unwrap(),
            i128::from(r["adjusted"].as_i64().unwrap())
        );
        assert_eq!(pool.ttl(Hash256([255; 32])), Err(Error::NotFound));
        let before = pool.ttl(tx.hash()).unwrap();
        // At least one positive entry overflows when subtracting MIN.
        if before >= 0 {
            assert!(pool.adjust_ttl(i128::MIN).is_err());
            assert_eq!(pool.ttl(tx.hash()).unwrap(), before);
        }
    }
    for row in fixture()["gc"].as_array().unwrap() {
        if row["synchronized"] == true && row["gate"].is_null() {
            continue;
        }
        let mut p = Pool::new(
            Config {
                tx_ttl: 5,
                ..Default::default()
            },
            Default::default(),
            0,
        );
        p.force_push(tx.clone(), Default::default()).unwrap();
        p.update_gc_height(0, row["gate"].as_u64().unwrap_or(0));
        let height = row["height"].as_u64().unwrap();
        let gone = if row["synchronized"] == true {
            p.collect_expired(height, usize::MAX, |_| Ok(true))
        } else {
            p.collect_at(height, usize::MAX, |_| Ok(true))
        }
        .unwrap();
        assert_eq!(gone.len() as u64, row["result"].as_u64().unwrap());
        assert_eq!(p.len() as u64, row["remaining"].as_u64().unwrap());
    }
}
#[test]
fn gc_origin_passes_match_native_caps_stale_nonce_and_failed_deletes() {
    for row in fixture()["origins"].as_array().unwrap() {
        let mut p = Pool::new(Default::default(), Default::default(), 10);
        let mut origin = None;
        let mut guarded = BTreeSet::new();
        for (i, t) in row["txs"].as_array().unwrap().iter().enumerate() {
            let t = Arc::new(Signed::decode(&hex::decode(t.as_str().unwrap()).unwrap()).unwrap());
            origin = Some(t.transaction().view().origin().unwrap());
            if row["guarded"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!(i + 1))
            {
                guarded.insert(t.hash());
            }
            p.force_push(t, Default::default()).unwrap();
        }
        let key = origin.unwrap_or(AccountPublicKey([1; 32]));
        p.note_origin(Some(key), row["count"].as_u64().unwrap().into());
        if row["account_kind"] != "unavailable" {
            p.collect_origins(
                |_| {
                    Ok(if row["account_kind"] == "missing" {
                        None
                    } else {
                        Some(AccountInfo {
                            nonce: row["account_nonce"].as_u64().unwrap().into(),
                            generalized: false,
                        })
                    })
                },
                |h| Ok(!guarded.contains(&h)),
            )
            .unwrap();
        }
        let nonces: Vec<_> = p
            .peek(100, Some(key), None)
            .iter()
            .map(|t| t.transaction().view().nonce().to_u64().unwrap())
            .collect();
        assert_eq!(serde_json::json!(nonces), row["remaining_nonces"], "{row}");
        assert_eq!(p.origins_len() as u64, row["cache_size"].as_u64().unwrap());
    }
    let v = fixture();
    let mut p = Pool::new(Default::default(), Default::default(), 0);
    for n in 0..v["capacity"]["limit"].as_u64().unwrap() {
        let mut k = [0; 32];
        k[..8].copy_from_slice(&n.to_be_bytes());
        p.note_origin(Some(AccountPublicKey(k)), 1.into());
    }
    p.note_origin(Some(AccountPublicKey([0; 32])), 100.into());
    assert_eq!(
        p.origins[&Some(AccountPublicKey([0; 32]))],
        v["capacity"]["existing_after_full"]
            .as_u64()
            .unwrap()
            .into()
    );
    p.origins.clear();
    for n in 0..101 {
        p.note_origin(Some(AccountPublicKey([n; 32])), 1.into());
    }
    p.collect_origins(
        |_| panic!("empty pool must not look up accounts"),
        |_| panic!(),
    )
    .unwrap();
    assert_eq!(
        p.origins_len() as u64,
        v["origin_cap"]["after_count"].as_u64().unwrap()
    );
}
