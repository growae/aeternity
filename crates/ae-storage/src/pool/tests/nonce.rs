use super::*;

#[test]
fn worker_next_nonce_uses_selected_account_pool_maximum_and_live_gas_policy() {
    let data: J = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_durable_pool.json"
    ))
    .unwrap();
    for row in data["rows"].as_array().unwrap() {
        let temp = Temp::new();
        let (db, network) = fixture_database(&temp, row);
        let txs: Vec<_> = row["txs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                Arc::new(
                    ae_tx::storage::decode(
                        &hex::decode(r.as_str().unwrap()).unwrap(),
                        Default::default(),
                    )
                    .unwrap(),
                )
            })
            .collect();
        let account = txs[0].transaction().leaf().origin().unwrap();
        let mut cfg = config();
        let worker = worker::Worker::spawn(
            db,
            network,
            cfg.clone(),
            worker::Settings {
                origins_interval: None,
                ..Default::default()
            },
        )
        .unwrap();
        let client = worker.client();
        let base = Unsigned::from(row["nonce"].as_u64().unwrap());
        let expected = |maximum: &Unsigned| match row["kind"].as_str().unwrap() {
            "missing" => None,
            "generalized" => Some(0.into()),
            "basic" => Some(maximum.max(&base).add(&1.into())),
            _ => panic!(),
        };
        assert_eq!(
            client.next_nonce(account).unwrap().wait().unwrap(),
            expected(&base)
        );
        assert_eq!(
            client.minimum_miner_gas_price().unwrap().wait().unwrap(),
            1.into()
        );
        let mut maximum = base.clone();
        for tx in txs.iter().rev() {
            // Force insertion also exercises pooled entries whose account is
            // absent or generalized; neither may override selected state.
            client.force_push(tx.clone()).unwrap().wait().unwrap();
            maximum = maximum.max(tx.transaction().leaf().nonce().clone());
            assert_eq!(
                client.next_nonce(account).unwrap().wait().unwrap(),
                expected(&maximum)
            );
        }
        for tx in &txs {
            client.delete(tx.hash()).unwrap().wait().unwrap();
        }
        assert_eq!(
            client.next_nonce(account).unwrap().wait().unwrap(),
            expected(&base)
        );
        cfg.policy.minimum_miner_gas_price = 10_000_000_000u64.into();
        client
            .reconfigure(cfg.policy, cfg.failures)
            .unwrap()
            .wait()
            .unwrap();
        assert_eq!(
            client.minimum_miner_gas_price().unwrap().wait().unwrap(),
            10_000_000_000u64.into()
        );
        worker.shutdown().unwrap();
        assert!(matches!(
            client.next_nonce(account),
            Err(worker::Error::Stopped)
        ));
    }
}
