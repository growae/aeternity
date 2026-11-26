use super::*;

#[test]
fn settings_reads_follow_committed_reconfiguration_and_recovery_without_writes() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = spawn(db.clone());
    let client = worker.client();
    until(&client, |i| i.background_runs > 0 && !i.in_flight);
    client.force_push(tx()).unwrap().wait().unwrap();
    let original = client.policy_settings(1024).unwrap().wait().unwrap();
    let revision = db.pool_view().unwrap().revision;
    assert_eq!(
        client.policy_settings(1024).unwrap().wait().unwrap().value,
        original.value
    );
    assert_eq!(db.pool_view().unwrap().revision, revision);
    assert!(matches!(
        client.policy_settings(1).unwrap().wait(),
        Err(Error::Service(crate::pool::Failure::Policy(
            policy::Error::ResourceLimit
        )))
    ));
    let mut updated = config();
    updated.policy.minimum_miner_gas_price = 7.into();
    updated.policy.maximum_auth_gas = 89.into();
    updated.policy.nonce_offset = 13.into();
    updated.policy.tx_ttl = 123;
    client
        .reconfigure(updated.policy.clone(), updated.failures.clone())
        .unwrap()
        .wait()
        .unwrap();
    let read = client.policy_settings(1024).unwrap().wait().unwrap();
    assert_eq!(
        read.value,
        crate::pool::PolicySettings {
            minimum_miner_gas_price: 7.into(),
            maximum_auth_gas: 89.into(),
            nonce_offset: 13.into(),
            tx_ttl: 123
        }
    );
    // Failed recovery must not publish an unapplied policy snapshot.
    updated.limits.record_bytes = 1;
    updated.policy.tx_ttl = 456;
    assert!(client.recover(updated).unwrap().wait().is_err());
    assert_eq!(
        client.policy_settings(1024).unwrap().wait().unwrap().value,
        read.value
    );
    client.recover(config()).unwrap().wait().unwrap();
    assert_eq!(
        client.policy_settings(1024).unwrap().wait().unwrap().value,
        original.value
    );
    drop((original, read));
    assert_eq!(client.account_query_usage(), (0, 0));
    worker.shutdown().unwrap();
}

#[test]
fn settings_and_account_reads_share_retained_credits_across_worker_retirement() {
    let temp = Temp::new();
    let db = database(&temp);
    let worker = Worker::spawn(
        db.clone(),
        NetworkProfile::mainnet(),
        config(),
        Settings {
            requests: 2,
            ..settings()
        },
    )
    .unwrap();
    let client = worker.client();
    until(&client, |i| i.background_runs > 0 && !i.in_flight);
    let held = client.policy_settings(1024).unwrap().wait().unwrap();
    let owner = tx().transaction().view().origin().unwrap();
    let account = client
        .pending_account(owner, Default::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(client.policy_settings(1024).err(), Some(Error::Saturated));
    drop(account);
    let (release, gate) = gate(&client);
    let queued = client.policy_settings(1024).unwrap();
    drop(queued);
    assert_eq!(client.account_query_usage().0, 2);
    worker.request_shutdown();
    assert_eq!(client.policy_settings(1024).err(), Some(Error::Saturated));
    release.send(()).unwrap();
    gate.wait().unwrap();
    worker.shutdown().unwrap();
    assert_eq!(client.account_query_usage().0, 1);
    assert_eq!(client.policy_settings(1024).err(), Some(Error::Stopped));
    let replacement = spawn(db);
    assert!(!client.same_worker(&replacement.client()));
    assert_eq!(replacement.client().account_query_usage(), (0, 0));
    drop(held);
    assert_eq!(client.account_query_usage(), (0, 0));
    replacement.shutdown().unwrap();
}
