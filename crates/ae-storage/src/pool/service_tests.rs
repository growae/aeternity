use super::*;
use crate::pool::tests::{Temp, config, database, tx};

#[test]
fn failure_dispatch_uses_original_transaction_type_names() {
    let data: serde_json::Value = serde_json::from_str(include_str!(
        "../../../ae-mempool/tests/fixtures/erlang_pool.json"
    ))
    .unwrap();
    let mut kinds = BTreeSet::new();
    for row in data["keys"].as_array().unwrap() {
        let tx = Signed::decode(&hex::decode(row["tx"].as_str().unwrap()).unwrap()).unwrap();
        let name = kind_name(tx.transaction().view().kind());
        kinds.insert(name);
        assert_eq!(name, row["kind"].as_str().unwrap());
    }
    assert_eq!(kinds.len(), 27);
}

#[test]
fn admission_faults_do_not_publish_memory_and_ambiguous_writes_require_recovery() {
    let temp = Temp::new();
    let db = database(&temp);
    let events = crate::pool::tests::metrics::Events::default();
    let _binding = events.bind(&db);
    let mut service = Service::start(db.clone(), NetworkProfile::mainnet(), config()).unwrap();
    let t = tx();
    let before = service.inspection();
    for backend in [false, true] {
        for fail_at in 0..4 {
            let mut call = 0;
            let error = if backend {
                Error::Backend("injected pool WAL failure".into())
            } else {
                Error::ResourceLimit
            };
            let result = service.submit(t.clone(), Some((policy::Event::Created, false)), || {
                let now = call;
                call += 1;
                if now == fail_at {
                    Err(error.clone())
                } else {
                    Ok(())
                }
            });
            assert_eq!(result, Err(Failure::Repository(error)));
            assert_eq!(
                events.take(),
                vec![(ae_chain::instrumentation::Counter::PoolPush, 1)]
            );
            assert_eq!(service.inspection(), before);
            assert_eq!(
                db.pool_view().unwrap().location(t.hash()).unwrap(),
                Location::Unknown
            );
            if backend {
                assert_eq!(service.refresh(), Err(Failure::RestartRequired));
                service.recover(config()).unwrap();
            } else {
                service.refresh().unwrap();
            }
        }
    }
    // Model a durable write whose acknowledgement never reached the owner.
    let stored = db.pool_view().unwrap();
    let mut writes = repo::Writes::new();
    stored.retain(&t, &mut writes).unwrap();
    db.commit_pool(&stored, writes, usize::MAX, || Ok(()))
        .unwrap();
    assert_eq!(service.refresh(), Err(Failure::RestartRequired));
    service.recover(config()).unwrap();
    assert_eq!(service.peek(1, None, None).unwrap(), vec![t]);
    assert!(events.take().is_empty());
}

#[test]
fn forced_admission_is_counted_before_storage_reads_and_normal_admission_is_not() {
    use ae_chain::instrumentation::Counter;
    let temp = Temp::new();
    let db = database(&temp);
    let events = crate::pool::tests::metrics::Events::default();
    let _binding = events.bind(&db);
    let mut service = Service::start(db, NetworkProfile::mainnet(), config()).unwrap();
    service.uncertain = true;
    assert_eq!(
        service.push(tx(), policy::Event::Created, false),
        Err(Failure::RestartRequired)
    );
    assert!(events.take().is_empty());
    assert_eq!(service.force_push(tx()), Err(Failure::RestartRequired));
    assert_eq!(events.take(), vec![(Counter::PoolPush, 1)]);
}
