use super::*;
use serde_json::{Value as Json, json};
use std::{sync::mpsc, time::Duration};

fn name() -> Name {
    ["ae", "epoch", "gauge"].map(Part::atom).to_vec()
}
fn value(n: i64) -> Value {
    Value::Integer(n.into())
}
fn point() -> Part {
    Part::atom("value")
}
fn read(client: &Client) -> i64 {
    let Reading::Values(v) = client.get_value(&name(), &Points::One(point())).unwrap() else {
        panic!("disabled")
    };
    let Value::Integer(n) = &v[0].1 else {
        panic!("integer")
    };
    n.try_into().unwrap()
}
fn wait(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !predicate() {
        assert!(Instant::now() < deadline, "worker progress deadline");
        thread::sleep(Duration::from_millis(1));
    }
}
fn setup(limits: Limits) -> (Arc<Store>, Owner, Client) {
    let store = Arc::new(Store::new(limits).unwrap());
    let owner = Owner::spawn(store.clone()).unwrap();
    let client = owner.client();
    client
        .create(&name(), Kind::Gauge, true)
        .unwrap()
        .blocking_wait()
        .unwrap();
    (store, owner, client)
}
struct Block {
    release: Option<mpsc::Sender<()>>,
    ticket: Option<Ticket<()>>,
}
impl Block {
    fn release(mut self) {
        self.release.take().unwrap().send(()).unwrap();
        self.ticket.take().unwrap().blocking_wait().unwrap();
    }
}
impl Drop for Block {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}
fn block(client: &Client, manager: Manager) -> Block {
    let (entered, started) = mpsc::sync_channel(0);
    let (release, released) = mpsc::channel();
    let ticket = client
        .call(manager, 128, move |_| {
            entered.send(()).unwrap();
            released.recv().unwrap();
            Ok(())
        })
        .unwrap();
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    Block {
        release: Some(release),
        ticket: Some(ticket),
    }
}
fn cached(v: Option<Value>) -> Json {
    match v {
        None => json!({"atom":"not_found"}),
        Some(Value::Integer(n)) => json!({"tuple":[{"atom":"ok"},{"integer":n.to_string()}]}),
        _ => panic!("numeric cache fixture"),
    }
}
fn alias(client: &Client, alias: &Alias) -> Json {
    match client.alias_value(alias) {
        Ok(Reading::Values(v)) => cached(Some(v[0].1.clone())),
        Err(Error::NotFound) => json!({"tuple":[{"atom":"error"},{"atom":"not_found"}]}),
        other => panic!("unexpected alias {other:?}"),
    }
}
fn generation(client: &Client, manager: Manager) -> u64 {
    client
        .info()
        .managers
        .iter()
        .find(|i| i.manager == manager)
        .unwrap()
        .generation
}

#[test]
fn original_cache_values_expiry_tokens_aliases_and_manager_replacement() {
    let native: Json = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_metrics_registry_cache.json"
    ))
    .unwrap();
    let (store, owner, client) = setup(Limits::default());
    let alias_name = Alias::Atom("gauge".into());
    client
        .add_alias(&alias_name, &name(), &point())
        .unwrap()
        .blocking_wait()
        .unwrap();
    client.update(&name(), &value(7)).unwrap();
    client.set_cache(&name(), 60_000).unwrap();
    assert_eq!(json!(read(&client)), native["initial"]);
    client.update(&name(), &value(11)).unwrap();
    assert_eq!(json!(read(&client)), native["cached_update"]);
    client.reset(&name()).unwrap();
    assert_eq!(json!(read(&client)), native["cached_reset"]);
    client.set_cache(&name(), 0).unwrap();
    assert_eq!(json!(read(&client)), native["cache_off_after_reset"]);
    client.set_cache(&name(), 60_000).unwrap();
    assert_eq!(json!(read(&client)), native["cache_reenabled_old_value"]);

    client.cache_delete(&name(), None).unwrap();
    assert_eq!(
        cached(client.cache_read(&name(), &point()).unwrap()),
        native["cache_delete"]
    );
    client.set_cache(&name(), 20).unwrap();
    client.update(&name(), &value(15)).unwrap();
    assert_eq!(read(&client), 15);
    client
        .barrier(Manager::Cache)
        .unwrap()
        .blocking_wait()
        .unwrap();
    let held = block(&client, Manager::Cache);
    thread::sleep(Duration::from_millis(50));
    assert_eq!(json!(read(&client)), native["suspended_beyond_ttl"]);
    client.update(&name(), &value(17)).unwrap();
    assert_eq!(json!(read(&client)), native["suspended_update"]);
    held.release();
    wait(|| client.cache_read(&name(), &point()).unwrap().is_none());
    assert_eq!(json!(read(&client)), native["after_expiry"]);

    client
        .cache_write(&name(), &point(), &value(21), 60_000)
        .unwrap();
    client
        .barrier(Manager::Cache)
        .unwrap()
        .blocking_wait()
        .unwrap();
    let old = client
        .cache_info(&name(), &point())
        .unwrap()
        .unwrap()
        .timer
        .unwrap();
    client
        .cache_write(&name(), &point(), &value(22), 60_000)
        .unwrap();
    client
        .barrier(Manager::Cache)
        .unwrap()
        .blocking_wait()
        .unwrap();
    let current = client
        .cache_info(&name(), &point())
        .unwrap()
        .unwrap()
        .timer
        .unwrap();
    assert_ne!(old, current);
    store.cache_timeout(&name(), &point(), old).unwrap();
    assert_eq!(
        cached(client.cache_read(&name(), &point()).unwrap()),
        native["old_timeout_retains_new_value"]
    );
    store.cache_timeout(&name(), &point(), current).unwrap();
    assert_eq!(
        cached(client.cache_read(&name(), &point()).unwrap()),
        native["current_timeout_removes_value"]
    );

    client
        .cache_write(&name(), &point(), &value(33), 60_000)
        .unwrap();
    client
        .barrier(Manager::Cache)
        .unwrap()
        .blocking_wait()
        .unwrap();
    let old = client.cache_info(&name(), &point()).unwrap().unwrap();
    client
        .restart(Manager::Cache, 1)
        .unwrap()
        .blocking_wait()
        .unwrap();
    let current = client.cache_info(&name(), &point()).unwrap().unwrap();
    assert_eq!(old.sequence, current.sequence);
    assert_eq!(
        json!(old.timer != current.timer),
        native["cache_restart_replaces_timer"]
    );
    assert_eq!(
        cached(client.cache_read(&name(), &point()).unwrap()),
        native["cache_restart_value"]
    );
    client
        .restart(Manager::Alias, 1)
        .unwrap()
        .blocking_wait()
        .unwrap();
    assert_eq!(alias(&client, &alias_name), native["alias_restart_value"]);
    assert_eq!(
        alias(&client, &alias_name),
        native["alias_retained_after_heir_transfer"]
    );
    assert_eq!(generation(&client, Manager::Admin), 1);
    assert_eq!(
        client
            .add_alias(&alias_name, &name(), &point())
            .unwrap()
            .blocking_wait(),
        Err(Error::Exists)
    );
    client.delete(&name()).unwrap().blocking_wait().unwrap();
    assert!(client.cache_read(&name(), &point()).unwrap().is_none());
    assert_eq!(
        alias(&client, &alias_name),
        native["alias_after_target_delete"]
    );
    let mapping = client.resolve_alias(&alias_name).unwrap().unwrap();
    assert_eq!((mapping.0.clone(), mapping.1.clone()), (name(), point()));
    assert_eq!(
        json!({"tuple":[{"list":[{"atom":"ae"},{"atom":"epoch"},{"atom":"gauge"}]},{"atom":"value"}]}),
        native["alias_mapping_after_target_delete"]
    );
    client
        .create(&name(), Kind::Gauge, true)
        .unwrap()
        .blocking_wait()
        .unwrap();
    assert_eq!(
        alias(&client, &alias_name),
        native["alias_after_target_recreate"]
    );
    // The two ETS ownership rows and unrelated report/logger sibling identity
    // are reference metadata, not assertions about these three Rust workers.
    drop(owner);
    let info = client.info();
    assert!(info.joined && !info.running);
    assert!(info.managers.iter().all(|m| m.joined && !m.running));
    assert_eq!(
        store.get_value(&name(), &Points::default_points()),
        Err(Error::Closed)
    );
}

#[test]
fn actual_panics_recover_independently_and_budget_is_shared_by_three_managers() {
    let (store, owner, client) = setup(Limits::default());
    client.update(&name(), &value(11)).unwrap();
    for (index, manager) in [
        Manager::Admin,
        Manager::Cache,
        Manager::Alias,
        Manager::Admin,
        Manager::Cache,
    ]
    .into_iter()
    .enumerate()
    {
        let previous = generation(&client, manager);
        let failed: Ticket<()> = client
            .call(manager, 128, |_| {
                panic!("injected registry manager failure")
            })
            .unwrap();
        assert_eq!(failed.blocking_wait(), Err(Error::Closed));
        wait(|| generation(&client, manager) == previous + 1);
        assert_eq!(read(&client), 11);
        assert_eq!(client.info().restart_attempts, index as u64 + 1);
        assert!(
            client
                .info()
                .managers
                .iter()
                .filter(|m| m.generation > 1)
                .all(|m| m.previous_joined)
        );
    }
    assert_eq!(
        client
            .restart(Manager::Alias, generation(&client, Manager::Alias))
            .unwrap()
            .blocking_wait(),
        Err(Error::RestartLimit)
    );
    wait(|| !client.info().running);
    assert_eq!(client.info().last_error, Some(Error::RestartLimit));
    assert_eq!(client.update(&name(), &value(1)), Err(Error::Closed));
    // Producer/source tables remain live until the enclosing owner is joined.
    assert_eq!(
        store.get_value(&name(), &Points::One(point())).unwrap(),
        Reading::Values(vec![(point(), value(11))])
    );
    drop(owner);
    assert_eq!(store.retained_bytes(), 0);
    assert!(client.info().managers.iter().all(|m| m.joined));
}

#[test]
fn credits_cover_all_workers_cancelled_queued_work_and_unread_results() {
    let (_, owner, client) = setup(Limits {
        requests: 3,
        ..Default::default()
    });
    let held = block(&client, Manager::Admin);
    let cancelled = client
        .create(&vec![Part::atom("queued")], Kind::Counter, true)
        .unwrap();
    drop(cancelled);
    let unread = client.barrier(Manager::Alias).unwrap();
    wait(|| {
        client
            .info()
            .managers
            .iter()
            .any(|m| m.manager == Manager::Alias && m.completed == 1)
    });
    assert_eq!(client.info().pending, 3);
    assert!(matches!(client.barrier(Manager::Cache), Err(Error::Busy)));
    held.release();
    wait(|| client.info().pending == 1);
    assert!(client.metric_info(&vec![Part::atom("queued")]).is_ok());
    unread.blocking_wait().unwrap();
    assert_eq!(client.info().pending, 0);
    assert_eq!(client.info().pending_bytes, 0);
    drop(owner);
}

#[test]
fn replacement_waits_for_accepted_work_and_fences_stale_requests() {
    let (_, owner, client) = setup(Limits::default());
    let held = block(&client, Manager::Admin);
    let restart = client.restart(Manager::Admin, 1).unwrap();
    wait(|| client.parent.lock().slots[&Manager::Admin].lock().closing);
    assert_eq!(generation(&client, Manager::Admin), 1);
    assert!(matches!(
        client.create(&name(), Kind::Gauge, true),
        Err(Error::Closed)
    ));
    client.update(&name(), &value(9)).unwrap();
    held.release();
    assert_eq!(restart.blocking_wait(), Ok(2));
    assert_eq!(read(&client), 9);
    assert!(matches!(
        client.restart(Manager::Admin, 1),
        Err(Error::Stale)
    ));
    drop(owner);
}

#[test]
fn shutdown_drains_metadata_then_closes_all_retained_store_and_alias_handles() {
    let (store, owner, client) = setup(Limits::default());
    assert!(matches!(
        Owner::spawn(store.clone()),
        Err(Error::AlreadyRunning)
    ));
    assert_eq!(read(&client), 0);
    let held = block(&client, Manager::Alias);
    let alias_name = Alias::Atom("late".into());
    let pending = client.add_alias(&alias_name, &name(), &point()).unwrap();
    client.shutdown();
    assert!(matches!(client.barrier(Manager::Admin), Err(Error::Closed)));
    held.release();
    pending.blocking_wait().unwrap();
    wait(|| !client.info().running);
    assert!(store.resolve_alias(&alias_name).unwrap().is_some());
    drop(owner);
    assert_eq!(store.resolve_alias(&alias_name), Err(Error::Closed));
    assert_eq!(store.set_global_enabled(true), Err(Error::Closed));
    assert_eq!(client.info().pending, 0);
}

#[test]
fn cache_fill_is_atomic_bounded_and_shared_by_reporter_reads() {
    let (store, owner, client) = setup(Limits {
        subscriptions: 2,
        ..Default::default()
    });
    client.update(&name(), &value(42)).unwrap();
    client.set_cache(&name(), 60_000).unwrap();
    let before = store.retained_bytes();
    assert_eq!(
        client.get_value(
            &name(),
            &Points::Many(vec![point(), Part::atom("one"), Part::atom("two")])
        ),
        Err(Error::Limit)
    );
    assert_eq!(store.retained_bytes(), before);
    assert!(client.cache_read(&name(), &point()).unwrap().is_none());
    let repeated = Points::Many(vec![point(), point()]);
    assert_eq!(
        client.get_value(&name(), &repeated).unwrap(),
        Reading::Values(vec![(point(), value(42)); 2])
    );
    client.update(&name(), &value(99)).unwrap();
    let sample = store
        .read(&Selector::Metric(name()), &Points::One(point()))
        .unwrap();
    assert_eq!(
        sample[0].values,
        vec![(point(), report::Value::Integer(42.into()))]
    );
    assert_eq!(
        client.aggregate(&name(), &Points::One(point())).unwrap(),
        vec![(point(), 42.into())]
    );
    client.set_enabled(&name(), false).unwrap();
    assert_eq!(
        client.get_value(&name(), &repeated).unwrap(),
        Reading::Disabled
    );
    client.set_enabled(&name(), true).unwrap();
    assert_eq!(read(&client), 42);
    client.cache_delete(&name(), None).unwrap();
    assert_eq!(store.retained_bytes(), before);
    assert_eq!(read(&client), 99);
    drop(owner);
}
