use super::*;
use serde_json::{Value, json};
fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_metrics_routing.json"
    ))
    .unwrap()
}
fn part(value: &Value) -> Part {
    if let Some(a) = value.get("atom") {
        Part::atom(a.as_str().unwrap())
    } else {
        Part::Integer(value["integer"].as_str().unwrap().parse().unwrap())
    }
}
fn entry(value: &Value) -> Entry {
    Entry {
        name: value["name"].as_array().unwrap().iter().map(part).collect(),
        kind: value["type"].as_str().unwrap().into(),
        enabled: value["enabled"].as_bool().unwrap(),
    }
}
fn atoms(value: &Value) -> BTreeSet<String> {
    value["known_atoms"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap().into())
        .collect()
}
fn rule(value: &Value) -> Rule {
    let bytes = |k| hex::decode(value[k].as_str().unwrap()).unwrap();
    Rule {
        name: bytes("name"),
        kind: bytes("type"),
        datapoints: bytes("datapoints"),
        actions: bytes("actions"),
    }
}
fn json_part(p: &Part) -> Value {
    match p {
        Part::Atom(a) => json!({"atom":a}),
        Part::Integer(n) => json!({"integer":n.to_string()}),
    }
}
fn json_entry(e: &Entry) -> Value {
    json!({"name":e.name.iter().map(json_part).collect::<Vec<_>>(),"type":e.kind,"enabled":e.enabled})
}
fn json_cells(engine: &Engine) -> Value {
    json!(engine.cells().iter().map(|c|json!({"name":c.key.name.iter().map(json_part).collect::<Vec<_>>(),"datapoint":json_part(&c.key.datapoint),"destinations":c.destinations})).collect::<Vec<_>>())
}
#[test]
fn rule_precedence_and_selection_match_original_metric_routing() {
    let native = fixture();
    let entries = native["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(entry)
        .collect::<Vec<_>>();
    for (i, row) in native["rules"].as_array().unwrap().iter().enumerate() {
        let mut engine = Engine::new(Default::default(), entries.clone(), atoms(row)).unwrap();
        engine.check_config(vec![rule(&row["rule"])]).unwrap();
        assert_eq!(
            json_cells(&engine),
            row["cells"],
            "rule {i}: {:?}",
            row["rule"]
        );
    }
    for (i, row) in native["selections"].as_array().unwrap().iter().enumerate() {
        let mut engine = Engine::new(Default::default(), entries.clone(), atoms(row)).unwrap();
        let command = hex::decode(row["command"].as_str().unwrap()).unwrap();
        let actual = engine.select(&command, &entries, row["filtered"].as_bool().unwrap());
        if row["result"]["ok"] == false {
            assert_eq!(
                actual,
                Err(Error::InvalidRule),
                "selection {i}: {:?}",
                String::from_utf8_lossy(&command)
            );
            continue;
        }
        let actual = actual.unwrap();
        assert_eq!(
            json!(actual.entries.iter().map(json_entry).collect::<Vec<_>>()),
            row["result"]["entries"],
            "selection {i}"
        );
        let points = actual
            .datapoints
            .map(|p| json!(p.iter().map(json_part).collect::<Vec<_>>()))
            .unwrap_or(json!("default"));
        assert_eq!(points, row["result"]["datapoints"], "selection {i}");
    }
}
fn key(value: &Value) -> Key {
    Key {
        name: value["name"].as_array().unwrap().iter().map(part).collect(),
        datapoint: part(&value["datapoint"]),
    }
}
#[test]
fn lazy_defaults_manual_overrides_reload_and_removed_metrics_match_original_server() {
    let native = fixture();
    let entries = native["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(entry)
        .collect::<Vec<_>>();
    for row in native["lifecycles"].as_array().unwrap() {
        let mut engine = Engine::new(
            Config {
                rules: vec![Rule {
                    actions: b"log".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            entries.clone(),
            atoms(row),
        )
        .unwrap();
        assert_eq!(json_cells(&engine), row["initial_cells"]);
        let checks = row["lookups"].as_array().unwrap();
        let k = key(&checks[0]);
        let check = |s: &mut Engine, index: usize| {
            assert_eq!(
                json!(s.destinations(&key(&checks[index])).unwrap()),
                checks[index]["result"]
            );
        };
        check(&mut engine, 0);
        engine.add(k.clone(), Destination::Send).unwrap();
        engine.add(k.clone(), Destination::Send).unwrap();
        check(&mut engine, 1);
        assert!(engine.delete(&k, Destination::Send).unwrap());
        assert!(!engine.delete(&k, Destination::Send).unwrap());
        assert!(!engine.delete(&k, Destination::Log).unwrap());
        check(&mut engine, 2);
        engine
            .check_config(vec![Rule {
                actions: b"send".to_vec(),
                ..Default::default()
            }])
            .unwrap();
        assert_eq!(json_cells(&engine), row["after_config"]);
        engine
            .register(vec![Entry {
                name: key(&checks[3]).name,
                kind: "gauge".into(),
                enabled: true,
            }])
            .unwrap();
        check(&mut engine, 3);
        for e in entries
            .iter()
            .map(|e| e.name.clone())
            .chain([key(&checks[3]).name])
        {
            engine.unregister(&e).unwrap();
        }
        engine.check_config(vec![]).unwrap();
        for i in 4..8 {
            check(&mut engine, i)
        }
        assert_eq!(json_cells(&engine), row["retained_cells"]);
    }
}
#[test]
fn operational_limits_rollback_configuration_and_insertions_without_erasing_overrides() {
    let key = Key {
        name: vec![Part::atom("ae"), Part::atom("epoch"), Part::atom("x")],
        datapoint: Part::atom("value"),
    };
    let mut engine = Engine::new(
        Config {
            limits: Limits {
                cells: 1,
                ..Default::default()
            },
            ..Default::default()
        },
        vec![Entry {
            name: key.name.clone(),
            kind: "gauge".into(),
            enabled: true,
        }],
        standard_atoms(),
    )
    .unwrap();
    engine.add(key.clone(), Destination::Send).unwrap();
    let before = engine.cells();
    assert_eq!(
        engine.check_config(vec![Rule::default()]),
        Err(Error::Limit)
    );
    assert_eq!(engine.cells(), before);
    assert!(engine.config.rules.is_empty());
    assert_eq!(
        engine.destinations(&key).unwrap(),
        Some(vec!["send".into()])
    );
    assert!(engine.delete(&key, Destination::Send).unwrap());
    assert_eq!(engine.destinations(&key).unwrap(), Some(vec![]));
    assert_eq!(
        engine.select(b"type=*/ae.epoch.**/not_an_atom", &[], false),
        Err(Error::InvalidRule)
    );
    assert_eq!(engine.cells()[0].destinations, Vec::<String>::new());
}

fn setup() -> Setup {
    Setup {
        config: Config {
            rules: vec![Rule {
                actions: b"log".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        },
        entries: vec![Entry {
            name: vec![Part::atom("ae"), Part::atom("epoch"), Part::atom("sample")],
            kind: "gauge".into(),
            enabled: true,
        }],
        ..Default::default()
    }
}
fn sample_key() -> Key {
    Key {
        name: setup().entries[0].name.clone(),
        datapoint: Part::atom("value"),
    }
}
#[test]
fn worker_restarts_retain_table_and_rules_while_old_clients_cannot_mutate_replacements() {
    let owner = Owner::new(setup()).unwrap();
    let store = owner.store();
    let worker = Worker::spawn(store.clone()).unwrap();
    let old = worker.client();
    assert!(matches!(
        Worker::spawn(store.clone()),
        Err(Error::AlreadyRunning)
    ));
    let key = sample_key();
    assert_eq!(
        old.destinations(key.clone()).unwrap().blocking_wait(),
        Ok(Some(vec!["log".into()]))
    );
    old.add(key.clone(), Destination::Send)
        .unwrap()
        .blocking_wait()
        .unwrap();
    old.check_config(vec![Rule {
        actions: b"none".to_vec(),
        ..Default::default()
    }])
    .unwrap()
    .blocking_wait()
    .unwrap();
    let cells = store.cells().unwrap();
    worker.shutdown().unwrap();
    assert_eq!(
        old.destinations(key.clone()).unwrap().blocking_wait(),
        Ok(Some(vec!["send".into()]))
    );
    let missing = Key {
        name: vec![Part::atom("new")],
        datapoint: Part::atom("value"),
    };
    assert!(matches!(
        old.destinations(missing.clone()),
        Err(Error::Stopped)
    ));
    let next = Worker::spawn(store.clone()).unwrap();
    assert_eq!(store.cells().unwrap(), cells);
    assert!(matches!(
        old.add(key.clone(), Destination::Log),
        Err(Error::Stopped)
    ));
    next.client()
        .register(vec![Entry {
            name: vec![Part::atom("ae"), Part::atom("epoch"), Part::atom("new")],
            kind: "gauge".into(),
            enabled: true,
        }])
        .unwrap()
        .blocking_wait()
        .unwrap();
    let fresh = Key {
        name: vec![Part::atom("ae"), Part::atom("epoch"), Part::atom("new")],
        datapoint: Part::atom("value"),
    };
    assert_eq!(
        next.client().destinations(fresh).unwrap().blocking_wait(),
        Ok(Some(vec![]))
    );
    drop(owner);
    assert!(store.is_closed());
    assert_eq!(store.cached(&key), Err(Error::Closed));
    assert!(matches!(old.destinations(key), Err(Error::Closed)));
    next.shutdown().unwrap();
    assert!(matches!(Worker::spawn(store), Err(Error::Closed)));
}
#[test]
fn bounded_worker_drains_accepted_requests_and_dropped_replies_before_join() {
    let mut settings = setup();
    settings.config.limits.requests = 4;
    let owner = Owner::new(settings).unwrap();
    let worker = Worker::spawn(owner.store()).unwrap();
    let client = worker.client();
    let key = sample_key();
    let (entered, release) = worker::pause(&client);
    entered.recv().unwrap();
    let add = client.add(key.clone(), Destination::Send).unwrap();
    drop(client.add(key.clone(), Destination::Log).unwrap());
    let read = client.destinations(key.clone()).unwrap();
    assert!(matches!(
        client.add(key.clone(), Destination::Send),
        Err(Error::Busy)
    ));
    assert_eq!(client.info().pending, 4);
    worker.request_shutdown();
    assert!(matches!(
        client.delete(key.clone(), Destination::Send),
        Err(Error::Stopped)
    ));
    release.send(()).unwrap();
    worker.shutdown().unwrap();
    add.blocking_wait().unwrap();
    assert_eq!(
        read.blocking_wait(),
        Ok(Some(vec!["log".into(), "send".into()]))
    );
    assert_eq!(client.info().completed, 4);
    assert_eq!(client.info().pending_bytes, 0);
    let oversized = Owner::new(Setup {
        config: Config {
            limits: Limits {
                pending_bytes: 1,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    })
    .unwrap();
    let tiny = Worker::spawn(oversized.store()).unwrap();
    assert!(matches!(
        tiny.client().add(key, Destination::Log),
        Err(Error::Busy)
    ));
    tiny.shutdown().unwrap();
}
#[test]
fn worker_panic_preserves_parent_table_and_batch_routing_respects_empty_defaults() {
    let setup = setup();
    let entries = setup.entries.clone();
    let key = sample_key();
    let owner = Owner::new(setup).unwrap();
    let store = owner.store();
    let worker = Worker::spawn(store.clone()).unwrap();
    let client = worker.client();
    assert_eq!(
        client
            .route(entries.clone(), vec![key.clone()])
            .unwrap()
            .blocking_wait(),
        Ok(vec![Some(vec!["log".into()])])
    );
    worker::fail(&client);
    assert_eq!(worker.shutdown(), Err(Error::Panicked));
    assert_eq!(
        client.destinations(key.clone()).unwrap().blocking_wait(),
        Ok(Some(vec!["log".into()]))
    );
    let next = Worker::spawn(store.clone()).unwrap();
    let active = next.client();
    active
        .add(key.clone(), Destination::Send)
        .unwrap()
        .blocking_wait()
        .unwrap();
    active
        .delete(key.clone(), Destination::Send)
        .unwrap()
        .blocking_wait()
        .unwrap();
    assert_eq!(
        active
            .route(entries, vec![key.clone()])
            .unwrap()
            .blocking_wait(),
        Ok(vec![Some(vec![])])
    );
    next.shutdown().unwrap();
    assert_eq!(store.cached(&key), Ok(Some(vec![])));
    drop(owner);
    assert!(store.is_closed());
}

#[test]
fn observer_metric_identities_preserve_original_predefined_and_probe_types() {
    use crate::observe::metrics::{Metric, Number};
    let native = fixture();
    for name in [
        "ae.epoch.aecore.chain.height.value",
        "ae.epoch.aecore.peers.count.value",
        "ae.epoch.aecore.peers.ping.success.value",
        "ae.epoch.aecore.peers.ping.failure.value",
        "ae.epoch.aecore.chain.total_difficulty",
        "ae.epoch.aecore.tx_pool.pending_txs",
    ] {
        let (metric, key) = Metric {
            name,
            value: Number::Integer(0),
        }
        .routing();
        let expected = native["predefined"]
            .as_array()
            .unwrap()
            .iter()
            .chain(native["probes"].as_array().unwrap())
            .map(entry)
            .find(|e| e.name == key.name)
            .unwrap();
        assert_eq!(metric, expected, "{name}");
        assert_eq!(key.datapoint, Part::atom(name.rsplit_once('.').unwrap().1));
    }
}
