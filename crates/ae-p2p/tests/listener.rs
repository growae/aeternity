use ae_p2p::listener::{Config, Error, Listener};
use serde_json::Value;

#[test]
fn pinned_ranch_connection_and_acceptor_traces_match_every_snapshot() {
    let data: Value =
        serde_json::from_str(include_str!("fixtures/erlang_ranch_listener.json")).unwrap();
    let mut snapshots = 0;
    for case in data["cases"].as_array().unwrap() {
        let mut listener = Listener::<u64>::new(Config {
            acceptors: case["acceptors"].as_u64().unwrap() as usize,
            maximum_connections: case["max_connections"].as_u64().unwrap() as usize,
        })
        .unwrap();
        for step in case["steps"].as_array().unwrap() {
            let action = &step["action"];
            if let Some(id) = action.get("open") {
                listener
                    .started(
                        action["acceptor"].as_u64().unwrap() as usize - 1,
                        id.as_u64().unwrap(),
                    )
                    .unwrap();
            } else if let Some(id) = action.get("stop") {
                assert!(listener.closed(&id.as_u64().unwrap()));
            } else if let Some(maximum) = action.get("limit") {
                listener.set_maximum(maximum.as_u64().unwrap() as usize);
            }
            assert_eq!(
                listener.active(),
                step["active"].as_u64().unwrap() as usize,
                "{case} {step}"
            );
            assert_eq!(
                listener.config().maximum_connections,
                step["max_connections"].as_u64().unwrap() as usize
            );
            let sleeping: Vec<_> = listener.sleeping().iter().rev().map(|n| n + 1).collect();
            assert_eq!(
                serde_json::to_value(sleeping).unwrap(),
                step["sleeping_acceptors"]
            );
            assert_eq!(
                listener.active(),
                step["children"].as_u64().unwrap() as usize
            );
            snapshots += 1;
        }
    }
    assert_eq!(snapshots, 33);
}

#[test]
fn stale_closes_and_invalid_starts_never_refund_an_acceptor() {
    assert!(matches!(
        Listener::<u64>::new(Config {
            acceptors: 65_537,
            ..Default::default()
        }),
        Err(Error::Configuration)
    ));
    let mut empty = Listener::<u64>::new(Config {
        acceptors: 0,
        maximum_connections: 0,
    })
    .unwrap();
    assert_eq!(empty.available(), None);
    assert_eq!(empty.started(0, 1), Err(Error::Acceptor));
    let mut listener = Listener::new(Config {
        acceptors: 2,
        maximum_connections: 1,
    })
    .unwrap();
    listener.started(0, 1).unwrap();
    assert_eq!(listener.started(0, 2), Err(Error::Paused));
    assert_eq!(listener.started(1, 1), Err(Error::Duplicate));
    assert_eq!(listener.active(), 1);
    assert_eq!(listener.available(), Some(1));
    assert!(!listener.closed(&99));
    assert!(listener.closed(&1));
    assert!(!listener.closed(&1));
    assert!(listener.sleeping().is_empty());
    listener.started(0, 2).unwrap();
    listener.started(1, 3).unwrap();
    listener.set_maximum(0);
    assert!(listener.closed(&2) && listener.closed(&3));
    assert_eq!(listener.available(), None);
    listener.set_maximum(1);
    assert_eq!(listener.available(), Some(0));
}
