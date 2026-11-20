use super::*;
use serde_json::{Value, json};

fn hash(n: u64) -> Hash256 {
    let mut bytes = [0; 32];
    bytes[24..].copy_from_slice(&n.to_be_bytes());
    Hash256(bytes)
}
fn number(h: Hash256) -> u64 {
    u64::from_be_bytes(h.0[24..].try_into().unwrap())
}
fn snapshot(cache: &Cache) -> Value {
    json!({
        "size":cache.len(),
        "queue":cache.entries().map(number).collect::<Vec<_>>(),
        "members":cache.members.iter().copied().map(number).collect::<Vec<_>>(),
        "hit":num_bigint::BigUint::from_bytes_be(cache.stats.hit.as_bytes()).to_string(),
        "miss":num_bigint::BigUint::from_bytes_be(cache.stats.miss.as_bytes()).to_string(),
    })
}

#[test]
fn original_fifo_traces_preserve_resize_reset_and_unbounded_counters() {
    let data: Value =
        serde_json::from_str(include_str!("../../tests/fixtures/erlang_gossip.json")).unwrap();
    for row in data["rows"].as_array().unwrap() {
        let mut cache = Cache::new(row["capacity"].as_u64().unwrap() as usize, 1000).unwrap();
        let seed = Unsigned::from_decimal(row["seed"].as_str().unwrap()).unwrap();
        cache.stats = Stats {
            hit: seed.clone(),
            miss: seed,
        };
        for (i, step) in row["steps"].as_array().unwrap().iter().enumerate() {
            match step["op"].as_str().unwrap() {
                "check" => assert_eq!(
                    cache.check(hash(step["hash"].as_u64().unwrap())),
                    step["hit"].as_bool().unwrap()
                ),
                "capacity" => cache
                    .reconfigure(step["capacity"].as_u64().unwrap() as usize)
                    .unwrap(),
                "reset" => cache.reset(),
                other => panic!("unexpected operation {other}"),
            }
            assert_eq!(
                snapshot(&cache),
                step["state"],
                "capacity {} seed {} step {i}",
                row["capacity"],
                row["seed"]
            );
        }
    }
    let mut live = Cache::new(2, 2).unwrap();
    let hits: Vec<_> = [1, 2, 1, 3, 1]
        .into_iter()
        .map(|n| live.check(hash(n)))
        .collect();
    assert_eq!(json!(hits), data["runtime"]["hits"]);
    assert_eq!(
        live.stats.hit,
        data["runtime"]["stats"]["hit"].as_u64().unwrap().into()
    );
    assert_eq!(
        live.stats.miss,
        data["runtime"]["stats"]["miss"].as_u64().unwrap().into()
    );
    live.reset();
    assert!(live.is_empty());
    assert_eq!(live.stats(), &Stats::default());
}

#[test]
fn invalid_configuration_is_atomic_and_shrinking_keeps_fifo_until_reset() {
    assert_eq!(Cache::new(0, 5).unwrap_err(), Error::InvalidCapacity);
    assert_eq!(Cache::new(6, 5).unwrap_err(), Error::ResourceLimit);
    let mut c = Cache::new(3, 4).unwrap();
    for n in 1..=3 {
        assert!(!c.check(hash(n)));
    }
    assert!(c.check(hash(1)));
    let before = snapshot(&c);
    assert_eq!(c.reconfigure(0), Err(Error::InvalidCapacity));
    assert_eq!(c.reconfigure(5), Err(Error::ResourceLimit));
    assert_eq!(snapshot(&c), before);
    assert_eq!(c.capacity(), 3);
    c.reconfigure(1).unwrap();
    assert!(!c.check(hash(4)));
    assert_eq!(c.entries().map(number).collect::<Vec<_>>(), [2, 3, 4]);
    c.reset();
    assert_eq!(c.capacity(), 1);
    c.check(hash(5));
    c.check(hash(6));
    assert_eq!(c.entries().map(number).collect::<Vec<_>>(), [6]);
}
