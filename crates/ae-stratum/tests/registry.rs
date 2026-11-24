use ae_stratum::{
    Error,
    nonce::{Kind, Part, cache::Cache},
    registry::{Limits, Registry},
    session::{Membership, Registration, WorkerRegistration},
};
use num_bigint::BigInt;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/erlang_stratum_owners.json")).unwrap()
}
fn workers(w: &BTreeMap<Vec<u8>, BigInt>) -> Value {
    Value::Object(
        w.iter()
            .map(|(k, v)| {
                (
                    String::from_utf8(k.clone()).unwrap(),
                    serde_json::from_str(&v.to_string()).unwrap(),
                )
            })
            .collect(),
    )
}
fn account(r: &Registry, a: &[u8]) -> Value {
    r.find_account(a).map_or(
        json!("not_found"),
        |e| json!({"connection":e.connection,"workers":workers(&e.workers)}),
    )
}
fn connection(r: &Registry, c: u64) -> Value {
    r.find_connection(c).map_or(json!("not_found"),|(a,e)|json!({"account":String::from_utf8(a.to_vec()).unwrap(),"workers":workers(&e.workers)}))
}
#[test]
fn every_native_registry_transition_membership_and_timestamp_projection_matches() {
    let mut r = Registry::new(Default::default()).unwrap();
    let mut quota = 20;
    let mut alive: BTreeSet<u64> = [1, 2, 3].into();
    for row in fixture()["registry"].as_array().unwrap() {
        let op = &row["operation"];
        let s = |i: usize| op[i].as_str().unwrap().as_bytes();
        let n = |i: usize| op[i].as_u64().unwrap();
        let now = BigInt::from(row["step"].as_u64().unwrap());
        let result = match s(0) {
            b"quota" => {
                quota = n(1) as usize;
                json!("ok")
            }
            b"add" => json!(match r.add(s(1), s(2), n(3), now).unwrap() {
                Registration::Added => "ok",
                Registration::AlreadyPresent => "already_present",
            }),
            b"worker" => json!(match r.add_worker(s(1), s(2), quota, now).unwrap() {
                WorkerRegistration::Added => "ok",
                WorkerRegistration::AlreadyPresent => "worker_already_present",
                WorkerRegistration::Exhausted => "worker_count_exhausted",
                WorkerRegistration::AccountNotFound => "account_not_found",
            }),
            b"find_account" => account(&r, s(1)),
            b"find_connection" => connection(&r, n(1)),
            b"delete_account" => json!(if r.delete_account(s(1)) {
                "ok"
            } else {
                "not_found"
            }),
            b"delete_connection" => json!(if r.delete_connection(n(1)) {
                "ok"
            } else {
                "not_found"
            }),
            b"kill" => {
                alive.remove(&n(1));
                json!("ok")
            }
            b"notify" => json!(
                r.connections()
                    .filter(|c| alive.contains(c))
                    .collect::<Vec<_>>()
            ),
            _ => panic!("operation"),
        };
        assert_eq!(result, row["result"], "step {}", row["step"]);
        assert_eq!(json!(r.len()), row["size"]);
        assert_eq!(
            json!(r.connections().collect::<Vec<_>>()),
            row["connections"]
        );
        assert_eq!(json!(r.accounts().iter().map(|(a,e)|json!({"account":String::from_utf8(a.clone()).unwrap(),"connection":e.connection,"workers":workers(&e.workers)})).collect::<Vec<_>>()),row["entries"]);
        for m in row["membership"].as_array().unwrap() {
            let a = m["account"].as_str().unwrap().as_bytes();
            let c = m["connection"].as_u64().unwrap();
            assert_eq!(json!(r.member_account(a)), m["account_member"]);
            assert_eq!(json!(r.member_connection(c)), m["connection_member"]);
            assert_eq!(
                json!(match r.membership(a, c) {
                    Membership::Both => "both",
                    Membership::AccountOnly => "account_only",
                    Membership::Neither => "neither",
                }),
                m["both"]
            );
        }
    }
    assert_eq!(r.retained_bytes(), 0);
}
#[test]
fn native_nonce_search_limits_rng_consumption_and_value_only_release_match() {
    for row in fixture()["nonces"].as_array().unwrap() {
        let mut cache = Cache::new(4096).unwrap();
        for v in row["initial"].as_array().unwrap() {
            cache
                .allocate(7, |_| Ok(v.as_u64().unwrap()))
                .unwrap()
                .unwrap();
        }
        let draws = row["draws"].as_array().unwrap();
        let mut consumed = 0;
        let result = cache
            .allocate(row["bytes"].as_u64().unwrap() as u8, |_| {
                let v = draws[consumed].as_u64().unwrap();
                consumed += 1;
                Ok(v)
            })
            .unwrap();
        assert_eq!(
            result.map_or(
                Value::Null,
                |p| json!({"value":p.value(),"bytes":p.bytes()})
            ),
            row["result"]
        );
        assert_eq!(json!(consumed), row["consumed"]);
        assert_eq!(json!(cache.values()), row["after_allocate"]);
        let free = Part::new(
            Kind::Miner,
            row["free_value"].as_u64().unwrap(),
            row["free_bytes"].as_u64().unwrap() as u8,
        )
        .unwrap();
        cache.free(free);
        cache.free(free);
        assert_eq!(json!(cache.values()), row["after_free"]);
    }
}
#[test]
fn resource_and_input_failures_preserve_both_indexes_and_nonce_reservations() {
    let mut r = Registry::new(Limits {
        accounts: 1,
        retained_bytes: 230,
        field_bytes: 2,
    })
    .unwrap();
    r.add(b"a", b"w", 1, 1.into()).unwrap();
    let before = r.clone();
    assert_eq!(
        r.add(b"b", b"w", 2, 2.into()),
        Err(Error::Limit("registry accounts"))
    );
    assert_eq!(
        r.add_worker(b"a", b"x", 2, 2.into()),
        Err(Error::Limit("registry bytes"))
    );
    assert_eq!(
        r.add_worker(b"a", b"x", 2, BigInt::from(1) << 16385),
        Err(Error::Limit("integer"))
    );
    assert_eq!(
        r.add(b"long", b"w", 2, 2.into()),
        Err(Error::Limit("registry field"))
    );
    assert_eq!(r, before);
    assert!(r.delete_connection(1));
    assert_eq!(r.retained_bytes(), 0);
    r.add(b"b", b"x", 2, 2.into()).unwrap();
    let mut cache = Cache::new(2).unwrap();
    let a = cache.allocate(1, |_| Ok(1)).unwrap().unwrap();
    let before = cache.clone();
    assert_eq!(cache.allocate(1, |_| Ok(256)), Err(Error::Nonce));
    assert_eq!(cache, before);
    assert_eq!(
        cache.allocate(2, |_| Err(Error::Entropy)),
        Err(Error::Entropy)
    );
    assert_eq!(cache, before);
    let mut count = 0;
    assert_eq!(
        cache
            .allocate(7, |_| {
                count += 1;
                Ok(1)
            })
            .unwrap(),
        None
    );
    assert_eq!(count, 50);
    assert_eq!(cache, before);
    cache.allocate(7, |_| Ok(2)).unwrap();
    assert_eq!(
        cache.allocate(1, |_| panic!("capacity checked first")),
        Err(Error::Limit("nonce cache"))
    );
    cache.free(a);
    assert_eq!(cache.allocate(2, |_| Ok(1)).unwrap().unwrap().value(), 1);
}
