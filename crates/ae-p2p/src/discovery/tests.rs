use super::*;
use serde_json::{Value, json};
fn fixtures() -> Value {
    serde_json::from_str(include_str!("../../tests/fixtures/erlang_discovery.json")).unwrap()
}
fn key(n: u64) -> PeerPublicKey {
    let mut a = [0; 32];
    a[24..].copy_from_slice(&n.to_be_bytes());
    PeerPublicKey(a)
}
fn number(id: PeerPublicKey) -> u64 {
    u64::from_be_bytes(id.0[24..].try_into().unwrap())
}
fn ip(v: &Value) -> Ipv4Addr {
    Ipv4Addr::from(std::array::from_fn::<_, 4, _>(|i| {
        v[i].as_u64().unwrap() as u8
    }))
}
fn from_json(v: &Value) -> Peer {
    Peer {
        id: key(v["id"].as_u64().unwrap()),
        address: ip(&v["address"]),
        source: ip(&v["source"]),
        host: hex::decode(v["host"].as_str().unwrap()).unwrap(),
        port: v["port"].as_u64().unwrap() as u16,
        trusted: v["trusted"].as_bool().unwrap(),
    }
}
fn peer_json(p: &Peer) -> Value {
    json!({"id":number(p.id),"address":p.address.octets(),"source":p.source.octets(),"host":hex::encode(&p.host),"port":p.port,"trusted":p.trusted})
}
fn outcome(v: Update) -> &'static str {
    match v {
        Update::Verified => "verified",
        Update::Unverified => "unverified",
        Update::Ignored => "ignored",
    }
}
fn target(v: &str) -> Target {
    match v {
        "verified" => Target::Verified,
        "unverified" => Target::Unverified,
        _ => Target::Both,
    }
}
fn allowed(ids: &Value, p: &Peer) -> bool {
    ids.as_str() == Some("all") || ids.as_array().unwrap().contains(&json!(number(p.id)))
}
fn config(v: &Value) -> Config {
    Config {
        verified_buckets: v["verif_bcount"].as_u64().unwrap() as usize,
        verified_size: v["verif_bsize"].as_u64().unwrap() as usize,
        unverified_buckets: v["unver_bcount"].as_u64().unwrap() as usize,
        unverified_size: v["unver_bsize"].as_u64().unwrap() as usize,
        maximum_references: v["unver_max_refs"].as_u64().unwrap() as usize,
        maximum_update_lapse: v["max_update_lapse"].as_u64().unwrap(),
        standby_times: v["standby_times"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_u64().unwrap())
            .collect(),
        maximum_rejections: v["max_rejections"].as_u64().unwrap(),
        ..Default::default()
    }
}
fn effects_json(effects: Vec<Effect>) -> Vec<Value> {
    effects
        .into_iter()
        .map(|e| match e {
            Effect::Persist(p) => json!({"kind":"persist","peer":peer_json(&p)}),
            Effect::Delete(p) => json!({"kind":"delete","peer":peer_json(&p)}),
        })
        .collect()
}
fn invariant(s: &Pool) {
    for (class, buckets, maximum) in [
        (Class::Verified, &s.verified, s.config.verified_size),
        (Class::Unverified, &s.unverified, s.config.unverified_size),
    ] {
        for (index, ids) in buckets.iter().enumerate() {
            assert!(ids.len() <= maximum);
            assert_eq!(ids.len(), ids.iter().collect::<BTreeSet<_>>().len());
            for id in ids {
                let p = &s.peers[id];
                assert_eq!(p.class(), class);
                match class {
                    Class::Verified => assert_eq!(p.verified, Some(index)),
                    Class::Unverified => assert!(p.unverified.contains(&index)),
                };
            }
        }
    }
    for (&id, p) in &s.peers {
        assert_eq!(p.peer.id, id);
        if let Some(index) = p.verified {
            assert!(p.unverified.is_empty());
            assert!(s.verified[index].contains(&id));
            assert!(s.all_verified.indices.contains_key(&id));
        } else {
            assert!(!p.unverified.is_empty());
            assert!(p.unverified.len() <= s.config.maximum_references);
            assert!(!s.all_verified.indices.contains_key(&id));
        }
        for &index in &p.unverified {
            assert!(s.unverified[index].contains(&id));
        }
        assert_eq!(
            p.unverified.len(),
            p.unverified.iter().collect::<BTreeSet<_>>().len()
        );
        let available = !p.selected && !s.standby.contains(&id);
        assert_eq!(
            s.available_verified.indices.contains_key(&id),
            available && p.class() == Class::Verified
        );
        assert_eq!(
            s.available_unverified.indices.contains_key(&id),
            available && p.class() == Class::Unverified
        );
        if s.standby.contains(&id) {
            assert!(!p.selected);
            assert!(p.rejected > 0);
            assert!(p.rejected_at.is_some());
        }
    }
    for lookup in [
        &s.all_verified,
        &s.available_verified,
        &s.available_unverified,
    ] {
        assert_eq!(lookup.ids.len(), lookup.indices.len());
        for (index, &id) in lookup.ids.iter().enumerate() {
            assert_eq!(lookup.indices.get(&id), Some(&index));
            assert!(s.peers.contains_key(&id));
        }
    }
    for id in &s.standby {
        assert!(s.peers.contains_key(id));
    }
    assert_eq!(s.count(Count::All, Target::Both), s.peers.len());
    assert_eq!(
        s.count(Count::Available, Target::Both),
        s.available_verified.ids.len() + s.available_unverified.ids.len()
    );
    assert_eq!(
        s.references(Target::Both),
        s.verified.iter().map(Vec::len).sum::<usize>()
            + s.unverified.iter().map(Vec::len).sum::<usize>()
    );
}
fn compare(s: &Pool, v: &Value) -> usize {
    invariant(s);
    let peers:Vec<_>=s.peers.iter().map(|(&id,e)| {let st=s.state(id).unwrap();let mut buckets:Vec<_>=e.unverified.iter().map(|x|x+1).collect();buckets.sort();
        json!({"peer":peer_json(&e.peer),"class":match st.class {Class::Verified=>"verified",Class::Unverified=>"unverified"},"available":st.available,"selected":st.selected,"rejections":st.rejections,"updated":st.updated_at,"selected_at":st.selected_at,"rejected_at":st.rejected_at,"references":st.references,"buckets":buckets,"verified_bucket":e.verified.map(|i|i+1)})
    }).collect();
    assert_eq!(json!(peers), v["peers"]);
    let mut inconsistent = 0;
    for c in v["counts"].as_array().unwrap() {
        let t = target(c["target"].as_str().unwrap());
        let kind = c["kind"].as_str().unwrap();
        let k = match kind {
            "all" => Count::All,
            "available" => Count::Available,
            _ => Count::Standby,
        };
        let native = c["value"].as_i64().unwrap();
        let actual = s.count(k, t) as i64;
        if k == Count::All && native != actual {
            inconsistent += 1;
        } else {
            assert_eq!(actual, native);
        }
    }
    inconsistent
}

#[test]
fn original_salted_bucket_indices_include_little_endian_and_byte_truncation() {
    let data = fixtures();
    let mut count = 0;
    for v in data["buckets"].as_array().unwrap() {
        let c = Config {
            verified_buckets: v["verified_count"].as_u64().unwrap() as usize,
            unverified_buckets: v["unverified_count"].as_u64().unwrap() as usize,
            verified_group_shard: v["verified_shard"].as_u64().unwrap() as usize,
            unverified_group_shard: v["group_shard"].as_u64().unwrap() as usize,
            unverified_source_shard: v["source_shard"].as_u64().unwrap() as usize,
            ..Default::default()
        };
        let s = Pool::with_seed(
            c,
            &hex::decode(v["secret"].as_str().unwrap()).unwrap(),
            [0; 32],
        )
        .unwrap();
        for row in v["rows"].as_array().unwrap() {
            let address = ip(&row["address"]);
            assert_eq!(json!(address_group(address)), row["group"]);
            assert_eq!(
                s.verified_bucket_index(address),
                row["verified"].as_u64().unwrap() as usize
            );
            assert_eq!(
                s.unverified_bucket_index(ip(&row["source"]), address),
                row["unverified"].as_u64().unwrap() as usize
            );
            count += 1;
        }
    }
    assert_eq!(count, 60);
}
#[test]
fn original_membership_standby_eviction_and_persistence_traces() {
    let data = fixtures();
    let mut steps = 0;
    let mut counter_errors = 0;
    for scenario in data["scenarios"].as_array().unwrap() {
        let mut s = Pool::with_seed(config(&scenario["config"]), &[1, 2, 3, 4], [0; 32]).unwrap();
        for row in scenario["steps"].as_array().unwrap() {
            let op = row["op"].as_str().unwrap();
            let now = row["now"].as_u64().unwrap_or(0);
            let id = key(row["id"].as_u64().unwrap_or(0));
            let mut effects = vec![];
            match op {
                "update" => {
                    let change = s.update(now, from_json(&row["peer"])).unwrap();
                    assert_eq!(json!(outcome(change.result)), row["result"]);
                    effects = change.effects;
                }
                "ensure" => {
                    let p = from_json(&row["peer"]);
                    let mut complete = false;
                    for _ in 0..10000 {
                        let change = s.update(now, p.clone()).unwrap();
                        effects.extend(change.effects);
                        if s.state(p.id).unwrap().references
                            >= row["references"].as_u64().unwrap() as usize
                        {
                            assert_eq!(json!(outcome(change.result)), row["result"]);
                            complete = true;
                            break;
                        }
                    }
                    assert!(complete);
                }
                "verify" => {
                    let change = s.verify(now, id).unwrap();
                    assert_eq!(json!(outcome(change.result)), row["result"]);
                    effects = change.effects;
                }
                "select" => s.select(now, id).unwrap(),
                "release" => s.release(now, id).unwrap(),
                "reject" => effects = s.reject(now, id).unwrap().effects,
                "delete" => effects = s.delete(id).unwrap().effects,
                "random" => {
                    let result = s
                        .random_select(now, target(row["target"].as_str().unwrap()), |p| {
                            allowed(&row["filter"], p)
                        })
                        .unwrap();
                    let result = match result {
                        Selection::Selected(p) => json!({"kind":"selected","id":number(p.id)}),
                        Selection::Wait(d) => json!({"kind":"wait","delay":d}),
                        Selection::Unavailable => json!({"kind":"unavailable"}),
                    };
                    assert_eq!(result, row["result"]);
                }
                "subset" => {
                    let mut ids: Vec<_> = s
                        .random_subset(usize::MAX, |p| allowed(&row["filter"], p))
                        .into_iter()
                        .map(|p| number(p.id))
                        .collect();
                    ids.sort();
                    assert_eq!(json!(ids), row["result"]);
                }
                _ => panic!("unknown {op}"),
            }
            let mut effects = effects_json(effects);
            let mut native = row["effects"].as_array().unwrap().clone();
            // Compound ensure loops can require different random trial counts.
            if op == "ensure" {
                effects.dedup();
                native.dedup();
            }
            assert_eq!(effects, native, "{} {row}", scenario["label"]);
            counter_errors += compare(&s, &row["state"]);
            steps += 1;
        }
    }
    assert_eq!(counter_errors, 28);
    assert_eq!(steps, 92);
    println!(
        "Discovery native traces: {steps} steps; {counter_errors} inconsistent native raw counters deliberately repaired"
    );
}
fn peer(n: u64, trusted: bool) -> Peer {
    Peer {
        id: key(n),
        address: Ipv4Addr::new(10, n as u8, 2, 3),
        source: Ipv4Addr::new(20, n as u8, 4, 5),
        host: format!("peer{n}").into_bytes(),
        port: 3000 + n as u16,
        trusted,
    }
}
fn small() -> Config {
    Config {
        verified_buckets: 4,
        verified_size: 3,
        unverified_buckets: 8,
        unverified_size: 3,
        maximum_update_lapse: 30,
        standby_times: vec![2, 5, 11],
        maximum_rejections: 3,
        ..Default::default()
    }
}

#[test]
fn randomized_mutations_preserve_every_reference_lookup_and_persistence_effect() {
    let mut s = Pool::with_seed(small(), &[7; 128], [4; 32]).unwrap();
    let mut random = StdRng::from_seed([3; 32]);
    let mut stored = BTreeMap::new();
    for now in 0..10000 {
        let id = key(random.gen_range(1..=120));
        let op = random.gen_range(0..10);
        let mut effects = vec![];
        match op {
            0..=4 => {
                let mut p = peer(number(id), number(id).is_multiple_of(13));
                p.source =
                    Ipv4Addr::new(random.gen_range(0..=255), random.gen_range(0..=255), 1, 2);
                effects = s.update(now, p).unwrap().effects;
            }
            5 => {
                if s.find(id).is_some() {
                    effects = s.verify(now, id).unwrap().effects;
                }
            }
            6 => {
                let _ = s
                    .random_select(now, Target::Both, |p| p.address.octets()[1] % 3 != 0)
                    .unwrap();
            }
            7 => s.release(now, id).unwrap(),
            8 => effects = s.reject(now, id).unwrap().effects,
            _ => {
                if s.state(id).is_some_and(|p| !p.selected) {
                    effects = s.delete(id).unwrap().effects;
                }
            }
        }
        for effect in effects {
            match effect {
                Effect::Persist(p) => {
                    stored.insert(p.id, p);
                }
                Effect::Delete(p) => {
                    stored.remove(&p.id);
                }
            }
        }
        let expected: BTreeMap<_, _> = s
            .all(Target::Both)
            .filter(|p| !p.trusted)
            .map(|p| (p.id, p.clone()))
            .collect();
        assert_eq!(stored, expected);
        invariant(&s);
    }
}
#[test]
fn subset_contains_verified_selected_peers_but_selection_respects_filters_and_availability() {
    let mut s = Pool::with_seed(Default::default(), &[7; 128], [9; 32]).unwrap();
    for n in 1..=24 {
        s.update(0, peer(n, n <= 16)).unwrap();
    }
    for n in 1..=16 {
        s.select(0, key(n)).unwrap();
    }
    assert_eq!(
        s.random_select(0, Target::Verified, |_| true).unwrap(),
        Selection::Unavailable
    );
    let subset = s.random_subset(8, |p| number(p.id).is_multiple_of(2));
    assert_eq!(subset.len(), 8);
    assert_eq!(
        subset.iter().map(|p| p.id).collect::<BTreeSet<_>>().len(),
        8
    );
    assert!(subset.iter().all(|p| s.state(p.id).unwrap().selected));
    assert!(
        s.random_subset(0, |_| panic!("zero subset must not invoke filter"))
            .is_empty()
    );
    assert_eq!(
        s.random_select(0, Target::Both, |_| false).unwrap(),
        Selection::Unavailable
    );
    for n in 17..=24 {
        assert_eq!(
            s.random_select(0, Target::Both, |p| p.id == key(n))
                .unwrap(),
            Selection::Selected(peer(n, false))
        );
    }
    assert_eq!(
        s.random_select(0, Target::Both, |_| true).unwrap(),
        Selection::Unavailable
    );
    invariant(&s);
}
#[test]
fn invalid_commands_do_not_change_peer_state_or_advance_the_clock() {
    let mut s = Pool::with_seed(small(), &[], [8; 32]).unwrap();
    s.update(100, peer(1, false)).unwrap();
    s.select(100, key(1)).unwrap();
    let before = s.state(key(1));
    assert_eq!(s.select(100, key(1)), Err(Error::Selected));
    assert_eq!(s.delete(key(1)), Err(Error::Selected));
    assert_eq!(s.verify(200, key(2)), Err(Error::Missing));
    let mut huge = peer(2, false);
    huge.host = vec![0; s.config.maximum_host_bytes + 1];
    assert_eq!(s.update(200, huge), Err(Error::PeerLimit));
    assert_eq!(s.release(99, key(1)), Err(Error::TimeReversed));
    assert_eq!(s.reject(u64::MAX, key(1)), Err(Error::TimeOverflow));
    assert_eq!(s.state(key(1)), before);
    s.release(100, key(1)).unwrap();
    invariant(&s);
    let original = s.clone();
    s.delete(key(1)).unwrap();
    assert_eq!(original.find(key(1)), Some(&peer(1, false)));
    assert!(s.find(key(1)).is_none());
}
#[test]
fn selection_probability_preserves_native_integer_threshold_and_fallback() {
    for probability in [0.0, 1.0] {
        let mut s = Pool::with_seed(
            Config {
                verified_probability: probability,
                ..Default::default()
            },
            &[1; 128],
            [2; 32],
        )
        .unwrap();
        s.update(0, peer(1, true)).unwrap();
        s.update(0, peer(2, false)).unwrap();
        let mut verified = 0;
        for now in 0..10000 {
            let Selection::Selected(p) = s.random_select(now, Target::Both, |_| true).unwrap()
            else {
                panic!()
            };
            verified += usize::from(p.trusted);
            s.release(now, p.id).unwrap();
        }
        if probability == 1.0 {
            assert_eq!(verified, 10000);
        } else {
            assert!(
                (1..40).contains(&verified),
                "native zero probability still selects verified for integer draw zero: {verified}"
            );
        }
        assert_eq!(
            s.random_select(10000, Target::Both, |p| !p.trusted)
                .unwrap(),
            Selection::Selected(peer(2, false))
        );
    }
}
#[test]
fn invalid_configuration_is_bounded_before_entropy_or_allocation() {
    for config in [
        Config {
            verified_buckets: usize::MAX,
            ..Default::default()
        },
        Config {
            maximum_references: 33,
            ..Default::default()
        },
        Config {
            eviction_skew: f64::NAN,
            ..Default::default()
        },
        Config {
            verified_probability: 1.1,
            ..Default::default()
        },
        Config {
            standby_times: vec![],
            ..Default::default()
        },
        Config {
            unverified_source_shard: 3,
            ..Default::default()
        },
        Config {
            maximum_host_bytes: usize::MAX,
            ..Default::default()
        },
    ] {
        assert!(matches!(Pool::new(config), Err(Error::Configuration)));
    }
    assert!(matches!(
        Pool::with_seed(Default::default(), &[0; 4097], [0; 32]),
        Err(Error::Configuration)
    ));
    let s = Pool::new(small()).unwrap();
    assert_eq!(s.count(Count::All, Target::Both), 0);
    invariant(&s);
}
