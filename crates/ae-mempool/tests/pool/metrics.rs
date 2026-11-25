use super::*;

#[test]
fn original_public_admission_updates_precede_insertion_and_exclude_cached_inputs() {
    use pool::{Admission, AdmissionEvent, Event, PushOptions};
    let data: J =
        serde_json::from_str(include_str!("../fixtures/erlang_metrics_pool.json")).unwrap();
    let mut compared = 0;
    let mut boundaries = BTreeMap::new();
    for row in data["rows"].as_array().unwrap() {
        let label = row["label"].as_str().unwrap();
        // Forced admission occurs before storage reads in the actual service.
        // Storage exceptions occur before policy, and arbitrary Erlang terms
        // cannot be constructed as Signed. These boundaries are explicit.
        if matches!(
            label,
            "forced" | "forced_unavailable" | "illegal" | "storage_failure"
        ) {
            *boundaries.entry(label).or_insert(0) += 1;
            continue;
        }
        let mut input = row.clone();
        input["names"] = json!([]);
        let trees = state(&input);
        let network = ae_protocol::NetworkProfile::mainnet();
        let env = Admission {
            trees: &trees,
            network: &network,
            protocol: Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
            consensus: ae_protocol::ConsensusType::Pow,
            height: 10,
            record: Default::default(),
        };
        let mut cfg = config();
        cfg.allow_reentry = label == "reentry";
        let mut limits = Limits::default();
        if label == "server_failure" {
            limits.entries = 0;
        }
        let mut pool = Pool::new(cfg, limits, 10);
        let mut updates = vec![];
        let result = pool.push_observed(
            tx(&row["tx"]),
            PushOptions {
                event: Event::Received,
                location: match label {
                    "known" => Location::Pool,
                    "deleted" | "reentry" => Location::Deleted,
                    "accepted" => Location::Block(Hash256([99; 32])),
                    _ => Location::Unknown,
                },
                gossip_cached: matches!(label, "cached" | "cached_bad"),
            },
            &env,
            |event| {
                updates.push(json!([
                    "update",
                    match event {
                        AdmissionEvent::Submit => "push",
                        AdmissionEvent::Ignore => "push.ignore",
                        AdmissionEvent::Reject => "push.error",
                    },
                    1
                ]));
            },
        );
        let expected: Vec<_> = row["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e[0] == "update")
            .cloned()
            .collect();
        assert_eq!(updates, expected, "{label}: {}", row["protocol"]);
        if label == "server_failure" {
            // Native fake insertion returns an error; Rust reaches a real
            // insertion capacity failure after the same Submit observation.
            assert_eq!(result, Err(pool::Error::ResourceLimit));
            assert!(pool.is_empty());
            assert_eq!(row["result"], "insertion_failure");
        } else {
            let got = match result {
                Ok(_) => "ok",
                Err(pool::Error::AlreadyKnown) => "already_known",
                Err(pool::Error::AlreadyAccepted) => "already_accepted",
                Err(ref e) => error_name(e),
            };
            assert_eq!(got, row["result"], "{label}");
        }
        compared += 1;
    }
    assert_eq!(compared, 27);
    assert_eq!(
        boundaries,
        BTreeMap::from([
            ("forced", 3),
            ("forced_unavailable", 3),
            ("illegal", 3),
            ("storage_failure", 3)
        ])
    );
}

#[test]
fn operational_precheck_limits_emit_no_native_rejection_or_illegal_counter() {
    use pool::{Admission, Event, PushOptions};
    let row = DATA["admission"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["result"] == "ok")
        .unwrap();
    let trees = state(row);
    let network = ae_protocol::NetworkProfile::mainnet();
    let env = Admission {
        trees: &trees,
        network: &network,
        protocol: Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
        consensus: ae_protocol::ConsensusType::Pow,
        height: 10,
        record: Default::default(),
    };
    let mut pool = Pool::new(
        config(),
        Limits {
            record_bytes: 0,
            ..Default::default()
        },
        10,
    );
    assert_eq!(
        pool.push_observed(
            tx(&row["tx"]),
            PushOptions {
                event: Event::Received,
                location: Location::Unknown,
                gossip_cached: false
            },
            &env,
            |_| panic!("operational precheck is not a native admission result")
        ),
        Err(pool::Error::Record(ae_tx::storage::Error::ResourceLimit))
    );
    assert!(pool.is_empty());
}
