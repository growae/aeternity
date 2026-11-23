use super::*;
use ae_storage::pool::worker::GossipCache;
static LIVE: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../../fixtures/erlang_hc_live_gossip.json")));
fn drive(c: &mut Coordinator) {
    c.run(&otp_order::profile(&LIVE["profile"]), 1000).unwrap();
    assert_eq!(c.queued_events(), 0);
}
fn publications(c: &mut Coordinator, events: &mut Vec<Vec<u8>>) {
    while let Some(output) = c.pop_output() {
        match output {
            ServiceOutput::Publication(Output::Vote { signed, .. }) => events.push(signed.encode()),
            other => panic!("unexpected output {other:?}"),
        }
    }
}
fn snapshot(c: &Coordinator, cache: &GossipCache, events: &[Vec<u8>], row: &J) {
    assert_eq!(
        c.host().pool().unwrap().len(),
        row["size"].as_u64().unwrap() as usize
    );
    let mut events = events.to_vec();
    events.sort();
    assert_eq!(list(events.iter().map(bin).collect()), row["events"]);
    if row["stats"].is_null() {
        assert!(cache.stats().is_err());
    } else {
        let stats = cache.stats().unwrap();
        assert_eq!(
            stats.hit.to_u64().unwrap(),
            row["stats"]["hit"].as_u64().unwrap()
        );
        assert_eq!(
            stats.miss.to_u64().unwrap(),
            row["stats"]["miss"].as_u64().unwrap()
        );
    }
}
#[test]
fn original_live_cache_pool_and_eoe_lifecycle_matches_signed_publications_and_finalization() {
    let (cache, mut controller) = GossipCache::managed(200, 100_000).unwrap();
    let mut owner = Some(controller.start().unwrap());
    let mut h = host(&[1, 2, 3], ServiceLimits::default());
    add_parent(&mut h);
    let mut c = Coordinator::new(h, 1000.into(), api::Config::default());
    c.set_live_gossip_cache(Some(Arc::new(cache.clone())));
    c.negotiate(negotiation(7)).unwrap();
    drive(&mut c);
    let mut events = vec![];
    for row in LIVE["local"]["steps"].as_array().unwrap() {
        let label = row["label"].as_str().unwrap();
        match label {
            "generated" => (),
            "reset_duplicate" => cache.reset().unwrap(),
            "unavailable_duplicate" => {
                owner.take().unwrap().shutdown();
            }
            "restarted_duplicate" => owner = Some(controller.start().unwrap()),
            "cached_received" | "cached_created" => (),
            other => panic!("unknown native step {other}"),
        }
        if label != "generated" {
            let tx = SignedTransaction::<Transaction>::decode(
                &hex::decode(row["signed"].as_str().unwrap()).unwrap(),
            )
            .unwrap();
            let event = if row["event"] == "tx_created" {
                hc_votes::Event::Created
            } else {
                hc_votes::Event::Received
            };
            let result = c.admit(tx, event).unwrap();
            if label.starts_with("cached") {
                assert_eq!(result, PushOutcome::Cached);
            } else {
                assert!(matches!(
                    result,
                    PushOutcome::Accepted {
                        inserted: false,
                        ..
                    }
                ));
            }
            drive(&mut c);
        }
        publications(&mut c, &mut events);
        snapshot(&c, &cache, &events, row);
    }
    c.request_finalize(77, Arc::new(trees(5)), c.now()).unwrap();
    drive(&mut c);
    let mut finalized = false;
    while let Some(output) = c.pop_output() {
        if let ServiceOutput::Finalized {
            request: 77,
            replies,
        } = output
        {
            assert_eq!(pair_json(&replies), LIVE["local"]["result"]);
            finalized = true;
        }
    }
    assert!(finalized);
    drop(owner);
}
#[test]
fn original_native_rejections_record_misses_and_both_events_use_cache_fast_path() {
    let cache = GossipCache::default();
    let mut c = Coordinator::new(
        host(&[], ServiceLimits::default()),
        1000.into(),
        Default::default(),
    );
    c.set_live_gossip_cache(Some(Arc::new(cache.clone())));
    for row in LIVE["invalid"]["steps"].as_array().unwrap() {
        let tx = SignedTransaction::<Transaction>::decode(
            &hex::decode(row["signed"].as_str().unwrap()).unwrap(),
        )
        .unwrap();
        let event = if row["event"] == "tx_created" {
            hc_votes::Event::Created
        } else {
            hc_votes::Event::Received
        };
        let reason = match c.admit(tx, event) {
            Ok(PushOutcome::Cached) => "ok",
            Err(api::Error::Host(local_host::Error::Pool(hc_votes::Error::OnlyHcVoteAllowed))) => {
                "only_hc_vote_tx_allowed"
            }
            Err(api::Error::Host(local_host::Error::Pool(hc_votes::Error::Signature(_)))) => {
                "signature_check_failed"
            }
            other => panic!("unexpected admission {other:?}"),
        };
        assert_eq!(reason, row["result"]);
        snapshot(&c, &cache, &[], row);
        assert!(c.pop_output().is_none());
    }
}
#[test]
fn operational_pool_outbox_and_late_routing_failures_do_not_cache_failed_admissions() {
    for case in 0..5 {
        let cache = GossipCache::default();
        let mut limits = ServiceLimits::default();
        let mut config = api::Config::default();
        match case {
            0 => limits.pool.maximum_entries = 0,
            1 => limits.maximum_outbox_entries = 0,
            2 => config.maximum_outputs = 0,
            3 => config.maximum_queued_events = 1,
            4 => config.maximum_output_bytes = 1,
            _ => unreachable!(),
        }
        let mut c = Coordinator::new(host(&[], limits), 1000.into(), config);
        c.set_live_gossip_cache(Some(Arc::new(cache.clone())));
        let tx = vote(Mode::Fork, VoteType::Vote, 2, false);
        assert!(c.admit(tx.clone(), hc_votes::Event::Received).is_err());
        assert_eq!(cache.stats().unwrap(), Default::default());
        assert!(c.host().pool().unwrap().is_empty());
        assert_eq!(c.queued_events(), 0);
        assert!(c.pop_output().is_none());
        // Restore the failed local capacity. A fresh coordinator is needed for
        // immutable host limits; the live cache remains the same exact source.
        if case < 2 {
            c = Coordinator::new(
                host(&[], ServiceLimits::default()),
                1000.into(),
                Default::default(),
            );
            c.set_live_gossip_cache(Some(Arc::new(cache.clone())));
        } else {
            c.set_config(Default::default());
        }
        assert!(matches!(
            c.admit(tx, hc_votes::Event::Received).unwrap(),
            PushOutcome::Accepted { inserted: true, .. }
        ));
        assert_eq!(cache.stats().unwrap().miss, 1.into());
        assert_eq!(cache.stats().unwrap().hit, 0.into());
        assert_eq!(c.host().pool().unwrap().len(), 1);
    }
}
#[test]
fn generated_callback_retry_keeps_cache_pool_and_publications_atomic() {
    let cache = GossipCache::default();
    let mut c = Coordinator::new(
        host(&[1, 2, 3], ServiceLimits::default()),
        1000.into(),
        api::Config {
            maximum_outputs: 0,
            ..Default::default()
        },
    );
    c.set_live_gossip_cache(Some(Arc::new(cache.clone())));
    c.negotiate(negotiation(7)).unwrap();
    assert!(c.step(&otp_order::profile(&LIVE["profile"])).is_err());
    assert_eq!(cache.stats().unwrap(), Default::default());
    assert!(c.host().pool().unwrap().is_empty());
    assert_eq!(c.queued_events(), 2);
    assert_eq!(c.machine(Mode::Length).state(), State::AwaitEpoch);
    c.set_config(Default::default());
    drive(&mut c);
    let mut events = vec![];
    publications(&mut c, &mut events);
    snapshot(&c, &cache, &events, &LIVE["local"]["steps"][0]);
}

#[test]
fn native_child_restart_keeps_the_admitted_vote_in_the_shared_cache() {
    let cache = GossipCache::default();
    let mut c = Coordinator::new(
        host(&[], ServiceLimits::default()),
        1000.into(),
        Default::default(),
    );
    c.set_live_gossip_cache(Some(Arc::new(cache.clone())));
    let tx = vote(Mode::Fork, VoteType::Proposal, 2, true);
    assert!(matches!(
        c.admit(tx.clone(), hc_votes::Event::Received).unwrap(),
        PushOutcome::Accepted { inserted: true, .. }
    ));
    drive(&mut c);
    let mut restarted = false;
    let mut publications = 0;
    while let Some(output) = c.pop_output() {
        match output {
            ServiceOutput::Restarted {
                mode: Mode::Fork, ..
            } => restarted = true,
            ServiceOutput::Publication(Output::Vote { .. }) => publications += 1,
            other => panic!("unexpected output {other:?}"),
        }
    }
    assert!(restarted);
    assert_eq!(publications, 1);
    assert_eq!(c.host().pool().unwrap().len(), 1);
    assert_eq!(
        c.admit(tx, hc_votes::Event::Created).unwrap(),
        PushOutcome::Cached
    );
    assert_eq!(cache.stats().unwrap().miss, 1.into());
    assert_eq!(cache.stats().unwrap().hit, 1.into());
    drive(&mut c);
    assert!(c.pop_output().is_none());
}
