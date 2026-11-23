use super::*;
use ae_sync::preset_keys::{self, Observed, Ticket};
use ae_types::AccountPublicKey;
use conductor::supervision::Notice;
use serde_json::{Value, json};

fn native() -> Value {
    serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_preset_key_lifecycle.json"
    )))
    .unwrap()
}
fn reply<T: Clone>(ticket: Result<Ticket<T>, preset_keys::Error>) -> Result<T, preset_keys::Error> {
    ticket
        .unwrap()
        .wait_timeout(Duration::from_secs(5))
        .expect("key reply")
}
fn public(value: Result<AccountPublicKey, preset_keys::Error>) -> Value {
    match value {
        Ok(key) => json!({"tuple":[{"atom":"ok"},{"bytes":hex::encode(key.0)}]}),
        Err(error) => json!({"tuple":[{"atom":"error"},{"atom":match error {
            preset_keys::Error::NoActive => "no_active",
            preset_keys::Error::NoCandidate => "no_candidate",
            error => panic!("unexpected key reply {error:?}"),
        }}]}),
    }
}
fn state(keys: &preset_keys::Client) -> Value {
    json!({"active":public(reply(keys.active())),"candidate":public(reply(keys.candidate())),"ready":reply(keys.is_ready()).unwrap()})
}
fn drive(
    c: &mut Conductor<Otp26>,
    now: Instant,
    output: &mut Vec<Output>,
) -> Result<Progress, conductor::Error> {
    let progress = c.step(1_000_000, now)?;
    if let Some(value) = c.take_output() {
        output.push(value);
    }
    Ok(progress)
}
fn replaced(c: &mut Conductor<Otp26>, generation: u64, now: Instant, output: &mut Vec<Output>) {
    let end = Instant::now() + Duration::from_secs(5);
    while c.status().consensus.key_generation < generation
        || c.status().consensus.pending_notices > 0
    {
        drive(c, now, output).unwrap();
        assert!(Instant::now() < end, "{:?}", c.status());
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn native_normal_key_replacement_preserves_configured_keys_and_siblings() {
    let node = Node::new(0);
    let mut c = Conductor::start_with_targets(
        node.db.clone(),
        node.config.clone(),
        [1, 2]
            .into_iter()
            .map(|n| {
                let k = signer(n);
                (k.public_key(), k)
            })
            .collect(),
        config(&node, false),
        Arc::new(ParentBackend {
            missing: AtomicBool::new(false),
        }),
        Targets,
    )
    .unwrap();
    let fixture = native();
    let original = fixture["lifecycle"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["kind"] == "normal")
        .unwrap();
    let binding = c.preset_key_binding();
    assert!(binding.uses_database(&node.db));
    let old = binding.current().unwrap();
    let eoe = c.eoe();
    let votes = c.votes();
    let cache = c.parent();
    let connector = c.connector();
    let importer = c.importer();
    reply(old.promote_candidate(signer(1).public_key())).unwrap();
    reply(old.set_candidate(signer(2).public_key())).unwrap();
    assert_eq!(state(&old), original["before"]);
    c.request_key_restart(1).unwrap();
    let mut output = vec![];
    replaced(&mut c, 2, Instant::now(), &mut output);
    let fresh = binding.current().unwrap();
    assert_eq!(!old.same_worker(&fresh), original["worker_changed"]);
    assert_eq!(state(&fresh), original["after"]);
    assert!(matches!(old.is_ready(), Err(preset_keys::Error::Stopped)));
    assert_eq!(original["old_client"]["atom"], "noproc");
    assert!(c.request_key_restart(1).is_err());
    let sig = reply(fresh.sign_binary(&[0, 1, 2], signer(1).public_key())).unwrap();
    assert_eq!(hex::encode(sig.0), original["signature"]["bytes"]);
    assert!(eoe.same_runtime(&c.eoe()));
    assert!(cache.same_runtime(&c.parent()));
    assert!(Arc::ptr_eq(&importer, &c.importer()));
    assert!(votes.info().accepting);
    connector.barrier(Duration::from_secs(5)).unwrap();
    assert_eq!(c.status().consensus.connector_generation, 1);
    assert_eq!(original["siblings_unchanged"], true);
    assert_eq!(c.status().consensus.attempts, 1);
    assert!(output.iter().any(|o|matches!(o,Output::ConsensusSupervision(Notice::KeysRetired { generation:1,info,previous_failure:None })if info.joined && info.exit==Some(preset_keys::Exit::Shutdown))));
    assert!(old.info().joined);
    let exit = shutdown(c);
    assert!(exit.owner.keys.unwrap().joined);
}

#[test]
fn unread_key_receipts_keep_the_same_capacity_across_composed_replacement() {
    let node = Node::new(0);
    let mut c = start(&node, false);
    let binding = c.preset_key_binding();
    let old = binding.current().unwrap();
    let capacity = old.capacity();
    let mut receipts = vec![];
    for _ in 0..64 {
        let receipt = old.is_ready().unwrap();
        assert_eq!(receipt.wait_timeout(Duration::from_secs(5)), Some(Ok(true)));
        receipts.push(receipt);
    }
    c.request_key_restart(1).unwrap();
    replaced(&mut c, 2, Instant::now(), &mut vec![]);
    let fresh = binding.current().unwrap();
    assert_eq!(capacity.usage().requests, 64);
    assert_eq!(fresh.info().retained_requests, 0);
    assert!(matches!(fresh.is_ready(), Err(preset_keys::Error::Busy)));
    let known: Vec<Observed<_>> = receipts.into_iter().map(Into::into).collect();
    assert_eq!(capacity.usage().requests, 0);
    assert!(reply(fresh.is_ready()).unwrap());
    assert!(known.iter().all(|r| r.try_result() == Some(Ok(true))));
    drop(shutdown(c));
}

#[test]
fn native_key_and_mixed_consensus_exhaustion_obeys_thirty_and_thirty_one_seconds() {
    let reference = native();
    assert_eq!(reference["policy"], json!({"intensity":3,"period":30}));
    for mixed in [false, true] {
        let native = reference["lifecycle"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| {
                r["kind"] == "shared_consensus_exhaustion"
                    && (r["steps"][1]["child"] == "cache") == mixed
            })
            .unwrap();
        for last in [30, 31] {
            let node = Node::new(0);
            let mut c = start(&node, false);
            let now = Instant::now();
            let mut output = vec![];
            for attempt in 1..=4 {
                if mixed && attempt == 2 {
                    c.parent().request_stop();
                } else if mixed && attempt == 3 {
                    c.eoe().request_stop();
                } else {
                    c.request_key_restart(c.status().consensus.key_generation)
                        .unwrap();
                }
                let at = now + Duration::from_secs(if attempt == 4 { last } else { 0 });
                let end = Instant::now() + Duration::from_secs(5);
                let stopped = loop {
                    match drive(&mut c, at, &mut output) {
                        Err(conductor::Error::ConsensusRestartLimit) => break true,
                        Err(error) => panic!("{error:?}"),
                        Ok(_) => (),
                    }
                    if c.status().consensus.attempts == attempt && !c.status().consensus.restarting
                    {
                        break false;
                    }
                    assert!(Instant::now() < end, "{:?}", c.status());
                    std::thread::sleep(Duration::from_millis(1));
                };
                assert_eq!(stopped, attempt == 4 && last == 30);
                if attempt <= 3 {
                    assert_eq!(native["steps"][(attempt - 1) as usize]["replacement"], true);
                }
            }
            assert_eq!(c.status().consensus.attempts, 4);
            if last == 30 {
                assert_eq!(native["root_stopped"], true);
                assert!(c.preset_key_binding().is_closed());
                assert!(c.eoe().is_closed() && c.parent().is_closed());
                assert_eq!(
                    c.step(1_000_000, now + Duration::from_secs(100)),
                    Err(conductor::Error::ConsensusRestartLimit)
                );
            } else {
                assert!(c.status().failure.is_none());
            }
            drop(shutdown(c));
        }
    }
}

#[test]
fn shutdown_retains_joined_key_retirement_before_replacement_publication() {
    let node = Node::new(0);
    let mut c = start(&node, false);
    let old = c.preset_key_binding().current().unwrap();
    c.request_key_restart(1).unwrap();
    let now = Instant::now();
    let end = now + Duration::from_secs(5);
    while c.output().is_none() {
        c.step(1000, now).unwrap();
        assert!(Instant::now() < end);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        matches!(c.output(),Some(Output::ConsensusSupervision(Notice::KeysRetired { generation:1,info,.. }))if info.joined)
    );
    assert_eq!(c.status().consensus.attempts, 0);
    assert!(c.status().consensus.keys_restarting);
    assert!(c.preset_key_binding().current().is_none());
    let exit = shutdown(c);
    assert!(old.info().joined);
    assert!(exit.owner.keys.unwrap().joined);
    assert_eq!(exit.consensus.status.attempts, 0);
}

#[test]
fn saturated_key_quota_pauses_new_production_until_receipts_are_released() {
    let node = Node::new(0);
    let mut c = start(&node, true);
    let keys = c.preset_key_binding().current().unwrap();
    let receipts: Vec<_> = (0..64).map(|_| keys.is_ready().unwrap()).collect();
    let mut wall = 1000;
    let mut now = Instant::now();
    for _ in 0..10 {
        tick(&mut c, &mut wall, &mut now);
        assert!(c.take_output().is_none());
    }
    assert!(c.status().failure.is_none());
    assert_eq!(keys.capacity().usage().requests, 64);
    assert_eq!(c.status().consensus.attempts, 0);
    drop(receipts);
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        tick(&mut c, &mut wall, &mut now);
        if matches!(
            c.take_output(),
            Some(Output::Attempt(production::Completed {
                outcome: production::Outcome::Produced { .. },
                ..
            }))
        ) {
            break;
        }
        assert!(Instant::now() < end, "{:?}", c.status());
    }
    assert!(c.status().failure.is_none());
    drop(shutdown(c));
}

#[test]
fn production_retires_exact_key_work_and_resumes_across_protocols() {
    for profile in [0, 2, 4] {
        for stage in [
            Progress::Production(production::Progress::DecisionRequested),
            Progress::Production(production::Progress::KeyPrepared),
            Progress::Production(production::Progress::Signed(production::Kind::Key)),
        ] {
            let node = Node::new(profile);
            let mut c = start(&node, true);
            let mut wall = 1000;
            let mut now = Instant::now();
            let end = now + Duration::from_secs(30);
            loop {
                let progress = tick(&mut c, &mut wall, &mut now);
                c.take_output();
                if progress == stage {
                    break;
                }
                assert!(
                    Instant::now() < end,
                    "{profile}/{stage:?}: {:?}",
                    c.status()
                );
            }
            let eoe = c.eoe();
            let old = c.preset_key_binding().current().unwrap();
            c.request_key_restart(1).unwrap();
            let mut stopped = None;
            let mut produced = None;
            while produced.is_none() {
                tick(&mut c, &mut wall, &mut now);
                match c.take_output() {
                    Some(Output::Stopped(report)) => stopped = Some(report),
                    Some(Output::Attempt(done))
                        if matches!(done.outcome, production::Outcome::Produced { .. }) =>
                    {
                        produced = Some(done)
                    }
                    _ => (),
                }
                assert!(
                    Instant::now() < end,
                    "{profile}/{stage:?}: {:?}",
                    c.status()
                );
            }
            let report = stopped.expect("old production settled");
            assert!(!report.signing.pending());
            assert!(old.info().joined);
            assert!(!old.same_worker(&c.preset_key_binding().current().unwrap()));
            assert!(eoe.same_runtime(&c.eoe()));
            assert_eq!(c.status().consensus.key_generation, 2);
            assert_eq!(c.status().consensus.attempts, 1);
            node.db.view().unwrap().verify_tip().unwrap();
            drop(shutdown(c));
        }
    }
}
