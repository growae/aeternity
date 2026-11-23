use super::*;
use controls::until;

fn replaced(
    c: &mut Conductor<Otp26>,
    wall: &mut u64,
    now: &mut Instant,
) -> (conductor::session::Status, Vec<production::Stopped>) {
    let end = Instant::now() + Duration::from_secs(15);
    let mut stopped = vec![];
    loop {
        assert!(Instant::now() < end, "{:?}", c.status());
        tick(c, wall, now);
        match c.take_output() {
            Some(Output::Stopped(report)) => stopped.push(*report),
            Some(Output::SessionReplaced {
                retired,
                generation,
            }) => {
                assert_eq!(generation, retired.generation + 1);
                assert_eq!(c.status().session_generation, generation);
                return (retired, stopped);
            }
            Some(output) => panic!("unexpected output {output:?}"),
            None => (),
        }
    }
}

#[test]
fn logical_replacement_restores_autostart_and_preserves_live_service_generations() {
    let native: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_hc_session_options.json"
    )))
    .unwrap();
    for row in native["cases"].as_array().unwrap() {
        let autostart = row["configured_autostart"].as_bool().unwrap();
        let node = Node::new(0);
        let mut c = start(&node, autostart);
        assert_eq!(
            serde_json::json!(c.status().producing),
            row["before_control"]
        );
        if autostart {
            c.stop_production().unwrap();
        } else {
            c.start_production().unwrap();
        }
        let eoe = c.eoe();
        let pool = c.pool();
        let votes = c.votes();
        let parent = c.parent();
        let connector = c.connector();
        let importer = c.importer();
        let keys = c.preset_key_binding().current().unwrap();
        keys.set_candidate(signer(12).public_key())
            .unwrap()
            .wait_timeout(Duration::from_secs(3))
            .unwrap()
            .unwrap();
        let cursor = c.status().cursor;
        c.restart_production_session(1).unwrap();
        assert!(c.status().session_replacing && !c.status().producing);
        assert_eq!(c.start_production(), Err(conductor::Error::Backpressured));
        assert_eq!(
            c.restart_production_session(1),
            Err(conductor::Error::Backpressured)
        );
        let (previous, stopped) = replaced(&mut c, &mut 1000, &mut Instant::now());
        assert_eq!(serde_json::json!(previous.producing), row["after_control"]);
        assert!(stopped.is_empty());
        let status = c.status();
        assert_eq!(serde_json::json!(status.producing), row["after_restart"]);
        assert_eq!(
            serde_json::json!(status.attempts),
            row["fresh_worker_count"]
        );
        assert_eq!(status.attempts, 0);
        assert_eq!(status.stops, 0);
        assert_eq!(status.deadline, None);
        assert_eq!(status.cursor, cursor);
        assert_eq!(status.consensus.attempts, 0);
        assert_eq!(status.pool.restart_attempts, 0);
        assert_eq!(status.votes.restart_attempts, 0);
        assert!(eoe.same_runtime(&c.eoe()));
        assert!(parent.same_runtime(&c.parent()));
        assert!(pool.same_worker(&c.pool()));
        assert!(votes.info().accepting);
        assert!(keys.same_worker(&c.preset_key_binding().current().unwrap()));
        assert_eq!(
            keys.candidate()
                .unwrap()
                .wait_timeout(Duration::from_secs(3)),
            Some(Ok(signer(12).public_key()))
        );
        assert!(Arc::ptr_eq(&importer, &c.importer()));
        connector.barrier(Duration::from_secs(3)).unwrap();
        assert_eq!(status.consensus.connector_generation, 1);
        assert_eq!(
            c.restart_production_session(1),
            Err(conductor::Error::StaleSession {
                requested: 1,
                current: 2
            })
        );
        assert!(!c.status().session_replacing);
        shutdown(c);
    }
}

#[test]
fn session_retirement_settles_candidate_and_signing_work_across_protocols() {
    for profile in [0, 2, 4] {
        for stage in [
            Progress::Production(production::Progress::DecisionRequested),
            Progress::Production(production::Progress::KeyPrepared),
            Progress::Production(production::Progress::Signed(production::Kind::Key)),
        ] {
            let node = Node::new(profile);
            let mut c = start(&node, true);
            let before = node.db.view().unwrap().status();
            let mut wall = 1000;
            let mut now = Instant::now();
            until(&mut c, &mut wall, &mut now, stage);
            let eoe = c.eoe();
            let keys = c.preset_key_binding().current().unwrap();
            c.restart_production_session(1).unwrap();
            let (old, stopped) = replaced(&mut c, &mut wall, &mut now);
            assert_eq!(old.attempts, 1);
            assert_eq!(stopped.len(), 1);
            assert!(!stopped[0].signing.pending());
            assert!(stopped[0].admission.is_none());
            assert_eq!(node.db.view().unwrap().status(), before);
            assert!(eoe.same_runtime(&c.eoe()));
            assert!(keys.same_worker(&c.preset_key_binding().current().unwrap()));
            assert_eq!(c.status().attempts, 0);
            let end = Instant::now() + Duration::from_secs(15);
            loop {
                assert!(
                    Instant::now() < end,
                    "{profile}/{stage:?}: {:?}",
                    c.status()
                );
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
            }
            assert_eq!(c.status().session_generation, 2);
            assert_eq!(c.status().consensus.attempts, 0);
            node.db.view().unwrap().verify_tip().unwrap();
            shutdown(c);
        }
    }
}

#[test]
fn unread_committed_admission_survives_session_retirement_and_is_not_replayed() {
    let node = Node::new(1);
    let mut c = start(&node, true);
    let mut wall = 1000;
    let mut now = Instant::now();
    until(
        &mut c,
        &mut wall,
        &mut now,
        Progress::Production(production::Progress::Submitted(production::Kind::Key)),
    );
    let hash = c
        .attempt()
        .unwrap()
        .pending_block()
        .unwrap()
        .hash()
        .unwrap();
    wait(|| node.db.view().unwrap().status().tip == Some(hash));
    let committed = node.db.view().unwrap().status();
    c.restart_production_session(1).unwrap();
    let (_, stopped) = replaced(&mut c, &mut wall, &mut now);
    assert_eq!(stopped.len(), 1);
    assert_eq!(stopped[0].pending_hash, Some(hash));
    assert_eq!(
        stopped[0]
            .admission
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .accepted[0]
            .hash,
        hash
    );
    assert_eq!(node.db.view().unwrap().status(), committed);
    until(&mut c, &mut wall, &mut now, Progress::Started);
    assert_eq!(c.attempt().unwrap().parent(), hash);
    assert_eq!(c.status().attempts, 1);
    assert_eq!(node.db.view().unwrap().status(), committed);
    shutdown(c);
}

#[test]
fn unread_stopped_report_backpressures_session_replacement_and_shutdown_retains_request() {
    let node = Node::new(0);
    let mut c = start(&node, true);
    let mut wall = 1000;
    let mut now = Instant::now();
    until(&mut c, &mut wall, &mut now, Progress::Started);
    c.restart_production_session(1).unwrap();
    let end = Instant::now() + Duration::from_secs(10);
    while c.output().is_none() {
        assert!(Instant::now() < end);
        tick(&mut c, &mut wall, &mut now);
    }
    assert!(matches!(c.output(), Some(Output::Stopped(_))));
    for _ in 0..32 {
        assert_eq!(tick(&mut c, &mut wall, &mut now), Progress::Output);
    }
    assert_eq!(c.status().session_generation, 1);
    assert!(c.status().session_replacing);
    assert!(c.pool().info().accepting && c.votes().info().accepting);
    let exit = shutdown(c);
    assert_eq!(exit.session_retirement.unwrap().generation, 1);
    assert!(matches!(exit.output, Some(Output::Stopped(_))));
    assert!(exit.attempt.is_none() && exit.retiring.is_none());
}

#[test]
fn consensus_restart_history_survives_logical_session_replacements() {
    let node = Node::new(0);
    let mut c = start(&node, false);
    let mut now = Instant::now();
    let mut wall = 1000;
    for attempt in 1..=4 {
        c.request_key_restart(c.status().consensus.key_generation)
            .unwrap();
        let end = Instant::now() + Duration::from_secs(10);
        loop {
            let result = c.step(wall, now);
            c.take_output();
            if attempt == 4 && result == Err(conductor::Error::ConsensusRestartLimit) {
                break;
            }
            result.unwrap();
            if c.status().consensus.attempts == attempt && !c.status().consensus.restarting {
                break;
            }
            assert!(Instant::now() < end, "{:?}", c.status());
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(c.status().consensus.attempts, attempt);
        if attempt < 4 {
            let generation = c.status().session_generation;
            c.restart_production_session(generation).unwrap();
            replaced(&mut c, &mut wall, &mut now);
            assert_eq!(c.status().consensus.attempts, attempt);
        }
    }
    assert_eq!(c.status().session_generation, 4);
    assert_eq!(
        c.status().failure,
        Some(conductor::Error::ConsensusRestartLimit)
    );
    assert_eq!(
        c.restart_production_session(4),
        Err(conductor::Error::ConsensusRestartLimit)
    );
    shutdown(c);
}

#[test]
fn shared_worker_history_survives_logical_session_replacements() {
    let node = Node::new(0);
    let mut c = start(&node, false);
    let mut now = Instant::now();
    let mut wall = 1000;
    for attempt in 1..=6 {
        c.request_pool_restart(c.status().pool.generation).unwrap();
        let end = Instant::now() + Duration::from_secs(10);
        loop {
            let result = c.step(wall, now);
            c.take_output();
            if attempt == 6 && result == Err(conductor::Error::PoolRestartLimit) {
                break;
            }
            result.unwrap();
            if c.status().pool.restart_attempts == attempt && !c.status().pool.restarting {
                break;
            }
            assert!(Instant::now() < end, "{:?}", c.status());
            std::thread::sleep(Duration::from_millis(1));
        }
        if attempt < 6 {
            let generation = c.status().session_generation;
            c.restart_production_session(generation).unwrap();
            replaced(&mut c, &mut wall, &mut now);
            assert_eq!(c.status().pool.restart_attempts, attempt);
            assert_eq!(c.status().votes.restart_attempts, attempt);
        }
    }
    assert_eq!(c.status().session_generation, 6);
    assert_eq!(c.status().failure, Some(conductor::Error::PoolRestartLimit));
    shutdown(c);
}
