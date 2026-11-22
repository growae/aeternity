use super::*;
use ae_sync::chain::hyperchain::{delivery, eoe::owner::candidate as build, packing};

fn until(c: &mut Conductor<Otp26>, wall: &mut u64, clock: &mut Instant, target: Progress) {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            Instant::now() < end,
            "waiting for {target:?}: {:?}",
            c.status()
        );
        let p = tick(c, wall, clock);
        assert!(c.output().is_none(), "{target:?}: {:?}", c.output());
        if p == target {
            break;
        }
    }
}
fn settled(c: &mut Conductor<Otp26>, wall: &mut u64, clock: &mut Instant) -> production::Stopped {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(Instant::now() < end, "settling {:?}", c.status());
        tick(c, wall, clock);
        if let Some(o) = c.take_output() {
            let Output::Stopped(s) = o else {
                panic!("{o:?}")
            };
            assert!(!c.status().stopping);
            return *s;
        }
    }
}
fn resume(c: &mut Conductor<Otp26>, wall: &mut u64, clock: &mut Instant) {
    c.start_production().unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(Instant::now() < end, "resuming {:?}", c.status());
        tick(c, wall, clock);
        if let Some(o) = c.take_output() {
            match o {
                Output::Attempt(done)
                    if matches!(done.outcome, production::Outcome::Produced { .. }) =>
                {
                    break;
                }
                Output::Attempt(_) => (),
                _ => panic!("{o:?}"),
            }
        }
    }
}
#[test]
fn stop_at_candidate_boundaries_retires_without_new_submissions_and_resumes() {
    use build::Progress as M;
    use production::Progress as P;
    let stages = [
        Progress::Started,
        Progress::Production(P::DecisionRequested),
        Progress::Production(P::Waiting),
        Progress::Production(P::Decided),
        Progress::Production(P::Micro(M::Requested { refresh: false })),
        Progress::Production(P::Micro(M::Prepared)),
        Progress::Production(P::Micro(M::Effects(delivery::Progress::Idle))),
        Progress::Production(P::Micro(M::Packing(packing::Progress::Waiting))),
        Progress::Production(P::Micro(M::Packing(packing::Progress::Selected))),
        Progress::Production(P::KeyPrepared),
        Progress::Production(P::Effects(delivery::Progress::Idle)),
        Progress::Production(P::Signed(production::Kind::Key)),
    ];
    for target in stages {
        let node = Node::new(1);
        let mut c = start(&node, true);
        let mut wall = 1000;
        let mut clock = Instant::now();
        let before = node.db.view().unwrap().status();
        until(&mut c, &mut wall, &mut clock, target);
        if target == Progress::Production(P::Micro(M::Packing(packing::Progress::Waiting))) {
            assert_eq!(tick(&mut c, &mut wall, &mut clock), target);
        }
        c.stop_production().unwrap();
        c.stop_production().unwrap();
        assert!(!c.status().producing && c.status().stopping);
        assert!(c.attempt().is_none());
        let report = settled(&mut c, &mut wall, &mut clock);
        assert!(report.admission.is_none(), "{target:?}: {report:?}");
        assert_eq!(Some(report.parent), before.tip);
        assert_eq!(node.db.view().unwrap().status(), before);
        assert_eq!(c.status().stops, 1);
        let submitted = c.pool().info().submitted;
        for _ in 0..20 {
            tick(&mut c, &mut wall, &mut clock);
            assert!(c.output().is_none() && c.attempt().is_none());
        }
        assert_eq!(c.pool().info().submitted, submitted);
        assert!(c.pool().info().accepting && c.votes().info().accepting && !c.parent().is_closed());
        if target == Progress::Production(P::Micro(M::Packing(packing::Progress::Waiting))) {
            assert!(
                report
                    .candidate
                    .unwrap()
                    .packing
                    .unwrap()
                    .selection
                    .unwrap()
                    .is_ok()
            );
        }
        resume(&mut c, &mut wall, &mut clock);
        assert_eq!(c.status().attempts, 2, "{target:?}");
        shutdown(c);
    }
}
#[test]
fn stop_with_unread_committed_key_reports_it_once_and_resume_uses_current_tip() {
    let node = Node::new(1);
    let mut c = start(&node, true);
    let mut wall = 1000;
    let mut clock = Instant::now();
    until(
        &mut c,
        &mut wall,
        &mut clock,
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
    c.stop_production().unwrap();
    c.start_production().unwrap();
    assert!(c.status().producing && c.status().stopping);
    let report = settled(&mut c, &mut wall, &mut clock);
    assert_eq!(report.pending_hash, Some(hash));
    assert_eq!(report.admission.unwrap().unwrap().accepted[0].hash, hash);
    assert_eq!(c.status().attempts, 1);
    until(&mut c, &mut wall, &mut clock, Progress::Started);
    assert_eq!(c.attempt().unwrap().parent(), hash);
    c.stop_production().unwrap();
    settled(&mut c, &mut wall, &mut clock);
    assert_eq!(node.db.view().unwrap().status().tip, Some(hash));
    shutdown(c);
}
#[test]
fn stop_at_epoch_finalization_and_micro_receipts_preserves_the_admitted_prefix() {
    for target in [
        Progress::Production(production::Progress::Micro(build::Progress::Requested {
            refresh: false,
        })),
        Progress::Production(production::Progress::Signed(production::Kind::Micro)),
        Progress::Production(production::Progress::Submitted(production::Kind::Micro)),
        Progress::Production(production::Progress::Accepted(production::Kind::Micro)),
    ] {
        let node = Node::new(1);
        let storage = inserted(&node);
        for b in node.blocks().iter().take(4) {
            storage.insert(b, 1_000_000, Origin::Gossip).unwrap();
        }
        drop(storage);
        let before = node.db.view().unwrap().status();
        let mut c = start(&node, true);
        let mut wall = 25003;
        let mut clock = Instant::now();
        until(&mut c, &mut wall, &mut clock, target);
        if target == Progress::Production(production::Progress::Submitted(production::Kind::Micro))
        {
            let hash = c
                .attempt()
                .unwrap()
                .pending_block()
                .unwrap()
                .hash()
                .unwrap();
            wait(|| node.db.view().unwrap().status().tip == Some(hash));
        }
        c.stop_production().unwrap();
        let s = settled(&mut c, &mut wall, &mut clock);
        if matches!(
            target,
            Progress::Production(
                production::Progress::Submitted(_) | production::Progress::Accepted(_)
            )
        ) {
            let hash = s.micro.unwrap().hash;
            assert_eq!(s.parent, hash);
            assert_eq!(node.db.view().unwrap().status().tip, Some(hash));
            assert_eq!(
                node.db.view().unwrap().status().revision,
                before.revision + 1
            );
        } else {
            assert!(s.micro.is_none());
            assert_eq!(node.db.view().unwrap().status(), before);
        }
        assert_eq!(c.status().completed, 0);
        assert_eq!(c.status().attempts, 1);
        resume(&mut c, &mut wall, &mut clock);
        shutdown(c);
    }
}
#[tokio::test]
async fn stopped_producer_keeps_external_admission_and_top_delivery_live() {
    let node = Node::new(1);
    let mut c = start(&node, true);
    let mut wall = 1000;
    let mut clock = Instant::now();
    until(&mut c, &mut wall, &mut clock, Progress::Started);
    c.stop_production().unwrap();
    settled(&mut c, &mut wall, &mut clock);
    for b in node.blocks().iter().take(2) {
        c.importer()
            .import_block(b.encode().unwrap(), 1_000_000)
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    let top = node.db.view().unwrap().status().tip.unwrap();
    let end = Instant::now() + Duration::from_secs(10);
    while c.parent().snapshot().cache.child_top().hash != top {
        assert!(Instant::now() < end);
        tick(&mut c, &mut wall, &mut clock);
    }
    assert_eq!(c.status().attempts, 1);
    assert!(c.pool().info().accepting && c.votes().info().accepting);
    c.start_production().unwrap();
    until(&mut c, &mut wall, &mut clock, Progress::Started);
    assert_eq!(c.attempt().unwrap().parent(), top);
    shutdown(c);
}
#[test]
fn generation_shutdown_keeps_a_retiring_attempt_and_unread_admission() {
    let node = Node::new(1);
    let mut c = start(&node, true);
    let mut wall = 1000;
    let mut clock = Instant::now();
    until(
        &mut c,
        &mut wall,
        &mut clock,
        Progress::Production(production::Progress::Submitted(production::Kind::Key)),
    );
    let hash = c
        .attempt()
        .unwrap()
        .pending_block()
        .unwrap()
        .hash()
        .unwrap();
    c.stop_production().unwrap();
    let s = shutdown(c);
    assert!(s.attempt.is_none());
    assert_eq!(s.retiring.unwrap().report().pending_hash, Some(hash));
    assert!(s.admission.is_some());
}

struct GateClock {
    gate: Arc<(Mutex<(bool, bool)>, std::sync::Condvar)>,
    time: ManualClock,
}
impl conductor::runtime::Clock for GateClock {
    fn now(&self) -> Result<(u64, Instant), conductor::Error> {
        let (lock, wake) = &*self.gate;
        let mut gate = lock.lock().unwrap();
        gate.0 = true;
        wake.notify_all();
        while !gate.1 {
            gate = wake.wait(gate).unwrap();
        }
        conductor::runtime::Clock::now(&self.time)
    }
}
#[test]
fn runtime_controls_are_bounded_ordered_and_work_under_output_pressure() {
    let node = Node::new(1);
    let c = start(&node, false);
    let time = ManualClock(Arc::new(Mutex::new((1000, Instant::now()))));
    let gate = Arc::new((Mutex::new((false, false)), std::sync::Condvar::new()));
    let runtime = match conductor::runtime::Runtime::spawn(
        c,
        GateClock {
            gate: gate.clone(),
            time: time.clone(),
        },
        conductor::runtime::Settings {
            poll: Duration::from_millis(1),
            outputs: 1,
        },
    ) {
        Ok(r) => r,
        Err(e) => panic!("{}", e.error),
    };
    let handle = runtime.handle();
    wait(|| gate.0.lock().unwrap().0);
    for _ in 0..32 {
        handle.start_production().unwrap();
        handle.stop_production().unwrap();
    }
    let controls = handle.controls();
    assert_eq!(
        (controls.submitted, controls.applied, controls.pending),
        (64, 0, 64)
    );
    let saturated = handle.start_production();
    // Always release the clock before any assertion can unwind the runtime.
    gate.0.lock().unwrap().1 = true;
    gate.1.notify_all();
    assert_eq!(saturated, Err(conductor::Error::Backpressured));
    wait(|| handle.controls().applied == 64);
    assert!(!handle.status().producing);
    assert_eq!(handle.status().attempts, 0);
    handle.start_production().unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    while handle.status().completed < 2 {
        assert!(Instant::now() < end, "{:?}", handle.status());
        assert!(handle.status().failure.is_none(), "{:?}", handle.status());
        if let Some(d) = handle.status().deadline {
            let mut t = time.0.lock().unwrap();
            t.0 += d.saturating_duration_since(t.1).as_millis() as u64;
            t.1 = d;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    handle.stop_production().unwrap();
    handle.start_production().unwrap();
    handle.stop_production().unwrap();
    wait(|| handle.controls().applied == 68);
    assert!(!handle.status().producing);
    assert_eq!(handle.status().completed, 2);
    let s = runtime.shutdown().unwrap();
    assert_eq!(s.queued_outputs.len(), 1);
    assert!(matches!(s.output, Some(Output::Attempt(_))));
    assert_eq!(handle.stop_production(), Err(conductor::Error::Closed));
    assert!(s.pool.is_ok() && s.parent.is_ok() && s.connector_joined);
}
#[test]
fn pending_stopped_output_survives_repeated_controls_and_blocks_a_fresh_attempt() {
    let node = Node::new(1);
    let mut c = start(&node, true);
    let mut wall = 1000;
    let mut clock = Instant::now();
    until(&mut c, &mut wall, &mut clock, Progress::Started);
    c.stop_production().unwrap();
    while c.output().is_none() {
        tick(&mut c, &mut wall, &mut clock);
    }
    for _ in 0..10 {
        c.start_production().unwrap();
        assert_eq!(tick(&mut c, &mut wall, &mut clock), Progress::Output);
        c.stop_production().unwrap();
    }
    assert!(matches!(c.output(), Some(Output::Stopped(_))));
    assert_eq!(c.status().attempts, 1);
    assert_eq!(c.status().stops, 1);
    let s = shutdown(c);
    assert!(matches!(s.output, Some(Output::Stopped(_))));
}

#[test]
fn original_conductor_start_stop_sequences_match_producer_control_state() {
    let data: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/erlang_hc_control.json")).unwrap();
    let mut calls = 0;
    for case in data["cases"].as_array().unwrap() {
        let node = Node::new(1);
        let mut config = config(&node, case["initial_running"].as_bool().unwrap());
        config.block_producer = case["eligible"].as_bool().unwrap();
        let mut c = new(&node, config).unwrap();
        let mut wall = 1000;
        let mut clock = Instant::now();
        if case["seed_worker"] == true {
            until(&mut c, &mut wall, &mut clock, Progress::Started);
        }
        for step in case["steps"].as_array().unwrap() {
            let result = match step["action"].as_str().unwrap() {
                "start" => c.start_production(),
                "stop" => c.stop_production(),
                _ => panic!(),
            };
            let expected = if step["reply"] == "ok" {
                Ok(())
            } else {
                Err(conductor::Error::NotBlockProducer)
            };
            assert_eq!(result, expected);
            assert_eq!(c.status().producing, step["running"].as_bool().unwrap());
            if c.status().stopping {
                settled(&mut c, &mut wall, &mut clock);
            }
            assert!(
                c.pool().info().accepting && c.votes().info().accepting && !c.parent().is_closed()
            );
            calls += 1;
        }
        shutdown(c);
    }
    assert_eq!(calls, 15);
}
#[test]
fn stopping_does_not_clear_an_unknown_dependency_failure_or_enable_replay() {
    let node = Node::new(1);
    let mut c = start(&node, true);
    let mut wall = 1000;
    let mut clock = Instant::now();
    until(
        &mut c,
        &mut wall,
        &mut clock,
        Progress::Production(production::Progress::Submitted(production::Kind::Key)),
    );
    c.stop_production().unwrap();
    c.importer().stop();
    let failure = c.step(wall, clock).unwrap_err();
    assert_eq!(
        failure,
        conductor::Error::Importer(ae_sync::chain::Error::Closed)
    );
    c.stop_production().unwrap();
    assert_eq!(c.start_production(), Err(failure.clone()));
    assert_eq!(c.step(wall, clock), Err(failure));
    let s = shutdown(c);
    assert!(s.retiring.is_some() && s.admission.is_some());
    assert_eq!(s.status.attempts, 1);
}

#[test]
fn stop_discards_unsent_pool_failures_but_observes_an_accepted_failure_batch_once() {
    let data: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/erlang_hc_pool_packing.json"
    ))
    .unwrap();
    let row = data["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["corpus_index"] == 1)
        .unwrap();
    let case = &row["parents"][0]["cases"][2];
    for submit in [false, true] {
        let node = Node::new(1);
        let mut c = start(&node, true);
        for tx in case["transactions"].as_array().unwrap() {
            c.pool()
                .force_push(Arc::new(
                    SignedTransaction::decode(&hex::decode(tx.as_str().unwrap()).unwrap()).unwrap(),
                ))
                .unwrap()
                .wait()
                .unwrap();
        }
        let before = node.db.view().unwrap().status();
        let mut wall = 1000;
        let mut clock = Instant::now();
        let end = Instant::now() + Duration::from_secs(10);
        let rejected = loop {
            assert!(Instant::now() < end);
            if let Progress::Production(production::Progress::Micro(build::Progress::Packing(
                packing::Progress::Applied { rejected, .. },
            ))) = tick(&mut c, &mut wall, &mut clock)
            {
                assert!(rejected > 0);
                break rejected;
            }
            assert!(c.output().is_none());
        };
        if submit {
            assert_eq!(
                tick(&mut c, &mut wall, &mut clock),
                Progress::Production(production::Progress::Micro(build::Progress::Packing(
                    packing::Progress::Waiting
                )))
            );
        }
        let submitted = c.pool().info().submitted;
        c.stop_production().unwrap();
        let report = settled(&mut c, &mut wall, &mut clock)
            .candidate
            .unwrap()
            .packing
            .unwrap();
        assert_eq!(c.pool().info().submitted, submitted);
        assert_eq!(node.db.view().unwrap().status(), before);
        if submit {
            assert_eq!(report.failure_reports.unwrap().unwrap().len(), rejected);
            assert_eq!(report.discarded_failures, 0);
        } else {
            assert!(report.failure_reports.is_none());
            assert_eq!(report.discarded_failures, rejected);
        }
        shutdown(c);
    }
}
