use super::*;
use ae_hyperchain::eoe::{coordinator as api, runtime};
use api::{CallFailure, Coordinator, ServiceOutput};
use std::time::Duration;
static COORDINATOR: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_hc_coordinator.json")));
fn negotiation(epoch: u64) -> Negotiation {
    Negotiation {
        epoch: epoch.into(),
        height: 50.into(),
        hash: key(9).to_vec(),
        leader: pubkey(1),
        validators: vec![
            (pubkey(1), 4.into()),
            (pubkey(2), 3.into()),
            (pubkey(3), 3.into()),
        ],
        seed: Some(key(8).to_vec()),
        current_length: 10.into(),
    }
}
fn vote(mode: Mode, kind: VoteType, n: u64, short: bool) -> SignedTransaction<Transaction> {
    let fields: Payload = [
        (payload::HEIGHT.to_vec(), Field::Integer(50.into())),
        match mode {
            Mode::Fork => (payload::FORK_HASH.to_vec(), Field::Binary(key(9).to_vec())),
            Mode::Length => (payload::LENGTH_DELTA.to_vec(), Field::Integer(0.into())),
        },
    ]
    .into();
    let mut p = payload::create_payload(&fields, &signer(n), Limits::default()).unwrap();
    if short {
        p.insert(payload::SIGNATURE.to_vec(), Field::Binary(vec![1]));
    }
    payload::create_transaction(
        pubkey(n),
        7.into(),
        mode.type_number(kind),
        &p,
        &signer(n),
        &NetworkProfile::mainnet(),
        Limits::default(),
    )
    .unwrap()
}
fn run(c: &mut Coordinator) {
    c.run(&otp_order::profile(&COORDINATOR["profile"]), 1000)
        .unwrap();
    assert_eq!(c.queued_events(), 0);
}
fn quorum(c: &mut Coordinator, mode: Mode) {
    for kind in [VoteType::Vote, VoteType::Commit] {
        c.admit(vote(mode, kind, 2, false), hc_votes::Event::Received)
            .unwrap();
        run(c);
    }
    assert_eq!(c.machine(mode).state(), State::Complete);
}
fn start_request(c: &mut Coordinator, id: u64) {
    c.request_finalize(id, Arc::new(trees(5)), c.now()).unwrap();
    run(c);
}
fn pair_json(replies: &[Reply<Finalized>; 2]) -> J {
    list(
        replies
            .iter()
            .map(|r| match r {
                Reply::NotReady => tuple(vec![atom("error"), atom("not_ready")]),
                Reply::NoConsensus => tuple(vec![atom("error"), atom("no_consensus")]),
                Reply::Finalized(f) => final_json(f.clone()),
            })
            .collect(),
    )
}
fn snapshot(c: &Coordinator, owned: &[u64], block_time: u64) -> J {
    let stakers = owned.iter().map(|n| pubkey(*n)).collect();
    named(
        [("fork", Mode::Fork), ("length", Mode::Length)]
            .into_iter()
            .map(|(name, mode)| {
                let mut state = state_json(c.machine(mode), &stakers, block_time);
                state[1]
                    .as_array_mut()
                    .unwrap()
                    .retain(|pair| pair[0] != atom("from"));
                (name, state)
            })
            .collect(),
    )
}
#[test]
fn actual_original_supervisor_calls_timers_and_restarts_match() {
    for row in COORDINATOR["runs"].as_array().unwrap() {
        let owned: Vec<_> = row["owned"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_u64().unwrap())
            .collect();
        let option = |name| {
            row["options"][1]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| p[0] == atom(name))
                .map(|p| &p[1])
        };
        let block_time = option("block_time").map_or(1000, small);
        let mut h = host(&owned, ServiceLimits::default());
        let mut selected = tip((option("chain_error") != Some(&atom("true"))).then_some(20));
        if option("consensus") == Some(&atom("pow")) {
            selected.consensus = ConsensusType::Pow;
        }
        h.select_tip(selected);
        if option("entropy_missing") != Some(&atom("true")) {
            add_parent(&mut h);
        }
        let mut c = Coordinator::new(h, block_time.into(), api::Config::default());
        let name = row["name"].as_str().unwrap();
        let result = match name {
            "restarts" => {
                let mut rows = vec![];
                for n in 1..=4 {
                    c.admit(
                        vote(Mode::Fork, VoteType::Proposal, 2, true),
                        hc_votes::Event::Received,
                    )
                    .unwrap();
                    run(&mut c);
                    rows.push(tuple(vec![
                        int(n),
                        atom(
                            if n == 4 && c.is_stopped()
                                || n < 4
                                    && c.machine(Mode::Fork).state() == State::AwaitEpoch
                                    && c.machine(Mode::Length).state() == State::AwaitEpoch
                            {
                                "true"
                            } else {
                                "false"
                            },
                        ),
                    ]));
                }
                Some(list(rows))
            }
            _ => {
                if name == "fork_not_ready" {
                    c.negotiate_child(Mode::Length, negotiation(7)).unwrap();
                } else if name != "not_ready" {
                    c.negotiate(negotiation(if name == "zero_timeout" { 4 } else { 7 }))
                        .unwrap();
                }
                run(&mut c);
                match name {
                    "fork_timeout" | "length_timeout" => {
                        quorum(
                            &mut c,
                            if name == "fork_timeout" {
                                Mode::Length
                            } else {
                                Mode::Fork
                            },
                        );
                        c.advance(block_time).unwrap();
                        run(&mut c);
                        start_request(&mut c, 1);
                    }
                    "pending" | "pending_top_missing" => {
                        start_request(&mut c, 1);
                        quorum(&mut c, Mode::Fork);
                        quorum(&mut c, Mode::Length);
                    }
                    "mixed_views" => {
                        quorum(&mut c, Mode::Length);
                        start_request(&mut c, 1);
                        quorum(&mut c, Mode::Fork);
                    }
                    "replaced_call" | "orphaned_call_exit" => {
                        start_request(&mut c, 1);
                        start_request(&mut c, 2);
                        quorum(&mut c, Mode::Fork);
                        quorum(&mut c, Mode::Length);
                        c.advance(6000).unwrap();
                        run(&mut c);
                        assert_eq!(c.pending_requests(), 1);
                        if name == "orphaned_call_exit" {
                            c.new_epoch(8.into()).unwrap();
                            run(&mut c);
                            c.admit(
                                vote(Mode::Fork, VoteType::Proposal, 2, true),
                                hc_votes::Event::Received,
                            )
                            .unwrap();
                            run(&mut c);
                        }
                    }
                    _ => start_request(&mut c, 1),
                };
                None
            }
        };
        let mut published = vec![];
        let mut entropy = vec![];
        let mut pairs = BTreeMap::new();
        while let Some(output) = c.pop_output() {
            match output {
                ServiceOutput::Publication(Output::Vote { signed, .. }) => {
                    published.push(signed.encode())
                }
                ServiceOutput::Publication(Output::ParentBlock { block, .. }) => {
                    entropy.push(block.height)
                }
                ServiceOutput::Finalized { request, replies } => {
                    pairs.insert(request, pair_json(&replies));
                }
                ServiceOutput::CallFailed {
                    request: 1,
                    failure: CallFailure::WorkerExited(Mode::Fork),
                } if name == "orphaned_call_exit" => {}
                ServiceOutput::CallFailed { .. } => panic!("unexpected call failure {name}"),
                ServiceOutput::Restarted { .. } | ServiceOutput::Stopped => {}
            }
        }
        let result = result.unwrap_or_else(|| {
            if name == "replaced_call" || name == "orphaned_call_exit" {
                list(vec![
                    atom(if name == "replaced_call" {
                        "pending"
                    } else {
                        "worker_exit"
                    }),
                    pairs.remove(&2).unwrap(),
                ])
            } else {
                pairs.remove(&1).unwrap()
            }
        });
        assert_eq!(
            normalize(&result),
            normalize(&row["result"]),
            "result {name}"
        );
        let state = if c.is_stopped() {
            atom("stopped")
        } else {
            snapshot(&c, &owned, block_time)
        };
        assert_eq!(normalize(&state), normalize(&row["state"]), "state {name}");
        published.sort();
        assert_eq!(
            list(published.iter().map(bin).collect()),
            row["events"],
            "publications {name}"
        );
        // Missing entropy has no publication; its request height is covered by
        // the prior selected-state reference and explicit cache lookup tests.
        if name != "entropy_missing" {
            entropy.sort();
            assert_eq!(
                list(entropy.iter().map(num).collect()),
                row["entropy"],
                "entropy {name}"
            );
        }
    }
}

#[test]
fn background_runtime_delivers_votes_finalization_and_real_state_timeouts() {
    for (owned, block_time) in [(vec![1, 2, 3], 1000), (vec![], 30)] {
        let mut h = host(&owned, ServiceLimits::default());
        add_parent(&mut h);
        let service = Coordinator::new(h, block_time.into(), api::Config::default());
        let runtime = runtime::Runtime::spawn(
            service,
            otp_order::profile(&COORDINATOR["profile"]),
            runtime::Config::default(),
        )
        .unwrap();
        assert!(
            runtime
                .try_submit(1, runtime::Command::Negotiate(negotiation(7)))
                .is_ok()
        );
        let mut publications = 0;
        if !owned.is_empty() {
            while publications < 10 {
                match runtime.recv_timeout(Duration::from_secs(2)).unwrap() {
                    runtime::Output::Service(ServiceOutput::Publication(Output::Vote {
                        ..
                    })) => publications += 1,
                    runtime::Output::Applied { result: Ok(_), .. } => {}
                    other => panic!("unexpected {other:?}"),
                }
            }
        }
        assert!(
            runtime
                .try_submit(
                    2,
                    runtime::Command::Finalize {
                        request: 9,
                        view: Arc::new(trees(5))
                    }
                )
                .is_ok()
        );
        loop {
            match runtime.recv_timeout(Duration::from_secs(2)).unwrap() {
                runtime::Output::Service(ServiceOutput::Finalized {
                    request: 9,
                    replies,
                }) => {
                    if owned.is_empty() {
                        assert_eq!(*replies, [Reply::NoConsensus, Reply::NoConsensus]);
                    } else {
                        let row = COORDINATOR["runs"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .find(|r| r["name"] == "local")
                            .unwrap();
                        assert_eq!(pair_json(&replies), row["result"]);
                    };
                    break;
                }
                runtime::Output::Applied { result: Ok(_), .. }
                | runtime::Output::Service(ServiceOutput::Publication(_)) => {}
                other => panic!("unexpected runtime output {other:?}"),
            }
        }
        runtime.shutdown().unwrap();
    }
}

#[test]
fn stale_timers_cancel_and_replaced_calls_wait_until_explicit_cancellation() {
    let mut h = host(&[1], ServiceLimits::default());
    add_parent(&mut h);
    let mut c = Coordinator::new(h, 1000.into(), api::Config::default());
    c.negotiate(negotiation(7)).unwrap();
    run(&mut c);
    start_request(&mut c, 1);
    start_request(&mut c, 2);
    // The queued vote moves fork to Finalize before its old Vote timeout is
    // consumed. Its newly armed timeout must not be confused with that event.
    c.admit(
        vote(Mode::Fork, VoteType::Vote, 2, false),
        hc_votes::Event::Received,
    )
    .unwrap();
    c.advance(1000).unwrap();
    run(&mut c);
    assert_eq!(c.machine(Mode::Fork).state(), State::Finalize);
    assert_eq!(c.machine(Mode::Length).state(), State::Complete);
    assert_eq!(c.next_deadline(), Some(2000));
    c.admit(
        vote(Mode::Fork, VoteType::Commit, 2, false),
        hc_votes::Event::Received,
    )
    .unwrap();
    run(&mut c);
    assert_eq!(c.pending_requests(), 1);
    assert_eq!(c.next_deadline(), None);
    c.advance(1_000_000).unwrap();
    run(&mut c);
    assert_eq!(c.pending_requests(), 1);
    c.cancel_request(1).unwrap();
    assert_eq!(c.pending_requests(), 0);
    start_request(&mut c, 1);
    let mut completed = vec![];
    let mut canceled = 0;
    while let Some(o) = c.pop_output() {
        match o {
            ServiceOutput::Finalized { request, .. } => completed.push(request),
            ServiceOutput::CallFailed {
                request: 1,
                failure: CallFailure::Cancelled,
            } => canceled += 1,
            _ => {}
        }
    }
    assert_eq!(completed, vec![2, 1]);
    assert_eq!(canceled, 1);
    assert_eq!(c.advance(999_999), Err(api::Error::TimeWentBackwards));
    assert_eq!(c.now(), 1_000_000);
}

#[test]
fn queue_output_and_request_limits_leave_admission_and_callbacks_retryable() {
    let order = otp_order::profile(&COORDINATOR["profile"]);
    let mut c = Coordinator::new(
        host(&[], ServiceLimits::default()),
        1000.into(),
        api::Config {
            maximum_queued_events: 1,
            ..Default::default()
        },
    );
    let signed = vote(Mode::Fork, VoteType::Proposal, 2, false);
    assert_eq!(
        c.admit(signed.clone(), hc_votes::Event::Received),
        Err(api::Error::ResourceLimit)
    );
    assert!(c.host().pool().is_empty());
    assert_eq!(c.queued_events(), 0);
    assert!(c.pop_output().is_none());
    c.set_config(api::Config::default());
    c.admit(signed, hc_votes::Event::Received).unwrap();
    run(&mut c);
    assert_eq!(c.host().pool().len(), 1);
    let mut c = Coordinator::new(
        host(&[1], ServiceLimits::default()),
        1000.into(),
        api::Config {
            maximum_outputs: 0,
            ..Default::default()
        },
    );
    c.negotiate(negotiation(7)).unwrap();
    let before = c.machine(Mode::Length).clone();
    assert_eq!(c.step(&order), Err(api::Error::ResourceLimit));
    assert_eq!(c.machine(Mode::Length), &before);
    assert_eq!(c.queued_events(), 2);
    assert!(c.host().pool().is_empty());
    assert!(c.pop_output().is_none());
    c.set_config(api::Config {
        maximum_requests: 1,
        ..Default::default()
    });
    run(&mut c);
    assert_eq!(c.host().pool().len(), 2);
    start_request(&mut c, 7);
    let queued = c.queued_events();
    assert_eq!(
        c.request_finalize(7, Arc::new(trees(5)), 0),
        Err(api::Error::DuplicateRequest)
    );
    assert_eq!(
        c.request_finalize(8, Arc::new(trees(5)), 0),
        Err(api::Error::ResourceLimit)
    );
    assert_eq!(c.queued_events(), queued);
    c.cancel_request(7).unwrap();
    start_request(&mut c, 8);
    assert_eq!(c.pending_requests(), 1);
    println!("Coordinator atomic retries: admission, publication, request capacity");
}

#[test]
fn restart_window_is_shared_inclusive_and_preserves_sibling_and_pool() {
    for (last_time, stopped) in [(30_000, true), (31_000, false)] {
        let mut c = Coordinator::new(
            host(&[], ServiceLimits::default()),
            1000.into(),
            api::Config::default(),
        );
        for n in 0..4 {
            c.advance(if n == 3 { last_time } else { 0 }).unwrap();
            c.admit(
                vote(
                    if n % 2 == 0 { Mode::Fork } else { Mode::Length },
                    VoteType::Proposal,
                    2,
                    true,
                ),
                hc_votes::Event::Received,
            )
            .unwrap();
            run(&mut c);
        }
        assert_eq!(c.is_stopped(), stopped);
        assert_eq!(c.host().pool().len(), 2);
        let mut restarts = 0;
        let mut stop_events = 0;
        while let Some(o) = c.pop_output() {
            match o {
                ServiceOutput::Restarted { .. } => restarts += 1,
                ServiceOutput::Stopped => stop_events += 1,
                _ => {}
            }
        }
        assert_eq!(restarts, if stopped { 3 } else { 4 });
        assert_eq!(stop_events, usize::from(stopped));
        if stopped {
            assert_eq!(c.negotiate(negotiation(7)), Err(api::Error::Stopped));
        }
    }
    // A local budget stop is retryable, rather than an OTP worker crash.
    let mut c = Coordinator::new(
        host(&[], ServiceLimits::default()),
        1000.into(),
        api::Config::default(),
    );
    c.admit(
        vote(Mode::Fork, VoteType::Proposal, 2, true),
        hc_votes::Event::Received,
    )
    .unwrap();
    let mut limits = c.config();
    limits.machine.maximum_bytes = 0;
    c.set_config(limits);
    let order = otp_order::profile(&COORDINATOR["profile"]);
    assert!(matches!(
        c.step(&order),
        Err(api::Error::Step(StepError::Core(Failure::ResourceLimit)))
    ));
    assert_eq!(c.queued_events(), 2);
    c.set_config(api::Config::default());
    run(&mut c);
    let mut restarts = 0;
    while let Some(o) = c.pop_output() {
        if matches!(o, ServiceOutput::Restarted { .. }) {
            restarts += 1;
        }
    }
    assert_eq!(restarts, 1);
}

#[test]
fn runtime_submission_is_bounded_and_shutdown_releases_output_backpressure() {
    let c = Coordinator::new(
        host(&[1, 2, 3], ServiceLimits::default()),
        1000.into(),
        api::Config::default(),
    );
    let runtime = runtime::Runtime::spawn(
        c,
        otp_order::profile(&COORDINATOR["profile"]),
        runtime::Config {
            command_capacity: 1,
            output_capacity: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let mut saw_full = false;
    for sequence in 0..16 {
        match runtime.try_submit(sequence, runtime::Command::Negotiate(negotiation(7))) {
            Ok(()) => {}
            Err(runtime::SubmitError::Full(_)) => {
                saw_full = true;
                break;
            }
            _ => panic!("unexpected submission failure"),
        }
    }
    assert!(saw_full);
    let start = std::time::Instant::now();
    runtime.shutdown().unwrap();
    assert!(start.elapsed() < Duration::from_secs(2));
    let c = Coordinator::new(
        host(&[], ServiceLimits::default()),
        1000.into(),
        api::Config::default(),
    );
    let runtime = runtime::Runtime::spawn(
        c,
        otp_order::profile(&COORDINATOR["profile"]),
        runtime::Config {
            maximum_command_bytes: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        runtime.try_submit(1, runtime::Command::Negotiate(negotiation(7))),
        Err(runtime::SubmitError::ResourceLimit(_))
    ));
    runtime.shutdown().unwrap();
}

#[test]
fn background_runtime_reports_resource_stop_once_and_resumes_after_configuration() {
    let mut h = host(&[1, 2, 3], ServiceLimits::default());
    add_parent(&mut h);
    let c = Coordinator::new(
        h,
        1000.into(),
        api::Config {
            maximum_outputs: 0,
            ..Default::default()
        },
    );
    let runtime = runtime::Runtime::spawn(
        c,
        otp_order::profile(&COORDINATOR["profile"]),
        runtime::Config::default(),
    )
    .unwrap();
    assert!(
        runtime
            .try_submit(1, runtime::Command::Negotiate(negotiation(7)))
            .is_ok()
    );
    assert!(matches!(
        runtime.recv_timeout(Duration::from_secs(2)).unwrap(),
        runtime::Output::Applied {
            sequence: 1,
            result: Ok(_)
        }
    ));
    assert_eq!(
        runtime.recv_timeout(Duration::from_secs(2)).unwrap(),
        runtime::Output::WorkStopped(api::Error::ResourceLimit)
    );
    assert_eq!(
        runtime.recv_timeout(Duration::from_millis(30)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    );
    assert!(
        runtime
            .try_submit(2, runtime::Command::Configure(api::Config::default()))
            .is_ok()
    );
    let mut publications = 0;
    while publications < 10 {
        match runtime.recv_timeout(Duration::from_secs(2)).unwrap() {
            runtime::Output::Applied {
                sequence: 2,
                result: Ok(_),
            } => {}
            runtime::Output::Service(ServiceOutput::Publication(Output::Vote { .. })) => {
                publications += 1
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(
        runtime
            .try_submit(
                3,
                runtime::Command::Finalize {
                    request: 1,
                    view: Arc::new(trees(5))
                }
            )
            .is_ok()
    );
    loop {
        match runtime.recv_timeout(Duration::from_secs(2)).unwrap() {
            runtime::Output::Service(ServiceOutput::Finalized {
                request: 1,
                replies,
            }) => {
                assert!(
                    replies
                        .iter()
                        .all(|r| matches!(r, Reply::Finalized(Finalized::Signed(_))))
                );
                break;
            }
            runtime::Output::Applied {
                sequence: 3,
                result: Ok(_),
            }
            | runtime::Output::Service(ServiceOutput::Publication(_)) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    runtime.shutdown().unwrap();
}

#[path = "entropy.rs"]
mod entropy;
