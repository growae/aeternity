use super::*;
use std::sync::{Mutex, mpsc};
use std::time::Instant;

fn spawn(c: Coordinator, capacity: usize) -> runtime::Runtime {
    runtime::Runtime::spawn(
        c,
        otp_order::profile(&COORDINATOR["profile"]),
        runtime::Config {
            command_capacity: 8,
            output_capacity: capacity,
            ..Default::default()
        },
    )
    .unwrap()
}
fn finished(rt: &runtime::Runtime) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !rt.is_finished() {
        assert!(Instant::now() < deadline, "EOE did not finish");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn retirement_preserves_channel_pending_send_outbox_and_unsubmitted_receipts_in_order() {
    let mut h = host(&[1, 2, 3], ServiceLimits::default());
    add_parent(&mut h);
    let mut c = Coordinator::new(h, 1000.into(), api::Config::default());
    c.negotiate(negotiation(7)).unwrap();
    run(&mut c);
    start_request(&mut c, 77);
    run(&mut c);
    let mut expected = vec![];
    let mut copy = c.clone();
    while let Some(output) = copy.pop_output() {
        expected.push(runtime::Output::Service(output));
    }
    assert!(expected.len() > 4);
    assert!(expected.iter().any(|o| matches!(
        o,
        runtime::Output::Service(ServiceOutput::Finalized { request: 77, .. })
    )));
    let rt = spawn(c, 1);
    let client = rt.client();
    let first = rt.recv_timeout(Duration::from_secs(2)).unwrap();
    // More than one channel's capacity remains before the command loop can
    // run. Neither queued command can execute without draining more output.
    let mut call = addressed(&client, runtime::Command::NewEpoch(99.into()));
    assert!(rt.try_submit(91, runtime::Command::Retry).is_ok());
    let retired = rt.retire();
    assert_eq!(retired.result, Ok(()));
    assert_eq!(
        retired.queued,
        vec![runtime::Submission::Caller, runtime::Submission::Stream(91)]
    );
    assert_eq!(retired.interrupted, None);
    let actual = std::iter::once(first)
        .chain(retired.outputs)
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert_ne!(
        retired.state.unwrap().pool.as_ref().unwrap().epoch,
        99.into()
    );
    assert_eq!(call.try_reply(), Err(runtime::CallError::Closed));
    assert!(call.replacement_pending());
    assert_eq!(client.outstanding(), 1);
    drop(call);
    assert_eq!(client.outstanding(), 0);
}

struct PanickingCache {
    entered: mpsc::SyncSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}
impl ae_mempool::gossip::Transactional for PanickingCache {
    fn transact(&self, operation: ae_mempool::gossip::Transaction<'_>) {
        self.entered.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
        // The callback has committed effects, but there is no command reply.
        assert!(operation(&mut |_| false));
        panic!("injected EOE cache callback panic after committed admission");
    }
}

#[test]
fn panic_retains_committed_effects_known_unread_reply_and_unknown_accepted_calls() {
    let (entered, wait) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    let mut c = Coordinator::new(
        host(&[], ServiceLimits::default()),
        1000.into(),
        Default::default(),
    );
    c.new_epoch(7.into()).unwrap();
    run(&mut c);
    c.set_live_gossip_cache(Some(Arc::new(PanickingCache {
        entered,
        release: Mutex::new(resume),
    })));
    let rt = spawn(c, 4);
    let client = rt.client();
    let known = addressed(&client, runtime::Command::Retry);
    let mut interrupted = addressed(
        &client,
        runtime::Command::Admit {
            signed: Box::new(vote(Mode::Fork, VoteType::Proposal, 2, false)),
            event: hc_votes::Event::Received,
        },
    );
    wait.recv_timeout(Duration::from_secs(2)).unwrap();
    let mut queued = addressed(&client, runtime::Command::NewEpoch(99.into()));
    assert!(rt.try_submit(92, runtime::Command::Retry).is_ok());
    release.send(()).unwrap();
    // The closure classification is published before the lost private reply.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        assert!(Instant::now() < deadline, "private receipt did not close");
        match interrupted.try_reply() {
            Ok(None) => std::thread::sleep(Duration::from_millis(1)),
            Err(runtime::CallError::Closed) => break,
            other => panic!("unexpected interrupted receipt {other:?}"),
        }
    }
    assert!(interrupted.replacement_pending());
    finished(&rt);
    let retired = rt.retire();
    assert!(retired.result.is_err());
    assert_eq!(retired.interrupted, Some(runtime::Submission::Caller));
    assert_eq!(
        retired.queued,
        vec![runtime::Submission::Caller, runtime::Submission::Stream(92)]
    );
    assert_eq!(known.wait(Duration::ZERO), Ok(Ok(runtime::Applied::Unit)));
    assert_eq!(queued.try_reply(), Err(runtime::CallError::Closed));
    let state = retired.state.unwrap();
    assert_eq!(state.pool.as_ref().unwrap().epoch, 7.into());
    assert_eq!(state.pool.as_ref().unwrap().entries, 1);
    assert_eq!(retired.outputs.len(), 1);
    assert!(matches!(
        &retired.outputs[0],
        runtime::Output::Service(ServiceOutput::Publication(Output::Vote { .. }))
    ));
    assert!(client.is_closed());
}

#[test]
fn operational_work_stop_remains_terminal_after_join_and_keeps_retry_state() {
    let c = Coordinator::new(
        host(&[1], ServiceLimits::default()),
        1000.into(),
        api::Config {
            maximum_outputs: 0,
            ..Default::default()
        },
    );
    let rt = spawn(c, 1);
    let client = rt.client();
    addressed(&client, runtime::Command::Negotiate(negotiation(7)))
        .wait(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    assert_eq!(
        rt.recv_timeout(Duration::from_secs(2)),
        Ok(runtime::Output::WorkStopped(api::Error::ResourceLimit))
    );
    assert!(!rt.replacement_pending());
    client.request_stop();
    assert!(!rt.replacement_pending());
    let retired = rt.retire();
    assert_eq!(
        retired.info.health,
        runtime::Health::WorkStopped(api::Error::ResourceLimit)
    );
    assert!(!client.replacement_pending());
    let state = retired.state.unwrap();
    assert_eq!(state.queued_events, 2);
    assert_eq!(state.generations, [0, 0]);
}

#[test]
fn native_restart_exhaustion_closes_generation_and_preserves_native_stop_output() {
    let mut c = Coordinator::new(
        host(&[], ServiceLimits::default()),
        1000.into(),
        Default::default(),
    );
    for n in 0..4 {
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
    assert!(c.is_stopped());
    let rt = spawn(c, 64);
    let old = rt.client();
    let route = runtime::Route::new(old.clone());
    finished(&rt);
    assert!(rt.replacement_pending());
    route.replace(None);
    let retired = rt.retire();
    assert!(retired.state.unwrap().stopped);
    assert_eq!(
        retired
            .outputs
            .iter()
            .filter(|o| matches!(o, runtime::Output::Service(ServiceOutput::Restarted { .. })))
            .count(),
        3
    );
    assert_eq!(
        retired
            .outputs
            .iter()
            .filter(|o| matches!(o, runtime::Output::Service(ServiceOutput::Stopped)))
            .count(),
        1
    );
    let replacement = spawn(
        Coordinator::new(
            host(&[], ServiceLimits::default()),
            1000.into(),
            Default::default(),
        ),
        4,
    );
    route.replace(Some(replacement.client()));
    assert!(old.is_closed());
    assert!(!old.same_runtime(&route.current().unwrap()));
    assert!(matches!(
        old.call(runtime::Command::Retry),
        Err(runtime::SubmitError::Disconnected(_))
    ));
    addressed(&route.current().unwrap(), runtime::Command::Retry)
        .wait(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    replacement.shutdown().unwrap();
}
