use super::*;
mod child_signing;
use crate::{
    preset_keys,
    test_support::hyperchain::{
        Node,
        conductor::{signer, start},
    },
};
use std::{thread, time::Duration};

#[track_caller]
fn until(mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !f() {
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn actual_key_panic_retains_unknown_and_unexecuted_work_without_replay() {
    let node = Node::new(0);
    let mut c = start(&node, false);
    let binding = c.preset_key_binding();
    let old = binding.current().unwrap();
    let eoe = c.eoe();
    let parent = c.parent();
    let (gate, release) = old.pause_for_test();
    let unknown = old.panic_for_test();
    let queued = old.set_candidate(signer(12).public_key()).unwrap();
    release.send(()).unwrap();
    let now = Instant::now();
    let mut notices = vec![];
    until(|| {
        c.step(1000, now).unwrap();
        if let Some(o) = c.take_output() {
            notices.push(o);
        }
        c.status().consensus.key_generation == 2
    });
    assert_eq!(gate.try_result(), Some(Ok(())));
    assert_eq!(
        unknown.try_result(),
        Some(Err(preset_keys::Error::Disconnected))
    );
    assert_eq!(queued.try_result(), Some(Err(preset_keys::Error::Stopped)));
    assert!(notices.iter().any(|n| matches!(n, Output::ConsensusSupervision(Notice::KeysRetired { info, .. }) if info.joined && info.exit == Some(preset_keys::Exit::Panicked))));
    let fresh = binding.current().unwrap();
    assert!(!old.same_worker(&fresh));
    assert_eq!(
        fresh
            .candidate()
            .unwrap()
            .wait_timeout(Duration::from_secs(5)),
        Some(Err(preset_keys::Error::NoCandidate))
    );
    assert_eq!(
        fresh.active().unwrap().wait_timeout(Duration::from_secs(5)),
        Some(Err(preset_keys::Error::NoActive))
    );
    assert!(eoe.same_runtime(&c.eoe()) && parent.same_runtime(&c.parent()));
    assert_eq!(c.status().consensus.attempts, 1);
    assert!(c.shutdown().owner.keys.unwrap().joined);
}

#[test]
fn staged_producer_selection_cannot_overwrite_a_later_worker_change() {
    let node = Node::new(0);
    let mut c = start(&node, true);
    let now = Instant::now();
    until(|| {
        c.step(1000, now).unwrap() == Progress::Production(production::Progress::DecisionRequested)
    });
    let keys = c.preset_key_binding().current().unwrap();
    assert_eq!(
        keys.promote_candidate(signer(14).public_key())
            .unwrap()
            .wait_timeout(Duration::from_secs(5)),
        Some(Ok(()))
    );
    let mut failure = None;
    until(|| {
        failure = c.owner.as_mut().unwrap().step().err();
        failure.is_some()
    });
    assert_eq!(
        failure,
        Some(owner::Error::Producer(delivery::Error::Preset(
            preset_keys::Error::StaleSelection
        )))
    );
    assert_eq!(
        keys.active().unwrap().wait_timeout(Duration::from_secs(5)),
        Some(Ok(signer(14).public_key()))
    );
    assert_eq!(
        keys.candidate()
            .unwrap()
            .wait_timeout(Duration::from_secs(5)),
        Some(Err(preset_keys::Error::NoCandidate))
    );
    assert!(c.shutdown().owner.keys.unwrap().joined);
}

#[test]
fn key_constructor_failures_and_panics_obey_notice_capacity_and_shared_budget() {
    for panic in [false, true] {
        for exhausted in [false, true] {
            let node = Node::new(0);
            let mut c = start(&node, false);
            let old = c.preset_key_binding().current().unwrap();
            let binding = c.preset_key_binding();
            let eoe = c.eoe();
            c.consensus.limit = 1;
            c.request_key_restart(1).unwrap();
            let now = Instant::now();
            until(|| {
                c.consensus
                    .poll_keys(c.owner.as_mut().unwrap(), now)
                    .unwrap();
                old.info().joined
            });
            for _ in 0..10 {
                c.consensus
                    .poll_keys(c.owner.as_mut().unwrap(), now)
                    .unwrap();
            }
            assert_eq!(c.consensus.status().attempts, 0);
            assert!(binding.current().is_none());
            assert!(matches!(
                c.consensus.take_notice(),
                Some(Notice::KeysRetired { generation: 1, .. })
            ));
            for attempt in 1..=if exhausted { 3 } else { 1 } {
                c.owner.as_mut().unwrap().fail_key_restart_for_test(panic);
                c.consensus
                    .poll_keys(c.owner.as_mut().unwrap(), now)
                    .unwrap();
                assert!(binding.current().is_none());
                for _ in 0..10 {
                    c.consensus
                        .poll_keys(c.owner.as_mut().unwrap(), now)
                        .unwrap();
                }
                assert_eq!(c.consensus.status().attempts, attempt);
                match c.consensus.take_notice().unwrap() {
                    Notice::KeysStartFailed {
                        attempt: actual,
                        error,
                    } => {
                        assert_eq!(actual, attempt);
                        assert_eq!(
                            error,
                            if panic {
                                owner::Error::WorkerPanicked
                            } else {
                                owner::Error::Preset(preset_keys::Error::Spawn(
                                    std::io::ErrorKind::Other,
                                ))
                            }
                        );
                    }
                    other => panic!("{other:?}"),
                }
            }
            if exhausted {
                assert_eq!(c.step(1000, now), Err(Error::ConsensusRestartLimit));
                assert_eq!(c.status().consensus.attempts, 4);
                assert!(binding.is_closed());
            } else {
                c.consensus
                    .poll_keys(c.owner.as_mut().unwrap(), now)
                    .unwrap();
                assert_eq!(c.consensus.status().attempts, 2);
                assert!(matches!(
                    c.consensus.take_notice(),
                    Some(Notice::KeysRestarted { generation: 2 })
                ));
                assert!(binding.current().unwrap().info().accepting);
                assert!(eoe.same_runtime(&c.eoe()));
            }
            assert!(c.shutdown().owner.keys.unwrap().joined);
        }
    }
}
