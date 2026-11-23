use super::*;
use managed::vote_pool::{ControlError, Notice};

pub(super) fn replaced(c: &mut Conductor<Otp26>, generation: u64) -> Vec<Notice> {
    let now = Instant::now();
    let end = now + Duration::from_secs(5);
    let mut notices = Vec::new();
    loop {
        c.step(1000, now).unwrap();
        if let Some(Output::VoteSupervision(notice)) = c.take_output() {
            notices.push(notice);
        }
        if c.status().votes.generation == generation && c.status().votes.pending_notices == 0 {
            return notices;
        }
        assert!(Instant::now() < end, "{:?}", c.status());
        std::thread::yield_now();
    }
}

#[test]
fn vote_replacement_keeps_active_production_importer_and_consensus_children() {
    for index in [1, 3, 5] {
        let node = Node::new(index);
        let mut c = start(&node, true);
        let importer = c.importer();
        let parent = c.parent();
        let eoe = c.eoe();
        let old = c.votes();
        let binding = c.vote_binding();
        let mut wall = 1000;
        let mut now = Instant::now();
        let end = now + Duration::from_secs(25);
        loop {
            let progress = tick(&mut c, &mut wall, &mut now);
            c.take_output();
            if progress == Progress::Production(production::Progress::KeyPrepared) {
                break;
            }
            assert!(Instant::now() < end);
        }
        c.request_vote_restart(1).unwrap();
        let mut notices = Vec::new();
        let mut produced = false;
        while !produced || notices.len() < 2 {
            tick(&mut c, &mut wall, &mut now);
            match c.take_output() {
                Some(Output::VoteSupervision(notice)) => notices.push(notice),
                Some(Output::Attempt(done)) => {
                    produced |= matches!(done.outcome, production::Outcome::Produced { .. })
                }
                Some(Output::Stopped(_)) => panic!("pool replacement cancelled production"),
                _ => (),
            }
            assert!(Instant::now() < end, "{:?}", c.status());
        }
        assert!(matches!(
            notices.as_slice(),
            [
                Notice::Retired { generation: 1, .. },
                Notice::Restarted { generation: 2 }
            ]
        ));
        assert!(old.info().joined && !old.info().accepting);
        assert!(binding.current().unwrap().info().accepting);
        assert_eq!(c.request_vote_restart(1), Err(ControlError::Stale));
        assert!(Arc::ptr_eq(&importer, &c.importer()));
        assert!(parent.same_runtime(&c.parent()) && eoe.same_runtime(&c.eoe()));
        assert_eq!(c.status().consensus.attempts, 0);
        assert_eq!(c.status().votes.restart_attempts, 1);
        node.db.view().unwrap().verify_tip().unwrap();
        assert!(shutdown(c).owner.errors.is_empty());
    }
}

#[test]
fn dedicated_runtime_recovers_votes_and_keeps_the_old_receipt_report() {
    let node = Node::new(0);
    let c = start(&node, false);
    let old = c.votes();
    let binding = c.vote_binding();
    let eoe = c.eoe();
    c.request_vote_restart(1).unwrap();
    let runtime =
        conductor::runtime::Runtime::spawn(c, conductor::runtime::SystemClock, Default::default())
            .unwrap_or_else(|_| panic!("spawn conductor"));
    let handle = runtime.handle();
    wait(|| handle.status().votes.generation == 2 || handle.status().failure.is_some());
    assert!(handle.status().failure.is_none(), "{:?}", handle.status());
    assert!(old.info().joined && binding.current().unwrap().info().accepting && !eoe.is_closed());
    let exit = runtime.shutdown().unwrap();
    assert!(exit.owner.errors.is_empty());
    assert_eq!(exit.status.votes.restart_attempts, 1);
    assert!(binding.is_closed());
    assert!(
        exit.queued_outputs
            .iter()
            .chain(exit.output.iter())
            .any(|o| matches!(
                o,
                Output::VoteSupervision(Notice::Retired { generation: 1, .. })
            ))
    );
}
