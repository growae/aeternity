use super::*;
use conductor::pool_supervision::{ControlError, Notice};

pub(super) fn transactions(index: usize) -> Vec<Arc<ae_mempool::pool::Signed>> {
    let data: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/erlang_hc_pool_packing.json"
    ))
    .unwrap();
    let row = data["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["corpus_index"] == index)
        .unwrap();
    row["parents"][0]["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "linear")
        .unwrap()["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            Arc::new(
                ae_tx::SignedTransaction::decode(&hex::decode(v.as_str().unwrap()).unwrap())
                    .unwrap(),
            )
        })
        .collect()
}

#[test]
fn restored_pool_preserves_membership_and_unread_replies_without_republishing_old_admissions() {
    for index in [1, 3, 5] {
        let node = Node::new(index);
        let mut c = start(&node, false);
        let old = c.pool();
        let cursor = old.event_cursor();
        let txs = transactions(index);
        let replies: Vec<_> = txs
            .iter()
            .map(|tx| {
                old.push(tx.clone(), ae_mempool::pool::Event::Created, false)
                    .unwrap()
            })
            .collect();
        c.request_pool_restart(1).unwrap();
        let notices = replaced(&mut c, 2);
        assert_eq!(notices.len(), 2);
        assert!(old.info().joined);
        for reply in replies {
            assert!(matches!(
                reply.wait().unwrap().outcome,
                ae_mempool::pool::PushOutcome::Accepted { .. }
            ));
        }
        let next = c.pool();
        let mut expected = txs.iter().map(|t| t.hash()).collect::<Vec<_>>();
        expected.sort();
        let mut actual = next
            .peek(10, None, None)
            .unwrap()
            .wait()
            .unwrap()
            .iter()
            .map(|t| t.hash())
            .collect::<Vec<_>>();
        actual.sort();
        assert_eq!(actual, expected);
        assert_eq!(
            old.events_after(&cursor, 10).unwrap().events.len(),
            txs.len()
        );
        assert!(old.events_after(&cursor, 10).unwrap().closed);
        assert_eq!(
            next.events_after(&cursor, 10).err(),
            Some(pool::worker::events::ReadError::WrongStream)
        );
        assert!(
            next.events_after(&next.oldest_event_cursor(), 10)
                .unwrap()
                .events
                .is_empty()
        );
        assert_eq!(next.info().submitted, 1);
        assert!(shutdown(c).pool.unwrap().joined);
    }
}

pub(super) fn replaced(c: &mut Conductor<Otp26>, generation: u64) -> Vec<Notice> {
    let now = Instant::now();
    let end = now + Duration::from_secs(5);
    let mut notices = Vec::new();
    loop {
        c.step(1000, now).unwrap();
        if let Some(Output::PoolSupervision(notice)) = c.take_output() {
            notices.push(notice);
        }
        if c.status().pool.generation == generation && c.status().pool.pending_notices == 0 {
            return notices;
        }
        assert!(Instant::now() < end, "{:?}", c.status());
        std::thread::yield_now();
    }
}

#[test]
fn pool_recovery_retires_active_production_and_resumes_with_the_same_consensus_and_importer() {
    for index in [1, 3, 5] {
        for micro in [false, true] {
            let node = Node::new(index);
            let mut c = start(&node, true);
            let importer = c.importer();
            let parent = c.parent();
            let eoe = c.eoe();
            let old = c.pool();
            let votes = c.votes();
            let binding = c.pool_binding();
            let mut wall = 1000;
            let mut now = Instant::now();
            let end = now + Duration::from_secs(25);
            loop {
                let progress = tick(&mut c, &mut wall, &mut now);
                c.take_output();
                if if micro {
                    matches!(
                        progress,
                        Progress::Production(production::Progress::Micro(
                            managed::candidate::Progress::Packing(_)
                        ))
                    )
                } else {
                    progress == Progress::Production(production::Progress::KeyPrepared)
                } {
                    break;
                }
                assert!(Instant::now() < end, "{:?}", c.status());
            }
            assert_eq!(c.request_pool_restart(0), Err(ControlError::Stale));
            c.request_pool_restart(1).unwrap();
            assert!(binding.current().is_none());
            let mut notices = Vec::new();
            let mut stopped = 0;
            let mut produced = false;
            while !produced || notices.len() < 2 {
                tick(&mut c, &mut wall, &mut now);
                match c.take_output() {
                    Some(Output::PoolSupervision(notice)) => notices.push(notice),
                    Some(Output::Stopped(report)) => {
                        stopped += 1;
                        if micro {
                            assert!(report.candidate.is_some());
                        }
                    }
                    Some(Output::Attempt(done)) => {
                        produced |= matches!(done.outcome, production::Outcome::Produced { .. })
                    }
                    _ => (),
                }
                assert!(Instant::now() < end, "{:?}", c.status());
            }
            assert_eq!(stopped, 1);
            assert!(matches!(
                notices.as_slice(),
                [
                    Notice::Retired { generation: 1, .. },
                    Notice::Restarted { generation: 2 }
                ]
            ));
            assert!(old.info().joined && !old.info().accepting);
            assert!(!old.same_worker(&binding.current().unwrap().client));
            assert!(
                Arc::ptr_eq(&importer, &c.importer())
                    && parent.same_runtime(&c.parent())
                    && eoe.same_runtime(&c.eoe())
            );
            assert!(votes.info().accepting && c.status().votes.generation == 1);
            assert_eq!(c.status().consensus.attempts, 0);
            assert_eq!(c.status().pool.restart_attempts, 1);
            node.db.view().unwrap().verify_tip().unwrap();
            assert!(shutdown(c).owner.errors.is_empty());
        }
    }
}

#[test]
fn dedicated_runtime_recovers_the_pool_and_shutdown_transfers_retirement_reports() {
    let node = Node::new(0);
    let c = start(&node, false);
    let old = c.pool();
    let binding = c.pool_binding();
    c.request_pool_restart(1).unwrap();
    let runtime =
        conductor::runtime::Runtime::spawn(c, conductor::runtime::SystemClock, Default::default())
            .unwrap_or_else(|_| panic!("runtime"));
    let handle = runtime.handle();
    wait(|| handle.status().pool.generation == 2 || handle.status().failure.is_some());
    assert!(handle.status().failure.is_none(), "{:?}", handle.status());
    assert!(old.info().joined && binding.current().unwrap().client.info().accepting);
    let exit = runtime.shutdown().unwrap();
    assert!(exit.pool.unwrap().joined && binding.is_closed());
    assert!(
        exit.queued_outputs
            .iter()
            .chain(exit.output.iter())
            .any(|o| matches!(
                o,
                Output::PoolSupervision(Notice::Retired { generation: 1, .. })
            ))
    );
}
