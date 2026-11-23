use super::*;

fn stopped(owner: &managed::Owner) {
    owner.runtime_client().request_stop();
    let end = Instant::now() + Duration::from_secs(5);
    while !owner.runtime_finished() {
        assert!(Instant::now() < end);
        std::thread::yield_now();
    }
}
#[test]
fn retirement_delivers_completed_outputs_before_replacement_and_keeps_caller_capacity() {
    let node = Node::new(0);
    let source = source(&node);
    let mut config = cfg(&node);
    config.calls.requests = 1;
    let mut owner = start(&node, &source, config, &[12]);
    let old = owner.runtime_client();
    let mut request = owner.finalize().unwrap();
    let id = request.id();
    unit(owner.retry_runtime().unwrap());
    stopped(&owner);
    owner.begin_recovery().unwrap();
    assert!(owner.recovering());
    assert!(owner.take_retirement().is_none());
    assert_eq!(owner.restart_runtime(), Err(managed::Error::Backpressured));
    let result = complete(&mut owner, &mut request);
    assert_eq!(
        result.replies(),
        &[eoe::Reply::NotReady, eoe::Reply::NotReady]
    );
    let clone = result.clone();
    let retired = owner.take_retirement().unwrap();
    assert!(retired.requests.is_empty());
    assert!(retired.runtime.result.is_ok());
    owner.restart_runtime().unwrap();
    assert!(!owner.recovering());
    assert!(!owner.runtime_client().same_runtime(&old));
    assert!(owner.votes().info().accepting);
    drop((request, result));
    assert!(matches!(
        owner.finalize(),
        Err(managed::Error::Backpressured)
    ));
    drop(clone);
    let mut next = owner.finalize().unwrap();
    assert!(next.id() > id);
    assert_eq!(
        complete(&mut owner, &mut next).replies(),
        &[eoe::Reply::NotReady, eoe::Reply::NotReady]
    );
    assert!(owner.shutdown().errors.is_empty());
}
#[test]
fn retirement_distinguishes_known_rejection_from_unfinished_accepted_work_without_replay() {
    for rejected in [false, true] {
        let node = Node::new(0);
        let source = source(&node);
        let mut config = cfg(&node);
        if rejected {
            config.eoe.coordinator.maximum_requests = 0;
        }
        let mut owner = start(&node, &source, config, &[12]);
        prepare(&owner);
        let mut request = owner.finalize().unwrap();
        unit(owner.retry_runtime().unwrap());
        let cursor = owner.epoch_cursor().clone();
        let votes = owner.votes();
        stopped(&owner);
        owner.begin_recovery().unwrap();
        let retired = owner.take_retirement().unwrap();
        assert_eq!(retired.requests.len(), 1);
        let expected = if rejected {
            managed::RequestError::Rejected(coordinator::Error::ResourceLimit)
        } else {
            managed::RequestError::Unknown
        };
        assert_eq!(retired.requests[0].outcome, expected);
        assert_eq!(request.try_reply().unwrap_err(), expected);
        assert_eq!(source.snapshots().live_snapshots(), 0);
        owner.restart_runtime().unwrap();
        assert_eq!(owner.epoch_cursor(), &cursor);
        assert!(votes.info().accepting);
        let mut next = owner.finalize().unwrap();
        if rejected {
            let end = Instant::now() + Duration::from_secs(5);
            loop {
                owner.step().unwrap();
                if let Err(error) = next.try_reply() {
                    assert_eq!(error, expected);
                    break;
                }
                assert!(Instant::now() < end);
            }
        } else {
            assert_eq!(
                complete(&mut owner, &mut next).replies(),
                &[eoe::Reply::NotReady, eoe::Reply::NotReady]
            );
        }
        assert!(owner.shutdown().errors.is_empty());
    }
}
#[test]
fn operational_work_stop_is_ineligible_for_process_replacement() {
    let node = Node::new(0);
    let source = source(&node);
    let mut config = cfg(&node);
    config.eoe.coordinator.maximum_outputs = 0;
    let mut owner = start(&node, &source, config, &[12, 13, 14]);
    prepare(&owner);
    let mut request = owner.finalize().unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        if matches!(
            owner.step().unwrap(),
            managed::Progress::Output(runtime::Output::WorkStopped(_))
        ) {
            break;
        }
        assert!(Instant::now() < end);
        std::thread::yield_now();
    }
    assert!(!owner.replacement_pending());
    assert_eq!(owner.begin_recovery(), Err(managed::Error::Configuration));
    let old = owner.runtime_client();
    unit(owner.configure_runtime(Default::default()).unwrap());
    let result = complete(&mut owner, &mut request);
    assert!(matches!(
        &result.replies()[0],
        eoe::Reply::Finalized(local_host::Finalized::Signed(_))
    ));
    assert!(owner.runtime_client().same_runtime(&old));
    assert!(owner.shutdown().errors.is_empty());
}

#[test]
fn shutdown_during_recovery_transfers_undrained_outputs_and_joined_runtime_diagnostics() {
    let node = Node::new(0);
    let source = source(&node);
    let mut config = cfg(&node);
    config.eoe.runtime.output_capacity = 1;
    let mut owner = start(&node, &source, config, &[12]);
    let mut first = owner.finalize().unwrap();
    unit(owner.retry_runtime().unwrap());
    let mut second = owner.finalize().unwrap();
    // The second completion blocks at the one-slot output channel. A third
    // accepted command stays queued and must not execute on the replacement.
    let mut queued = owner.retry_runtime().unwrap();
    stopped(&owner);
    owner.begin_recovery().unwrap();
    let exit = owner.shutdown();
    assert!(exit.errors.is_empty());
    assert_eq!(exit.abandoned_requests, vec![first.id(), second.id()]);
    assert_eq!(
        first.try_reply().unwrap_err(),
        managed::RequestError::Unknown
    );
    assert_eq!(
        second.try_reply().unwrap_err(),
        managed::RequestError::Unknown
    );
    let retired = exit.retirement.unwrap();
    assert!(retired.result.is_ok());
    // Depending on scheduling, the second finalize itself may still be queued.
    let ids: Vec<_> = exit
        .retired_outputs
        .iter()
        .map(|output| match output {
            runtime::Output::Service(coordinator::ServiceOutput::Finalized { request, .. }) => {
                *request
            }
            other => panic!("unexpected retained output {other:?}"),
        })
        .collect();
    assert!(ids == vec![first.id()] || ids == vec![first.id(), second.id()]);
    assert!(!retired.queued.is_empty());
    assert_eq!(queued.try_reply(), Err(runtime::CallError::Closed));
    assert_eq!(source.snapshots().live_snapshots(), 0);
}

#[tokio::test]
async fn producer_retirement_keeps_known_and_unknown_receipts_without_publishing_staged_keys() {
    use ae_sync::chain::{Importer, hyperchain::delivery};
    use common::hyperchain::parents::Pair;
    for unknown in [false, true] {
        let node = Node::new(1);
        let source = source(&node);
        let pair = Pair::new(false);
        let (importer, accepted) = Importer::new_hyperchain_scoped(
            node.db.clone(),
            node.config.clone(),
            pair.inputs(),
            Default::default(),
        )
        .unwrap();
        let mut config = cfg(&node);
        config.eoe.runtime.output_capacity = 1;
        let mut owner = candidate::owner(&node, &source, &pair, config);
        owner
            .prepare_producer(
                &accepted.producer(),
                managed::producer::Input {
                    now: 1000.into(),
                    config: managed::producer::Config {
                        production_time: 1.into(),
                        ..Default::default()
                    },
                },
            )
            .unwrap();
        let initial = owner.production().unwrap().remaining_parents().len()
            + usize::from(owner.production().unwrap().negotiation().is_some());
        assert!(initial > 0);
        assert!(owner.signing().active().is_none());
        let mut barrier = None;
        if unknown {
            for _ in 0..2 {
                assert!(matches!(
                    owner
                        .entropy(3.into())
                        .unwrap()
                        .wait(Duration::from_secs(5))
                        .unwrap()
                        .unwrap(),
                    runtime::Applied::Entropy(Some(_))
                ));
            }
            barrier = Some(owner.entropy(3.into()).unwrap());
        }
        owner.step().unwrap();
        assert!(owner.production().unwrap().in_flight());
        if !unknown {
            unit(owner.retry_runtime().unwrap());
        }
        stopped(&owner);
        owner.begin_recovery().unwrap();
        let end = Instant::now() + Duration::from_secs(5);
        let retired = loop {
            owner.step().unwrap();
            if let Some(retired) = owner.take_retirement() {
                break retired;
            }
            assert!(Instant::now() < end);
        };
        let producer = retired.producer.unwrap();
        assert_eq!(
            producer.acknowledged as usize
                + producer.parents.len()
                + usize::from(producer.negotiation.is_some()),
            initial
        );
        if unknown {
            assert_eq!(
                producer.failure,
                Some(delivery::Error::ReceiptLost(delivery::Target::Eoe))
            );
            assert_eq!(producer.acknowledged, 0);
        } else {
            assert!(producer.failure.is_none());
            assert_eq!(producer.acknowledged, 1);
        }
        // The third output filler may execute, but the producer behind it cannot.
        drop(barrier);
        owner.restart_runtime().unwrap();
        assert!(owner.signing().active().is_none());
        assert!(owner.production().is_none());
        assert!(owner.shutdown().errors.is_empty());
        drop(accepted);
        importer.stop();
        importer.join().await;
    }
}
