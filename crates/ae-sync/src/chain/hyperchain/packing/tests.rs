use super::*;
use crate::test_support::hyperchain::Node;

fn spawn(node: &Node) -> worker::Worker {
    worker::Worker::spawn(
        node.db.clone(),
        node.config.profile.network.clone(),
        pool::Config {
            consensus: ae_protocol::ConsensusType::Pos,
            ..Default::default()
        },
        worker::Settings {
            origins_interval: None,
            ..Default::default()
        },
    )
    .unwrap()
}
fn observed(pool: worker::Client, stage: Stage, error: worker::Error) -> Cancelled {
    // State after the cancellation path consumed a terminal receipt. These
    // saved outcomes must remain exact until the originating worker is joined.
    Cancelled {
        pool,
        retired: false,
        observed: Some((stage, error)),
        pending: None,
        report: Default::default(),
    }
}
#[test]
fn consumed_departure_receipts_require_the_exact_joined_pool_and_preserve_the_first_result() {
    for stage in [Stage::Selection, Stage::Failures] {
        for error in [worker::Error::Stopped, worker::Error::Disconnected] {
            let node = Node::new(1);
            let worker = spawn(&node);
            let old = worker.client();
            let mut cancelled = observed(old.clone(), stage, error.clone());
            assert_eq!(cancelled.step(), Err(pool_error(stage, error.clone())));
            assert_eq!(cancelled.retire_pool(&old), Err(Error::Binding));
            worker.shutdown().unwrap();
            let replacement = spawn(&node);
            let unrelated = replacement.client();
            replacement.shutdown().unwrap();
            assert!(!cancelled.retire_pool(&unrelated).unwrap());
            assert_eq!(cancelled.step(), Err(pool_error(stage, error.clone())));
            assert!(cancelled.retire_pool(&old).unwrap());
            assert!(cancelled.step().unwrap());
            let report = cancelled.finish();
            match stage {
                Stage::Selection => assert_eq!(report.selection, Some(Err(error))),
                Stage::Failures => assert_eq!(report.failure_reports, Some(Err(error))),
            }
            assert_eq!(unrelated.info().submitted, 0);
        }
    }
}

#[test]
fn joining_a_pool_does_not_convert_an_operational_mutation_failure_into_recoverable_departure() {
    let node = Node::new(1);
    let worker = spawn(&node);
    let client = worker.client();
    let error = worker::Error::Service(pool::Failure::RestartRequired);
    let mut cancelled = observed(client.clone(), Stage::Failures, error.clone());
    assert_eq!(
        cancelled.step(),
        Err(pool_error(Stage::Failures, error.clone()))
    );
    worker.shutdown().unwrap();
    cancelled.retire_pool(&client).unwrap();
    assert_eq!(cancelled.step(), Err(pool_error(Stage::Failures, error)));
    assert!(cancelled.report.failure_reports.is_none());
}

#[test]
fn a_real_terminal_selection_receipt_is_retained_after_its_ticket_has_been_consumed() {
    let node = Node::new(1);
    let worker = spawn(&node);
    let client = worker.client();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let view = snapshots.view().unwrap();
    let call = client
        .candidate_at(
            view.value.clone(),
            ae_types::Hash256([255; 32]),
            1_000_000,
            Default::default(),
            Default::default(),
        )
        .unwrap();
    let mut cancelled = Cancelled {
        pool: client.clone(),
        retired: false,
        observed: None,
        pending: Some(CancelReceipt::Selection(call)),
        report: Default::default(),
    };
    let end = std::time::Instant::now() + Duration::from_secs(5);
    let error = loop {
        match cancelled.step() {
            Err(error) => break error,
            Ok(false) => {
                assert!(std::time::Instant::now() < end);
                std::thread::yield_now();
            }
            Ok(true) => panic!("unknown parent must fail selection"),
        }
    };
    assert!(matches!(
        error,
        Error::Pool {
            error: worker::Error::Service(pool::Failure::Repository(_)),
            ..
        }
    ));
    assert_eq!(cancelled.step(), Err(error.clone()));
    worker.shutdown().unwrap();
    cancelled.retire_pool(&client).unwrap();
    assert_eq!(cancelled.step(), Err(error));
    assert!(cancelled.report.selection.is_none());
}
