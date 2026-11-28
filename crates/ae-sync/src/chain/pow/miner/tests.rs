use super::*;
use ae_consensus::{cuckoo::solver::Phase, target::HIGHEST_TARGET};
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

fn work(start: u64, repeats: u64) -> Work {
    Work {
        algorithm: ConsensusAlgorithm::Cuckoo,
        hash: blake2b_256(b"wsffgujnjkqhduihsahswgdf"),
        target: HIGHEST_TARGET,
        edge_bits: 15,
        nonces: Nonces::reserve(u128::from(start), repeats).unwrap(),
    }
}
fn until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "mining worker deadline");
        thread::sleep(Duration::from_millis(1));
    }
}
fn pause(worker: &Worker, phase: Phase, panic: bool) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (entered, wait) = mpsc::channel();
    let (release, resumed) = mpsc::channel();
    let mut used = false;
    worker.on_progress(move |p| {
        if !used && p.phase == phase {
            used = true;
            entered.send(()).unwrap();
            resumed.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(!panic, "controlled miner panic");
        }
    });
    (wait, release)
}
fn received(wait: &mpsc::Receiver<()>) {
    wait.recv_timeout(Duration::from_secs(5)).unwrap();
}
#[test]
fn on_demand_callback_matches_native_headers_without_graph_search() {
    use ae_codec::headers::{Header, HeaderMode};
    let rows: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../ae-consensus/tests/fixtures/erlang_on_demand.json"
    ))
    .unwrap();
    let worker = Worker::spawn_on_demand(Config::default()).unwrap();
    let client = worker.client();
    let mut count = 0;
    for row in rows["blocks"].as_array().unwrap() {
        if row["result"]["ok"] != true {
            continue;
        }
        let expected = &row["result"]["value"];
        let Header::Key(mut header) = Header::decode(
            &hex::decode(expected["before_hex"].as_str().unwrap()).unwrap(),
            HeaderMode::ProofOfWork,
        )
        .unwrap() else {
            panic!()
        };
        let work = Work::on_demand(&header).unwrap();
        assert_eq!(hex::encode_upper(work.hash.0), expected["sealing_input"]);
        let ticket = client.submit(work).unwrap();
        let result = ticket.blocking_wait();
        assert_eq!(result.id, ticket.id());
        assert_eq!(result.work, work);
        assert_eq!(result.result, Ok(Outcome::OnDemand { seal: 1338 }));
        assert_eq!((result.attempts, result.work_units), (Some(1), Some(0)));
        assert!(result.external.is_none());
        ticket.request_cancel();
        assert!(!ticket.cancel_if_pending());
        assert!(Arc::ptr_eq(&result, &ticket.try_result().unwrap()));
        ae_consensus::on_demand::seal(&mut header);
        assert_eq!(
            hex::encode_upper(Header::Key(header).encode().unwrap()),
            expected["sealed_hex"]
        );
        count += 1;
    }
    assert_eq!(count, 24);
    let status = worker.shutdown();
    assert!(!status.running && !status.accepting);
    assert_eq!(status.completed, 24);
    assert_eq!(status.pending, 0);
}
#[test]
fn on_demand_workers_bind_consensus_and_retain_receipt_capacity_after_join() {
    let worker = Worker::spawn_on_demand(Config {
        requests: 1,
        ..Default::default()
    })
    .unwrap();
    let client = worker.client();
    let cuckoo = work(38, 1);
    assert!(matches!(client.submit(cuckoo), Err(Error::Configuration)));
    let request = Work {
        algorithm: ConsensusAlgorithm::OnDemand,
        target: Target(1338),
        edge_bits: 0,
        nonces: Nonces::reserve(1338, 1).unwrap(),
        ..cuckoo
    };
    let graph = Worker::spawn(Config::default()).unwrap();
    assert!(matches!(
        graph.client().submit(request),
        Err(Error::Configuration)
    ));
    assert_eq!(graph.shutdown().submitted, 0);
    for invalid in [
        Work {
            edge_bits: 29,
            ..request
        },
        Work {
            target: Target(1339),
            ..request
        },
        Work {
            nonces: Nonces::reserve(1339, 1).unwrap(),
            ..request
        },
        Work {
            nonces: Nonces::reserve(1338, 2).unwrap(),
            ..request
        },
        Work {
            algorithm: ConsensusAlgorithm::Hyperchain,
            ..request
        },
    ] {
        assert!(matches!(client.submit(invalid), Err(Error::Configuration)));
    }
    let ticket = client.submit(request).unwrap();
    let result = ticket.blocking_wait();
    drop(ticket);
    assert!(matches!(client.submit(request), Err(Error::Busy)));
    let status = worker.shutdown();
    assert_eq!(status.completed, 1);
    assert!(!status.running && !status.accepting);
    assert_eq!(result.result, Ok(Outcome::OnDemand { seal: 1338 }));
    assert!(matches!(client.submit(request), Err(Error::Closed)));
    drop(result);
    assert_eq!(client.retained(), 0);
}
fn found(receipt: &Completion, expected_nonce: u64) {
    let Ok(Outcome::Found { nonce, proof }) = &receipt.result else {
        panic!("{receipt:?}");
    };
    assert_eq!(*nonce, expected_nonce);
    assert!(cuckoo::verify_proof_from_hash(
        receipt.work.hash,
        *nonce,
        proof.as_ref(),
        receipt.work.edge_bits
    ));
    let rows: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../ae-consensus/tests/fixtures/erlang_pow_solver_solutions.json"
    ))
    .unwrap();
    assert!(rows.as_array().unwrap().iter().any(|r| r["nonce"] == *nonce
        && r["proof"] == serde_json::json!(proof.as_slice())
        && r["accepted"] == true));
}
#[test]
fn cpu_worker_searches_reserved_nonces_and_retains_exact_verified_result() {
    let worker = Worker::spawn(Config::default()).unwrap();
    let client = worker.client();
    let request = work(36, 4);
    let ticket = client.submit(request).unwrap();
    let result = ticket.blocking_wait();
    found(&result, 38);
    assert_eq!(result.id, ticket.id());
    assert_eq!(result.work, request);
    assert_eq!(result.attempts, Some(3));
    assert!(result.work_units.unwrap() > 0);
    assert!(!ticket.cancel_if_pending());
    ticket.request_cancel();
    assert!(!ticket.cancel_if_pending());
    assert!(Arc::ptr_eq(&result, &ticket.try_result().unwrap()));
    let status = worker.shutdown();
    assert!(!status.running && !status.accepting);
    assert_eq!(status.completed, 1);
    assert_eq!(status.pending, 0);
    found(&ticket.blocking_wait(), 38);
    assert!(matches!(client.submit(request), Err(Error::Closed)));
}
#[test]
fn retained_tickets_and_result_copies_share_one_capacity_credit() {
    let worker = Worker::spawn(Config {
        requests: 1,
        ..Config::default()
    })
    .unwrap();
    let client = worker.client();
    let ticket = client.submit(work(38, 1)).unwrap();
    let cloned = ticket.clone();
    let result = ticket.blocking_wait();
    assert_eq!(client.retained(), 1);
    assert!(matches!(client.submit(work(1, 1)), Err(Error::Busy)));
    drop(ticket);
    drop(cloned);
    assert!(matches!(client.submit(work(1, 1)), Err(Error::Busy)));
    drop(result);
    until(|| client.retained() == 0);
    let exhausted = client.submit(work(1, 1)).unwrap().blocking_wait();
    assert_eq!(exhausted.result, Ok(Outcome::Exhausted));
    assert_eq!(exhausted.attempts, Some(1));
    assert_eq!(worker.shutdown().completed, 2);
}
#[test]
fn cooperative_cancellation_interrupts_actual_trim_sort_and_cycle_search() {
    for phase in [Phase::Trim, Phase::Sort, Phase::Search] {
        let worker = Worker::spawn(Config::default()).unwrap();
        let (wait, release) = pause(&worker, phase, false);
        let ticket = worker.client().submit(work(38, 1)).unwrap();
        received(&wait);
        ticket.request_cancel();
        assert!(ticket.try_result().is_none());
        assert!(!worker.is_finished());
        release.send(()).unwrap();
        let result = ticket.blocking_wait();
        assert!(
            matches!(result.result, Err(Error::Solve { failure, .. }) if failure.error == solver::Error::Cancelled && failure.progress.phase == phase)
        );
        found(
            &worker.client().submit(work(38, 1)).unwrap().blocking_wait(),
            38,
        );
        assert_eq!(worker.shutdown().completed, 2);
    }
}
#[test]
fn shutdown_joins_active_search_cancels_queue_and_keeps_old_receipts_bound() {
    let worker = Worker::spawn(Config::default()).unwrap();
    let client = worker.client();
    let (wait, release) = pause(&worker, Phase::Trim, false);
    let active = client.submit(work(38, 1)).unwrap();
    received(&wait);
    let queued = client.submit(work(38, 1)).unwrap();
    worker.request_shutdown();
    assert!(!worker.is_finished());
    assert!(matches!(client.submit(work(38, 1)), Err(Error::Closed)));
    release.send(()).unwrap();
    let status = worker.shutdown();
    assert_eq!(status.completed, 2);
    assert_eq!(status.pending, 0);
    assert!(
        matches!(active.blocking_wait().result, Err(Error::Solve { failure, .. }) if failure.error == solver::Error::Cancelled)
    );
    let cancelled = queued.blocking_wait();
    assert_eq!(cancelled.result, Err(Error::Cancelled));
    assert_eq!(cancelled.attempts, Some(0));
    let replacement = Worker::spawn(Config::default()).unwrap();
    let new = replacement.client().submit(work(38, 1)).unwrap();
    assert_ne!(new.id().worker, active.id().worker);
    found(&new.blocking_wait(), 38);
    assert!(Arc::ptr_eq(&cancelled, &queued.blocking_wait()));
    assert!(matches!(client.submit(work(38, 1)), Err(Error::Closed)));
    replacement.shutdown();
}
#[test]
fn panicked_worker_resolves_accepted_receipts_without_fabricated_accounting() {
    let worker = Worker::spawn(Config::default()).unwrap();
    let client = worker.client();
    let (wait, release) = pause(&worker, Phase::Trim, true);
    let active = client.submit(work(38, 1)).unwrap();
    received(&wait);
    let queued = client.submit(work(1, 1)).unwrap();
    release.send(()).unwrap();
    until(|| worker.is_finished());
    let status = worker.shutdown();
    assert_eq!(status.failure, Some(Error::Panicked));
    assert_eq!(status.completed, 2);
    let active = active.blocking_wait();
    assert_eq!(active.result, Err(Error::Panicked));
    assert_eq!(active.attempts, None);
    assert_eq!(active.work_units, None);
    let queued = queued.blocking_wait();
    assert_eq!(queued.result, Err(Error::Panicked));
    assert_eq!(queued.attempts, Some(0));
    assert_eq!(queued.work_units, Some(0));
}
#[test]
fn dropping_callers_does_not_cancel_accepted_mining_and_releases_credit_after_completion() {
    let worker = Worker::spawn(Config {
        requests: 2,
        ..Config::default()
    })
    .unwrap();
    let client = worker.client();
    let (wait, release) = pause(&worker, Phase::Trim, false);
    let active = client.submit(work(38, 1)).unwrap();
    received(&wait);
    let queued = client.submit(work(38, 1)).unwrap();
    drop(active);
    drop(queued);
    assert_eq!(client.retained(), 2);
    release.send(()).unwrap();
    until(|| client.status().completed == 2 && client.retained() == 0);
    assert_eq!(client.status().failure, None);
    found(&client.submit(work(38, 1)).unwrap().blocking_wait(), 38);
    assert_eq!(worker.shutdown().completed, 3);
}
#[test]
fn whole_job_work_limit_and_key_mode_guards_preserve_operational_errors() {
    let request = work(36, 4);
    let solo = solver::solve(
        Input {
            hash: request.hash,
            nonce: 36,
            target: request.target,
            edge_bits: 15,
        },
        Limits::default(),
        |_| false,
    )
    .unwrap();
    assert!(solo.solution.is_none());
    let worker = Worker::spawn(Config {
        solver: Limits {
            work: solo.progress.work + 1,
            ..Limits::default()
        },
        ..Config::default()
    })
    .unwrap();
    let limited = worker.client().submit(request).unwrap().blocking_wait();
    assert_eq!(limited.attempts, Some(2));
    assert_eq!(limited.work_units, Some(solo.progress.work));
    assert!(
        matches!(limited.result,Err(Error::Solve { nonce: 37, failure }) if failure.error == solver::Error::WorkLimit)
    );
    worker.shutdown();
    let header = KeyHeader {
        common: ae_codec::headers::CommonHeader {
            version: 1,
            height: 1,
            previous_hash: [1; 32],
            previous_key_hash: [1; 32],
            state_hash: [2; 32],
        },
        miner: [3; 32],
        beneficiary: [4; 32],
        target: HIGHEST_TARGET.0,
        seal: [5; 42],
        nonce: 99,
        time: 123,
        info: None,
        hole: false,
        end_of_epoch: false,
    };
    let key = Work::key(&header, Nonces::reserve(0, 1).unwrap()).unwrap();
    assert_eq!(key.edge_bits, 29);
    assert_eq!(
        key.hash,
        blake2b_256(&cuckoo::sealing_bytes(&header).unwrap())
    );
    let worker = Worker::spawn(Config {
        maximum_attempts: 1,
        solver: Limits {
            memory_bytes: 1024,
            ..Limits::default()
        },
        ..Config::default()
    })
    .unwrap();
    let client = worker.client();
    assert!(matches!(client.submit(request), Err(Error::Configuration)));
    assert!(matches!(
        client.submit(Work {
            edge_bits: 0,
            ..key
        }),
        Err(Error::Configuration)
    ));
    assert_eq!(client.status().submitted, 0);
    let failed = client.submit(key).unwrap().blocking_wait();
    assert!(
        matches!(failed.result,Err(Error::Solve { failure, .. }) if matches!(failure.error,solver::Error::Memory {..}))
    );
    assert_eq!(failed.work_units, Some(0));
    worker.shutdown();
    let retry = Worker::spawn(Config::default()).unwrap();
    found(&retry.client().submit(request).unwrap().blocking_wait(), 38);
    retry.shutdown();
    assert!(matches!(
        Worker::spawn(Config {
            requests: 0,
            ..Config::default()
        }),
        Err(Error::Configuration)
    ));
}

#[test]
fn first_pending_cancellation_wins_once_and_preserves_active_and_queued_receipts() {
    let worker = Worker::spawn(Config::default()).unwrap();
    let (wait, release) = pause(&worker, Phase::Allocate, false);
    let active = worker.client().submit(work(38, 1)).unwrap();
    received(&wait);
    let queued = worker.client().submit(work(38, 1)).unwrap();
    assert!(active.cancel_if_pending());
    assert!(!active.cancel_if_pending());
    queued.request_cancel();
    assert!(!queued.cancel_if_pending());
    assert!(active.try_result().is_none());
    assert!(queued.try_result().is_none());
    release.send(()).unwrap();
    let result = active.blocking_wait();
    assert!(
        matches!(result.result, Err(Error::Solve { failure, .. }) if failure.error == solver::Error::Cancelled)
    );
    assert_eq!(result.id, active.id());
    assert_eq!(result.work, active.work());
    assert!(Arc::ptr_eq(&result, &active.blocking_wait()));
    assert!(!active.cancel_if_pending());
    assert_eq!(queued.blocking_wait().result, Err(Error::Cancelled));
    assert_eq!(worker.shutdown().completed, 2);
}
