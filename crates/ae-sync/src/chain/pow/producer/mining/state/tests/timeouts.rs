use super::*;
use ae_consensus::cuckoo::solver::{self, Phase};
use std::sync::mpsc;

struct Pause {
    entered: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
}
impl Pause {
    fn new(mining: &Mining<Otp26>, index: usize) -> Self {
        let (entered, wait) = mpsc::channel();
        let (release, resumed) = mpsc::channel();
        let mut used = false;
        mining.cpus[index]
            .worker
            .as_ref()
            .unwrap()
            .on_progress(move |p| {
                if !used && p.phase == Phase::Allocate {
                    used = true;
                    entered.send(()).unwrap();
                    resumed.recv_timeout(Duration::from_secs(10)).unwrap();
                }
            });
        Self {
            entered: wait,
            release,
        }
    }
    fn wait(&self) {
        self.entered.recv_timeout(Duration::from_secs(10)).unwrap();
    }
}
impl Drop for Pause {
    fn drop(&mut self) {
        let _ = self.release.send(());
    }
}
fn header() -> KeyHeader {
    let Block::Key(header) = fixtures::blocks(&NetworkProfile::mainnet()).remove(0) else {
        panic!()
    };
    header
}
fn poll(mining: &mut Mining<Otp26>, header: &KeyHeader, now: Instant) {
    mining
        .poll(
            Hash256(header.common.previous_hash),
            AccountPublicKey(header.miner),
            now,
        )
        .unwrap();
}
fn cancelled(result: &miner::Completion) {
    assert!(
        matches!(result.result, Err(miner::Error::Solve { failure, .. }) if failure.error == solver::Error::Cancelled && failure.progress.phase == Phase::Allocate)
    );
}

#[test]
fn native_default_and_finite_boundaries_preserve_timed_out_jobs_until_joined() {
    let native: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_pow_mining_timeout.json"
    )))
    .unwrap();
    let header = header();
    assert_eq!(
        configuration(&header).attempt_timeout.unwrap().as_millis(),
        native["lookup"][0]["result"].as_u64().unwrap() as u128
    );
    let node = Node::new();
    let importer = importer(&node);
    for row in native["expired"].as_array().unwrap() {
        let timeout = Duration::from_millis(row["timeout"].as_u64().unwrap());
        let mut config = configuration(&header);
        config.attempt_timeout = Some(timeout);
        let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
        let mut mining = ready(&importer, snapshots, &header, config);
        let paused = Pause::new(&mining, 0);
        let ctx = context(&header, true, false);
        let now = ctx.now;
        mining.act(&importer, ctx).unwrap();
        paused.wait();
        let ticket = mining.cpus[0].job.as_ref().unwrap().ticket.clone();
        poll(
            &mut mining,
            &header,
            now + timeout - Duration::from_nanos(1),
        );
        assert_eq!(mining.status().timeouts, 0);
        poll(&mut mining, &header, now + timeout);
        assert_eq!(mining.status().timeouts, 1);
        assert_eq!(mining.status().timeouts_pending, 1);
        assert!(ticket.try_result().is_none());
        let mut retired = mining.retire();
        let mut outputs = vec![];
        assert!(!retired.poll(&mut outputs));
        assert!(outputs.is_empty());
        drop(paused);
        let original = ticket.blocking_wait();
        cancelled(&original);
        until(|| retired.poll(&mut outputs));
        let [
            Completion::Searched {
                result,
                timed_out: true,
                selected: false,
                ..
            },
            Completion::Joined(status),
        ] = outputs.as_slice()
        else {
            panic!("{outputs:?}")
        };
        assert!(Arc::ptr_eq(result, &original));
        assert_eq!(status.submitted, 1);
        assert_eq!(status.completed, 1);
        assert!(retired.poll(&mut outputs));
        assert_eq!(outputs.len(), 2);
    }
}

#[test]
fn simultaneous_deadlines_fire_under_output_pressure_without_reusing_pending_instances() {
    let node = Node::new();
    let importer = importer(&node);
    let header = header();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let mut config = configuration(&header);
    config.instances = vec![config.instances[0].clone(); 3];
    config.retry = Duration::ZERO;
    config.attempt_timeout = Some(Duration::from_millis(5));
    let mut mining = ready(&importer, snapshots, &header, config);
    let pauses = [Pause::new(&mining, 1), Pause::new(&mining, 2)];
    let ctx = context(&header, true, false);
    let now = ctx.now;
    mining.act(&importer, ctx).unwrap();
    for pause in &pauses {
        pause.wait();
    }
    let tickets: Vec<_> = mining
        .cpus
        .iter()
        .map(|c| c.job.as_ref().unwrap().ticket.clone())
        .collect();
    let first = tickets[0].blocking_wait();
    poll(&mut mining, &header, now);
    assert!(
        matches!(&mining.output, Some(Completion::Searched { result, timed_out: false, .. }) if Arc::ptr_eq(result, &first))
    );
    for _ in 0..3 {
        poll(&mut mining, &header, now + Duration::from_millis(5));
    }
    assert_eq!(mining.status().timeouts, 2);
    assert_eq!(mining.status().timeouts_pending, 2);
    assert!(tickets[1..].iter().all(|t| t.try_result().is_none()));
    assert!(mining.status().workers.iter().all(|w| w.submitted == 1));
    mining.take_output();
    // A free instance may retry, but neither pending instance can be reused.
    mining
        .act(
            &importer,
            Context {
                now: now + Duration::from_millis(5),
                ..context(&header, true, false)
            },
        )
        .unwrap();
    assert_eq!(
        mining
            .status()
            .workers
            .iter()
            .map(|w| w.submitted)
            .collect::<Vec<_>>(),
        [2, 1, 1]
    );
    drop(pauses);
    for ticket in &tickets[1..] {
        cancelled(&ticket.blocking_wait());
    }
    for ticket in &tickets[1..] {
        poll(&mut mining, &header, now + Duration::from_millis(6));
        let Some(Completion::Searched {
            result,
            timed_out: true,
            selected: false,
            ..
        }) = mining.take_output()
        else {
            panic!()
        };
        assert!(Arc::ptr_eq(&result, &ticket.blocking_wait()));
    }
    assert_eq!(mining.status().timeouts_pending, 0);
    mining
        .act(
            &importer,
            Context {
                now: now + Duration::from_millis(6),
                ..context(&header, true, false)
            },
        )
        .unwrap();
    for (index, original) in tickets.iter().enumerate().skip(1) {
        let replacement = &mining.cpus[index].job.as_ref().unwrap().ticket;
        assert_ne!(replacement.id(), original.id());
        assert_ne!(replacement.work().nonces, original.work().nonces);
    }
    retire(mining);
}

#[test]
fn infinity_and_invalidation_do_not_become_elapsed_timeouts() {
    for invalidate in [false, true] {
        let node = Node::new();
        let importer = importer(&node);
        let header = header();
        let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
        let mut config = configuration(&header);
        config.attempt_timeout = invalidate.then_some(Duration::from_millis(1));
        let mut mining = ready(&importer, snapshots, &header, config);
        let paused = Pause::new(&mining, 0);
        mining
            .act(&importer, context(&header, true, false))
            .unwrap();
        paused.wait();
        if invalidate {
            mining.invalidate().unwrap();
        }
        poll(
            &mut mining,
            &header,
            Instant::now() + Duration::from_secs(86400),
        );
        assert_eq!(mining.status().timeouts, 0);
        assert_eq!(mining.status().active, 1);
        assert!(mining.output.is_none());
        // Ordinary retirement still cancels an infinite attempt.
        let mut retired = mining.retire();
        drop(paused);
        let mut outputs = vec![];
        until(|| retired.poll(&mut outputs));
        assert!(outputs.iter().any(|o| matches!(o, Completion::Searched { result, timed_out: false, .. } if {cancelled(result); true})));
    }
}

#[test]
fn completed_receipt_observed_after_deadline_keeps_its_exact_result() {
    let node = Node::new();
    let importer = importer(&node);
    let header = header();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let mut config = configuration(&header);
    config.attempt_timeout = Some(Duration::from_millis(1));
    let mut mining = ready(&importer, snapshots, &header, config);
    mining
        .act(&importer, context(&header, true, false))
        .unwrap();
    let original = mining.cpus[0].job.as_ref().unwrap().ticket.blocking_wait();
    poll(
        &mut mining,
        &header,
        Instant::now() + Duration::from_secs(60),
    );
    assert_eq!(mining.status().timeouts, 0);
    let Some(Completion::Searched {
        result,
        timed_out: false,
        ..
    }) = mining.take_output()
    else {
        panic!()
    };
    assert!(Arc::ptr_eq(&result, &original));
    assert!(
        matches!(result.result, Err(miner::Error::Solve { failure, .. }) if matches!(failure.error, solver::Error::Memory { .. }))
    );
    retire(mining);
}

#[test]
fn a_public_proof_replayed_after_timeout_is_retained_but_never_selected() {
    let node = Node::new();
    let (metrics, _binding) = MiningMetrics::bind(node.db.instrumentation());
    let importer = importer(&node);
    let header = header();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let mut config = configuration(&header);
    config.attempt_timeout = Some(Duration::from_millis(1));
    let mut mining = ready(&importer, snapshots, &header, config);
    let paused = Pause::new(&mining, 0);
    mining
        .act(&importer, context(&header, true, false))
        .unwrap();
    paused.wait();
    poll(
        &mut mining,
        &header,
        Instant::now() + Duration::from_secs(1),
    );
    drop(paused);
    // This is an unchanged public proof replay, not a fresh solver success.
    let replay = Arc::new(replay(&mining, &header));
    mining.searched(0, replay.clone(), Instant::now()).unwrap();
    let Some(Completion::Searched {
        result,
        timed_out: true,
        selected: false,
        ..
    }) = mining.take_output()
    else {
        panic!()
    };
    assert!(Arc::ptr_eq(&result, &replay));
    assert!(mining.solution.is_none() && mining.admission.is_none());
    assert_eq!(node.db.view().unwrap().status().revision, 1);
    retire(mining);
    assert_eq!(metrics.counts(), (0, 0));
}

#[test]
fn invalid_duration_and_unrepresentable_deadline_fail_before_cpu_submission() {
    let node = Node::new();
    let importer = importer(&node);
    let header = header();
    let snapshots = snapshots::Owner::start(node.db.clone(), Default::default()).unwrap();
    let mut config = configuration(&header);
    config.attempt_timeout = Some(Duration::ZERO);
    assert!(matches!(
        Mining::new(
            &importer,
            Inputs {
                snapshots: snapshots.clone(),
                config: config.clone(),
                clock: Arc::new(Clock(header.time))
            }
        ),
        Err(Error::Configuration)
    ));
    config.attempt_timeout = Some(Duration::MAX);
    config.instances = vec![config.instances[0].clone(); 2];
    let mut mining = ready(&importer, snapshots, &header, config);
    assert_eq!(
        mining.act(&importer, context(&header, true, false)),
        Err(Error::Time)
    );
    assert_eq!(mining.status().active, 0);
    assert!(mining.status().workers.iter().all(|w| w.submitted == 0));
    retire(mining);
}
