use super::*;
use ae_sync::{chain::pow::group as production, production_group as supervisor};
use lifecycle::{Gate, GateClock, Release};

fn node() -> Node {
    let mut node = Node::new(1, false);
    drop(node.generator.take().unwrap().shutdown().unwrap());
    node
}
fn config(node: &Node) -> production::Config<Otp26> {
    production::Config {
        generator: configuration(&node.db, &node.network, node.cycle),
        producer: node.config(),
        poll: Duration::from_millis(2),
        outputs: 8,
    }
}
fn spawn(
    node: &Node,
    cfg: production::Config<Otp26>,
    clocks: Arc<dyn production::Clocks>,
) -> Result<production::Worker<Otp26>, production::Error> {
    production::Worker::spawn(
        &node.importer,
        node._snapshots.clone(),
        node.pool.as_ref().unwrap().client(),
        node.keys(),
        cfg,
        clocks,
    )
}
fn start(node: &Node) -> production::Worker<Otp26> {
    spawn(
        node,
        config(node),
        Arc::new(production::SystemClocks::default()),
    )
    .unwrap()
}
fn generation(client: &production::Client<Otp26>, n: u64) -> production::Generation<Otp26> {
    until(|| {
        assert!(client.status().failure.is_none(), "{:?}", client.status());
        while let Some(output) = client.front() {
            assert!(client.acknowledge(&output));
        }
        client
            .current()
            .is_some_and(|c| c.id.generator == n && c.id.producer == n)
    });
    client.current().unwrap()
}
fn output(
    client: &production::Client<Otp26>,
    condition: impl Fn(&production::Event) -> bool,
) -> Arc<production::Output> {
    let mut found = None;
    until(|| {
        assert!(client.status().failure.is_none(), "{:?}", client.status());
        while let Some(o) = client.front() {
            assert!(client.acknowledge(&o));
            if condition(&o.event) {
                found = Some(o);
                return true;
            }
        }
        false
    });
    found.unwrap()
}
fn admitted(client: &production::Client<Otp26>) -> Hash256 {
    let o = output(client, |e| {
        matches!(e, production::Event::Produced { output, .. }
        if matches!(output.completion, producer::Completion::Admitted { .. }))
    });
    let production::Event::Produced { output, .. } = &o.event else {
        unreachable!()
    };
    let producer::Completion::Admitted { hash, result, .. } = &output.completion else {
        unreachable!()
    };
    assert!(result.is_ok(), "{result:?}");
    *hash
}
fn finish(worker: production::Worker<Otp26>) -> production::Report {
    let mut report = worker.shutdown().unwrap();
    until(|| report.poll().unwrap());
    assert!(!report.status.retiring);
    report
}

#[test]
fn either_actual_child_failure_restarts_both_and_preserves_stable_services() {
    for generator_fails in [false, true] {
        let node = node();
        let signer = node.keys().info().keys;
        let worker = start(&node);
        let client = worker.client();
        let first = generation(&client, 1);
        node.push(1);
        let first_hash = admitted(&client);
        if generator_fails {
            first.generator.request_shutdown();
        } else {
            first.producer.request_shutdown();
        }
        let retired = output(&client, |e| matches!(e, production::Event::Retired(_)));
        let production::Event::Retired(retired) = &retired.event else {
            unreachable!()
        };
        let (g, exit) = retired.generator.as_ref().unwrap();
        assert_eq!(*g, 1);
        assert!(!exit.status.running);
        let (p, exit) = retired.producer.as_ref().unwrap();
        assert_eq!(*p, 1);
        assert!(!exit.status.running);
        assert_eq!(exit.status.admissions, 1);
        let second = generation(&client, 2);
        assert!(matches!(
            first.generator.start(),
            Err(generator::RequestError::Closed)
        ));
        assert_eq!(
            first.producer.set_enabled(true),
            Err(producer::Error::Closed)
        );
        assert_eq!(node.keys().info().keys, signer);
        assert!(node.pool.as_ref().unwrap().client().info().accepting);
        node.push(2);
        let second_hash = admitted(&client);
        let Block::Micro(block) = node.db.view().unwrap().require_block(second_hash).unwrap()
        else {
            panic!()
        };
        assert_eq!(block.header.common.previous_hash, first_hash.0);
        assert_eq!(block.transactions, vec![node.transaction(2)]);
        assert_eq!(second.producer.status().admissions, 1);
        assert!(finish(worker).status.failure.is_none());
    }
}

#[test]
fn disabled_desired_state_survives_pair_reconstruction() {
    let node = node();
    let worker = start(&node);
    let client = worker.client();
    let first = generation(&client, 1);
    let revision = client.set_enabled(false).unwrap();
    assert_eq!(client.set_enabled(false).unwrap(), revision);
    until(|| client.status().applied_revision == Some(revision));
    first.producer.request_shutdown();
    let second = generation(&client, 2);
    until(|| client.status().applied_revision == Some(revision));
    assert!(!second.producer.status().enabled);
    node.push(1);
    std::thread::sleep(Duration::from_millis(node.cycle * 2));
    assert_eq!(second.producer.status().signatures, 0);
    assert_eq!(node.db.view().unwrap().status().revision, 2);
    assert_eq!(client.set_enabled(true).unwrap(), revision + 1);
    admitted(&client);
    assert!(finish(worker).status.failure.is_none());
}

#[test]
fn retained_output_credit_blocks_replacement_and_shutdown_still_joins() {
    for shutdown in [false, true] {
        let node = node();
        let mut cfg = config(&node);
        cfg.outputs = 1;
        let worker = spawn(&node, cfg, Arc::new(production::SystemClocks::default())).unwrap();
        let client = worker.client();
        until(|| client.current().is_some() && client.front().is_some());
        let first = client.current().unwrap();
        let ready = client.front().unwrap();
        assert!(matches!(ready.event, production::Event::Ready(_)));
        assert!(client.acknowledge(&ready));
        assert!(!client.acknowledge(&ready));
        first.producer.request_shutdown();
        until(|| client.status().retiring);
        assert!(!first.generator.status().running);
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(client.status().supervisor.generator_generation, 1);
        assert!(client.current().is_none());
        if shutdown {
            let report = finish(worker);
            assert_eq!(report.settled.len(), 1);
            assert_eq!(report.status.supervisor.generator_generation, 1);
        } else {
            drop(ready);
            generation(&client, 2);
            assert!(finish(worker).status.failure.is_none());
        }
    }
}

#[test]
fn root_lease_survives_joined_report_and_factory_drop_does_not_stop_importer() {
    let node = node();
    let worker = start(&node);
    generation(&worker.client(), 1);
    let another = || {
        spawn(
            &node,
            config(&node),
            Arc::new(production::SystemClocks::default()),
        )
    };
    assert!(matches!(
        another(),
        Err(production::Error::Ownership(
            snapshots::Error::AlreadyRunning
        ))
    ));
    let report = finish(worker);
    assert!(report.is_settled());
    assert!(matches!(
        another(),
        Err(production::Error::Ownership(
            snapshots::Error::AlreadyRunning
        ))
    ));
    drop(report);
    let worker = another().unwrap();
    generation(&worker.client(), 1);
    node.push(1);
    admitted(&worker.client());
    assert!(finish(worker).status.failure.is_none());
    assert_eq!(node.db.view().unwrap().status().revision, 3);
    assert!(node.keys().info().accepting);
}

struct Clocks {
    fail_producer: Option<u64>,
    first_gate: Option<Arc<Gate>>,
}
impl production::Clocks for Clocks {
    fn generator(&self, _: u64) -> Arc<dyn generator::Clock> {
        Arc::new(generator::SystemClock)
    }
    fn producer(&self, generation: u64) -> Box<dyn producer::Clock> {
        assert_ne!(
            self.fail_producer,
            Some(generation),
            "controlled partial construction failure"
        );
        if generation == 1
            && let Some(gate) = &self.first_gate
        {
            Box::new(GateClock(gate.clone()))
        } else {
            Box::new(producer::SystemClock)
        }
    }
    fn second(&self) -> u64 {
        0
    }
}
#[test]
fn partial_constructor_failure_retires_the_real_generator_before_retry() {
    let node = node();
    let worker = spawn(
        &node,
        config(&node),
        Arc::new(Clocks {
            fail_producer: Some(2),
            first_gate: None,
        }),
    )
    .unwrap();
    let client = worker.client();
    generation(&client, 1).producer.request_shutdown();
    let failed = output(&client, |e| matches!(e, production::Event::StartFailed(_)));
    let production::Event::StartFailed(f) = &failed.event else {
        unreachable!()
    };
    assert_eq!(f.child, supervisor::Member::Conductor);
    assert_eq!(f.generation, 2);
    assert!(matches!(f.error, supervisor::StartError::Panicked));
    let retired = output(
        &client,
        |e| matches!(e, production::Event::Retired(r) if r.producer.is_none()),
    );
    let production::Event::Retired(r) = &retired.event else {
        unreachable!()
    };
    assert_eq!(r.generator.as_ref().unwrap().0, 2);
    assert!(!r.generator.as_ref().unwrap().1.status.running);
    generation(&client, 3);
    node.push(1);
    admitted(&client);
    assert_eq!(client.status().supervisor.attempts, 2);
    assert!(finish(worker).status.failure.is_none());
}

#[test]
fn initial_constructor_failure_returns_joined_partial_child_and_keeps_stable_sources() {
    let node = node();
    let worker = spawn(
        &node,
        config(&node),
        Arc::new(Clocks {
            fail_producer: Some(1),
            first_gate: None,
        }),
    )
    .unwrap();
    until(|| worker.is_finished());
    let report = finish(worker);
    assert_eq!(
        report.status.failure,
        Some(production::Error::Startup(supervisor::Member::Conductor))
    );
    assert!(matches!(
        report.unpublished.as_slice(),
        [production::Event::StartFailed(_)]
    ));
    assert_eq!(report.settled.len(), 1);
    assert!(report.settled[0].producer.is_none());
    assert!(
        !report.settled[0]
            .generator
            .as_ref()
            .unwrap()
            .1
            .status
            .running
    );
    drop(report);
    let worker = start(&node);
    generation(&worker.client(), 1);
    node.push(1);
    admitted(&worker.client());
    assert!(finish(worker).status.failure.is_none());
}

#[test]
fn sixth_actual_pair_failure_exhausts_the_native_restart_budget() {
    let node = node();
    let worker = spawn(
        &node,
        config(&node),
        Arc::new(Clocks {
            fail_producer: None,
            first_gate: None,
        }),
    )
    .unwrap();
    let client = worker.client();
    for n in 1..=6 {
        generation(&client, n).producer.request_shutdown();
    }
    until(|| {
        while let Some(o) = client.front() {
            assert!(client.acknowledge(&o));
        }
        worker.is_finished()
    });
    let report = finish(worker);
    assert_eq!(
        report.status.failure,
        Some(production::Error::Supervision(
            supervisor::Error::RestartLimit
        ))
    );
    assert_eq!(report.status.supervisor.attempts, 6);
    assert_eq!(report.status.supervisor.generator_generation, 6);
    assert_eq!(report.status.supervisor.conductor_generation, 6);
    assert!(node.keys().info().accepting);
    assert!(node.pool.as_ref().unwrap().client().info().accepting);
}

#[test]
fn sibling_failure_retains_exact_signing_and_admission_receipts_before_replacement() {
    for admission in [false, true] {
        let node = node();
        node.push(1);
        let gate = Gate::new();
        let worker = spawn(
            &node,
            config(&node),
            Arc::new(Clocks {
                fail_producer: None,
                first_gate: Some(gate.clone()),
            }),
        )
        .unwrap();
        let _release = Release(gate.clone());
        let client = worker.client();
        let first = generation(&client, 1);
        gate.reach(&first.producer, |s| {
            s.pending
                && if admission {
                    s.admissions == 1
                } else {
                    s.signatures == 1
                }
        });
        let disabled = client.set_enabled(false).unwrap();
        first.generator.request_shutdown();
        until(|| client.current().is_none());
        assert_eq!(client.status().supervisor.generator_generation, 1);
        assert!(!first.producer.status().accepting);
        gate.allow(usize::MAX);
        let retired = output(&client, |e| matches!(e, production::Event::Retired(_)));
        let production::Event::Retired(r) = &retired.event else {
            unreachable!()
        };
        let (_, p) = r.producer.as_ref().unwrap();
        assert_eq!(p.status.admissions, u64::from(admission));
        assert_eq!(p.completions.len(), 1);
        if admission {
            let producer::Completion::Admitted { hash, result, .. } = &p.completions[0] else {
                panic!()
            };
            assert_eq!(result.as_ref().unwrap().accepted.len(), 1);
            assert_eq!(node.db.view().unwrap().status().tip, Some(*hash));
        } else {
            assert!(matches!(
                p.completions[0],
                producer::Completion::Discarded { result: Ok(_), .. }
            ));
        }
        let second = generation(&client, 2);
        until(|| client.status().applied_revision == Some(disabled));
        assert!(!second.producer.status().enabled);
        assert_eq!(
            node.db.view().unwrap().status().revision,
            2 + u64::from(admission)
        );
        assert!(finish(worker).status.failure.is_none());
        assert_eq!(p.completions.len(), 1);
    }
}

#[test]
fn shutdown_fences_a_blocked_producer_without_waiting_for_output_capacity() {
    let node = node();
    node.push(1);
    let gate = Gate::new();
    let mut cfg = config(&node);
    cfg.outputs = 1;
    let worker = spawn(
        &node,
        cfg,
        Arc::new(Clocks {
            fail_producer: None,
            first_gate: Some(gate.clone()),
        }),
    )
    .unwrap();
    let _release = Release(gate.clone());
    let client = worker.client();
    until(|| client.current().is_some() && gate.calls() > 0);
    let first = client.current().unwrap();
    let output = client.front().unwrap();
    client.request_shutdown();
    assert_eq!(client.set_enabled(true), Err(production::Error::Closed));
    assert!(!first.producer.status().accepting);
    gate.allow(usize::MAX);
    let report = finish(worker);
    assert_eq!(report.outputs.len(), 1);
    assert!(Arc::ptr_eq(&report.outputs[0], &output));
    assert_eq!(
        report.settled[0]
            .producer
            .as_ref()
            .unwrap()
            .1
            .status
            .signatures,
        0
    );
    assert_eq!(node.db.view().unwrap().status().revision, 2);
    assert!(node.keys().info().accepting);
}

struct ConstructionClocks(Arc<Gate>);
impl production::Clocks for ConstructionClocks {
    fn generator(&self, _: u64) -> Arc<dyn generator::Clock> {
        Arc::new(generator::SystemClock)
    }
    fn producer(&self, generation: u64) -> Box<dyn producer::Clock> {
        if generation == 2 {
            producer::Clock::now(&GateClock(self.0.clone())).unwrap();
        }
        Box::new(producer::SystemClock)
    }
    fn second(&self) -> u64 {
        0
    }
}
#[test]
fn disable_or_shutdown_during_construction_prevents_replacement_activation() {
    for shutdown in [false, true] {
        let node = node();
        let gate = Gate::new();
        let worker = spawn(
            &node,
            config(&node),
            Arc::new(ConstructionClocks(gate.clone())),
        )
        .unwrap();
        let _release = Release(gate.clone());
        let client = worker.client();
        generation(&client, 1).producer.request_shutdown();
        until(|| gate.calls() == 1);
        assert!(client.current().is_none());
        node.push(1);
        let revision = client.set_enabled(false).unwrap();
        if shutdown {
            client.request_shutdown();
        }
        gate.allow(usize::MAX);
        if shutdown {
            let report = finish(worker);
            let (_, p) = report.settled.last().unwrap().producer.as_ref().unwrap();
            assert_eq!(p.status.signatures, 0);
            assert_eq!(report.status.supervisor.generator_generation, 2);
        } else {
            let second = generation(&client, 2);
            until(|| client.status().applied_revision == Some(revision));
            std::thread::sleep(Duration::from_millis(node.cycle * 2));
            assert!(!second.producer.status().enabled);
            assert_eq!(second.producer.status().signatures, 0);
            assert!(finish(worker).status.failure.is_none());
        }
        assert_eq!(node.db.view().unwrap().status().revision, 2);
    }
}

struct SupervisorClock {
    producer: Arc<Gate>,
    fail: std::sync::atomic::AtomicBool,
}
impl production::Clocks for SupervisorClock {
    fn generator(&self, _: u64) -> Arc<dyn generator::Clock> {
        Arc::new(generator::SystemClock)
    }
    fn producer(&self, _: u64) -> Box<dyn producer::Clock> {
        Box::new(GateClock(self.producer.clone()))
    }
    fn second(&self) -> u64 {
        assert!(
            !self.fail.load(Ordering::SeqCst),
            "controlled supervisor clock failure"
        );
        0
    }
}
#[test]
fn supervisor_unwind_preserves_accepted_child_receipts_and_joins_both_children() {
    for admission in [false, true] {
        let node = node();
        node.push(1);
        let gate = Gate::new();
        let clocks = Arc::new(SupervisorClock {
            producer: gate.clone(),
            fail: false.into(),
        });
        let worker = spawn(&node, config(&node), clocks.clone()).unwrap();
        let _release = Release(gate.clone());
        let client = worker.client();
        let first = generation(&client, 1);
        gate.reach(&first.producer, |s| {
            s.pending
                && if admission {
                    s.admissions == 1
                } else {
                    s.signatures == 1
                }
        });
        clocks.fail.store(true, Ordering::SeqCst);
        until(|| client.current().is_none());
        assert!(!first.producer.status().accepting);
        gate.allow(usize::MAX);
        until(|| worker.is_finished());
        let mut report = finish(worker);
        assert_eq!(report.status.failure, Some(production::Error::Panicked));
        assert!(!first.generator.status().running);
        let (_, p) = report.settled[0].producer.as_ref().unwrap();
        assert_eq!(p.completions.len(), 1);
        match &p.completions[0] {
            producer::Completion::Admitted {
                result: Ok(r),
                hash,
                ..
            } if admission => {
                assert_eq!(r.accepted.len(), 1);
                assert_eq!(node.db.view().unwrap().status().tip, Some(*hash));
            }
            producer::Completion::Discarded { result: Ok(_), .. } if !admission => (),
            other => panic!("unexpected completion {other:?}"),
        }
        for _ in 0..10 {
            assert!(report.poll().unwrap());
        }
        assert_eq!(
            report.settled[0]
                .producer
                .as_ref()
                .unwrap()
                .1
                .completions
                .len(),
            1
        );
        assert_eq!(
            node.db.view().unwrap().status().revision,
            2 + u64::from(admission)
        );
    }
}

#[test]
fn invalid_configuration_and_foreign_snapshot_cannot_start_a_pair() {
    let node = node();
    let clocks = || Arc::new(production::SystemClocks::default());
    for outputs in [0, 129] {
        let mut cfg = config(&node);
        cfg.outputs = outputs;
        assert!(matches!(
            spawn(&node, cfg, clocks()),
            Err(production::Error::Configuration)
        ));
    }
    for poll in [Duration::ZERO, Duration::from_secs(2)] {
        let mut cfg = config(&node);
        cfg.poll = poll;
        assert!(matches!(
            spawn(&node, cfg, clocks()),
            Err(production::Error::Configuration)
        ));
    }
    let mut cfg = config(&node);
    cfg.producer.micro_cycle_ms += 1;
    assert!(matches!(
        spawn(&node, cfg, clocks()),
        Err(production::Error::Binding)
    ));
    let other = Node::new(1, false);
    let worker = production::Worker::spawn(
        &node.importer,
        other._snapshots.clone(),
        node.pool.as_ref().unwrap().client(),
        node.keys(),
        config(&node),
        clocks(),
    )
    .unwrap();
    until(|| worker.is_finished());
    let report = finish(worker);
    assert_eq!(report.status.supervisor.phase, supervisor::Phase::Failed);
    assert_eq!(report.status.supervisor.generator_generation, 1);
    assert_eq!(report.status.supervisor.conductor_generation, 0);
    let [production::Event::StartFailed(f)] = report.unpublished.as_slice() else {
        panic!()
    };
    assert!(matches!(
        f.error,
        supervisor::StartError::Returned(production::StartError::Generator(
            generator::Error::Binding
        ))
    ));
    assert!(report.settled[0].generator.is_none());
    assert!(report.settled[0].producer.is_none());
    assert_eq!(node.db.view().unwrap().status().revision, 2);
}

#[test]
fn drop_fences_and_joins_instead_of_detaching_a_blocked_child() {
    let node = node();
    let gate = Gate::new();
    let worker = spawn(
        &node,
        config(&node),
        Arc::new(Clocks {
            fail_producer: None,
            first_gate: Some(gate.clone()),
        }),
    )
    .unwrap();
    let _release = Release(gate.clone());
    let client = worker.client();
    let first = generation(&client, 1);
    until(|| gate.calls() > 0);
    let dropped = std::thread::spawn(move || drop(worker));
    until(|| !client.status().accepting);
    assert!(!dropped.is_finished());
    assert!(!first.producer.status().accepting);
    gate.allow(usize::MAX);
    dropped.join().unwrap();
    assert!(!first.generator.status().running);
    assert!(!first.producer.status().running);
    let next = start(&node);
    generation(&next.client(), 1);
    node.push(1);
    admitted(&next.client());
    assert!(finish(next).status.failure.is_none());
}
