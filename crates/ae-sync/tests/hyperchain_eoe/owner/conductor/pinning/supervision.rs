use super::*;
use conductor::supervision::Notice;

fn native(kind: &str) -> serde_json::Value {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../fixtures/erlang_consensus_supervision.json"
    ))
    .unwrap();
    fixture["lifecycle"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["kind"] == kind)
        .unwrap()
        .clone()
}
fn native_generations(row: &serde_json::Value, event: &str) -> Vec<u64> {
    row["trace"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["child"] == "pinning" && e["event"] == event)
        .map(|e| e["generation"].as_u64().unwrap())
        .collect()
}

fn tick(
    c: &mut Conductor<Otp26>,
    now: Instant,
    notices: &mut Vec<Notice>,
    reports: &mut Vec<pinning::Report>,
) -> Result<conductor::Progress, conductor::Error> {
    let progress = c.step(1_000_000, now)?;
    while let Some(output) = c.take_output() {
        match output {
            Output::PinningSupervision(n) => notices.push(n),
            Output::Pinning(r) => {
                assert!(
                    r.error.is_none() || r.error == Some(pinning::Error::Cancelled),
                    "{r:?}"
                );
                reports.push(*r);
            }
            _ => (),
        }
    }
    Ok(progress)
}
fn replaced(
    c: &mut Conductor<Otp26>,
    now: Instant,
    generation: u64,
    notices: &mut Vec<Notice>,
) -> Vec<pinning::Report> {
    let mut reports = vec![];
    let end = Instant::now() + Duration::from_secs(5);
    while c.status().consensus.pinning_generation < generation {
        tick(c, now, notices, &mut reports).unwrap();
        assert!(Instant::now() < end, "{:?}", c.status());
        thread::sleep(Duration::from_millis(1));
    }
    while tick(c, now, notices, &mut reports).unwrap() == conductor::Progress::Output {}
    reports
}
fn records(node: &Node) -> Vec<ae_storage::pinning_children::Record> {
    node.db
        .pinning_children()
        .unwrap()
        .records(Default::default())
        .unwrap()
}
#[tokio::test]
async fn permanent_pinning_replacement_preserves_siblings_state_and_exact_child_transactions() {
    for index in [0, 2, 4] {
        let node = Node::new(index);
        let wallet = Wallet::new();
        let mut c = configured(&node, &wallet, |_| {});
        prefix(&node, &mut c, &mut vec![], 6).await;
        let original = records(&node);
        assert_eq!(original.len(), 1);
        let importer = c.importer();
        let parent = c.parent();
        let pins = c.parent_pins().unwrap();
        let pool = c.pool();
        let votes = c.votes();
        let old = c.pinning().unwrap();
        old.request_stop();
        wait(|| !old.status().running);
        let mut notices = vec![];
        replaced(&mut c, Instant::now(), 2, &mut notices);
        old.request_stop();
        assert!(!old.status().running);
        assert!(c.pinning().unwrap().status().running);
        assert!(Arc::ptr_eq(&importer, &c.importer()));
        assert!(
            !parent.is_closed()
                && pins.check().is_ok()
                && pool.info().accepting
                && votes.info().accepting
        );
        let mut reports = vec![];
        for block in node.blocks().into_iter().skip(6).take(3) {
            within(
                importer
                    .import_block(block.encode().unwrap(), 1_000_000)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
            settled(
                &mut c,
                &mut reports,
                node.db.view().unwrap().status().revision,
            );
        }
        assert_eq!(children(&reports).len(), 1);
        assert_eq!(children(&reports)[0].0, pinning::ChildKind::Proof);
        assert!(records(&node).contains(&original[0]));
        assert_eq!(records(&node).len(), 2);
        assert_eq!(wallet.posted.lock().unwrap().len(), 1);
        assert_eq!(wallet.requests.load(Ordering::Acquire), 3);
        assert!(matches!(
            notices.as_slice(),
            [
                Notice::Retired { generation: 1, .. },
                Notice::Restarted { generation: 2 }
            ]
        ));
        let mut generations = vec![1];
        generations.extend(notices.iter().filter_map(|n| match n {
            Notice::Restarted { generation } => Some(*generation),
            _ => None,
        }));
        assert_eq!(
            generations,
            native_generations(&native("normal"), "started")
        );
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/erlang_consensus_supervision.json"
        ))
        .unwrap();
        assert!(
            fixture["lifecycle"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|c| ["normal", "boom"].contains(&c["kind"].as_str().unwrap()))
                .all(|c| c["siblings_unchanged"] == true)
        );
        drop(notices);
        drop(shutdown(c));
    }
}
#[tokio::test]
async fn restart_intensity_matches_native_permanent_child_and_inclusive_window() {
    for last in [30, 31] {
        let node = Node::new(0);
        let wallet = Wallet::new();
        let mut c = configured(&node, &wallet, |_| {});
        let origin = Instant::now();
        let mut notices = vec![];
        tick(&mut c, origin, &mut notices, &mut vec![]).unwrap();
        for (i, second) in [0, 10, 20, last].into_iter().enumerate() {
            let old = c.pinning().unwrap();
            old.request_stop();
            wait(|| !old.status().running);
            let now = origin + Duration::from_secs(second);
            if last == 30 && i == 3 {
                let end = Instant::now() + Duration::from_secs(5);
                loop {
                    match tick(&mut c, now, &mut notices, &mut vec![]) {
                        Err(conductor::Error::ConsensusRestartLimit) => break,
                        Ok(_) => (),
                        other => panic!("{other:?}"),
                    }
                    assert!(Instant::now() < end);
                    thread::sleep(Duration::from_millis(1));
                }
            } else {
                replaced(&mut c, now, i as u64 + 2, &mut notices);
            }
        }
        assert_eq!(c.status().consensus.attempts, 4);
        assert_eq!(
            c.status().consensus.pinning_generation,
            if last == 30 { 4 } else { 5 }
        );
        assert_eq!(
            c.status().failure,
            (last == 30).then_some(conductor::Error::ConsensusRestartLimit)
        );
        assert_eq!(wallet.requests.load(Ordering::Acquire), 0);
        let exit = shutdown(c);
        assert_eq!(exit.consensus.status.attempts, 4);
        if last == 30 {
            assert_eq!(
                counters_from_native_limit(),
                exit.consensus.status.pinning_generation
            );
            assert!(matches!(
                exit.consensus.notices.as_slice(),
                [Notice::Retired { generation: 4, .. }]
            ));
        }
    }
}

fn counters_from_native_limit() -> u64 {
    native_generations(&native("pinning_limit"), "started").len() as u64
}
#[tokio::test]
async fn failed_replacement_is_reported_counts_against_budget_and_can_retry_after_release() {
    let node = Node::new(0);
    let wallet = Wallet::new();
    let mut c = configured(&node, &wallet, |cfg| {
        cfg.parent
            .pins
            .as_mut()
            .unwrap()
            .agent
            .as_mut()
            .unwrap()
            .outputs = 1
    });
    let old = c.pinning().unwrap();
    old.request_stop();
    wait(|| !old.status().running);
    let mut notices = vec![];
    let end = Instant::now() + Duration::from_secs(5);
    while c.pinning().is_some() {
        tick(&mut c, Instant::now(), &mut notices, &mut vec![]).unwrap();
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(1));
    }
    let blocker =
        ae_storage::pinning_children::Writer::start(node.db.clone(), Default::default()).unwrap();
    tick(&mut c, Instant::now(), &mut notices, &mut vec![]).unwrap();
    assert_eq!(c.status().consensus.attempts, 1);
    assert!(c.pinning().is_none());
    assert!(matches!(
        notices.last(),
        Some(Notice::StartFailed {
            attempt: 1,
            error: pinning::Error::Children(ae_storage::pinning_children::Failure::AlreadyRunning)
        })
    ));
    drop(blocker);
    replaced(&mut c, Instant::now(), 2, &mut notices);
    assert_eq!(c.status().consensus.attempts, 2);
    assert_eq!(
        c.status().consensus.attempts,
        native_generations(&native("failed_restart"), "attempt").len() as u64 - 1
    );
    assert_eq!(
        c.status().consensus.pinning_generation,
        native_generations(&native("failed_restart"), "started").len() as u64
    );
    assert_eq!(wallet.requests.load(Ordering::Acquire), 0);
    drop(notices);
    drop(shutdown(c));
}
#[tokio::test]
async fn restarting_during_parent_broadcast_preserves_unknown_receipt_without_reposting() {
    let node = Node::new(0);
    let wallet = Wallet::new();
    let mut c = configured(&node, &wallet, |_| {});
    prefix(&node, &mut c, &mut vec![], 5).await;
    wallet.mode.store(3, Ordering::Release);
    within(
        c.importer()
            .import_block(node.blocks()[5].encode().unwrap(), 1_000_000)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    wait(|| wallet.posted.lock().unwrap().len() == 1);
    let old = c.pinning().unwrap();
    old.request_stop();
    wait(|| !old.status().running);
    assert_eq!(old.status().failure, Some(pinning::Error::Cancelled));
    let mut notices = vec![];
    let mut reports = replaced(&mut c, Instant::now(), 2, &mut notices);
    wallet.mode.store(0, Ordering::Release);
    settled(
        &mut c,
        &mut reports,
        node.db.view().unwrap().status().revision,
    );
    for block in node.blocks().into_iter().skip(6).take(3) {
        within(
            c.importer()
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        settled(
            &mut c,
            &mut reports,
            node.db.view().unwrap().status().revision,
        );
    }
    assert!(records(&node).is_empty());
    assert_eq!(wallet.posted.lock().unwrap().len(), 1);
    assert_eq!(wallet.requests.load(Ordering::Acquire), 2);
    assert!(
        reports.iter().flat_map(|r| &r.effects).any(|e| matches!(
            e,
            pinning::Effect::Parent(parent_pins::posting::Outcome::Unknown { .. })
        )) || notices.iter().any(|n| {
            match n {
                Notice::Retired { child, .. } => child
                    .outputs
                    .iter()
                    .chain(child.result.as_ref().unwrap().retained.iter())
                    .flat_map(|r| &r.effects)
                    .any(|e| {
                        matches!(
                            e,
                            pinning::Effect::Parent(parent_pins::posting::Outcome::Unknown { .. })
                        )
                    }),
                _ => false,
            }
        })
    );
    drop(notices);
    drop(shutdown(c));
}

#[tokio::test]
async fn bounded_retirement_notices_delay_replacement_without_losing_owners_or_receipts() {
    let node = Node::new(0);
    let wallet = Wallet::new();
    let mut c = configured(&node, &wallet, |cfg| {
        cfg.parent
            .pins
            .as_mut()
            .unwrap()
            .agent
            .as_mut()
            .unwrap()
            .outputs = 1
    });
    let old = c.pinning().unwrap();
    old.request_stop();
    wait(|| !old.status().running);
    let end = Instant::now() + Duration::from_secs(5);
    while c.output().is_none() {
        c.step(1_000_000, Instant::now()).unwrap();
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(matches!(
        c.output(),
        Some(Output::PinningSupervision(Notice::Retired {
            generation: 1,
            ..
        }))
    ));
    c.step(1_000_000, Instant::now()).unwrap();
    assert_eq!(c.status().consensus.pinning_generation, 2);
    assert_eq!(c.status().consensus.pending_notices, 1);
    let second = c.pinning().unwrap();
    second.request_stop();
    wait(|| !second.status().running);
    for _ in 0..10 {
        c.step(1_000_000, Instant::now()).unwrap();
    }
    assert_eq!(c.status().consensus.attempts, 1);
    assert_eq!(c.status().consensus.pending_notices, 1);
    let retained = c.take_output().unwrap();
    let mut notices = vec![];
    replaced(&mut c, Instant::now(), 3, &mut notices);
    assert_eq!(c.status().consensus.attempts, 2);
    assert!(matches!(
        notices.as_slice(),
        [
            Notice::Restarted { generation: 2 },
            Notice::Retired { generation: 2, .. },
            Notice::Restarted { generation: 3 }
        ]
    ));
    old.request_stop();
    second.request_stop();
    assert!(c.pinning().unwrap().status().running);
    let exit = shutdown(c);
    assert_eq!(exit.consensus.status.attempts, 2);
    assert_eq!(wallet.requests.load(Ordering::Acquire), 0);
    drop((notices, retained, exit));
}

#[tokio::test]
async fn dedicated_runtime_replaces_only_pinning_and_continues_actual_proof_admission() {
    let node = Node::new(0);
    let wallet = Wallet::new();
    let mut c = configured(&node, &wallet, |_| {});
    prefix(&node, &mut c, &mut vec![], 6).await;
    let importer = c.importer();
    let old = c.pinning().unwrap();
    let running = conductor::runtime::Runtime::spawn(
        c,
        conductor::runtime::SystemClock,
        conductor::runtime::Settings {
            poll: Duration::from_millis(1),
            outputs: 1,
        },
    )
    .unwrap_or_else(|_| panic!("runtime spawn"));
    let handle = running.handle();
    old.request_stop();
    let mut outputs = vec![];
    let end = Instant::now() + Duration::from_secs(10);
    while handle.status().consensus.pinning_generation < 2 {
        while let Some(o) = running.try_output() {
            outputs.push(o);
        }
        assert!(
            handle.status().failure.is_none() && Instant::now() < end,
            "{:?}",
            handle.status()
        );
        thread::sleep(Duration::from_millis(1));
    }
    for block in node.blocks().into_iter().skip(6).take(3) {
        within(
            importer
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
    }
    let head = node.db.view().unwrap().status();
    loop {
        while let Some(o) = running.try_output() {
            outputs.push(o);
        }
        let checkpoint = node.db.pinning_agent(Default::default()).unwrap().unwrap();
        if checkpoint.consumed == head {
            assert!(!checkpoint.state.enabled);
            break;
        }
        assert!(
            handle.status().failure.is_none() && Instant::now() < end,
            "{:?}",
            handle.status()
        );
        thread::sleep(Duration::from_millis(1));
    }
    let mut exit = running.shutdown().unwrap();
    outputs.extend(exit.queued_outputs);
    outputs.extend(exit.output);
    outputs.extend(
        exit.consensus
            .notices
            .into_iter()
            .map(Output::PinningSupervision),
    );
    let child = exit.pinning.take().unwrap().unwrap();
    outputs.extend(
        child
            .outputs
            .into_iter()
            .chain(child.retained)
            .map(|r| Output::Pinning(Box::new(r))),
    );
    assert!(outputs.iter().any(|o| matches!(
        o,
        Output::PinningSupervision(Notice::Restarted { generation: 2 })
    )));
    assert_eq!(
        outputs
            .iter()
            .filter_map(|o| match o {
                Output::Pinning(r) => Some(r),
                _ => None,
            })
            .flat_map(|r| &r.effects)
            .filter(|e| matches!(
                e,
                pinning::Effect::Child {
                    kind: pinning::ChildKind::Proof,
                    ..
                }
            ))
            .count(),
        1
    );
    assert_eq!(records(&node).len(), 2);
    assert_eq!(wallet.posted.lock().unwrap().len(), 1);
    assert_eq!(wallet.requests.load(Ordering::Acquire), 3);
    assert!(handle.joined() && !old.status().running);
}
