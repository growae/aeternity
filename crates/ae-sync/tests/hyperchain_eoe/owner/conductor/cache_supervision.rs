use super::*;
use conductor::supervision::Notice;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    thread,
};

fn native(kind: &str) -> serde_json::Value {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/erlang_cache_supervision.json"
    ))
    .unwrap();
    fixture["lifecycle"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["kind"] == kind)
        .unwrap()
        .clone()
}

fn drive(
    c: &mut Conductor<Otp26>,
    now: Instant,
    outputs: &mut Vec<Output>,
) -> Result<Progress, conductor::Error> {
    let progress = c.step(1_000_000, now)?;
    if let Some(output) = c.take_output() {
        outputs.push(output);
    }
    Ok(progress)
}
fn replaced(c: &mut Conductor<Otp26>, now: Instant, generation: u64, outputs: &mut Vec<Output>) {
    let end = Instant::now() + Duration::from_secs(5);
    while c.status().consensus.cache_generation < generation
        || c.status().consensus.pending_notices > 0
    {
        drive(c, now, outputs).unwrap();
        assert!(Instant::now() < end, "{:?}", c.status());
        thread::sleep(Duration::from_millis(1));
    }
}

#[tokio::test]
async fn cache_replacement_rebuilds_at_selected_tip_without_replacing_importer_or_siblings() {
    for index in [0, 2, 4] {
        let node = Node::new(index);
        let mut c = start(&node, false);
        let importer = c.importer();
        let connector = c.connector();
        let votes = c.votes();
        let old = c.parent();
        assert_eq!(
            old.get_block(2.into(), 1000.into()).unwrap().unwrap(),
            parent(2, 1002)
        );
        let blocks = node.blocks();
        assert!(matches!(blocks[1], Block::Key(_)));
        for block in &blocks[..2] {
            importer
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait()
                .await
                .unwrap();
        }
        let selected = node.db.view().unwrap().status();
        // Deliberately leave the old top_changed event in the conductor inbox.
        old.request_stop();
        let mut outputs = vec![];
        let now = Instant::now();
        replaced(&mut c, now, 2, &mut outputs);
        let new = c.parent();
        assert!(!new.same_runtime(&old));
        assert!(old.is_closed());
        assert_eq!(old.get_once(2.into()), Err(cache::Error::Closed));
        old.request_stop();
        assert!(!new.is_closed());
        assert_eq!(new.snapshot().cache.child_top().hash, selected.tip.unwrap());
        assert_eq!(
            new.snapshot().cache.child_top().height,
            blocks[1].header().common().height.into()
        );
        assert!(!new.snapshot().cache.blocks().contains_key(&2.into()));
        assert!(Arc::ptr_eq(&importer, &c.importer()));
        assert!(votes.info().accepting && c.pool().info().accepting);
        connector.barrier(Duration::from_secs(5)).unwrap();
        assert_eq!(c.status().consensus.connector_generation, 1);
        assert_eq!(c.status().consensus.attempts, 1);
        assert_eq!(
            native("normal")["cache_starts"],
            c.status().consensus.cache_generation
        );
        assert_eq!(native("normal")["siblings_unchanged"], true);
        assert_eq!(
            new.get_block(2.into(), 1000.into()).unwrap().unwrap(),
            parent(2, 1002)
        );
        let end = Instant::now() + Duration::from_secs(5);
        while c.status().cursor.revision() < selected.revision {
            drive(&mut c, now, &mut outputs).unwrap();
            assert!(Instant::now() < end);
        }
        assert_eq!(new.snapshot().cache.child_top().hash, selected.tip.unwrap());
        importer
            .import_block(blocks[2].encode().unwrap(), 1_000_000)
            .unwrap()
            .wait()
            .await
            .unwrap();
        node.db.view().unwrap().verify_tip().unwrap();
        let retired = outputs
            .iter()
            .find_map(|o| match o {
                Output::ConsensusSupervision(Notice::CacheRetired {
                    generation: 1,
                    child,
                }) => Some(child),
                _ => None,
            })
            .unwrap();
        assert!(retired.child.result.is_ok());
        assert!(
            retired
                .child
                .snapshot
                .cache
                .blocks()
                .contains_key(&2.into())
        );
        assert!(outputs.iter().any(|o| matches!(o, Output::ConsensusSupervision(Notice::CacheRestarted { generation: 2, revision }) if *revision == selected.revision)));
        drop(shutdown(c));
    }
}

#[test]
fn active_production_settles_old_generation_before_resuming_after_cache_replacement() {
    for stage in [
        Progress::Started,
        Progress::Production(production::Progress::KeyPrepared),
        Progress::Production(production::Progress::Submitted(production::Kind::Key)),
    ] {
        let node = Node::new(1);
        let mut c = start(&node, true);
        let mut wall = 1000;
        let mut now = Instant::now();
        let end = now + Duration::from_secs(25);
        loop {
            let progress = tick(&mut c, &mut wall, &mut now);
            c.take_output();
            if progress == stage {
                break;
            }
            assert!(Instant::now() < end, "{stage:?}: {:?}", c.status());
        }
        c.parent().request_stop();
        let mut stopped = None;
        let mut produced = None;
        while produced.is_none() {
            tick(&mut c, &mut wall, &mut now);
            if let Some(output) = c.take_output() {
                match output {
                    Output::Stopped(s) => stopped = Some(s),
                    Output::Attempt(done)
                        if matches!(done.outcome, production::Outcome::Produced { .. }) =>
                    {
                        produced = Some(done)
                    }
                    _ => (),
                }
            }
            assert!(Instant::now() < end, "{stage:?}: {:?}", c.status());
        }
        let stopped = stopped.expect("old production retired");
        let production::Outcome::Produced { key, .. } = produced.unwrap().outcome else {
            unreachable!()
        };
        let view = node.db.view().unwrap();
        view.verify_tip().unwrap();
        assert_eq!(view.status().tip, Some(key.hash));
        if let Some(Ok(accepted)) = stopped.admission {
            let previous = &accepted.accepted[0];
            assert_ne!(previous.hash, key.hash);
            assert!(view.require_block(previous.hash).is_ok());
        }
        assert_eq!(c.status().consensus.cache_generation, 2);
        assert_eq!(c.status().consensus.attempts, 1);
        assert!(c.status().producing);
        drop(shutdown(c));
    }
}

#[test]
fn joined_cache_gap_shutdown_keeps_retirement_without_a_synthetic_active_exit() {
    let node = Node::new(0);
    let mut c = start(&node, false);
    c.parent().request_stop();
    let now = Instant::now();
    let end = now + Duration::from_secs(5);
    while c.output().is_none() {
        c.step(1000, now).unwrap();
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(matches!(
        c.output(),
        Some(Output::ConsensusSupervision(Notice::CacheRetired {
            generation: 1,
            ..
        }))
    ));
    let exit = shutdown(c);
    assert!(exit.parent.is_none());
    assert!(exit.consensus.status.cache_restarting);
    assert_eq!(exit.consensus.status.attempts, 0);
}

#[tokio::test]
async fn an_inflight_import_cannot_switch_its_parent_generation_before_commit() {
    let node = Node::new(0);
    let backend = Arc::new(ParentBackend {
        missing: AtomicBool::new(true),
    });
    let mut c = Conductor::start_with_targets(
        node.db.clone(),
        node.config.clone(),
        Default::default(),
        config(&node, false),
        backend.clone(),
        Targets,
    )
    .unwrap();
    let old = c.parent();
    let importer = c.importer();
    let before = node.db.view().unwrap().status();
    let bytes = node.blocks()[0].encode().unwrap();
    let call = importer.import_block(bytes.clone(), 1_000_000).unwrap();
    wait(|| old.snapshot().lookup_calls > 0);
    old.request_stop();
    backend.missing.store(false, Ordering::Release);
    replaced(&mut c, Instant::now(), 2, &mut vec![]);
    let error = call.wait().await.unwrap_err();
    assert!(
        matches!(
            error.error,
            ae_sync::chain::Error::Validation {
                operational: true,
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(node.db.view().unwrap().status(), before);
    let new = c.parent();
    new.post_block(parent(8, 1008)).unwrap();
    new.barrier(Duration::from_secs(5)).unwrap();
    wait(|| new.snapshot().cache.lookup(&1.into()).is_ok());
    importer
        .import_block(bytes, 1_000_000)
        .unwrap()
        .wait()
        .await
        .unwrap();
    node.check(0);
    assert!(Arc::ptr_eq(&importer, &c.importer()) && old.is_closed());
    drop(shutdown(c));
}

#[derive(Clone)]
struct ControlledTargets {
    mode: Arc<AtomicUsize>,
    entered: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
}
impl cache::Targets for ControlledTargets {
    fn targets(&mut self, _: &Unsigned) -> Result<Vec<BigInt>, cache::TargetError> {
        match self.mode.swap(0, Ordering::AcqRel) {
            1 => return Err(cache::TargetError::Native),
            2 => panic!("controlled parent cache callback panic"),
            3 => {
                self.entered.store(true, Ordering::Release);
                let end = Instant::now() + Duration::from_secs(5);
                while !self.release.load(Ordering::Acquire) && Instant::now() < end {
                    thread::sleep(Duration::from_millis(1));
                }
                assert!(
                    self.release.load(Ordering::Acquire),
                    "held cache callback expired"
                );
            }
            4 => return Err(cache::TargetError::Unavailable),
            _ => (),
        }
        Ok(vec![1.into(), 6.into()])
    }
}
fn controlled(node: &Node) -> (Conductor<Otp26>, ControlledTargets) {
    let targets = ControlledTargets {
        mode: Arc::new(AtomicUsize::new(0)),
        entered: Arc::new(AtomicBool::new(false)),
        release: Arc::new(AtomicBool::new(false)),
    };
    let c = Conductor::start_with_targets(
        node.db.clone(),
        node.config.clone(),
        Default::default(),
        config(node, false),
        Arc::new(ParentBackend {
            missing: AtomicBool::new(false),
        }),
        targets.clone(),
    )
    .unwrap();
    c.parent().post_block(parent(8, 1008)).unwrap();
    c.parent().barrier(Duration::from_secs(5)).unwrap();
    wait(|| c.parent().snapshot().cache.lookup(&1.into()).is_ok());
    (c, targets)
}

#[test]
fn native_cache_exceptions_and_panics_restart_but_failure_backlogs_stay_terminal() {
    for mode in [1, 2, 4] {
        let node = Node::new(0);
        let (mut c, targets) = controlled(&node);
        let old = c.parent();
        old.barrier(Duration::from_secs(5)).unwrap();
        if mode == 4 {
            old.configure(cache::Limits {
                maximum_failures: 0,
                ..Default::default()
            });
        }
        targets.mode.store(mode, Ordering::Release);
        old.post_block(parent(9, 1009)).unwrap();
        wait(|| old.is_closed());
        let mut outputs = vec![];
        if mode == 4 {
            old.request_stop();
            assert!(matches!(
                drive(&mut c, Instant::now(), &mut outputs),
                Err(conductor::Error::Parent(_))
            ));
            assert_eq!(c.status().consensus.attempts, 0);
            let exit = shutdown(c);
            let child = exit.parent.unwrap().unwrap();
            assert_eq!(child.reason, Some(cache::Error::FailureBacklog));
            assert!(child.unreported.is_some());
        } else {
            replaced(&mut c, Instant::now(), 2, &mut outputs);
            let retired = outputs
                .iter()
                .find_map(|o| match o {
                    Output::ConsensusSupervision(Notice::CacheRetired { child, .. }) => Some(child),
                    _ => None,
                })
                .unwrap();
            if mode == 1 {
                assert_eq!(
                    retired.child.result.as_ref().unwrap().reason,
                    Some(cache::Error::Targets(cache::TargetError::Native))
                );
                assert_eq!(retired.child.failures.len(), 1);
                assert_eq!(
                    native("native")["cache_starts"],
                    c.status().consensus.cache_generation
                );
            } else {
                assert!(retired.child.result.is_err());
            }
            assert!(!c.parent().is_closed());
            assert_eq!(c.status().consensus.connector_generation, 1);
            drop(shutdown(c));
        }
    }
}

#[tokio::test]
async fn queued_top_receipt_is_retained_after_join_and_never_replayed_on_fresh_cache() {
    let node = Node::new(0);
    let (mut c, targets) = controlled(&node);
    let blocks = node.blocks();
    assert!(matches!(blocks[1], Block::Key(_)));
    for block in &blocks[..2] {
        c.importer()
            .import_block(block.encode().unwrap(), 1_000_000)
            .unwrap()
            .wait()
            .await
            .unwrap();
    }
    let selected = node.db.view().unwrap().status();
    let old = c.parent();
    old.barrier(Duration::from_secs(5)).unwrap();
    targets.mode.store(3, Ordering::Release);
    old.post_block(parent(9, 1009)).unwrap();
    wait(|| targets.entered.load(Ordering::Acquire));
    // Consume the initial event, then accept ChildTop behind the gated callback.
    for _ in 0..5 {
        c.step(1_000_000, Instant::now()).unwrap();
        c.take_output();
    }
    let old_call = old.call(parent_cache::Event::Get(99.into())).unwrap();
    old.request_stop();
    let now = Instant::now();
    let mut outputs = vec![];
    for _ in 0..5 {
        drive(&mut c, now, &mut outputs).unwrap();
    }
    assert_eq!(c.status().consensus.attempts, 0);
    assert!(c.status().consensus.cache_restarting);
    targets.release.store(true, Ordering::Release);
    replaced(&mut c, now, 2, &mut outputs);
    assert_eq!(
        old_call.wait(Duration::from_secs(1)),
        Err(cache::Error::Closed)
    );
    let retired = outputs
        .iter()
        .find_map(|o| match o {
            Output::ConsensusSupervision(Notice::CacheRetired { child, .. }) => Some(child),
            _ => None,
        })
        .unwrap();
    let top = retired.top.as_ref().expect("accepted old top is retained");
    assert_eq!(top.record.change().revision, selected.revision);
    assert_eq!(top.result, Err(cache::Error::Closed));
    assert_eq!(native("queued_killed")["old_call_exited"], true);
    assert_eq!(native("queued_killed")["old_read_replayed"], false);
    assert_eq!(
        c.parent().snapshot().cache.child_top().hash,
        selected.tip.unwrap()
    );
    assert!(
        !c.parent()
            .snapshot()
            .cache
            .blocks()
            .contains_key(&99.into())
    );
    let end = Instant::now() + Duration::from_secs(5);
    while c.status().cursor.revision() < selected.revision {
        drive(&mut c, now, &mut outputs).unwrap();
        assert!(Instant::now() < end);
    }
    node.check(1);
    drop(shutdown(c));
}

#[tokio::test]
async fn dedicated_conductor_runtime_keeps_the_same_importer_after_cache_replacement() {
    let node = Node::new(0);
    let c = start(&node, false);
    let old = c.parent();
    let importer = c.importer();
    let runtime =
        conductor::runtime::Runtime::spawn(c, conductor::runtime::SystemClock, Default::default())
            .unwrap_or_else(|_| panic!("spawn conductor"));
    let handle = runtime.handle();
    old.request_stop();
    wait(|| handle.status().consensus.cache_generation == 2 || handle.status().failure.is_some());
    assert!(handle.status().failure.is_none(), "{:?}", handle.status());
    importer
        .import_block(node.blocks()[0].encode().unwrap(), 1_000_000)
        .unwrap()
        .wait()
        .await
        .unwrap();
    node.check(0);
    let exit = runtime.shutdown().unwrap();
    assert!(exit.parent.as_ref().is_some_and(Result::is_ok));
    assert_eq!(exit.consensus.status.attempts, 1);
    assert!(old.is_closed() && handle.joined());
}

#[tokio::test]
async fn an_accepted_entropy_read_lost_with_the_cache_is_reported_without_replay() {
    // Capture the complete native rejection batch without delivering it.
    // Multiple effects can target the same parent height; only an accepted
    // old call must be retired, while its unsent siblings remain deliverable.
    let expected = {
        let node = Node::new(0);
        let c = Conductor::start_with_targets(
            node.db.clone(),
            node.config.clone(),
            Default::default(),
            config(&node, false),
            Arc::new(ParentBackend {
                missing: AtomicBool::new(true),
            }),
            Targets,
        )
        .unwrap();
        let failure = c
            .importer()
            .import_block(node.blocks()[0].encode().unwrap(), 1_000_000)
            .unwrap()
            .wait()
            .await
            .unwrap_err();
        assert!(matches!(
            failure.error,
            ae_sync::chain::Error::Validation {
                operational: false,
                ..
            }
        ));
        shutdown(c).accepted.unwrap().entries().to_vec()
    };
    assert!(!expected.is_empty());
    let node = Node::new(0);
    let backend = Arc::new(ParentBackend {
        missing: AtomicBool::new(true),
    });
    let targets = ControlledTargets {
        mode: Arc::new(AtomicUsize::new(0)),
        entered: Arc::new(AtomicBool::new(false)),
        release: Arc::new(AtomicBool::new(false)),
    };
    let mut c = Conductor::start_with_targets(
        node.db.clone(),
        node.config.clone(),
        Default::default(),
        config(&node, false),
        backend.clone(),
        targets.clone(),
    )
    .unwrap();
    let importer = c.importer();
    let bytes = node.blocks()[0].encode().unwrap();
    let failure = importer
        .import_block(bytes.clone(), 1_000_000)
        .unwrap()
        .wait()
        .await
        .unwrap_err();
    assert!(matches!(
        failure.error,
        ae_sync::chain::Error::Validation {
            operational: false,
            ..
        }
    ));
    let old = c.parent();
    old.barrier(Duration::from_secs(5)).unwrap();
    targets.mode.store(3, Ordering::Release);
    old.post_block(parent(8, 1008)).unwrap();
    wait(|| targets.entered.load(Ordering::Acquire));
    // The failed native validation retained its RequestParent effect. Accept
    // that read behind the held callback, without allowing it to execute.
    let now = Instant::now();
    for _ in 0..5 {
        drive(&mut c, now, &mut vec![]).unwrap();
    }
    old.request_stop();
    targets.release.store(true, Ordering::Release);
    backend.missing.store(false, Ordering::Release);
    let mut outputs = vec![];
    replaced(&mut c, now, 2, &mut outputs);
    let retired = outputs
        .iter()
        .find_map(|o| match o {
            Output::ConsensusSupervision(Notice::CacheRetired { child, .. }) => Some(child),
            _ => None,
        })
        .unwrap();
    let lost = retired
        .delivery
        .as_ref()
        .expect("accepted entropy receipt is retained");
    assert!(matches!(
        lost.effect,
        ae_chain::repository::hyperchain::inputs::Effect::RequestParent(_)
    ));
    assert_eq!(lost.result, Err(cache::Error::Closed));
    assert_eq!(lost.effect, expected[0]);
    for _ in 0..5 {
        drive(&mut c, now, &mut outputs).unwrap();
    }
    // Other owners may read the new cache in the background. Check this
    // delivery outbox itself after shutdown, not the global lookup count.
    c.parent().post_block(parent(8, 1008)).unwrap();
    c.parent().barrier(Duration::from_secs(5)).unwrap();
    wait(|| c.parent().snapshot().cache.lookup(&1.into()).is_ok());
    importer
        .import_block(bytes, 1_000_000)
        .unwrap()
        .wait()
        .await
        .unwrap();
    node.check(0);
    let exit = shutdown(c);
    let acknowledged = exit.delivery.acknowledged() as usize;
    assert_eq!(
        1 + acknowledged + exit.delivery.pending().len(),
        expected.len()
    );
    assert_eq!(
        exit.delivery.pending().iter().collect::<Vec<_>>(),
        expected[1 + acknowledged..].iter().collect::<Vec<_>>()
    );
}
