use super::*;
use ae_storage::pinning_agent as journal;

fn saved(node: &Node) -> journal::Checkpoint {
    node.db.pinning_agent(Default::default()).unwrap().unwrap()
}
async fn remaining(
    node: &Node,
    c: &mut Conductor<Otp26>,
    from: usize,
    reports: &mut Vec<pinning::Report>,
) {
    for block in node.blocks().into_iter().skip(from).take(9 - from) {
        within(
            c.importer()
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
        settled(c, reports, node.db.view().unwrap().status().revision);
    }
}
fn stored_children(node: &Node) -> Vec<ae_storage::pinning_children::Record> {
    node.db
        .pinning_children()
        .unwrap()
        .records(Default::default())
        .unwrap()
}
#[tokio::test]
async fn reopened_agent_preserves_state_and_submits_only_the_remaining_proof_across_profiles() {
    for index in [0, 2, 4] {
        let node = Node::new(index);
        let wallet = Wallet::new();
        let mut c = configured(&node, &wallet, |_| {});
        prefix(&node, &mut c, &mut vec![], 6).await;
        let before = saved(&node);
        assert!(before.state.enabled && before.state.active.as_ref().unwrap().child_note);
        let note = stored_children(&node);
        assert_eq!(note.len(), 1);
        drop(shutdown(c));
        let node = node.reopen();
        assert_eq!(saved(&node), before);
        let mut invalid = configuration(&node, &wallet);
        invalid.parent.pins.as_mut().unwrap().accounts.clear();
        assert!(matches!(
            new(&node, invalid),
            Err(conductor::Error::Pinning(pinning::Error::Journal(
                journal::Failure::Binding
            )))
        ));
        let mut c = configured(&node, &wallet, |_| {});
        assert_eq!(c.pinning().unwrap().status().state, before.state);
        let mut reports = vec![];
        remaining(&node, &mut c, 6, &mut reports).await;
        assert_eq!(children(&reports).len(), 1);
        assert_eq!(children(&reports)[0].0, pinning::ChildKind::Proof);
        assert!(stored_children(&node).contains(&note[0]));
        assert_eq!(stored_children(&node).len(), 2);
        assert!(!saved(&node).state.enabled);
        assert_eq!(wallet.posted.lock().unwrap().len(), 1);
        assert_eq!(wallet.requests.load(Ordering::Acquire), 3);
        drop(shutdown(c));
    }
}
#[tokio::test]
async fn completed_events_under_output_pressure_recover_without_replaying_note_or_parent() {
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
    prefix(&node, &mut c, &mut vec![], 5).await;
    within(
        c.importer()
            .import_block(node.blocks()[5].encode().unwrap(), 1_000_000)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    wait(|| {
        saved(&node)
            .state
            .active
            .as_ref()
            .is_some_and(|a| a.child_note)
    });
    let checkpoint = saved(&node);
    assert_eq!(checkpoint.pending.as_ref().unwrap().next, 2);
    assert_eq!(
        checkpoint.consumed.revision + 1,
        node.db.view().unwrap().status().revision
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
    }
    drop(shutdown(c));
    let node = node.reopen();
    let mut c = configured(&node, &wallet, |_| {});
    let mut reports = vec![];
    settled(
        &mut c,
        &mut reports,
        node.db.view().unwrap().status().revision,
    );
    assert_eq!(children(&reports).len(), 1);
    assert_eq!(children(&reports)[0].0, pinning::ChildKind::Proof);
    assert_eq!(stored_children(&node).len(), 2);
    assert_eq!(wallet.posted.lock().unwrap().len(), 1);
    assert_eq!(wallet.requests.load(Ordering::Acquire), 3);
    assert!(!saved(&node).state.enabled);
    assert!(saved(&node).pending.is_none());
    drop(shutdown(c));
}
#[tokio::test]
async fn failed_proof_resumes_after_reopen_without_repeating_the_completed_note() {
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
            .children
            .records = 1
    });
    prefix(&node, &mut c, &mut vec![], 5).await;
    let error = durable_children::import_until_failure(&node, &mut c).await;
    assert!(matches!(error, pinning::Error::Children(_)));
    let before = saved(&node);
    assert!(before.pending.is_some());
    let note = stored_children(&node)[0].clone();
    drop(shutdown(c));
    let node = node.reopen();
    let mut c = configured(&node, &wallet, |_| {});
    let mut reports = vec![];
    settled(
        &mut c,
        &mut reports,
        node.db.view().unwrap().status().revision,
    );
    assert_eq!(children(&reports).len(), 1);
    assert_eq!(children(&reports)[0].0, pinning::ChildKind::Proof);
    assert!(stored_children(&node).contains(&note));
    assert_eq!(stored_children(&node).len(), 2);
    assert!(!saved(&node).state.enabled);
    assert_eq!(wallet.posted.lock().unwrap().len(), 1);
    drop(shutdown(c));
}
#[tokio::test]
async fn interrupted_parent_result_and_prepared_request_survive_newer_selected_tip() {
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
    let prepared = saved(&node).pending.unwrap().operation.post.unwrap();
    for block in node.blocks().into_iter().skip(6).take(3) {
        within(
            c.importer()
                .import_block(block.encode().unwrap(), 1_000_000)
                .unwrap()
                .wait(),
        )
        .await
        .unwrap();
    }
    drop(shutdown(c));
    let before = saved(&node);
    let operation = &before.pending.as_ref().unwrap().operation;
    assert_eq!(operation.post.as_ref(), Some(&prepared));
    assert!(matches!(
        operation.parent,
        Some(journal::ParentResult::Unknown { .. })
    ));
    wallet.mode.store(0, Ordering::Release);
    let node = node.reopen();
    let mut c = configured(&node, &wallet, |_| {});
    let mut reports = vec![];
    settled(
        &mut c,
        &mut reports,
        node.db.view().unwrap().status().revision,
    );
    assert!(reports.iter().flat_map(|r| &r.effects).any(|e| matches!(
        e,
        pinning::Effect::RecoveredParent(journal::ParentResult::Unknown { .. })
    )));
    assert!(stored_children(&node).is_empty());
    assert_eq!(wallet.posted.lock().unwrap().len(), 1);
    assert_eq!(wallet.requests.load(Ordering::Acquire), 2);
    assert!(!saved(&node).state.enabled);
    drop(shutdown(c));
}
#[tokio::test]
async fn recovery_limits_rollback_and_source_mismatch_fail_before_parent_effects() {
    for mode in ["limit", "rollback", "source"] {
        let node = Node::new(0);
        let wallet = Wallet::new();
        let mut c = configured(&node, &wallet, |_| {});
        prefix(&node, &mut c, &mut vec![], 4).await;
        drop(shutdown(c));
        let c = configured(&node, &wallet, |cfg| {
            cfg.parent.pins.as_mut().unwrap().agent = None
        });
        for block in node.blocks().into_iter().skip(4).take(2) {
            within(
                c.importer()
                    .import_block(block.encode().unwrap(), 1_000_000)
                    .unwrap()
                    .wait(),
            )
            .await
            .unwrap();
        }
        drop(shutdown(c));
        if mode == "rollback" {
            node.db
                .rollback(
                    ae_chain::repository::rollback::Target::Height(1),
                    Default::default(),
                )
                .unwrap();
        } else if mode == "source" {
            let checkpoint = saved(&node);
            let mut writer =
                journal::Writer::start(node.db.clone(), checkpoint.binding, Default::default())
                    .unwrap();
            let revision = checkpoint.consumed.revision + 1;
            let change = node.db.view().unwrap().event(revision).unwrap().unwrap();
            writer
                .begin(journal::Batch {
                    revision,
                    old_tip: change.old_tip,
                    tip: change.new_tip,
                    source: ae_types::Hash256([0; 32]),
                    events: vec![],
                    next: 0,
                    operation: Default::default(),
                })
                .unwrap();
        }
        let before = saved(&node);
        let mut cfg = configuration(&node, &wallet);
        if mode == "limit" {
            cfg.parent
                .pins
                .as_mut()
                .unwrap()
                .agent
                .as_mut()
                .unwrap()
                .maximum_recovery_records = 1;
        }
        let error = match new(&node, cfg) {
            Err(conductor::Error::Pinning(e)) => e,
            _ => panic!("unexpected startup result"),
        };
        assert!(
            match mode {
                "limit" => error == pinning::Error::ResourceLimit,
                "rollback" => matches!(error, pinning::Error::Rollback(_)),
                _ => error == pinning::Error::State("pending event source mismatch"),
            },
            "{mode}: {error:?}"
        );
        assert_eq!(saved(&node), before);
        assert_eq!(wallet.requests.load(Ordering::Acquire), 0);
    }
}

#[path = "checkpoint/recovery.rs"]
mod recovery;
