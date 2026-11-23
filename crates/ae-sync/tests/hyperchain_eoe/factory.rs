use super::*;
use ae_hyperchain::eoe::pool;

fn capacity_config(node: &Node) -> live::Config {
    let mut c = config(node);
    c.runtime.command_capacity = 1;
    c.runtime.maximum_vote_reply_bytes = Unsigned::from(0).as_bytes().len() + 256;
    c
}
fn query() -> runtime::Command<live::StateReader, live::StateReader> {
    runtime::Command::Votes(runtime::VoteQuery {
        epoch: 7.into(),
        kind: None,
        maximum: 0,
    })
}
#[test]
fn factory_preserves_unread_call_and_vote_snapshot_credits_across_replacements() {
    let node = Node::new(0);
    let source = source(&node);
    let hub = pool::Hub::new(node.config.profile.network.clone(), Default::default()).unwrap();
    let pool = hub.start_pool(Default::default()).unwrap();
    let live::Recoverable {
        runtime,
        epochs,
        mut factory,
        ..
    } = source
        .connect_recoverable(
            capacity_config(&node),
            BTreeMap::new(),
            node.config.profile.order.clone(),
            hub.clone(),
        )
        .unwrap();
    let old = runtime.client();
    let runtime::Applied::Votes(snapshot) = apply(&old, query()) else {
        panic!()
    };
    let clone = snapshot.clone();
    let held = call(&old, runtime::Command::NewEpoch(3.into()));
    let end = Instant::now() + Duration::from_secs(5);
    while pool.client().inspect(|p| p.epoch().clone()).unwrap() != 3.into() {
        assert!(Instant::now() < end);
        std::thread::yield_now();
    }
    assert!(matches!(factory.spawn(), Err(live::Error::AlreadyRunning)));
    runtime.retire();
    let next = factory.spawn().unwrap();
    let client = next.client();
    assert!(!old.same_runtime(&client));
    assert_eq!(factory.capacity().outstanding(), 1);
    assert!(matches!(
        client.call(runtime::Command::Retry),
        Err(runtime::SubmitError::Full(_))
    ));
    assert_eq!(
        held.wait(Duration::from_secs(5)).unwrap().unwrap(),
        runtime::Applied::Unit
    );
    assert!(matches!(
        call(&client, query()).wait(Duration::from_secs(5)).unwrap(),
        Err(coordinator::Error::ResourceLimit)
    ));
    drop(snapshot);
    assert!(factory.capacity().retained_vote_bytes() > 0);
    drop(clone);
    let runtime::Applied::Votes(snapshot) = apply(&client, query()) else {
        panic!()
    };
    assert_eq!(snapshot.epoch(), &3.into());
    next.retire();
    let third = factory.spawn().unwrap();
    assert!(matches!(
        call(&third.client(), query())
            .wait(Duration::from_secs(5))
            .unwrap(),
        Err(coordinator::Error::ResourceLimit)
    ));
    drop(snapshot);
    assert_eq!(factory.capacity().retained_vote_bytes(), 0);
    assert!(matches!(
        apply(&third.client(), query()),
        runtime::Applied::Votes(_)
    ));
    drop(epochs);
    assert!(third.client().is_closed());
    third.shutdown().unwrap();
}
#[test]
fn factory_keeps_pool_epoch_cursor_and_signer_but_does_not_replay_existing_votes() {
    let node = Node::new(0);
    let source = source(&node);
    let hub = pool::Hub::new(node.config.profile.network.clone(), Default::default()).unwrap();
    let pool = hub.start_pool(Default::default()).unwrap();
    let keys = [12, 13, 14]
        .into_iter()
        .map(|n| {
            let k = signer(n);
            (k.public_key(), k)
        })
        .collect();
    let live::Recoverable {
        runtime,
        mut epochs,
        mut factory,
        mut signing,
    } = source
        .connect_recoverable(
            config(&node),
            keys,
            node.config.profile.order.clone(),
            hub.clone(),
        )
        .unwrap();
    signing.promote(signer(12).public_key()).unwrap();
    signing.set_candidate(signer(13).public_key()).unwrap();
    let cursor = epochs.cursor().clone();
    let old = runtime.client();
    runtime.retire();
    let signed = vote(&node, eoe::Mode::Fork, eoe::VoteType::Proposal, 13);
    pool.client()
        .admit(pool::Admission {
            signed,
            event: hc_votes::Event::Received,
            protocol: node.config.profile.network.protocol_at(0).unwrap(),
            consensus: ae_protocol::ConsensusType::Pos,
        })
        .unwrap();
    let insert = inserted(&node);
    for block in node.blocks().iter().take(6) {
        insert.insert(block, 1_000_000, Origin::Gossip).unwrap();
    }
    let end = Instant::now() + Duration::from_secs(5);
    while epochs.cursor().revision() < node.db.events().cursor().revision() {
        assert!(Instant::now() < end);
        epochs.step().unwrap();
    }
    assert!(epochs.cursor().revision() > cursor.revision());
    assert_eq!(epochs.acknowledged(), 1);
    let current = epochs.cursor().clone();
    let next = factory.spawn().unwrap();
    assert!(!next.client().same_runtime(&old));
    assert_eq!(epochs.cursor(), &current);
    assert_eq!(epochs.acknowledged(), 1);
    assert_eq!(
        pool.client()
            .inspect(|p| (p.epoch().clone(), p.len()))
            .unwrap(),
        (2.into(), 1)
    );
    assert_eq!(signing.active(), Some(signer(12).public_key()));
    assert_eq!(signing.candidate(), Some(signer(13).public_key()));
    assert!(matches!(
        next.recv_timeout(Duration::from_millis(20)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    let exit = next.retire();
    assert_eq!(exit.state.unwrap().generations, [0, 0]);
    assert_eq!(source.snapshots().live_snapshots(), 0);
}
#[test]
fn retired_epoch_factory_cannot_restart_or_take_over_a_new_feed() {
    let node = Node::new(0);
    let source = source(&node);
    let hub = pool::Hub::new(node.config.profile.network.clone(), Default::default()).unwrap();
    let live::Recoverable {
        runtime,
        epochs,
        mut factory,
        ..
    } = source
        .connect_recoverable(
            config(&node),
            BTreeMap::new(),
            node.config.profile.order.clone(),
            hub.clone(),
        )
        .unwrap();
    drop(epochs);
    assert!(hub.is_closed());
    runtime.retire();
    assert!(matches!(factory.spawn(), Err(live::Error::Closed)));
    let new_hub = pool::Hub::new(node.config.profile.network.clone(), Default::default()).unwrap();
    let live::Recoverable {
        runtime, epochs, ..
    } = source
        .connect_recoverable(
            config(&node),
            BTreeMap::new(),
            node.config.profile.order.clone(),
            new_hub,
        )
        .unwrap();
    assert!(matches!(factory.spawn(), Err(live::Error::Closed)));
    assert!(!runtime.client().is_closed());
    drop(epochs);
    runtime.shutdown().unwrap();
}
