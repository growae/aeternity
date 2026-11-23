use super::*;
use managed::vote_pool::{ControlError, Notice};

fn joined(owner: &mut managed::Owner, old: &votes::Client) {
    let end = Instant::now() + Duration::from_secs(5);
    while !old.info().joined {
        step(owner);
        assert!(Instant::now() < end);
        std::thread::yield_now();
    }
}

#[tokio::test]
async fn pool_gap_preserves_committed_epochs_eoe_publications_cache_and_reply_credits() {
    for index in [1, 3, 5] {
        let node = Node::new(index);
        let source = source(&node);
        let mut config = cfg(&node);
        config.calls.requests = 1;
        config.votes.reply_bytes = Unsigned::from(0).as_bytes().len() + 128;
        let mut owner = start(&node, &source, config, &[]);
        let old = owner.votes();
        let binding = owner.vote_binding();
        let eoe = owner.runtime_client();
        let hub = eoe.independent_pool().unwrap().clone();
        let original = hub.client().unwrap();
        let tx = vote(&node, eoe::Mode::Fork, eoe::VoteType::Vote, 13);
        common::within(old.push(tx.encode(), hc_votes::Event::Received).unwrap())
            .await
            .unwrap();
        let unread = old.stats().unwrap();
        common::within(async {
            while old.info().completed < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let mut publications = old.publications().listen().unwrap();
        assert_eq!(owner.request_vote_restart(0), Err(ControlError::Stale));
        owner.request_vote_restart(1).unwrap();
        assert!(binding.current().is_none());
        joined(&mut owner, &old);
        assert!(original.is_closed() && hub.client().is_none());
        assert!(owner.vote_status().restarting);
        assert_eq!(owner.vote_status().restart_attempts, 0);
        let insert = inserted(&node);
        for block in node.blocks().iter().take(6) {
            insert.insert(block, 1_000_000, Origin::Gossip).unwrap();
        }
        until(&mut owner, |o| {
            o.epoch_cursor().revision() == node.db.events().cursor().revision()
        });
        assert_eq!(owner.acknowledged_epochs(), 1);
        assert!(hub.client().is_none());
        assert!(eoe.same_runtime(&owner.runtime_client()) && !eoe.is_closed());
        assert_eq!(
            *common::within(publications.recv())
                .await
                .unwrap()
                .transaction,
            tx
        );
        let Some(Notice::Retired {
            generation: 1,
            worker,
        }) = owner.take_vote_notice()
        else {
            panic!("joined retirement");
        };
        assert!(worker.info.joined && worker.result.is_ok());
        step(&mut owner);
        assert!(matches!(
            owner.take_vote_notice(),
            Some(Notice::Restarted { generation: 2 })
        ));
        assert_eq!(owner.vote_status().restart_attempts, 1);
        let next = binding.current().unwrap();
        assert_eq!(
            hub.client()
                .unwrap()
                .inspect(|p| (p.epoch().clone(), p.len()))
                .unwrap(),
            (0.into(), 0)
        );
        assert!(matches!(
            common::within(next.stats().unwrap()).await,
            Err(votes::Error::Worker(votes::Fault::Busy))
        ));
        let held = common::within(unread).await.unwrap();
        assert_eq!(held.entries, 1);
        assert!(matches!(
            common::within(next.stats().unwrap()).await,
            Err(votes::Error::Worker(votes::Fault::Busy))
        ));
        drop(held);
        assert_eq!(
            common::within(next.push(tx.encode(), hc_votes::Event::Created).unwrap())
                .await
                .unwrap(),
            hc_votes::PushOutcome::Cached
        );
        assert_eq!(
            common::within(next.stats().unwrap()).await.unwrap().entries,
            0
        );
        assert!(matches!(
            old.stats(),
            Err(votes::Error::Worker(votes::Fault::Stopped))
        ));
        let exit = owner.shutdown();
        assert!(exit.errors.is_empty() && exit.vote_notices.is_empty());
        assert!(binding.is_closed() && next.info().joined);
    }
}

#[test]
fn eoe_and_vote_retirement_can_overlap_without_replacing_the_shared_epoch_source() {
    let node = Node::new(0);
    let source = source(&node);
    let mut config = cfg(&node);
    config.calls.requests = 1;
    let mut owner = start(&node, &source, config, &[]);
    let cursor = owner.epoch_cursor().clone();
    let eoe = owner.runtime_client();
    let votes = owner.votes();
    owner.request_vote_restart(1).unwrap();
    eoe.request_stop();
    let end = Instant::now() + Duration::from_secs(5);
    while !owner.runtime_finished() {
        assert!(Instant::now() < end);
        std::thread::yield_now();
    }
    owner.begin_recovery().unwrap();
    joined(&mut owner, &votes);
    assert!(owner.take_retirement().unwrap().runtime.result.is_ok());
    assert!(matches!(
        owner.take_vote_notice(),
        Some(Notice::Retired { .. })
    ));
    owner.restart_runtime().unwrap();
    step(&mut owner);
    assert!(matches!(
        owner.take_vote_notice(),
        Some(Notice::Restarted { generation: 2 })
    ));
    assert!(!owner.runtime_client().same_runtime(&eoe));
    assert!(owner.votes().info().accepting);
    assert_eq!(owner.epoch_cursor(), &cursor);
    assert!(owner.shutdown().errors.is_empty());
}

#[test]
fn shutdown_transfers_unconsumed_vote_retirement_and_terminal_hub_loss_closes_bindings() {
    let node = Node::new(0);
    let source = source(&node);
    let mut config = cfg(&node);
    config.calls.requests = 1;
    let mut owner = start(&node, &source, config, &[]);
    let old = owner.votes();
    owner.request_vote_restart(1).unwrap();
    joined(&mut owner, &old);
    let exit = owner.shutdown();
    assert!(exit.errors.is_empty());
    assert!(matches!(
        exit.vote_notices.as_slice(),
        [Notice::Retired { generation: 1, .. }]
    ));
    let mut owner = start(&node, &source, cfg(&node), &[]);
    let binding = owner.vote_binding();
    owner.runtime_client().independent_pool().unwrap().close();
    assert_eq!(
        owner.step(),
        Err(managed::Error::Votes(votes::Error::AuthorityClosed))
    );
    assert!(binding.is_closed());
    assert_eq!(owner.request_vote_restart(1), Err(ControlError::Closed));
    assert!(!owner.shutdown().errors.is_empty());
}
