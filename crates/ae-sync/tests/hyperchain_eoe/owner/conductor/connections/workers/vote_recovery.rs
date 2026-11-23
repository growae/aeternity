use super::super::super::vote_supervision::replaced;
use super::*;

#[tokio::test]
async fn optional_bindings_follow_votes_and_shutdown_does_not_close_the_producer_publisher() {
    let node = Node::new(1);
    let mut c = policy_producer(&node);
    let producer_binding = c.vote_binding();
    let (config, _resources) = all_services().await;
    let (mut h, s) = optional::create(&mut c, config.clone()).unwrap();
    let task = tokio::spawn(s.run());
    worker_ready(&mut h).await;
    let binding = h.bindings().votes.unwrap();
    let old = binding.current().unwrap();
    c.request_vote_restart(1).unwrap();
    within(async {
        while h.status().ready || binding.current().is_some() {
            h.changed().await.unwrap();
        }
    })
    .await;
    assert!(h.status().running && h.status().error.is_none() && !binding.is_closed());
    replaced(&mut c, 2);
    within(async {
        while h.status().votes_generation != 2 || !h.status().ready {
            h.changed().await.unwrap();
        }
    })
    .await;
    assert!(old.info().joined && binding.current().unwrap().info().accepting);
    assert_eq!(h.status().restart_attempts, 1);
    h.shutdown();
    assert!(within(task).await.unwrap().joined);
    assert!(binding.is_closed());
    assert!(!producer_binding.is_closed());
    assert!(producer_binding.current().unwrap().info().accepting);
    // Recreating optional services retains the same per-node restart history.
    let (mut next, service) = optional::create(&mut c, config).unwrap();
    let task = tokio::spawn(service.run());
    worker_ready(&mut next).await;
    assert_eq!(next.status().votes_generation, 2);
    assert_eq!(next.status().restart_attempts, 1);
    c.request_vote_restart(2).unwrap();
    replaced(&mut c, 3);
    within(async {
        while next.status().votes_generation != 3 {
            next.changed().await.unwrap();
        }
    })
    .await;
    next.shutdown();
    assert!(within(task).await.unwrap().joined);
    assert!(c.votes().info().accepting);
    assert!(shutdown(c).owner.errors.is_empty());
}

#[tokio::test]
async fn mixed_vote_and_optional_replacements_exhaust_one_shared_native_worker_budget() {
    for last_is_vote in [false, true] {
        let node = Node::new(1);
        let mut c = policy_producer(&node);
        let eoe = c.eoe();
        let (config, _resources) = all_services().await;
        let (mut h, s) = optional::create(&mut c, config).unwrap();
        let task = tokio::spawn(s.run());
        worker_ready(&mut h).await;
        for attempt in 1..=6 {
            let vote = (attempt % 2 == 0) == last_is_vote;
            if vote {
                let old = c.status().votes.generation;
                c.request_vote_restart(old).unwrap();
                if attempt < 6 {
                    replaced(&mut c, old + 1);
                } else {
                    let now = Instant::now();
                    let end = now + Duration::from_secs(5);
                    loop {
                        if let Err(e) = c.step(1000, now) {
                            assert_eq!(
                                e,
                                conductor::Error::Owner(managed::Error::VoteRestartLimit)
                            );
                            break;
                        }
                        c.take_output();
                        assert!(Instant::now() < end);
                        tokio::task::yield_now().await;
                    }
                }
            } else {
                let g = h.status().capabilities_generation;
                let result = h.restart(Child::Capabilities, g).unwrap().wait().await;
                assert_eq!(
                    result,
                    if attempt < 6 {
                        Ok(g + 1)
                    } else {
                        Err(supervisor::Error::RestartLimit)
                    }
                );
            }
            assert_eq!(c.status().votes.restart_attempts, attempt);
        }
        assert_eq!(
            c.step(1000, Instant::now()),
            Err(conductor::Error::Owner(managed::Error::VoteRestartLimit))
        );
        let status = within(task).await.unwrap();
        assert_eq!(status.error, Some(supervisor::Error::RestartLimit));
        assert_eq!(status.restart_attempts, 6);
        assert!(status.joined && c.vote_binding().is_closed());
        assert!(!c.pool().info().accepting && !c.votes().info().accepting);
        assert!(eoe.same_runtime(&c.eoe()) && !eoe.is_closed());
        assert_eq!(c.status().consensus.attempts, 0);
        drop(shutdown(c));
    }
}
