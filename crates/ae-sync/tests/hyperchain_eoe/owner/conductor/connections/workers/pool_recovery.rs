use super::super::super::pool_supervision::{replaced, transactions};
use super::*;

#[tokio::test]
async fn optional_admission_and_maintenance_follow_pool_generations_without_owning_the_publisher() {
    let node = Node::new(1);
    let mut c = policy_producer(&node);
    let producer = c.pool_binding();
    let tx = transactions(1).remove(0);
    c.pool()
        .push(tx.clone(), ae_mempool::pool::Event::Created, false)
        .unwrap()
        .await
        .unwrap();
    let (config, _resources) = all_services().await;
    let (mut h, service) = optional::create(&mut c, config.clone()).unwrap();
    let task = tokio::spawn(service.run());
    worker_ready(&mut h).await;
    let bindings = h.bindings();
    let old = bindings.admission.current().unwrap().client;
    let maintenance = bindings.pool_gc.as_ref().unwrap().current().unwrap();
    let pending = maintenance.ttl(tx.hash()).unwrap();
    let submitted = maintenance.info().submitted;
    c.request_pool_restart(1).unwrap();
    within(async {
        while h.status().ready || bindings.admission.current().is_some() {
            h.changed().await.unwrap();
        }
    })
    .await;
    assert!(!bindings.admission.is_closed() && h.status().error.is_none());
    assert!(matches!(
        maintenance.ttl(tx.hash()),
        Err(ae_sync::pool_gc::Error::AdmissionUnavailable)
    ));
    assert_eq!(maintenance.info().submitted, submitted);
    replaced(&mut c, 2);
    assert!(within(pending).await.unwrap() >= 0);
    within(async {
        while !h.status().ready || h.status().admission_generation != 2 {
            h.changed().await.unwrap();
        }
    })
    .await;
    assert!(old.info().joined);
    assert!(within(maintenance.ttl(tx.hash()).unwrap()).await.unwrap() >= 0);
    assert_eq!(maintenance.info().last_admission_generation, Some(2));
    assert_eq!(h.status().restart_attempts, 1);
    h.shutdown();
    assert!(within(task).await.unwrap().joined);
    assert!(bindings.admission.is_closed() && !producer.is_closed());
    assert!(producer.current().unwrap().client.info().accepting);
    let (mut h, service) = optional::create(&mut c, config).unwrap();
    let task = tokio::spawn(service.run());
    worker_ready(&mut h).await;
    assert_eq!(
        (h.status().admission_generation, h.status().restart_attempts),
        (2, 1)
    );
    c.request_pool_restart(2).unwrap();
    replaced(&mut c, 3);
    within(async {
        while h.status().admission_generation != 3 {
            h.changed().await.unwrap();
        }
    })
    .await;
    h.shutdown();
    assert!(within(task).await.unwrap().joined);
    assert!(shutdown(c).pool.unwrap().joined);
}

#[tokio::test]
async fn ordinary_votes_and_optional_replacements_share_one_terminal_worker_group() {
    for last_pool in [false, true] {
        let node = Node::new(1);
        let mut c = policy_producer(&node);
        let eoe = c.eoe();
        let (config, _resources) = all_services().await;
        let (mut h, service) = optional::create(&mut c, config).unwrap();
        let task = tokio::spawn(service.run());
        worker_ready(&mut h).await;
        for attempt in 1..=6 {
            let kind = if last_pool {
                attempt % 3
            } else {
                (attempt - 1) % 3
            };
            match kind {
                0 => {
                    let generation = c.status().pool.generation;
                    c.request_pool_restart(generation).unwrap();
                    if attempt < 6 {
                        replaced(&mut c, generation + 1);
                    } else {
                        let now = Instant::now();
                        let end = now + Duration::from_secs(5);
                        loop {
                            match c.step(1000, now) {
                                Err(conductor::Error::PoolRestartLimit) => break,
                                other => {
                                    other.unwrap();
                                    c.take_output();
                                }
                            }
                            assert!(Instant::now() < end);
                            tokio::task::yield_now().await;
                        }
                    }
                }
                1 => {
                    let generation = c.status().votes.generation;
                    c.request_vote_restart(generation).unwrap();
                    super::super::super::vote_supervision::replaced(&mut c, generation + 1);
                }
                _ => {
                    let generation = h.status().capabilities_generation;
                    let result = h
                        .restart(Child::Capabilities, generation)
                        .unwrap()
                        .wait()
                        .await;
                    assert_eq!(
                        result,
                        if attempt < 6 {
                            Ok(generation + 1)
                        } else {
                            Err(supervisor::Error::RestartLimit)
                        }
                    );
                }
            }
            assert_eq!(c.status().pool.restart_attempts, attempt);
        }
        assert!(matches!(
            c.step(1000, Instant::now()),
            Err(conductor::Error::PoolRestartLimit
                | conductor::Error::Owner(managed::Error::VoteRestartLimit))
        ));
        let status = within(task).await.unwrap();
        assert!(status.joined && status.error == Some(supervisor::Error::RestartLimit));
        assert_eq!(status.restart_attempts, 6);
        assert!(c.pool_binding().is_closed() && c.vote_binding().is_closed());
        assert!(!c.pool().info().accepting && !c.votes().info().accepting);
        assert!(eoe.same_runtime(&c.eoe()) && !eoe.is_closed());
        assert_eq!(c.status().consensus.attempts, 0);
        drop(shutdown(c));
    }
}
