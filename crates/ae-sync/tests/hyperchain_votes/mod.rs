use super::*;

fn supervised(
    node: &Node,
    config: votes::Config,
    limits: votes::eoe::Limits,
) -> (OutputOwner, votes::Worker, votes::Publications) {
    let publications = votes::Publications::new(&node.db, config).unwrap();
    let owner = Owner::new(
        runtime(
            3,
            runtime::Config {
                output_capacity: 1,
                ..Default::default()
            },
        ),
        publications.clone(),
    )
    .unwrap_or_else(|_| panic!("owner refused"));
    let worker =
        votes::Worker::attach_supervised_eoe(node.db.clone(), config, &owner, limits).unwrap();
    (owner, worker, publications)
}

#[tokio::test]
async fn supervised_replacement_preserves_vote_client_claim_outputs_and_unknown_receipt() {
    let node = Node::new();
    let config = votes::Config {
        events: 1,
        ..Default::default()
    };
    let (mut owner, worker, publications) = supervised(&node, config, Default::default());
    let client = worker.client();
    let old = owner.client();
    negotiate(&old);
    let other = vote(eoe::Mode::Fork, eoe::VoteType::Vote, 4).encode();
    let accepted = client.push(other.clone(), policy::Event::Received).unwrap();
    within(async {
        while old.outstanding() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert_eq!(route(&mut owner).await, Routed::Published);
    assert_eq!(route(&mut owner).await, Routed::Backpressured);
    let head = owner.pending().unwrap().clone();
    owner.begin_retirement();
    assert_eq!(
        within(accepted).await,
        Err(votes::Error::RetiredReceipt(
            votes::eoe::ReceiptError::Closed
        ))
    );
    assert!(client.info().accepting);
    assert_eq!(owner.pending(), Some(&head));
    assert_eq!(owner.retained_outputs(), 9);
    assert!(owner.take_retirement().is_none());
    assert_eq!(
        within(client.stats().unwrap()).await.unwrap_err(),
        votes::Error::Worker(votes::Fault::Busy)
    );
    let blocked = owner
        .replace_runtime(runtime(0, Default::default()))
        .expect_err("old outputs must drain");
    assert_eq!(blocked.error, votes::Error::Worker(votes::Fault::Busy));
    blocked.runtime.shutdown().unwrap();
    let duplicate = Owner::new(runtime(0, Default::default()), publications.clone())
        .err()
        .expect("publication claim must remain exclusive");
    assert_eq!(duplicate.error, votes::Error::Worker(votes::Fault::Busy));
    duplicate.runtime.shutdown().unwrap();
    let mut events = publications.listen().unwrap();
    let mut created = vec![within(events.recv()).await.unwrap().transaction.encode()];
    for _ in 0..9 {
        created.push(
            publication(&mut owner, &mut events)
                .await
                .transaction
                .encode(),
        );
    }
    assert_created(created, "local");
    let retired = owner.take_retirement().expect("drained generation");
    assert_eq!(retired.queued, vec![runtime::Submission::Caller]);
    assert_eq!(retired.result, Ok(()));
    assert_eq!(owner.route().unwrap(), Routed::Idle);
    owner
        .replace_runtime(runtime(0, Default::default()))
        .unwrap_or_else(|_| panic!("replacement refused"));
    assert!(!old.same_runtime(&owner.client()));
    assert!(matches!(
        old.call(runtime::Command::Retry),
        Err(runtime::SubmitError::Disconnected(_))
    ));
    assert_eq!(within(client.stats().unwrap()).await.unwrap().entries, 0);
    assert!(matches!(
        within(client.push(other.clone(), policy::Event::Received).unwrap())
            .await
            .unwrap(),
        policy::PushOutcome::Accepted { inserted: true, .. }
    ));
    assert_eq!(
        publication(&mut owner, &mut events)
            .await
            .transaction
            .encode(),
        other
    );
    assert_eq!(within(client.stats().unwrap()).await.unwrap().entries, 1);
    worker.shutdown().unwrap();
    owner.shutdown().unwrap();
}

#[tokio::test]
async fn supervised_live_timeout_still_poisoned_and_operational_stop_is_not_replacement() {
    let node = Node::new();
    let (mut owner, worker, _) = supervised(
        &node,
        Default::default(),
        votes::eoe::Limits {
            receipt_timeout: Duration::from_millis(30),
        },
    );
    negotiate(&owner.client());
    let client = worker.client();
    assert_eq!(
        within(client.stats().unwrap()).await.unwrap_err(),
        votes::Error::UnknownReceipt(votes::eoe::ReceiptError::TimedOut)
    );
    assert!(!client.info().accepting);
    assert!(!owner.replacement_pending());
    assert_eq!(
        worker.shutdown(),
        Err(votes::Error::UnknownReceipt(
            votes::eoe::ReceiptError::TimedOut
        ))
    );
    owner.begin_retirement();
    owner.shutdown().unwrap();

    let (mut owner, worker, _) = supervised(&node, Default::default(), Default::default());
    let fixed = owner.client();
    call(
        &fixed,
        runtime::Command::Configure(coordinator::Config {
            maximum_outputs: 0,
            ..Default::default()
        }),
    )
    .wait(Duration::from_secs(2))
    .unwrap()
    .unwrap();
    negotiate(&fixed);
    assert_eq!(
        route(&mut owner).await,
        Routed::Output(runtime::Output::WorkStopped(
            coordinator::Error::ResourceLimit
        ))
    );
    fixed.request_stop();
    assert!(!owner.replacement_pending());
    let client = worker.client();
    assert_eq!(
        within(client.stats().unwrap()).await.unwrap_err(),
        votes::Error::AuthorityClosed
    );
    assert!(!client.info().accepting);
    assert_eq!(worker.shutdown(), Err(votes::Error::AuthorityClosed));
    owner.begin_retirement();
    assert_eq!(
        owner.retirement().unwrap().info.health,
        runtime::Health::WorkStopped(coordinator::Error::ResourceLimit)
    );
    owner.shutdown().unwrap();
}

#[tokio::test]
async fn attached_fixed_bridge_must_be_released_before_replacing_its_output_runtime() {
    let node = Node::new();
    let (mut owner, worker, _) = owner(&node, 0, Default::default(), Default::default());
    owner.begin_retirement();
    owner.take_retirement().unwrap();
    let blocked = owner
        .replace_runtime(runtime(0, Default::default()))
        .expect_err("fixed bridge remains attached");
    assert_eq!(blocked.error, votes::Error::Worker(votes::Fault::Busy));
    worker.shutdown().unwrap();
    owner
        .replace_runtime(blocked.runtime)
        .unwrap_or_else(|_| panic!("replacement refused"));
    owner.shutdown().unwrap();
}
