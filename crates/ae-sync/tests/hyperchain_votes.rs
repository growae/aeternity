mod common;
use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::{
    eoe::{self, coordinator, local_host, runtime},
    parent_block::ParentBlock,
};
use ae_mempool::{hc_votes as policy, pool::Signed};
use ae_protocol::{ConsensusType, NetworkProfile, Protocol, contracts::Version};
use ae_state::{
    Trees,
    accounts::Account,
    contracts::{Code, Contract, Nonce},
};
use ae_sync::votes::{
    self,
    eoe::{Owner, Routed},
};
use ae_trie::MemoryStore;
use ae_types::Unsigned;
use common::{Node, within};
use serde_json::{Value as J, json};
use std::{
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};
static DATA: LazyLock<J> = LazyLock::new(|| {
    fn unpack(v: &J, table: &[J]) -> J {
        match v {
            J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                unpack(&table[v["$ref"].as_u64().unwrap() as usize], table)
            }
            J::Object(m) => J::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), unpack(v, table)))
                    .collect(),
            ),
            J::Array(a) => J::Array(a.iter().map(|v| unpack(v, table)).collect()),
            _ => v.clone(),
        }
    }
    let data: J = serde_json::from_str(include_str!(
        "../../ae-hyperchain/tests/fixtures/erlang_hc_coordinator.json"
    ))
    .unwrap();
    unpack(&data["root"], data["values"].as_array().unwrap())
});
type OutputOwner = Owner<MemoryStore, MemoryStore>;
fn key(n: u64) -> [u8; 32] {
    let mut k = [0; 32];
    k[24..].copy_from_slice(&n.to_be_bytes());
    k
}
fn signer(n: u64) -> Ed25519SecretKey {
    Ed25519SecretKey::from_seed(&key(n))
}
fn trees(nonce: u64) -> Trees {
    let mut trees = Trees {
        accounts: Default::default(),
        contracts: Default::default(),
        calls: Default::default(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    };
    let mut account = Account::new(
        signer(1).public_key(),
        Unsigned::from_decimal("100000000000000000000").unwrap(),
    );
    account.set_nonce(nonce.into());
    trees.accounts.enter(&account).unwrap();
    let mut contract = Contract::new(
        signer(1).public_key(),
        Nonce::Number(&1.into()),
        Version { vm: 8, abi: 3 },
        Code::Inline(vec![0]),
        0.into(),
    )
    .unwrap();
    contract.public_key = key(101);
    trees
        .contracts
        .insert(&contract, &Default::default())
        .unwrap();
    trees
}
fn runtime(owned: u64, config: runtime::Config) -> runtime::Runtime {
    let mut host = local_host::LocalHost::new(
        local_host::Config {
            network: NetworkProfile::mainnet(),
            election_contract: key(101),
            parent_epoch_length: 5.into(),
            parent_start_height: 11.into(),
            limits: Default::default(),
        },
        local_host::SelectedTip {
            trees: Some(Arc::new(trees(20))),
            protocol: Protocol::Ceres,
            consensus: ConsensusType::Pos,
        },
        (1..=owned)
            .map(|n| {
                let k = signer(n);
                (k.public_key(), k)
            })
            .collect(),
    );
    host.insert_parent(ParentBlock {
        hash: key(99).to_vec(),
        height: 41.into(),
        previous_hash: key(98).to_vec(),
        time: 12345.into(),
    })
    .unwrap();
    runtime::Runtime::spawn(
        coordinator::Coordinator::new(host, 1000.into(), Default::default()),
        common::hyperchain::order(&DATA["profile"]),
        config,
    )
    .unwrap()
}
fn owner(
    node: &Node,
    owned: u64,
    config: votes::Config,
    limits: runtime::Config,
) -> (OutputOwner, votes::Worker, votes::Publications) {
    let publications = votes::Publications::new(&node.db, config).unwrap();
    let owner = match Owner::new(runtime(owned, limits), publications.clone()) {
        Ok(o) => o,
        Err(_) => panic!("owner refused"),
    };
    let worker =
        votes::Worker::attach_eoe(node.db.clone(), config, &owner, Default::default()).unwrap();
    (owner, worker, publications)
}
fn call(client: &runtime::Client, command: runtime::Command) -> runtime::Call {
    match client.call(command) {
        Ok(c) => c,
        Err(_) => panic!("command refused"),
    }
}
fn negotiate(client: &runtime::Client) {
    call(
        client,
        runtime::Command::Negotiate(eoe::Negotiation {
            epoch: 7.into(),
            height: 50.into(),
            hash: key(9).to_vec(),
            leader: signer(1).public_key(),
            validators: vec![
                (signer(1).public_key(), 4.into()),
                (signer(2).public_key(), 3.into()),
                (signer(3).public_key(), 3.into()),
            ],
            seed: Some(key(8).to_vec()),
            current_length: 10.into(),
        }),
    )
    .wait(Duration::from_secs(2))
    .unwrap()
    .unwrap();
}
fn vote(mode: eoe::Mode, kind: eoe::VoteType, n: u64) -> Signed {
    let fields: eoe::Payload = [
        (
            eoe::payload::HEIGHT.to_vec(),
            eoe::Field::Integer(50.into()),
        ),
        match mode {
            eoe::Mode::Fork => (
                eoe::payload::FORK_HASH.to_vec(),
                eoe::Field::Binary(key(9).to_vec()),
            ),
            eoe::Mode::Length => (
                eoe::payload::LENGTH_DELTA.to_vec(),
                eoe::Field::Integer(0.into()),
            ),
        },
    ]
    .into();
    let payload = eoe::payload::create_payload(&fields, &signer(n), Default::default()).unwrap();
    eoe::payload::create_transaction(
        signer(n).public_key(),
        7.into(),
        mode.type_number(kind),
        &payload,
        &signer(n),
        &NetworkProfile::mainnet(),
        Default::default(),
    )
    .unwrap()
}
async fn route(owner: &mut OutputOwner) -> Routed {
    within(async {
        loop {
            match owner.route().unwrap() {
                Routed::Idle => tokio::time::sleep(Duration::from_millis(1)).await,
                r => break r,
            }
        }
    })
    .await
}
async fn publication(owner: &mut OutputOwner, events: &mut votes::Consumer) -> votes::Publication {
    assert_eq!(route(owner).await, Routed::Published);
    within(events.recv()).await.unwrap()
}
fn expected(name: &str) -> &'static J {
    DATA["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == name)
        .unwrap()
}
fn assert_created(mut events: Vec<Vec<u8>>, name: &str) {
    events.sort();
    assert_eq!(
        json!([
            "list",
            events
                .into_iter()
                .map(|v| json!(["binary", hex::encode(v)]))
                .collect::<Vec<_>>()
        ]),
        expected(name)["events"]
    );
}
#[tokio::test]
async fn generated_and_received_votes_share_native_pool_and_epoch_order() {
    let node = Node::new();
    let (mut owner, worker, publications) = owner(&node, 3, Default::default(), Default::default());
    let client = worker.client();
    let mut events = publications.listen().unwrap();
    negotiate(&owner.client());
    let mut created = vec![];
    for _ in 0..10 {
        let p = publication(&mut owner, &mut events).await;
        assert_eq!(p.event, policy::Event::Created);
        created.push(p.transaction.encode());
    }
    assert_created(created.clone(), "local");
    assert_eq!(within(client.stats().unwrap()).await.unwrap().entries, 10);
    let peek = within(
        client
            .peek(
                7.into(),
                Some(eoe::Mode::Fork.type_number(eoe::VoteType::Vote)),
                10,
            )
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(peek.len(), 2);
    assert!(matches!(
        within(
            client
                .push(created[0].clone(), policy::Event::Received)
                .unwrap()
        )
        .await
        .unwrap(),
        policy::PushOutcome::Accepted {
            inserted: false,
            event: policy::Event::Received,
            ..
        }
    ));
    let repeated = publication(&mut owner, &mut events).await;
    assert_eq!(repeated.transaction.encode(), created[0]);
    assert_eq!(repeated.event, policy::Event::Received);
    for epoch in [7, 7, 2, 0, 9] {
        within(client.advance_epoch(epoch.into()).unwrap())
            .await
            .unwrap();
        let stats = within(client.stats().unwrap()).await.unwrap();
        assert_eq!(stats.epoch, epoch.into());
        assert_eq!(stats.entries, 10);
    }
    within(client.advance_epoch(10.into()).unwrap())
        .await
        .unwrap();
    assert_eq!(within(client.stats().unwrap()).await.unwrap().entries, 0);
    assert!(
        within(client.peek(7.into(), None, 10).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(peek.len(), 2); // Prior replies are immutable snapshots.
    call(
        &owner.client(),
        runtime::Command::SelectTip(local_host::SelectedTip {
            trees: None,
            protocol: Protocol::Ceres,
            consensus: ConsensusType::Pow,
        }),
    )
    .wait(Duration::from_secs(2))
    .unwrap()
    .unwrap();
    assert_eq!(
        within(
            client
                .push(created[0].clone(), policy::Event::Received)
                .unwrap()
        )
        .await,
        Err(votes::Error::Pool(policy::Error::InvalidAtProtocol))
    );
    assert_eq!(within(client.stats().unwrap()).await.unwrap().entries, 0);
    worker.shutdown().unwrap();
    owner.shutdown().unwrap();
}
#[tokio::test]
async fn received_quorum_drives_the_same_callbacks_and_native_finalization() {
    let node = Node::new();
    let (mut owner, worker, publications) = owner(&node, 1, Default::default(), Default::default());
    let client = worker.client();
    let mut events = publications.listen().unwrap();
    negotiate(&owner.client());
    let mut created = vec![];
    for _ in 0..2 {
        created.push(
            publication(&mut owner, &mut events)
                .await
                .transaction
                .encode(),
        );
    }
    call(
        &owner.client(),
        runtime::Command::Finalize {
            request: 77,
            view: Arc::new(trees(5)),
        },
    )
    .wait(Duration::from_secs(2))
    .unwrap()
    .unwrap();
    let mut received = vec![];
    for mode in [eoe::Mode::Fork, eoe::Mode::Length] {
        for kind in [eoe::VoteType::Vote, eoe::VoteType::Commit] {
            let signed = vote(mode, kind, 2);
            received.push(signed.encode());
            assert!(matches!(
                within(
                    client
                        .push(signed.encode(), policy::Event::Received)
                        .unwrap()
                )
                .await
                .unwrap(),
                policy::PushOutcome::Accepted { inserted: true, .. }
            ));
        }
    }
    let mut routed_received = vec![];
    let mut finalization = None;
    while finalization.is_none() || created.len() < 2 || routed_received.len() < 4 {
        match route(&mut owner).await {
            Routed::Published => {
                let p = within(events.recv()).await.unwrap();
                match p.event {
                    policy::Event::Created => created.push(p.transaction.encode()),
                    policy::Event::Received => routed_received.push(p.transaction.encode()),
                }
            }
            Routed::Output(runtime::Output::Service(coordinator::ServiceOutput::Finalized {
                request: 77,
                replies,
            })) => finalization = Some(replies),
            Routed::Output(runtime::Output::Service(coordinator::ServiceOutput::Publication(
                local_host::Output::ParentBlock { .. },
            ))) => (),
            other => panic!("unexpected output {other:?}"),
        }
    }
    let mut all = created;
    all.extend(routed_received.clone());
    assert_created(all, "pending");
    received.sort();
    routed_received.sort();
    assert_eq!(received, routed_received);
    let result: Vec<_> = finalization
        .unwrap()
        .iter()
        .map(|reply| match reply {
            eoe::Reply::Finalized(local_host::Finalized::Signed(tx)) => json!([
                "tuple",
                [["atom", "ok"], ["binary", hex::encode(tx.encode())]]
            ]),
            other => panic!("unexpected finalization {other:?}"),
        })
        .collect();
    assert_eq!(json!(["list", result]), expected("pending")["result"]);
    assert_eq!(within(client.stats().unwrap()).await.unwrap().entries, 6);
    worker.shutdown().unwrap();
    owner.shutdown().unwrap();
}
#[tokio::test]
async fn publication_pressure_and_bridge_replacement_retain_the_single_output_owner() {
    let node = Node::new();
    let config = votes::Config {
        events: 1,
        ..Default::default()
    };
    let (mut owner, worker, publications) = owner(&node, 3, config, Default::default());
    assert!(matches!(
        votes::Worker::attach_eoe(node.db.clone(), config, &owner, Default::default()),
        Err(votes::Error::Worker(votes::Fault::Busy))
    ));
    assert!(matches!(
        votes::Worker::spawn(
            node.db.clone(),
            NetworkProfile::mainnet(),
            Default::default(),
            config,
            publications.clone()
        ),
        Err(votes::Error::Worker(votes::Fault::Busy))
    ));
    negotiate(&owner.client());
    assert_eq!(route(&mut owner).await, Routed::Published);
    assert_eq!(route(&mut owner).await, Routed::Backpressured);
    let pending = owner.pending().unwrap().clone();
    worker.shutdown().unwrap();
    assert_eq!(owner.pending(), Some(&pending));
    let worker =
        votes::Worker::attach_eoe(node.db.clone(), config, &owner, Default::default()).unwrap();
    assert_eq!(
        within(worker.client().stats().unwrap())
            .await
            .unwrap()
            .entries,
        10
    );
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
    worker.shutdown().unwrap();
    owner.shutdown().unwrap();
}
#[tokio::test]
async fn shutdown_quarantines_unknown_receipt_without_retry_or_redirect() {
    let node = Node::new();
    let (mut owner, worker, publications) = owner(
        &node,
        3,
        Default::default(),
        runtime::Config {
            output_capacity: 1,
            ..Default::default()
        },
    );
    negotiate(&owner.client());
    let client = worker.client();
    let other = vote(eoe::Mode::Fork, eoe::VoteType::Vote, 4).encode();
    let ticket = client.push(other.clone(), policy::Event::Received).unwrap();
    within(async {
        while owner.client().outstanding() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    let start = Instant::now();
    assert_eq!(
        worker.shutdown(),
        Err(votes::Error::UnknownReceipt(
            votes::eoe::ReceiptError::Interrupted
        ))
    );
    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(
        within(ticket).await,
        Err(votes::Error::UnknownReceipt(
            votes::eoe::ReceiptError::Interrupted
        ))
    );
    assert!(!client.info().accepting);
    let replacement = runtime(0, Default::default());
    assert!(matches!(
        client.push(other.clone(), policy::Event::Received),
        Err(votes::Error::Worker(votes::Fault::Stopped))
    ));
    let mut events = publications.listen().unwrap();
    let mut created = vec![];
    let mut received = vec![];
    for _ in 0..11 {
        let p = publication(&mut owner, &mut events).await;
        match p.event {
            policy::Event::Created => created.push(p.transaction.encode()),
            policy::Event::Received => received.push(p.transaction.encode()),
        }
    }
    assert_created(created, "local");
    assert_eq!(received, vec![other]);
    let runtime::Applied::Votes(snapshot) = call(
        &replacement.client(),
        runtime::Command::Votes(runtime::VoteQuery {
            epoch: 7.into(),
            kind: None,
            maximum: 100,
        }),
    )
    .wait(Duration::from_secs(2))
    .unwrap()
    .unwrap() else {
        panic!()
    };
    assert_eq!(snapshot.entries(), 0);
    owner.shutdown().unwrap();
    replacement.shutdown().unwrap();
}
#[tokio::test]
async fn claims_database_binding_and_start_failure_keep_runtime_ownership() {
    let node = Node::new();
    let another = Node::new();
    let (owner, worker, publications) = owner(&node, 0, Default::default(), Default::default());
    assert!(matches!(
        votes::Worker::attach_eoe(
            another.db.clone(),
            Default::default(),
            &owner,
            Default::default()
        ),
        Err(votes::Error::Worker(votes::Fault::Configuration))
    ));
    let runtime = runtime(0, Default::default());
    let fixed = runtime.client();
    let failed = match Owner::new(runtime, publications) {
        Err(e) => e,
        Ok(_) => panic!("duplicate owner accepted"),
    };
    assert_eq!(failed.error, votes::Error::Worker(votes::Fault::Busy));
    assert!(!fixed.is_closed());
    failed.runtime.shutdown().unwrap();
    assert!(fixed.is_closed());
    worker.shutdown().unwrap();
    owner.shutdown().unwrap();
}

#[tokio::test]
async fn encrypted_gossip_reads_and_publishes_through_the_eoe_authority() {
    use ae_p2p::{
        message::{Kind, Message, Value},
        runtime as net,
    };
    use ae_sync::gossip;
    use std::collections::BTreeMap;
    let node = Node::new();
    let (mut owner, worker, _) = owner(&node, 3, Default::default(), Default::default());
    let client = worker.client();
    let (peers, incoming, peer_task) = node.service(Default::default());
    let (mut handle, _events, service) = gossip::create_with_vote_client(
        node.db.clone(),
        node.client(),
        peers.clone(),
        incoming,
        NetworkProfile::mainnet(),
        Default::default(),
        client.clone(),
    )
    .unwrap();
    let task = tokio::spawn(service.run());
    let (local, (remote, mut messages, driver)) = common::pair(81, 82).await;
    let driver = tokio::spawn(driver.run());
    within(
        peers
            .attach(common::connection(local), false)
            .unwrap()
            .wait(),
    )
    .await
    .unwrap();
    negotiate(&owner.client());
    for _ in 0..10 {
        assert_eq!(route(&mut owner).await, Routed::Published);
    }
    let mut wire = vec![];
    while wire.len() < 10 {
        let net::Event::Message(message) = within(messages.recv()).await.unwrap() else {
            panic!()
        };
        assert_eq!(message.message().kind(), Kind::Transactions);
        let Value::List(transactions) = &message.message().fields()["txs"] else {
            panic!()
        };
        for tx in transactions {
            let Value::Binary(tx) = tx else { panic!() };
            wire.push(tx.clone());
        }
    }
    assert_created(wire.clone(), "local");
    let message = Message::new(
        Kind::Transactions,
        1,
        BTreeMap::from([(
            "txs".into(),
            Value::List(vec![Value::Binary(wire[0].clone())]),
        )]),
        Default::default(),
    )
    .unwrap();
    within(remote.send(&message).unwrap()).await.unwrap();
    within(async {
        loop {
            if handle.status().counts.votes_admitted == 1 {
                break;
            }
            handle.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(within(client.stats().unwrap()).await.unwrap().entries, 10);
    assert_eq!(
        within(handle.peek_votes(7.into(), None, 100).unwrap().wait())
            .await
            .unwrap()
            .len(),
        10
    );
    assert_eq!(route(&mut owner).await, Routed::Published);
    let net::Event::Message(message) = within(messages.recv()).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        message.message().fields()["txs"],
        Value::List(vec![Value::Binary(wire[0].clone())])
    );
    handle.shutdown();
    assert!(!within(task).await.unwrap().running);
    peers.shutdown();
    assert!(!within(peer_task).await.unwrap().running);
    remote.shutdown();
    within(driver).await.unwrap();
    worker.shutdown().unwrap();
    owner.shutdown().unwrap();
}

#[tokio::test]
async fn undersized_publication_sink_returns_the_exact_retained_output() {
    let node = Node::new();
    let config = votes::Config {
        event_bytes: 128,
        ..Default::default()
    };
    let (mut owner, worker, _) = owner(&node, 0, config, Default::default());
    let signed = vote(eoe::Mode::Fork, eoe::VoteType::Vote, 2);
    assert!(matches!(
        within(
            worker
                .client()
                .push(signed.encode(), policy::Event::Received)
                .unwrap()
        )
        .await
        .unwrap(),
        policy::PushOutcome::Accepted { inserted: true, .. }
    ));
    within(async {
        loop {
            match owner.route() {
                Ok(Routed::Idle) => tokio::time::sleep(Duration::from_millis(1)).await,
                Err(votes::Error::Pool(policy::Error::ResourceLimit)) => break,
                other => panic!("unexpected route {other:?}"),
            }
        }
    })
    .await;
    let runtime::Output::Service(coordinator::ServiceOutput::Publication(
        local_host::Output::Vote {
            signed: retained,
            event,
        },
    )) = owner.take_pending().unwrap()
    else {
        panic!()
    };
    assert_eq!(*retained, signed);
    assert_eq!(event, policy::Event::Received);
    assert!(owner.pending().is_none());
    assert_eq!(
        within(worker.client().stats().unwrap())
            .await
            .unwrap()
            .entries,
        1
    );
    worker.shutdown().unwrap();
    owner.shutdown().unwrap();
}
