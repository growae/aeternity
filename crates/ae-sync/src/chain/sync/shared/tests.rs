use super::*;

#[test]
fn scheduled_ping_membership_matches_native_active_and_retained_suspect_tasks() {
    struct Owner(Book<Otp26>);
    impl PeerMembership for Owner {
        fn peers_in_sync(&self) -> Result<Vec<PeerPublicKey>, Error> {
            Ok(self.0.peers_in_sync())
        }
    }
    fn peer(n: u8) -> PeerPublicKey {
        let mut bytes = [0; 32];
        bytes[31] = n;
        PeerPublicKey(bytes)
    }
    let rows: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/erlang_sync_ping.json"
    ))
    .unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 12);
    for row in rows.as_array().unwrap() {
        let mut book = Book::<Otp26>::new();
        let specs: &[(u8, bool, u64)] = match row["mode"].as_str().unwrap() {
            "empty" => &[],
            "active" => &[(1, false, 100)],
            "suspect" => &[(1, true, 100)],
            "mixed" => &[(1, true, 100), (2, false, 200)],
            _ => panic!("unknown native case"),
        };
        for &(id, suspect, height) in specs {
            let limits = coordination::Limits::default();
            let chain = Chain::new(
                id.into(),
                vec![peer(id)],
                vec![Point {
                    height,
                    hash: Hash256([0; 32]),
                }],
                limits,
            )
            .unwrap();
            let mut state = Scheduler::new(chain.clone(), limits)
                .unwrap()
                .state()
                .clone();
            state.suspect = suspect;
            // Native membership is in the chain, not the active workers.
            assert!(state.workers.is_empty());
            book.groups.push(Group {
                model: Scheduler::restore(state, limits).unwrap(),
                statistics: statistics::Task::new(chain.id(), chain.target(), limits.items)
                    .unwrap(),
                statistics_failure: None,
                members: BTreeMap::new(),
                payloads: BTreeMap::new(),
                agreed: None,
                cursor: None,
                selected: None,
                finished: false,
                failed: None,
            });
        }
        let owner: Arc<dyn PeerMembership> = Arc::new(Owner(book));
        let peers = PeerSet(Arc::downgrade(&owner));
        let key = peer(row["peer"].as_u64().unwrap().try_into().unwrap());
        let actual = if peers.contains(key) {
            serde_json::json!({"kind":"log_ping", "outcome":"ok"})
        } else {
            serde_json::json!({"kind":"job", "queue":"sync_ping"})
        };
        assert_eq!(actual, row["event"], "{row}");
        assert_eq!(Arc::strong_count(&owner), 1);
        drop(owner);
        // A different live owner cannot revive the connection's old binding.
        let _replacement: Arc<dyn PeerMembership> = Arc::new(Owner(Book::new()));
        assert_eq!(peers.peers(), Err(Error::Closed));
        assert!(!peers.contains(key));
    }
}

#[tokio::test]
async fn rejected_workers_publish_native_completion_order_and_only_idle_completion_latches_gc() {
    use crate::events::Delivery;
    let ((a, _, _da), (b, _, _db)) = crate::test_support::pair(7, 8).await;
    let clients = if a.peer() < b.peer() {
        [&a, &b]
    } else {
        [&b, &a]
    };
    let corpus: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_business_events.json"
    )))
    .unwrap();
    let limits = coordination::Limits::default();
    let blocks =
        crate::test_support::ledger_fixtures::blocks(&ae_protocol::NetworkProfile::mainnet());
    for row in corpus["cases"].as_array().unwrap() {
        let count = row["workers"].as_u64().unwrap() as usize;
        let mut book = Book::<Otp26>::new();
        let header = blocks[2].header();
        let mut registration = clients
            .iter()
            .take(count.max(1))
            .copied()
            .collect::<Vec<_>>();
        if row["reverse"] == true {
            registration.reverse();
        }
        for client in &registration {
            let chain = Chain::from_header(1, client.peer(), &header, limits).unwrap();
            assert!(matches!(
                book.join(&chain, client, limits, 1).unwrap(),
                Join::Joined(1)
            ));
        }
        if row["other"] != "none" {
            let mut different = header.clone();
            let ae_codec::headers::Header::Key(ref mut key) = different else {
                panic!()
            };
            key.nonce += 1;
            let chain = Chain::from_header(2, a.peer(), &different, limits).unwrap();
            assert!(matches!(
                book.join(&chain, &a, limits, 1).unwrap(),
                Join::Joined(2)
            ));
            let group = book.group_mut(2).unwrap();
            let mut state = group.model.state().clone();
            state.suspect = row["other"] == "suspect";
            group.model = Scheduler::restore(state, limits).unwrap();
            book.lifecycle_changed();
            assert!(book.peers_in_sync().contains(&a.peer()));
        }
        let item = Item {
            point: Point {
                height: 1,
                hash: blocks[0].hash().unwrap(),
            },
            got: Some(Ready {
                peer: clients[0].peer(),
                ticket: 1,
            }),
            attempts: 0,
        };
        let lease = Lease {
            task: 1,
            sequence: 4,
            peer: clients[0].peer(),
            generation: clients[0].generation().clone(),
        };
        let group = book.group_mut(1).unwrap();
        let mut state = group.model.state().clone();
        state.adding = vec![item];
        assert_eq!(
            state.workers,
            registration.iter().map(|c| c.peer()).collect::<Vec<_>>()
        );
        // A rejected result can arrive after its native workers have exited.
        if count == 0 {
            state.workers.clear();
        }
        group.model = Scheduler::restore(state, limits).unwrap();
        group.members.get_mut(&lease.peer).unwrap().lease = Some((4, Kind::Post { posted: 0 }));
        let mut sub = book.feed().subscribe(None).unwrap();
        let rejected = row["rejected"].as_bool().unwrap();
        let error = Error::Import(chain::Failure {
            progress: Some(Default::default()),
            error: if rejected {
                chain::Error::BlockedByWhitelist
            } else {
                chain::Error::Validation {
                    operational: false,
                    reason: "fixture".into(),
                }
            },
        });
        let suspects = book.post_failed(&lease, item, error, vec![]).unwrap();
        assert_eq!(suspects.len(), if rejected { count } else { 0 });
        book.close_events();
        let mut actual = vec![];
        while let Some(delivery) = sub.recv().await {
            let Delivery::Event(record) = delivery else {
                panic!()
            };
            match &record.event {
                lifecycle::Event::IsSyncing(value) => {
                    actual.push(serde_json::json!({"kind":"syncing","value":value}))
                }
                lifecycle::Event::PeerDone {
                    completion,
                    syncing,
                } => {
                    let index = clients
                        .iter()
                        .position(|c| c.peer() == completion.peer)
                        .unwrap();
                    assert_eq!(&completion.generation, clients[index].generation());
                    assert_eq!(completion.reason, lifecycle::DoneReason::Suspect);
                    assert_eq!(*syncing, row["syncing"].as_bool().unwrap());
                    actual.push(serde_json::json!({"kind":"done","peer":index+1}));
                }
            }
        }
        let expected: Vec<_> = row["effects"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "done" || e["kind"] == "syncing")
            .cloned()
            .collect();
        assert_eq!(actual, expected, "{row}");
        assert_eq!(
            book.notices().count,
            if rejected { count as u64 } else { 0 }
        );
        assert_eq!(
            book.notices().gc_ready.is_some(),
            rejected && count > 0 && row["syncing"] == false
        );
    }
}

#[tokio::test]
async fn idle_transition_without_completion_never_latches_gc_and_overflow_cannot_erase_readiness() {
    let ((client, _, _driver), (_, _, _remote)) = crate::test_support::pair(7, 8).await;
    let limits = coordination::Limits::default();
    let blocks =
        crate::test_support::ledger_fixtures::blocks(&ae_protocol::NetworkProfile::mainnet());
    let mut book = Book::<Otp26>::with_events(crate::events::Config {
        records: 1,
        bytes: 192,
        event_bytes: 128,
        subscribers: 1,
    })
    .unwrap();
    let mut sub = book.feed().subscribe(None).unwrap();
    assert!(book.notices().gc_ready.is_none());
    let chain = Chain::from_header(1, client.peer(), &blocks[2].header(), limits).unwrap();
    book.join(&chain, &client, limits, 1).unwrap();
    book.done(
        client.peer(),
        client.generation().clone(),
        lifecycle::DoneReason::Ping,
    );
    assert!(book.notices().gc_ready.is_none());
    book.detach(1, &client, limits);
    assert!(!book.lifecycle().0.iter().any(|t| !t.suspect));
    assert!(book.notices().gc_ready.is_none());
    book.done(
        client.peer(),
        client.generation().clone(),
        lifecycle::DoneReason::Exhausted,
    );
    let ready = book.notices().gc_ready.unwrap();
    for _ in 0..5 {
        book.done(
            client.peer(),
            client.generation().clone(),
            lifecycle::DoneReason::Ping,
        );
    }
    assert_eq!(book.notices().gc_ready, Some(ready));
    assert_eq!(book.notices().count, 7);
    assert!(matches!(
        sub.recv().await,
        Some(crate::events::Delivery::Gap { missed: 8 })
    ));
    assert_eq!(book.feed().info().retained, 1);
    assert!(book.notices().error.is_none());
}
#[tokio::test]
async fn posting_and_retry_height_updates_apply_only_to_exact_worker_generations() {
    let ((a, _, _da), (b, _, _db)) = crate::test_support::pair(7, 8).await;
    let ((replacement, _, _dr), (_, _, _ds)) = crate::test_support::pair(7, 8).await;
    let limits = coordination::Limits::default();
    let blocks =
        crate::test_support::ledger_fixtures::blocks(&ae_protocol::NetworkProfile::mainnet());
    let mut book = Book::<Otp26>::new();
    let header = blocks[2].header();
    let chain = Chain::from_header(1, a.peer(), &header, limits).unwrap();
    assert!(matches!(
        book.join(&chain, &a, limits, 1).unwrap(),
        Join::Joined(1)
    ));
    let chain = Chain::from_header(2, b.peer(), &header, limits).unwrap();
    assert!(matches!(
        book.join(&chain, &b, limits, 2).unwrap(),
        Join::Joined(1)
    ));
    let point = Point {
        height: 1,
        hash: blocks[0].hash().unwrap(),
    };
    let item = Item {
        point,
        got: Some(Ready {
            peer: a.peer(),
            ticket: 1,
        }),
        attempts: 0,
    };
    let lease = Lease {
        task: 1,
        sequence: 4,
        peer: a.peer(),
        generation: a.generation().clone(),
    };
    {
        let group = book.group_mut(1).unwrap();
        let mut state = group.model.state().clone();
        state.agreed = Some(point);
        state.adding = vec![item];
        group.model = Scheduler::restore(state, limits).unwrap();
        group.members.get_mut(&a.peer()).unwrap().lease = Some((4, Kind::Post { posted: 1 }));
    }
    let before_statistics = book.statistics().tasks;
    let stale = Lease {
        generation: replacement.generation().clone(),
        ..lease.clone()
    };
    assert_eq!(book.post_complete(&stale, &[], 4), Err(Error::Stale));
    assert_eq!(book.statistics().tasks, before_statistics);
    book.post_complete(&lease, &[], 4).unwrap();
    assert_eq!(book.sync_height(&a), Some(1));
    assert_eq!(book.sync_height(&b), Some(1));
    assert_eq!(book.sync_height(&replacement), None);
    let mut item = item;
    item.point.height = 2;
    {
        let group = book.group_mut(1).unwrap();
        let mut state = group.model.state().clone();
        state.adding = vec![item];
        group.model = Scheduler::restore(state, limits).unwrap();
        group.members.get_mut(&a.peer()).unwrap().lease = Some((4, Kind::Post { posted: 0 }));
    }
    let error = Error::Import(chain::Failure {
        progress: Some(Default::default()),
        error: chain::Error::Validation {
            operational: false,
            reason: "invalid body".into(),
        },
    });
    assert!(
        book.post_failed(&lease, item, error, vec![])
            .unwrap()
            .is_empty()
    );
    assert_eq!(book.sync_height(&a), Some(2));
    assert_eq!(book.sync_height(&b), Some(2));
    book.detach(1, &replacement, limits);
    assert_eq!(book.sync_height(&a), Some(2));
    book.detach(1, &a, limits);
    assert_eq!(book.sync_height(&a), None);
    assert_eq!(book.sync_height(&b), Some(2));
    let lease = Lease {
        task: 1,
        sequence: 5,
        peer: b.peer(),
        generation: b.generation().clone(),
    };
    book.group_mut(1)
        .unwrap()
        .members
        .get_mut(&b.peer())
        .unwrap()
        .lease = Some((5, Kind::Forward));
    book.forwarded(&lease, point).unwrap();
    assert_eq!(book.sync_height(&b), None);
}

#[tokio::test]
async fn suspect_tasks_survive_last_member_and_remain_excluded_from_global_progress() {
    let ((client, _, _driver), (_, _, _remote)) = crate::test_support::pair(7, 8).await;
    let limits = coordination::Limits {
        peers: 1,
        ..Default::default()
    };
    let blocks =
        crate::test_support::ledger_fixtures::blocks(&ae_protocol::NetworkProfile::mainnet());
    let native: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/erlang_sync_lifecycle.json"
    )))
    .unwrap();
    let mut header = blocks[2].header();
    let ae_codec::headers::Header::Key(ref mut key) = header else {
        panic!()
    };
    key.common.height = 10;
    for row in native["removed"].as_array().unwrap() {
        let candidate = Chain::from_header(1, client.peer(), &header, limits).unwrap();
        let mut book = Book::<Otp26>::new();
        book.join(&candidate, &client, limits, 3).unwrap();
        let group = book.group_mut(1).unwrap();
        let mut state = group.model.state().clone();
        state.suspect = row["suspect"].as_bool().unwrap();
        group.model = Scheduler::restore(state, limits).unwrap();
        book.lifecycle_changed();
        book.detach(1, &client, limits);
        let (tasks, target, _) = book.lifecycle();
        let tasks_json: Vec<_> = tasks
            .iter()
            .map(|t| serde_json::json!({"id":t.id,"target":t.target,"suspect":t.suspect}))
            .collect();
        assert_eq!(serde_json::json!(tasks_json), row["tasks"]);
        assert_eq!(target, row["top_target"].as_u64().unwrap());
        assert_eq!(
            tasks.iter().any(|t| !t.suspect),
            row["is_syncing"].as_bool().unwrap()
        );
    }
    let candidate = Chain::from_header(1, client.peer(), &header, limits).unwrap();
    let mut book = Book::<Otp26>::new();
    assert!(matches!(
        book.join(&candidate, &client, limits, 3),
        Ok(Join::Joined(1))
    ));
    let (tasks, target, transitions) = book.lifecycle();
    assert_eq!(target, 10);
    assert_eq!(transitions, 1);
    assert!(lifecycle::progress(&tasks, target, 1).unwrap().syncing);
    {
        let group = book.group_mut(1).unwrap();
        let mut state = group.model.state().clone();
        state.suspect = true;
        group.model = Scheduler::restore(state, limits).unwrap();
        group.failed = Some(Error::BlockedByWhitelist);
    }
    book.detach(1, &client, limits);
    let (tasks, target, transitions) = book.lifecycle();
    assert_eq!(
        tasks,
        vec![lifecycle::Task {
            id: 1,
            target: 10,
            suspect: true
        }]
    );
    assert_eq!(target, 0);
    assert_eq!(transitions, 2);
    assert_eq!(book.statistics().terminated, 0);
    assert_eq!(book.statistics().tasks.len(), 1);
    assert!(!lifecycle::progress(&tasks, target, 1).unwrap().syncing);
    assert!(matches!(
        book.join(&candidate, &client, limits, 3),
        Err(Error::BlockedByWhitelist)
    ));
    assert_eq!(book.lifecycle().1, 0);
    // Retained suspect hints consume the same explicit task bound.
    let mut different = header;
    let ae_codec::headers::Header::Key(ref mut key) = different else {
        panic!()
    };
    key.nonce += 1;
    let candidate = Chain::from_header(2, client.peer(), &different, limits).unwrap();
    assert!(matches!(
        book.join(&candidate, &client, limits, 3),
        Err(Error::Busy)
    ));
}

#[tokio::test]
async fn failed_statistics_do_not_reject_a_completed_post_or_retain_terminated_tasks() {
    let ((client, _, _driver), (_, _, _remote)) = crate::test_support::pair(7, 8).await;
    let limits = coordination::Limits::default();
    let blocks =
        crate::test_support::ledger_fixtures::blocks(&ae_protocol::NetworkProfile::mainnet());
    let mut book = Book::<Otp26>::new();
    let candidate = Chain::from_header(1, client.peer(), &blocks[2].header(), limits).unwrap();
    book.join(&candidate, &client, limits, 1).unwrap();
    let point = Point {
        height: 1,
        hash: blocks[0].hash().unwrap(),
    };
    let item = Item {
        point,
        got: Some(Ready {
            peer: client.peer(),
            ticket: 1,
        }),
        attempts: 0,
    };
    let lease = Lease {
        task: 1,
        sequence: 4,
        peer: client.peer(),
        generation: client.generation().clone(),
    };
    {
        let group = book.group_mut(1).unwrap();
        let mut state = group.model.state().clone();
        state.agreed = Some(point);
        state.adding = vec![item];
        group.model = Scheduler::restore(state, limits).unwrap();
        group.members.get_mut(&client.peer()).unwrap().lease = Some((4, Kind::Post { posted: 1 }));
    }
    let invalid = statistics::Sample {
        height: 1,
        start_us: i128::MIN,
        end_us: i128::MAX,
        micro_blocks: 0,
        transactions: 0,
        work_us: None,
    };
    book.post_complete(&lease, &[invalid], 2).unwrap();
    assert!(book.group(1).unwrap().model.state().adding.is_empty());
    assert!(book.snapshot(1, &client).is_ok());
    let status = book.statistics();
    assert_eq!(status.tasks[0].failure, Some(statistics::Error::Overflow));
    assert_eq!(status.tasks[0].total.generations, 0);
    assert_eq!(status.estimate(1), Err(statistics::Error::Stopped));
    assert_eq!(status.estimate(2), Err(statistics::Error::UnknownTask));
    book.detach(1, &client, limits);
    let status = book.statistics();
    assert!(status.tasks.is_empty());
    assert_eq!(status.terminated, 1);
    assert_eq!(
        status.last_terminated.unwrap().failure,
        Some(statistics::Error::Overflow)
    );
}
