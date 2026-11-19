use super::*;
mod coordinator;
use ae_hyperchain::eoe::local_host::{
    self, Config, Finalized, LocalHost, Output, SelectedTip, ServiceLimits,
};
use ae_mempool::hc_votes::{self, PushOutcome};
use ae_protocol::{ConsensusType, Protocol};
use ae_tx::CanonicalTransaction;

static SERVICE: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_hc_eoe_service.json")));
fn view_event(e: Event<u64>) -> Event<Arc<Trees>> {
    match e {
        Event::Negotiate(n) => Event::Negotiate(n),
        Event::Transaction { received, signed } => Event::Transaction { received, signed },
        Event::Timeout(t) => Event::Timeout(t),
        Event::ParentBlock { epoch, block } => Event::ParentBlock { epoch, block },
        Event::NewEpoch => Event::NewEpoch,
        Event::Ignore => Event::Ignore,
        Event::GetFinalize {
            request,
            view,
            nonce_offset,
        } => Event::GetFinalize {
            request,
            view: Arc::new(trees(view)),
            nonce_offset,
        },
    }
}
fn config(limits: ServiceLimits) -> Config {
    Config {
        network: NetworkProfile::mainnet(),
        election_contract: key(101),
        parent_epoch_length: 5.into(),
        parent_start_height: 11.into(),
        limits,
    }
}
fn tip(nonce: Option<u64>) -> SelectedTip {
    SelectedTip {
        trees: nonce.map(|n| Arc::new(trees(n))),
        protocol: Protocol::Ceres,
        consensus: ConsensusType::Pos,
    }
}
fn host(owned: &[u64], limits: ServiceLimits) -> LocalHost {
    LocalHost::new(
        config(limits),
        tip(Some(20)),
        owned
            .iter()
            .map(|n| {
                let k = signer(*n);
                (k.public_key(), k)
            })
            .collect(),
    )
}
fn add_parent(h: &mut LocalHost) {
    h.insert_parent(ParentBlock {
        hash: key(99).to_vec(),
        height: 41.into(),
        previous_hash: key(98).to_vec(),
        time: 12345.into(),
    })
    .unwrap();
}
fn final_json(f: Finalized) -> J {
    match f {
        Finalized::Signed(tx) => tuple(vec![atom("ok"), bin(tx.encode())]),
        Finalized::TopUnavailable => tuple(vec![
            atom("error"),
            tuple(vec![atom("error"), atom("chain_too_short")]),
        ]),
        Finalized::EntropyNotInCache => tuple(vec![atom("error"), atom("not_in_cache")]),
    }
}
fn output_json(outputs: &[Output]) -> J {
    list(
        outputs
            .iter()
            .filter_map(|o| match o {
                Output::Vote { event, signed } => Some(tuple(vec![
                    atom(match event {
                        hc_votes::Event::Created => "tx_created",
                        hc_votes::Event::Received => "tx_received",
                    }),
                    bin(signed.encode()),
                ])),
                _ => None,
            })
            .collect(),
    )
}
fn pool_json(h: &LocalHost) -> J {
    named(vec![
        ("epoch", num(h.pool().epoch())),
        ("size", int(h.pool().len())),
        (
            "votes",
            list(
                [7, 999]
                    .into_iter()
                    .flat_map(|e| (1..=6).map(move |t| (e, t)))
                    .map(|(e, t)| {
                        tuple(vec![
                            int(e),
                            int(t),
                            list(
                                h.pool()
                                    .peek(&e.into(), Some(&t.into()))
                                    .map(|v| bin(v.encode()))
                                    .collect(),
                            ),
                        ])
                    })
                    .collect(),
            ),
        ),
    ])
}
fn native_error(e: StepError<local_host::Error>) -> J {
    native_json(match e {
        StepError::Core(Failure::Native(n)) => n,
        StepError::Host(local_host::Error::MissingKey(_)) => Native::Badarg,
        e => panic!("unexpected native error {e:?}"),
    })
}
fn mapped_transition(t: Transition<Finalized>) -> Transition<J> {
    Transition {
        kind: t.kind,
        timeout: t.timeout,
        replies: t
            .replies
            .into_iter()
            .map(|(id, r)| {
                (
                    id,
                    match r {
                        Reply::NotReady => Reply::NotReady,
                        Reply::NoConsensus => Reply::NoConsensus,
                        Reply::Finalized(f) => Reply::Finalized(final_json(f)),
                    },
                )
            })
            .collect(),
    }
}
#[test]
fn original_pool_process_and_eoe_service_outputs_match() {
    let order = otp_order::profile(&SERVICE["profile"]);
    for row in SERVICE["runs"].as_array().unwrap() {
        let owned: Vec<_> = row["owned"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n.as_u64().unwrap())
            .collect();
        let stakers = owned.iter().map(|n| pubkey(*n)).collect();
        let get = |name| {
            row["config"][1]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| p[0] == atom(name))
                .map(|p| &p[1])
        };
        let mut host = host(&owned, ServiceLimits::default());
        let mut selected = tip((get("chain_error") != Some(&atom("true"))).then_some(20));
        selected.protocol =
            Protocol::try_from(u32::try_from(get("protocol").map_or(6, small)).unwrap()).unwrap();
        if get("consensus") == Some(&atom("pow")) {
            selected.consensus = ConsensusType::Pow;
        }
        host.select_tip(selected);
        if get("entropy_missing") != Some(&atom("true")) {
            add_parent(&mut host);
        }
        let cached = get("cached") == Some(&atom("true"));
        if cached {
            // The reference cache adapter returns true for all requests. Seed
            // every signed input and the local proposal for this finite trace.
            let mut hashes = BTreeSet::new();
            for step in row["steps"].as_array().unwrap() {
                match event(&step["input"]) {
                    Event::Transaction { signed, .. } => {
                        hashes.insert(ae_crypto::blake2b_256(&signed.encode()));
                    }
                    Event::Negotiate(n) => {
                        let mode = if row["mode"] == "fork" {
                            Mode::Fork
                        } else {
                            Mode::Length
                        };
                        let mut client = ClientState::empty(mode);
                        client
                            .initialize(
                                &n.epoch,
                                &n.hash,
                                &BTreeMap::new(),
                                &n.current_length,
                                &1000.into(),
                            )
                            .unwrap();
                        let vote = host
                            .create_vote(
                                n.leader,
                                &n.epoch,
                                &mode.type_number(VoteType::Proposal),
                                &client.proposal(&n.height).unwrap(),
                                Limits::default(),
                            )
                            .unwrap();
                        hashes.insert(ae_crypto::blake2b_256(&vote.encode()));
                    }
                    _ => {}
                }
            }
            host.set_gossip_cache(hashes).unwrap();
        }
        let mut machine = Machine::new(
            if row["mode"] == "fork" {
                Mode::Fork
            } else {
                Mode::Length
            },
            stakers,
            1000.into(),
        );
        let stakers = owned.iter().map(|n| pubkey(*n)).collect();
        assert_eq!(
            normalize(&state_json(&machine, &stakers, 1000)),
            normalize(&row["initial"])
        );
        for (i, step) in row["steps"].as_array().unwrap().iter().enumerate() {
            host.drain_outbox();
            let e = view_event(event(&step["input"]));
            let mut deliver = true;
            if let Event::Transaction { received, signed } = &e {
                let outcome = host.admit(
                    *signed.clone(),
                    if *received {
                        hc_votes::Event::Received
                    } else {
                        hc_votes::Event::Created
                    },
                );
                let admission = match outcome {
                    Ok(PushOutcome::Cached) => {
                        deliver = false;
                        atom("ok")
                    }
                    Ok(PushOutcome::Accepted { .. }) => atom("ok"),
                    Err(local_host::Error::Pool(e)) => {
                        deliver = false;
                        tuple(vec![
                            atom("error"),
                            atom(match e {
                                hc_votes::Error::InvalidAtProtocol => "invalid_at_protocol",
                                hc_votes::Error::Signature(_) => "signature_check_failed",
                                e => panic!("unexpected pool failure {e:?}"),
                            }),
                        ])
                    }
                    Err(e) => panic!("unexpected admission {e:?}"),
                };
                assert_eq!(
                    admission, step["admission"],
                    "admission {} {} {i}",
                    row["mode"], row["name"]
                );
            }
            let result = if deliver {
                machine.step(e, &mut host, &order, Limits::default())
            } else {
                Ok(Transition {
                    kind: TransitionKind::KeepData,
                    timeout: None,
                    replies: vec![],
                })
            };
            if !step["native"].is_null() {
                assert_eq!(native_error(result.unwrap_err()), step["native"]);
            } else {
                assert_eq!(
                    normalize(&transition_json(mapped_transition(result.unwrap_or_else(
                        |e| panic!("{} {} {i}: {e:?}", row["mode"], row["name"])
                    )))),
                    normalize(&step["result"]),
                    "result {} {} {i}",
                    row["mode"],
                    row["name"]
                );
            }
            assert_eq!(
                normalize(&state_json(&machine, &stakers, 1000)),
                normalize(&step["after_state"]),
                "state {} {} {i}",
                row["mode"],
                row["name"]
            );
            assert_eq!(
                output_json(host.outbox()),
                step["events"],
                "events {} {} {i}",
                row["mode"],
                row["name"]
            );
            assert_eq!(
                list(
                    host.outbox()
                        .iter()
                        .filter_map(|o| match o {
                            Output::ParentBlock { epoch, block } =>
                                Some(tuple(vec![num(epoch), block_json(block)])),
                            _ => None,
                        })
                        .collect()
                ),
                step["parent_events"],
                "parents {} {} {i}",
                row["mode"],
                row["name"]
            );
            assert_eq!(
                normalize(&pool_json(&host)),
                normalize(&step["pool"]),
                "pool {} {} {i}",
                row["mode"],
                row["name"]
            );
        }
    }
}

#[test]
fn publication_budget_rolls_back_pool_and_machine_then_retries() {
    let row = SERVICE["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "local" && r["mode"] == "fork")
        .unwrap();
    let order = otp_order::profile(&SERVICE["profile"]);
    let mut host = host(
        &[1, 2, 3],
        ServiceLimits {
            maximum_outbox_entries: 2,
            ..Default::default()
        },
    );
    let mut machine = Machine::new(
        Mode::Fork,
        [pubkey(1), pubkey(2), pubkey(3)].into(),
        1000.into(),
    );
    machine
        .step(
            view_event(event(&row["steps"][0]["input"])),
            &mut host,
            &order,
            Limits::default(),
        )
        .unwrap();
    let before = machine.clone();
    let pool = host.pool().clone();
    let outbox = host.outbox().to_vec();
    let proposal = view_event(event(&row["steps"][1]["input"]));
    assert_eq!(
        machine.step(proposal.clone(), &mut host, &order, Limits::default()),
        Err(StepError::Host(local_host::Error::ResourceLimit))
    );
    assert_eq!(machine, before);
    assert_eq!(host.pool(), &pool);
    assert_eq!(host.outbox(), outbox);
    host.drain_outbox();
    machine
        .step(proposal, &mut host, &order, Limits::default())
        .unwrap();
    assert_eq!(machine.state(), State::Vote);
    assert_eq!(host.outbox().len(), 2);
    assert_eq!(host.pool().len(), 3);
}

#[test]
fn core_resource_and_order_failures_preserve_retryable_state_and_effects() {
    struct BadOrder;
    impl ae_vm_fate::maps::MapOrder for BadOrder {
        fn indices(
            &self,
            keys: &[&ae_vm_fate::value::Value],
            _: ae_vm_fate::maps::Traversal,
            _: ae_vm_fate::Limits,
        ) -> Result<Vec<usize>, ae_vm_fate::Error> {
            Ok(vec![keys.len(); keys.len()])
        }
    }
    let order = otp_order::profile(&DATA["profile"]);
    let mut failures = 0;
    let mut bad_orders = 0;
    for row in
        DATA["runs"].as_array().unwrap().iter().filter(|r| {
            ["basic", "parent_seed", "duplicates"].contains(&r["name"].as_str().unwrap())
        })
    {
        let keys: BTreeMap<_, _> = row["owned"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| {
                let k = signer(n.as_u64().unwrap());
                (k.public_key(), k)
            })
            .collect();
        let mut machine = Machine::new(
            if row["mode"] == "fork" {
                Mode::Fork
            } else {
                Mode::Length
            },
            keys.keys().copied().collect(),
            1000.into(),
        );
        let mut host = TestHost {
            keys: Arc::new(keys),
            published: vec![],
            entropy_heights: vec![],
            parent_events: vec![],
            reject: false,
            chain_error: false,
            entropy_missing: false,
        };
        for step in row["steps"].as_array().unwrap() {
            let e = event(&step["input"]);
            let before = machine.clone();
            let original = host.clone();
            let expected = machine.step(e.clone(), &mut host, &order, Limits::default());
            if expected.is_err() {
                continue;
            }
            let limits = [
                Limits {
                    maximum_bytes: 0,
                    ..Default::default()
                },
                Limits {
                    maximum_bytes: 600,
                    ..Default::default()
                },
                Limits {
                    maximum_fields: 0,
                    ..Default::default()
                },
                Limits {
                    maximum_validators: 1,
                    ..Default::default()
                },
                Limits {
                    maximum_buffered_votes: 0,
                    ..Default::default()
                },
                Limits {
                    maximum_parent_blocks: 0,
                    ..Default::default()
                },
                Limits {
                    fate: ae_vm_fate::Limits {
                        max_nodes: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ];
            for limit in limits {
                let mut trial = before.clone();
                let mut staged = original.clone();
                let result = trial.step(e.clone(), &mut staged, &order, limit);
                if let Err(error) = result {
                    assert!(
                        matches!(
                            error,
                            StepError::Core(
                                Failure::ResourceLimit
                                    | Failure::Fate(ae_vm_fate::Error::ResourceLimit)
                            )
                        ) || matches!(&error,StepError::Host(s) if s.contains("ResourceLimit")),
                        "{error:?}"
                    );
                    assert_eq!(trial, before);
                    assert_eq!(staged.published, original.published);
                    assert_eq!(staged.entropy_heights, original.entropy_heights);
                    assert_eq!(staged.parent_events, original.parent_events);
                    assert_eq!(
                        trial.step(e.clone(), &mut staged, &order, Limits::default()),
                        expected
                    );
                    assert_eq!(trial, machine);
                    assert_eq!(staged.published, host.published);
                    assert_eq!(staged.parent_events, host.parent_events);
                    failures += 1;
                }
            }
            let mut trial = before.clone();
            let mut staged = original.clone();
            if let Err(StepError::Core(Failure::InvalidOrder)) =
                trial.step(e.clone(), &mut staged, &BadOrder, Limits::default())
            {
                assert_eq!(trial, before);
                assert_eq!(staged.published, original.published);
                assert_eq!(
                    trial.step(e, &mut staged, &order, Limits::default()),
                    expected
                );
                assert_eq!(trial, machine);
                bad_orders += 1;
            }
        }
    }
    assert!(failures > 100);
    assert_eq!(bad_orders, 6);
    println!("EOE resource retries {failures}; invalid-order retries {bad_orders}");
}

#[test]
fn finalization_read_failures_preserve_outbox_and_retry_at_every_prefix() {
    use ae_trie::{MemoryStore, NodeReader};
    use ae_types::Hash256;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    #[derive(Clone)]
    struct ReadFault {
        inner: MemoryStore,
        remaining: Arc<AtomicUsize>,
        log: Arc<Mutex<Vec<Hash256>>>,
    }
    impl NodeReader for ReadFault {
        fn node(&self, hash: &Hash256) -> Result<Option<Vec<u8>>, ae_trie::Error> {
            self.log.lock().unwrap().push(*hash);
            let n = self.remaining.load(Ordering::Relaxed);
            if n == 0 {
                return Err(ae_trie::Error::Backend("EOE read fault".into()));
            }
            self.remaining.store(n - 1, Ordering::Relaxed);
            self.inner.node(hash)
        }
    }
    let base = trees(20);
    let remaining = Arc::new(AtomicUsize::new(usize::MAX));
    let log = Arc::new(Mutex::new(vec![]));
    let store = |inner| ReadFault {
        inner,
        remaining: remaining.clone(),
        log: log.clone(),
    };
    let selected = Arc::new(Trees {
        accounts: ae_state::accounts::Accounts::from_root(
            base.accounts.root_hash(),
            store(base.accounts.tree().store().clone()),
        )
        .unwrap(),
        contracts: ae_state::contracts::Contracts::from_root(
            base.contracts.root_hash(),
            store(base.contracts.tree().store().clone()),
        )
        .unwrap(),
        calls: ae_state::calls::Calls::new(store(MemoryStore::default())),
        channels: ae_state::channels::Channels::new(store(MemoryStore::default())),
        names: ae_state::names::Names::new(
            store(MemoryStore::default()),
            store(MemoryStore::default()),
        ),
        oracles: ae_state::oracles::Oracles::new(
            store(MemoryStore::default()),
            store(MemoryStore::default()),
        ),
    });
    let mut h = LocalHost::new(
        config(ServiceLimits::default()),
        SelectedTip {
            trees: Some(selected),
            protocol: Protocol::Ceres,
            consensus: ConsensusType::Pos,
        },
        [(pubkey(1), signer(1))].into(),
    );
    h.insert_parent(ParentBlock {
        hash: key(99).to_vec(),
        height: 41.into(),
        previous_hash: key(98).to_vec(),
        time: 12345.into(),
    })
    .unwrap();
    log.lock().unwrap().clear();
    let roots = h.clone();
    let expected = h
        .finalize(&7.into(), pubkey(1), &[1, 2, 3], &1.into(), None)
        .unwrap();
    let reads = log.lock().unwrap().clone();
    assert!(!reads.is_empty());
    for prefix in 0..reads.len() {
        let mut trial = roots.clone();
        log.lock().unwrap().clear();
        remaining.store(prefix, Ordering::Relaxed);
        let error = trial
            .finalize(&7.into(), pubkey(1), &[1, 2, 3], &1.into(), None)
            .unwrap_err();
        assert!(error.is_operational());
        assert_eq!(&*log.lock().unwrap(), &reads[..=prefix]);
        assert!(trial.outbox().is_empty());
        assert!(trial.pool().is_empty());
        remaining.store(usize::MAX, Ordering::Relaxed);
        log.lock().unwrap().clear();
        assert_eq!(
            trial
                .finalize(&7.into(), pubkey(1), &[1, 2, 3], &1.into(), None)
                .unwrap(),
            expected
        );
        assert_eq!(&*log.lock().unwrap(), &reads);
        assert_eq!(trial.outbox(), h.outbox());
    }
    println!("EOE finalization backend prefixes {}", reads.len());
}
