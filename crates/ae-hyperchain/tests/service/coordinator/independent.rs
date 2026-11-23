use super::*;
use ae_hyperchain::eoe::pool;
use ae_mempool::hc_votes::VotePool;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

static NATIVE: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../../fixtures/erlang_eoe_pool_lifetime.json")));
fn native(name: &str) -> &'static J {
    &NATIVE["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == name)
        .unwrap()["result"]
}
fn hub(config: pool::Config) -> (pool::Hub, pool::Owner) {
    let hub = pool::Hub::new(NetworkProfile::mainnet(), config).unwrap();
    let owner = hub.start_pool(Default::default()).unwrap();
    (hub, owner)
}
fn coordinator(hub: &pool::Hub, owned: &[u64]) -> Coordinator {
    Coordinator::new(
        host(owned, Default::default())
            .with_independent_pool(hub.clone())
            .unwrap(),
        1_000_000_000.into(),
        Default::default(),
    )
}
fn admission(mode: Mode, epoch: u64, short: bool) -> pool::Admission {
    let original = vote(mode, VoteType::Proposal, 1, short);
    let ae_tx::TransactionLeaf::HcVote(vote) = original.transaction().view().leaf().clone() else {
        unreachable!()
    };
    let fields = vote
        .data()
        .iter()
        .map(|(k, v)| (k.clone(), Field::Binary(v.clone())))
        .collect();
    let signed = payload::create_transaction(
        pubkey(1),
        epoch.into(),
        vote.vote_type().clone(),
        &fields,
        &signer(1),
        &NetworkProfile::mainnet(),
        Limits::default(),
    )
    .unwrap();
    pool::Admission {
        signed,
        event: hc_votes::Event::Received,
        protocol: Protocol::Ceres,
        consensus: ConsensusType::Pos,
    }
}
fn pump(c: &mut Coordinator, inbox: &mut pool::Inbox) {
    for _ in 0..1000 {
        if c.is_stopped() {
            return;
        }
        if c.queued_events() > 0 {
            run(c);
        } else if let Some(input) = inbox.peek() {
            c.receive(&input).unwrap();
            inbox.acknowledge();
        } else {
            return;
        }
    }
    panic!("input loop did not become idle");
}
fn seed(hub: &pool::Hub, c: &mut Coordinator, inbox: &mut pool::Inbox) {
    for mode in [Mode::Fork, Mode::Length] {
        hub.client()
            .unwrap()
            .admit(admission(mode, 7, false))
            .unwrap();
    }
    hub.new_epoch(7.into()).unwrap();
    pump(c, inbox);
    for mode in [Mode::Fork, Mode::Length] {
        hub.client()
            .unwrap()
            .admit(admission(mode, 7, false))
            .unwrap();
    }
    pump(c, inbox);
    c.negotiate(negotiation(7)).unwrap();
    pump(c, inbox);
}
fn assert_pool(actual: &VotePool, expected: &J) {
    let fields = expected[1].as_array().unwrap();
    assert_eq!(actual.epoch(), &uint(&fields[1]));
    let entries = fields[2][1].as_array().unwrap();
    assert_eq!(actual.len(), entries.len());
    let expected_votes = entries.iter().map(|p| p[1].clone()).collect::<Vec<_>>();
    let epochs = fields[3][1].as_array().unwrap();
    let mut actual_votes = vec![];
    for entry in epochs {
        let epoch = uint(&entry[0]);
        let votes: Vec<_> = actual.peek(&epoch, None).collect();
        assert_eq!(votes.len(), entry[1][1].as_array().unwrap().len());
        for vote in votes {
            actual_votes.push(tuple(vec![
                atom("hc_vote_tx"),
                tuple(vec![atom("id"), atom("account"), bin(vote.voter().0)]),
                num(vote.epoch()),
                num(vote.vote_type()),
                map(vote.data().iter().map(|(k, v)| (bin(k), bin(v))).collect()),
            ]));
        }
    }
    let mut a: Vec<_> = actual_votes.iter().map(ToString::to_string).collect();
    let mut e: Vec<_> = expected_votes.iter().map(ToString::to_string).collect();
    a.sort();
    e.sort();
    assert_eq!(a, e);
}
fn machines(c: &Coordinator) -> J {
    let mut states = serde_json::Map::new();
    for (name, mode) in [("fork", Mode::Fork), ("length", Mode::Length)] {
        let m = c.machine(mode);
        states.insert(name.into(), json!({"state": match m.state() { State::AwaitEpoch=>"await_eoe",State::Proposal=>"proposal",State::Vote=>"vote",State::Finalize=>"finalize",State::Complete=>"complete"},"epoch":optional(m.epoch(),num),"proposal_present":m.proposal().is_some(),"votes":m.votes().len(),"buffered":m.buffered().len()}));
    }
    J::Object(states)
}
#[test]
fn native_eoe_replacement_and_pool_replacement_keep_separate_lifetimes() {
    for name in ["eoe_normal", "eoe_killed"] {
        let (hub, owner) = hub(Default::default());
        let fixed = owner.client();
        let mut c = coordinator(&hub, &[]);
        let mut inbox = c.subscribe(Default::default()).unwrap().unwrap();
        seed(&hub, &mut c, &mut inbox);
        assert_pool(&fixed.inspect(Clone::clone).unwrap(), &native(name)["pool"]);
        let before = fixed.inspect(Clone::clone).unwrap();
        if name == "eoe_normal" {
            let _retired = inbox.retire();
        } else {
            drop(inbox);
        }
        drop(c);
        let mut replacement = coordinator(&hub, &[]);
        let inbox = replacement.subscribe(Default::default()).unwrap().unwrap();
        assert!(inbox.peek().is_none());
        assert!(fixed.same_pool(&hub.client().unwrap()));
        assert_eq!(fixed.inspect(Clone::clone).unwrap(), before);
        assert_eq!(machines(&replacement), native(name)["new_machines"]);
    }
    let (hub, owner) = hub(Default::default());
    let fixed = owner.client();
    let mut c = coordinator(&hub, &[]);
    let mut inbox = c.subscribe(Default::default()).unwrap().unwrap();
    seed(&hub, &mut c, &mut inbox);
    let before = machines(&c);
    assert_pool(&owner.retire(), &native("pool_normal")["before"]);
    assert!(fixed.is_closed());
    assert!(c.snapshot().pool.is_err());
    let replacement = hub.start_pool(Default::default()).unwrap();
    assert_pool(
        &replacement.client().inspect(Clone::clone).unwrap(),
        &native("pool_normal")["after"],
    );
    assert_eq!(machines(&c), before);
    assert_eq!(before, native("pool_normal")["state"]);
}
#[test]
fn native_epoch_during_eoe_absence_clears_future_votes_without_replay() {
    let (hub, owner) = hub(Default::default());
    let mut c = coordinator(&hub, &[]);
    let mut inbox = c.subscribe(Default::default()).unwrap().unwrap();
    seed(&hub, &mut c, &mut inbox);
    owner
        .client()
        .admit(admission(Mode::Length, 999, false))
        .unwrap();
    assert_pool(
        &owner.client().inspect(Clone::clone).unwrap(),
        &native("epoch_during_eoe_gap")["before"],
    );
    let old = inbox.retire();
    assert_eq!(old.pending.len(), 1);
    drop(c);
    hub.new_epoch(9.into()).unwrap();
    assert_pool(
        &owner.client().inspect(Clone::clone).unwrap(),
        &native("epoch_during_eoe_gap")["after_epoch"],
    );
    owner
        .client()
        .admit(admission(Mode::Fork, 9, false))
        .unwrap();
    assert_pool(
        &owner.client().inspect(Clone::clone).unwrap(),
        &native("epoch_during_eoe_gap")["during"],
    );
    let mut c = coordinator(&hub, &[]);
    let mut inbox = c.subscribe(Default::default()).unwrap().unwrap();
    assert!(inbox.peek().is_none());
    assert_eq!(machines(&c), native("epoch_during_eoe_gap")["new_machines"]);
    assert!(c.receive(&old.pending[0]).is_err());
    for mode in [Mode::Fork, Mode::Length] {
        owner.client().admit(admission(mode, 9, false)).unwrap();
    }
    pump(&mut c, &mut inbox);
    c.negotiate(negotiation(9)).unwrap();
    pump(&mut c, &mut inbox);
    assert_eq!(
        machines(&c),
        native("epoch_during_eoe_gap")["after_explicit_events"]
    );
}
#[test]
fn native_inner_restart_exhaustion_preserves_pool_and_excludes_stale_child_events() {
    let (hub, owner) = hub(Default::default());
    let mut c = coordinator(&hub, &[]);
    let mut inbox = c.subscribe(Default::default()).unwrap().unwrap();
    owner
        .client()
        .admit(admission(Mode::Length, 999, false))
        .unwrap();
    hub.new_epoch(7.into()).unwrap();
    pump(&mut c, &mut inbox);
    for (n, row) in native("inner_exhaustion")["trace"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        owner
            .client()
            .admit(admission(Mode::Fork, 7, true))
            .unwrap();
        pump(&mut c, &mut inbox);
        assert_pool(&owner.client().inspect(Clone::clone).unwrap(), &row["pool"]);
        assert_eq!(c.is_stopped(), row["outer_replaced"].as_bool().unwrap());
        assert_eq!(c.snapshot().generations, [(n + 1) as u64, 0]);
    }
    drop(inbox);
    drop(c);
    let c = coordinator(&hub, &[]);
    assert_eq!(machines(&c), native("inner_exhaustion")["new_machines"]);

    // Two already-delivered malformed votes target the old fork incarnation.
    let mut c = coordinator(&hub, &[]);
    let mut inbox = c.subscribe(Default::default()).unwrap().unwrap();
    for _ in 0..2 {
        owner
            .client()
            .admit(admission(Mode::Fork, 7, true))
            .unwrap();
    }
    pump(&mut c, &mut inbox);
    assert_eq!(c.snapshot().generations, [1, 0]);
    owner
        .client()
        .admit(admission(Mode::Fork, 7, true))
        .unwrap();
    pump(&mut c, &mut inbox);
    assert_eq!(c.snapshot().generations, [2, 0]);
}
#[test]
fn independent_publication_handoff_and_retained_byte_credits_are_atomic() {
    let (hub, owner) = hub(pool::Config {
        maximum_inputs: 1,
        ..Default::default()
    });
    let mut inbox = hub.subscribe(Default::default()).unwrap();
    let mut publications = hub.publications().unwrap();
    assert!(matches!(hub.publications(), Err(pool::Error::Busy)));
    assert!(matches!(
        hub.start_pool(Default::default()),
        Err(pool::Error::Busy)
    ));
    let a = admission(Mode::Fork, 7, false);
    owner.client().admit(a.clone()).unwrap();
    let bytes = hub.retained_publication_bytes();
    let retained = publications.acknowledge().unwrap();
    assert_eq!(
        owner.client().admit(admission(Mode::Length, 7, false)),
        Err(pool::Error::ResourceLimit)
    );
    assert_eq!(owner.client().inspect(VotePool::len).unwrap(), 1);
    assert_eq!(hub.retained_publication_bytes(), bytes);
    assert_eq!(hub.new_epoch(7.into()), Err(pool::Error::ResourceLimit));
    assert_eq!(
        owner.client().inspect(|p| p.epoch().clone()).unwrap(),
        0.into()
    );
    let input = inbox.acknowledge().unwrap();
    assert_eq!(inbox.retained_bytes(), bytes);
    drop(retained);
    assert_eq!(hub.retained_publication_bytes(), bytes);
    drop(input);
    assert_eq!(inbox.retained_bytes(), 0);
    assert_eq!(hub.retained_publication_bytes(), 0);
    assert!(matches!(
        owner.client().admit(a.clone()).unwrap(),
        PushOutcome::Accepted {
            inserted: false,
            ..
        }
    ));
    drop(publications);
    drop(owner);
    let replacement = hub.start_pool(Default::default()).unwrap();
    let mut publications = hub.publications().unwrap();
    assert_eq!(
        publications.acknowledge().unwrap().signed().encode(),
        a.signed.encode()
    );
    assert!(replacement.client().inspect(VotePool::is_empty).unwrap());
    let retired = inbox.retire();
    assert_eq!(retired.pending.len(), 1);
    assert!(hub.retained_publication_bytes() > 0);
    drop(retired);
    assert_eq!(hub.retained_publication_bytes(), 0);
}
#[test]
fn independent_batch_failure_keeps_pool_publications_and_subscriptions_unchanged() {
    let (hub, owner) = hub(Default::default());
    let inbox = hub.subscribe(Default::default()).unwrap();
    let publications = hub.publications().unwrap();
    let good = admission(Mode::Fork, 7, false);
    let mut invalid = admission(Mode::Length, 7, false);
    invalid.consensus = ConsensusType::Pow;
    assert_eq!(
        owner.client().admit_batch(vec![good.clone(), invalid]),
        Err(pool::Error::Admission(hc_votes::Error::InvalidAtProtocol))
    );
    assert!(owner.client().inspect(VotePool::is_empty).unwrap());
    assert!(publications.peek().is_none());
    assert!(inbox.peek().is_none());
    assert_eq!(hub.retained_publication_bytes(), 0);
    assert_eq!(inbox.retained_bytes(), 0);
    let old = owner.client();
    drop(owner);
    let _replacement = hub.start_pool(Default::default()).unwrap();
    assert_eq!(old.admit(good), Err(pool::Error::Closed));
    // Epoch reaches EOE while the pool itself is absent, with no replay later.
    drop(_replacement);
    hub.new_epoch(9.into()).unwrap();
    assert!(
        matches!(inbox.peek().unwrap().event(),pool::Event::NewEpoch(e) if e==&Unsigned::from(9))
    );
    let replacement = hub.start_pool(Default::default()).unwrap();
    assert_eq!(
        replacement.client().inspect(|p| p.epoch().clone()).unwrap(),
        0.into()
    );
}
#[test]
fn generated_vote_batch_rolls_back_under_pressure_and_retries_without_cache_loss() {
    let (hub, owner) = hub(pool::Config {
        maximum_publications: 2,
        ..Default::default()
    });
    let mut publications = hub.publications().unwrap();
    let mut c = coordinator(&hub, &[2, 3]);
    let cache = ae_storage::pool::worker::GossipCache::new(200, 100_000).unwrap();
    c.set_live_gossip_cache(Some(Arc::new(cache.clone())));
    let mut inbox = c.subscribe(Default::default()).unwrap().unwrap();
    c.negotiate(negotiation(7)).unwrap();
    run(&mut c);
    owner
        .client()
        .admit(admission(Mode::Fork, 7, false))
        .unwrap();
    c.receive(&inbox.peek().unwrap()).unwrap();
    inbox.acknowledge();
    c.step(&otp_order::profile(&COORDINATOR["profile"]))
        .unwrap(); // length ignores fork
    let before = machines(&c);
    assert_eq!(
        c.step(&otp_order::profile(&COORDINATOR["profile"])),
        Err(api::Error::Host(local_host::Error::PoolCommit(
            pool::Error::ResourceLimit
        )))
    );
    assert_eq!(machines(&c), before);
    assert_eq!(owner.client().inspect(VotePool::len).unwrap(), 1);
    assert_eq!(cache.stats().unwrap().miss, 0.into());
    publications.acknowledge();
    c.step(&otp_order::profile(&COORDINATOR["profile"]))
        .unwrap();
    assert_eq!(owner.client().inspect(VotePool::len).unwrap(), 3);
    assert_eq!(cache.stats().unwrap().miss, 2.into());
    assert!(publications.acknowledge().is_some());
    assert!(publications.acknowledge().is_some());
    assert!(publications.peek().is_none());
}
struct PausedSource {
    calls: AtomicUsize,
    entered: mpsc::SyncSender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}
impl local_host::SelectedSource<ae_trie::MemoryStore, ae_trie::MemoryStore> for PausedSource {
    fn capture(&self) -> Result<SelectedTip, local_host::SelectedError> {
        if self.calls.fetch_add(1, Ordering::AcqRel) == 1 {
            self.entered.send(()).unwrap();
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(3))
                .unwrap();
        }
        Ok(tip(Some(20)))
    }
}
#[test]
fn staged_callbacks_allow_concurrent_admission_and_never_rebind_a_retired_pool() {
    for replace in [false, true] {
        let (hub, owner) = hub(Default::default());
        let (entered, wait) = mpsc::sync_channel(1);
        let (resume, release) = mpsc::sync_channel(1);
        let h = host(&[2, 3], Default::default())
            .with_independent_pool(hub.clone())
            .unwrap()
            .with_selected_source(Arc::new(PausedSource {
                calls: AtomicUsize::new(0),
                entered,
                resume: Mutex::new(release),
            }));
        let mut c = Coordinator::new(h, 1_000_000_000.into(), Default::default());
        let mut inbox = c.subscribe(Default::default()).unwrap().unwrap();
        c.negotiate(negotiation(7)).unwrap();
        run(&mut c);
        owner
            .client()
            .admit(admission(Mode::Fork, 7, false))
            .unwrap();
        c.receive(&inbox.peek().unwrap()).unwrap();
        inbox.acknowledge();
        c.step(&otp_order::profile(&COORDINATOR["profile"]))
            .unwrap();
        let worker = std::thread::spawn(move || {
            let result = c.step(&otp_order::profile(&COORDINATOR["profile"]));
            (c, result)
        });
        wait.recv_timeout(Duration::from_secs(2)).unwrap();
        // This read and admission finish while the EOE callback is paused.
        assert_eq!(owner.client().inspect(VotePool::len).unwrap(), 1);
        let owner = if replace {
            drop(owner);
            hub.start_pool(Default::default()).unwrap()
        } else {
            owner
        };
        owner
            .client()
            .admit(admission(Mode::Length, 999, false))
            .unwrap();
        resume.send(()).unwrap();
        let (c, result) = worker.join().unwrap();
        if replace {
            assert_eq!(
                result,
                Err(api::Error::Host(local_host::Error::PoolCommit(
                    pool::Error::Closed
                )))
            );
            assert_eq!(owner.client().inspect(VotePool::len).unwrap(), 1);
            assert_eq!(c.queued_events(), 1);
        } else {
            assert_eq!(result, Ok(true));
            assert_eq!(owner.client().inspect(VotePool::len).unwrap(), 4);
        }
    }
}
#[test]
fn runtime_pool_queries_survive_pool_replacement_and_retirement_preserves_pending_inputs() {
    let (hub, owner) = hub(Default::default());
    let mut publications = hub.publications().unwrap();
    let rt = runtime::Runtime::spawn(
        coordinator(&hub, &[]),
        otp_order::profile(&COORDINATOR["profile"]),
        runtime::Config {
            output_capacity: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let client = rt.client();
    let query = || {
        runtime::Command::Votes(runtime::VoteQuery {
            epoch: 7.into(),
            kind: None,
            maximum: 10,
        })
    };
    owner
        .client()
        .admit(admission(Mode::Fork, 7, false))
        .unwrap();
    let runtime::Applied::Votes(reply) = addressed(&client, query())
        .wait(Duration::from_secs(2))
        .unwrap()
        .unwrap()
    else {
        panic!("vote reply")
    };
    assert_eq!(reply.entries(), 1);
    drop(owner);
    assert!(matches!(
        addressed(&client, query())
            .wait(Duration::from_secs(2))
            .unwrap(),
        Err(api::Error::Host(local_host::Error::IndependentPool(
            pool::Error::Closed
        )))
    ));
    assert!(!client.is_closed());
    let replacement = hub.start_pool(Default::default()).unwrap();
    let runtime::Applied::Votes(reply) = addressed(&client, query())
        .wait(Duration::from_secs(2))
        .unwrap()
        .unwrap()
    else {
        panic!("vote reply")
    };
    assert_eq!(reply.entries(), 0);
    assert!(rt.retire().result.is_ok());
    // Preexisting service outputs fill the channel before command/input
    // processing can begin, avoiding a timing race with the new admission.
    let mut h = host(&[], Default::default());
    add_parent(&mut h);
    let mut c = Coordinator::new(
        h.with_independent_pool(hub.clone()).unwrap(),
        1_000_000_000.into(),
        Default::default(),
    );
    for _ in 0..4 {
        c.entropy_hash(&9.into()).unwrap();
    }
    let rt = runtime::Runtime::spawn(
        c,
        otp_order::profile(&COORDINATOR["profile"]),
        runtime::Config {
            output_capacity: 1,
            ..Default::default()
        },
    )
    .unwrap();
    rt.recv_timeout(Duration::from_secs(2)).unwrap();
    let mut barrier = addressed(&rt.client(), runtime::Command::Retry);
    replacement
        .client()
        .admit(admission(Mode::Length, 7, false))
        .unwrap();
    let retired = rt.retire();
    assert!(retired.result.is_ok());
    assert!(!retired.inputs.as_ref().unwrap().pending.is_empty());
    assert_eq!(barrier.try_reply(), Err(runtime::CallError::Closed));
    assert_eq!(replacement.client().inspect(VotePool::len).unwrap(), 1);
    assert!(publications.acknowledge().is_some());
    assert!(publications.acknowledge().is_some());
    let next = runtime::Runtime::spawn(
        coordinator(&hub, &[]),
        otp_order::profile(&COORDINATOR["profile"]),
        Default::default(),
    )
    .unwrap();
    assert!(next.retire().inputs.unwrap().pending.is_empty());
}

#[test]
fn generated_round_and_signed_finalization_match_native_with_independent_pool() {
    let reference = unpack(include_str!("../../fixtures/erlang_hc_live_gossip.json"));
    let (hub, owner) = hub(Default::default());
    let mut h = host(&[1, 2, 3], Default::default());
    add_parent(&mut h);
    let mut c = Coordinator::new(
        h.with_independent_pool(hub.clone()).unwrap(),
        1000.into(),
        Default::default(),
    );
    let cache = ae_storage::pool::worker::GossipCache::new(200, 100_000).unwrap();
    c.set_live_gossip_cache(Some(Arc::new(cache.clone())));
    let mut inbox = c.subscribe(Default::default()).unwrap().unwrap();
    let mut publications = hub.publications().unwrap();
    c.negotiate(negotiation(7)).unwrap();
    pump(&mut c, &mut inbox);
    let mut events = vec![];
    while let Some(p) = publications.acknowledge() {
        events.push(p.signed().encode());
    }
    events.sort();
    let generated = &reference["local"]["steps"][0];
    assert_eq!(list(events.iter().map(bin).collect()), generated["events"]);
    assert_eq!(
        owner.client().inspect(VotePool::len).unwrap(),
        generated["size"].as_u64().unwrap() as usize
    );
    assert_eq!(
        cache.stats().unwrap().miss.to_u64().unwrap(),
        generated["stats"]["miss"].as_u64().unwrap()
    );
    c.request_finalize(77, Arc::new(trees(5)), c.now()).unwrap();
    pump(&mut c, &mut inbox);
    let mut replies = None;
    while let Some(output) = c.pop_output() {
        match output {
            ServiceOutput::Finalized {
                request: 77,
                replies: r,
            } => replies = Some(r),
            ServiceOutput::Publication(Output::ParentBlock { .. }) => (),
            other => panic!("unexpected output {other:?}"),
        }
    }
    assert_eq!(pair_json(&replies.unwrap()), reference["local"]["result"]);
}
#[test]
fn runtime_preserves_failed_input_until_explicit_retry_and_wakes_without_commands() {
    let (hub, owner) = hub(Default::default());
    let mut c = coordinator(&hub, &[]);
    c.set_config(api::Config {
        maximum_queued_events: 1,
        ..Default::default()
    });
    let rt = runtime::Runtime::spawn(
        c,
        otp_order::profile(&COORDINATOR["profile"]),
        Default::default(),
    )
    .unwrap();
    owner
        .client()
        .admit(admission(Mode::Fork, 7, true))
        .unwrap();
    assert_eq!(
        rt.recv_timeout(Duration::from_secs(2)),
        Ok(runtime::Output::WorkStopped(api::Error::ResourceLimit))
    );
    assert_eq!(owner.client().inspect(VotePool::len).unwrap(), 1);
    addressed(
        &rt.client(),
        runtime::Command::Configure(Default::default()),
    )
    .wait(Duration::from_secs(2))
    .unwrap()
    .unwrap();
    assert!(matches!(
        rt.recv_timeout(Duration::from_secs(2)),
        Ok(runtime::Output::Service(ServiceOutput::Restarted {
            mode: Mode::Fork,
            ..
        }))
    ));
    let retired = rt.retire();
    assert!(retired.inputs.unwrap().pending.is_empty());
    assert_eq!(retired.state.unwrap().generations, [1, 0]);
}

#[test]
fn standalone_machine_commits_state_only_after_independent_publication_capacity() {
    let (hub, owner) = hub(pool::Config {
        maximum_publications: 1,
        ..Default::default()
    });
    let mut publications = hub.publications().unwrap();
    owner
        .client()
        .admit(admission(Mode::Length, 999, false))
        .unwrap();
    let mut h = host(&[1], Default::default())
        .with_independent_pool(hub.clone())
        .unwrap();
    let mut machine = Machine::new(Mode::Fork, [pubkey(1)].into(), 1000.into());
    let before = machine.clone();
    assert!(matches!(
        machine.step(
            Event::Negotiate(negotiation(7)),
            &mut h,
            &otp_order::profile(&COORDINATOR["profile"]),
            Limits::default()
        ),
        Err(StepError::Host(local_host::Error::PoolCommit(
            pool::Error::ResourceLimit
        )))
    ));
    assert_eq!(machine, before);
    assert_eq!(owner.client().inspect(VotePool::len).unwrap(), 1);
    publications.acknowledge();
    machine
        .step(
            Event::Negotiate(negotiation(7)),
            &mut h,
            &otp_order::profile(&COORDINATOR["profile"]),
            Limits::default(),
        )
        .unwrap();
    assert_eq!(machine.state(), State::Proposal);
    assert_eq!(owner.client().inspect(VotePool::len).unwrap(), 2);
    assert!(h.outbox().is_empty());
    assert!(publications.acknowledge().is_some());
    assert!(publications.peek().is_none());
}
