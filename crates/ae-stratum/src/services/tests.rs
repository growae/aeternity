use super::*;
use crate::{
    nonce::Kind,
    session::{Action, Effect, Event, Phase, Registration, Session, Targets},
    share::User,
    target,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
const ACCOUNT: &str = "ak_zrHosArjUGvPYvg9WoT5KNEoJmoJEsXfaDu7TQLHtod5mJS7Y";
struct Input {
    next: u64,
    clocks: Arc<AtomicUsize>,
    fail: Arc<AtomicUsize>,
}
impl Sources for Input {
    fn now(&mut self) -> Result<BigInt, Fault> {
        match self.fail.load(Ordering::Acquire) {
            2 => return Err(Fault::Clock),
            3 => panic!("injected clock failure"),
            _ => (),
        }
        Ok(BigInt::from(
            1000 + self.clocks.fetch_add(1, Ordering::AcqRel),
        ))
    }
    fn draw(&mut self, max: u64) -> Result<u64, Fault> {
        if self.fail.load(Ordering::Acquire) == 1 {
            return Err(Fault::Entropy);
        }
        let n = self.next & max;
        self.next += 1;
        Ok(n)
    }
}
fn owner(config: Config) -> (Owner, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let clocks = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicUsize::new(0));
    (
        Owner::with_sources(
            config,
            Input {
                next: 541580363,
                clocks: clocks.clone(),
                fail: fail.clone(),
            },
        )
        .unwrap(),
        clocks,
        fail,
    )
}
fn command(c: &Connection, id: u64, operation: Operation) -> Reply {
    match c.execute(session::Command { id, operation }).unwrap() {
        Event::Complete { id: n, reply } => {
            assert_eq!(n, id);
            reply
        }
        _ => panic!("completion"),
    }
}
fn register(c: &Connection, id: u64, a: &str, w: &str) -> Reply {
    command(
        c,
        id,
        Operation::Register {
            user: User {
                account: a.into(),
                worker: w.into(),
            },
        },
    )
}
fn allocate(c: &Connection, id: u64) -> Part {
    match command(c, id, Operation::AllocateNonce { bytes: 4 }) {
        Reply::Nonce(Some(n)) => n,
        _ => panic!("nonce"),
    }
}
#[test]
fn handles_bind_commands_leases_and_registry_cleanup_to_one_owner_incarnation() {
    let (owner, _, _) = owner(Default::default());
    let client = owner.client();
    let a = client.connect().unwrap();
    let b = client.connect().unwrap();
    let an = allocate(&a, 1);
    let bn = allocate(&b, 1);
    assert_ne!(an, bn);
    assert_eq!(
        b.execute(session::Command {
            id: 2,
            operation: Operation::ReleaseNonce { nonce: an }
        }),
        Err(Error::Lease)
    );
    assert_eq!(
        b.execute(session::Command {
            id: 1,
            operation: Operation::Membership {
                account: "a".into()
            }
        }),
        Err(Error::Stale)
    );
    assert_eq!(
        a.execute(session::Command {
            id: 2,
            operation: Operation::AllocateNonce { bytes: 4 }
        }),
        Err(Error::Lease)
    );
    assert_eq!(
        register(&a, 2, "a", "one"),
        Reply::Registered(Registration::Added)
    );
    assert_eq!(
        register(&b, 2, "a", "two"),
        Reply::Registered(Registration::AlreadyPresent)
    );
    assert_eq!(client.membership(b"a", &a).unwrap(), Membership::Both);
    assert_eq!(
        client.membership(b"a", &b).unwrap(),
        Membership::AccountOnly
    );
    b.close();
    assert_eq!(client.info().nonces, 1);
    assert!(client.member_account(b"a").unwrap());
    let clone = a.clone();
    drop(a);
    assert_eq!(client.info().connections, 1);
    drop(clone);
    assert_eq!(client.info().nonces, 0);
    assert_eq!(client.info().accounts, 0);
    let old = client.connect().unwrap();
    assert!(old.id() > b.id());
    let info = owner.shutdown();
    assert!(info.closed && old.is_closed());
    assert_eq!(info.connections, 0);
    let fresh = Owner::new(Default::default()).unwrap();
    assert!(matches!(
        fresh.client().find_connection(&old),
        Err(Error::Stale)
    ));
    assert!(matches!(client.connect(), Err(Error::Closed)));
    assert_eq!(
        old.execute(session::Command {
            id: 1,
            operation: Operation::DeleteConnection
        }),
        Err(Error::Closed)
    );
}
#[test]
fn current_worker_quotas_and_duplicate_precedence_read_time_only_for_insertions() {
    let (owner, clocks, _) = owner(Config {
        max_workers: 0,
        ..Default::default()
    });
    let client = owner.client();
    let c = client.connect().unwrap();
    assert_eq!(
        register(&c, 1, "a", "one"),
        Reply::Registered(Registration::Added)
    );
    assert_eq!(
        register(&c, 2, "b", "one"),
        Reply::Registered(Registration::AlreadyPresent)
    );
    for (id, a, w, expected) in [
        (3, "a", "one", WorkerRegistration::AlreadyPresent),
        (4, "a", "two", WorkerRegistration::Exhausted),
        (5, "b", "one", WorkerRegistration::AccountNotFound),
    ] {
        assert_eq!(
            command(
                &c,
                id,
                Operation::AddWorker {
                    user: User {
                        account: a.into(),
                        worker: w.into()
                    }
                }
            ),
            Reply::Worker(expected)
        );
    }
    assert_eq!(clocks.load(Ordering::Acquire), 1);
    assert!(client.member_connection(&c).unwrap());
    assert_eq!(*client.registered_connections().unwrap(), vec![c.id()]);
    client.set_max_workers(2).unwrap();
    assert_eq!(
        command(
            &c,
            6,
            Operation::AddWorker {
                user: User {
                    account: "a".into(),
                    worker: "two".into()
                }
            }
        ),
        Reply::Worker(WorkerRegistration::Added)
    );
    let view = client.find_connection(&c).unwrap().unwrap();
    assert_eq!(view.entry.workers[b"one".as_slice()], 1000.into());
    assert_eq!(view.entry.workers[b"two".as_slice()], 1001.into());
    assert_eq!(clocks.load(Ordering::Acquire), 2);
    assert!(client.delete_account(b"a").unwrap());
    assert!(!client.delete_account(b"a").unwrap());
    assert_eq!(command(&c, 7, Operation::DeleteConnection), Reply::Deleted);
    assert_eq!(client.info().registry_bytes, 0);
    assert!(!client.member_connection(&c).unwrap());
    assert!(client.registered_connections().unwrap().is_empty());
}
fn config() -> session::Config {
    session::Config {
        port: 9999,
        extra_nonce_bytes: 4,
        message_timeout_ms: 3600000,
        skip_blocks: 2,
        edge_bits: 29,
        max_jobs: 20,
        targets: Targets {
            initial: target::maximum(),
            maximum: target::maximum(),
            difference_threshold: 5.0,
            desired_solve_time: 30000.into(),
            maximum_solve_time: 60000.into(),
        },
    }
}
fn request(method: &str, params: Value) -> Event {
    Event::Data(
        serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .unwrap(),
    )
}
fn auth(account: &str, worker: &str) -> Event {
    request(
        "mining.authorize",
        json!([format!("{account}.{worker}"), null]),
    )
}
fn event(action: &str) -> Event {
    match action {
        "init" => Event::Init,
        "close" => Event::Close,
        "timeout" => Event::Timeout,
        "configure" => request("mining.configure", json!([])),
        "subscribe" => request("mining.subscribe", json!(["miner/1", null, null, 9999])),
        "bad_port" => request("mining.subscribe", json!(["miner/1", null, null, 9998])),
        "authorize" => auth(ACCOUNT, "one"),
        "worker_two" => auth(ACCOUNT, "two"),
        "worker_three" => auth(ACCOUNT, "three"),
        "different_account" => auth(
            "ak_11111111111111111111111111111111111111111111111111",
            "one",
        ),
        "set_target" => Event::Chain(Chain::SetTarget),
        "submit_missing" => request(
            "mining.submit",
            json!([
                format!("{ACCOUNT}.one"),
                "0123456789abcdef",
                "00000000",
                (0..42).collect::<Vec<_>>()
            ]),
        ),
        _ => panic!("action {action}"),
    }
}
#[test]
fn native_session_packets_run_against_owned_registry_and_nonce_services() {
    let native: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_stratum_sessions.json"
    ))
    .unwrap();
    let mut steps = 0;
    for scenario in native.as_array().unwrap().iter().filter(|s| {
        [
            "configure_flow",
            "direct_subscribe",
            "workers",
            "bad_port",
            "account_already_used",
            "account_removed",
            "repeated_requests",
            "before_subscribe",
            "subscribed_submit",
        ]
        .contains(&s["name"].as_str().unwrap())
    }) {
        let (owner, _, _) = owner(Config {
            max_workers: 2,
            ..Default::default()
        });
        let client = owner.client();
        let c = client.connect().unwrap();
        let other = client.connect().unwrap();
        let mut session = Session::new();
        let cfg = config();
        let now = BigInt::from(1000);
        let ctx = session::Context {
            config: &cfg,
            now: &now,
        };
        let mut timers = BTreeSet::new();
        for row in scenario["steps"].as_array().unwrap() {
            steps += 1;
            let action = row["action"].as_str().unwrap();
            match action {
                "other_owner" => {
                    register(&other, 1, ACCOUNT, "outside");
                    continue;
                }
                "delete_account" => {
                    assert!(client.delete_account(ACCOUNT.as_bytes()).unwrap());
                    continue;
                }
                _ => (),
            }
            let mut step = session.handle(event(action), &ctx).unwrap();
            loop {
                for effect in step.effects {
                    match effect {
                        Effect::ScheduleTimer { timer, .. } => {
                            timers.insert(timer.id);
                        }
                        Effect::CancelTimer { id } => {
                            timers.remove(&id);
                        }
                        Effect::Enqueue(_) => (),
                        Effect::Pool(_) => panic!("missing job has no pool effect"),
                    }
                }
                match step.action {
                    Action::Command(cmd) => {
                        // Apply cancellation effects before executing the native synchronous call.
                        if matches!(
                            cmd.operation,
                            Operation::AllocateNonce { .. } | Operation::Register { .. }
                        ) {
                            assert!(timers.is_empty());
                        }
                        step = session.handle(c.execute(cmd).unwrap(), &ctx).unwrap();
                    }
                    Action::Send(wire) => {
                        assert_eq!(
                            hex::encode(wire),
                            row["sent"].as_str().unwrap(),
                            "{}: {action}",
                            scenario["name"]
                        );
                        break;
                    }
                    Action::None | Action::Stop(_) => {
                        assert!(row["sent"].is_null());
                        break;
                    }
                }
            }
        }
        assert_eq!(session.phase(), Phase::Disconnected);
        assert!(timers.is_empty());
        assert_eq!(client.info().nonces, 0);
        assert_eq!(
            client.info().accounts,
            scenario["registry_after_close"].as_u64().unwrap() as usize
        );
        drop(other);
        assert_eq!(client.info().accounts, 0);
    }
    assert_eq!(steps, 62);
}
#[test]
fn broadcasts_are_per_connection_and_slow_consumers_close_without_leaking_leases() {
    let (owner, _, _) = owner(Config {
        notifications_per_connection: 1,
        ..Default::default()
    });
    let client = owner.client();
    let a = client.connect().unwrap();
    let b = client.connect().unwrap();
    let idle = client.connect().unwrap();
    allocate(&a, 1);
    allocate(&b, 1);
    register(&a, 2, "a", "one");
    register(&b, 2, "b", "one");
    command(
        &a,
        3,
        Operation::AddWorker {
            user: User {
                account: "a".into(),
                worker: "two".into(),
            },
        },
    );
    assert_eq!(
        client.broadcast(Chain::SetTarget).unwrap(),
        Broadcast {
            queued: 2,
            closed: 0
        }
    );
    let held = a.next_notification().unwrap().unwrap();
    assert_eq!(*held, Chain::SetTarget);
    assert!(idle.next_notification().unwrap().is_none());
    assert_eq!(
        client.broadcast(Chain::SetTarget).unwrap(),
        Broadcast {
            queued: 1,
            closed: 1
        }
    );
    assert!(b.is_closed());
    assert_eq!(client.info().nonces, 1);
    assert!(!client.member_account(b"b").unwrap());
    assert_eq!(
        client.broadcast(Chain::SetTarget).unwrap(),
        Broadcast {
            queued: 0,
            closed: 1
        }
    );
    assert!(a.is_closed());
    assert_eq!(client.info().accounts, 0);
    assert_eq!(client.info().nonces, 0);
    assert_eq!(client.info().retained_reply_bytes, 512);
    drop(held);
    assert_eq!(client.info().retained_reply_bytes, 0);
}
#[test]
fn retained_views_and_notifications_apply_backpressure_until_the_last_clone_is_dropped() {
    let (owner, _, _) = owner(Config {
        retained_reply_bytes: 1024,
        ..Default::default()
    });
    let client = owner.client();
    let c = client.connect().unwrap();
    register(&c, 1, "a", "one");
    let first = client.find_account(b"a").unwrap().unwrap();
    let held = first.clone();
    drop(first);
    let second = client.find_account(b"a").unwrap().unwrap();
    assert!(matches!(client.find_account(b"a"), Err(Error::Busy)));
    drop(second);
    assert_eq!(client.broadcast(Chain::SetTarget).unwrap().queued, 1);
    assert_eq!(client.broadcast(Chain::SetTarget), Err(Error::Busy));
    let notice = c.next_notification().unwrap().unwrap();
    let info = owner.shutdown();
    assert!(info.closed && info.retained_reply_bytes > 0);
    assert_eq!(held.account, b"a");
    assert_eq!(*notice, Chain::SetTarget);
    drop(held);
    drop(notice);
    assert_eq!(client.info().retained_reply_bytes, 0);
}
#[test]
fn clock_entropy_and_provider_panics_close_the_shared_generation() {
    for mode in [1, 2, 3] {
        let (owner, _, fail) = owner(Default::default());
        let client = owner.client();
        let a = client.connect().unwrap();
        let b = client.connect().unwrap();
        allocate(&a, 1);
        register(&a, 2, "a", "one");
        fail.store(mode, Ordering::Release);
        let op = if mode == 1 {
            Operation::AllocateNonce { bytes: 4 }
        } else {
            Operation::Register {
                user: User {
                    account: "b".into(),
                    worker: "one".into(),
                },
            }
        };
        let expected = match mode {
            1 => Fault::Entropy,
            2 => Fault::Clock,
            _ => Fault::Panicked,
        };
        assert_eq!(
            b.execute(session::Command {
                id: 1,
                operation: op
            }),
            Err(Error::Fault(expected.clone()))
        );
        assert!(a.is_closed() && b.is_closed());
        let info = client.info();
        assert!(info.closed);
        assert_eq!(info.failure, Some(expected));
        assert_eq!(
            (
                info.connections,
                info.accounts,
                info.nonces,
                info.registry_bytes
            ),
            (0, 0, 0, 0)
        );
        assert!(owner.shutdown().closed);
    }
}
struct HeldClock {
    entered: mpsc::SyncSender<()>,
    release: mpsc::Receiver<()>,
}
impl Sources for HeldClock {
    fn now(&mut self) -> Result<BigInt, Fault> {
        self.entered.send(()).unwrap();
        self.release.recv_timeout(Duration::from_secs(5)).unwrap();
        Ok(0.into())
    }
    fn draw(&mut self, _: u64) -> Result<u64, Fault> {
        Ok(0)
    }
}
#[test]
fn contention_rejects_without_consuming_commands_and_shutdown_waits_for_admitted_work() {
    let (entered, entry) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel(1);
    let owner = Owner::with_sources(
        Default::default(),
        HeldClock {
            entered,
            release: released,
        },
    )
    .unwrap();
    let client = owner.client();
    let a = client.connect().unwrap();
    let b = client.connect().unwrap();
    let pending = a.clone();
    let call = thread::spawn(move || register(&pending, 1, "a", "one"));
    entry.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        b.execute(session::Command {
            id: 1,
            operation: Operation::DeleteConnection
        }),
        Err(Error::Busy)
    );
    let closing = thread::spawn(move || owner.shutdown());
    let end = Instant::now() + Duration::from_secs(5);
    while !a.is_closed() {
        assert!(Instant::now() < end);
        thread::yield_now();
    }
    assert!(!closing.is_finished());
    release.send(()).unwrap();
    assert_eq!(call.join().unwrap(), Reply::Registered(Registration::Added));
    let info = closing.join().unwrap();
    assert!(info.closed);
    assert_eq!((info.connections, info.accounts, info.nonces), (0, 0, 0));
    assert!(b.is_closed());
}
#[test]
fn system_entropy_and_wall_clock_services_enforce_capacity_and_reclaim_abandoned_connections() {
    let owner = Owner::new(Config {
        connections: 2,
        registry: registry::Limits {
            field_bytes: 64,
            ..Default::default()
        },
        ..Default::default()
    })
    .unwrap();
    let client = owner.client();
    let a = client.connect().unwrap();
    let b = client.connect().unwrap();
    assert!(matches!(client.connect(), Err(Error::Limit)));
    let an = allocate(&a, 1);
    let bn = allocate(&b, 1);
    assert_ne!(an.value(), bn.value());
    register(&a, 2, "a", "one");
    let view = client.find_account(b"a").unwrap().unwrap();
    assert!(view.entry.workers[b"one".as_slice()] > 0.into());
    assert_eq!(
        a.execute(session::Command {
            id: 3,
            operation: Operation::ReleaseNonce {
                nonce: Part::new(Kind::Miner, an.value(), an.bytes()).unwrap()
            }
        }),
        Err(Error::Lease)
    );
    let too_long = Operation::Membership {
        account: "x".repeat(65),
    };
    assert_eq!(
        b.execute(session::Command {
            id: 2,
            operation: too_long
        }),
        Err(Error::State(crate::Error::Limit("registry field")))
    );
    assert!(b.is_closed());
    assert_eq!(client.info().nonces, 1);
    let next = client.connect().unwrap();
    assert!(next.id() > b.id());
    drop(a);
    assert_eq!(client.info().nonces, 0);
    assert_eq!(client.info().accounts, 0);
    drop(next);
    assert_eq!(client.info().connections, 0);
}

#[test]
fn owned_nonce_exhaustion_preserves_the_connection_and_released_values_can_be_reused() {
    let (owner, _, _) = owner(Default::default());
    let client = owner.client();
    let mut connections = Vec::new();
    for _ in 0..256 {
        let c = client.connect().unwrap();
        assert!(matches!(
            command(&c, 1, Operation::AllocateNonce { bytes: 1 }),
            Reply::Nonce(Some(_))
        ));
        connections.push(c);
    }
    let waiting = client.connect().unwrap();
    assert_eq!(
        command(&waiting, 1, Operation::AllocateNonce { bytes: 1 }),
        Reply::Nonce(None)
    );
    assert!(!waiting.is_closed());
    assert_eq!(client.info().nonces, 256);
    // After fifty collisions the next candidate is the value held by index 50.
    let released = connections.remove(50);
    let value = released.0.shared.lock().connections[&released.id()]
        .nonce
        .unwrap();
    released.close();
    assert_eq!(
        command(&waiting, 2, Operation::AllocateNonce { bytes: 1 }),
        Reply::Nonce(Some(value))
    );
    assert_eq!(client.info().nonces, 256);
    drop(connections);
    drop(waiting);
    assert_eq!(client.info().nonces, 0);
}

fn drive(
    session: &mut Session,
    connection: &Connection,
    event: Event,
    config: &session::Config,
    time: u64,
) -> session::Step {
    let now = BigInt::from(time);
    let context = session::Context { config, now: &now };
    let mut step = session.handle(event, &context).unwrap();
    let mut effects = Vec::new();
    loop {
        effects.extend(step.effects);
        match step.action {
            Action::Command(command) => {
                step = session
                    .handle(connection.execute(command).unwrap(), &context)
                    .unwrap()
            }
            action => return session::Step { action, effects },
        }
    }
}
#[test]
fn saved_mainnet_proofs_and_native_numeric_failures_use_owned_membership_and_cleanup() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_stratum_numeric_shares.json"
    ))
    .unwrap();
    let mut count = 0;
    for row in corpus["rows"].as_array().unwrap().iter().filter(|r| {
        r["variant"] == "normal" && (r["mode"] == "integer" || r["mode"] == "first_float")
    }) {
        count += 1;
        let (extra, miner) = crate::nonce::Nonce(row["nonce"].as_u64().unwrap())
            .split(Kind::Extra, 4)
            .unwrap();
        let owner = Owner::with_sources(
            Default::default(),
            Input {
                next: extra.value(),
                clocks: Arc::new(AtomicUsize::new(0)),
                fail: Arc::new(AtomicUsize::new(0)),
            },
        )
        .unwrap();
        let client = owner.client();
        let connection = client.connect().unwrap();
        let mut s = Session::new();
        let mut cfg = config();
        cfg.targets.initial = row["target"].as_str().unwrap().parse().unwrap();
        cfg.targets.maximum = cfg.targets.initial.clone();
        for name in ["init", "subscribe", "authorize", "set_target"] {
            drive(&mut s, &connection, event(name), &cfg, 1000);
        }
        let work = crate::job::Work::new(
            row["hash"].as_str().unwrap().into(),
            row["version"].as_u64().unwrap() as u32,
            cfg.targets.initial.clone(),
        )
        .unwrap();
        let id = work.id().unwrap();
        drive(
            &mut s,
            &connection,
            Event::Chain(Chain::Block(work)),
            &cfg,
            1000,
        );
        let proof: Vec<Value> = row["pow"]["list"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| {
                if let Some(integer) = n.get("integer") {
                    serde_json::from_str(integer.as_str().unwrap()).unwrap()
                } else {
                    json!(f64::from_bits(
                        u64::from_str_radix(n["float"].as_str().unwrap(), 16).unwrap()
                    ))
                }
            })
            .collect();
        let packet = json!({"jsonrpc":"2.0","id":7,"method":"mining.submit","params":[format!("{ACCOUNT}.one"),id,miner.to_hex(),proof]});
        let out = drive(
            &mut s,
            &connection,
            Event::Data(crate::wire::json::encode(&packet, Default::default()).unwrap()),
            &cfg,
            61000,
        );
        if row["mode"] == "integer" {
            let Action::Send(wire) = out.action else {
                panic!("accepted block reply")
            };
            assert_eq!(
                serde_json::from_slice::<Value>(&wire).unwrap()["result"],
                true
            );
            assert!(matches!(
                out.effects.as_slice(),
                [
                    Effect::Pool(crate::share::Effect::SubmitShare { .. }),
                    Effect::Pool(crate::share::Effect::SubmitSolution { .. })
                ]
            ));
            assert_eq!(client.info().nonces, 1);
            drive(&mut s, &connection, Event::Close, &cfg, 61000);
        } else {
            assert_eq!(
                out.action,
                Action::Stop(session::StopReason::Work(crate::Error::ProofArithmetic))
            );
            assert!(out.effects.is_empty());
        }
        assert_eq!(s.phase(), Phase::Disconnected);
        assert_eq!((client.info().accounts, client.info().nonces), (0, 0));
    }
    assert_eq!(count, 12);
}
