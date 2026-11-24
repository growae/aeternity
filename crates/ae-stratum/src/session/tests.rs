use super::*;
use crate::{nonce::Nonce, wire::json as wire_json};
use std::collections::{BTreeMap, BTreeSet};
const ACCOUNT: &str = "ak_zrHosArjUGvPYvg9WoT5KNEoJmoJEsXfaDu7TQLHtod5mJS7Y";
fn native(t: &Value) -> Value {
    if let Some(s) = t.get("atom").and_then(Value::as_str) {
        return match s {
            "true" => json!(true),
            "false" => json!(false),
            "null" => Value::Null,
            _ => json!(s),
        };
    }
    if let Some(s) = t.get("binary").and_then(Value::as_str) {
        return json!(String::from_utf8(hex::decode(s).unwrap()).unwrap());
    }
    if let Some(s) = t.get("integer").and_then(Value::as_str) {
        return serde_json::from_str(s).unwrap();
    }
    if let Some(s) = t.get("float").and_then(Value::as_str) {
        return json!(f64::from_bits(u64::from_str_radix(s, 16).unwrap()));
    }
    if let Some(a) = t
        .get("list")
        .or_else(|| t.get("tuple"))
        .and_then(Value::as_array)
    {
        return Value::Array(a.iter().map(native).collect());
    }
    Value::Object(
        t["map"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| (native(&p[0]).as_str().unwrap().into(), native(&p[1])))
            .collect(),
    )
}
fn number(i: &BigInt) -> Value {
    serde_json::from_str(&i.to_string()).unwrap()
}
fn big(v: &Value) -> BigInt {
    v.to_string().parse().unwrap()
}
fn typed(v: Value) -> Value {
    match v {
        Value::Number(n) if n.as_str().contains(['.', 'e', 'E']) => {
            json!({"float_bits":format!("{:016x}",n.as_f64().unwrap().to_bits())})
        }
        Value::Array(a) => Value::Array(a.into_iter().map(typed).collect()),
        Value::Object(m) => Value::Object(m.into_iter().map(|(k, v)| (k, typed(v))).collect()),
        v => v,
    }
}
fn phase(p: Phase) -> &'static str {
    match p {
        Phase::Connected => "connected",
        Phase::Configured => "configured",
        Phase::Subscribed => "subscribed",
        Phase::Authorized => "authorized",
        Phase::Disconnected => "disconnected",
    }
}
fn config() -> Config {
    Config {
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
// Explicit test responses for the original registry and seeded nonce draw.
// This is not a production registry or a reproduction of Erlang's RNG.
#[derive(Default)]
struct World {
    users: BTreeMap<String, (u8, BTreeSet<String>)>,
    nonces: BTreeSet<u64>,
    exhausted: bool,
}
impl World {
    fn reply(&mut self, op: &Operation) -> Reply {
        match op {
            Operation::AllocateNonce { bytes } => {
                if self.exhausted {
                    return Reply::Nonce(None);
                }
                let n = Part::new(Kind::Extra, 541580363, *bytes).unwrap();
                assert!(self.nonces.insert(n.value()));
                Reply::Nonce(Some(n))
            }
            Operation::Membership { account } => Reply::Membership(match self.users.get(account) {
                Some((1, _)) => Membership::Both,
                Some(_) => Membership::AccountOnly,
                None => Membership::Neither,
            }),
            Operation::Register { user } => {
                if self.users.contains_key(&user.account)
                    || self.users.values().any(|(owner, _)| *owner == 1)
                {
                    Reply::Registered(Registration::AlreadyPresent)
                } else {
                    self.users
                        .insert(user.account.clone(), (1, [user.worker.clone()].into()));
                    Reply::Registered(Registration::Added)
                }
            }
            Operation::AddWorker { user } => {
                Reply::Worker(match self.users.get_mut(&user.account) {
                    None => WorkerRegistration::AccountNotFound,
                    Some((_, workers)) if workers.contains(&user.worker) => {
                        WorkerRegistration::AlreadyPresent
                    }
                    Some((_, workers)) if workers.len() >= 2 => WorkerRegistration::Exhausted,
                    Some((_, workers)) => {
                        workers.insert(user.worker.clone());
                        WorkerRegistration::Added
                    }
                })
            }
            Operation::ReleaseNonce { nonce } => {
                self.nonces.remove(&nonce.value());
                Reply::Released
            }
            Operation::DeleteConnection => {
                self.users.retain(|_, (owner, _)| *owner != 1);
                Reply::Deleted
            }
        }
    }
}
fn drive(
    s: &mut Session,
    event: Event,
    w: &mut World,
    c: &Config,
    now: i64,
) -> (Step, Vec<Operation>) {
    let now = BigInt::from(now);
    let ctx = Context {
        config: c,
        now: &now,
    };
    let mut step = s.handle(event, &ctx).unwrap();
    let mut effects = step.effects;
    let mut ops = Vec::new();
    while let Action::Command(command) = step.action {
        let reply = w.reply(&command.operation);
        ops.push(command.operation);
        step = s
            .handle(
                Event::Complete {
                    id: command.id,
                    reply,
                },
                &ctx,
            )
            .unwrap();
        effects.extend(step.effects);
    }
    (
        Step {
            action: step.action,
            effects,
        },
        ops,
    )
}
fn packet(method: &str, params: Value) -> Vec<u8> {
    wire_json::encode(
        &json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}),
        Default::default(),
    )
    .unwrap()
}
fn action_event(action: &str) -> Event {
    match action {
        "init"=>Event::Init,"close"=>Event::Close,"timeout"=>Event::Timeout,"set_target"=>Event::Chain(Chain::SetTarget),
        "block"=>Event::Chain(Chain::Block(Work::new("0".repeat(64),6,1.into()).unwrap())),
        "parse_error"=>Event::Data(b"[".to_vec()),
        "invalid_version"=>Event::Data(br#"{"jsonrpc":"1.0","id":1,"method":"mining.configure","params":[]}"#.to_vec()),
        "raw_response"=>Event::Data(br#"{"jsonrpc":"2.0","id":1,"result":true,"error":null}"#.to_vec()),
        "raw_notify"=>Event::Data(wire_json::encode(&json!({"jsonrpc":"2.0","id":null,"method":"mining.set_target","params":["f".repeat(64)]}),Default::default()).unwrap()),
        a=>Event::Data(match a {
            "configure"=>packet("mining.configure",json!([])),
            "subscribe"|"bad_port"=>packet("mining.subscribe",json!(["miner/1",null,null,if a=="bad_port"{9998}else{9999}])),
            "authorize"|"worker_two"|"worker_three"|"different_account"=>{
                let account=if a=="different_account"{"ak_11111111111111111111111111111111111111111111111111"}else{ACCOUNT};
                let worker=match a{"worker_two"=>"two","worker_three"=>"three",_=>"one"};packet("mining.authorize",json!([format!("{account}.{worker}"),null]))
            },
            "submit_missing"=>packet("mining.submit",json!([format!("{ACCOUNT}.one"),"0123456789abcdef","00000000",(0..42).collect::<Vec<_>>() ])),
            "invalid_method"=>packet("mining.unknown",json!([])),"invalid_params"=>packet("mining.subscribe",json!([])),_=>panic!("{a}"),
        }),
    }
}
fn wire(action: &Action) -> Value {
    match action {
        Action::Send(bytes) => wire_json::decode(bytes, Default::default()).unwrap(),
        _ => Value::Null,
    }
}
fn chain(c: &Chain) -> Value {
    let event = match c {
        Chain::SetTarget => json!({"event":"set_target"}),
        Chain::Block(w) => {
            json!({"event":"recv_block","block":{"block_hash":w.hash_text(),"block_version":w.version,"block_target":number(&w.block_target)}})
        }
        Chain::Notify(i) => {
            json!({"event":"notify","job_info":{"job_id":i.id,"block_hash":i.work.hash_text(),"block_version":i.work.version,"block_target":number(&i.work.block_target),"share_target":number(&i.share_target),"desired_solve_time":number(&i.desired),"max_solve_time":number(&i.maximum)}})
        }
    };
    json!(["chain", event])
}
fn queued(step: &Step) -> Value {
    json!(
        step.effects
            .iter()
            .filter_map(|e| if let Effect::Enqueue(c) = e {
                Some(chain(c))
            } else {
                None
            })
            .collect::<Vec<_>>()
    )
}
fn handshake_state(s: &Session, w: &World) -> Value {
    json!({"session":{"phase":phase(s.phase),"timer_phase":s.timer.map_or("undefined",|t|phase(t.phase)),"account":s.account.as_deref().unwrap_or("undefined"),"extra_nonce":s.extra_nonce.map_or_else(||"undefined".into(),Part::to_hex),"accept_blocks":s.accept_blocks,"block_count":s.block_count,"initial_share_target":s.targets.initial.as_ref().map_or(json!("undefined"),number)},"registry":w.users.iter().map(|(a,(owner,workers))|json!([a,if *owner==1{"self"}else{"other"},workers.iter().collect::<Vec<_>>()])).collect::<Vec<_>>(),"nonce_cache":w.nonces.iter().copied().collect::<Vec<_>>()})
}
#[test]
fn all_original_handshake_ownership_timeout_and_error_traces_match() {
    let rows: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_stratum_sessions.json"
    ))
    .unwrap();
    let c = config();
    let mut count = 0;
    let mut sent = 0;
    for row in rows.as_array().unwrap() {
        let mut s = Session::new();
        let mut w = World::default();
        for step in row["steps"].as_array().unwrap() {
            count += 1;
            let a = step["action"].as_str().unwrap();
            let result = match a {
                "stale_timer" => {
                    s.timer.as_mut().unwrap().phase = Phase::Connected;
                    None
                }
                "other_owner" => {
                    w.users
                        .insert(ACCOUNT.into(), (2, ["outside".into()].into()));
                    None
                }
                "delete_account" => {
                    w.users.remove(ACCOUNT);
                    None
                }
                _ => Some(drive(&mut s, action_event(a), &mut w, &c, 1000).0),
            };
            if let Some(result) = result {
                let kind = match result.action {
                    Action::Send(_) => {
                        sent += 1;
                        "send"
                    }
                    Action::None => "no_send",
                    Action::Stop(_) => "stop",
                    _ => panic!("unresolved command"),
                };
                assert_eq!(
                    kind,
                    step["result"].as_str().unwrap(),
                    "{} {a}",
                    row["name"]
                );
                let expected = step["sent"].as_str().map_or(Value::Null, |s| {
                    wire_json::decode(&hex::decode(s).unwrap(), Default::default()).unwrap()
                });
                assert_eq!(wire(&result.action), expected, "{} {a}", row["name"]);
                assert_eq!(
                    queued(&result),
                    native(&step["queued"]),
                    "{} {a}",
                    row["name"]
                );
            }
            let expected = json!({"session":native(&step["state"]["session"]),"registry":native(&step["state"]["registry"]),"nonce_cache":step["state"]["nonce_cache"]});
            assert_eq!(handshake_state(&s, &w), expected, "{} {a}", row["name"]);
        }
        drive(&mut s, Event::Close, &mut w, &c, 1000);
        assert!(w.nonces.is_empty());
        assert_eq!(
            w.users.len() as u64,
            row["registry_after_close"].as_u64().unwrap()
        );
    }
    assert_eq!((count, sent), (111, 61));
}
fn validity(v: Option<share::Validity>) -> &'static str {
    use share::Validity::*;
    match v {
        None => "undefined",
        Some(ValidBlock) => "valid_block",
        Some(ValidShare) => "valid_share",
        Some(UserNotFound) => "user_not_found",
        Some(InvalidMinerNonce) => "invalid_miner_nonce",
        Some(DuplicateShare) => "duplicate_share",
        Some(InvalidSolution) => "invalid_solution",
        Some(HighTargetShare) => "high_target_share",
        Some(MaxSolveTimeExceeded) => "max_solve_time_exceeded",
        Some(JobNotFound) => "job_not_found",
    }
}
fn share_status(s: &Share) -> Value {
    json!({"user":[s.user.account,s.user.worker],"miner_nonce":s.miner_nonce.to_hex(),"pow":s.proof.to_json(),"created":number(&s.created),"validity":validity(s.validity)})
}
fn job_status(j: &Job, created: bool) -> Value {
    let mut v = json!({"id":j.id(),"block_hash":j.work().hash_text(),"block_version":j.work().version,"block_target":target::to_hex(&j.work().block_target).unwrap(),"share_target":target::to_hex(&j.share_target()).unwrap(),"desired_solve_time":number(j.desired_solve_time()),"max_solve_time":number(j.maximum_solve_time()),"shares":j.shares().iter().map(share_status).collect::<Vec<_>>(),"error_shares":j.errors().iter().map(share_status).collect::<Vec<_>>()});
    if created {
        v["created"] = number(j.created());
    }
    v
}
fn handoff_state(s: &Session) -> Value {
    json!({"phase":phase(s.phase),"accept_blocks":s.accept_blocks,"block_count":s.block_count,"initial_share_target":number(s.targets.initial.as_ref().unwrap()),"max_share_target":number(s.targets.maximum.as_ref().unwrap()),"threshold":s.targets.difference_threshold.unwrap(),"desired":number(s.targets.desired_solve_time.as_ref().unwrap()),"maximum":number(s.targets.maximum_solve_time.as_ref().unwrap()),"jobs":s.jobs.as_ref().unwrap().iter().map(|j|job_status(j,false)).collect::<Vec<_>>()})
}
fn from_projection(v: &Value) -> Session {
    let mut s = Session::new();
    s.phase = match v["phase"].as_str().unwrap() {
        "authorized" => Phase::Authorized,
        "connected" => Phase::Connected,
        "subscribed" => Phase::Subscribed,
        _ => panic!("phase"),
    };
    s.accept_blocks = v["accept_blocks"].as_bool().unwrap();
    s.block_count = v["block_count"].as_u64().unwrap();
    s.targets = Targets {
        initial: big(&v["initial_share_target"]),
        maximum: big(&v["max_share_target"]),
        difference_threshold: v["threshold"].as_f64().unwrap(),
        desired_solve_time: big(&v["desired"]),
        maximum_solve_time: big(&v["maximum"]),
    }
    .into();
    let mut jobs = Jobs::default();
    for j in v["jobs"].as_array().unwrap() {
        jobs.add(
            Job::new(
                j["id"].as_str().unwrap().into(),
                Work::new(
                    j["block_hash"].as_str().unwrap().into(),
                    j["block_version"].as_u64().unwrap() as u32,
                    target::from_hex(j["block_target"].as_str().unwrap()).unwrap(),
                )
                .unwrap(),
                target::from_hex(j["share_target"].as_str().unwrap()).unwrap(),
                Timing {
                    desired: big(&j["desired_solve_time"]),
                    maximum: big(&j["max_solve_time"]),
                    created: 1000.into(),
                },
                Limits::default(),
            )
            .unwrap(),
        )
        .unwrap();
    }
    s.jobs = Some(jobs);
    s
}
fn native_chain(v: &Value) -> Chain {
    let m = &v[1];
    let b = if m["event"] == "recv_block" {
        &m["block"]
    } else {
        &m["job_info"]
    };
    let w = Work::new(
        b["block_hash"].as_str().unwrap().into(),
        b["block_version"].as_u64().unwrap() as u32,
        big(&b["block_target"]),
    )
    .unwrap();
    if m["event"] == "recv_block" {
        Chain::Block(w)
    } else {
        Chain::Notify(JobInfo {
            id: b["job_id"].as_str().unwrap().into(),
            work: w,
            share_target: big(&b["share_target"]),
            desired: big(&b["desired_solve_time"]),
            maximum: big(&b["max_solve_time"]),
        })
    }
}
#[test]
fn original_handoffs_preserve_exact_threshold_skip_eviction_and_queued_timing() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_stratum_session_edges.json"
    ))
    .unwrap();
    let c = config();
    let mut count = 0;
    for row in corpus["handoffs"].as_array().unwrap() {
        let mut s = from_projection(&native(&row["initial"]));
        let mut w = World::default();
        let mut pending = None;
        for step in row["steps"].as_array().unwrap() {
            count += 1;
            let event = native_chain(&native(&step["event"]));
            if let Chain::Notify(_) = &event {
                assert_eq!(pending.take(), Some(event.clone()));
                s.set(Update::DesiredSolveTime(123.into())).unwrap();
                s.set(Update::MaximumSolveTime(777.into())).unwrap();
            }
            let (out, _) = drive(&mut s, Event::Chain(event), &mut w, &c, 1000);
            for e in &out.effects {
                if let Effect::Enqueue(c) = e {
                    assert!(pending.replace(c.clone()).is_none());
                }
            }
            assert_eq!(wire(&out.action), step["wire"], "{}", row["name"]);
            assert_eq!(queued(&out), native(&step["queued"]));
            assert_eq!(
                typed(handoff_state(&s)),
                typed(native(&step["state"])),
                "{}",
                row["name"]
            );
        }
        assert_eq!(typed(handoff_state(&s)), typed(native(&row["final"])));
        assert!(pending.is_none());
    }
    assert_eq!(count, 44);
}
fn pool_effect(e: &share::Effect) -> Value {
    match e {
        share::Effect::SubmitShare {
            account,
            target,
            hash,
        } => {
            json!({"kind":"share","account":account,"target":number(target),"hash":hex::encode(hash.0)})
        }
        share::Effect::SubmitSolution { hash, nonce, proof } => {
            json!({"kind":"solution","hash":hex::encode(hash.0),"nonce":nonce.0,"pow":proof.to_vec()})
        }
    }
}
#[test]
fn wire_valid_submissions_reach_native_work_outcomes_and_terminal_failures_cleanup() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_stratum_numeric_shares.json"
    ))
    .unwrap();
    let rows = corpus["rows"].as_array().unwrap();
    let mut count = 0;
    let mut failures = 0;
    let c = config();
    for row in rows {
        if native(&row["wire"])[0] != "ok" {
            continue;
        }
        count += 1;
        let variant = row["variant"].as_str().unwrap();
        let expected = native(&row["result"]);
        let raw = native(&row["pow"]);
        let (extra, miner) = Nonce(row["nonce"].as_u64().unwrap())
            .split(Kind::Extra, 4)
            .unwrap();
        let miner = if variant == "nonce_size" {
            Part::new(Kind::Miner, 0, 3).unwrap()
        } else {
            miner
        };
        let now = if variant == "late" { 61001 } else { 61000 };
        let mut t: BigInt = row["target"].as_str().unwrap().parse().unwrap();
        if variant == "high_target" {
            t -= 1;
        }
        let w = Work::new(
            row["hash"].as_str().unwrap().into(),
            row["version"].as_u64().unwrap() as u32,
            t.clone(),
        )
        .unwrap();
        let mut j = Job::new(
            "0123456789abcdef".into(),
            w,
            t,
            Timing {
                desired: 30000.into(),
                maximum: 60000.into(),
                created: 1000.into(),
            },
            Limits::default(),
        )
        .unwrap();
        if matches!(variant, "duplicate_same" | "duplicate_integer") {
            let p = if variant == "duplicate_same" {
                raw.clone()
            } else {
                native(
                    &rows
                        .iter()
                        .find(|r| {
                            r["hash"] == row["hash"]
                                && r["mode"] == "integer"
                                && r["variant"] == "normal"
                        })
                        .unwrap()["pow"],
                )
            };
            j.add_share(Share {
                user: User {
                    account: ACCOUNT.into(),
                    worker: "one".into(),
                },
                miner_nonce: miner,
                proof: Proof::from_json(&p).unwrap(),
                validity: None,
                created: now.into(),
            })
            .unwrap();
        }
        let mut jobs = Jobs::default();
        if variant != "missing" {
            jobs.add(j).unwrap();
        }
        let mut s = Session::new();
        s.phase = Phase::Authorized;
        s.account = Some(ACCOUNT.into());
        s.extra_nonce = Some(extra);
        s.targets = c.targets.clone().into();
        s.jobs = Some(jobs);
        s.accept_blocks = true;
        let mut world = World::default();
        world.nonces.insert(extra.value());
        if variant != "unauthorized" {
            world
                .users
                .insert(ACCOUNT.into(), (1, ["one".into()].into()));
        }
        let before = s.jobs.clone();
        let bytes=wire_json::encode(&json!({"jsonrpc":"2.0","id":7,"method":"mining.submit","params":[format!("{ACCOUNT}.one"),"0123456789abcdef",miner.to_hex(),raw]}),Default::default()).unwrap();
        let (out, ops) = drive(&mut s, Event::Data(bytes), &mut world, &c, now);
        let effects: Vec<_> = out
            .effects
            .iter()
            .filter_map(|e| {
                if let Effect::Pool(p) = e {
                    Some(pool_effect(p))
                } else {
                    None
                }
            })
            .collect();
        if expected.get("exception").is_some() {
            failures += 1;
            assert_eq!(
                out.action,
                Action::Stop(StopReason::Work(crate::Error::ProofArithmetic))
            );
            assert_eq!(s.jobs, before);
            assert_eq!(s.phase, Phase::Disconnected);
            assert!(world.nonces.is_empty() && world.users.is_empty());
            assert!(matches!(
                ops.as_slice(),
                [
                    Operation::Membership { .. },
                    Operation::ReleaseNonce { .. },
                    Operation::DeleteConnection
                ]
            ));
            let (again, ops) = drive(&mut s, Event::Close, &mut world, &c, now);
            assert!(ops.is_empty() && again.effects.is_empty());
        } else {
            assert_eq!(
                wire(&out.action),
                expected["response"],
                "{} {variant}",
                row["mode"]
            );
            assert_eq!(s.phase, Phase::Authorized);
        }
        if variant == "missing" {
            assert!(ops.is_empty());
        }
        assert_eq!(typed(json!(effects)), typed(expected["effects"].clone()));
        assert_eq!(
            typed(json!(
                s.jobs
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|j| job_status(j, true))
                    .collect::<Vec<_>>()
            )),
            typed(expected["retained"].clone()),
            "{} {variant}",
            row["mode"]
        );
    }
    assert_eq!((count, failures), (480, 144));
}
#[test]
fn exhausted_nonce_allocation_keeps_native_phase_and_canceled_timer_record() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_stratum_session_edges.json"
    ))
    .unwrap();
    let mut c = config();
    c.extra_nonce_bytes = 1;
    for row in corpus["exhaustion"].as_array().unwrap() {
        let mut s = Session::new();
        let mut w = World {
            exhausted: true,
            ..Default::default()
        };
        drive(&mut s, Event::Init, &mut w, &c, 1000);
        if row["phase"] == "configured" {
            drive(&mut s, action_event("configure"), &mut w, &c, 1000);
        }
        let old = s.timer.unwrap();
        assert!(old.active);
        let (out, ops) = drive(&mut s, action_event("subscribe"), &mut w, &c, 1000);
        assert_eq!(wire(&out.action), row["wire"]);
        assert_eq!(phase(s.phase), row["phase"].as_str().unwrap());
        assert_eq!(
            s.timer,
            Some(Timer {
                active: false,
                ..old
            })
        );
        assert_eq!(ops, vec![Operation::AllocateNonce { bytes: 1 }]);
        assert_eq!(out.effects, vec![Effect::CancelTimer { id: old.id }]);
        // A delivered timeout is still checked by stored phase, not active flag.
        let (out, _) = drive(&mut s, Event::Timeout, &mut w, &c, 1000);
        assert_eq!(out.action, Action::Stop(StopReason::Timeout));
    }
}
#[test]
fn suspended_commands_reject_interleaving_mismatched_and_repeated_completions() {
    let c = config();
    let now = 1000.into();
    let ctx = Context {
        config: &c,
        now: &now,
    };
    let mut s = Session::new();
    s.handle(Event::Init, &ctx).unwrap();
    let begin = s.handle(action_event("subscribe"), &ctx).unwrap();
    let Action::Command(command) = begin.action else {
        panic!("allocation")
    };
    let pending = s.clone();
    for event in [
        Event::Close,
        Event::Timeout,
        action_event("configure"),
        Event::Chain(Chain::SetTarget),
    ] {
        assert_eq!(s.handle(event, &ctx), Err(Error::Busy));
        assert_eq!(s, pending);
    }
    for (id, reply) in [
        (command.id + 1, Reply::Nonce(None)),
        (command.id, Reply::Released),
        (
            command.id,
            Reply::Nonce(Some(Part::new(Kind::Miner, 0, 4).unwrap())),
        ),
        (
            command.id,
            Reply::Nonce(Some(Part::new(Kind::Extra, 0, 3).unwrap())),
        ),
    ] {
        assert_eq!(
            s.handle(Event::Complete { id, reply }, &ctx),
            Err(Error::UnexpectedCompletion)
        );
        assert_eq!(s, pending);
    }
    let reply = Reply::Nonce(Some(Part::new(Kind::Extra, 42, 4).unwrap()));
    s.handle(
        Event::Complete {
            id: command.id,
            reply: reply.clone(),
        },
        &ctx,
    )
    .unwrap();
    let completed = s.clone();
    assert_eq!(
        s.handle(
            Event::Complete {
                id: command.id,
                reply
            },
            &ctx
        ),
        Err(Error::UnexpectedCompletion)
    );
    assert_eq!(s, completed);
    let mut exhausted = Session::new();
    exhausted.sequence = u64::MAX;
    let before = exhausted.clone();
    assert_eq!(
        exhausted.handle(Event::Init, &ctx),
        Err(Error::SequenceExhausted)
    );
    assert_eq!(exhausted, before);
}
#[test]
fn registration_result_order_and_pre_target_administrative_changes_are_preserved() {
    let c = config();
    let now = 1000.into();
    let ctx = Context {
        config: &c,
        now: &now,
    };
    let mut s = Session::new();
    s.set(Update::MaximumTarget(100.into())).unwrap();
    s.set(Update::DesiredSolveTime((-7).into())).unwrap();
    s.set(Update::MaximumSolveTime(9.into())).unwrap();
    s.set(Update::DifferenceThreshold(-1.0)).unwrap();
    assert!(s.targets.initial.is_none());
    assert_eq!(s.targets.maximum, Some(100.into()));
    assert_eq!(s.targets.desired_solve_time, Some((-7).into()));
    let before = s.clone();
    assert_eq!(
        s.set(Update::DifferenceThreshold(f64::NAN)),
        Err(Error::Configuration)
    );
    assert_eq!(s, before);
    let mut world = World::default();
    drive(&mut s, Event::Init, &mut world, &c, 1000);
    drive(&mut s, action_event("subscribe"), &mut world, &c, 1000);
    let Step {
        action: Action::Command(member),
        ..
    } = s.handle(action_event("authorize"), &ctx).unwrap()
    else {
        panic!("membership")
    };
    let Step {
        action: Action::Command(register),
        effects,
    } = s
        .handle(
            Event::Complete {
                id: member.id,
                reply: Reply::Membership(Membership::Neither),
            },
            &ctx,
        )
        .unwrap()
    else {
        panic!("register")
    };
    assert!(matches!(effects.as_slice(), [Effect::CancelTimer { .. }]));
    assert!(matches!(register.operation, Operation::Register { .. }));
    // An interleaving registry insert can make add return already_present;
    // first authorization ignores it in the original implementation.
    let out = s
        .handle(
            Event::Complete {
                id: register.id,
                reply: Reply::Registered(Registration::AlreadyPresent),
            },
            &ctx,
        )
        .unwrap();
    assert_eq!(wire(&out.action)["result"], json!(true));
    assert_eq!(out.effects, vec![Effect::Enqueue(Chain::SetTarget)]);
    assert_eq!(s.phase, Phase::Authorized);
    assert!(s.timer.is_none());
    let step = s.handle(Event::Chain(Chain::SetTarget), &ctx).unwrap();
    assert!(matches!(step.action, Action::Send(_)));
    assert_eq!(s.targets, Settings::from(c.targets));
}

#[test]
fn nonce_completion_reads_current_timer_policy_and_invalid_internal_events_cleanup() {
    let mut c = config();
    let now = BigInt::from(1000);
    let mut s = Session::new();
    s.handle(
        Event::Init,
        &Context {
            config: &c,
            now: &now,
        },
    )
    .unwrap();
    let Step {
        action: Action::Command(command),
        ..
    } = s
        .handle(
            action_event("subscribe"),
            &Context {
                config: &c,
                now: &now,
            },
        )
        .unwrap()
    else {
        panic!("nonce allocation")
    };
    c.message_timeout_ms = 5;
    let allocated = Part::new(Kind::Extra, 42, 4).unwrap();
    let out = s
        .handle(
            Event::Complete {
                id: command.id,
                reply: Reply::Nonce(Some(allocated)),
            },
            &Context {
                config: &c,
                now: &2000.into(),
            },
        )
        .unwrap();
    assert!(matches!(
        out.effects.as_slice(),
        [Effect::ScheduleTimer { after_ms: 5, .. }]
    ));
    let mut world = World::default();
    world.nonces.insert(42);
    let (out, ops) = drive(&mut s, Event::Init, &mut world, &c, 2000);
    assert_eq!(out.action, Action::Stop(StopReason::InvalidEvent));
    assert_eq!(ops, vec![Operation::ReleaseNonce { nonce: allocated }]);
    assert!(world.nonces.is_empty());
    assert!(matches!(
        out.effects.as_slice(),
        [Effect::CancelTimer { .. }]
    ));

    let mut s = Session::new();
    let mut world = World::default();
    for action in ["init", "subscribe", "authorize", "set_target"] {
        drive(&mut s, action_event(action), &mut world, &c, 1000);
    }
    let info = JobInfo {
        id: "0123456789abcdef".into(),
        work: Work::new("00".repeat(32), 6, 1.into()).unwrap(),
        share_target: 10.into(),
        desired: 30000.into(),
        maximum: 60000.into(),
    };
    let (out, ops) = drive(
        &mut s,
        Event::Chain(Chain::Notify(info)),
        &mut world,
        &c,
        2000,
    );
    assert_eq!(out.action, Action::Stop(StopReason::InvalidEvent));
    assert!(matches!(
        ops.as_slice(),
        [Operation::ReleaseNonce { .. }, Operation::DeleteConnection]
    ));
    assert!(world.nonces.is_empty() && world.users.is_empty());
    assert_eq!(s.phase, Phase::Disconnected);
    assert!(s.jobs.as_ref().unwrap().is_empty());
}

#[test]
fn terminal_retarget_failure_does_not_publish_event_local_changes() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_stratum_session_edges.json"
    ))
    .unwrap();
    let initial = &corpus["handoffs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "retarget")
        .unwrap()["initial"];
    let mut s = from_projection(&native(initial));
    let nonce = Part::new(Kind::Extra, 42, 4).unwrap();
    s.extra_nonce = Some(nonce);
    s.account = Some(ACCOUNT.into());
    s.set(Update::DesiredSolveTime(0.into())).unwrap();
    let before = s.clone();
    let mut world = World::default();
    world.nonces.insert(42);
    world
        .users
        .insert(ACCOUNT.into(), (1, ["one".into()].into()));
    let (out, ops) = drive(
        &mut s,
        Event::Chain(Chain::Block(
            Work::new("a1".repeat(32), 6, 1.into()).unwrap(),
        )),
        &mut world,
        &config(),
        1000,
    );
    assert_eq!(
        out.action,
        Action::Stop(StopReason::Work(crate::Error::Arithmetic))
    );
    assert_eq!(s.block_count, before.block_count);
    assert_eq!(s.jobs, before.jobs);
    assert_eq!(s.targets, before.targets);
    assert_eq!(s.accept_blocks, before.accept_blocks);
    assert_eq!(s.phase, Phase::Disconnected);
    assert!(world.nonces.is_empty() && world.users.is_empty());
    assert!(out.effects.is_empty());
    assert_eq!(
        ops,
        vec![
            Operation::ReleaseNonce { nonce },
            Operation::DeleteConnection
        ]
    );
}

#[test]
fn configured_graph_sizes_and_authorization_time_job_capacities_match_original() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_stratum_session_config.json"
    ))
    .unwrap();
    for row in corpus["graphs"].as_array().unwrap() {
        let mut c = config();
        c.edge_bits = row["edge_bits"].as_u64().unwrap() as u8;
        let (extra, miner) = Nonce(row["nonce"].as_u64().unwrap())
            .split(Kind::Extra, 4)
            .unwrap();
        let target: BigInt = row["target"].as_str().unwrap().parse().unwrap();
        let job = Job::new(
            "0123456789abcdef".into(),
            Work::new(
                row["hash"].as_str().unwrap().into(),
                row["version"].as_u64().unwrap() as u32,
                target.clone(),
            )
            .unwrap(),
            target,
            Timing {
                desired: 30000.into(),
                maximum: 60000.into(),
                created: 1000.into(),
            },
            Limits::default(),
        )
        .unwrap();
        let mut jobs = Jobs::default();
        jobs.add(job).unwrap();
        let mut s = Session::new();
        s.phase = Phase::Authorized;
        s.targets = c.targets.clone().into();
        s.extra_nonce = Some(extra);
        s.jobs = Some(jobs);
        s.account = Some(ACCOUNT.into());
        let mut world = World::default();
        world.nonces.insert(extra.value());
        world
            .users
            .insert(ACCOUNT.into(), (1, ["one".into()].into()));
        let request = packet(
            "mining.submit",
            json!([
                format!("{ACCOUNT}.one"),
                "0123456789abcdef",
                miner.to_hex(),
                row["proof"]
            ]),
        );
        let (out, _) = drive(&mut s, Event::Data(request), &mut world, &c, 61000);
        let j = s.jobs.as_ref().unwrap().front().unwrap();
        if row["tag"] == "ok" {
            assert_eq!(wire(&out.action)["result"], json!(true));
            assert_eq!(
                validity(j.shares()[0].validity),
                row["validity"].as_str().unwrap()
            );
        } else {
            assert_eq!(
                wire(&out.action)["error"],
                json!([20, "Other/Unknown", "invalid_solution"])
            );
            assert_eq!(
                validity(j.errors()[0].validity),
                row["validity"].as_str().unwrap()
            );
        }
        assert_eq!(s.phase, Phase::Authorized);
    }
    for row in corpus["capacities"].as_array().unwrap() {
        let mut c = config();
        c.max_jobs = row["capacity"].as_u64().unwrap() as usize;
        c.skip_blocks = 0;
        let mut s = Session::new();
        let mut world = World::default();
        for a in ["init", "subscribe"] {
            drive(&mut s, action_event(a), &mut world, &c, 1000);
        }
        let (auth, _) = drive(&mut s, action_event("authorize"), &mut world, &c, 1000);
        assert_eq!(wire(&auth.action), row["authorization"]);
        drive(&mut s, action_event("set_target"), &mut world, &c, 1000);
        // Native max_jobs is read when creating the authorized queue only.
        c.max_jobs = 10;
        for step in row["steps"].as_array().unwrap() {
            let work = Work::new(step["hash"].as_str().unwrap().into(), 6, 1.into()).unwrap();
            let (out, _) = drive(
                &mut s,
                Event::Chain(Chain::Block(work)),
                &mut world,
                &c,
                1000,
            );
            assert_eq!(wire(&out.action), step["wire"]);
            assert_eq!(
                json!(
                    s.jobs
                        .as_ref()
                        .unwrap()
                        .iter()
                        .map(|j| j.work().hash_text())
                        .collect::<Vec<_>>()
                ),
                step["retained"]
            );
        }
    }
    assert_eq!(corpus["graphs"].as_array().unwrap().len(), 18);
    for (bits, jobs) in [(0, 20), (64, 20), (29, 0), (29, 1025)] {
        let mut c = config();
        c.edge_bits = bits;
        c.max_jobs = jobs;
        let mut s = Session::new();
        let before = s.clone();
        assert_eq!(
            s.handle(
                Event::Init,
                &Context {
                    config: &c,
                    now: &0.into()
                }
            ),
            Err(Error::Configuration)
        );
        assert_eq!(s, before);
    }
}
