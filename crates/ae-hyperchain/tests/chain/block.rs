use super::*;
use ae_hyperchain::{
    block::{self as b, Host},
    genesis as g, leader,
};
use ae_protocol::Activation;
use ae_vm_fate::abi;
use num_traits::ToPrimitive;
use std::collections::BTreeMap;

static BLOCK: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_hc_block.json")));
type R = Reader<MemoryStore, MemoryStore, Otp26>;
fn reader(row: &J) -> R {
    let mut p = super::profile(8);
    p.contracts = cc::Contracts {
        owner: AccountPublicKey(key(1)),
        staking: bytes(&BLOCK["staking"]).try_into().unwrap(),
        election: bytes(&BLOCK["election"]).try_into().unwrap(),
        rewards: bytes(&BLOCK["election"]).try_into().unwrap(),
    };
    p.network = NetworkProfile::custom(
        b"ae_hc_reference".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
        }],
    )
    .unwrap();
    p.order = otp_order::profile(&BLOCK["profile"]);
    p.maximum_steps = 1_000_000;
    R::new(chain::Selected::new(Default::default()), Arc::new(p))
}
fn config() -> b::Config {
    b::Config {
        parent_epoch_length: 5.into(),
        parent_start: 1.into(),
        parent_finality: 2.into(),
        pin_sync_margin: 1.into(),
        child_block_time: 4.into(),
        cache_limits: Default::default(),
    }
}
fn previous(row: &J, op: &J, trees: &Trees) -> chain::View {
    let p = &op["previous"];
    let mut v = view(
        trees.clone(),
        row["protocol"].as_u64().unwrap(),
        p["height"].as_u64().unwrap(),
    );
    v.environment.network_id = b"ae_hc_reference".to_vec();
    v.environment.key_hash = bytes(&p["key_hash"]).try_into().unwrap();
    v.environment.timestamp = p["time"].as_i64().unwrap().into();
    v.environment.difficulty = p["difficulty"].as_i64().unwrap().into();
    v
}
#[derive(Debug, Clone, PartialEq, Eq)]
enum HostError {
    Native,
    Operational,
}
#[derive(Clone, PartialEq, Eq)]
struct Probe {
    missing: bool,
    proof: Result<Option<b::Pin>, HostError>,
    heights: BTreeMap<BigInt, Vec<u8>>,
    entropy_reads: Vec<BigInt>,
    parent_events: Vec<J>,
    pin_reads: Vec<Vec<u8>>,
    key_reads: Vec<BigInt>,
}
impl Default for Probe {
    fn default() -> Self {
        Self {
            missing: false,
            proof: Ok(Some(b::Pin {
                epoch: 1.into(),
                child_height: 3.into(),
                child_hash: key(333).to_vec(),
                parent_height: 7.into(),
            })),
            heights: BTreeMap::from([(3.into(), key(333).to_vec())]),
            entropy_reads: vec![],
            parent_events: vec![],
            pin_reads: vec![],
            key_reads: vec![],
        }
    }
}
impl Probe {
    fn clear(&mut self) {
        self.entropy_reads.clear();
        self.parent_events.clear();
        self.pin_reads.clear();
        self.key_reads.clear();
    }
    fn reads(&self) -> J {
        json!({"entropy_reads":list(self.entropy_reads.iter().map(int).collect()),"parent_events":list(self.parent_events.clone()),"pin_reads":list(self.pin_reads.iter().map(bin).collect()),"key_reads":list(self.key_reads.iter().map(int).collect())})
    }
}
impl Host for Probe {
    type Error = HostError;
    fn is_operational(e: &Self::Error) -> bool {
        *e == HostError::Operational
    }
    fn entropy(&mut self, epoch: &BigInt) -> Result<Option<Vec<u8>>, Self::Error> {
        let h: BigInt = (epoch - BigInt::from(3)).max(0.into()) * 5 + 1;
        self.entropy_reads.push(h.clone());
        if self.missing {
            return Ok(None);
        }
        let n = h.to_u64().unwrap();
        self.parent_events.push(tuple(vec![
            int(epoch),
            tuple(vec![
                atom("block"),
                bin(key(1000 + n)),
                int(&h),
                bin(key(999 + n)),
                int(n * 100),
            ]),
        ]));
        Ok(Some(key(1000 + n).to_vec()))
    }
    fn pin(&mut self, hash: &[u8]) -> Result<Option<b::Pin>, Self::Error> {
        self.pin_reads.push(hash.to_vec());
        self.proof.clone()
    }
    fn key_hash(&mut self, height: &BigInt) -> Result<Option<Vec<u8>>, Self::Error> {
        self.key_reads.push(height.clone());
        Ok(self.heights.get(height).cloned())
    }
}
fn map(rows: Vec<(J, J)>) -> J {
    json!([
        "map",
        rows.into_iter()
            .map(|(k, v)| json!([k, v]))
            .collect::<Vec<_>>()
    ])
}
pub(super) fn cache(s: &b::State) -> J {
    json!({"leaders":named(vec![
        ("epochs",list(s.leaders.epochs().iter().map(int).collect())),
        ("epoch_infos",map(s.leaders.ranges().iter().map(|(e,(f,l))|(int(e),tuple(vec![int(f),int(l)]))).collect())),
        ("schedule",map(s.leaders.schedule().iter().map(|(h,p)|(int(h),object(p))).collect()))]),
        "starts":list(s.epoch_starts.iter().map(|e|tuple(vec![int(&e.epoch),int(&e.height),int(&e.time)])).collect())})
}
pub(super) fn events(es: Vec<b::Event>) -> J {
    list(
        es.into_iter()
            .map(|e| match e {
                b::Event::NoPin => tuple(vec![atom("pin"), tuple(vec![atom("no_proof_posted")])]),
                b::Event::IncorrectPin => tuple(vec![
                    atom("pin"),
                    tuple(vec![atom("incorrect_proof_posted")]),
                ]),
                b::Event::PinAccepted {
                    reward,
                    recipient,
                    epoch,
                    height,
                } => tuple(vec![
                    atom("pin"),
                    tuple(vec![
                        atom("pin_accepted"),
                        named(vec![
                            ("reward", int(reward)),
                            ("recipient", object(&recipient)),
                            ("epoch", value(&epoch)),
                            ("height", int(height)),
                        ]),
                    ]),
                ]),
                b::Event::NewEpoch(e) => tuple(vec![atom("new_epoch"), epoch_json(e)]),
            })
            .collect(),
    )
}
fn abort(s: &'static str) -> J {
    exception(native::Exception::error(Term::tagged(
        "handled_abort",
        vec![Term::Atom(s)],
    )))
}
fn apply(r: &R, row: &J, op: &J, s: &mut b::State, host: &mut Probe, t: &Trees) -> Trees {
    host.clear();
    let mut v = previous(row, op, t);
    let roots = t.roots();
    let h = op["height"].as_u64().unwrap();
    let (next, result) = match op["kind"].as_str().unwrap() {
        "leader" => {
            v.context.height = h;
            let out = match s.leader_for_timeslot(
                r,
                &RunEnv::View(Arc::new(v)),
                host,
                &h.into(),
                config().cache_limits,
            ) {
                Ok(p) => reply(object(&p)),
                Err(b::Error::Leader(e)) => ok(tuple(vec![
                    atom("error"),
                    atom(match e {
                        leader::Error::NotInCache => "not_in_cache",
                        leader::Error::EpochDidNotEnd => "epoch_did_not_end",
                        e => panic!("{e:?}"),
                    }),
                ])),
                Err(e) => panic!("leader {e:?}"),
            };
            (t.clone(), out)
        }
        "pin" => {
            v.context.height = h;
            let ObjectPayload::Bytes(k) = s.leaders.leader(&h.into()).unwrap() else {
                panic!("leader")
            };
            let caller = AccountPublicKey(k.as_slice().try_into().unwrap());
            let spec = g::InitialCall {
                caller,
                contract: r.profile.contracts.election,
                nonce: t
                    .accounts
                    .lookup(caller)
                    .unwrap()
                    .unwrap()
                    .nonce()
                    .add(&1.into()),
                abi: 3.into(),
                fee: 999999999999u64.into(),
                amount: 0.into(),
                gas: 2000000.into(),
                gas_price: 1000000000.into(),
                calldata: abi::create_calldata(
                    b"pin",
                    vec![Value::Bytes(key(777).to_vec())],
                    Limits::default(),
                )
                .unwrap(),
            };
            (
                with_execution(&r.profile, &v, |cfg| g::call(t, &spec, &v.context, cfg)).unwrap(),
                ok(list(vec![])),
            )
        }
        kind => match s.pre_transform(
            r,
            &v,
            t,
            host,
            if kind == "key" {
                b::Kind::Key
            } else {
                b::Kind::Micro
            },
            h,
            &config(),
        ) {
            Ok(out) => {
                if kind == "key"
                    && h == 4
                    && op["reads"]["entropy_reads"] == list(vec![])
                    && out.trees.roots() == roots
                {
                    assert_eq!(out.wait_ms, Some(1.into()));
                }
                (out.trees, ok(events(out.events)))
            }
            Err(b::Error::ParentNotSynced { wait_ms }) => {
                assert_eq!(wait_ms, 1.into());
                (t.clone(), abort("parent_chain_not_synced"))
            }
            Err(e) => panic!("block {kind} {h}: {e:?}"),
        },
    };
    assert_eq!(
        normalize(&result),
        normalize(&op["result"]),
        "{} {h}",
        op["kind"]
    );
    assert_eq!(normalize(&cache(s)), normalize(&op["cache"]), "cache {h}");
    assert_eq!(
        normalize(&host.reads()),
        normalize(&op["reads"]),
        "host {h}"
    );
    if !op["trees"].is_null() {
        check(&next, &op["trees"])
    } else {
        assert_eq!(next.roots(), roots)
    }
    assert_eq!(t.roots(), roots);
    next
}
fn field<'a>(m: &'a J, name: &str) -> &'a J {
    &m[1]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r[0] == atom(name))
        .unwrap()[1]
}
fn number(v: &J) -> BigInt {
    v[1].as_str().unwrap().parse().unwrap()
}
fn proof_host(op: &J) -> Probe {
    let proof = &op["pin"];
    let proof = if proof == &atom("exception") {
        Err(HostError::Native)
    } else if proof[1][0] == atom("ok") {
        let m = &proof[1][1];
        Ok(Some(b::Pin {
            epoch: number(field(m, "epoch")),
            child_height: number(field(m, "height")),
            child_hash: bytes(&field(m, "block_hash")[1]),
            parent_height: number(field(m, "pc_height")),
        }))
    } else {
        Ok(None)
    };
    Probe {
        proof,
        missing: op["missing_entropy"].as_bool().unwrap(),
        heights: op["heights"][1]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| (number(&r[0]), bytes(&r[1][1])))
            .collect(),
        ..Default::default()
    }
}
#[test]
fn original_block_transitions_pins_cache_gc_and_events() {
    for row in BLOCK["rows"].as_array().unwrap() {
        let r = reader(row);
        let mut s = b::State::default();
        let mut host = Probe::default();
        let mut t = trees(&row["initial"]);
        let mut pinned = None;
        for op in row["trace"].as_array().unwrap() {
            t = apply(&r, row, op, &mut s, &mut host, &t);
            if op["kind"] == "pin" {
                pinned = Some((s.clone(), t.clone()));
            }
        }
        assert_eq!(
            s.leaders.epochs(),
            &[4.into(), 5.into(), 6.into(), 7.into(), 8.into()]
        );
        let (pin_state, pin_trees) = pinned.unwrap();
        check(&pin_trees, &row["pin_initial"]);
        assert_eq!(normalize(&cache(&pin_state)), normalize(&row["pin_cache"]));
        for op in row["pin_cases"].as_array().unwrap() {
            apply(
                &r,
                row,
                op,
                &mut pin_state.clone(),
                &mut proof_host(op),
                &pin_trees,
            );
        }
        apply(
            &r,
            row,
            &row["one"],
            &mut b::State::default(),
            &mut Probe::default(),
            &trees(&row["one_initial"]),
        );
    }
}
#[test]
fn original_spillover_leader_validation_and_stall_search() {
    for row in BLOCK["rows"].as_array().unwrap() {
        let r = reader(row);
        let t = trees(&row["initial"]);
        let mut s = b::State::default();
        let mut host = Probe::default();
        for op in row["spillover"].as_array().unwrap() {
            apply(&r, row, op, &mut s, &mut host, &t);
        }
        for c in row["leader_checks"]["validation"].as_array().unwrap() {
            let hole = c["hole"].as_bool().unwrap();
            let actual = match s.leaders.valid_producer(
                &c["height"].as_i64().unwrap().into(),
                &ObjectPayload::Bytes(bytes(&c["producer"])),
                hole,
            ) {
                Ok(v) => ok(atom(if v { "true" } else { "false" })),
                Err(leader::Error::NotInCache) => abort(if hole {
                    "no_leader_known_for_hole_block"
                } else {
                    "parent_chain_block_not_synced"
                }),
                Err(e) => panic!("{e:?}"),
            };
            assert_eq!(actual, c["result"]);
        }
        for c in row["leader_checks"]["own"].as_array().unwrap() {
            let keys: Vec<_> = c["keys"]
                .as_array()
                .unwrap()
                .iter()
                .map(|k| ObjectPayload::Bytes(bytes(k)))
                .collect();
            let found = s
                .leaders
                .own_slot(
                    &c["from"].as_i64().unwrap().into(),
                    &c["to"].as_i64().unwrap().into(),
                    100,
                    |k| keys.contains(k),
                )
                .unwrap();
            let actual = ok(found
                .map(|(h, p)| tuple(vec![atom("ok"), int(h), object(&p)]))
                .unwrap_or(atom("none")));
            assert_eq!(actual, c["result"]);
        }
        apply(
            &r,
            row,
            &row["missing"],
            &mut b::State::default(),
            &mut Probe {
                missing: true,
                ..Default::default()
            },
            &t,
        );
    }
}
#[test]
fn original_epoch_history_and_signed_pin_intervals() {
    let mut s = b::State::default();
    for c in BLOCK["pure"]["starts"].as_array().unwrap() {
        let e = c["epoch"].as_i64().unwrap();
        let actual = match s.cache_epoch_start(e.into(), (e * 3).into(), (100 + e).into()) {
            Ok(()) => ok(atom("ok")),
            Err(b::Native::SkippedEpoch { previous, next }) => {
                exception(native::Exception::error(Term::tagged(
                    "skipped_epoch",
                    vec![Term::Integer(previous), Term::Integer(next)],
                )))
            }
            Err(e) => panic!("{e:?}"),
        };
        assert_eq!(actual, c["result"]);
        assert_eq!(normalize(&cache(&s)), normalize(&c["cache"]));
    }
    for c in BLOCK["pure"]["intervals"].as_array().unwrap() {
        let (a, z) = b::pin_interval(&config(), &c["epoch"].as_i64().unwrap().into());
        assert_eq!(tuple(vec![int(a), int(z)]), c["result"]);
    }
}
#[test]
fn operational_failures_roll_back_schedules_host_effects_and_reward_roots() {
    let row = &BLOCK["rows"][2];
    let mut r = reader(row);
    let t = trees(&row["initial"]);
    let op = &row["trace"][1];
    let v = previous(row, op, &t);
    let mut s = b::State::default();
    let mut host = Probe::default();
    let original_host = host.clone();
    let mut cfg = config();
    cfg.cache_limits.maximum_slots = 3;
    assert!(matches!(
        s.pre_transform(&r, &v, &t, &mut host, b::Kind::Key, 1, &cfg),
        Err(b::Error::Leader(leader::Error::ResourceLimit))
    ));
    assert_eq!(s, b::State::default());
    assert!(host == original_host);
    check(&t, &row["initial"]);
    Arc::make_mut(&mut r.profile).maximum_steps = 1;
    assert!(
        matches!(s.pre_transform(&r,&v,&t,&mut host,b::Kind::Key,1,&config()),Err(b::Error::Chain(e)) if e.is_operational())
    );
    assert_eq!(s, b::State::default());
    assert!(host == original_host);
    Arc::make_mut(&mut r.profile).maximum_steps = 1_000_000;
    let mut t = t;
    for op in row["trace"].as_array().unwrap().iter().take(8) {
        t = apply(&r, row, op, &mut s, &mut host, &t);
    }
    let op = &row["pin_cases"][0];
    host = proof_host(op);
    let start = host.clone();
    let before = s.clone();
    let v = previous(row, op, &t);
    cfg.cache_limits.maximum_slots = s.leaders.schedule().len();
    assert!(matches!(
        s.pre_transform(&r, &v, &t, &mut host, b::Kind::Key, 6, &cfg),
        Err(b::Error::Leader(leader::Error::ResourceLimit))
    ));
    assert_eq!(s, before);
    assert!(host == start);
    check(&t, &row["pin_initial"]);
    host.proof = Err(HostError::Operational);
    assert!(matches!(
        s.pre_transform(&r, &v, &t, &mut host, b::Kind::Key, 6, &config()),
        Err(b::Error::Host(HostError::Operational))
    ));
    assert_eq!(s, before);
    assert!(host.pin_reads.is_empty());
    apply(&r, row, op, &mut s, &mut proof_host(op), &t);
}
