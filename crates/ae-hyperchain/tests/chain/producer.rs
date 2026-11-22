use super::*;
use ae_codec::headers::{CommonHeader, Header, HeaderMode, KeyHeader};
use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::{
    block::{self as b, Host as BlockHost},
    eoe::{self, coordinator, local_host},
    producer::{self as p, Host},
    signing::{self, Keys},
};
use ae_protocol::Activation;
use num_traits::ToPrimitive;
use std::collections::BTreeMap;
static DATA: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_hc_producer.json")));
type R = Reader<MemoryStore, MemoryStore, Otp26>;
fn signer(n: u64) -> Ed25519SecretKey {
    Ed25519SecretKey::from_seed(&key(n))
}
fn public(n: u64) -> AccountPublicKey {
    signer(n).public_key()
}
fn reader(row: &J) -> R {
    let mut p = super::profile(8);
    p.contracts = cc::Contracts {
        owner: AccountPublicKey(key(1)),
        staking: bytes(&DATA["staking"]).try_into().unwrap(),
        election: bytes(&DATA["election"]).try_into().unwrap(),
        rewards: bytes(&DATA["election"]).try_into().unwrap(),
    };
    p.network = NetworkProfile::custom(
        b"ae_hc_reference".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
        }],
    )
    .unwrap();
    p.order = otp_order::profile(&DATA["profile"]);
    p.maximum_steps = 1_000_000;
    R::new(chain::Selected::new(Default::default()), Arc::new(p))
}
fn cfg(c: &J) -> p::Config {
    p::Config {
        block_time: c["block_time"].as_i64().unwrap().into(),
        production_time: c["production_time"].as_i64().unwrap().into(),
        maximum_stall_reads: 1000,
        cache_limits: Default::default(),
    }
}
fn context(row: &J, step: &J, t: &Trees) -> chain::View {
    let j = &step["view"];
    let mut v = view(
        t.clone(),
        row["protocol"].as_u64().unwrap(),
        j["height"].as_u64().unwrap(),
    );
    v.environment.key_hash = bytes(&j["key_hash"]).try_into().unwrap();
    v.environment.timestamp = j["time"].as_i64().unwrap().into();
    v.environment.difficulty = j["difficulty"].as_i64().unwrap().into();
    v.environment.network_id = b"ae_hc_reference".to_vec();
    v
}
fn keys_json(keys: &Keys) -> J {
    json!({"active":keys.active().map(|k|bin(k.0)).unwrap_or(atom("not_set")),"candidate":keys.candidate().map(|k|bin(k.0)).unwrap_or(atom("not_set"))})
}
fn account(p: &ObjectPayload) -> Option<AccountPublicKey> {
    match p {
        ObjectPayload::Bytes(b) => b.as_slice().try_into().ok().map(AccountPublicKey),
        _ => None,
    }
}
#[derive(Debug, Clone)]
enum Fault {
    Operational,
    MissingTime,
}
#[derive(Clone)]
struct Probe {
    keys: Keys,
    missing: bool,
    time_reads: Vec<BigInt>,
    entropy_reads: Vec<BigInt>,
    events: Vec<J>,
    fail_negotiate: bool,
}
impl Probe {
    fn new(c: &J) -> Self {
        Self {
            keys: Keys::new(
                c["owned"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|n| signer(n.as_u64().unwrap())),
            ),
            missing: c["missing"].as_bool().unwrap(),
            time_reads: vec![],
            entropy_reads: vec![],
            events: vec![],
            fail_negotiate: false,
        }
    }
    fn clear(&mut self) {
        self.time_reads.clear();
        self.entropy_reads.clear();
        self.events.clear();
    }
    fn reads(&self) -> J {
        json!({"time_reads":list(self.time_reads.iter().map(int).collect()),"entropy_reads":list(self.entropy_reads.iter().map(int).collect()),"events":list(self.events.clone())})
    }
}
impl BlockHost for Probe {
    type Error = Fault;
    fn is_operational(e: &Fault) -> bool {
        matches!(e, Fault::Operational)
    }
    fn entropy(&mut self, e: &BigInt) -> Result<Option<Vec<u8>>, Fault> {
        let h: BigInt = (e - BigInt::from(3)).max(0.into()) * 5 + 1;
        self.entropy_reads.push(h.clone());
        if self.missing {
            return Ok(None);
        }
        let n = h.to_u64().unwrap();
        self.events.push(tuple(vec![
            atom("parent"),
            int(e),
            tuple(vec![
                atom("block"),
                bin(key(1000 + n)),
                int(h),
                bin(key(999 + n)),
                int(n * 100),
            ]),
        ]));
        Ok(Some(key(1000 + n).to_vec()))
    }
    fn pin(&mut self, _: &[u8]) -> Result<Option<b::Pin>, Fault> {
        panic!("producer does not read pins")
    }
    fn key_hash(&mut self, _: &BigInt) -> Result<Option<Vec<u8>>, Fault> {
        panic!("producer does not read pin heights")
    }
}
impl Host for Probe {
    fn key_time(&mut self, h: &BigInt) -> Result<BigInt, Fault> {
        self.time_reads.push(h.clone());
        if h < &0.into() || h > &20.into() {
            Err(Fault::MissingTime)
        } else {
            Ok(h * 1000 + 9000)
        }
    }
    fn key_present(&mut self, k: &ObjectPayload) -> Result<bool, Fault> {
        Ok(account(k).is_some_and(|k| self.keys.contains(k)))
    }
    fn set_candidate(&mut self, k: &ObjectPayload) -> Result<bool, Fault> {
        Ok(account(k).is_some_and(|k| self.keys.set_candidate(k).is_ok()))
    }
    fn negotiate(&mut self, n: p::Negotiation) -> Result<(), Fault> {
        if self.fail_negotiate {
            return Err(Fault::Operational);
        }
        self.events.push(tuple(vec![
            atom("negotiate"),
            value(&n.info.epoch),
            int(n.slot),
            bin(n.hash),
            object(&n.leader),
            optional(n.info.validators, |vs| {
                list(
                    vs.iter()
                        .map(|s| tuple(vec![object(&s.address), value(&s.stake)]))
                        .collect(),
                )
            }),
            optional(n.info.seed, bin),
            int(n.info.length),
        ]));
        Ok(())
    }
}
fn decision(d: p::Decision) -> J {
    ok(match d {
        p::Decision::Wait { reason, after } => tuple(vec![
            atom("wait"),
            atom(match reason {
                p::Wait::SlotFilled => "slot_filled",
                p::Wait::NotProducer => "not_producer",
            }),
            int(after),
        ]),
        p::Decision::RetryNotInCache { after } => {
            tuple(vec![atom("error"), atom("not_in_cache"), int(after)])
        }
        p::Decision::Produce {
            leader,
            holes,
            after,
        } => tuple(vec![
            atom("ok"),
            match holes {
                None => tuple(vec![atom("leader"), object(&leader)]),
                Some(n) => tuple(vec![atom("leader_hole"), object(&leader), int(n)]),
            },
            int(after),
        ]),
    })
}
fn slot(s: p::Slot) -> J {
    let mut fields = vec![
        ("epoch", value(&s.epoch)),
        ("slot", int(s.slot)),
        ("t0", int(s.t0)),
        ("delta", int(s.delta)),
        ("now", int(s.now)),
    ];
    if let Some(i) = s.epoch_info {
        fields.push(("epoch_info", epoch_json(i)));
    }
    ok(named(fields))
}
fn badarith() -> J {
    exception(native::Exception::error(Term::Atom("badarith")))
}
fn run_case(r: &R, row: &J, c: &J, t: &Trees) {
    let mut state = b::State::default();
    let mut host = Probe::new(c);
    let cfg = cfg(c);
    let roots = t.roots();
    for step in c["steps"].as_array().unwrap() {
        let v = context(row, step, t);
        let h = step["top"].as_i64().unwrap().into();
        let now = step["now"].as_i64().unwrap().into();
        let s = match p::slot_info(r, &v, &h, &now, &cfg.block_time, &mut host) {
            Ok(s) => slot(s),
            Err(p::Error::Badarith) => badarith(),
            Err(e) => panic!("slot {e:?}"),
        };
        assert_eq!(normalize(&s), normalize(&step["slot"]), "{}", c["name"]);
        host.clear();
        let d = match p::next(&mut state, r, &v, &h, &now, &mut host, &cfg) {
            Ok(d) => decision(d),
            Err(p::Error::Badarith) => badarith(),
            Err(e) => panic!("producer {e:?}"),
        };
        assert_eq!(d, step["result"], "{}", c["name"]);
        assert_eq!(keys_json(&host.keys), step["keys"]);
        assert_eq!(
            normalize(&super::block::cache(&state)),
            normalize(&step["cache"])
        );
        assert_eq!(normalize(&host.reads()), normalize(&step["reads"]));
        assert_eq!(t.roots(), roots);
    }
}
#[test]
fn original_producer_clocks_stalls_negotiation_and_keys_match() {
    for row in DATA["rows"].as_array().unwrap() {
        let r = reader(row);
        let t = trees(&row["initial"]);
        for c in row["cases"].as_array().unwrap() {
            run_case(&r, row, c, &t)
        }
        run_case(&r, row, &row["second_case"], &trees(&row["second"]));
        run_case(&r, row, &row["one_case"], &trees(&row["one"]));
    }
}
fn result(r: Result<(), signing::Error>) -> J {
    ok(match r {
        Ok(()) => atom("ok"),
        Err(signing::Error::Signature) => {
            tuple(vec![atom("error"), atom("signature_verification_failed")])
        }
        Err(signing::Error::Padding(p)) => tuple(vec![
            atom("error"),
            tuple(vec![
                atom("erroneous_seal"),
                list(p.iter().map(int).collect()),
                list(vec![int(0); 26]),
                list(
                    std::iter::once(int(1))
                        .chain(std::iter::repeat_n(int(0), 25))
                        .collect(),
                ),
            ]),
        ]),
        Err(e) => panic!("{e:?}"),
    })
}
fn decode(j: &J) -> Header {
    Header::decode(&bytes(j), HeaderMode::Hyperchain).unwrap()
}
#[test]
fn original_header_seals_micro_signatures_and_key_selection_match() {
    for row in DATA["rows"].as_array().unwrap() {
        let data = &row["signing"];
        let mut keys = Keys::new([signer(12), signer(13)]);
        for c in data["seals"].as_array().unwrap() {
            let Header::Key(mut h) = decode(&c["unsigned"]) else {
                panic!("key")
            };
            assert_eq!(
                Header::Key(h.clone()).signature_bytes().unwrap(),
                bytes(&c["signature_bytes"])
            );
            let signing::Seal::Found(seal) = keys.seal(&h).unwrap() else {
                panic!("seal")
            };
            assert_eq!(json!(seal.to_vec()), c["seal"]);
            h.seal = seal;
            assert_eq!(Header::Key(h).encode().unwrap(), bytes(&c["signed"]));
            for check in c["checks"].as_array().unwrap() {
                let Header::Key(h) = decode(&check["header"]) else {
                    panic!("key")
                };
                assert_eq!(result(signing::verify_key(&h)), check["result"]);
            }
        }
        for c in data["key_ops"].as_array().unwrap() {
            let v = match c["name"].as_str().unwrap() {
                "set12" => {
                    keys.set_candidate(public(12)).unwrap();
                    ok(atom("ok"))
                }
                "set_missing" => {
                    assert_eq!(
                        keys.set_candidate(public(99)),
                        Err(signing::Error::MissingKey)
                    );
                    ok(tuple(vec![atom("error"), atom("key_not_found")]))
                }
                "promote13" => {
                    keys.promote(public(13)).unwrap();
                    ok(atom("ok"))
                }
                "sign12" => reply(bin(keys.sign(public(12), &[1, 2, 3]).unwrap().0)),
                "sign_missing" => {
                    assert_eq!(
                        keys.sign(public(99), &[1, 2, 3]),
                        Err(signing::Error::MissingKey)
                    );
                    ok(tuple(vec![atom("error"), atom("not_found")]))
                }
                "promote_missing" => {
                    assert_eq!(keys.promote(public(99)), Err(signing::Error::MissingKey));
                    ok(tuple(vec![atom("error"), atom("key_not_found")]))
                }
                n => panic!("{n}"),
            };
            assert_eq!(v, c["result"]);
            assert_eq!(keys_json(&keys), c["state"]);
        }
        let Header::Key(mut unknown) = decode(&data["seals"][0]["unsigned"]) else {
            panic!("key")
        };
        unknown.beneficiary = public(99).0;
        assert_eq!(
            keys.seal(&unknown).unwrap(),
            signing::Seal::NoSolution { retry_ms: 1000 }
        );
        assert_eq!(
            data["missing"],
            ok(tuple(vec![
                atom("continue_mining"),
                tuple(vec![atom("error"), atom("no_solution")])
            ]))
        );
        for c in data["micro"].as_array().unwrap() {
            let Header::Micro(h) = decode(&c["unsigned"]) else {
                panic!("micro")
            };
            let _ = keys.promote(public(99));
            assert_eq!(keys.sign_micro(&h, None), Err(signing::Error::NoActive));
            assert_eq!(
                c["absent"],
                ok(tuple(vec![atom("error"), atom("no_active")]))
            );
            keys.promote(public(13)).unwrap();
            assert_eq!(
                Header::Micro(keys.sign_micro(&h, None).unwrap())
                    .encode()
                    .unwrap(),
                bytes(&c["default_signed"])
            );
            assert_eq!(
                Header::Micro(h.clone()).signature_bytes().unwrap(),
                bytes(&c["signature_bytes"])
            );
            let signed = keys.sign_micro(&h, Some(public(12))).unwrap();
            assert_eq!(
                Header::Micro(signed.clone()).encode().unwrap(),
                bytes(&c["signed"])
            );
            assert_eq!(
                result(signing::verify_micro(&signed, public(12))),
                c["valid"]
            );
            assert_eq!(
                result(signing::verify_micro(&signed, public(13))),
                c["wrong"]
            );
        }
    }
}
#[test]
fn original_micro_validation_time_signature_order_and_cache_effects_match() {
    for row in DATA["rows"].as_array().unwrap() {
        let r = reader(row);
        let t = trees(&row["initial"]);
        let mut state = b::State::default();
        let mut host = Probe::new(&row["cases"][0]);
        for c in row["micro_checks"].as_array().unwrap() {
            if c["reset"] == true {
                state = b::State::default();
            }
            host.clear();
            host.missing = c["missing"].as_bool().unwrap();
            let Header::Micro(h) = decode(&c["header"]) else {
                panic!("micro")
            };
            let v = context(row, c, &t);
            r.selected.replace(chain::Selection {
                views: BTreeMap::from([(h.common.previous_hash.to_vec(), Arc::new(v))]),
                ..Default::default()
            });
            let out = match signing::validate_micro(
                &mut state,
                &r,
                &h,
                c["previous_time"].as_u64().unwrap(),
                &mut host,
                Default::default(),
            ) {
                Ok(()) => ok(atom("ok")),
                Err(signing::MicroError::TimeTooLow) => {
                    ok(tuple(vec![atom("error"), atom("micro_block_time_too_low")]))
                }
                Err(signing::MicroError::Signing(e)) => result(Err(e)),
                Err(e) => panic!("{e:?}"),
            };
            assert_eq!(out, c["result"]);
            assert_eq!(
                normalize(&super::block::cache(&state)),
                normalize(&c["cache"])
            );
            assert_eq!(normalize(&host.reads()), normalize(&c["reads"]));
            check(&t, &row["initial"]);
        }
    }
}
fn case<'a>(row: &'a J, name: &str) -> &'a J {
    row["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == name)
        .unwrap()
}
#[test]
fn producer_resource_failures_do_not_publish_cache_key_or_negotiation_effects() {
    let row = &DATA["rows"][2];
    let mut r = reader(row);
    let t = trees(&row["initial"]);
    let c = case(row, "tick");
    let step = &c["steps"][0];
    let v = context(row, step, &t);
    let mut state = b::State::default();
    let mut host = Probe::new(c);
    let mut config = cfg(c);
    config.cache_limits.maximum_slots = 3;
    assert!(matches!(
        p::next(
            &mut state,
            &r,
            &v,
            &1.into(),
            &10000.into(),
            &mut host,
            &config
        ),
        Err(p::Error::Block(b::Error::Leader(
            ae_hyperchain::leader::Error::ResourceLimit
        )))
    ));
    assert_eq!(state, b::State::default());
    assert!(
        host.events.is_empty() && host.time_reads.is_empty() && host.keys.candidate().is_none()
    );
    config = cfg(c);
    Arc::make_mut(&mut r.profile).maximum_steps = 1;
    assert!(
        matches!(p::next(&mut state,&r,&v,&1.into(),&10000.into(),&mut host,&config),Err(p::Error::Chain(e)) if e.is_operational())
    );
    Arc::make_mut(&mut r.profile).maximum_steps = 1_000_000;
    config.maximum_stall_reads = 0;
    assert!(matches!(
        p::next(
            &mut state,
            &r,
            &v,
            &1.into(),
            &15000.into(),
            &mut host,
            &config
        ),
        Err(p::Error::ResourceLimit)
    ));
    assert_eq!(state, b::State::default());
    assert!(host.events.is_empty());
    config = cfg(c);
    host.fail_negotiate = true;
    assert!(matches!(
        p::next(
            &mut state,
            &r,
            &v,
            &2.into(),
            &11000.into(),
            &mut host,
            &config
        ),
        Err(p::Error::Host(Fault::Operational))
    ));
    assert_eq!(state, b::State::default());
    assert!(host.events.is_empty() && host.keys.candidate().is_none());
    host.fail_negotiate = false;
    assert!(matches!(
        p::next(
            &mut state,
            &r,
            &v,
            &2.into(),
            &11000.into(),
            &mut host,
            &config
        ),
        Ok(p::Decision::Produce { .. })
    ));
    assert!(host.events.iter().any(|e| e[1][0] == atom("negotiate")));
    check(&t, &row["initial"]);
}
#[derive(Clone)]
struct NoPins;
impl b::host::PinSource for NoPins {
    type Error = ();
    fn is_operational(_: &()) -> bool {
        true
    }
    fn get_pin(&mut self, _: &[u8]) -> Result<Option<b::Pin>, ()> {
        Ok(None)
    }
}
fn header(h: u64, p: u32) -> KeyHeader {
    KeyHeader {
        common: CommonHeader {
            version: p,
            height: h,
            previous_hash: key(0),
            previous_key_hash: key(0),
            state_hash: key(0),
        },
        miner: key(0),
        beneficiary: key(0),
        target: h as u32,
        seal: [0; 42],
        nonce: 0,
        time: 9000 + h * 1000,
        info: None,
        hole: false,
        end_of_epoch: false,
    }
}
fn live(r: &R, row: &J, t: &Trees, owned: &[u64]) -> p::host::LiveHost<NoPins> {
    let mut host = local_host::LocalHost::new(
        local_host::Config {
            network: r.profile.network.clone(),
            election_contract: r.profile.contracts.election,
            parent_epoch_length: 5.into(),
            parent_start_height: 1.into(),
            limits: Default::default(),
        },
        local_host::SelectedTip {
            trees: Some(Arc::new(t.clone())),
            protocol: Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
            consensus: ConsensusType::Pos,
        },
        owned
            .iter()
            .map(|n| {
                let k = signer(*n);
                (k.public_key(), k)
            })
            .collect(),
    );
    host.insert_parent(ae_hyperchain::parent_block::ParentBlock {
        hash: key(1001).to_vec(),
        height: 1.into(),
        previous_hash: Some(key(1000).to_vec()),
        time: 100.into(),
    })
    .unwrap();
    p::host::LiveHost::new(
        coordinator::Coordinator::new(host, 1000.into(), Default::default()),
        r.selected.clone(),
        Arc::new(
            (0..=20)
                .map(|h| {
                    (
                        BigInt::from(h),
                        header(h, row["protocol"].as_u64().unwrap() as u32),
                    )
                })
                .collect(),
        ),
        NoPins,
    )
}
#[test]
fn live_producer_routes_entropy_and_negotiation_to_both_eoe_workers() {
    let row = &DATA["rows"][2];
    let r = reader(row);
    let t = trees(&row["initial"]);
    for owned in [&[12, 13, 14][..], &[][..]] {
        let c = case(row, if owned.is_empty() { "no_keys" } else { "tick" });
        let step = c["steps"].as_array().unwrap().last().unwrap();
        let v = context(row, step, &t);
        let mut host = live(&r, row, &t, owned);
        let mut state = b::State::default();
        let d = p::next(
            &mut state,
            &r,
            &v,
            &2.into(),
            &11000.into(),
            &mut host,
            &cfg(c),
        )
        .unwrap();
        assert_eq!(decision(d), step["result"]);
        assert_eq!(host.coordinator.queued_events(), 6);
        assert_eq!(keys_json(&host.keys), step["keys"]);
        host.coordinator.run(&r.profile.order, 1000).unwrap();
        for mode in [eoe::Mode::Fork, eoe::Mode::Length] {
            let m = host.coordinator.machine(mode);
            assert_eq!(m.epoch(), Some(&1.into()));
            assert_eq!(m.height(), Some(&3.into()));
            assert_eq!(m.parents().get(&1.into()).unwrap().hash, key(1001));
            assert_eq!(m.parents().get(&2.into()).unwrap().hash, key(1001));
            assert_ne!(m.state(), eoe::State::AwaitEpoch);
        }
        if !owned.is_empty() {
            let leader = host.keys.candidate().unwrap();
            let mut candidate = header(3, 8);
            candidate.miner = leader.0;
            candidate.beneficiary = leader.0;
            let signing::Seal::Found(seal) = host.keys.seal(&candidate).unwrap() else {
                panic!("seal")
            };
            candidate.seal = seal;
            signing::verify_key(&candidate).unwrap();
        }
    }
}
#[test]
fn live_native_failure_preserves_prefix_and_queue_pressure_is_retryable() {
    let row = &DATA["rows"][2];
    let r = reader(row);
    let t = trees(&row["one"]);
    let c = &row["one_case"];
    let step = &c["steps"][0];
    let v = context(row, step, &t);
    let config = cfg(c);
    let mut host = live(&r, row, &t, &[12, 13, 14]);
    let mut state = b::State::default();
    let mut small = host.coordinator.config();
    small.maximum_queued_events = 4;
    host.coordinator.set_config(small);
    assert!(matches!(
        p::next(
            &mut state,
            &r,
            &v,
            &0.into(),
            &9000.into(),
            &mut host,
            &config
        ),
        Err(p::Error::Host(p::host::Error::Coordinator(
            coordinator::Error::ResourceLimit
        )))
    ));
    assert_eq!(state, b::State::default());
    assert_eq!(host.coordinator.queued_events(), 0);
    assert_eq!(host.keys.candidate(), None);
    host.coordinator.set_config(Default::default());
    assert!(matches!(
        p::next(
            &mut state,
            &r,
            &v,
            &0.into(),
            &9000.into(),
            &mut host,
            &config
        ),
        Err(p::Error::Badarith)
    ));
    assert_eq!(host.coordinator.queued_events(), 6);
    assert_eq!(
        normalize(&super::block::cache(&state)),
        normalize(&step["cache"])
    );
    assert_eq!(host.keys.candidate(), None);
    check(&t, &row["one"]);
}
