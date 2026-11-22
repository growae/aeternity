use ae_execution::{Context, contracts, transactions};
use ae_hyperchain::{
    chain::{self, Reader, RunEnv},
    consensus_contract::{self as cc, Contract},
};
use ae_protocol::{ConsensusType, NetworkProfile, Protocol};
use ae_state::Trees;
use ae_trie::{MemoryStore, Nibbles, Trie, Value as TrieValue};
use ae_types::{AccountPublicKey, Hash256};
use ae_vm_fate::{
    Limits, encoding,
    machine::{
        self,
        program::remote::context::{Consensus, Environment},
    },
    maps::order::Otp26,
    native::{self, Term},
    value::{ObjectKind, ObjectPayload, Value},
};
use num_bigint::BigInt;
use serde_json::{Value as J, json};
use std::{
    sync::{Arc, LazyLock, Mutex},
    thread,
    time::{Duration, Instant},
};
#[path = "chain/block.rs"]
mod block;
#[path = "chain/durable.rs"]
mod durable;
#[path = "chain/genesis.rs"]
mod genesis;
#[path = "chain/ledger.rs"]
mod ledger;
#[path = "../../ae-vm-fate/tests/support/otp_order.rs"]
mod otp_order;
#[path = "chain/producer.rs"]
mod producer;
fn unpack(s: &str) -> J {
    fn u(v: &J, p: &[J]) -> J {
        match v {
            J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                u(&p[v["$ref"].as_u64().unwrap() as usize], p)
            }
            J::Object(m) => J::Object(m.iter().map(|(k, v)| (k.clone(), u(v, p))).collect()),
            J::Array(a) => J::Array(a.iter().map(|v| u(v, p)).collect()),
            v => v.clone(),
        }
    }
    let d: J = serde_json::from_str(s).unwrap();
    u(&d["root"], d["values"].as_array().unwrap())
}
static DATA: LazyLock<J> = LazyLock::new(|| unpack(include_str!("fixtures/erlang_hc_chain.json")));
fn bytes(v: &J) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn key(n: u64) -> [u8; 32] {
    let mut k = [0; 32];
    k[24..].copy_from_slice(&n.to_be_bytes());
    k
}
fn atom(s: &str) -> J {
    json!(["atom", s])
}
fn int(n: impl ToString) -> J {
    json!(["integer", n.to_string()])
}
fn bin(b: impl AsRef<[u8]>) -> J {
    json!(["binary", hex::encode(b)])
}
fn tuple(xs: Vec<J>) -> J {
    json!(["tuple", xs])
}
fn list(xs: Vec<J>) -> J {
    json!(["list", xs])
}
fn named(xs: Vec<(&str, J)>) -> J {
    json!([
        "map",
        xs.into_iter()
            .map(|(k, v)| json!([atom(k), v]))
            .collect::<Vec<_>>()
    ])
}
fn ok(v: J) -> J {
    json!({"kind":"ok","value":v})
}
fn reply(v: J) -> J {
    ok(tuple(vec![atom("ok"), v]))
}
fn normalize(v: &J) -> J {
    if let Some(a) = v.as_array() {
        if a.len() == 2 && a[0] == "map" {
            let mut xs: Vec<_> = a[1].as_array().unwrap().iter().map(normalize).collect();
            xs.sort_by_key(|x| x[0].to_string());
            return json!(["map", xs]);
        }
        return J::Array(a.iter().map(normalize).collect());
    }
    if let Some(m) = v.as_object() {
        return J::Object(m.iter().map(|(k, v)| (k.clone(), normalize(v))).collect());
    }
    v.clone()
}
fn object(v: &ObjectPayload) -> J {
    match v {
        ObjectPayload::Bytes(b) => bin(b),
        ObjectPayload::List(xs) => list(xs.iter().map(object).collect()),
    }
}
fn value(v: &Value) -> J {
    match v {
        Value::Integer(n) => int(n),
        Value::Boolean(b) => atom(if *b { "true" } else { "false" }),
        Value::String(b) => bin(b),
        Value::Bytes(b) => tuple(vec![atom("bytes"), bin(b)]),
        Value::Tuple(xs) => tuple(vec![atom("tuple"), tuple(xs.iter().map(value).collect())]),
        Value::List(xs) => list(xs.iter().map(value).collect()),
        Value::Object(k, p) => tuple(vec![
            atom(match k {
                ObjectKind::Address => "address",
                ObjectKind::Contract => "contract",
                ObjectKind::Oracle => "oracle",
                ObjectKind::OracleQuery => "oracle_query",
                ObjectKind::Channel => "channel",
            }),
            object(p),
        ]),
        Value::Variant {
            arities,
            tag,
            values,
        } => tuple(vec![
            atom("variant"),
            list(arities.iter().map(int).collect()),
            int(tag),
            tuple(values.iter().map(value).collect()),
        ]),
        Value::Map(m) => json!([
            "map",
            m.iter()
                .map(|(k, v)| json!([value(k), value(v)]))
                .collect::<Vec<_>>()
        ]),
        v => panic!("unhandled {v:?}"),
    }
}
fn term(t: &Term) -> J {
    match t {
        Term::Atom(s) => atom(s),
        Term::Integer(n) => int(n),
        Term::Binary(b) => bin(b),
        Term::Tuple(xs) => tuple(xs.iter().map(term).collect()),
        Term::List(xs) => list(xs.iter().map(term).collect()),
        Term::Data(v) => value(v),
        _ => panic!("unsupported {t:?}"),
    }
}
fn exception(e: native::Exception) -> J {
    json!({"kind":"exception","class":match e.class{native::Class::Error=>"error",native::Class::Exit=>"exit"},"reason":term(&e.reason)})
}
fn native(n: chain::Native) -> J {
    exception(native::Exception::error(match n {
        chain::Native::FunctionClause => Term::Atom("function_clause"),
        chain::Native::Badarith => Term::Atom("badarith"),
        chain::Native::Badmatch(v) => Term::tagged("badmatch", vec![Term::Data(Box::new(v))]),
        chain::Native::BadmatchQuery(v) => Term::tagged(
            "badmatch",
            vec![Term::tagged("ok", vec![Term::Data(Box::new(v))])],
        ),
        chain::Native::BadmatchChainTooShort => Term::tagged(
            "badmatch",
            vec![Term::tagged("error", vec![Term::Atom("chain_too_short")])],
        ),
    }))
}
fn chain_error(e: chain::Error) -> J {
    match e {
        chain::Error::ChainTooShort => ok(tuple(vec![atom("error"), atom("chain_too_short")])),
        chain::Error::Native(n) => native(n),
        e => panic!("unexpected {e:?}"),
    }
}
fn tree(rows: &J) -> Trie<MemoryStore> {
    let mut t = Trie::new(MemoryStore::default());
    for r in rows.as_array().unwrap() {
        t.put(
            &Nibbles::from_bytes(&bytes(&r["key"])),
            TrieValue::Bytes(bytes(&r["encoded"])),
        )
        .unwrap();
    }
    t
}
fn trees(j: &J) -> Trees {
    let a = tree(&j["accounts"]);
    let c = tree(&j["contracts"]);
    let calls = tree(&j["calls"]);
    Trees {
        accounts: ae_state::accounts::Accounts::from_root(a.root_hash(), a.store().clone())
            .unwrap(),
        contracts: ae_state::contracts::Contracts::from_root(c.root_hash(), c.store().clone())
            .unwrap(),
        calls: ae_state::calls::Calls::from_root(calls.root_hash(), calls.store().clone()).unwrap(),
        channels: Default::default(),
        names: Default::default(),
        oracles: Default::default(),
    }
}
fn check(t: &Trees, j: &J) {
    let roots = t.roots();
    for (k, v) in [
        ("account_root", roots.accounts),
        ("contract_root", roots.contracts),
        ("call_root", roots.calls),
        ("channel_root", roots.channels),
        ("name_root", roots.names),
        ("oracle_root", roots.oracles),
    ] {
        assert_eq!(
            v.map(|h| json!(hex::encode(h.0))).unwrap_or(J::Null),
            j[k],
            "{k}"
        );
    }
    assert_eq!(hex::encode(roots.hash().0), j["state_root"]);
}
fn contracts() -> cc::Contracts {
    cc::Contracts {
        owner: AccountPublicKey(key(1)),
        election: key(2),
        staking: key(3),
        rewards: key(4),
    }
}
fn profile(v: u64) -> chain::Profile<Otp26> {
    chain::Profile {
        contracts: contracts(),
        network: NetworkProfile::mainnet(),
        context: machine::Context {
            current_contract: key(2),
            vm_version: match v {
                5 => ae_vm_fate::types::Version::One,
                7 => ae_vm_fate::types::Version::Two,
                _ => ae_vm_fate::types::Version::Three,
            },
            in_auth: false,
            on_chain: true,
            reserved_gas: 0.into(),
            arithmetic_word_size: ae_vm_fate::maps::order::WordSize::Bits64,
            unicode_profile: ae_vm_fate::unicode::Profile::Otp26Unicode15,
            bls_profile: ae_crypto::bls::Profile::Mcl11976beLlvm64VintArm64,
        },
        order: otp_order::profile(&DATA["profile"]),
        overrides: Default::default(),
        diagnostic: ae_vm_fate::diagnostic::Profile::Release,
        limits: Limits::default(),
        maximum_steps: 100000,
        maximum_history_reads: 1000,
    }
}
fn view(t: Trees, p: u64, h: u64) -> chain::View {
    chain::View {
        trees: Arc::new(t),
        context: Context::transaction(h, Protocol::try_from(p as u32).unwrap()),
        environment: Environment {
            auth_tx_hash: None,
            origin: key(1),
            gas_price: 0.into(),
            fee: 0.into(),
            beneficiary: key(0),
            timestamp: 0.into(),
            difficulty: 0.into(),
            block_gas_limit: 6_000_000.into(),
            network_id: b"ae_mainnet".to_vec(),
            consensus: Consensus::Pos,
            key_hash: key(0),
        },
        history: Default::default(),
        aevm: None,
    }
}
fn reader() -> Reader<MemoryStore, MemoryStore, Otp26> {
    Reader::new(
        chain::Selected::new(Default::default()),
        Arc::new(profile(8)),
    )
}
fn with_execution<T>(
    p: &chain::Profile<Otp26>,
    v: &chain::View,
    f: impl FnOnce(&transactions::Config<'_, Otp26>) -> T,
) -> T {
    let vm = contracts::fate::Config {
        context: &p.context,
        order: &p.order,
        overrides: &p.overrides,
        environment: &v.environment,
        history: v.history.clone(),
        auth_tx: None,
        metering_floor: None,
        diagnostic_profile: p.diagnostic,
    };
    let mut config = transactions::Config::primitives(&p.network);
    config.consensus = ConsensusType::Pos;
    config.fate = Some(&vm);
    config.fate_limits = p.limits;
    config.maximum_fate_steps = p.maximum_steps;
    f(&config)
}
fn optional<T>(o: Option<T>, f: impl FnOnce(T) -> J) -> J {
    o.map(f).unwrap_or(atom("undefined"))
}
fn epoch_json(i: chain::EpochInfo) -> J {
    named(vec![
        ("epoch", value(&i.epoch)),
        ("first", int(i.first)),
        ("length", int(i.length)),
        ("last", int(i.last)),
        ("seed", optional(i.seed, bin)),
        (
            "validators",
            optional(i.validators, |v| {
                list(
                    v.into_iter()
                        .map(|s| tuple(vec![object(&s.address), value(&s.stake)]))
                        .collect(),
                )
            }),
        ),
    ])
}
fn final_json(i: chain::FinalizeInfo) -> J {
    named(vec![
        ("epoch", value(&i.epoch)),
        ("fork", optional(i.fork, bin)),
        ("epoch_length", optional(i.epoch_length, |v| value(&v))),
        ("pc_hash", bin(i.parent_hash)),
        ("producer", object(&i.producer)),
        (
            "votes",
            optional(i.votes, |vs| {
                list(
                    vs.into_iter()
                        .map(|v| {
                            named(vec![
                                ("producer", object(&v.producer)),
                                ("hash", bin(v.hash)),
                                ("signature", bin(v.signature)),
                            ])
                        })
                        .collect(),
                )
            }),
        ),
        (
            "length_votes",
            optional(i.length_votes, |vs| {
                list(
                    vs.into_iter()
                        .map(|v| {
                            named(vec![
                                ("producer", object(&v.producer)),
                                ("epoch_delta", value(&v.epoch_delta)),
                                ("signature", bin(v.signature)),
                            ])
                        })
                        .collect(),
                )
            }),
        ),
    ])
}
#[test]
fn original_consensus_contract_calls_match_funding_state_pruning_and_returns() {
    for row in DATA["runs"].as_array().unwrap() {
        let t = trees(&row["initial"]);
        check(&t, &row["initial"]);
        let v = view(t.clone(), row["protocol"].as_u64().unwrap(), 50);
        let p = profile(row["vm"].as_u64().unwrap());
        let prepared = cc::prepare(
            contracts(),
            Contract::Election,
            &t,
            bytes(&row["calldata"]),
            0.into(),
        )
        .unwrap();
        let result = with_execution(&p, &v, |config| cc::execute(prepared, &v.context, config));
        match result {
            Ok(done) => {
                assert_eq!(row["direct"]["kind"], "ok");
                check(&done.trees, &row["direct"]["trees"]);
                assert_eq!(
                    hex::encode(done.call.serialize().unwrap()),
                    row["direct"]["call"]
                );
                assert_eq!(hex::encode(done.call.id), row["direct"]["call_id"]);
                assert_eq!(done.trees.calls.root_hash(), None);
            }
            Err(e) => {
                assert_eq!(
                    normalize(&exception(
                        e.return_exception().unwrap_or_else(|| panic!("{e:?}"))
                    )),
                    normalize(&row["direct"])
                );
            }
        }
        let args = row["arguments"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| encoding::decode(&bytes(v), Limits::default()).unwrap())
            .collect();
        let query = with_execution(&p, &v, |config| {
            cc::query(
                contracts(),
                Contract::Election,
                &t,
                &v.context,
                config,
                b"probe",
                args,
                Limits::default(),
            )
        });
        let actual = match query {
            Ok(v) => reply(value(&v)),
            Err(e) => exception(e.return_exception().unwrap_or_else(|| panic!("{e:?}"))),
        };
        assert_eq!(normalize(&actual), normalize(&row["query"]));
        check(&t, &row["initial"]);
    }
}
#[test]
fn original_chain_query_endpoints_decode_contract_results() {
    let r = reader();
    let env = RunEnv::View(Arc::new(view(trees(&DATA["queries"]["initial"]), 6, 50)));
    for row in DATA["queries"]["rows"].as_array().unwrap() {
        let actual = match row["name"].as_str().unwrap() {
            "epoch" => reply(value(&r.epoch(&env).unwrap())),
            "epoch_length" => reply(value(&r.epoch_length(&env).unwrap())),
            "epoch_info" => reply(epoch_json(r.epoch_info(&env).unwrap())),
            "epoch_info_epoch" => reply(epoch_json(
                r.epoch_info_for_epoch(&env, Value::Integer(5.into()))
                    .unwrap(),
            )),
            "validators" => reply(list(
                r.validators(&env)
                    .unwrap()
                    .iter()
                    .map(|v| tuple(v.iter().map(value).collect()))
                    .collect(),
            )),
            "schedule" => reply(list(
                r.validator_schedule(
                    &env,
                    key(8).to_vec(),
                    vec![
                        chain::Staker {
                            address: ObjectPayload::Bytes(key(1).to_vec()),
                            stake: Value::Integer(4.into()),
                        },
                        chain::Staker {
                            address: ObjectPayload::Bytes(key(2).to_vec()),
                            stake: Value::Integer(3.into()),
                        },
                    ],
                    Value::Integer(3.into()),
                )
                .unwrap()
                .iter()
                .map(object)
                .collect(),
            )),
            "pin" => ok(optional(r.pin_info(&env).unwrap(), |v| value(&v))),
            "reward" => {
                let p = r.pin_reward(&env).unwrap();
                ok(named(vec![
                    ("base_pin_reward", value(&p.base)),
                    ("cur_pin_reward", value(&p.current)),
                    ("carry_over_pin_reward", value(&p.carry_over)),
                ]))
            }
            "finalize" => ok(optional(r.finalize_info(&env).unwrap(), final_json)),
            s => panic!("{s}"),
        };
        assert_eq!(normalize(&actual), normalize(&row["result"]));
    }
}
#[test]
fn original_bad_epoch_info_keeps_the_complete_query_badmatch() {
    let row = &DATA["queries"]["bad_epoch_info"];
    let r = reader();
    let env = RunEnv::View(Arc::new(view(trees(&row["initial"]), 6, 50)));
    assert_eq!(
        normalize(&chain_error(r.epoch_info(&env).unwrap_err())),
        normalize(&row["result"])
    );
}
#[test]
fn original_epoch_and_finalization_decoders_preserve_values_and_native_failures() {
    for group in ["epochs", "finals"] {
        for row in DATA["decoders"][group].as_array().unwrap() {
            let v = encoding::decode(&bytes(&row["encoded"]), Limits::default()).unwrap();
            let result = if group == "epochs" {
                chain::decode_epoch_info(Value::Integer(7.into()), &v).map(epoch_json)
            } else {
                chain::decode_finalize(&v).map(final_json)
            };
            let actual = match result {
                Ok(v) => ok(v),
                Err(n) => native(n),
            };
            assert_eq!(normalize(&actual), normalize(&row["result"]));
        }
    }
}
fn selection() -> chain::Selection {
    let mut s = chain::Selection {
        top: Some(80.into()),
        ..Default::default()
    };
    for row in DATA["selection"]["views"].as_array().unwrap() {
        let h = row["height"].as_u64().unwrap();
        let hash = bytes(&row["hash"]);
        s.heights.insert(h.into(), hash.clone());
        let mut v = view(trees(&row["trees"]), 6, h);
        v.environment.timestamp = 1.into();
        v.environment.key_hash = hash.as_slice().try_into().unwrap();
        s.views.insert(hash, Arc::new(v));
    }
    s
}
#[test]
fn original_selection_epoch_walk_and_signed_parent_targets_match() {
    use ae_hyperchain::parent_cache::runtime::Targets;
    let r = reader();
    r.selected.replace(selection());
    for row in DATA["selection"]["rows"].as_array().unwrap() {
        let n = row["name"].as_str().unwrap();
        if n.starts_with("empty") {
            r.selected.replace(Default::default());
        }
        let result = match n {
            "top" | "empty_top" => r.epoch(&RunEnv::Top).map(|v| reply(value(&v))),
            "height" => r
                .epoch(&RunEnv::Height(69.into()))
                .map(|v| reply(value(&v))),
            "hash" => r
                .epoch(&RunEnv::Hash(key(59).to_vec()))
                .map(|v| reply(value(&v))),
            "missing_height" => r
                .epoch(&RunEnv::Height(99.into()))
                .map(|v| reply(value(&v))),
            "missing_info" => r
                .epoch_info(&RunEnv::Height(99.into()))
                .map(|v| reply(epoch_json(v))),
            "negative" => r
                .epoch(&RunEnv::Height((-1).into()))
                .map(|v| reply(value(&v))),
            "start_current" | "start_old" | "start_future" => r
                .epoch_start_height(
                    &match n {
                        "start_current" => 7,
                        "start_old" => 5,
                        _ => 8,
                    }
                    .into(),
                )
                .map(|v| reply(int(v))),
            "targets" | "early_targets" => {
                let mut targets = chain::ParentTargets {
                    reader: r.clone(),
                    epoch_length: 5.into(),
                    start_height: 1.into(),
                };
                Ok(ok(list(
                    targets
                        .targets(&if n == "targets" { 80.into() } else { 0.into() })
                        .unwrap()
                        .iter()
                        .map(int)
                        .collect(),
                )))
            }
            "empty_info" => r.epoch_info(&RunEnv::Top).map(|v| reply(epoch_json(v))),
            s => panic!("{s}"),
        };
        let actual = result.unwrap_or_else(chain_error);
        assert_eq!(normalize(&actual), normalize(&row["result"]), "{n}");
    }
}

#[test]
fn query_limits_preserve_selected_roots_and_allow_retry() {
    let good = reader();
    good.selected.replace(selection());
    let before = good.selected.snapshot();
    let root = before.views[&key(80).to_vec()].trees.roots();
    for steps in [true, false] {
        let mut limited = good.clone();
        let p = Arc::make_mut(&mut limited.profile);
        if steps {
            p.maximum_steps = 0;
        } else {
            p.limits.max_bytes = 0;
        }
        let chain::Error::Contract(e) = limited.epoch(&RunEnv::Top).unwrap_err() else {
            panic!("expected operational contract limit")
        };
        assert!(!e.is_native(), "{e:?}");
        assert_eq!(
            good.selected.snapshot().views[&key(80).to_vec()]
                .trees
                .roots(),
            root
        );
        assert_eq!(good.epoch(&RunEnv::Top).unwrap(), Value::Integer(7.into()));
    }
    let mut bounded = good.clone();
    Arc::make_mut(&mut bounded.profile).maximum_history_reads = 1;
    assert!(matches!(
        bounded.epoch_start_height(&5.into()),
        Err(chain::Error::ResourceLimit)
    ));
    assert_eq!(
        good.epoch_start_height(&5.into()).unwrap(),
        BigInt::from(51)
    );
    // An explicitly acquired immutable view survives a later selection change.
    let explicit = RunEnv::View(before.views[&key(80).to_vec()].clone());
    good.selected.replace(Default::default());
    assert!(matches!(
        good.epoch(&RunEnv::Top),
        Err(chain::Error::ChainTooShort)
    ));
    assert_eq!(good.epoch(&explicit).unwrap(), Value::Integer(7.into()));
    assert_eq!(before.views[&key(80).to_vec()].trees.roots(), root);
}

#[test]
fn epoch_walk_pins_one_capture_even_when_selection_changes_during_acquisition() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Clone)]
    struct Replacing {
        selected: chain::Selected,
        captures: Arc<AtomicUsize>,
    }
    impl chain::Source<MemoryStore, MemoryStore> for Replacing {
        type Snapshot = Arc<chain::Selection>;
        fn capture(&self) -> Result<Self::Snapshot, chain::LookupError> {
            let snapshot = self.selected.snapshot();
            self.captures.fetch_add(1, Ordering::SeqCst);
            self.selected.replace(Default::default());
            Ok(snapshot)
        }
    }
    let selected = Replacing {
        selected: chain::Selected::new(selection()),
        captures: Arc::new(AtomicUsize::new(0)),
    };
    let r = Reader::new(selected.clone(), Arc::new(profile(8)));
    assert_eq!(r.epoch_start_height(&5.into()).unwrap(), BigInt::from(51));
    assert_eq!(selected.captures.load(Ordering::SeqCst), 1);
    assert!(selected.selected.snapshot().top.is_none());
    assert!(matches!(
        r.epoch(&RunEnv::Top),
        Err(chain::Error::ChainTooShort)
    ));
    assert_eq!(selected.captures.load(Ordering::SeqCst), 2);
}

#[test]
fn capture_failures_stay_operational_and_explicit_views_do_not_capture() {
    use ae_hyperchain::parent_cache::runtime::{TargetError, Targets};
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Clone)]
    struct Failing(Arc<AtomicUsize>);
    impl chain::Source<MemoryStore, MemoryStore> for Failing {
        type Snapshot = Arc<chain::Selection>;
        fn capture(&self) -> Result<Self::Snapshot, chain::LookupError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(chain::LookupError::Backend(Arc::new(
                std::io::Error::other("capture failed"),
            )))
        }
    }
    let selected = Failing(Arc::new(AtomicUsize::new(0)));
    let r = Reader::new(selected.clone(), Arc::new(profile(8)));
    let s = selection();
    let explicit = RunEnv::View(s.views[&key(80).to_vec()].clone());
    assert_eq!(r.epoch(&explicit).unwrap(), Value::Integer(7.into()));
    assert!(matches!(
        r.epoch(&RunEnv::Height((-1).into())),
        Err(chain::Error::Native(chain::Native::FunctionClause))
    ));
    assert_eq!(selected.0.load(Ordering::SeqCst), 0);
    for env in [
        RunEnv::Top,
        RunEnv::Height(80.into()),
        RunEnv::Hash(key(80).to_vec()),
    ] {
        let error = r.epoch(&env).unwrap_err();
        assert!(error.is_operational());
        assert!(matches!(
            error,
            chain::Error::Lookup(chain::LookupError::Backend(_))
        ));
    }
    let mut target = chain::ParentTargets {
        reader: r.clone(),
        epoch_length: 5.into(),
        start_height: 1.into(),
    };
    assert_eq!(target.targets(&80.into()), Err(TargetError::Unavailable));
    assert!(r.pin().err().unwrap().is_operational());
    assert_eq!(selected.0.load(Ordering::SeqCst), 5);
}

#[test]
fn target_callbacks_distinguish_native_failures_from_unavailable_state_and_limits() {
    use ae_hyperchain::parent_cache::runtime::{TargetError, Targets};
    let r = reader();
    let mut target = chain::ParentTargets {
        reader: r.clone(),
        epoch_length: 5.into(),
        start_height: 1.into(),
    };
    assert_eq!(target.targets(&80.into()), Err(TargetError::Native));
    let mut s = selection();
    let saved = s.views.remove(key(80).as_slice()).unwrap();
    r.selected.replace(s.clone());
    assert_eq!(target.targets(&80.into()), Err(TargetError::Unavailable));
    let mut missing = saved.as_ref().clone();
    Arc::make_mut(&mut missing.trees).contracts = Default::default();
    s.views.insert(key(80).to_vec(), Arc::new(missing));
    r.selected.replace(s.clone());
    assert_eq!(target.targets(&80.into()), Err(TargetError::Native));
    s.views.insert(key(80).to_vec(), saved);
    r.selected.replace(s);
    let mut limited = target.clone();
    Arc::make_mut(&mut limited.reader.profile).maximum_steps = 0;
    assert_eq!(limited.targets(&80.into()), Err(TargetError::Unavailable));
    assert_eq!(
        target.targets(&80.into()).unwrap(),
        vec![26.into(), 46.into()]
    );
}

struct ParentProbe(Mutex<Vec<BigInt>>);
impl ae_hyperchain::parent_connector::fetch::Backend for ParentProbe {
    fn fetch(
        &self,
        host: usize,
        query: &ae_hyperchain::parent_connector::fetch::Query,
        _: &[u8; 16],
        _: &ae_hyperchain::parent_connector::fetch::Context,
    ) -> Result<
        ae_hyperchain::parent_connector::fetch::Remote,
        ae_hyperchain::parent_connector::fetch::BackendError,
    > {
        use ae_hyperchain::{
            parent_block::ParentBlock,
            parent_connector::fetch::{Query, Remote},
        };
        let Query::Height(h) = query else {
            panic!("unexpected query {query:?}")
        };
        if host == 0 {
            self.0.lock().unwrap().push(h.clone());
        }
        if h < &BigInt::from(0) {
            return Ok(Remote::NotFound);
        }
        let n: u64 = h.to_string().parse().unwrap();
        Ok(Remote::Block(ParentBlock {
            hash: key(n).to_vec(),
            height: n.into(),
            previous_hash: key(n - 1).to_vec(),
            time: (n * 100).into(),
        }))
    }
}
fn wait(mut condition: impl FnMut() -> bool) {
    let until = Instant::now() + Duration::from_secs(4);
    while !condition() {
        assert!(Instant::now() < until, "chain/cache pipeline timed out");
        thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn original_cache_connector_uses_contract_targets_and_observes_selected_state_replacement() {
    use ae_hyperchain::{
        parent_cache::{
            self, Cache, ChildTop, Entry,
            runtime::{self, ConnectorLink, Mailbox},
        },
        parent_connector::runtime as connector,
    };
    let r = reader();
    let mut selected = selection();
    selected.top = Some(0.into());
    r.selected.replace(selected.clone());
    let targets = chain::ParentTargets {
        reader: r.clone(),
        epoch_length: 5.into(),
        start_height: 1.into(),
    };
    let cache = Cache::new(
        parent_cache::Config {
            child_start_height: 0.into(),
            max_size: 200.into(),
            retry_interval: 1_000_000_000.into(),
            finality: 2.into(),
        },
        ChildTop {
            height: 0.into(),
            hash: Hash256(key(0)),
        },
    );
    let mailbox = Mailbox::new(cache, runtime::Config::default()).unwrap();
    let client = mailbox.client();
    let probe = Arc::new(ParentProbe(Mutex::new(vec![])));
    let parent = connector::Runtime::spawn(
        probe.clone(),
        3,
        [0; 16],
        mailbox.sink(),
        connector::Config {
            fetch_interval: None,
            ..Default::default()
        },
    )
    .unwrap();
    let service = mailbox
        .spawn(
            targets,
            ConnectorLink::new(parent.client()),
            r.profile.order.clone(),
        )
        .unwrap();
    let has = |h: i64| {
        matches!(
            client.snapshot().cache.blocks().get(&h.into()),
            Some(Entry::Block(_))
        )
    };
    let snapshot = || {
        let s = client.snapshot();
        json!({"child":s.cache.child_top().height.to_u64().unwrap().to_string(),"top":s.cache.top_height().to_u64().unwrap().to_string(),
            "blocks":s.cache.blocks().iter().map(|(h,e)|json!([h.to_string(),match e {
                Entry::Requested(_)=>json!("requested"),
                Entry::Block(b)=>json!({"hash":hex::encode(&b.hash),"height":b.height.to_u64().unwrap().to_string(),"previous":hex::encode(&b.previous_hash),"time":b.time.to_u64().unwrap().to_string()})
            }])).collect::<Vec<_>>()})
    };
    let idle = || {
        for _ in 0..2 {
            wait(|| client.snapshot().pending_requests == 0);
            parent.client().barrier(Duration::from_secs(3)).unwrap();
            client.barrier(Duration::from_secs(3)).unwrap();
        }
    };
    wait(|| has(11));
    idle();
    assert_eq!(snapshot(), DATA["cache"]["initial"]);
    client
        .child_top(ChildTop {
            height: 80.into(),
            hash: Hash256(key(80)),
        })
        .unwrap();
    wait(|| has(26) && has(46));
    idle();
    assert_eq!(snapshot(), DATA["cache"]["next"]);
    let mut replacement = selected.views[&key(59).to_vec()].as_ref().clone();
    replacement.context = Context::transaction(80, Protocol::try_from(6).unwrap());
    replacement.environment.key_hash = key(80);
    selected
        .views
        .insert(key(80).to_vec(), Arc::new(replacement));
    r.selected.replace(selected);
    client
        .child_top(ChildTop {
            height: 80.into(),
            hash: Hash256(key(800)),
        })
        .unwrap();
    wait(|| has(16) && has(36));
    idle();
    assert_eq!(snapshot(), DATA["cache"]["replaced"]);
    assert_eq!(
        json!(
            probe
                .0
                .lock()
                .unwrap()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        ),
        DATA["cache"]["queries"]
    );
    assert!(service.shutdown().unwrap().reason.is_none());
    parent.shutdown().unwrap();
}
