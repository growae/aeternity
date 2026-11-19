use super::{
    bytes, check, epoch_json, exception, final_json, key, list, named, normalize, object, ok,
    optional, reply, trees, tuple, unpack, value, view, with_execution,
};
use ae_hyperchain::{
    chain::{self, Reader, RunEnv},
    consensus_contract::{self as cc, Contract},
    epochs::{self, Transition},
    genesis as g,
};
use ae_protocol::{Activation, NetworkProfile, Protocol, contracts::Version};
use ae_state::Trees;
use ae_trie::MemoryStore;
use ae_types::{AccountPublicKey, Unsigned};
use ae_vm_fate::{
    maps::order::Otp26,
    native::{self, Term},
    value::Value,
};
use serde_json::Value as J;
use std::sync::{Arc, LazyLock};
static DATA: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("../fixtures/erlang_hc_genesis.json")));
fn api(j: &J) -> Vec<u8> {
    ae_codec::api::decode(j.as_str().unwrap()).unwrap().1
}
fn account(j: &J) -> AccountPublicKey {
    AccountPublicKey(bytes(j).try_into().unwrap())
}
fn number(j: &J) -> Unsigned {
    Unsigned::from_decimal(
        &j.as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| j.to_string()),
    )
    .unwrap()
}
fn plan(row: &J) -> g::Plan {
    let deployments: Vec<_> = row["specs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| g::Deployment {
            owner: AccountPublicKey(api(&s["owner_pubkey"]).try_into().unwrap()),
            public_key: api(&s["pubkey"]).try_into().unwrap(),
            nonce: number(&s["nonce"]),
            amount: number(&s["amount"]),
            version: Version {
                vm: s["vm_version"].as_u64().unwrap() as u16,
                abi: s["abi_version"].as_u64().unwrap() as u16,
            },
            code: api(&s["code"]),
            calldata: api(&s["call_data"]),
        })
        .collect();
    let contracts = cc::Contracts {
        owner: AccountPublicKey(key(1)),
        staking: deployments[0].public_key,
        election: deployments[1].public_key,
        rewards: deployments[1].public_key,
    };
    g::Plan {
        contracts,
        deployments,
        calls: row["calls"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| g::InitialCall {
                caller: AccountPublicKey(api(&s["caller"]).try_into().unwrap()),
                contract: api(&s["contract_pubkey"]).try_into().unwrap(),
                nonce: number(&s["nonce"]),
                abi: number(&s["abi_version"]),
                fee: number(&s["fee"]),
                amount: number(&s["amount"]),
                gas: number(&s["gas"]),
                gas_price: number(&s["gas_price"]),
                calldata: api(&s["call_data"]),
            })
            .collect(),
        validators: row["validators"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| g::Validator {
                owner: account(&s["owner"]),
                sign_key: account(&s["sign_key"]),
                caller: account(&s["caller"]),
                stake: number(&s["stake"]),
                restake: s["restake"].as_bool().unwrap(),
            })
            .collect(),
        epoch_length: 3.into(),
        pin_reward: 7.into(),
    }
}
fn reader(row: &J) -> Reader<MemoryStore, MemoryStore, Otp26> {
    let mut p = super::profile(8);
    p.contracts = plan(row).contracts;
    p.network = NetworkProfile::custom(
        b"ae_hc_reference".to_vec(),
        vec![Activation {
            height: 0,
            protocol: Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
        }],
    )
    .unwrap();
    p.order = super::otp_order::profile(&DATA["profile"]);
    p.maximum_steps = 1_000_000;
    Reader {
        selected: chain::Selected::new(Default::default()),
        profile: Arc::new(p),
    }
}
fn context(row: &J, t: Trees, h: u64) -> chain::View {
    let mut v = view(t, row["protocol"].as_u64().unwrap(), h);
    v.environment.network_id = b"ae_hc_reference".to_vec();
    v
}
fn queries(r: &Reader<MemoryStore, MemoryStore, Otp26>, v: &chain::View, rows: &J) {
    let env = RunEnv::View(Arc::new(v.clone()));
    let roots = v.trees.roots();
    for row in rows.as_array().unwrap() {
        let n = row["name"].as_str().unwrap();
        let actual = match n {
            "epoch" => reply(value(&r.epoch(&env).unwrap())),
            "length" => reply(value(&r.epoch_length(&env).unwrap())),
            "info" => reply(epoch_json(r.epoch_info(&env).unwrap())),
            "validators" => reply(list(
                r.validators(&env)
                    .unwrap()
                    .iter()
                    .map(|v| tuple(v.iter().map(value).collect()))
                    .collect(),
            )),
            "pin" => ok(optional(r.pin_info(&env).unwrap(), |v| value(&v))),
            "pin_reward" => {
                let p = r.pin_reward(&env).unwrap();
                ok(named(vec![
                    ("base_pin_reward", value(&p.base)),
                    ("cur_pin_reward", value(&p.current)),
                    ("carry_over_pin_reward", value(&p.carry_over)),
                ]))
            }
            "finalize" => ok(optional(r.finalize_info(&env).unwrap(), final_json)),
            "staking_epoch" => reply(value(
                &r.query(&env, Contract::Staking, b"get_current_epoch", vec![])
                    .unwrap(),
            )),
            "schedule" => {
                let stakers = r.epoch_info(&env).unwrap().validators.unwrap();
                // The public schedule query uses the current staking contract's
                // distribution, which can differ from an earlier epoch lock.
                let current = r
                    .validators(&env)
                    .unwrap()
                    .into_iter()
                    .map(|xs| {
                        let Value::Object(_, address) = &xs[0] else {
                            panic!("staker")
                        };
                        chain::Staker {
                            address: address.clone(),
                            stake: xs[1].clone(),
                        }
                    })
                    .collect();
                assert!(!stakers.is_empty());
                reply(list(
                    r.validator_schedule(
                        &env,
                        key(90).to_vec(),
                        current,
                        Value::Integer(12.into()),
                    )
                    .unwrap()
                    .iter()
                    .map(object)
                    .collect(),
                ))
            }
            _ => panic!("{n}"),
        };
        assert_eq!(normalize(&actual), normalize(&row["result"]), "{n}");
    }
    assert_eq!(v.trees.roots(), roots);
}
fn transition(name: &str) -> Transition {
    match name {
        "key" => Transition::Key {
            leader: AccountPublicKey(key(12)),
        },
        "micro" => Transition::Micro {
            leader: AccountPublicKey(key(13)),
        },
        "reward_restake" => Transition::Reward {
            height: 1.into(),
            beneficiary: AccountPublicKey(key(12)),
            amount: 100.into(),
        },
        "reward_cash" => Transition::Reward {
            height: 1.into(),
            beneficiary: AccountPublicKey(key(13)),
            amount: 200.into(),
        },
        "end_first" => Transition::End {
            leader: AccountPublicKey(key(12)),
            seed: key(99).to_vec(),
            next_pin_reward: (-1).into(),
            carry_over: true,
        },
        "key_second" => Transition::Key {
            leader: AccountPublicKey(key(13)),
        },
        "reward_second" => Transition::Reward {
            height: 4.into(),
            beneficiary: AccountPublicKey(key(13)),
            amount: 300.into(),
        },
        "end_second" => Transition::End {
            leader: AccountPublicKey(key(13)),
            seed: key(98).to_vec(),
            next_pin_reward: 11.into(),
            carry_over: false,
        },
        "end_third" => Transition::End {
            leader: AccountPublicKey(key(14)),
            seed: key(97).to_vec(),
            next_pin_reward: (-1).into(),
            carry_over: true,
        },
        _ => panic!("transition {name}"),
    }
}
#[test]
fn original_production_contract_genesis_matches_every_stage_and_atomic_batch() {
    for row in DATA["rows"].as_array().unwrap() {
        let r = reader(row);
        let p = plan(row);
        let t = trees(&row["initial"]);
        let v = context(row, t.clone(), 0);
        let mut next = t.clone();
        for stage in row["stages"].as_array().unwrap() {
            next = with_execution(&r.profile, &v, |cfg| {
                match stage["name"].as_str().unwrap() {
                    "staking" => g::deploy(&next, &p.deployments[0], &v.context, cfg).unwrap(),
                    "election" => g::deploy(&next, &p.deployments[1], &v.context, cfg).unwrap(),
                    "initial_call" => g::call(&next, &p.calls[0], &v.context, cfg).unwrap(),
                    "validator1" => g::register_validator(
                        &next,
                        p.contracts.staking,
                        &p.validators[0],
                        &v.context,
                        cfg,
                    )
                    .unwrap(),
                    "validator2" => g::register_validator(
                        &next,
                        p.contracts.staking,
                        &p.validators[1],
                        &v.context,
                        cfg,
                    )
                    .unwrap(),
                    "epochs" => epochs::apply(
                        &next,
                        p.contracts,
                        &Transition::Initialize {
                            length: 3.into(),
                            pin_reward: 7.into(),
                        },
                        &v.context,
                        cfg,
                    )
                    .unwrap(),
                    _ => panic!("stage"),
                }
            });
            check(&next, &stage["trees"]);
        }
        let batch =
            with_execution(&r.profile, &v, |cfg| g::transform(&t, &p, &v.context, cfg)).unwrap();
        assert_eq!(batch.roots(), next.roots());
        check(&t, &row["initial"]);
        queries(&r, &context(row, next, 0), &row["queries"]);
    }
}
#[test]
fn original_epoch_progression_and_cross_contract_rewards_match() {
    for row in DATA["rows"].as_array().unwrap() {
        let r = reader(row);
        let p = plan(row);
        let mut t = trees(&row["stages"][5]["trees"]);
        for stage in row["lifecycle"].as_array().unwrap() {
            let h = stage["height"].as_u64().unwrap();
            let v = context(row, t.clone(), h);
            t = with_execution(&r.profile, &v, |cfg| {
                epochs::apply(
                    &t,
                    p.contracts,
                    &transition(stage["name"].as_str().unwrap()),
                    &v.context,
                    cfg,
                )
            })
            .unwrap();
            check(&t, &stage["trees"]);
            queries(&r, &context(row, t.clone(), h), &stage["queries"]);
        }
    }
}

fn genesis_exception(e: g::Error) -> J {
    exception(native::Exception::error(match e {
        g::Error::UnexpectedPublicKey { computed, expected } => Term::tagged(
            "unexpected_pubkey",
            vec![
                Term::Binary(computed.to_vec()),
                Term::Binary(expected.to_vec()),
            ],
        ),
        g::Error::CallFailed { return_type, value } => Term::tagged(
            "failed_apply_hard_fork_contract_call",
            vec![Term::tagged(
                "call_error",
                vec![
                    Term::Atom(match return_type {
                        ae_state::calls::ReturnType::Ok => "ok",
                        ae_state::calls::ReturnType::Revert => "revert",
                        ae_state::calls::ReturnType::Error => "error",
                    }),
                    Term::Binary(value),
                ],
            )],
        ),
        e => panic!("unexpected {e:?}"),
    }))
}
#[test]
fn original_genesis_and_epoch_rejections_preserve_input_roots() {
    for row in DATA["rows"].as_array().unwrap() {
        let r = reader(row);
        let p = plan(row);
        let t = trees(&row["stages"][5]["trees"]);
        for f in row["failures"].as_array().unwrap() {
            let name = f["name"].as_str().unwrap();
            let h = match name {
                "late_init" => 1,
                "early_end" => 2,
                _ => 0,
            };
            let v = context(row, t.clone(), h);
            let actual = with_execution(&r.profile, &v, |cfg| match name {
                "late_init" | "early_end" => {
                    let tr = if name == "late_init" {
                        Transition::Initialize {
                            length: 3.into(),
                            pin_reward: 7.into(),
                        }
                    } else {
                        transition("end_first")
                    };
                    let e = epochs::apply(&t, p.contracts, &tr, &v.context, cfg).unwrap_err();
                    exception(e.return_exception().unwrap_or_else(|| panic!("{e:?}")))
                }
                "duplicate_validator" | "small_stake" => {
                    let mut val = p.validators[0].clone();
                    if name == "small_stake" {
                        val.stake = 1.into();
                        val.restake = false;
                    }
                    genesis_exception(
                        g::register_validator(&t, p.contracts.staking, &val, &v.context, cfg)
                            .unwrap_err(),
                    )
                }
                "wrong_pubkey" => {
                    let mut d = p.deployments[0].clone();
                    d.public_key = key(99);
                    genesis_exception(
                        g::deploy(&trees(&row["initial"]), &d, &v.context, cfg).unwrap_err(),
                    )
                }
                _ => panic!("{name}"),
            });
            assert_eq!(normalize(&actual), normalize(&f["result"]), "{name}");
            check(&t, &row["stages"][5]["trees"]);
        }
    }
}
#[test]
fn genesis_batch_resource_and_late_stage_failures_are_retryable() {
    let row = &DATA["rows"][0];
    let r = reader(row);
    let p = plan(row);
    let t = trees(&row["initial"]);
    let v = context(row, t.clone(), 0);
    let mut limited = r.profile.as_ref().clone();
    limited.maximum_steps = 0;
    assert!(with_execution(&limited, &v, |cfg| g::transform(&t, &p, &v.context, cfg)).is_err());
    check(&t, &row["initial"]);
    // This fails after the first deployment has already executed in the batch.
    let mut bad = p.clone();
    bad.deployments[1].public_key = key(99);
    assert!(matches!(
        with_execution(&r.profile, &v, |cfg| g::transform(
            &t, &bad, &v.context, cfg
        )),
        Err(g::Error::UnexpectedPublicKey { .. })
    ));
    check(&t, &row["initial"]);
    let done = with_execution(&r.profile, &v, |cfg| g::transform(&t, &p, &v.context, cfg)).unwrap();
    check(&done, &row["stages"][5]["trees"]);
}
#[test]
fn epoch_work_limits_never_publish_temporary_funding_or_staking_changes() {
    let row = &DATA["rows"][0];
    let r = reader(row);
    let p = plan(row);
    let t = trees(&row["lifecycle"][6]["trees"]);
    let v = context(row, t.clone(), 6);
    for limit in [0, 1, 25, 100] {
        let mut limited = r.profile.as_ref().clone();
        limited.maximum_steps = limit;
        let e = with_execution(&limited, &v, |cfg| {
            epochs::apply(&t, p.contracts, &transition("end_second"), &v.context, cfg)
        })
        .unwrap_err();
        assert!(!e.is_native(), "{e:?}");
        check(&t, &row["lifecycle"][6]["trees"]);
    }
    let done = with_execution(&r.profile, &v, |cfg| {
        epochs::apply(&t, p.contracts, &transition("end_second"), &v.context, cfg)
    })
    .unwrap();
    check(&done, &row["lifecycle"][7]["trees"]);
}
