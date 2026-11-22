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
fn queries<
    A: ae_trie::NodeWriter + Clone + std::fmt::Debug,
    C: ae_trie::NodeWriter + Clone + std::fmt::Debug,
>(
    r: &Reader<A, C, Otp26>,
    v: &chain::View<A, C>,
    rows: &J,
) {
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

#[test]
fn production_contract_genesis_commits_reopens_and_queries_persistent_roots() {
    use ae_chain::repository::{self as repo, Identity, ReadConfig};
    use ae_codec::headers::{Header, HeaderMode};
    use ae_protocol::ConsensusType;
    use ae_storage::Database;
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    for row in DATA["rows"].as_array().unwrap() {
        let r = reader(row);
        let initial = trees(&row["initial"]);
        let context = context(row, initial.clone(), 0);
        let transformed = with_execution(&r.profile, &context, |cfg| {
            g::transform(&initial, &plan(row), &context.context, cfg)
        })
        .unwrap();
        let roots = transformed.roots();
        let header = g::header(&r.profile.network, 0, &roots).unwrap();
        let hash = Header::Key(header.clone()).hash().unwrap();
        let identity = Identity::for_consensus(
            &r.profile.network,
            hash,
            ae_crypto::blake2b_256(b"isolated HC OTP26 genesis profile"),
            ConsensusType::Pos,
        );
        let config = ReadConfig {
            mode: HeaderMode::Hyperchain,
            ..Default::default()
        };
        let path = Directory(std::env::temp_dir().join(format!(
            "ae-hc-genesis-{}-{}",
            std::process::id(),
            row["protocol"]
        )));
        let database =
            Database::open_with_config(&path.0, identity.clone(), config.clone()).unwrap();
        let before = database.view().unwrap().status();
        let small = repo::Limits {
            commit_bytes: 1,
            ..Default::default()
        };
        assert!(matches!(
            repo::bootstrap(
                &database.view().unwrap(),
                &r.profile.network,
                &header,
                &transformed,
                small
            ),
            Err(repo::InsertError::Repository(repo::Error::ResourceLimit))
        ));
        assert_eq!(database.view().unwrap().status(), before);
        let first = repo::bootstrap(
            &database.view().unwrap(),
            &r.profile.network,
            &header,
            &transformed,
            Default::default(),
        )
        .unwrap();
        let stale = repo::bootstrap(
            &database.view().unwrap(),
            &r.profile.network,
            &header,
            &transformed,
            Default::default(),
        )
        .unwrap();
        database.commit(first).unwrap();
        assert!(database.commit(stale).is_err());
        assert_eq!(database.view().unwrap().status().revision, 1);
        assert_eq!(
            database
                .view()
                .unwrap()
                .require_record(hash)
                .unwrap()
                .difficulty,
            0.into()
        );
        assert!(matches!(
            repo::bootstrap(
                &database.view().unwrap(),
                &r.profile.network,
                &header,
                &transformed,
                Default::default()
            ),
            Err(repo::InsertError::Repository(repo::Error::NotEmpty))
        ));
        drop(database);
        // A mode mismatch is rejected even though genesis has no HC-only flags.
        assert!(matches!(
            Database::open(&path.0, identity.clone()),
            Err(repo::Error::Identity)
        ));
        let pow = Identity::new(&r.profile.network, hash, identity.execution_profile);
        assert!(matches!(
            Database::open(&path.0, pow),
            Err(repo::Error::Identity)
        ));
        let database = Database::open_with_config(&path.0, identity.clone(), config).unwrap();
        {
            let pinned = database.view().unwrap();
            pinned.verify_tip().unwrap();
            assert!(matches!(
                repo::View::load_with_config(pinned.source().clone(), Default::default()),
                Err(repo::Error::Identity)
            ));
            let rules = ae_chain::key::Rules::new(
                ae_chain::admission::PowRules {
                    now_ms: 0,
                    micro_cycle_ms: 3_000,
                    configured_gas_limit: 6_000_000.into(),
                    body_limits: Default::default(),
                },
                hash,
            );
            // Persistence does not enable the currently PoW-only importer.
            for consensus in [ConsensusType::Pow, ConsensusType::Pos] {
                let mut execution =
                    ae_execution::transactions::Config::primitives(&r.profile.network);
                execution.consensus = consensus;
                assert!(matches!(
                    repo::prepare(
                        &pinned,
                        &ae_chain::block::Block::Key(header.clone()),
                        &execution,
                        &rules,
                        Default::default()
                    ),
                    Err(repo::InsertError::WrongConsensus)
                ));
            }
            assert_eq!(
                pinned.require_block(hash).unwrap().header(),
                Header::Key(header.clone())
            );
            let persisted = pinned.trees(hash, Default::default()).unwrap();
            assert_eq!(persisted.roots(), roots);
            let stored = chain::View {
                trees: Arc::new(persisted),
                context: context.context.clone(),
                environment: context.environment.clone(),
                history: context.history.clone(),
                aevm: context.aevm.clone(),
            };
            let reader = Reader {
                selected: chain::Selected::new(Default::default()),
                profile: r.profile.clone(),
            };
            queries(&reader, &stored, &row["queries"]);
            let mut scan = pinned.scan_state(hash, Default::default()).unwrap();
            while !scan.is_complete() {
                scan.next_page(Default::default()).unwrap();
            }
            assert_eq!(pinned.status().tip, Some(hash));
            assert_eq!(pinned.canonical_key(0).unwrap(), Some(hash));
        }
        drop(database);
        check(&initial, &row["initial"]);
    }
}

#[test]
fn genesis_header_bytes_hashes_and_zero_score_match_the_original_callbacks() {
    use ae_codec::headers::{Header, HeaderMode};
    let fixture: J =
        serde_json::from_str(include_str!("../fixtures/erlang_hc_bootstrap.json")).unwrap();
    for row in fixture["rows"].as_array().unwrap() {
        let protocol = row["protocol"].as_u64().unwrap() as u32;
        let network = NetworkProfile::custom(
            b"ae_hc_reference".to_vec(),
            vec![Activation {
                height: 0,
                protocol: Protocol::try_from(protocol).unwrap(),
            }],
        )
        .unwrap();
        let roots = if row["label"] == "empty" {
            Default::default()
        } else {
            let state = DATA["rows"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["protocol"] == row["protocol"])
                .unwrap();
            trees(&state["stages"][5]["trees"]).roots()
        };
        let header = g::header(&network, row["time"].as_u64().unwrap(), &roots).unwrap();
        assert_eq!(header.common.state_hash.as_slice(), bytes(&row["root"]));
        assert_eq!(row["difficulty"], 0);
        let full = Header::Key(header.clone());
        assert_eq!(full.encode().unwrap(), bytes(&row["bytes"]));
        assert_eq!(full.hash().unwrap().as_ref(), bytes(&row["hash"]));
        assert_eq!(
            Header::decode(&bytes(&row["bytes"]), HeaderMode::Hyperchain).unwrap(),
            full
        );
        let mut raw = header;
        raw.common.state_hash = [0; 32];
        let raw = Header::Key(raw);
        assert_eq!(raw.encode().unwrap(), bytes(&row["raw_bytes"]));
        assert_eq!(raw.hash().unwrap().as_ref(), bytes(&row["raw_hash"]));
    }
}

#[test]
fn bootstrap_rejects_noncanonical_hyperchain_genesis_without_writes() {
    use ae_chain::repository::{self as repo, Identity, ReadConfig};
    use ae_codec::headers::{Header, HeaderMode};
    use ae_protocol::ConsensusType;
    let network = reader(&DATA["rows"][2]).profile.network.clone();
    let state = trees(&DATA["rows"][2]["initial"]);
    let good = g::header(&network, 123, &state.roots()).unwrap();
    for field in 0..13 {
        let mut header = good.clone();
        match field {
            0 => header.target = 1,
            1 => header.miner[0] = 1,
            2 => header.beneficiary[0] = 1,
            3 => header.seal[0] = 1,
            4 => header.nonce = 1,
            5 => header.info = Some([1; 4]),
            6 => header.common.previous_key_hash[0] = 1,
            7 => header.common.previous_hash[0] ^= 1,
            8 => header.hole = true,
            9 => header.end_of_epoch = true,
            10 => header.common.height = 1,
            11 => header.common.state_hash[0] ^= 1,
            12 => header.common.version = 7,
            _ => unreachable!(),
        }
        // Bind the deliberately invalid header, so validation cannot pass just
        // by rejecting a different configured genesis hash.
        let identity = Identity::for_consensus(
            &network,
            Header::Key(header.clone()).hash().unwrap(),
            ae_types::Hash256::ZERO,
            ConsensusType::Pos,
        );
        let path =
            std::env::temp_dir().join(format!("ae-hc-bad-genesis-{}-{field}", std::process::id()));
        let database = ae_storage::Database::open_with_config(
            &path,
            identity,
            ReadConfig {
                mode: HeaderMode::Hyperchain,
                ..Default::default()
            },
        )
        .unwrap();
        let before = database.view().unwrap().status();
        assert!(
            matches!(
                repo::bootstrap(
                    &database.view().unwrap(),
                    &network,
                    &header,
                    &state,
                    Default::default()
                ),
                Err(repo::InsertError::WrongGenesis)
            ),
            "field {field}"
        );
        assert_eq!(database.view().unwrap().status(), before);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn bootstrap_crash_worker() {
    use ae_chain::repository::{self as repo, Identity, ReadConfig};
    use ae_codec::headers::{Header, HeaderMode};
    use std::io::{BufRead, Write};
    let Ok(path) = std::env::var("AE_HC_BOOTSTRAP_CRASH_PATH") else {
        return;
    };
    let row = &DATA["rows"][2];
    let r = reader(row);
    let state = trees(&row["stages"][5]["trees"]);
    let header = g::header(&r.profile.network, 42, &state.roots()).unwrap();
    let identity = Identity::for_consensus(
        &r.profile.network,
        Header::Key(header.clone()).hash().unwrap(),
        ae_types::Hash256::ZERO,
        ae_protocol::ConsensusType::Pos,
    );
    let database = ae_storage::Database::open_with_config(
        path,
        identity,
        ReadConfig {
            mode: HeaderMode::Hyperchain,
            ..Default::default()
        },
    )
    .unwrap();
    let plan = repo::bootstrap(
        &database.view().unwrap(),
        &r.profile.network,
        &header,
        &state,
        Default::default(),
    )
    .unwrap();
    println!("AE_HC_BOOTSTRAP_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "commit");
    database.commit(plan).unwrap();
    println!("AE_HC_BOOTSTRAP_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn killed_hyperchain_bootstrap_recovers_a_complete_genesis_or_empty_database() {
    use ae_chain::repository::{Identity, Read, ReadConfig};
    use ae_codec::headers::{Header, HeaderMode};
    use std::io::{BufRead, Write};
    use std::process::{Command, Stdio};
    use std::time::Duration;
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    struct Worker(std::process::Child);
    impl Drop for Worker {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let row = &DATA["rows"][2];
    let r = reader(row);
    let state = trees(&row["stages"][5]["trees"]);
    let header = g::header(&r.profile.network, 42, &state.roots()).unwrap();
    let hash = Header::Key(header.clone()).hash().unwrap();
    let identity = Identity::for_consensus(
        &r.profile.network,
        hash,
        ae_types::Hash256::ZERO,
        ae_protocol::ConsensusType::Pos,
    );
    let config = ReadConfig {
        mode: HeaderMode::Hyperchain,
        ..Default::default()
    };
    for phase in ["before", "after", "race0", "race1", "race5", "race10"] {
        let path = Directory(std::env::temp_dir().join(format!(
            "ae-hc-bootstrap-crash-{}-{phase}",
            std::process::id()
        )));
        let database =
            ae_storage::Database::open_with_config(&path.0, identity.clone(), config.clone())
                .unwrap();
        let before = database.view().unwrap().source().prefix(&[], 100).unwrap();
        drop(database);
        let mut child = Worker(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "genesis::bootstrap_crash_worker", "--nocapture"])
                .env("AE_HC_BOOTSTRAP_CRASH_PATH", &path.0)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let output = child.0.stdout.take().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let listener = std::thread::spawn(move || {
            for line in std::io::BufReader::new(output).lines() {
                if sender.send(line.unwrap()).is_err() {
                    break;
                }
            }
        });
        let wait = |marker: &str| {
            let deadline = std::time::Instant::now() + Duration::from_secs(60);
            loop {
                let line = receiver
                    .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                    .unwrap_or_else(|e| panic!("worker {phase} before {marker}: {e}"));
                if line == marker {
                    break;
                }
            }
        };
        wait("AE_HC_BOOTSTRAP_READY");
        if phase != "before" {
            let input = child.0.stdin.as_mut().unwrap();
            input.write_all(b"commit\n").unwrap();
            input.flush().unwrap();
            if phase == "after" {
                wait("AE_HC_BOOTSTRAP_DURABLE");
            } else {
                std::thread::sleep(Duration::from_millis(
                    phase.strip_prefix("race").unwrap().parse().unwrap(),
                ));
            }
        }
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        listener.join().unwrap();
        let database =
            ae_storage::Database::open_with_config(&path.0, identity.clone(), config.clone())
                .unwrap();
        let view = database.view().unwrap();
        let revision = view.status().revision;
        assert!(revision <= 1);
        if phase == "before" {
            assert_eq!(revision, 0);
        } else if phase == "after" {
            assert_eq!(revision, 1);
        }
        if revision == 0 {
            assert_eq!(view.source().prefix(&[], 100).unwrap(), before);
        } else {
            view.verify_tip().unwrap();
            assert_eq!(view.status().tip, Some(hash));
            assert_eq!(view.canonical_key(0).unwrap(), Some(hash));
            assert_eq!(view.require_record(hash).unwrap().difficulty, 0.into());
            assert_eq!(
                view.require_block(hash).unwrap().header(),
                Header::Key(header.clone())
            );
            let stored = view.trees(hash, Default::default()).unwrap();
            assert_eq!(stored.roots(), state.roots());
            let mut scan = view.scan_state(hash, Default::default()).unwrap();
            while !scan.is_complete() {
                scan.next_page(Default::default()).unwrap();
            }
        }
        eprintln!("HC bootstrap crash recovery {phase}: revision {revision}");
    }
}
