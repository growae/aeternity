use super::*;
use ae_chain::{key as transition, rewards};
use ae_types::Hash256;
#[path = "key/on_demand.rs"]
mod on_demand;
static KEY: LazyLock<J> =
    LazyLock::new(|| unpack_fixture(include_str!("../fixtures/erlang_key_transition.json")));
fn unsigned(v: &J) -> Unsigned {
    Unsigned::from_decimal(v.as_str().unwrap()).unwrap()
}
fn key_header(v: &J) -> ae_codec::headers::KeyHeader {
    let Header::Key(h) = Header::decode(&bytes(v), HeaderMode::ProofOfWork).unwrap() else {
        panic!("key header required")
    };
    h
}
fn history(row: &J) -> rewards::History {
    row["history"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                Hash256(bytes(&r["hash"]).try_into().unwrap()),
                rewards::Record {
                    header: key_header(&r["header"]),
                    fees: Some(unsigned(&r["fees"])),
                    reported_fraud: r["fraud"].as_bool(),
                },
            )
        })
        .collect()
}
fn settings(row: &J) -> transition::Rules {
    let mut r = transition::Rules::new(
        super::rules(),
        Hash256(bytes(&row["genesis"]).try_into().unwrap()),
    );
    r.reward_delay = row["delay"].as_u64().unwrap();
    if !row["accounts"].is_null() {
        let pairs = |v: &J| {
            v[1].as_array()
                .unwrap()
                .iter()
                .map(|p| {
                    (
                        AccountPublicKey(term_bytes(&p[1][0]).try_into().unwrap()),
                        unsigned(&p[1][1][1]),
                    )
                })
                .collect()
        };
        r.custom_forks.insert(
            Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
            ae_chain::forks::Allocations {
                accounts: pairs(&row["accounts"]),
                extra: pairs(&row["extra"]),
            },
        );
    }
    r
}
fn with_context<T>(
    row: &J,
    f: impl FnOnce(&ex::Parent<'_>, &mut transactions::Config<'_>) -> T,
) -> T {
    let network = match row["network"].as_str().unwrap() {
        "ae_mainnet" => NetworkProfile::mainnet(),
        "ae_uat" => NetworkProfile::uat(),
        id => NetworkProfile::custom(
            id.as_bytes().to_vec(),
            vec![Activation {
                height: 0,
                protocol: Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
            }],
        )
        .unwrap(),
    };
    let vm = vm();
    let env = environment();
    let order = otp_order::profile(&KEY["profile"]);
    let overrides = Overrides::default();
    let fate = fate::Config {
        context: &vm,
        order: &order,
        overrides: &overrides,
        environment: &env,
        history: Arc::default(),
        auth_tx: None,
        metering_floor: None,
        diagnostic_profile: ae_vm_fate::diagnostic::Profile::Test,
    };
    let aevm = aevm();
    let mut config = transactions::Config {
        fate: Some(&fate),
        aevm: Some(&aevm),
        ..transactions::Config::primitives(&network)
    };
    let previous = Header::decode(&bytes(&row["previous"]), HeaderMode::ProofOfWork).unwrap();
    let previous_key = if row["previous_key"].is_null() {
        key_header(&row["previous"])
    } else {
        key_header(&row["previous_key"])
    };
    let parent = ex::Parent {
        headers: ae_consensus::headers::Context {
            previous: &previous,
            previous_key: &previous_key,
            recent_keys: &[],
        },
        previous_generation: ex::PreviousGeneration::Unavailable,
        fees: 123.into(),
        reported_fraud: row["second_fraud"].as_bool().unwrap(),
    };
    f(&parent, &mut config)
}
fn run<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    row: &J,
    t: &Trees<A, C>,
) -> Result<transition::Prepared<A, C>, transition::Error<A, C>> {
    with_context(row, |parent, config| {
        transition::prepare(
            t,
            &key_header(&row["header"]),
            parent,
            &history(row),
            config,
            &settings(row),
        )
    })
}
fn check_state<A: NodeReader, C: NodeReader>(t: &Trees<A, C>, state: &J) {
    assert_eq!(bin(t.roots().hash().0), *field(state, "root"));
    for (name, tree) in [
        ("contracts", t.contracts.tree()),
        ("calls", t.calls.tree()),
        ("channels", t.channels.tree()),
        ("names", t.names.tree()),
        ("name_cache", t.names.cache()),
        ("oracles", t.oracles.tree()),
        ("oracle_cache", t.oracles.cache()),
    ] {
        assert_eq!(entries(tree), *field(state, name), "{name}");
    }
    assert_eq!(entries(t.accounts.tree()), *field(state, "accounts"));
}
fn check<A: NodeReader, C: NodeReader>(p: &transition::Prepared<A, C>, row: &J) {
    assert_eq!(row["result"]["kind"], "ok");
    let v = &row["result"]["value"];
    check_state(p.trees(), field(v, "state"));
    assert_eq!(int(magnitude(p.fees())), *field(v, "fees"));
    assert_eq!(*field(v, "events"), json!(["list", []]));
}
#[test]
fn key_expiry_rewards_and_all_public_fork_migrations_match_erlang() {
    let mut count = 0;
    for row in KEY["rows"]
        .as_array()
        .unwrap()
        .iter()
        .chain(KEY["forks"].as_array().unwrap())
        .chain(KEY["custom"].as_array().unwrap())
        .chain(KEY["mixed"].as_array().unwrap())
    {
        let t = trees(&row["before"]);
        let before = roots(&t);
        let p = run(row, &t).unwrap_or_else(|e| {
            panic!(
                "case {count} {} {} {}: {e:?}",
                row["network"], row["protocol"], row["height"]
            )
        });
        check(&p, row);
        assert_eq!(roots(&t), before);
        assert_eq!(
            p.reward().is_some(),
            row["height"].as_u64().unwrap() > row["delay"].as_u64().unwrap()
        );
        count += 1;
    }
    assert_eq!(count, 756);
}
#[test]
fn complete_coinbase_boundaries_fraud_rules_and_developer_rounding_match_erlang() {
    for row in KEY["coinbase"].as_array().unwrap() {
        let h = unsigned(&row["height"]);
        assert_eq!(
            ae_protocol::coinbase::mining_reward(&h),
            unsigned(&row["reward"]),
            "{h:?}"
        );
        assert_eq!(
            ae_protocol::coinbase::fraud_reward(&h),
            unsigned(&row["fraud"]),
            "{h:?}"
        );
    }
    for row in KEY["calculations"].as_array().unwrap() {
        let r = rewards::calculate(
            row["fraud1"].as_bool().unwrap(),
            row["fraud2"].as_bool().unwrap(),
            &unsigned(&row["fees"]),
            &unsigned(&row["mining"]),
            &unsigned(&row["report"]),
            row["genesis"].as_bool().unwrap(),
        );
        assert_eq!(
            tuple(vec![
                int(magnitude(&r.first)),
                int(magnitude(&r.second)),
                int(r.locked)
            ]),
            row["result"]
        );
    }
    for row in KEY["splits"].as_array().unwrap() {
        let bs = row["beneficiaries"][1]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| rewards::Beneficiary {
                key: AccountPublicKey(term_bytes(&v[1][0]).try_into().unwrap()),
                shares: v[1][1][1].as_str().unwrap().parse().unwrap(),
                from: Protocol::Fortuna,
                through: Protocol::Ceres,
            })
            .collect();
        let p = rewards::Policy::custom(true, bs).unwrap();
        let ((a, b), dev) = p.split(
            &unsigned(&row["first"]),
            &unsigned(&row["second"]),
            Protocol::Fortuna,
        );
        assert_eq!(
            tuple(vec![
                tuple(vec![int(magnitude(&a)), int(magnitude(&b))]),
                json!([
                    "list",
                    dev.into_iter()
                        .map(|(k, n)| tuple(vec![bin(k.0), int(magnitude(&n))]))
                        .collect::<Vec<_>>()
                ])
            ]),
            row["result"]["value"]
        );
    }
}

#[test]
fn real_first_public_key_blocks_validate_and_execute_from_reconstructed_genesis() {
    use ae_state::genesis::{self, Network};
    let rows: J = serde_json::from_str(include_str!("../fixtures/public_first_keys.json")).unwrap();
    for row in rows.as_array().unwrap() {
        let (network, profile) = if row["network"] == "ae_mainnet" {
            (Network::Mainnet, NetworkProfile::mainnet())
        } else {
            (Network::Uat, NetworkProfile::uat())
        };
        let previous = genesis::header(network).unwrap();
        let Header::Key(ref previous_key) = previous else {
            panic!()
        };
        let header = key_header(&row["hex"]);
        let (accounts, _) = genesis::state(network).unwrap();
        let mut t = Trees {
            accounts,
            contracts: Contracts::new(MemoryStore::default()),
            calls: Default::default(),
            channels: Default::default(),
            names: Default::default(),
            oracles: Default::default(),
        };
        let before = roots(&t);
        let parent = ex::Parent {
            headers: ae_consensus::headers::Context {
                previous: &previous,
                previous_key,
                recent_keys: &[],
            },
            previous_generation: ex::PreviousGeneration::Missing,
            fees: 0.into(),
            reported_fraud: false,
        };
        let mut rules = transition::Rules::new(super::rules(), previous.hash().unwrap());
        rules.admission.now_ms = header.time;
        let config = transactions::Config::primitives(&profile);
        let h = rewards::History::default();
        let result = transition::pow_key(&header, &mut t, &parent, &h, &config, &rules).unwrap();
        assert_eq!(result.hash(), Header::Key(header.clone()).hash().unwrap());
        assert_eq!(result.state_hash().0, header.common.state_hash);
        assert_eq!(result.closed_generation_fees(), &0.into());
        assert!(result.reward().is_none());
        assert_eq!(roots(&t), before);
        let mut invalid = header.clone();
        invalid.nonce ^= 1;
        let error =
            transition::pow_key(&invalid, &mut t, &parent, &h, &config, &rules).unwrap_err();
        assert!(matches!(
            error,
            transition::Error::Admission(PowError::Header(
                ae_consensus::headers::Error::IncorrectProofOfWork
            ))
        ));
        assert!(!error.is_operational());
        assert_eq!(roots(&t), before);
        rules.checkpoints.insert(1, Hash256::ZERO);
        assert!(matches!(
            transition::pow_key(&header, &mut t, &parent, &h, &config, &rules),
            Err(transition::Error::Header(
                ae_consensus::headers::Error::Checkpoint
            ))
        ));
        assert_eq!(roots(&t), before);
        rules.checkpoints.clear();
        t.accounts
            .enter(&ae_state::accounts::Account::new(
                AccountPublicKey(small(91)),
                1.into(),
            ))
            .unwrap();
        let changed = roots(&t);
        assert!(matches!(
            transition::pow_key(&header, &mut t, &parent, &h, &config, &rules),
            Err(transition::Error::ParentState)
        ));
        assert_eq!(roots(&t), changed);
    }
}

fn row(protocol: u32, height: u64, mode: &str) -> &'static J {
    KEY["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["protocol"] == protocol
                && r["height"] == height
                && r["mode"] == mode
                && r["first_fraud"] == false
                && r["second_fraud"] == false
                && r["same_beneficiary"] == false
        })
        .unwrap()
}
#[test]
fn unavailable_reward_history_and_local_limits_leave_input_reusable() {
    let row = row(6, 8, "rich");
    let input = trees(&row["before"]);
    let before = roots(&input);
    for mode in 0..9 {
        with_context(row, |parent, config| {
            let mut h = history(row);
            let mut r = settings(row);
            let header = key_header(&row["header"]);
            match mode {
                0 => r.max_calls = 0,
                1 => r.reward_delay = 1,
                2 => r.max_reward_history = 0,
                3 => h.clear(),
                4 => {
                    for v in h.values_mut() {
                        v.header.common.height += 1;
                    }
                }
                5 => {
                    for v in h.values_mut() {
                        v.fees = None;
                    }
                }
                6 => {
                    for v in h.values_mut() {
                        v.reported_fraud = None;
                    }
                }
                7 => r.admission.configured_gas_limit = (-1).into(),
                8 => {
                    let mut invalid = header.clone();
                    invalid.common.version -= 1;
                    let e =
                        transition::prepare(&input, &invalid, parent, &h, config, &r).unwrap_err();
                    assert!(matches!(e, transition::Error::VersionRegression));
                    assert!(!e.is_operational());
                    return;
                }
                _ => unreachable!(),
            }
            let e = transition::prepare(&input, &header, parent, &h, config, &r).unwrap_err();
            assert!(e.is_operational(), "{mode}: {e:?}");
            assert!(
                matches!(
                    (mode, &e),
                    (
                        0,
                        transition::Error::GasFees(transition::GasFeesError::ResourceLimit)
                    ) | (1, transition::Error::Rewards(rewards::Error::Delay))
                        | (2, transition::Error::Rewards(rewards::Error::ResourceLimit))
                        | (
                            3,
                            transition::Error::Rewards(rewards::Error::HistoryMissing(_))
                        )
                        | (
                            4,
                            transition::Error::Rewards(rewards::Error::HistoryInvalid)
                        )
                        | (
                            5,
                            transition::Error::Rewards(rewards::Error::FeesMissing(_))
                        )
                        | (
                            6,
                            transition::Error::Rewards(rewards::Error::FraudMissing(_))
                        )
                        | (7, transition::Error::InvalidGas)
                ),
                "{mode}: {e:?}"
            );
        });
        assert_eq!(roots(&input), before);
        check(&run(row, &input).unwrap(), row);
    }
    let row = KEY["forks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["network"] == "ae_uat" && r["protocol"] == 4 && r["mode"] == "rich")
        .unwrap();
    let input = trees(&row["before"]);
    let before = roots(&input);
    for mode in 0..4 {
        with_context(row, |parent, config| {
            let mut r = settings(row);
            match mode {
                0 => r.fork_limits.max_accounts = 0,
                1 => r.fork_limits.max_contracts = 0,
                2 => config.fate = None,
                3 => config.consensus = ae_protocol::ConsensusType::Pos,
                _ => unreachable!(),
            }
            let e = transition::prepare(
                &input,
                &key_header(&row["header"]),
                parent,
                &history(row),
                config,
                &r,
            )
            .unwrap_err();
            assert!(e.is_operational(), "{mode}: {e:?}");
        });
        assert_eq!(roots(&input), before);
        check(&run(row, &input).unwrap(), row);
    }
}

#[test]
fn key_expiry_and_reward_storage_failure_prefixes_preserve_every_root_and_retry() {
    use super::storage::{Control, wrapped};
    use std::rc::Rc;
    let row = row(6, 8, "rich");
    let input = trees(&row["before"]);
    let before = roots(&input);
    let control = Rc::new(Control::default());
    let base = wrapped(&input, &control);
    control.reset();
    let successful = run(row, &base).unwrap();
    let counts = [control.reads.get(), control.writes.get()];
    control.reset();
    check(&successful, row);
    let mut prefixes = 0;
    for (dimension, count) in counts.into_iter().enumerate() {
        assert!(count > 0);
        for permitted in 0..count {
            control.reset();
            if dimension == 0 {
                control.read_limit.set(Some(permitted));
            } else {
                control.write_limit.set(Some(permitted));
            }
            let e = run(row, &base).unwrap_err();
            assert!(e.is_operational(), "{dimension} {permitted}: {e:?}");
            assert_eq!(roots(&base), before);
            control.reset();
            check(&run(row, &base).unwrap(), row);
            prefixes += 1;
        }
    }
    eprintln!("Key expiry/reward storage failure prefixes: {prefixes}");
    assert!(prefixes > 100);
}

fn migration(row: &J) -> ae_chain::forks::Migration {
    ae_chain::forks::Migration {
        allocations: Default::default(),
        contracts: row["specs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| ae_chain::forks::Deployment {
                key: bytes(&s["pubkey"]).try_into().unwrap(),
                nonce: unsigned(&s["nonce"]),
                amount: unsigned(&s["amount"]),
                version: ae_protocol::contracts::Version {
                    vm: s["vm_version"].as_u64().unwrap() as u16,
                    abi: s["abi_version"].as_u64().unwrap() as u16,
                },
                code: bytes(&s["code"]),
                calldata: bytes(&s["call_data"]),
            })
            .collect(),
    }
}
fn migrate<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    r: &J,
    t: &mut Trees<A, C>,
) -> Result<(), ae_chain::forks::Error<A, C>> {
    let p = r["protocol"].as_u64().unwrap() as u32;
    with_context(row(p, 6, "empty"), |_, config| {
        ae_chain::forks::apply(
            t,
            &migration(r),
            &ae_execution::Context::transaction(
                r["height"].as_u64().unwrap(),
                Protocol::try_from(p).unwrap(),
            ),
            config,
            &unsigned(&r["gas"]),
            Default::default(),
        )
    })
}
#[test]
fn ordered_isolated_fork_deployments_match_success_and_failure_side_effects() {
    use ae_chain::forks::Error;
    let mut successes = 0;
    for row in KEY["migrations"].as_array().unwrap() {
        let mut t = trees(&row["before"]);
        let before = roots(&t);
        match migrate(row, &mut t) {
            Ok(()) => {
                assert_eq!(row["result"]["kind"], "ok");
                check_state(&t, &row["result"]["value"]);
                successes += 1;
            }
            Err(e) => {
                assert_eq!(row["result"]["kind"], "native");
                assert_eq!(roots(&t), before);
                let reason = &row["result"]["reason"];
                match (row["mode"].as_str().unwrap(), e) {
                    ("revert" | "error" | "low_gas", Error::CallFailed { return_type, value }) => {
                        assert_eq!(
                            *reason,
                            tuple(vec![
                                atom("failed_apply_hard_fork_contracts"),
                                tuple(vec![
                                    atom("call_error"),
                                    atom(match return_type {
                                        ae_state::calls::ReturnType::Ok => "ok",
                                        ae_state::calls::ReturnType::Revert => "revert",
                                        ae_state::calls::ReturnType::Error => "error",
                                    }),
                                    bin(value)
                                ])
                            ])
                        );
                    }
                    ("bad_pubkey", Error::UnexpectedPublicKey) => {
                        assert_eq!(reason[1][0], atom("unexpected_pubkey"))
                    }
                    ("bad_nonce", Error::Execution(e)) => {
                        assert!(
                            matches!(*e,transactions::Error::Contract(ref c) if matches!(**c,ae_execution::contracts::Error::Primitive(ae_execution::Error::NonceTooHigh))),
                            "{e:?}"
                        );
                        assert_eq!(reason[1][1], atom("tx_nonce_too_high_for_account"));
                    }
                    (mode, e) => panic!("{} {mode}: {e:?}", row["protocol"]),
                }
            }
        }
    }
    assert_eq!(successes, 30);
}

#[test]
fn isolated_deployment_storage_failures_discard_all_partial_outputs() {
    use super::storage::{Control, wrapped};
    use std::rc::Rc;
    let row = KEY["migrations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["protocol"] == 6 && r["owner"] == "absent" && r["mode"] == "spend_first")
        .unwrap();
    let input = trees(&row["before"]);
    let control = Rc::new(Control::default());
    let base = wrapped(&input, &control);
    let before = roots(&base);
    let mut success = base.clone();
    control.reset();
    migrate(row, &mut success).unwrap();
    let counts = [control.reads.get(), control.writes.get()];
    control.reset();
    check_state(&success, &row["result"]["value"]);
    let mut prefixes = 0;
    for (dimension, count) in counts.into_iter().enumerate() {
        assert!(count > 0);
        for permitted in 0..count {
            control.reset();
            if dimension == 0 {
                control.read_limit.set(Some(permitted));
            } else {
                control.write_limit.set(Some(permitted));
            }
            let mut t = base.clone();
            migrate(row, &mut t).unwrap_err();
            assert_eq!(roots(&t), before);
            control.reset();
            migrate(row, &mut t).unwrap();
            check_state(&t, &row["result"]["value"]);
            prefixes += 1;
        }
    }
    eprintln!("Isolated migration storage failure prefixes: {prefixes}");
    assert!(prefixes > 100);
}

#[test]
fn public_reward_policy_and_genesis_anchor_cannot_be_replaced_by_local_preferences() {
    for network in [NetworkProfile::mainnet(), NetworkProfile::uat()] {
        let disabled = rewards::Policy::custom(false, vec![]).unwrap();
        let policy = rewards::Policy::for_network(&network, Some(disabled));
        let ((a, b), dev) = policy.split(&1000.into(), &1000.into(), Protocol::Fortuna);
        assert_eq!((a, b), (891.into(), 891.into()));
        assert_eq!(dev.len(), 1);
        assert_eq!(dev[0].1, 218.into());
        let ((a, b), dev) = policy.split(&1000.into(), &1000.into(), Protocol::Ceres);
        assert_eq!((a, b), (1000.into(), 1000.into()));
        assert!(dev.is_empty());
    }
    let public = KEY["forks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["network"] == "ae_uat" && r["protocol"] == 4 && r["mode"] == "empty")
        .unwrap();
    let input = trees(&public["before"]);
    for gas in [-1, 0, 1] {
        with_context(public, |parent, config| {
            let mut rules = settings(public);
            rules.admission.configured_gas_limit = gas.into();
            let result = transition::prepare(
                &input,
                &key_header(&public["header"]),
                parent,
                &history(public),
                config,
                &rules,
            )
            .unwrap();
            check(&result, public);
        });
    }
    let row = row(6, 3, "empty");
    let t = trees(&row["before"]);
    let normal = run(row, &t).unwrap();
    let result = with_context(row, |parent, config| {
        let mut rules = settings(row);
        rules.genesis_hash = Hash256::ZERO;
        transition::prepare(
            &t,
            &key_header(&row["header"]),
            parent,
            &history(row),
            config,
            &rules,
        )
        .unwrap()
    });
    let award = normal.reward().unwrap();
    assert_eq!(award.first, settings(row).genesis_hash);
    assert_eq!(award.distribution.first, 0.into());
    assert_eq!(
        result.reward().unwrap().distribution.first,
        Unsigned::from_decimal("4938271560493827156").unwrap()
    );
    assert_ne!(roots(result.trees()), roots(normal.trees()));
    let mut accounts = Accounts::default();
    let root = accounts.root_hash();
    assert!(matches!(
        rewards::lock(&mut accounts, &(-1).into()),
        Err(rewards::Error::InsufficientLockedBalance)
    ));
    assert_eq!(accounts.root_hash(), root);
    rewards::lock(&mut accounts, &0.into()).unwrap();
    assert_eq!(accounts.root_hash(), root);
}

#[test]
fn community_fork_resolutions_apply_original_transition_states() {
    use ae_protocol::signaling::{Fork, Prior};
    let mut checked = 0;
    for row in KEY["custom"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["protocol"].as_u64().unwrap() > 2)
    {
        let header = key_header(&row["header"]);
        let height = header.common.height;
        if Header::decode(&bytes(&row["previous"]), HeaderMode::ProofOfWork)
            .unwrap()
            .common()
            .version
            == header.common.version
        {
            continue;
        }
        let input = trees(&row["before"]);
        let before = roots(&input);
        with_context(row, |parent, config| {
            let p = NetworkProfile::custom(
                config.network.network_id().to_vec(),
                vec![Activation {
                    height: 0,
                    protocol: Protocol::try_from(parent.headers.previous.common().version).unwrap(),
                }],
            )
            .unwrap()
            .with_community_fork(Some(Fork {
                start: height - 1,
                end: height,
                threshold: 1,
                info: 7,
                version: header.common.version,
            }))
            .unwrap();
            let unresolved = transactions::Config::primitives(&p);
            let error = transition::pow_key(
                &header,
                &mut input.clone(),
                parent,
                &history(row),
                &unresolved,
                &settings(row),
            )
            .unwrap_err();
            assert!(error.is_operational());
            assert!(matches!(
                error,
                transition::Error::Admission(PowError::Header(
                    ae_consensus::headers::Error::Protocol(
                        ae_protocol::ProfileError::MissingForkDecision { .. }
                    )
                ))
            ));
            let selected = p.resolve(height, Prior::SignalingResult(true)).unwrap();
            let bound = transactions::Config {
                fate: config.fate,
                aevm: config.aevm,
                ..transactions::Config::primitives(&selected)
            };
            let prepared = transition::prepare(
                &input,
                &header,
                parent,
                &history(row),
                &bound,
                &settings(row),
            )
            .unwrap();
            check(&prepared, row);
            let failed = p.resolve(height, Prior::SignalingResult(false)).unwrap();
            assert!(matches!(
                failed.validate_version(height, header.common.version),
                Err(ae_protocol::ProfileError::WrongProtocol { .. })
            ));
        });
        assert_eq!(roots(&input), before);
        checked += 1;
    }
    assert_eq!(checked, 24);
}
