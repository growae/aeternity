use super::*;
use ae_chain::{
    key::{
        self as transition,
        candidate::{self, Candidate},
    },
    rewards,
};
use ae_codec::headers::KeyHeader;
use ae_consensus::target::{HIGHEST_TARGET, TargetError};
use ae_types::Hash256;
static NATIVE: LazyLock<J> =
    LazyLock::new(|| unpack_fixture(include_str!("../fixtures/erlang_pow_key_candidate.json")));
fn header(v: &J) -> KeyHeader {
    let Header::Key(h) = Header::decode(&bytes(v), HeaderMode::ProofOfWork).unwrap() else {
        panic!()
    };
    h
}
fn configured<T>(
    row: &J,
    profile: &J,
    f: impl FnOnce(
        &ex::Parent<'_>,
        &rewards::History,
        &transactions::Config<'_>,
        &transition::Rules,
    ) -> T,
) -> T {
    let p = Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap();
    let height = row["height"].as_u64().unwrap();
    let mut network = match row["network"].as_str().unwrap() {
        "ae_mainnet" => NetworkProfile::mainnet(),
        "ae_uat" => NetworkProfile::uat(),
        id => NetworkProfile::custom(
            id.as_bytes().to_vec(),
            if row["advance"].as_bool().unwrap() {
                vec![
                    Activation {
                        height: 0,
                        protocol: Protocol::try_from(p as u32 - 1).unwrap(),
                    },
                    Activation {
                        height,
                        protocol: p,
                    },
                ]
            } else {
                vec![Activation {
                    height: 0,
                    protocol: p,
                }]
            },
        )
        .unwrap(),
    };
    if row["signal"].as_bool().unwrap() {
        network = network
            .with_community_fork(Some(ae_protocol::signaling::Fork {
                start: height,
                end: height + 1,
                threshold: 1,
                info: 987654,
                version: 9,
            }))
            .unwrap();
    }
    let context = vm();
    let env = environment();
    let order = otp_order::profile(profile);
    let overrides = Overrides::default();
    let fate = fate::Config {
        context: &context,
        order: &order,
        overrides: &overrides,
        environment: &env,
        history: Arc::default(),
        auth_tx: None,
        metering_floor: None,
        diagnostic_profile: ae_vm_fate::diagnostic::Profile::Test,
    };
    let aevm = aevm();
    let config = transactions::Config {
        fate: Some(&fate),
        aevm: Some(&aevm),
        ..transactions::Config::primitives(&network)
    };
    let previous = Header::decode(&bytes(&row["parent"]), HeaderMode::ProofOfWork).unwrap();
    let key = header(&row["key"]);
    let parent = ex::Parent {
        headers: ae_consensus::headers::Context {
            previous: &previous,
            previous_key: &key,
            recent_keys: &[],
        },
        previous_generation: ex::PreviousGeneration::Unavailable,
        fees: 123.into(),
        reported_fraud: true,
    };
    let mut rules = transition::Rules::new(
        super::rules(),
        Hash256(bytes(&row["genesis"]).try_into().unwrap()),
    );
    rules.reward_delay = row["delay"].as_u64().unwrap();
    if row["advance"].as_bool().unwrap() && row["network"] == "ae_pow_key_reference" {
        let account = |n: u8| {
            let mut k = [0; 32];
            k[31] = n;
            AccountPublicKey(k)
        };
        rules.custom_forks.insert(
            p,
            ae_chain::forks::Allocations {
                accounts: vec![
                    (account(1), 111.into()),
                    (account(0), 222.into()),
                    (account(91), 0.into()),
                ],
                extra: vec![(account(1), 333.into()), (account(92), 444.into())],
            },
        );
    }
    let history = row["history"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            (
                Hash256(bytes(&v["hash"]).try_into().unwrap()),
                rewards::Record {
                    header: header(&v["header"]),
                    fees: Some(Unsigned::from_decimal(v["fees"].as_str().unwrap()).unwrap()),
                    reported_fraud: v["fraud"].as_bool(),
                },
            )
        })
        .collect();
    f(&parent, &history, &config, &rules)
}
fn input(row: &J) -> candidate::Input {
    let mut miner = [0; 32];
    miner[31] = 9;
    let mut beneficiary = [0; 32];
    beneficiary[31] = 3;
    candidate::Input {
        miner: AccountPublicKey(miner),
        beneficiary: AccountPublicKey(beneficiary),
        calculation_time: row["clocks"][0].as_u64().unwrap(),
        release_info: row["release"].as_u64().unwrap() as u32,
    }
}
#[test]
fn original_pow_key_candidates_match_temporary_and_final_bytes_and_all_state_trees() {
    let mut cases = 0;
    for profile in ["profile", "repeat_profile"] {
        for (index, row) in NATIVE["rows"].as_array().unwrap().iter().enumerate() {
            if !row["result"]["error"].is_null() {
                continue;
            }
            configured(row, &NATIVE[profile], |parent, history, config, rules| {
                let trees = trees(&row["before"]);
                let roots = trees.roots();
                let candidate =
                    Candidate::prepare(&trees, parent, history, config, rules, input(row))
                        .unwrap_or_else(|e| panic!("case {index}: {e:?}"));
                assert_eq!(
                    hex::encode(Header::Key(candidate.temporary().clone()).encode().unwrap()),
                    row["result"]["temporary"],
                    "temporary case {index}"
                );
                let adjustment: Vec<_> = row["adjustment"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(header)
                    .collect();
                let final_header = candidate
                    .finish(
                        row["clocks"][1].as_u64().unwrap(),
                        HIGHEST_TARGET,
                        &adjustment,
                    )
                    .unwrap();
                assert_eq!(
                    hex::encode(Block::Key(final_header).encode().unwrap()),
                    row["result"]["block"],
                    "final case {index}"
                );
                let t = candidate.trees();
                let expected = &row["result"]["state"];
                assert_eq!(
                    bin(t.roots().hash().0),
                    *field(expected, "root"),
                    "root case {index}"
                );
                for (name, tree) in [
                    ("accounts", t.accounts.tree()),
                    ("contracts", t.contracts.tree()),
                    ("calls", t.calls.tree()),
                    ("channels", t.channels.tree()),
                    ("names", t.names.tree()),
                    ("name_cache", t.names.cache()),
                    ("oracles", t.oracles.tree()),
                    ("oracle_cache", t.oracles.cache()),
                ] {
                    assert_eq!(entries(tree), *field(expected, name), "{name} case {index}");
                }
                assert_eq!(trees.roots(), roots);
                assert_eq!(
                    candidate.reward().is_some(),
                    row["height"].as_u64().unwrap() > row["delay"].as_u64().unwrap()
                );
            });
            cases += 1;
        }
    }
    assert_eq!(cases, 428);
}
#[test]
fn adjustment_errors_follow_state_calculation_and_keep_prepared_state_retryable() {
    let mut cases = 0;
    for row in NATIVE["rows"].as_array().unwrap() {
        let kind = row["failure"].as_str().unwrap();
        if !["short_headers", "long_headers", "zero_target"].contains(&kind) {
            continue;
        }
        configured(row, &NATIVE["profile"], |parent, history, config, rules| {
            let t = trees(&row["before"]);
            let roots = t.roots();
            let c = Candidate::prepare(&t, parent, history, config, rules, input(row)).unwrap();
            let prepared = c.trees().roots();
            let headers: Vec<_> = row["adjustment"]
                .as_array()
                .unwrap()
                .iter()
                .map(header)
                .collect();
            let error = c.finish(10017, HIGHEST_TARGET, &headers).unwrap_err();
            match kind {
                "short_headers" => assert!(matches!(
                    error,
                    candidate::Error::Adjustment {
                        expected: 18,
                        actual: 17
                    }
                )),
                "long_headers" => assert!(matches!(
                    error,
                    candidate::Error::Adjustment {
                        expected: 18,
                        actual: 19
                    }
                )),
                _ => assert!(matches!(
                    error,
                    candidate::Error::Target(TargetError::ZeroTarget)
                )),
            }
            let native_error = if kind == "zero_target" {
                tuple(vec![atom("error"), atom("badarith")])
            } else {
                tuple(vec![
                    atom("error"),
                    tuple(vec![
                        atom("failed_to_adjust_target"),
                        tuple(vec![
                            atom("wrong_headers_for_target_adjustment"),
                            json!(["integer", "18"]),
                            json!(["integer", if kind == "short_headers" { "17" } else { "19" }]),
                        ]),
                    ]),
                ])
            };
            assert_eq!(row["result"]["error"], native_error);
            assert_eq!(c.trees().roots(), prepared);
            assert_eq!(t.roots(), roots);
            let valid: Vec<_> = row["history"]
                .as_array()
                .unwrap()
                .iter()
                .take(18)
                .map(|r| header(&r["header"]))
                .collect();
            assert!(c.finish(10018, HIGHEST_TARGET, &valid).is_ok());
        });
        cases += 1;
    }
    assert_eq!(cases, 3);
}

#[test]
fn candidate_guards_and_execution_limits_preserve_the_input_state() {
    let row = NATIVE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["protocol"] == 6
                && r["height"] == 19
                && r["mode"] == "rich"
                && r["parent_kind"] == "micro"
        })
        .unwrap();
    configured(row, &NATIVE["profile"], |parent, history, config, rules| {
        let t = trees(&row["before"]);
        let before = t.roots();
        let mut wrong =
            transactions::Config::<ae_vm_fate::maps::order::Otp26>::primitives(config.network);
        wrong.consensus = ae_protocol::ConsensusType::Pos;
        assert!(matches!(
            Candidate::prepare(&t, parent, history, &wrong, rules, input(row)),
            Err(candidate::Error::WrongConsensus)
        ));
        let mut previous = parent.headers.previous.clone();
        let mut key = parent.headers.previous_key.clone();
        for fault in 0..4 {
            match fault {
                0 => common_mut(&mut previous).state_hash = [99; 32],
                1 => {
                    previous = parent.headers.previous.clone();
                    key.common.height += 1;
                }
                2 => {
                    key = parent.headers.previous_key.clone();
                    if let Header::Micro(m) = &mut previous {
                        m.common.previous_key_hash = [99; 32];
                    }
                }
                _ => {
                    previous = parent.headers.previous.clone();
                    common_mut(&mut previous).height = u64::MAX;
                    key.common.height = u64::MAX;
                    if let Header::Micro(m) = &mut previous {
                        m.common.previous_key_hash = Header::Key(key.clone()).hash().unwrap().0;
                    }
                }
            }
            let broken = ex::Parent {
                headers: ae_consensus::headers::Context {
                    previous: &previous,
                    previous_key: &key,
                    recent_keys: &[],
                },
                previous_generation: ex::PreviousGeneration::Unavailable,
                fees: 123.into(),
                reported_fraud: true,
            };
            let result = Candidate::prepare(&t, &broken, history, config, rules, input(row));
            assert!(matches!(
                result,
                Err(candidate::Error::Parent | candidate::Error::Height)
            ));
            assert_eq!(t.roots(), before);
        }
        let mut restricted = rules.clone();
        restricted.max_calls = 0;
        assert!(matches!(
            Candidate::prepare(&t, parent, history, config, &restricted, input(row)),
            Err(candidate::Error::Transition(_))
        ));
        assert_eq!(t.roots(), before);
        assert!(Candidate::prepare(&t, parent, history, config, rules, input(row)).is_ok());
    });
}

fn common_mut(h: &mut Header) -> &mut ae_codec::headers::CommonHeader {
    match h {
        Header::Key(k) => &mut k.common,
        Header::Micro(m) => &mut m.common,
    }
}

#[test]
fn key_candidate_backend_failure_prefixes_discard_partial_state_and_allow_retry() {
    use super::storage::{Control, wrapped};
    use std::rc::Rc;
    let row = NATIVE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["protocol"] == 6
                && r["height"] == 19
                && r["mode"] == "rich"
                && r["parent_kind"] == "micro"
        })
        .unwrap();
    configured(row, &NATIVE["profile"], |parent, history, config, rules| {
        let trees = trees(&row["before"]);
        let roots = trees.roots();
        let control = Rc::new(Control::default());
        let base = wrapped(&trees, &control);
        control.reset();
        let expected = Candidate::prepare(&base, parent, history, config, rules, input(row))
            .unwrap()
            .trees()
            .roots();
        let counts = [control.reads.get(), control.writes.get()];
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
                assert!(matches!(
                    Candidate::prepare(&base, parent, history, config, rules, input(row)),
                    Err(candidate::Error::Transition(_))
                ));
                assert_eq!(base.roots(), roots);
                control.reset();
                assert_eq!(
                    Candidate::prepare(&base, parent, history, config, rules, input(row))
                        .unwrap()
                        .trees()
                        .roots(),
                    expected
                );
                prefixes += 1;
            }
        }
        eprintln!("PoW key candidate storage failure prefixes: {prefixes}");
        assert!(prefixes > 100);
    });
}

#[test]
fn pre_minerva_signalling_info_matches_native_header_construction_rejection() {
    let native: J = serde_json::from_str(include_str!(
        "../fixtures/erlang_pow_key_candidate_edges.json"
    ))
    .unwrap();
    for row in native["rows"].as_array().unwrap() {
        assert_eq!(
            row["result"]["error"],
            tuple(vec![atom("error"), atom("function_clause")])
        );
        configured(row, &native["profile"], |parent, history, config, rules| {
            let t = trees(&row["before"]);
            let c = Candidate::prepare(&t, parent, history, config, rules, input(row)).unwrap();
            let adjustment: Vec<_> = row["adjustment"]
                .as_array()
                .unwrap()
                .iter()
                .map(header)
                .collect();
            assert!(matches!(
                c.finish(10017, HIGHEST_TARGET, &adjustment),
                Err(candidate::Error::Encoding(
                    ae_codec::headers::HeaderError::VersionInfo
                ))
            ));
        });
    }
}
