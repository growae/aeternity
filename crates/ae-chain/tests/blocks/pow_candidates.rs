use super::*;
use ae_chain::hyperchain::candidate::{self as core, Micro, Packing, PowParent, PowStart};
use ae_hyperchain::chain::{self, Profile, Reader, Selected, Selection};
use std::convert::Infallible;
static NATIVE: LazyLock<J> =
    LazyLock::new(|| unpack_fixture(include_str!("../fixtures/erlang_pow_candidates.json")));
fn check(candidate: &Micro, expected: &J) {
    assert_eq!(
        hex::encode(Block::Micro(candidate.block().clone()).encode().unwrap()),
        expected["block"].as_str().unwrap()
    );
    let native = trees(&expected["state"]);
    assert_eq!(candidate.trees().roots(), native.roots());
    let context = &expected["context"];
    assert_eq!(
        candidate.context().height.to_string(),
        context["height"].as_u64().unwrap().to_string()
    );
    assert_eq!(
        candidate.environment().timestamp.to_string(),
        context["time"].as_u64().unwrap().to_string()
    );
    assert_eq!(
        candidate.environment().difficulty.to_string(),
        context["difficulty"].as_str().unwrap()
    );
    assert_eq!(
        hex::encode(candidate.environment().key_hash),
        context["key_hash"]
    );
    assert_eq!(
        hex::encode(candidate.environment().beneficiary),
        context["beneficiary"]
    );
}
fn transactions(values: &J) -> Vec<core::Signed> {
    values
        .as_array()
        .unwrap()
        .iter()
        .map(|v| core::Signed::decode(&bytes(v)).unwrap())
        .collect()
}
#[test]
fn original_pow_candidates_pack_update_and_preserve_historical_vm_roots() {
    let mut cases = 0;
    for capture in ["profile", "repeat_profile"] {
        for row in NATIVE["rows"].as_array().unwrap() {
            let header = Header::decode(&bytes(&row["parent"]), HeaderMode::ProofOfWork).unwrap();
            let Header::Key(key) =
                Header::decode(&bytes(&row["key"]), HeaderMode::ProofOfWork).unwrap()
            else {
                panic!()
            };
            let mut env = environment();
            env.key_hash = Header::Key(key.clone()).hash().unwrap().0;
            let view = chain::View {
                trees: Arc::new(trees(&row["before"])),
                context: ae_execution::Context::transaction(
                    key.common.height,
                    Protocol::try_from(key.common.version).unwrap(),
                ),
                environment: env,
                history: Arc::default(),
                aevm: Some(aevm()),
            };
            let profile = Profile {
                contracts: ae_hyperchain::consensus_contract::Contracts {
                    owner: AccountPublicKey([0; 32]),
                    election: [0; 32],
                    staking: [0; 32],
                    rewards: [0; 32],
                },
                network: NetworkProfile::mainnet(),
                context: vm(),
                order: otp_order::profile(&NATIVE[capture]),
                overrides: Overrides::default(),
                diagnostic: ae_vm_fate::diagnostic::Profile::Test,
                limits: Default::default(),
                maximum_steps: 100_000,
                maximum_history_reads: 1000,
            };
            let reader = Reader::new(Selected::new(Selection::default()), Arc::new(profile));
            if cases == 0 {
                let roots = view.trees.roots();
                for fault in 0..4 {
                    let mut invalid = view.clone();
                    match fault {
                        0 => {
                            invalid.environment.consensus =
                                machine::program::remote::context::Consensus::Pos
                        }
                        1 => invalid.environment.key_hash = [99; 32],
                        2 => {
                            Arc::make_mut(&mut invalid.trees).accounts =
                                Accounts::new(MemoryStore::default())
                        }
                        _ => (),
                    }
                    let result = Micro::begin_pow(
                        PowParent {
                            header: &header,
                            key: &key,
                            view: &invalid,
                            reported_fraud: false,
                        },
                        &reader,
                        &6_000_000.into(),
                        PowStart {
                            now: if fault == 3 { i128::MAX } else { 0 },
                            micro_cycle_ms: 3000,
                            proof: None,
                            limits: Default::default(),
                        },
                    );
                    match fault {
                        0 | 1 => assert!(matches!(
                            result,
                            Err(core::Error::Chain(
                                ae_chain::hyperchain::Error::Configuration
                            ))
                        )),
                        2 => assert!(matches!(
                            result,
                            Err(core::Error::Chain(ae_chain::hyperchain::Error::ParentState))
                        )),
                        _ => assert!(matches!(result, Err(core::Error::ResourceLimit))),
                    }
                    assert_eq!(view.trees.roots(), roots);
                }
            }
            let proof = row["proof"].as_bool().unwrap().then(|| {
                Box::new(
                    fraud::Proof::decode(
                        &bytes(&row["proof_bytes"][0]),
                        HeaderMode::ProofOfWork,
                        Default::default(),
                    )
                    .unwrap(),
                )
            });
            let candidate = Micro::begin_pow(
                PowParent {
                    header: &header,
                    key: &key,
                    view: &view,
                    reported_fraud: row["reported"].as_bool().unwrap(),
                },
                &reader,
                &6_000_000.into(),
                PowStart {
                    now: row["now"].as_i64().unwrap().into(),
                    micro_cycle_ms: row["cycle"].as_u64().unwrap(),
                    proof,
                    limits: Default::default(),
                },
            )
            .unwrap();
            let mut packing =
                Packing::new::<Infallible>(candidate, 0.into(), 0, core::Timeout::Disabled, 3000)
                    .unwrap();
            let offered = transactions(&row["offered"]);
            assert!(packing.request::<Infallible>(|| 0).unwrap().is_some());
            packing
                .selected::<Infallible, _, _>(&offered, false, &reader)
                .unwrap();
            assert!(packing.request::<Infallible>(|| 0).unwrap().is_some());
            packing
                .selected::<Infallible, _, _>(&[], false, &reader)
                .unwrap();
            let mut candidate = packing.finish::<Infallible>().unwrap();
            check(&candidate, &row["result"]);
            for update in row["updates"].as_array().unwrap() {
                let previous = Block::Micro(candidate.block().clone()).encode().unwrap();
                let roots = candidate.trees().roots();
                let result =
                    candidate.update::<Infallible, _, _>(&transactions(&update["txs"]), &reader);
                if let Some(error) = update["error"].as_str() {
                    assert!(matches!(
                        (error, result),
                        ("no_update_to_block_candidate", Err(core::Error::NoUpdate))
                            | ("block_is_full", Err(core::Error::Full))
                    ));
                    assert_eq!(
                        previous,
                        Block::Micro(candidate.block().clone()).encode().unwrap()
                    );
                    assert_eq!(roots, candidate.trees().roots());
                } else {
                    result.unwrap();
                    check(&candidate, &update["result"]);
                }
            }
            cases += 1;
        }
    }
    assert_eq!(cases, 480);
}
