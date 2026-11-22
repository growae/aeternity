use super::*;

static FINALIZED: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../../fixtures/erlang_hc_finalized.json")).unwrap()
});

pub(super) fn check_calls<A: ae_trie::NodeReader, C: ae_trie::NodeReader>(
    trees: &Trees<A, C>,
    expected: &J,
) {
    let Some(calls) = expected["calls"].as_array() else {
        return;
    };
    for row in calls {
        let id = bytes(&row["id"]).try_into().unwrap();
        let native = ae_state::calls::Call::deserialize(id, &bytes(&row["encoded"])).unwrap();
        let actual = trees.calls.lookup(&native.contract, id).unwrap().unwrap();
        assert_eq!(actual, native);
        assert_eq!(actual.serialize().unwrap(), bytes(&row["encoded"]));
        assert_eq!(actual.return_type, ae_state::calls::ReturnType::Ok);
        assert_eq!(number(&actual.gas_used).to_string(), row["gas_used"]);
        assert_eq!(actual.return_value, bytes(&row["return_value"]));
    }
}

#[test]
fn native_epoch_finalization_matches_calls_signed_blocks_and_successor_epochs() {
    for profile in ["profile", "repeat_profile"] {
        let (mut blocks, mut calls, mut epoch_ends) = (0, 0, 0);
        for row in FINALIZED["rows"].as_array().unwrap() {
            walk_corpus(&FINALIZED, profile, row, |s| {
                blocks += 1;
                calls += s.expected["calls"].as_array().unwrap().len();
                if let Block::Key(k) = s.block {
                    assert_eq!(
                        k.end_of_epoch,
                        s.expected["end_of_epoch"].as_bool().unwrap()
                    );
                    epoch_ends += usize::from(k.end_of_epoch);
                }
            });
        }
        assert_eq!((blocks, calls, epoch_ends), (91, 14, 14));
    }
}

#[test]
fn native_finalized_admission_matches_successes_rejections_and_cache_prefixes() {
    for profile in ["profile", "repeat_profile"] {
        let (mut accepted, mut rejected, mut wrong_forks) = (0, 0, 0);
        for row in FINALIZED["rows"].as_array().unwrap() {
            walk_corpus(&FINALIZED, profile, row, |s| {
                if !s.expected["fork_probe"].is_null() {
                    wrong_fork(&s, &s.expected["fork_probe"]);
                    wrong_forks += 1;
                }
                for p in s.expected["probes"].as_array().unwrap() {
                    let b = Block::decode(
                        &bytes(&p["block"]),
                        DecodeConfig {
                            mode: HeaderMode::Hyperchain,
                            ..Default::default()
                        },
                    )
                    .unwrap();
                    let (mut state, mut host) = (s.state.clone(), s.host.clone());
                    let roots = s.parent.view.trees.roots();
                    let result = match hc::execute(
                        &b, s.parent, s.history, s.reader, &mut state, &mut host, s.rules,
                    ) {
                        Ok(out) => {
                            check(&out.trees, &s.expected["trees"]);
                            accepted += 1;
                            ok(atom("ok"))
                        }
                        Err(e) => {
                            assert!(!e.is_operational::<Host>(), "{} {e:?}", p["name"]);
                            rejected += 1;
                            failures::rejection(e, &b)
                        }
                    };
                    assert_eq!(
                        normalize(&result),
                        normalize(&p["result"]),
                        "{} {} {}",
                        row["protocol"],
                        s.expected["height"],
                        p["name"]
                    );
                    assert_eq!(normalize(&block::cache(&state)), normalize(&p["cache"]));
                    assert_eq!(roots, s.parent.view.trees.roots());
                }
            });
        }
        assert_eq!((accepted, rejected), (14, 210));
        assert_eq!(wrong_forks, 14);
    }
}

fn wrong_fork(s: &Step<'_>, probe: &J) {
    let decode = |value: &J| {
        Block::decode(
            &bytes(value),
            DecodeConfig {
                mode: HeaderMode::Hyperchain,
                ..Default::default()
            },
        )
        .unwrap()
    };
    let micro = decode(&probe["micro"]["block"]);
    let (mut state, mut host) = (s.state.clone(), s.host.clone());
    let out = hc::execute(
        &micro, s.parent, s.history, s.reader, &mut state, &mut host, s.rules,
    )
    .unwrap();
    check(&out.trees, &probe["micro"]["trees"]);
    check_calls(&out.trees, &probe["micro"]);
    assert_eq!(hex::encode(micro.hash().unwrap().0), probe["micro"]["hash"]);
    assert_eq!(number(&out.fees).to_string(), probe["micro"]["fees"]);
    assert_eq!(
        normalize(&list(out.transactions.iter().map(tx_event).collect())),
        normalize(&probe["micro"]["events"])
    );
    assert_eq!(
        normalize(&block::cache(&state)),
        normalize(&probe["micro"]["cache"])
    );
    let closing = decode(&probe["key_block"]);
    let Block::Key(k) = &closing else { panic!() };
    let header = micro.header();
    let pv = context(&header, s.parent.key, &out.trees, s.history);
    let parent = hc::Parent {
        header: &header,
        key: s.parent.key,
        view: &pv,
        fees: &out.fees,
        execution_history: history(s.history, Some(k)),
    };
    let error = hc::execute(
        &closing, &parent, s.history, s.reader, &mut state, &mut host, s.rules,
    )
    .unwrap_err();
    assert!(matches!(error, hc::Error::InvalidEndOfEpoch));
    assert_eq!(
        normalize(&failures::rejection(error, &closing)),
        normalize(&probe["result"])
    );
    assert_eq!(normalize(&block::cache(&state)), normalize(&probe["cache"]));
}

#[test]
fn finalized_contract_calls_and_epoch_events_survive_commit_and_reopen() {
    assert_eq!(
        storage::commit_and_reopen(&FINALIZED, "finalized-reopen"),
        91
    );
}

#[test]
fn finalized_epochs_rollback_and_replay_across_protocol_activations() {
    storage::rollback_and_replay(&FINALIZED, "finalized-rollback");
}
