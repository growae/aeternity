use super::*;
use ae_chain::admission::BodyError;
use ae_hyperchain::signing;

pub(super) fn rejection(e: hc::Error<Unavailable>, b: &Block) -> J {
    let header = |e| tuple(vec![atom("header"), e]);
    let reason = match e {
        hc::Error::Protocol(ae_protocol::ProfileError::WrongProtocol { .. }) => {
            header(atom("protocol_version_mismatch"))
        }
        hc::Error::FutureTimestamp => header(atom("block_from_the_future")),
        hc::Error::MicroInterval => header(atom("bad_micro_block_interval")),
        hc::Error::Height => atom(match b {
            Block::Key(_) => "height_inconsistent_for_keyblock_with_previous_hash",
            Block::Micro(_) => "height_inconsistent_for_microblock_with_previous_hash",
        }),
        hc::Error::WrongTarget => atom("wrong_number_of_non_holes"),
        hc::Error::InvalidEndOfEpoch => atom("eoe_invalid"),
        hc::Error::Micro(signing::MicroError::Signing(signing::Error::Signature)) => {
            atom("signature_verification_failed")
        }
        hc::Error::Body(BodyError::TransactionsHash) => {
            tuple(vec![atom("block"), atom("malformed_txs_hash")])
        }
        hc::Error::Seal(signing::Error::Padding(p)) => header(tuple(vec![
            atom("erroneous_seal"),
            list(p.iter().map(int).collect()),
            list(vec![int(0); 26]),
            list(std::iter::once(int(1)).chain(vec![int(0); 25]).collect()),
        ])),
        hc::Error::StateHash { actual, expected } => {
            return json!({"kind":"exception","class":"throw","reason":tuple(vec![
                atom("aec_chain_state_error"),
                tuple(vec![atom("root_hash_mismatch"), bin(actual.0), bin(expected.0)])
            ])});
        }
        e => panic!("unexpected native rejection {e:?}"),
    };
    ok(tuple(vec![atom("error"), reason]))
}

#[test]
fn native_admission_failures_preserve_exact_reasons_and_cache_prefixes() {
    let mut count = 0;
    for row in LEDGER["rows"].as_array().unwrap() {
        walk("profile", row, |s| {
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
                let e = hc::execute(
                    &b, s.parent, s.history, s.reader, &mut state, &mut host, s.rules,
                )
                .unwrap_err();
                assert!(!e.is_operational::<Host>(), "{} {e:?}", p["name"]);
                assert_eq!(
                    normalize(&rejection(e, &b)),
                    normalize(&p["result"]),
                    "{} {}",
                    row["protocol"],
                    p["name"]
                );
                assert_eq!(
                    normalize(&block::cache(&state)),
                    normalize(&p["cache"]),
                    "{} {}",
                    row["protocol"],
                    p["name"]
                );
                assert_eq!(roots, s.parent.view.trees.roots());
                count += 1;
            }
        });
    }
    assert_eq!(count, 147);
}

fn unsigned(j: &J) -> Unsigned {
    Unsigned::from_field_bytes(
        &j.as_str()
            .unwrap()
            .parse::<BigInt>()
            .unwrap()
            .to_bytes_be()
            .1,
    )
    .unwrap()
}
#[test]
fn native_pos_fork_selection_includes_zero_scores_and_equal_height_ties() {
    let rows = LEDGER["fork_choice"].as_array().unwrap();
    assert_eq!(rows.len(), 144);
    for r in rows {
        assert_eq!(
            hc::keep_tip(
                &unsigned(&r["old_score"]),
                &unsigned(&r["new_score"]),
                r["old_height"].as_u64().unwrap(),
                r["new_height"].as_u64().unwrap()
            ),
            r["keep"].as_bool().unwrap()
        );
    }
}

#[test]
fn operational_failures_discard_staged_caches_host_reads_and_roots_before_retry() {
    let row = LEDGER["rows"].as_array().unwrap().last().unwrap();
    let mut checked = 0;
    walk("profile", row, |s| {
        let (mut state, mut host) = (s.state.clone(), s.host.clone());
        let roots = s.parent.view.trees.roots();
        if matches!(s.block, Block::Micro(m) if m.header.common.height == 1) {
            host.unavailable = true;
            let before = host.clone();
            let e = hc::execute(
                s.block, s.parent, s.history, s.reader, &mut state, &mut host, s.rules,
            )
            .unwrap_err();
            assert!(matches!(
                e,
                hc::Error::Micro(signing::MicroError::Leader(cb::Error::Host(_)))
            ));
            assert!(e.is_operational::<Host>());
            assert_eq!(&state, s.state);
            assert_eq!(host, before);
            host.unavailable = false;

            let mut rules = s.rules.clone();
            rules.body_limits.max_transactions = 0;
            let e = hc::execute(
                s.block, s.parent, s.history, s.reader, &mut state, &mut host, &rules,
            )
            .unwrap_err();
            assert!(matches!(e, hc::Error::Body(BodyError::ResourceLimit)));
            assert!(e.is_operational::<Host>());
            assert_eq!(&state, s.state);
            assert_eq!(&host, s.host);
            checked += 2;
        } else if matches!(s.block, Block::Key(k) if k.common.height == 3) {
            let mut rules = s.rules.clone();
            rules.max_reward_history = 0;
            let e = hc::execute(
                s.block, s.parent, s.history, s.reader, &mut state, &mut host, &rules,
            )
            .unwrap_err();
            assert!(matches!(
                e,
                hc::Error::Rewards(rewards::Error::ResourceLimit)
            ));
            assert!(e.is_operational::<Host>());
            assert_eq!(&state, s.state);
            assert_eq!(&host, s.host);

            let mut history = s.history.clone();
            let (_, record) = history
                .iter_mut()
                .find(|(_, r)| r.header.common.height == 1)
                .unwrap();
            record.fees = None;
            let e = hc::execute(
                s.block, s.parent, &history, s.reader, &mut state, &mut host, s.rules,
            )
            .unwrap_err();
            assert!(matches!(
                e,
                hc::Error::Rewards(rewards::Error::FeesMissing(_))
            ));
            assert!(e.is_operational::<Host>());
            assert_eq!(&state, s.state);
            assert_eq!(&host, s.host);

            let mut profile = s.reader.profile.as_ref().clone();
            profile.maximum_steps = 0;
            let limited = R::new(s.reader.selected.clone(), Arc::new(profile));
            let e = hc::execute(
                s.block, s.parent, s.history, &limited, &mut state, &mut host, s.rules,
            )
            .unwrap_err();
            assert!(e.is_operational::<Host>(), "{e:?}");
            assert_eq!(&state, s.state);
            assert_eq!(&host, s.host);
            checked += 3;
        } else {
            return;
        }
        assert_eq!(roots, s.parent.view.trees.roots());
        let out = hc::execute(
            s.block, s.parent, s.history, s.reader, &mut state, &mut host, s.rules,
        )
        .unwrap();
        check(&out.trees, &s.expected["trees"]);
        assert_eq!(
            normalize(&block::cache(&state)),
            normalize(&s.expected["cache"])
        );
    });
    assert_eq!(checked, 5);
}
