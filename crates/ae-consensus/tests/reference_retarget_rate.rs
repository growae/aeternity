// Original aec_target/aeminer_pow, fa2a6d7/c6b6a6d.
// Copyright (c) Aeternity developers. ISC notice in repository LICENSE.
use ae_consensus::target::{MineRate, Target, TargetError, WorkSample, retarget_with_rate};
use serde_json::Value;

#[test]
fn configured_intervals_match_all_original_adjustment_results() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/erlang_retarget_rate.json")).unwrap();
    let mut successes = 0;
    let mut outside = 0;
    for row in fixture["rows"].as_array().unwrap() {
        let input = &row["input"];
        let rate = input["rate"]
            .as_number()
            .and_then(|n| n.as_str().parse().ok())
            .map_or(MineRate::NonInteger, MineRate::Integer);
        let samples: Vec<_> = input["samples"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| WorkSample {
                height: s["height"].as_u64().unwrap(),
                timestamp: s["time"].as_u64().unwrap(),
                target: Target(s["target"].as_u64().unwrap().try_into().unwrap()),
            })
            .collect();
        let actual = retarget_with_rate(&samples, &rate);
        if let Some(expected) = row["recalculate"].get("ok") {
            if let Some(target) = expected.as_u64().and_then(|n| u32::try_from(n).ok()) {
                assert_eq!(actual, Ok(Target(target)), "{input}");
                assert_eq!(row["verify_exact"]["ok"], true);
                assert_eq!(row["verify_wrong"]["ok"]["expected"], *expected);
                successes += 1;
            } else {
                assert_eq!(actual, Err(TargetError::OutsideHeaderDomain), "{input}");
                outside += 1;
            }
        } else {
            assert!(actual.is_err(), "{input}: {actual:?}");
            if samples.len() != 18 {
                assert_eq!(actual, Err(TargetError::InvalidWindow));
            }
        }
    }
    assert_eq!(fixture["rows"].as_array().unwrap().len(), 448);
    assert_eq!(successes + outside, 242);
    eprintln!(
        "Native configured retarget: 448 cases; {successes} header-domain successes; {outside} out-of-domain results"
    );
}
