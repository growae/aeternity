use ae_consensus::{
    cuckoo,
    target::{Target, TargetError, WorkSample, retarget},
};
use ae_types::Unsigned;
use serde_json::Value;
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/erlang_pow.json")).unwrap()
}

#[test]
fn compact_targets_and_difficulty_match_erlang_including_noncanonical_values() {
    let f = fixture();
    let rows = f["targets"].as_array().unwrap();
    assert_eq!(rows.len(), 1087);
    for row in rows {
        let target = Target(row["compact"].as_u64().unwrap().try_into().unwrap());
        assert_eq!(
            target.value(),
            Unsigned::from_decimal(row["value"].as_str().unwrap()).unwrap()
        );
        assert_eq!(
            Target::from_value(&target.value()).unwrap().0,
            u32::try_from(row["normalized"].as_u64().unwrap()).unwrap()
        );
        if row["difficulty"].is_null() {
            assert_eq!(target.difficulty(), Err(TargetError::ZeroTarget));
        } else {
            assert_eq!(
                target.difficulty().unwrap(),
                Unsigned::from_decimal(row["difficulty"].as_str().unwrap()).unwrap()
            );
        }
    }
}
#[test]
fn digishield_windows_match_erlang_with_clamping_and_negative_solve_times() {
    let f = fixture();
    let rows = f["retarget"].as_array().unwrap();
    assert_eq!(rows.len(), 204);
    for row in rows {
        let mut samples: Vec<_> = row["samples"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(i, s)| WorkSample {
                height: i as u64,
                timestamp: s[1].as_u64().unwrap(),
                target: Target(s[0].as_u64().unwrap().try_into().unwrap()),
            })
            .collect();
        samples.reverse(); // Public API orders full header samples by height.
        let result = retarget(&samples);
        if row["result"].is_null() {
            assert!(matches!(
                result,
                Err(TargetError::ZeroTarget | TargetError::ZeroCapacity)
            ));
        } else if let Ok(expected) = row["result"].as_str().unwrap().parse::<u32>() {
            assert_eq!(result, Ok(Target(expected)));
        } else {
            assert_eq!(result, Err(TargetError::OutsideHeaderDomain));
        }
    }
    assert_eq!(retarget(&[]), Err(TargetError::InvalidWindow));
}
#[test]
fn specialized_siphash_and_proofs_match_erlang() {
    let f = fixture();
    assert_eq!(f["siphash"].as_array().unwrap().len(), 209);
    for row in f["siphash"].as_array().unwrap() {
        let keys = std::array::from_fn(|i| row["keys"][i].as_u64().unwrap());
        assert_eq!(
            cuckoo::siphash(keys, row["nonce"].as_u64().unwrap()),
            row["result"].as_u64().unwrap()
        );
    }
    let mut accepted = 0;
    for row in f["proofs"].as_array().unwrap() {
        let data = hex::decode(row["data"].as_str().unwrap()).unwrap();
        let solution: Vec<_> = row["solution"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect();
        let result = cuckoo::verify(
            &data,
            row["nonce"].as_u64().unwrap(),
            &solution,
            Target(row["target"].as_u64().unwrap().try_into().unwrap()),
            row["edge_bits"].as_u64().unwrap().try_into().unwrap(),
        );
        assert_eq!(result, row["accepted"].as_bool().unwrap());
        accepted += usize::from(result);
    }
    assert_eq!(f["proofs"].as_array().unwrap().len(), 517);
    assert_eq!(accepted, 11);
}
