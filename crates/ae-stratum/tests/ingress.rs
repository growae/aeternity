use ae_stratum::{
    ingress::{Id, Input, Solution},
    nonce::Nonce,
    share::Effect,
};
use ae_types::Hash256;
use num_bigint::BigInt;
#[test]
fn accepted_effect_codec_is_lossless_bounded_and_rejects_noncanonical_or_reordered_input() {
    for n in [BigInt::from(0), 1.into(), BigInt::from(1) << 16383] {
        for solution in [false, true] {
            let input = Input {
                account: "ak_é漢字".into(),
                target: n.clone(),
                hash: Hash256([5; 32]),
                solution: solution.then(|| Solution {
                    nonce: Nonce(u64::MAX),
                    proof: Box::new(std::array::from_fn(|n| u64::MAX - n as u64)),
                }),
            };
            let raw = input.encode().unwrap();
            assert_eq!(Input::decode(&raw).unwrap(), input);
            let mut extra = raw.clone();
            extra.push(0);
            assert!(Input::decode(&extra).is_err());
            let mut effects = vec![Effect::SubmitShare {
                account: input.account.clone(),
                target: input.target.clone(),
                hash: input.hash,
            }];
            if let Some(s) = input.solution.clone() {
                effects.push(Effect::SubmitSolution {
                    hash: input.hash,
                    nonce: s.nonce,
                    proof: s.proof,
                });
            }
            assert_eq!(Input::from_effects(&effects).unwrap(), input);
            effects.push(effects[0].clone());
            assert!(Input::from_effects(&effects).is_err());
        }
    }
    let negative = Input {
        account: "ak_worker".into(),
        target: (-1).into(),
        hash: Hash256::ZERO,
        solution: None,
    };
    assert!(negative.validate().is_err());
    assert!(negative.encode().is_err());
    assert!(
        Input::from_effects(&[Effect::SubmitShare {
            account: negative.account,
            target: negative.target,
            hash: negative.hash
        }])
        .is_err()
    );
    assert!(Input::from_effects(&[]).is_err());
    assert!(Input::decode(&vec![0; 8193]).is_err());
    let bad = Effect::SubmitSolution {
        hash: Hash256::ZERO,
        nonce: Nonce(0),
        proof: Box::new([0; 42]),
    };
    assert!(Input::from_effects(&[bad]).is_err());
    assert!(
        Id {
            epoch: 0,
            sequence: 1
        }
        .validate()
        .is_err()
    );
    assert!(
        Id {
            epoch: 1,
            sequence: 0
        }
        .validate()
        .is_err()
    );
    assert!(
        Id {
            epoch: u64::MAX,
            sequence: u64::MAX
        }
        .validate()
        .is_ok()
    );
    let mut input = Input {
        account: "x".repeat(1025),
        target: 0.into(),
        hash: Hash256::ZERO,
        solution: None,
    };
    assert!(input.encode().is_err());
    input.account = "ak_worker".into();
    input.target = BigInt::from(1) << 16384;
    assert!(input.encode().is_err());
}
