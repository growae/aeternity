use ae_codec::headers::{CommonHeader, Header, HeaderMode, KeyHeader, MicroHeader};
use ae_consensus::headers::{self, Checkpoints, Context, Error};
use ae_crypto::Ed25519SecretKey;
use ae_protocol::NetworkProfile;
use ae_types::Hash256;

fn decode(row: &serde_json::Value) -> Header {
    Header::decode(
        &hex::decode(row["hex"].as_str().unwrap()).unwrap(),
        HeaderMode::ProofOfWork,
    )
    .unwrap()
}
fn fixture() -> (Vec<KeyHeader>, Header, Header) {
    let data: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/mainnet_context.json")).unwrap();
    let keys = data["keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| match decode(row) {
            Header::Key(k) => k,
            _ => panic!("key header"),
        })
        .collect();
    (
        keys,
        decode(&data["previous"]),
        decode(&data["micro_previous"]),
    )
}
#[test]
fn saved_mainnet_header_uses_the_explicit_interval_and_preserves_its_actual_proof() {
    use ae_consensus::target::{MineRate, Target, TargetError};
    let (keys, previous, _) = fixture();
    let ctx = Context {
        previous: &previous,
        previous_key: &keys[17],
        recent_keys: &keys[..18],
    };
    let candidate = Header::Key(keys[18].clone());
    let native: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/erlang_retarget_rate.json")).unwrap();
    let mut checked = 0;
    for row in native["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["input"]["case"] == "mainnet_ceres" && r["input"]["order"] == "ascending")
    {
        let rate = row["input"]["rate"]
            .as_number()
            .and_then(|n| n.as_str().parse().ok())
            .map_or(MineRate::NonInteger, MineRate::Integer);
        let actual = headers::validate_header_with_rate(
            &candidate,
            &NetworkProfile::mainnet(),
            keys[18].time,
            &ctx,
            &Checkpoints::new(),
            &rate,
        );
        let expected = if let Some(target) = row["recalculate"]["ok"].as_u64() {
            if target == u64::from(keys[18].target) {
                Ok(())
            } else {
                Err(Error::WrongTarget {
                    actual: Target(keys[18].target),
                    expected: Target(target.try_into().unwrap()),
                })
            }
        } else {
            Err(Error::Target(TargetError::InvalidRate))
        };
        assert_eq!(actual, expected, "{}", row["input"]["rate"]);
        checked += 1;
    }
    assert_eq!(checked, 14);
}
#[test]
fn mainnet_ceres_boundary_has_matching_context_work_and_micro_signatures() {
    let (keys, previous, micro_previous) = fixture();
    let ctx = Context {
        previous: &previous,
        previous_key: &keys[17],
        recent_keys: &keys[..18],
    };
    let candidate = Header::Key(keys[18].clone());
    let now = keys[18].time;
    assert!(
        headers::validate_header(
            &candidate,
            &NetworkProfile::mainnet(),
            now,
            &ctx,
            &Checkpoints::new()
        )
        .is_ok()
    );
    let micro_ctx = Context {
        previous: &micro_previous,
        previous_key: &keys[17],
        recent_keys: &[],
    };
    assert!(matches!(&previous, Header::Micro(_)));
    assert!(
        headers::validate_header(
            &previous,
            &NetworkProfile::mainnet(),
            now,
            &micro_ctx,
            &Checkpoints::new()
        )
        .is_ok()
    );
    let Header::Micro(mut bad) = previous.clone() else {
        unreachable!()
    };
    bad.signature[0] ^= 1;
    assert_eq!(
        headers::validate_context(&Header::Micro(bad), &micro_ctx, &Checkpoints::new()),
        Err(Error::MicroSignature)
    );
}
#[test]
fn invalid_parents_history_target_and_checkpoints_are_distinct() {
    let (keys, previous, _) = fixture();
    let ctx = Context {
        previous: &previous,
        previous_key: &keys[17],
        recent_keys: &keys[..18],
    };
    let candidate = Header::Key(keys[18].clone());
    let mut checkpoints = Checkpoints::new();
    checkpoints.insert(941700, candidate.hash().unwrap());
    assert!(headers::validate_context(&candidate, &ctx, &checkpoints).is_ok());
    checkpoints.insert(941700, Hash256::ZERO);
    assert_eq!(
        headers::validate_context(&candidate, &ctx, &checkpoints),
        Err(Error::Checkpoint)
    );
    let missing = Context {
        recent_keys: &keys[1..18],
        ..ctx
    };
    assert_eq!(
        headers::validate_context(&candidate, &missing, &Checkpoints::new()),
        Err(Error::MissingHistory {
            required: 18,
            actual: 17
        })
    );
    let mut bad_history = keys[..18].to_vec();
    bad_history.swap(0, 1);
    let bad_ctx = Context {
        recent_keys: &bad_history,
        ..ctx
    };
    assert_eq!(
        headers::validate_context(&candidate, &bad_ctx, &Checkpoints::new()),
        Err(Error::InvalidHistory)
    );
    for field in 0..5 {
        let mut bad = keys[18].clone();
        let expected = match field {
            0 => {
                bad.common.previous_hash = [0; 32];
                Error::PreviousHash
            }
            1 => {
                bad.common.previous_key_hash = [0; 32];
                Error::PreviousKeyHash
            }
            2 => {
                bad.common.height += 1;
                Error::Height
            }
            3 => {
                bad.time = 0;
                Error::KeyFromPast
            }
            _ => {
                bad.target ^= 1;
                Error::WrongTarget {
                    actual: ae_consensus::target::Target(bad.target),
                    expected: ae_consensus::target::Target(keys[18].target),
                }
            }
        };
        assert_eq!(
            headers::validate_context(&Header::Key(bad), &ctx, &Checkpoints::new()),
            Err(expected)
        );
    }
}
fn synthetic() -> (Ed25519SecretKey, KeyHeader, MicroHeader) {
    let key = Ed25519SecretKey::from_seed(&[17; 32]);
    let k = KeyHeader {
        common: CommonHeader {
            version: 1,
            height: 1,
            previous_hash: [0; 32],
            previous_key_hash: [0; 32],
            state_hash: [0; 32],
        },
        miner: key.public_key().0,
        beneficiary: [0; 32],
        target: 0x1f1f1f1f,
        seal: [0; 42],
        nonce: 0,
        time: 100,
        info: None,
        hole: false,
        end_of_epoch: false,
    };
    let hash = Header::Key(k.clone()).hash().unwrap().0;
    let micro = MicroHeader {
        common: CommonHeader {
            version: 1,
            height: 1,
            previous_hash: hash,
            previous_key_hash: hash,
            state_hash: [0; 32],
        },
        transactions_hash: [0; 32],
        time: 101,
        proof_of_fraud: None,
        signature: [0; 64],
    };
    (key, k, micro)
}
fn signed(mut m: MicroHeader, key: &Ed25519SecretKey) -> Header {
    m.signature = key
        .sign(&Header::Micro(m.clone()).signature_bytes().unwrap())
        .0;
    Header::Micro(m)
}
#[test]
fn first_micro_strict_time_subsequent_interval_and_future_boundary() {
    let (key, k, mut m) = synthetic();
    let previous = Header::Key(k.clone());
    let ctx = Context {
        previous: &previous,
        previous_key: &k,
        recent_keys: &[],
    };
    assert!(
        headers::validate_header(
            &signed(m.clone(), &key),
            &NetworkProfile::mainnet(),
            100,
            &ctx,
            &Checkpoints::new()
        )
        .is_ok()
    );
    m.time = 100;
    assert_eq!(
        headers::validate_context(&signed(m.clone(), &key), &ctx, &Checkpoints::new()),
        Err(Error::MicroInterval)
    );
    m.time = 101;
    let first = signed(m.clone(), &key);
    let ctx = Context {
        previous: &first,
        ..ctx
    };
    m.common.previous_hash = first.hash().unwrap().0;
    m.time = 3100;
    assert_eq!(
        headers::validate_context(&signed(m.clone(), &key), &ctx, &Checkpoints::new()),
        Err(Error::MicroInterval)
    );
    m.time = 3101;
    assert!(headers::validate_context(&signed(m.clone(), &key), &ctx, &Checkpoints::new()).is_ok());
    m.time = 540100;
    assert_eq!(
        headers::validate_standalone(&signed(m.clone(), &key), &NetworkProfile::mainnet(), 100),
        Err(Error::FutureTimestamp)
    );
    m.time -= 1;
    assert!(
        headers::validate_standalone(&signed(m, &key), &NetworkProfile::mainnet(), 100).is_ok()
    );
}
