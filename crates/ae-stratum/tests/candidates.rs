use ae_codec::headers::{Header, HeaderMode};
use ae_stratum::{
    candidate::{Active, Candidate, Error},
    ingress::{Input, Solution},
    nonce::Nonce,
};
use ae_types::Hash256;
use serde_json::Value;
use std::sync::Arc;

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/erlang_stratum_candidates.json")).unwrap()
}
fn bytes(row: &Value, name: &str) -> Vec<u8> {
    hex::decode(row[name].as_str().unwrap()).unwrap()
}
fn solution(row: &Value) -> Solution {
    Solution {
        nonce: Nonce(row["nonce"].as_u64().unwrap()),
        proof: Box::new(std::array::from_fn(|i| row["proof"][i].as_u64().unwrap())),
    }
}
#[test]
fn original_candidate_work_sealing_and_all_saved_proofs_match() {
    let data = fixture();
    let mut valid = 0;
    for row in data["rows"].as_array().unwrap() {
        let candidate = Candidate::decode(&bytes(row, "sealing")).unwrap();
        let work = candidate.work().unwrap();
        assert_eq!(hex::encode(candidate.hash().0), row["hash"]);
        assert_eq!(work.hash(), candidate.hash());
        assert_eq!(work.hash_text(), row["hash"].as_str().unwrap());
        assert_eq!(u64::from(work.version), row["version"].as_u64().unwrap());
        assert_eq!(work.block_target.to_string(), row["target"].to_string());
        let sealed = candidate.seal(&solution(row)).unwrap();
        assert_eq!(sealed.bytes(), bytes(row, "sealed"));
        assert_eq!(hex::encode(sealed.hash().0), row["block_hash"]);
        assert_eq!(sealed.work_hash(), candidate.hash());
        assert_eq!(sealed.parent(), candidate.parent());
        let verified = ae_consensus::cuckoo::verify_key_seal(sealed.header()).unwrap();
        assert_eq!(verified, row["valid"].as_bool().unwrap(), "{}", row["id"]);
        valid += usize::from(verified);
        let input = Input {
            account: "ak_miner".into(),
            target: work.block_target,
            hash: candidate.hash(),
            solution: Some(solution(row)),
        };
        assert_eq!(candidate.solution(&input).unwrap(), sealed);
        assert_eq!(
            Candidate::new(candidate.header().clone()).unwrap(),
            candidate
        );
    }
    assert_eq!(data["rows"].as_array().unwrap().len(), 31);
    assert_eq!(valid, 12);
}

#[test]
fn original_conductor_selection_consumes_all_work_before_insertion() {
    for row in fixture()["selection"].as_array().unwrap() {
        let mut active = Active::new(4).unwrap();
        for raw in row["before"].as_array().unwrap().iter().rev() {
            let c =
                Arc::new(Candidate::decode(&hex::decode(raw.as_str().unwrap()).unwrap()).unwrap());
            assert!(active.publish(c.clone(), c.parent()).unwrap());
        }
        let claimed = active.claim(&bytes(row, "reply"));
        assert_eq!(claimed.is_some(), row["mined"] == 1, "{}", row["name"]);
        let after: Vec<_> = active
            .candidates()
            .iter()
            .map(|c| hex::encode(c.sealing_bytes()))
            .collect();
        assert_eq!(serde_json::to_value(after).unwrap(), row["after"]);
        assert!(active.claim(&bytes(row, "reply")).is_none());
    }
}

#[test]
fn bounded_active_work_retains_old_candidates_and_rejects_without_eviction() {
    let data = fixture();
    let a = Arc::new(Candidate::decode(&bytes(&data["rows"][0], "sealing")).unwrap());
    let b = Arc::new(Candidate::decode(&bytes(&data["rows"][1], "sealing")).unwrap());
    assert_ne!(a.parent(), b.parent());
    assert!(matches!(Active::new(0), Err(Error::Capacity)));
    assert!(matches!(Active::new(4097), Err(Error::Capacity)));
    let mut active = Active::new(2).unwrap();
    assert!(!active.publish(a.clone(), b.parent()).unwrap());
    assert!(active.candidates().is_empty());
    assert!(active.publish(a.clone(), a.parent()).unwrap());
    assert!(active.publish(b.clone(), b.parent()).unwrap());
    assert_eq!(active.candidates(), &[b.clone(), a.clone()]);
    assert_eq!(active.publish(a.clone(), a.parent()), Err(Error::Capacity));
    assert!(!active.publish(a.clone(), b.parent()).unwrap());
    let mut wrong = a.sealing_bytes().to_vec();
    wrong[0] ^= 1;
    assert!(active.claim(&wrong).is_none());
    assert_eq!(active.candidates(), &[b, a.clone()]);
    assert_eq!(active.claim(a.sealing_bytes()), Some(a));
    assert!(active.candidates().is_empty());
}

#[test]
fn candidate_binding_and_encoding_do_not_mistake_a_seal_for_admission() {
    let data = fixture();
    let row = &data["rows"][0];
    let candidate = Candidate::decode(&bytes(row, "sealing")).unwrap();
    assert_eq!(Candidate::decode(&bytes(row, "sealed")), Err(Error::Sealed));
    for change in 0..4 {
        let mut h = candidate.header().clone();
        match change {
            0 => h.hole = true,
            1 => h.end_of_epoch = true,
            2 => h.common.version = 0,
            _ => h.info = Some([0; 4]),
        }
        assert!(Candidate::new(h).is_err());
    }
    let mut too_long = candidate.sealing_bytes().to_vec();
    too_long.extend_from_slice(&[0; 8192]);
    assert!(Candidate::decode(&too_long).is_err());
    let mut proof = solution(row);
    proof.proof[0] = u64::from(u32::MAX) + 1;
    assert_eq!(candidate.seal(&proof), Err(Error::ProofWidth));
    *proof.proof = [0; 42];
    let sealed = candidate.seal(&proof).unwrap();
    assert!(!ae_consensus::cuckoo::verify_key_seal(sealed.header()).unwrap());
    assert_eq!(
        Header::decode(sealed.bytes(), HeaderMode::ProofOfWork).unwrap(),
        Header::Key(sealed.header().clone())
    );
    let mut input = Input {
        account: "ak_miner".into(),
        target: 0.into(),
        hash: Hash256::ZERO,
        solution: Some(solution(row)),
    };
    assert_eq!(candidate.solution(&input), Err(Error::Binding));
    input.hash = candidate.hash();
    input.solution = None;
    assert_eq!(candidate.solution(&input), Err(Error::Binding));
    input.solution = Some(solution(row));
    input.target = (-1).into();
    assert!(candidate.solution(&input).is_err());
}
