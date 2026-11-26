use super::*;
use ae_chain::key::candidate;
use ae_storage::pow::key::Error as Storage;

#[test]
fn constructor_errors_separate_native_not_found_from_capacity_and_exceptions() {
    for error in [
        key::Error::BlockNotFound(Hash256([1; 32])),
        key::Error::AdjustmentHistory(repository::Error::Missing(Hash256([2; 32]))),
        key::Error::AdjustmentHistory(repository::Error::MissingCanonicalKey(19)),
    ] {
        assert_eq!(
            construction_error(preparation::Error::Preparation(Storage::Candidate(error))),
            Error::NotFound
        );
    }
    assert_eq!(
        construction_error(preparation::Error::Finish(key::Error::Candidate(
            candidate::Error::Adjustment {
                expected: 18,
                actual: 17
            }
        ))),
        Error::NotFound
    );
    for error in [
        preparation::Error::Busy,
        preparation::Error::Preparation(Storage::Snapshot(snapshots::Error::ResourceLimit)),
        preparation::Error::Preparation(Storage::Candidate(key::Error::AdjustmentHistory(
            repository::Error::ResourceLimit,
        ))),
    ] {
        assert_eq!(construction_error(error), Error::Busy);
    }
    for error in [
        preparation::Error::Time,
        preparation::Error::Finish(key::Error::Candidate(candidate::Error::Target(
            ae_consensus::target::TargetError::InvalidRate,
        ))),
        preparation::Error::Preparation(Storage::Candidate(key::Error::AdjustmentHistory(
            repository::Error::Backend("x".repeat(8192)),
        ))),
    ] {
        let Error::Preparation(text) = construction_error(error) else {
            panic!("operational failure lost")
        };
        assert!(!text.is_empty() && text.len() <= 4096);
    }
    assert_eq!(
        construction_error(preparation::Error::Panicked),
        Error::Panicked
    );
}
