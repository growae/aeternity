use ae_consensus::{
    cuckoo::{
        self,
        solver::{self, Error, Input, Limits, Nonces, Phase},
    },
    target::{HIGHEST_TARGET, Target},
};
use ae_crypto::blake2b_256;
use serde_json::Value;

fn native() -> Value {
    serde_json::from_str(include_str!("fixtures/erlang_pow_solver.json")).unwrap()
}
fn input(row: &Value, target: Target) -> Input {
    let data = hex::decode(row["data"].as_str().unwrap()).unwrap();
    let hash = blake2b_256(&data);
    assert_eq!(hex::encode(hash.0), row["hash"].as_str().unwrap());
    Input {
        hash,
        nonce: row["nonce"].as_u64().unwrap(),
        edge_bits: row["edge_bits"].as_u64().unwrap() as u8,
        target,
    }
}
fn winning() -> Input {
    Input {
        hash: blake2b_256(b"wsffgujnjkqhduihsahswgdf"),
        nonce: 38,
        edge_bits: 15,
        target: HIGHEST_TARGET,
    }
}
#[test]
fn actual_small_graph_solving_covers_native_search_and_matches_original_verification() {
    let fixture = native();
    let verified: Value =
        serde_json::from_str(include_str!("fixtures/erlang_pow_solver_solutions.json")).unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 594);
    let mut found = 0;
    for row in rows {
        let task = input(row, HIGHEST_TARGET);
        let report = solver::solve(task, Limits::default(), |_| false).unwrap();
        let proofs = row["solutions"].as_array().unwrap();
        let expected = verified.as_array().unwrap().iter().find(|p| {
            p["data"] == row["data"]
                && p["nonce"] == row["nonce"]
                && p["edge_bits"] == row["edge_bits"]
        });
        assert_eq!(report.solution.is_some(), expected.is_some(), "{row}");
        assert!(proofs.is_empty() || report.solution.is_some());
        if let Some(proof) = report.solution {
            found += 1;
            let expected = expected.unwrap();
            assert_eq!(expected["proof"], serde_json::json!(proof.as_slice()));
            assert_eq!(expected["accepted"], true);
            assert_eq!(expected["cycle"], true);
            assert!(proofs.is_empty() || proofs.iter().any(|p| p["edges"] == expected["proof"]));
            assert!(cuckoo::verify_proof_from_hash(
                task.hash,
                task.nonce,
                &proof,
                task.edge_bits
            ));
            for (name, target) in [("half", Target(0x207fffff)), ("low", Target(0x01010000))] {
                let report =
                    solver::solve(Input { target, ..task }, Limits::default(), |_| false).unwrap();
                assert_eq!(report.solution.is_some(), expected[name] == true, "{row}");
            }
        }
        assert_eq!(report.progress.phase, Phase::Complete);
        assert!(report.progress.peak_buffer_bytes <= Limits::default().memory_bytes);
    }
    assert_eq!(found, 13);
    assert_eq!(
        rows.iter()
            .filter(|r| !r["solutions"].as_array().unwrap().is_empty())
            .count(),
        5
    );
}
#[test]
fn native_nonce_reservation_preserves_strict_boundary_and_large_integer_behavior() {
    let f = native();
    let rows = f["nonces"].as_array().unwrap();
    assert_eq!(rows.len(), 35);
    for row in rows {
        let requested = row["requested"].as_str().unwrap().parse().unwrap();
        let repeats = row["repeats"].as_u64().unwrap();
        let range = Nonces::reserve(requested, repeats).unwrap();
        assert_eq!(range.start(), row["start"].as_u64().unwrap());
        assert_eq!(range.next().to_string(), row["next"].as_str().unwrap());
        assert_eq!(range.repeats(), repeats);
        assert_eq!(range.at(0), Some(range.start()));
        assert_eq!(range.at(repeats - 1), Some(range.next() - 1));
        assert_eq!(range.at(repeats), None);
    }
    assert_eq!(Nonces::reserve(0, 0), Err(Error::Configuration));
    assert_eq!(Nonces::reserve(u128::MAX, 1).unwrap().start(), 0);
}
#[test]
fn cancellation_in_every_search_phase_and_work_limits_allow_exact_retry() {
    let task = winning();
    let limits = Limits::default();
    let mut observed = Vec::new();
    let expected = solver::solve(task, limits, |p| {
        observed.push(p);
        false
    })
    .unwrap();
    assert!(expected.solution.is_some());
    for phase in [
        Phase::Allocate,
        Phase::Trim,
        Phase::Build,
        Phase::Sort,
        Phase::Search,
        Phase::Complete,
    ] {
        let result = solver::solve(task, limits, |p| p.phase == phase).unwrap_err();
        assert_eq!(result.error, Error::Cancelled);
        assert_eq!(result.progress.phase, phase);
    }
    assert!(observed.windows(2).all(|w| w[1].work - w[0].work <= 1024));
    for phase in [Phase::Trim, Phase::Sort, Phase::Search] {
        let first = observed.iter().find(|p| p.phase == phase).unwrap().work;
        let later = observed
            .iter()
            .find(|p| p.phase == phase && p.work > first)
            .unwrap()
            .work;
        let failed =
            solver::solve(task, limits, |p| p.phase == phase && p.work >= later).unwrap_err();
        assert_eq!(failed.error, Error::Cancelled);
        assert_eq!(failed.progress.phase, phase);
        assert_eq!(failed.progress.work, later);
    }
    let mut tested = 0;
    for p in observed.iter().filter(|p| p.work > 0).step_by(19) {
        let fail = solver::solve(
            task,
            Limits {
                work: p.work - 1,
                ..limits
            },
            |_| false,
        )
        .unwrap_err();
        assert_eq!(fail.error, Error::WorkLimit);
        assert!(fail.progress.work < p.work);
        tested += 1;
    }
    assert!(tested > 5);
    assert_eq!(solver::solve(task, limits, |_| false).unwrap(), expected);
}
#[test]
fn memory_and_graph_guards_are_operational_and_trimming_preserves_real_proofs() {
    let task = winning();
    let limits = Limits::default();
    let expected = solver::solve(task, limits, |_| false).unwrap().solution;
    for trim_rounds in [0, 1, 2, 14, 64] {
        let solved = solver::solve(
            task,
            Limits {
                trim_rounds,
                ..limits
            },
            |_| false,
        )
        .unwrap();
        assert_eq!(solved.solution, expected);
    }
    for (trim_rounds, memory_bytes, phase) in
        [(32, 1024, Phase::Allocate), (0, 13_312, Phase::Build)]
    {
        let failed = solver::solve(
            task,
            Limits {
                trim_rounds,
                memory_bytes,
                ..limits
            },
            |_| false,
        )
        .unwrap_err();
        assert!(matches!(failed.error, Error::Memory { .. }));
        assert_eq!(failed.progress.phase, phase);
    }
    for bits in [0, 64, 255] {
        assert_eq!(
            solver::solve(
                Input {
                    edge_bits: bits,
                    ..task
                },
                limits,
                |_| false
            )
            .unwrap_err()
            .error,
            Error::Configuration
        );
    }
    for bits in [29, 31, 63] {
        let error = solver::solve(
            Input {
                edge_bits: bits,
                ..task
            },
            Limits {
                memory_bytes: 1024,
                ..limits
            },
            |_| false,
        )
        .unwrap_err();
        assert!(matches!(error.error, Error::Memory { .. }));
        assert_eq!(error.progress.work, 0);
    }
    for bits in 1..6 {
        assert_eq!(
            solver::solve(
                Input {
                    edge_bits: bits,
                    ..task
                },
                limits,
                |_| false
            )
            .unwrap()
            .solution,
            None
        );
    }
    let proof = expected.unwrap();
    assert!(!cuckoo::verify_proof_from_hash(
        task.hash,
        task.nonce,
        &proof,
        cuckoo::MAINNET_EDGE_BITS
    ));
    assert_eq!(
        solver::solve(task, limits, |_| false).unwrap().solution,
        expected
    );
}
