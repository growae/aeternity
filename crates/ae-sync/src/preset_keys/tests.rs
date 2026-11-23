use super::*;
use ae_codec::headers::CommonHeader;
use ae_crypto::{Ed25519SecretKey, verify_ed25519};
use serde_json::{Value, json};
use std::sync::mpsc;

fn secret(n: u8) -> Ed25519SecretKey {
    let mut seed = [0; 32];
    seed[31] = n;
    Ed25519SecretKey::from_seed(&seed)
}
fn key(n: u8) -> AccountPublicKey {
    secret(n).public_key()
}
fn keys() -> Keys {
    Keys::new([secret(1), secret(2)])
}
fn result<T: Clone>(ticket: Result<Ticket<T>, Error>) -> Result<T, Error> {
    ticket
        .unwrap()
        .wait_timeout(Duration::from_secs(5))
        .expect("worker reply")
}
fn wait_for_release(c: &Client) {
    // Reply publication precedes the final drop of the executing job. Both
    // caller and worker ownership must end before its credit is available.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while c.info().retained_requests != 0 {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
}
fn worker() -> (Worker, Client) {
    let worker = Worker::spawn(keys(), Config::default()).unwrap();
    let client = worker.client();
    (worker, client)
}
fn atom(s: &str) -> Value {
    json!({"atom":s})
}
fn native_error(e: Error) -> Value {
    json!({"tuple":[atom("error"),atom(match e {
        Error::KeyNotFound => "key_not_found",
        Error::NoActive => "no_active",
        Error::NoCandidate => "no_candidate",
        Error::Signing(signing::Error::MissingKey) => "not_found",
        e => panic!("unexpected native result {e:?}"),
    })]})
}
fn native<T>(r: Result<T, Error>, encode: impl FnOnce(T) -> Value) -> Value {
    match r {
        Ok(v) => encode(v),
        Err(e) => native_error(e),
    }
}
fn pubkey(r: Result<AccountPublicKey, Error>) -> Value {
    native(
        r,
        |k| json!({"tuple":[atom("ok"),{"bytes":hex::encode(k.0)}]}),
    )
}

#[test]
fn original_preset_key_selection_and_signatures() {
    let cases: Value =
        serde_json::from_str(include_str!("../../tests/fixtures/erlang_preset_keys.json")).unwrap();
    let mut count = 0;
    for case in cases.as_array().unwrap() {
        let keys = Keys::new(
            case["owned_seeds"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| secret(n.as_u64().unwrap() as u8)),
        );
        let worker = Worker::spawn(keys, Config::default()).unwrap();
        let c = worker.client();
        for step in case["steps"].as_array().unwrap() {
            let name = step["name"].as_str().unwrap();
            let actual = match name {
                "ready" => native(result(c.is_ready()), |b| {
                    atom(if b { "true" } else { "false" })
                }),
                "known" | "missing" => native(result(c.is_key_present(key(1))), |b| {
                    atom(if b { "true" } else { "false" })
                }),
                "random_empty" | "random_single" => pubkey(result(c.set_random_candidate())),
                "unknown_promote" | "promote_single" => {
                    native(result(c.promote_candidate(key(1))), |_| atom("ok"))
                }
                "promote_other" => native(result(c.promote_candidate(key(2))), |_| atom("ok")),
                "unknown_promotion_clears" => {
                    native(result(c.promote_candidate(key(99))), |_| atom("ok"))
                }
                "set_first" | "new_candidate_preserves_active" => {
                    native(result(c.set_candidate(key(1))), |_| atom("ok"))
                }
                "unknown_set_preserves" => native(result(c.set_candidate(key(99))), |_| atom("ok")),
                "missing_sign"
                | "explicit_without_active"
                | "explicit_not_active"
                | "unknown_sign" => {
                    let n = match name {
                        "explicit_without_active" => 2,
                        "unknown_sign" => 99,
                        _ => 1,
                    };
                    native(
                        result(c.sign_binary(&[0, 1, 2], key(n))),
                        |sig| json!({"tuple":[atom("ok"),{"bytes":hex::encode(sig.0)}]}),
                    )
                }
                _ => panic!("unconsumed native step {name}"),
            };
            assert_eq!(actual, step["result"], "{name}");
            assert_eq!(
                json!({
                    "active":pubkey(result(c.active())),
                    "candidate":pubkey(result(c.candidate())),
                    "ready":result(c.is_ready()).unwrap()
                }),
                step["state"],
                "{name}"
            );
            count += 1;
        }
        assert_eq!(worker.retire().exit, Some(Exit::Shutdown));
    }
    assert_eq!(count, 16);
}

// The release sender is dropped during unwinding, so an assertion failure
// cannot leave Worker::drop waiting forever on the test-only paused job.
fn pause(c: &Client) -> (Ticket<()>, mpsc::Sender<()>) {
    let (entered, wait) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let ticket = c
        .submit(0, || {
            move |_| {
                entered.send(()).unwrap();
                let _ = blocked.recv();
                Ok(())
            }
        })
        .unwrap();
    wait.recv_timeout(Duration::from_secs(5)).unwrap();
    (ticket, release)
}

#[test]
fn timed_out_and_dropped_callers_do_not_cancel_accepted_selection() {
    let (w, c) = worker();
    let (gate, release) = pause(&c);
    let selection = c.set_candidate(key(1)).unwrap();
    assert_eq!(selection.wait_timeout(Duration::from_millis(1)), None);
    drop(selection);
    let observed = c.candidate().unwrap();
    w.request_shutdown();
    assert!(matches!(c.is_ready(), Err(Error::Stopped)));
    release.send(()).unwrap();
    assert_eq!(gate.wait_timeout(Duration::from_secs(5)), Some(Ok(())));
    let info = w.retire();
    assert_eq!(observed.try_result(), Some(Ok(key(1))));
    assert!(info.joined);
    assert_eq!(info.exit, Some(Exit::Shutdown));
    assert_eq!(info.retained_requests, 2);
    drop((gate, observed));
    assert_eq!(c.info().retained_requests, 0);
}

#[test]
fn graceful_shutdown_drains_before_join_and_replacement_resets_only_selection() {
    let mut configured = keys();
    configured.promote(key(1)).unwrap();
    configured.set_candidate(key(2)).unwrap();
    let w = Worker::spawn(configured.clone(), Config::default()).unwrap();
    let c = w.client();
    assert_eq!(result(c.active()), Err(Error::NoActive));
    assert_eq!(result(c.candidate()), Err(Error::NoCandidate));
    let (gate, release) = pause(&c);
    let promote = c.promote_candidate(key(1)).unwrap();
    let set = c.set_candidate(key(2)).unwrap();
    let before = c.snapshot().unwrap();
    w.request_shutdown();
    assert!(!w.is_finished());
    release.send(()).unwrap();
    assert_eq!(w.retire().exit, Some(Exit::Shutdown));
    assert_eq!(promote.try_result(), Some(Ok(())));
    assert_eq!(set.try_result(), Some(Ok(())));
    let before = before.try_result().unwrap().unwrap();
    assert_eq!(
        before.selection,
        Selection {
            active: Some(key(1)),
            candidate: Some(key(2))
        }
    );
    assert_eq!(gate.try_result(), Some(Ok(())));
    let replacement = Worker::spawn(configured, Config::default()).unwrap();
    let fresh = replacement.client();
    assert!(!c.same_worker(&fresh));
    assert!(c.same_worker(&c.clone()));
    assert!(matches!(c.set_candidate(key(1)), Err(Error::Stopped)));
    assert_eq!(result(fresh.active()), Err(Error::NoActive));
    assert_eq!(result(fresh.candidate()), Err(Error::NoCandidate));
    assert_eq!(
        result(fresh.commit_selection(before, Selection::default())).unwrap_err(),
        Error::StaleSelection
    );
    let sig = result(fresh.sign_binary(&[0, 1, 2], key(1))).unwrap();
    assert_eq!(
        hex::encode(sig.0),
        "b37e0ff9f884985e47566f6a74da6da668ca6a891cf76e851b5acecd7eb2ef55034e365063aa041e833e7fd7894013020294119cd1532d410f0f12edae82e001"
    );
}

#[test]
fn stale_staging_and_invalid_selection_cannot_overwrite_promotion() {
    let (_w, c) = worker();
    let old = result(c.snapshot()).unwrap();
    result(c.promote_candidate(key(1))).unwrap();
    let next = result(c.snapshot()).unwrap();
    assert_eq!(next.revision(), old.revision() + 1);
    assert_eq!(
        result(c.commit_selection(old, Selection::default())).unwrap_err(),
        Error::StaleSelection
    );
    assert_eq!(
        result(c.commit_selection(
            next.clone(),
            Selection {
                active: Some(key(2)),
                candidate: Some(key(99))
            }
        ))
        .unwrap_err(),
        Error::KeyNotFound
    );
    assert_eq!(result(c.snapshot()).unwrap().selection, next.selection);
    assert_eq!(result(c.snapshot()).unwrap().revision(), next.revision());
    let new = result(c.commit_selection(
        next,
        Selection {
            active: Some(key(1)),
            candidate: Some(key(2)),
        },
    ))
    .unwrap();
    assert_eq!(new.revision(), 2);
    result(c.promote_candidate(key(99))).unwrap_err();
    assert_eq!(result(c.snapshot()).unwrap().revision(), 3);
    assert_eq!(
        result(c.snapshot()).unwrap().selection,
        Selection::default()
    );
}

#[test]
fn revision_exhaustion_preserves_state() {
    let (_w, c) = worker();
    result(c.submit(0, || {
        |s| {
            s.revision = u64::MAX;
            Ok(())
        }
    }))
    .unwrap();
    assert_eq!(result(c.set_candidate(key(1))), Err(Error::ResourceLimit));
    assert_eq!(
        result(c.snapshot()).unwrap().selection,
        Selection::default()
    );
    // A no-op needs no revision and remains legal.
    assert_eq!(
        result(c.promote_candidate(key(99))),
        Err(Error::KeyNotFound)
    );
}

#[test]
fn panic_distinguishes_unknown_executed_work_from_unexecuted_queue() {
    let (w, c) = worker();
    let completed = c.sign_binary(b"already replied", key(1)).unwrap();
    let known = completed.wait_timeout(Duration::from_secs(5)).unwrap();
    let (entered, wait) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let unknown: Ticket<()> = c
        .submit(0, || {
            move |s| {
                s.select(Selection {
                    active: Some(key(1)),
                    candidate: None,
                })?;
                entered.send(s.snapshot()).unwrap();
                let _ = blocked.recv();
                panic!("preset-key panic after selection commit before reply")
            }
        })
        .unwrap();
    let committed = wait.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(committed.selection.active, Some(key(1)));
    let queued = c.set_candidate(key(2)).unwrap();
    release.send(()).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !w.is_finished() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(c.info().exit, Some(Exit::Panicked));
    assert!(!c.info().joined);
    let retired = w.retire();
    assert_eq!(retired.exit, Some(Exit::Panicked));
    assert!(retired.joined);
    for _ in 0..3 {
        assert_eq!(unknown.try_result(), Some(Err(Error::Disconnected)));
        assert_eq!(queued.try_result(), Some(Err(Error::Stopped)));
        assert_eq!(completed.try_result(), Some(known.clone()));
    }
    let (_replacement, fresh) = worker();
    assert_eq!(
        result(fresh.snapshot()).unwrap().selection,
        Selection::default()
    );
    assert_eq!(
        result(fresh.commit_selection(committed, Selection::default())).unwrap_err(),
        Error::StaleSelection
    );
    assert!(matches!(c.is_ready(), Err(Error::Stopped)));
    drop((unknown, queued, completed));
    assert_eq!(c.info().retained_bytes, 0);
}

#[test]
fn completed_ticket_clones_retain_slot_and_byte_quota() {
    let w = Worker::spawn(
        keys(),
        Config {
            requests: 1,
            ..Config::default()
        },
    )
    .unwrap();
    let c = w.client();
    let ticket = c.sign_binary(b"retained", key(1)).unwrap();
    assert!(ticket.wait_timeout(Duration::from_secs(5)).unwrap().is_ok());
    let clone = ticket.clone();
    let bytes = c.info().retained_bytes;
    assert!(bytes >= b"retained".len());
    assert!(matches!(c.is_ready(), Err(Error::Busy)));
    drop(ticket);
    assert!(matches!(c.is_ready(), Err(Error::Busy)));
    assert_eq!(c.info().retained_bytes, bytes);
    drop(clone);
    wait_for_release(&c);
    assert_eq!(c.info().retained_requests, 0);
    assert_eq!(c.info().retained_bytes, 0);
    assert!(result(c.is_ready()).unwrap());
}

#[test]
fn byte_quota_limits_payload_before_acceptance() {
    let (_w, probe) = worker();
    let overflow: Result<Ticket<()>, Error> = probe.submit(usize::MAX, || {
        |_| panic!("overflowing request must not be accepted")
    });
    assert!(matches!(overflow, Err(Error::ResourceLimit)));
    assert_eq!(probe.info().retained_requests, 0);
    let sample = probe.sign_binary(&[], key(1)).unwrap();
    sample
        .wait_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let overhead = probe.info().retained_bytes;
    let w = Worker::spawn(
        keys(),
        Config {
            requests: 10,
            retained_bytes: overhead + 8,
            message_bytes: 8,
            keys: 2,
        },
    )
    .unwrap();
    let c = w.client();
    assert!(matches!(
        c.sign_binary(&[0; 9], key(1)),
        Err(Error::ResourceLimit)
    ));
    let first = c.sign_binary(&[0; 8], key(1)).unwrap();
    first.wait_timeout(Duration::from_secs(5)).unwrap().unwrap();
    assert_eq!(c.info().retained_bytes, overhead + 8);
    assert!(matches!(c.sign_binary(&[], key(1)), Err(Error::Busy)));
    drop(first);
    wait_for_release(&c);
    assert!(result(c.sign_binary(&[0; 8], key(1))).is_ok());
    let tiny = Worker::spawn(
        keys(),
        Config {
            retained_bytes: 1,
            ..Config::default()
        },
    )
    .unwrap();
    assert!(matches!(
        tiny.client().is_ready(),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(tiny.client().info().retained_requests, 0);
}

#[test]
fn concurrent_acceptance_obeys_one_shared_limit_and_drains_exact_receipts() {
    let w = Worker::spawn(
        keys(),
        Config {
            requests: 16,
            ..Config::default()
        },
    )
    .unwrap();
    let c = w.client();
    let (gate, release) = pause(&c);
    let callers: Vec<_> = (0..8u8)
        .map(|id| {
            let c = c.clone();
            std::thread::spawn(move || {
                let mut accepted = Vec::new();
                for _ in 0..32 {
                    match c.sign_binary(&[id; 256], key(1)) {
                        Ok(ticket) => accepted.push((id, ticket)),
                        Err(Error::Busy) => (),
                        _ => panic!("unexpected acceptance result"),
                    }
                }
                accepted
            })
        })
        .collect();
    let accepted: Vec<_> = callers
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    assert_eq!(accepted.len(), 15);
    assert_eq!(c.info().retained_requests, 16);
    w.request_shutdown();
    release.send(()).unwrap();
    assert_eq!(w.retire().exit, Some(Exit::Shutdown));
    for (id, ticket) in &accepted {
        let sig = ticket.try_result().unwrap().unwrap();
        assert!(verify_ed25519(&key(1), &[*id; 256], &sig));
    }
    drop((gate, accepted));
    assert_eq!(c.info().retained_requests, 0);
    assert_eq!(c.info().retained_bytes, 0);
}

#[test]
fn configuration_limits_and_empty_readiness() {
    for config in [
        Config {
            requests: 0,
            ..Config::default()
        },
        Config {
            retained_bytes: 0,
            ..Config::default()
        },
        Config {
            message_bytes: 0,
            ..Config::default()
        },
    ] {
        assert!(matches!(
            Worker::spawn(keys(), config),
            Err(Error::InvalidSettings)
        ));
    }
    assert!(matches!(
        Worker::spawn(
            keys(),
            Config {
                keys: 1,
                ..Config::default()
            }
        ),
        Err(Error::ResourceLimit)
    ));
    let w = Worker::spawn(
        Keys::default(),
        Config {
            keys: 0,
            ..Config::default()
        },
    )
    .unwrap();
    assert!(result(w.client().is_ready()).unwrap());
}

#[test]
fn header_signing_uses_canonical_bytes_without_changing_candidate() {
    let (_w, c) = worker();
    let common = CommonHeader {
        version: 6,
        height: 42,
        previous_hash: [1; 32],
        previous_key_hash: [2; 32],
        state_hash: [3; 32],
    };
    let micro = MicroHeader {
        common: common.clone(),
        transactions_hash: [4; 32],
        time: 123,
        proof_of_fraud: None,
        signature: [0; 64],
    };
    assert_eq!(
        result(c.sign_micro(micro.clone(), None)),
        Err(Error::NoActive)
    );
    result(c.promote_candidate(key(1))).unwrap();
    result(c.set_candidate(key(2))).unwrap();
    let before = result(c.snapshot()).unwrap();
    let active = result(c.sign_micro(micro.clone(), None)).unwrap();
    signing::verify_micro(&active, key(1)).unwrap();
    let explicit = result(c.sign_micro(micro, Some(key(2)))).unwrap();
    signing::verify_micro(&explicit, key(2)).unwrap();
    let header = KeyHeader {
        common,
        miner: key(1).0,
        beneficiary: key(2).0,
        target: 1,
        seal: [0; 42],
        nonce: 0,
        time: 123,
        info: Some([0; 4]),
        hole: false,
        end_of_epoch: false,
    };
    let sig = result(c.key_header_signature(header.clone(), key(1))).unwrap();
    let bytes = Header::Key(header).signature_bytes().unwrap();
    assert!(verify_ed25519(&key(1), &bytes, &sig));
    let after = result(c.snapshot()).unwrap();
    assert_eq!(after.selection, before.selection);
    assert_eq!(after.revision(), before.revision());
}
