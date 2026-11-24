use super::*;
fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../../../ae-stratum/tests/fixtures/erlang_stratum_payout_lifecycle.json"
    ))
    .unwrap()
}
fn counts(db: &Database) -> serde_json::Value {
    serde_json::json!(db.stratum(Default::default()).unwrap().info().counts)
}
fn state(i: &Info) -> serde_json::Value {
    let pending = i.pending.as_ref().filter(|r| r.hash().unwrap().is_some());
    serde_json::json!({"active":false,"protocol":i.context.protocol as u32,"height":i.context.height,
        "balance":match &i.context.balance{None=>serde_json::json!({"atom":"undefined"}),Some(n)=>serde_json::json!({"integer":BigInt::from_bytes_be(num_bigint::Sign::Plus,n.as_bytes()).to_string()})},
        "pending_id":match pending{None=>serde_json::json!({"atom":"undefined"}),Some(r)=>serde_json::json!({"tuple":[{"integer":r.payment.id.0.to_string()},{"integer":r.payment.id.1.to_string()}]})},
        "pending_hash":match pending{None=>serde_json::json!({"atom":"undefined"}),Some(r)=>serde_json::json!({"binary":hex::encode(r.hash().unwrap().unwrap().0)})}})
}
#[test]
fn original_startup_restores_sent_records_and_refreshes_protocol_only_for_a_new_owner() {
    let f = fixture();
    let mut count = 0;
    for row in f["restart"].as_array().unwrap() {
        let name = row["name"].as_str().unwrap();
        let t = Temp::new();
        let db = database(&t);
        set_top(
            &db,
            280,
            Protocol::Roma,
            if name == "unknown_balance" {
                None
            } else {
                Some(1000)
            },
            0,
        );
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        let mut payments = vec![];
        let mut sent = |id: core::PaymentId, h: Option<u8>| {
            let mut p = pay(id);
            p.total = 100.into();
            p.tx_hash = h.map(|n| Hash256(key(n)));
            payments.push(Command::Payment(p));
        };
        match name {
            "sent" | "restart_protocol" => sent((100, 0), Some(44)),
            "oldest_sent" => {
                sent((101, 0), Some(45));
                sent((100, 1), Some(44));
            }
            "unpaid_before_sent" => {
                sent((99, 0), None);
                sent((100, 0), Some(44));
            }
            _ => {}
        }
        commit(&mut w, payments);
        let rounds = super::super::super::rounds::Config::from_percentages(
            [(b"fixture-pool".to_vec(), Number::Float(100.0f64.to_bits()))].into(),
            2,
            180,
            Default::default(),
        )
        .unwrap();
        w.begin_rounds(rounds, Default::default()).unwrap();
        let initial = w.begin_payouts(config(), Default::default()).unwrap();
        assert_eq!(state(&initial), row["initial"], "{name}");
        assert_eq!(counts(&db), row["counts_initial"], "{name}");
        if name == "restart_protocol" {
            set_top(&db, 281, Protocol::Ceres, Some(2000), 0);
            drop(w);
            let mut replacement = Writer::start(db.clone(), Default::default()).unwrap();
            let next = replacement
                .begin_payouts(config(), Default::default())
                .unwrap();
            assert_eq!(state(&next), row["after_event"]);
            assert_eq!(counts(&db), row["counts_after"]);
        } else if name == "key_top" {
            set_top(&db, 281, Protocol::Ceres, Some(2000), 0);
            let next = w
                .refresh_payout_context(initial.epoch, Default::default())
                .unwrap();
            assert_eq!(state(&next), row["after_event"]);
        }
        count += 1;
    }
    assert_eq!(count, 8);
    // Micro/DOWN dispatch belongs to the live owner, not this storage API.
}
#[test]
fn original_payout_selection_gates_and_failed_push_preserve_exact_accounting_effects() {
    let f = fixture();
    let mut count = 0;
    for row in f["policy"].as_array().unwrap().iter().filter(|r| {
        !r["name"].as_str().unwrap().starts_with("confirm_")
            && !r["name"].as_str().unwrap().starts_with("down_")
    }) {
        let name = row["name"].as_str().unwrap();
        let t = Temp::new();
        let db = database(&t);
        let cached = match name {
            "unknown_balance" => None,
            "state_low" => Some(GROSS - 1),
            "equal_balance" | "equal_cached" => Some(GROSS),
            _ => Some(GROSS + 1_000_000_000),
        };
        set_top(&db, 280, Protocol::Roma, cached, 0);
        let mut w = Writer::start(db.clone(), Default::default()).unwrap();
        reward(&mut w, true);
        let e = w.begin_payouts(config(), Default::default()).unwrap().epoch;
        if name != "empty" {
            let mut p = pay((100, 0));
            if name == "malformed" {
                p.relative.clear();
            }
            if name == "sent_gate" {
                p.tx_hash = Some(Hash256(key(44)));
            }
            commit(&mut w, vec![Command::Payment(p)]);
        }
        if name == "normal" {
            commit(&mut w, vec![Command::Payment(pay((101, 0)))]);
        }
        let actual = match name {
            "equal_balance" => GROSS,
            "actual_low" => GROSS - 1,
            _ => GROSS + 1_000_000_000,
        };
        set_top(&db, 280, Protocol::Roma, Some(actual), 0);
        if name == "in_flight" {
            prepared(&mut w, e);
        }
        assert_eq!(counts(&db), row["counts_before"], "{name}");
        let selection = w.prepare_payout(e, 0.into(), Default::default());
        match name {
            "normal" | "equal_cached" | "failed_push" => {
                let Selection::Prepared(r) = selection.unwrap() else {
                    panic!()
                };
                let r = signed(&mut w, e, &r);
                assert_eq!(
                    hex::encode(r.hash().unwrap().unwrap().0),
                    row["after_callback"]["pending_hash"]["binary"]
                        .as_str()
                        .unwrap()
                );
                if name == "failed_push" {
                    let r = w
                        .begin_payout_submission(e, &r, Default::default())
                        .unwrap();
                    w.reject_payout_submission(
                        e,
                        &r,
                        "deliberate_rejection".into(),
                        Default::default(),
                    )
                    .unwrap();
                }
            }
            "in_flight" => assert!(matches!(selection.unwrap(), Selection::Existing(_))),
            "equal_balance" | "actual_low" | "malformed" => assert!(selection.is_err(), "{name}"),
            _ => assert!(
                matches!(selection.unwrap(), Selection::Deferred(_)),
                "{name}"
            ),
        }
        assert_eq!(counts(&db), row["counts_after"], "{name}");
        let payments = db
            .stratum(Default::default())
            .unwrap()
            .query(|t| t.select_payments(true, None))
            .unwrap();
        if let Some(p) = payments.first() {
            let expected = row["payments"]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| p["tuple"][5]["binary"].is_string())
                .unwrap();
            assert_eq!(
                hex::encode(p.tx_hash.unwrap().0),
                expected["tuple"][5]["binary"].as_str().unwrap(),
                "{name}"
            );
        }
        count += 1;
    }
    assert_eq!(count, 11);
}
#[test]
fn original_confirmation_depth_and_partial_cleanup_match_with_checked_real_transaction_hashes() {
    let f = fixture();
    let mut count = 0;
    for row in f["policy"].as_array().unwrap().iter().filter(|r| {
        r["name"].as_str().unwrap().starts_with("confirm_")
            && !matches!(r["name"].as_str().unwrap(), "confirm_busy" | "confirm_none")
    }) {
        let name = row["name"].as_str().unwrap();
        let t = Temp::new();
        let db = database(&t);
        let (mut w, e) = setup(&db);
        reward(&mut w, name != "confirm_missing_reward");
        if name == "confirm_partial" {
            commit(&mut w, vec![Command::Payment(pay((100, 1)))]);
        }
        let r = prepared(&mut w, e);
        let r = signed(&mut w, e, &r);
        if !matches!(name, "confirm_mempool" | "confirm_unknown") {
            let h = include(&db, &r, if name == "confirm_future" { 281 } else { 100 });
            if name == "confirm_missing_block" {
                db.db.delete(repo::key(repo::BLOCKS, h.0)).unwrap();
            }
        }
        if name == "confirm_mempool" {
            let mut pool =
                crate::pool::Service::start(db.clone(), network(), pool_config()).unwrap();
            pool.push(Arc::new(signed_tx(&r)), Event::Created, false)
                .unwrap();
        }
        set_top(
            &db,
            match name {
                "confirm_before" => 279,
                "confirm_after" => 281,
                _ => 280,
            },
            Protocol::Roma,
            Some(GROSS + 1),
            0,
        );
        assert_eq!(counts(&db), row["counts_before"], "{name}");
        let result = w.confirm_payout(e, &r, Default::default());
        match name {
            "confirm_missing_block" => assert!(result.is_err()),
            "confirm_at" | "confirm_after" | "confirm_partial" => {
                assert!(matches!(result.unwrap(), Confirmation::Confirmed(_)))
            }
            "confirm_missing_reward" => {
                assert_eq!(result.unwrap(), Confirmation::MissingReward);
                assert_eq!(stored(&db).pending, Some(r));
            }
            _ => assert_eq!(result.unwrap(), Confirmation::Pending),
        }
        assert_eq!(counts(&db), row["counts_after"], "{name}");
        count += 1;
    }
    assert_eq!(count, 9);
    // Native adapters use arbitrary hash 44. This test uses actual signed
    // bytes and membership checks; the retained native expectation is the
    // depth decision/table effect, not a fictitious preimage of that hash.
}
