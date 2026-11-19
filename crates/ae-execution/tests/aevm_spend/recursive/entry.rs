use super::*;
static ENTRY: LazyLock<J> = LazyLock::new(|| {
    support::fixture(include_str!(
        "../../fixtures/erlang_aevm_zero_address_transactions.json"
    ))
});
#[test]
fn address_zero_transactions_preserve_original_abi_admission_gas_and_trees() {
    let rows = ENTRY["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 540);
    for row in rows {
        let mut s = state(row);
        let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
        let receipt = aevm::process_call(
            &tx,
            &mut s.trees,
            &transaction_context(row),
            &expanded_config(),
            Limits::default(),
        )
        .unwrap();
        s.events = Arc::new(receipt_events(receipt));
        assert_eq!(
            support::logical(tuple(vec![atom("ok"), snapshot(&s)])),
            support::logical(row["result"]["value"].clone()),
            "{row}"
        );
    }
}
#[test]
fn solidity_vm_remains_disabled_in_all_production_protocols() {
    let rows = ENTRY["solidity_admission"].as_array().unwrap();
    assert_eq!(rows.len(), 24);
    for row in rows {
        let protocol = Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap();
        let version = ae_protocol::contracts::Version {
            vm: 2,
            abi: row["abi"].as_u64().unwrap() as u16,
        };
        assert!(!version.legal());
        assert_eq!(json!(version.can_call(protocol)), row["call"]);
        assert_eq!(json!(version.can_create(protocol)), row["create"]);
        assert_eq!(row["constructor"]["kind"], "native");
        assert_eq!(row["constructor"]["class"], "error");
        let expected = tuple(vec![
            atom("illegal"),
            atom("ct_version"),
            map(vec![("abi", int(version.abi)), ("vm", int(2))]),
        ]);
        assert_eq!(
            support::logical(expected),
            support::logical(row["constructor"]["reason"].clone())
        );
        assert_eq!(
            Contract::new(
                AccountPublicKey(Word::from(9).0),
                ae_state::contracts::Nonce::Number(&1.into()),
                version,
                Code::Inline(vec![]),
                0.into()
            ),
            Err(ae_state::contracts::Error::InvalidVersion)
        );
    }
}
#[test]
fn zero_address_transaction_backend_failures_leave_nonce_fee_and_call_roots_retryable() {
    use crate::aevm_spend::invocations::failures::{Control, wrapped};
    use std::sync::atomic::Ordering::Relaxed;
    let row = ENTRY["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["protocol"] == 4
                && r["target"] == 0
                && r["case_name"] == "store"
                && r["gas"] == 100000
                && r["dry_run"] == false
        })
        .unwrap();
    let input = state(row);
    let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
    let context = transaction_context(row);
    let cfg = expanded_config();
    let a = Arc::new(Control::default());
    let c = Arc::new(Control::default());
    let base = wrapped(&input.trees, &a, &c);
    let before = base.roots();
    let check = |trees, receipt| {
        let s = State {
            trees,
            onchain: None,
            account: input.account,
            vm_version: input.vm_version,
            network_id: input.network_id.clone(),
            context: input.context.clone(),
            event_context: EventContext::None,
            events: Arc::new(receipt_events(receipt)),
        };
        assert_eq!(
            support::logical(snapshot(&s)),
            support::logical(row["result"]["value"][1][1].clone())
        );
    };
    let mut t = base.clone();
    let receipt = aevm::process_call(&tx, &mut t, &context, &cfg, Limits::default())
        .unwrap_or_else(|_| panic!("baseline"));
    check(t, receipt);
    for (control, count) in [(&a, a.writes.load(Relaxed)), (&c, c.writes.load(Relaxed))] {
        assert!(count > 0);
        for remaining in 0..count {
            control.remaining.store(remaining, Relaxed);
            let mut t = base.clone();
            assert!(aevm::process_call(&tx, &mut t, &context, &cfg, Limits::default()).is_err());
            assert_eq!(t.roots(), before);
        }
        control.remaining.store(usize::MAX, Relaxed);
        control.read_error.store(true, Relaxed);
        let mut t = base.clone();
        assert!(aevm::process_call(&tx, &mut t, &context, &cfg, Limits::default()).is_err());
        assert_eq!(t.roots(), before);
        control.read_error.store(false, Relaxed);
    }
    let mut t = base.clone();
    let receipt = aevm::process_call(&tx, &mut t, &context, &cfg, Limits::default())
        .unwrap_or_else(|_| panic!("retry"));
    check(t, receipt);
}
