use super::*;
use ae_vm_aevm::{
    execution::{self, BlockHashMode, Failure, Stop},
    runtime::Outcome,
};
static INSTRUCTIONS: LazyLock<J> = LazyLock::new(|| {
    support::fixture(include_str!(
        "../../fixtures/erlang_aevm_chain_instructions.json"
    ))
});
fn chain(row: &J) -> State {
    let mut trees = trees_from(&map(vec![("accounts", list(vec![]))]));
    for (key, balance) in [
        (BigUint::from(1u8), 11u64),
        ((BigUint::from(1u8) << 160) + 1u8, 22),
    ] {
        trees
            .accounts
            .enter(&Account::new(
                AccountPublicKey(Word::from_unsigned(&key).0),
                balance.into(),
            ))
            .unwrap();
    }
    let onchain = if row["mode"] == "offchain" {
        let mut inner = trees.clone();
        inner
            .accounts
            .enter(&Account::new(AccountPublicKey(Word::from(2).0), 33.into()))
            .unwrap();
        Some(Arc::new(inner))
    } else {
        None
    };
    State {
        trees,
        onchain,
        account: AccountPublicKey(Word::from(1).0),
        vm_version: VM::from_id(row["vm"].as_u64().unwrap() as u32).unwrap(),
        network_id: Arc::from(b"ae_mainnet".as_slice()),
        context: Arc::new(Context::transaction(300, Protocol::Lima)),
        event_context: EventContext::None,
        events: Arc::default(),
    }
}
fn vm(row: &J) -> execution::State {
    let mut s = execution::State::new(
        VM::from_id(row["vm"].as_u64().unwrap() as u32).unwrap(),
        bytes(&row["code"]),
        row["gas"].as_u64().unwrap().into(),
        FloatProfile::Otp26Word64,
    );
    s.abi_version = if s.version == VM::Solidity1 { 2 } else { 1 };
    s.context.number = row["height"].as_u64().unwrap_or(300).into();
    s.stack = row["stack"][1]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| Word::from_unsigned(&support::uint(n)))
        .collect();
    s.block_hash_mode = if row["hash_mode"] == "sha3" {
        BlockHashMode::Sha3
    } else {
        BlockHashMode::Chain
    };
    for p in field(&row["before"], "ext_code_sizes")[1]
        .as_array()
        .unwrap()
    {
        s.external_code_sizes
            .insert(support::uint(&p[0]), support::uint(&p[1]));
    }
    for p in field(&row["before"], "ext_code_blocks")[1]
        .as_array()
        .unwrap()
    {
        s.external_code_blocks
            .insert(support::uint(&p[0]), bytes(&p[1]));
    }
    s
}
fn projection(s: &execution::State) -> J {
    let mut memory = s
        .memory
        .words()
        .iter()
        .map(|(k, v)| (int(k), int(v.unsigned())))
        .collect::<Vec<_>>();
    memory.push((atom("mem_size"), int(s.memory.size_in_words())));
    assert!(s.logs.is_empty());
    map(vec![
        ("cp", int(s.cp)),
        ("gas", int(&s.gas)),
        ("memory", support::map(memory)),
        (
            "stack",
            list(s.stack.iter().map(|w| int(w.unsigned())).collect()),
        ),
        ("logs", list(vec![])),
        ("out", bin(&s.output)),
        ("return_data", bin(&s.return_data)),
        (
            "jumpdests",
            support::map(
                s.jump_destinations
                    .iter()
                    .map(|i| (int(i), atom("true")))
                    .collect(),
            ),
        ),
        (
            "ext_code_sizes",
            support::map(
                s.external_code_sizes
                    .iter()
                    .map(|(k, v)| (int(k), int(v)))
                    .collect(),
            ),
        ),
        (
            "ext_code_blocks",
            support::map(
                s.external_code_blocks
                    .iter()
                    .map(|(k, v)| (int(k), bin(v)))
                    .collect(),
            ),
        ),
    ])
}
fn check(row: &J, result: Result<Outcome<State>, aevm::Error>) -> bool {
    let (actual, complete) = match result {
        Ok(Outcome::Complete { state, .. }) => (
            json!({"kind":"ok", "value":tuple(vec![atom("ok"), projection(&state)])}),
            true,
        ),
        Ok(Outcome::Error { reason, gas }) => {
            let reason = match reason {
                Failure::OutOfGas => "out_of_gas",
                Failure::PopEmpty => "error_pop_empty_stack",
                e => panic!("{e:?}"),
            };
            (
                json!({"kind":"ok", "value":tuple(vec![atom("error"), atom(reason), int(gas)])}),
                false,
            )
        }
        Err(outer_call::Fault::Runtime(Fault::Native(Native::BlockHashNoCommonAncestor))) => (
            json!({"kind":"native", "class":"error", "reason":tuple(vec![atom("case_clause"), tuple(vec![atom("error"),atom("not_found")])])}),
            false,
        ),
        other => panic!("unexpected {other:?}: {row}"),
    };
    assert_eq!(
        support::logical(actual),
        support::logical(row["result"].clone()),
        "{row}"
    );
    complete
}
#[test]
fn four_chain_read_instructions_match_original_vm_and_actual_chain() {
    let cfg = config();
    let host = aevm::Host::new(&cfg);
    let rows = INSTRUCTIONS["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 1000);
    let mut successes = 0;
    for row in rows {
        let chain = chain(row);
        let before = chain.trees.roots();
        let s = vm(row);
        successes += usize::from(check(
            row,
            outer_call::evaluate(&s, &chain, &host, Limits::default()),
        ));
        assert_eq!(chain.trees.roots(), before);
        assert_eq!(s, vm(row));
    }
    assert_eq!(successes, 560);
}
#[test]
fn chain_read_resource_limits_preserve_retryable_inputs() {
    let cfg = config();
    let host = aevm::Host::new(&cfg);
    for row in INSTRUCTIONS["runs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["gas"] == 10000 && r["vm"] == 6 && r["mode"] == "offchain")
    {
        let chain = chain(row);
        let before = chain.trees.roots();
        let s = vm(row);
        for limits in [
            Limits {
                max_work: 0,
                ..Limits::default()
            },
            Limits {
                max_bytes: 0,
                ..Limits::default()
            },
            Limits {
                max_memory_words: 0,
                ..Limits::default()
            },
        ] {
            match outer_call::evaluate(&s, &chain, &host, limits) {
                Err(outer_call::Fault::Runtime(Fault::Stop(Stop::ResourceLimit))) => {}
                result => {
                    check(row, result);
                }
            }
            assert_eq!(chain.trees.roots(), before);
            assert_eq!(s, vm(row));
        }
        check(
            row,
            outer_call::evaluate(&s, &chain, &host, Limits::default()),
        );
    }
}
static HISTORY: LazyLock<J> = LazyLock::new(|| {
    support::fixture(include_str!(
        "../../fixtures/erlang_aevm_chain_history.json"
    ))
});
fn history() -> ae_execution::chain_history::Snapshot {
    use ae_execution::chain_history::{HeaderLink, Snapshot};
    Snapshot {
        selected: HISTORY["history"]["selected"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| (p[0].as_u64().unwrap(), bytes(&p[1]).try_into().unwrap()))
            .collect(),
        headers: HISTORY["history"]["headers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| {
                (
                    bytes(&p["hash"]).try_into().unwrap(),
                    HeaderLink {
                        height: p["height"].as_u64().unwrap(),
                        previous_key: bytes(&p["previous"]).try_into().unwrap(),
                    },
                )
            })
            .collect(),
    }
}
#[test]
fn block_hash_window_and_fork_history_match_original_vm_and_chain_api() {
    let mut cfg = config();
    cfg.environment.history = Arc::new(history());
    let rows = HISTORY["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 1640);
    let mut successes = 0;
    for row in rows {
        cfg.environment.key_hash = bytes(&row["key_hash"]).try_into().unwrap();
        let chain = chain(row);
        let before = chain.trees.roots();
        successes += usize::from(check(
            row,
            outer_call::evaluate(&vm(row), &chain, &aevm::Host::new(&cfg), Limits::default()),
        ));
        assert_eq!(chain.trees.roots(), before);
    }
    assert_eq!(successes, 1005);
}
fn history_row() -> &'static J {
    HISTORY["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["vm"] == 6
                && r["height"] == 300
                && r["gas"] == 1000
                && r["key_hash"] == bin(Word::from(20300).0)
                && r["stack"] == list(vec![int(299)])
        })
        .unwrap()
}
#[test]
fn missing_or_cyclic_history_is_retryable_and_only_read_inside_the_window() {
    use ae_execution::chain_history::{Error as E, HeaderLink};
    let row = history_row();
    let chain = chain(row);
    let original = vm(row);
    let before = chain.trees.roots();
    let mut cfg = config();
    cfg.environment.key_hash = Word::from(20300).0;
    let run = |s: &execution::State, cfg: &aevm::Config, limits| {
        outer_call::evaluate(s, &chain, &aevm::Host::new(cfg), limits)
    };
    assert!(matches!(
        run(&original, &cfg, Limits::default()),
        Err(outer_call::Fault::Runtime(Fault::Host(
            aevm::HostError::History(E::NeedHeight(299))
        )))
    ));
    let mut missing = history();
    missing.headers.remove(&Word::from(20300).0);
    cfg.environment.history = Arc::new(missing);
    assert!(matches!(
        run(&original, &cfg, Limits::default()),
        Err(outer_call::Fault::Runtime(Fault::Host(
            aevm::HostError::History(E::NeedHeader(_))
        )))
    ));
    let mut cycle = history();
    cycle.headers.insert(
        Word::from(20300).0,
        HeaderLink {
            height: 300,
            previous_key: Word::from(20300).0,
        },
    );
    cfg.environment.history = Arc::new(cycle);
    assert!(matches!(
        run(
            &original,
            &cfg,
            Limits {
                max_work: 1000,
                ..Limits::default()
            }
        ),
        Err(outer_call::Fault::Runtime(Fault::Stop(Stop::ResourceLimit)))
    ));
    cfg.environment.history = Arc::default();
    for height in [0, 43, 300, 301] {
        let mut s = original.clone();
        s.stack = vec![Word::from(height)];
        s.gas = 20u8.into();
        let Ok(Outcome::Complete { state, .. }) = run(&s, &cfg, Limits::default()) else {
            panic!("outside window")
        };
        assert_eq!(state.stack, vec![Word::ZERO]);
        assert_eq!(state.gas, 0u8.into());
    }
    let mut low_gas = original.clone();
    low_gas.gas = 19u8.into();
    assert!(matches!(
        run(&low_gas, &cfg, Limits::default()),
        Ok(Outcome::Error {
            reason: Failure::OutOfGas,
            ..
        })
    ));
    cfg.environment.history = Arc::new(history());
    let mut saw_stop = false;
    let mut saw_success = false;
    for n in [0, 1, 4, 16, 64, 256, 1024] {
        match run(
            &original,
            &cfg,
            Limits {
                max_work: n,
                ..Limits::default()
            },
        ) {
            Err(outer_call::Fault::Runtime(Fault::Stop(Stop::ResourceLimit))) => saw_stop = true,
            result => {
                assert!(check(row, result));
                saw_success = true;
                break;
            }
        }
    }
    assert!(saw_stop && saw_success);
    assert_eq!(chain.trees.roots(), before);
    assert_eq!(original, vm(row));
    check(row, run(&original, &cfg, Limits::default()));
}
#[test]
fn balance_read_failure_is_not_absence_and_code_queries_do_not_read_backend() {
    use crate::aevm_spend::invocations::failures::{Control, wrapped};
    use std::sync::atomic::Ordering::Relaxed;
    let row = INSTRUCTIONS["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["vm"] == 6
                && r["gas"] == 10000
                && r["case_name"] == "balance"
                && r["mode"] == "offchain"
        })
        .unwrap();
    let input = chain(row);
    let a = Arc::new(Control::default());
    let c = Arc::new(Control::default());
    let chain = State {
        trees: wrapped(&input.trees, &a, &c),
        onchain: Some(Arc::new(wrapped(
            input.onchain.as_ref().unwrap(),
            &Arc::default(),
            &Arc::default(),
        ))),
        account: input.account,
        vm_version: input.vm_version,
        network_id: input.network_id,
        context: input.context,
        event_context: input.event_context,
        events: input.events,
    };
    let before = chain.trees.roots();
    let cfg = config();
    let host = aevm::Host::new(&cfg);
    let mut s = vm(row);
    a.read_error.store(true, Relaxed);
    c.read_error.store(true, Relaxed);
    assert!(matches!(
        outer_call::evaluate(&s, &chain, &host, Limits::default()),
        Err(outer_call::Fault::Runtime(Fault::Host(
            aevm::HostError::Chain(aevm_chain::Error::Accounts(_))
        )))
    ));
    s.code = vec![0x3b, 0];
    let Ok(Outcome::Complete { state, .. }) =
        outer_call::evaluate(&s, &chain, &host, Limits::default())
    else {
        panic!("external code uses VM table")
    };
    assert_eq!(state.stack, vec![Word::from(3)]);
    s.external_code_sizes.clear();
    s.external_code_blocks.clear();
    let Ok(Outcome::Complete { state, .. }) =
        outer_call::evaluate(&s, &chain, &host, Limits::default())
    else {
        panic!("production empty code table")
    };
    assert_eq!(state.stack, vec![Word::ZERO]);
    a.read_error.store(false, Relaxed);
    c.read_error.store(false, Relaxed);
    let mut s = vm(row);
    s.call_stack = vec![0.into(); 1024];
    let Ok(Outcome::Complete { state, .. }) =
        outer_call::evaluate(&s, &chain, &host, Limits::default())
    else {
        panic!("balance is independent of call-stack guard")
    };
    assert_eq!(state.stack, vec![Word::from(11)]);
    assert_eq!(chain.trees.roots(), before);
}
static TRANSACTIONS: LazyLock<J> = LazyLock::new(|| {
    support::fixture(include_str!(
        "../../fixtures/erlang_aevm_chain_read_transactions.json"
    ))
});
fn transaction_config(row: &J) -> aevm::Config {
    let mut cfg = expanded_config();
    cfg.environment.history = Arc::new(history());
    cfg.environment.key_hash = bytes(&row["key_hash"]).try_into().unwrap();
    cfg
}
fn transaction_input(row: &J) -> (State, ContractTransaction, Context) {
    let s = super::state(row);
    let tx = ContractTransaction::decode(&bytes(&row["transaction"])).unwrap();
    let mut context = transaction_context(row);
    context.height = 300;
    (s, tx, context)
}
#[test]
fn chain_read_transactions_match_original_gas_accounts_calls_stores_and_events() {
    let rows = TRANSACTIONS["runs"].as_array().unwrap();
    assert_eq!(rows.len(), 972);
    for row in rows {
        let (mut s, tx, context) = transaction_input(row);
        let receipt = aevm::process_call(
            &tx,
            &mut s.trees,
            &context,
            &transaction_config(row),
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
fn transaction_history_stops_and_write_failures_do_not_publish_partial_state() {
    use crate::aevm_spend::invocations::failures::{Control, wrapped};
    use ae_execution::chain_history::Error as E;
    use std::sync::atomic::Ordering::Relaxed;
    for name in ["store_hash", "nested_hash", "extcopy", "balance_high"] {
        let row = TRANSACTIONS["runs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| {
                r["protocol"] == 4
                    && r["case_name"] == name
                    && r["gas"] == 100000
                    && r["dry_run"] == false
                    && r["key_hash"] == bin(Word::from(20300).0)
            })
            .unwrap();
        let (input, tx, context) = transaction_input(row);
        let mut cfg = transaction_config(row);
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
        if matches!(name, "store_hash" | "nested_hash") {
            cfg.environment.history = Arc::default();
            let mut t = base.clone();
            assert!(matches!(
                aevm::process_call(&tx, &mut t, &context, &cfg, Limits::default()),
                Err(outer_call::Fault::Runtime(Fault::Host(
                    aevm::HostError::History(E::NeedHeight(299))
                )))
            ));
            assert_eq!(t.roots(), before);
            cfg.environment.history = Arc::new(history());
        }
        a.writes.store(0, Relaxed);
        c.writes.store(0, Relaxed);
        let mut t = base.clone();
        let receipt = aevm::process_call(&tx, &mut t, &context, &cfg, Limits::default())
            .unwrap_or_else(|_| panic!("transaction failed"));
        check(t, receipt);
        let counts = [a.writes.load(Relaxed), c.writes.load(Relaxed)];
        for (control, count) in [(&a, counts[0]), (&c, counts[1])] {
            assert!(count > 0);
            for left in 0..count {
                control.remaining.store(left, Relaxed);
                let mut t = base.clone();
                assert!(
                    aevm::process_call(&tx, &mut t, &context, &cfg, Limits::default()).is_err()
                );
                assert_eq!(t.roots(), before);
            }
            control.remaining.store(usize::MAX, Relaxed);
        }
        let mut t = base.clone();
        let receipt = aevm::process_call(&tx, &mut t, &context, &cfg, Limits::default())
            .unwrap_or_else(|_| panic!("transaction failed"));
        check(t, receipt);
    }
}
