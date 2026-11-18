use ae_execution::fate_store::Backend;
use ae_state::contracts::store::Store;
use ae_trie::{MemoryStore, Nibbles, Trie, Value as TrieValue};
use ae_vm_fate::{
    Limits,
    code::{Instruction, Operand},
    gas::Meter,
    machine::{Context, Control, Error, Exception, Executor, Outcome, Reason, Register, State},
    maps::listing::Overrides,
    opcodes::Opcode,
    stack::Accumulator,
    store::{Stores, Version},
    types::Version as VmVersion,
    value::Value,
};
use num_bigint::BigInt;
use serde_json::Value as J;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::LazyLock,
};
#[path = "../../ae-vm-fate/tests/support/otp_order.rs"]
mod otp_order;
#[path = "../../ae-vm-fate/tests/support/values.rs"]
mod values;
use values::{bytes, rows, string, value};
#[path = "support/fate_auth.rs"]
mod auth_tests;
#[path = "support/fate_context.rs"]
mod context_tests;
#[path = "support/fate_create.rs"]
mod create_tests;
#[path = "support/fate_deployment.rs"]
mod deployment_tests;
#[path = "support/fate_hosts.rs"]
mod host_tests;
#[path = "support/fate_names.rs"]
mod name_tests;
#[path = "support/fate_oracle_writes.rs"]
mod oracle_write_tests;
#[path = "support/fate_program.rs"]
mod program_tests;
#[path = "support/fate_remote.rs"]
mod remote_tests;
static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_instructions.json")).unwrap()
});
static NUMBERS: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_numbers.json")).unwrap()
});
static NATIVE_LIMITS: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_integer_limits.json")).unwrap()
});
static COMPARISONS: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_comparisons.json")).unwrap()
});
static BINARY: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_binary.json")).unwrap()
});
static BINARY_LIMITS: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_binary_limits.json")).unwrap()
});
static CRYPTO: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_crypto.json")).unwrap()
});
static POSEIDON: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_poseidon.json")).unwrap()
});
static BLS_FIELDS: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_bls_fields.json")).unwrap()
});
static BLS_POINTS: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_bls_points.json")).unwrap()
});
static BLS_SCALARS: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_bls_scalars.json")).unwrap()
});
static BLS_GT: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_bls_gt.json")).unwrap()
});
static BLS_PAIRING: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_bls_pairing.json")).unwrap()
});
static BLS_REVERSE: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_bls_reverse.json")).unwrap()
});
static PROGRAMS: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_programs.json")).unwrap()
});
const KEY: [u8; 32] = [1; 32];
fn number(j: &J) -> BigInt {
    string(j).parse().unwrap()
}
fn register(kind: &J, name: &J) -> Register {
    match string(kind) {
        "arg" => Register::Argument(value(name)),
        "var" => Register::Variable(value(name)),
        _ => unreachable!(),
    }
}
fn memory(j: &J) -> BTreeMap<Register, Value> {
    rows(j)
        .iter()
        .map(|v| (register(&v[0], &v[1]), value(&v[2])))
        .collect()
}
fn pairs(j: &J) -> BTreeMap<Vec<u8>, Vec<u8>> {
    rows(j)
        .iter()
        .map(|v| (bytes(&v[0]), bytes(&v[1])))
        .collect()
}
fn backend() -> Backend {
    let mut tree = Trie::new(MemoryStore::default());
    for (k, v) in pairs(&FIXTURE["base"]) {
        tree.put(&Nibbles::from_bytes(&k), TrieValue::Bytes(v))
            .unwrap();
    }
    Backend(Store::new(tree))
}
fn state(row: &J, gas: &J) -> State<Backend> {
    let j = &row["initial"];
    let protocol = row["protocol"].as_u64().unwrap() as u32;
    let mut stores = Stores::new(Version::for_protocol(protocol));
    if !j["store"].is_null() {
        stores.put_contract(KEY, backend());
        for term in rows(&j["store"]["terms"]) {
            assert_eq!(term[2], true);
            stores
                .put_value(&KEY, number(&term[0]), value(&term[1]))
                .unwrap();
        }
    }
    State {
        accumulator: Accumulator {
            top: value(&j["top"]),
            rest: rows(&j["rest"]).iter().map(value).collect(),
        },
        memory: memory(&j["memory"]),
        stores,
        meter: Meter {
            protocol,
            gas: number(gas),
            created_cells: number(&j["cells"]),
            metering_floor: row["floor"].as_u64().map(|v| v as u32),
        },
    }
}
fn context(row: &J) -> Context {
    Context {
        current_contract: KEY,
        vm_version: match row["vm"].as_u64().unwrap() {
            5 => VmVersion::One,
            7 => VmVersion::Two,
            8 => VmVersion::Three,
            _ => unreachable!(),
        },
        in_auth: row["auth"].as_bool().unwrap(),
        on_chain: row["onchain"].as_bool().unwrap(),
        reserved_gas: 18.into(),
        unicode_profile: ae_vm_fate::unicode::Profile::Otp26Unicode15,
        bls_profile: ae_crypto::bls::Profile::Mcl11976beLlvm64VintArm64,
        arithmetic_word_size: match row["word_size"].as_u64().unwrap_or(64) {
            32 => ae_vm_fate::maps::order::WordSize::Bits32,
            64 => ae_vm_fate::maps::order::WordSize::Bits64,
            _ => unreachable!(),
        },
    }
}
fn instruction(j: &J) -> Instruction {
    Instruction {
        opcode: *Opcode::ALL
            .iter()
            .find(|op| op.info().name == string(&j[0]))
            .unwrap(),
        operands: rows(&j[1])
            .iter()
            .map(|a| match string(&a[0]) {
                "stack" => Operand::Stack,
                "arg" => Operand::Argument(value(&a[1])),
                "var" => Operand::Variable(value(&a[1])),
                "immediate" => Operand::Immediate(value(&a[1])),
                _ => unreachable!(),
            })
            .collect(),
    }
}
fn overrides(row: &J) -> Overrides {
    let encoded: Vec<_> = rows(&row["overrides"])
        .iter()
        .map(|p| {
            (
                ae_vm_fate::encoding::encode(&value(&p[0]), Limits::default()).unwrap(),
                ae_vm_fate::encoding::encode(&value(&p[1]), Limits::default()).unwrap(),
            )
        })
        .collect();
    Overrides::from_encoded(
        encoded.iter().map(|(k, v)| (k.as_slice(), v.as_slice())),
        Limits::default(),
    )
    .unwrap()
}
fn assert_state(actual: &State<Backend>, expected: &J) {
    assert_eq!(actual.accumulator.top, value(&expected["top"]), "top");
    assert_eq!(
        actual.accumulator.rest.iter().cloned().collect::<Vec<_>>(),
        rows(&expected["rest"])
            .iter()
            .map(value)
            .collect::<Vec<_>>(),
        "rest"
    );
    assert_eq!(actual.meter.gas, number(&expected["gas"]), "gas");
    assert_eq!(
        actual.meter.created_cells,
        number(&expected["cells"]),
        "cells"
    );
    assert_eq!(actual.memory, memory(&expected["memory"]), "memory");
    if expected["store"].is_null() {
        assert!(!actual.stores.has_contract(&KEY));
        return;
    }
    let a = actual.stores.entry(&KEY).unwrap();
    let e = &expected["store"];
    assert_eq!(a.dirty(), e["dirty"].as_bool().unwrap(), "dirty");
    let terms: BTreeMap<_, _> = rows(&e["terms"])
        .iter()
        .map(|t| (number(&t[0]), (value(&t[1]), t[2].as_bool().unwrap())))
        .collect();
    assert_eq!(a.terms(), &terms, "decoded store cache");
    assert_eq!(
        a.backend().0.read_cache(),
        &pairs(&e["reads"]),
        "read cache"
    );
    assert_eq!(
        a.backend().0.write_cache(),
        &pairs(&e["writes"]),
        "write cache"
    );
    assert_eq!(
        a.backend().0.cached_subtrees(),
        &rows(&e["subtrees"])
            .iter()
            .map(bytes)
            .collect::<BTreeSet<_>>(),
        "subtree cache"
    );
}
fn assert_reason(actual: &Reason, expected: &J) {
    let name = match actual {
        Reason::NameEncoding { operation, error } => {
            assert_eq!(*operation, string(&expected[1]));
            let label = match error {
                ae_codec::idna::Error::NoRegistrar => "no_registrar",
                ae_codec::idna::Error::RegistrarUnknown => "registrar_unknown",
                ae_codec::idna::Error::MultipleNamespaces => "multiple_namespaces",
                ae_codec::idna::Error::NoLabelInRegistrar => "no_label_in_registrar",
                ae_codec::idna::Error::InvalidCodepoint(cp) => {
                    assert_eq!(*cp as u64, expected[2]["codepoint"].as_u64().unwrap());
                    "invalid_codepoint"
                }
                ae_codec::idna::Error::BadLabel(b) => match b {
                    ae_codec::idna::BadLabel::Nfc => "nfc",
                    ae_codec::idna::BadLabel::Hyphen => "hyphen",
                    ae_codec::idna::BadLabel::InitialCombiner => "initial_combiner",
                    ae_codec::idna::BadLabel::Context => "context",
                    ae_codec::idna::BadLabel::ContextJ => "contextj",
                    ae_codec::idna::BadLabel::ContextO => "contexto",
                    ae_codec::idna::BadLabel::Bidi => "bidi",
                    ae_codec::idna::BadLabel::Alabel => "alabel",
                    ae_codec::idna::BadLabel::TooLong => "too_long",
                },
                other => panic!("unexpected {other:?}"),
            };
            assert_eq!(label, string(&expected[2]["error"]));
            "name_error"
        }
        Reason::Primop { operation, reason } => {
            assert_eq!(*operation, string(&expected[1]));
            assert_eq!(*reason, string(&expected[2]));
            "primop"
        }
        Reason::LogIllegalInt(n) => {
            assert_eq!(*n, number(&expected[1]));
            "log_integer"
        }
        Reason::LogIllegalBits => "log_bits",
        Reason::NotImplemented(n) => {
            assert_eq!(*n, string(&expected[1]));
            "not_implemented"
        }
        Reason::DisabledName(n) => {
            assert_eq!(*n, string(&expected[1]));
            "disabled_name"
        }
        Reason::OutOfGas => "out_of_gas",
        Reason::EmptyStack => "empty_stack",
        Reason::MissingMapKey => "missing_map_key",
        Reason::HeadEmpty => "head_empty",
        Reason::TailEmpty => "tail_empty",
        Reason::DivisionByZero => "division_by_zero",
        Reason::ModuloByZero => "mod_by_zero",
        Reason::Arithmetic(reason) => {
            assert_eq!(*reason, string(&expected[1]));
            "arithmetic"
        }
        Reason::Undefined(r) => {
            assert_eq!(r, &register(&expected[1], &expected[2]));
            "undefined"
        }
        Reason::Type {
            operation,
            arguments,
        } => {
            assert_eq!(*operation, string(&expected[1]));
            assert_eq!(
                *arguments,
                rows(&expected[2]).iter().map(value).collect::<Vec<_>>()
            );
            "type"
        }
        Reason::ValueType {
            value: v,
            expected: t,
        } => {
            assert_eq!(*v, value(&expected[1]));
            assert_eq!(*t, string(&expected[2]));
            "value_type"
        }
        Reason::BadMap {
            operation,
            value: v,
        } => {
            assert_eq!(*operation, string(&expected[1]));
            assert_eq!(*v, value(&expected[2]));
            "bad_map"
        }
        Reason::InvalidTupleSize(v) => {
            assert_eq!(*v, value(&expected[1]));
            "invalid_tuple_size"
        }
        Reason::ElementBounds(v) => {
            assert_eq!(*v, value(&expected[1]));
            "element_index_out_of_bounds"
        }
        Reason::BadVariantSize(n) => {
            assert_eq!(*n, expected[1].as_u64().unwrap() as usize);
            "bad_variant_size"
        }
        Reason::BadVariantTag(n) => {
            assert_eq!(*n, expected[1].as_u64().unwrap() as usize);
            "bad_variant_tag"
        }
        Reason::AuthTransactionType(kind) => {
            assert_eq!(*kind, string(&expected[1]));
            "auth_tx_type"
        }
        Reason::Unavailable(op) => {
            assert_eq!(*op, string(&expected[1]));
            "unavailable"
        }
        Reason::Disabled(op) => {
            assert_eq!(op.info().name, string(&expected[1]));
            "disabled"
        }
        Reason::NotAllowedInAuth(op) => {
            assert_eq!(op.info().name, string(&expected[1]));
            "auth"
        }
        Reason::NotAllowedOffchain(op) => {
            assert_eq!(op.info().name, string(&expected[1]));
            "offchain"
        }
        Reason::UserExit(v) => {
            assert_eq!(*v, bytes(&expected[1]));
            "exit"
        }
        Reason::MissingFunction(v) => {
            assert_eq!(*v, value(&expected[1]));
            "missing_function"
        }
        Reason::MissingBlock(v) => {
            assert_eq!(*v, value(&expected[1]));
            "missing_block"
        }
        Reason::InvalidInitCall => "invalid_init",
        Reason::PrivateFunction(v) => {
            assert_eq!(*v, value(&expected[1]));
            "private"
        }
        Reason::NonPayableFunction(v) => {
            assert_eq!(*v, value(&expected[1]));
            "nonpayable"
        }
        Reason::FunctionArity {
            given,
            expected: arity,
        } => {
            assert_eq!(*given, expected[1].as_u64().unwrap() as usize);
            assert_eq!(*arity, expected[2].as_u64().unwrap() as usize);
            "arity"
        }
        Reason::CallType {
            arguments,
            expected: types,
        } => {
            assert_eq!(
                *arguments,
                rows(&expected[1]).iter().map(value).collect::<Vec<_>>()
            );
            assert_eq!(
                *types,
                rows(&expected[2])
                    .iter()
                    .map(values::ty)
                    .collect::<Vec<_>>()
            );
            "call_type"
        }
        Reason::MissingContract(key) => {
            assert_eq!(*key, bytes(&expected[1]));
            "missing_contract"
        }
        Reason::ReentrantCall => "reentrant",
        Reason::RemoteTypeMismatch => "remote_type",
        Reason::BadGasCap(n) => {
            assert_eq!(*n, number(&expected[1]));
            "gas_cap"
        }
        Reason::CallError(s) => {
            assert_eq!(*s, string(&expected[1]));
            "call_error"
        }
        Reason::BadReturnType {
            value: v,
            expected: t,
        } => {
            assert_eq!(*v, value(&expected[1]));
            assert_eq!(*t, values::ty(&expected[2]));
            "return_type"
        }
    };
    assert_eq!(name, string(&expected[0]));
}
fn assert_outcome(out: Outcome<Backend>, expected: &J) {
    let kind = match out {
        Outcome::Continue { state, control, .. } => {
            assert_state(&state, &expected["state"]);
            match control {
                Control::Next => "ok",
                Control::Jump(v) => {
                    assert_eq!(v, value(&expected["target"]));
                    "jump"
                }
            }
        }
        Outcome::Abort { state, reason, .. } => {
            assert_reason(&reason, &expected["reason"]);
            assert_state(&state, &expected["state"]);
            "abort"
        }
        Outcome::Revert { state, value, .. } => {
            assert_eq!(value, bytes(&expected["value"]));
            assert_state(&state, &expected["state"]);
            "revert"
        }
        Outcome::Exception(e) => {
            assert_eq!(
                match e {
                    Exception::UnknownInstruction => "unknown",
                    Exception::Native(s) => s,
                    Exception::Exit(s) => s,
                    Exception::DetachedValueType { .. } => "detached_value_type",
                    Exception::DetachedDuplicateIndex => "dup",
                    Exception::StoreData => "store_data",
                    Exception::Host { .. } => "host",
                },
                string(&expected["reason"])
            );
            "exception"
        }
    };
    assert_eq!(kind, string(&expected["kind"]));
}
#[test]
fn instruction_results_abort_checkpoints_and_store_caches_match_original_dispatcher() {
    check_runs(&FIXTURE);
}
fn check_runs(fixture: &J) {
    check_run_rows(fixture, &fixture["cases"]);
}
fn check_run_rows(fixture: &J, cases: &J) {
    assert_eq!(fixture["base"], FIXTURE["base"]);
    let order = otp_order::profile(&fixture["profile"]);
    let contracts = BTreeMap::new();
    for (i, row) in rows(cases).iter().enumerate() {
        let overrides = overrides(row);
        let ctx = context(row);
        let executor = Executor {
            context: &ctx,
            order: &order,
            overrides: &overrides,
            contracts: &contracts,
        };
        let ins = instruction(&row["op"]);
        let mut initial = state(row, &row["initial"]["gas"]);
        for run in rows(&row["runs"]) {
            initial.meter.gas = number(&run[0]);
            let expected = &fixture["results"][run[1].as_u64().unwrap() as usize];
            let result = std::panic::catch_unwind(|| {
                assert_outcome(
                    executor.step(&initial, &ins, Limits::default()).unwrap(),
                    expected,
                );
                let mut unchanged = row["initial"].clone();
                unchanged["gas"] = run[0].clone();
                assert_state(&initial, &unchanged);
            });
            assert!(
                result.is_ok(),
                "case {i}, protocol {}, instruction {}, gas {}, expected {expected}",
                row["protocol"],
                row["op"],
                run[0]
            );
        }
    }
}
#[test]
fn bls_reverse_conversion_defined_results_and_failure_checkpoints_match_original() {
    check_runs(&BLS_REVERSE);
    check_sequences(&BLS_REVERSE);
    check_fetches(&BLS_REVERSE);
    check_run_rows(&BLS_REVERSE, &BLS_REVERSE["prefix"]);
}

#[test]
fn bls_undefined_native_write_is_an_operational_stop_without_publishing_state() {
    let order = otp_order::profile(&BLS_REVERSE["profile"]);
    let overrides = Overrides::default();
    let contracts = BTreeMap::new();
    for row in rows(&BLS_REVERSE["prefix"]).iter().filter(|r| r["vm"] != 5) {
        let ctx = context(row);
        let executor = Executor {
            context: &ctx,
            order: &order,
            overrides: &overrides,
            contracts: &contracts,
        };
        let original = instruction(&row["op"]);
        let Operand::Immediate(raw) = &original.operands[1] else {
            unreachable!()
        };
        for source in [
            Operand::Immediate(raw.clone()),
            Operand::Stack,
            Operand::Variable(Value::Integer(3.into())),
            Operand::Variable(Value::Integer((-3).into())),
        ] {
            for destination in [
                Operand::Stack,
                Operand::Variable(Value::Integer((-4).into())),
                Operand::Immediate(Value::Integer(0.into())),
            ] {
                let mut input = state(row, &serde_json::json!("1000000"));
                input.accumulator.top = raw.clone();
                input.accumulator.rest = [Value::Integer(99.into())].into();
                input
                    .memory
                    .insert(Register::Variable(Value::Integer(3.into())), raw.clone());
                input.stores.put_value(&KEY, 3.into(), raw.clone()).unwrap();
                let before = format!("{input:?}");
                let ins = Instruction {
                    opcode: original.opcode,
                    operands: vec![destination, source.clone()],
                };
                for _ in 0..2 {
                    assert!(matches!(executor.step(&input, &ins, Limits::default()),
                        Err(Error::UnresolvedNativeBehavior { opcode, profile })
                        if opcode == original.opcode && profile == ctx.bls_profile));
                    assert_eq!(format!("{input:?}"), before);
                }
                assert!(matches!(
                    executor.step(
                        &input,
                        &ins,
                        Limits {
                            max_nodes: 0,
                            ..Limits::default()
                        }
                    ),
                    Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
                ));
                assert_eq!(format!("{input:?}"), before);
            }
        }
    }
}
#[test]
fn chained_instructions_reuse_dirty_registers_and_store_caches() {
    check_sequences(&FIXTURE);
}
fn check_sequences(fixture: &J) {
    let order = otp_order::profile(&fixture["profile"]);
    let overrides = Overrides::default();
    let contracts = BTreeMap::new();
    for row in rows(&fixture["sequences"]) {
        let ctx = context(row);
        let executor = Executor {
            context: &ctx,
            order: &order,
            overrides: &overrides,
            contracts: &contracts,
        };
        let mut input = state(row, &row["initial"]["gas"]);
        for (i, step) in rows(&row["steps"]).iter().enumerate() {
            let out = executor
                .step(&input, &instruction(&step[0]), Limits::default())
                .unwrap();
            if let Outcome::Continue { state, .. } = &out {
                input = state.clone();
            } else {
                assert_eq!(i + 1, rows(&row["steps"]).len());
            }
            assert_outcome(out, &fixture["results"][step[1].as_u64().unwrap() as usize]);
        }
    }
}
#[test]
fn absent_contract_is_an_operational_fetch_request_and_retries_the_same_instruction() {
    let row = rows(&FIXTURE["cases"])
        .iter()
        .find(|r| {
            r["protocol"] == 7
                && r["op"][0] == "STORE"
                && r["op"][1][1] == serde_json::json!(["var", ["integer", "-1"]])
        })
        .unwrap();
    let ctx = context(row);
    let order = otp_order::profile(&FIXTURE["profile"]);
    let overrides = Overrides::default();
    let mut contracts = BTreeMap::new();
    let mut input = state(row, &serde_json::json!("1000000"));
    input.stores = Stores::new(Version::Arcus);
    let ins = instruction(&row["op"]);
    let run = |contracts: &BTreeMap<_, _>, limits| {
        Executor {
            context: &ctx,
            order: &order,
            overrides: &overrides,
            contracts,
        }
        .step(&input, &ins, limits)
    };
    assert!(matches!(
        run(&contracts, Limits::default()),
        Err(Error::NeedContract(KEY))
    ));
    contracts.insert(KEY, backend());
    assert!(matches!(
        run(
            &contracts,
            Limits {
                max_nodes: 0,
                ..Limits::default()
            }
        ),
        Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
    ));
    let high = rows(&row["runs"])
        .iter()
        .find(|r| r[0] == "1000000")
        .unwrap();
    assert_outcome(
        run(&contracts, Limits::default()).unwrap(),
        &FIXTURE["results"][high[1].as_u64().unwrap() as usize],
    );
    assert!(!input.stores.has_contract(&KEY));
    assert_eq!(input.meter.gas, 1000000.into());
}

#[test]
fn unimplemented_instruction_body_is_an_operational_stop() {
    let row = &FIXTURE["cases"][0];
    let input = state(row, &row["initial"]["gas"]);
    let context = context(row);
    let order = otp_order::profile(&FIXTURE["profile"]);
    let overrides = Overrides::default();
    let contracts = BTreeMap::new();
    let executor = Executor {
        context: &context,
        order: &order,
        overrides: &overrides,
        contracts: &contracts,
    };
    let call = Instruction {
        opcode: Opcode::Call,
        operands: vec![Operand::Immediate(Value::Integer(0.into()))],
    };
    assert!(matches!(
        executor.step(&input, &call, Limits::default()),
        Err(Error::Unsupported(Opcode::Call))
    ));
    assert_state(&input, &row["initial"]);
}

#[test]
fn arithmetic_bits_and_variants_match_original_dispatcher_and_sequences() {
    check_runs(&NUMBERS);
    check_sequences(&NUMBERS);
}

#[test]
fn byte_string_character_and_address_instructions_match_original_dispatcher() {
    check_runs(&BINARY);
    check_sequences(&BINARY);
}

#[test]
fn hashing_signature_verification_and_recovery_match_original_dispatcher() {
    check_runs(&CRYPTO);
    check_sequences(&CRYPTO);
}
#[test]
fn hash_unfolding_fetches_and_native_gas_boundaries_match_original_runtime() {
    check_fetches(&CRYPTO);
    check_native_cases(&CRYPTO["native"]);
}
#[test]
fn hash_unfolding_work_limits_leave_inputs_retryable() {
    check_unfold_work(&CRYPTO, "SHA3");
}

#[test]
fn poseidon_field_boundaries_and_gas_checkpoints_match_original_dispatcher() {
    check_runs(&POSEIDON);
    check_sequences(&POSEIDON);
    check_fetches(&POSEIDON);
}

#[test]
fn bls_integer_to_field_gates_and_store_writes_match_original_dispatcher() {
    check_runs(&BLS_FIELDS);
    check_sequences(&BLS_FIELDS);
    check_fetches(&BLS_FIELDS);
    check_native_cases(&BLS_FIELDS["native"]);
}

#[test]
fn bls_point_and_target_operations_gates_and_store_effects_match_original_dispatcher() {
    check_runs(&BLS_POINTS);
    check_sequences(&BLS_POINTS);
    check_fetches(&BLS_POINTS);
}

#[test]
fn bls_scalar_multiplication_gates_and_store_effects_match_original_dispatcher() {
    check_runs(&BLS_SCALARS);
    check_sequences(&BLS_SCALARS);
    check_fetches(&BLS_SCALARS);
}

#[test]
fn bls_gt_multiplication_and_power_gates_and_store_effects_match_original_dispatcher() {
    check_runs(&BLS_GT);
    check_sequences(&BLS_GT);
    check_fetches(&BLS_GT);
}

#[test]
fn bls_pairing_gates_and_store_effects_match_original_dispatcher() {
    check_runs(&BLS_PAIRING);
    check_sequences(&BLS_PAIRING);
    check_fetches(&BLS_PAIRING);
}

#[test]
fn comparisons_unfold_each_operand_and_match_historical_ordering_and_abort_states() {
    check_runs(&COMPARISONS);
    check_sequences(&COMPARISONS);
}

#[test]
fn comparison_traversal_native_gas_capacity_matches_original_runtime() {
    check_native_cases(&COMPARISONS["native"]);
}

#[test]
fn comparison_fetches_happen_after_simple_gas_and_preserve_retry_checkpoints() {
    check_fetches(&COMPARISONS);
}
fn check_fetches(fixture: &J) {
    let order = otp_order::profile(&fixture["profile"]);
    let overrides = Overrides::default();
    let fetched = BTreeMap::from([(KEY, backend())]);
    let empty = BTreeMap::new();
    let mut stopped_before_fetch = 0;
    let mut fetched_before_abort = 0;
    for row in rows(&fixture["fetches"]) {
        let ctx = context(row);
        let ins = instruction(&row["op"]);
        for run in rows(&row["runs"]) {
            let input = state(row, &run[0]);
            let expected = &fixture["results"][run[1].as_u64().unwrap() as usize];
            let exec = |contracts, limits| {
                Executor {
                    context: &ctx,
                    order: &order,
                    overrides: &overrides,
                    contracts,
                }
                .step(&input, &ins, limits)
            };
            assert_outcome(exec(&fetched, Limits::default()).unwrap(), expected);
            if expected["state"]["store"].is_null() {
                stopped_before_fetch += 1;
                assert_outcome(exec(&empty, Limits::default()).unwrap(), expected);
            } else {
                fetched_before_abort += usize::from(expected["kind"] == "abort");
                assert!(matches!(
                    exec(&empty, Limits::default()),
                    Err(Error::NeedContract(KEY))
                ));
            }
            let mut unchanged = row["initial"].clone();
            unchanged["gas"] = run[0].clone();
            assert_state(&input, &unchanged);
        }
    }
    assert!(stopped_before_fetch > 0 && fetched_before_abort > 0);
}

#[test]
fn comparison_unfold_work_is_bounded_and_failed_attempts_keep_inputs_retryable() {
    check_unfold_work(&COMPARISONS, "EQ");
}
fn check_unfold_work(fixture: &J, opcode: &str) {
    let row = rows(&fixture["fetches"])
        .iter()
        .find(|r| r["protocol"] == 8 && r["op"][0] == opcode)
        .unwrap();
    let ctx = context(row);
    let order = otp_order::profile(&fixture["profile"]);
    let overrides = Overrides::default();
    let contracts = BTreeMap::from([(KEY, backend())]);
    let input = state(row, &serde_json::json!("1000000"));
    let ins = instruction(&row["op"]);
    let expected_run = rows(&row["runs"])
        .iter()
        .find(|r| r[0] == "1000000")
        .unwrap();
    let expected = &fixture["results"][expected_run[1].as_u64().unwrap() as usize];
    let exec = Executor {
        context: &ctx,
        order: &order,
        overrides: &overrides,
        contracts: &contracts,
    };
    let mut exhausted = 0;
    let mut passed = 0;
    let budgets = [0, 8, 16, 32, 64, 128, 256, 512, 1024, 4096, 1000000]
        .into_iter()
        .map(|max_nodes| Limits {
            max_nodes,
            ..Limits::default()
        })
        .chain([32, 128, 1024, 4096].into_iter().map(|max_bytes| Limits {
            max_bytes,
            ..Limits::default()
        }));
    for limits in budgets {
        match exec.step(&input, &ins, limits) {
            Err(Error::Data(ae_vm_fate::Error::ResourceLimit)) => exhausted += 1,
            Ok(out @ Outcome::Continue { .. }) => {
                if let Outcome::Continue {
                    remaining_limits, ..
                } = &out
                {
                    assert!(remaining_limits.max_nodes < limits.max_nodes);
                    assert!(remaining_limits.max_bytes < limits.max_bytes);
                }
                passed += 1;
                assert_outcome(out, expected);
            }
            other => panic!("unexpected result at budget {limits:?}: {other:?}"),
        }
        assert!(!input.stores.has_contract(&KEY));
        assert_state(&input, &row["initial"]);
    }
    assert!(exhausted > 0 && passed > 0);
    assert_outcome(
        exec.step(&input, &ins, Limits::default()).unwrap(),
        expected,
    );
}

#[test]
fn signed_runtime_variant_arities_preserve_inference_order_hash_and_wire_rejection() {
    let order = otp_order::profile(&NUMBERS["profile"]);
    for row in rows(&NUMBERS["variants"]) {
        let v = value(&row["value"]);
        let encoded = ae_vm_fate::encoding::encode(&v, Limits::default());
        assert_ne!(encoded, Err(ae_vm_fate::Error::ResourceLimit));
        assert_eq!(
            encoded.ok(),
            row["encoded"].as_str().map(|s| hex::decode(s).unwrap())
        );
        assert_eq!(
            ae_vm_fate::types::infer(&v, Limits::default()).unwrap(),
            values::ty(&row["inferred"])
        );
        assert_eq!(
            order.hash(&v, Limits::default()).unwrap(),
            row["hash"].as_u64().unwrap() as u32
        );
        for (j, other) in rows(&NUMBERS["variants"]).iter().enumerate() {
            let w = value(&other["value"]);
            assert_eq!(
                ae_vm_fate::value::compare(&v, &w, Limits::default())
                    .unwrap()
                    .is_lt(),
                row["comparisons"][j][0]
            );
            assert_eq!(
                ae_vm_fate::maps::order::compare(&v, &w, Limits::default())
                    .unwrap()
                    .is_lt(),
                row["comparisons"][j][1]
            );
        }
    }
}

#[test]
fn expanding_arithmetic_and_long_powers_stop_operationally_and_retry_unchanged_inputs() {
    let order = otp_order::profile(&NUMBERS["profile"]);
    let overrides = Overrides::default();
    let contracts = BTreeMap::new();
    for (opcode, left, right, limits) in [
        (
            "BSL",
            BigInt::from(17),
            BigInt::from(1024),
            Limits {
                max_bytes: 128,
                ..Limits::default()
            },
        ),
        (
            "MUL",
            BigInt::from(1) << 256,
            BigInt::from(1) << 256,
            Limits {
                max_bytes: 200,
                ..Limits::default()
            },
        ),
        (
            "POW",
            BigInt::from(17),
            BigInt::from(255),
            Limits {
                max_bytes: 128,
                ..Limits::default()
            },
        ),
        (
            "POW",
            BigInt::from(1),
            BigInt::from(1) << 80,
            Limits {
                max_nodes: 40,
                ..Limits::default()
            },
        ),
    ] {
        let row = rows(&NUMBERS["cases"])
            .iter()
            .find(|r| {
                r["protocol"] == 5
                    && r["op"][0] == opcode
                    && value(&r["initial"]["top"]) == Value::Integer(left.clone())
                    && value(&r["initial"]["rest"][0]) == Value::Integer(right.clone())
            })
            .unwrap();
        let input = state(row, &row["initial"]["gas"]);
        let ctx = context(row);
        let executor = Executor {
            context: &ctx,
            order: &order,
            overrides: &overrides,
            contracts: &contracts,
        };
        let ins = instruction(&row["op"]);
        assert!(
            matches!(
                executor.step(&input, &ins, limits),
                Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
            ),
            "{opcode}"
        );
        assert_state(&input, &row["initial"]);
        let high = rows(&row["runs"])
            .iter()
            .find(|r| r[0] == row["initial"]["gas"])
            .unwrap();
        assert_outcome(
            executor.step(&input, &ins, Limits::default()).unwrap(),
            &NUMBERS["results"][high[1].as_u64().unwrap() as usize],
        );
    }
}

#[test]
fn binary_and_unicode_expansion_limits_preserve_inputs_for_retry() {
    let order = otp_order::profile(&BINARY["profile"]);
    let overrides = Overrides::default();
    let contracts = BTreeMap::new();
    for (opcode, operand, second, limits) in [
        (
            "ADDR_TO_STR",
            Value::Object(
                ae_vm_fate::value::ObjectKind::Address,
                ae_vm_fate::value::ObjectPayload::Bytes(vec![255; 33]),
            ),
            None,
            Limits {
                max_nodes: 1000,
                ..Limits::default()
            },
        ),
        (
            "INT_TO_STR",
            Value::Integer(BigInt::from(1) << 256),
            None,
            Limits {
                max_nodes: 64,
                ..Limits::default()
            },
        ),
        (
            "INT_TO_BYTES",
            Value::Integer((-1).into()),
            Some(Value::Integer(128.into())),
            Limits {
                max_bytes: 100,
                ..Limits::default()
            },
        ),
        (
            "STR_TO_LIST",
            Value::String(b"123456789abcdefgH".to_vec()),
            None,
            Limits {
                max_bytes: 200,
                ..Limits::default()
            },
        ),
        (
            "STR_TO_UPPER",
            Value::String("ßİΣﬃ".as_bytes().to_vec()),
            None,
            Limits {
                max_nodes: 24,
                ..Limits::default()
            },
        ),
    ] {
        let row = rows(&BINARY["cases"])
            .iter()
            .find(|r| {
                r["protocol"] == 8
                    && (r["vm"] == 8 || opcode == "INT_TO_STR")
                    && r["op"][0] == opcode
                    && value(&r["initial"]["top"]) == operand
                    && second
                        .as_ref()
                        .is_none_or(|v| value(&r["initial"]["rest"][0]) == *v)
            })
            .unwrap();
        let input = state(row, &row["initial"]["gas"]);
        let ctx = context(row);
        let executor = Executor {
            context: &ctx,
            order: &order,
            overrides: &overrides,
            contracts: &contracts,
        };
        let ins = instruction(&row["op"]);
        assert!(
            matches!(
                executor.step(&input, &ins, limits),
                Err(Error::Data(ae_vm_fate::Error::ResourceLimit))
            ),
            "{opcode}"
        );
        assert_state(&input, &row["initial"]);
        let high = rows(&row["runs"])
            .iter()
            .find(|r| r[0] == row["initial"]["gas"])
            .unwrap();
        let expected = &BINARY["results"][high[1].as_u64().unwrap() as usize];
        assert_eq!(expected["kind"], "ok");
        assert_outcome(
            executor.step(&input, &ins, Limits::default()).unwrap(),
            expected,
        );
    }
}

fn large_value(j: &J) -> Value {
    match string(&j[0]) {
        "filled_bytes" => Value::Bytes(vec![
            j[2].as_u64().unwrap() as u8;
            j[1].as_u64().unwrap() as usize
        ]),
        "padded_bytes" => {
            let mut bytes = vec![0; j[1].as_u64().unwrap() as usize];
            *bytes.last_mut().unwrap() = j[2].as_u64().unwrap() as u8;
            Value::Bytes(bytes)
        }
        "power" => Value::Integer(BigInt::from(1) << j[1].as_u64().unwrap() as usize),
        "ones" => Value::Integer((BigInt::from(1) << j[1].as_u64().unwrap() as usize) - 1),
        "negative" => {
            let Value::Integer(n) = large_value(&j[1]) else {
                unreachable!()
            };
            Value::Integer(-n)
        }
        "bits" => {
            let Value::Integer(n) = large_value(&j[1]) else {
                unreachable!()
            };
            Value::Bits(n)
        }
        _ => value(j),
    }
}
fn assert_compact(v: &Value, j: &J) {
    match (v, string(&j[0])) {
        (Value::Bytes(b), "bytes_digest") => {
            assert_eq!(b.len() as u64, j[1]);
            assert_eq!(hex::encode(ae_crypto::sha256(b).0), string(&j[2]));
        }
        (Value::Integer(n), "integer_digest") => {
            let b = if n == &BigInt::from(0) {
                vec![0]
            } else {
                n.magnitude().to_bytes_be()
            };
            assert_eq!(n.sign() == num_bigint::Sign::Minus, j[1]);
            assert_eq!(b.len() as u64, j[2]);
            assert_eq!(hex::encode(ae_crypto::sha256(&b).0), string(&j[3]));
        }
        (Value::Bits(n), "bits") => assert_compact(&Value::Integer(n.clone()), &j[1]),
        _ => assert_eq!(*v, value(j)),
    }
}
#[test]
fn native_integer_capacity_and_bitwise_nil_results_match_original_runtime() {
    check_native_cases(&NATIVE_LIMITS);
}
#[test]
fn byte_integer_native_capacity_and_zero_padding_match_original_runtime() {
    check_native_cases(&BINARY_LIMITS);
}
fn check_native_cases(fixture: &J) {
    let order = otp_order::profile(&fixture["profile"]);
    let overrides = Overrides::default();
    let contracts = BTreeMap::new();
    for (i, row) in rows(&fixture["cases"]).iter().enumerate() {
        let mut input = state(&FIXTURE["cases"][0], &FIXTURE["cases"][0]["initial"]["gas"]);
        input.meter.gas = if row["gas"].is_string() {
            number(&row["gas"])
        } else {
            let Value::Integer(n) = large_value(&row["gas"]) else {
                unreachable!()
            };
            n
        };
        input.meter.protocol = row["protocol"].as_u64().unwrap() as u32;
        input.stores = Stores::new(Version::for_protocol(input.meter.protocol));
        input.stores.put_contract(KEY, backend());
        let args: Vec<_> = rows(&row["arguments"]).iter().map(large_value).collect();
        input.accumulator = Accumulator {
            top: args[0].clone(),
            rest: args[1..]
                .iter()
                .cloned()
                .chain([Value::Integer(42.into())])
                .collect(),
        };
        let mut ctx = context(&FIXTURE["cases"][0]);
        ctx.vm_version = VmVersion::Three;
        ctx.arithmetic_word_size = match fixture["word_size"].as_u64().unwrap() {
            64 => ae_vm_fate::maps::order::WordSize::Bits64,
            32 => ae_vm_fate::maps::order::WordSize::Bits32,
            _ => unreachable!(),
        };
        let opcode = *Opcode::ALL
            .iter()
            .find(|op| op.info().name == string(&row["opcode"]))
            .unwrap();
        let instruction = Instruction {
            opcode,
            operands: vec![Operand::Stack; opcode.info().arity],
        };
        let executor = Executor {
            context: &ctx,
            order: &order,
            overrides: &overrides,
            contracts: &contracts,
        };
        let out = executor
            .step(&input, &instruction, Limits::default())
            .unwrap();
        let expected = &row["result"];
        let (kind, state) = match out {
            Outcome::Continue { state, control, .. } => {
                assert_eq!(control, Control::Next);
                ("ok", Some(state))
            }
            Outcome::Abort { state, reason, .. } => {
                assert_reason(&reason, &expected["reason"]);
                ("abort", Some(state))
            }
            Outcome::Exception(Exception::Native(e)) => {
                assert_eq!(e, string(&expected["reason"]), "case {i}: {row}");
                ("exception", None)
            }
            other => panic!("case {i}: unexpected {other:?}"),
        };
        assert_eq!(kind, string(&expected["kind"]), "case {i}: {row}");
        if let Some(st) = state {
            let e = &expected["state"];
            assert_compact(&st.accumulator.top, &e["top"]);
            assert_eq!(st.accumulator.rest.len(), rows(&e["rest"]).len());
            for (v, j) in st.accumulator.rest.iter().zip(rows(&e["rest"])) {
                assert_compact(v, j);
            }
            for (n, j) in [
                (&st.meter.gas, &e["gas"]),
                (&st.meter.created_cells, &e["cells"]),
            ] {
                if j.is_string() {
                    assert_eq!(*n, number(j));
                } else {
                    assert_compact(&Value::Integer(n.clone()), j);
                }
            }
            assert_eq!(e["memory_unchanged"], true);
            assert_eq!(st.memory, input.memory);
            assert_eq!(e["stores_unchanged"], true);
            let st = st.stores.entry(&KEY).unwrap();
            let before = input.stores.entry(&KEY).unwrap();
            assert_eq!(st.terms(), before.terms());
            assert_eq!(st.dirty(), before.dirty());
            assert_eq!(st.backend().0.read_cache(), before.backend().0.read_cache());
            assert_eq!(
                st.backend().0.write_cache(),
                before.backend().0.write_cache()
            );
        }
    }
}

#[test]
fn native_overflow_bits_term_preserves_helper_acceptance_and_rejection() {
    let row = &NATIVE_LIMITS["bits_nil"];
    let v = value(&row["value"]);
    assert_eq!(v, Value::BitsNil);
    let order = otp_order::profile(&NATIVE_LIMITS["profile"]);
    assert!(row["encoded"].is_null());
    assert_eq!(
        ae_vm_fate::encoding::encode(&v, Limits::default()),
        Err(ae_vm_fate::Error::InvalidValue)
    );
    assert!(row["inferred"].is_null());
    assert_eq!(
        ae_vm_fate::types::infer(&v, Limits::default()),
        Err(ae_vm_fate::Error::InvalidValue)
    );
    assert_eq!(
        order.hash(&v, Limits::default()).unwrap(),
        row["hash"].as_u64().unwrap() as u32
    );
    assert_eq!(value(&row["refcount"]), Value::Map(BTreeMap::new()));
    assert!(
        ae_vm_fate::maps::refcount(&v, Limits::default())
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        ae_vm_fate::maps::unfold(
            v.clone(),
            &order,
            |_: &BigInt| Ok::<_, ()>(BTreeMap::new()),
            Limits::default()
        )
        .unwrap(),
        value(&row["unfold"])
    );
    for r in rows(&row["traversal"]) {
        use ae_vm_fate::gas::CostModel::*;
        let model = match string(&r["model"]) {
            "simple" => Simple,
            "serial" => Serial,
            "unfold" => Unfold,
            "unfold_compare" => UnfoldCompare,
            "unfold_serial" => UnfoldSerial,
            "final" => Final,
            _ => unreachable!(),
        };
        let actual = ae_vm_fate::gas::traversal(
            r["protocol"].as_u64().unwrap() as u32,
            model,
            &v,
            1000.into(),
            &order,
            Limits::default(),
        );
        assert_eq!(
            actual.ok().map(|r| r.gas),
            r["gas"].as_str().map(|s| s.parse().unwrap())
        );
    }
}
