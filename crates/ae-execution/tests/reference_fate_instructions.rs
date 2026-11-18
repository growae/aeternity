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
static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_instructions.json")).unwrap()
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
    stores.put_contract(KEY, backend());
    for term in rows(&j["store"]["terms"]) {
        assert_eq!(term[2], true);
        stores
            .put_value(&KEY, number(&term[0]), value(&term[1]))
            .unwrap();
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
        Reason::OutOfGas => "out_of_gas",
        Reason::EmptyStack => "empty_stack",
        Reason::MissingMapKey => "missing_map_key",
        Reason::HeadEmpty => "head_empty",
        Reason::TailEmpty => "tail_empty",
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
        Reason::Unavailable(op) => {
            assert_eq!(*op, string(&expected[1]));
            "unavailable"
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
                    Exception::DetachedDuplicateIndex => "dup",
                    Exception::StoreData => "store_data",
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
    let order = otp_order::profile(&FIXTURE["profile"]);
    let contracts = BTreeMap::new();
    for (i, row) in rows(&FIXTURE["cases"]).iter().enumerate() {
        let overrides = overrides(row);
        let ctx = context(row);
        let executor = Executor {
            context: &ctx,
            order: &order,
            overrides: &overrides,
            contracts: &contracts,
        };
        let ins = instruction(&row["op"]);
        for run in rows(&row["runs"]) {
            let expected = &FIXTURE["results"][run[1].as_u64().unwrap() as usize];
            let result = std::panic::catch_unwind(|| {
                let initial = state(row, &run[0]);
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
fn chained_instructions_reuse_dirty_registers_and_store_caches() {
    let order = otp_order::profile(&FIXTURE["profile"]);
    let overrides = Overrides::default();
    let contracts = BTreeMap::new();
    for row in rows(&FIXTURE["sequences"]) {
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
            assert_outcome(out, &FIXTURE["results"][step[1].as_u64().unwrap() as usize]);
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
    let add = Instruction {
        opcode: Opcode::Add,
        operands: vec![
            Operand::Stack,
            Operand::Immediate(Value::Integer(1.into())),
            Operand::Immediate(Value::Integer(2.into())),
        ],
    };
    assert!(matches!(
        executor.step(&input, &add, Limits::default()),
        Err(Error::Unsupported(Opcode::Add))
    ));
    assert_state(&input, &row["initial"]);
}
