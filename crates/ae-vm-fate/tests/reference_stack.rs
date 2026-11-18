use ae_vm_fate::{
    Error, Limits, encoding,
    gas::Meter,
    maps,
    stack::{Accumulator, Error as StackError},
    value::Value,
};
use serde_json::Value as J;
use std::sync::LazyLock;
#[path = "support/otp_order.rs"]
mod otp_order;
#[path = "support/values.rs"]
mod values;
use values::{rows, string, value};
static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    serde_json::from_str(include_str!("fixtures/erlang_fate_stack.json")).unwrap()
});
fn state(row: &J, protocol: u32) -> (Accumulator, Meter) {
    (
        Accumulator {
            top: value(&row["top"]),
            rest: rows(&row["rest"]).iter().map(value).collect(),
        },
        Meter {
            protocol,
            gas: string(&row["gas"]).parse().unwrap(),
            created_cells: string(&row["cells"]).parse().unwrap(),
            metering_floor: None,
        },
    )
}
#[test]
fn accumulator_results_and_abort_states_match_original_engine() {
    for (i, row) in rows(&FIXTURE["cases"]).iter().enumerate() {
        let protocol = row["protocol"].as_u64().unwrap() as u32;
        for run in rows(&row["operations"]) {
            let (mut stack, mut meter) = state(&row["initial"], protocol);
            let op = &run[0];
            let expected = &FIXTURE["results"][run[1].as_u64().unwrap() as usize];
            let result = match string(&op[0]) {
                "pop" => stack.pop(&mut meter).map(Some),
                "push" => stack
                    .push(value(&op[1]), &mut meter, Limits::default())
                    .map(|_| None),
                "dup" => stack
                    .duplicate(&value(&op[1]), &mut meter, Limits::default())
                    .map(|_| None),
                "pop_n" => stack
                    .pop_n(
                        op[1].as_u64().unwrap() as usize,
                        &mut meter,
                        Limits::default(),
                    )
                    .map(|vs| Some(Value::List(vs))),
                "drop" => {
                    stack.drop_n(op[1].as_u64().unwrap() as usize, &mut meter);
                    Ok(None)
                }
                _ => unreachable!(),
            };
            let actual = match result {
                Ok(v) => {
                    assert_eq!(
                        v,
                        expected["value"]
                            .is_array()
                            .then(|| value(&expected["value"])),
                        "case {i}: {op}"
                    );
                    "ok"
                }
                Err(StackError::OutOfGas) => {
                    // Reference states are captured after runtime_error/3.
                    // Every case has an empty call stack, so recovered gas is 0.
                    meter.gas = 0.into();
                    "out_of_gas"
                }
                Err(StackError::EmptyStack) => {
                    meter.gas = 0.into();
                    "empty_stack"
                }
                Err(StackError::InvalidDuplicateIndex) => "invalid_duplicate_index",
                Err(StackError::ReferenceException(reason)) => {
                    assert_eq!(reason, string(&expected["reason"]), "case {i}: {op}");
                    "exception"
                }
                Err(e) => panic!("case {i}: {op}: {e:?}"),
            };
            assert_eq!(actual, string(&expected["kind"]), "case {i}: {op}");
            if expected["state"].is_object() {
                let (expected_stack, expected_meter) = state(&expected["state"], protocol);
                assert_eq!(stack, expected_stack, "case {i}: {op}");
                assert_eq!(meter.gas, expected_meter.gas, "case {i}: {op}");
                assert_eq!(
                    meter.created_cells, expected_meter.created_cells,
                    "case {i}: {op}"
                );
            }
        }
    }
}
#[test]
fn void_has_runtime_hashes_but_no_wire_or_persistent_map_value_representation() {
    let profile = otp_order::profile(&FIXTURE["profile"]);
    for row in rows(&FIXTURE["void_hashes"]) {
        assert_eq!(
            profile
                .hash(&value(&row["value"]), Limits::default())
                .unwrap(),
            row["hash"].as_u64().unwrap() as u32
        );
    }
    for row in rows(&FIXTURE["void_rejections"]) {
        for key in ["encode", "refcount", "allocate", "unfold", "infer"] {
            assert_eq!(row[key], false);
        }
        let v = value(&row["value"]);
        assert_eq!(
            ae_vm_fate::types::infer(&v, Limits::default()),
            Err(Error::InvalidValue)
        );
        for protocol in 1..=8 {
            let admitted = ae_vm_fate::gas::traversal(
                protocol,
                ae_vm_fate::gas::CostModel::Simple,
                &v,
                1_000_000.into(),
                &profile,
                Limits::default(),
            )
            .is_ok();
            assert_eq!(
                admitted,
                row["traversal"][(protocol - 1) as usize].as_bool().unwrap()
            );
        }
        assert_eq!(
            encoding::encode(&v, Limits::default()),
            Err(Error::InvalidValue)
        );
        assert_eq!(
            maps::refcount(&v, Limits::default()),
            Err(Error::InvalidValue)
        );
        assert!(matches!(
            maps::allocate(vec![], vec![v.clone()], &profile, Limits::default()),
            Err(maps::MapError::Data(Error::InvalidValue))
        ));
        assert!(matches!(
            maps::unfold(
                v,
                &profile,
                |_: &num_bigint::BigInt| Ok::<_, ()>(Default::default()),
                Limits::default()
            ),
            Err(maps::MapError::Data(Error::InvalidValue))
        ));
    }
    let old: J = serde_json::from_str(include_str!("fixtures/erlang_fate_order_0.json")).unwrap();
    assert_eq!(
        otp_order::profile(&old).hash(&Value::Void, Limits::default()),
        Err(Error::InvalidOrderProfile)
    );
}
#[test]
fn resource_aborts_leave_stack_and_meter_available_for_retry() {
    let mut stack = Accumulator::default();
    let mut meter = Meter {
        protocol: 5,
        gas: 100.into(),
        created_cells: 0.into(),
        metering_floor: None,
    };
    let value = Value::Bytes(vec![7; 128]);
    assert_eq!(
        stack.push(
            value.clone(),
            &mut meter,
            Limits {
                max_bytes: 127,
                ..Limits::default()
            }
        ),
        Err(StackError::Data(Error::ResourceLimit))
    );
    assert_eq!(stack, Accumulator::default());
    assert_eq!(meter.gas, 100.into());
    stack.push(value, &mut meter, Limits::default()).unwrap();
    let before = stack.clone();
    let gas = meter.gas.clone();
    assert_eq!(
        stack.duplicate(
            &Value::Integer(0.into()),
            &mut meter,
            Limits {
                max_bytes: 255,
                ..Limits::default()
            }
        ),
        Err(StackError::Data(Error::ResourceLimit))
    );
    assert_eq!(stack, before);
    assert_eq!(meter.gas, gas);
    stack
        .duplicate(
            &Value::Integer(0.into()),
            &mut meter,
            Limits {
                max_bytes: 256,
                ..Limits::default()
            },
        )
        .unwrap();
    let before = stack.clone();
    let cells = meter.created_cells.clone();
    assert_eq!(
        stack.pop_n(
            3,
            &mut meter,
            Limits {
                max_nodes: 2,
                ..Limits::default()
            }
        ),
        Err(StackError::Data(Error::ResourceLimit))
    );
    assert_eq!(stack, before);
    assert_eq!(meter.created_cells, cells);
    assert_eq!(
        stack.pop_n(3, &mut meter, Limits::default()).unwrap().len(),
        3
    );
}
