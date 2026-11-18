use ae_vm_fate::{
    Error, Limits,
    gas::{self, CostModel, Meter, UnfoldHost},
    maps::{MapOrder, Traversal},
    value::Value,
};
use num_bigint::BigInt;
use serde_json::{Value as J, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::LazyLock,
};
#[path = "support/values.rs"]
mod values;
use values::{rows, string, value};
static FIXTURE: LazyLock<J> =
    LazyLock::new(|| serde_json::from_str(include_str!("fixtures/erlang_fate_gas.json")).unwrap());
struct ReferenceOrder<'a>(&'a J);
impl MapOrder for ReferenceOrder<'_> {
    fn indices(&self, keys: &[&Value], mode: Traversal, _: Limits) -> Result<Vec<usize>, Error> {
        assert_eq!(mode, Traversal::Fold);
        if keys.len() <= 1 {
            return Ok((0..keys.len()).collect());
        }
        let set: BTreeSet<_> = keys.iter().copied().collect();
        for row in rows(self.0) {
            let expected: Vec<_> = rows(row).iter().map(value).collect();
            if expected.iter().collect::<BTreeSet<_>>() == set {
                return Ok(expected
                    .iter()
                    .map(|k| keys.iter().position(|v| *v == k).unwrap())
                    .collect());
            }
        }
        panic!("missing reference order {keys:?}");
    }
}
fn model(v: &J) -> CostModel {
    match string(v) {
        "simple" => CostModel::Simple,
        "serial" => CostModel::Serial,
        "unfold" => CostModel::Unfold,
        "unfold_compare" => CostModel::UnfoldCompare,
        "unfold_serial" => CostModel::UnfoldSerial,
        "final" => CostModel::Final,
        _ => unreachable!(),
    }
}
struct Host<'a> {
    mode: &'a str,
    store: &'a BTreeMap<BigInt, Value>,
    trace: Vec<J>,
}
impl UnfoldHost for Host<'_> {
    type Error = ();
    fn map_size(&mut self, id: &BigInt, _: Limits) -> Result<BigInt, ()> {
        self.trace.push(json!(["size", id.to_string()]));
        Ok(match self.mode {
            "dummy" => 0.into(),
            "negative" => (-1).into(),
            "large" => BigInt::from(1) << 200,
            "normal" => {
                let Value::Map(m) = self.store.get(id).ok_or(())? else {
                    return Err(());
                };
                m.len().into()
            }
            _ => unreachable!(),
        })
    }
    fn unfold(&mut self, id: &BigInt, _: Limits) -> Result<Value, ()> {
        self.trace.push(json!(["unfold", id.to_string()]));
        if self.mode == "dummy" {
            Ok(Value::Tuple(vec![]))
        } else {
            self.store.get(id).cloned().ok_or(())
        }
    }
}
fn limits() -> Limits {
    Limits {
        max_depth: 512,
        ..Limits::default()
    }
}
fn compare(case: &J, mode: &str, store: &BTreeMap<BigInt, Value>) {
    let v = value(&case["value"]);
    let order = ReferenceOrder(&case["orders"]);
    for m in rows(&case["models"]) {
        for (protocol, runs) in std::iter::once((None, &m["raw"])).chain(
            rows(&m["protocols"])
                .iter()
                .map(|r| (Some(r[0].as_u64().unwrap() as u32), &r[1])),
        ) {
            for r in rows(runs) {
                let mut host = Host {
                    mode,
                    store,
                    trace: vec![],
                };
                let gas = string(&r[0]).parse().unwrap();
                let result = match protocol {
                    None => {
                        gas::raw_traversal(model(&m["model"]), &v, gas, &mut host, &order, limits())
                    }
                    Some(p) => gas::traversal_with_host(
                        p,
                        model(&m["model"]),
                        &v,
                        gas,
                        &mut host,
                        &order,
                        limits(),
                    ),
                };
                let actual = match result {
                    Ok(c) => c.gas.to_string(),
                    Err(gas::Error::OutOfGas) => "out_of_gas".into(),
                    Err(gas::Error::Data(Error::ResourceLimit) | gas::Error::InvalidOrder) => {
                        panic!("unexpected operational failure")
                    }
                    Err(_) => "exception".into(),
                };
                assert_eq!(
                    actual,
                    string(&r[1]),
                    "{mode} {protocol:?} {} gas={} {v:?}",
                    m["model"],
                    r[0]
                );
                assert_eq!(
                    host.trace,
                    rows(&r[2]),
                    "callback order {mode} {protocol:?} {} gas={} {v:?}",
                    m["model"],
                    r[0]
                );
            }
        }
    }
}
#[test]
fn traversal_milligas_rounding_historical_gates_and_unfold_callbacks_match_erlang() {
    for case in rows(&FIXTURE["traversal"]) {
        compare(case, "dummy", &BTreeMap::new());
    }
    for h in rows(&FIXTURE["hosts"]) {
        let store = rows(&h["store"])
            .iter()
            .map(|r| (string(&r[0]).parse().unwrap(), value(&r[1])))
            .collect();
        for case in rows(&h["values"]) {
            compare(case, string(&h["name"]), &store);
        }
    }
}
#[test]
fn cell_store_value_and_dry_run_floor_metering_match_erlang() {
    for c in rows(&FIXTURE["primitives"]) {
        for r in rows(&c["runs"]) {
            let mut meter = Meter {
                gas: string(&r[0]).parse().unwrap(),
                created_cells: string(&c["cells"]).parse().unwrap(),
                protocol: c["protocol"].as_u64().unwrap() as u32,
                metering_floor: c["floor"].as_u64().map(|p| p as u32),
            };
            let before = meter.clone();
            let n = string(&c["argument"]).parse().unwrap();
            let result = match string(&c["kind"]) {
                "cells" => meter.new_cells(n),
                "values" => meter.store_values(n),
                "floor" => meter.store_read_floor(n),
                "spend" => meter.spend(n),
                _ => unreachable!(),
            };
            let actual = match result {
                Ok(()) => json!([meter.gas.to_string(), meter.created_cells.to_string()]),
                Err(gas::Error::OutOfGas) => {
                    assert_eq!(meter, before);
                    json!("out_of_gas")
                }
                Err(e) => panic!("unexpected {e:?}"),
            };
            assert_eq!(actual, r[1], "{c} gas={}", r[0]);
        }
    }
}
#[test]
fn operational_traversal_limits_bound_recursion_without_changing_early_protocol_gates() {
    let order = ReferenceOrder(&J::Null);
    let no_work = Limits {
        max_nodes: 0,
        max_bytes: 0,
        max_depth: 0,
    };
    let malformed = Value::ContractBytearray(vec![0; 100]);
    let result = gas::traversal(
        4,
        CostModel::Final,
        &malformed,
        (-1).into(),
        &order,
        no_work,
    )
    .unwrap();
    assert_eq!(result.gas, BigInt::from(-1));
    assert_eq!(result.remaining_limits.max_nodes, 0);
    assert!(matches!(
        gas::traversal(
            5,
            CostModel::Simple,
            &Value::Boolean(true),
            100.into(),
            &order,
            no_work
        ),
        Err(gas::Error::Data(Error::ResourceLimit))
    ));
    let bytes = Value::String(vec![0; 10]);
    let bound = Limits {
        max_bytes: 9,
        ..limits()
    };
    assert!(matches!(
        gas::traversal(5, CostModel::Serial, &bytes, 100.into(), &order, bound),
        Err(gas::Error::Data(Error::ResourceLimit))
    ));
    assert!(gas::traversal(5, CostModel::Simple, &bytes, 100.into(), &order, bound).is_ok());
    let sm = Value::StoreMap {
        id: 0.into(),
        cache: Default::default(),
    };
    let store = [(0.into(), sm.clone())].into();
    let mut host = Host {
        mode: "negative",
        store: &store,
        trace: vec![],
    };
    assert!(matches!(
        gas::traversal_with_host(
            5,
            CostModel::Unfold,
            &sm,
            100.into(),
            &mut host,
            &order,
            Limits {
                max_depth: 8,
                ..limits()
            }
        ),
        Err(gas::Error::Data(Error::ResourceLimit))
    ));
    assert!(host.trace.len() <= 18);
}
