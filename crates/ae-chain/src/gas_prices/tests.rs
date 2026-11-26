use super::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/erlang_gas_observations.json"
    ))
    .unwrap()
}
pub(super) fn number(v: &Value) -> BigInt {
    v.as_number().unwrap().as_str().parse().unwrap()
}
fn stats(v: &Value) -> Stats {
    Stats {
        minimum: (!v["minimum"].is_null()).then(|| number(&v["minimum"])),
        used: number(&v["used"]),
        total: number(&v["total"]),
    }
}
fn output(row: &Summary) -> Value {
    json!({"minutes":row.minutes,"min_gas_price":serde_json::from_str::<Value>(&row.minimum.to_string()).unwrap(),
        "utilization":serde_json::from_str::<Value>(&row.utilization.to_string()).unwrap()})
}
fn hash(id: &Value) -> Hash256 {
    let mut h = [0; 32];
    h[24..].copy_from_slice(&id.as_i64().unwrap().to_be_bytes());
    Hash256(h)
}
struct Inputs {
    headers: BTreeMap<Hash256, Value>,
    reads: usize,
    stats: usize,
}
impl Source for Inputs {
    fn header(&mut self, h: Hash256) -> Result<Option<Header>, Error> {
        self.reads += 1;
        Ok(self.headers.get(&h).map(|r| Header {
            previous: hash(&r["previous"]),
            time: r["time"].as_u64().unwrap(),
            protocol: r["protocol"].as_u64().unwrap() as u32,
            micro: r["kind"] == "micro",
        }))
    }
    fn stats(&mut self, h: Hash256, _: u32) -> Result<Stats, Error> {
        self.stats += 1;
        Ok(stats(&self.headers[&h]))
    }
}
fn inputs(row: &Value) -> Inputs {
    Inputs {
        headers: row["headers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| (hash(&h["id"]), h.clone()))
            .collect(),
        reads: 0,
        stats: 0,
    }
}
#[test]
fn original_gas_utilization_float_rounding_and_overflow() {
    let f = fixture();
    assert_eq!(f["arithmetic"].as_array().unwrap().len(), 162);
    for r in f["arithmetic"].as_array().unwrap() {
        match stats(r).summary(1) {
            Ok(v) => {
                assert_eq!(r["result"]["kind"], "ok", "{r}");
                assert_eq!(output(&v), r["result"]["value"], "{r}");
            }
            Err(Error::Arithmetic) => assert_eq!(r["result"]["reason"], "badarith", "{r}"),
            e => panic!("{r}: {e:?}"),
        }
    }
}
#[test]
fn original_recent_windows_cutoffs_keys_empty_micros_and_end_of_history() {
    let f = fixture();
    assert_eq!(f["windows"].as_array().unwrap().len(), 111);
    for r in f["windows"].as_array().unwrap() {
        let result = recent(
            &mut inputs(r),
            hash(&r["top"]),
            r["now"].as_i64().unwrap().into(),
            Limits::default(),
            || Ok(()),
        );
        match result {
            Ok(rows) => assert_eq!(
                Value::Array(rows.iter().map(output).collect()),
                r["result"]["value"],
                "{r}"
            ),
            Err(Error::MissingHeader(_)) => {
                assert_eq!(r["result"]["value"]["error"], "block_not_found", "{r}")
            }
            e => panic!("{r}: {e:?}"),
        }
    }
}
#[test]
fn bounded_observations_reject_partial_results_and_allow_cooperative_cancellation() {
    let f = fixture();
    let r = &f["windows"][0];
    let top = hash(&r["top"]);
    let now = r["now"].as_i64().unwrap().into();
    let mut source = inputs(r);
    assert!(matches!(
        recent(
            &mut source,
            top,
            now,
            Limits {
                steps: 0,
                ..Default::default()
            },
            || Ok(())
        ),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(source.reads, 0);
    let mut checks = 0;
    assert!(matches!(
        recent(&mut source, top, now, Limits::default(), || {
            checks += 1;
            Err(Error::Backend(crate::repository::Error::PolicyClosed))
        }),
        Err(Error::Backend(crate::repository::Error::PolicyClosed))
    ));
    assert_eq!(checks, 1);
    assert_eq!(source.reads, 0);
    assert!(matches!(
        recent(
            &mut source,
            top,
            now,
            Limits {
                integer_bytes: 0,
                ..Default::default()
            },
            || Ok(())
        ),
        Err(Error::ResourceLimit)
    ));
    let mut source = inputs(r);
    let header = source.headers.get_mut(&top).unwrap();
    header["previous"] = r["top"].clone();
    assert!(matches!(
        recent(
            &mut source,
            top,
            now,
            Limits {
                steps: 5,
                ..Default::default()
            },
            || Ok(())
        ),
        Err(Error::ResourceLimit)
    ));
    assert_eq!(source.reads, 5);
}
