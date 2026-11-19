use ae_hyperchain::{parent_block::ParentBlock, parent_cache::*, parent_connector};
use ae_types::{Hash256, Unsigned};
use ae_vm_fate::{
    maps::{MapOrder, Traversal},
    value::Value,
};
use num_bigint::BigInt;
use serde_json::{Value as J, json};
use std::sync::LazyLock;
#[path = "../../ae-vm-fate/tests/support/otp_order.rs"]
mod otp_order;

fn unpack(s: &str) -> J {
    fn visit(v: &J, values: &[J]) -> J {
        match v {
            J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                visit(&values[v["$ref"].as_u64().unwrap() as usize], values)
            }
            J::Object(m) => J::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), visit(v, values)))
                    .collect(),
            ),
            J::Array(xs) => J::Array(xs.iter().map(|v| visit(v, values)).collect()),
            _ => v.clone(),
        }
    }
    let d: J = serde_json::from_str(s).unwrap();
    if d["format"] == "interned-json-v1" {
        visit(&d["root"], d["values"].as_array().unwrap())
    } else {
        d
    }
}
static CACHE: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("fixtures/erlang_hc_parent_cache.json")));
static CONNECTOR: LazyLock<J> =
    LazyLock::new(|| unpack(include_str!("fixtures/erlang_hc_parent_connector.json")));
fn n(v: &J) -> BigInt {
    v.as_str().unwrap().parse().unwrap()
}
fn u(v: &J) -> Unsigned {
    Unsigned::from_decimal(v.as_str().unwrap()).unwrap()
}
fn bytes(v: &J) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn block(v: &J) -> ParentBlock {
    ParentBlock {
        hash: bytes(&v["hash"]),
        height: u(&v["height"]),
        previous_hash: bytes(&v["previous_hash"]),
        time: u(&v["time"]),
    }
}
fn block_json(b: &ParentBlock) -> J {
    json!({"hash":hex::encode(&b.hash),"height":integer(&b.height).to_string(),"previous_hash":hex::encode(&b.previous_hash),"time":integer(&b.time).to_string()})
}
fn snapshot(c: &Cache) -> J {
    json!({"height":integer(&c.child_top().height).to_string(),"hash":hex::encode(c.child_top().hash.as_bytes()),"top":integer(c.top_height()).to_string(),
        "blocks":c.blocks().iter().map(|(h,e)| json!([h.to_string(),match e {Entry::Requested(t)=>json!({"requested":t.to_string()}),Entry::Block(b)=>json!({"block":block_json(b)})}])).collect::<Vec<_>>(),
        "index":c.hash_index().iter().map(|(h,n)| json!([hex::encode(h),n.to_string()])).collect::<Vec<_>>()})
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostError {
    Unavailable,
    Native,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct ScriptedHost {
    offsets: Vec<BigInt>,
    empty_second: bool,
    now: BigInt,
    fetch: FetchResult,
    effects: Vec<J>,
    retries: Vec<u64>,
    fail_request: Option<(usize, HostError)>,
}
impl Host for ScriptedHost {
    type Error = HostError;
    fn is_operational(e: &HostError) -> bool {
        *e == HostError::Unavailable
    }
    fn targets(&mut self, height: &Unsigned) -> Result<Vec<BigInt>, HostError> {
        let empty = self.empty_second && self.effects.iter().any(|e| e.get("target").is_some());
        self.effects
            .push(json!({"target":integer(height).to_string()}));
        Ok(if empty {
            vec![]
        } else {
            self.offsets.iter().map(|o| integer(height) + o).collect()
        })
    }
    fn now_ms(&mut self) -> BigInt {
        self.now.clone()
    }
    fn request(&mut self, height: &BigInt) -> Result<(), HostError> {
        if let Some((after, e)) = self.fail_request
            && self
                .effects
                .iter()
                .filter(|e| e.get("request").is_some())
                .count()
                >= after
        {
            return Err(e);
        }
        self.effects.push(json!({"request":height.to_string()}));
        Ok(())
    }
    fn fetch(&mut self, height: &BigInt) -> Result<FetchResult, HostError> {
        self.effects.push(json!({"fetch":height.to_string()}));
        Ok(self.fetch.clone())
    }
    fn retry_initialize(&mut self, after: u64) -> Result<(), HostError> {
        self.retries.push(after);
        Ok(())
    }
}
fn setup(row: &J) -> (Cache, ScriptedHost) {
    let c = &row["config"];
    let s = &row["initial"];
    let cache = Cache::new(
        Config {
            child_start_height: u(&c["start"]),
            max_size: u(&c["size"]),
            retry_interval: u(&c["retry"]),
            finality: u(&c["finality"]),
        },
        ChildTop {
            height: u(&s["height"]),
            hash: Hash256::try_from(bytes(&s["hash"]).as_slice()).unwrap(),
        },
    );
    let fetch = match c["fetch"].as_str() {
        Some("not_found") => FetchResult::NotFound,
        Some("no_parent_chain_agreement") => FetchResult::NoAgreement,
        Some("exited") => FetchResult::Exited,
        None => FetchResult::Block(block(&c["fetch"])),
        other => panic!("unexpected fetch {other:?}"),
    };
    let host = ScriptedHost {
        offsets: c["offsets"].as_array().unwrap().iter().map(n).collect(),
        empty_second: c["empty_second"].as_bool().unwrap(),
        now: 0.into(),
        fetch,
        effects: vec![],
        retries: vec![],
        fail_request: None,
    };
    (cache, host)
}
fn event(v: &J) -> Event {
    match v["kind"].as_str().unwrap() {
        "initialize" => Event::Initialize,
        "post" => Event::PostBlock(block(&v["block"])),
        "get" => Event::Get(n(&v["height"])),
        "child" => Event::ChildTop(ChildTop {
            height: u(&v["height"]),
            hash: Hash256::try_from(bytes(&v["hash"]).as_slice()).unwrap(),
        }),
        "ignore" => Event::Ignore,
        "unknown" => Event::UnknownCall,
        _ => unreachable!(),
    }
}
fn reply(r: Result<Reply, StepError<HostError>>) -> (&'static str, J) {
    match r {
        Ok(r) => (
            "ok",
            match r {
                Reply::None => json!("none"),
                Reply::Unhandled => json!("unhandled"),
                Reply::Block(Ok(b)) => block_json(&b),
                Reply::Block(Err(LookupError::NotInCache)) => json!("not_in_cache"),
                Reply::Block(Err(LookupError::NotFinal)) => json!("not_final"),
            },
        ),
        Err(StepError::Cache(Error::Native(e))) => (
            "native",
            json!(match e {
                Native::EmptyInitializeTargets => "empty_initialize",
                Native::EmptyMaximumTargets => "empty_maximum",
                Native::EmptyEviction => "empty_eviction",
            }),
        ),
        other => panic!("unexpected result {other:?}"),
    }
}
#[test]
fn original_cache_callbacks_match_complete_states_effects_and_failures() {
    let order = otp_order::profile(&CACHE["profile"]);
    let mut steps = 0;
    let mut retries = vec![];
    for row in CACHE["runs"].as_array().unwrap() {
        let (mut c, mut h) = setup(row);
        assert_eq!(snapshot(&c), row["initial"]);
        for (i, s) in row["steps"].as_array().unwrap().iter().enumerate() {
            h.now = n(&s["now"]);
            h.effects.clear();
            let (kind, r) = reply(c.step(event(&s["input"]), &mut h, &order, Limits::default()));
            assert_eq!(json!(kind), s["kind"], "{} {i}", row["name"]);
            assert_eq!(r, s["reply"], "{} {i}", row["name"]);
            assert_eq!(snapshot(&c), s["after_state"], "{} {i}", row["name"]);
            assert_eq!(json!(h.effects), s["effects"], "{} {i}", row["name"]);
            steps += 1;
        }
        retries.extend(h.retries);
    }
    assert_eq!(retries, [1000; 3]);
    assert_eq!(steps, 86);
}

#[test]
fn original_connector_quorum_uses_whole_blocks_and_configured_host_denominator() {
    use parent_connector::{Consensus, Response};
    for row in CONNECTOR["runs"].as_array().unwrap() {
        let responses: Vec<_> = row["responses"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| match r.as_str() {
                Some("not_found") => Response::NotFound,
                Some("other") => Response::OtherError,
                None => Response::Block {
                    block: block(&r["block"]),
                    node: r["node"].as_u64().unwrap(),
                },
                _ => unreachable!(),
            })
            .collect();
        let result = parent_connector::consensus(
            &responses,
            row["total"].as_u64().unwrap() as usize,
            parent_connector::Limits::default(),
        )
        .unwrap();
        let j = match result {
            Consensus::Block { block, node } => json!({"block":block_json(&block),"node":node}),
            Consensus::NotFound => json!("not_found"),
            Consensus::NoAgreement => json!("no_parent_chain_agreement"),
        };
        assert_eq!(j, row["result"], "{}", row["name"]);
    }
    assert_eq!(
        CONNECTOR["pmap"],
        json!({"good":[3,2,1],"errors":0,"failed_good":[3],"failed_errors":2})
    );
    for seed in CONNECTOR["seeds"].as_array().unwrap() {
        assert_eq!(
            parent_connector::increment_seed(&bytes(&seed["input"]))
                .unwrap()
                .as_slice(),
            bytes(&seed["output"])
        );
    }
    for seed in CONNECTOR["invalid_seeds"].as_array().unwrap() {
        assert_eq!(seed["output_size"], 16);
        assert_eq!(
            parent_connector::increment_seed(&vec![0; seed["size"].as_u64().unwrap() as usize]),
            None
        );
    }
}

#[test]
fn cache_operational_limits_and_backend_failures_rollback_and_retry() {
    let row = CACHE["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "markers_over_size")
        .unwrap();
    let order = otp_order::profile(&CACHE["profile"]);
    let (c, h) = setup(row);
    let e = event(&row["steps"][0]["input"]);
    let mut expected = c.clone();
    let mut expected_host = h.clone();
    expected
        .step(e.clone(), &mut expected_host, &order, Limits::default())
        .unwrap();
    // Legacy max_size is two, yet all eight requests must remain represented.
    assert_eq!(expected.blocks().len(), 8);
    for limits in [
        Limits {
            maximum_entries: 7,
            ..Limits::default()
        },
        Limits {
            maximum_requests: 7,
            ..Limits::default()
        },
        Limits {
            maximum_targets: 7,
            ..Limits::default()
        },
        Limits {
            maximum_bytes: 1,
            ..Limits::default()
        },
    ] {
        let mut limited = c.clone();
        let mut host = h.clone();
        assert_eq!(
            limited.step(e.clone(), &mut host, &order, limits),
            Err(StepError::Cache(Error::ResourceLimit))
        );
        assert_eq!(limited, c);
        assert_eq!(host, h);
        limited
            .step(e.clone(), &mut host, &order, Limits::default())
            .unwrap();
        assert_eq!(limited, expected);
        assert_eq!(host, expected_host);
    }
    let mut failed = c.clone();
    let mut host = h.clone();
    host.fail_request = Some((3, HostError::Unavailable));
    let before = host.clone();
    assert_eq!(
        failed.step(e.clone(), &mut host, &order, Limits::default()),
        Err(StepError::Host(HostError::Unavailable))
    );
    assert_eq!(failed, c);
    assert_eq!(host, before);
    host.fail_request = None;
    failed
        .step(e.clone(), &mut host, &order, Limits::default())
        .unwrap();
    assert_eq!(failed, expected);
    assert_eq!(host, expected_host);
    // Native failure leaves cache state untouched but preserves emitted prefix.
    let mut failed = c.clone();
    let mut host = h.clone();
    host.fail_request = Some((3, HostError::Native));
    assert_eq!(
        failed.step(e, &mut host, &order, Limits::default()),
        Err(StepError::Host(HostError::Native))
    );
    assert_eq!(failed, c);
    assert_eq!(host.effects.len(), 4);
}

struct BadOrder;
impl MapOrder for BadOrder {
    fn indices(
        &self,
        keys: &[&Value],
        _: Traversal,
        _: ae_vm_fate::Limits,
    ) -> Result<Vec<usize>, ae_vm_fate::Error> {
        Ok(vec![keys.len(); keys.len()])
    }
}
#[test]
fn bad_order_and_connector_resource_failures_remain_explicit() {
    let (mut c, mut h) = setup(&CACHE["runs"][0]);
    let before = c.clone();
    let before_host = h.clone();
    assert_eq!(
        c.step(Event::Initialize, &mut h, &BadOrder, Limits::default()),
        Err(StepError::Cache(Error::InvalidOrder))
    );
    assert_eq!(c, before);
    assert_eq!(h, before_host);
    let b = block(&CACHE["runs"][0]["config"]["fetch"]);
    let response = parent_connector::Response::Block { block: b, node: 1 };
    assert_eq!(
        parent_connector::consensus(
            std::slice::from_ref(&response),
            0,
            parent_connector::Limits::default()
        ),
        Err(parent_connector::Error::InvalidResponseCount)
    );
    for limits in [
        parent_connector::Limits {
            maximum_responses: 0,
            ..Default::default()
        },
        parent_connector::Limits {
            maximum_bytes: 1,
            ..Default::default()
        },
    ] {
        assert_eq!(
            parent_connector::consensus(std::slice::from_ref(&response), 1, limits),
            Err(parent_connector::Error::ResourceLimit)
        );
    }
}
