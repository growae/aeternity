use ae_execution::dry_run::prepare::{self, Input};
use ae_node::http::dry_run::{
    self as dry, Config, Error, Limits, Location, LookupError, Profile, Source,
};
use ae_protocol::Protocol;
use ae_tx::{CanonicalTransaction, SignedTransaction, Transaction};
use ae_types::Hash256;
use ae_vm_fate::maps::order::Otp26;
use num_bigint::BigInt;
use serde_json::{Value as J, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, LazyLock},
};

#[path = "../../ae-vm-fate/tests/support/otp_order.rs"]
mod otp_order;
static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    fn unpack(v: &J, pool: &[J]) -> J {
        match v {
            J::Object(m) if m.len() == 1 && m.contains_key("$ref") => {
                unpack(&pool[m["$ref"].as_u64().unwrap() as usize], pool)
            }
            J::Object(m) => J::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), unpack(v, pool)))
                    .collect(),
            ),
            J::Array(a) => J::Array(a.iter().map(|v| unpack(v, pool)).collect()),
            v => v.clone(),
        }
    }
    let v: J = serde_json::from_str(include_str!(
        "fixtures/erlang_http_dry_run_preparation.json"
    ))
    .unwrap();
    unpack(&v["root"], v["values"].as_array().unwrap())
});
fn bytes(v: &J) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn hash(v: &J) -> Hash256 {
    Hash256(bytes(v).try_into().unwrap())
}
fn small(n: u8) -> Hash256 {
    let mut b = [0; 32];
    b[31] = n;
    Hash256(b)
}
fn keys() -> BTreeSet<String> {
    FIXTURE["existing_keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect()
}
struct Backend {
    available: bool,
    protocol: Protocol,
    pool: BTreeMap<Hash256, Option<Arc<SignedTransaction<Transaction>>>>,
    trace: Vec<J>,
    fail: Option<&'static str>,
}
impl Backend {
    fn new(row: &J) -> Self {
        Self {
            available: row["top_available"].as_bool().unwrap(),
            protocol: Protocol::try_from(row["protocol"].as_u64().unwrap() as u32).unwrap(),
            pool: FIXTURE["pool"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| {
                    (
                        hash(&v["hash"]),
                        (v["location"] == "mempool").then(|| {
                            Arc::new(SignedTransaction::decode(&bytes(&v["signed"])).unwrap())
                        }),
                    )
                })
                .collect(),
            trace: vec![],
            fail: None,
        }
    }
}
impl Source for Backend {
    type Error = &'static str;
    fn top_height(&mut self) -> Result<Option<u64>, Self::Error> {
        self.trace.push(json!({"kind":"top_block"}));
        if self.fail == Some("top_block") {
            return Err("storage unavailable");
        }
        Ok(self.available.then_some(50))
    }
    fn protocol(&mut self, height: u64) -> Result<Protocol, Self::Error> {
        self.trace.push(json!({"kind":"protocol","height":height}));
        Ok(self.protocol)
    }
    fn top_hash(&mut self) -> Result<Option<Hash256>, Self::Error> {
        self.trace.push(json!({"kind":"top_hash"}));
        if self.fail == Some("top_hash") {
            return Err("storage unavailable");
        }
        Ok(self.available.then_some(small(1)))
    }
    fn transaction(&mut self, hash: Hash256) -> Result<Location, LookupError<Self::Error>> {
        self.trace
            .push(json!({"kind":"find_tx","hash":hex::encode_upper(hash.0)}));
        if self.fail == Some("find_tx") {
            return Err(LookupError::Operational("storage unavailable"));
        }
        if hash == small(31) {
            return Err(LookupError::Native);
        }
        Ok(match self.pool.get(&hash).and_then(Option::as_ref) {
            Some(tx) => Location::Mempool(tx.clone()),
            None => Location::Other,
        })
    }
}
fn config<'a>(
    row: &J,
    gas: &'a BigInt,
    keys: &'a BTreeSet<String>,
    order: &'a Otp26,
) -> Config<'a, Otp26> {
    Config {
        profile: if row["profile"] == "public" {
            Profile::Public
        } else {
            Profile::Internal
        },
        gas_limit: gas,
        existing_keys: keys,
        order,
        state: Default::default(),
        limits: Default::default(),
    }
}
fn prepared(v: &dry::Request<'_>) -> J {
    json!({"top":hex::encode_upper(v.top.0),"profile":match v.profile {Profile::Public=>"public",Profile::Internal=>"internal"},"tx_events":v.tx_events,
        "accounts":v.accounts.iter().map(|a|json!({"pub_key":hex::encode_upper(a.public_key.0),"amount":serde_json::from_str::<J>(&a.amount.to_string()).unwrap()})).collect::<Vec<_>>(),
        "inputs":v.inputs.iter().map(|i|match i {Input::Transaction(tx)=>json!({"kind":"tx","transaction":hex::encode_upper(tx.encode())}),Input::CallRequest(r)=>json!({"kind":"call_req","request":r})}).collect::<Vec<_>>()})
}
#[test]
fn native_dispatch_preparation_preflight_errors_and_lookup_order() {
    let rows = FIXTURE["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 444);
    let keys = keys();
    for profile in [&FIXTURE["order_profile"], &FIXTURE["repeat_order_profile"]] {
        let order = otp_order::profile(profile);
        for (i, row) in rows.iter().enumerate() {
            let gas = row["gas_limit"]
                .as_number()
                .unwrap()
                .as_str()
                .parse()
                .unwrap();
            let config = config(row, &gas, &keys, &order);
            let mut source = Backend::new(row);
            match dry::prepare(&row["request"], &mut source, &config) {
                Ok(request) => {
                    assert_eq!(prepared(&request), row["prepared"], "row {i}");
                    assert_eq!(row["response"]["code"], 200);
                    let trace = row["trace"].as_array().unwrap();
                    assert_eq!(trace.last().unwrap(), &json!({"kind":"dry_run"}));
                    // The captured native core stub is an observation point,
                    // not evidence of VM execution or HTTP response rendering.
                    assert_eq!(source.trace, trace[..trace.len() - 1], "row {i}");
                }
                Err(Error::Rejected { code, reason }) => {
                    assert!(row["prepared"].is_null(), "row {i}");
                    assert_eq!(
                        json!({"code":code,"headers":[],"body":{"reason":reason}}),
                        row["response"],
                        "row {i}: {}",
                        row["request"]
                    );
                    assert_eq!(json!(source.trace), row["trace"], "row {i}");
                }
                Err(error) => panic!("row {i}: unexpected operational error {error:?}"),
            }
        }
    }
}
#[test]
fn malformed_calls_and_negative_funding_remain_deferred() {
    let keys = keys();
    let order = otp_order::profile(&FIXTURE["order_profile"]);
    let gas = 6_000_000.into();
    let row = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["profile"] == "internal"
                && r["request"]["txs"][0].get("call_req") == Some(&J::Null)
                && r["response"]["code"] == 200
        })
        .unwrap();
    let config = config(row, &gas, &keys, &order);
    let mut source = Backend::new(row);
    let request = dry::prepare(&row["request"], &mut source, &config).unwrap();
    assert!(matches!(
        prepare::all(request.inputs, Default::default(), Default::default()),
        Err(prepare::Failure {
            error: prepare::Error::BadCallRequest,
            ..
        })
    ));
    let row = FIXTURE["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["profile"] == "internal" && r["request"]["accounts"][0]["amount"] == -1)
        .unwrap();
    let mut source = Backend::new(row);
    let request = dry::prepare(&row["request"], &mut source, &config).unwrap();
    assert_eq!(request.accounts[0].amount, BigInt::from(-1));
}
#[test]
fn operational_source_and_decoder_stops_are_never_swallowed_as_input_errors() {
    let keys = keys();
    let order = otp_order::profile(&FIXTURE["order_profile"]);
    let gas = 6_000_000.into();
    let rows = FIXTURE["rows"].as_array().unwrap();
    for row in rows
        .iter()
        .filter(|r| r["response"]["code"] == 200 && r["request"]["txs"][0]["tx_hash"].is_string())
        .take(8)
    {
        let config = config(row, &gas, &keys, &order);
        let mut source = Backend::new(row);
        source.fail = Some("find_tx");
        assert!(matches!(
            dry::prepare(&row["request"], &mut source, &config),
            Err(Error::Source("storage unavailable"))
        ));
        source.fail = None;
        assert!(dry::prepare(&row["request"], &mut source, &config).is_ok());
    }
    let row = rows
        .iter()
        .find(|r| {
            r["profile"] == "internal"
                && r["request"]["txs"][0]["tx"].is_string()
                && r["response"]["code"] == 200
        })
        .unwrap();
    for limits in [
        Limits {
            requests: 0,
            ..Default::default()
        },
        Limits {
            encoded_bytes: 1,
            ..Default::default()
        },
        Limits {
            work_bytes: 0,
            ..Default::default()
        },
    ] {
        let mut config = config(row, &gas, &keys, &order);
        config.limits = limits;
        let mut source = Backend::new(row);
        assert!(matches!(
            dry::prepare(&row["request"], &mut source, &config),
            Err(Error::Capacity)
        ));
    }
    let row = rows
        .iter()
        .find(|r| {
            r["profile"] == "public"
                && r["request"]["txs"][0]["tx_hash"]
                    == ae_codec::api::encode(ae_codec::api::ApiType::TxHash, &small(13).0)
        })
        .unwrap();
    let mut config = config(row, &gas, &keys, &order);
    config.limits.transaction.max_layers = 0;
    let mut source = Backend::new(row);
    assert!(matches!(
        dry::prepare(&row["request"], &mut source, &config),
        Err(Error::Capacity)
    ));
}
