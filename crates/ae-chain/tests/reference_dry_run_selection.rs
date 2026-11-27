use ae_chain::dry_run::{self as select, Error, Selector, Source, StateError};
use ae_codec::headers::{Header, HeaderMode, KeyHeader};
use ae_execution::{Context, dry_run as dry, transactions};
use ae_protocol::{NetworkProfile, Protocol};
use ae_state::Trees;
use ae_trie::{MemoryStore, Overlay, OverlayLimits};
use ae_types::{AccountPublicKey, Hash256};
use num_bigint::BigInt;
use serde_json::{Value as J, json};
use std::sync::LazyLock;

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
    let data: J =
        serde_json::from_str(include_str!("fixtures/erlang_dry_run_selection.json")).unwrap();
    unpack(&data["root"], data["values"].as_array().unwrap())
});
fn bytes(v: &J) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
fn hash(v: &J) -> Hash256 {
    Hash256(bytes(v).try_into().unwrap())
}
fn integer(v: &J) -> BigInt {
    v.to_string().parse().unwrap()
}
fn atom(s: &str) -> J {
    json!({"atom":s})
}
fn tuple(a: Vec<J>) -> J {
    json!({"tuple":a})
}
fn exception(reason: J) -> J {
    json!({"kind":"exception","class":"error","reason":reason})
}
fn returned(reason: &str) -> J {
    json!({"kind":"returned","reason":{"binary":hex::encode_upper(reason)}})
}
#[derive(Debug)]
enum Fault {
    Returned,
    Raised,
}
struct NativeSource<'a> {
    row: &'a J,
    trace: Vec<J>,
}
impl NativeSource<'_> {
    fn key(&self) -> Hash256 {
        hash(&self.row["seed"]["key"]["hash"])
    }
    fn header_value(&self, v: &J) -> Header {
        Header::decode(
            &bytes(&v["header"]),
            if self.row["family"] == "pos" {
                HeaderMode::Hyperchain
            } else {
                HeaderMode::ProofOfWork
            },
        )
        .unwrap()
    }
    fn record(&self, h: Hash256) -> Option<&J> {
        self.row["seed"]
            .as_object()
            .unwrap()
            .values()
            .find(|v| hash(&v["hash"]) == h)
    }
    fn record_call(&mut self, call: &str, arg: J) {
        self.trace.push(json!({"call":call,"arg":arg}));
    }
}
impl Source for NativeSource<'_> {
    type State = Vec<u8>;
    type Error = Fault;
    fn top(&mut self) -> Result<Option<Hash256>, Fault> {
        self.record_call("top", J::Null);
        Ok((self.row["failure"] != "no_top").then(|| hash(&self.row["seed"]["micro2"]["hash"])))
    }
    fn key_header(&mut self, height: &BigInt) -> Result<Option<KeyHeader>, Fault> {
        self.record_call("height", serde_json::from_str(&height.to_string()).unwrap());
        Ok(if height == &BigInt::from(50) {
            let Header::Key(k) = self.header_value(&self.row["seed"]["key"]) else {
                panic!()
            };
            Some(k)
        } else {
            None
        })
    }
    fn header(&mut self, h: Hash256) -> Result<Option<Header>, Fault> {
        self.record_call("header", json!(hex::encode_upper(h.0)));
        Ok(
            if self.row["failure"] == "missing_leader" && h == self.key() {
                None
            } else {
                self.record(h).map(|v| self.header_value(v))
            },
        )
    }
    fn state(&mut self, h: Hash256) -> Result<Vec<u8>, StateError<Fault>> {
        self.record_call("state", json!(hex::encode_upper(h.0)));
        match self.row["failure"].as_str().unwrap() {
            "gc" => Err(StateError::GarbageCollected),
            "returned_state" => Err(StateError::Source(Fault::Returned)),
            "raised_state" => Err(StateError::Source(Fault::Raised)),
            _ => self
                .record(h)
                .map(|v| bytes(&v["trees"]))
                .ok_or(StateError::GarbageCollected),
        }
    }
}
fn selector(row: &J) -> Selector {
    let seed = &row["seed"];
    let missing = || {
        let mut h = [0; 32];
        h[31] = 99;
        Hash256(h)
    };
    match row["selector"].as_str().unwrap() {
        "top" => Selector::Top,
        "height" => Selector::Height(50.into()),
        "high" => Selector::Height(BigInt::from(1) << 100),
        "negative" => Selector::Height((-1).into()),
        "hash_key" => Selector::Hash(hash(&seed["key"]["hash"])),
        "hash_micro" => Selector::Hash(hash(&seed["micro2"]["hash"])),
        "in_key" => Selector::InMicro(hash(&seed["key"]["hash"])),
        "in_micro1" => Selector::InMicro(hash(&seed["micro1"]["hash"])),
        "in_micro2" => Selector::InMicro(hash(&seed["micro2"]["hash"])),
        "missing_hash" => Selector::Hash(missing()),
        "missing_in" => Selector::InMicro(missing()),
        v => panic!("unknown selector {v}"),
    }
}
fn selection_error(e: Error<Fault>) -> J {
    match e {
        Error::Returned(r) => returned(r.reason()),
        Error::InvalidHash => exception(atom("invalid_hash")),
        Error::StateGarbageCollected => exception(atom("state_garbage_collected")),
        Error::InvalidHeight => exception(atom("function_clause")),
        Error::MissingLeader(_) => exception(tuple(vec![atom("badmatch"), atom("error")])),
        Error::Source(Fault::Returned) => exception(tuple(vec![
            atom("try_clause"),
            tuple(vec![atom("error"), atom("no_state")]),
        ])),
        Error::Source(Fault::Raised) => exception(atom("storage_fault")),
        e => panic!("unexpected selector error {e:?}"),
    }
}
#[test]
fn original_selectors_preserve_state_environment_and_failure_order() {
    let mut successes = 0;
    for (i, row) in FIXTURE["rows"].as_array().unwrap().iter().enumerate() {
        let mut source = NativeSource { row, trace: vec![] };
        let result = select::select(&mut source, &selector(row));
        assert_eq!(
            json!(source.trace),
            row["selection"]["trace"],
            "trace row {i}"
        );
        let expected = &row["selection"]["result"];
        match result {
            Err(e) => assert_eq!(selection_error(e), *expected, "row {i}"),
            Ok(s) => {
                successes += 1;
                assert_eq!(expected["kind"], "selected");
                assert_eq!(s.state, bytes(&expected["trees"]), "state row {i}");
                assert_eq!(
                    s.state,
                    bytes(&source.record(s.state_hash).unwrap()["trees"])
                );
                let env = &expected["env"];
                assert_eq!(s.timestamp, integer(&env["time"]), "timestamp row {i}");
                assert_eq!(s.key.common.height, env["height"].as_u64().unwrap());
                assert_eq!(
                    s.key.common.version as u64,
                    env["protocol"].as_u64().unwrap()
                );
                assert_eq!(s.key_hash, hash(&env["key_hash"]));
                assert_eq!(s.key.beneficiary.as_slice(), bytes(&env["beneficiary"]));
                let difficulty = match row["family"].as_str().unwrap() {
                    "on_demand" => BigInt::from(ae_consensus::on_demand::TAG),
                    "pos" => BigInt::from(s.key.target),
                    "pow" => BigInt::from_bytes_be(
                        num_bigint::Sign::Plus,
                        ae_consensus::target::Target(s.key.target)
                            .difficulty()
                            .unwrap()
                            .as_bytes(),
                    ),
                    _ => panic!(),
                };
                assert_eq!(
                    difficulty,
                    integer(&env["vm_difficulty"]),
                    "VM difficulty row {i}"
                );
                assert_eq!(env["context"], "aetx_transaction");
                assert_eq!(env["dry_run"], false);
                assert!(env["floor"].is_null());
            }
        }
    }
    assert_eq!(FIXTURE["rows"].as_array().unwrap().len(), 478);
    assert_eq!(successes, 424);
}
fn initial(raw: &[u8]) -> Trees<Overlay<MemoryStore>, Overlay<MemoryStore>> {
    Trees::deserialize_from_binary(
        raw,
        Overlay::new(MemoryStore::default(), OverlayLimits::default()),
        Overlay::new(MemoryStore::default(), OverlayLimits::default()),
        Default::default(),
    )
    .unwrap()
}
fn setup_error(e: dry::Error<MemoryStore, MemoryStore>) -> J {
    match e {
        dry::Error::NegativeBalance { balance, .. } => {
            let n: J = serde_json::from_str(&balance.to_string()).unwrap();
            exception(tuple(vec![
                atom("illegal_field"),
                atom("balance"),
                atom("int"),
                n.clone(),
                atom("int"),
                n,
            ]))
        }
        dry::Error::Preparation(dry::prepare::Failure {
            index: Some(0),
            error: dry::prepare::Error::BadCallRequest,
        }) => returned("Internal error {bad_dry_run_call_request,#{}}"),
        e => panic!("unexpected setup error {e:?}"),
    }
}
#[test]
fn original_signed_funding_precedes_preparation_without_mutating_source() {
    let network = NetworkProfile::mainnet();
    let config = transactions::Config::primitives(&network);
    let mut checked = [0, 0];
    for (i, row) in FIXTURE["rows"].as_array().unwrap().iter().enumerate() {
        if row["selection"]["result"]["kind"] != "selected" {
            continue;
        }
        let selected = &row["selection"]["result"];
        let raw = bytes(&selected["trees"]);
        let trees = initial(&raw);
        let context = Context::transaction(
            selected["env"]["height"].as_u64().unwrap(),
            Protocol::try_from(selected["env"]["protocol"].as_u64().unwrap() as u32).unwrap(),
        );
        let funding: Vec<_> = row["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| dry::Funding {
                account: AccountPublicKey(bytes(&a["pub_key"]).try_into().unwrap()),
                amount: integer(&a["amount"]),
            })
            .collect();
        let options = dry::Options {
            profile: if row["force"] == true {
                dry::Profile::Public
            } else {
                dry::Profile::Unspecified
            },
            ..Default::default()
        };
        match dry::setup(trees.clone(), &context, &funding, &options) {
            Err(e) => {
                checked[1] += 1;
                let observed = setup_error(e);
                for stage in ["setup", "empty", "malformed"] {
                    assert_eq!(observed, row[stage]["result"], "{stage} row {i}");
                }
            }
            Ok(setup) => {
                checked[0] += 1;
                let native = &row["setup"]["result"];
                assert_eq!(
                    setup
                        .trees()
                        .serialize_to_binary(Default::default())
                        .unwrap(),
                    bytes(&native["trees"]),
                    "funding row {i}"
                );
                assert_eq!(setup.context().height, context.height);
                assert_eq!(
                    setup.context().protocol as u64,
                    native["env"]["protocol"].as_u64().unwrap()
                );
                assert_eq!(
                    json!(setup.metering_floor().map(|p| p as u32)),
                    native["env"]["floor"]
                );
                assert!(setup.context().dry_run);
                let out = setup
                    .requests(&config, vec![], Default::default(), Default::default())
                    .unwrap();
                assert!(out.results.is_empty() && out.events.is_empty());
                assert_eq!(
                    row["empty"]["result"],
                    json!({"kind":"executed","results":[],"events":[]})
                );
                assert_eq!(
                    out.trees.serialize_to_binary(Default::default()).unwrap(),
                    bytes(&native["trees"])
                );
                let setup = dry::setup(trees.clone(), &context, &funding, &options).unwrap();
                let bad = json!({});
                let e = setup
                    .requests(
                        &config,
                        vec![dry::prepare::Input::CallRequest(&bad)],
                        Default::default(),
                        Default::default(),
                    )
                    .unwrap_err();
                assert_eq!(
                    setup_error(e),
                    row["malformed"]["result"],
                    "malformed row {i}"
                );
            }
        }
        assert_eq!(
            trees.serialize_to_binary(Default::default()).unwrap(),
            raw,
            "source changed row {i}"
        );
        assert!(!context.dry_run);
    }
    assert_eq!(checked, [328, 96]);
}
