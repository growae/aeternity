use super::{Config, Error, Location, LookupError, Profile, Source, execution, response, schema};
use ae_codec::headers::{Header, HeaderMode, KeyHeader};
use ae_execution::{Context, contracts, dry_run as core, transactions};
use ae_protocol::{NetworkProfile, Protocol};
use ae_state::Trees;
use ae_trie::{MemoryStore, Overlay, OverlayLimits};
use ae_tx::{SignedTransaction, Transaction};
use ae_types::{Hash256, Unsigned};
use ae_vm_fate::{machine, maps::listing::Overrides};
use num_bigint::{BigInt, Sign};
use serde_json::{Value as J, json};
use std::{
    collections::BTreeSet,
    sync::{Arc, LazyLock},
};

#[path = "../../../../ae-vm-fate/tests/support/otp_order.rs"]
// Share the native fixture decoder without coupling HTTP tests to mining tests.
#[allow(clippy::duplicate_mod)]
mod otp_order;
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
static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    let v: J = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_dry_run_pipeline.json"
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
type State = Trees<Overlay<MemoryStore>, Overlay<MemoryStore>>;
type View = ae_hyperchain::chain::View<Overlay<MemoryStore>, Overlay<MemoryStore>>;
struct Backend<'a> {
    row: &'a J,
    trace: Vec<J>,
    trees: State,
}
impl<'a> Backend<'a> {
    fn new(row: &'a J) -> Self {
        Self {
            row,
            trace: vec![],
            trees: Trees::deserialize_from_binary(
                &bytes(&row["seed"]["initial"]),
                Overlay::new(MemoryStore::default(), OverlayLimits::default()),
                Overlay::new(MemoryStore::default(), OverlayLimits::default()),
                Default::default(),
            )
            .unwrap(),
        }
    }
    fn view(&mut self, h: Hash256) -> Result<Arc<View>, ae_chain::dry_run::Error<&'static str>> {
        let selected = ae_chain::dry_run::select(self, &ae_chain::dry_run::Selector::Hash(h))?;
        let key = selected.key;
        let difficulty = ae_consensus::target::Target(key.target)
            .difficulty()
            .unwrap();
        Ok(Arc::new(ae_hyperchain::chain::View {
            trees: Arc::new(selected.state),
            context: Context::transaction(
                key.common.height,
                Protocol::try_from(key.common.version).unwrap(),
            ),
            environment: machine::program::remote::context::Environment {
                auth_tx_hash: None,
                origin: [0; 32],
                gas_price: 0.into(),
                fee: 0.into(),
                beneficiary: key.beneficiary,
                timestamp: selected.timestamp,
                difficulty: BigInt::from_bytes_be(Sign::Plus, difficulty.as_bytes()),
                block_gas_limit: 6_000_000.into(),
                network_id: b"ae_mainnet".to_vec(),
                consensus: machine::program::remote::context::Consensus::Pow,
                key_hash: selected.key_hash.0,
            },
            history: Arc::default(),
            aevm: None,
        }))
    }
}
impl Source for Backend<'_> {
    type Error = &'static str;
    fn top_height(&mut self) -> Result<Option<u64>, Self::Error> {
        self.trace.push(json!({"call":"top_block"}));
        Ok(self.row["input"]["top_available"]
            .as_bool()
            .unwrap()
            .then_some(50))
    }
    fn protocol(&mut self, height: u64) -> Result<Protocol, Self::Error> {
        self.trace.push(json!({"call":"protocol","height":height}));
        Ok(Protocol::try_from(self.row["input"]["protocol"].as_u64().unwrap() as u32).unwrap())
    }
    fn top_hash(&mut self) -> Result<Option<Hash256>, Self::Error> {
        self.trace.push(json!({"call":"top_hash"}));
        Ok(self.row["input"]["top_available"]
            .as_bool()
            .unwrap()
            .then(|| hash(&self.row["seed"]["hash"])))
    }
    fn transaction(&mut self, h: Hash256) -> Result<Location, LookupError<Self::Error>> {
        self.trace
            .push(json!({"call":"find_tx","hash":hex::encode_upper(h.0)}));
        Ok(
            match self.row["seed"]["pool"]
                .as_array()
                .unwrap()
                .iter()
                .find(|t| hash(&t["hash"]) == h)
            {
                Some(t) => Location::Mempool(Arc::new(
                    SignedTransaction::<Transaction>::decode(&bytes(&t["signed"])).unwrap(),
                )),
                None => Location::Other,
            },
        )
    }
}
impl ae_chain::dry_run::Source for Backend<'_> {
    type State = State;
    type Error = &'static str;
    fn top(&mut self) -> Result<Option<Hash256>, Self::Error> {
        panic!("HTTP has already selected its top")
    }
    fn key_header(&mut self, _: &BigInt) -> Result<Option<KeyHeader>, Self::Error> {
        panic!("HTTP uses hashes")
    }
    fn header(&mut self, h: Hash256) -> Result<Option<Header>, Self::Error> {
        self.trace
            .push(json!({"call":"header","hash":hex::encode_upper(h.0)}));
        Ok((h == hash(&self.row["seed"]["hash"])).then(|| {
            Header::decode(&bytes(&self.row["seed"]["header"]), HeaderMode::ProofOfWork).unwrap()
        }))
    }
    fn state(&mut self, h: Hash256) -> Result<State, ae_chain::dry_run::StateError<Self::Error>> {
        self.trace
            .push(json!({"call":"state","hash":hex::encode_upper(h.0)}));
        assert_eq!(self.row["input"]["state_mode"], "ordinary");
        Ok(self.trees.clone())
    }
}
fn configured<R>(profile: &J, f: impl FnOnce(&transactions::Config<'_>) -> R) -> R {
    let network = NetworkProfile::mainnet();
    let vm = machine::Context {
        current_contract: [0; 32],
        vm_version: ae_vm_fate::types::Version::Three,
        in_auth: false,
        on_chain: true,
        reserved_gas: 0.into(),
        arithmetic_word_size: ae_vm_fate::maps::order::WordSize::Bits64,
        unicode_profile: ae_vm_fate::unicode::Profile::Otp26Unicode15,
        bls_profile: ae_crypto::bls::Profile::Mcl11976beLlvm64VintArm64,
    };
    let environment = machine::program::remote::context::Environment {
        auth_tx_hash: None,
        origin: [0; 32],
        gas_price: 0.into(),
        fee: 0.into(),
        beneficiary: [0; 32],
        timestamp: 0.into(),
        difficulty: 0.into(),
        block_gas_limit: 6_000_000.into(),
        network_id: b"ae_mainnet".to_vec(),
        consensus: machine::program::remote::context::Consensus::Pow,
        key_hash: [0; 32],
    };
    let order = otp_order::profile(profile);
    let overrides = Overrides::default();
    let fate = contracts::fate::Config {
        control: None,
        context: &vm,
        order: &order,
        overrides: &overrides,
        environment: &environment,
        history: Arc::default(),
        auth_tx: None,
        metering_floor: None,
        diagnostic_profile: ae_vm_fate::diagnostic::Profile::Test,
    };
    f(&transactions::Config {
        fate: Some(&fate),
        ..transactions::Config::primitives(&network)
    })
}

// These cases compose the real schema, helper preparation, state selector,
// disposable execution and renderer. Heap/deadline/worker exceptions remain
// native reference rows, not claims of Rust worker or live-socket parity.
fn covered(row: &J) -> bool {
    matches!(
        row["input"]["name"].as_str().unwrap(),
        "empty"
            | "spend_success"
            | "spend_failure_middle"
            | "spend_failure_last"
            | "call_success"
            | "call_failure_middle"
            | "call_stateless"
            | "call_out_of_gas"
            | "floor"
            | "metering_disabled"
            | "over_gas"
            | "missing_top"
            | "hash_lookup"
            | "missing_hash"
            | "schema_before_top"
    )
}
fn compare(row: &J, profile: &J) {
    let mut source = Backend::new(row);
    let original = source.trees.roots();
    let v3 = row["module"] == "oas_endpoints";
    let wire = bytes(&row["raw"]);
    let parsed = schema::request(
        schema::Body::Complete(&wire),
        v3,
        row["query"]["query"].as_str().unwrap(),
        5_000_000,
    );
    let (code, body) = match parsed {
        Err(error) => {
            let r = error.response();
            (r.status, serde_json::from_slice(&r.body).unwrap())
        }
        Ok(parsed) => {
            let mode = if row["operation"] == "ProtectedDryRunTxs" {
                Profile::Public
            } else {
                Profile::Internal
            };
            let gas: BigInt = row["input"]["gas_limit"]
                .as_number()
                .unwrap()
                .as_str()
                .parse()
                .unwrap();
            let keys: BTreeSet<_> = [
                "txs",
                "top",
                "accounts",
                "tx_events",
                "tx",
                "tx_hash",
                "call_req",
                "calldata",
                "contract",
                "amount",
                "gas",
                "caller",
                "nonce",
                "abi_version",
                "context",
                "stateful",
                "pub_key",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect();
            let order = otp_order::profile(profile);
            let config = Config {
                profile: mode,
                gas_limit: &gas,
                existing_keys: &keys,
                order: &order,
                state: Default::default(),
                limits: Default::default(),
            };
            match super::prepare(&parsed.input, &mut source, &config) {
                Err(Error::Rejected { code, reason }) => (code, json!({"reason":reason})),
                Err(error) => panic!("unexpected preparation {error:?}"),
                Ok(request) => configured(profile, |base| {
                    let events = request.tx_events;
                    let options = core::Options {
                        store_read_gas_metering: row["input"]["metering"].as_bool().unwrap(),
                        control: Some(ae_types::work::Control::new(|| false)),
                        ..Default::default()
                    };
                    let result = execution::execute(
                        request,
                        |h| source.view(h),
                        base,
                        &options,
                        Default::default(),
                        Default::default(),
                    )
                    .unwrap();
                    assert_eq!(result.view.trees.roots(), original);
                    let prices: Vec<_> = result
                        .output
                        .results
                        .iter()
                        .filter_map(|r| {
                            r.result
                                .as_ref()
                                .ok()?
                                .call
                                .as_ref()
                                .map(|c| c.gas_price.clone())
                        })
                        .collect();
                    let floor = Unsigned::from(row["input"]["floor"].as_u64().unwrap());
                    let rendered = response::body(
                        &result.output,
                        mode,
                        events,
                        Some(&floor),
                        Default::default(),
                    )
                    .unwrap();
                    assert_eq!(
                        prices,
                        result
                            .output
                            .results
                            .iter()
                            .filter_map(|r| r
                                .result
                                .as_ref()
                                .ok()?
                                .call
                                .as_ref()
                                .map(|c| c.gas_price.clone()))
                            .collect::<Vec<_>>()
                    );
                    let rendered = schema::render(200, rendered, v3, parsed.strings).unwrap();
                    // Capacity errors are operational and leave the retained view intact.
                    assert!(matches!(
                        response::body(
                            &result.output,
                            mode,
                            events,
                            Some(&floor),
                            response::Limits {
                                json_bytes: 0,
                                ..Default::default()
                            }
                        ),
                        Err(response::Error::Capacity)
                    ));
                    assert_eq!(result.view.trees.roots(), original);
                    (200, rendered)
                }),
            }
        }
    };
    assert_eq!(
        code,
        row["result"]["response"]["code"].as_u64().unwrap() as u16
    );
    assert_eq!(body, row["result"]["response"]["body"]);
    assert_eq!(source.trace, row["trace"].as_array().unwrap().clone());
    assert_eq!(source.trees.roots(), original);
}
#[test]
fn schema_preparation_execution_and_rendering_match_native_pipeline() {
    let rows = FIXTURE["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 560);
    assert_eq!(rows.iter().filter(|r| covered(r)).count(), 472);
    for (index, row) in rows.iter().enumerate().filter(|(_, r)| covered(r)) {
        for profile in [&FIXTURE["profile"], &FIXTURE["repeat_profile"]] {
            let result = std::panic::catch_unwind(|| compare(row, profile));
            assert!(
                result.is_ok(),
                "native row {index}: {} {} {} {}",
                row["input"]["name"],
                row["input"]["protocol"],
                row["operation"],
                row["module"]
            );
        }
    }
}

fn native_kind(value: &str) -> ae_tx::TransactionKind {
    ae_tx::TransactionKind::ALL
        .into_iter()
        .find(|kind| {
            format!("{kind:?}").to_lowercase() == value.trim_end_matches("_tx").replace('_', "")
        })
        .unwrap()
}
fn native_event(row: &J) -> transactions::batch::Event {
    use ae_execution::fate_chain::TransferEvent;
    use ae_tx::{CanonicalTransaction, TransactionLeaf};
    let kind = native_kind(row["type"].as_str().unwrap());
    let tx_hash = hash(&row["hash"]);
    let value = match row["kind"].as_str().unwrap() {
        "channel" => transactions::Event::Channel {
            channel: bytes(&row["key"]).try_into().unwrap(),
            transaction_hash: tx_hash,
            transaction_type: kind,
        },
        "delta" => transactions::Event::Primitive(ae_execution::Event::FeeDebit {
            account: ae_types::AccountPublicKey(bytes(&row["key"]).try_into().unwrap()),
            amount: 0.into(),
        }),
        "internal" => {
            let tx = Transaction::decode(&bytes(&row["transaction"])).unwrap();
            let event = match tx.leaf() {
                TransactionLeaf::Spend(tx) => TransferEvent::InternalCall(tx.clone()),
                TransactionLeaf::Name(tx) => {
                    assert_eq!(bytes(&row["key"]), b"Chain.aens_revoke");
                    TransferEvent::NameCall {
                        info: "Chain.aens_revoke",
                        transaction: Some(tx.clone()),
                    }
                }
                TransactionLeaf::Oracle(tx) => {
                    assert_eq!(bytes(&row["key"]), b"Chain.oracle_extend");
                    TransferEvent::OracleCall {
                        info: "Chain.oracle_extend",
                        transaction: tx.clone(),
                    }
                }
                other => panic!("unexpected native event {other:?}"),
            };
            transactions::Event::Contract(contracts::Event::Internal(event))
        }
        other => panic!("unexpected native event kind {other}"),
    };
    transactions::batch::Event {
        transaction_hash: tx_hash,
        transaction_type: kind,
        value,
    }
}
#[test]
fn native_result_types_reasons_event_order_and_callback_serialization() {
    use ae_tx::TransactionKind;
    use transactions::batch::Failure;
    let fixture: J = serde_json::from_str(include_str!(
        "../../../tests/fixtures/erlang_dry_run_rendering.json"
    ))
    .unwrap();
    let rows = fixture["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 34);
    for (index, row) in rows.iter().enumerate() {
        let mut output = core::Output {
            trees: Backend::new(&FIXTURE["rows"][0]).trees,
            context: Context::transaction(50, Protocol::Ceres),
            results: vec![],
            events: row["events"]
                .as_array()
                .unwrap()
                .iter()
                .map(native_event)
                .collect(),
        };
        let failure = match row["error"].as_str() {
            Some("gas_limit_exceeded") => Some((
                native_kind(row["type"].as_str().unwrap()),
                Failure::GasLimitExceeded,
            )),
            Some("tx_nonce_already_used_for_account") => Some((
                TransactionKind::Spend,
                Failure::Transaction(Box::new(transactions::Error::Primitive(
                    ae_execution::Error::NonceAlreadyUsed,
                ))),
            )),
            Some("crash") => Some((
                TransactionKind::Spend,
                Failure::Transaction(Box::new(transactions::Error::MinimumFeeCaseClause(
                    (-1).into(),
                ))),
            )),
            Some("not_allowed_to_pay_for") => Some((
                TransactionKind::PayingFor,
                Failure::Transaction(Box::new(transactions::Error::NotAllowedToPayFor(
                    native_kind(row["type"].as_str().unwrap()),
                ))),
            )),
            None => None,
            other => panic!("unknown captured error {other:?}"),
        };
        if let Some((transaction_type, failure)) = failure {
            output.results.push(core::ResultItem {
                transaction_type,
                result: Err(failure),
            });
        }
        let expected = &row["output"];
        assert_eq!(
            response::body(&output, Profile::Internal, true, None, Default::default()).unwrap(),
            *expected,
            "native rendering row {index}"
        );
        let without =
            response::body(&output, Profile::Public, false, None, Default::default()).unwrap();
        assert_eq!(without, json!({"results":expected["results"]}));
    }
}
