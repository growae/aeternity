use crate::http::dry_run::{Location, LookupError, Source};
use ae_codec::headers::{Header, HeaderMode, KeyHeader};
use ae_execution::{Context, contracts, transactions};
use ae_protocol::{NetworkProfile, Protocol};
use ae_state::Trees;
use ae_trie::{MemoryStore, Overlay, OverlayLimits};
use ae_tx::{SignedTransaction, Transaction};
use ae_types::Hash256;
use ae_vm_fate::{machine, maps::listing::Overrides};
use num_bigint::{BigInt, Sign};
use serde_json::{Value as J, json};
use std::sync::{Arc, LazyLock};

#[path = "../../../ae-vm-fate/tests/support/otp_order.rs"]
// Share the native fixture decoder without coupling HTTP tests to mining tests.
#[allow(clippy::duplicate_mod)]
pub(crate) mod otp_order;
pub(crate) fn unpack(v: &J, pool: &[J]) -> J {
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
pub(crate) static FIXTURE: LazyLock<J> = LazyLock::new(|| {
    let v: J =
        serde_json::from_str(include_str!("../fixtures/erlang_dry_run_pipeline.json")).unwrap();
    unpack(&v["root"], v["values"].as_array().unwrap())
});
pub(crate) fn bytes(v: &J) -> Vec<u8> {
    hex::decode(v.as_str().unwrap()).unwrap()
}
pub(crate) fn hash(v: &J) -> Hash256 {
    Hash256(bytes(v).try_into().unwrap())
}
pub(crate) type State = Trees<Overlay<MemoryStore>, Overlay<MemoryStore>>;
pub(crate) type View = ae_hyperchain::chain::View<Overlay<MemoryStore>, Overlay<MemoryStore>>;
pub(crate) struct Backend<'a> {
    pub(crate) row: &'a J,
    pub(crate) trace: Vec<J>,
    pub(crate) trees: State,
}
impl<'a> Backend<'a> {
    pub(crate) fn new(row: &'a J) -> Self {
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
    pub(crate) fn view(
        &mut self,
        h: Hash256,
    ) -> Result<Arc<View>, ae_chain::dry_run::Error<&'static str>> {
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
        match self.row["input"]["state_mode"].as_str().unwrap() {
            "ordinary" => Ok(self.trees.clone()),
            // Original controlled aec_chain stub delays state lookup by one
            // second. Public core timers expire; internal profiles continue.
            "timeout" => {
                std::thread::sleep(std::time::Duration::from_secs(1));
                Ok(self.trees.clone())
            }
            "gc" => Err(ae_chain::dry_run::StateError::GarbageCollected),
            "raised" => Err(ae_chain::dry_run::StateError::Source(
                "controlled_state_failure",
            )),
            mode => panic!("unsupported source mode {mode}"),
        }
    }
}
pub(crate) fn configured<R>(profile: &J, f: impl FnOnce(&transactions::Config<'_>) -> R) -> R {
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
pub(crate) fn covered(row: &J) -> bool {
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
            | "unknown_top"
            | "pruned_state"
            | "bad_call"
            | "pruned_before_bad_call"
    ) || (row["input"]["name"] == "negative_before_bad_call" && row["module"] == "endpoints")
}
