// Explicit historical VM choices. A descriptor is not a mainnet attestation.
use super::auxiliary::Loaded;
use ae_codec::rlp::{self, Rlp};
use ae_execution::{
    contracts::{aevm, fate},
    transactions::{self, owned::Owned},
};
use ae_protocol::{ConsensusType, NetworkProfile};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use ae_vm_fate::{
    machine::{self, program::remote::context},
    maps::order::{Atom, Mixing, Otp26, WordSize},
};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Resolution(super::resolution::Error),
    Network,
    Limit,
    Field(&'static str),
    Order(ae_vm_fate::Error),
    Owned(transactions::owned::Error),
}
#[derive(Debug, Clone)]
pub struct Specification {
    word: WordSize,
    mixing: Mixing,
    atoms: BTreeMap<Atom, u32>,
    arithmetic: WordSize,
    diagnostic: ae_vm_fate::diagnostic::Profile,
    store: ae_vm_aevm::store::OrderProfile,
    channel_atoms: Option<BTreeSet<Vec<u8>>>,
}
fn field<'a>(object: &'a Map<String, Value>, key: &'static str) -> Result<&'a Value, Error> {
    object.get(key).ok_or(Error::Field(key))
}
fn text<'a>(object: &'a Map<String, Value>, key: &'static str) -> Result<&'a str, Error> {
    field(object, key)?.as_str().ok_or(Error::Field(key))
}
fn parse_word(object: &Map<String, Value>, key: &'static str) -> Result<WordSize, Error> {
    match field(object, key)?.as_u64() {
        Some(32) => Ok(WordSize::Bits32),
        Some(64) => Ok(WordSize::Bits64),
        _ => Err(Error::Field(key)),
    }
}
impl Specification {
    pub fn from_value(value: &Value) -> Result<Self, Error> {
        super::budget(value).map_err(|_| Error::Limit)?;
        let m = value.as_object().ok_or(Error::Field("profile"))?;
        const FIELDS: [&str; 11] = [
            "version",
            "fate_word_size",
            "fate_mixing",
            "fate_atoms",
            "arithmetic_word_size",
            "unicode",
            "bls",
            "diagnostic",
            "aevm_float",
            "aevm_store",
            "channel_state_atoms",
        ];
        if m.len() != FIELDS.len() || m.keys().any(|k| !FIELDS.contains(&k.as_str())) {
            return Err(Error::Field("profile fields"));
        }
        if field(m, "version")?.as_u64() != Some(1) {
            return Err(Error::Field("version"));
        }
        if text(m, "unicode")? != "otp26-unicode15" {
            return Err(Error::Field("unicode"));
        }
        if text(m, "bls")? != "mcl11976be-llvm64-vint-arm64" {
            return Err(Error::Field("bls"));
        }
        if text(m, "aevm_float")? != "otp26-word64" {
            return Err(Error::Field("aevm_float"));
        }
        let word = parse_word(m, "fate_word_size")?;
        let arithmetic = parse_word(m, "arithmetic_word_size")?;
        let mixing = match text(m, "fate_mixing")? {
            "jenkins" => Mixing::Jenkins,
            "crc32c" => Mixing::Crc32c,
            _ => return Err(Error::Field("fate_mixing")),
        };
        let raw = field(m, "fate_atoms")?
            .as_object()
            .ok_or(Error::Field("fate_atoms"))?;
        let mut atoms = BTreeMap::new();
        for (name, index) in raw {
            let atom = Atom::from_name(name).ok_or(Error::Field("fate_atoms"))?;
            let index = index
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .ok_or(Error::Field("fate_atoms"))?;
            atoms.insert(atom, index);
        }
        Otp26::new(word, mixing, atoms.clone()).map_err(Error::Order)?;
        let diagnostic = match text(m, "diagnostic")? {
            "release" => ae_vm_fate::diagnostic::Profile::Release,
            "test" => ae_vm_fate::diagnostic::Profile::Test,
            _ => return Err(Error::Field("diagnostic")),
        };
        let store = match text(m, "aevm_store")? {
            "otp26-word64-jenkins" => ae_vm_aevm::store::OrderProfile::Otp26Word64Jenkins,
            "otp26-word64-crc32c" => ae_vm_aevm::store::OrderProfile::Otp26Word64Crc32c,
            _ => return Err(Error::Field("aevm_store")),
        };
        let channel_atoms = match field(m, "channel_state_atoms")? {
            Value::Null => None,
            Value::Array(a) => Some(
                a.iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.as_bytes().to_vec())
                            .ok_or(Error::Field("channel_state_atoms"))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            _ => return Err(Error::Field("channel_state_atoms")),
        };
        Ok(Self {
            word,
            mixing,
            atoms,
            arithmetic,
            diagnostic,
            store,
            channel_atoms,
        })
    }
    pub fn missing_atoms(&self) -> Vec<Atom> {
        Atom::ALL
            .into_iter()
            .filter(|a| !self.atoms.contains_key(a))
            .collect()
    }
    // Versioned byte binding, not Debug formatting or host detection. The
    // historical override FILE bytes are bound even if only whitespace changes.
    pub fn canonical(&self, auxiliary: &Loaded, effective_gas_limit: &Unsigned) -> Vec<u8> {
        let word = [bits(self.word)];
        let arithmetic = [bits(self.arithmetic)];
        let mut atoms: Vec<_> = self
            .atoms
            .iter()
            .map(|(a, i)| (a.name().as_bytes(), i.to_be_bytes()))
            .collect();
        atoms.sort_by_key(|(name, _)| *name);
        let atom_rows = atoms
            .iter()
            .map(|(a, i)| Rlp::List(vec![Rlp::Bytes(a), Rlp::Bytes(i)]))
            .collect();
        let channels = match &self.channel_atoms {
            None => Rlp::Bytes(b"unrestricted"),
            Some(a) => Rlp::List(a.iter().map(|a| Rlp::Bytes(a)).collect()),
        };
        let file = match &auxiliary.inputs[1].hash {
            None => Rlp::List(vec![]),
            Some(hash) => Rlp::List(vec![Rlp::Bytes(&hash.0)]),
        };
        rlp::encode(&Rlp::List(vec![
            Rlp::Bytes(b"ae-rust-execution-profile"),
            Rlp::Bytes(&[1]),
            Rlp::Bytes(&word),
            Rlp::Bytes(match self.mixing {
                Mixing::Jenkins => b"jenkins",
                Mixing::Crc32c => b"crc32c",
            }),
            Rlp::List(atom_rows),
            Rlp::Bytes(&arithmetic),
            Rlp::Bytes(b"otp26-unicode15"),
            Rlp::Bytes(b"mcl11976be-llvm64-vint-arm64"),
            Rlp::Bytes(match self.diagnostic {
                ae_vm_fate::diagnostic::Profile::Release => b"release",
                ae_vm_fate::diagnostic::Profile::Test => b"test",
            }),
            Rlp::Bytes(b"otp26-word64"),
            Rlp::Bytes(match self.store {
                ae_vm_aevm::store::OrderProfile::Otp26Word64Jenkins => b"jenkins",
                ae_vm_aevm::store::OrderProfile::Otp26Word64Crc32c => b"crc32c",
            }),
            channels,
            file,
            Rlp::Bytes(effective_gas_limit.as_bytes()),
        ]))
    }
}
fn bits(word: WordSize) -> u8 {
    match word {
        WordSize::Bits32 => 32,
        WordSize::Bits64 => 64,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Resources {
    pub signatures: transactions::SignatureLimits,
    pub channels: ae_execution::channels::unilateral::Limits,
    pub oracles: ae_execution::oracles::abi::Limits,
    pub fate: ae_vm_fate::Limits,
    pub fate_steps: usize,
    pub aevm: ae_vm_aevm::execution::Limits,
}
impl Default for Resources {
    fn default() -> Self {
        Self {
            signatures: Default::default(),
            channels: Default::default(),
            oracles: Default::default(),
            fate: Default::default(),
            fate_steps: 1_000_000,
            aevm: Default::default(),
        }
    }
}
pub struct Bound {
    execution: Owned,
    fingerprint: Hash256,
    canonical: Vec<u8>,
    missing_atoms: Vec<Atom>,
    gas_limit: Unsigned,
    name_claim_bid_timeout: Option<Unsigned>,
}
pub fn name_claim_bid_timeout(
    resolved: &super::resolution::Resolved<'_>,
) -> Result<Option<Unsigned>, Error> {
    use super::resolution::Strategy;
    resolved
        .find(
            &["mining", "name_claim_bid_timeout"],
            &[
                Strategy::User,
                Strategy::Application {
                    app: "aecore",
                    key: "name_claim_bid_timeout",
                },
            ],
        )
        .map_err(Error::Resolution)?
        .map(|v| {
            let number = v
                .as_number()
                .ok_or(Error::Field("name_claim_bid_timeout"))?;
            Unsigned::from_decimal(number.as_str())
                .map_err(|_| Error::Field("name_claim_bid_timeout"))
        })
        .transpose()
}
impl Bound {
    pub fn build(
        specification: &Specification,
        auxiliary: &Loaded,
        network: &NetworkProfile,
        consensus: ConsensusType,
        gas_limit: &Unsigned,
        resources: Resources,
    ) -> Result<Self, Error> {
        Self::build_with_name_auction_timeout(
            specification,
            auxiliary,
            network,
            consensus,
            gas_limit,
            resources,
            None,
        )
    }
    pub fn build_with_name_auction_timeout(
        specification: &Specification,
        auxiliary: &Loaded,
        network: &NetworkProfile,
        consensus: ConsensusType,
        gas_limit: &Unsigned,
        resources: Resources,
        name_claim_bid_timeout: Option<Unsigned>,
    ) -> Result<Self, Error> {
        if auxiliary.network() != network.network_id() {
            return Err(Error::Network);
        }
        let gas_limit = network.block_gas_limit(gas_limit);
        let base = specification.canonical(auxiliary, &gas_limit);
        // Preserve existing bindings when no override is configured. Explicit
        // overrides alter execution, including zero, and must bind the database.
        let canonical = match &name_claim_bid_timeout {
            None => base,
            Some(value) => rlp::encode(&Rlp::List(vec![
                Rlp::Bytes(b"ae-rust-execution-auction-v1"),
                Rlp::Bytes(&base),
                Rlp::Bytes(value.as_bytes()),
            ])),
        };
        let fingerprint = ae_crypto::blake2b_256(&canonical);
        let order = Otp26::new(
            specification.word,
            specification.mixing,
            specification.atoms.clone(),
        )
        .map_err(Error::Order)?;
        let history = Arc::default();
        let context = machine::Context {
            current_contract: [0; 32],
            vm_version: ae_vm_fate::types::Version::Three,
            in_auth: false,
            on_chain: true,
            reserved_gas: 0.into(),
            arithmetic_word_size: specification.arithmetic,
            unicode_profile: ae_vm_fate::unicode::Profile::Otp26Unicode15,
            bls_profile: ae_crypto::bls::Profile::Mcl11976beLlvm64VintArm64,
        };
        let gas = num_bigint::BigInt::from_bytes_be(num_bigint::Sign::Plus, gas_limit.as_bytes());
        let environment = context::Environment {
            auth_tx_hash: None,
            origin: [0; 32],
            gas_price: 0.into(),
            fee: 0.into(),
            beneficiary: [0; 32],
            timestamp: 0.into(),
            difficulty: 0.into(),
            block_gas_limit: gas.clone(),
            network_id: network.network_id().to_vec(),
            consensus: match consensus {
                ConsensusType::Pow => context::Consensus::Pow,
                ConsensusType::Pos => context::Consensus::Pos,
            },
            key_hash: [0; 32],
        };
        let fate = fate::Config {
            context: &context,
            order: &order,
            overrides: &auxiliary.overrides,
            environment: &environment,
            history: Arc::clone(&history),
            auth_tx: None,
            metering_floor: None,
            diagnostic_profile: specification.diagnostic,
        };
        let aevm = aevm::Config {
            environment: aevm::Environment {
                genesis_height: 0,
                key_hash: [0; 32],
                history,
                beneficiary: AccountPublicKey([0; 32]),
                difficulty: 0.into(),
                timestamp_ms: 0.into(),
                block_gas_limit: gas,
                auth_tx_hash: None,
            },
            network_id: network.network_id().into(),
            event_context: ae_execution::aevm_chain::EventContext::None,
            float_profile: ae_vm_aevm::gas::FloatProfile::Otp26Word64,
            store_order: specification.store,
        };
        let execution = Owned::capture(&transactions::Config {
            network,
            consensus,
            fate: Some(&fate),
            aevm: Some(&aevm),
            signature_limits: resources.signatures,
            channel_limits: resources.channels,
            name_claim_bid_timeout: name_claim_bid_timeout.as_ref(),
            channel_state_atoms: specification.channel_atoms.as_ref(),
            oracle_limits: resources.oracles,
            fate_limits: resources.fate,
            maximum_fate_steps: resources.fate_steps,
            aevm_limits: resources.aevm,
        })
        .map_err(Error::Owned)?;
        Ok(Self {
            execution,
            fingerprint,
            canonical,
            missing_atoms: specification.missing_atoms(),
            gas_limit,
            name_claim_bid_timeout,
        })
    }
    pub fn execution(&self) -> &Owned {
        &self.execution
    }
    pub fn fingerprint(&self) -> Hash256 {
        self.fingerprint
    }
    pub fn canonical(&self) -> &[u8] {
        &self.canonical
    }
    pub fn missing_atoms(&self) -> &[Atom] {
        &self.missing_atoms
    }
    pub fn effective_gas_limit(&self) -> &Unsigned {
        &self.gas_limit
    }
    pub fn name_claim_bid_timeout(&self) -> Option<&Unsigned> {
        self.name_claim_bid_timeout.as_ref()
    }
}
