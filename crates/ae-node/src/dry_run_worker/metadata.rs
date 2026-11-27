use super::codec::{Decoder, Encoder, Error, Wire, enumeration, structure};
use ae_execution::{Context, chain_history, contracts, transactions};
use ae_protocol::{NetworkProfile, Protocol};
use ae_vm_fate::{
    machine,
    maps::{
        listing::Overrides,
        order::{Atom, Mixing, Otp26, WordSize},
    },
};
use std::{collections::BTreeSet, sync::Arc};

enumeration!(Protocol{Roma=1,Minerva=2,Fortuna=3,Lima=4,Iris=5,Ceres=6,Arcus=7,Salus=8});
enumeration!(ae_protocol::ConsensusType{Pow=0,Pos=1});
enumeration!(ae_execution::Origin{Transaction=0,Contract=1,Generalized=2});
enumeration!(ae_execution::dry_run::Profile{Public=0,Internal=1,Replay=2,Includability=3,Unspecified=4,Other=5});
enumeration!(machine::program::remote::context::Consensus{Pow=0,Pos=1});
enumeration!(WordSize{Bits32=0,Bits64=1});
enumeration!(Mixing{Jenkins=0,Crc32c=1});
enumeration!(ae_vm_fate::types::Version{One=5,Two=7,Three=8});
enumeration!(ae_vm_fate::unicode::Profile{Otp26Unicode15=0});
enumeration!(ae_crypto::bls::Profile{Mcl11976beLlvm64VintArm64=0});
enumeration!(ae_vm_fate::diagnostic::Profile{Release=0,Test=1});
enumeration!(ae_vm_aevm::gas::FloatProfile{Otp26Word64=0});
enumeration!(ae_vm_aevm::store::OrderProfile{Otp26Word64Jenkins=0,Otp26Word64Crc32c=1});
structure!(ae_protocol::Activation { height, protocol });
structure!(ae_protocol::signaling::Fork {
    start,
    end,
    threshold,
    info,
    version
});
structure!(Context {
    name_claim_bid_timeout,
    height,
    protocol,
    origin,
    dry_run,
    payer,
    ga_authenticated,
    ga_nonces,
    ga_transaction,
    ga_transaction_hash
});
structure!(machine::Context {
    current_contract,
    vm_version,
    in_auth,
    on_chain,
    reserved_gas,
    arithmetic_word_size,
    unicode_profile,
    bls_profile
});
structure!(machine::program::remote::context::Environment {
    auth_tx_hash,
    origin,
    gas_price,
    fee,
    beneficiary,
    timestamp,
    difficulty,
    block_gas_limit,
    network_id,
    consensus,
    key_hash
});
structure!(chain_history::HeaderLink {
    height,
    previous_key
});
structure!(chain_history::Snapshot { selected, headers });
structure!(contracts::aevm::Environment {
    genesis_height,
    key_hash,
    history,
    beneficiary,
    difficulty,
    timestamp_ms,
    block_gas_limit,
    auth_tx_hash
});
structure!(contracts::aevm::Config {
    control,
    environment,
    network_id,
    event_context,
    float_profile,
    store_order
});
structure!(ae_state::StateRoots {
    accounts,
    calls,
    channels,
    contracts,
    names,
    oracles
});
structure!(ae_chain::repository::Roots {
    state,
    names,
    oracles
});
structure!(ae_codec::rlp::DecodeLimits {
    max_input_bytes,
    max_items,
    max_depth
});
structure!(ae_tx::TransactionLimits {
    rlp,
    max_layers,
    max_total_bytes
});
structure!(ae_state::transport::Limits {
    max_input_bytes,
    max_items,
    max_depth,
    max_entries,
    max_key_bytes,
    max_work_bytes
});
structure!(ae_vm_fate::Limits {
    max_depth,
    max_nodes,
    max_bytes
});
structure!(ae_vm_aevm::heap::Limits {
    max_depth,
    max_nodes,
    max_bytes
});
structure!(ae_vm_aevm::execution::Limits {
    max_work,
    max_bytes,
    max_memory_words,
    max_depth
});
structure!(transactions::SignatureLimits {
    max_layers,
    max_encoded_bytes
});
structure!(ae_execution::channels::unilateral::Limits { transaction, state });
structure!(ae_execution::oracles::abi::Limits { aevm, fate });
structure!(ae_execution::dry_run::Options {
    control,
    profile,
    store_read_gas_metering,
    tx_events,
    gas_limit,
    max_transactions,
    max_accounts,
    max_funding_bytes
});
structure!(ae_execution::dry_run::prepare::Limits {
    requests,
    encoded_bytes,
    number_chars,
    work_bytes,
    transaction
});
structure!(crate::http::dry_run::response::Limits {
    results,
    events,
    json_bytes,
    transaction,
    diagnostic
});
structure!(crate::http::transaction::Limits {
    layers,
    json_bytes,
    work_bytes
});
structure!(ae_trie::OverlayLimits { nodes, bytes });
structure!(ae_execution::dry_run::Funding { account, amount });

impl Wire for NetworkProfile {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        self.network_id().to_vec().put(e)?;
        self.activations().to_vec().put(e)?;
        self.community_fork().put(e)?;
        self.resolution().put(e)
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        let id = Vec::<u8>::get(d)?;
        let activations = Vec::<ae_protocol::Activation>::get(d)?;
        let fork = Option::<ae_protocol::signaling::Fork>::get(d)?;
        let resolution = Option::<(u64, u32)>::get(d)?;
        let mut profile = match id.as_slice() {
            b"ae_mainnet" => Self::mainnet(),
            b"ae_uat" => Self::uat(),
            _ => Self::custom(id, activations.clone()).map_err(|_| Error::Invalid)?,
        };
        if profile.activations() != activations {
            return Err(Error::Invalid);
        }
        profile = profile
            .with_community_fork(fork)
            .map_err(|_| Error::Invalid)?;
        if let Some((height, version)) = resolution {
            let fork = fork.ok_or(Error::Invalid)?;
            let prior = if height == fork.end {
                ae_protocol::signaling::Prior::SignalingResult(version == fork.version)
            } else {
                ae_protocol::signaling::Prior::Version(version)
            };
            profile = profile.resolve(height, prior).map_err(|_| Error::Invalid)?;
            if profile.resolution() != resolution {
                return Err(Error::Invalid);
            }
        }
        Ok(profile)
    }
}
impl Wire for Atom {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        self.name().to_owned().put(e)
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        Self::from_name(&String::get(d)?).ok_or(Error::Invalid)
    }
}
impl Wire for Otp26 {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        let (word, mixing, atoms) = self.components();
        word.put(e)?;
        mixing.put(e)?;
        atoms.put(e)
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        Self::new(Wire::get(d)?, Wire::get(d)?, Wire::get(d)?).map_err(|_| Error::Invalid)
    }
}
impl Wire for ae_tx::WireTransaction {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        e.blob(&self.encode())
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        let bytes = d.blob()?;
        Self::decode_with_limits(
            bytes,
            ae_codec::rlp::DecodeLimits {
                max_input_bytes: bytes.len(),
                max_items: 65536,
                max_depth: 64,
            },
        )
        .map_err(|_| Error::Invalid)
    }
}
impl Wire for ae_tx::TransactionKind {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        (self.tag() as u32).put(e)
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        let tag = ae_codec::object::ObjectTag::try_from(u32::get(d)? as u64)
            .map_err(|_| Error::Invalid)?;
        Self::from_tag(tag).ok_or(Error::Invalid)
    }
}
impl Wire for ae_execution::aevm_chain::EventContext {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::None => false.put(e),
            Self::Transaction { kind, hash } => {
                true.put(e)?;
                kind.put(e)?;
                hash.put(e)
            }
        }
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok(if bool::get(d)? {
            Self::Transaction {
                kind: Wire::get(d)?,
                hash: Wire::get(d)?,
            }
        } else {
            Self::None
        })
    }
}
impl Wire for Overrides {
    fn put(&self, e: &mut Encoder) -> Result<(), Error> {
        self.entries().len().put(e)?;
        for (k, v) in self.entries() {
            for value in [k, v] {
                let limits = ae_vm_fate::Limits {
                    max_bytes: e.available().saturating_sub(8),
                    max_nodes: 1_000_000,
                    max_depth: 128,
                };
                ae_vm_fate::value::check_limits(value, limits).map_err(|_| Error::Capacity)?;
                e.blob(&ae_vm_fate::encoding::encode(value, limits).map_err(|_| Error::Capacity)?)?;
            }
        }
        Ok(())
    }
    fn get(d: &mut Decoder<'_>) -> Result<Self, Error> {
        let n = usize::get(d)?;
        if n > 65536 {
            return Err(Error::Capacity);
        }
        let mut limits = ae_vm_fate::Limits {
            max_bytes: 64 << 20,
            max_nodes: 1_000_000,
            max_depth: 128,
        };
        let mut out = Self::default();
        for _ in 0..n {
            let k = d.blob()?;
            let v = d.blob()?;
            out.insert_encoded(k, v, &mut limits)
                .map_err(|_| Error::Invalid)?;
        }
        Ok(out)
    }
}

pub(super) struct Fate {
    pub control: Option<ae_types::work::Control>,
    pub context: machine::Context,
    pub order: Otp26,
    pub overrides: Overrides,
    pub environment: machine::program::remote::context::Environment,
    pub history: Arc<chain_history::Snapshot>,
    pub auth_tx: Option<Arc<ae_tx::WireTransaction>>,
    pub metering_floor: Option<u32>,
    pub diagnostic_profile: ae_vm_fate::diagnostic::Profile,
}
structure!(Fate {
    control,
    context,
    order,
    overrides,
    environment,
    history,
    auth_tx,
    metering_floor,
    diagnostic_profile
});
pub(super) struct Execution {
    pub control: Option<ae_types::work::Control>,
    pub name_claim_bid_timeout: Option<ae_types::Unsigned>,
    pub network: NetworkProfile,
    pub consensus: ae_protocol::ConsensusType,
    pub fate: Option<Fate>,
    pub aevm: Option<contracts::aevm::Config>,
    pub signature_limits: transactions::SignatureLimits,
    pub channel_limits: ae_execution::channels::unilateral::Limits,
    pub channel_state_atoms: Option<BTreeSet<Vec<u8>>>,
    pub oracle_limits: ae_execution::oracles::abi::Limits,
    pub fate_limits: ae_vm_fate::Limits,
    pub maximum_fate_steps: usize,
    pub aevm_limits: ae_vm_aevm::execution::Limits,
}
structure!(Execution {
    control,
    name_claim_bid_timeout,
    network,
    consensus,
    fate,
    aevm,
    signature_limits,
    channel_limits,
    channel_state_atoms,
    oracle_limits,
    fate_limits,
    maximum_fate_steps,
    aevm_limits
});
impl Execution {
    pub fn capture(c: &transactions::Config<'_>) -> Self {
        Self {
            control: c.control.cloned(),
            name_claim_bid_timeout: c.name_claim_bid_timeout.cloned(),
            network: c.network.clone(),
            consensus: c.consensus,
            fate: c.fate.map(|f| Fate {
                control: f.control.clone(),
                context: f.context.clone(),
                order: f.order.clone(),
                overrides: f.overrides.clone(),
                environment: f.environment.clone(),
                history: f.history.clone(),
                auth_tx: f.auth_tx.clone(),
                metering_floor: f.metering_floor,
                diagnostic_profile: f.diagnostic_profile,
            }),
            aevm: c.aevm.cloned(),
            signature_limits: c.signature_limits,
            channel_limits: c.channel_limits,
            channel_state_atoms: c.channel_state_atoms.cloned(),
            oracle_limits: c.oracle_limits,
            fate_limits: c.fate_limits,
            maximum_fate_steps: c.maximum_fate_steps,
            aevm_limits: c.aevm_limits,
        }
    }
    pub fn with_config<T>(&self, f: impl FnOnce(&transactions::Config<'_>) -> T) -> T {
        let fate = self.fate.as_ref().map(|x| contracts::fate::Config {
            control: x.control.clone(),
            context: &x.context,
            order: &x.order,
            overrides: &x.overrides,
            environment: &x.environment,
            history: x.history.clone(),
            auth_tx: x.auth_tx.clone(),
            metering_floor: x.metering_floor,
            diagnostic_profile: x.diagnostic_profile,
        });
        f(&transactions::Config {
            control: self.control.as_ref(),
            name_claim_bid_timeout: self.name_claim_bid_timeout.as_ref(),
            network: &self.network,
            consensus: self.consensus,
            fate: fate.as_ref(),
            aevm: self.aevm.as_ref(),
            signature_limits: self.signature_limits,
            channel_limits: self.channel_limits,
            channel_state_atoms: self.channel_state_atoms.as_ref(),
            oracle_limits: self.oracle_limits,
            fate_limits: self.fate_limits,
            maximum_fate_steps: self.maximum_fate_steps,
            aevm_limits: self.aevm_limits,
        })
    }
}
