// Consensus dispatch and startup checks, aec_consensus.erl at fa2a6d7.
use super::{
    MAX_BYTES, MAX_DEPTH, MAX_NODES, budget,
    resolution::{Resolved, Strategy},
};
use ae_protocol::ConsensusType;
use ae_sync::chain::hyperchain;
use ae_vm_fate::{
    Limits,
    maps::{Traversal, order::binary_indices},
};
use num_bigint::BigInt;
use serde_json::{Value, json};
use std::{collections::BTreeMap, fmt};
use zeroize::Zeroizing;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Resolution(super::resolution::Error),
    Limit,
    Shape,
    Height,
    Genesis,
    Unknown,
    CannotDisable,
    Kind,
    Hyperchain(hyperchain::configuration::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "consensus configuration error: {self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    PowCuckoo,
    OnDemand,
    Hyperchain,
    Unknown,
}
impl Kind {
    pub fn module(self) -> &'static str {
        match self {
            Self::PowCuckoo => "aec_consensus_bitcoin_ng",
            Self::OnDemand => "aec_consensus_on_demand",
            Self::Hyperchain => "aec_consensus_hc",
            Self::Unknown => "undefined",
        }
    }
    fn parse(value: &Value) -> Self {
        match value.as_str() {
            Some("pow_cuckoo") => Self::PowCuckoo,
            Some("on_demand") => Self::OnDemand,
            Some("hyperchain") => Self::Hyperchain,
            _ => Self::Unknown,
        }
    }
}
#[derive(Clone, PartialEq)]
pub struct Specification {
    kind: Kind,
    config: Value,
}
impl fmt::Debug for Specification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsensusSpecification")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}
impl Specification {
    pub fn kind(&self) -> Kind {
        self.kind
    }
    pub fn config(&self) -> &Value {
        &self.config
    }
    pub fn hyperchain(&self) -> Result<hyperchain::configuration::Configuration, Error> {
        if self.kind != Kind::Hyperchain {
            return Err(Error::Kind);
        }
        let bytes = Zeroizing::new(serde_json::to_vec(&self.config).map_err(|_| Error::Shape)?);
        hyperchain::configuration::Configuration::parse(&bytes).map_err(Error::Hyperchain)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Schedule {
    entries: Vec<(BigInt, Specification)>,
}
impl Schedule {
    // Raw native function boundary; file-schema validation precedes this in
    // the node loader. Public network rules do not inspect local overrides.
    pub fn select(network: &[u8], value: Option<&Value>) -> Result<Self, Error> {
        let default = if network == b"ae_dev" {
            Kind::OnDemand
        } else {
            Kind::PowCuckoo
        };
        if matches!(network, b"ae_mainnet" | b"ae_uat") || value.is_none() {
            return Ok(Self {
                entries: vec![(
                    0.into(),
                    Specification {
                        kind: default,
                        config: json!({}),
                    },
                )],
            });
        }
        let value = value.expect("present");
        budget(value).map_err(|_| Error::Limit)?;
        let object = value.as_object().ok_or(Error::Shape)?;
        if object.len() > 1024 {
            return Err(Error::Limit);
        }
        let entries: Vec<_> = object.iter().collect();
        let keys: Vec<_> = entries.iter().map(|(k, _)| k.as_bytes()).collect();
        let order = binary_indices(
            &keys,
            Traversal::Fold,
            Limits {
                max_nodes: MAX_NODES,
                max_bytes: MAX_BYTES,
                max_depth: MAX_DEPTH,
            },
        )
        .map_err(|_| Error::Limit)?;
        let mut normalized = BTreeMap::new();
        for i in order {
            let (height, v) = entries[i];
            let object = v.as_object().ok_or(Error::Shape)?;
            let kind = Kind::parse(object.get("type").ok_or(Error::Shape)?);
            let height = integer_key(height).ok_or(Error::Height)?;
            let config = object.get("config").cloned().unwrap_or_else(|| json!({}));
            normalized.insert(height, Specification { kind, config });
        }
        Ok(Self {
            entries: normalized.into_iter().collect(),
        })
    }
    pub fn entries(&self) -> &[(BigInt, Specification)] {
        &self.entries
    }
    // Original consensus_type_from examines only the genesis entry. This is
    // deliberately not inferred from the active height or network name.
    pub fn family(&self) -> ConsensusType {
        if self
            .entries
            .first()
            .is_some_and(|(h, s)| *h == BigInt::from(0) && s.kind == Kind::Hyperchain)
        {
            ConsensusType::Pos
        } else {
            ConsensusType::Pow
        }
    }
    pub fn at(&self, height: &BigInt) -> Result<&Specification, Error> {
        if self
            .entries
            .first()
            .is_none_or(|(h, _)| *h != BigInt::from(0))
        {
            return Err(Error::Genesis);
        }
        let index = self
            .entries
            .partition_point(|(h, _)| h <= height)
            .saturating_sub(1);
        Ok(&self.entries[index].1)
    }
    pub fn check(self) -> Result<Checked, Error> {
        if self
            .entries
            .first()
            .is_none_or(|(h, _)| *h != BigInt::from(0))
        {
            return Err(Error::Genesis);
        }
        if self.entries.iter().any(|(_, s)| s.kind == Kind::Unknown) {
            return Err(Error::Unknown);
        }
        // Only PoW can be turned off. All three native assert_config
        // callbacks currently accept any value; service startup validates it.
        if self.entries[..self.entries.len() - 1]
            .iter()
            .any(|(_, s)| s.kind != Kind::PowCuckoo)
        {
            return Err(Error::CannotDisable);
        }
        Ok(Checked(self))
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Checked(Schedule);
impl Checked {
    pub fn entries(&self) -> &[(BigInt, Specification)] {
        self.0.entries()
    }
    pub fn family(&self) -> ConsensusType {
        self.0.family()
    }
    pub fn at(&self, height: &BigInt) -> &Specification {
        self.0.at(height).expect("checked genesis")
    }
    pub fn genesis(&self) -> &Specification {
        &self.0.entries[0].1
    }
}
impl Resolved<'_> {
    pub fn consensus(&self, network: &[u8]) -> Result<Schedule, Error> {
        if matches!(network, b"ae_mainnet" | b"ae_uat") {
            return Schedule::select(network, None);
        }
        let value = self
            .find(
                &["chain", "consensus"],
                &[
                    Strategy::Application {
                        app: "aecore",
                        key: "consensus",
                    },
                    Strategy::User,
                ],
            )
            .map_err(Error::Resolution)?;
        Schedule::select(network, value.as_ref())
    }
}
pub(super) fn integer_key(text: &str) -> Option<BigInt> {
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || digits.len() > 4096 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}
