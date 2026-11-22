// Rust-owned persistence schema. Consensus blocks and authenticated nodes keep
// their original encodings; this database is not an Erlang/Mnesia directory.
use crate::{block::Block, fraud};
use ae_crypto::blake2b_256;
use ae_protocol::{ConsensusType, NetworkProfile};
use ae_state::{
    StateRoots, Trees, accounts::Accounts, calls::Calls, channels::Channels, contracts::Contracts,
    names::Names, oracles::Oracles,
};
use ae_trie::{NodeReader, Overlay, OverlayLimits, Value};
use ae_types::{Hash256, Unsigned};
use std::collections::BTreeMap;
mod blocks;
mod codec;
mod decoding;
mod history;
pub mod hyperchain;
pub mod retention;
pub mod rollback;
pub mod scan;
pub mod serving;
mod signaling;
use codec::*;
pub use decoding::ReadConfig;
pub use history::vm_history;
pub mod ledger;
pub use ledger::{
    HyperchainConfig, HyperchainError, InsertError, Limits, Update, bootstrap, prepare,
    prepare_hyperchain,
};

pub const IDENTITY: &[u8] = b"\0identity";
pub const STATUS: &[u8] = b"\0status";
pub const FINALIZED: &[u8] = b"\0finalized";
pub const NODES: u8 = 1;
pub const BLOCKS: u8 = 2;
pub const RECORDS: u8 = 3;
pub const CHILDREN: u8 = 4;
pub const HEIGHTS: u8 = 5;
pub const CANONICAL: u8 = 6;
pub const KEYS: u8 = 7;
pub const TRANSACTIONS: u8 = 8;
pub const LOCATIONS: u8 = 9;
pub const ENDS: u8 = 10;
pub const EVENTS: u8 = 11;
pub const PROOFS: u8 = 12;
pub const SIGNALS: u8 = 13;
pub const GC_MARKS: u8 = 14;
pub const GC_PENDING: u8 = 15;
// 16..=18 belong to the storage pool and peer repositories.
pub const HC_EVENTS: u8 = 19;
pub const HC_STATE: &[u8] = b"\0hc-state";
pub const GC_STATE: &[u8] = b"\0gc";
pub fn key(prefix: u8, suffix: impl AsRef<[u8]>) -> Vec<u8> {
    let mut k = vec![prefix];
    k.extend_from_slice(suffix.as_ref());
    k
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Backend(String),
    Corrupt(&'static str),
    Missing(Hash256),
    MissingCanonicalKey(u64),
    ResourceLimit,
    UnsupportedBlock,
    TransactionStorage(ae_tx::storage::Error),
    TransactionMetadataConflict,
    Identity,
    Conflict,
    PolicyOwned,
    PolicyClosed,
    RecoveryRequired,
    GcActive,
    IncompleteImport,
    Pruned { height: u64, floor: u64 },
    AlreadyPresent,
    Uninitialized,
    NotEmpty,
    Protocol(ae_protocol::ProfileError),
    NoCommunityFork,
    NotLastSignalingBlock,
    Trie(ae_trie::Error),
}
impl From<ae_trie::Error> for Error {
    fn from(e: ae_trie::Error) -> Self {
        Self::Trie(e)
    }
}
impl From<ae_tx::storage::Error> for Error {
    fn from(e: ae_tx::storage::Error) -> Self {
        Self::TransactionStorage(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
// Implementations must provide immutable, consistent reads for their lifetime.
// Prefix scans fail on exhaustion; a truncated result is never a complete view.
pub type Entries = Vec<(Vec<u8>, Vec<u8>)>;
pub type Visitor<'a> = dyn FnMut(&[u8], &[u8]) -> Result<(), Error> + 'a;
pub trait Read: NodeReader + Clone {
    fn source_id(&self) -> u64;
    fn resilience(&self) -> crate::resilience::Snapshot {
        Default::default()
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    fn prefix(&self, prefix: &[u8], limit: usize) -> Result<Entries, Error>;
    fn range(&self, start: &[u8], end: &[u8], limit: usize) -> Result<Entries, Error>;
    fn visit_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        visit: &mut Visitor<'_>,
    ) -> Result<(), Error>;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub consensus: ConsensusType,
    pub network: Vec<u8>,
    pub genesis: Hash256,
    pub protocols: Hash256,
    pub execution_profile: Hash256,
}
impl Identity {
    pub fn new(network: &NetworkProfile, genesis: Hash256, execution_profile: Hash256) -> Self {
        Self::for_consensus(network, genesis, execution_profile, ConsensusType::Pow)
    }
    pub fn for_consensus(
        network: &NetworkProfile,
        genesis: Hash256,
        execution_profile: Hash256,
        consensus: ConsensusType,
    ) -> Self {
        Self {
            consensus,
            network: network.network_id().into(),
            genesis,
            protocols: protocols(network),
            execution_profile,
        }
    }
    pub fn matches(&self, network: &NetworkProfile) -> bool {
        self.network == network.network_id() && self.protocols == protocols(network)
    }
    pub fn encode(&self) -> Vec<u8> {
        self.encode_version(11)
    }
    // Archive ownership was introduced with version 7. Bind the identity
    // fields using that stable layout, independently of writer capabilities.
    pub fn binding(&self) -> Vec<u8> {
        match self.consensus {
            ConsensusType::Pow => self.encode_version(7),
            ConsensusType::Pos => self.encode_version(10),
        }
    }
    pub fn header_mode(&self) -> ae_codec::headers::HeaderMode {
        match self.consensus {
            ConsensusType::Pow => ae_codec::headers::HeaderMode::ProofOfWork,
            ConsensusType::Pos => ae_codec::headers::HeaderMode::Hyperchain,
        }
    }
    fn encode_version(&self, version: u64) -> Vec<u8> {
        let mut fields = vec![
            number(version),
            binary(&self.network),
            binary(self.genesis.0),
            binary(self.protocols.0),
            binary(self.execution_profile.0),
        ];
        if version >= 10 {
            fields.push(number(match self.consensus {
                ConsensusType::Pow => 0,
                ConsensusType::Pos => 1,
            }));
        }
        list(fields).encode()
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let v = decode(bytes)?;
        let all = items(&v)?;
        let version = integer(all.first().ok_or(Error::Corrupt("identity fields"))?)?;
        if !matches!(version, 1..=11) {
            return Err(Error::Corrupt("schema version"));
        }
        let a = fields(&v, if version >= 10 { 6 } else { 5 })?;
        let consensus = if version < 10 {
            ConsensusType::Pow
        } else {
            match integer(&a[5])? {
                0 => ConsensusType::Pow,
                1 => ConsensusType::Pos,
                _ => return Err(Error::Corrupt("identity consensus")),
            }
        };
        let network = blob(&a[1])?.to_vec();
        if network.is_empty() || network.len() > 1024 {
            return Err(Error::Identity);
        }
        let id = Self {
            consensus,
            network,
            genesis: hash(&a[2])?,
            protocols: hash(&a[3])?,
            execution_profile: hash(&a[4])?,
        };
        if id.encode_version(version) != bytes {
            return Err(Error::Corrupt("identity encoding"));
        }
        Ok(id)
    }
}
fn protocols(p: &NetworkProfile) -> Hash256 {
    let v = list(
        p.activations()
            .iter()
            .map(|a| list(vec![number(a.height), number(a.protocol as u32 as u64)]))
            .collect(),
    );
    let v = if let Some(f) = p.community_fork() {
        list(vec![
            binary(b"community_fork"),
            v,
            list(vec![
                number(f.start),
                number(f.end),
                number(f.threshold),
                number(f.info.into()),
                number(f.version.into()),
            ]),
        ])
    } else {
        v
    };
    blake2b_256(&v.encode())
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Status {
    pub revision: u64,
    pub tip: Option<Hash256>,
}
impl Status {
    pub fn encode(self) -> Vec<u8> {
        list(vec![number(self.revision), optional(self.tip)]).encode()
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let v = decode(bytes)?;
        let a = fields(&v, 2)?;
        let s = Self {
            revision: integer(&a[0])?,
            tip: optional_hash(&a[1])?,
        };
        if (s.revision == 0) != s.tip.is_none() || s.encode() != bytes {
            return Err(Error::Corrupt("status"));
        }
        Ok(s)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Roots {
    pub state: StateRoots,
    pub names: Option<Hash256>,
    pub oracles: Option<Hash256>,
}
impl Roots {
    pub fn of<A: NodeReader, C: NodeReader>(t: &Trees<A, C>) -> Self {
        Self {
            state: t.roots(),
            names: t.names.cache_root_hash(),
            oracles: t.oracles.cache_root_hash(),
        }
    }
    pub fn load<S: NodeReader + Clone>(&self, store: S) -> Result<Trees<S, S>, Error> {
        Ok(Trees {
            accounts: Accounts::from_root(self.state.accounts, store.clone())
                .map_err(|_| Error::Corrupt("account root"))?,
            calls: Calls::from_root(self.state.calls, store.clone())
                .map_err(|_| Error::Corrupt("call root"))?,
            contracts: Contracts::from_root(self.state.contracts, store.clone())
                .map_err(|_| Error::Corrupt("contract root"))?,
            channels: Channels::from_root(self.state.channels, store.clone())
                .map_err(|_| Error::Corrupt("channel root"))?,
            names: Names::from_roots(self.state.names, self.names, store.clone(), store.clone())
                .map_err(|_| Error::Corrupt("name roots"))?,
            oracles: Oracles::from_roots(self.state.oracles, self.oracles, store.clone(), store)
                .map_err(|_| Error::Corrupt("oracle roots"))?,
        })
    }
    pub fn array(&self) -> [Option<Hash256>; 8] {
        [
            self.state.accounts,
            self.state.calls,
            self.state.channels,
            self.state.contracts,
            self.state.names,
            self.state.oracles,
            self.names,
            self.oracles,
        ]
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub roots: Roots,
    pub difficulty: Unsigned,
    pub fees: Unsigned,
    pub fraud: bool,
    pub fork: Hash256,
}
impl Record {
    pub fn encode(&self) -> Vec<u8> {
        let mut v = vec![number(1)];
        v.extend(self.roots.array().into_iter().map(optional));
        v.extend([
            binary(self.difficulty.as_bytes()),
            binary(self.fees.as_bytes()),
            number(u64::from(self.fraud)),
            binary(self.fork.0),
        ]);
        list(v).encode()
    }
    pub fn decode(b: &[u8]) -> Result<Self, Error> {
        let v = decode(b)?;
        let a = fields(&v, 13)?;
        if integer(&a[0])? != 1 {
            return Err(Error::Corrupt("record version"));
        }
        let mut roots = [None; 8];
        for (r, v) in roots.iter_mut().zip(&a[1..9]) {
            *r = optional_hash(v)?;
        }
        let [accounts, calls, channels, contracts, names, oracles, nc, oc] = roots;
        let fraud = match integer(&a[11])? {
            0 => false,
            1 => true,
            _ => return Err(Error::Corrupt("fraud flag")),
        };
        let r = Self {
            roots: Roots {
                state: StateRoots {
                    accounts,
                    calls,
                    channels,
                    contracts,
                    names,
                    oracles,
                },
                names: nc,
                oracles: oc,
            },
            difficulty: unsigned(&a[9])?,
            fees: unsigned(&a[10])?,
            fraud,
            fork: hash(&a[12])?,
        };
        if r.encode() != b {
            return Err(Error::Corrupt("record encoding"));
        }
        Ok(r)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub revision: u64,
    pub inserted: Option<Hash256>,
    pub removed: Vec<Hash256>,
    pub discarded: Vec<Hash256>,
    pub old_tip: Option<Hash256>,
    pub new_tip: Hash256,
    pub ancestor: Option<Hash256>,
    // Detached tip first; attached ancestor child first.
    pub detached: Vec<Hash256>,
    pub attached: Vec<Hash256>,
    pub requeue: Vec<Hash256>,
    pub confirm: Vec<(Hash256, Hash256)>,
}
impl Change {
    pub fn encode(&self) -> Vec<u8> {
        self.encode_version(2)
    }
    fn encode_version(&self, version: u64) -> Vec<u8> {
        let mut fields = vec![
            number(version),
            number(self.revision),
            if version == 1 {
                binary(self.inserted.expect("legacy insertion").0)
            } else {
                optional(self.inserted)
            },
            optional(self.old_tip),
            binary(self.new_tip.0),
            optional(self.ancestor),
            list(self.detached.iter().map(|h| binary(h.0)).collect()),
            list(self.attached.iter().map(|h| binary(h.0)).collect()),
            list(self.requeue.iter().map(|h| binary(h.0)).collect()),
            list(
                self.confirm
                    .iter()
                    .map(|(t, b)| list(vec![binary(t.0), binary(b.0)]))
                    .collect(),
            ),
        ];
        if version == 2 {
            fields.push(list(self.removed.iter().map(|h| binary(h.0)).collect()));
            fields.push(list(self.discarded.iter().map(|h| binary(h.0)).collect()));
        }
        list(fields).encode()
    }
    pub fn decode(b: &[u8]) -> Result<Self, Error> {
        let value = decode(b)?;
        let all = items(&value)?;
        let version = integer(all.first().ok_or(Error::Corrupt("empty event"))?)?;
        let a = fields(
            &value,
            match version {
                1 => 10,
                2 => 12,
                _ => return Err(Error::Corrupt("event version")),
            },
        )?;
        let hashes = |v: &Value| {
            items(v)?
                .iter()
                .map(hash)
                .collect::<Result<Vec<_>, Error>>()
        };
        let c = Self {
            revision: integer(&a[1])?,
            inserted: if version == 1 {
                Some(hash(&a[2])?)
            } else {
                optional_hash(&a[2])?
            },
            old_tip: optional_hash(&a[3])?,
            new_tip: hash(&a[4])?,
            ancestor: optional_hash(&a[5])?,
            detached: hashes(&a[6])?,
            attached: hashes(&a[7])?,
            requeue: hashes(&a[8])?,
            confirm: items(&a[9])?
                .iter()
                .map(|p| {
                    let a = fields(p, 2)?;
                    Ok((hash(&a[0])?, hash(&a[1])?))
                })
                .collect::<Result<_, Error>>()?,
            removed: if version == 1 {
                vec![]
            } else {
                hashes(&a[10])?
            },
            discarded: if version == 1 {
                vec![]
            } else {
                hashes(&a[11])?
            },
        };
        if c.removed.windows(2).any(|w| w[0] >= w[1])
            || c.discarded.windows(2).any(|w| w[0] >= w[1])
            || c.removed.contains(&c.new_tip)
            || c.encode_version(version) != b
            || (c.inserted.is_some() && (!c.removed.is_empty() || !c.discarded.is_empty()))
            || (c.inserted.is_none()
                && (c.old_tip.is_none()
                    || c.ancestor != Some(c.new_tip)
                    || !c.attached.is_empty()
                    || !c.requeue.is_empty()
                    || !c.confirm.is_empty()))
        {
            return Err(Error::Corrupt("event encoding"));
        }
        Ok(c)
    }
}

#[derive(Debug, Clone)]
pub struct View<S> {
    source: S,
    identity: Identity,
    status: Status,
    config: ReadConfig,
}
impl<S: Read> View<S> {
    pub fn load(source: S) -> Result<Self, Error> {
        Self::load_with_config(source, ReadConfig::default())
    }
    pub fn load_with_config(source: S, config: ReadConfig) -> Result<Self, Error> {
        let identity = Identity::decode(
            &source
                .get(IDENTITY)?
                .ok_or(Error::Corrupt("missing identity"))?,
        )?;
        if identity.header_mode() != config.mode {
            return Err(Error::Identity);
        }
        let status = Status::decode(
            &source
                .get(STATUS)?
                .ok_or(Error::Corrupt("missing status"))?,
        )?;
        Ok(Self {
            source,
            identity,
            status,
            config,
        })
    }
    pub fn config(&self) -> &ReadConfig {
        &self.config
    }
    pub fn resilience(&self) -> crate::resilience::Snapshot {
        self.source.resilience()
    }
    pub fn finalized_height(&self) -> Result<Option<u64>, Error> {
        self.source
            .get(FINALIZED)?
            .map(|b| {
                let v = decode(&b)?;
                let f = fields(&v, 2)?;
                let h = integer(&f[1])?;
                if integer(&f[0])? != 1 || encode_finalized(h)? != b {
                    return Err(Error::Corrupt("finalized height"));
                }
                Ok(h)
            })
            .transpose()
    }
    pub fn identity(&self) -> &Identity {
        &self.identity
    }
    pub fn status(&self) -> Status {
        self.status
    }
    pub fn source(&self) -> &S {
        &self.source
    }
    pub fn block(&self, h: Hash256) -> Result<Option<Block>, Error> {
        self.source
            .get(&key(BLOCKS, h.0))?
            .map(|b| {
                let block = blocks::decode(self, &b)?;
                if block.hash().map_err(|_| Error::Corrupt("block hash"))? != h {
                    return Err(Error::Corrupt("block identity"));
                }
                Ok(block)
            })
            .transpose()
    }
    pub fn transaction(
        &self,
        hash: Hash256,
    ) -> Result<Option<ae_tx::SignedTransaction<ae_tx::Transaction>>, Error> {
        self.source
            .get(&key(TRANSACTIONS, hash.0))?
            .map(|b| {
                let config = self.config.transaction_record();
                if b.len() > config.max_record_bytes {
                    return Err(ae_tx::storage::Error::ResourceLimit.into());
                }
                let tx = if b.first() == Some(&0) {
                    ae_tx::storage::decode(&b, config)?
                } else {
                    // Version-1 databases stored canonical signed transport bytes.
                    let t = ae_tx::SignedTransaction::decode_with_state_config(
                        &b,
                        config.transaction,
                        config.state,
                    )
                    .map_err(|e| {
                        Error::TransactionStorage(ae_tx::storage::Error::Transaction(e))
                    })?;
                    if t.encode() != b {
                        return Err(Error::Corrupt("legacy transaction encoding"));
                    }
                    t
                };
                if tx.hash() != hash {
                    return Err(Error::Corrupt("transaction hash"));
                }
                Ok(tx)
            })
            .transpose()
    }
    pub fn require_block(&self, h: Hash256) -> Result<Block, Error> {
        self.block(h)?.ok_or(Error::Missing(h))
    }
    pub fn record(&self, h: Hash256) -> Result<Option<Record>, Error> {
        self.source
            .get(&key(RECORDS, h.0))?
            .map(|b| Record::decode(&b))
            .transpose()
    }
    pub fn require_record(&self, h: Hash256) -> Result<Record, Error> {
        self.record(h)?.ok_or(Error::Missing(h))
    }
    pub fn trees(
        &self,
        h: Hash256,
        limits: OverlayLimits,
    ) -> Result<Trees<Overlay<S>, Overlay<S>>, Error> {
        self.require_retained(self.require_block(h)?.header().common().height)?;
        self.load_trees(h, limits)
    }
    fn load_trees(
        &self,
        h: Hash256,
        limits: OverlayLimits,
    ) -> Result<Trees<Overlay<S>, Overlay<S>>, Error> {
        let r = self.require_record(h)?;
        let b = self.require_block(h)?;
        if r.roots.state.hash().0 != b.header().common().state_hash {
            return Err(Error::Corrupt("state binding"));
        }
        r.roots.load(Overlay::new(self.source.clone(), limits))
    }
    pub fn location(&self, h: Hash256) -> Result<Option<Hash256>, Error> {
        self.source
            .get(&key(LOCATIONS, h.0))?
            .map(|b| fixed_hash(&b))
            .transpose()
    }
    pub fn canonical_key(&self, h: u64) -> Result<Option<Hash256>, Error> {
        self.source
            .get(&key(KEYS, h.to_be_bytes()))?
            .map(|b| fixed_hash(&b))
            .transpose()
    }
    pub fn is_canonical(&self, h: Hash256) -> Result<bool, Error> {
        Ok(self.source.get(&key(CANONICAL, h.0))?.is_some())
    }
    pub fn event(&self, revision: u64) -> Result<Option<Change>, Error> {
        if revision == 0 || revision > self.status.revision {
            return Ok(None);
        }
        let b = self
            .source
            .get(&key(EVENTS, revision.to_be_bytes()))?
            .ok_or(Error::Corrupt("missing committed event"))?;
        let e = Change::decode(&b)?;
        if e.revision != revision {
            return Err(Error::Corrupt("event revision"));
        }
        Ok(Some(e))
    }
    pub fn discovered_proof(&self, h: Hash256) -> Result<Option<fraud::Proof>, Error> {
        self.source
            .get(&key(PROOFS, h.0))?
            .map(|b| {
                fraud::Proof::decode(
                    &b,
                    ae_codec::headers::HeaderMode::ProofOfWork,
                    Default::default(),
                )
                .map_err(|_| Error::Corrupt("stored proof"))
            })
            .transpose()
    }
    pub fn children(&self, h: Hash256, limit: usize) -> Result<Vec<Hash256>, Error> {
        self.source
            .prefix(&key(CHILDREN, h.0), limit)?
            .into_iter()
            .map(|(k, _)| {
                if k.len() == 65 {
                    fixed_hash(&k[33..])
                } else {
                    Err(Error::Corrupt("child key"))
                }
            })
            .collect()
    }
    pub fn chain_ends(&self, limit: usize) -> Result<Vec<Hash256>, Error> {
        self.source
            .prefix(&[ENDS], limit)?
            .into_iter()
            .map(|(k, _)| fixed_hash(&k[1..]))
            .collect()
    }
    pub fn verify_tip(&self) -> Result<(), Error> {
        self.finalized_height()?;
        let retention = self.retention()?;
        if let Some(h) = self.status.tip {
            let b = self.require_block(h)?;
            self.require_retained(b.header().common().height)?;
            let r = self.require_record(h)?;
            if r.roots.state.hash().0 != b.header().common().state_hash || !self.is_canonical(h)? {
                return Err(Error::Corrupt("tip state"));
            }
            r.roots.load(self.source.clone())?;
            let ev = self
                .event(self.status.revision)?
                .ok_or(Error::Corrupt("missing last event"))?;
            if ev.revision != self.status.revision || ev.new_tip != h {
                return Err(Error::Corrupt("tip event"));
            }
            if self.canonical_key(0)? != Some(self.identity.genesis) {
                return Err(Error::Corrupt("genesis index"));
            }
            let generation = match b {
                Block::Key(_) => h,
                Block::Micro(ref m) => Hash256(m.header.common.previous_key_hash),
            };
            let generation_height = match (&b, self.identity.consensus) {
                (Block::Micro(_), ConsensusType::Pos) => match self.require_block(generation)? {
                    Block::Key(k) => k.common.height,
                    _ => return Err(Error::Corrupt("tip generation type")),
                },
                _ => b.header().common().height,
            };
            if self.canonical_key(generation_height)? != Some(generation) {
                return Err(Error::Corrupt("tip generation index"));
            }
            if self.identity.consensus == ConsensusType::Pos {
                self.hyperchain_checkpoint()?;
                self.hyperchain_event(self.status.revision)?;
            }
        } else if retention.epoch != 0 || !self.source.prefix(&[BLOCKS], 1)?.is_empty() {
            return Err(Error::Corrupt("blocks without initialized chain"));
        }
        Ok(())
    }
}
pub type Writes = BTreeMap<Vec<u8>, Option<Vec<u8>>>;
pub fn encode_finalized(height: u64) -> Result<Vec<u8>, Error> {
    if height == 0 {
        return Err(Error::Corrupt("zero finalized height"));
    }
    Ok(list(vec![number(1), number(height)]).encode())
}
