// aec_chain_state insertion, generation metadata and PoW chain selection,
// fa2a6d7. Copyright (c) 2017, Aeternity Anstalt. ISC: repository LICENSE.
use super::*;
use crate::{execution, key as key_execution, rewards};
use ae_codec::headers::{Header, KeyHeader};
use ae_consensus::target::Target;
use ae_execution::transactions::{self, batch};
use ae_protocol::ConsensusType;
use ae_trie::MemoryStore;
use ae_vm_fate::maps::MapOrder;

#[derive(Debug, Clone, Copy)]
pub enum Origin {
    Gossip,
    Sync,
}
#[derive(Debug, Clone, Copy)]
pub enum SyncPolicy {
    Unlimited,
    Depth(u64),
    Finalized(u64),
}
#[derive(Debug, Clone, Copy)]
pub struct Policy {
    pub origin: Origin,
    pub gossip_depth: u64,
    pub sync: SyncPolicy,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            origin: Origin::Gossip,
            gossip_depth: 5,
            sync: SyncPolicy::Unlimited,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub overlay: OverlayLimits,
    pub history: usize,
    pub ancestry: usize,
    pub siblings: usize,
    pub commit_bytes: usize,
    pub policy: Policy,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            overlay: Default::default(),
            history: 100_000,
            ancestry: 100_000,
            siblings: 10_000,
            commit_bytes: 512 * 1024 * 1024,
            policy: Default::default(),
        }
    }
}
#[derive(Debug)]
pub enum InsertError<S = MemoryStore> {
    Repository(Error),
    Key(Box<key_execution::Error<Overlay<S>, Overlay<S>>>),
    Micro(Box<execution::Error<Overlay<S>, Overlay<S>>>),
    Encoding,
    WrongConsensus,
    WrongGenesis,
    GenesisMicro,
    Policy { height: u64, top: u64 },
    InvalidHistory,
}
impl<S> From<Error> for InsertError<S> {
    fn from(e: Error) -> Self {
        Self::Repository(e)
    }
}
impl<S> InsertError<S> {
    pub fn is_operational(&self) -> bool {
        match self {
            Self::Key(e) => e.is_operational(),
            Self::Micro(e) => e.is_operational(),
            Self::Repository(_)
            | Self::WrongConsensus
            | Self::InvalidHistory
            | Self::Policy { .. } => true,
            _ => false,
        }
    }
}
#[derive(Debug)]
pub struct Update {
    identity: Identity,
    source_id: u64,
    expected: Status,
    change: Change,
    writes: Writes,
    events: Vec<batch::Event>,
    key_fraud: Option<(KeyHeader, KeyHeader)>,
    proof: Option<fraud::Proof>,
}
impl Update {
    pub fn identity(&self) -> &Identity {
        &self.identity
    }
    pub fn source_id(&self) -> u64 {
        self.source_id
    }
    pub fn expected(&self) -> Status {
        self.expected
    }
    pub fn change(&self) -> &Change {
        &self.change
    }
    pub fn writes(&self) -> &Writes {
        &self.writes
    }
    pub fn events(&self) -> &[batch::Event] {
        &self.events
    }
    pub fn discovered_proof(&self) -> Option<&fraud::Proof> {
        self.proof.as_ref()
    }
    pub fn key_fraud(&self) -> Option<&(KeyHeader, KeyHeader)> {
        self.key_fraud.as_ref()
    }
}
fn put(w: &mut Writes, k: Vec<u8>, v: Vec<u8>) {
    w.insert(k, Some(v));
}
fn publish(mut u: Update, limit: usize) -> Result<Update, Error> {
    put(
        &mut u.writes,
        STATUS.to_vec(),
        Status {
            revision: u.change.revision,
            tip: Some(u.change.new_tip),
        }
        .encode(),
    );
    put(
        &mut u.writes,
        key(EVENTS, u.change.revision.to_be_bytes()),
        u.change.encode(),
    );
    let mut total = 0usize;
    for (k, v) in &u.writes {
        total = total
            .checked_add(k.len())
            .and_then(|n| n.checked_add(v.as_ref().map_or(0, Vec::len)))
            .ok_or(Error::ResourceLimit)?;
        if total > limit {
            return Err(Error::ResourceLimit);
        }
    }
    Ok(u)
}
fn raw_nodes(w: &mut Writes, nodes: &BTreeMap<Hash256, Vec<u8>>) -> Result<(), Error> {
    for (h, b) in nodes {
        if blake2b_256(b) != *h {
            return Err(Error::Corrupt("pending node hash"));
        }
        put(w, key(NODES, h.0), b.clone());
    }
    Ok(())
}
fn stores<A, C>(t: &Trees<A, C>) -> [&C; 7]
where
    A: NodeReader,
    C: NodeReader,
{
    [
        t.calls.tree().store(),
        t.contracts.tree().store(),
        t.channels.tree().store(),
        t.names.tree().store(),
        t.names.cache().store(),
        t.oracles.tree().store(),
        t.oracles.cache().store(),
    ]
}
pub fn bootstrap<S: Read>(
    view: &View<S>,
    network: &NetworkProfile,
    header: &KeyHeader,
    trees: &Trees,
    limits: Limits,
) -> Result<Update, InsertError<S>> {
    if view.status.tip.is_some() {
        return Err(Error::NotEmpty.into());
    }
    let block = Block::Key(header.clone());
    let hash = block.hash().map_err(|_| InsertError::Encoding)?;
    if !view.identity.matches(network)
        || hash != view.identity.genesis
        || header.common.height != 0
        || header.hole
        || header.end_of_epoch
        || network.validate_version(0, header.common.version).is_err()
        || trees.roots().hash().0 != header.common.state_hash
    {
        return Err(InsertError::WrongGenesis);
    }
    let expected_public = match network.network_id() {
        b"ae_mainnet" => Some(ae_state::genesis::Network::Mainnet),
        b"ae_uat" => Some(ae_state::genesis::Network::Uat),
        _ => None,
    };
    if let Some(n) = expected_public
        && (ae_state::genesis::header(n).map_err(|_| InsertError::WrongGenesis)?
            != Header::Key(header.clone())
            || trees.names.cache_root_hash().is_some()
            || trees.oracles.cache_root_hash().is_some())
    {
        return Err(InsertError::WrongGenesis);
    }
    // Root equality alone cannot prove that the supplied genesis is complete.
    // Traverse all trees before making a publishable update.
    for tree in [
        trees.accounts.tree(),
        trees.calls.tree(),
        trees.contracts.tree(),
        trees.channels.tree(),
        trees.names.tree(),
        trees.names.cache(),
        trees.oracles.tree(),
        trees.oracles.cache(),
    ] {
        for entry in tree.iter() {
            entry.map_err(Error::from)?;
        }
    }
    let difficulty = Target(header.target)
        .difficulty()
        .map_err(|_| InsertError::WrongGenesis)?;
    let record = Record {
        roots: Roots::of(trees),
        difficulty,
        fees: 0.into(),
        fraud: false,
        fork: hash,
    };
    let mut writes = Writes::new();
    raw_nodes(&mut writes, trees.accounts.tree().store().nodes())?;
    for s in stores(trees) {
        raw_nodes(&mut writes, s.nodes())?;
    }
    store_block(&mut writes, &block, &record, &view.config)?;
    put(&mut writes, key(CANONICAL, hash.0), vec![]);
    put(&mut writes, key(KEYS, 0u64.to_be_bytes()), hash.0.to_vec());
    put(&mut writes, key(ENDS, hash.0), vec![]);
    let change = Change {
        revision: 1,
        inserted: hash,
        old_tip: None,
        new_tip: hash,
        ancestor: None,
        detached: vec![],
        attached: vec![hash],
        requeue: vec![],
        confirm: vec![],
    };
    Ok(publish(
        Update {
            identity: view.identity.clone(),
            source_id: view.source.source_id(),
            expected: view.status,
            change,
            writes,
            events: vec![],
            key_fraud: None,
            proof: None,
        },
        limits.commit_bytes,
    )?)
}
fn store_block(w: &mut Writes, b: &Block, r: &Record, config: &ReadConfig) -> Result<(), Error> {
    let hash = b.hash().map_err(|_| Error::Corrupt("block hash"))?;
    let h = b.header();
    let encoded = blocks::encode(b, config)?;
    let mut transaction_records = Vec::new();
    if let Block::Micro(original) = b {
        for tx in &original.transactions {
            let record = ae_tx::storage::encode(tx, config.transaction_record())?;
            if ae_tx::storage::decode(&record, config.transaction_record())? != *tx {
                return Err(Error::UnsupportedBlock);
            }
            transaction_records.push((tx.hash(), record));
        }
    }
    put(w, key(BLOCKS, hash.0), encoded);
    put(w, key(RECORDS, hash.0), r.encode());
    let mut child = h.common().previous_hash.to_vec();
    child.extend(hash.0);
    put(w, key(CHILDREN, child), vec![]);
    let mut height = h.common().height.to_be_bytes().to_vec();
    height.extend(hash.0);
    put(w, key(HEIGHTS, height), vec![]);
    for (hash, record) in transaction_records {
        put(w, key(TRANSACTIONS, hash.0), record);
    }
    Ok(())
}
fn require_key<S: Read>(v: &View<S>, h: Hash256) -> Result<KeyHeader, Error> {
    match v.require_block(h)? {
        Block::Key(k) => Ok(k),
        _ => Err(Error::Corrupt("expected key ancestor")),
    }
}
fn keep_pow_tip(old: &Unsigned, new: &Unsigned) -> bool {
    old >= new
}
fn incoming(previous: &Header, record: &Record) -> (num_bigint::BigInt, bool) {
    if matches!(previous, Header::Key(_)) {
        (0.into(), false)
    } else {
        (rewards::number(&record.fees), record.fraud)
    }
}
fn selection<S: Read>(
    v: &View<S>,
    block: &Block,
    record: &Record,
    limit: usize,
) -> Result<Change, Error> {
    let old = v.status.tip.ok_or(Error::Uninitialized)?;
    let inserted = block.hash().map_err(|_| Error::Corrupt("candidate hash"))?;
    let revision = v
        .status
        .revision
        .checked_add(1)
        .ok_or(Error::ResourceLimit)?;
    let parent = Hash256(block.header().common().previous_hash);
    if parent == old {
        return Ok(Change {
            revision,
            inserted,
            old_tip: Some(old),
            new_tip: inserted,
            ancestor: Some(old),
            detached: vec![],
            attached: vec![inserted],
            requeue: vec![],
            confirm: vec![],
        });
    }
    let old_record = v.require_record(old)?;
    if keep_pow_tip(&old_record.difficulty, &record.difficulty) {
        return Ok(Change {
            revision,
            inserted,
            old_tip: Some(old),
            new_tip: old,
            ancestor: None,
            detached: vec![],
            attached: vec![],
            requeue: vec![],
            confirm: vec![],
        });
    }
    let mut paths = [vec![], vec![inserted]];
    let mut positions = [BTreeMap::new(), BTreeMap::from([(inserted, 0usize)])];
    let mut next = [Some(old), Some(parent)];
    let (ancestor, indices) = 'walk: loop {
        for side in 0..2 {
            let Some(h) = next[side] else { continue };
            if paths[side].len() >= limit {
                return Err(Error::ResourceLimit);
            }
            let i = paths[side].len();
            if positions[side].insert(h, i).is_some() {
                return Err(Error::Corrupt("ancestry cycle"));
            }
            paths[side].push(h);
            if let Some(j) = positions[1 - side].get(&h) {
                break 'walk (h, if side == 0 { [i, *j] } else { [*j, i] });
            }
            if h == v.identity.genesis {
                next[side] = None;
            } else {
                let b = v.require_block(h)?;
                if b.header().common().height == 0 {
                    return Err(Error::Corrupt("foreign genesis"));
                }
                next[side] = Some(Hash256(b.header().common().previous_hash));
            }
        }
        if next.iter().all(Option::is_none) {
            return Err(Error::Corrupt("disconnected branches"));
        }
    };
    let detached = paths[0][..indices[0]].to_vec();
    let mut attached = paths[1][..indices[1]].to_vec();
    attached.reverse();
    Ok(Change {
        revision,
        inserted,
        old_tip: Some(old),
        new_tip: inserted,
        ancestor: Some(ancestor),
        detached,
        attached,
        requeue: vec![],
        confirm: vec![],
    })
}
fn canonical<S: Read>(
    v: &View<S>,
    block: &Block,
    change: &mut Change,
    w: &mut Writes,
) -> Result<(), Error> {
    let candidate = block.hash().map_err(|_| Error::Corrupt("candidate hash"))?;
    for h in &change.detached {
        let b = v.require_block(*h)?;
        w.insert(key(CANONICAL, h.0), None);
        match b {
            Block::Key(k) => {
                w.insert(key(KEYS, k.common.height.to_be_bytes()), None);
            }
            Block::Micro(m) => {
                for t in m.transactions {
                    w.insert(key(LOCATIONS, t.hash().0), None);
                    change.requeue.push(t.hash());
                }
            }
        }
    }
    // Original add_locations walks backward from the new tip. Preserve its
    // last-write behavior even if a transaction occurs on both branches.
    for h in change.attached.iter().rev() {
        let stored = if *h == candidate {
            None
        } else {
            Some(v.require_block(*h)?)
        };
        let b = stored.as_ref().unwrap_or(block);
        put(w, key(CANONICAL, h.0), vec![]);
        match b {
            Block::Key(k) => put(w, key(KEYS, k.common.height.to_be_bytes()), h.0.to_vec()),
            Block::Micro(m) => {
                for t in &m.transactions {
                    put(w, key(LOCATIONS, t.hash().0), h.0.to_vec());
                    change.confirm.push((t.hash(), *h));
                }
            }
        }
    }
    Ok(())
}
fn check_policy(height: u64, top: u64, p: Policy) -> Result<(), (u64, u64)> {
    let accepted = match p.origin {
        Origin::Gossip => height >= top.saturating_sub(p.gossip_depth),
        Origin::Sync => match p.sync {
            SyncPolicy::Unlimited => true,
            SyncPolicy::Depth(d) => height >= top.saturating_sub(d),
            SyncPolicy::Finalized(h) => height > h,
        },
    };
    if accepted { Ok(()) } else { Err((height, top)) }
}
pub fn prepare<S: Read, O: MapOrder>(
    view: &View<S>,
    block: &Block,
    config: &transactions::Config<'_, O>,
    rules: &key_execution::Rules,
    limits: Limits,
) -> Result<Update, InsertError<S>> {
    if config.consensus != ConsensusType::Pow {
        return Err(InsertError::WrongConsensus);
    }
    if !view.identity.matches(config.network) || view.identity.genesis != rules.genesis_hash {
        return Err(Error::Identity.into());
    }
    let current = block.hash().map_err(|_| InsertError::Encoding)?;
    if view.block(current)?.is_some() {
        return Err(Error::AlreadyPresent.into());
    }
    let top = view.status.tip.ok_or(Error::Uninitialized)?;
    let header = block.header();
    if header.common().height == 0 {
        return Err(if matches!(block, Block::Micro(_)) {
            InsertError::GenesisMicro
        } else {
            InsertError::WrongGenesis
        });
    }
    let top_height = view.require_block(top)?.header().common().height;
    check_policy(header.common().height, top_height, limits.policy)
        .map_err(|(height, top)| InsertError::Policy { height, top })?;
    let parent_hash = Hash256(header.common().previous_hash);
    let previous = view.require_block(parent_hash)?.header();
    let previous_key_hash = match &previous {
        Header::Key(_) => parent_hash,
        Header::Micro(m) => Hash256(m.common.previous_key_hash),
    };
    let previous_key = require_key(view, previous_key_hash)?;
    let input_record = view.require_record(parent_hash)?;
    let parent_is_key = matches!(previous, Header::Key(_));
    let mut recent = vec![];
    let needed = if matches!(block, Block::Micro(_)) {
        0
    } else if header.common().height > 18 {
        18
    } else if header.common().height > 11 {
        11
    } else {
        0
    };
    let mut h = previous_key_hash;
    for _ in 0..needed {
        if recent.len() >= limits.history {
            return Err(Error::ResourceLimit.into());
        }
        let k = require_key(view, h)?;
        h = Hash256(k.common.previous_key_hash);
        recent.push(k);
    }
    recent.reverse();
    let older = if matches!(block,Block::Micro(m) if m.proof.is_some())
        && parent_is_key
        && previous_key.common.height > 0
    {
        Some(require_key(
            view,
            Hash256(previous_key.common.previous_key_hash),
        )?)
    } else {
        None
    };
    let (incoming_fees, incoming_fraud) = incoming(&previous, &input_record);
    let parent = execution::Parent {
        headers: ae_consensus::headers::Context {
            previous: &previous,
            previous_key: &previous_key,
            recent_keys: &recent,
        },
        previous_generation: older.as_ref().map_or(
            execution::PreviousGeneration::Missing,
            execution::PreviousGeneration::Found,
        ),
        fees: incoming_fees,
        reported_fraud: incoming_fraud,
    };
    let mut state = view.trees(parent_hash, limits.overlay)?;
    let vm_key = match block {
        Block::Key(k) => k,
        Block::Micro(_) => &previous_key,
    };
    let vm_history = if config.fate.is_some() || config.aevm.is_some() {
        std::sync::Arc::new(super::history::vm_history(view, vm_key, limits.history)?)
    } else {
        Default::default()
    };
    let (fees, fraud, events) =
        super::history::with_history(config, vm_history, |config| -> Result<_, InsertError<S>> {
            Ok(match block {
                Block::Micro(m) => {
                    let r = execution::pow_micro(m, &mut state, &parent, config, &rules.admission)
                        .map_err(|e| InsertError::Micro(Box::new(e)))?;
                    (
                        rewards::unsigned(r.fees().clone()),
                        r.reported_fraud(),
                        r.events().to_vec(),
                    )
                }
                Block::Key(k) => {
                    let mut history = rewards::History::new();
                    if k.common.height > rules.reward_delay {
                        let count = rules
                            .reward_delay
                            .checked_add(1)
                            .ok_or(Error::ResourceLimit)?;
                        if count > limits.history as u64 {
                            return Err(Error::ResourceLimit.into());
                        }
                        let mut next = previous_key_hash;
                        for _ in 0..count {
                            let header = require_key(view, next)?;
                            let r = view.require_record(next)?;
                            let prev = Hash256(header.common.previous_key_hash);
                            let genesis = next == view.identity.genesis;
                            history.insert(
                                next,
                                rewards::Record {
                                    header,
                                    fees: Some(r.fees),
                                    reported_fraud: Some(r.fraud),
                                },
                            );
                            if genesis {
                                break;
                            }
                            next = prev;
                        }
                    }
                    let r = key_execution::pow_key(k, &mut state, &parent, &history, config, rules)
                        .map_err(|e| InsertError::Key(Box::new(e)))?;
                    (
                        r.closed_generation_fees().clone(),
                        parent.reported_fraud,
                        vec![],
                    )
                }
            })
        })?;
    let difficulty = match block {
        Block::Key(k) => input_record.difficulty.add(
            &Target(k.target)
                .difficulty()
                .map_err(|_| InsertError::Encoding)?,
        ),
        Block::Micro(_) => input_record.difficulty,
    };
    let mut record = Record {
        roots: Roots::of(&state),
        difficulty,
        fees,
        fraud,
        fork: input_record.fork,
    };
    let siblings = if parent_hash == top {
        vec![]
    } else {
        view.children(parent_hash, limits.siblings)?
    };
    let mut proof = None;
    let mut key_fraud = None;
    if !siblings.is_empty() {
        record.fork = current;
    }
    for h in siblings {
        match (block, view.require_block(h)?) {
            (Block::Micro(m), Block::Micro(other)) if proof.is_none() => {
                proof = Some(fraud::Proof {
                    first: Header::Micro(m.header.clone()),
                    second: Header::Micro(other.header),
                    public_key: previous_key.miner.to_vec(),
                });
            }
            (Block::Key(k), Block::Key(other)) if key_fraud.is_none() && k.miner == other.miner => {
                key_fraud = Some((k.clone(), other));
            }
            _ => {}
        }
    }
    let mut change = selection(view, block, &record, limits.ancestry)?;
    let mut writes = Writes::new();
    raw_nodes(&mut writes, state.accounts.tree().store().pending())?;
    for s in stores(&state) {
        raw_nodes(&mut writes, s.pending())?;
    }
    store_block(&mut writes, block, &record, &view.config)?;
    canonical(view, block, &mut change, &mut writes)?;
    if matches!(block, Block::Key(_)) {
        put(&mut writes, key(ENDS, current.0), vec![]);
        writes.insert(key(ENDS, header.common().previous_key_hash), None);
    }
    if let Some(p) = &proof {
        let generation = Hash256(header.common().previous_key_hash);
        if view.discovered_proof(generation)?.is_none() {
            put(
                &mut writes,
                key(PROOFS, generation.0),
                p.encode().map_err(|_| InsertError::Encoding)?,
            );
        }
    }
    Ok(publish(
        Update {
            identity: view.identity.clone(),
            source_id: view.source.source_id(),
            expected: view.status,
            change,
            writes,
            events,
            key_fraud,
            proof,
        },
        limits.commit_bytes,
    )?)
}

#[cfg(test)]
#[path = "ledger_tests.rs"]
mod tests;
