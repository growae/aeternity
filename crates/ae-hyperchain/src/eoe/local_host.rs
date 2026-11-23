mod shared;
// Local service boundary for aec_eoe_gen_vote, aeternity fa2a6d7.
// Publication is an outbox operation. The caller drains it after a successful
// FSM step (including retained effects of a modeled native exception).
// Shared pool admissions are staged until the complete EOE callback commits.
// Live cache reads are service I/O: they may request blocks even if a later
// local step rolls back. Only notifications and signing effects are staged.
use super::{Failure, Host, Limits, Payload, finalization, payload};
use crate::{parent_block::ParentBlock, parent_cache::runtime as cache};
use ae_crypto::Ed25519SecretKey;
use ae_mempool::hc_votes::{self, Admission, PushOutcome, VotePool};
use ae_protocol::{ConsensusType, NetworkProfile, Protocol};
use ae_state::Trees;
use ae_trie::{MemoryStore, NodeReader};
use ae_tx::{SignedTransaction, Transaction};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, atomic::AtomicBool},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ResourceLimit,
    Payload(Failure),
    Pool(hc_votes::Error),
    Finalization(finalization::Error),
    MissingKey(AccountPublicKey),
    ParentCache(cache::Error),
    SelectedState(SelectedError),
    IndependentPool(super::pool::Error),
    PoolCommit(super::pool::Error),
}
impl Error {
    pub fn is_operational(&self) -> bool {
        match self {
            Self::ParentCache(
                cache::Error::Closed
                | cache::Error::Timeout
                | cache::Error::Core(crate::parent_cache::Error::Native(_))
                | cache::Error::Targets(cache::TargetError::Native),
            ) => false,
            Self::MissingKey(_)
            | Self::Payload(Failure::Native(_))
            | Self::IndependentPool(super::pool::Error::Closed) => false,
            Self::Finalization(
                finalization::Error::MissingContract(_)
                | finalization::Error::MissingAccount(_)
                | finalization::Error::Transaction(_),
            ) => false,
            Self::Pool(hc_votes::Error::Signature(e)) => e.is_operational(),
            Self::Pool(hc_votes::Error::OnlyHcVoteAllowed | hc_votes::Error::InvalidAtProtocol) => {
                false
            }
            _ => true,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finalized {
    Signed(Box<SignedTransaction<Transaction>>),
    TopUnavailable,
    EntropyNotInCache,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    Vote {
        event: hc_votes::Event,
        signed: Box<SignedTransaction<Transaction>>,
    },
    ParentBlock {
        epoch: Unsigned,
        block: ParentBlock,
    },
}
#[derive(Debug, Clone, Copy)]
pub struct ServiceLimits {
    pub maximum_outbox_entries: usize,
    pub maximum_outbox_bytes: usize,
    pub maximum_parent_blocks: usize,
    pub maximum_parent_bytes: usize,
    pub maximum_cached_hashes: usize,
    pub pool: hc_votes::Limits,
}
impl Default for ServiceLimits {
    fn default() -> Self {
        Self {
            maximum_outbox_entries: 100_000,
            maximum_outbox_bytes: 256 * 1024 * 1024,
            maximum_parent_blocks: 100_000,
            maximum_parent_bytes: 16 * 1024 * 1024,
            maximum_cached_hashes: 100_000,
            pool: Default::default(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct Config {
    pub network: NetworkProfile,
    pub election_contract: [u8; 32],
    pub parent_epoch_length: Unsigned,
    pub parent_start_height: Unsigned,
    pub limits: ServiceLimits,
}

// A selected tip couples the admission protocol/mode and the tree view. None
// is the original no-top state; its supplied protocol/mode come from genesis.
#[derive(Debug, Clone)]
pub struct SelectedTip<A = MemoryStore, C = MemoryStore> {
    pub trees: Option<Arc<Trees<A, C>>>,
    pub protocol: Protocol,
    pub consensus: ConsensusType,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectedError {
    ResourceLimit,
    Backend(String),
}
// The live node captures one actual selected snapshot per operation. Pending
// finalization asks for the current top when it completes, not the tip from a
// possibly delayed top_changed notification. Explicit caller views bypass it.
pub trait SelectedSource<A, C>: Send + Sync {
    fn capture(&self) -> Result<SelectedTip<A, C>, SelectedError>;
}
#[derive(Clone)]
pub struct LocalHost<A = MemoryStore, C = MemoryStore> {
    config: Config,
    tip: SelectedTip<A, C>,
    selected_source: Option<Arc<dyn SelectedSource<A, C>>>,
    keys: Arc<BTreeMap<AccountPublicKey, Ed25519SecretKey>>,
    pool: VotePool,
    independent: Option<super::pool::Hub>,
    staged: Option<shared::Staged>,
    gossip: BTreeSet<Hash256>,
    parents: BTreeMap<Unsigned, ParentBlock>,
    parent_bytes: usize,
    parent_cache: Option<cache::Client>,
    parent_route: Option<cache::Route>,
    shutdown: Arc<AtomicBool>,
    outbox: Vec<Output>,
    outbox_bytes: usize,
}
impl<A: NodeReader + Clone, C: NodeReader + Clone> LocalHost<A, C> {
    pub fn new(
        config: Config,
        tip: SelectedTip<A, C>,
        keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
    ) -> Self {
        Self::with_shared_keys(config, tip, Arc::new(keys))
    }
    // Replacement hosts share immutable key material while receiving fresh
    // local state, shutdown controls and outboxes. Producer key selection is
    // retained separately by its owner.
    pub fn with_shared_keys(
        config: Config,
        tip: SelectedTip<A, C>,
        keys: Arc<BTreeMap<AccountPublicKey, Ed25519SecretKey>>,
    ) -> Self {
        Self {
            config,
            tip,
            selected_source: None,
            keys,
            pool: VotePool::default(),
            independent: None,
            staged: None,
            gossip: BTreeSet::new(),
            parents: BTreeMap::new(),
            parent_bytes: 0,
            parent_cache: None,
            parent_route: None,
            shutdown: Arc::new(AtomicBool::new(false)),
            outbox: vec![],
            outbox_bytes: 0,
        }
    }
    pub fn select_tip(&mut self, tip: SelectedTip<A, C>) {
        self.tip = tip;
    }
    pub fn with_selected_source(mut self, source: Arc<dyn SelectedSource<A, C>>) -> Self {
        self.selected_source = Some(source);
        self
    }
    pub fn pool(&self) -> Result<VotePool, Error> {
        self.inspect_pool(Clone::clone)
    }
    pub fn network(&self) -> &NetworkProfile {
        &self.config.network
    }
    pub fn stakers(&self) -> BTreeSet<AccountPublicKey> {
        self.keys.keys().copied().collect()
    }
    pub(crate) fn signing_keys(&self) -> Arc<BTreeMap<AccountPublicKey, Ed25519SecretKey>> {
        self.keys.clone()
    }
    // Immutable key material is shared; candidate/active selection belongs to
    // the producer's staged Keys value and exposes no secret bytes.
    pub fn signer(&self) -> crate::signing::Keys {
        crate::signing::Keys::from_shared(self.keys.clone())
    }
    pub fn update_epoch(&mut self, epoch: Unsigned) -> Result<(), Error> {
        if let Some(hub) = &self.independent {
            hub.new_epoch(epoch).map_err(Error::IndependentPool)
        } else {
            self.pool.update_epoch(epoch);
            Ok(())
        }
    }
    pub fn outbox(&self) -> &[Output] {
        &self.outbox
    }
    pub fn drain_outbox(&mut self) -> Vec<Output> {
        self.outbox_bytes = 0;
        std::mem::take(&mut self.outbox)
    }
    pub fn set_gossip_cache(&mut self, hashes: BTreeSet<Hash256>) -> Result<(), Error> {
        if hashes.len() > self.config.limits.maximum_cached_hashes {
            return Err(Error::ResourceLimit);
        }
        self.gossip = hashes;
        Ok(())
    }
    pub fn insert_parent(&mut self, block: ParentBlock) -> Result<(), Error> {
        let old = self.parents.get(&block.height);
        if old.is_none() && self.parents.len() >= self.config.limits.maximum_parent_blocks {
            return Err(Error::ResourceLimit);
        }
        let previous = old.map_or(Ok(0), parent_size)?;
        let bytes = self
            .parent_bytes
            .checked_sub(previous)
            .and_then(|n| n.checked_add(parent_size(&block).ok()?))
            .ok_or(Error::ResourceLimit)?;
        if bytes > self.config.limits.maximum_parent_bytes {
            return Err(Error::ResourceLimit);
        }
        self.parents.insert(block.height.clone(), block);
        self.parent_bytes = bytes;
        Ok(())
    }
    pub fn remove_parent(&mut self, height: &Unsigned) -> Option<ParentBlock> {
        let block = self.parents.remove(height)?;
        self.parent_bytes -= parent_size(&block).expect("inserted parent is bounded");
        Some(block)
    }
    // A configured live cache is authoritative: missing/nonfinal blocks and
    // cache failures never fall back to manually supplied offline parents.
    pub fn set_parent_cache(&mut self, client: Option<cache::Client>) {
        self.parent_cache = client;
        self.parent_route = None;
    }
    pub fn set_parent_route(&mut self, route: cache::Route) {
        self.parent_route = Some(route);
    }
    pub(super) fn set_shutdown(&mut self, shutdown: Arc<AtomicBool>) {
        self.shutdown = shutdown;
    }
    pub fn entropy_hash(&mut self, child_epoch: &Unsigned) -> Result<Option<Vec<u8>>, Error> {
        let height = self.entropy_height(child_epoch);
        let captured = match &self.parent_route {
            Some(route) => Some(
                route
                    .current()
                    .ok_or(Error::ParentCache(cache::Error::Closed))?,
            ),
            None => self.parent_cache.clone(),
        };
        let block = match &captured {
            Some(cache) => cache
                .get_block_cancelled(super::big(&height), 1000.into(), &self.shutdown)
                .map_err(Error::ParentCache)?
                .ok(),
            None => self.parents.get(&height).cloned(),
        };
        let Some(block) = block else { return Ok(None) };
        let size = parent_size(&block)?
            .checked_add(child_epoch.as_bytes().len())
            .ok_or(Error::ResourceLimit)?;
        let hash = block.hash.clone();
        self.enqueue(
            Output::ParentBlock {
                epoch: child_epoch.clone(),
                block,
            },
            size,
        )?;
        Ok(Some(hash))
    }
    fn enqueue(&mut self, output: Output, bytes: usize) -> Result<(), Error> {
        let bytes = self
            .outbox_bytes
            .checked_add(bytes)
            .ok_or(Error::ResourceLimit)?;
        if self
            .outbox
            .len()
            .saturating_add(self.staged.as_ref().map_or(0, |s| s.admissions.len()))
            >= self.config.limits.maximum_outbox_entries
            || bytes.saturating_add(self.staged.as_ref().map_or(0, |s| s.bytes))
                > self.config.limits.maximum_outbox_bytes
        {
            return Err(Error::ResourceLimit);
        }
        self.outbox.push(output);
        self.outbox_bytes = bytes;
        Ok(())
    }
    // Network input uses Received; locally created votes use Created. The
    // outbox event feeds the FSM only after admission succeeded. Duplicates
    // still emit events, while gossip-cache hits do not.
    pub fn admit(
        &mut self,
        signed: SignedTransaction<Transaction>,
        event: hc_votes::Event,
    ) -> Result<PushOutcome, Error> {
        let mut next = self.clone();
        let outcome = next.admit_using(signed, event, None)?;
        let committed = next.commit_pool(None)?;
        *self = next;
        Ok(committed.into_iter().next().unwrap_or(outcome))
    }
    pub(super) fn admit_using(
        &mut self,
        signed: SignedTransaction<Transaction>,
        event: hc_votes::Event,
        checker: Option<&mut dyn FnMut(Hash256) -> bool>,
    ) -> Result<PushOutcome, Error> {
        let hash =
            hc_votes::admission_hash(&signed, self.config.limits.pool).map_err(Error::Pool)?;
        if checker.map_or_else(|| self.gossip.contains(&hash), |check| check(hash)) {
            return Ok(PushOutcome::Cached);
        }
        // Native cache hits do not read the chain, including during recovery.
        // On a miss, even wrong-type validation follows selected-state capture.
        let selected = self
            .selected_source
            .as_ref()
            .map(|s| s.capture().map_err(Error::SelectedState))
            .transpose()?;
        let tip = selected.as_ref().unwrap_or(&self.tip);
        if self.independent.is_some() {
            return self.stage_vote(super::pool::Admission {
                signed,
                event,
                protocol: tip.protocol,
                consensus: tip.consensus,
            });
        }
        let mut pool = self.pool.clone();
        let outcome = pool
            .push(
                &signed,
                event,
                &Admission {
                    network: &self.config.network,
                    protocol: tip.protocol,
                    consensus: tip.consensus,
                    limits: self.config.limits.pool,
                },
                |_| false,
            )
            .map_err(Error::Pool)?;
        if matches!(outcome, PushOutcome::Accepted { .. }) {
            let bytes = signed.encode().len();
            self.enqueue(
                Output::Vote {
                    event,
                    signed: Box::new(signed),
                },
                bytes,
            )?;
        }
        self.pool = pool;
        Ok(outcome)
    }
    pub fn entropy_height(&self, child_epoch: &Unsigned) -> Unsigned {
        let epoch = child_epoch.checked_sub(&3.into()).unwrap_or(0.into());
        let product = super::big(&epoch) * super::big(&self.config.parent_epoch_length);
        // Multiplication of unsigned operands is always nonnegative.
        let (_, bytes) = product.to_bytes_be();
        Unsigned::from_field_bytes(&bytes)
            .expect("nonnegative product")
            .add(&self.config.parent_start_height)
    }
}
fn parent_size(block: &ParentBlock) -> Result<usize, Error> {
    [
        block.hash.len(),
        block.previous_hash.as_ref().map_or(0, Vec::len),
        block.height.as_bytes().len(),
        block.time.as_bytes().len(),
    ]
    .into_iter()
    .try_fold(0usize, |n, size| {
        n.checked_add(size).ok_or(Error::ResourceLimit)
    })
}
impl<A: NodeReader + Clone, C: NodeReader + Clone> Host for LocalHost<A, C> {
    type Error = Error;
    type View = Arc<Trees<A, C>>;
    type Finalized = Finalized;
    fn is_operational(error: &Error) -> bool {
        error.is_operational()
    }
    fn commit(&mut self) -> Result<(), Error> {
        self.commit_pool(None).map(|_| ())
    }
    fn create_vote(
        &mut self,
        voter: AccountPublicKey,
        epoch: &Unsigned,
        kind: &Unsigned,
        fields: &Payload,
        limits: Limits,
    ) -> Result<SignedTransaction<Transaction>, Error> {
        let key = self.keys.get(&voter).ok_or(Error::MissingKey(voter))?;
        let fields = payload::create_payload(fields, key, limits).map_err(Error::Payload)?;
        payload::create_transaction(
            voter,
            epoch.clone(),
            kind.clone(),
            &fields,
            key,
            &self.config.network,
            limits,
        )
        .map_err(Error::Payload)
    }
    fn publish(&mut self, vote: SignedTransaction<Transaction>) -> Result<bool, Error> {
        self.publish_using(vote, None)
    }
    fn finalize(
        &mut self,
        epoch: &Unsigned,
        leader: AccountPublicKey,
        calldata: &[u8],
        nonce_offset: &Unsigned,
        view: Option<&Self::View>,
    ) -> Result<Finalized, Error> {
        let trees = match view {
            Some(view) => Some(view.clone()),
            None => match &self.selected_source {
                Some(source) => source.capture().map_err(Error::SelectedState)?.trees,
                None => self.tip.trees.clone(),
            },
        };
        let Some(trees) = trees else {
            return Ok(Finalized::TopUnavailable);
        };
        let child_epoch = epoch.add(&2.into());
        // Stage notification before building/signing, exactly as get_entropy_hash.
        // Operational errors roll it back; modeled native failures retain it.
        let old_len = self.outbox.len();
        let old_bytes = self.outbox_bytes;
        if self.entropy_hash(&child_epoch)?.is_none() {
            return Ok(Finalized::EntropyNotInCache);
        }
        let result = (|| {
            let tx = finalization::transaction(
                &trees,
                leader,
                self.config.election_contract,
                calldata.to_vec(),
                nonce_offset,
            )
            .map_err(Error::Finalization)?;
            let key = self.keys.get(&leader).ok_or(Error::MissingKey(leader))?;
            Ok(Finalized::Signed(Box::new(finalization::sign(
                tx,
                key,
                &self.config.network,
            ))))
        })();
        if result.as_ref().is_err_and(Error::is_operational) {
            self.outbox.truncate(old_len);
            self.outbox_bytes = old_bytes;
        }
        result
    }
}

// A borrowed checker exists only inside the cache transaction. It cannot leak
// into a cloned host or a later callback; the coordinator stages the host.
pub(super) struct Cached<'h, 'c, A, C> {
    pub host: &'h mut LocalHost<A, C>,
    pub checker: Option<&'c mut dyn FnMut(Hash256) -> bool>,
}
impl<A: NodeReader + Clone, C: NodeReader + Clone> LocalHost<A, C> {
    fn publish_using(
        &mut self,
        vote: SignedTransaction<Transaction>,
        checker: Option<&mut dyn FnMut(Hash256) -> bool>,
    ) -> Result<bool, Error> {
        match self.admit_using(vote, hc_votes::Event::Created, checker) {
            Ok(_) => Ok(true),
            Err(Error::Pool(e)) if !Error::Pool(e.clone()).is_operational() => Ok(false),
            Err(e) => Err(e),
        }
    }
}
impl<A: NodeReader + Clone, C: NodeReader + Clone> Host for Cached<'_, '_, A, C> {
    type Error = Error;
    type View = Arc<Trees<A, C>>;
    type Finalized = Finalized;
    fn is_operational(error: &Error) -> bool {
        error.is_operational()
    }
    fn create_vote(
        &mut self,
        voter: AccountPublicKey,
        epoch: &Unsigned,
        kind: &Unsigned,
        fields: &Payload,
        limits: Limits,
    ) -> Result<SignedTransaction<Transaction>, Error> {
        self.host.create_vote(voter, epoch, kind, fields, limits)
    }
    fn publish(&mut self, vote: SignedTransaction<Transaction>) -> Result<bool, Error> {
        let check = self
            .checker
            .as_mut()
            .map(|c| &mut **c as &mut dyn FnMut(Hash256) -> bool);
        self.host.publish_using(vote, check)
    }
    fn finalize(
        &mut self,
        epoch: &Unsigned,
        leader: AccountPublicKey,
        calldata: &[u8],
        nonce_offset: &Unsigned,
        view: Option<&Self::View>,
    ) -> Result<Finalized, Error> {
        self.host
            .finalize(epoch, leader, calldata, nonce_offset, view)
    }
}
