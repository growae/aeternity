use super::*;
use ae_codec::headers::Header;
use ae_mempool::pool as policy;
use ae_protocol::{ConsensusType, NetworkProfile, Protocol};
use ae_types::AccountPublicKey;
#[derive(Debug, Clone)]
pub struct Config {
    pub policy: policy::Config,
    pub limits: policy::Limits,
    pub failures: policy::FailureSettings,
    pub consensus: ConsensusType,
    pub events: usize,
    pub commit_bytes: usize,
    pub overlay: ae_trie::OverlayLimits,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            policy: Default::default(),
            limits: Default::default(),
            failures: Default::default(),
            consensus: ConsensusType::Pow,
            events: 1024,
            commit_bytes: 64 * 1024 * 1024,
            overlay: Default::default(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Repository(Error),
    Policy(policy::Error),
    AlreadyRunning,
    RestartRequired,
}
impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Repository(e)
    }
}
impl From<policy::Error> for Failure {
    fn from(e: policy::Error) -> Self {
        Self::Policy(e)
    }
}
impl Failure {
    pub fn is_operational(&self) -> bool {
        match self {
            Self::Policy(e) => e.is_operational(),
            _ => true,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inspection {
    pub chain: Status,
    pub revision: u64,
    pub active: usize,
    pub visited: usize,
    pub retained_record_bytes: usize,
    pub gc_height: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submission {
    pub outcome: policy::PushOutcome,
    pub chain: Status,
    pub revision: u64,
}
#[derive(Clone)]
struct Live {
    pool: policy::Pool,
    members: BTreeSet<Hash256>,
    chain: Status,
    revision: u64,
    height: u64,
}
struct Plan {
    live: Live,
    writes: repo::Writes,
}
// One owner serializes transient state; chain commits remain independent.
// A future request worker can own this service without exposing its Pool.
pub struct Service {
    db: Arc<Database>,
    network: NetworkProfile,
    config: Config,
    live: Live,
    _lease: Arc<()>,
    uncertain: bool,
}
fn tip<S: Read>(stored: &Stored<S>) -> Result<(Hash256, Header), Failure> {
    let hash = stored.chain.status().tip.ok_or(Error::Uninitialized)?;
    Ok((hash, stored.chain.require_block(hash)?.header()))
}
fn lookup<S: ae_trie::NodeReader>(
    accounts: &ae_state::accounts::Accounts<S>,
    key: AccountPublicKey,
) -> Result<Option<policy::AccountInfo>, policy::Error> {
    accounts
        .lookup(key)
        .map(|a| a.map(policy::AccountInfo::from))
        .map_err(policy::Error::Account)
}
fn restore<S: Read>(stored: &Stored<S>, config: &Config) -> Result<Live, Failure> {
    let (hash, header) = tip(stored)?;
    let height = header.common().height;
    let trees = stored.chain.trees(hash, config.overlay)?;
    let members: BTreeSet<_> = stored.members(config.limits.entries)?.into_iter().collect();
    let mut txs = Vec::with_capacity(members.len());
    let mut bytes = 0usize;
    for hash in &members {
        if matches!(stored.location(*hash)?, Location::Block(_)) {
            return Err(Error::Corrupt("canonical transaction in durable pool").into());
        }
        let tx = stored
            .transaction(*hash)?
            .ok_or(Error::Corrupt("pool record missing"))?;
        let mut record = stored.chain.config().transaction_record();
        record.max_record_bytes = record.max_record_bytes.min(config.limits.record_bytes);
        let n = ae_tx::storage::encode(&tx, record)
            .map_err(Error::from)?
            .len();
        bytes = bytes.checked_add(n).ok_or(Error::ResourceLimit)?;
        if bytes > config.limits.retained_record_bytes {
            return Err(Error::ResourceLimit.into());
        }
        txs.push(Arc::new(tx));
    }
    let mut pool = policy::Pool::new(config.policy.clone(), config.limits, height);
    pool.reconcile(
        policy::Reconcile {
            key_top: false,
            requeue: &txs,
            confirmed: &[],
        },
        |k| lookup(&trees.accounts, k),
        stored.chain.config().transaction_record(),
    )?;
    Ok(Live {
        pool,
        members,
        chain: stored.chain.status(),
        revision: stored.revision,
        height,
    })
}
impl Service {
    pub fn start(
        db: Arc<Database>,
        network: NetworkProfile,
        config: Config,
    ) -> Result<Self, Failure> {
        if !db.identity.matches(&network) {
            return Err(Error::Identity.into());
        }
        let lease = Arc::new(());
        {
            let mut owner = db.pool_owner.lock().map_err(|_| backend_lock())?;
            if owner.upgrade().is_some() {
                return Err(Failure::AlreadyRunning);
            }
            *owner = Arc::downgrade(&lease);
        }
        let live = {
            let stored = db.pool_view()?;
            let live = restore(&stored, &config)?;
            db.commit_pool(&stored, Default::default(), config.commit_bytes, || Ok(()))?;
            live
        };
        Ok(Self {
            db,
            network,
            config,
            live,
            _lease: lease,
            uncertain: false,
        })
    }
    pub fn recover(&mut self, config: Config) -> Result<(), Failure> {
        let db = self.db.clone();
        let stored = db.pool_view()?;
        let live = restore(&stored, &config)?;
        db.commit_pool(&stored, Default::default(), config.commit_bytes, || Ok(()))?;
        self.live = live;
        self.config = config;
        self.uncertain = false;
        Ok(())
    }
    fn plan(&self, stored: &Stored<Snapshot<'_>>) -> Result<Plan, Failure> {
        if self.uncertain {
            return Err(Failure::RestartRequired);
        }
        let current = stored.chain.status();
        if current.revision < self.live.chain.revision || stored.revision < self.live.revision {
            return Err(Error::Corrupt("pool revision moved backwards").into());
        }
        if current == self.live.chain {
            if stored.revision != self.live.revision {
                return Err(Failure::RestartRequired);
            }
            return Ok(Plan {
                live: self.live.clone(),
                writes: Default::default(),
            });
        }
        if current.revision - self.live.chain.revision > self.config.events as u64 {
            return Err(Error::ResourceLimit.into());
        }
        let mut events = vec![];
        let mut removed = BTreeSet::new();
        let mut discarded = BTreeSet::new();
        for revision in self.live.chain.revision + 1..=current.revision {
            let event = stored
                .chain
                .event(revision)?
                .ok_or(Error::Corrupt("missing chain event"))?;
            removed.extend(event.removed.iter().copied());
            discarded.extend(event.discarded.iter().copied());
            events.push(event);
        }
        // A later administrative rollback can physically remove state needed
        // by a delayed event. Rebuild as at startup, never guess old state.
        if events.iter().any(|e| {
            removed.contains(&e.new_tip)
                || e.requeue
                    .iter()
                    .chain(e.confirm.iter().map(|(h, _)| h))
                    .any(|h| discarded.contains(h))
        }) {
            return Ok(Plan {
                live: restore(stored, &self.config)?,
                writes: Default::default(),
            });
        }
        let mut live = self.live.clone();
        let mut collected = BTreeSet::new();
        for event in events {
            if event.old_tip != live.chain.tip {
                return Err(Error::Corrupt("pool event continuity").into());
            }
            let changed = event.old_tip != Some(event.new_tip);
            for hash in &event.requeue {
                live.members.insert(*hash);
            }
            for (hash, _) in &event.confirm {
                live.members.remove(hash);
            }
            if changed {
                let header = stored.chain.require_block(event.new_tip)?.header();
                let height = header.common().height;
                let key = matches!(header, Header::Key(_));
                if event.inserted.is_none() {
                    live.pool.update_gc_height(height, height);
                } else {
                    let trees = match stored.chain.trees(event.new_tip, self.config.overlay) {
                        Ok(t) => t,
                        Err(Error::Pruned { .. }) => {
                            return Ok(Plan {
                                live: restore(stored, &self.config)?,
                                writes: Default::default(),
                            });
                        }
                        Err(e) => return Err(e.into()),
                    };
                    if key && height > live.height {
                        let gone = live.pool.collect_expired(height, usize::MAX, |h| {
                            Ok(live.members.contains(&h))
                        })?;
                        for hash in gone {
                            live.members.remove(&hash);
                            collected.insert(hash);
                        }
                    }
                    let changed: BTreeSet<_> = event
                        .requeue
                        .iter()
                        .chain(event.confirm.iter().map(|(h, _)| h))
                        .copied()
                        .collect();
                    let mut requeue = vec![];
                    let mut confirmed = vec![];
                    for hash in changed {
                        let tx = Arc::new(
                            stored
                                .transaction(hash)?
                                .ok_or(Error::Corrupt("event transaction missing"))?,
                        );
                        if live.members.contains(&hash) {
                            requeue.push(tx);
                        } else {
                            confirmed.push(tx);
                        }
                    }
                    live.pool.reconcile(
                        policy::Reconcile {
                            key_top: key,
                            requeue: &requeue,
                            confirmed: &confirmed,
                        },
                        |k| lookup(&trees.accounts, k),
                        stored.chain.config().transaction_record(),
                    )?;
                    if key {
                        live.pool.update_gc_height(height, height);
                    }
                }
                live.height = height;
            }
            live.chain = Status {
                revision: event.revision,
                tip: Some(event.new_tip),
            };
        }
        if live.chain != current {
            return Err(Error::Corrupt("pool event tip").into());
        }
        live.revision = stored.revision;
        let mut writes = repo::Writes::new();
        for hash in collected {
            if !live.members.contains(&hash) {
                writes.insert(repo::key(MEMBERS, hash.0), None);
            }
        }
        Ok(Plan { live, writes })
    }
    fn finish(
        &mut self,
        stored: &Stored<Snapshot<'_>>,
        mut plan: Plan,
        check: impl FnMut() -> Result<(), Error>,
    ) -> Result<(), Failure> {
        match self
            .db
            .commit_pool(stored, plan.writes, self.config.commit_bytes, check)
        {
            Ok(revision) => {
                plan.live.revision = revision;
                self.live = plan.live;
                Ok(())
            }
            Err(e) => {
                if matches!(e, Error::Backend(_)) {
                    self.uncertain = true;
                }
                Err(e.into())
            }
        }
    }
    pub fn refresh(&mut self) -> Result<Inspection, Failure> {
        let db = self.db.clone();
        let stored = db.pool_view()?;
        let plan = self.plan(&stored)?;
        self.finish(&stored, plan, || Ok(()))?;
        Ok(self.inspection())
    }
    fn inspection(&self) -> Inspection {
        let (active, visited) = self.live.pool.sizes();
        Inspection {
            chain: self.live.chain,
            revision: self.live.revision,
            active,
            visited,
            retained_record_bytes: self.live.pool.retained_record_bytes(),
            gc_height: self.live.pool.gc_height(),
        }
    }
    pub fn push(
        &mut self,
        tx: Arc<Signed>,
        event: policy::Event,
        gossip_cached: bool,
    ) -> Result<Submission, Failure> {
        if event == policy::Event::Received && gossip_cached {
            // The original cache shortcut does not enter the pool service or
            // read chain state. This is the last observed revision, not a new admission.
            return Ok(Submission {
                outcome: policy::PushOutcome::Cached,
                chain: self.live.chain,
                revision: self.live.revision,
            });
        }
        self.submit(tx, Some((event, gossip_cached)), || Ok(()))
    }
    pub fn force_push(&mut self, tx: Arc<Signed>) -> Result<Submission, Failure> {
        self.submit(tx, None, || Ok(()))
    }
    fn submit(
        &mut self,
        tx: Arc<Signed>,
        options: Option<(policy::Event, bool)>,
        check: impl FnMut() -> Result<(), Error>,
    ) -> Result<Submission, Failure> {
        let db = self.db.clone();
        let stored = db.pool_view()?;
        let mut plan = self.plan(&stored)?;
        let mut location = stored.location(tx.hash())?;
        if location == Location::Pool
            && plan.writes.get(&repo::key(MEMBERS, tx.hash().0)) == Some(&None)
        {
            location = Location::Deleted;
        }
        let outcome = if let Some((event, gossip_cached)) = options {
            let (hash, header) = tip(&stored)?;
            let trees = stored.chain.trees(hash, self.config.overlay)?;
            plan.live.pool.push(
                tx.clone(),
                policy::PushOptions {
                    event,
                    location,
                    gossip_cached,
                },
                &policy::Admission {
                    trees: &trees,
                    network: &self.network,
                    protocol: Protocol::try_from(header.common().version)
                        .map_err(Error::Protocol)?,
                    consensus: self.config.consensus,
                    height: header.common().height,
                    record: stored.chain.config().transaction_record(),
                },
            )?
        } else {
            plan.live
                .pool
                .force_push(tx.clone(), stored.chain.config().transaction_record())?
        };
        if matches!(outcome, policy::PushOutcome::Accepted { .. }) {
            stored.retain(&tx, &mut plan.writes)?;
            if !matches!(location, Location::Block(_)) {
                plan.live.members.insert(tx.hash());
            }
        }
        self.finish(&stored, plan, check)?;
        Ok(Submission {
            outcome,
            chain: self.live.chain,
            revision: self.live.revision,
        })
    }
    pub fn peek(
        &mut self,
        max: usize,
        account: Option<AccountPublicKey>,
        nonce: Option<&Unsigned>,
    ) -> Result<Vec<Arc<Signed>>, Failure> {
        let db = self.db.clone();
        let stored = db.pool_view()?;
        let plan = self.plan(&stored)?;
        let values = plan.live.pool.peek(max, account, nonce);
        self.finish(&stored, plan, || Ok(()))?;
        Ok(values)
    }
    pub fn inspect(&mut self, hash: Hash256) -> Result<policy::Inspection, Failure> {
        let db = self.db.clone();
        let stored = db.pool_view()?;
        let plan = self.plan(&stored)?;
        let value = plan.live.pool.inspect(hash, stored.location(hash)?)?;
        self.finish(&stored, plan, || Ok(()))?;
        Ok(value)
    }
    pub fn max_nonce(&mut self, account: AccountPublicKey) -> Result<Option<Unsigned>, Failure> {
        let db = self.db.clone();
        let stored = db.pool_view()?;
        let plan = self.plan(&stored)?;
        let value = plan.live.pool.max_nonce(account).cloned();
        self.finish(&stored, plan, || Ok(()))?;
        Ok(value)
    }
    pub fn delete(&mut self, hash: Hash256) -> Result<(), Failure> {
        let db = self.db.clone();
        let stored = db.pool_view()?;
        let mut plan = self.plan(&stored)?;
        plan.live.pool.delete(hash, stored.location(hash)?)?;
        plan.live.members.remove(&hash);
        plan.writes.insert(repo::key(MEMBERS, hash.0), None);
        self.finish(&stored, plan, || Ok(()))
    }
    pub fn candidate(
        &mut self,
        gas: u64,
        ignore: &BTreeSet<Hash256>,
        options: policy::SelectOptions,
        now: impl FnMut() -> i64,
    ) -> Result<policy::Candidate, Failure> {
        let db = self.db.clone();
        let stored = db.pool_view()?;
        let mut plan = self.plan(&stored)?;
        let (block, header) = tip(&stored)?;
        let trees = stored.chain.trees(block, self.config.overlay)?;
        let result = plan.live.pool.candidate(
            policy::CandidateRequest {
                block,
                height: header.common().height,
                protocol: Protocol::try_from(header.common().version).map_err(Error::Protocol)?,
                max_gas: gas,
                ignore,
                options,
            },
            |k| lookup(&trees.accounts, k),
            now,
        )?;
        self.finish(&stored, plan, || Ok(()))?;
        Ok(result)
    }
    pub fn record_failure(
        &mut self,
        hash: Hash256,
        reason: &str,
        tuple: bool,
    ) -> Result<bool, Failure> {
        let db = self.db.clone();
        let stored = db.pool_view()?;
        let mut plan = self.plan(&stored)?;
        let limit = plan.live.pool.transaction(hash).and_then(|tx| {
            self.config.failures.limit(
                kind_name(tx.transaction().view().kind()),
                self.config.failures.reason(reason, tuple),
            )
        });
        let removed = plan.live.pool.record_failure(hash, limit)?;
        if removed {
            plan.live.members.remove(&hash);
            plan.writes.insert(repo::key(MEMBERS, hash.0), None);
        }
        self.finish(&stored, plan, || Ok(()))?;
        Ok(removed)
    }
    pub fn collect(&mut self, origins: bool, expiry_limit: usize) -> Result<Vec<Hash256>, Failure> {
        let db = self.db.clone();
        let stored = db.pool_view()?;
        let mut plan = self.plan(&stored)?;
        let (hash, header) = tip(&stored)?;
        let mut gone =
            plan.live
                .pool
                .collect_expired(header.common().height, expiry_limit, |h| {
                    Ok(plan.live.members.contains(&h))
                })?;
        if origins {
            let trees = stored.chain.trees(hash, self.config.overlay)?;
            gone.extend(plan.live.pool.collect_origins(
                |k| lookup(&trees.accounts, k),
                |h| Ok(plan.live.members.contains(&h)),
            )?);
        }
        for hash in &gone {
            plan.live.members.remove(hash);
            plan.writes.insert(repo::key(MEMBERS, hash.0), None);
        }
        self.finish(&stored, plan, || Ok(()))?;
        Ok(gone)
    }
    pub fn sync_target(&mut self, target: u64) -> Result<(), Failure> {
        let db = self.db.clone();
        let stored = db.pool_view()?;
        let mut plan = self.plan(&stored)?;
        plan.live.pool.update_gc_height(plan.live.height, target);
        self.finish(&stored, plan, || Ok(()))
    }
    pub fn reconfigure(
        &mut self,
        policy: policy::Config,
        failures: policy::FailureSettings,
    ) -> Result<(), Failure> {
        let db = self.db.clone();
        let stored = db.pool_view()?;
        let mut plan = self.plan(&stored)?;
        plan.live.pool.reconfigure(policy.clone());
        self.finish(&stored, plan, || Ok(()))?;
        self.config.policy = policy;
        self.config.failures = failures;
        Ok(())
    }
}
fn kind_name(kind: ae_tx::TransactionKind) -> &'static str {
    use ae_tx::TransactionKind::*;
    match kind {
        ChannelCloseMutual => "channel_close_mutual_tx",
        ChannelCloseSolo => "channel_close_solo_tx",
        ChannelCreate => "channel_create_tx",
        ChannelDeposit => "channel_deposit_tx",
        ChannelForceProgress => "channel_force_progress_tx",
        ChannelOffchain => "channel_offchain_tx",
        ChannelSetDelegates => "channel_set_delegates_tx",
        ChannelSettle => "channel_settle_tx",
        ChannelSlash => "channel_slash_tx",
        ChannelSnapshotSolo => "channel_snapshot_solo_tx",
        ChannelWithdraw => "channel_withdraw_tx",
        ContractCall => "contract_call_tx",
        ContractCreate => "contract_create_tx",
        GaAttach => "ga_attach_tx",
        GaMeta => "ga_meta_tx",
        HcVote => "hc_vote_tx",
        NameClaim => "name_claim_tx",
        NamePreclaim => "name_preclaim_tx",
        NameRevoke => "name_revoke_tx",
        NameTransfer => "name_transfer_tx",
        NameUpdate => "name_update_tx",
        OracleExtend => "oracle_extend_tx",
        OracleQuery => "oracle_query_tx",
        OracleRegister => "oracle_register_tx",
        OracleResponse => "oracle_response_tx",
        PayingFor => "paying_for_tx",
        Spend => "spend_tx",
    }
}
#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
