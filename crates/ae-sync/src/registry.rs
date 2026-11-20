use ae_p2p::{
    discovery,
    registry::{self as policy, Command, Effect, Outcome, Prepared, Registry},
};
use ae_storage::{
    Database,
    peers::{self, Record, Version, Writer},
};
use ae_types::PeerPublicKey;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);
pub mod startup;
pub mod worker;

#[derive(Debug, Clone)]
pub struct Config {
    pub policy: policy::Config,
    pub storage: peers::Limits,
    pub effects: usize,
    pub effect_bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            policy: Default::default(),
            storage: Default::default(),
            effects: 8192,
            effect_bytes: 8 << 20,
        }
    }
}
#[derive(Debug)]
pub enum Error {
    Policy(policy::Error),
    Storage(peers::Failure),
    Configuration,
    EffectsFull,
    RecoveryRequired,
    Inconsistent,
    SequenceExhausted,
    Startup(startup::Error),
    StartupPending,
    TrustedPending,
}
impl From<startup::Error> for Error {
    fn from(error: startup::Error) -> Self {
        Self::Startup(error)
    }
}
impl From<policy::Error> for Error {
    fn from(e: policy::Error) -> Self {
        Self::Policy(e)
    }
}
impl From<peers::Failure> for Error {
    fn from(e: peers::Failure) -> Self {
        Self::Storage(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryId {
    owner: u64,
    sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub id: DeliveryId,
    pub effect: Effect,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recovery {
    Unchanged,
    NotCommitted,
    Committed(Outcome),
}

trait Store {
    fn recover(&mut self) -> Result<(Version, Vec<Record>), peers::Failure>;
    fn commit(
        &mut self,
        version: Version,
        changes: &[peers::Change],
    ) -> Result<Version, peers::Failure>;
    fn uncertain(&self) -> bool;
}
impl Store for Writer {
    fn recover(&mut self) -> Result<(Version, Vec<Record>), peers::Failure> {
        self.recover()
    }
    fn commit(&mut self, v: Version, c: &[peers::Change]) -> Result<Version, peers::Failure> {
        self.commit(v, c)
    }
    fn uncertain(&self) -> bool {
        self.recovery_required()
    }
}
struct Pending {
    prepared: Prepared,
    writes: Vec<peers::Change>,
    budget: (usize, usize),
    retire: Vec<PeerPublicKey>,
    initial: bool,
}
struct Core<S> {
    registry: Registry,
    store: S,
    version: Version,
    records: BTreeMap<PeerPublicKey, Record>,
    pending: Option<Pending>,
    failed: bool,
    queue: VecDeque<Delivery>,
    effect_bytes: usize,
    limit: (usize, usize),
    owner: u64,
    sequence: u64,
    host_bytes: usize,
    startup: Option<startup::State>,
}
// This owner retains effects independently of a command's reply. The executor
// must be a single ordered consumer: process the front delivery idempotently
// by token, then acknowledge it. Repeated observations are not new work.
// Address restoration/configuration/DNS are explicit startup responsibilities;
// saved_peers retains even unresolved or unsupported-source records losslessly.
pub struct Service {
    core: Core<Writer>,
}
impl Service {
    pub fn start(db: Arc<Database>, local: PeerPublicKey, config: Config) -> Result<Self, Error> {
        let registry = Registry::new(local, config.policy.clone())?;
        let store = Writer::start(db, config.storage)?;
        Ok(Self {
            core: Core::start(registry, store, &config)?,
        })
    }
    pub fn configure_startup(
        &mut self,
        now: u64,
        configuration: startup::Configuration,
        settings: startup::Settings,
    ) -> Result<startup::Progress, Error> {
        let network = self.core.store.network_id().to_vec();
        self.core
            .configure_startup(now, &network, configuration, settings)
    }
    pub fn advance_startup(
        &mut self,
        now: u64,
        maximum: usize,
    ) -> Result<startup::Progress, Error> {
        self.core.advance_startup(now, maximum)
    }
    pub fn startup_status(&self) -> Option<startup::Progress> {
        self.core.startup.as_ref().map(startup::State::status)
    }
    pub fn startup_diagnostics(
        &self,
        offset: usize,
        maximum: usize,
    ) -> Result<Vec<startup::Diagnostic>, Error> {
        if maximum > 4096 {
            return Err(startup::Error::ResourceLimit.into());
        }
        let state = self
            .core
            .startup
            .as_ref()
            .ok_or(startup::Error::NotConfigured)?;
        let start = offset.min(state.diagnostics.len());
        let end = start.saturating_add(maximum).min(state.diagnostics.len());
        Ok(state.diagnostics[start..end].to_vec())
    }
    pub fn apply(&mut self, now: u64, command: Command) -> Result<Outcome, Error> {
        self.core.apply(now, command)
    }
    pub fn recover(&mut self) -> Result<Recovery, Error> {
        self.core.recover()
    }
    pub fn recovery_required(&self) -> bool {
        self.core.pending.is_some() || self.core.failed
    }
    pub fn policy(&self) -> &Registry {
        &self.core.registry
    }
    pub fn saved_peers(&self) -> impl Iterator<Item = &Record> {
        self.core.records.values()
    }
    pub fn version(&self) -> Version {
        self.core.version
    }
    pub fn next_effect(&self) -> Result<Option<&Delivery>, Error> {
        self.core.next_effect()
    }
    pub fn acknowledge(&mut self, id: DeliveryId) -> bool {
        self.core.acknowledge(id)
    }
    pub fn queued_effects(&self) -> (usize, usize) {
        (self.core.queue.len(), self.core.effect_bytes)
    }
}
fn record(peer: &discovery::Peer) -> Record {
    Record {
        id: peer.id,
        host: peer.host.clone(),
        address: peer.address.into(),
        source: peer.source.into(),
        port: peer.port,
        trusted: peer.trusted,
    }
}
fn records(rows: Vec<Record>) -> Result<BTreeMap<PeerPublicKey, Record>, Error> {
    let mut result = BTreeMap::new();
    for row in rows {
        if result.insert(row.id, row).is_some() {
            return Err(Error::Inconsistent);
        }
    }
    Ok(result)
}
fn apply_writes(rows: &mut BTreeMap<PeerPublicKey, Record>, writes: &[peers::Change]) {
    for write in writes {
        match write {
            peers::Change::Put(row) => {
                rows.insert(row.id, row.clone());
            }
            peers::Change::Delete(id) => {
                rows.remove(id);
            }
        }
    }
}
impl<S: Store> Core<S> {
    fn start(registry: Registry, mut store: S, config: &Config) -> Result<Self, Error> {
        if config.effects == 0
            || config.effects > 1_000_000
            || config.effect_bytes == 0
            || config.effect_bytes > 1 << 30
        {
            return Err(Error::Configuration);
        }
        let (version, rows) = store.recover()?;
        let owner = NEXT_OWNER
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| Error::SequenceExhausted)?;
        Ok(Self {
            registry,
            store,
            version,
            records: records(rows)?,
            pending: None,
            failed: false,
            queue: VecDeque::new(),
            effect_bytes: 0,
            limit: (config.effects, config.effect_bytes),
            owner,
            sequence: 0,
            host_bytes: config.policy.pool.maximum_host_bytes,
            startup: None,
        })
    }
    fn ready(&self) -> Result<(), Error> {
        self.storage_ready()?;
        if self.startup.as_ref().is_some_and(|s| !s.installed) {
            return Err(Error::StartupPending);
        }
        Ok(())
    }
    fn storage_ready(&self) -> Result<(), Error> {
        if self.failed {
            Err(Error::Inconsistent)
        } else if self.pending.is_some() {
            Err(Error::RecoveryRequired)
        } else {
            Ok(())
        }
    }
    fn plan(&self, now: u64, command: Command) -> Result<Pending, Error> {
        let remove = match &command {
            Command::Delete(id) => Some(*id),
            Command::Block(info) => Some(info.id),
            _ => None,
        };
        let prepared = self.registry.prepare(now, command)?;
        let mut writes: Vec<_> = prepared
            .persistence()
            .map(|e| match e {
                discovery::Effect::Persist(p) => peers::Change::Put(record(p)),
                discovery::Effect::Delete(p) => peers::Change::Delete(p.id),
            })
            .collect();
        // Deleting/blocking an address still awaiting startup resolution must
        // remove its saved row as well, so it cannot reappear on restart.
        if let Some(id) = remove
            && self.records.contains_key(&id)
            && !matches!(prepared.result(), Outcome::Ignored)
            && !writes
                .iter()
                .any(|w| matches!(w,peers::Change::Delete(k) if *k==id))
        {
            writes.push(peers::Change::Delete(id));
        }
        let retire = remove
            .filter(|_| !matches!(prepared.result(), Outcome::Ignored))
            .into_iter()
            .collect();
        self.budget(prepared, writes, retire, false)
    }
    fn budget(
        &self,
        prepared: Prepared,
        writes: Vec<peers::Change>,
        retire: Vec<PeerPublicKey>,
        initial: bool,
    ) -> Result<Pending, Error> {
        let budget = prepared.effect_budget()?;
        self.sequence
            .checked_add(budget.0 as u64)
            .ok_or(Error::SequenceExhausted)?;
        let mut needed = budget;
        for delivery in &self.queue {
            if prepared.effect_is_current(&delivery.effect) {
                needed.0 = needed.0.checked_add(1).ok_or(Error::EffectsFull)?;
                needed.1 = needed
                    .1
                    .checked_add(delivery.effect.retained_bytes()?)
                    .ok_or(Error::EffectsFull)?;
            }
        }
        if needed.0 > self.limit.0 || needed.1 > self.limit.1 {
            return Err(Error::EffectsFull);
        }
        Ok(Pending {
            prepared,
            writes,
            budget,
            retire,
            initial,
        })
    }
    fn apply(&mut self, now: u64, command: Command) -> Result<Outcome, Error> {
        self.ready()?;
        let command = match &command {
            Command::Add(peer) if self.protected(peer.id) => return Err(Error::TrustedPending),
            Command::Resolve { peer, .. } if self.protected(peer.id) => {
                return Err(Error::TrustedPending);
            }
            Command::Accept { token, peer } if self.protected(peer.id) => Command::RejectInbound {
                token: *token,
                reason: policy::Rejection::TrustedPending,
            },
            _ => command,
        };
        self.execute(now, command)
    }
    fn protected(&self, id: PeerPublicKey) -> bool {
        self.startup
            .as_ref()
            .is_some_and(|s| s.trusted.contains_key(&id))
    }
    fn execute(&mut self, now: u64, command: Command) -> Result<Outcome, Error> {
        self.storage_ready()?;
        let pending = self.plan(now, command)?;
        self.write(pending)
    }
    fn write(&mut self, pending: Pending) -> Result<Outcome, Error> {
        match self.store.commit(self.version, &pending.writes) {
            Ok(version) => self.publish(pending, version),
            Err(error) => {
                if self.store.uncertain() {
                    self.pending = Some(pending);
                }
                Err(error.into())
            }
        }
    }
    fn publish(&mut self, pending: Pending, version: Version) -> Result<Outcome, Error> {
        let increment = u64::from(!pending.writes.is_empty());
        if !version.same_database(self.version)
            || self.version.revision().checked_add(increment) != Some(version.revision())
        {
            self.failed = true;
            return Err(Error::Inconsistent);
        }
        let change = match self.registry.commit(pending.prepared) {
            Ok(change) => change,
            Err(_) => {
                self.failed = true;
                return Err(Error::Inconsistent);
            }
        };
        apply_writes(&mut self.records, &pending.writes);
        self.version = version;
        self.queue
            .retain(|d| self.registry.effect_is_current(&d.effect));
        self.effect_bytes = self
            .queue
            .iter()
            .map(|d| d.effect.retained_bytes().expect("preflight budget"))
            .sum();
        let mut actual = (0, 0);
        for effect in change.effects {
            if !self.registry.effect_is_current(&effect) {
                continue;
            }
            self.sequence += 1;
            let bytes = effect.retained_bytes().expect("preflight budget");
            actual.0 += 1;
            actual.1 += bytes;
            self.effect_bytes += bytes;
            self.queue.push_back(Delivery {
                id: DeliveryId {
                    owner: self.owner,
                    sequence: self.sequence,
                },
                effect,
            });
        }
        assert_eq!(actual, pending.budget);
        if let Some(startup) = &mut self.startup {
            if pending.initial {
                startup.installed = true;
                startup.blocks.clear();
            }
            for id in pending.retire {
                startup.retire(id);
            }
            startup.reconcile(&self.registry);
        }
        Ok(change.result)
    }
    fn recover(&mut self) -> Result<Recovery, Error> {
        if self.failed {
            return Err(Error::Inconsistent);
        }
        let (version, rows) = self.store.recover()?;
        let rows = records(rows)?;
        if version == self.version && rows == self.records {
            return Ok(if self.pending.take().is_some() {
                Recovery::NotCommitted
            } else {
                Recovery::Unchanged
            });
        }
        let Some(pending) = &self.pending else {
            self.failed = true;
            return Err(Error::Inconsistent);
        };
        let mut expected = self.records.clone();
        apply_writes(&mut expected, &pending.writes);
        if pending.writes.is_empty()
            || !version.same_database(self.version)
            || self.version.revision().checked_add(1) != Some(version.revision())
            || rows != expected
        {
            return Err(Error::Inconsistent);
        }
        let pending = self.pending.take().unwrap();
        Ok(Recovery::Committed(self.publish(pending, version)?))
    }
    fn configure_startup(
        &mut self,
        now: u64,
        network: &[u8],
        configuration: startup::Configuration,
        settings: startup::Settings,
    ) -> Result<startup::Progress, Error> {
        self.storage_ready()?;
        if self.registry.revision() != 0 || self.startup.is_some() || !self.queue.is_empty() {
            return Err(startup::Error::NotFresh.into());
        }
        let startup = startup::State::build(
            network,
            &self.registry,
            &self.records,
            configuration,
            settings,
        )?;
        self.startup = Some(startup);
        self.install_startup(now)?;
        Ok(self.startup.as_ref().unwrap().status())
    }
    fn install_startup(&mut self, now: u64) -> Result<(), Error> {
        self.storage_ready()?;
        let startup = self.startup.as_ref().ok_or(startup::Error::NotConfigured)?;
        let prepared = self.registry.prepare_initial_blocks(now, &startup.blocks)?;
        let writes = startup
            .blocks
            .iter()
            .filter(|p| self.records.contains_key(&p.id))
            .map(|p| peers::Change::Delete(p.id))
            .collect();
        let pending = self.budget(prepared, writes, vec![], true)?;
        self.write(pending)?;
        Ok(())
    }
    fn advance_startup(&mut self, now: u64, maximum: usize) -> Result<startup::Progress, Error> {
        self.storage_ready()?;
        let startup = self.startup.as_ref().ok_or(startup::Error::NotConfigured)?;
        if maximum == 0 || maximum > startup.settings.maximum_step {
            return Err(startup::Error::ResourceLimit.into());
        }
        if !startup.installed {
            self.install_startup(now)?;
        }
        self.startup.as_mut().unwrap().progress.backpressured = false;
        for _ in 0..maximum {
            let Some(item) = self.startup.as_ref().unwrap().queue.front().cloned() else {
                break;
            };
            if let startup::Item::Saved(row) = &item {
                let reason = if self.records.get(&row.id) != Some(row) {
                    Some(startup::Reason::Stale)
                } else if self.registry.pool().find(row.id).is_some() {
                    Some(startup::Reason::AlreadyPresent)
                } else {
                    None
                };
                if let Some(reason) = reason {
                    let state = self.startup.as_mut().unwrap();
                    state.queue.pop_front();
                    state.diagnostic(startup::Origin::Saved(row.id), reason);
                    continue;
                }
            }
            let (peer, source, trusted) = self.startup.as_ref().unwrap().intent(&item);
            if !self.registry.resolution_capacity(&peer.host, peer.id) {
                let state = self.startup.as_mut().unwrap();
                let item = state.queue.pop_front().unwrap();
                state.queue.push_back(item);
                state.progress.backpressured = true;
                continue;
            }
            match self.execute(
                now,
                Command::Resolve {
                    peer,
                    source,
                    trusted,
                },
            ) {
                Ok(_) => {
                    let state = self.startup.as_mut().unwrap();
                    state.queue.pop_front();
                    state.queued_trusted.remove(&item.id());
                    state.progress.submitted += 1;
                    state.reconcile(&self.registry);
                }
                Err(Error::EffectsFull) => {
                    self.startup.as_mut().unwrap().progress.backpressured = true;
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(self.startup.as_ref().unwrap().status())
    }
    fn next_effect(&self) -> Result<Option<&Delivery>, Error> {
        self.ready()?;
        Ok(self.queue.front())
    }
    fn acknowledge(&mut self, id: DeliveryId) -> bool {
        if self.queue.front().is_some_and(|d| d.id == id) {
            let delivery = self.queue.pop_front().unwrap();
            self.effect_bytes -= delivery.effect.retained_bytes().expect("preflight budget");
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests;
