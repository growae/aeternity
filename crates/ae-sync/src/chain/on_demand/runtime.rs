// An explicitly owned, synchronous client_request adapter. Call on a blocking
// owner thread: accepted worker tickets are always consumed before returning.
// There is no automatic mining loop, retry queue or implicit public endpoint.
use super::control::{self, Command, Host};
use crate::{
    chain::{
        self,
        pow::{generator::Clock, packing, preparation},
    },
    keys,
};
use ae_chain::{
    block::Block,
    hyperchain::candidate,
    publication,
    repository::{self as repo, ledger, pow},
};
use ae_execution::transactions::owned::Owned;
use ae_protocol::ConsensusAlgorithm;
use ae_storage::{events, pool::worker, snapshots};
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use ae_vm_fate::maps::{MapOrder, order::Otp26};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct Config {
    pub beneficiary: AccountPublicKey,
    pub release_info: u32,
    pub commands: control::Limits,
    pub key: pow::key::Limits,
    pub micro: candidate::Limits,
    pub result_bytes: usize,
    pub event_catchup: usize,
    // Explicit startup reconciliation; otherwise a candidate key selected
    // before this owner started is not silently promoted.
    pub resume_current: bool,
}
impl Config {
    pub fn new(beneficiary: AccountPublicKey) -> Self {
        Self {
            beneficiary,
            release_info: 0,
            commands: Default::default(),
            key: Default::default(),
            micro: Default::default(),
            result_bytes: 64 << 20,
            event_catchup: 1024,
            resume_current: false,
        }
    }
}
#[derive(Debug)]
pub enum Error {
    Binding,
    Configuration,
    Stopped,
    RecoveryRequired,
    Time,
    ResourceLimit,
    Panicked,
    Repository(repo::Error),
    Events(events::Error),
    Snapshot(snapshots::Error),
    Keys(keys::Error),
    Pool(worker::Error),
    Preparation(preparation::Error),
    Micro(ae_storage::pow::Error),
    Packing(packing::Error),
    AdmissionStart(chain::Error),
    Admission(usize),
    Encoding,
}
#[derive(Debug)]
pub struct Admission {
    pub block: Arc<Block>,
    pub origin: ledger::Origin,
    pub result: Result<chain::Report, chain::Failure>,
}
#[derive(Debug)]
pub struct Promotion {
    pub miner: AccountPublicKey,
    pub result: Result<keys::PublicKeys, keys::Error>,
}
#[derive(Debug)]
pub struct Report {
    pub command: control::Report<Arc<Block>, Error>,
    // Journaled before any subsequent promotion, cancellation or event read.
    pub admissions: Vec<Admission>,
    pub promotions: Vec<Promotion>,
    pub cancelled_packing: Option<packing::CancelReport>,
    pub recovery_required: bool,
}
#[derive(Clone)]
pub struct Stop(Arc<AtomicBool>);
impl Stop {
    pub fn request(&self) {
        self.0.store(true, Ordering::Release);
    }
}
struct Leases {
    _group: ae_storage::pow::GroupLease,
    _producer: ae_storage::pow::ProducerLease,
    _generator: ae_storage::pow::GeneratorLease,
}
// A caller retiring an owner with an uncertain pool receipt must keep this
// value until the same pool worker has joined. No replacement owner can acquire
// these leases while this receipt is retained.
pub struct Retired {
    _leases: Leases,
    pending: Option<packing::Cancelled>,
}
impl Retired {
    pub fn pending_pool(&self) -> bool {
        self.pending.is_some()
    }
    pub fn settle_pool(
        &mut self,
        joined: &worker::Client,
    ) -> Result<Option<packing::CancelReport>, packing::Error> {
        let Some(pending) = self.pending.as_mut() else {
            return Ok(None);
        };
        if !pending.retire_pool(joined)? {
            return Err(packing::Error::Binding);
        }
        if pending.step()? {
            Ok(Some(self.pending.take().unwrap().finish()))
        } else {
            Ok(None)
        }
    }
}
pub struct Controller<O = Otp26> {
    importer: chain::Importer<O>,
    snapshots: snapshots::Owner,
    pool: worker::Client,
    keys: keys::Client,
    clock: Arc<dyn Clock>,
    execution: Arc<Owned<O>>,
    builder: preparation::Builder<O>,
    micro: pow::Config<O>,
    config: Config,
    cursor: events::Cursor,
    stop: Stop,
    failed: bool,
    packing: Option<packing::Packing<O>>,
    quarantine: Option<packing::Cancelled>,
    leases: Leases,
    resume: Option<AccountPublicKey>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Controller<O> {
    pub fn new(
        importer: &chain::Importer<O>,
        snapshots: snapshots::Owner,
        pool: worker::Client,
        keys: keys::Client,
        clock: Arc<dyn Clock>,
        config: Config,
    ) -> Result<Self, Error> {
        let db = importer.database();
        let chain::strategy::Strategy::Pow(profile) = &importer.context.strategy else {
            return Err(Error::Binding);
        };
        if profile.algorithm != ConsensusAlgorithm::OnDemand
            || !snapshots.uses_database(db)
            || !pool.uses_database(db)
        {
            return Err(Error::Binding);
        }
        importer.check_open().map_err(Error::AdmissionStart)?;
        if !pool.info().accepting || !keys.info().accepting {
            return Err(Error::Stopped);
        }
        if config.event_catchup == 0 || config.key.history == 0 || config.result_bytes == 0 {
            return Err(Error::Configuration);
        }
        let (view, cursor) = db.event_snapshot().map_err(Error::Repository)?;
        let identity = view.identity().clone();
        let selected = view
            .require_block(view.status().tip.ok_or(Error::Binding)?)
            .map_err(Error::Repository)?;
        drop(view);
        let execution = Arc::new(profile.execution.clone());
        let micro = execution.with_config(|base| {
            let fate = base.fate.expect("Owned retains FATE");
            let gas = &profile.rules.admission.configured_gas_limit;
            if gas.sign() == num_bigint::Sign::Minus {
                return Err(Error::Configuration);
            }
            Ok(pow::Config {
                identity: identity.clone(),
                profile: Arc::new(pow::Profile {
                    contracts: ae_hyperchain::consensus_contract::Contracts {
                        owner: AccountPublicKey([0; 32]),
                        election: [0; 32],
                        staking: [0; 32],
                        rewards: [0; 32],
                    },
                    network: base.network.clone(),
                    context: fate.context.clone(),
                    order: fate.order.clone(),
                    overrides: fate.overrides.clone(),
                    diagnostic: fate.diagnostic_profile,
                    limits: base.fate_limits,
                    maximum_steps: base.maximum_fate_steps,
                    maximum_history_reads: config.key.history,
                }),
                execution: pow::Execution {
                    overlays: config.key.overlay,
                    maximum_history_headers: config.key.history,
                    configured_block_gas_limit: Unsigned::from_field_bytes(&gas.to_bytes_be().1)
                        .map_err(|_| Error::Configuration)?,
                    aevm: base.aevm.cloned(),
                },
                micro_cycle_ms: profile.rules.admission.micro_cycle_ms,
            })
        })?;
        let builder = preparation::Builder::new(
            snapshots.clone(),
            preparation::Config {
                identity,
                execution: execution.clone(),
                rules: Arc::new(profile.rules.clone()),
                limits: config.key,
            },
        )
        .map_err(Error::Preparation)?;
        let leases = Leases {
            _group: ae_storage::pow::GroupLease::acquire(db).map_err(Error::Snapshot)?,
            _producer: ae_storage::pow::ProducerLease::acquire(db).map_err(Error::Snapshot)?,
            _generator: ae_storage::pow::GeneratorLease::acquire(db).map_err(Error::Snapshot)?,
        };
        let mut owner = Self {
            importer: importer.borrowed(),
            snapshots,
            pool,
            keys,
            clock,
            execution,
            builder,
            micro,
            config,
            cursor,
            stop: Stop(Arc::new(AtomicBool::new(false))),
            failed: false,
            packing: None,
            quarantine: None,
            leases,
            resume: None,
        };
        if owner.config.resume_current {
            owner.resume = Some(owner.leader(&selected)?);
        }
        Ok(owner)
    }
    pub fn stop_handle(&self) -> Stop {
        self.stop.clone()
    }
    pub fn recovery_required(&self) -> bool {
        self.failed || self.quarantine.is_some()
    }
    pub fn retire(mut self) -> Retired {
        self.stop.request();
        let pending = self
            .quarantine
            .take()
            .or_else(|| self.packing.take().map(packing::Packing::cancel));
        Retired {
            _leases: self.leases,
            pending,
        }
    }
    pub fn execute(&mut self, command: Command) -> Report {
        let limits = self.config.commands;
        let mut host = Runtime {
            owner: self,
            admissions: vec![],
            promotions: vec![],
            bytes: 0,
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            host.check().map_err(control::Error::Host)?;
            Ok(control::run(command, &mut host, limits))
        }));
        let command = match result {
            Ok(Ok(report)) => report,
            Ok(Err(error)) => control::Report {
                produced: vec![],
                result: Err(error),
            },
            Err(_) => {
                host.owner.failed = true;
                control::Report {
                    produced: host
                        .admissions
                        .iter()
                        .filter(|a| a.result.is_ok())
                        .map(|a| a.block.clone())
                        .collect(),
                    result: Err(control::Error::Host(Error::Panicked)),
                }
            }
        };
        if let Some(pending) = host.owner.packing.take() {
            host.owner.quarantine = Some(pending.cancel());
        }
        let cancelled_packing = if let Some(pending) = host.owner.quarantine.as_mut() {
            loop {
                match pending.step() {
                    Ok(true) => break Some(host.owner.quarantine.take().unwrap().finish()),
                    Ok(false) => pause(),
                    Err(_) => {
                        host.owner.failed = true;
                        break None;
                    }
                }
            }
        } else {
            None
        };
        Report {
            command,
            admissions: host.admissions,
            promotions: host.promotions,
            cancelled_packing,
            recovery_required: host.owner.recovery_required(),
        }
    }
    fn leader(&self, block: &Block) -> Result<AccountPublicKey, Error> {
        let key = match block {
            Block::Key(key) => key.clone(),
            Block::Micro(micro) => {
                let Block::Key(key) = self
                    .importer
                    .database()
                    .view()
                    .map_err(Error::Repository)?
                    .require_block(Hash256(micro.header.common.previous_key_hash))
                    .map_err(Error::Repository)?
                else {
                    return Err(Error::Binding);
                };
                key
            }
        };
        Ok(AccountPublicKey(key.miner))
    }
    fn promote(
        &mut self,
        miner: AccountPublicKey,
        journal: &mut Vec<Promotion>,
    ) -> Result<(), Error> {
        let info = self.keys.info();
        if !info.accepting {
            return Err(Error::Stopped);
        }
        if miner == info.keys.candidate {
            let mut ticket = self.keys.promote_candidate(miner).map_err(Error::Keys)?;
            let result = wait_key(&mut ticket);
            let error = result.as_ref().err().cloned();
            journal.push(Promotion { miner, result });
            if let Some(error) = error {
                self.failed = true;
                return Err(Error::Keys(error));
            }
        }
        Ok(())
    }
}
fn pause() {
    thread::sleep(Duration::from_millis(1));
}
fn wait_key<T>(ticket: &mut keys::Ticket<T>) -> Result<T, keys::Error> {
    loop {
        if let Some(result) = ticket.try_result() {
            return result;
        }
        pause();
    }
}
struct Runtime<'a, O> {
    owner: &'a mut Controller<O>,
    admissions: Vec<Admission>,
    promotions: Vec<Promotion>,
    bytes: usize,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Runtime<'_, O> {
    fn check(&self) -> Result<(), Error> {
        if self.owner.recovery_required() {
            return Err(Error::RecoveryRequired);
        }
        if self.owner.stop.0.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        self.owner
            .importer
            .check_open()
            .map_err(Error::AdmissionStart)
    }
    fn time(&self) -> Result<u64, Error> {
        u64::try_from(self.owner.clock.unix_millis()).map_err(|_| Error::Time)
    }
    fn events(&mut self) -> Result<(), Error> {
        self.check()?;
        if let Some(miner) = self.owner.resume {
            self.owner.promote(miner, &mut self.promotions)?;
            self.owner.resume = None;
        }
        for _ in 0..self.owner.config.event_catchup {
            let batch = match self
                .owner
                .importer
                .database()
                .events()
                .after(&self.owner.cursor, 1)
            {
                Ok(batch) => batch,
                Err(error) => {
                    self.owner.failed = true;
                    return Err(Error::Events(error));
                }
            };
            if batch.closed {
                self.owner.failed = true;
                return Err(Error::Stopped);
            }
            let Some(record) = batch.records.first() else {
                return Ok(());
            };
            if record.change().old_tip != Some(record.change().new_tip)
                && record.context().origin != publication::Origin::Rollback
                && record
                    .context()
                    .block
                    .is_some_and(|b| b.kind == publication::Kind::Key)
            {
                let block = self
                    .owner
                    .importer
                    .database()
                    .view()
                    .map_err(Error::Repository)?
                    .require_block(record.change().new_tip)
                    .map_err(Error::Repository)?;
                let miner = self.owner.leader(&block)?;
                self.owner.promote(miner, &mut self.promotions)?;
            }
            self.owner.cursor = batch.cursor;
        }
        // Do not skip a promotion or silently rebuild a missing event history.
        if self
            .owner
            .importer
            .database()
            .events()
            .after(&self.owner.cursor, 1)
            .map_err(Error::Events)?
            .records
            .is_empty()
        {
            Ok(())
        } else {
            Err(Error::ResourceLimit)
        }
    }
    fn tip(&mut self) -> Result<Hash256, Error> {
        self.events()?;
        self.owner
            .importer
            .database()
            .view()
            .map_err(Error::Repository)?
            .status()
            .tip
            .ok_or(Error::Binding)
    }
    fn admit(&mut self, block: Block, origin: ledger::Origin) -> Result<Arc<Block>, Error> {
        self.check()?;
        let bytes = block.encode().map_err(|_| Error::Encoding)?;
        let retained = self
            .bytes
            .checked_add(bytes.len())
            .ok_or(Error::ResourceLimit)?;
        if retained > self.owner.config.result_bytes {
            return Err(Error::ResourceLimit);
        }
        let now = self.time()?;
        self.check()?;
        let mut ticket = match origin {
            ledger::Origin::Sync => self.owner.importer.import_synced_block(bytes, now),
            ledger::Origin::Gossip => self.owner.importer.import_block(bytes, now),
            _ => return Err(Error::Binding),
        }
        .map_err(Error::AdmissionStart)?;
        let result = loop {
            if let Some(result) = ticket.try_result() {
                break result;
            }
            pause();
        };
        self.bytes = retained;
        let block = Arc::new(block);
        let failed = result.is_err();
        let index = self.admissions.len();
        self.admissions.push(Admission {
            block: block.clone(),
            origin,
            result,
        });
        if failed {
            self.owner.failed = true;
            return Err(Error::Admission(index));
        }
        // Consume selected-key promotion even if stop arrived during commit.
        // A stop prevents the next operation, but never hides this receipt.
        if !self.owner.stop.0.load(Ordering::Acquire) {
            self.events()?;
        }
        Ok(block)
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Host for Runtime<'_, O> {
    type Block = Arc<Block>;
    type Error = Error;
    fn is_leader(&mut self) -> Result<bool, Error> {
        let top = self.top()?;
        let info = self.owner.keys.info();
        if !info.accepting {
            return Err(Error::Stopped);
        }
        Ok(self.owner.leader(&top)? == info.keys.signing)
    }
    fn key(&mut self) -> Result<Arc<Block>, Error> {
        let parent = self.tip()?;
        let info = self.owner.keys.info();
        if !info.accepting {
            return Err(Error::Stopped);
        }
        let done = self
            .owner
            .builder
            .start(
                preparation::Request {
                    parent,
                    miner: info.keys.candidate,
                    beneficiary: self.owner.config.beneficiary,
                    release_info: self.owner.config.release_info,
                },
                self.owner.clock.clone(),
            )
            .map_err(Error::Preparation)?
            .wait()
            .ok_or(Error::Panicked)?;
        let prepared = done.result.map_err(Error::Preparation)?;
        self.admit(Block::Key(prepared.header), ledger::Origin::Sync)
    }
    fn micro(&mut self) -> Result<Arc<Block>, Error> {
        let parent = self.tip()?;
        let mut prepared = ae_storage::pow::prepare_micro(
            &self.owner.snapshots,
            &self.owner.micro,
            pow::Input {
                parent,
                now: self.time()?.into(),
                limits: self.owner.config.micro,
            },
        )
        .map_err(Error::Micro)?;
        prepared.value.reader = prepared
            .value
            .reader
            .with_owned_execution(self.owner.execution.clone());
        self.owner.packing = Some(
            packing::Packing::start(
                prepared,
                self.owner.pool.clone(),
                packing::Config {
                    timeout: candidate::Timeout::Disabled,
                    window_ms: i64::try_from(self.owner.micro.micro_cycle_ms).unwrap_or(i64::MAX),
                },
            )
            .map_err(|e| Error::Packing(e.error))?,
        );
        loop {
            self.check()?;
            let pending = self.owner.packing.as_mut().unwrap();
            if pending.ready() {
                break;
            }
            pending.step().map_err(Error::Packing)?;
            pause();
        }
        let pending = self.owner.packing.take().unwrap();
        let prepared = match pending.finish() {
            Ok(prepared) => prepared,
            Err(e) => {
                self.owner.packing = Some(e.pending);
                return Err(Error::Packing(e.error));
            }
        };
        let mut block = prepared.value.candidate.block().clone();
        self.check()?;
        let mut ticket = self
            .owner
            .keys
            .sign_micro(block.header.clone())
            .map_err(Error::Keys)?;
        block.header = wait_key(&mut ticket).map_err(Error::Keys)?.header;
        self.admit(Block::Micro(block), ledger::Origin::Gossip)
    }
    fn pool_empty(&mut self) -> Result<bool, Error> {
        self.check()?;
        self.owner
            .pool
            .peek(1, None, None)
            .map_err(Error::Pool)?
            .wait()
            .map(|txs| txs.is_empty())
            .map_err(Error::Pool)
    }
    fn top(&mut self) -> Result<Arc<Block>, Error> {
        let hash = self.tip()?;
        self.owner
            .importer
            .database()
            .view()
            .map_err(Error::Repository)?
            .require_block(hash)
            .map(Arc::new)
            .map_err(Error::Repository)
    }
    fn transactions(&self, block: &Arc<Block>) -> Vec<Hash256> {
        match block.as_ref() {
            Block::Key(_) => vec![],
            Block::Micro(m) => m.transactions.iter().map(|tx| tx.hash()).collect(),
        }
    }
}
