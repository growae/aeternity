// Continuous PoS production and owned child shutdown. Scheduling follows
// aec_conductor (aeternity fa2a6d7); ISC copyright: repository LICENSE.
use super::{delivery, eoe, scoped};
use crate::{chain, votes};
use ae_chain::{publication, repository as repo};
use ae_crypto::Ed25519SecretKey;
use ae_hyperchain::{
    eoe::{coordinator, local_host, runtime as eoe_runtime},
    parent_cache::{self, runtime as cache},
    parent_connector::{fetch::Backend, runtime as connector},
};
use ae_storage::{Database, events, pool, snapshots};
use ae_types::AccountPublicKey;
use ae_vm_fate::maps::MapOrder;
use eoe::owner::{self, production};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Instant,
};

pub mod runtime;

pub struct ParentConfig {
    pub cache: parent_cache::Config,
    pub cache_runtime: cache::Config,
    pub connector: connector::Config,
    pub hosts: usize,
    pub seed: [u8; 16],
}
pub struct Config {
    pub eoe: owner::Config,
    pub production: production::Config,
    pub pool: pool::Config,
    pub pool_worker: pool::worker::Settings,
    pub admission: chain::Config,
    pub snapshots: snapshots::Config,
    pub gossip: pool::worker::GossipCache,
    pub parent: ParentConfig,
    pub events_per_step: usize,
    pub producing: bool,
    pub block_producer: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    NotBlockProducer,
    Time,
    Closed,
    Backpressured,
    Source(eoe::Error),
    Events(events::Error),
    Rollback(u64),
    Importer(chain::Error),
    Pool(pool::worker::Error),
    Votes(votes::Error),
    Parent(String),
    Owner(owner::Error),
    Delivery(delivery::Error),
    Production(production::Error),
    Supervision(eoe_runtime::Output),
}
#[derive(Debug)]
pub enum Output {
    Attempt(production::Completed),
    Stopped(Box<production::Stopped>),
    Rejected {
        error: production::Error,
        parent: ae_types::Hash256,
        micro: Option<chain::Accepted>,
    },
    // This output is retained until consumed. It is never treated as proof
    // that a failed service was recovered or its accepted work was cancelled.
    Supervision(eoe_runtime::Output),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    Idle,
    Advanced,
    Started,
    Retrying,
    Production(production::Progress),
    Output,
}
#[derive(Debug, Clone)]
pub struct Status {
    pub producing: bool,
    pub stopping: bool,
    pub stops: u64,
    pub block_producer: bool,
    pub attempts: u64,
    pub completed: u64,
    pub rejected: u64,
    pub retries: u64,
    pub selected_created_keys: u64,
    pub top_changes: u64,
    pub epoch_revision: u64,
    pub cursor: events::Cursor,
    pub deadline: Option<Instant>,
    pub failure: Option<Error>,
}
// A shutdown report preserves transient work instead of replaying it into
// replacement children. Receipt results are observed only after importer join.
pub struct Shutdown<O> {
    pub status: Status,
    pub output: Option<Output>,
    pub attempt: Option<production::Attempt<O>>,
    pub retiring: Option<production::Retiring<O>>,
    pub admission: Option<Result<chain::Report, chain::Failure>>,
    pub owner: owner::Shutdown,
    pub delivery: delivery::Delivery<O, eoe::StateReader, eoe::StateReader>,
    pub accepted: Result<repo::hyperchain::inputs::Effects, chain::Error>,
    pub queued_outputs: Vec<Output>,
    pub tops: Option<TopDelivery>,
    pub pool: Result<pool::worker::Info, pool::worker::Error>,
    pub parent: Result<cache::Exit, String>,
    pub connector_joined: bool,
}
pub struct TopDelivery {
    pub records: VecDeque<Arc<publication::Payload>>,
    pub after: events::Cursor,
    pub receipt: Option<cache::Call>,
}
// These children belong to one generation. Public clients can share admission
// and gossip, but cannot replace individual producer dependencies in this owner.
pub struct Conductor<O: MapOrder + Clone + Send + Sync + 'static> {
    owner: Option<owner::Owner>,
    importer: Arc<chain::Importer<O>>,
    tops: Option<TopDelivery>,
    producer: scoped::Producer<O>,
    delivery: Option<delivery::Delivery<O, eoe::StateReader, eoe::StateReader>>,
    pool: Option<pool::worker::Worker>,
    parent: Option<cache::Runtime>,
    connector: Option<connector::Runtime>,
    source: events::Reader,
    production: production::Config,
    event_limit: usize,
    attempt: Option<production::Attempt<O>>,
    retiring: Option<production::Retiring<O>>,
    restart: bool,
    pending: Option<Output>,
    status: Status,
    tick: Option<Instant>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Conductor<O> {
    pub fn start(
        database: Arc<Database>,
        execution: repo::HyperchainConfig<O>,
        keys: BTreeMap<AccountPublicKey, Ed25519SecretKey>,
        config: Config,
        backend: Arc<impl Backend>,
        targets: impl cache::Targets,
    ) -> Result<Self, Error> {
        if config.producing && !config.block_producer {
            return Err(Error::NotBlockProducer);
        }
        if config.events_per_step == 0
            || config.events_per_step > 65_536
            || config.pool.consensus != ae_protocol::ConsensusType::Pos
            || !(repo::hyperchain::producer::Binding {
                network: config.eoe.eoe.host.network.clone(),
                election: config.eoe.eoe.host.election_contract,
                parent_start: config.eoe.eoe.host.parent_start_height.clone(),
                parent_length: config.eoe.eoe.host.parent_epoch_length.clone(),
                block_time: config.eoe.eoe.block_time.clone(),
            })
            .matches(&execution)
        {
            return Err(Error::Configuration);
        }
        let events = database.events();
        let cursor = events.cursor();
        let child = {
            let view = database.view().map_err(|e| Error::Importer(e.into()))?;
            let hash = view.status().tip.ok_or(Error::Configuration)?;
            let block = view
                .require_block(hash)
                .map_err(|e| Error::Importer(e.into()))?;
            parent_cache::ChildTop {
                hash,
                height: block.header().common().height.into(),
            }
        };
        let selected = eoe::Source::new(
            database.clone(),
            execution.identity.clone(),
            execution.profile.network.clone(),
            config.snapshots,
        )
        .map_err(Error::Source)?;
        // Build both directions together. No replaceable connector link escapes.
        let mailbox = cache::Mailbox::new(
            parent_cache::Cache::new(config.parent.cache, child),
            config.parent.cache_runtime,
        )
        .map_err(|e| Error::Parent(e.to_string()))?;
        let connector = connector::Runtime::spawn(
            backend,
            config.parent.hosts,
            config.parent.seed,
            mailbox.sink(),
            config.parent.connector,
        )
        .map_err(|e| Error::Parent(e.to_string()))?;
        let parent = mailbox
            .spawn(
                targets,
                cache::ConnectorLink::new(connector.client()),
                execution.profile.order.clone(),
            )
            .map_err(|e| Error::Parent(e.to_string()))?;
        let pool = pool::worker::Worker::spawn_with_cache(
            database.clone(),
            execution.profile.network.clone(),
            config.pool,
            config.pool_worker,
            config.gossip.clone(),
        )
        .map_err(Error::Pool)?;
        let order = execution.profile.order.clone();
        let cached = scoped::Cached {
            cache: parent.client(),
            pins: config.production.pins.clone(),
            parent_start: config.eoe.eoe.host.parent_start_height.clone(),
            parent_length: config.eoe.eoe.host.parent_epoch_length.clone(),
            limits: config.production.inputs,
        };
        let (importer, accepted) = chain::Importer::new_hyperchain_scoped(
            database.clone(),
            execution,
            cached,
            config.admission,
        )
        .map_err(Error::Importer)?;
        let producer = accepted.producer();
        let publications =
            votes::Publications::new(&database, config.eoe.votes).map_err(Error::Votes)?;
        let owner = owner::Owner::start(
            owner::Inputs {
                source: selected,
                database,
                publications,
                gossip: config.gossip,
                parent: Some(parent.client()),
            },
            config.eoe,
            keys,
            order,
        )
        .map_err(Error::Owner)?;
        let delivery = owner.parent_delivery(accepted).map_err(Error::Owner)?;
        Ok(Self {
            owner: Some(owner),
            importer: Arc::new(importer),
            tops: None,
            producer,
            delivery: Some(delivery),
            pool: Some(pool),
            parent: Some(parent),
            connector: Some(connector),
            source: events,
            production: config.production,
            event_limit: config.events_per_step,
            attempt: None,
            retiring: None,
            restart: config.producing,
            pending: None,
            tick: None,
            status: Status {
                producing: config.producing,
                stopping: false,
                stops: 0,
                block_producer: config.block_producer,
                attempts: 0,
                completed: 0,
                rejected: 0,
                retries: 0,
                selected_created_keys: 0,
                top_changes: 0,
                epoch_revision: cursor.revision(),
                cursor,
                deadline: None,
                failure: None,
            },
        })
    }
    pub fn status(&self) -> Status {
        self.status.clone()
    }
    pub fn importer(&self) -> Arc<chain::Importer<O>> {
        self.importer.clone()
    }
    pub fn pool(&self) -> pool::worker::Client {
        self.pool.as_ref().expect("pool").client()
    }
    pub fn votes(&self) -> votes::Client {
        self.owner.as_ref().expect("owner").votes()
    }
    pub fn parent(&self) -> cache::Client {
        self.parent.as_ref().expect("parent").client()
    }
    pub fn connector(&self) -> connector::Client {
        self.connector.as_ref().expect("connector").client()
    }
    pub fn attempt(&self) -> Option<&production::Attempt<O>> {
        self.attempt.as_ref()
    }
    pub fn output(&self) -> Option<&Output> {
        self.pending.as_ref()
    }
    pub fn take_output(&mut self) -> Option<Output> {
        self.pending.take()
    }
    pub fn start_production(&mut self) -> Result<(), Error> {
        if !self.status.block_producer {
            return Err(Error::NotBlockProducer);
        }
        if let Some(e) = &self.status.failure {
            return Err(e.clone());
        }
        if !self.status.producing {
            self.status.producing = true;
            self.restart = true;
        }
        Ok(())
    }
    // Cancellation prevents new producer work immediately. Previously accepted
    // calls are retired by step before another attempt can acquire the lease.
    // Independent admission, pool, votes and parent services remain available.
    pub fn stop_production(&mut self) -> Result<(), Error> {
        self.status.producing = false;
        self.restart = false;
        self.status.deadline = None;
        if let Some(attempt) = self.attempt.take() {
            match self
                .owner
                .as_mut()
                .expect("owner")
                .retire_production(attempt)
            {
                Ok(retiring) => {
                    self.retiring = Some(retiring);
                    self.status.stopping = true;
                }
                Err(attempt) => {
                    self.attempt = Some(*attempt);
                    let error = Error::Production(production::Error::Binding);
                    self.status.failure = Some(error.clone());
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    pub fn step(&mut self, wall_ms: u64, now: Instant) -> Result<Progress, Error> {
        if let Some(e) = &self.status.failure {
            return Err(e.clone());
        }
        let result = self.advance(wall_ms, now);
        if let Err(e) = &result {
            self.status.failure = Some(e.clone());
        }
        result
    }
    fn advance(&mut self, wall_ms: u64, now: Instant) -> Result<Progress, Error> {
        if self.tick.is_some_and(|last| now < last) {
            return Err(Error::Time);
        }
        self.tick = Some(now);
        self.producer.check().map_err(Error::Importer)?;
        if !self.pool().info().accepting || self.parent().is_closed() {
            return Err(Error::Closed);
        }
        let health = self.parent().snapshot().health;
        if health != cache::Health::Running {
            return Err(Error::Parent(format!("cache: {health:?}")));
        }
        let health = self.connector().snapshot().health;
        if matches!(
            health,
            connector::Health::Stopped | connector::Health::WorkStopped(_)
        ) {
            return Err(Error::Parent(format!("connector: {health:?}")));
        }
        // Retained outputs apply backpressure to production, not ownership:
        // shutdown still joins every child and transfers this output intact.
        if self.pending.is_some() {
            return Ok(Progress::Output);
        }
        let mut advanced = self.consume_events()?;
        let owner = self.owner.as_mut().expect("owner");
        match owner.step().map_err(Error::Owner)? {
            owner::Progress::Output(eoe_runtime::Output::Service(
                coordinator::ServiceOutput::Publication(local_host::Output::ParentBlock { .. }),
            )) => advanced = true,
            owner::Progress::Output(o) => {
                let fatal = matches!(
                    o,
                    eoe_runtime::Output::WorkStopped(_)
                        | eoe_runtime::Output::Service(coordinator::ServiceOutput::Stopped)
                );
                self.pending = Some(Output::Supervision(o.clone()));
                if fatal {
                    return Err(Error::Supervision(o));
                }
                return Ok(Progress::Output);
            }
            owner::Progress::Idle => (),
            _ => advanced = true,
        }
        self.status.epoch_revision = owner.epoch_cursor().revision();
        match self
            .delivery
            .as_mut()
            .expect("delivery")
            .step()
            .map_err(Error::Delivery)?
        {
            delivery::Progress::Idle
            | delivery::Progress::Waiting
            | delivery::Progress::Backpressured
            | delivery::Progress::SourceBusy => (),
            _ => advanced = true,
        }
        if let Some(retiring) = &mut self.retiring {
            if retiring
                .step(owner, &self.importer)
                .map_err(Error::Production)?
            {
                let stopped = self
                    .retiring
                    .take()
                    .expect("retiring")
                    .finish()
                    .expect("settled");
                self.status.stopping = false;
                self.status.stops = self.status.stops.saturating_add(1);
                self.pending = Some(Output::Stopped(Box::new(stopped)));
                return Ok(Progress::Output);
            }
            return Ok(Progress::Advanced);
        }
        if let Some(attempt) = &mut self.attempt {
            let p = match attempt.step(owner, &self.importer, wall_ms, now) {
                Ok(p) => p,
                Err(e) if definitely_busy(&e) => {
                    attempt.retry().map_err(Error::Production)?;
                    self.status.retries = self.status.retries.saturating_add(1);
                    return Ok(Progress::Retrying);
                }
                Err(e) if native_rejection(&e) => {
                    let parent = attempt.parent();
                    let micro = attempt.accepted_micro().cloned();
                    self.attempt = None;
                    self.restart = true;
                    self.status.deadline = None;
                    self.status.rejected = self.status.rejected.saturating_add(1);
                    self.pending = Some(Output::Rejected {
                        error: e,
                        parent,
                        micro,
                    });
                    return Ok(Progress::Output);
                }
                Err(e) => return Err(Error::Production(e)),
            };
            self.status.deadline = attempt.deadline();
            if attempt.ready() {
                let completed = self
                    .attempt
                    .take()
                    .expect("attempt")
                    .finish()
                    .expect("ready");
                if matches!(
                    completed.outcome,
                    production::Outcome::Wait(_) | production::Outcome::RetryNotInCache
                ) {
                    self.restart = true;
                }
                self.status.completed = self.status.completed.saturating_add(1);
                self.status.deadline = None;
                self.pending = Some(Output::Attempt(completed));
                return Ok(Progress::Output);
            }
            return Ok(Progress::Production(p));
        }
        // Consume the committed backlog before starting a new attempt. Otherwise
        // an old locally-created event could incorrectly restart a later attempt.
        let caught_up = self.status.cursor.revision() == self.source.info().status.revision
            && self.status.epoch_revision >= self.status.cursor.revision();
        if self.status.producing && self.restart && caught_up {
            self.attempt = Some(
                owner
                    .begin_production(
                        self.producer.clone(),
                        self.pool.as_ref().expect("pool").client(),
                        self.production.clone(),
                    )
                    .map_err(Error::Production)?,
            );
            self.restart = false;
            self.status.attempts = self.status.attempts.saturating_add(1);
            return Ok(Progress::Started);
        }
        Ok(if advanced {
            Progress::Advanced
        } else {
            Progress::Idle
        })
    }
    fn consume_events(&mut self) -> Result<bool, Error> {
        if self.tops.is_none() {
            let batch = self
                .source
                .after(&self.status.cursor, self.event_limit)
                .map_err(Error::Events)?;
            if batch.closed {
                return Err(Error::Closed);
            }
            if batch.records.is_empty() {
                self.status.cursor = batch.cursor;
                return Ok(false);
            }
            self.tops = Some(TopDelivery {
                records: batch.records.into(),
                after: batch.cursor,
                receipt: None,
            });
        }
        let pending = self.tops.as_mut().expect("top batch");
        let record = pending.records.front().expect("top event");
        let context = record.context();
        if context.origin == publication::Origin::Rollback {
            return Err(Error::Rollback(record.change().revision));
        }
        let changed = record.change().old_tip != Some(record.change().new_tip);
        let key = context.block.filter(|b| b.kind == publication::Kind::Key);
        if changed && let Some(block) = key {
            if let Some(call) = &mut pending.receipt {
                match call.try_reply() {
                    Ok(None) => return Ok(false),
                    Ok(Some(Ok(parent_cache::Reply::None))) => pending.receipt = None,
                    other => return Err(Error::Parent(format!("child top receipt: {other:?}"))),
                }
            } else {
                match self.parent.as_ref().expect("parent").client().call(
                    parent_cache::Event::ChildTop(parent_cache::ChildTop {
                        hash: block.hash,
                        height: block.height.into(),
                    }),
                ) {
                    Ok(call) => {
                        pending.receipt = Some(call);
                        return Ok(true);
                    }
                    Err(cache::Error::Full) => return Ok(false),
                    Err(e) => return Err(Error::Parent(format!("child top submission: {e:?}"))),
                }
            }
        }
        if changed {
            self.status.top_changes = self.status.top_changes.saturating_add(1);
        }
        if restart_on_top(context, changed) {
            self.status.selected_created_keys = self.status.selected_created_keys.saturating_add(1);
            self.restart = true;
        }
        pending.records.pop_front();
        if pending.records.is_empty() {
            self.status.cursor = self.tops.take().expect("top batch").after;
        }
        Ok(true)
    }
    pub fn shutdown(mut self) -> Shutdown<O> {
        self.finish()
    }
    fn finish(&mut self) -> Shutdown<O> {
        self.status.producing = false;
        if let Some(attempt) = &mut self.attempt {
            attempt.cancel();
        }
        self.importer.stop();
        self.importer.jobs.join_blocking();
        let admission = self
            .attempt
            .as_mut()
            .and_then(production::Attempt::take_abandoned_admission)
            .or_else(|| {
                self.retiring
                    .as_mut()
                    .and_then(production::Retiring::take_abandoned_admission)
            })
            .map(|mut ticket| {
                ticket.try_result().unwrap_or(Err(chain::Failure {
                    progress: None,
                    error: chain::Error::Closed,
                }))
            });
        // The importer has joined: take any accepted effects still in its host.
        // Delivery itself retains its previously acquired batch and receipt.
        let mut delivery = self.delivery.take().expect("delivery");
        let accepted = delivery.take_source_after_join();
        let owner = self.owner.take().expect("owner").shutdown();
        let pool = self.pool.take().expect("pool").shutdown();
        let parent = self
            .parent
            .take()
            .expect("parent")
            .shutdown()
            .map_err(|_| "parent cache panicked".to_string());
        let connector_joined = self.connector.take().expect("connector").shutdown().is_ok();
        Shutdown {
            status: self.status.clone(),
            output: self.pending.take(),
            attempt: self.attempt.take(),
            retiring: self.retiring.take(),
            admission,
            owner,
            delivery,
            accepted,
            queued_outputs: vec![],
            tops: self.tops.take(),
            pool,
            parent,
            connector_joined,
        }
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Drop for Conductor<O> {
    fn drop(&mut self) {
        if self.owner.is_some() {
            let _ = self.finish();
        }
    }
}

fn definitely_busy(error: &production::Error) -> bool {
    fn busy(e: &chain::Error) -> bool {
        matches!(
            e,
            chain::Error::Busy | chain::Error::Storage(repo::Error::Conflict)
        )
    }
    match error {
        production::Error::Source(e) => busy(e),
        production::Error::Admission(f) => {
            f.progress.as_ref().is_some_and(|r| r.accepted.is_empty()) && busy(&f.error)
        }
        _ => false,
    }
}
fn native_rejection(error: &production::Error) -> bool {
    match error {
        production::Error::Decision(f) => !f.operational,
        production::Error::Key { operational, .. } => !operational,
        production::Error::Admission(f) => {
            f.progress.as_ref().is_some_and(|r| r.accepted.is_empty())
                && matches!(
                    f.error,
                    chain::Error::Validation {
                        operational: false,
                        ..
                    }
                )
        }
        _ => false,
    }
}

fn restart_on_top(context: publication::Context, changed: bool) -> bool {
    changed
        && context.origin == publication::Origin::Created
        && context
            .block
            .is_some_and(|b| b.kind == publication::Kind::Key)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_conductor_top_and_candidate_failure_restart_decisions_match() {
        let data: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/erlang_hc_conductor.json"
        ))
        .unwrap();
        for row in data["cases"].as_array().unwrap() {
            let context = publication::Context {
                origin: match row["origin"].as_str().unwrap() {
                    "created" => publication::Origin::Created,
                    "received" => publication::Origin::Received,
                    "synced" => publication::Origin::Synced,
                    _ => panic!(),
                },
                block: Some(publication::BlockInfo {
                    hash: ae_types::Hash256::ZERO,
                    previous: ae_types::Hash256::ZERO,
                    height: 2,
                    kind: if row["block_type"] == "key" {
                        publication::Kind::Key
                    } else {
                        publication::Kind::Micro
                    },
                }),
            };
            let restart = row["running"].as_bool().unwrap()
                && restart_on_top(context, row["changed"].as_bool().unwrap());
            assert_eq!(
                u64::from(restart),
                row["result"]["dispatches"].as_u64().unwrap()
            );
        }
        for row in data["replies"].as_array().unwrap() {
            let failure = production::Error::Decision(repo::hyperchain::producer::Failure {
                kind: repo::hyperchain::producer::Kind::Chain,
                operational: false,
                reason: row["reason"].as_str().unwrap().into(),
            });
            let retry = row["running"].as_bool().unwrap() && native_rejection(&failure);
            assert_eq!(
                u64::from(retry),
                row["result"]["dispatches"].as_u64().unwrap()
            );
        }
        assert_eq!(
            (
                data["cases"].as_array().unwrap().len(),
                data["replies"].as_array().unwrap().len()
            ),
            (24, 8)
        );
    }
}
