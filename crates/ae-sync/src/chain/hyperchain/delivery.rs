// Single-consumer delivery of accepted native entropy effects. This outbox
// is transient: a receipt acknowledges cache/EOE ownership, not persistence.
// The node event loop also drains EOE publications; this pump never reads that
// stream and cannot create or replace an independent vote authority.
use super::scoped::Accepted;
use crate::chain;
use ae_chain::repository::hyperchain::inputs::{Effect, Effects};
use ae_hyperchain::{
    eoe::{coordinator, runtime as eoe},
    parent_cache::{self, runtime as cache},
};
use ae_types::Unsigned;
use ae_vm_fate::maps::MapOrder;
use num_bigint::Sign;
use std::collections::VecDeque;
mod service;
pub use service::{Service, SpawnError, Status};

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub maximum_effects: usize,
    pub maximum_bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            maximum_effects: 4096,
            maximum_bytes: 16 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    Cache,
    Eoe,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    WorkerPanicked,
    ResourceLimit,
    EpochRange,
    Source(chain::Error),
    Closed(Target),
    Cache(cache::Error),
    Eoe(coordinator::Error),
    Preset(crate::preset_keys::Error),
    // Submitted work may have executed. It must not be silently resubmitted.
    ReceiptLost(Target),
    UnexpectedReply(Target),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    Idle,
    SourceBusy,
    Backpressured,
    Submitted,
    Waiting,
    Acknowledged,
}
enum Receipt {
    Cache(cache::Call),
    Eoe(eoe::Call),
}
#[derive(Debug)]
pub struct RetiredCacheCall {
    pub effect: Effect,
    // Closed means no result was observed from the joined old generation.
    pub result: Result<parent_cache::Reply, cache::Error>,
}
#[derive(Debug)]
pub struct RetiredEoeCall {
    pub effect: Effect,
    pub result: Result<Result<eoe::Applied, coordinator::Error>, eoe::CallError>,
}
pub struct Delivery<O, A, C> {
    source: Accepted<O, Effects>,
    cache: cache::Client,
    route: Option<cache::Route>,
    eoe: eoe::Client<A, C>,
    eoe_route: Option<eoe::Route<A, C>>,
    config: Config,
    pending: VecDeque<Effect>,
    retained_bytes: usize,
    receipt: Option<Receipt>,
    halted: Option<Error>,
    acknowledged: u64,
}
impl<O: MapOrder + Clone, A, C> Delivery<O, A, C> {
    pub fn new(source: Accepted<O, Effects>, cache: cache::Client, eoe: eoe::Client<A, C>) -> Self {
        Self {
            source,
            cache,
            route: None,
            eoe,
            eoe_route: None,
            config: Config::default(),
            pending: VecDeque::new(),
            retained_bytes: 0,
            receipt: None,
            halted: None,
            acknowledged: 0,
        }
    }
    pub(crate) fn set_route(&mut self, route: cache::Route) {
        self.route = Some(route);
    }
    pub(crate) fn set_eoe_route(&mut self, route: eoe::Route<A, C>) {
        self.eoe_route = Some(route);
    }
    // Only the unsent suffix may follow a replacement. An accepted call stays
    // bound to its joined generation, including unknown execution outcomes.
    pub(crate) fn retire_eoe(
        &mut self,
        old: &eoe::Client<A, C>,
    ) -> Result<Option<RetiredEoeCall>, Error> {
        if !old.is_closed() || self.eoe_route.is_none() {
            return Err(Error::Configuration);
        }
        if !self.eoe.same_runtime(old) {
            return Ok(None);
        }
        let retired = if let Some(Receipt::Eoe(call)) = &mut self.receipt {
            let result = match call.try_reply() {
                Ok(None) => return Err(Error::Configuration),
                Ok(Some(result)) => Ok(result),
                Err(error) => Err(error),
            };
            self.receipt = None;
            let effect = self.pending.pop_front().expect("accepted EOE effect");
            if matches!(result, Ok(Ok(eoe::Applied::Unit))) {
                self.acknowledged = self.acknowledged.saturating_add(1);
            }
            if self.pending.is_empty() {
                self.retained_bytes = 0;
            }
            Some(RetiredEoeCall { effect, result })
        } else {
            None
        };
        if self.halted.as_ref().is_some_and(eoe_departure) {
            self.halted = None;
        }
        Ok(retired)
    }
    // The supervisor calls this only after joining this exact cache. Preserve
    // its observed result; a lost read is reported and never submitted again.
    pub(crate) fn retire_cache(
        &mut self,
        old: &cache::Client,
    ) -> Result<Option<RetiredCacheCall>, Error> {
        if !old.is_closed() || self.route.is_none() {
            return Err(Error::Configuration);
        }
        if !self.cache.same_runtime(old) {
            return Ok(None);
        }
        let retired = if let Some(Receipt::Cache(call)) = &mut self.receipt {
            let result = match call.try_reply() {
                Ok(None) => return Err(Error::Configuration),
                Ok(Some(result)) => result,
                Err(error) => Err(error),
            };
            self.receipt = None;
            let effect = self.pending.pop_front().expect("accepted cache effect");
            if matches!(result, Ok(parent_cache::Reply::Block(_))) {
                self.acknowledged = self.acknowledged.saturating_add(1);
            }
            if self.pending.is_empty() {
                self.retained_bytes = 0;
            }
            Some(RetiredCacheCall { effect, result })
        } else {
            None
        };
        if matches!(
            self.halted,
            Some(
                Error::Closed(Target::Cache)
                    | Error::ReceiptLost(Target::Cache)
                    | Error::Cache(cache::Error::Closed)
            )
        ) {
            self.halted = None;
        }
        Ok(retired)
    }
    pub(crate) fn take_source_after_join(&mut self) -> Result<Effects, chain::Error> {
        self.source.take()
    }
    pub fn pending(&self) -> &VecDeque<Effect> {
        &self.pending
    }
    // Conservatively charge the complete acquired batch until its last receipt.
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    pub fn in_flight(&self) -> bool {
        self.receipt.is_some()
    }
    pub fn acknowledged(&self) -> u64 {
        self.acknowledged
    }
    pub fn failure(&self) -> Option<&Error> {
        self.halted.as_ref()
    }
    // Only a definitely rejected command may be retried. Fixed clients never
    // redirect this outbox into replacement services. Unknown outcomes stay
    // quarantined for lifecycle reconciliation by the owner.
    pub fn retry(&mut self, config: Config) -> Result<(), Error> {
        if config.maximum_effects == 0 || config.maximum_bytes == 0 {
            return Err(Error::Configuration);
        }
        if let Some(e @ (Error::ReceiptLost(_) | Error::UnexpectedReply(_))) = &self.halted {
            return Err(e.clone());
        }
        if self.pending.len() > config.maximum_effects || self.retained_bytes > config.maximum_bytes
        {
            return Err(Error::ResourceLimit);
        }
        self.config = config;
        self.halted = None;
        Ok(())
    }
    // At most one downstream submission or acknowledgement per call; no waiting on
    // owner execution, network I/O, EOE computation or the publication stream.
    pub fn step(&mut self) -> Result<Progress, Error> {
        if let Some(error) = &self.halted {
            return Err(error.clone());
        }
        let result = self.advance();
        if let Err(error) = &result {
            self.halted = Some(error.clone());
        }
        result
    }
    fn advance(&mut self) -> Result<Progress, Error> {
        if let Some(receipt) = &mut self.receipt {
            let result = match receipt {
                Receipt::Cache(call) => match call
                    .try_reply()
                    .map_err(|_| Error::ReceiptLost(Target::Cache))?
                {
                    None => return Ok(Progress::Waiting),
                    Some(Ok(parent_cache::Reply::Block(_))) => Ok(()),
                    Some(Ok(_)) => return Err(Error::UnexpectedReply(Target::Cache)),
                    Some(Err(e)) => Err(Error::Cache(e)),
                },
                Receipt::Eoe(call) => match call
                    .try_reply()
                    .map_err(|_| Error::ReceiptLost(Target::Eoe))?
                {
                    None => return Ok(Progress::Waiting),
                    Some(Ok(eoe::Applied::Unit)) => Ok(()),
                    Some(Ok(_)) => return Err(Error::UnexpectedReply(Target::Eoe)),
                    Some(Err(e)) => Err(Error::Eoe(e)),
                },
            };
            self.receipt = None;
            result?;
            self.pending
                .pop_front()
                .expect("receipt owns a pending effect");
            self.acknowledged = self.acknowledged.saturating_add(1);
            if self.pending.is_empty() {
                self.retained_bytes = 0;
            }
            return Ok(Progress::Acknowledged);
        }
        if let Some(route) = &self.route {
            self.cache = route.current().ok_or(Error::Closed(Target::Cache))?;
        }
        if let Some(route) = &self.eoe_route {
            self.eoe = route.current().ok_or(Error::Closed(Target::Eoe))?;
        }
        if self.cache.is_closed() {
            return Err(Error::Closed(Target::Cache));
        }
        if self.eoe.is_closed() {
            return Err(Error::Closed(Target::Eoe));
        }
        if self.pending.is_empty() {
            let mut oversized = false;
            let batch = self
                .source
                .try_take_if(|effects| {
                    oversized = effects.entries().len() > self.config.maximum_effects
                        || effects.bytes() > self.config.maximum_bytes;
                    !oversized
                })
                .map_err(Error::Source)?;
            if oversized {
                return Err(Error::ResourceLimit);
            }
            let Some(mut batch) = batch else {
                return Ok(Progress::SourceBusy);
            };
            self.retained_bytes = batch.bytes();
            self.pending = batch.drain().into();
        }
        let Some(effect) = self.pending.front() else {
            return Ok(Progress::Idle);
        };
        self.receipt = Some(match effect {
            Effect::RequestParent(height) => {
                match self.cache.call(parent_cache::Event::Get(height.clone())) {
                    Ok(call) => Receipt::Cache(call),
                    Err(cache::Error::Full) => return Ok(Progress::Backpressured),
                    Err(cache::Error::Closed) => return Err(Error::Closed(Target::Cache)),
                    Err(error) => return Err(Error::Cache(error)),
                }
            }
            Effect::Parent { epoch, block } => {
                if epoch.sign() == Sign::Minus {
                    return Err(Error::EpochRange);
                }
                let epoch = Unsigned::from_field_bytes(&epoch.to_bytes_be().1)
                    .map_err(|_| Error::EpochRange)?;
                match self.eoe.call(eoe::Command::NotifyParent {
                    epoch,
                    block: block.clone(),
                }) {
                    Ok(call) => Receipt::Eoe(call),
                    Err(eoe::SubmitError::Full(_)) => return Ok(Progress::Backpressured),
                    Err(eoe::SubmitError::Disconnected(_)) => {
                        return Err(Error::Closed(Target::Eoe));
                    }
                    Err(eoe::SubmitError::ResourceLimit(_)) => return Err(Error::ResourceLimit),
                }
            }
        });
        Ok(Progress::Submitted)
    }
}
pub(crate) fn eoe_departure(error: &Error) -> bool {
    matches!(
        error,
        Error::Closed(Target::Eoe) | Error::ReceiptLost(Target::Eoe)
    )
}
