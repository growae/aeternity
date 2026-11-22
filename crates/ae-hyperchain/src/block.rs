// aec_consensus_hc pre-transform, schedule acquisition and pinning, fa2a6d7.
// Copyright (c) 2022, Aeternity. ISC notice in repository LICENSE.
use crate::{
    chain::{self, EpochInfo, Reader, RunEnv, View},
    consensus_contract,
    epochs::{self, Transition},
    leader,
};
use ae_state::Trees;
use ae_trie::{MemoryStore, NodeWriter};
use ae_types::{AccountPublicKey, Unsigned};
use ae_vm_fate::{
    maps::MapOrder,
    value::{ObjectPayload, Value},
};
use num_bigint::{BigInt, Sign};
use std::sync::Arc;
pub mod host;

#[derive(Debug, Clone)]
pub struct Config {
    pub parent_epoch_length: Unsigned,
    pub parent_start: Unsigned,
    pub parent_finality: Unsigned,
    pub pin_sync_margin: Unsigned,
    pub child_block_time: Unsigned,
    pub cache_limits: leader::Limits,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pin {
    pub epoch: BigInt,
    pub child_height: BigInt,
    pub child_hash: Vec<u8>,
    pub parent_height: BigInt,
}
pub trait Host: Clone {
    type Error;
    fn is_operational(error: &Self::Error) -> bool;
    fn entropy(&mut self, epoch: &BigInt) -> Result<Option<Vec<u8>>, Self::Error>;
    fn pin(&mut self, transaction_hash: &[u8]) -> Result<Option<Pin>, Self::Error>;
    fn key_hash(&mut self, height: &BigInt) -> Result<Option<Vec<u8>>, Self::Error>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Key,
    Micro,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Native {
    FunctionClause,
    InvalidLeader(ObjectPayload),
    InvalidReward(Value),
    PinCase(Value),
    SkippedEpoch { previous: BigInt, next: BigInt },
}
#[derive(Debug)]
pub enum Error<E, A = MemoryStore, C = MemoryStore> {
    Chain(chain::Error<A, C>),
    Contract(consensus_contract::Error<A, C>),
    Host(E),
    Leader(leader::Error),
    Native(Native),
    ParentNotSynced { wait_ms: Unsigned },
}
impl<E, A, C> Error<E, A, C> {
    pub fn operational<H: Host<Error = E>>(&self) -> bool {
        match self {
            Self::Chain(e) => e.is_operational(),
            Self::Contract(e) => !e.is_native(),
            Self::Host(e) => H::is_operational(e),
            Self::Leader(leader::Error::ResourceLimit) => true,
            _ => false,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    NoPin,
    IncorrectPin,
    PinAccepted {
        reward: BigInt,
        recipient: ObjectPayload,
        epoch: Value,
        height: BigInt,
    },
    NewEpoch(EpochInfo),
}
pub struct Outcome<A, C> {
    pub trees: Trees<A, C>,
    pub events: Vec<Event>,
    pub wait_ms: Option<Unsigned>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochStart {
    pub epoch: BigInt,
    pub height: BigInt,
    pub time: BigInt,
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct State {
    pub leaders: leader::Cache,
    pub epoch_starts: Vec<EpochStart>,
}
fn integer(v: &Value) -> Result<BigInt, Native> {
    match v {
        Value::Integer(n) => Ok(n.clone()),
        _ => Err(Native::FunctionClause),
    }
}
fn unsigned(n: &BigInt) -> Option<Unsigned> {
    if n.sign() == Sign::Minus {
        None
    } else {
        Unsigned::from_field_bytes(&n.to_bytes_be().1).ok()
    }
}
fn big(n: &Unsigned) -> BigInt {
    BigInt::from_bytes_be(Sign::Plus, n.as_bytes())
}
fn account(p: &ObjectPayload) -> Result<AccountPublicKey, Native> {
    match p {
        ObjectPayload::Bytes(b) if b.len() == 32 => {
            Ok(AccountPublicKey(b.as_slice().try_into().unwrap()))
        }
        _ => Err(Native::InvalidLeader(p.clone())),
    }
}
pub fn pin_interval(config: &Config, epoch: &BigInt) -> (BigInt, BigInt) {
    let first = big(&config.parent_start) + epoch * big(&config.parent_epoch_length);
    (
        first.clone() - big(&config.pin_sync_margin) + big(&config.parent_finality),
        first + big(&config.parent_epoch_length) - 1
            + big(&config.pin_sync_margin)
            + big(&config.parent_finality),
    )
}
impl State {
    pub fn cache_epoch_start(
        &mut self,
        epoch: BigInt,
        height: BigInt,
        time: BigInt,
    ) -> Result<(), Native> {
        if let Some(last) = self.epoch_starts.first() {
            if last.epoch >= epoch {
                return Ok(());
            }
            if epoch != &last.epoch + 1 {
                return Err(Native::SkippedEpoch {
                    previous: last.epoch.clone(),
                    next: epoch,
                });
            }
        }
        self.epoch_starts.insert(
            0,
            EpochStart {
                epoch,
                height,
                time,
            },
        );
        self.epoch_starts.truncate(5);
        Ok(())
    }
    fn cache_info<
        A: NodeWriter + Clone,
        C: NodeWriter + Clone,
        O: MapOrder,
        H: Host,
        S: chain::Source<A, C>,
    >(
        &mut self,
        reader: &Reader<A, C, O, S>,
        env: &RunEnv<A, C>,
        host: &mut H,
        info: EpochInfo,
        seed: Option<Vec<u8>>,
        limits: leader::Limits,
    ) -> Result<(), Error<H::Error, A, C>> {
        let epoch = integer(&info.epoch).map_err(Error::Native)?;
        let seed = match seed.or(info.seed) {
            Some(s) => s,
            None if epoch <= 2.into() => match host.entropy(&epoch).map_err(Error::Host)? {
                Some(s) => s,
                None => return Ok(()),
            },
            None => return Err(Error::Native(Native::FunctionClause)),
        };
        // Seed acquisition happens before the original first-slot cache check.
        if self.leaders.leader(&info.first).is_ok() {
            return Ok(());
        }
        let validators = info
            .validators
            .ok_or(Error::Native(Native::FunctionClause))?;
        let schedule = reader
            .validator_schedule(env, seed, validators, Value::Integer(info.length.clone()))
            .map_err(Error::Chain)?;
        self.leaders
            .insert(epoch, info.first, info.length, schedule, limits)
            .map_err(Error::Leader)
    }
    fn cache_epoch<
        A: NodeWriter + Clone,
        C: NodeWriter + Clone,
        O: MapOrder,
        H: Host,
        S: chain::Source<A, C>,
    >(
        &mut self,
        reader: &Reader<A, C, O, S>,
        env: &RunEnv<A, C>,
        host: &mut H,
        epoch: BigInt,
        seed: Option<Vec<u8>>,
        limits: leader::Limits,
    ) -> Result<(), Error<H::Error, A, C>> {
        // The original helper catches exceptions from epoch_info_for_epoch,
        // but Rust resource/backend exhaustion must never be silently swallowed.
        match reader.epoch_info_for_epoch(env, Value::Integer(epoch)) {
            Ok(info) => self.cache_info(reader, env, host, info, seed, limits),
            Err(e) if !e.is_operational() => Ok(()),
            Err(e) => Err(Error::Chain(e)),
        }
    }
    fn leader_inner<
        A: NodeWriter + Clone,
        C: NodeWriter + Clone,
        O: MapOrder,
        H: Host,
        S: chain::Source<A, C>,
    >(
        &mut self,
        reader: &Reader<A, C, O, S>,
        env: &RunEnv<A, C>,
        host: &mut H,
        height: &BigInt,
        limits: leader::Limits,
    ) -> Result<ObjectPayload, Error<H::Error, A, C>> {
        if let Ok(p) = self.leaders.leader(height) {
            return Ok(p.clone());
        }
        let info = reader.epoch_info(env).map_err(Error::Chain)?;
        let epoch = integer(&info.epoch).map_err(Error::Native)?;
        let last = info.last.clone();
        self.cache_info(reader, env, host, info, None, limits)?;
        self.cache_epoch(reader, env, host, epoch + 1, None, limits)?;
        if height > &last {
            return Err(Error::Leader(leader::Error::EpochDidNotEnd));
        }
        self.leaders.leader(height).cloned().map_err(Error::Leader)
    }
    pub fn leader_for_timeslot<
        A: NodeWriter + Clone,
        C: NodeWriter + Clone,
        O: MapOrder,
        H: Host,
        S: chain::Source<A, C>,
    >(
        &mut self,
        reader: &Reader<A, C, O, S>,
        env: &RunEnv<A, C>,
        host: &mut H,
        height: &BigInt,
        limits: leader::Limits,
    ) -> Result<ObjectPayload, Error<H::Error, A, C>> {
        let mut next = self.clone();
        let mut staged = host.clone();
        let result = next.leader_inner(reader, env, &mut staged, height, limits);
        if !result.as_ref().is_err_and(|e| e.operational::<H>()) {
            *self = next;
            *host = staged;
        }
        result
    }
    #[allow(clippy::too_many_arguments)]
    pub fn pre_transform<
        A: NodeWriter + Clone,
        C: NodeWriter + Clone,
        O: MapOrder,
        H: Host,
        S: chain::Source<A, C>,
    >(
        &mut self,
        reader: &Reader<A, C, O, S>,
        previous: &View<A, C>,
        trees: &Trees<A, C>,
        host: &mut H,
        kind: Kind,
        height: u64,
        config: &Config,
    ) -> Result<Outcome<A, C>, Error<H::Error, A, C>> {
        let mut next = self.clone();
        let mut staged = host.clone();
        let result =
            next.transform_inner(reader, previous, trees, &mut staged, kind, height, config);
        // Retain original cache/notification prefixes on modeled native exits;
        // operational failures leave local effects retryable. Live reads may
        // already have queued I/O, independently of this publication boundary.
        if !result.as_ref().is_err_and(|e| e.operational::<H>()) {
            *self = next;
            *host = staged;
        }
        result
    }
    #[allow(clippy::too_many_arguments)]
    fn transform_inner<
        A: NodeWriter + Clone,
        C: NodeWriter + Clone,
        O: MapOrder,
        H: Host,
        S: chain::Source<A, C>,
    >(
        &mut self,
        reader: &Reader<A, C, O, S>,
        previous: &View<A, C>,
        trees: &Trees<A, C>,
        host: &mut H,
        kind: Kind,
        height: u64,
        config: &Config,
    ) -> Result<Outcome<A, C>, Error<H::Error, A, C>> {
        let mut result = Outcome {
            trees: trees.clone(),
            events: vec![],
            wait_ms: None,
        };
        if height == 0 {
            return Ok(result);
        }
        let mut view = previous.clone();
        view.context.height = height;
        view.trees = Arc::new(trees.clone());
        let env = RunEnv::View(Arc::new(view.clone()));
        let info = reader.epoch_info(&env).map_err(Error::Chain)?;
        let h = BigInt::from(height);
        let wait = config
            .child_block_time
            .div_rem_u64(4)
            .expect("nonzero divisor")
            .0;
        if kind == Kind::Key && (h < info.first || h > info.last) {
            result.wait_ms = Some(wait);
            return Ok(result);
        }
        let leader = self.leader_inner(reader, &env, host, &h, config.cache_limits)?;
        let leader_account = account(&leader).map_err(Error::Native)?;
        if kind == Kind::Micro {
            result.trees = reader
                .with_execution(&view, |cfg| {
                    epochs::apply(
                        trees,
                        reader.profile.contracts,
                        &Transition::Micro {
                            leader: leader_account,
                        },
                        &view.context,
                        cfg,
                    )
                })
                .map_err(Error::Contract)?;
            return Ok(result);
        }
        let epoch = integer(&info.epoch).map_err(Error::Native)?;
        if h == info.first.clone().min(2.into()) {
            self.cache_epoch_start(epoch.clone(), h, view.environment.timestamp.clone())
                .map_err(Error::Native)?;
        }
        if BigInt::from(height) != info.last {
            result.trees = reader
                .with_execution(&view, |cfg| {
                    epochs::apply(
                        trees,
                        reader.profile.contracts,
                        &Transition::Key {
                            leader: leader_account,
                        },
                        &view.context,
                        cfg,
                    )
                })
                .map_err(Error::Contract)?;
            return Ok(result);
        }
        let pin = reader.pin_info(&env).map_err(Error::Chain)?;
        let correct = match pin {
            None => {
                result.events.push(Event::NoPin);
                false
            }
            Some(Value::Bytes(hash)) => {
                let valid = match host.pin(&hash) {
                    Ok(Some(pin)) => match host.key_hash(&pin.child_height) {
                        Ok(Some(hash)) if hash == pin.child_hash => {
                            let (min, max) = pin_interval(config, &pin.epoch);
                            pin.parent_height >= min && pin.parent_height <= max
                        }
                        Ok(_) => false,
                        Err(e) if !H::is_operational(&e) => false,
                        Err(e) => return Err(Error::Host(e)),
                    },
                    Ok(None) => false,
                    Err(e) if !H::is_operational(&e) => false,
                    Err(e) => return Err(Error::Host(e)),
                };
                if !valid {
                    result.events.push(Event::IncorrectPin);
                }
                valid
            }
            Some(v) => return Err(Error::Native(Native::PinCase(v))),
        };
        if correct {
            let reward = reader.pin_reward(&env).map_err(Error::Chain)?.current;
            let Value::Integer(amount) = &reward else {
                return Err(Error::Native(Native::InvalidReward(reward)));
            };
            let value = unsigned(amount)
                .ok_or_else(|| Error::Native(Native::InvalidReward(reward.clone())))?;
            result.trees = reader
                .with_execution(&view, |cfg| {
                    epochs::apply(
                        trees,
                        reader.profile.contracts,
                        &Transition::Reward {
                            height: info.last.clone(),
                            beneficiary: leader_account,
                            amount: value,
                        },
                        &view.context,
                        cfg,
                    )
                })
                .map_err(Error::Contract)?;
            result.events.push(Event::PinAccepted {
                reward: amount.clone(),
                recipient: leader,
                epoch: info.epoch.clone(),
                height: info.last,
            });
        }
        let seed = host
            .entropy(&(&epoch + 2))
            .map_err(Error::Host)?
            .ok_or(Error::ParentNotSynced { wait_ms: wait })?;
        view.trees = Arc::new(result.trees.clone());
        let env = RunEnv::View(Arc::new(view.clone()));
        self.cache_epoch(
            reader,
            &env,
            host,
            epoch + 2,
            Some(seed.clone()),
            config.cache_limits,
        )?;
        result.trees = reader
            .with_execution(&view, |cfg| {
                epochs::apply(
                    &result.trees,
                    reader.profile.contracts,
                    &Transition::End {
                        leader: leader_account,
                        seed,
                        next_pin_reward: (-1).into(),
                        carry_over: !correct,
                    },
                    &view.context,
                    cfg,
                )
            })
            .map_err(Error::Contract)?;
        view.trees = Arc::new(result.trees.clone());
        let next = reader
            .epoch_info(&RunEnv::View(Arc::new(view)))
            .map_err(Error::Chain)?;
        result.events.push(Event::NewEpoch(next));
        Ok(result)
    }
}
