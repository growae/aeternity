// aec_consensus_hc next_producer/get_slot_info, aeternity fa2a6d7.
// Copyright (c) 2022, Aeternity. ISC notice in repository LICENSE.
use crate::{
    block,
    chain::{self, EpochInfo, Reader, RunEnv, View},
    leader,
};
use ae_trie::{MemoryStore, NodeWriter};
use ae_vm_fate::{
    maps::MapOrder,
    value::{ObjectPayload, Value},
};
use num_bigint::BigInt;
use num_traits::Zero;
use std::sync::Arc;
pub mod host;

#[derive(Debug, Clone)]
pub struct Config {
    pub block_time: BigInt,
    pub production_time: BigInt,
    pub maximum_stall_reads: usize,
    pub cache_limits: leader::Limits,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slot {
    pub epoch: Value,
    pub slot: BigInt,
    pub t0: BigInt,
    pub delta: BigInt,
    pub now: BigInt,
    pub epoch_info: Option<EpochInfo>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Negotiation {
    pub info: EpochInfo,
    pub slot: BigInt,
    pub hash: Vec<u8>,
    pub leader: ObjectPayload,
}
pub trait Host: block::Host {
    fn key_time(&mut self, height: &BigInt) -> Result<BigInt, Self::Error>;
    fn key_present(&mut self, key: &ObjectPayload) -> Result<bool, Self::Error>;
    fn set_candidate(&mut self, key: &ObjectPayload) -> Result<bool, Self::Error>;
    fn negotiate(&mut self, input: Negotiation) -> Result<(), Self::Error>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wait {
    SlotFilled,
    NotProducer,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Wait {
        reason: Wait,
        after: BigInt,
    },
    RetryNotInCache {
        after: BigInt,
    },
    Produce {
        leader: ObjectPayload,
        holes: Option<BigInt>,
        after: BigInt,
    },
}
#[derive(Debug)]
pub enum Error<E, A = MemoryStore, C = MemoryStore> {
    Block(block::Error<E, A, C>),
    Chain(chain::Error<A, C>),
    Host(E),
    Badarith,
    ResourceLimit,
}
impl<E, A, C> Error<E, A, C> {
    fn operational<H: Host<Error = E>>(&self) -> bool {
        match self {
            Self::Block(e) => e.operational::<H>(),
            Self::Chain(e) => e.is_operational(),
            Self::Host(e) => H::is_operational(e),
            Self::ResourceLimit => true,
            Self::Badarith => false,
        }
    }
}
pub fn slot_info<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder, H: Host>(
    reader: &Reader<A, C, O>,
    view: &View<A, C>,
    top: &BigInt,
    now: &BigInt,
    block_time: &BigInt,
    host: &mut H,
) -> Result<Slot, Error<H::Error, A, C>> {
    if top.is_zero() {
        return Ok(Slot {
            epoch: Value::Integer(1.into()),
            slot: 1.into(),
            t0: now.clone(),
            delta: 0.into(),
            now: now.clone(),
            epoch_info: None,
        });
    }
    let info = reader
        .epoch_info(&RunEnv::View(Arc::new(view.clone())))
        .map_err(Error::Chain)?;
    let first = info.epoch == Value::Integer(1.into());
    let height = if first { 1.into() } else { &info.first - 1 };
    let t0 = host.key_time(&height).map_err(Error::Host)?;
    if block_time.is_zero() {
        return Err(Error::Badarith);
    }
    let delta = (now - &t0) / block_time
        + if first {
            BigInt::from(1)
        } else {
            BigInt::from(0)
        };
    Ok(Slot {
        epoch: info.epoch.clone(),
        slot: &info.first + &delta,
        t0,
        delta,
        now: now.clone(),
        epoch_info: Some(info),
    })
}
fn until_next<E, A, C>(s: &Slot, time: &BigInt) -> Result<BigInt, Error<E, A, C>> {
    if time.is_zero() {
        Err(Error::Badarith)
    } else {
        Ok(time - (&s.now - &s.t0) % time)
    }
}
#[allow(clippy::too_many_arguments)]
fn activate<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder, H: Host>(
    reader: &Reader<A, C, O>,
    view: &View<A, C>,
    top: &BigInt,
    slot: Slot,
    key: ObjectPayload,
    host: &mut H,
    config: &Config,
) -> Result<Decision, Error<H::Error, A, C>> {
    let info = match slot.epoch_info.clone() {
        Some(i) => i,
        None => reader
            .epoch_info(&RunEnv::View(Arc::new(view.clone())))
            .map_err(Error::Chain)?,
    };
    // Negotiation is sent before checking whether this node owns the key.
    if info.last == slot.slot {
        host.negotiate(Negotiation {
            info,
            slot: slot.slot.clone(),
            hash: view.environment.key_hash.to_vec(),
            leader: key.clone(),
        })
        .map_err(Error::Host)?;
    }
    let after = until_next(&slot, &config.block_time)?;
    if !host.set_candidate(&key).map_err(Error::Host)? {
        return Ok(Decision::Wait {
            reason: Wait::NotProducer,
            after,
        });
    }
    let next = top + 1;
    let holes = if next == slot.slot {
        None
    } else {
        Some(&slot.slot - next)
    };
    Ok(Decision::Produce {
        leader: key,
        holes,
        after: after - &config.production_time,
    })
}
#[allow(clippy::too_many_arguments)]
fn next_inner<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder, H: Host>(
    state: &mut block::State,
    reader: &Reader<A, C, O>,
    view: &View<A, C>,
    top: &BigInt,
    now: &BigInt,
    host: &mut H,
    config: &Config,
) -> Result<Decision, Error<H::Error, A, C>> {
    let mut slot = slot_info(reader, view, top, now, &config.block_time, host)?;
    if &slot.slot == top {
        return Ok(Decision::Wait {
            reason: Wait::SlotFilled,
            after: until_next(&slot, &config.block_time)?,
        });
    }
    let env = RunEnv::View(Arc::new(view.clone()));
    match state.leader_for_timeslot(reader, &env, host, &slot.slot, config.cache_limits) {
        Ok(key) => activate(reader, view, top, slot, key, host, config),
        Err(block::Error::Leader(leader::Error::EpochDidNotEnd)) => {
            let info = reader.epoch_info(&env).map_err(Error::Chain)?;
            let max = slot.slot.clone().min(info.last + info.length);
            let mut h = top + 1;
            let mut reads = 0;
            while h <= max {
                if reads >= config.maximum_stall_reads {
                    return Err(Error::ResourceLimit);
                }
                reads += 1;
                let Ok(key) = state.leaders.leader(&h) else {
                    break;
                };
                if host.key_present(key).map_err(Error::Host)? {
                    slot.slot = h;
                    return activate(reader, view, top, slot, key.clone(), host, config);
                }
                h += 1;
            }
            Ok(Decision::Wait {
                reason: Wait::NotProducer,
                after: config.block_time.clone(),
            })
        }
        Err(block::Error::Leader(leader::Error::NotInCache)) => Ok(Decision::RetryNotInCache {
            after: &config.block_time / 5,
        }),
        Err(e) => Err(Error::Block(e)),
    }
}
// The caller supplies a coherent top-key height and previous transaction view,
// plus the observed wall-clock time. A negative delay or hole count is retained
// as in Erlang; this layer does not clamp clock regressions or sleep.
#[allow(clippy::too_many_arguments)]
pub fn next<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder, H: Host>(
    state: &mut block::State,
    reader: &Reader<A, C, O>,
    view: &View<A, C>,
    top: &BigInt,
    now: &BigInt,
    host: &mut H,
    config: &Config,
) -> Result<Decision, Error<H::Error, A, C>> {
    let mut staged_state = state.clone();
    let mut staged_host = host.clone();
    let result = next_inner(
        &mut staged_state,
        reader,
        view,
        top,
        now,
        &mut staged_host,
        config,
    );
    if !result.as_ref().is_err_and(|e| e.operational::<H>()) {
        *state = staged_state;
        *host = staged_host;
    }
    result
}
