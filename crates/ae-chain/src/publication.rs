// aec_conductor event ordering and origin rules, aeternity fa2a6d7.
// Copyright (c) Aeternity developers. ISC: LICENSE.
use crate::{
    block::{Block, Light},
    repository::{Change, Update},
};
use ae_execution::{
    contracts,
    fate_chain::TransferEvent,
    transactions::{self, batch},
};
use ae_types::Hash256;
use std::{mem::size_of, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Bootstrap,
    Rollback,
    Synced,
    Received,
    Created,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Key,
    Micro,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockInfo {
    pub hash: Hash256,
    pub kind: Kind,
    pub previous: Hash256,
    pub height: u64,
}
impl BlockInfo {
    pub(crate) fn of(block: &Block, hash: Hash256) -> Self {
        let header = block.header();
        Self {
            hash,
            kind: match block {
                Block::Key(_) => Kind::Key,
                Block::Micro(_) => Kind::Micro,
            },
            previous: Hash256(header.common().previous_hash),
            height: header.common().height,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Context {
    pub origin: Origin,
    pub block: Option<BlockInfo>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Propagation {
    Created,
    Received,
}
#[derive(Debug, PartialEq, Eq)]
pub enum Effect<'a> {
    Transaction {
        block: BlockInfo,
        origin: Origin,
        event: &'a batch::Event,
    },
    BlockCreated(BlockInfo),
    BlockToPublish {
        block: BlockInfo,
        origin: Propagation,
    },
    TopChanged(BlockInfo),
    // Administrative rollback has no native conductor insertion origin.
    Reverted(&'a Change),
}
#[derive(Debug)]
pub struct Payload {
    change: Change,
    context: Context,
    events: Vec<batch::Event>,
    body: Option<Arc<[u8]>>,
    bytes: usize,
}
impl Payload {
    pub fn change(&self) -> &Change {
        &self.change
    }
    pub fn context(&self) -> Context {
        self.context
    }
    // Canonical key bytes or the native light-micro wire representation.
    pub fn body(&self) -> Option<&Arc<[u8]>> {
        self.body.as_ref()
    }
    pub fn retained_bytes(&self) -> usize {
        self.bytes
    }
    pub fn effects(&self) -> impl Iterator<Item = Effect<'_>> {
        let context = self.context;
        let block = context.block;
        let changed = self.change.old_tip != Some(self.change.new_tip);
        let created = context.origin == Origin::Created;
        let received = context.origin == Origin::Received;
        let insertion = matches!(
            context.origin,
            Origin::Synced | Origin::Received | Origin::Created
        );
        // Update events are already chronological; the transaction batch
        // adapter performed the native reverse of aetx_env's LIFO list.
        let tx = self.events.iter().filter_map(move |event| {
            block.map(|block| Effect::Transaction {
                block,
                origin: context.origin,
                event,
            })
        });
        let created_event = block.filter(|_| created).map(Effect::BlockCreated);
        let created_publish = block
            .filter(|_| created)
            .map(|block| Effect::BlockToPublish {
                block,
                origin: Propagation::Created,
            });
        let top = block
            .filter(|_| insertion && changed)
            .map(Effect::TopChanged);
        let received_publish =
            block
                .filter(|_| received && changed)
                .map(|block| Effect::BlockToPublish {
                    block,
                    origin: Propagation::Received,
                });
        let reverted =
            (context.origin == Origin::Rollback).then_some(Effect::Reverted(&self.change));
        tx.chain(created_event)
            .chain(created_publish)
            .chain(top)
            .chain(received_publish)
            .chain(reverted)
    }
    // An oversized transient notification must never reject a valid commit.
    // Account before cloning; the source records an explicit gap on None.
    pub fn capture(update: &Update, limit: usize) -> Option<Self> {
        let context = update.publication();
        let change = update.change();
        let mut bytes = size_of::<Self>().checked_add(128)?;
        for n in [
            change.removed.len(),
            change.discarded.len(),
            change.detached.len(),
            change.attached.len(),
            change.requeue.len(),
        ] {
            bytes = bytes.checked_add(n.checked_mul(32)?)?;
        }
        bytes = bytes.checked_add(change.confirm.len().checked_mul(64)?)?;
        let emit = matches!(context.origin, Origin::Received | Origin::Created);
        let events = if emit { update.events() } else { &[] };
        for event in events {
            bytes = bytes.checked_add(event_bytes(event)?)?;
            if bytes > limit {
                return None;
            }
        }
        let publish = context.origin == Origin::Created
            || (context.origin == Origin::Received && change.old_tip != Some(change.new_tip));
        let body = if publish {
            Some(update.publication_body()?)
        } else {
            None
        };
        bytes = bytes.checked_add(body.map_or(0, |b| b.len()))?;
        if bytes > limit {
            return None;
        }
        Some(Self {
            change: change.clone(),
            context,
            events: events.to_vec(),
            body: body.cloned(),
            bytes,
        })
    }
}
pub(crate) fn wire(block: &Block, origin: Origin, change: &Change) -> Option<Arc<[u8]>> {
    if origin != Origin::Created
        && !(origin == Origin::Received && change.old_tip != Some(change.new_tip))
    {
        return None;
    }
    // Capture from the immutable admitted block. Rust storage contains a
    // separate envelope and micro transaction references, not network bytes.
    match block {
        Block::Key(_) => block.encode(),
        Block::Micro(m) => Light::from_micro(m).encode(),
    }
    .ok()
    .map(Arc::from)
}
fn primitive_bytes(event: &ae_execution::Event) -> usize {
    use ae_execution::Event::*;
    match event {
        ContractDelta { amount, .. }
        | OracleQueryFee { amount, .. }
        | OracleRefund { amount, .. }
        | NameDebit { amount, .. }
        | NameRefund { amount, .. }
        | NameFeeLocked { amount }
        | Spend { amount, .. }
        | FeeDebit { amount, .. } => amount.as_bytes().len(),
    }
}
fn event_bytes(event: &batch::Event) -> Option<usize> {
    let heap = match &event.value {
        transactions::Event::Primitive(e) => primitive_bytes(e),
        transactions::Event::Channel { .. } => 0,
        transactions::Event::Contract(e) => match e {
            contracts::Event::Delta { amount, .. } => amount.as_bytes().len(),
            contracts::Event::Aevm(e) => primitive_bytes(&e.event),
            contracts::Event::Internal(e) => match e {
                TransferEvent::DeploymentCall { .. } => 0,
                TransferEvent::NameCall { transaction, .. } => {
                    transaction.as_ref().map_or(0, |t| t.wire_size())
                }
                TransferEvent::InternalCall(t) => t.wire_size(),
                TransferEvent::OracleCall { transaction, .. } => transaction.wire_size(),
                TransferEvent::NameDelta { amount, .. }
                | TransferEvent::Delta { amount, .. }
                | TransferEvent::OracleDelta { amount, .. } => amount.as_bytes().len(),
            },
        },
    };
    // Includes the enum/Vec allocation overhead and conservative AST overhead
    // for internal transactions. This is an accounting bound, not allocator RSS.
    size_of::<batch::Event>()
        .checked_add(512)?
        .checked_add(heap.checked_mul(4)?)
}

#[cfg(test)]
#[path = "publication/tests.rs"]
mod tests;
