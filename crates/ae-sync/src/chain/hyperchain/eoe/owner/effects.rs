use super::{Client, parent, runtime};
use crate::chain::hyperchain::delivery::{Error, Progress, Target};
use ae_chain::repository::hyperchain::inputs::{Effect, Effects};
use ae_hyperchain::parent_cache;
use ae_types::Unsigned;
use std::collections::VecDeque;

enum Receipt {
    Cache(parent::Call),
    Eoe(runtime::Call),
}
// Effects were bounded during candidate execution. Ownership moves here once;
// receipts are retained across polling and unknown results cannot be retried.
pub(super) struct Pending {
    pub pending: VecDeque<Effect>,
    pub acknowledged: u64,
    cache: parent::Client,
    client: Client,
    receipt: Option<Receipt>,
}
impl Pending {
    pub fn new(effects: &mut Effects, cache: parent::Client, client: Client) -> Self {
        Self {
            pending: effects.drain().into(),
            acknowledged: 0,
            cache,
            client,
            receipt: None,
        }
    }
    pub fn in_flight(&self) -> bool {
        self.receipt.is_some()
    }
    pub fn ready(&self) -> bool {
        self.pending.is_empty() && self.receipt.is_none()
    }
    pub fn step(&mut self) -> Result<Progress, Error> {
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
                    None if self.client.is_closed() => return Err(Error::ReceiptLost(Target::Eoe)),
                    None => return Ok(Progress::Waiting),
                    Some(Ok(runtime::Applied::Unit)) => Ok(()),
                    Some(Ok(_)) => return Err(Error::UnexpectedReply(Target::Eoe)),
                    Some(Err(e)) => Err(Error::Eoe(e)),
                },
            };
            self.receipt = None;
            result?;
            self.pending
                .pop_front()
                .expect("acknowledged candidate effect");
            self.acknowledged = self.acknowledged.saturating_add(1);
            return Ok(Progress::Acknowledged);
        }
        let Some(effect) = self.pending.front() else {
            return Ok(Progress::Idle);
        };
        if self.cache.is_closed() {
            return Err(Error::Closed(Target::Cache));
        }
        if self.client.is_closed() {
            return Err(Error::Closed(Target::Eoe));
        }
        self.receipt = Some(match effect {
            Effect::RequestParent(height) => {
                match self.cache.call(parent_cache::Event::Get(height.clone())) {
                    Ok(call) => Receipt::Cache(call),
                    Err(parent::Error::Full) => return Ok(Progress::Backpressured),
                    Err(parent::Error::Closed) => return Err(Error::Closed(Target::Cache)),
                    Err(e) => return Err(Error::Cache(e)),
                }
            }
            Effect::Parent { epoch, block } => {
                if epoch.sign() == num_bigint::Sign::Minus {
                    return Err(Error::EpochRange);
                }
                let epoch = Unsigned::from_field_bytes(&epoch.to_bytes_be().1)
                    .map_err(|_| Error::EpochRange)?;
                match self.client.call(runtime::Command::NotifyParent {
                    epoch,
                    block: block.clone(),
                }) {
                    Ok(call) => Receipt::Eoe(call),
                    Err(runtime::SubmitError::Full(_)) => return Ok(Progress::Backpressured),
                    Err(runtime::SubmitError::Disconnected(_)) => {
                        return Err(Error::Closed(Target::Eoe));
                    }
                    Err(runtime::SubmitError::ResourceLimit(_)) => {
                        return Err(Error::ResourceLimit);
                    }
                }
            }
        });
        Ok(Progress::Submitted)
    }
}
