use super::super::super::delivery::{Error, Target};
use super::*;
pub use ae_chain::repository::hyperchain::producer::{Config, Input};
use ae_chain::repository::hyperchain::{inputs::Effect, producer};
use ae_hyperchain::parent_cache;

#[derive(Debug)]
pub struct Decision {
    pub outcome: Result<ae_hyperchain::producer::Decision, producer::Failure>,
    pub status: repo::Status,
    pub source_id: u64,
    pub cursor: events::Cursor,
    pub key_hash: ae_types::Hash256,
    pub key_height: u64,
}
enum Receipt {
    Parent(parent::Call),
    Eoe(runtime::Call),
    Negotiation(runtime::Call),
}
// One fixed attempt. Earlier acknowledged effects never repeat after a later
// failure. The node keeps this object on shutdown to inspect unknown work.
pub struct Pending {
    prepared: producer::Prepared,
    cursor: events::Cursor,
    parents: VecDeque<Effect>,
    receipt: Option<Receipt>,
    cache: parent::Client,
    client: Client,
    failure: Option<Error>,
    acknowledged: u64,
}
impl Pending {
    pub(super) fn new(
        mut produced: ae_storage::hyperchain::Produced,
        cache: parent::Client,
        client: Client,
    ) -> Self {
        let parents = produced.prepared.parents.drain().into();
        Self {
            prepared: produced.prepared,
            cursor: produced.cursor,
            parents,
            receipt: None,
            cache,
            client,
            failure: None,
            acknowledged: 0,
        }
    }
    pub fn remaining_parents(&self) -> &VecDeque<Effect> {
        &self.parents
    }
    pub fn negotiation(&self) -> Option<&eoe::Negotiation> {
        self.prepared.negotiation.as_ref()
    }
    pub fn decision(&self) -> &Result<ae_hyperchain::producer::Decision, producer::Failure> {
        &self.prepared.decision
    }
    pub fn cursor(&self) -> &events::Cursor {
        &self.cursor
    }
    pub fn in_flight(&self) -> bool {
        self.receipt.is_some()
    }
    pub fn failure(&self) -> Option<&Error> {
        self.failure.as_ref()
    }
    pub fn acknowledged(&self) -> u64 {
        self.acknowledged
    }
    pub fn ready(&self) -> bool {
        self.parents.is_empty()
            && self.prepared.negotiation.is_none()
            && self.receipt.is_none()
            && self.failure.is_none()
    }
    pub(super) fn retry(&mut self) -> Result<(), Error> {
        if let Some(error) = &self.failure
            && !retryable(error)
        {
            return Err(error.clone());
        }
        self.failure = None;
        Ok(())
    }
    pub(super) fn step(&mut self) -> Result<bool, Error> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        let result = self.advance();
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }
    fn advance(&mut self) -> Result<bool, Error> {
        if let Some(receipt) = &mut self.receipt {
            let result = match receipt {
                Receipt::Parent(call) => match call
                    .try_reply()
                    .map_err(|_| Error::ReceiptLost(Target::Cache))?
                {
                    None => return Ok(false),
                    Some(Ok(parent_cache::Reply::Block(_))) => Ok(()),
                    Some(Ok(_)) => Err(Error::UnexpectedReply(Target::Cache)),
                    Some(Err(e)) => Err(Error::Cache(e)),
                },
                Receipt::Eoe(call) | Receipt::Negotiation(call) => match call
                    .try_reply()
                    .map_err(|_| Error::ReceiptLost(Target::Eoe))?
                {
                    None if self.client.is_closed() => return Err(Error::ReceiptLost(Target::Eoe)),
                    None => return Ok(false),
                    Some(Ok(runtime::Applied::Unit)) => Ok(()),
                    Some(Ok(_)) => Err(Error::UnexpectedReply(Target::Eoe)),
                    Some(Err(e)) => Err(Error::Eoe(e)),
                },
            };
            let negotiation = matches!(self.receipt, Some(Receipt::Negotiation(_)));
            self.receipt = None;
            result?;
            if negotiation {
                self.prepared.negotiation = None;
            } else {
                self.parents
                    .pop_front()
                    .expect("acknowledged parent effect");
            }
            self.acknowledged = self.acknowledged.saturating_add(1);
            return Ok(true);
        }
        if self.client.is_closed() {
            return Err(Error::Closed(Target::Eoe));
        }
        if self.cache.is_closed() {
            return Err(Error::Closed(Target::Cache));
        }
        let command = match self.parents.front() {
            Some(Effect::RequestParent(height)) => {
                return match self.cache.call(parent_cache::Event::Get(height.clone())) {
                    Ok(call) => {
                        self.receipt = Some(Receipt::Parent(call));
                        Ok(true)
                    }
                    Err(parent::Error::Full) => Ok(false),
                    Err(e) => Err(Error::Cache(e)),
                };
            }
            Some(Effect::Parent { epoch, block }) => {
                if epoch.sign() == num_bigint::Sign::Minus {
                    return Err(Error::EpochRange);
                }
                let epoch = Unsigned::from_field_bytes(&epoch.to_bytes_be().1)
                    .map_err(|_| Error::EpochRange)?;
                runtime::Command::NotifyParent {
                    epoch,
                    block: block.clone(),
                }
            }
            None => match &self.prepared.negotiation {
                Some(input) => runtime::Command::Negotiate(input.clone()),
                None => return Ok(false),
            },
        };
        let negotiation = matches!(command, runtime::Command::Negotiate(_));
        match self.client.call(command) {
            Ok(call) => {
                self.receipt = Some(if negotiation {
                    Receipt::Negotiation(call)
                } else {
                    Receipt::Eoe(call)
                });
                Ok(true)
            }
            Err(runtime::SubmitError::Full(_)) => Ok(false),
            Err(runtime::SubmitError::Disconnected(_)) => Err(Error::Closed(Target::Eoe)),
            Err(runtime::SubmitError::ResourceLimit(_)) => Err(Error::ResourceLimit),
        }
    }
    pub(super) fn finish(self) -> (ae_hyperchain::signing::Keys, Decision) {
        assert!(self.ready());
        let p = self.prepared;
        (
            p.keys,
            Decision {
                outcome: p.decision,
                status: p.status,
                source_id: p.source_id,
                cursor: self.cursor,
                key_hash: p.key_hash,
                key_height: p.key_height,
            },
        )
    }
}
pub(super) fn retryable(error: &Error) -> bool {
    matches!(
        error,
        Error::ResourceLimit | Error::Eoe(_) | Error::Cache(_)
    ) && !matches!(
        error,
        Error::Eoe(coordinator::Error::Stopped) | Error::Cache(parent::Error::Closed)
    )
}
