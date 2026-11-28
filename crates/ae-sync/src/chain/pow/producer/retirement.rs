use super::*;

#[derive(Debug)]
pub enum Completion {
    Mining(Box<mining::Completion>),
    Promoted {
        expected: AccountPublicKey,
        result: Result<keys::PublicKeys, keys::Error>,
    },
    Discarded {
        parent: Hash256,
        result: Result<keys::SignedMicro, keys::Error>,
    },
    Admitted {
        parent: Hash256,
        hash: Hash256,
        result: Result<chain::Report, chain::Failure>,
    },
}
pub(super) struct Signing {
    pub block: Box<MicroBlock>,
    pub signer: AccountPublicKey,
    pub parent: Hash256,
    pub epoch: u64,
    pub ticket: keys::Ticket<keys::SignedMicro>,
}
pub(super) struct Signed {
    pub block: Box<MicroBlock>,
    pub signer: AccountPublicKey,
    pub parent: Hash256,
    pub epoch: u64,
}
impl Signed {
    pub fn discard(self) -> Completion {
        Completion::Discarded {
            parent: self.parent,
            result: Ok(keys::SignedMicro {
                header: self.block.header,
                signer: self.signer,
            }),
        }
    }
}
pub(super) enum Pending {
    Promote {
        expected: AccountPublicKey,
        ticket: keys::Ticket<keys::PublicKeys>,
    },
    Sign(Signing),
    Admit {
        parent: Hash256,
        hash: Hash256,
        ticket: chain::Ticket,
    },
}
impl Pending {
    pub fn poll_retired(&mut self) -> Option<Completion> {
        match self {
            Self::Promote { expected, ticket } => {
                ticket.try_result().map(|result| Completion::Promoted {
                    expected: *expected,
                    result,
                })
            }
            Self::Sign(signing) => {
                signing
                    .ticket
                    .try_result()
                    .map(|result| Completion::Discarded {
                        parent: signing.parent,
                        result,
                    })
            }
            Self::Admit {
                parent,
                hash,
                ticket,
            } => ticket.try_result().map(|result| Completion::Admitted {
                parent: *parent,
                hash: *hash,
                result,
            }),
        }
    }
}
pub struct Report {
    pub commands: Option<Box<on_demand::service::Report>>,
    pub status: Status,
    // Transfer all unacknowledged publications on exit, even when the caller
    // did not retain a Client. External Arc clones continue to hold credits.
    pub outputs: Vec<Arc<Output>>,
    pub completions: Vec<Completion>,
    pub generator_results: Vec<Result<(), generator::RequestError>>,
    pub generator_stop_rejected: Option<generator::RequestError>,
    pub(super) pending: Option<Pending>,
    pub(super) generator: Vec<generator::Ticket>,
    pub mining: Option<mining::Retirement>,
    pub(super) _lease: ProducerLease,
}
impl Report {
    // Poll only the original accepted requests. This method never signs,
    // promotes, inserts, cancels or redirects work to replacement services.
    pub fn poll(&mut self) -> bool {
        match self.poll_owned() {
            Ok(ready) => ready,
            Err(error) => {
                self.status.failure.get_or_insert(Error::Commands(error));
                false
            }
        }
    }
    pub(in crate::chain::pow) fn poll_owned(&mut self) -> Result<bool, on_demand::service::Error> {
        if let Some(commands) = &mut self.commands {
            commands.poll()?;
        }
        if let Some(mining) = &mut self.mining {
            let mut completed = Vec::new();
            mining.poll(&mut completed);
            self.completions.extend(
                completed
                    .into_iter()
                    .map(|c| Completion::Mining(Box::new(c))),
            );
        }
        if let Some(pending) = &mut self.pending
            && let Some(result) = pending.poll_retired()
        {
            self.completions.push(result);
            self.pending = None;
        }
        let mut index = 0;
        while index < self.generator.len() {
            if let Some(result) = self.generator[index].try_result() {
                self.generator_results.push(result);
                self.generator.remove(index);
            } else {
                index += 1;
            }
        }
        Ok(self.is_settled())
    }
    pub fn is_settled(&self) -> bool {
        self.commands.as_ref().is_none_or(|r| r.is_settled())
            && self.pending.is_none()
            && self.generator.is_empty()
            && self
                .mining
                .as_ref()
                .is_none_or(mining::Retirement::is_settled)
    }
}
