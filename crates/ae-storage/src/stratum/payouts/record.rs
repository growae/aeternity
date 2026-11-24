use super::*;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Context {
    pub chain: repo::Status,
    pub height: u64,
    pub protocol: Protocol,
    pub balance: Option<Unsigned>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub chain: repo::Status,
    pub prepared: payout::Prepared,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Accepted {
        chain: repo::Status,
        revision: u64,
        inserted: bool,
    },
    Rejected(String),
    Observed {
        chain: repo::Status,
        revision: u64,
        location: Location,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dispatch {
    Ready(u64),
    Sending(u64),
    Done { attempt: u64, outcome: Outcome },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signed {
    pub signature: Signature,
    pub date: core::Date,
    pub transaction: Vec<u8>,
    pub dispatch: Dispatch,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub sequence: u64,
    // Complete original row, including legacy optional fields. It remains
    // unchanged here; expected_payment derives the atomic sent-row update.
    pub payment: core::Payment,
    pub request: Option<Box<Request>>,
    pub signed: Option<Signed>,
}
impl Record {
    pub(super) fn observed_revision(&self) -> u64 {
        let request = self.request.as_ref().map_or(0, |q| q.chain.revision);
        let outcome = match self.signed.as_ref().map(|s| &s.dispatch) {
            Some(Dispatch::Done {
                outcome: Outcome::Accepted { chain, .. } | Outcome::Observed { chain, .. },
                ..
            }) => chain.revision,
            _ => 0,
        };
        request.max(outcome)
    }
    pub fn hash(&self) -> Result<Option<Hash256>, Failure> {
        match (&self.request, &self.signed) {
            (Some(q), Some(s)) => Ok(Some(q.prepared.attach_signature(s.signature)?.hash())),
            (Some(_), None) => Ok(None),
            (None, None) => Ok(self.payment.tx_hash),
            _ => Err(Error::Corrupt("Stratum payout signature without request").into()),
        }
    }
    pub fn expected_payment(&self) -> Result<core::Payment, Failure> {
        let mut p = self.payment.clone();
        if let Some(s) = &self.signed {
            let q = self
                .request
                .as_ref()
                .ok_or(Error::Corrupt("Stratum payout request missing"))?;
            p.tx_hash = self.hash()?;
            p.absolute = Some(q.prepared.absolute().clone());
            p.date = Some(s.date);
        }
        Ok(p)
    }
    pub fn fingerprint(
        &self,
        limits: Limits,
        accounting: core::Limits,
    ) -> Result<Hash256, Failure> {
        Ok(ae_crypto::blake2b_256(&self.encode(limits, accounting)?))
    }
    pub(super) fn validate(&self, limits: Limits, accounting: core::Limits) -> Result<(), Failure> {
        if self.sequence == 0 {
            return Err(Error::Corrupt("Stratum payout sequence").into());
        }
        core::Record::Payment(self.payment.clone()).encode(accounting)?;
        match &self.request {
            None => {
                if self.payment.tx_hash.is_none() || self.signed.is_some() {
                    return Err(Error::Corrupt("Stratum legacy payout").into());
                }
            }
            Some(q) => {
                valid_chain(q.chain)?;
                q.prepared.encode(limits.payout)?;
                let p = &q.prepared.request().payment;
                if self.payment.tx_hash.is_some()
                    || p.id != self.payment.id
                    || p.total != self.payment.total
                    || p.relative != self.payment.relative
                    || q.prepared.request().gross != self.payment.gross
                {
                    return Err(Error::Corrupt("Stratum payout preparation source").into());
                }
                if let Some(s) = &self.signed {
                    s.date.validate()?;
                    if s.transaction.len() > limits.payout.transaction_bytes + 128 {
                        return Err(Error::ResourceLimit.into());
                    }
                    if q.prepared.attach_signature(s.signature)?.encode() != s.transaction {
                        return Err(Error::Corrupt("Stratum payout signed bytes").into());
                    }
                    match &s.dispatch {
                        Dispatch::Ready(_) => {}
                        Dispatch::Sending(n) if *n > 0 => {}
                        Dispatch::Done { attempt, outcome } if *attempt > 0 => match outcome {
                            Outcome::Accepted {
                                chain, revision, ..
                            } => {
                                valid_chain(*chain)?;
                                if *revision == 0 {
                                    return Err(
                                        Error::Corrupt("Stratum payout pool revision").into()
                                    );
                                }
                            }
                            Outcome::Rejected(reason)
                                if !reason.is_empty() && reason.len() <= 4096 => {}
                            Outcome::Observed {
                                chain, location, ..
                            } if *location != Location::Unknown => {
                                valid_chain(*chain)?;
                            }
                            _ => return Err(Error::ResourceLimit.into()),
                        },
                        _ => return Err(Error::Corrupt("Stratum payout dispatch attempt").into()),
                    }
                }
            }
        }
        Ok(())
    }
}
pub(super) fn valid_chain(chain: repo::Status) -> Result<(), Failure> {
    repo::Status::decode(&chain.encode())?;
    if chain.tip.is_none() || chain.revision == 0 {
        return Err(Error::Corrupt("Stratum payout chain observation").into());
    }
    Ok(())
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completion {
    pub record: Hash256,
    pub sequence: u64,
    pub payment: core::PaymentId,
    pub hash: Hash256,
    pub block: Hash256,
    pub height: u64,
    pub chain: repo::Status,
    pub pruned: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    pub config: Config,
    pub network: Vec<u8>,
    pub epoch: u64,
    pub context: Context,
    pub next: u64,
    pub pending: Option<Record>,
    pub last: Option<Completion>,
}
impl Info {
    pub(super) fn validate(&self, limits: Limits, accounting: core::Limits) -> Result<(), Failure> {
        limits.validate()?;
        valid_chain(self.context.chain)?;
        if self.network.is_empty()
            || self.network.len() > 1024
            || self.epoch == 0
            || self.next == 0
            || self
                .context
                .balance
                .as_ref()
                .is_some_and(|b| b.as_bytes().len() > 2048)
        {
            return Err(Error::Corrupt("Stratum payout metadata fields").into());
        }
        if let Some(r) = &self.pending {
            r.validate(limits, accounting)?;
            if r.sequence.checked_add(1) != Some(self.next) {
                return Err(Error::Corrupt("Stratum pending sequence").into());
            }
            if let Some(q) = &r.request {
                let p = q.prepared.request();
                if p.environment.caller != self.config.caller
                    || p.environment.contract != self.config.contract
                    || p.network != self.network
                {
                    return Err(Error::Corrupt("Stratum payout identity").into());
                }
            }
        }
        if let Some(c) = &self.last {
            valid_chain(c.chain)?;
            if c.sequence == 0
                || c.sequence >= self.next
                || self
                    .pending
                    .as_ref()
                    .is_some_and(|r| r.sequence <= c.sequence)
            {
                return Err(Error::Corrupt("Stratum completed sequence").into());
            }
        }
        Ok(())
    }
}
