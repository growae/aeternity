use super::*;
pub(super) struct Authority {
    pub sequence: u64,
    pub checked: task::Checked,
}
impl Worker {
    pub(super) fn current(&self, record: &Record) -> Result<bool, task::Error> {
        let Some(authority) = &self.authority else {
            return Ok(false);
        };
        let Some(request) = &record.request else {
            return Ok(false);
        };
        let encoded = request
            .prepared
            .encode(self.inputs.limits.payout)
            .map_err(|e| {
                task::Error::Evaluation(Box::new(task::EvaluationFailure::Input(Box::new(e))))
            })?;
        if authority.sequence != record.sequence
            || authority.checked.evaluation().preparation_hash() != ae_crypto::blake2b_256(&encoded)
        {
            return Ok(false);
        }
        self.preflight.is_current(&authority.checked)
    }
    // The caller holds the shared dispatch gate. This method either authorizes
    // these exact bytes against a current observation or starts one owned check.
    pub(super) fn authorize(&mut self, record: &Record) -> Result<bool, Error> {
        if let Some(hash) = record.hash()? {
            let view = self.db.view().map_err(storage::Failure::from)?;
            if let Some(block) = view.location(hash).map_err(storage::Failure::from)? {
                if !view.is_canonical(block).map_err(storage::Failure::from)? {
                    return Err(Error::Mining("payout location is not canonical"));
                }
                self.authority = None;
                self.shared
                    .notice(Notice::AwaitingConfirmation { hash, block });
                return Ok(false);
            }
        }
        if self
            .current(record)
            .map_err(|e| Error::PayoutPreflight(Arc::new(e)))?
        {
            return Ok(true);
        }
        self.authority = None;
        let request = record
            .request
            .as_ref()
            .ok_or(Error::Mining("preflight request missing"))?;
        match self.preflight.start(Arc::new(request.prepared.clone())) {
            Ok(task) => {
                self.pending = Some(Pending::Preflight {
                    record: Box::new(record.clone()),
                    task,
                    result: None,
                });
                self.shared.phase(Phase::Preflight);
            }
            Err(error) => self.shared.notice(Notice::PreflightStart(Arc::new(error))),
        }
        Ok(false)
    }
    // Entry after durable Sending intent, before any pool ticket exists.
    pub(super) fn submit(
        &mut self,
        writer: &mut storage::Writer,
        record: Record,
        tx: Signed,
        dispatch: bool,
    ) -> Result<(), Error> {
        let shared = self.shared.clone();
        let gate = shared.dispatch.lock().unwrap_or_else(|e| e.into_inner());
        if !*gate {
            drop(gate);
            self.withdraw(writer, record, Withdrawal::Stopped, dispatch)?;
            return Ok(());
        }
        // Intent publication may have overlapped a chain commit.
        // No pool ticket exists yet, so a stale/error observation
        // can be durably restored to Ready without ambiguity.
        let current = self.current(&record);
        if !matches!(current, Ok(true)) {
            drop(gate);
            let reason = match current {
                Ok(false) => Withdrawal::Stale,
                Err(error) => Withdrawal::Preflight(Arc::new(error)),
                Ok(true) => unreachable!(),
            };
            self.withdraw(writer, record, reason, dispatch)?;
            return Ok(());
        }
        let Some(pool) = self.pool(dispatch) else {
            drop(gate);
            self.withdraw(writer, record, Withdrawal::PoolUnavailable, dispatch)?;
            return Ok(());
        };
        let accepted = pool.push(Arc::new(tx), Event::Created, false);
        drop(gate);
        match accepted {
            Ok(ticket) => {
                self.pending = Some(Pending::Submission {
                    record: Box::new(record),
                    ticket,
                    result: None,
                });
                self.shared.phase(Phase::Submitting);
            }
            Err(error) => {
                self.withdraw(writer, record, Withdrawal::Pool(error), dispatch)?;
            }
        }
        Ok(())
    }
    pub(super) fn withdraw(
        &mut self,
        writer: &mut storage::Writer,
        record: Record,
        reason: Withdrawal,
        dispatch: bool,
    ) -> Result<(), Error> {
        // No ticket was accepted. Keep that proof and the exact Sending record
        // through failed/uncertain cleanup publication and report/shutdown.
        self.pending = Some(Pending::Unaccepted {
            record: Box::new(record),
            reason,
        });
        self.authority = None;
        self.complete(writer, dispatch)
    }
}
