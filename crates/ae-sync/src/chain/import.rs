use super::*;

struct Poison<'a>(&'a AtomicBool);
impl Drop for Poison<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Release);
        }
    }
}
impl<O: MapOrder> Context<O> {
    fn check(
        &self,
        cancel: &blocking::Cancellation,
        client: Option<&net::Client>,
    ) -> Result<(), Error> {
        if self.recovery.load(Ordering::Acquire) {
            return Err(Error::RecoveryRequired);
        }
        if self.stopped.load(Ordering::Acquire) || cancel.cancelled() {
            return Err(Error::Cancelled);
        }
        if client.is_some_and(net::Client::is_closed) {
            return Err(Error::Stale);
        }
        Ok(())
    }
    pub(super) fn apply(
        &self,
        input: Input,
        requested: Hash256,
        direction: Direction,
        now_ms: u64,
        cancel: blocking::Cancellation,
    ) -> Result<Report, Failure> {
        let _poison = Poison(&self.recovery);
        let mut progress = Report::default();
        let operation = || -> Result<generation::Generation, Error> {
            self.check(&cancel, None)?;
            let owned;
            let message = match &input {
                Input::Packet(bytes) => {
                    owned = Message::decode_packet(bytes, self.config.messages)?;
                    &owned
                }
                Input::Live(live) => {
                    self.check(&cancel, Some(&live.client))?;
                    // A driver may have different limits from this importer.
                    // Recheck the retained envelope inside our work budget.
                    live.received
                        .message()
                        .encode_packet(self.config.messages)?;
                    live.received.message()
                }
            };
            let body = if message.kind() == Kind::Response {
                match message.details() {
                    Details::Response {
                        kind: Kind::Generation,
                        message: Some(Ok(body)),
                    } => body.as_ref(),
                    Details::Response {
                        kind: Kind::Generation,
                        message: None,
                    } => {
                        let Some(Value::Binary(reason)) = message.fields().get("reason") else {
                            return Err(Error::Generation(generation::Error::Message));
                        };
                        return Err(Error::Refused(reason.clone()));
                    }
                    Details::Response {
                        message: Some(Err(error)),
                        ..
                    } => return Err(Error::Message(error.clone())),
                    _ => return Err(Error::Generation(generation::Error::Message)),
                }
            } else {
                message
            };
            let view = self.db.view()?;
            let generation = generation::Generation::decode(
                body,
                direction,
                view.config().block(),
                self.config.blocks,
            )
            .map_err(Error::Generation)?;
            let height = generation.key().header().common().height;
            if height > 0 {
                let network = view.protocol_profile(
                    self.execution.network(),
                    height,
                    self.config.ledger.history,
                )?;
                let mut rules = self.rules.admission.clone();
                rules.now_ms = now_ms;
                self.whitelist(generation.key())?;
                ae_chain::admission::validate_pow(generation.key(), &network, None, &rules)
                    .map_err(|e| Error::Validation {
                        operational: e.is_operational(),
                        reason: format!("{e:?}"),
                    })?;
            }
            generation
                .check(requested, view.config().mode)
                .map_err(Error::Generation)?;
            Ok(generation)
        };
        let generation = operation().map_err(|error| Failure {
            progress: Some(progress.clone()),
            error,
        })?;
        let client = match &input {
            Input::Live(live) => Some(&live.client),
            _ => None,
        };
        let mut rules = self.rules.clone();
        rules.admission.now_ms = now_ms;
        for block in generation.blocks() {
            match self.insert(block, &rules, &cancel, client) {
                Ok(accepted) => progress.accepted.push(accepted),
                Err(error) => {
                    return Err(Failure {
                        progress: Some(progress),
                        error,
                    });
                }
            }
        }
        // Keep a live Received's transport byte credit until all insertions
        // finish. The blocking pool retains its work/reply credits separately.
        drop(input);
        Ok(progress)
    }
    fn insert(
        &self,
        block: &Block,
        rules: &Rules,
        cancel: &blocking::Cancellation,
        client: Option<&net::Client>,
    ) -> Result<Accepted, Error> {
        let hash = block.hash().map_err(|_| Error::Hash)?;
        for attempt in 0..=self.config.conflicts {
            self.check(cancel, client)?;
            let view = self.db.view()?;
            let height = block.header().common().height;
            let network = view.protocol_profile(
                self.execution.network(),
                height,
                self.config.ledger.history,
            )?;
            let previous = view
                .block(Hash256(block.header().common().previous_hash))?
                .map(|b| b.header());
            // add_synced_block validates before the conductor's duplicate
            // check. In particular, a known header is not permission to
            // accept a malformed replacement micro body.
            self.whitelist(block)?;
            ae_chain::admission::validate_pow(block, &network, previous.as_ref(), &rules.admission)
                .map_err(|e| Error::Validation {
                    operational: e.is_operational(),
                    reason: format!("{e:?}"),
                })?;
            if view.block(hash)?.is_some() {
                view.require_record(hash)?;
                return Ok(Accepted {
                    hash,
                    outcome: Outcome::Present,
                });
            }
            let update = self
                .execution
                .with_config(|config| {
                    ledger::prepare(&view, block, config, rules, self.config.ledger)
                })
                .map_err(|e| match e {
                    ledger::InsertError::Repository(error) => Error::Storage(error),
                    ledger::InsertError::Policy { height, top } => Error::Policy { height, top },
                    ledger::InsertError::WrongConsensus => Error::WrongConsensus,
                    other => Error::Validation {
                        operational: other.is_operational(),
                        reason: format!("{other:?}"),
                    },
                })?;
            self.check(cancel, client)?;
            match self.commit(update) {
                Ok(committed) => {
                    return Ok(Accepted {
                        hash,
                        outcome: Outcome::Committed {
                            revision: committed.change().revision,
                        },
                    });
                }
                Err(repository::Error::Conflict) if attempt < self.config.conflicts => continue,
                Err(error @ repository::Error::Backend(_)) => {
                    // Never retry an uncertain publication, or let later jobs
                    // use this owner. Reopen/reconcile the database first.
                    self.recovery.store(true, Ordering::Release);
                    return Err(Error::CommitUncertain { hash, error });
                }
                Err(error) => return Err(error.into()),
            }
        }
        unreachable!("last conflict is returned")
    }
    fn whitelist(&self, block: &Block) -> Result<(), Error> {
        if let Block::Key(key) = block
            && let Some(expected) = self.rules.checkpoints.get(&key.common.height)
            && block.hash().map_err(|_| Error::Hash)? != *expected
        {
            return Err(Error::BlockedByWhitelist);
        }
        Ok(())
    }
    fn commit(
        &self,
        update: repository::Update,
    ) -> Result<ae_storage::Committed, repository::Error> {
        #[cfg(test)]
        if let Some(hook) = &self.commit_hook {
            return hook(&self.db, update);
        }
        self.db.commit(update)
    }
}
