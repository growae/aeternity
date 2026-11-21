use super::*;

struct Poison<'a>(&'a Mutex<Option<Error>>);
impl Drop for Poison<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get_or_insert(Error::WorkerPanicked);
        }
    }
}
impl<O: MapOrder> Context<O> {
    pub(super) fn apply_policy(
        &self,
        event: ae_chain::resilience::Event,
        config: ae_chain::resilience::Config,
        cancel: blocking::Cancellation,
    ) -> Result<ae_chain::resilience::Snapshot, Error> {
        let _poison = Poison(&self.recovery);
        self.check(&cancel, None)?;
        let update = || self.db.update_resilience(event, config);
        #[cfg(test)]
        let update = || match &self.policy_hook {
            Some(hook) => hook(&self.db, event, config),
            None => update(),
        };
        update().map_err(|error| {
            if matches!(error, repository::Error::Backend(_)) {
                let error = Error::PolicyUncertain { error };
                self.recovery
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get_or_insert_with(|| error.clone());
                error
            } else {
                Error::Storage(error)
            }
        })
    }

    pub(super) fn apply_block(
        &self,
        bytes: Vec<u8>,
        now_ms: u64,
        cancel: blocking::Cancellation,
        origin: ledger::Origin,
    ) -> Result<Report, Failure> {
        let _poison = Poison(&self.recovery);
        let result = (|| {
            self.check(&cancel, None)?;
            let view = self.db.view()?;
            let block = Block::decode(&bytes, view.config().block()).map_err(Error::Block)?;
            let mut rules = self.rules.clone();
            rules.admission.now_ms = now_ms;
            self.insert(&block, &rules, &cancel, None, origin)
        })();
        Self::block_report(result)
    }
    pub(super) fn apply_received_block(
        &self,
        block: Block,
        now_ms: u64,
        cancel: blocking::Cancellation,
        client: &net::Client,
    ) -> Result<Report, Failure> {
        let _poison = Poison(&self.recovery);
        Self::block_report(self.insert_gossip(&block, now_ms, &cancel, Some(client)))
    }
    fn insert_gossip(
        &self,
        block: &Block,
        now_ms: u64,
        cancel: &blocking::Cancellation,
        client: Option<&net::Client>,
    ) -> Result<Accepted, Error> {
        let mut rules = self.rules.clone();
        rules.admission.now_ms = now_ms;
        self.insert(block, &rules, cancel, client, ledger::Origin::Gossip)
    }
    fn block_report(result: Result<Accepted, Error>) -> Result<Report, Failure> {
        result
            .map(|accepted| Report {
                accepted: vec![accepted],
            })
            .map_err(|error| Failure {
                progress: Some(Report::default()),
                error,
            })
    }
    fn check(
        &self,
        cancel: &blocking::Cancellation,
        client: Option<&net::Client>,
    ) -> Result<(), Error> {
        if self
            .recovery
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
        {
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
    pub(super) fn decode(
        &self,
        input: &Input,
        requested: Hash256,
        direction: Direction,
        now_ms: u64,
        cancel: &blocking::Cancellation,
    ) -> Result<generation::Generation, Error> {
        self.check(cancel, None)?;
        let owned;
        let message = match input {
            Input::Packet(bytes) => {
                owned = Message::decode_packet(bytes, self.config.messages)?;
                &owned
            }
            Input::Live(live) => {
                self.check(cancel, Some(&live.client))?;
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
            ae_chain::admission::validate_pow(generation.key(), &network, None, &rules).map_err(
                |e| Error::Validation {
                    operational: e.is_operational(),
                    reason: format!("{e:?}"),
                },
            )?;
        }
        generation
            .check(requested, view.config().mode)
            .map_err(Error::Generation)?;
        Ok(generation)
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
        let generation = self
            .decode(&input, requested, direction, now_ms, &cancel)
            .map_err(|error| Failure {
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
            match self.insert(block, &rules, &cancel, client, ledger::Origin::Sync) {
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
        origin: ledger::Origin,
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
                    micro_transactions: match block {
                        Block::Micro(b) => Some(b.transactions.len() as u64),
                        Block::Key(_) => None,
                    },
                });
            }
            let update = self
                .execution
                .with_config(|config| {
                    let mut limits = self.config.ledger;
                    limits.policy.origin = origin;
                    ledger::prepare(&view, block, config, rules, limits)
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
                        micro_transactions: match block {
                            Block::Micro(b) => Some(b.transactions.len() as u64),
                            Block::Key(_) => None,
                        },
                    });
                }
                Err(repository::Error::Conflict) if attempt < self.config.conflicts => continue,
                Err(error @ repository::Error::Backend(_)) => {
                    // Never retry an uncertain publication, or let later jobs
                    // use this owner. Reopen/reconcile the database first.
                    let error = Error::CommitUncertain { hash, error };
                    self.recovery
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .get_or_insert_with(|| error.clone());
                    return Err(error);
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
