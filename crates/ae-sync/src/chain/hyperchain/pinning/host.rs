use super::*;
use ae_types::Unsigned;
use num_bigint::BigInt;
pub(super) struct Host<O> {
    pub source: Weak<eoe::Source>,
    pub execution: repo::HyperchainConfig<O>,
    pub producer: scoped::Producer<O>,
    pub pool: crate::bindings::Admission,
    pub parent: parent::Client<O>,
    pub keys: Keys,
    pub config: Config,
    pub shared: Arc<Shared>,
    pub effects: Vec<Effect>,
    pub children: children::Writer,
    pub agent: durable::Writer,
    pub database: Arc<Database>,
}
impl<O: MapOrder + Clone + Send + Sync + 'static> Host<O> {
    fn check(&self) -> Result<(), Error> {
        if self.shared.stop.load(Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        self.producer.check().map_err(Error::Source)?;
        if self.pool.is_closed()
            || (!self.pool.managed()
                && self
                    .pool
                    .current()
                    .is_none_or(|p| !p.client.info().accepting))
        {
            return Err(Error::Closed);
        }
        self.parent.check().map_err(Error::Utilities)
    }
    fn read<T>(
        &self,
        mut submit: impl FnMut(
            &pool::worker::Client,
        ) -> Result<pool::worker::Ticket<T>, pool::worker::Error>,
    ) -> Result<T, Error> {
        let ticket = loop {
            self.check()?;
            let client = self.pool_client()?;
            match submit(&client) {
                Ok(t) => break t,
                Err(pool::worker::Error::Saturated) => thread::sleep(self.config.poll),
                Err(pool::worker::Error::Stopped) if self.pool.managed() => {
                    thread::sleep(self.config.poll)
                }
                Err(e) => return Err(Error::Pool(e)),
            }
        };
        loop {
            self.check()?;
            match ticket.wait_timeout(self.config.poll) {
                Err(pool::worker::Error::Timeout) => (),
                r => return r.map_err(Error::Pool),
            }
        }
    }
    fn pool_client(&self) -> Result<pool::worker::Client, Error> {
        loop {
            self.check()?;
            if let Some(pool) = self.pool.current()
                && pool.client.info().accepting
            {
                return Ok(pool.client);
            }
            thread::sleep(self.config.poll);
        }
    }
    fn minimum_gas_price(&self) -> Result<Unsigned, Error> {
        self.check()?;
        let source = self.source.upgrade().ok_or(Error::Closed)?;
        let selected = source
            .snapshots()
            .view()
            .map_err(|_| Error::State("gas price snapshot"))?;
        let protocol = selected
            .value
            .protocol_profile(
                &self.execution.profile.network,
                1,
                self.execution.limits.history,
            )
            .map_err(|_| Error::State("height-one protocol"))?
            .protocol_at(1)
            .map_err(|_| Error::Configuration)?;
        let miner = self.read(|pool| pool.minimum_miner_gas_price())?;
        if miner.as_bytes().len() > self.config.transaction.integer_bytes {
            return Err(Error::ResourceLimit);
        }
        Ok(miner.max(protocol.minimum_gas_price().into()))
    }
    fn operation(&self) -> Result<durable::Operation, Error> {
        self.agent
            .checkpoint()
            .map_err(Error::Journal)?
            .pending
            .as_ref()
            .map(|p| p.operation.clone())
            .ok_or(Error::State("missing pending event"))
    }
    fn child_intent(
        &self,
        kind: ChildKind,
        leader: AccountPublicKey,
        hash: &[u8],
    ) -> Result<children::Intent, Error> {
        let metadata = self.parent.metadata().map_err(Error::Utilities)?;
        Ok(children::Intent {
            network: self.execution.profile.network.network_id().to_vec(),
            parent_chain: match metadata.chain {
                parent_pins::Chain::Aeternity => children::Chain::Aeternity,
                parent_pins::Chain::Dogecoin => children::Chain::Dogecoin,
                parent_pins::Chain::Bitcoin => children::Chain::Bitcoin,
            },
            parent_network: metadata.network.to_vec(),
            leader,
            kind: match kind {
                ChildKind::Note => children::Kind::Note,
                ChildKind::Proof => {
                    children::Kind::Proof(self.execution.profile.contracts.election)
                }
            },
            parent_hash: hash.to_vec(),
        })
    }
    fn child_decision(
        &mut self,
        kind: ChildKind,
        leader: AccountPublicKey,
        hash: &[u8],
    ) -> Result<(), Error> {
        let mut operation = self.operation()?;
        let decision = match kind {
            ChildKind::Note => &operation.note,
            ChildKind::Proof => &operation.proof,
        };
        if let Some(durable::ChildResult::Skipped(error)) = decision {
            return Err(Error::Transaction(*error));
        }
        let id = self
            .child_intent(kind, leader, hash)?
            .id()
            .map_err(|e| Error::Children(e.into()))?;
        if let Some(durable::ChildResult::Completed(expected)) = decision
            && *expected != id
        {
            return Err(Error::State("child intent changed"));
        }
        if matches!(decision, Some(durable::ChildResult::Completed(_)))
            && self
                .children
                .get(&self.child_intent(kind, leader, hash)?)
                .map_err(Error::Children)?
                .is_none()
        {
            return Err(Error::State("missing completed child record"));
        }
        let result = self.child(kind, leader, hash);
        let stored = match &result {
            Ok(()) => Some(durable::ChildResult::Completed(id)),
            Err(Error::Transaction(error @ (core::Error::Signing | core::Error::Encoding)))
                if kind == ChildKind::Note =>
            {
                Some(durable::ChildResult::Skipped(*error))
            }
            _ => None,
        };
        if let Some(stored) = stored {
            match kind {
                ChildKind::Note => operation.note = Some(stored),
                ChildKind::Proof => operation.proof = Some(stored),
            }
            self.agent.observe(operation).map_err(Error::Journal)?;
        }
        result
    }
    fn child(
        &mut self,
        kind: ChildKind,
        leader: AccountPublicKey,
        hash: &[u8],
    ) -> Result<(), Error> {
        self.check()?;
        let intent = self.child_intent(kind, leader, hash)?;
        if let Some(record) = self.children.get(&intent).map_err(Error::Children)? {
            return self.recover_child(kind, record);
        }
        let nonce = self
            .read(|pool| pool.next_nonce(leader))?
            .unwrap_or_else(|| 1.into());
        let minimum_gas_price = self.minimum_gas_price()?;
        let input = core::TransactionInput {
            leader,
            nonce,
            minimum_gas_price,
            parent_hash: hash,
            network: self.execution.profile.network.network_id(),
            keys: &self.keys,
            limits: self.config.transaction,
        };
        if self.parent.metadata().map_err(Error::Utilities)?.signer == parent::Signer::None {
            return Err(Error::Transaction(core::Error::Signing));
        }
        let tx = match kind {
            ChildKind::Note => core::note(input),
            ChildKind::Proof => core::proof(self.execution.profile.contracts.election, input),
        }
        .map_err(Error::Transaction)?;
        let record = children::Record {
            intent,
            transaction: tx.encode(),
            receipt: None,
        };
        let record = match self.children.reserve(record).map_err(Error::Children)? {
            children::Reservation::New(r) => r,
            children::Reservation::Existing(r) => return self.recover_child(kind, r),
        };
        self.admit_child(kind, record, Arc::new(tx))
    }
    fn recover_child(&mut self, kind: ChildKind, record: children::Record) -> Result<(), Error> {
        let transaction = Arc::new(record.signed().map_err(|e| Error::Children(e.into()))?);
        let observed = self.children.observe(&record).map_err(Error::Children)?;
        let (result, outcome) = if let Some(receipt) = observed.and_then(|r| r.receipt) {
            (ChildResult::Recovered(receipt), Ok(()))
        } else {
            (
                ChildResult::Unresolved,
                Err(Error::ChildUnresolved(
                    record.intent.id().map_err(|e| Error::Children(e.into()))?,
                )),
            )
        };
        self.effects.push(Effect::Child {
            kind,
            transaction,
            result,
        });
        outcome
    }
    fn admit_child(
        &mut self,
        kind: ChildKind,
        record: children::Record,
        tx: Arc<ae_mempool::pool::Signed>,
    ) -> Result<(), Error> {
        let ticket = loop {
            if let Err(error) = self.check() {
                self.effects.push(Effect::Child {
                    kind,
                    transaction: tx,
                    result: ChildResult::Unresolved,
                });
                return Err(error);
            }
            let client = match self.pool_client() {
                Ok(client) => client,
                Err(error) => {
                    self.effects.push(Effect::Child {
                        kind,
                        transaction: tx,
                        result: ChildResult::Unresolved,
                    });
                    return Err(error);
                }
            };
            match client.push(tx.clone(), ae_mempool::pool::Event::Received, false) {
                Ok(t) => break t,
                Err(pool::worker::Error::Saturated) => thread::sleep(self.config.poll),
                Err(pool::worker::Error::Stopped) if self.pool.managed() => {
                    thread::sleep(self.config.poll)
                }
                Err(e) => {
                    self.effects.push(Effect::Child {
                        kind,
                        transaction: tx,
                        result: ChildResult::Unresolved,
                    });
                    return Err(Error::Pool(e));
                }
            }
        };
        loop {
            if let Err(error) = self.check() {
                self.effects.push(Effect::Child {
                    kind,
                    transaction: tx,
                    result: ChildResult::Pending(ticket),
                });
                return Err(error);
            }
            match ticket.wait_timeout(self.config.poll) {
                Err(pool::worker::Error::Timeout) => (),
                result => {
                    let failure = result
                        .as_ref()
                        .err()
                        .filter(|e| e.is_operational())
                        .cloned();
                    let receipt = match &result {
                        Ok(r) => Some(children::Receipt::Submitted(r.clone())),
                        Err(e) if !e.is_operational() => {
                            Some(children::Receipt::Rejected(format!("{e:?}")))
                        }
                        _ => None,
                    };
                    let persisted = receipt
                        .map(|r| self.children.record_receipt(&record, r))
                        .transpose();
                    self.effects.push(Effect::Child {
                        kind,
                        transaction: tx,
                        result: ChildResult::Known(result),
                    });
                    persisted.map_err(Error::Children)?;
                    return failure.map_or(Ok(()), |e| Err(Error::Pool(e)));
                }
            }
        }
    }
}
impl<O: MapOrder + Clone + Send + Sync + 'static> core::Host for Host<O> {
    type Error = Error;
    fn operational(e: &Error) -> bool {
        !matches!(
            e,
            Error::Transaction(core::Error::Signing | core::Error::Encoding)
        )
    }
    fn leader(&mut self, height: &BigInt) -> Result<AccountPublicKey, Error> {
        self.check()?;
        let mut operation = self.operation()?;
        if let Some((leader, _)) = operation.leader {
            return Ok(leader);
        }
        let leader = self
            .producer
            .cached_leader(height)
            .map_err(Error::Source)?
            .ok_or(Error::LeaderUnavailable)?;
        operation.leader = Some((leader, self.parent.mapped(leader)));
        self.agent.observe(operation).map_err(Error::Journal)?;
        Ok(leader)
    }
    fn mapped(&self, leader: AccountPublicKey) -> bool {
        self.operation()
            .ok()
            .and_then(|o| o.leader)
            .is_some_and(|(l, m)| l == leader && m)
    }
    fn post(&mut self, leader: AccountPublicKey) -> Result<Option<Vec<u8>>, Error> {
        self.check()?;
        let mut operation = self.operation()?;
        if let Some(result) = &operation.parent {
            if let durable::ParentResult::Submitted { intent, .. }
            | durable::ParentResult::Unknown { intent, .. } = result
            {
                let prepared = operation
                    .post
                    .as_ref()
                    .ok_or(Error::State("missing parent request"))?;
                if *intent
                    != journal::parent_intent(
                        &self.parent.metadata().map_err(Error::Utilities)?,
                        prepared,
                        65536,
                    )?
                {
                    return Err(Error::State("parent intent changed"));
                }
                let record = self
                    .database
                    .parent_posts()
                    .map_err(|e| Error::Journal(e.into()))?
                    .get(*intent, 4 << 20)
                    .map_err(|e| Error::Journal(e.into()))?
                    .ok_or(Error::State("missing completed parent record"))?;
                if let durable::ParentResult::Submitted { hash, .. } = result
                    && record.response.as_ref() != Some(hash)
                {
                    return Err(Error::State("parent response changed"));
                }
            }
            let hash = result.hash();
            self.effects.push(Effect::RecoveredParent(result.clone()));
            return Ok(hash);
        }
        let prepared = if let Some(post) = &operation.post {
            post.clone()
        } else {
            let fee = self.minimum_gas_price()?.mul_u64(1_000_000);
            let post = self
                .parent
                .prepare_post(leader, 1.into(), fee)
                .map_err(Error::Utilities)?;
            let checkpoint = self.agent.checkpoint().map_err(Error::Journal)?;
            let pending = checkpoint
                .pending
                .as_ref()
                .ok_or(Error::State("missing epoch"))?;
            match pending.events.get(pending.next) {
                Some(core::Event::Epoch { epoch, first, .. })
                    if &post.pin.epoch + 1 == *epoch && &post.pin.height + 1 == *first => {}
                _ => return Err(Error::StaleEpoch),
            }
            let post = durable::Post {
                pin: post.pin,
                account: post.account,
                amount: post.amount,
                fee: post.fee,
            };
            operation.post = Some(post.clone());
            self.agent
                .observe(operation.clone())
                .map_err(Error::Journal)?;
            post
        };
        let intent = journal::parent_intent(
            &self.parent.metadata().map_err(Error::Utilities)?,
            &prepared,
            65536,
        )?;
        let post = parent_pins::posting::Post {
            pin: prepared.pin,
            account: prepared.account,
            amount: prepared.amount,
            fee: prepared.fee,
        };
        let mut request = loop {
            self.check()?;
            match self.parent.service().post(post.clone()) {
                Ok(r) => break r,
                Err(parent_pins::Error::Busy) => thread::sleep(self.config.poll),
                Err(e) => return Err(Error::Parent(e)),
            }
        };
        let mut interrupted = None;
        loop {
            if let Err(e) = self.check() {
                interrupted.get_or_insert(e);
                request.cancel();
            }
            if let Some(outcome) = request.try_result() {
                let hash = match &outcome {
                    parent_pins::posting::Outcome::Submitted { hash, .. } => Some(hash.clone()),
                    _ => None,
                };
                operation.parent = Some(match &outcome {
                    parent_pins::posting::Outcome::Submitted { hash, .. } => {
                        durable::ParentResult::Submitted {
                            hash: hash.clone(),
                            intent,
                        }
                    }
                    parent_pins::posting::Outcome::Unknown { reason, .. } => {
                        durable::ParentResult::Unknown {
                            intent,
                            reason: format!("{reason:?}"),
                        }
                    }
                    parent_pins::posting::Outcome::NotSubmitted(reason) => {
                        durable::ParentResult::NotSubmitted(format!("{reason:?}"))
                    }
                });
                let persisted = self.agent.observe(operation).map_err(Error::Journal);
                self.effects.push(Effect::Parent(outcome));
                persisted?;
                return interrupted.map_or(Ok(hash), Err);
            }
            thread::sleep(self.config.poll);
        }
    }
    fn included(&mut self, hash: Option<&[u8]>) -> Result<bool, Error> {
        self.check()?;
        let Some(hash) = hash else { return Ok(false) };
        let mut operation = self.operation()?;
        if let Some(included) = operation.included {
            return Ok(included);
        }
        let mut request = loop {
            self.check()?;
            match self.parent.service().request(hash) {
                Ok(r) => break r,
                Err(parent_pins::Error::Busy) => thread::sleep(self.config.poll),
                Err(e) => return Err(Error::Parent(e)),
            }
        };
        loop {
            self.check()?;
            if let Some(result) = request.try_result() {
                let included = result
                    .map(|p| p.is_some_and(|p| p.parent_height != (-1).into()))
                    .map_err(Error::Parent)?;
                operation.included = Some(included);
                self.agent.observe(operation).map_err(Error::Journal)?;
                return Ok(included);
            }
            thread::sleep(self.config.poll);
        }
    }
    fn note(&mut self, leader: AccountPublicKey, hash: &[u8]) -> Result<(), Error> {
        self.child_decision(ChildKind::Note, leader, hash)
    }
    fn proof(&mut self, leader: AccountPublicKey, hash: &[u8]) -> Result<(), Error> {
        self.child_decision(ChildKind::Proof, leader, hash)
    }
}
