use super::*;
use ae_state::names::NamePointerResolver;
use ae_types::IdTag;
impl Pool {
    pub fn push<A: NodeReader, C: NodeReader>(
        &mut self,
        tx: Arc<Signed>,
        options: PushOptions,
        env: &Admission<'_, A, C>,
    ) -> Result<PushOutcome, Error> {
        self.push_observed(tx, options, env, |_| {})
    }
    // The native push counter precedes the insertion call, including insertion
    // failure. Cached inputs and operational precheck failures emit no event.
    pub fn push_observed<A: NodeReader, C: NodeReader>(
        &mut self,
        tx: Arc<Signed>,
        options: PushOptions,
        env: &Admission<'_, A, C>,
        observe: impl FnOnce(AdmissionEvent),
    ) -> Result<PushOutcome, Error> {
        match self.check_push(&tx, options, env) {
            Ok(None) => Ok(PushOutcome::Cached),
            Ok(Some((bytes, residency))) => {
                observe(AdmissionEvent::Submit);
                self.accept(tx, bytes, residency, options.event)
            }
            Err(error) => {
                if error == Error::AlreadyKnown {
                    observe(AdmissionEvent::Ignore);
                } else if !error.is_operational() {
                    observe(AdmissionEvent::Reject);
                }
                Err(error)
            }
        }
    }
    fn check_push<A: NodeReader, C: NodeReader>(
        &self,
        tx: &Signed,
        options: PushOptions,
        env: &Admission<'_, A, C>,
    ) -> Result<Option<(usize, u64)>, Error> {
        let PushOptions {
            event,
            location,
            gossip_cached,
        } = options;
        if event == Event::Received && gossip_cached {
            return Ok(None);
        }
        let bytes = self.encode_size(tx, env.record)?;
        let mut lookup = |k| {
            env.trees
                .accounts
                .lookup(k)
                .map(|a| a.map(AccountInfo::from))
                .map_err(Error::Account)
        };
        let residency = match location {
            Location::Block(_) => return Err(Error::AlreadyAccepted),
            Location::Pool => return Err(Error::AlreadyKnown),
            Location::Deleted if !self.config.allow_reentry => return Err(Error::AlreadyKnown),
            Location::Deleted => self.residency(tx, &mut lookup)?,
            Location::Unknown => {
                self.validate(tx, event, env)?;
                self.residency(tx, &mut lookup)?
            }
        };
        Ok(Some((bytes, residency)))
    }
    pub fn force_push(
        &mut self,
        tx: Arc<Signed>,
        record: ae_tx::storage::Config<'_>,
    ) -> Result<PushOutcome, Error> {
        let bytes = self.encode_size(&tx, record)?;
        self.accept(tx, bytes, self.config.tx_ttl, Event::Created)
    }
    fn accept(
        &mut self,
        tx: Arc<Signed>,
        bytes: usize,
        residency: u64,
        event: Event,
    ) -> Result<PushOutcome, Error> {
        let key = Key::of(&tx)?;
        if self.active.contains_key(&key) {
            return Ok(PushOutcome::Accepted {
                hash: key.hash,
                inserted: false,
                event,
            });
        }
        self.check_capacity(&key, bytes)?;
        let expiry = self.expiry(&tx, residency)?;
        let hash = key.hash;
        self.insert(key, tx, bytes, expiry);
        Ok(PushOutcome::Accepted {
            hash,
            inserted: true,
            event,
        })
    }
    pub fn validate<A: NodeReader, C: NodeReader>(
        &self,
        tx: &Signed,
        event: Event,
        env: &Admission<'_, A, C>,
    ) -> Result<(), Error> {
        let view = tx.transaction().view();
        if !view
            .valid_at_consensus_with_state_config(
                env.protocol,
                env.consensus,
                env.record.transaction,
                env.record.state,
            )
            .map_err(Error::Transaction)?
        {
            return Err(Error::InvalidAtProtocol);
        }
        ae_execution::transactions::verify_with_channel_state(
            view,
            tx.signatures(),
            env.network,
            env.protocol,
            false,
            self.limits.signatures,
            &env.trees.channels,
        )
        .map_err(Error::Signature)?;
        let inner = innermost(view);
        let account = inner.origin().map_err(|_| Error::NoOrigin)?;
        let account = env
            .trees
            .accounts
            .lookup(account)
            .map_err(Error::Account)?
            .map(AccountInfo::from);
        nonce_check(
            account.as_ref(),
            &inner.nonce(),
            event == Event::Created,
            &self.config,
        )?;
        meta_gas(view, &self.config)?;
        let pricing = view.pricing(&env.height.into(), env.protocol);
        if number(view.fee()) < pricing.minimum_fee {
            return Err(Error::TooLowFee);
        }
        if view
            .gas_price()
            .is_some_and(|p| p < &env.protocol.minimum_gas_price().into())
        {
            return Err(Error::TooLowGasPrice);
        }
        if pricing.minimum_gas_price < number(&self.config.minimum_miner_gas_price) {
            return Err(Error::TooLowMinerGasPrice);
        }
        if inner.layers().is_empty()
            && let TransactionLeaf::Spend(spend) = inner.leaf()
        {
            let mut id = spend.recipient();
            let mut hops = 0;
            while id.tag == IdTag::Name {
                if hops == self.limits.name_hops {
                    return Err(Error::ResourceLimit);
                }
                hops += 1;
                id = env
                    .trees
                    .names
                    .resolve_id(id.value, b"account_pubkey")
                    .map_err(|e| match e {
                        ae_state::names::Error::NameNotFound
                        | ae_state::names::Error::NameRevoked
                        | ae_state::names::Error::PointerNotFound
                        | ae_state::names::Error::PointerNotId => Error::UnresolvedRecipient,
                        e => Error::Name(e),
                    })?;
            }
            if env
                .trees
                .accounts
                .lookup(AccountPublicKey(id.value))
                .map_err(Error::Account)?
                .is_some_and(|a| !a.is_payable())
            {
                return Err(Error::NotPayable);
            }
        }
        if view.ttl().is_some_and(|ttl| ttl < &env.height.into()) {
            return Err(Error::TtlExpired);
        }
        Ok(())
    }
}
