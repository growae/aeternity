use super::*;
use ae_tx::{GaMeta, WireTransaction};
use std::sync::Arc;

#[derive(Clone)]
struct Environment {
    context: Context,
    // The Erlang environment permits duplicate authenticated identities.
    ids: Vec<AccountPublicKey>,
}
impl Environment {
    fn add(&mut self, meta: &GaMeta) {
        self.context.origin = Origin::Generalized;
        self.ids.insert(0, meta.account);
        self.context.ga_authenticated.insert(meta.account);
        self.context
            .ga_nonces
            .insert(meta.account, meta.auth_id().0.to_vec());
        self.context.ga_transaction_hash = None;
    }
    fn reset(&mut self, meta: &GaMeta, origin: Origin) {
        self.context.origin = origin;
        if let Some(i) = self.ids.iter().position(|k| k == &meta.account) {
            self.ids.remove(i);
        }
        self.context.ga_authenticated = self.ids.iter().copied().collect();
        // Original reset deletes the key; it does not restore an enclosing nonce.
        self.context.ga_nonces.remove(&meta.account);
    }
}
enum Frame<'a, A, C> {
    Payer { gas: BigInt, events: Vec<Event> },
    Meta(Box<MetaFrame<'a, A, C>>),
}
struct MetaFrame<'a, A, C> {
    meta: &'a GaMeta,
    inner_size: usize,
    checkpoint: Trees<A, C>,
    failure_env: Environment,
    origin: Origin,
    receipt: contracts::Receipt,
}
struct Budget {
    bytes: usize,
}
impl Budget {
    fn authentication<A, C>(
        &mut self,
        tx: TransactionRef<'_>,
        env: &mut Environment,
        network: &NetworkProfile,
    ) -> Result<(), Error<A, C>> {
        let Some(Wrapper::GaMeta(meta)) = tx.layer().map(|l| l.wrapper()) else {
            unreachable!()
        };
        let inner = tx.inner().expect("meta inner");
        if inner.wire_size() > self.bytes {
            return Err(SignatureError::ResourceLimit.into());
        }
        let bytes = inner.encode();
        // Retained wire transaction plus the prefixed hash input. Cumulative
        // across wrappers, distinct from consensus gas and signature checks.
        let cost = bytes
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(network.network_id().len()))
            .and_then(|n| n.checked_add(meta.fee.as_bytes().len()))
            .and_then(|n| n.checked_add(meta.gas_price.as_bytes().len()))
            .ok_or(SignatureError::ResourceLimit)?;
        self.bytes = self
            .bytes
            .checked_sub(cost)
            .ok_or(SignatureError::ResourceLimit)?;
        let wire = WireTransaction::decode(&bytes).map_err(Error::AuthenticationTransaction)?;
        let mut input = network.network_id().to_vec();
        input.extend_from_slice(&bytes);
        let input = if env.context.protocol >= Protocol::Ceres {
            meta.serialize_auth_data(&input)
        } else {
            input
        };
        env.context.ga_transaction_hash = Some(ae_crypto::blake2b_256(&input));
        env.context.ga_transaction = Some(Arc::new(wire));
        Ok(())
    }
}
fn authenticate<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: TransactionRef<'_>,
    trees: &mut Trees<A, C>,
    env: &Context,
    config: &Config<'_, O>,
) -> Result<contracts::Receipt, Error<A, C>> {
    let Some(Wrapper::GaMeta(meta)) = tx.layer().map(|l| l.wrapper()) else {
        unreachable!()
    };
    let channels = trees.channels.clone();
    let relevant = || match signatures::relevant(
        tx.inner().expect("meta inner"),
        meta.account,
        config.network,
        config.signature_limits,
        &channels,
    ) {
        Ok(true) => Ok(()),
        Ok(false) => Err(contracts::Error::Rejected("non_relevant_signature")),
        Err(SignatureError::Channel(e)) => Err(contracts::Error::ChannelState(e)),
        Err(_) => Err(contracts::Error::ResourceLimit),
    };
    if meta.abi_version == 1.into() {
        contracts::aevm::authenticate(
            meta,
            tx.wire_size(),
            trees,
            env,
            config.aevm.ok_or(Error::MissingVmConfiguration)?,
            config.aevm_limits,
            relevant,
        )
        .map_err(|e| Error::Aevm(Box::new(e)))
    } else {
        contracts::authentication::process(
            meta,
            tx.wire_size(),
            trees,
            env,
            config.fate.ok_or(Error::MissingVmConfiguration)?,
            config.fate_limits,
            config.maximum_fate_steps,
            relevant,
        )
        .map_err(|e| Error::Contract(Box::new(e)))
    }
}
fn payer<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: TransactionRef<'_>,
    next: &mut Trees<A, C>,
    env: &mut Environment,
    config: &Config<'_, O>,
) -> Result<Frame<'static, A, C>, Error<A, C>> {
    let Some(Wrapper::PayingFor(payer)) = tx.layer().map(|l| l.wrapper()) else {
        unreachable!()
    };
    check_wrapper(tx, &env.context)?;
    let inner = tx.inner().expect("payer inner");
    if inner
        .layers()
        .iter()
        .any(|l| matches!(l.wrapper(), Wrapper::PayingFor(_)))
    {
        return Err(Error::RecursivePayer);
    }
    if inner.leaf().kind() == TransactionKind::ChannelOffchain {
        return Err(Error::NotAllowedToPayFor(TransactionKind::ChannelOffchain));
    }
    let mut changes = BTreeMap::new();
    crate::prepare_sender(
        &next.accounts,
        &mut changes,
        payer.payer,
        &payer.nonce,
        &env.context,
    )?;
    let fee_payer = crate::fee_payer(payer.payer, &payer.fee, &env.context);
    crate::debit(&next.accounts, &mut changes, fee_payer, &payer.fee)?;
    next.accounts
        .apply(&changes.into_values().collect::<Vec<_>>())
        .map_err(crate::Error::from)?;
    env.context.payer = Some(payer.payer);
    verify_with_authenticated_ids(
        inner,
        tx.layer().unwrap().inner_signatures(),
        config.network,
        &env.context,
        &env.ids,
        config.signature_limits,
        &next.channels,
    )?;
    Ok(Frame::Payer {
        gas: BigInt::from(3_000) + size_gas(tx.wire_size()) - size_gas(inner.wire_size()),
        events: vec![Event::Primitive(crate::Event::FeeDebit {
            account: fee_payer,
            amount: payer.fee.clone(),
        })],
    })
}
fn finish_meta<A: NodeWriter + Clone, C: NodeWriter + Clone>(
    frame: Frame<'_, A, C>,
    result: Result<Receipt, Error<A, C>>,
    next: &mut Trees<A, C>,
    env: &mut Environment,
) -> Result<Receipt, Error<A, C>> {
    let Frame::Meta(frame) = frame else {
        unreachable!()
    };
    let MetaFrame {
        meta,
        inner_size,
        checkpoint,
        failure_env,
        origin,
        receipt,
    } = *frame;
    let mut gas = big(&receipt.gas);
    let mut events;
    let call;
    match result {
        Ok(inner) => {
            gas += big(&inner.gas);
            if env.context.protocol >= Protocol::Arcus {
                gas -= size_gas(inner_size);
            }
            events = inner.events;
            call = receipt.call;
        }
        Err(e) if failures::inner_failure(&e, env.context.protocol) => {
            *next = checkpoint;
            *env = failure_env;
            events = vec![];
            call = contracts::authentication::fail_inner(meta, next)
                .map_err(|e| Error::Contract(Box::new(e)))?;
        }
        Err(e) => return Err(e),
    }
    events.extend(receipt.events.into_iter().map(Event::Contract));
    env.reset(meta, origin);
    Ok(Receipt {
        transaction_hash: None,
        gas: unsigned(gas)?,
        call: Some(call),
        events,
        context: env.context.clone(),
    })
}

pub(super) fn process<A: NodeWriter + Clone, C: NodeWriter + Clone, O: MapOrder>(
    tx: &Transaction,
    trees: &mut Trees<A, C>,
    context: &Context,
    config: &Config<'_, O>,
    source: Option<&SignedTransaction<Transaction>>,
) -> Result<Receipt, Error<A, C>> {
    if tx.layers().len() > config.signature_limits.max_layers {
        return Err(SignatureError::ResourceLimit.into());
    }
    let mut budget = Budget {
        bytes: config.signature_limits.max_encoded_bytes,
    };
    let mut next = trees.clone();
    let mut env = Environment {
        context: context.clone(),
        ids: context.ga_authenticated.iter().copied().collect(),
    };
    let mut current = tx.view();
    let mut stack = Vec::new();
    let mut result = loop {
        let Some(layer) = current.layer() else {
            break process_leaf(current.leaf(), &mut next, &env.context, config, source);
        };
        match layer.wrapper() {
            Wrapper::PayingFor(_) => match payer(current, &mut next, &mut env, config) {
                Ok(frame) => stack.push(frame),
                Err(e) => break Err(e),
            },
            Wrapper::GaMeta(meta) => {
                if let Err(e) = check_wrapper(current, &env.context) {
                    break Err(e);
                }
                if let Err(e) = budget.authentication::<A, C>(current, &mut env, config.network) {
                    break Err(e);
                }
                let receipt = match authenticate(current, &mut next, &env.context, config) {
                    Ok(r) => r,
                    Err(e) => break Err(e),
                };
                let before = env.clone();
                let origin = env.context.origin;
                env.add(meta);
                let inner = current.inner().expect("meta inner");
                let verified = verify_with_authenticated_ids(
                    inner,
                    layer.inner_signatures(),
                    config.network,
                    &env.context,
                    &env.ids,
                    config.signature_limits,
                    &next.channels,
                );
                let failure_env = if verified.is_err() {
                    before
                } else {
                    env.clone()
                };
                let frame = Frame::Meta(Box::new(MetaFrame {
                    meta,
                    inner_size: inner.wire_size(),
                    checkpoint: next.clone(),
                    failure_env,
                    origin,
                    receipt,
                }));
                if let Err(e) = verified {
                    break finish_meta(frame, Err(e.into()), &mut next, &mut env);
                }
                stack.push(frame);
            }
        }
        current = current.inner().expect("wrapper inner");
    };
    while let Some(frame) = stack.pop() {
        result = match frame {
            Frame::Payer { gas, events } => result.and_then(|mut r| {
                r.gas = unsigned(big(&r.gas) + gas)?;
                r.events.extend(events);
                Ok(r)
            }),
            frame => finish_meta(frame, result, &mut next, &mut env),
        };
    }
    let result = result?;
    *trees = next;
    Ok(result)
}
