use super::*;
impl Context {
    fn decode(&self, bytes: &[u8]) -> Result<Signed, Error> {
        let view = self.db.view().map_err(|_| Error::Storage)?;
        let config = view.config().transaction_record();
        Signed::decode_with_state_config(bytes, config.transaction, config.state)
            .map_err(|_| Error::Protocol)
    }
    fn vote(
        &self,
        tx: Arc<Signed>,
        event: votes::Event,
        slot: mpsc::OwnedPermit<Arc<Signed>>,
    ) -> Result<votes::PushOutcome, Error> {
        // Publication capacity is reserved before mutation; abandoning an
        // observer cannot erase an accepted vote's outbound event.
        let view = self.db.view().map_err(|_| Error::Storage)?;
        let tip = view.status().tip.ok_or(Error::Storage)?;
        let header = view
            .require_block(tip)
            .map_err(|_| Error::Storage)?
            .header();
        let protocol = Protocol::try_from(header.common().version).map_err(|_| Error::Storage)?;
        let admission = votes::Admission {
            network: &self.network,
            protocol,
            consensus: self.consensus,
            limits: self.config.votes,
        };
        let mut pool = self.votes.lock().map_err(|_| Error::Storage)?;
        let result = pool
            .push(&tx, event, &admission, |hash| {
                self.admission.gossip_cache().check(hash)
            })
            .map_err(Error::Votes)?;
        if matches!(result, votes::PushOutcome::Accepted { .. }) {
            slot.send(tx);
        }
        Ok(result)
    }
}
pub(super) async fn incoming(
    context: Context,
    jobs: blocking::Pool,
    forwarded: pool::Forwarded,
) -> Result<Input, Error> {
    let pool::Forwarded { event, client, .. } = forwarded;
    let net::Event::Message(received) = event else {
        return Err(Error::Protocol);
    };
    let ctx = context.clone();
    let (received, transactions, malformed) = jobs
        .submit_wait(context.config.input_bytes, move |cancel| {
            if cancel.cancelled() {
                return Err(Error::Closed);
            }
            let Some(Value::List(values)) = received.message().fields().get("txs") else {
                return Err(Error::Protocol);
            };
            if values.len() > ctx.config.input_items {
                client.shutdown();
                return Err(Error::Busy);
            }
            let view = ctx.db.view().map_err(|_| Error::Storage)?;
            let record = view.config().transaction_record();
            let mut remaining = ctx.config.input_bytes;
            let mut transactions = Vec::with_capacity(values.len());
            let mut malformed = 0;
            for value in values {
                if cancel.cancelled() {
                    return Err(Error::Closed);
                }
                let Value::Binary(bytes) = value else {
                    malformed += 1;
                    continue;
                };
                let Some(next) = remaining.checked_sub(bytes.len()) else {
                    client.shutdown();
                    return Err(Error::Busy);
                };
                remaining = next;
                match Signed::decode_with_state_config(bytes, record.transaction, record.state) {
                    Ok(tx) => transactions.push((Arc::new(tx), bytes.len())),
                    Err(_) => malformed += 1,
                }
            }
            Ok((received, transactions, malformed))
        })
        .await?
        .await??;
    let mut counts = Input {
        decoded: transactions.len() as u64,
        malformed,
        ..Default::default()
    };
    // Retain the transport byte charge through admission, not just decoding.
    let held = received;
    for (tx, bytes) in transactions {
        if tx.transaction().view().kind() == TransactionKind::HcVote {
            let ctx = context.clone();
            let slot = ctx
                .published
                .clone()
                .reserve_owned()
                .await
                .map_err(|_| Error::Closed)?;
            let result = jobs
                .submit_wait(bytes.max(1), move |cancel| {
                    if cancel.cancelled() {
                        return Err(Error::Closed);
                    }
                    ctx.vote(tx, votes::Event::Received, slot)
                })
                .await?
                .await?;
            match result {
                Ok(votes::PushOutcome::Accepted { .. }) => counts.votes += 1,
                Ok(votes::PushOutcome::Cached) => (),
                Err(_) => counts.rejected += 1,
            }
        } else {
            let admission = context.admission.clone();
            let submitted = jobs
                .submit_wait(bytes.max(1), move |cancel| {
                    if cancel.cancelled() {
                        return Err(worker::Error::Disconnected);
                    }
                    admission.receive(tx)
                })
                .await?
                .await?;
            let outcome = match submitted {
                Ok(ticket) => ticket.await,
                Err(error) => Err(error),
            };
            match outcome {
                Ok(s)
                    if matches!(
                        s.outcome,
                        ae_mempool::pool::PushOutcome::Accepted { inserted: true, .. }
                    ) =>
                {
                    counts.admitted += 1
                }
                Ok(_) => (),
                Err(_) => counts.rejected += 1,
            }
        }
    }
    drop(held);
    Ok(counts)
}
pub(super) async fn command(
    context: Context,
    jobs: blocking::Pool,
    command: Command,
) -> Result<Input, Error> {
    let timeout = context.config.work_timeout;
    let mut counts = Input::default();
    match command {
        Command::Vote {
            bytes,
            event,
            reply,
            _permit: permit,
        } => {
            let result = bounded(timeout, async {
                let slot = context
                    .published
                    .clone()
                    .reserve_owned()
                    .await
                    .map_err(|_| Error::Closed)?;
                jobs.submit_wait(bytes.len().max(1), move |cancel| {
                    if cancel.cancelled() {
                        return Err(Error::Closed);
                    }
                    let tx = Arc::new(context.decode(&bytes)?);
                    context.vote(tx, event, slot)
                })
                .await?
                .await?
            })
            .await;
            if matches!(&result, Ok(votes::PushOutcome::Accepted { .. })) {
                counts.votes = 1;
            }
            let error = result.as_ref().err().cloned();
            let _ = reply.send(result);
            drop(permit);
            if let Some(error) = error {
                return Err(error);
            }
        }
        Command::Epoch {
            epoch,
            reply,
            _permit: permit,
        } => {
            let result = bounded(timeout, async {
                jobs.submit_wait(epoch.as_bytes().len().saturating_add(128), move |cancel| {
                    if cancel.cancelled() {
                        return Err(Error::Closed);
                    }
                    context
                        .votes
                        .lock()
                        .map_err(|_| Error::Storage)?
                        .update_epoch(epoch);
                    Ok(())
                })
                .await?
                .await?
            })
            .await;
            let error = result.as_ref().err().cloned();
            let _ = reply.send(result);
            drop(permit);
            if let Some(error) = error {
                return Err(error);
            }
        }
        Command::Peek {
            epoch,
            kind,
            maximum,
            reply,
            _permit: permit,
        } => {
            let weight = epoch
                .as_bytes()
                .len()
                .saturating_add(kind.as_ref().map_or(0, |k| k.as_bytes().len()))
                .saturating_add(128);
            let result = bounded(timeout, async {
                jobs.submit_wait(weight, move |cancel| {
                    if cancel.cancelled() {
                        return Err(Error::Closed);
                    }
                    let votes = context
                        .votes
                        .lock()
                        .map_err(|_| Error::Storage)?
                        .peek(&epoch, kind.as_ref())
                        .take(maximum)
                        .cloned()
                        .collect();
                    Ok(votes)
                })
                .await?
                .await?
            })
            .await;
            let error = result.as_ref().err().cloned();
            let _ = reply.send(result);
            drop(permit);
            if let Some(error) = error {
                return Err(error);
            }
        }
    }
    Ok(counts)
}
pub(super) async fn bounded<T>(
    duration: Duration,
    future: impl std::future::Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| Error::Timeout)?
}
pub(super) async fn feed(
    admission: worker::Client,
    jobs: blocking::Pool,
    cursor: worker::events::Cursor,
    limit: usize,
) -> Result<FeedResult, Error> {
    jobs.submit_wait(128, move |cancel| {
        loop {
            if cancel.cancelled() {
                return Err(Error::Closed);
            }
            let batch = admission
                .wait_for_events(&cursor, limit, Duration::from_millis(100))
                .map_err(Error::Feed)?;
            if batch.closed || !batch.events.is_empty() {
                return Ok(FeedResult::Batch(batch));
            }
        }
    })
    .await?
    .await?
}
pub(super) async fn recover(
    admission: worker::Client,
    jobs: blocking::Pool,
    maximum: usize,
) -> Result<FeedResult, Error> {
    let ticket = jobs
        .submit_wait(128, move |cancel| {
            if cancel.cancelled() {
                return Err(Error::Closed);
            }
            admission.gossip_snapshot(maximum).map_err(Error::Worker)
        })
        .await?
        .await??;
    ticket
        .await
        .map(FeedResult::Snapshot)
        .map_err(Error::Worker)
}
pub(super) async fn fanout(
    context: Context,
    jobs: blocking::Pool,
    peers: pool::Handle,
    tx: Arc<Signed>,
    policy: Policy,
    random: RandomState,
    round: u64,
) -> Result<Fanout, Error> {
    if policy.max_peers == 0 {
        return Ok(Fanout { sent: 0, failed: 0 });
    }
    let clients = peers
        .peers()
        .map_err(|_| Error::Closed)?
        .wait()
        .await
        .map_err(|_| Error::Closed)?;
    let tickets = jobs
        .submit_wait(context.config.input_bytes, move |cancel| {
            if cancel.cancelled() {
                return Err(Error::Closed);
            }
            let mut clients: Vec<_> = clients.into_iter().filter(|c| !c.is_closed()).collect();
            clients.sort_by_cached_key(|c| random.hash_one((round, c.peer().0)));
            clients.truncate(policy.max_peers);
            if clients.is_empty() {
                return Ok(vec![]);
            }
            let view = context.db.view().map_err(|_| Error::Storage)?;
            let mut config = view.config().transaction_record();
            config.max_record_bytes = config.max_record_bytes.min(context.config.input_bytes);
            // Storage encoding checks nested values and metadata before wire encode.
            ae_tx::storage::encode(&tx, config).map_err(|_| Error::Protocol)?;
            let message = Message::new(
                Kind::Transactions,
                1,
                BTreeMap::from([("txs".into(), Value::List(vec![Value::Binary(tx.encode())]))]),
                context.config.messages,
            )
            .map_err(|_| Error::Protocol)?;
            let prepared = net::Prepared::new(&message, context.config.messages)
                .map_err(|_| Error::Protocol)?;
            let mut tickets = Vec::with_capacity(clients.len());
            for client in clients {
                if cancel.cancelled() {
                    return Err(Error::Closed);
                }
                tickets.push(client.send_prepared(&prepared));
            }
            Ok(tickets)
        })
        .await?
        .await??;
    let mut result = Fanout { sent: 0, failed: 0 };
    for ticket in tickets {
        match ticket {
            Ok(ticket) => match ticket.await {
                Ok(net::Reply::Sent) => result.sent += 1,
                _ => result.failed += 1,
            },
            Err(_) => result.failed += 1,
        }
    }
    Ok(result)
}
