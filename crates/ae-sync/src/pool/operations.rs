use super::*;

fn failure(_: impl fmt::Debug) -> session::Failure {
    session::Failure::Action
}
pub(super) async fn tree(db: Arc<Database>, jobs: blocking::Pool, config: Config) -> Outcome {
    let result = match jobs.submit(128, move |_| {
        db.pool_view()
            .map_err(failure)?
            .sync_snapshot(config.session.exchange)
            .map(|s| session::Reply::Tree(s.tree))
            .map_err(failure)
    }) {
        Ok(pending) => pending.await.map_err(failure).and_then(|r| r),
        Err(_) => Err(session::Failure::ResourceLimit),
    };
    Outcome::reply(result)
}

enum Decoded {
    Reply(session::Reply),
    Transactions(Vec<(Arc<Signed>, usize)>),
}
pub(super) async fn action(
    action: session::Action,
    client: net::Client,
    db: Arc<Database>,
    admission: worker::Client,
    jobs: blocking::Pool,
    config: Config,
) -> Outcome {
    let finish = matches!(*action.work, session::Work::Finish(_));
    let result = perform(action, client, db, admission, jobs, config).await;
    // The native finish worker ignores the connection result and returns the
    // originally requested flag through Manager's pending Work::Finish.
    if finish {
        Outcome::reply(Ok(session::Reply::Finished))
    } else {
        result.unwrap_or_else(|e| Outcome::reply(Err(e)))
    }
}
async fn perform(
    action: session::Action,
    client: net::Client,
    db: Arc<Database>,
    admission: worker::Client,
    jobs: blocking::Pool,
    config: Config,
) -> Result<Outcome, session::Failure> {
    let work = action.work;
    let weight = match &*work {
        session::Work::Unfold { .. } => config.session.exchange.wire_bytes,
        session::Work::Get(_) => config.session.get_bytes,
        _ => 128,
    };
    let connection = client.clone();
    let prepared = jobs
        .submit(weight, move |cancel| {
            if cancel.cancelled() {
                return Err(session::Failure::Action);
            }
            let request = match &*work {
                session::Work::Init => wire::Request::Init,
                session::Work::Unfold { request, .. } => wire::Request::Unfold(
                    protocol::encode_batch(request, config.session.exchange)
                        .map_err(session::Failure::Protocol)?,
                ),
                session::Work::Get(keys) => {
                    wire::Request::Get(session::encode_get(keys, config.session)?)
                }
                session::Work::Finish(done) => wire::Request::Finish(*done),
                session::Work::Tree => return Err(session::Failure::InvalidReply),
            };
            let message = request
                .clone()
                .into_message(config.messages)
                .map_err(failure)?;
            if cancel.cancelled() {
                return Err(session::Failure::Action);
            }
            let ticket = connection.request(&message, None).map_err(failure)?;
            Ok((request, ticket))
        })
        .map_err(|_| session::Failure::ResourceLimit)?;
    let (request, ticket) = prepared.await.map_err(failure)??;
    let reply = ticket.await.map_err(failure)?;
    if matches!(request, wire::Request::Finish(_)) {
        return Ok(Outcome::reply(Ok(session::Reply::Finished)));
    }
    let net::Reply::Response(received) = reply else {
        return Err(session::Failure::InvalidReply);
    };
    let connection = client.clone();
    let decoded = jobs
        .submit(weight, move |cancel| {
            if cancel.cancelled() {
                return Err(session::Failure::Action);
            }
            let response = request.response(received.message());
            let response = match response {
                Err(wire::ResponseError::Peer(reason))
                    if matches!(request, wire::Request::Unfold(_))
                        && reason == b"tree_not_ready" =>
                {
                    return Ok(Decoded::Reply(session::Reply::TreeNotReady));
                }
                other => other.map_err(failure)?,
            };
            match response {
                wire::Reply::Init => Ok(Decoded::Reply(session::Reply::Init)),
                wire::Reply::Unfold(values) => Ok(Decoded::Reply(session::Reply::Unfold(
                    protocol::decode_batch(&values, config.session.exchange)
                        .map_err(session::Failure::Protocol)?,
                ))),
                wire::Reply::Transactions(values) => {
                    if values.len() > config.session.get_items {
                        return Err(session::Failure::ResourceLimit);
                    }
                    let read = db.view().map_err(failure)?.config().clone();
                    let decode = read.transaction_record();
                    let mut remaining = config.session.get_bytes;
                    let mut transactions = Vec::with_capacity(values.len());
                    for value in values {
                        remaining = remaining
                            .checked_sub(value.len())
                            .ok_or(session::Failure::ResourceLimit)?;
                        if cancel.cancelled() {
                            return Err(session::Failure::Action);
                        }
                        let transaction = match Signed::decode_with_state_config(
                            &value,
                            decode.transaction,
                            decode.state,
                        ) {
                            Ok(transaction) => transaction,
                            Err(_) => {
                                connection.shutdown();
                                return Err(session::Failure::InvalidReply);
                            }
                        };
                        transactions.push((Arc::new(transaction), value.len()));
                    }
                    Ok(Decoded::Transactions(transactions))
                }
                wire::Reply::Finished(_) => Err(session::Failure::InvalidReply),
            }
        })
        .map_err(|_| session::Failure::ResourceLimit)?
        .await
        .map_err(failure)??;
    match decoded {
        Decoded::Reply(reply) => Ok(Outcome::reply(Ok(reply))),
        Decoded::Transactions(transactions) => {
            let count = transactions.len();
            let mut result = Outcome {
                reply: Ok(session::Reply::Fetched(count)),
                fetched: count,
                admitted: 0,
                rejected: 0,
            };
            // Every envelope was decoded before the first actual admission.
            // Each accepted durable ticket survives cancellation of this observer.
            for (transaction, bytes) in transactions {
                let admission = admission.clone();
                let submitted = jobs
                    .submit(bytes.saturating_add(128), move |cancel| {
                        if cancel.cancelled() {
                            return Err(worker::Error::Disconnected);
                        }
                        admission.receive(transaction)
                    })
                    .map_err(|_| session::Failure::ResourceLimit)?
                    .await
                    .map_err(failure)?;
                let admitted = match submitted {
                    Ok(ticket) => ticket.await,
                    Err(error) => Err(error),
                };
                match admitted {
                    Ok(submission)
                        if matches!(
                            submission.outcome,
                            ae_mempool::pool::PushOutcome::Accepted { inserted: true, .. }
                        ) =>
                    {
                        result.admitted += 1
                    }
                    Ok(_) => (),
                    Err(_) => result.rejected += 1,
                }
            }
            Ok(result)
        }
    }
}

pub(super) fn reason(error: session::RequestError) -> &'static [u8] {
    match error {
        session::RequestError::TooManySyncs => b"too_many_simultaneous_sync_attempts",
        session::RequestError::AlreadySyncing => b"already_syncing_with_peer",
        session::RequestError::NoActiveSync => b"no_active_sync",
        session::RequestError::TreeNotReady => b"tree_not_ready",
        session::RequestError::Protocol(_) => b"bad_request",
    }
}
pub(super) async fn send(ticket: Result<net::Ticket, net::Error>) -> Result<(), Error> {
    match ticket.map_err(|_| Error::Closed)?.await {
        Ok(net::Reply::Sent) => Ok(()),
        _ => Err(Error::Closed),
    }
}
pub(super) async fn serve(
    received: net::Received,
    tree: Option<Arc<protocol::Snapshot>>,
    db: Arc<Database>,
    client: net::Client,
    jobs: blocking::Pool,
    config: Config,
) -> Result<(), Error> {
    let kind = received.message().kind();
    let response_kind = if kind == Kind::PoolGet {
        Kind::Transactions
    } else {
        Kind::PoolUnfold
    };
    let weight = if kind == Kind::PoolGet {
        config.session.get_bytes
    } else {
        config.session.exchange.wire_bytes
    };
    let ticket = jobs
        .submit(weight, move |cancel| {
            if cancel.cancelled() {
                return Err(Error::Closed);
            }
            let request =
                wire::Request::from_message(received.message()).map_err(|_| Error::Protocol)?;
            let result: Result<wire::Reply, &'static [u8]> = match request {
                wire::Request::Get(hashes) => {
                    let limits = ae_storage::pool::sync::GetLimits {
                        requests: config.session.get_items,
                        wire_bytes: config.session.get_bytes,
                    };
                    let stored = db.pool_view().map_err(|_| Error::Storage)?;
                    stored
                        .sync_get(&hashes, limits)
                        .map(|transactions| {
                            wire::Reply::Transactions(
                                transactions.iter().map(|tx| tx.encode()).collect(),
                            )
                        })
                        .map_err(|_| b"storage_error".as_slice())
                }
                wire::Request::Unfold(request) => tree
                    .ok_or(Error::Protocol)?
                    .respond_wire(&request, config.session.exchange)
                    .map(wire::Reply::Unfold)
                    .map_err(|_| b"bad_request".as_slice()),
                _ => return Err(Error::Protocol),
            };
            let response = match result {
                Ok(reply) => reply.into_response(config.messages),
                Err(reason) => {
                    Message::response_error(response_kind, reason.to_vec(), config.messages)
                }
            }
            .map_err(|_| Error::Protocol)?;
            if cancel.cancelled() {
                return Err(Error::Closed);
            }
            client.send(&response).map_err(|_| Error::Closed)
        })?
        .await??;
    send(Ok(ticket)).await
}
