use super::*;
use ae_p2p::{
    message::{Details, Kind, Message, Peer},
    requests::PingAction,
};
use std::future::Future;

async fn bounded<T>(
    timeout: Duration,
    future: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| Error::Timeout)?
}
type Response = (Option<Box<network::Ping>>, Result<net::Reply, net::Error>);
struct Session {
    ctx: Context,
    jobs: blocking::Pool,
    client: net::Client,
    requests: Tasks<u16, Response>,
}
pub(super) async fn run(
    ctx: Context,
    jobs: blocking::Pool,
    client: net::Client,
    mut input: mpsc::Receiver<Input>,
) -> Result<(), Error> {
    let monitor = client.monitor();
    let mut session = Session {
        ctx,
        jobs,
        client,
        requests: Tasks::new(),
    };
    tokio::select! {
        _ = monitor.wait() => Ok(()),
        result = async {
            loop {
                tokio::select! {
                    input = input.recv() => match input {
                        Some(input) => bounded(session.ctx.config.operation_timeout, session.input(input)).await?,
                        None => return Ok(()),
                    },
                    (version, (scheduled, result)) = session.requests.next() => {
                        bounded(session.ctx.config.operation_timeout, session.response(version, scheduled, result)).await?;
                    },
                }
            }
        } => result,
    }
}
impl Session {
    async fn current(&mut self) -> Result<network::Connection, Error> {
        self.ctx
            .network
            .connection(self.client.clone())?
            .wait()
            .await?
            .ok_or(Error::Stale)
    }
    async fn local(&mut self) -> Result<ping::Local, Error> {
        let connection = self.current().await?;
        let db = self.ctx.db.clone();
        let temporary = connection.kind == policy::Kind::Temporary;
        self.jobs
            .submit_wait(256, move |_| {
                snapshot::local(
                    &db.view().map_err(|e| Error::Storage(e.to_string()))?,
                    temporary,
                )
                .map_err(|e| Error::Storage(e.to_string()))
            })
            .await?
            .await?
    }
    async fn ping(
        &mut self,
        version: u16,
        response_to: Option<&Message>,
    ) -> Result<Message, Error> {
        let local = self.local().await?;
        let share = match response_to {
            Some(m) => ping::Remote::from_message(m)?.share.clone(),
            None => self.ctx.config.ping.share.into(),
        };
        let excluded = ping::exclusions(&self.client.peer().0, response_to)?
            .into_iter()
            .filter_map(|b| b.as_slice().try_into().ok().map(PeerPublicKey))
            .collect();
        // Oversized initial-v2 shares are not a protocol rejection. The wire
        // policy accepts them; this adapter may fail its bounded sampler budget.
        let maximum = share
            .to_u64()
            .ok()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or(Error::Busy)?;
        let peers = if maximum == 0 {
            vec![]
        } else {
            match self
                .ctx
                .network
                .command(policy::Command::Sample {
                    maximum,
                    percent_connected: Some(self.ctx.config.ping.connected_percent),
                    exclude: excluded,
                })?
                .wait()
                .await?
            {
                policy::Outcome::Peers(peers) => peers
                    .into_iter()
                    .map(|p| Peer {
                        host: p.host,
                        port: u64::from(p.port).into(),
                        pubkey: p.id.0.to_vec(),
                    })
                    .collect(),
                _ => return Err(Error::Stale),
            }
        };
        Ok(ping::encode(
            &local,
            peers,
            &self.ctx.config.capabilities,
            ping::Encoding {
                version,
                minimum_version: self.client.minimum_ping_version(),
                port: self.ctx.config.port,
                share,
                limits: self.ctx.config.messages,
            },
        )?)
    }
    async fn apply(
        &mut self,
        message: &Message,
        previous: Option<u16>,
    ) -> Result<Option<Result<(), ping::Rejection>>, Error> {
        let local = self.local().await?;
        let target = *self.ctx.target.borrow();
        let decision = ping::evaluate(&local, message, previous, target, self.ctx.config.ping)?;
        for effect in decision.effects {
            self.current().await?;
            match effect {
                ping::Effect::Peers(peers) => {
                    // Native workers independently drop unusable addresses.
                    // Raw peer bytes remain represented by the protocol layer.
                    let peers = peers
                        .into_iter()
                        .filter_map(|p| {
                            Some(policy::Info {
                                id: PeerPublicKey(p.pubkey.as_slice().try_into().ok()?),
                                port: p.port.to_u64().ok()?.try_into().ok()?,
                                host: p.host,
                            })
                        })
                        .collect();
                    if self
                        .ctx
                        .network
                        .learn(self.client.clone(), peers)?
                        .wait()
                        .await?
                        == policy::Outcome::Stale
                    {
                        return Err(Error::Stale);
                    }
                }
                ping::Effect::Pool => {
                    if self
                        .ctx
                        .network
                        .synchronize(self.client.clone())?
                        .wait()
                        .await?
                        == policy::Outcome::Stale
                    {
                        return Err(Error::Stale);
                    }
                }
                ping::Effect::Status {
                    temporary,
                    genesis,
                    best,
                    difficulty,
                } => {
                    let weight = genesis
                        .len()
                        .saturating_add(best.len())
                        .saturating_add(difficulty.as_bytes().len());
                    self.ctx.published.send(
                        EventKind::Status {
                            client: self.client.clone(),
                            temporary,
                            genesis,
                            best,
                            difficulty,
                        },
                        weight,
                    )?;
                }
                ping::Effect::Capabilities(caps) => {
                    // The original map has one key, chain_poi; last duplicate wins.
                    if let Some(chain_poi) = caps.into_iter().last() {
                        let weight = chain_poi
                            .height
                            .as_bytes()
                            .len()
                            .saturating_add(chain_poi.root_hash.len())
                            .saturating_add(chain_poi.genesis.len())
                            .saturating_add(chain_poi.top.len())
                            .saturating_add(chain_poi.poi.len());
                        self.ctx.published.send(
                            EventKind::Capabilities {
                                client: self.client.clone(),
                                chain_poi,
                            },
                            weight,
                        )?;
                    }
                }
                effect => {
                    let (intent, weight) = match effect {
                        ping::Effect::Generation(hash) => {
                            let n = hash.len();
                            (ChainIntent::Generation(hash), n)
                        }
                        ping::Effect::SyncDone => (ChainIntent::Done, 0),
                        ping::Effect::Sync { best, difficulty } => {
                            let n = best.len().saturating_add(difficulty.as_bytes().len());
                            (ChainIntent::Start { best, difficulty }, n)
                        }
                        _ => unreachable!(),
                    };
                    self.ctx.published.send(
                        EventKind::Chain {
                            client: self.client.clone(),
                            intent,
                        },
                        weight,
                    )?;
                }
            }
        }
        if decision.response.is_none() {
            return Err(Error::MalformedCapabilities);
        }
        Ok(decision.response)
    }
    async fn input(&mut self, input: Input) -> Result<(), Error> {
        match input {
            Input::Ping(ping) => {
                if self.current().await?.token != ping.token {
                    return Err(Error::Stale);
                }
                let version = self.client.ping_version();
                let message = self.ping(version, None).await?;
                let ticket = self.client.request(&message, None)?;
                self.ctx
                    .progress
                    .send_modify(|s| s.outgoing_pings = s.outgoing_pings.saturating_add(1));
                self.requests
                    .insert(version, async move { (Some(ping), ticket.await) });
            }
            Input::Message(received) => {
                self.current().await?;
                if received.message().kind() == Kind::Ping {
                    self.ctx
                        .progress
                        .send_modify(|s| s.incoming_pings = s.incoming_pings.saturating_add(1));
                    let observation = received.ping.ok_or(Error::Stale)?;
                    let response = if observation.busy {
                        Message::response_error(
                            Kind::Ping,
                            b"already_pinging".to_vec(),
                            self.ctx.config.messages,
                        )?
                    } else {
                        match self.apply(received.message(), observation.previous).await? {
                            Some(Ok(())) => Message::response_ok(
                                &self
                                    .ping(received.message().version(), Some(received.message()))
                                    .await?,
                                self.ctx.config.messages,
                            )?,
                            Some(Err(reason)) => {
                                self.ctx
                                    .progress
                                    .send_modify(|s| s.rejected = s.rejected.saturating_add(1));
                                Message::response_error(
                                    Kind::Ping,
                                    reason.reason().to_vec(),
                                    self.ctx.config.messages,
                                )?
                            }
                            None => unreachable!(),
                        }
                    };
                    self.client.send(&response)?.await?;
                    if !observation.busy {
                        self.ctx
                            .network
                            .ping_received(self.client.clone())?
                            .wait()
                            .await?;
                    }
                } else {
                    self.serve(received).await?;
                }
            }
        }
        Ok(())
    }
    async fn response(
        &mut self,
        _version: u16,
        scheduled: Option<Box<network::Ping>>,
        result: Result<net::Reply, net::Error>,
    ) -> Result<(), Error> {
        let received = match result {
            Ok(net::Reply::Response(received)) => received,
            Ok(net::Reply::Sent) => return Err(Error::Stale),
            Err(error) => {
                if let Some(ping) = scheduled {
                    ping.complete(false);
                }
                // A v2 timeout can fall back to v1 without closing the driver.
                if self.client.is_closed() {
                    return Err(error.into());
                }
                return Ok(());
            }
        };
        let observation = received.ping.ok_or(Error::Stale)?;
        let result = match received.message().details() {
            Details::Response {
                message: Some(Ok(ping)),
                ..
            } => self.apply(ping, observation.previous).await? == Some(Ok(())),
            Details::Response { message: None, .. } => false,
            _ => return Err(Error::Stale),
        };
        if let Some(ping) = scheduled {
            ping.complete(result);
        }
        if observation.action == PingAction::ProbeV2
            && self.client.selected_ping_version().is_none()
        {
            let message = self.ping(2, None).await?;
            let ticket = self.client.probe(&message)?;
            self.ctx
                .progress
                .send_modify(|s| s.probes = s.probes.saturating_add(1));
            self.requests.insert(2, async move { (None, ticket.await) });
        }
        Ok(())
    }
    async fn serve(&mut self, received: net::Received) -> Result<(), Error> {
        if received.message().kind() == Kind::GetNodeInfo {
            let Some(metadata) = &self.ctx.config.node_info else {
                return Ok(());
            };
            let counts = self
                .ctx
                .network
                .pool_counts(self.client.clone())?
                .wait()
                .await?
                .ok_or(Error::Stale)?;
            let response = info::response(
                Some(metadata),
                self.ctx.network.network_id(),
                &(counts.verified as u64).into(),
                &(counts.unverified as u64).into(),
                self.ctx.config.messages,
            )?
            .ok_or(Error::Stale)?;
            self.current().await?;
            self.client.send(&response)?.await?;
            self.ctx
                .progress
                .send_modify(|s| s.served = s.served.saturating_add(1));
            return Ok(());
        }
        if !matches!(
            received.message().kind(),
            Kind::GetHeaderByHash
                | Kind::GetHeaderByHeight
                | Kind::GetBlockTransactions
                | Kind::GetSuccessors
                | Kind::GetGeneration
        ) {
            return self.ctx.published.send(
                EventKind::Unhandled {
                    client: self.client.clone(),
                    message: received,
                },
                self.ctx
                    .config
                    .messages
                    .work_bytes
                    .saturating_add(self.ctx.config.messages.body_bytes),
            );
        }
        let db = self.ctx.db.clone();
        let limits = self.ctx.config.messages;
        let weight = limits
            .body_bytes
            .checked_add(limits.work_bytes)
            .ok_or(Error::Busy)?;
        let amount = u32::try_from(weight).map_err(|_| Error::Busy)?;
        let permit = self
            .ctx
            .storage_bytes
            .clone()
            .acquire_many_owned(amount)
            .await
            .map_err(|_| Error::Closed)?;
        // Keep the original Received permit alive through the blocking job.
        let (_permit, response) = self
            .jobs
            .submit_wait(weight, move |_| {
                let stored = db.pool_view().map_err(|e| Error::Storage(e.to_string()))?;
                snapshot::serve(&stored, received.message(), limits)
                    .map(|response| (permit, response))
                    .map_err(|e| Error::Storage(e.to_string()))
            })
            .await?
            .await??;
        if let Some(response) = response {
            self.current().await?;
            self.client.send(&response)?.await?;
            self.ctx
                .progress
                .send_modify(|s| s.served = s.served.saturating_add(1));
        }
        Ok(())
    }
}
