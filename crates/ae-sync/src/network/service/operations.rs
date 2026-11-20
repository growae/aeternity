use super::*;

impl Service {
    pub(super) async fn apply(
        &mut self,
        command: policy::Command,
    ) -> Result<policy::Outcome, Error> {
        let now = self.now()?;
        for attempt in 0..2 {
            let ticket = self.client.apply(now, command.clone())?;
            match self.wait(ticket).await {
                Ok(outcome) => return Ok(outcome),
                Err(registry::worker::Error::Service(registry::Error::Storage(_))) => {
                    let ticket = self.client.recover()?;
                    let recovered = self.wait(ticket).await;
                    self.current.counts.storage_recoveries =
                        self.current.counts.storage_recoveries.saturating_add(1);
                    match recovered {
                        Ok(registry::Recovery::Committed(outcome)) => return Ok(outcome),
                        Ok(registry::Recovery::NotCommitted | registry::Recovery::Unchanged)
                            if attempt == 0 => {}
                        Ok(_) => {
                            self.fail(Error::Storage);
                            return Err(Error::Storage);
                        }
                        Err(error) => {
                            let error = Error::from(error);
                            self.fail(error.clone());
                            return Err(error);
                        }
                    }
                }
                Err(registry::worker::Error::Service(registry::Error::EffectsFull)) => {
                    return Err(Error::Busy);
                }
                Err(error @ registry::worker::Error::Service(registry::Error::Policy(_))) => {
                    return Err(error.into());
                }
                Err(error) => {
                    let error = Error::from(error);
                    self.fail(error.clone());
                    return Err(error);
                }
            }
        }
        unreachable!("bounded recovery")
    }
    async fn acknowledge(&mut self, id: registry::DeliveryId) -> Result<(), Error> {
        let ticket = self.client.acknowledge(id)?;
        if !self.wait(ticket).await? {
            return Err(Error::Registry("lost delivery ownership".into()));
        }
        self.refresh().await
    }
    fn resolve(
        &mut self,
        token: policy::Token,
        host: Vec<u8>,
        purpose: Resolution,
    ) -> Result<bool, Error> {
        if self.pending.len() >= self.config.completions {
            return Ok(false);
        }
        if self.resolutions.contains_key(&token) || self.ready_dials.contains_key(&token) {
            return Err(Error::Registry("duplicate resolver ownership".into()));
        }
        match self.resolver.submit(token, host) {
            Ok(pending) => {
                self.resolutions.insert(token, purpose);
                self.dns.insert(token, pending);
                Ok(true)
            }
            Err(resolver::Error::Busy) => Ok(false),
            Err(resolver::Error::Stopped) => Err(Error::Closed),
            Err(error) => {
                self.resolutions.insert(token, purpose);
                self.pending
                    .push_back(Completion::Dns(resolver::Completion {
                        token,
                        result: Err(error),
                    }));
                Ok(true)
            }
        }
    }
    pub(super) async fn effect(&mut self) -> Result<bool, Error> {
        if self.stopping {
            return Ok(false);
        }
        let Some(delivery) = self.frame.as_ref().and_then(|f| f.delivery.clone()) else {
            return Ok(false);
        };
        let accepted = match delivery.effect {
            policy::Effect::Resolve { token, host } => {
                self.resolve(token, host, Resolution::Discovery)?
            }
            policy::Effect::Dial { token, peer, probe } => self.resolve(
                token,
                peer.host.clone(),
                Resolution::Dial {
                    peer,
                    probe: probe.is_some(),
                },
            )?,
            policy::Effect::CancelResolve(token) => {
                self.resolutions.remove(&token);
                self.dns.remove(&token);
                true
            }
            policy::Effect::Close(token) => {
                self.resolutions.remove(&token);
                self.dns.remove(&token);
                self.ready_dials.remove(&token);
                self.ping_jobs.remove(&token);
                self.ping_tokens.remove(&token);
                self.sockets.cancel(token);
                true
            }
            policy::Effect::Ping { token, peer } => {
                if let Some(client) = self
                    .admitted
                    .get(&token)
                    .map(|a| &a.client)
                    .filter(|c| c.peer() == peer && !c.is_closed())
                    .cloned()
                {
                    if self.ping_tokens.contains_key(&token) {
                        true
                    } else if let Ok(permit) = self.events.clone().try_reserve_owned() {
                        let deadline = Instant::now() + self.config.ping_timeout;
                        let (reply, response) = oneshot::channel();
                        self.ping_tokens.insert(token, client.clone());
                        self.ping_jobs.insert(token, async move {
                            tokio::time::timeout_at(deadline, response)
                                .await
                                .ok()
                                .and_then(Result::ok)
                        });
                        permit.send(Event::Ping(Box::new(Ping {
                            token,
                            client,
                            deadline,
                            reply,
                        })));
                        true
                    } else {
                        false
                    }
                } else {
                    true
                }
            }
            effect @ (policy::Effect::Blocked(_)
            | policy::Effect::Unblocked(_)
            | policy::Effect::ProbeStatus { .. }) => {
                if let Ok(permit) = self.events.clone().try_reserve_owned() {
                    permit.send(Event::Notice(effect));
                    true
                } else {
                    false
                }
            }
            policy::Effect::Store(_) => {
                return Err(Error::Registry("uncommitted store effect".into()));
            }
        };
        if accepted {
            // wait() may collect completions, but cannot apply them until this
            // accepted effect is acknowledged. It is never submitted twice.
            self.acknowledge(delivery.id).await?;
        } else {
            self.effect_retry = Instant::now() + self.config.retry_interval;
        }
        Ok(accepted)
    }
    pub(super) async fn dial_one(&mut self) -> Result<bool, Error> {
        if self.stopping || self.ready_dials.is_empty() {
            return Ok(false);
        }
        self.dial_retry = Instant::now() + self.config.retry_interval;
        if self.sockets.occupied() >= self.config.transport.connections {
            return Ok(false);
        }
        let (&token, (peer, probe)) = self.ready_dials.first_key_value().unwrap();
        let peer = peer.clone();
        let probe = *probe;
        let ticket = self.client.connection(peer.id)?;
        let current = self.wait(ticket).await?;
        self.ready_dials.remove(&token);
        if self.stopping {
            return Ok(false);
        }
        if current.is_some_and(|c| {
            c.token == token && c.status == policy::Status::Connecting && c.probe.is_some() == probe
        }) {
            // Only the endpoint used by this attempt changes. Registry pool
            // grouping/source metadata stays at its independently stored value.
            self.sockets.dial(token, peer, probe)?;
        }
        Ok(true)
    }
    pub(super) async fn completed(&mut self, completion: Completion) -> Result<(), Error> {
        match completion {
            Completion::Dns(result) => {
                let Some(purpose) = self.resolutions.remove(&result.token) else {
                    return Ok(());
                };
                match purpose {
                    Resolution::Discovery => {
                        self.apply(policy::Command::Resolved {
                            token: result.token,
                            address: result.result.ok(),
                        })
                        .await?;
                        self.refresh().await?;
                    }
                    Resolution::Dial { mut peer, probe } => match result.result {
                        Ok(address) => {
                            peer.address = address;
                            self.ready_dials.insert(result.token, (peer, probe));
                            self.dial_retry = Instant::now();
                        }
                        Err(_) => {
                            if probe {
                                self.current.counts.probe_dead =
                                    self.current.counts.probe_dead.saturating_add(1);
                            }
                            self.apply(policy::Command::Failed {
                                peer: peer.id,
                                token: result.token,
                                reason: if probe {
                                    policy::Failure::Dead
                                } else {
                                    policy::Failure::Failed
                                },
                            })
                            .await?;
                            self.refresh().await?;
                        }
                    },
                }
            }
            Completion::Socket(event) => self.socket_event(event).await?,
            Completion::Ping { token, outcome } => {
                let Some(client) = self.ping_tokens.remove(&token) else {
                    return Ok(());
                };
                let command = if let Some(success) = outcome {
                    let count = if success {
                        &mut self.current.counts.pings_completed
                    } else {
                        &mut self.current.counts.pings_failed
                    };
                    *count = count.saturating_add(1);
                    policy::Command::PingDone {
                        peer: client.peer(),
                        token,
                    }
                } else {
                    self.sockets.cancel(token);
                    self.current.counts.pings_failed =
                        self.current.counts.pings_failed.saturating_add(1);
                    policy::Command::Failed {
                        peer: client.peer(),
                        token,
                        reason: policy::Failure::Down,
                    }
                };
                self.apply(command).await?;
                self.refresh().await?;
            }
        }
        Ok(())
    }
    async fn socket_event(&mut self, event: transport::Event) -> Result<(), Error> {
        match event {
            transport::Event::Ready(ready) => self.ready(*ready).await?,
            transport::Event::Probe {
                token,
                peer,
                result,
            } => {
                let command = if result.is_ok() {
                    self.current.counts.probe_alive =
                        self.current.counts.probe_alive.saturating_add(1);
                    policy::Command::Alive { peer, token }
                } else {
                    self.current.counts.probe_dead =
                        self.current.counts.probe_dead.saturating_add(1);
                    policy::Command::Failed {
                        peer,
                        token,
                        reason: policy::Failure::Dead,
                    }
                };
                self.apply(command).await?;
                self.refresh().await?;
            }
            transport::Event::Failed { token, peer, .. } => {
                let command = if let Some(peer) = peer {
                    policy::Command::Failed {
                        peer,
                        token,
                        reason: policy::Failure::Failed,
                    }
                } else {
                    self.current.counts.rejected = self.current.counts.rejected.saturating_add(1);
                    policy::Command::AbandonInbound(token)
                };
                self.apply(command).await?;
                self.refresh().await?;
            }
            transport::Event::Closed { token, peer, exit } => {
                self.admitted.remove(&token);
                self.ping_tokens.remove(&token);
                self.ping_jobs.remove(&token);
                self.current.counts.closed = self.current.counts.closed.saturating_add(1);
                self.apply(policy::Command::Failed {
                    peer,
                    token,
                    reason: if matches!(exit.reason, net::Error::RemoteClose | net::Error::Shutdown)
                    {
                        policy::Failure::Closed
                    } else {
                        policy::Failure::Down
                    },
                })
                .await?;
                self.refresh().await?;
            }
        }
        Ok(())
    }
    async fn ready(&mut self, ready: transport::Ready) -> Result<(), Error> {
        let transport::Ready {
            token,
            peer,
            inbound,
            connection,
            address,
        } = ready;
        let client = connection.client().clone();
        if self.stopping || client.is_closed() {
            drop(connection);
            let command = if inbound {
                policy::Command::AbandonInbound(token)
            } else {
                policy::Command::Failed {
                    peer: peer.id,
                    token,
                    reason: policy::Failure::Start,
                }
            };
            self.apply(command).await?;
            self.refresh().await?;
            return Ok(());
        }
        // The owner holds the actual driver while durable admission publishes.
        // Only then may pool/gossip/application handlers see its messages.
        let command = if inbound {
            policy::Command::Accept {
                token,
                peer: peer.clone(),
            }
        } else {
            policy::Command::Connected {
                peer: peer.id,
                token,
            }
        };
        let outcome = self.apply(command).await?;
        if !matches!(
            outcome,
            policy::Outcome::Accepted(_) | policy::Outcome::Done
        ) || self.stopping
        {
            self.current.counts.rejected = self.current.counts.rejected.saturating_add(1);
            drop(connection);
            self.refresh().await?;
            return Ok(());
        }
        let attached = match self.pool.attach(connection, false) {
            Ok(ticket) => self.wait(ticket.wait()).await,
            Err(error) => Err(error),
        };
        if attached.is_ok() && !self.stopping {
            let kind = match outcome {
                policy::Outcome::Accepted(kind) => kind,
                _ => policy::Kind::Permanent,
            };
            self.admitted.insert(
                token,
                Admitted {
                    client,
                    connection: Connection {
                        token,
                        peer: peer.id,
                        address,
                        kind,
                    },
                },
            );
            let count = if inbound {
                &mut self.current.counts.accepted
            } else {
                &mut self.current.counts.dialed
            };
            *count = count.saturating_add(1);
        } else {
            self.sockets.cancel(token);
            self.apply(policy::Command::Failed {
                peer: peer.id,
                token,
                reason: policy::Failure::Closed,
            })
            .await?;
        }
        self.refresh().await
    }
    pub(super) async fn inbound(
        &mut self,
        socket: tokio::net::TcpStream,
        address: std::net::SocketAddr,
    ) -> Result<(), Error> {
        let std::net::SocketAddr::V4(address) = address else {
            self.current.counts.rejected = self.current.counts.rejected.saturating_add(1);
            return Ok(());
        };
        let result = self
            .apply(policy::Command::ReserveInbound {
                address: *address.ip(),
            })
            .await;
        let token = match result {
            Ok(policy::Outcome::Reserved(token)) => token,
            Err(Error::Registry(_)) if !self.stopping => {
                self.current.counts.rejected = self.current.counts.rejected.saturating_add(1);
                return Ok(());
            }
            Err(error) => return Err(error),
            Ok(_) => return Err(Error::Registry("unexpected inbound reservation".into())),
        };
        if self.sockets.inbound(token, socket).is_err() {
            self.current.counts.rejected = self.current.counts.rejected.saturating_add(1);
            self.apply(policy::Command::AbandonInbound(token)).await?;
        }
        self.refresh().await
    }
    async fn token(&mut self, client: &net::Client) -> Result<Option<policy::Token>, Error> {
        let token = self.admitted.iter().find_map(|(token, owned)| {
            (owned.client.peer() == client.peer()
                && owned.client.generation() == client.generation())
            .then_some(*token)
        });
        let Some(token) = token else { return Ok(None) };
        let ticket = self.client.connection(client.peer())?;
        let current = self.wait(ticket).await?;
        // A retired driver may still await its terminal event after replacement.
        // Never turn its late application callback into work for the new peer.
        Ok(current
            .filter(|c| c.token == token && c.status == policy::Status::Connected)
            .map(|_| token))
    }
    async fn learn(
        &mut self,
        client: &net::Client,
        peers: Vec<policy::Info>,
    ) -> Result<policy::Outcome, Error> {
        let Some(token) = self.token(client).await? else {
            return Ok(policy::Outcome::Stale);
        };
        let source = self.admitted[&token].connection.address;
        for peer in peers {
            self.apply(policy::Command::Resolve {
                peer,
                source: Some(source),
                trusted: false,
            })
            .await?;
        }
        Ok(policy::Outcome::Done)
    }
    pub(super) async fn command(&mut self, mut command: Command) {
        if self.stopping {
            command.finish(Err(Error::Closed));
            return;
        }
        let result = match &mut command.work {
            Work::Policy(policy) => self.apply(policy.clone()).await,
            Work::Learn { client, peers } => self.learn(client, std::mem::take(peers)).await,
            Work::Connection { client, reply } => match self.token(client).await {
                Ok(token) => {
                    let info = token
                        .and_then(|t| self.admitted.get(&t))
                        .map(|a| a.connection);
                    let _ = reply.take().unwrap().send(info);
                    Ok(policy::Outcome::Done)
                }
                Err(error) => Err(error),
            },
            Work::PingReceived(client) => match self.token(client).await {
                Ok(Some(token)) => {
                    self.apply(policy::Command::PingDone {
                        peer: client.peer(),
                        token,
                    })
                    .await
                }
                Ok(None) => Ok(policy::Outcome::Stale),
                Err(error) => Err(error),
            },
            Work::Disconnect(client) => match self.token(client).await {
                Ok(Some(token)) => {
                    self.sockets.cancel(token);
                    Ok(policy::Outcome::Done)
                }
                Ok(None) => Ok(policy::Outcome::Stale),
                Err(error) => Err(error),
            },
            Work::Synchronize(client) => match self.token(client).await {
                Ok(Some(_)) => match self.pool.synchronize(client.peer()) {
                    Ok(ticket) => self
                        .wait(ticket.wait())
                        .await
                        .map(policy::Outcome::Bool)
                        .map_err(Error::from),
                    Err(error) => Err(error.into()),
                },
                Ok(None) => Ok(policy::Outcome::Stale),
                Err(error) => Err(error),
            },
        };
        command.finish(result);
        if let Err(error) = self.refresh().await {
            self.fail(error);
        }
    }
}
