use super::*;

impl Service {
    pub(super) fn provisional_token(&self, client: &net::Client) -> Option<policy::Token> {
        if client.is_closed() {
            return None;
        }
        self.provisional.iter().find_map(|(token, owned)| {
            (owned.client.peer() == client.peer()
                && owned.client.generation() == client.generation())
            .then_some(*token)
        })
    }
    pub(super) async fn connection_info(
        &mut self,
        client: &net::Client,
    ) -> Result<Option<Connection>, Error> {
        if let Some(token) = self.provisional_token(client) {
            return Ok(Some(self.provisional[&token].connection));
        }
        if client.is_closed() {
            return Ok(None);
        }
        let Some(mut connection) = self
            .admitted
            .values()
            .find(|a| {
                a.client.peer() == client.peer() && a.client.generation() == client.generation()
            })
            .map(|a| a.connection)
        else {
            return Ok(None);
        };
        // Retired registry ownership does not retire the authenticated stream.
        // Keep this generation's local facts until its terminal monitor fires.
        connection.closing |= client.is_disconnecting() || self.token(client).await?.is_none();
        Ok(Some(connection))
    }
    pub(super) async fn authenticated(
        &mut self,
        ready: transport::Authenticated,
    ) -> Result<(), Error> {
        let transport::Authenticated {
            token,
            address,
            connection,
            ..
        } = ready;
        let client = connection.client().clone();
        if self.stopping || client.is_closed() {
            drop(connection);
            self.apply(policy::Command::AbandonInbound(token)).await?;
            return self.refresh().await;
        }
        self.provisional.insert(
            token,
            Admitted {
                connection: Connection {
                    token,
                    peer: client.peer(),
                    address,
                    kind: policy::Kind::Permanent,
                    admitted: false,
                    closing: false,
                },
                client: client.clone(),
            },
        );
        let attached = match self.pool.attach_provisional(connection) {
            Ok(ticket) => self.wait(ticket.wait()).await,
            Err(error) => Err(error),
        };
        if attached.is_err() || self.stopping {
            self.sockets.cancel(token);
            self.provisional.remove(&token);
            self.apply(policy::Command::AbandonInbound(token)).await?;
        }
        self.refresh().await
    }
    pub(super) async fn first_ping(
        &mut self,
        client: &net::Client,
        port: u16,
        retirement: &net::PingRetirement,
    ) -> Result<policy::Outcome, Error> {
        if client.is_closed() {
            return Ok(policy::Outcome::Stale);
        }
        let Some(token) = self.provisional_token(client) else {
            return Ok(match self.connection_info(client).await? {
                Some(connection) if connection.admitted => {
                    policy::Outcome::Accepted(connection.kind)
                }
                None => policy::Outcome::Stale,
                Some(_) => policy::Outcome::Stale,
            });
        };
        if !self.sockets.begin_admission(token) {
            self.sockets.cancel(token);
            self.provisional.remove(&token);
            self.apply(policy::Command::AbandonInbound(token)).await?;
            self.refresh().await?;
            return Ok(policy::Outcome::Stale);
        }
        let address = self.provisional[&token].connection.address;
        let outcome = self
            .apply(policy::Command::Accept {
                token,
                peer: policy::Info {
                    id: client.peer(),
                    host: address.to_string().into_bytes(),
                    port,
                },
            })
            .await?;
        match outcome {
            policy::Outcome::Accepted(kind) if !self.stopping => {
                let promoted = match self.pool.promote(client.clone()) {
                    Ok(ticket) => self.wait(ticket.wait()).await,
                    Err(error) => Err(error),
                };
                let mut owned = self.provisional.remove(&token).expect("owned admission");
                if promoted.is_err() || self.stopping {
                    self.sockets.cancel(token);
                    self.apply(policy::Command::Failed {
                        peer: client.peer(),
                        token,
                        reason: policy::Failure::Closed,
                    })
                    .await?;
                    self.refresh().await?;
                    return Ok(policy::Outcome::Stale);
                }
                owned.connection.kind = kind;
                owned.connection.admitted = true;
                self.admitted.insert(token, owned);
                self.current.counts.accepted = self.current.counts.accepted.saturating_add(1);
            }
            policy::Outcome::Rejected(_) if !self.stopping => {
                self.current.counts.rejected = self.current.counts.rejected.saturating_add(1);
                // The driver now owns this exact callback's eventual stop.
                // Callback abandonment/deadline still retires it boundedly.
                retirement.retire();
                self.rejected_admissions.insert(token);
            }
            _ => {
                self.sockets.cancel(token);
                self.provisional.remove(&token);
            }
        }
        self.refresh().await?;
        Ok(outcome)
    }
    pub(super) async fn ping_closing(
        &mut self,
        client: &net::Client,
        close: &mut Option<Box<net::PingClose>>,
    ) -> Result<policy::Outcome, Error> {
        let Some(connection) = self.connection_info(client).await? else {
            return Ok(policy::Outcome::Stale);
        };
        if connection.kind != policy::Kind::Temporary || !connection.admitted {
            return Err(Error::Configuration);
        }
        if !close.as_ref().expect("owned continuation").was_closing()
            && let Some(token) = self.token(client).await?
        {
            let outcome = self
                .apply(policy::Command::PingDone {
                    peer: client.peer(),
                    token,
                })
                .await?;
            if outcome != policy::Outcome::Done {
                return Ok(outcome);
            }
        }
        if self.stopping || client.is_closed() {
            return Err(Error::Closed);
        }
        self.admitted
            .get_mut(&connection.token)
            .expect("owned closing callback")
            .connection
            .closing = true;
        // The response and negotiation already completed; durable policy
        // retirement precedes releasing the driver's post-ping fence.
        close
            .take()
            .expect("owned continuation")
            .close()
            .map_err(|e| Error::Transport(transport::Error::Runtime(e)))?;
        Ok(policy::Outcome::Done)
    }
}
