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
        Ok(self
            .token(client)
            .await?
            .and_then(|t| self.admitted.get(&t))
            .map(|a| a.connection))
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
            return Ok(
                match self
                    .token(client)
                    .await?
                    .and_then(|t| self.admitted.get(&t))
                {
                    Some(owned) => policy::Outcome::Accepted(owned.connection.kind),
                    None => policy::Outcome::Stale,
                },
            );
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
}
