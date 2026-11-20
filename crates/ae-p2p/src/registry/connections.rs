use super::*;

impl State {
    fn connection_capacity(&self) -> bool {
        self.connections.len() + self.pending.len() < self.config.maximum_connections
    }
    pub(super) fn remove_connection(
        &mut self,
        id: PeerPublicKey,
        close: bool,
        effects: &mut Vec<Effect>,
    ) -> Option<Connection> {
        let owned = self.connections.remove(&id)?;
        if let Some(timer) = owned.timer {
            self.timers.remove(&timer);
        }
        if close {
            effects.push(Effect::Close(owned.connection.token));
        }
        Some(owned.connection)
    }
    fn matching(&self, id: PeerPublicKey, token: Token) -> Option<&Connection> {
        self.connections
            .get(&id)
            .map(|c| &c.connection)
            .filter(|c| c.token == token)
    }
    pub(super) fn connect(
        &mut self,
        id: Option<PeerPublicKey>,
        probe: Option<Class>,
        effects: &mut Vec<Effect>,
    ) -> Result<Outcome, Error> {
        match probe {
            None => {
                if let Some(t) = self.connect_timer.take() {
                    self.timers.remove(&t);
                }
            }
            Some(class) => {
                if let Some(t) = self.probe_timers[Self::class_index(class)].take() {
                    self.timers.remove(&t);
                }
            }
        }
        let counts = self.counts();
        let allowed = match probe {
            None => counts.outbound < self.config.maximum_outbound,
            Some(_) => counts.probes < self.config.maximum_probes,
        };
        if !allowed || !self.connection_capacity() {
            if let Some(class) = probe {
                self.schedule_probe(class, Some(1000))?;
            } else if allowed {
                self.schedule_connect(Some(100))?;
            }
            return Ok(Outcome::Unavailable);
        }
        let selection = if let Some(id) = id {
            match self.pool.find(id).cloned() {
                Some(peer) if self.pool.state(id).is_some_and(|s| !s.selected) => {
                    self.pool.select(self.now, id)?;
                    Selection::Selected(peer)
                }
                _ => Selection::Unavailable,
            }
        } else {
            let groups: BTreeSet<_> = self
                .connections
                .values()
                .filter(|c| {
                    c.connection.direction == Direction::Outbound && c.connection.probe.is_none()
                })
                .map(|c| discovery::address_group(c.connection.peer.address))
                .collect();
            let filter = self.config.single_outbound_per_group;
            let target = match probe {
                None => Target::Both,
                Some(Class::Verified) => Target::Verified,
                Some(Class::Unverified) => Target::Unverified,
            };
            self.pool.random_select(self.now, target, |p| {
                !filter || !groups.contains(&discovery::address_group(p.address))
            })?
        };
        let result = match selection {
            Selection::Selected(peer) => {
                let token = self.next_token()?;
                let timer = self.new_timer(
                    self.config.attempt_timeout,
                    TimerKind::Owner(peer.id, token),
                )?;
                self.connections.insert(
                    peer.id,
                    OwnedConnection {
                        connection: Connection {
                            peer: peer.clone(),
                            token,
                            direction: Direction::Outbound,
                            status: Status::Connecting,
                            kind: Kind::Permanent,
                            probe,
                        },
                        timer: Some(timer),
                    },
                );
                if let Some(class) = probe {
                    self.last_probe[Self::class_index(class)] = Some(self.now);
                } else {
                    self.last_connect = Some(self.now);
                }
                effects.push(Effect::Dial { token, peer, probe });
                Outcome::Selected(token)
            }
            Selection::Wait(delay) => Outcome::Wait(delay),
            Selection::Unavailable => Outcome::Unavailable,
        };
        match (&result, probe) {
            (Outcome::Wait(delay), None) => self.schedule_connect(Some(*delay))?,
            (Outcome::Selected(_), None) => self.schedule_connect(None)?,
            (Outcome::Wait(delay), Some(class)) => self.schedule_probe(class, Some(*delay))?,
            (Outcome::Unavailable, Some(class)) => self.schedule_probe(class, Some(1000))?,
            (_, Some(class)) => self.schedule_probe(class, None)?,
            _ => {}
        }
        Ok(result)
    }
    pub(super) fn reserve_inbound(&mut self, address: Ipv4Addr) -> Result<Outcome, Error> {
        if self.pending.len() >= self.config.maximum_pending_inbound || !self.connection_capacity()
        {
            return Err(Error::ResourceLimit);
        }
        let token = self.next_token()?;
        let timer = self.new_timer(self.config.admission_timeout, TimerKind::Inbound(token))?;
        self.pending
            .insert(token, PendingInbound { address, timer });
        Ok(Outcome::Reserved(token))
    }
    pub(super) fn accept(
        &mut self,
        token: Token,
        info: Info,
        effects: &mut Vec<Effect>,
    ) -> Result<Outcome, Error> {
        let Some(pending) = self.pending.remove(&token) else {
            return Ok(Outcome::Stale);
        };
        self.timers.remove(&pending.timer);
        let reject = |reason, effects: &mut Vec<Effect>| {
            effects.push(Effect::Close(token));
            Ok(Outcome::Rejected(reason))
        };
        let id = info.id;
        if self.blocked_or_local(id) {
            return reject(Rejection::Blocked, effects);
        }
        let peer = match self.pool.find(id).cloned() {
            Some(mut old) if old.socket() == SocketAddrV4::new(pending.address, info.port) => {
                old.source = pending.address;
                old
            }
            Some(old) if old.trusted => return reject(Rejection::TrustedAddressChanged, effects),
            Some(_) => {
                self.remove_connection(id, true, effects);
                self.pool.release(self.now, id)?;
                let change = self.pool.delete(id)?;
                self.store(change, effects);
                info.resolved(pending.address, None, false)
            }
            None => info.resolved(pending.address, None, false),
        };
        let temporary = self.counts().inbound >= self.config.maximum_inbound;
        if let Some(old) = self.connections.get(&id) {
            if self.local > id && (temporary || old.connection.status == Status::Connected) {
                return reject(Rejection::AlreadyConnected, effects);
            }
            let change = self.pool.reject(self.now, id)?;
            self.store(change, effects);
            self.remove_connection(id, true, effects);
        }
        let counts = self.counts();
        let kind = if counts.inbound < self.config.maximum_inbound {
            Kind::Permanent
        } else {
            Kind::Temporary
        };
        if kind == Kind::Temporary && counts.temporary >= self.config.maximum_temporary {
            return reject(Rejection::Capacity, effects);
        }
        let change = self.pool.update(self.now, peer.clone())?;
        match self.store(change, effects) {
            Update::Ignored => return reject(Rejection::Ignored, effects),
            Update::Unverified => {
                let change = self.pool.verify(self.now, id)?;
                self.store(change, effects);
            }
            Update::Verified => {}
        }
        self.pool.select(self.now, id)?;
        self.connections.insert(
            id,
            OwnedConnection {
                connection: Connection {
                    peer,
                    token,
                    direction: Direction::Inbound,
                    status: Status::Connected,
                    kind,
                    probe: None,
                },
                timer: None,
            },
        );
        Ok(Outcome::Accepted(kind))
    }
    pub(super) fn connected(
        &mut self,
        peer: PeerPublicKey,
        token: Token,
        effects: &mut Vec<Effect>,
    ) -> Result<Outcome, Error> {
        let Some(connection) = self.matching(peer, token) else {
            return Ok(Outcome::Stale);
        };
        let valid = connection.direction == Direction::Outbound
            && connection.status == Status::Connecting
            && connection.probe.is_none();
        if !valid {
            self.remove_connection(peer, true, effects);
            let change = self.pool.reject(self.now, peer)?;
            self.store(change, effects);
            self.schedule_connect(None)?;
            return Ok(Outcome::Rejected(Rejection::Invalid));
        }
        let change = self.pool.verify(self.now, peer)?;
        self.store(change, effects);
        let owned = self.connections.get_mut(&peer).unwrap();
        if let Some(t) = owned.timer.take() {
            self.timers.remove(&t);
        }
        owned.connection.status = Status::Connected;
        effects.push(Effect::Ping { token, peer });
        self.schedule_connect(None)?;
        Ok(Outcome::Done)
    }
    fn set_probe_status(&mut self, peer: PeerPublicKey, alive: bool, effects: &mut Vec<Effect>) {
        if self.probe_status.insert(peer, alive) != Some(alive) {
            effects.push(Effect::ProbeStatus { peer, alive });
        }
    }
    pub(super) fn alive(
        &mut self,
        peer: PeerPublicKey,
        token: Token,
        effects: &mut Vec<Effect>,
    ) -> Result<Outcome, Error> {
        let Some(connection) = self.matching(peer, token) else {
            return Ok(Outcome::Stale);
        };
        let valid = connection.direction == Direction::Outbound
            && connection.status == Status::Connecting
            && connection.probe.is_some();
        self.remove_connection(peer, true, effects);
        if valid {
            self.set_probe_status(peer, true, effects);
            self.pool.release(self.now, peer)?;
            let change = self.pool.verify(self.now, peer)?;
            self.store(change, effects);
        } else {
            let change = self.pool.reject(self.now, peer)?;
            self.store(change, effects);
        }
        self.start_probes()?;
        Ok(if valid {
            Outcome::Done
        } else {
            Outcome::Rejected(Rejection::Invalid)
        })
    }
    pub(super) fn failed(
        &mut self,
        peer: PeerPublicKey,
        token: Token,
        reason: Failure,
        effects: &mut Vec<Effect>,
    ) -> Result<Outcome, Error> {
        let Some(connection) = self.matching(peer, token) else {
            return Ok(Outcome::Stale);
        };
        if (reason == Failure::Closed && connection.probe.is_some())
            || (reason == Failure::Dead
                && (connection.probe.is_none() || connection.status != Status::Connecting))
            || (reason == Failure::Start && connection.status != Status::Connecting)
        {
            return Ok(Outcome::Ignored);
        }
        self.remove_connection(peer, true, effects);
        if reason == Failure::Dead {
            self.set_probe_status(peer, false, effects);
        }
        if matches!(reason, Failure::Down | Failure::Dead) {
            let change = self.pool.reject(self.now, peer)?;
            self.store(change, effects);
        } else {
            self.pool.release(self.now, peer)?;
        }
        if reason == Failure::Dead {
            self.start_probes()?;
        } else {
            self.schedule_connect(None)?;
        }
        Ok(Outcome::Done)
    }
    pub(super) fn ping_done(
        &mut self,
        peer: PeerPublicKey,
        token: Token,
        effects: &mut Vec<Effect>,
    ) -> Result<Outcome, Error> {
        let Some(connection) = self.matching(peer, token) else {
            return Ok(Outcome::Stale);
        };
        if connection.status != Status::Connected {
            return Ok(Outcome::Ignored);
        }
        if connection.kind == Kind::Temporary {
            return self.failed(peer, token, Failure::Closed, effects);
        }
        let timer = self.new_timer(self.config.ping_interval, TimerKind::Owner(peer, token))?;
        if let Some(old) = self
            .connections
            .get_mut(&peer)
            .unwrap()
            .timer
            .replace(timer)
        {
            self.timers.remove(&old);
        }
        Ok(Outcome::Done)
    }
}
