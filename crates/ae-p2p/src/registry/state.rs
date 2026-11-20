use super::*;

impl State {
    pub(super) fn validate_command(&self, command: &Command) -> Result<(), Error> {
        let host = match command {
            Command::Add(p) => Some(&p.host),
            Command::Block(p)
            | Command::Resolve { peer: p, .. }
            | Command::Accept { peer: p, .. } => Some(&p.host),
            _ => None,
        };
        if host.is_some_and(|h| h.len() > self.config.pool.maximum_host_bytes) {
            return Err(Error::ResourceLimit);
        }
        if let Command::Sample {
            percent_connected,
            exclude,
            ..
        } = command
        {
            if percent_connected.is_some_and(|n| n > 100) {
                return Err(Error::Configuration);
            }
            if exclude.len() > self.config.maximum_exclusions {
                return Err(Error::ResourceLimit);
            }
        }
        Ok(())
    }
    pub(super) fn counts(&self) -> Counts {
        let mut result = Counts {
            pending_inbound: self.pending.len(),
            blocked: self.blocked.len(),
            hosts: self.hosts.len(),
            resolving_peers: self.hosts.values().map(|r| r.peers.len()).sum(),
            ..Default::default()
        };
        for owned in self.connections.values() {
            let c = &owned.connection;
            if c.probe.is_some() {
                result.probes += 1;
            } else {
                match c.direction {
                    Direction::Inbound => result.inbound += 1,
                    Direction::Outbound => result.outbound += 1,
                }
            }
            if c.direction == Direction::Inbound && c.kind == Kind::Temporary {
                result.temporary += 1;
            }
        }
        result
    }
    pub(super) fn reconcile(&mut self) {
        self.sockets.clear();
        for peer in self.pool.all(Target::Both) {
            self.sockets
                .entry(peer.socket())
                .or_default()
                .insert(peer.id);
        }
        self.probe_status
            .retain(|id, _| self.pool.find(*id).is_some());
    }
    pub(super) fn store<T>(
        &mut self,
        change: discovery::Change<T>,
        effects: &mut Vec<Effect>,
    ) -> T {
        effects.extend(change.effects.into_iter().map(Effect::Store));
        self.reconcile();
        change.result
    }
    pub(super) fn next_token(&mut self) -> Result<Token, Error> {
        self.sequence = self.sequence.checked_add(1).ok_or(Error::ResourceLimit)?;
        Ok(Token {
            registry: self.registry,
            sequence: self.sequence,
        })
    }
    fn maybe_unblock(&mut self) {
        match self.next_unblock {
            None => self.next_unblock = Some(self.now + self.config.unblock_interval),
            Some(t) if self.now > t => {
                self.blocked.clear();
                self.next_unblock = Some(self.now + self.config.unblock_interval);
            }
            _ => {}
        }
    }
    pub(super) fn blocked_or_local(&mut self, id: PeerPublicKey) -> bool {
        self.maybe_unblock();
        id == self.local || self.blocked.contains_key(&id)
    }
    pub(super) fn add(
        &mut self,
        mut peer: Peer,
        effects: &mut Vec<Effect>,
    ) -> Result<Outcome, Error> {
        if self.blocked_or_local(peer.id) {
            return Ok(Outcome::Ignored);
        }
        let id = peer.id;
        let existing = self.pool.find(id).cloned();
        match (self.sockets.get(&peer.socket()), existing) {
            (None, None) => self.start_probes()?,
            (Some(ids), Some(mut old)) if ids.contains(&id) => {
                old.source = peer.source;
                peer = old;
            }
            _ => return Ok(Outcome::Ignored),
        }
        let new = self.pool.find(id).is_none();
        let trusted = peer.trusted;
        let change = self.pool.update(self.now, peer)?;
        let updated = self.store(change, effects);
        if new && trusted && updated != Update::Ignored {
            self.connect(Some(id), None, effects)?;
        }
        Ok(Outcome::Updated(updated))
    }
    fn delete(&mut self, id: PeerPublicKey, effects: &mut Vec<Effect>) -> Result<(), Error> {
        self.cancel_resolving_peer(id, effects);
        self.remove_connection(id, true, effects);
        self.pool.release(self.now, id)?;
        if self.pool.find(id).is_some() {
            let change = self.pool.delete(id)?;
            self.store(change, effects);
        }
        Ok(())
    }
    pub(super) fn block(
        &mut self,
        info: Info,
        effects: &mut Vec<Effect>,
    ) -> Result<Outcome, Error> {
        self.block_address(
            crate::address::Address {
                id: info.id,
                host: info.host,
                port: Some(info.port.into()),
            },
            effects,
        )
    }
    pub(super) fn block_address(
        &mut self,
        info: crate::address::Address,
        effects: &mut Vec<Effect>,
    ) -> Result<Outcome, Error> {
        self.maybe_unblock();
        let info = match self.pool.find(info.id) {
            Some(p) if p.trusted => return Ok(Outcome::Ignored),
            Some(p) => crate::address::Address {
                id: p.id,
                host: p.host.clone(),
                port: Some(p.port.into()),
            },
            None => info,
        };
        if !self.blocked.contains_key(&info.id) && self.blocked.len() >= self.config.maximum_blocked
        {
            return Err(Error::ResourceLimit);
        }
        let id = info.id;
        self.blocked.insert(id, info);
        effects.push(Effect::Blocked(id));
        self.delete(id, effects)?;
        Ok(Outcome::Done)
    }
    fn connected_sample(
        &mut self,
        maximum: usize,
        direction: Option<Direction>,
        exclude: &BTreeSet<PeerPublicKey>,
    ) -> Vec<Info> {
        if maximum == 0 {
            return vec![];
        }
        let mut peers: Vec<_> = self
            .connections
            .values()
            .filter(|c| {
                c.connection.status == Status::Connected
                    && direction.is_none_or(|d| c.connection.direction == d)
                    && !exclude.contains(&c.connection.peer.id)
            })
            .map(|c| Info::from(&c.connection.peer))
            .collect();
        peers.shuffle(&mut self.random);
        peers.truncate(maximum);
        peers
    }
    fn sample(
        &mut self,
        maximum: usize,
        percent: Option<u8>,
        exclude: Vec<PeerPublicKey>,
    ) -> Vec<Info> {
        if maximum == 0 {
            return vec![];
        }
        let requested = maximum;
        let maximum = maximum.min(self.pool.count(discovery::Count::All, Target::Both));
        let mut excluded: BTreeSet<_> = exclude.into_iter().collect();
        let mut connected = vec![];
        if let Some(percent) = percent {
            let target = ((requested as u128 * u128::from(percent) + 50) / 100)
                .max(1)
                .min(self.connections.len() as u128) as usize;
            connected = self.connected_sample(target, None, &excluded);
            if !connected.is_empty() {
                connected.truncate(self.random.gen_range(1..=connected.len()));
            }
            excluded.extend(
                self.connections
                    .values()
                    .filter(|c| c.connection.status == Status::Connected)
                    .map(|c| c.connection.peer.id),
            );
        }
        let mut result: Vec<_> = self
            .pool
            .random_subset(maximum - connected.len(), |p| !excluded.contains(&p.id))
            .iter()
            .map(Info::from)
            .collect();
        result.extend(connected);
        result
    }
    pub(super) fn apply(
        &mut self,
        command: Command,
        effects: &mut Vec<Effect>,
    ) -> Result<Outcome, Error> {
        match command {
            Command::Add(p) => {
                let result = self.add(p, effects)?;
                self.schedule_connect(None)?;
                Ok(result)
            }
            Command::Delete(id) => {
                self.delete(id, effects)?;
                self.schedule_connect(None)?;
                Ok(Outcome::Done)
            }
            Command::Block(info) => {
                let result = self.block(info, effects)?;
                self.schedule_connect(None)?;
                Ok(result)
            }
            Command::Unblock(id) => {
                self.maybe_unblock();
                if self.blocked.remove(&id).is_some() {
                    effects.push(Effect::Unblocked(id));
                }
                self.schedule_connect(None)?;
                Ok(Outcome::Done)
            }
            Command::IsBlocked(id) => {
                self.maybe_unblock();
                Ok(Outcome::Bool(self.blocked.contains_key(&id)))
            }
            Command::Resolve {
                peer,
                source,
                trusted,
            } => self.resolve(peer, source, trusted, effects),
            Command::Resolved { token, address } => self.resolved(token, address, false, effects),
            Command::Connect(id) => self.connect(id, None, effects),
            Command::Probe(class) => self.connect(None, Some(class), effects),
            Command::ReserveInbound { address } => self.reserve_inbound(address),
            Command::Accept { token, peer } => self.accept(token, peer, effects),
            Command::AbandonInbound(token) => self.reject_inbound(token, None, effects),
            Command::RejectInbound { token, reason } => {
                self.reject_inbound(token, Some(reason), effects)
            }
            Command::Connected { peer, token } => self.connected(peer, token, effects),
            Command::Alive { peer, token } => self.alive(peer, token, effects),
            Command::Failed {
                peer,
                token,
                reason,
            } => self.failed(peer, token, reason, effects),
            Command::Suspect(id) => {
                if self.remove_connection(id, true, effects).is_some() {
                    let change = self.pool.reject(self.now, id)?;
                    self.store(change, effects);
                }
                Ok(Outcome::Done)
            }
            Command::PingDone { peer, token } => self.ping_done(peer, token, effects),
            Command::Timeout(token) => self.timeout(token, effects),
            Command::Sample {
                maximum,
                percent_connected,
                exclude,
            } => Ok(Outcome::Peers(self.sample(
                maximum,
                percent_connected,
                exclude,
            ))),
            Command::SampleConnected { maximum, direction } => Ok(Outcome::Peers(
                self.connected_sample(maximum, direction, &BTreeSet::new()),
            )),
            Command::Shutdown => {
                let ids: Vec<_> = self.connections.keys().copied().collect();
                for id in ids {
                    self.remove_connection(id, true, effects);
                    self.pool.release(self.now, id)?;
                }
                effects.extend(self.pending.keys().copied().map(Effect::Close));
                effects.extend(
                    self.hosts
                        .values()
                        .filter_map(|r| r.attempt)
                        .map(Effect::CancelResolve),
                );
                self.pending.clear();
                self.hosts.clear();
                self.timers.clear();
                self.connect_timer = None;
                self.probe_timers = [None; 2];
                self.closed = true;
                Ok(Outcome::Done)
            }
        }
    }
}
