use super::*;

impl State {
    pub(super) fn resolution_capacity(&self, host: &[u8], id: PeerPublicKey) -> bool {
        let existing = self.hosts.get(host);
        let replaces = existing.is_some_and(|r| r.peers.contains_key(&id));
        self.config.maximum_peers_per_host != 0
            && (replaces || self.counts().resolving_peers < self.config.maximum_resolving_peers)
            && (existing.is_some() || self.hosts.len() < self.config.maximum_hosts)
            && (replaces
                || existing.is_none_or(|r| r.peers.len() < self.config.maximum_peers_per_host))
    }
    pub(super) fn cancel_resolving_peer(&mut self, peer: PeerPublicKey, effects: &mut Vec<Effect>) {
        let hosts: Vec<_> = self
            .hosts
            .iter_mut()
            .filter_map(|(host, r)| {
                r.peers.remove(&peer);
                r.peers.is_empty().then(|| host.clone())
            })
            .collect();
        for host in hosts {
            let removed = self.hosts.remove(&host).unwrap();
            self.timers.remove(&removed.timer);
            if let Some(token) = removed.attempt {
                effects.push(Effect::CancelResolve(token));
            }
        }
    }
    pub(super) fn resolve(
        &mut self,
        peer: Info,
        source: Option<Ipv4Addr>,
        trusted: bool,
        effects: &mut Vec<Effect>,
    ) -> Result<Outcome, Error> {
        if self.blocked_or_local(peer.id) {
            return Ok(Outcome::Ignored);
        }
        let host = peer.host.clone();
        let id = peer.id;
        if !self.resolution_capacity(&host, id) {
            return Err(Error::ResourceLimit);
        }
        let data = ResolutionPeer {
            info: peer,
            source,
            trusted,
        };
        if let Some(r) = self.hosts.get_mut(&host) {
            r.peers.insert(id, data);
            if r.retries > self.config.resolver_retries {
                r.peers.retain(|_, p| p.trusted);
            }
            if r.peers.is_empty() {
                let removed = self.hosts.remove(&host).unwrap();
                self.timers.remove(&removed.timer);
                if let Some(token) = removed.attempt {
                    effects.push(Effect::CancelResolve(token));
                }
                return Ok(Outcome::Done);
            }
            if r.attempt.is_none() {
                let old = r.timer;
                let retries = r.retries;
                self.timers.remove(&old);
                let timer = self.retry_timer(&host, retries)?;
                self.hosts.get_mut(&host).unwrap().timer = timer;
            }
        } else {
            let attempt = self.next_token()?;
            let timer = self.new_timer(
                self.config.resolution_timeout,
                TimerKind::Resolve(host.clone()),
            )?;
            self.hosts.insert(
                host.clone(),
                Resolution {
                    peers: BTreeMap::from([(id, data)]),
                    retries: 0,
                    attempt: Some(attempt),
                    timer,
                },
            );
            effects.push(Effect::Resolve {
                token: attempt,
                host,
            });
        }
        Ok(Outcome::Done)
    }
    fn retry_timer(&mut self, host: &[u8], retries: u64) -> Result<Token, Error> {
        let table = &self.config.resolver_backoff;
        let delay = table[(retries.max(1).min(table.len() as u64) - 1) as usize];
        self.new_timer(delay, TimerKind::Resolve(host.to_vec()))
    }
    pub(super) fn start_resolution(
        &mut self,
        host: Vec<u8>,
        effects: &mut Vec<Effect>,
    ) -> Result<(), Error> {
        let token = self.next_token()?;
        let timer = self.new_timer(
            self.config.resolution_timeout,
            TimerKind::Resolve(host.clone()),
        )?;
        let r = self.hosts.get_mut(&host).unwrap();
        self.timers.remove(&r.timer);
        r.timer = timer;
        r.attempt = Some(token);
        effects.push(Effect::Resolve { token, host });
        Ok(())
    }
    pub(super) fn resolved(
        &mut self,
        token: Token,
        address: Option<Ipv4Addr>,
        timed_out: bool,
        effects: &mut Vec<Effect>,
    ) -> Result<Outcome, Error> {
        let Some(host) = self
            .hosts
            .iter()
            .find(|(_, r)| r.attempt == Some(token))
            .map(|(h, _)| h.clone())
        else {
            return Ok(Outcome::Stale);
        };
        let mut resolution = self.hosts.remove(&host).unwrap();
        self.timers.remove(&resolution.timer);
        let initial_failure = resolution.retries == 0 && address.is_none();
        if timed_out {
            effects.push(Effect::CancelResolve(token));
        }
        if let Some(address) = address {
            for data in resolution.peers.into_values() {
                self.add(
                    data.info.resolved(address, data.source, data.trusted),
                    effects,
                )?;
            }
        } else {
            resolution.retries = resolution.retries.saturating_add(1);
            if resolution.retries > self.config.resolver_retries {
                resolution.peers.retain(|_, p| p.trusted);
            }
            if !resolution.peers.is_empty() {
                resolution.attempt = None;
                resolution.timer = self.retry_timer(&host, resolution.retries)?;
                self.hosts.insert(host, resolution);
            }
        }
        if !initial_failure {
            self.schedule_connect(None)?;
        }
        Ok(Outcome::Done)
    }
}
