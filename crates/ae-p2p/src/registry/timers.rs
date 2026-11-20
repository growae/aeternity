use super::*;

impl State {
    pub(super) fn class_index(class: Class) -> usize {
        match class {
            Class::Verified => 0,
            Class::Unverified => 1,
        }
    }
    pub(super) fn new_timer(&mut self, delay: u64, kind: TimerKind) -> Result<Token, Error> {
        let at = self.now.checked_add(delay).ok_or(Error::TimeOverflow)?;
        let token = self.next_token()?;
        self.timers.insert(token, Deadline { at, kind });
        Ok(token)
    }
    fn exponential_delay(&self, maximum: u64) -> u64 {
        let n = self.counts().outbound;
        if n == 0 {
            0
        } else if n > 6 {
            maximum
        } else {
            (1000u64 << (n - 1)).min(maximum)
        }
    }
    pub(super) fn schedule_connect(&mut self, minimum: Option<u64>) -> Result<(), Error> {
        if let Some(old) = self.connect_timer.take() {
            self.timers.remove(&old);
        }
        if self.counts().outbound >= self.config.maximum_outbound {
            return Ok(());
        }
        let jitter = self.random.gen_range(101..=150);
        let elapsed = self.last_connect.map_or(u64::MAX, |t| self.now - t);
        let delay = if self.config.analytics {
            jitter
        } else {
            jitter.max(self.exponential_delay(30_000).saturating_sub(elapsed))
        };
        self.connect_timer =
            Some(self.new_timer(delay.max(minimum.unwrap_or(0)), TimerKind::Connect)?);
        Ok(())
    }
    pub(super) fn schedule_probe(
        &mut self,
        class: Class,
        minimum: Option<u64>,
    ) -> Result<(), Error> {
        let i = Self::class_index(class);
        if let Some(old) = self.probe_timers[i].take() {
            self.timers.remove(&old);
        }
        let elapsed = self.last_probe[i].map_or(u64::MAX, |t| self.now - t);
        let delay = 100
            .max(self.exponential_delay(1000).saturating_sub(elapsed))
            .max(minimum.unwrap_or(0));
        self.probe_timers[i] = Some(self.new_timer(delay, TimerKind::Probe(class))?);
        Ok(())
    }
    pub(super) fn start_probes(&mut self) -> Result<(), Error> {
        for class in [Class::Verified, Class::Unverified] {
            let i = Self::class_index(class);
            if self.probe_timers[i].is_none() {
                self.last_probe[i] = Some(self.now);
                self.probe_timers[i] = Some(
                    self.new_timer(self.config.ping_interval * 3 / 2, TimerKind::Probe(class))?,
                );
            }
        }
        Ok(())
    }
    pub(super) fn timeout(
        &mut self,
        token: Token,
        effects: &mut Vec<Effect>,
    ) -> Result<Outcome, Error> {
        let Some(timer) = self.timers.get(&token) else {
            return Ok(Outcome::Stale);
        };
        if timer.at > self.now {
            return Ok(Outcome::Ignored);
        }
        let kind = self.timers.remove(&token).unwrap().kind;
        match kind {
            TimerKind::Connect => {
                self.connect_timer = None;
                self.connect(None, None, effects)
            }
            TimerKind::Probe(class) => {
                self.probe_timers[Self::class_index(class)] = None;
                self.connect(None, Some(class), effects)
            }
            TimerKind::Inbound(owner) => {
                if self.pending.get(&owner).is_some_and(|p| p.timer == token) {
                    self.pending.remove(&owner);
                    effects.push(Effect::Close(owner));
                    Ok(Outcome::Done)
                } else {
                    Ok(Outcome::Stale)
                }
            }
            TimerKind::Owner(peer, owner) => {
                let Some(c) = self
                    .connections
                    .get_mut(&peer)
                    .filter(|c| c.connection.token == owner && c.timer == Some(token))
                else {
                    return Ok(Outcome::Stale);
                };
                c.timer = None;
                if c.connection.status == Status::Connected {
                    effects.push(Effect::Ping { token: owner, peer });
                    Ok(Outcome::Done)
                } else {
                    let failure = if c.connection.probe.is_some() {
                        Failure::Dead
                    } else {
                        Failure::Failed
                    };
                    self.failed(peer, owner, failure, effects)
                }
            }
            TimerKind::Resolve(host) => {
                let Some(r) = self.hosts.get(&host).filter(|r| r.timer == token) else {
                    return Ok(Outcome::Stale);
                };
                if let Some(attempt) = r.attempt {
                    self.resolved(attempt, None, true, effects)
                } else {
                    self.start_resolution(host, effects)?;
                    Ok(Outcome::Done)
                }
            }
        }
    }
}
