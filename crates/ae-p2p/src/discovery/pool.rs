use super::*;

#[derive(Clone)]
pub struct Pool {
    pub(super) config: Config,
    pub(super) secret: Zeroizing<Vec<u8>>,
    pub(super) random: StdRng,
    pub(super) peers: BTreeMap<PeerPublicKey, Entry>,
    pub(super) standby: BTreeSet<PeerPublicKey>,
    pub(super) verified: Vec<Vec<PeerPublicKey>>,
    pub(super) unverified: Vec<Vec<PeerPublicKey>>,
    pub(super) all_verified: Lookup,
    pub(super) available_verified: Lookup,
    pub(super) available_unverified: Lookup,
    last_time: Option<u64>,
}
impl Pool {
    pub fn new(config: Config) -> Result<Self, Error> {
        config.validate()?;
        let mut entropy = Zeroizing::new([0u8; 160]);
        rand::rngs::OsRng
            .try_fill_bytes(entropy.as_mut())
            .map_err(|_| Error::Entropy)?;
        Self::with_seed(config, &entropy[..128], entropy[128..].try_into().unwrap())
    }
    pub fn with_seed(config: Config, secret: &[u8], seed: [u8; 32]) -> Result<Self, Error> {
        config.validate()?;
        if secret.len() > 4096 {
            return Err(Error::Configuration);
        }
        let seed = Zeroizing::new(seed);
        Ok(Self {
            verified: vec![vec![]; config.verified_buckets],
            unverified: vec![vec![]; config.unverified_buckets],
            config,
            secret: Zeroizing::new(secret.to_vec()),
            random: StdRng::from_seed(*seed),
            peers: BTreeMap::new(),
            standby: BTreeSet::new(),
            all_verified: Lookup::default(),
            available_verified: Lookup::default(),
            available_unverified: Lookup::default(),
            last_time: None,
        })
    }
    pub fn config(&self) -> &Config {
        &self.config
    }
    fn time(&mut self, now: u64) -> Result<(), Error> {
        if self.last_time.is_some_and(|last| now < last) {
            return Err(Error::TimeReversed);
        }
        if now
            .checked_add(*self.config.standby_times.iter().max().unwrap())
            .is_none()
        {
            return Err(Error::TimeOverflow);
        }
        self.last_time = Some(now);
        Ok(())
    }
    pub fn find(&self, id: PeerPublicKey) -> Option<&Peer> {
        self.peers.get(&id).map(|e| &e.peer)
    }
    pub fn state(&self, id: PeerPublicKey) -> Option<State> {
        self.peers.get(&id).map(|p| State {
            class: p.class(),
            available: !p.selected && !self.standby.contains(&id),
            selected: p.selected,
            standby: self.standby.contains(&id),
            rejections: p.rejected,
            updated_at: p.updated,
            selected_at: p.selected_at,
            rejected_at: p.rejected_at,
            references: usize::from(p.verified.is_some()) + p.unverified.len(),
        })
    }
    pub fn all(&self, target: Target) -> impl Iterator<Item = &Peer> {
        self.peers
            .values()
            .filter(move |e| target.accepts(e.class()))
            .map(|e| &e.peer)
    }
    pub fn available(&self, target: Target) -> impl Iterator<Item = &Peer> {
        self.all(target)
            .filter(|p| !self.peers[&p.id].selected && !self.standby.contains(&p.id))
    }
    pub fn count(&self, count: Count, target: Target) -> usize {
        self.peers
            .iter()
            .filter(|(id, p)| {
                target.accepts(p.class())
                    && match count {
                        Count::All => true,
                        Count::Available => !p.selected && !self.standby.contains(id),
                        Count::Standby => self.standby.contains(id),
                    }
            })
            .count()
    }
    pub fn references(&self, target: Target) -> usize {
        self.peers
            .values()
            .filter(|p| target.accepts(p.class()))
            .map(|p| usize::from(p.verified.is_some()) + p.unverified.len())
            .sum()
    }
    pub fn update(&mut self, now: u64, peer: Peer) -> Result<Change<Update>, Error> {
        if peer.host.len() > self.config.maximum_host_bytes {
            return Err(Error::PeerLimit);
        }
        self.time(now)?;
        let mut effects = vec![];
        let id = peer.id;
        let trusted = peer.trusted;
        match self.peers.get_mut(&id) {
            Some(current) => {
                if current.peer.address != peer.address || current.peer.trusted != trusted {
                    return Ok(Change {
                        result: Update::Ignored,
                        effects,
                    });
                }
                current.peer.source = peer.source;
                current.updated = now;
            }
            None => {
                self.peers.insert(
                    id,
                    Entry {
                        peer,
                        verified: None,
                        unverified: vec![],
                        selected: false,
                        rejected: 0,
                        updated: now,
                        selected_at: None,
                        rejected_at: None,
                    },
                );
            }
        }
        if !trusted {
            effects.push(Effect::Persist(self.peers[&id].peer.clone()));
        }
        let result = if trusted {
            self.add_verified(now, id, &mut effects)
        } else {
            self.add_unverified(now, id, None, &mut effects)
        };
        if result == Update::Ignored {
            self.remove_peer(id, &mut effects);
        }
        Ok(Change { result, effects })
    }
    pub fn verify(&mut self, now: u64, id: PeerPublicKey) -> Result<Change<Update>, Error> {
        if !self.peers.contains_key(&id) {
            return Err(Error::Missing);
        }
        self.time(now)?;
        let mut effects = vec![];
        let result = self.add_verified(now, id, &mut effects);
        Ok(Change { result, effects })
    }
    pub fn select(&mut self, now: u64, id: PeerPublicKey) -> Result<(), Error> {
        let p = self.peers.get(&id).ok_or(Error::Missing)?;
        if p.selected {
            return Err(Error::Selected);
        }
        self.time(now)?;
        self.select_inner(now, id);
        Ok(())
    }
    fn select_inner(&mut self, now: u64, id: PeerPublicKey) {
        let p = self.peers.get_mut(&id).unwrap();
        p.selected = true;
        p.selected_at = Some(now);
        self.standby.remove(&id);
        self.unavailable(id);
    }
    pub fn release(&mut self, now: u64, id: PeerPublicKey) -> Result<(), Error> {
        self.time(now)?;
        if let Some(p) = self.peers.get_mut(&id).filter(|p| p.selected) {
            p.rejected = 0;
            p.rejected_at = None;
            p.selected = false;
            self.make_available(id);
        }
        Ok(())
    }
    pub fn reject(&mut self, now: u64, id: PeerPublicKey) -> Result<Change<()>, Error> {
        self.time(now)?;
        let mut effects = vec![];
        if let Some(p) = self.peers.get_mut(&id).filter(|p| p.selected) {
            p.rejected = p.rejected.saturating_add(1);
            p.rejected_at = Some(now);
            p.selected = false;
            if p.verified.is_some() {
                p.rejected = 0;
                p.rejected_at = None;
                self.remove_verified(id);
                if self.add_unverified(now, id, None, &mut effects) == Update::Ignored {
                    self.remove_peer(id, &mut effects);
                }
            } else if !p.peer.trusted && p.rejected > self.config.maximum_rejections {
                self.remove_peer(id, &mut effects);
            } else {
                self.standby.insert(id);
            }
        }
        Ok(Change {
            result: (),
            effects,
        })
    }
    pub fn delete(&mut self, id: PeerPublicKey) -> Result<Change<()>, Error> {
        let p = self.peers.get(&id).ok_or(Error::Missing)?;
        if p.selected {
            return Err(Error::Selected);
        }
        let mut effects = vec![];
        self.remove_peer(id, &mut effects);
        Ok(Change {
            result: (),
            effects,
        })
    }
    fn recovery_time(&self, id: PeerPublicKey) -> u64 {
        let p = &self.peers[&id];
        let delays = &self.config.standby_times;
        p.rejected_at.unwrap() + delays[(p.rejected.min(delays.len() as u64) - 1) as usize]
    }
    fn refresh(&mut self, now: u64) {
        let recovered: Vec<_> = self
            .standby
            .iter()
            .copied()
            .filter(|&id| now >= self.recovery_time(id))
            .collect();
        for id in recovered {
            self.standby.remove(&id);
            self.make_available(id);
        }
    }
    pub fn random_select(
        &mut self,
        now: u64,
        target: Target,
        mut filter: impl FnMut(&Peer) -> bool,
    ) -> Result<Selection, Error> {
        self.time(now)?;
        let order = match target {
            Target::Verified => vec![Class::Verified],
            Target::Unverified => vec![Class::Unverified],
            Target::Both => {
                if self.random.gen_range(0..1001)
                    <= (self.config.verified_probability * 1000.0).floor() as usize
                {
                    vec![Class::Verified, Class::Unverified]
                } else {
                    vec![Class::Unverified, Class::Verified]
                }
            }
        };
        self.refresh(now);
        for class in order {
            let mut ids = match class {
                Class::Verified => self.available_verified.ids.clone(),
                Class::Unverified => self.available_unverified.ids.clone(),
            };
            while !ids.is_empty() {
                let index = self.random.gen_range(0..ids.len());
                let id = ids.swap_remove(index);
                if filter(&self.peers[&id].peer) {
                    self.select_inner(now, id);
                    return Ok(Selection::Selected(self.peers[&id].peer.clone()));
                }
            }
        }
        let wait = self
            .standby
            .iter()
            .filter(|id| target.accepts(self.peers[id].class()) && filter(&self.peers[id].peer))
            .map(|&id| self.recovery_time(id) - now)
            .min();
        Ok(wait.map_or(Selection::Unavailable, Selection::Wait))
    }
    pub fn random_subset(
        &mut self,
        maximum: usize,
        mut filter: impl FnMut(&Peer) -> bool,
    ) -> Vec<Peer> {
        let mut ids = self.all_verified.ids.clone();
        let mut result = Vec::with_capacity(maximum.min(ids.len()));
        while !ids.is_empty() && result.len() < maximum {
            let index = self.random.gen_range(0..ids.len());
            let id = ids.swap_remove(index);
            if filter(&self.peers[&id].peer) {
                result.push(self.peers[&id].peer.clone());
            }
        }
        result
    }
    pub(super) fn unavailable(&mut self, id: PeerPublicKey) {
        self.available_verified.remove(id);
        self.available_unverified.remove(id);
    }
    pub(super) fn make_available(&mut self, id: PeerPublicKey) {
        let p = &self.peers[&id];
        if p.selected || self.standby.contains(&id) {
            return;
        }
        match p.class() {
            Class::Verified => self.available_verified.insert(id, &mut self.random),
            Class::Unverified => self.available_unverified.insert(id, &mut self.random),
        }
    }
}
