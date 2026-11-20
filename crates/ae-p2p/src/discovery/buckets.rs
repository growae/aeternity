use super::*;
impl Pool {
    // Indices are one-based, matching the native salted bucket functions.
    pub fn verified_bucket_index(&self, address: Ipv4Addr) -> usize {
        let slot = hash_modulo(
            &[&self.secret, &address.octets()],
            self.config.verified_group_shard,
        ) as u8;
        hash_modulo(
            &[&self.secret, &address_group(address), &[slot]],
            self.config.verified_buckets,
        )
    }
    pub fn unverified_bucket_index(&self, source: Ipv4Addr, address: Ipv4Addr) -> usize {
        let source_slot = hash_modulo(
            &[&self.secret, &address_group(address)],
            self.config.unverified_source_shard / self.config.unverified_group_shard,
        ) as u8;
        let group_slot = hash_modulo(
            &[&self.secret, &address.octets()],
            self.config.unverified_group_shard,
        ) as u8;
        hash_modulo(
            &[
                &self.secret,
                &address_group(source),
                &[source_slot, group_slot],
            ],
            self.config.unverified_buckets,
        )
    }
    pub(super) fn remove_verified(&mut self, id: PeerPublicKey) {
        if let Some(index) = self.peers.get_mut(&id).unwrap().verified.take() {
            self.verified[index].retain(|&p| p != id);
            self.all_verified.remove(id);
            self.available_verified.remove(id);
        }
    }
    fn remove_unverified(&mut self, id: PeerPublicKey) {
        for index in std::mem::take(&mut self.peers.get_mut(&id).unwrap().unverified) {
            self.unverified[index].retain(|&p| p != id);
        }
        self.available_unverified.remove(id);
    }
    pub(super) fn remove_peer(&mut self, id: PeerPublicKey, effects: &mut Vec<Effect>) {
        self.remove_verified(id);
        self.remove_unverified(id);
        self.standby.remove(&id);
        let p = self.peers.remove(&id).unwrap();
        debug_assert!(!p.selected);
        effects.push(Effect::Delete(p.peer));
    }
    pub(super) fn add_verified(
        &mut self,
        now: u64,
        id: PeerPublicKey,
        effects: &mut Vec<Effect>,
    ) -> Update {
        if self.peers[&id].verified.is_some() {
            return Update::Verified;
        }
        let index = self.verified_bucket_index(self.peers[&id].peer.address) - 1;
        if !self.make_space(Class::Verified, index, now, Some(id), effects) {
            return if self.peers[&id].unverified.is_empty() {
                Update::Ignored
            } else {
                Update::Unverified
            };
        }
        self.verified[index].insert(0, id);
        self.peers.get_mut(&id).unwrap().verified = Some(index);
        self.all_verified.insert(id, &mut self.random);
        self.remove_unverified(id);
        self.make_available(id);
        Update::Verified
    }
    pub(super) fn add_unverified(
        &mut self,
        now: u64,
        id: PeerPublicKey,
        keep: Option<PeerPublicKey>,
        effects: &mut Vec<Effect>,
    ) -> Update {
        let p = &self.peers[&id];
        if p.verified.is_some() {
            return Update::Verified;
        }
        let refs = p.unverified.len();
        if refs > 0
            && (refs >= self.config.maximum_references
                || self.random.gen_range(0..(1u64 << refs)) != 0)
        {
            return Update::Unverified;
        }
        let index = self.unverified_bucket_index(p.peer.source, p.peer.address) - 1;
        if p.unverified.contains(&index) {
            return Update::Unverified;
        }
        if !self.make_space(Class::Unverified, index, now, keep, effects) {
            return if refs == 0 {
                Update::Ignored
            } else {
                Update::Unverified
            };
        }
        self.unverified[index].insert(0, id);
        self.peers.get_mut(&id).unwrap().unverified.insert(0, index);
        if refs == 0 {
            self.make_available(id);
        }
        Update::Unverified
    }
    fn make_space(
        &mut self,
        class: Class,
        index: usize,
        now: u64,
        keep: Option<PeerPublicKey>,
        effects: &mut Vec<Effect>,
    ) -> bool {
        let (bucket, capacity) = match class {
            Class::Verified => (&self.verified[index], self.config.verified_size),
            Class::Unverified => (&self.unverified[index], self.config.unverified_size),
        };
        if bucket.len() < capacity {
            return true;
        }
        let mut survivors = Vec::with_capacity(bucket.len());
        let mut stale = vec![];
        let mut evict = vec![];
        for &id in bucket.iter().rev() {
            let p = &self.peers[&id];
            if p.peer.trusted || p.selected || Some(id) == keep {
                survivors.push(id);
            } else if now - p.updated > self.config.maximum_update_lapse {
                stale.push(id);
            } else {
                survivors.push(id);
                evict.push(id);
            }
        }
        let remove = if !stale.is_empty() {
            stale
        } else if !evict.is_empty() {
            evict.sort_by_key(|id| match class {
                Class::Verified => self.peers[id].selected_at.unwrap_or(0),
                Class::Unverified => self.peers[id].updated,
            });
            let sample: f64 = self.random.sample(rand::distributions::Open01);
            let which = ((evict.len() as f64 * sample.powf(self.config.eviction_skew)).floor()
                as usize)
                .min(evict.len() - 1);
            vec![evict[which]]
        } else {
            return false;
        };
        let evicted = if remove.len() == 1 && evict.contains(&remove[0]) {
            Some(remove[0])
        } else {
            None
        };
        survivors.retain(|id| !remove.contains(id));
        match class {
            Class::Verified => self.verified[index] = survivors,
            Class::Unverified => self.unverified[index] = survivors,
        }
        for id in remove {
            match class {
                Class::Verified => {
                    self.peers.get_mut(&id).unwrap().verified = None;
                    self.all_verified.remove(id);
                    self.available_verified.remove(id);
                    if Some(id) != evicted
                        || self.add_unverified(now, id, keep, effects) == Update::Ignored
                    {
                        self.remove_peer(id, effects);
                    }
                }
                Class::Unverified => {
                    self.peers
                        .get_mut(&id)
                        .unwrap()
                        .unverified
                        .retain(|&i| i != index);
                    if self.peers[&id].unverified.is_empty() {
                        self.remove_peer(id, effects);
                    }
                }
            }
        }
        true
    }
}
