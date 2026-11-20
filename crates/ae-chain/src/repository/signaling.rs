use super::*;
use ae_codec::headers::{Header, KeyHeader};
use ae_protocol::signaling::{Fork, Prior};

#[cfg(test)]
#[path = "signaling_tests.rs"]
mod tests;

fn key_header<S: Read>(view: &View<S>, hash: Hash256) -> Result<KeyHeader, Error> {
    match view.require_block(hash)? {
        Block::Key(k) => Ok(k),
        _ => Err(Error::Corrupt("signaling key header")),
    }
}
fn matches(k: &KeyHeader, fork: Fork) -> u64 {
    u64::from(k.info == Some(fork.info.to_be_bytes()))
}
fn walk<S: Read>(
    view: &View<S>,
    block: &KeyHeader,
    fork: Fork,
    limit: usize,
) -> Result<u64, Error> {
    let mut current = block.clone();
    let mut count = 0u64;
    for _ in 0..limit {
        if !fork.contains(current.common.height) {
            return Err(Error::Corrupt("signaling interval ancestry"));
        }
        count += matches(&current, fork);
        if current.common.height == fork.start {
            return Ok(count);
        }
        let previous = key_header(view, Hash256(current.common.previous_key_hash))?;
        if previous.common.height.checked_add(1) != Some(current.common.height) {
            return Err(Error::Corrupt("signaling ancestor height"));
        }
        current = previous;
    }
    Err(Error::ResourceLimit)
}
fn encoded_count(count: u64) -> Vec<u8> {
    list(vec![number(1), number(count)]).encode()
}
impl<S: Read> View<S> {
    pub fn signal_count(&self, hash: Hash256) -> Result<Option<u64>, Error> {
        self.source
            .get(&key(SIGNALS, hash.0))?
            .map(|b| {
                let v = decode(&b)?;
                let f = fields(&v, 2)?;
                let n = integer(&f[1])?;
                if integer(&f[0])? != 1 || encoded_count(n) != b {
                    return Err(Error::Corrupt("signal count"));
                }
                Ok(n)
            })
            .transpose()
    }
    fn result_count(
        &self,
        block: &KeyHeader,
        fork: Fork,
        limit: usize,
    ) -> Result<(u64, bool), Error> {
        if block.common.height.checked_add(1) != Some(fork.end) {
            return Err(Error::NotLastSignalingBlock);
        }
        let hash = Header::Key(block.clone())
            .hash()
            .map_err(|_| Error::Corrupt("signal header hash"))?;
        if let Some(count) = self.signal_count(hash)? {
            if count > fork.end - fork.start {
                return Err(Error::Corrupt("signal count bound"));
            }
            Ok((count, false))
        } else {
            Ok((walk(self, block, fork, limit)?, true))
        }
    }
    pub fn fork_result(
        &self,
        network: &NetworkProfile,
        hash: Hash256,
        limit: usize,
    ) -> Result<bool, Error> {
        if !self.identity.matches(network) {
            return Err(Error::Identity);
        }
        let fork = network.community_fork().ok_or(Error::NoCommunityFork)?;
        Ok(fork.succeeds(self.result_count(&key_header(self, hash)?, fork, limit)?.0))
    }
    pub fn protocol_profile(
        &self,
        network: &NetworkProfile,
        height: u64,
        limit: usize,
    ) -> Result<NetworkProfile, Error> {
        Ok(self.resolve_protocol(network, height, limit)?.0)
    }
    pub(super) fn resolve_protocol(
        &self,
        network: &NetworkProfile,
        height: u64,
        limit: usize,
    ) -> Result<(NetworkProfile, Writes), Error> {
        if !self.identity.matches(network) {
            return Err(Error::Identity);
        }
        let mut writes = Writes::new();
        let mut prior = Prior::Missing;
        if let Some(fork) = network.community_fork()
            && height >= fork.end
            && let Some(hash) = self.canonical_key(height - 1)?
        {
            let previous = key_header(self, hash)?;
            if previous.common.height != height - 1 {
                return Err(Error::Corrupt("selected signaling height"));
            }
            prior = if height == fork.end {
                let (count, new) = self.result_count(&previous, fork, limit)?;
                if new {
                    writes.insert(key(SIGNALS, hash.0), Some(encoded_count(count)));
                }
                Prior::SignalingResult(fork.succeeds(count))
            } else {
                Prior::Version(previous.common.version)
            };
        }
        Ok((
            network.resolve(height, prior).map_err(Error::Protocol)?,
            writes,
        ))
    }
    pub(super) fn insert_signal(
        &self,
        block: &KeyHeader,
        fork: Fork,
        limit: usize,
        writes: &mut Writes,
    ) -> Result<(), Error> {
        if !fork.contains(block.common.height) {
            return Ok(());
        }
        let count =
            if let Some(previous) = self.signal_count(Hash256(block.common.previous_key_hash))? {
                if previous > block.common.height - fork.start {
                    return Err(Error::Corrupt("signal predecessor count"));
                }
                previous + matches(block, fork)
            } else {
                walk(self, block, fork, limit)?
            };
        let hash = Header::Key(block.clone())
            .hash()
            .map_err(|_| Error::Corrupt("signal header hash"))?;
        writes.insert(key(SIGNALS, hash.0), Some(encoded_count(count)));
        Ok(())
    }
}
