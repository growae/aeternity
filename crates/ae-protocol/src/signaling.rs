// aec_hard_forks community-fork configuration and selected-chain decision.
// aeternity fa2a6d7. ISC notice in repository LICENSE.
use crate::{Activation, ProfileError, Protocol};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fork {
    pub start: u64,
    pub end: u64,
    pub threshold: u64,
    pub info: u32,
    pub version: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Prior {
    Missing,
    SignalingResult(bool),
    Version(u32),
}
impl Fork {
    pub fn validate(self, previous: Activation) -> Result<(), ProfileError> {
        if previous.height >= self.start
            || self.start >= self.end
            || self.threshold == 0
            || self.threshold > self.end - self.start
            || self.version <= Protocol::Minerva as u32
            || self.version <= previous.protocol as u32
        {
            return Err(ProfileError::InvalidFork);
        }
        Ok(())
    }
    pub fn contains(self, height: u64) -> bool {
        height >= self.start && height < self.end
    }
    pub fn info_at(self, height: u64) -> Option<u32> {
        self.contains(height).then_some(self.info)
    }
    pub fn succeeds(self, count: u64) -> bool {
        count >= self.threshold
    }
    pub fn version_at(
        self,
        height: u64,
        scheduled: Protocol,
        prior: Prior,
    ) -> Result<u32, ProfileError> {
        if height < self.end {
            return Ok(scheduled as u32);
        }
        match (height == self.end, prior) {
            (_, Prior::Missing) => Ok(self.version),
            (true, Prior::SignalingResult(true)) => Ok(self.version),
            (true, Prior::SignalingResult(false)) => Ok(scheduled as u32),
            (false, Prior::Version(version)) => Ok(version),
            _ => Err(ProfileError::WrongForkDecision),
        }
    }
}
