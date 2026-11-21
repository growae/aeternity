use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chain {
    id: u64,
    peers: Vec<PeerPublicKey>,
    blocks: Vec<Point>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    First,
    Second,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    Equal,
    Different,
    Need { side: Side, height: u64 },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskMatch {
    New,
    Existing(usize),
    Need { chain: u64, height: u64 },
}
impl Chain {
    pub fn new(
        id: u64,
        peers: Vec<PeerPublicKey>,
        blocks: Vec<Point>,
        limits: Limits,
    ) -> Result<Self, Error> {
        let chain = Self { id, peers, blocks };
        chain.validate(limits)?;
        Ok(chain)
    }
    pub fn from_header(
        id: u64,
        peer: PeerPublicKey,
        header: &ae_codec::headers::Header,
        limits: Limits,
    ) -> Result<Self, Error> {
        let top = super::super::wire::target(header).map_err(|_| Error::State)?;
        let mut blocks = vec![top];
        if let ae_codec::headers::Header::Key(key) = header
            && top.height > 1
        {
            blocks.push(Point {
                height: top.height - 1,
                hash: Hash256(key.common.previous_key_hash),
            });
        }
        Self::new(id, vec![peer], blocks, limits)
    }
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn peers(&self) -> &[PeerPublicKey] {
        &self.peers
    }
    pub fn blocks(&self) -> &[Point] {
        &self.blocks
    }
    pub fn target(&self) -> Point {
        self.blocks[0]
    }
    pub fn with_id(mut self, id: u64) -> Self {
        self.id = id;
        self
    }
    pub(super) fn remove_peer(&mut self, peer: PeerPublicKey) {
        if let Some(index) = self.peers.iter().position(|p| *p == peer) {
            self.peers.remove(index);
        }
    }
    pub(super) fn validate(&self, limits: Limits) -> Result<(), Error> {
        limits.validate()?;
        if self.blocks.is_empty() {
            return Err(Error::Empty);
        }
        if self.blocks.len() > limits.hints || self.peers.len() > limits.peers {
            return Err(Error::Capacity);
        }
        if self.blocks.windows(2).any(|p| p[0].height < p[1].height) {
            return Err(Error::Order);
        }
        Ok(())
    }
    fn at(&self, height: u64) -> Option<Hash256> {
        self.blocks
            .iter()
            .take_while(|p| p.height >= height)
            .find(|p| p.height == height)
            .map(|p| p.hash)
    }
    pub fn compare(&self, other: &Self) -> Comparison {
        let (side, point, found) = if self.target().height > other.target().height {
            (Side::First, other.target(), self.at(other.target().height))
        } else {
            (Side::Second, self.target(), other.at(self.target().height))
        };
        match found {
            Some(hash) if hash == point.hash => Comparison::Equal,
            Some(_) => Comparison::Different,
            None => Comparison::Need {
                side,
                height: point.height,
            },
        }
    }
    // A threshold can exceed u64::MAX when computing height + chunk size;
    // Erlang uses arbitrary integers for that addition.
    pub fn next_known_hash(&self, height: u128) -> Hash256 {
        self.blocks
            .iter()
            .take_while(|p| u128::from(p.height) > height)
            .last()
            .unwrap_or(&self.blocks[0])
            .hash
    }
    pub fn merge(&self, other: &Self, limits: Limits) -> Result<Self, Error> {
        self.validate(limits)?;
        other.validate(limits)?;
        if self.id != other.id {
            return Err(Error::Identity);
        }
        let mut peers = self.peers.clone();
        peers.extend_from_slice(&other.peers);
        peers.sort_unstable();
        peers.dedup();
        let blocks = super::merge::hints(&self.blocks, &other.blocks);
        Self::new(self.id, peers, blocks, limits)
    }
}
pub fn match_tasks(candidate: &Chain, tasks: &[Chain]) -> TaskMatch {
    let mut missing = None;
    for (index, task) in tasks.iter().enumerate() {
        match candidate.compare(task) {
            Comparison::Equal => return TaskMatch::Existing(index),
            Comparison::Different => (),
            Comparison::Need { side, height } => {
                missing.get_or_insert(TaskMatch::Need {
                    chain: match side {
                        Side::First => candidate.id,
                        Side::Second => task.id,
                    },
                    height,
                });
            }
        }
    }
    missing.unwrap_or(TaskMatch::New)
}
