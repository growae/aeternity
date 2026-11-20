use super::*;
use ae_chain::block::DecodeConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Backward,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Message,
    Direction,
    Block(ae_chain::block::Error),
    KeyExpected,
    MicroExpected,
    HashMismatch,
    NonConsecutive,
    ResourceLimit,
    Encoding,
}
pub struct Generation {
    key: Block,
    micros: Vec<Block>,
    direction: Direction,
}
impl Generation {
    pub fn key(&self) -> &Block {
        &self.key
    }
    pub fn blocks(&self) -> impl Iterator<Item = &Block> {
        self.micros
            .iter()
            .chain((self.direction == Direction::Backward).then_some(&self.key))
    }
    pub fn decode(
        message: &Message,
        direction: Direction,
        config: DecodeConfig<'_>,
        maximum: usize,
    ) -> Result<Self, Error> {
        if message.kind() != Kind::Generation {
            return Err(Error::Message);
        }
        let fields = message.fields();
        let (Some(Value::Binary(key)), Some(Value::List(micros)), Some(Value::Boolean(forward))) = (
            fields.get("key_block"),
            fields.get("micro_blocks"),
            fields.get("forward"),
        ) else {
            return Err(Error::Message);
        };
        if *forward != (direction == Direction::Forward) {
            return Err(Error::Direction);
        }
        if micros.len().checked_add(1).is_none_or(|n| n > maximum) {
            return Err(Error::ResourceLimit);
        }
        // Native connection decoding checks the key first, then every micro;
        // no accepted prefix exists when any serialized block is malformed.
        let key = Block::decode(key, config).map_err(Error::Block)?;
        if !matches!(key, Block::Key(_)) {
            return Err(Error::KeyExpected);
        }
        let mut blocks = Vec::with_capacity(micros.len() + 1);
        for value in micros {
            let Value::Binary(bytes) = value else {
                return Err(Error::Message);
            };
            let b = Block::decode(bytes, config).map_err(Error::Block)?;
            if !matches!(b, Block::Micro(_)) {
                return Err(Error::MicroExpected);
            }
            blocks.push(b);
        }
        Ok(Self {
            key,
            micros: blocks,
            direction,
        })
    }
    // peer_get_generation validates the returned key before the sync helper
    // reaches this check, including for a forward response whose key will not
    // be inserted. Keep that admission boundary outside the structural gate.
    pub fn check(
        &self,
        requested: Hash256,
        mode: ae_codec::headers::HeaderMode,
    ) -> Result<(), Error> {
        if self.direction == Direction::Backward {
            if self.key.hash().map_err(|_| Error::Encoding)? != requested {
                return Err(Error::HashMismatch);
            }
            let mut previous = None;
            for block in self.blocks() {
                let Some(prior): Option<&Block> = previous.replace(block) else {
                    continue;
                };
                let first = prior.header();
                let second = block.header();
                let height = if matches!(block, Block::Key(_))
                    && mode == ae_codec::headers::HeaderMode::ProofOfWork
                {
                    first.common().height.checked_add(1)
                } else {
                    Some(first.common().height)
                };
                if height != Some(second.common().height)
                    || second.common().previous_hash != prior.hash().map_err(|_| Error::Encoding)?.0
                {
                    return Err(Error::NonConsecutive);
                }
            }
        }
        // Native forward insertion deliberately ignores the supplied key and
        // leaves micro context/admission to the conductor. Do not add a
        // backward hash/adjacency gate here.
        Ok(())
    }
}
