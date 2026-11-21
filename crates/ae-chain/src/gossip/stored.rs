use super::*;
use crate::{
    block::Block,
    repository::{self, Read, View},
};
use ae_types::Hash256;

#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub now_ms: u64,
    pub micro_cycle_ms: u64,
    pub gossip_depth: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparationError {
    Gossip(Error),
    Repository(repository::Error),
}
impl From<Error> for PreparationError {
    fn from(error: Error) -> Self {
        Self::Gossip(error)
    }
}
impl From<repository::Error> for PreparationError {
    fn from(error: repository::Error) -> Self {
        Self::Repository(error)
    }
}
// One captured storage view supplies the known-block check, both parents,
// selected height. Transaction lookup then uses the same chain/pool snapshot.
pub fn precheck_stored<S: Read>(
    view: &View<S>,
    header: &Header,
    settings: Settings,
) -> Result<Precheck, PreparationError> {
    let hash = header.hash().map_err(Error::Header)?;
    if view.block(hash)?.is_some() {
        view.require_record(hash)?;
        return Ok(Precheck::Known);
    }
    let Header::Micro(_) = header else {
        return Err(Error::MicroExpected.into());
    };
    let previous_hash = Hash256(header.common().previous_hash);
    let previous = view.block(previous_hash)?.map(|b| b.header());
    let key = match &previous {
        Some(Header::Key(key)) => Some(key.clone()),
        Some(Header::Micro(_)) => match view.block(Hash256(header.common().previous_key_hash))? {
            Some(Block::Key(key)) => Some(key),
            Some(Block::Micro(_)) => {
                return Err(repository::Error::Corrupt("micro previous key").into());
            }
            None => None,
        },
        None => None,
    };
    let connected = if previous.is_some() {
        view.require_record(previous_hash)?;
        true
    } else {
        false
    };
    let top = view
        .status()
        .tip
        .map(|h| view.require_block(h).map(|b| b.header().common().height))
        .transpose()?;
    precheck(
        header,
        Environment {
            known: false,
            previous: previous.as_ref(),
            previous_key: key.as_ref(),
            connected,
            top_height: top,
            gossip_depth: settings.gossip_depth,
            now_ms: settings.now_ms,
            micro_cycle_ms: settings.micro_cycle_ms,
            hyperchain: view.config().mode == ae_codec::headers::HeaderMode::Hyperchain,
        },
    )
    .map_err(Into::into)
}
