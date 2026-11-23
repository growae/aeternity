// PoS-only entry point. PoW preparation uses repository::pow explicitly.
use super::{Error, Identity, Read, View};
use crate::block::Block;
use ae_codec::headers::Header;
use ae_hyperchain::chain;
use ae_protocol::ConsensusType;
use ae_trie::Overlay;
use ae_types::{AccountPublicKey, Hash256, Unsigned};
use num_bigint::{BigInt, Sign};
use std::sync::Arc;
pub mod candidate;
pub mod inputs;
pub mod journal;
pub mod producer;
pub use super::execution::{Config, Profile, Reader, Snapshot};

pub fn reader<S: Read, O>(
    view: View<S>,
    expected: &Identity,
    profile: Arc<Profile<O>>,
    config: Config,
) -> Result<Reader<S, O>, Error> {
    super::execution::reader(view, expected, ConsensusType::Pos, profile, config)
}
