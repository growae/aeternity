use super::{Error, Parent, Rules};
use crate::{admission, block::Block};
use ae_codec::headers::Header;
use ae_hyperchain::{
    chain::{self, Reader, RunEnv},
    signing,
};
use ae_trie::NodeWriter;
use ae_types::{Hash256, Unsigned};
use ae_vm_fate::{maps::MapOrder, value::ObjectPayload};
use num_bigint::BigInt;

pub fn score(block: &Block) -> Unsigned {
    match block {
        Block::Key(k) => u64::from(k.target).into(),
        Block::Micro(_) => 0.into(),
    }
}
// Called only for competing forks. Direct extensions are always selected.
pub fn keep_tip(old: &Unsigned, new: &Unsigned, old_height: u64, new_height: u64) -> bool {
    old > new || (old == new && old_height > new_height)
}
pub(super) fn standalone<E, A, C>(
    block: &Block,
    parent: &Header,
    network: &ae_protocol::NetworkProfile,
    rules: &Rules,
) -> Result<(), Error<E, A, C>> {
    let h = block.header();
    h.encode().map_err(Error::Encoding)?;
    if h.common().height == 0 {
        return Err(Error::Genesis);
    }
    network
        .validate_version(h.common().height, h.common().version)
        .map_err(Error::Protocol)?;
    if let Block::Key(k) = block {
        signing::verify_key(k).map_err(Error::Seal)?;
    }
    if let Block::Micro(m) = block {
        let interval = if matches!(parent, Header::Micro(_)) {
            rules.micro_cycle_ms
        } else {
            1
        };
        if u128::from(m.header.time)
            < u128::from(ae_consensus::headers::timestamp(parent)) + u128::from(interval)
        {
            return Err(Error::MicroInterval);
        }
    }
    if u128::from(ae_consensus::headers::timestamp(&h)) >= u128::from(rules.now_ms) + 540_000 {
        return Err(Error::FutureTimestamp);
    }
    if let Block::Micro(m) = block {
        let gas = network.block_gas_limit(&rules.configured_gas_limit);
        admission::validate_body(m, &crate::rewards::number(&gas), rules.body_limits)
            .map_err(Error::Body)?;
    }
    Ok(())
}
pub(super) fn context<E, A, C>(
    block: &Block,
    parent: &Parent<'_, A, C>,
) -> Result<(), Error<E, A, C>> {
    let h = block.header();
    if h.common().previous_hash != parent.header.hash().map_err(Error::Encoding)?.0 {
        return Err(Error::PreviousHash);
    }
    let key = Header::Key(parent.key.clone())
        .hash()
        .map_err(Error::Encoding)?;
    if h.common().previous_key_hash != key.0 {
        return Err(Error::PreviousKeyHash);
    }
    match parent.header {
        Header::Key(_) if h.common().previous_hash != key.0 => return Err(Error::PreviousKeyHash),
        Header::Micro(m) if m.common.previous_key_hash != key.0 => {
            return Err(Error::PreviousKeyHash);
        }
        _ => {}
    }
    let expected = parent
        .header
        .common()
        .height
        .checked_add(u64::from(matches!(parent.header, Header::Key(_))))
        .ok_or(Error::Height)?;
    if h.common().height != expected {
        return Err(Error::Height);
    }
    Ok(())
}
pub(super) fn key_target<
    A: NodeWriter + Clone,
    C: NodeWriter + Clone,
    O: MapOrder,
    S: chain::Source<A, C>,
    E,
>(
    header: &ae_codec::headers::KeyHeader,
    parent: &Parent<'_, A, C>,
    reader: &Reader<A, C, O, S>,
    frame: chain::View<A, C>,
) -> Result<(), Error<E, A, C>> {
    if !header.end_of_epoch {
        let expected = u64::from(parent.key.target) + u64::from(!header.hole);
        return if u64::from(header.target) == expected {
            Ok(())
        } else {
            Err(Error::WrongTarget)
        };
    }
    if u64::from(header.target) != header.common.height {
        return Err(Error::InvalidEndOfEpoch);
    }
    let info = reader
        .finalize_info(&RunEnv::View(std::sync::Arc::new(frame)))
        .map_err(Error::Chain)?;
    match info {
        Some(i)
            if i.fork.as_deref() == Some(header.common.previous_key_hash.as_slice())
                && i.producer == ObjectPayload::Bytes(header.miner.to_vec()) =>
        {
            Ok(())
        }
        _ => Err(Error::InvalidEndOfEpoch),
    }
}
pub(super) fn leader<E, A, C>(
    header: &ae_codec::headers::KeyHeader,
    state: &ae_hyperchain::block::State,
) -> Result<(), Error<E, A, C>> {
    match state.leaders.valid_producer(
        &BigInt::from(header.common.height),
        &ObjectPayload::Bytes(header.miner.to_vec()),
        header.hole,
    ) {
        Ok(true) => Ok(()),
        Ok(false) => Err(Error::InvalidLeader),
        Err(_) => Err(Error::MissingLeader),
    }
}
pub(super) fn root<E, A: NodeWriter, C: NodeWriter>(
    trees: &ae_state::Trees<A, C>,
    header: &Header,
) -> Result<(), Error<E, A, C>> {
    let actual = trees.roots().hash();
    let expected = Hash256(header.common().state_hash);
    if actual == expected {
        Ok(())
    } else {
        Err(Error::StateHash { actual, expected })
    }
}
