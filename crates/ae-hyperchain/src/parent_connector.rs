// aec_parent_connector:responses_consensus/increment_seed, aeternity fa2a6d7.
// Copyright (c) 2022, Aeternity. ISC notice in repository LICENSE.
use crate::parent_block::ParentBlock;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response<N> {
    Block { block: ParentBlock, node: N },
    NotFound,
    OtherError,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Consensus<N> {
    Block { block: ParentBlock, node: N },
    NotFound,
    NoAgreement,
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub maximum_responses: usize,
    pub maximum_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            maximum_responses: 10_000,
            maximum_bytes: 64 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ResourceLimit,
    InvalidResponseCount,
}
// Responses are in aeu_lib:pmap's Good order (reverse configured-host order),
// not completion order. Failed workers still count toward configured_count.
// Agreement compares the entire block, including time and previous hash.
pub fn consensus<N: Clone>(
    responses: &[Response<N>],
    configured_count: usize,
    limits: Limits,
) -> Result<Consensus<N>, Error> {
    if responses.len() > configured_count {
        return Err(Error::InvalidResponseCount);
    }
    if responses.len() > limits.maximum_responses {
        return Err(Error::ResourceLimit);
    }
    let mut bytes = 0usize;
    let mut counts: BTreeMap<&ParentBlock, (&N, usize)> = BTreeMap::new();
    let mut not_found = 0usize;
    for response in responses {
        match response {
            Response::Block { block, node } => {
                for n in [
                    block.hash.len(),
                    block.previous_hash.len(),
                    block.height.as_bytes().len(),
                    block.time.as_bytes().len(),
                ] {
                    bytes = bytes.checked_add(n).ok_or(Error::ResourceLimit)?;
                    if bytes > limits.maximum_bytes {
                        return Err(Error::ResourceLimit);
                    }
                }
                counts.entry(block).or_insert((node, 0)).1 += 1;
            }
            Response::NotFound => not_found += 1,
            Response::OtherError => {}
        }
    }
    let threshold = configured_count / 2;
    if let Some((block, (node, _))) = counts
        .into_iter()
        .find(|(_, (_, count))| *count > threshold)
    {
        return Ok(Consensus::Block {
            block: block.clone(),
            node: node.clone(),
        });
    }
    Ok(if not_found > threshold {
        Consensus::NotFound
    } else {
        Consensus::NoAgreement
    })
}
// Invalid-size legacy seeds require fresh entropy; the caller supplies it.
// Valid 128-bit seeds increment with the original bit-syntax wraparound.
pub fn increment_seed(seed: &[u8]) -> Option<[u8; 16]> {
    let bytes = <[u8; 16]>::try_from(seed).ok()?;
    Some(u128::from_be_bytes(bytes).wrapping_add(1).to_be_bytes())
}
