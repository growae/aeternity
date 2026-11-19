// aec_parent_chain_block, aeternity fa2a6d7.
// Copyright (c) 2022, Aeternity. ISC notice in repository LICENSE.
use ae_types::Unsigned;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ParentBlock {
    pub hash: Vec<u8>,
    pub height: Unsigned,
    pub previous_hash: Vec<u8>,
    pub time: Unsigned,
}

// Original binary inputs longer than 15 bytes raise a badmatch; they are not
// truncated. None models the original non-binary fallback.
pub fn encode_network_id(id: Option<&[u8]>) -> Result<[u8; 15], Vec<u8>> {
    let mut result = [0; 15];
    if let Some(id) = id {
        if id.len() > 15 {
            return Err(id.to_vec());
        }
        result[..id.len()].copy_from_slice(id);
    }
    Ok(result)
}
