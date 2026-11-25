// aec_consensus_on_demand and generic aec_headers validation, fa2a6d7.
// Copyright (c) 2020, Aeternity Anstalt. ISC notice in repository LICENSE.
use crate::{
    headers::{self, Error},
    target::Target,
};
use ae_codec::headers::{Header, HeaderError, KeyHeader};
use ae_protocol::NetworkProfile;

pub const TAG: u32 = 1338;
pub fn seal(header: &mut KeyHeader) {
    // Native [1338] is not a 42-word seal, so its canonical encoding is zero.
    header.nonce = TAG.into();
    header.seal = [0; 42];
}
pub fn sealing_input(header: &KeyHeader) -> [u8; 32] {
    header.common.state_hash
}
pub fn validate_standalone(
    header: &Header,
    network: &NetworkProfile,
    now_ms: u64,
) -> Result<(), Error> {
    header.encode()?;
    if let Header::Key(k) = header {
        if k.hole || k.end_of_epoch {
            return Err(HeaderError::HyperchainFlags.into());
        }
        if k.common.height == 0 {
            return Err(Error::GenesisRequiresAnchor);
        }
        if k.target != TAG {
            return Err(Error::WrongTarget {
                actual: Target(k.target),
                expected: Target(TAG),
            });
        }
    }
    network.validate_version(header.common().height, header.common().version)?;
    // The plugin skips seal validation, but generic future-time validation
    // still runs for both key and micro blocks.
    if u128::from(headers::timestamp(header)) >= u128::from(now_ms) + 540_000 {
        return Err(Error::FutureTimestamp);
    }
    Ok(())
}
