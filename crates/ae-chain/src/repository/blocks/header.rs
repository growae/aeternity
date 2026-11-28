use super::*;
use ae_codec::rlp::{self, Rlp};

// Metadata queries inspect the stored envelope and authenticate its header.
// All body bytes stay borrowed; no transaction records are read and neither
// transaction contents nor fraud proofs are validated here.
pub(in crate::repository) fn decode(
    bytes: &[u8],
    config: &ReadConfig,
) -> Result<HeaderRecord, Error> {
    if bytes.len() > config.max_block_bytes {
        return Err(Error::ResourceLimit);
    }
    let body = |bytes| {
        rlp::decode_with_limits(bytes, config.header_body()?).map_err(|error| {
            if error == rlp::DecodeError::ResourceLimit {
                Error::ResourceLimit
            } else {
                Error::Corrupt("block record RLP")
            }
        })
    };
    let mut seal_present = None;
    let header = if bytes.starts_with(b"\0aeblock") {
        let (payload, present) = payload(bytes)?;
        let value = body(payload)?;
        let Rlp::List(fields) = value else {
            return Err(Error::Corrupt("block record fields"));
        };
        let [
            Rlp::Bytes(raw),
            Rlp::List(hashes),
            Rlp::Bytes(proof),
            flags @ ..,
        ] = fields.as_slice()
        else {
            return Err(Error::Corrupt("block record fields"));
        };
        if !(if present {
            matches!(flags, [Rlp::Bytes([1])])
        } else {
            flags.is_empty()
        }) {
            return Err(Error::Corrupt("block seal presence"));
        }
        let header =
            Header::decode(raw, config.mode).map_err(|_| Error::Corrupt("stored header"))?;
        validate_presence(&header, present)?;
        if present {
            seal_present = Some(true);
        }
        if hashes.len() > config.max_transactions {
            return Err(Error::ResourceLimit);
        }
        if hashes
            .iter()
            .any(|hash| !matches!(hash, Rlp::Bytes(bytes) if bytes.len() == 32))
        {
            return Err(Error::Corrupt("transaction hash"));
        }
        if matches!(header, Header::Key(_)) && (!hashes.is_empty() || !proof.is_empty()) {
            return Err(Error::Corrupt("key body"));
        }
        header
    } else {
        let (header, payload) = Header::decode_partial(bytes, config.mode)
            .map_err(|_| Error::Corrupt("stored header"))?;
        if let Header::Micro(micro) = &header {
            let value = body(payload)?;
            let Rlp::List(fields) = value else {
                return Err(Error::Corrupt("legacy block fields"));
            };
            let [
                Rlp::Bytes(tag),
                Rlp::Bytes(version),
                Rlp::List(txs),
                Rlp::List(proofs),
            ] = fields.as_slice()
            else {
                return Err(Error::Corrupt("legacy block fields"));
            };
            if *tag != [101]
                || *version != Unsigned::from(u64::from(micro.common.version)).as_bytes()
            {
                return Err(Error::Corrupt("legacy block tag/version"));
            }
            if txs.len() > config.max_transactions {
                return Err(Error::ResourceLimit);
            }
            if proofs.len() > 1
                || txs
                    .iter()
                    .chain(proofs)
                    .any(|item| !matches!(item, Rlp::Bytes(_)))
            {
                return Err(Error::Corrupt("legacy block body"));
            }
        } else if !payload.is_empty() {
            return Err(Error::Corrupt("key body"));
        }
        header
    };
    Ok(HeaderRecord {
        header,
        seal_present,
    })
}
