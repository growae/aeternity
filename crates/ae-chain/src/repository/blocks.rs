use super::*;
use crate::block::MicroBlock;
use ae_codec::headers::Header;

mod header;
mod stream;
pub(super) use header::decode as header;

const MAGIC: &[u8] = b"\0aeblock\x01";
const PRESENT_MAGIC: &[u8] = b"\0aeblock\x02";
fn payload(bytes: &[u8]) -> Result<(&[u8], bool), Error> {
    if let Some(payload) = bytes.strip_prefix(MAGIC) {
        Ok((payload, false))
    } else if let Some(payload) = bytes.strip_prefix(PRESENT_MAGIC) {
        Ok((payload, true))
    } else {
        Err(Error::Corrupt("block record version"))
    }
}
fn validate_presence(header: &Header, present: bool) -> Result<(), Error> {
    if present && !matches!(header, Header::Key(k) if k.seal == [0; 42]) {
        return Err(Error::Corrupt("block seal presence"));
    }
    Ok(())
}
fn bounded(block: &Block, config: &ReadConfig) -> Result<(), Error> {
    let mut size = block
        .header()
        .encode()
        .map_err(|_| Error::UnsupportedBlock)?
        .len();
    if let Block::Micro(m) = block {
        if m.transactions.len() > config.max_transactions {
            return Err(Error::ResourceLimit);
        }
        for tx in &m.transactions {
            size = size
                .checked_add(tx.encode().len())
                .ok_or(Error::ResourceLimit)?;
            if size > config.max_block_bytes {
                return Err(Error::ResourceLimit);
            }
        }
    }
    if block.encode().map_err(|_| Error::UnsupportedBlock)?.len() > config.max_block_bytes {
        return Err(Error::ResourceLimit);
    }
    Ok(())
}
pub(super) fn encode(block: &Block, config: &ReadConfig) -> Result<Vec<u8>, Error> {
    encode_with_presence(block, config, false)
}
pub(super) fn encode_with_presence(
    block: &Block,
    config: &ReadConfig,
    present: bool,
) -> Result<Vec<u8>, Error> {
    bounded(block, config)?;
    let present = present && matches!(block, Block::Key(k) if k.seal == [0; 42]);
    let header = block
        .header()
        .encode()
        .map_err(|_| Error::UnsupportedBlock)?;
    if Header::decode(&header, config.mode).map_err(|_| Error::UnsupportedBlock)? != block.header()
    {
        return Err(Error::UnsupportedBlock);
    }
    let (hashes, proof) = match block {
        Block::Key(_) => (vec![], vec![]),
        Block::Micro(m) => (
            m.transactions
                .iter()
                .map(|tx| binary(tx.hash().0))
                .collect(),
            m.proof
                .as_ref()
                .map(|p| p.encode())
                .transpose()
                .map_err(|_| Error::UnsupportedBlock)?
                .unwrap_or_default(),
        ),
    };
    if let Block::Micro(m) = block
        && let Some(original) = &m.proof
        && fraud::Proof::decode(&proof, config.mode, config.body).map_err(fraud_error)?
            != **original
    {
        return Err(Error::UnsupportedBlock);
    }
    let mut fields = vec![binary(header), list(hashes), binary(proof)];
    if present {
        fields.push(binary([1]));
    }
    let payload = list(fields).encode();
    ae_codec::rlp::decode_with_limits(&payload, config.body).map_err(|_| Error::ResourceLimit)?;
    let mut out = if present { PRESENT_MAGIC } else { MAGIC }.to_vec();
    out.extend(payload);
    if out.len() > config.max_block_bytes {
        return Err(Error::ResourceLimit);
    }
    Ok(out)
}
pub(super) fn decode<S: Read>(view: &View<S>, bytes: &[u8]) -> Result<Block, Error> {
    decode_with_transactions(view, bytes, |hash| view.transaction(hash))
}
pub(super) fn decode_with_transactions<S: Read>(
    view: &View<S>,
    bytes: &[u8],
    mut transaction: impl FnMut(
        Hash256,
    )
        -> Result<Option<ae_tx::SignedTransaction<ae_tx::Transaction>>, Error>,
) -> Result<Block, Error> {
    let config = view.config();
    if bytes.len() > config.max_block_bytes {
        return Err(Error::ResourceLimit);
    }
    if !bytes.starts_with(b"\0aeblock") {
        let mut block = Block::decode(bytes, config.block()).map_err(read_error)?;
        if block
            .encode()
            .map_err(|_| Error::Corrupt("legacy block encoding"))?
            != bytes
        {
            return Err(Error::Corrupt("legacy block encoding"));
        }
        if let Block::Micro(m) = &mut block {
            for tx in &mut m.transactions {
                let latest =
                    transaction(tx.hash())?.ok_or(Error::Corrupt("missing transaction"))?;
                if latest.encode() != tx.encode() {
                    return Err(Error::Corrupt("legacy transaction binding"));
                }
                *tx = latest;
            }
        }
        return Ok(block);
    }
    let (payload, present) = payload(bytes)?;
    let rlp = ae_codec::rlp::decode_with_limits(payload, config.body).map_err(|e| {
        if e == ae_codec::rlp::DecodeError::ResourceLimit {
            Error::ResourceLimit
        } else {
            Error::Corrupt("block record RLP")
        }
    })?;
    let value = Value::from_rlp(&rlp);
    let f = fields(&value, if present { 4 } else { 3 })?;
    if present && blob(&f[3])? != [1] {
        return Err(Error::Corrupt("block seal presence"));
    }
    let header =
        Header::decode(blob(&f[0])?, config.mode).map_err(|_| Error::Corrupt("stored header"))?;
    validate_presence(&header, present)?;
    let hashes = items(&f[1])?;
    if hashes.len() > config.max_transactions {
        return Err(Error::ResourceLimit);
    }
    let proof = blob(&f[2])?;
    let block = match header {
        Header::Key(k) => {
            if !hashes.is_empty() || !proof.is_empty() {
                return Err(Error::Corrupt("key body"));
            }
            Block::Key(k)
        }
        Header::Micro(header) => {
            let mut transactions = Vec::new();
            let mut used = 0usize;
            for h in hashes {
                let tx = transaction(hash(h)?)?.ok_or(Error::Corrupt("missing transaction"))?;
                used = used
                    .checked_add(tx.encode().len())
                    .ok_or(Error::ResourceLimit)?;
                if used > config.max_block_bytes {
                    return Err(Error::ResourceLimit);
                }
                transactions.push(tx);
            }
            let proof = if proof.is_empty() {
                None
            } else {
                Some(Box::new(
                    fraud::Proof::decode(proof, config.mode, config.body).map_err(fraud_error)?,
                ))
            };
            Block::Micro(MicroBlock {
                header,
                transactions,
                proof,
            })
        }
    };
    if encode_with_presence(&block, config, present)? != bytes {
        return Err(Error::Corrupt("block record encoding"));
    }
    Ok(block)
}
fn fraud_error(error: fraud::Error) -> Error {
    if matches!(
        error,
        fraud::Error::Object(ae_codec::object::ObjectError::Rlp(
            ae_codec::rlp::DecodeError::ResourceLimit
        ))
    ) {
        Error::ResourceLimit
    } else {
        Error::Corrupt("stored fraud proof")
    }
}
fn read_error(error: crate::block::Error) -> Error {
    match error {
        crate::block::Error::Transaction(e) if e.is_operational() => {
            Error::TransactionStorage(ae_tx::storage::Error::Transaction(e))
        }
        e if e.is_operational() => Error::ResourceLimit,
        _ => Error::Corrupt("legacy block encoding"),
    }
}
