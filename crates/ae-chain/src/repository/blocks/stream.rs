use super::*;
use ae_codec::rlp::{self, Rlp};
use ae_tx::{SignedTransaction, Transaction};

mod canonical;

// The block envelope is retained, but decoded transaction records are not.
// Construction validates the entire block before callers can read its state;
// accounting then makes another metered pass through the same snapshot.
pub(crate) struct StreamBlock<'a, S> {
    view: &'a View<S>,
    bytes: Vec<u8>,
    header: Header,
}
struct Envelope<'a> {
    header: Header,
    transactions: Vec<Rlp<'a>>,
    proof: &'a [u8],
    proof_shape: bool,
    modern: bool,
    canonical: bool,
}
impl<S: Read> View<S> {
    pub(crate) fn stream_block<E: From<Error>>(
        &self,
        hash: Hash256,
        proof_bytes: usize,
        checkpoint: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Option<StreamBlock<'_, S>>, E> {
        checkpoint()?;
        let Some(bytes) = self
            .source
            .get_bounded(&key(BLOCKS, hash.0), self.config.max_block_bytes)?
        else {
            return Ok(None);
        };
        let envelope = parse(&bytes, &self.config)?;
        if !envelope.modern {
            // Legacy Block::decode validates wire transactions and proof
            // before restoring the latest metadata from transaction records.
            for item in &envelope.transactions {
                checkpoint()?;
                let wire = blob(item)?;
                let tx = SignedTransaction::<Transaction>::decode_with_state_config(
                    wire,
                    self.config.transaction,
                    self.config.block().state,
                )
                .map_err(|e| read_error(crate::block::Error::Transaction(e)))?;
                if tx.encode() != wire {
                    return Err(Error::Corrupt("legacy block encoding").into());
                }
            }
            envelope.validate_proof(&self.config, proof_bytes)?;
            if !envelope.canonical {
                return Err(Error::Corrupt("legacy block encoding").into());
            }
        }
        envelope.visit(self, checkpoint, |_| Ok(()))?;
        if envelope.modern {
            envelope.validate_proof(&self.config, proof_bytes)?;
            if !envelope.canonical {
                return Err(Error::Corrupt("block record encoding").into());
            }
        }
        if envelope
            .header
            .hash()
            .map_err(|_| Error::Corrupt("block hash"))?
            != hash
        {
            return Err(Error::Corrupt("block identity").into());
        }
        let header = envelope.header.clone();
        drop(envelope);
        Ok(Some(StreamBlock {
            view: self,
            bytes,
            header,
        }))
    }
}
impl<S: Read> StreamBlock<'_, S> {
    pub(crate) fn header(&self) -> &Header {
        &self.header
    }
    pub(crate) fn for_each<E: From<Error>>(
        &self,
        checkpoint: &mut impl FnMut() -> Result<(), E>,
        visit: impl FnMut(&SignedTransaction<Transaction>) -> Result<(), E>,
    ) -> Result<(), E> {
        checkpoint()?;
        parse(&self.bytes, &self.view.config)?.visit(self.view, checkpoint, visit)
    }
}
impl Envelope<'_> {
    fn validate_proof(&self, config: &ReadConfig, proof_bytes: usize) -> Result<(), Error> {
        if !self.proof_shape {
            return Err(Error::Corrupt("stored fraud proof"));
        }
        if !self.proof.is_empty() {
            let limits = rlp::DecodeLimits {
                max_input_bytes: config.body.max_input_bytes.min(proof_bytes),
                // tag, version and three binary fields in one list.
                max_items: config.body.max_items.min(6),
                max_depth: config.body.max_depth.min(1),
            };
            let proof =
                fraud::Proof::decode(self.proof, config.mode, limits).map_err(fraud_error)?;
            if proof.encode().map_err(fraud_error)? != self.proof {
                return Err(Error::Corrupt("stored fraud proof"));
            }
        }
        Ok(())
    }
    fn visit<S: Read, E: From<Error>>(
        &self,
        view: &View<S>,
        checkpoint: &mut impl FnMut() -> Result<(), E>,
        mut visit: impl FnMut(&SignedTransaction<Transaction>) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut wire_payload = 0usize;
        for item in &self.transactions {
            checkpoint()?;
            let bytes = blob(item)?;
            let hash = if self.modern {
                Hash256(
                    bytes
                        .try_into()
                        .map_err(|_| Error::Corrupt("transaction hash"))?,
                )
            } else {
                // Wire canonicality was checked before any record lookup.
                ae_crypto::blake2b_256(bytes)
            };
            let tx = view
                .transaction(hash)?
                .ok_or(Error::Corrupt("missing transaction"))?;
            let encoded = tx.encode();
            if !self.modern && encoded != bytes {
                return Err(Error::Corrupt("legacy transaction binding").into());
            }
            wire_payload = wire_payload
                .checked_add(binary_size(&encoded)?)
                .ok_or(Error::ResourceLimit)?;
            if wire_payload > view.config.max_block_bytes {
                return Err(Error::ResourceLimit.into());
            }
            drop(encoded);
            visit(&tx)?;
            // tx, including restored call context and signatures, drops here.
        }
        if self.modern {
            // The stored hash-list can be much smaller than canonical block
            // transport. Preserve the full decoder's transport-size bound.
            let header = self
                .header
                .encode()
                .map_err(|_| Error::Corrupt("stored header"))?;
            let size = if let Header::Micro(m) = &self.header {
                let proof = if self.proof.is_empty() {
                    0
                } else {
                    binary_size(self.proof)?
                };
                let version = Unsigned::from(u64::from(m.common.version));
                let payload = 1usize
                    .checked_add(binary_size(version.as_bytes())?)
                    .and_then(|n| n.checked_add(framed_size(wire_payload).ok()?))
                    .and_then(|n| n.checked_add(framed_size(proof).ok()?))
                    .ok_or(Error::ResourceLimit)?;
                header
                    .len()
                    .checked_add(framed_size(payload)?)
                    .ok_or(Error::ResourceLimit)?
            } else {
                header.len()
            };
            if size > view.config.max_block_bytes {
                return Err(Error::ResourceLimit.into());
            }
        }
        Ok(())
    }
}
fn blob<'a>(value: &Rlp<'a>) -> Result<&'a [u8], Error> {
    if let Rlp::Bytes(bytes) = value {
        Ok(bytes)
    } else {
        Err(Error::Corrupt("block record fields"))
    }
}
fn framed_size(payload: usize) -> Result<usize, Error> {
    let prefix = if payload <= 55 {
        1
    } else {
        1 + (usize::BITS - payload.leading_zeros()).div_ceil(8) as usize
    };
    payload.checked_add(prefix).ok_or(Error::ResourceLimit)
}
fn binary_size(bytes: &[u8]) -> Result<usize, Error> {
    if matches!(bytes, [byte] if *byte <= 0x7f) {
        Ok(1)
    } else {
        framed_size(bytes.len())
    }
}
fn parse<'a>(bytes: &'a [u8], config: &ReadConfig) -> Result<Envelope<'a>, Error> {
    if bytes.len() > config.max_block_bytes {
        return Err(Error::ResourceLimit);
    }
    let modern = bytes.starts_with(b"\0aeblock");
    let (header, payload, canonical_header) = if modern {
        (
            None,
            bytes
                .strip_prefix(MAGIC)
                .ok_or(Error::Corrupt("block record version"))?,
            true,
        )
    } else {
        let (header, payload) = Header::decode_partial(bytes, config.mode)
            .map_err(|_| Error::Corrupt("stored header"))?;
        let canonical = header
            .encode()
            .map_err(|_| Error::Corrupt("stored header"))?
            == bytes[..bytes.len() - payload.len()];
        if matches!(header, Header::Key(_)) {
            return Ok(Envelope {
                header,
                transactions: vec![],
                proof: &[],
                proof_shape: true,
                modern,
                canonical: canonical && payload.is_empty(),
            });
        }
        (Some(header), payload, canonical)
    };
    let value = rlp::decode_with_limits(payload, config.header_body()?).map_err(|e| {
        if e == rlp::DecodeError::ResourceLimit {
            Error::ResourceLimit
        } else {
            Error::Corrupt("block record RLP")
        }
    })?;
    let canonical = canonical_header && canonical::matches(&value, payload)?;
    let Rlp::List(mut fields) = value else {
        return Err(Error::Corrupt("block record fields"));
    };
    let (header, proof, proof_shape, index, canonical) = if modern {
        let [Rlp::Bytes(raw), Rlp::List(_), Rlp::Bytes(proof)] = fields.as_slice() else {
            return Err(Error::Corrupt("block record fields"));
        };
        let header =
            Header::decode(raw, config.mode).map_err(|_| Error::Corrupt("stored header"))?;
        let canonical = canonical
            && header
                .encode()
                .map_err(|_| Error::Corrupt("stored header"))?
                == *raw;
        (header, *proof, true, 1, canonical)
    } else {
        let header = header.expect("legacy header");
        let [
            Rlp::Bytes(tag),
            Rlp::Bytes(version),
            Rlp::List(_),
            Rlp::List(proofs),
        ] = fields.as_slice()
        else {
            return Err(Error::Corrupt("legacy block fields"));
        };
        if *tag != [101]
            || *version != Unsigned::from(u64::from(header.common().version)).as_bytes()
        {
            return Err(Error::Corrupt("legacy block tag/version"));
        }
        if proofs.iter().any(|v| !matches!(v, Rlp::Bytes(_))) {
            return Err(Error::Corrupt("legacy block fields"));
        }
        // An explicit empty proof binary is invalid (unlike an empty list).
        let proof = proofs.first().map(blob).transpose()?.unwrap_or_default();
        let shape = proofs.is_empty() || (proofs.len() == 1 && !proof.is_empty());
        (header, proof, shape, 2, canonical)
    };
    let Rlp::List(transactions) = fields.swap_remove(index) else {
        unreachable!()
    };
    if transactions.len() > config.max_transactions {
        return Err(Error::ResourceLimit);
    }
    if transactions.iter().any(|v| !matches!(v, Rlp::Bytes(_))) {
        return Err(Error::Corrupt("block record fields"));
    }
    if modern && matches!(header, Header::Key(_)) && (!transactions.is_empty() || !proof.is_empty())
    {
        return Err(Error::Corrupt("key body"));
    }
    Ok(Envelope {
        header,
        transactions,
        proof,
        proof_shape,
        modern,
        canonical,
    })
}
