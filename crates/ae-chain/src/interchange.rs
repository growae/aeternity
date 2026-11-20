use crate::{
    block::Block,
    repository::{ReadConfig, Roots},
};
use ae_crypto::blake2b_256;
use ae_trie::Value;
use ae_types::{Hash256, Unsigned};

#[derive(Debug)]
pub enum Error {
    Malformed(&'static str),
    UnsupportedMode,
    ResourceLimit,
    Block(crate::block::Error),
    Transaction(ae_tx::storage::Error),
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub manifest_bytes: usize,
    pub chunk_bytes: usize,
    pub chunks: usize,
    pub blocks_per_chunk: usize,
    pub blocks: u64,
    pub total_bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            manifest_bytes: 8 * 1024 * 1024,
            chunk_bytes: 64 * 1024 * 1024,
            chunks: 100_000,
            blocks_per_chunk: 1024,
            blocks: 100_000_000,
            total_bytes: 1 << 50,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub blocks: u64,
    pub bytes: u64,
    pub hash: Hash256,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub network: Vec<u8>,
    pub genesis: Hash256,
    pub source_revision: String,
    pub source_format: String,
    pub exporter_version: String,
    pub anchor: Hash256,
    pub anchor_key: Hash256,
    pub height: u64,
    pub protocol: u32,
    pub roots: Roots,
    pub blocks: u64,
    pub chunks: Vec<Chunk>,
}
fn binary(b: impl AsRef<[u8]>) -> Value {
    Value::Bytes(b.as_ref().to_vec())
}
fn number(n: u64) -> Value {
    binary(Unsigned::from(n).as_bytes())
}
fn list(values: Vec<Value>) -> Value {
    Value::List(values)
}
fn blob(v: &Value) -> Result<&[u8], Error> {
    if let Value::Bytes(b) = v {
        Ok(b)
    } else {
        Err(Error::Malformed("binary"))
    }
}
fn items(v: &Value) -> Result<&[Value], Error> {
    if let Value::List(v) = v {
        Ok(v)
    } else {
        Err(Error::Malformed("list"))
    }
}
fn fields(v: &Value, n: usize) -> Result<&[Value], Error> {
    let f = items(v)?;
    if f.len() == n {
        Ok(f)
    } else {
        Err(Error::Malformed("fields"))
    }
}
fn integer(v: &Value) -> Result<u64, Error> {
    let raw = blob(v)?;
    let number = Unsigned::from_field_bytes(raw).map_err(|_| Error::Malformed("integer"))?;
    if number.as_bytes() != raw {
        return Err(Error::Malformed("integer encoding"));
    }
    number
        .to_u64()
        .map_err(|_| Error::Malformed("integer range"))
}
fn hash(v: &Value) -> Result<Hash256, Error> {
    Hash256::try_from(blob(v)?).map_err(|_| Error::Malformed("hash"))
}
fn string(v: &Value) -> Result<String, Error> {
    String::from_utf8(blob(v)?.to_vec()).map_err(|_| Error::Malformed("text"))
}
fn decode(bytes: &[u8], bound: usize) -> Result<Value, Error> {
    let rlp = ae_codec::rlp::decode_with_limits(
        bytes,
        ae_codec::rlp::DecodeLimits {
            max_input_bytes: bound,
            max_items: 1_000_000,
            max_depth: 8,
        },
    )
    .map_err(|e| {
        if e == ae_codec::rlp::DecodeError::ResourceLimit {
            Error::ResourceLimit
        } else {
            Error::Malformed("RLP")
        }
    })?;
    let v = Value::from_rlp(&rlp);
    if v.encode() != bytes {
        return Err(Error::Malformed("noncanonical RLP"));
    }
    Ok(v)
}
impl Manifest {
    pub fn encode(&self) -> Vec<u8> {
        list(vec![
            binary(b"ae-replay"),
            number(1),
            number(1),
            binary(&self.network),
            binary(self.genesis.0),
            binary(&self.source_revision),
            binary(&self.source_format),
            binary(&self.exporter_version),
            binary(self.anchor.0),
            binary(self.anchor_key.0),
            number(self.height),
            number(self.protocol.into()),
            list(
                self.roots
                    .array()
                    .iter()
                    .map(|h| binary(h.as_ref().map_or(&[][..], |h| &h.0)))
                    .collect(),
            ),
            number(self.blocks),
            list(
                self.chunks
                    .iter()
                    .map(|c| list(vec![number(c.blocks), number(c.bytes), binary(c.hash.0)]))
                    .collect(),
            ),
        ])
        .encode()
    }
    pub fn id(&self) -> Hash256 {
        blake2b_256(&self.encode())
    }
    pub fn validate(&self, limits: Limits) -> Result<(), Error> {
        if self.network.is_empty()
            || self.network.len() > 256
            || self.protocol == 0
            || [
                &self.source_revision,
                &self.source_format,
                &self.exporter_version,
            ]
            .iter()
            .any(|s| s.is_empty() || s.len() > 256)
        {
            return Err(Error::Malformed("identity"));
        }
        if self.chunks.is_empty() || self.blocks == 0 {
            return Err(Error::Malformed("empty replay"));
        }
        if self.chunks.len() > limits.chunks
            || self.blocks > limits.blocks
            || self.encode().len() > limits.manifest_bytes
        {
            return Err(Error::ResourceLimit);
        }
        let mut blocks = 0u64;
        let mut bytes = 0u64;
        for c in &self.chunks {
            if c.blocks == 0 || c.bytes == 0 {
                return Err(Error::Malformed("empty chunk"));
            }
            if c.blocks > limits.blocks_per_chunk as u64 || c.bytes > limits.chunk_bytes as u64 {
                return Err(Error::ResourceLimit);
            }
            blocks = blocks.checked_add(c.blocks).ok_or(Error::ResourceLimit)?;
            bytes = bytes
                .checked_add(c.bytes)
                .filter(|n| *n <= limits.total_bytes)
                .ok_or(Error::ResourceLimit)?;
        }
        if blocks != self.blocks {
            return Err(Error::Malformed("block count"));
        }
        Ok(())
    }
    pub fn decode(bytes: &[u8], limits: Limits) -> Result<Self, Error> {
        let v = decode(bytes, limits.manifest_bytes)?;
        let f = fields(&v, 15)?;
        if blob(&f[0])? != b"ae-replay" || integer(&f[1])? != 1 {
            return Err(Error::Malformed("manifest version"));
        }
        if integer(&f[2])? != 1 {
            return Err(Error::UnsupportedMode);
        }
        let roots = fields(&f[12], 8)?
            .iter()
            .map(|v| {
                if blob(v)?.is_empty() {
                    Ok(None)
                } else {
                    hash(v).map(Some)
                }
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let manifest = Self {
            network: blob(&f[3])?.to_vec(),
            genesis: hash(&f[4])?,
            source_revision: string(&f[5])?,
            source_format: string(&f[6])?,
            exporter_version: string(&f[7])?,
            anchor: hash(&f[8])?,
            anchor_key: hash(&f[9])?,
            height: integer(&f[10])?,
            protocol: u32::try_from(integer(&f[11])?).map_err(|_| Error::Malformed("protocol"))?,
            roots: Roots {
                state: ae_state::StateRoots {
                    accounts: roots[0],
                    calls: roots[1],
                    channels: roots[2],
                    contracts: roots[3],
                    names: roots[4],
                    oracles: roots[5],
                },
                names: roots[6],
                oracles: roots[7],
            },
            blocks: integer(&f[13])?,
            chunks: items(&f[14])?
                .iter()
                .map(|v| {
                    let c = fields(v, 3)?;
                    Ok(Chunk {
                        blocks: integer(&c[0])?,
                        bytes: integer(&c[1])?,
                        hash: hash(&c[2])?,
                    })
                })
                .collect::<Result<_, Error>>()?,
        };
        manifest.validate(limits)?;
        if manifest.encode() != bytes {
            return Err(Error::Malformed("manifest encoding"));
        }
        Ok(manifest)
    }
}
pub fn transaction_records(
    block: &Block,
    config: &ReadConfig,
) -> Result<Vec<(Hash256, Vec<u8>)>, Error> {
    match block {
        Block::Key(_) => Ok(vec![]),
        Block::Micro(m) => m
            .transactions
            .iter()
            .map(|t| {
                Ok((
                    t.hash(),
                    ae_tx::storage::encode(t, config.transaction_record())
                        .map_err(Error::Transaction)?,
                ))
            })
            .collect(),
    }
}
pub fn encode_chunk(
    index: u64,
    blocks: &[Block],
    config: &ReadConfig,
    limits: Limits,
) -> Result<(Chunk, Vec<u8>), Error> {
    if blocks.is_empty() || blocks.len() > limits.blocks_per_chunk {
        return Err(Error::ResourceLimit);
    }
    let mut frames = vec![];
    let mut bytes = 0usize;
    for block in blocks {
        let wire = block.encode().map_err(Error::Block)?;
        let records = transaction_records(block, config)?;
        bytes = bytes.checked_add(wire.len()).ok_or(Error::ResourceLimit)?;
        for (_, record) in &records {
            bytes = bytes
                .checked_add(record.len())
                .ok_or(Error::ResourceLimit)?;
        }
        if bytes > limits.chunk_bytes {
            return Err(Error::ResourceLimit);
        }
        frames.push(list(vec![
            binary(wire),
            list(records.iter().map(|(_, v)| binary(v)).collect()),
        ]));
    }
    let encoded = list(vec![
        binary(b"ae-replay-chunk"),
        number(1),
        number(index),
        list(frames),
    ])
    .encode();
    if encoded.len() > limits.chunk_bytes {
        return Err(Error::ResourceLimit);
    }
    let descriptor = Chunk {
        blocks: blocks.len() as u64,
        bytes: encoded.len() as u64,
        hash: blake2b_256(&encoded),
    };
    // Exported bytes must survive the same typed, lossless decoder as imports.
    if decode_chunk(&encoded, index, &descriptor, config, limits)? != blocks {
        return Err(Error::Malformed("block round trip"));
    }
    Ok((descriptor, encoded))
}
pub fn decode_chunk(
    bytes: &[u8],
    index: u64,
    descriptor: &Chunk,
    config: &ReadConfig,
    limits: Limits,
) -> Result<Vec<Block>, Error> {
    if bytes.len() > limits.chunk_bytes || descriptor.blocks > limits.blocks_per_chunk as u64 {
        return Err(Error::ResourceLimit);
    }
    if descriptor.bytes != bytes.len() as u64 || descriptor.hash != blake2b_256(bytes) {
        return Err(Error::Malformed("chunk checksum"));
    }
    let v = decode(bytes, limits.chunk_bytes)?;
    let f = fields(&v, 4)?;
    if blob(&f[0])? != b"ae-replay-chunk" || integer(&f[1])? != 1 || integer(&f[2])? != index {
        return Err(Error::Malformed("chunk identity"));
    }
    let frames = items(&f[3])?;
    if frames.is_empty() || frames.len() as u64 != descriptor.blocks {
        return Err(Error::Malformed("chunk count"));
    }
    frames
        .iter()
        .map(|v| {
            let f = fields(v, 2)?;
            let wire = blob(&f[0])?;
            if wire.len() > config.max_block_bytes {
                return Err(Error::ResourceLimit);
            }
            let records = items(&f[1])?;
            if records.len() > config.max_transactions {
                return Err(Error::ResourceLimit);
            }
            // Stored internal calls may have a contract caller, which is not
            // admitted by the network transaction decoder. Restore records and
            // bind their canonical bytes to the complete transport instead.
            let (header, body) = ae_codec::headers::Header::decode_partial(wire, config.mode)
                .map_err(|e| Error::Block(e.into()))?;
            let block = match header {
                ae_codec::headers::Header::Key(header) => {
                    if !records.is_empty() || !body.is_empty() {
                        return Err(Error::Malformed("key transactions"));
                    }
                    Block::Key(header)
                }
                ae_codec::headers::Header::Micro(header) => {
                    use ae_codec::{object, rlp::Rlp};
                    let envelope = object::decode_envelope_with_limits(body, config.body)
                        .map_err(|e| Error::Block(e.into()))?;
                    if envelope.tag != object::ObjectTag::MicroBlock
                        || envelope.version != u64::from(header.common.version).into()
                    {
                        return Err(Error::Malformed("micro body identity"));
                    }
                    let [Rlp::List(txs), Rlp::List(proofs)] = envelope.fields.as_slice() else {
                        return Err(Error::Malformed("micro body fields"));
                    };
                    if records.len() != txs.len() {
                        return Err(Error::Malformed("transaction count"));
                    }
                    let mut transactions = Vec::with_capacity(records.len());
                    for (tx, record) in txs.iter().zip(records) {
                        let Rlp::Bytes(tx) = tx else {
                            return Err(Error::Malformed("transaction transport"));
                        };
                        let restored =
                            ae_tx::storage::decode(blob(record)?, config.transaction_record())
                                .map_err(Error::Transaction)?;
                        if restored.encode() != *tx {
                            return Err(Error::Malformed("transaction binding"));
                        }
                        transactions.push(restored);
                    }
                    let proof = match proofs.as_slice() {
                        [] => None,
                        [Rlp::Bytes(bytes)] => Some(Box::new(
                            crate::fraud::Proof::decode(bytes, config.mode, config.body)
                                .map_err(|e| Error::Block(e.into()))?,
                        )),
                        _ => return Err(Error::Malformed("fraud proof shape")),
                    };
                    Block::Micro(crate::block::MicroBlock {
                        header,
                        transactions,
                        proof,
                    })
                }
            };
            if block.encode().map_err(Error::Block)? != wire {
                return Err(Error::Malformed("block transport"));
            }
            Ok(block)
        })
        .collect()
}
