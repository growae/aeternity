// aec_peer_messages, aeternity fa2a6d7. Copyright (c) 2018,
// Aeternity Anstalt. ISC notice: LICENSE.
pub use ae_codec::object::Value;
use ae_codec::{
    object::{self, Field, FieldType, ObjectError},
    rlp::{self, Rlp},
};
use ae_types::Unsigned;
use std::collections::BTreeMap;
mod schema;
pub const MAX_NESTING: usize = 128;

macro_rules! kinds {
    ($($name:ident=$tag:literal => $wire:literal),+ $(,)?)=>{
        #[derive(Debug,Clone,Copy,PartialEq,Eq,PartialOrd,Ord)]
        #[repr(u16)]
        pub enum Kind { $($name=$tag),+ }
        impl Kind {
            pub const ALL: &'static [Self]=&[$(Self::$name),+];
            pub fn from_tag(tag:u16)->Result<Self,Error>{match tag{$($tag=>Ok(Self::$name)),+, _=>Err(Error::UnknownTag)}}
            pub fn name(self)->&'static str{match self{$(Self::$name=>$wire),+}}
        }
    };
}
kinds! {
    Ping=1=>"ping",GetHeaderByHash=3=>"get_header_by_hash",Header=4=>"header",
    GetSuccessors=5=>"get_n_successors",HeaderHashes=6=>"header_hashes",
    GetBlockTransactions=7=>"get_block_txs",GetGeneration=8=>"get_generation",
    Transactions=9=>"txs",KeyBlock=10=>"key_block",MicroBlock=11=>"micro_block",
    Generation=12=>"generation",BlockTransactions=13=>"block_txs",
    GetHeaderByHeight=15=>"get_header_by_height",PoolInit=20=>"txps_init",
    PoolUnfold=21=>"txps_unfold",PoolGet=22=>"txps_get",PoolFinish=23=>"txps_finish",
    Response=100=>"response",GetNodeInfo=125=>"get_node_info",NodeInfo=126=>"node_info",Close=127=>"close",
}
impl Kind {
    pub fn latest(self, protocol: u16) -> Result<u16, Error> {
        if protocol != 1 {
            return Err(Error::UnsupportedVersion);
        }
        Ok(
            if matches!(
                self,
                Self::Ping | Self::GetHeaderByHeight | Self::GetSuccessors
            ) {
                2
            } else {
                1
            },
        )
    }
}
pub fn ping_versions(minimum: u64) -> Vec<u16> {
    [1, 2]
        .into_iter()
        .filter(|v| u64::from(*v) >= minimum)
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    UnknownTag,
    UnsupportedVersion,
    InvalidMessage,
    ResourceLimit,
    Rlp(rlp::DecodeError),
    Field(ObjectError),
}
impl From<rlp::DecodeError> for Error {
    fn from(e: rlp::DecodeError) -> Self {
        if e == rlp::DecodeError::ResourceLimit {
            Self::ResourceLimit
        } else {
            Self::Rlp(e)
        }
    }
}
impl From<ObjectError> for Error {
    fn from(e: ObjectError) -> Self {
        Self::Field(e)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub body_bytes: usize,
    pub work_bytes: usize,
    pub items: usize,
    pub depth: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            body_bytes: 16 * 1024 * 1024,
            work_bytes: 64 * 1024 * 1024,
            items: 100_000,
            depth: 64,
        }
    }
}

struct Budget {
    limits: Limits,
    bytes: usize,
    items: usize,
}
impl Budget {
    fn new(mut limits: Limits) -> Self {
        limits.depth = limits.depth.min(MAX_NESTING);
        Self {
            limits,
            bytes: limits.work_bytes,
            items: limits.items,
        }
    }
    fn rlp<'a>(&mut self, bytes: &'a [u8], depth: usize) -> Result<Rlp<'a>, Error> {
        if depth >= self.limits.depth {
            return Err(Error::ResourceLimit);
        }
        let d = rlp::decode_one_detailed(
            bytes,
            rlp::DecodeLimits {
                max_input_bytes: self.bytes.min(self.limits.body_bytes),
                max_items: self.items,
                max_depth: self.limits.depth - depth,
            },
        )
        .map_err(|e| Error::from(e.error))?;
        if !d.tail.is_empty() {
            return Err(Error::Rlp(rlp::DecodeError::TrailingBytes));
        }
        self.bytes -= bytes.len();
        self.items -= d.items;
        Ok(d.value)
    }
    fn value(&mut self, value: &Value, depth: usize) -> Result<(), Error> {
        if depth >= self.limits.depth || self.items == 0 {
            return Err(Error::ResourceLimit);
        }
        self.items -= 1;
        let bytes = match value {
            Value::Integer(n) => n.as_bytes().len(),
            Value::Binary(b) => b.len(),
            Value::Boolean(_) => 1,
            Value::Id(_) => 33,
            Value::List(v) | Value::Tuple(v) => {
                for x in v {
                    self.value(x, depth + 1)?;
                }
                0
            }
            Value::Record(v) => {
                for x in v.values() {
                    self.value(x, depth + 1)?;
                }
                0
            }
        };
        self.bytes = self.bytes.checked_sub(bytes).ok_or(Error::ResourceLimit)?;
        Ok(())
    }
    fn record_bytes(&mut self, fields: &[&[u8]]) -> Result<(), Error> {
        if self.limits.depth < 2 {
            return Err(Error::ResourceLimit);
        }
        self.items = self
            .items
            .checked_sub(fields.len() + 1)
            .ok_or(Error::ResourceLimit)?;
        for bytes in fields {
            self.bytes = self
                .bytes
                .checked_sub(bytes.len())
                .ok_or(Error::ResourceLimit)?;
        }
        Ok(())
    }
}
fn list(items: &[Vec<u8>]) -> Vec<u8> {
    let size: usize = items.iter().map(Vec::len).sum();
    let mut out = vec![];
    if size <= 55 {
        out.push(0xc0 + size as u8);
    } else {
        let n = size.to_be_bytes();
        let start = n.iter().position(|b| *b != 0).expect("nonzero length");
        out.push(0xf7 + (n.len() - start) as u8);
        out.extend_from_slice(&n[start..]);
    }
    for item in items {
        out.extend_from_slice(item);
    }
    out
}
fn binary(v: &Value) -> Result<&[u8], Error> {
    if let Value::Binary(b) = v {
        Ok(b)
    } else {
        Err(Error::InvalidMessage)
    }
}
fn values(v: &Value) -> Result<&[Value], Error> {
    if let Value::List(v) = v {
        Ok(v)
    } else {
        Err(Error::InvalidMessage)
    }
}
fn record(v: Value) -> Result<BTreeMap<String, Value>, Error> {
    if let Value::Record(v) = v {
        Ok(v)
    } else {
        Err(Error::InvalidMessage)
    }
}
fn unsigned(v: &Value) -> Result<&Unsigned, Error> {
    if let Value::Integer(n) = v {
        Ok(n)
    } else {
        Err(Error::InvalidMessage)
    }
}
fn boolean(v: &Value) -> Result<bool, Error> {
    if let Value::Boolean(b) = v {
        Ok(*b)
    } else {
        Err(Error::InvalidMessage)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub host: Vec<u8>,
    pub port: Unsigned,
    pub pubkey: Vec<u8>,
}
impl Peer {
    pub fn fields(&self) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("host".into(), Value::Binary(self.host.clone())),
            ("port".into(), Value::Integer(self.port.clone())),
            ("pubkey".into(), Value::Binary(self.pubkey.clone())),
        ])
    }
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        Budget::new(limits).record_bytes(&[&self.host, self.port.as_bytes(), &self.pubkey])?;
        encode_value(&schema::peer(), &Value::Record(self.fields()), limits)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainPoi {
    pub height: Unsigned,
    pub root_hash: Vec<u8>,
    pub genesis: Vec<u8>,
    pub top: Vec<u8>,
    pub poi: Vec<u8>,
}
impl ChainPoi {
    pub fn fields(&self) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("height".into(), Value::Integer(self.height.clone())),
            ("root_hash".into(), Value::Binary(self.root_hash.clone())),
            ("genesis".into(), Value::Binary(self.genesis.clone())),
            ("top".into(), Value::Binary(self.top.clone())),
            ("poi".into(), Value::Binary(self.poi.clone())),
        ])
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Details {
    None,
    Ping {
        peers: Vec<Peer>,
        capabilities: Option<Result<Vec<ChainPoi>, Error>>,
    },
    Response {
        kind: Kind,
        message: Option<Result<Box<Message>, Error>>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    kind: Kind,
    version: u16,
    fields: BTreeMap<String, Value>,
    details: Details,
}
impl Message {
    pub fn kind(&self) -> Kind {
        self.kind
    }
    pub fn version(&self) -> u16 {
        self.version
    }
    pub fn fields(&self) -> &BTreeMap<String, Value> {
        &self.fields
    }
    pub fn details(&self) -> &Details {
        &self.details
    }
    pub fn new(
        kind: Kind,
        version: u16,
        fields: BTreeMap<String, Value>,
        limits: Limits,
    ) -> Result<Self, Error> {
        let body = encode_fields(kind, version, &fields, limits)?;
        Self::decode(kind as u16, &body, limits)
    }
    pub fn decode(tag: u16, body: &[u8], limits: Limits) -> Result<Self, Error> {
        decode(Kind::from_tag(tag)?, body, &mut Budget::new(limits), 0)
    }
    pub fn decode_packet(packet: &[u8], limits: Limits) -> Result<Self, Error> {
        let tag = packet.get(..2).ok_or(Error::InvalidMessage)?;
        Self::decode(u16::from_be_bytes([tag[0], tag[1]]), &packet[2..], limits)
    }
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        encode_fields(self.kind, self.version, &self.fields, limits)
    }
    pub fn encode_packet(&self, limits: Limits) -> Result<Vec<u8>, Error> {
        let mut out = (self.kind as u16).to_be_bytes().to_vec();
        out.extend(self.encode(limits)?);
        Ok(out)
    }
    pub fn response_ok(message: &Self, limits: Limits) -> Result<Self, Error> {
        Self::new(
            Kind::Response,
            1,
            BTreeMap::from([
                ("result".into(), Value::Boolean(true)),
                ("type".into(), Value::Integer((message.kind as u64).into())),
                ("reason".into(), Value::Binary(vec![])),
                ("object".into(), Value::Binary(message.encode(limits)?)),
            ]),
            limits,
        )
    }
    pub fn response_error(kind: Kind, reason: Vec<u8>, limits: Limits) -> Result<Self, Error> {
        Self::new(
            Kind::Response,
            1,
            BTreeMap::from([
                ("result".into(), Value::Boolean(false)),
                ("type".into(), Value::Integer((kind as u64).into())),
                ("reason".into(), Value::Binary(reason)),
                ("object".into(), Value::Binary(vec![])),
            ]),
            limits,
        )
    }
}
fn encode_fields(
    kind: Kind,
    version: u16,
    fields: &BTreeMap<String, Value>,
    limits: Limits,
) -> Result<Vec<u8>, Error> {
    let template = schema::template(kind, version)?;
    let mut budget = Budget::new(limits);
    let mut encoded = vec![object::encode_field(
        &FieldType::Integer,
        &Value::Integer(u64::from(version).into()),
    )?];
    for f in template {
        let v = fields.get(&f.name).ok_or(Error::InvalidMessage)?;
        budget.value(v, 0)?;
        encoded.push(object::encode_field(&f.kind, v)?);
    }
    let out = list(&encoded);
    if out.len() > limits.body_bytes {
        return Err(Error::ResourceLimit);
    }
    Ok(out)
}
fn encode_value(kind: &FieldType, value: &Value, limits: Limits) -> Result<Vec<u8>, Error> {
    Budget::new(limits).value(value, 0)?;
    let out = object::encode_field(kind, value)?;
    if out.len() > limits.body_bytes {
        return Err(Error::ResourceLimit);
    }
    Ok(out)
}
fn decode(kind: Kind, body: &[u8], budget: &mut Budget, depth: usize) -> Result<Message, Error> {
    let Rlp::List(raw) = budget.rlp(body, depth)? else {
        return Err(Error::InvalidMessage);
    };
    let version = object::decode_field(
        &FieldType::Integer,
        raw.first().ok_or(Error::InvalidMessage)?,
    )?;
    let version = u16::try_from(
        unsigned(&version)?
            .to_u64()
            .map_err(|_| Error::UnsupportedVersion)?,
    )
    .map_err(|_| Error::UnsupportedVersion)?;
    let template = schema::template(kind, version)?;
    if kind != Kind::Close && raw.len() != template.len() + 1 {
        return Err(Error::InvalidMessage);
    }
    let fields: BTreeMap<_, _> = template
        .iter()
        .zip(&raw[1..])
        .map(|(f, v)| Ok((f.name.clone(), object::decode_field(&f.kind, v)?)))
        .collect::<Result<_, Error>>()?;
    let details = match kind {
        Kind::Ping => {
            let mut peers = vec![];
            for p in values(&fields["peers"])? {
                let raw = budget.rlp(binary(p)?, depth + 1)?;
                let p = record(object::decode_field(&schema::peer(), &raw)?)?;
                peers.push(Peer {
                    host: binary(&p["host"])?.to_vec(),
                    port: unsigned(&p["port"])?.clone(),
                    pubkey: binary(&p["pubkey"])?.to_vec(),
                });
            }
            let capabilities = if version == 2 {
                let result = capabilities(binary(&fields["capabilities"])?, budget, depth + 1);
                if result == Err(Error::ResourceLimit) {
                    return Err(Error::ResourceLimit);
                }
                Some(result)
            } else {
                None
            };
            Details::Ping {
                peers,
                capabilities,
            }
        }
        Kind::Response => {
            let tag = u16::try_from(
                unsigned(&fields["type"])?
                    .to_u64()
                    .map_err(|_| Error::UnknownTag)?,
            )
            .map_err(|_| Error::UnknownTag)?;
            let kind = Kind::from_tag(tag)?;
            let message = if boolean(&fields["result"])? {
                let result =
                    decode(kind, binary(&fields["object"])?, budget, depth + 1).map(Box::new);
                if result == Err(Error::ResourceLimit) {
                    return Err(Error::ResourceLimit);
                }
                Some(result)
            } else {
                None
            };
            Details::Response { kind, message }
        }
        _ => Details::None,
    };
    Ok(Message {
        kind,
        version,
        fields,
        details,
    })
}
pub fn decode_capabilities(bytes: &[u8], limits: Limits) -> Result<Vec<ChainPoi>, Error> {
    capabilities(bytes, &mut Budget::new(limits), 0)
}
fn capabilities(bytes: &[u8], budget: &mut Budget, depth: usize) -> Result<Vec<ChainPoi>, Error> {
    let raw = budget.rlp(bytes, depth)?;
    let decoded = object::decode_field(
        &FieldType::List(Box::new(FieldType::Tuple(vec![
            FieldType::Binary,
            FieldType::Binary,
        ]))),
        &raw,
    )?;
    let mut out = vec![];
    for v in values(&decoded)? {
        let Value::Tuple(pair) = v else {
            return Err(Error::InvalidMessage);
        };
        // The original decodes even unknown capability payloads before filtering.
        let raw = budget.rlp(binary(&pair[1])?, depth + 1)?;
        if binary(&pair[0])? != b"chain_poi" {
            continue;
        }
        let Rlp::List(items) = raw else {
            return Err(Error::InvalidMessage);
        };
        if items.len() != 1 {
            return Err(Error::InvalidMessage);
        }
        let p = record(object::decode_field(&schema::poi(), &items[0])?)?;
        out.push(ChainPoi {
            height: unsigned(&p["height"])?.clone(),
            root_hash: binary(&p["root_hash"])?.to_vec(),
            genesis: binary(&p["genesis"])?.to_vec(),
            top: binary(&p["top"])?.to_vec(),
            poi: binary(&p["poi"])?.to_vec(),
        });
    }
    Ok(out)
}
pub fn encode_capabilities(capabilities: &[ChainPoi], limits: Limits) -> Result<Vec<u8>, Error> {
    let mut budget = Budget::new(limits);
    let mut groups = vec![];
    // Bound borrowed payloads before constructing any owned field-map copies.
    for p in capabilities {
        budget.record_bytes(&[
            p.height.as_bytes(),
            &p.root_hash,
            &p.genesis,
            &p.top,
            &p.poi,
        ])?;
    }
    for p in capabilities {
        let p = Value::Record(p.fields());
        let inner = list(&[object::encode_field(&schema::poi(), &p)?]);
        groups.push(Value::Tuple(vec![
            Value::Binary(b"chain_poi".to_vec()),
            Value::Binary(inner),
        ]));
    }
    encode_value(
        &FieldType::List(Box::new(FieldType::Tuple(vec![
            FieldType::Binary,
            FieldType::Binary,
        ]))),
        &Value::List(groups),
        limits,
    )
}
