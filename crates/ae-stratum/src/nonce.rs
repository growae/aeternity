use crate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Extra,
    Miner,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Part {
    kind: Kind,
    value: u64,
    bytes: u8,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Nonce(pub u64);

impl Part {
    pub fn maximum(bytes: u8) -> Result<u64, Error> {
        if !(1..=7).contains(&bytes) {
            return Err(Error::Nonce);
        }
        Ok((1u64 << (8 * bytes)) - 1)
    }
    pub fn new(kind: Kind, value: u64, bytes: u8) -> Result<Self, Error> {
        if value > Self::maximum(bytes)? {
            return Err(Error::Nonce);
        }
        Ok(Self { kind, value, bytes })
    }
    pub fn kind(self) -> Kind {
        self.kind
    }
    pub fn value(self) -> u64 {
        self.value
    }
    pub fn bytes(self) -> u8 {
        self.bytes
    }
    pub fn complement(self) -> u8 {
        8 - self.bytes
    }
    pub fn to_hex(self) -> String {
        hex::encode(&self.value.to_le_bytes()[..self.bytes as usize])
    }
    pub fn from_hex(kind: Kind, text: &str, bytes: u8) -> Result<Self, Error> {
        Self::maximum(bytes)?;
        Self::new(kind, decode(text, bytes)?, bytes)
    }
    pub fn update(&mut self, value: u64) -> Result<(), Error> {
        let next = Self::new(self.kind, value, self.bytes)?;
        *self = next;
        Ok(())
    }
    pub fn merge(self, other: Self) -> Result<Nonce, Error> {
        if self.bytes + other.bytes != 8 || self.kind == other.kind {
            return Err(Error::Nonce);
        }
        let (extra, miner) = if self.kind == Kind::Extra {
            (self, other)
        } else {
            (other, self)
        };
        Ok(Nonce((extra.value << (8 * miner.bytes)) | miner.value))
    }
}
impl Nonce {
    pub fn to_hex(self) -> String {
        hex::encode(self.0.to_le_bytes())
    }
    pub fn from_hex(text: &str) -> Result<Self, Error> {
        decode(text, 8).map(Self)
    }
    // Both selectors return (extra, miner), as the original API does.
    pub fn split(self, kind: Kind, bytes: u8) -> Result<(Part, Part), Error> {
        Part::maximum(bytes)?;
        let miner_bytes = if kind == Kind::Miner {
            bytes
        } else {
            8 - bytes
        };
        let miner = Part::new(
            Kind::Miner,
            self.0 & Part::maximum(miner_bytes)?,
            miner_bytes,
        )?;
        let extra = Part::new(Kind::Extra, self.0 >> (8 * miner_bytes), 8 - miner_bytes)?;
        Ok((extra, miner))
    }
}
fn decode(text: &str, bytes: u8) -> Result<u64, Error> {
    if text.len() != usize::from(bytes) * 2 {
        return Err(Error::Hex);
    }
    let mut result = [0; 8];
    hex::decode_to_slice(text, &mut result[..bytes as usize]).map_err(|_| Error::Hex)?;
    Ok(u64::from_le_bytes(result))
}
