// aec_peer_connection, aeternity fa2a6d7. Copyright (c) 2018,
// Aeternity Anstalt. ISC notice: LICENSE.
pub const MAX_PAYLOAD: usize = 65_535 - 2 - 16;
pub const MAX_FRAGMENT: usize = MAX_PAYLOAD - 6;
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub message_bytes: usize,
    pub fragments: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            message_bytes: 16 * 1024 * 1024,
            fragments: 1024,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Truncated,
    InvalidSequence,
    ResourceLimit,
}
pub fn fragment(message: &[u8], limits: Limits) -> Result<Vec<Vec<u8>>, Error> {
    if message.len() > limits.message_bytes {
        return Err(Error::ResourceLimit);
    }
    if message.len() <= MAX_PAYLOAD {
        return Ok(vec![message.to_vec()]);
    }
    let count = message.len().div_ceil(MAX_FRAGMENT);
    if count > limits.fragments || count > usize::from(u16::MAX) {
        return Err(Error::ResourceLimit);
    }
    Ok(message
        .chunks(MAX_FRAGMENT)
        .enumerate()
        .map(|(i, b)| {
            let mut out = vec![0, 0];
            out.extend_from_slice(&((i + 1) as u16).to_be_bytes());
            out.extend_from_slice(&(count as u16).to_be_bytes());
            out.extend_from_slice(b);
            out
        })
        .collect())
}
#[derive(Debug)]
struct Partial {
    total: u16,
    next: u16,
    bytes: Vec<u8>,
}
#[derive(Debug)]
pub struct Assembler {
    limits: Limits,
    partial: Option<Partial>,
}
impl Assembler {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            partial: None,
        }
    }
    pub fn buffered_bytes(&self) -> usize {
        self.partial.as_ref().map_or(0, |p| p.bytes.len())
    }
    pub fn is_pending(&self) -> bool {
        self.partial.is_some()
    }
    pub fn reset(&mut self) {
        self.partial = None;
    }
    pub fn push(&mut self, packet: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let result = self.push_inner(packet);
        if result.is_err() {
            self.reset();
        }
        result
    }
    fn push_inner(&mut self, packet: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if packet.len() > MAX_PAYLOAD {
            return Err(Error::ResourceLimit);
        }
        let tag = packet.get(..2).ok_or(Error::Truncated)?;
        if tag != [0, 0] {
            if packet.len() > self.limits.message_bytes {
                return Err(Error::ResourceLimit);
            }
            return Ok(Some(packet.to_vec()));
        }
        let header = packet.get(2..6).ok_or(Error::Truncated)?;
        let n = u16::from_be_bytes([header[0], header[1]]);
        let total = u16::from_be_bytes([header[2], header[3]]);
        let bytes = &packet[6..];
        if total < 2 || n == 0 || n > total {
            return Err(Error::InvalidSequence);
        }
        if usize::from(total) > self.limits.fragments || bytes.len() > self.limits.message_bytes {
            return Err(Error::ResourceLimit);
        }
        if n == 1 {
            self.partial = Some(Partial {
                total,
                next: 2,
                bytes: bytes.to_vec(),
            });
            return Ok(None);
        }
        let p = self.partial.as_mut().ok_or(Error::InvalidSequence)?;
        if n != p.next || total != p.total {
            return Err(Error::InvalidSequence);
        }
        if p.bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.limits.message_bytes)
        {
            return Err(Error::ResourceLimit);
        }
        p.bytes.extend_from_slice(bytes);
        if n == total {
            return Ok(Some(self.partial.take().expect("active partial").bytes));
        }
        p.next += 1;
        Ok(None)
    }
}
