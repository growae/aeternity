// Private parent/worker protocol, not a network or consensus encoding.
use ae_worker_heap::{Guard, Limit, Source};
use std::io::{self, Read, Write};

pub const MAX_FRAME: usize = 64 << 20;
pub const BOOT_BYTES: usize = 17;
pub const HEADER_BYTES: usize = 13;
pub(crate) const INIT: u8 = 1;
pub(crate) const READ: u8 = 2;
pub(crate) const VALUE: u8 = 3;
pub(crate) const RESULT: u8 = 4;
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Protocol,
    Capacity,
    Arm(ae_worker_heap::ArmError),
}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
pub(crate) fn bootstrap(limit: Limit, frame: usize) -> Result<[u8; BOOT_BYTES], Error> {
    if limit.bytes == 0 || limit.bytes == usize::MAX || !(1..=MAX_FRAME).contains(&frame) {
        return Err(Error::Capacity);
    }
    let mut bytes = [0; BOOT_BYTES];
    bytes[..4].copy_from_slice(b"AEW1");
    bytes[4..12].copy_from_slice(
        &u64::try_from(limit.bytes)
            .map_err(|_| Error::Capacity)?
            .to_be_bytes(),
    );
    bytes[12] = match limit.source {
        Source::Native => 0,
        Source::Service => 1,
    };
    bytes[13..].copy_from_slice(&(frame as u32).to_be_bytes());
    Ok(bytes)
}
fn boot(bytes: [u8; BOOT_BYTES]) -> Result<(Limit, usize), Error> {
    if &bytes[..4] != b"AEW1" {
        return Err(Error::Protocol);
    }
    let limit = Limit {
        bytes: usize::try_from(u64::from_be_bytes(bytes[4..12].try_into().unwrap()))
            .map_err(|_| Error::Capacity)?,
        source: match bytes[12] {
            0 => Source::Native,
            1 => Source::Service,
            _ => return Err(Error::Protocol),
        },
    };
    let frame = u32::from_be_bytes(bytes[13..].try_into().unwrap()) as usize;
    bootstrap(limit, frame)?;
    Ok((limit, frame))
}
#[derive(Debug, Clone, Copy)]
pub(crate) struct Header {
    pub kind: u8,
    pub id: u64,
    pub bytes: usize,
}
impl Header {
    pub(crate) fn encode(self, maximum: usize) -> Result<[u8; HEADER_BYTES], Error> {
        if !matches!(self.kind, INIT | READ | VALUE | RESULT) {
            return Err(Error::Protocol);
        }
        if self.bytes > maximum || maximum > MAX_FRAME {
            return Err(Error::Capacity);
        }
        let mut out = [0; HEADER_BYTES];
        out[0] = self.kind;
        out[1..9].copy_from_slice(&self.id.to_be_bytes());
        out[9..].copy_from_slice(&(self.bytes as u32).to_be_bytes());
        Ok(out)
    }
    pub(crate) fn decode(raw: [u8; HEADER_BYTES], maximum: usize) -> Result<Self, Error> {
        let h = Self {
            kind: raw[0],
            id: u64::from_be_bytes(raw[1..9].try_into().unwrap()),
            bytes: u32::from_be_bytes(raw[9..].try_into().unwrap()) as usize,
        };
        h.encode(maximum)?;
        Ok(h)
    }
}
// The executable must pass the same Guard installed as its global allocator.
// Bootstrap is fixed-size and decoded on the stack before untrusted frames.
pub struct Session<R, W> {
    input: R,
    output: W,
    maximum: usize,
    next: u64,
    initialized: bool,
    finished: bool,
}
impl<R: Read, W: Write> Session<R, W> {
    pub fn start(mut input: R, output: W, guard: &Guard) -> Result<Self, Error> {
        let mut bytes = [0; BOOT_BYTES];
        input.read_exact(&mut bytes)?;
        let (limit, maximum) = boot(bytes)?;
        guard.arm(limit).map_err(Error::Arm)?;
        Ok(Self {
            input,
            output,
            maximum,
            next: 1,
            initialized: false,
            finished: false,
        })
    }
    fn receive(&mut self, kind: u8, id: u64, maximum: usize) -> Result<Vec<u8>, Error> {
        let mut raw = [0; HEADER_BYTES];
        self.input.read_exact(&mut raw)?;
        let h = Header::decode(raw, maximum.min(self.maximum))?;
        if h.kind != kind || h.id != id {
            return Err(Error::Protocol);
        }
        let mut bytes = vec![0; h.bytes];
        self.input.read_exact(&mut bytes)?;
        Ok(bytes)
    }
    fn send(&mut self, kind: u8, id: u64, bytes: &[u8]) -> Result<(), Error> {
        self.output.write_all(
            &Header {
                kind,
                id,
                bytes: bytes.len(),
            }
            .encode(self.maximum)?,
        )?;
        self.output.write_all(bytes)?;
        self.output.flush()?;
        Ok(())
    }
    pub fn initial(&mut self, maximum: usize) -> Result<Vec<u8>, Error> {
        if self.initialized || self.finished {
            return Err(Error::Protocol);
        }
        let bytes = self.receive(INIT, 0, maximum)?;
        self.initialized = true;
        Ok(bytes)
    }
    pub fn read(&mut self, query: &[u8]) -> Result<Vec<u8>, Error> {
        if !self.initialized || self.finished {
            return Err(Error::Protocol);
        }
        let id = self.next;
        self.next = self.next.checked_add(1).ok_or(Error::Capacity)?;
        self.send(READ, id, query)?;
        self.receive(VALUE, id, self.maximum)
    }
    pub fn finish(&mut self, result: &[u8]) -> Result<(), Error> {
        if !self.initialized || self.finished {
            return Err(Error::Protocol);
        }
        self.finished = true;
        self.send(RESULT, 0, result)
    }
}
