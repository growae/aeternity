use ae_codec::api::{self, ApiType};
use ae_types::Hash256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    Any,
    Key,
    Micro,
    Unknown(Vec<u8>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Key,
    Micro,
    Blocks { count: i64, kind: Kind },
    EmptyPool,
    Until { hashes: Vec<Vec<u8>>, maximum: u64 },
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub blocks: usize,
    pub hashes: usize,
    pub hash_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            blocks: 1024,
            hashes: 1024,
            hash_bytes: 128 * 1024,
        }
    }
}
pub trait Host {
    type Block: Clone;
    type Error;
    fn is_leader(&mut self) -> Result<bool, Self::Error>;
    fn key(&mut self) -> Result<Self::Block, Self::Error>;
    fn micro(&mut self) -> Result<Self::Block, Self::Error>;
    fn pool_empty(&mut self) -> Result<bool, Self::Error>;
    fn top(&mut self) -> Result<Self::Block, Self::Error>;
    fn transactions(&self, block: &Self::Block) -> Vec<Hash256>;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply<B> {
    Block(B),
    Blocks(Vec<B>),
    MaxReached,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error<E> {
    Host(E),
    FunctionClause,
    UnknownKind { leader: bool, kind: Vec<u8> },
    CouldNotMineTransaction,
    ResourceLimit,
}
#[derive(Debug)]
pub struct Report<B, E> {
    // Includes acknowledged blocks omitted from the native return value.
    // A host error may additionally retain a submission with unknown outcome.
    pub produced: Vec<B>,
    pub result: Result<Reply<B>, Error<E>>,
}
struct Run<'a, H: Host> {
    host: &'a mut H,
    produced: Vec<H::Block>,
    limits: Limits,
}
impl<H: Host> Run<'_, H> {
    fn emit(&mut self, key: bool) -> Result<H::Block, Error<H::Error>> {
        if self.produced.len() >= self.limits.blocks {
            return Err(Error::ResourceLimit);
        }
        let b = if key {
            self.host.key()
        } else {
            self.host.micro()
        }
        .map_err(Error::Host)?;
        self.produced.push(b.clone());
        Ok(b)
    }
    fn sequence(&mut self, n: i64, key: bool) -> Result<Vec<H::Block>, Error<H::Error>> {
        if n < 0 {
            return Err(Error::FunctionClause);
        }
        let mut out = vec![];
        for _ in 0..n {
            out.push(self.emit(key)?);
        }
        Ok(out)
    }
    fn command(&mut self, command: Command) -> Result<Reply<H::Block>, Error<H::Error>> {
        Ok(match command {
            Command::Key => Reply::Block(self.emit(true)?),
            Command::Micro => Reply::Block(self.emit(false)?),
            Command::Blocks { count, kind } => {
                let leader = self.host.is_leader().map_err(Error::Host)?;
                let out = match kind {
                    Kind::Any => {
                        let pairs = count / 2;
                        if pairs < 0 {
                            return Err(Error::FunctionClause);
                        }
                        let mut out = vec![];
                        for _ in 0..pairs {
                            out.push(self.emit(true)?);
                            out.push(self.emit(false)?);
                        }
                        out.extend(self.sequence(count % 2, true)?);
                        out
                    }
                    Kind::Key => self.sequence(count, true)?,
                    Kind::Micro => {
                        if !leader {
                            self.emit(true)?;
                        }
                        self.sequence(count, false)?
                    }
                    Kind::Unknown(kind) => return Err(Error::UnknownKind { leader, kind }),
                };
                Reply::Blocks(out)
            }
            Command::EmptyPool => {
                if self.host.pool_empty().map_err(Error::Host)? {
                    return Ok(Reply::Blocks(vec![self.host.top().map_err(Error::Host)?]));
                }
                let mut out = vec![];
                if !self.host.is_leader().map_err(Error::Host)? {
                    out.push(self.emit(true)?);
                }
                for _ in 0..10 {
                    out.push(self.emit(false)?);
                    if self.host.pool_empty().map_err(Error::Host)? {
                        return Ok(Reply::Blocks(out));
                    }
                }
                return Err(Error::CouldNotMineTransaction);
            }
            Command::Until {
                mut hashes,
                maximum,
            } => {
                if hashes.len() > self.limits.hashes
                    || hashes
                        .iter()
                        .try_fold(0usize, |n, h| n.checked_add(h.len()))
                        .is_none_or(|n| n > self.limits.hash_bytes)
                {
                    return Err(Error::ResourceLimit);
                }
                let mut out = vec![];
                for _ in 0..maximum {
                    if !self.host.is_leader().map_err(Error::Host)? {
                        out.push(self.emit(true)?);
                        continue;
                    }
                    let micro = self.emit(false)?;
                    out.push(self.emit(true)?);
                    let transactions = self.host.transactions(&micro);
                    hashes.retain(|h| {
                        !transactions.iter().any(|t| {
                            h.as_slice() == t.0
                                || h.as_slice() == api::encode(ApiType::TxHash, &t.0).as_bytes()
                        })
                    });
                    if hashes.is_empty() {
                        return Ok(Reply::Blocks(out));
                    }
                }
                Reply::MaxReached
            }
        })
    }
}
// The sequencer has no clock, I/O or implicit retry. The owned adapter performs
// one actual effect per call and retains any uncertain submission in its error.
pub fn run<H: Host>(command: Command, host: &mut H, limits: Limits) -> Report<H::Block, H::Error> {
    let mut run = Run {
        host,
        produced: vec![],
        limits,
    };
    let result = run.command(command);
    Report {
        produced: run.produced,
        result,
    }
}
