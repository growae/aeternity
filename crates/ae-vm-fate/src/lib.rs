#![forbid(unsafe_code)]
pub mod abi;
pub mod code;
pub mod diagnostic;
pub mod encoding;
pub mod gas;
pub mod machine;
pub mod maps;
pub mod opcodes;
pub mod stack;
pub mod store;
pub mod types;
pub mod unicode;
pub mod value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Truncated,
    InvalidEncoding,
    NonCanonical,
    TrailingBytes,
    InvalidValue,
    InvalidType,
    InvalidMapKey,
    UnorderedValue,
    InvalidOrderProfile,
    ResourceLimit,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

// Caller budgets are operational limits, never consensus rejection criteria.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_depth: usize,
    pub max_nodes: usize,
    pub max_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_depth: 256,
            max_nodes: 1_000_000,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}
struct Budget {
    limits: Limits,
    nodes: usize,
}
impl Budget {
    fn new(limits: Limits) -> Self {
        Self { limits, nodes: 0 }
    }
    fn visit(&mut self, depth: usize) -> Result<(), Error> {
        if depth > self.limits.max_depth || self.nodes >= self.limits.max_nodes {
            return Err(Error::ResourceLimit);
        }
        self.nodes += 1;
        Ok(())
    }
}
