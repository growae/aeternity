use ae_codec::headers::MicroHeader;
use ae_p2p::noise;
use ae_types::{AccountPublicKey, PeerPublicKey};
use std::{path::PathBuf, sync::Arc};
use zeroize::Zeroizing;

mod files;
mod keeper;
mod worker;
pub use worker::{Client, Ticket, Worker};

#[derive(Clone)]
pub struct Config {
    pub directory: PathBuf,
    password: Zeroizing<Vec<u8>>,
    pub requests: usize,
}
impl Config {
    pub fn new(directory: PathBuf, password: Vec<u8>) -> Self {
        Self {
            directory,
            password: Zeroizing::new(password),
            requests: 128,
        }
    }
    fn validate(&self) -> Result<(), Error> {
        if self.directory.as_os_str().is_empty()
            || self.password.len() > 65_536
            || !(1..=65_536).contains(&self.requests)
        {
            Err(Error::Configuration)
        } else {
            Ok(())
        }
    }
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyConfig")
            .field("directory", &self.directory)
            .field("requests", &self.requests)
            .finish_non_exhaustive()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileOperation {
    Directory,
    Lease,
    ReadPublic,
    ReadPrivate,
    WritePublic,
    WritePrivate,
    SyncDirectory,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Configuration,
    AlreadyRunning,
    IncompletePeerFiles,
    InvalidPeerFile,
    InvalidPeerPair,
    Io {
        operation: FileOperation,
        kind: std::io::ErrorKind,
    },
    Entropy,
    KeyNotFound,
    KeyChanged,
    Header,
    Busy,
    Stopped,
    Disconnected,
    Panicked,
    Spawn,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicKeys {
    pub signing: AccountPublicKey,
    pub candidate: AccountPublicKey,
    pub peer: PeerPublicKey,
}
#[derive(Debug, Clone)]
pub struct Info {
    pub running: bool,
    pub accepting: bool,
    pub keys: PublicKeys,
    pub pending: usize,
    pub submitted: u64,
    pub completed: u64,
    pub promotions: u64,
    pub last_error: Option<Error>,
    pub failure: Option<Error>,
}
#[derive(Debug, Clone)]
pub struct SignedMicro {
    pub header: MicroHeader,
    pub signer: AccountPublicKey,
}

#[cfg(test)]
mod tests;
