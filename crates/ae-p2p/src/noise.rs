use ae_types::{Hash256, PeerPublicKey};
use curve25519_dalek::montgomery::MontgomeryPoint;
use std::fmt;
use zeroize::Zeroizing;

pub mod connection;
pub mod record;
mod resolver;

pub const PROTOCOL: &str = "Noise_XK_25519_ChaChaPoly_BLAKE2b";
pub const MAX_CIPHERTEXT: usize = u16::MAX as usize;
pub const MAX_PLAINTEXT: usize = crate::framing::MAX_PAYLOAD;
pub const MAX_NETWORK_ID: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Configuration,
    ResourceLimit,
    InvalidRecord,
    WrongStep,
    Incomplete,
    Crypto,
    Closed,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

pub struct Identity {
    secret: Zeroizing<[u8; 32]>,
    public: PeerPublicKey,
}
impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity")
            .field("public", &self.public)
            .finish_non_exhaustive()
    }
}
impl Identity {
    pub fn from_secret(secret: [u8; 32]) -> Self {
        let public = PeerPublicKey(MontgomeryPoint::mul_base_clamped(secret).to_bytes());
        Self {
            secret: Zeroizing::new(secret),
            public,
        }
    }
    pub fn generate() -> Result<Self, Error> {
        let pair = snow::Builder::with_resolver(
            PROTOCOL.parse().map_err(|_| Error::Configuration)?,
            Box::new(resolver::Resolver),
        )
        .generate_keypair()
        .map_err(|_| Error::Crypto)?;
        let private = Zeroizing::new(pair.private);
        let secret = private
            .as_slice()
            .try_into()
            .map_err(|_| Error::Configuration)?;
        Ok(Self::from_secret(secret))
    }
    pub fn public_key(&self) -> PeerPublicKey {
        self.public
    }
}

pub struct Network(Vec<u8>);
impl Network {
    pub fn new(version: u64, genesis: Hash256, network_id: &[u8]) -> Result<Self, Error> {
        if network_id.len() > MAX_NETWORK_ID {
            return Err(Error::ResourceLimit);
        }
        let mut bytes = Vec::with_capacity(40 + network_id.len());
        bytes.extend_from_slice(&version.to_be_bytes());
        bytes.extend_from_slice(genesis.as_ref());
        bytes.extend_from_slice(network_id);
        Ok(Self(bytes))
    }
    pub fn prologue(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Initiator(PeerPublicKey),
    Responder,
}

pub struct Handshake {
    state: Option<snow::HandshakeState>,
}
impl Handshake {
    pub fn new(identity: &Identity, network: &Network, role: Role) -> Result<Self, Error> {
        Self::build(Self::builder(identity, network, &role)?, role)
    }
    fn builder<'a>(
        identity: &'a Identity,
        network: &'a Network,
        role: &'a Role,
    ) -> Result<snow::Builder<'a>, Error> {
        let mut builder = snow::Builder::with_resolver(
            PROTOCOL.parse().map_err(|_| Error::Configuration)?,
            Box::new(resolver::Resolver),
        )
        .local_private_key(identity.secret.as_ref())
        .map_err(|_| Error::Configuration)?
        .prologue(network.prologue())
        .map_err(|_| Error::Configuration)?;
        if let Role::Initiator(remote) = role {
            builder = builder
                .remote_public_key(remote.as_ref())
                .map_err(|_| Error::Configuration)?;
        }
        Ok(builder)
    }
    fn build(builder: snow::Builder<'_>, role: Role) -> Result<Self, Error> {
        let state = match role {
            Role::Initiator(_) => builder.build_initiator(),
            Role::Responder => builder.build_responder(),
        }
        .map_err(|_| Error::Crypto)?;
        Ok(Self { state: Some(state) })
    }
    pub fn is_finished(&self) -> bool {
        self.state
            .as_ref()
            .is_some_and(snow::HandshakeState::is_handshake_finished)
    }
    pub fn is_my_turn(&self) -> bool {
        self.state
            .as_ref()
            .is_some_and(snow::HandshakeState::is_my_turn)
            && !self.is_finished()
    }
    pub fn is_closed(&self) -> bool {
        self.state.is_none()
    }
    pub fn write(&mut self) -> Result<Vec<u8>, Error> {
        let state = self.state.as_mut().ok_or(Error::Closed)?;
        if state.is_handshake_finished() || !state.is_my_turn() {
            return Err(Error::WrongStep);
        }
        let mut out = vec![0; 96];
        match state.write_message(&[], &mut out) {
            Ok(length) => {
                out.truncate(length);
                Ok(out)
            }
            Err(_) => {
                self.state = None;
                Err(Error::Crypto)
            }
        }
    }
    pub fn read(&mut self, message: &[u8]) -> Result<(), Error> {
        let state = self.state.as_mut().ok_or(Error::Closed)?;
        if state.is_handshake_finished() || state.is_my_turn() {
            return Err(Error::WrongStep);
        }
        if !(16..=MAX_CIPHERTEXT).contains(&message.len()) {
            self.state = None;
            return Err(Error::InvalidRecord);
        }
        let mut payload = Zeroizing::new(vec![0; message.len()]);
        match state.read_message(message, &mut payload) {
            Ok(_) => Ok(()),
            Err(_) => {
                self.state = None;
                Err(Error::Crypto)
            }
        }
    }
    pub fn finish(mut self) -> Result<Transport, Error> {
        let state = self.state.take().ok_or(Error::Closed)?;
        if !state.is_handshake_finished() {
            return Err(Error::Incomplete);
        }
        let peer = PeerPublicKey::try_from(state.get_remote_static().ok_or(Error::Crypto)?)
            .map_err(|_| Error::Crypto)?;
        let hash: [u8; 64] = state
            .get_handshake_hash()
            .try_into()
            .map_err(|_| Error::Crypto)?;
        Ok(Transport {
            state: Some(state.into_transport_mode().map_err(|_| Error::Crypto)?),
            peer,
            hash,
        })
    }
}

pub struct Transport {
    state: Option<snow::TransportState>,
    peer: PeerPublicKey,
    hash: [u8; 64],
}
impl Transport {
    pub fn peer(&self) -> PeerPublicKey {
        self.peer
    }
    pub fn handshake_hash(&self) -> &[u8; 64] {
        &self.hash
    }
    pub fn is_closed(&self) -> bool {
        self.state.is_none()
    }
    pub fn close(&mut self) {
        self.state = None;
    }
    pub fn seal(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let state = self.state.as_mut().ok_or(Error::Closed)?;
        if plaintext.len() > MAX_PLAINTEXT {
            return Err(Error::ResourceLimit);
        }
        let mut out = vec![0; plaintext.len() + 16];
        match state.write_message(plaintext, &mut out) {
            Ok(length) => {
                out.truncate(length);
                Ok(out)
            }
            Err(_) => {
                self.close();
                Err(Error::Crypto)
            }
        }
    }
    pub fn open(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, Error> {
        let state = self.state.as_mut().ok_or(Error::Closed)?;
        if !(16..=MAX_CIPHERTEXT).contains(&ciphertext.len()) {
            self.close();
            return Err(Error::InvalidRecord);
        }
        let mut out = vec![0; ciphertext.len() - 16];
        match state.read_message(ciphertext, &mut out) {
            Ok(length) => {
                out.truncate(length);
                Ok(out)
            }
            Err(_) => {
                self.close();
                Err(Error::Crypto)
            }
        }
    }
}

#[cfg(test)]
mod tests;
