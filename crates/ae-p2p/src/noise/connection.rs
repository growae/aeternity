use super::{Error as NoiseError, Handshake, Identity, MAX_CIPHERTEXT, Network, Role, Transport};
use ae_types::PeerPublicKey;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::time::Instant;

#[derive(Debug)]
pub enum Error {
    Noise(NoiseError),
    Io(io::Error),
    Timeout,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noise(error) => write!(f, "Noise: {error}"),
            Self::Io(error) => write!(f, "I/O: {error}"),
            Self::Timeout => f.write_str("connection deadline elapsed"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Noise(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::Timeout => None,
        }
    }
}
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        if matches!(
            error.kind(),
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
        ) {
            Self::Timeout
        } else {
            Self::Io(error)
        }
    }
}
impl From<NoiseError> for Error {
    fn from(error: NoiseError) -> Self {
        Self::Noise(error)
    }
}

struct Socket(TcpStream);
impl Drop for Socket {
    fn drop(&mut self) {
        let _ = self.0.shutdown(Shutdown::Both);
    }
}

pub struct Connection {
    socket: Socket,
    transport: Transport,
}
impl Connection {
    pub fn handshake(
        stream: TcpStream,
        identity: &Identity,
        network: &Network,
        role: Role,
        deadline: Instant,
    ) -> Result<Self, Error> {
        let mut socket = Socket(stream);
        socket.0.set_nonblocking(false)?;
        socket.0.set_nodelay(true)?;
        let mut handshake = Handshake::new(identity, network, role)?;
        while !handshake.is_finished() {
            if handshake.is_my_turn() {
                let record = handshake.write()?;
                write_record(&mut socket.0, &record, deadline)?;
            } else {
                handshake.read(&read_record(&mut socket.0, deadline)?)?;
            }
        }
        Ok(Self {
            socket,
            transport: handshake.finish()?,
        })
    }
    pub fn peer(&self) -> PeerPublicKey {
        self.transport.peer()
    }
    pub fn handshake_hash(&self) -> &[u8; 64] {
        self.transport.handshake_hash()
    }
    pub fn is_closed(&self) -> bool {
        self.transport.is_closed()
    }
    pub fn close(&mut self) {
        self.transport.close();
        let _ = self.socket.0.shutdown(Shutdown::Both);
    }
    pub fn send(&mut self, plaintext: &[u8], deadline: Instant) -> Result<(), Error> {
        let ciphertext = match self.transport.seal(plaintext) {
            Ok(ciphertext) => ciphertext,
            Err(error) => {
                if self.transport.is_closed() {
                    self.close();
                }
                return Err(error.into());
            }
        };
        match write_record(&mut self.socket.0, &ciphertext, deadline) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.close();
                Err(error)
            }
        }
    }
    pub fn receive(&mut self, deadline: Instant) -> Result<Vec<u8>, Error> {
        if self.is_closed() {
            return Err(NoiseError::Closed.into());
        }
        let result = read_record(&mut self.socket.0, deadline)
            .and_then(|r| self.transport.open(&r).map_err(Error::from));
        if result.is_err() {
            self.close();
        }
        result
    }
}
fn remaining(deadline: Instant) -> Result<std::time::Duration, Error> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or(Error::Timeout)
}
fn read_exact(
    stream: &mut TcpStream,
    mut buffer: &mut [u8],
    deadline: Instant,
) -> Result<(), Error> {
    while !buffer.is_empty() {
        stream.set_read_timeout(Some(remaining(deadline)?))?;
        match stream.read(buffer) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
            Ok(n) => buffer = &mut buffer[n..],
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
fn read_record(stream: &mut TcpStream, deadline: Instant) -> Result<Vec<u8>, Error> {
    let mut prefix = [0; 2];
    read_exact(stream, &mut prefix, deadline)?;
    let length = usize::from(u16::from_be_bytes(prefix));
    if !(16..=MAX_CIPHERTEXT).contains(&length) {
        return Err(NoiseError::InvalidRecord.into());
    }
    let mut ciphertext = vec![0; length];
    read_exact(stream, &mut ciphertext, deadline)?;
    Ok(ciphertext)
}
fn write_record(stream: &mut TcpStream, ciphertext: &[u8], deadline: Instant) -> Result<(), Error> {
    let packet = super::record::encode(ciphertext)?;
    let mut remaining_bytes = packet.as_slice();
    while !remaining_bytes.is_empty() {
        stream.set_write_timeout(Some(remaining(deadline)?))?;
        match stream.write(remaining_bytes) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero).into()),
            Ok(n) => remaining_bytes = &remaining_bytes[n..],
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
