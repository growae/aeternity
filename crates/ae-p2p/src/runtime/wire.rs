use super::*;

pub(super) async fn read(socket: &TcpStream, mut bytes: &mut [u8]) -> Result<(), Error> {
    while !bytes.is_empty() {
        socket.readable().await?;
        match socket.try_read(bytes) {
            Ok(0) => return Err(Error::Io(io::ErrorKind::UnexpectedEof)),
            Ok(n) => bytes = &mut bytes[n..],
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
pub(super) async fn write(socket: &TcpStream, mut bytes: &[u8]) -> Result<(), Error> {
    while !bytes.is_empty() {
        socket.writable().await?;
        match socket.try_write(bytes) {
            Ok(0) => return Err(Error::Io(io::ErrorKind::WriteZero)),
            Ok(n) => bytes = &bytes[n..],
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
pub(super) async fn handshake(
    socket: &TcpStream,
    identity: &noise::Identity,
    network: &noise::Network,
    role: noise::Role,
    timeout: Duration,
) -> Result<noise::Transport, Error> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(Error::Configuration)?;
    tokio::time::timeout_at(deadline, async {
        let mut handshake = noise::Handshake::new(identity, network, role)?;
        while !handshake.is_finished() {
            if handshake.is_my_turn() {
                write(socket, &noise::record::encode(&handshake.write()?)?).await?;
            } else {
                let mut prefix = [0; 2];
                read(socket, &mut prefix).await?;
                let size = usize::from(u16::from_be_bytes(prefix));
                if size < 16 {
                    return Err(Error::Noise(noise::Error::InvalidRecord));
                }
                let mut record = vec![0; size];
                read(socket, &mut record).await?;
                handshake.read(&record)?;
            }
        }
        handshake.finish().map_err(Error::Noise)
    })
    .await
    .map_err(|_| Error::Timeout)?
}

pub(super) struct Outgoing {
    packet: Vec<u8>,
    next: usize,
    record: Vec<u8>,
    written: usize,
    pub deadline: Instant,
    pub answer: Option<queue::Answer>,
    _budget: tokio::sync::OwnedSemaphorePermit,
}
impl Outgoing {
    pub fn new(
        packet: Vec<u8>,
        deadline: Instant,
        answer: Option<queue::Answer>,
        budget: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        Self {
            packet,
            next: 0,
            record: vec![],
            written: 0,
            deadline,
            answer,
            _budget: budget,
        }
    }
    pub fn prepare(&mut self, crypto: &mut noise::Transport) -> Result<(), Error> {
        if self.written != self.record.len() || self.next == self.packet.len() {
            return Ok(());
        }
        let plaintext = if self.packet.len() <= framing::MAX_PAYLOAD {
            self.next = self.packet.len();
            self.packet.clone()
        } else {
            let total = self.packet.len().div_ceil(framing::MAX_FRAGMENT);
            let number = self.next / framing::MAX_FRAGMENT + 1;
            let end = (self.next + framing::MAX_FRAGMENT).min(self.packet.len());
            let mut bytes = vec![0, 0];
            bytes.extend_from_slice(&(number as u16).to_be_bytes());
            bytes.extend_from_slice(&(total as u16).to_be_bytes());
            bytes.extend_from_slice(&self.packet[self.next..end]);
            self.next = end;
            bytes
        };
        self.record = noise::record::encode(&crypto.seal(&plaintext)?)?;
        self.written = 0;
        Ok(())
    }
    pub fn write(&mut self, socket: &TcpStream) -> Result<bool, Error> {
        match socket.try_write(&self.record[self.written..]) {
            Ok(0) => return Err(Error::Io(io::ErrorKind::WriteZero)),
            Ok(n) => self.written += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
            Err(e) => return Err(e.into()),
        }
        Ok(self.written == self.record.len() && self.next == self.packet.len())
    }
}
