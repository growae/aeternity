use super::{Error, Mapping, Ports, Protocol, Settings};
use std::net::{Ipv4Addr, SocketAddrV4};
use tokio::{
    net::UdpSocket,
    time::{Instant, timeout, timeout_at},
};

pub(super) fn mapping_request(ports: Ports, lifetime: u32) -> [u8; 12] {
    let mut message = [0; 12];
    message[1] = 2; // TCP
    message[4..6].copy_from_slice(&ports.internal.to_be_bytes());
    message[6..8].copy_from_slice(&ports.external.to_be_bytes());
    message[8..].copy_from_slice(&lifetime.to_be_bytes());
    message
}
fn prefix(raw: &[u8], opcode: u8) -> Result<u32, Error> {
    if raw.len() < 8 || raw[0] != 0 || raw[1] != opcode + 128 {
        return Err(Error::Response);
    }
    let status = u16::from_be_bytes([raw[2], raw[3]]);
    if status != 0 {
        return Err(Error::NatPmp(status));
    }
    Ok(u32::from_be_bytes(raw[4..8].try_into().expect("prefix")))
}
pub(super) fn external_response(raw: &[u8]) -> Result<Ipv4Addr, Error> {
    prefix(raw, 0)?;
    if raw.len() != 12 {
        return Err(Error::Response);
    }
    Ok(Ipv4Addr::new(raw[8], raw[9], raw[10], raw[11]))
}
pub(super) fn mapping_response(raw: &[u8], ports: Ports, deleting: bool) -> Result<Mapping, Error> {
    let epoch = prefix(raw, 2)?;
    if raw.len() != 16 {
        return Err(Error::Response);
    }
    let internal = u16::from_be_bytes([raw[8], raw[9]]);
    let external = u16::from_be_bytes([raw[10], raw[11]]);
    let lifetime = u32::from_be_bytes(raw[12..].try_into().expect("mapping lifetime"));
    if internal != ports.internal
        || (deleting && (external != 0 || lifetime != 0))
        || (!deleting && (external == 0 || lifetime == 0))
    {
        return Err(Error::Response);
    }
    Ok(Mapping {
        protocol: Protocol::NatPmp,
        internal,
        external,
        lifetime: Some(lifetime),
        gateway_epoch: Some(epoch),
    })
}

async fn rpc(
    gateway: SocketAddrV4,
    message: &[u8],
    settings: &Settings,
) -> Result<(Vec<u8>, Ipv4Addr), Error> {
    timeout(settings.request_timeout, async {
        let socket = UdpSocket::bind(SocketAddrV4::new(settings.bind, 0)).await?;
        // Connected UDP authenticates both source IP and port, unlike the
        // reference's source-IP-only check. No detached receive task survives.
        socket.connect(gateway).await?;
        for attempt in 0..settings.attempts {
            socket.send(message).await?;
            let deadline = Instant::now() + settings.initial_retry * (1u32 << attempt);
            loop {
                let mut packet = [0u8; 64];
                let n = match timeout_at(deadline, socket.recv(&mut packet)).await {
                    Ok(result) => result?,
                    Err(_) => break,
                };
                // Delayed replies to a different operation do not complete it.
                if n < 2 || packet[1] != message[1] + 128 {
                    continue;
                }
                let local = match socket.local_addr()? {
                    std::net::SocketAddr::V4(a) => *a.ip(),
                    _ => return Err(Error::Address),
                };
                return Ok((packet[..n].to_vec(), local));
            }
        }
        Err(Error::Timeout)
    })
    .await
    .map_err(|_| Error::Timeout)?
}

pub(super) async fn discover(
    gateway: SocketAddrV4,
    settings: &Settings,
) -> Result<Ipv4Addr, Error> {
    let (raw, local) = rpc(gateway, &[0, 0], settings).await?;
    external_response(&raw)?;
    Ok(local)
}
pub(super) async fn map(
    gateway: SocketAddrV4,
    ports: Ports,
    lifetime: u32,
    settings: &Settings,
) -> Result<Mapping, Error> {
    ports.validate()?;
    let deleting = lifetime == 0;
    let request = mapping_request(
        if deleting {
            Ports {
                external: 0,
                ..ports
            }
        } else {
            ports
        },
        lifetime,
    );
    mapping_response(&rpc(gateway, &request, settings).await?.0, ports, deleting)
}
