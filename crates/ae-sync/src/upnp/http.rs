use super::{Error, Settings};

use tokio::io::{AsyncRead, AsyncWrite};
mod address;
mod transport;
pub(super) use address::Url;
pub(super) use transport::{Endpoint, Transport};

pub(super) use crate::http::{Received, Response};
#[cfg(test)]
pub(super) async fn request(
    url: &Url,
    local: std::net::Ipv4Addr,
    soap: Option<(&str, &str)>,
    settings: &Settings,
) -> Result<Response, Error> {
    let transport = Transport::new(std::sync::Arc::new(settings.clone()))?;
    match soap {
        None => transport.fetch(url, local).await.map(|f| f.response),
        Some((action, body)) => {
            transport
                .post(&transport.resolve(url).await?, local, action, body)
                .await
        }
    }
}

impl From<crate::http::Error> for Error {
    fn from(e: crate::http::Error) -> Self {
        match e {
            crate::http::Error::Http => Self::Http,
            crate::http::Error::Limit => Self::Limit,
            crate::http::Error::Io(k) => Self::Io(k),
        }
    }
}
fn limits(s: &Settings) -> crate::http::Limits {
    crate::http::Limits {
        header_bytes: s.header_bytes,
        body_bytes: s.body_bytes,
    }
}
async fn write<S: AsyncWrite + Unpin>(stream: &mut S, bytes: &[u8]) -> Result<(), Error> {
    crate::http::write(stream, bytes).await.map_err(Into::into)
}
async fn read<S: AsyncRead + Unpin>(
    stream: &mut S,
    settings: &Settings,
) -> Result<Received, Error> {
    crate::http::read(stream, &limits(settings))
        .await
        .map_err(Into::into)
}
#[cfg(test)]
pub(super) fn headers(
    raw: &[u8],
    settings: &Settings,
) -> Result<(u16, usize, crate::http::Framing), Error> {
    crate::http::headers(raw, &limits(settings)).map_err(Into::into)
}
#[cfg(test)]
#[derive(Default)]
pub(super) struct Chunks(crate::http::Chunks);
#[cfg(test)]
impl Chunks {
    pub fn advance(&mut self, raw: &[u8], settings: &Settings) -> Result<Option<Vec<u8>>, Error> {
        self.0.advance(raw, &limits(settings)).map_err(Into::into)
    }
}
