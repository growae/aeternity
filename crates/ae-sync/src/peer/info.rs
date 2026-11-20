use ae_p2p::message::{Error, Kind, Limits, Message, Value};
use ae_types::Unsigned;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub version: Vec<u8>,
    pub revision: Vec<u8>,
    pub vendor: Vec<u8>,
    pub os: Vec<u8>,
}
impl Metadata {
    pub fn current() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").as_bytes().to_vec(),
            // Release builders can supply a verified revision. An unavailable
            // revision is empty, as in the original missing-REVISION behavior.
            revision: option_env!("AETERNITY_REVISION")
                .unwrap_or("")
                .as_bytes()
                .to_vec(),
            vendor: b"GrowAE Rust node".to_vec(),
            os: format!("{}:{}", std::env::consts::FAMILY, std::env::consts::OS).into_bytes(),
        }
    }
    pub(super) fn validate(&self) -> Result<(), Error> {
        if [&self.version, &self.revision, &self.vendor, &self.os]
            .iter()
            .any(|value| value.len() > 1024)
        {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
}
// Disabled sharing intentionally has no response, including no error envelope.
// The runtime must skip its live count query in this case as well.
pub fn response(
    metadata: Option<&Metadata>,
    network_id: &[u8],
    verified: &Unsigned,
    unverified: &Unsigned,
    limits: Limits,
) -> Result<Option<Message>, Error> {
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let body = Message::new(
        Kind::NodeInfo,
        1,
        BTreeMap::from([
            ("version".into(), Value::Binary(metadata.version.clone())),
            ("revision".into(), Value::Binary(metadata.revision.clone())),
            ("vendor".into(), Value::Binary(metadata.vendor.clone())),
            ("os".into(), Value::Binary(metadata.os.clone())),
            ("network_id".into(), Value::Binary(network_id.to_vec())),
            ("verified_peers".into(), Value::Integer(verified.clone())),
            (
                "unverified_peers".into(),
                Value::Integer(unverified.clone()),
            ),
        ]),
        limits,
    )?;
    Message::response_ok(&body, limits).map(Some)
}
