use super::*;
use ae_codec::headers::{Header, HeaderMode};
use ae_storage::Database;

pub(super) fn target(header: &Header) -> Result<Point, Error> {
    Ok(Point {
        height: header.common().height,
        hash: match header {
            Header::Key(_) => header.hash().map_err(|_| Error::Header)?,
            Header::Micro(m) => Hash256(m.common.previous_key_hash),
        },
    })
}
pub(super) fn header(
    message: &Message,
    checkpoints: &BTreeMap<u64, Hash256>,
) -> Result<Header, Error> {
    let Some(Value::Binary(bytes)) = message.fields().get("hdr") else {
        return Err(Error::MalformedResponse);
    };
    let header = Header::decode(bytes, HeaderMode::ProofOfWork).map_err(|_| Error::Header)?;
    // aec_validation:validate_header delegates to the pre-conductor hook.
    // For PoW this checks the whitelist, not standalone PoW/block admission.
    if let Header::Key(k) = &header
        && k.common.height != 0
        && let Some(expected) = checkpoints.get(&k.common.height)
        && header.hash().map_err(|_| Error::Header)? != *expected
    {
        return Err(Error::BlockedByWhitelist);
    }
    Ok(header)
}
pub(super) fn successors(
    message: &Message,
    from: Hash256,
    maximum: usize,
    checkpoints: &BTreeMap<u64, Hash256>,
) -> Result<Vec<Point>, Error> {
    let Some(Value::List(rows)) = message.fields().get("header_hashes") else {
        return Err(Error::MalformedResponse);
    };
    if rows.len() > maximum {
        return Err(Error::ResourceLimit);
    }
    let mut decoded = vec![];
    for row in rows {
        let Value::Binary(bytes) = row else {
            return Err(Error::MalformedResponse);
        };
        // The original response callback filters binaries shorter than a u64.
        if bytes.len() < 8 {
            continue;
        }
        let height = u64::from_be_bytes(bytes[..8].try_into().expect("eight bytes"));
        let hash = &bytes[8..];
        if let Some(expected) = checkpoints.get(&height)
            && hash != expected.0
        {
            return Err(Error::BlockedByWhitelist);
        }
        decoded.push((height, hash));
    }
    if decoded.first().is_some_and(|(_, hash)| *hash == from.0) {
        return Err(Error::NoProgress);
    }
    if decoded
        .windows(2)
        .any(|pair| pair[0].0.checked_add(1) != Some(pair[1].0))
    {
        return Err(Error::NonConsecutive);
    }
    // There is no native predicate tying the first advertised height to the
    // start height. Actual imported block context determines validity.
    decoded
        .into_iter()
        .map(|(height, hash)| {
            Ok(Point {
                height,
                hash: Hash256(hash.try_into().map_err(|_| Error::Hash)?),
            })
        })
        .collect()
}
fn response(message: &Message, kind: Kind) -> Result<&Message, Error> {
    match message.details() {
        Details::Response {
            kind: actual,
            message: Some(Ok(body)),
        } if *actual == kind => Ok(body),
        Details::Response {
            kind: actual,
            message: None,
        } if *actual == kind => {
            let Some(Value::Binary(reason)) = message.fields().get("reason") else {
                return Err(Error::MalformedResponse);
            };
            Err(Error::Refused(reason.clone()))
        }
        Details::Response {
            message: Some(Err(e)),
            ..
        } => Err(Error::Message(e.clone())),
        _ => Err(Error::MalformedResponse),
    }
}
impl<O: MapOrder + Send + Sync + 'static> Context<O> {
    fn weight(&self) -> usize {
        self.config.messages.body_bytes + self.config.messages.work_bytes
    }
    pub(super) async fn live(&self, client: &net::Client) -> Result<(), Error> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if client.is_closed()
            || self
                .network
                .connection(client.clone())?
                .wait()
                .await?
                .is_none()
        {
            return Err(Error::Stale);
        }
        Ok(())
    }
    pub(super) async fn read<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Database) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let db = self.importer.context.db.clone();
        tokio::time::timeout(self.config.timeout, async {
            self.reads
                .submit_wait(self.weight(), move |_| f(&db))
                .await?
                .await?
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
    pub(super) async fn request<T: Send + 'static>(
        &self,
        client: &net::Client,
        kind: Kind,
        fields: BTreeMap<String, Value>,
        expected: Kind,
        decode: impl FnOnce(&Message, &Database) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let limits = self.config.messages;
        let driver = client.message_limits();
        if driver.body_bytes > limits.body_bytes
            || driver.work_bytes > limits.work_bytes
            || driver.items > limits.items
            || driver.depth > limits.depth
        {
            return Err(Error::Configuration);
        }
        tokio::time::timeout(self.config.timeout, async {
            let reserved = self.reads.reserve_wait(self.weight()).await?;
            self.live(client).await?;
            let request = Message::new(kind, kind.latest(1)?, fields, limits)?;
            let net::Reply::Response(received) = client.request(&request, None)?.await? else {
                return Err(Error::MalformedResponse);
            };
            self.live(client).await?;
            if received.generation != *client.generation() {
                return Err(Error::Stale);
            }
            let db = self.importer.context.db.clone();
            let result = reserved
                .submit(move |_| {
                    received.message().encode_packet(limits)?;
                    let body = response(received.message(), expected)?;
                    decode(body, &db)
                })?
                .await??;
            self.live(client).await?;
            Ok(result)
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
    pub(super) async fn header_by_hash(
        &self,
        client: &net::Client,
        hash: Hash256,
    ) -> Result<Header, Error> {
        let checkpoints = self.importer.context.rules.checkpoints.clone();
        self.request(
            client,
            Kind::GetHeaderByHash,
            BTreeMap::from([("hash".into(), Value::Binary(hash.0.to_vec()))]),
            Kind::Header,
            move |m, _| header(m, &checkpoints),
        )
        .await
    }
    pub(super) async fn header_by_height(
        &self,
        client: &net::Client,
        height: u64,
        top: Hash256,
    ) -> Result<Hash256, Error> {
        let checkpoints = self.importer.context.rules.checkpoints.clone();
        self.request(
            client,
            Kind::GetHeaderByHeight,
            BTreeMap::from([
                ("height".into(), Value::Integer(height.into())),
                ("top_hash".into(), Value::Binary(top.0.to_vec())),
            ]),
            Kind::Header,
            move |m, _| header(m, &checkpoints)?.hash().map_err(|_| Error::Header),
        )
        .await
    }
    pub(super) async fn successors(
        &self,
        client: &net::Client,
        from: Hash256,
        target: Hash256,
    ) -> Result<Vec<Point>, Error> {
        let checkpoints = self.importer.context.rules.checkpoints.clone();
        let maximum = self.config.maximum_successors;
        self.request(
            client,
            Kind::GetSuccessors,
            BTreeMap::from([
                ("from_hash".into(), Value::Binary(from.0.to_vec())),
                ("target_hash".into(), Value::Binary(target.0.to_vec())),
                (
                    "n".into(),
                    Value::Integer((self.config.headers as u64).into()),
                ),
            ]),
            Kind::HeaderHashes,
            move |m, _| successors(m, from, maximum, &checkpoints),
        )
        .await
    }
    pub(super) async fn stored_key(&self, hash: Hash256) -> Result<Option<Point>, Error> {
        self.read(move |db| {
            let view = db.view()?;
            let Some(block) = view.block(hash)? else {
                return Ok(None);
            };
            if !matches!(block, ae_chain::block::Block::Key(_)) {
                return Err(Error::KeyExpected);
            }
            view.require_record(hash)?;
            Ok(Some(Point {
                height: block.header().common().height,
                hash,
            }))
        })
        .await
    }
    pub(super) async fn anchor(&self, hash: Hash256) -> Result<Point, Error> {
        self.read(move |db| {
            let view = db.view()?;
            let block = view.require_block(hash)?;
            view.require_record(hash)?;
            Ok(Point {
                height: block.header().common().height,
                hash,
            })
        })
        .await
    }
    pub(super) async fn selected(&self) -> Result<Point, Error> {
        self.read(|db| {
            let view = db.view()?;
            let hash = view.status().tip.ok_or(repository::Error::Uninitialized)?;
            Ok(Point {
                height: view.require_block(hash)?.header().common().height,
                hash,
            })
        })
        .await
    }
}
